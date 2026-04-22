//! Top-level search execution: filter → sort/pagination → hydrate hits.

use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde_json::Value as J;

use arkimedb_core::Result;
use arkimedb_storage::Collection;

use crate::predicate::compile_es_query;
use crate::aggs::{compile_aggs, run_aggs};

/// Accept usize as either a JSON number or a numeric string (e.g. `"2"`),
/// matching how Elasticsearch tolerates legacy clients that quote sizes.
fn de_usize_lenient<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<usize, D::Error> {
    use serde::de::{self, Visitor};
    use std::fmt;
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = usize;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result { f.write_str("usize or numeric string") }
        fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<usize, E> { Ok(v as usize) }
        fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<usize, E> {
            if v < 0 { Err(E::custom("negative size")) } else { Ok(v as usize) }
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<usize, E> {
            if v < 0.0 { Err(E::custom("negative size")) } else { Ok(v as usize) }
        }
        fn visit_str<E: de::Error>(self, s: &str) -> std::result::Result<usize, E> {
            s.trim().parse::<usize>().map_err(|e| E::custom(e))
        }
        fn visit_string<E: de::Error>(self, s: String) -> std::result::Result<usize, E> { self.visit_str(&s) }
    }
    d.deserialize_any(V)
}

#[derive(Debug, Default, Clone)]
pub struct SortSpec {
    pub field: String,
    pub ascending: bool,
    pub missing_last: bool,
}

impl serde::Serialize for SortSpec {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(Some(1))?;
        m.serialize_entry(&self.field, &serde_json::json!({
            "order": if self.ascending { "asc" } else { "desc" },
            "missing": if self.missing_last { "_last" } else { "_first" },
        }))?;
        m.end()
    }
}

/// Elasticsearch sort clauses take many shapes inside a single array:
///   "field"                                       -> asc
///   { "field": "asc" | "desc" }                   -> direction only
///   { "field": { "order": "asc"|"desc",
///                "missing": "_first"|"_last" } }  -> full form
/// We accept all of them.
impl<'de> serde::Deserialize<'de> for SortSpec {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let v = J::deserialize(d)?;
        sort_from_json(v).map_err(serde::de::Error::custom)
    }
}

fn sort_from_json(v: J) -> std::result::Result<SortSpec, String> {
    match v {
        J::String(field) => Ok(SortSpec { field, ascending: true, missing_last: true }),
        J::Object(map) => {
            // Take first (and only) key/value.
            let (field, spec) = map.into_iter().next()
                .ok_or_else(|| "empty sort object".to_string())?;
            let (ascending, missing_last) = match spec {
                J::String(order) => (order.eq_ignore_ascii_case("asc"), true),
                J::Object(inner) => {
                    let asc = inner.get("order").and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("asc"))
                        .unwrap_or(true);
                    let missing_last = inner.get("missing").and_then(|v| v.as_str())
                        .map(|s| !s.eq_ignore_ascii_case("_first"))
                        .unwrap_or(true);
                    (asc, missing_last)
                }
                other => return Err(format!("unsupported sort spec value: {other}")),
            };
            Ok(SortSpec { field, ascending, missing_last })
        }
        other => Err(format!("unsupported sort clause: {other}")),
    }
}

/// Elasticsearch `sort` can be either a single clause or an array of clauses.
/// We normalize both to `Vec<SortSpec>`.
#[derive(Debug, Clone, Default)]
pub struct SortClauses(pub Vec<SortSpec>);

impl std::ops::Deref for SortClauses {
    type Target = Vec<SortSpec>;
    fn deref(&self) -> &Vec<SortSpec> { &self.0 }
}

impl serde::Serialize for SortClauses {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl<'de> serde::Deserialize<'de> for SortClauses {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let v = J::deserialize(d)?;
        let specs = match v {
            J::Array(arr) => arr.into_iter().map(sort_from_json)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(serde::de::Error::custom)?,
            J::Object(m) if m.is_empty() => vec![],
            J::Null => vec![],
            other => vec![sort_from_json(other).map_err(serde::de::Error::custom)?],
        };
        Ok(SortClauses(specs))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SearchRequest {
    pub query: Option<J>,
    #[serde(deserialize_with = "de_usize_lenient")]
    pub from: usize,
    #[serde(default = "default_size", deserialize_with = "de_usize_lenient")]
    pub size: usize,
    pub sort: Option<SortClauses>,
    #[serde(alias = "aggregations")]
    pub aggs: Option<J>,
    pub track_total_hits: Option<J>,
    pub _source: Option<J>,
    pub fields: Option<Vec<J>>,
    pub docvalue_fields: Option<Vec<J>>,
    pub stored_fields: Option<Vec<J>>,
    #[serde(skip)]
    pub _extra: (),
}

impl Default for SearchRequest {
    fn default() -> Self {
        Self {
            query: None,
            from: 0,
            size: default_size(),
            sort: None,
            aggs: None,
            track_total_hits: None,
            _source: None,
            fields: None,
            docvalue_fields: None,
            stored_fields: None,
            _extra: (),
        }
    }
}

fn default_size() -> usize { 10 }

#[derive(Debug, Serialize)]
pub struct Hit {
    pub _index: String,
    pub _id: String,
    pub _version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _source: Option<J>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<serde_json::Map<String, J>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<J>>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub took: u64,
    pub hits: Hits,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<serde_json::Map<String, J>>,
}

#[derive(Debug, Serialize)]
pub struct Hits {
    pub total: TotalHits,
    pub max_score: Option<f64>,
    pub hits: Vec<Hit>,
}

#[derive(Debug, Serialize)]
pub struct TotalHits {
    pub value: u64,
    pub relation: &'static str,
}

pub fn execute(cols: &[Arc<Collection>], req: &SearchRequest) -> Result<SearchResponse> {
    let t0 = std::time::Instant::now();
    let q_json = req.query.clone().unwrap_or_else(|| serde_json::json!({"match_all": {}}));

    // Compile + evaluate per collection (Phase A — server-side merge at the end).
    let mut all_hits: Vec<(Arc<Collection>, u32)> = Vec::new();
    let mut total: u64 = 0;
    for col in cols {
        let q = compile_es_query(&q_json, col)?;
        let _reidx = col.reindex_lock.read();
        let bm = q.eval(col)?;
        drop(_reidx);
        total += bm.len();
        for r in bm.iter() { all_hits.push((col.clone(), r)); }
    }

    // Aggregations (computed over the full matching set, not the page).
    let aggs_result = if let Some(aj) = &req.aggs {
        let spec = compile_aggs(aj)?;
        Some(run_aggs(cols, &q_json, &spec)?)
    } else { None };

    // Sort.
    if let Some(sort) = &req.sort {
        sort_hits(&mut all_hits, &sort.0)?;
    }

    // Pagination.
    let end = (req.from + req.size).min(all_hits.len());
    let start = req.from.min(end);
    let slice = &all_hits[start..end];

    // Hydrate. Group hits by collection and batch-load via a single read tx
    // per collection to avoid `begin_read()` * 2 per hit.
    let mut hits: Vec<Hit> = (0..slice.len()).map(|_| Hit {
        _index: String::new(), _id: String::new(), _version: 1,
        _source: None, fields: None, sort: None,
    }).collect();
    // Collect requested field names (stringified) from fields / docvalue_fields / stored_fields.
    let field_names: Vec<String> = collect_field_names(req);
    let want_fields = !field_names.is_empty();
    let source_enabled = !matches!(&req._source, Some(J::Bool(false)));
    let sort_clauses: &[SortSpec] = req.sort.as_ref().map(|s| s.0.as_slice()).unwrap_or(&[]);
    let want_sort_vals = !sort_clauses.is_empty();
    let want_raw = want_fields || source_enabled || want_sort_vals;

    // Group slice indices by collection (preserving original order on emit).
    let mut by_col: ahash::AHashMap<*const Collection, (Arc<Collection>, Vec<usize>)> = ahash::AHashMap::new();
    for (i, (col, _)) in slice.iter().enumerate() {
        by_col.entry(Arc::as_ptr(col)).or_insert_with(|| (col.clone(), Vec::new())).1.push(i);
    }
    for (_, (col, idxs)) in by_col {
        let rows: Vec<u32> = idxs.iter().map(|&i| slice[i].1).collect();
        let loaded = col.hydrate_rows(&rows, true, want_raw)?;
        let schema_guard = if want_fields { Some(col.schema.read()) } else { None };
        for (k, &slot) in idxs.iter().enumerate() {
            let (id_opt, raw_opt) = loaded[k].clone();
            let id = id_opt.unwrap_or_default();
            let full: J = match raw_opt {
                Some(b) if !b.is_empty() => serde_json::from_slice(&b)?,
                _ => J::Null,
            };
            let source_val = if source_enabled {
                let mut s = full.clone();
                if let Some(inc) = &req._source { apply_source_filter(&mut s, inc); }
                Some(s)
            } else { None };

            let fields_val = if want_fields {
                let mut m = serde_json::Map::new();
                let schema = schema_guard.as_ref().unwrap();
                // Expand wildcard field names (e.g., "*" or "foo.*") against
                // the leaf paths actually present in the document.
                let mut expanded: Vec<String> = Vec::with_capacity(field_names.len());
                let mut has_wild = false;
                for n in &field_names {
                    if n.contains('*') { has_wild = true; break; }
                }
                let leaves: Vec<String> = if has_wild {
                    let mut v = Vec::new();
                    collect_leaf_paths(&full, String::new(), &mut v);
                    v
                } else { Vec::new() };
                for name in &field_names {
                    if name.contains('*') {
                        for leaf in &leaves {
                            if glob_match(name, leaf) && !expanded.iter().any(|e| e == leaf) {
                                expanded.push(leaf.clone());
                            }
                        }
                    } else if !expanded.iter().any(|e| e == name) {
                        expanded.push(name.clone());
                    }
                }
                for name in &expanded {
                    if let Some(v) = extract_dotted(&full, name) {
                        let mut arr = match v {
                            J::Array(a) => a,
                            other => vec![other],
                        };
                        if matches!(
                            schema.fields.get(name),
                            Some(arkimedb_core::FieldType::Timestamp)
                        ) {
                            for e in arr.iter_mut() {
                                if let Some(s) = format_date_field(e) {
                                    *e = J::String(s);
                                }
                            }
                        }
                        // Only sort/dedupe values that ES would return from
                        // doc_values (keyword/numeric scalar single-valued
                        // fields). Preserve source order for list-valued
                        // Arkime fields like packetPos/packetLen where element
                        // ordering is semantically significant. Since we don't
                        // track doc_values:false explicitly, be conservative:
                        // don't sort/dedup numeric arrays (they're almost
                        // always ordered lists in Arkime), and only sort/dedup
                        // string/bool arrays.
                        let all_num = arr.iter().all(|v| v.is_number());
                        if !all_num {
                            sort_dedup_field_values(&mut arr);
                        }
                        m.insert(name.clone(), J::Array(arr));
                    }
                }
                Some(m)
            } else { None };

            let sort_val = if want_sort_vals {
                let mut sv: Vec<J> = Vec::with_capacity(sort_clauses.len());
                for s in sort_clauses {
                    let v = extract_dotted(&full, &s.field).unwrap_or(J::Null);
                    // ES returns a single scalar per sort key (the min/max of the field).
                    let scalar = match v {
                        J::Array(mut a) if !a.is_empty() => a.swap_remove(0),
                        other => other,
                    };
                    sv.push(scalar);
                }
                Some(sv)
            } else { None };

            hits[slot] = Hit {
                _index: col.name.clone(),
                _id: id,
                _version: 1,
                _source: source_val,
                fields: fields_val,
                sort: sort_val,
            };
        }
    }

    Ok(SearchResponse {
        took: t0.elapsed().as_millis() as u64,
        hits: Hits {
            total: TotalHits { value: total, relation: "eq" },
            max_score: None,
            hits,
        },
        aggregations: aggs_result.map(|r| r.into_json_map()),
    })
}

fn sort_dedup_field_values(arr: &mut Vec<J>) {
    // ES's `fields` API returns docValues: scalars deduped and sorted in natural
    // value order (numeric by value, strings by Unicode). Only sort if all values
    // are comparable primitives (all numbers, or all strings, or all bools).
    if arr.len() < 2 { return; }
    let all_num = arr.iter().all(|v| v.is_number());
    let all_str = arr.iter().all(|v| v.is_string());
    let all_bool = arr.iter().all(|v| v.is_boolean());
    if all_num {
        arr.sort_by(|a, b| {
            let af = a.as_f64().unwrap_or(f64::NAN);
            let bf = b.as_f64().unwrap_or(f64::NAN);
            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
        });
        arr.dedup_by(|a, b| a.as_f64() == b.as_f64());
    } else if all_str {
        arr.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
        arr.dedup_by(|a, b| a.as_str() == b.as_str());
    } else if all_bool {
        arr.sort_by_key(|v| v.as_bool().unwrap_or(false));
        arr.dedup_by(|a, b| a.as_bool() == b.as_bool());
    }
}

fn apply_source_filter(src: &mut J, spec: &J) {
    // minimal: _source:false → null; _source:["a","b"] → keep those
    match spec {
        J::Bool(false) => { *src = J::Null; }
        J::Array(list) => {
            if let J::Object(map) = src {
                let keep: ahash::AHashSet<String> = list.iter().filter_map(|v| v.as_str().map(String::from)).collect();
                map.retain(|k, _| keep.contains(k));
            }
        }
        _ => {}
    }
}

fn collect_field_names(req: &SearchRequest) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |v: &J| {
        if let Some(s) = v.as_str() { out.push(s.to_string()); return; }
        if let Some(o) = v.as_object() {
            if let Some(f) = o.get("field").and_then(|x| x.as_str()) { out.push(f.to_string()); }
        }
    };
    for list in [&req.fields, &req.docvalue_fields, &req.stored_fields] {
        if let Some(arr) = list { for v in arr { push(v); } }
    }
    out
}

/// Format a date-typed field value for the `fields` projection.
/// ES returns date fields as ISO-8601 strings in `fields`. We accept:
/// - JSON numbers (epoch ms) → format as UTC ISO-8601.
/// - Numeric strings (epoch ms as string) → same.
/// - Already-formatted strings → pass through (no change).
/// Returns `None` if the value is null, non-numeric, or conversion fails,
/// in which case the caller leaves the element untouched.
fn format_date_field(v: &J) -> Option<String> {
    let ms: i64 = match v {
        J::Number(n) => {
            if let Some(i) = n.as_i64() { i }
            else if let Some(f) = n.as_f64() { f as i64 }
            else { return None; }
        }
        J::String(s) => {
            // If it parses as an integer, treat as epoch-ms.
            if let Ok(i) = s.trim().parse::<i64>() { i } else { return None; }
        }
        _ => return None,
    };
    let nanos = (ms as i128).checked_mul(1_000_000)?;
    let dt = time::OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?;
    dt.format(&time::format_description::well_known::Rfc3339).ok()
}

/// Collect dotted leaf paths from a JSON doc. Arrays are treated as leaves
/// (we don't descend into elements) so `fields:["*"]` emits e.g. `source.mac`
/// with the full array rather than creating indexed paths.
fn collect_leaf_paths(v: &J, prefix: String, out: &mut Vec<String>) {
    match v {
        J::Object(m) if !m.is_empty() => {
            for (k, child) in m {
                let next = if prefix.is_empty() { k.clone() } else { format!("{}.{}", prefix, k) };
                match child {
                    J::Object(cm) if !cm.is_empty() => collect_leaf_paths(child, next, out),
                    _ => out.push(next),
                }
            }
        }
        _ => {
            if !prefix.is_empty() { out.push(prefix); }
        }
    }
}

/// Simple glob matcher supporting `*` and `?`. Delegates to core.
fn glob_match(pattern: &str, text: &str) -> bool {
    arkimedb_core::schema_glob_match(pattern, text)
}

/// Extract a value from a JSON document by dotted path. Matches ES "fields"
/// behavior for nested objects: `a.b.c` descends through objects; if an
/// intermediate value is an array, each element is descended and results
/// are flattened into a single array.
fn extract_dotted(doc: &J, path: &str) -> Option<J> {
    let parts: Vec<&str> = path.split('.').collect();
    fn walk(v: &J, parts: &[&str]) -> Vec<J> {
        if parts.is_empty() {
            if v.is_null() { return Vec::new(); }
            return vec![v.clone()];
        }
        match v {
            J::Object(m) => {
                if let Some(child) = m.get(parts[0]) { walk(child, &parts[1..]) } else { Vec::new() }
            }
            J::Array(a) => a.iter().flat_map(|e| walk(e, parts)).collect(),
            _ => Vec::new(),
        }
    }
    let found = walk(doc, &parts);
    if found.is_empty() { None }
    else if found.len() == 1 { Some(found.into_iter().next().unwrap()) }
    else { Some(J::Array(found)) }
}

fn sort_hits(hits: &mut Vec<(Arc<Collection>, u32)>, sort: &[SortSpec]) -> Result<()> {
    // Build per-collection row_id -> sort_value maps for each sort field
    // by scanning the postings index. Avoids hydrating every hit's full
    // _source just to compare one field.
    let mut col_ids: ahash::AHashMap<*const Collection, usize> = ahash::AHashMap::new();
    let mut by_col: Vec<Arc<Collection>> = Vec::new();
    for (c, _) in hits.iter() {
        let p = Arc::as_ptr(c);
        if !col_ids.contains_key(&p) {
            col_ids.insert(p, by_col.len());
            by_col.push(c.clone());
        }
    }

    // For each (collection, sort field) get a row -> Scalar map, using the
    // per-collection sort_cache. First sort of a field builds it; subsequent
    // sorts reuse. Writes to the collection clear the cache.
    let mut col_field_vals: Vec<Vec<Option<Arc<ahash::AHashMap<u32, arkimedb_core::Scalar>>>>> =
        vec![vec![None; sort.len()]; by_col.len()];
    for (ci, col) in by_col.iter().enumerate() {
        for (si, s) in sort.iter().enumerate() {
            if col.index.field_type(&s.field).is_none() { continue; }
            if let Some(m) = col.sort_cache.read().get(&s.field) {
                col_field_vals[ci][si] = Some(m.clone());
                continue;
            }
            let mut m = ahash::AHashMap::new();
            col.index.for_each_value(&s.field, |sc, bm| {
                for r in bm.iter() { m.entry(r).or_insert_with(|| sc.clone()); }
            });
            let arc = Arc::new(m);
            col.sort_cache.write().insert(s.field.clone(), arc.clone());
            col_field_vals[ci][si] = Some(arc);
        }
    }
    let col_field_indexed: Vec<Vec<bool>> = col_field_vals.iter()
        .map(|v| v.iter().map(|o| o.is_some()).collect())
        .collect();

    // Fallback: hydrate _source only for rows where at least one sort field
    // is NOT in the postings index. Hopefully rare.
    let need_fallback = col_field_indexed.iter().any(|v| v.iter().any(|b| !*b));
    let mut docs: Vec<Option<serde_json::Value>> = vec![None; hits.len()];
    let mut ids: Vec<String> = vec![String::new(); hits.len()];
    if need_fallback {
        let mut by_col_slots: Vec<Vec<(u32, usize)>> = vec![Vec::new(); by_col.len()];
        for (i, (c, rid)) in hits.iter().enumerate() {
            let ci = col_ids[&Arc::as_ptr(c)];
            by_col_slots[ci].push((*rid, i));
        }
        for (ci, col) in by_col.iter().enumerate() {
            let rows: Vec<u32> = by_col_slots[ci].iter().map(|(r, _)| *r).collect();
            let loaded = col.hydrate_rows(&rows, true, true)?;
            for (k, (_, slot)) in by_col_slots[ci].iter().enumerate() {
                let (id_opt, raw_opt) = &loaded[k];
                ids[*slot] = id_opt.clone().unwrap_or_default();
                docs[*slot] = raw_opt.as_ref()
                    .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok());
            }
        }
    }

    let mut perm: Vec<usize> = (0..hits.len()).collect();
    perm.sort_by(|&ai, &bi| {
        let (ca, ra) = &hits[ai];
        let (cb, rb) = &hits[bi];
        let cai = col_ids[&Arc::as_ptr(ca)];
        let cbi = col_ids[&Arc::as_ptr(cb)];
        for (si, s) in sort.iter().enumerate() {
            let a_sc = col_field_vals[cai][si].as_ref().and_then(|m| m.get(ra));
            let b_sc = col_field_vals[cbi][si].as_ref().and_then(|m| m.get(rb));
            let ord = if col_field_indexed[cai][si] && col_field_indexed[cbi][si] {
                cmp_scalar_opt(a_sc, b_sc, s.missing_last)
            } else {
                let av = docs[ai].as_ref().and_then(|v| v.get(&s.field));
                let bv = docs[bi].as_ref().and_then(|v| v.get(&s.field));
                cmp_json(av, bv, s.missing_last)
            };
            if ord != std::cmp::Ordering::Equal {
                return if s.ascending { ord } else { ord.reverse() };
            }
        }
        ids[ai].cmp(&ids[bi])
    });
    let reordered: Vec<(Arc<Collection>, u32)> = perm.into_iter().map(|i| hits[i].clone()).collect();
    *hits = reordered;
    Ok(())
}

fn cmp_scalar_opt(a: Option<&arkimedb_core::Scalar>, b: Option<&arkimedb_core::Scalar>, missing_last: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (None, None) => Equal,
        (None, _) => if missing_last { Greater } else { Less },
        (_, None) => if missing_last { Less } else { Greater },
        (Some(x), Some(y)) => cmp_scalar(x, y),
    }
}

fn cmp_scalar(a: &arkimedb_core::Scalar, b: &arkimedb_core::Scalar) -> std::cmp::Ordering {
    use arkimedb_core::Scalar::*;
    use std::cmp::Ordering::*;
    match (a, b) {
        (I64(x), I64(y)) => x.cmp(y),
        (U64(x), U64(y)) => x.cmp(y),
        (I64(x), U64(y)) => (*x as i128).cmp(&(*y as i128)),
        (U64(x), I64(y)) => (*x as i128).cmp(&(*y as i128)),
        (I64(x), F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Equal),
        (F64(x), I64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Equal),
        (U64(x), F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Equal),
        (F64(x), U64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Equal),
        (F64(x), F64(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (Ts(x), Ts(y)) => x.cmp(y),
        (Ts(x), I64(y)) => x.cmp(y),
        (I64(x), Ts(y)) => x.cmp(y),
        (Ip(x), Ip(y)) => x.cmp(y),
        (Str(x), Str(y)) => x.cmp(y),
        (Bool(x), Bool(y)) => x.cmp(y),
        _ => Equal,
    }
}

fn cmp_json(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>, missing_last: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (None, None) => Equal,
        (None, _) => if missing_last { Greater } else { Less },
        (_, None) => if missing_last { Less } else { Greater },
        (Some(x), Some(y)) => cmp_jv(x, y),
    }
}
fn cmp_jv(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            x.as_f64().partial_cmp(&y.as_f64()).unwrap_or(Equal)
        }
        (serde_json::Value::String(x), serde_json::Value::String(y)) => x.cmp(y),
        (serde_json::Value::Bool(x), serde_json::Value::Bool(y)) => x.cmp(y),
        _ => Equal,
    }
}


