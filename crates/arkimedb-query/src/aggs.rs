//! Aggregations: count, sum/min/max/avg, cardinality, terms, histogram,
//! date_histogram (incl. two-field span bucketing), plus nested sub-aggs.

use std::sync::Arc;
use ahash::{AHashMap, AHashSet};
use serde_json::Value as J;

use arkimedb_core::{Error, Result, Scalar, FieldType};
use arkimedb_storage::Collection;

use crate::predicate::{compile_es_query, cmp_scalar};

#[derive(Debug, Clone)]
pub enum AggKind {
    Count,
    ValueCount { field: String },
    Cardinality { field: String },
    Sum   { field: String },
    Min   { field: String },
    Max   { field: String },
    Avg   { field: String },
    Terms { field: String, size: usize, min_doc_count: u64 },
    TermsIpPort { ip_field: String, port_field: String, size: usize, sep: String },
    TermsAllIp { size: usize },
    Histogram     { field: String, interval: f64 },
    DateHistogram { field: String, interval_ms: i64, span_end_field: Option<String> },
}

#[derive(Debug, Clone)]
pub struct AggRequest {
    pub name: String,
    pub kind: AggKind,
    pub subs: Vec<AggRequest>,
}

#[derive(Debug, Clone)]
pub enum AggResult {
    Metric(Option<f64>),
    Long(u64),
    Cardinality(u64),
    Buckets(Vec<Bucket>),
    TermsBuckets { buckets: Vec<Bucket>, sum_other_doc_count: u64 },
    Multi(AHashMap<String, AggResult>),
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub key: J,
    pub key_as_string: Option<String>,
    pub doc_count: u64,
    pub subs: AHashMap<String, AggResult>,
}

impl AggResult {
    pub fn into_json_map(self) -> serde_json::Map<String, J> {
        match self {
            AggResult::Multi(m) => {
                let mut out = serde_json::Map::new();
                for (k, v) in m { out.insert(k, v.into_json()); }
                out
            }
            other => {
                let mut out = serde_json::Map::new();
                out.insert("_".into(), other.into_json());
                out
            }
        }
    }
    fn into_json(self) -> J {
        match self {
            AggResult::Metric(v) => serde_json::json!({ "value": v }),
            AggResult::Long(v)   => serde_json::json!({ "value": v }),
            AggResult::Cardinality(v) => serde_json::json!({ "value": v }),
            AggResult::Multi(m) => {
                let mut out = serde_json::Map::new();
                for (k, v) in m { out.insert(k, v.into_json()); }
                J::Object(out)
            }
            AggResult::Buckets(b) => {
                let buckets: Vec<J> = b.into_iter().map(|bk| {
                    let mut o = serde_json::Map::new();
                    o.insert("key".into(), bk.key);
                    if let Some(s) = bk.key_as_string { o.insert("key_as_string".into(), J::String(s)); }
                    o.insert("doc_count".into(), J::Number(bk.doc_count.into()));
                    for (k, v) in bk.subs { o.insert(k, v.into_json()); }
                    J::Object(o)
                }).collect();
                serde_json::json!({ "buckets": buckets })
            }
            AggResult::TermsBuckets { buckets: b, sum_other_doc_count } => {
                let buckets: Vec<J> = b.into_iter().map(|bk| {
                    let mut o = serde_json::Map::new();
                    o.insert("key".into(), bk.key);
                    if let Some(s) = bk.key_as_string { o.insert("key_as_string".into(), J::String(s)); }
                    o.insert("doc_count".into(), J::Number(bk.doc_count.into()));
                    for (k, v) in bk.subs { o.insert(k, v.into_json()); }
                    J::Object(o)
                }).collect();
                serde_json::json!({
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": sum_other_doc_count,
                    "buckets": buckets,
                })
            }
        }
    }
}

pub fn compile_aggs(j: &J) -> Result<Vec<AggRequest>> {
    let Some(obj) = j.as_object() else { return Err(Error::BadRequest("aggs must be object".into())); };
    let mut out = Vec::with_capacity(obj.len());
    for (name, spec) in obj { out.push(compile_one(name, spec)?); }
    Ok(out)
}
fn compile_one(name: &str, spec: &J) -> Result<AggRequest> {
    let o = spec.as_object().ok_or_else(|| Error::BadRequest("agg must be object".into()))?;
    let mut subs = Vec::new();
    if let Some(sub_j) = o.get("aggs").or(o.get("aggregations")) {
        if let Some(sub_obj) = sub_j.as_object() {
            for (k, v) in sub_obj { subs.push(compile_one(k, v)?); }
        }
    }
    let mut kind: Option<AggKind> = None;
    for (k, v) in o {
        if matches!(k.as_str(), "aggs" | "aggregations" | "meta") { continue; }
        kind = Some(match k.as_str() {
            "value_count" => AggKind::ValueCount { field: field_of(v)? },
            "cardinality" => AggKind::Cardinality { field: field_of(v)? },
            "sum" => AggKind::Sum { field: field_of(v)? },
            "min" => AggKind::Min { field: field_of(v)? },
            "max" => AggKind::Max { field: field_of(v)? },
            "avg" => AggKind::Avg { field: field_of(v)? },
            "terms" => {
                if let Some(script) = v.get("script") {
                    let src = script.get("source").or_else(|| script.get("inline")).and_then(|x| x.as_str()).unwrap_or("");
                    let size = v.get("size").and_then(|x| x.as_u64()).unwrap_or(10) as usize;
                    // Dual-IP: summary "allIp" script returns [source.ip, destination.ip]
                    if src.contains("source.ip") && src.contains("destination.ip")
                        && !src.contains("port") {
                        kind = Some(AggKind::TermsAllIp { size });
                        continue;
                    }
                    // IP + port composite. Detect separator: '_' vs ':' (default ':'; IPv6 falls back to '.').
                    let sep = if src.contains("'_'") || src.contains("\"_\"") { "_".to_string() } else { ":".to_string() };
                    if src.contains("destination.ip") && src.contains("destination.port") {
                        kind = Some(AggKind::TermsIpPort { ip_field: "destination.ip".into(), port_field: "destination.port".into(), size, sep });
                        continue;
                    }
                    if src.contains("source.ip") && src.contains("source.port") {
                        kind = Some(AggKind::TermsIpPort { ip_field: "source.ip".into(), port_field: "source.port".into(), size, sep });
                        continue;
                    }
                }
                let f = field_of(v)?;
                let size = v.get("size").and_then(|x| x.as_u64()).unwrap_or(10) as usize;
                let mdc  = v.get("min_doc_count").and_then(|x| x.as_u64()).unwrap_or(1);
                AggKind::Terms { field: f, size, min_doc_count: mdc }
            }
            "histogram" => {
                let f = field_of(v)?;
                let interval = v.get("interval").and_then(|x| x.as_f64()).unwrap_or(1.0);
                AggKind::Histogram { field: f, interval }
            }
            "date_histogram" => {
                let f = field_of(v)?;
                let interval = v.get("fixed_interval").or(v.get("interval"))
                    .and_then(|x| x.as_str()).unwrap_or("1d");
                let ms = parse_interval_ms(interval)?;
                let end_field = v.get("span_end_field").and_then(|x| x.as_str()).map(String::from);
                AggKind::DateHistogram { field: f, interval_ms: ms, span_end_field: end_field }
            }
            _ => continue,
        });
    }
    let kind = kind.unwrap_or(AggKind::Count);
    Ok(AggRequest { name: name.to_string(), kind, subs })
}
fn field_of(v: &J) -> Result<String> {
    v.get("field").and_then(|x| x.as_str()).map(String::from)
        .ok_or_else(|| Error::BadRequest("agg.field required".into()))
}
fn parse_interval_ms(s: &str) -> Result<i64> {
    // accept Nms/Ns/Nm/Nh/Nd/Nw
    let s = s.trim();
    let (num_s, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: i64 = num_s.parse().map_err(|_| Error::BadRequest("bad interval".into()))?;
    let mul = match unit {
        "ms" => 1, "s" => 1_000, "m" => 60_000, "h" => 3_600_000,
        "d" => 86_400_000, "w" => 604_800_000, "" => 86_400_000,
        _ => return Err(Error::BadRequest(format!("unsupported interval unit: {unit}"))),
    };
    Ok(n * mul)
}

pub fn run_aggs(cols: &[Arc<Collection>], query_json: &J, specs: &[AggRequest]) -> Result<AggResult> {
    // Compute the matching row bitmap per collection, once.
    let mut sets: Vec<(Arc<Collection>, roaring::RoaringBitmap)> = Vec::with_capacity(cols.len());
    for col in cols {
        let q = compile_es_query(query_json, col)?;
        let bm = q.eval(col)?;
        sets.push((col.clone(), bm));
    }
    let mut out = AHashMap::new();
    for spec in specs {
        out.insert(spec.name.clone(), run_one(&sets, spec)?);
    }
    Ok(AggResult::Multi(out))
}

fn run_one(sets: &[(Arc<Collection>, roaring::RoaringBitmap)], spec: &AggRequest) -> Result<AggResult> {
    match &spec.kind {
        AggKind::Count => {
            let total: u64 = sets.iter().map(|(_, b)| b.len()).sum();
            Ok(AggResult::Long(total))
        }
        AggKind::ValueCount { field } => {
            let mut n = 0u64;
            for (col, bm) in sets {
                col.index.for_each_value(field, |_s, vb| {
                    n += (vb & bm).len();
                });
            }
            Ok(AggResult::Long(n))
        }
        AggKind::Cardinality { field } => {
            // exact (not HLL) — scales to a few million distinct values comfortably
            let mut seen: AHashSet<Vec<u8>> = AHashSet::new();
            for (col, bm) in sets {
                col.index.for_each_value(field, |s, vb| {
                    if !(vb & bm).is_empty() {
                        seen.insert(arkimedb_storage::codec::encode_key(field, s));
                    }
                });
            }
            Ok(AggResult::Cardinality(seen.len() as u64))
        }
        AggKind::Sum { field } | AggKind::Min { field } | AggKind::Max { field } | AggKind::Avg { field } => {
            let mut sum = 0.0_f64; let mut n = 0u64;
            let mut mn = f64::INFINITY; let mut mx = f64::NEG_INFINITY;
            for (col, bm) in sets {
                col.index.for_each_value(field, |s, vb| {
                    let count = (vb & bm).len();
                    if count == 0 { return; }
                    let val = scalar_to_f64(s);
                    if let Some(v) = val {
                        sum += v * count as f64;
                        n   += count;
                        if v < mn { mn = v; }
                        if v > mx { mx = v; }
                    }
                });
            }
            Ok(AggResult::Metric(match spec.kind {
                AggKind::Sum{..} => Some(sum),
                AggKind::Min{..} => if n == 0 { None } else { Some(mn) },
                AggKind::Max{..} => if n == 0 { None } else { Some(mx) },
                AggKind::Avg{..} => if n == 0 { None } else { Some(sum / n as f64) },
                _ => unreachable!(),
            }))
        }
        AggKind::Terms { field, size, min_doc_count } => {
            // Merge term counts across all collections. Keep per-collection
            // bitmaps separate so sub-aggs see the right row set and so that
            // row-ID collisions across collections don't collapse counts.
            let mut merged: Vec<(Scalar, u64, Vec<(Arc<Collection>, roaring::RoaringBitmap)>, Vec<u8>)> = Vec::new();
            let mut keyed: AHashMap<Vec<u8>, usize> = AHashMap::new();
            for (col, bm) in sets {
                col.index.for_each_value(field, |s, vb| {
                    let hits = vb & bm;
                    if hits.is_empty() { return; }
                    let n = hits.len();
                    let k = arkimedb_storage::codec::encode_key(field, s);
                    match keyed.get(&k).copied() {
                        Some(i) => {
                            merged[i].1 += n;
                            merged[i].2.push((col.clone(), hits));
                        }
                        None => {
                            keyed.insert(k.clone(), merged.len());
                            merged.push((s.clone(), n, vec![(col.clone(), hits)], k));
                        }
                    }
                });
            }
            // Count desc, tiebreak by encoded key asc (ES default ordering).
            merged.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.3.cmp(&b.3)));
            let total: u64 = merged.iter().map(|m| m.1).sum();
            let mut kept: u64 = 0;
            let mut buckets = Vec::new();
            for (s, n, per_col, _) in merged.into_iter().take(*size) {
                if n < *min_doc_count { continue; }
                let subs_sets: Vec<(Arc<Collection>, roaring::RoaringBitmap)> = sets.iter().map(|(c, _)| {
                    let mut acc = roaring::RoaringBitmap::new();
                    for (cc, bb) in &per_col { if Arc::ptr_eq(cc, c) { acc |= bb; } }
                    (c.clone(), acc)
                }).collect();
                let mut sub_out = AHashMap::new();
                for sub in &spec.subs { sub_out.insert(sub.name.clone(), run_one(&subs_sets, sub)?); }
                kept += n;
                buckets.push(Bucket {
                    key: scalar_to_json(&s),
                    key_as_string: None,
                    doc_count: n,
                    subs: sub_out,
                });
            }
            Ok(AggResult::TermsBuckets { buckets, sum_other_doc_count: total.saturating_sub(kept) })
        }
        AggKind::TermsIpPort { ip_field, port_field, size, sep: sep_arg } => {
            use std::collections::BTreeMap;
            // key -> (display_scalar, total_count, Vec<(col, bitmap)>)
            let mut merged: BTreeMap<String, (Scalar, u64, Vec<(Arc<Collection>, roaring::RoaringBitmap)>)> = BTreeMap::new();
            for (col, bm) in sets {
                // Build per-port row bitmap map
                let mut port_map: Vec<(Scalar, roaring::RoaringBitmap)> = Vec::new();
                col.index.for_each_value(port_field, |s, vb| {
                    let h = vb & bm;
                    if !h.is_empty() { port_map.push((s.clone(), h)); }
                });
                col.index.for_each_value(ip_field, |ip_s, ip_vb| {
                    let ip_hits = ip_vb & bm;
                    if ip_hits.is_empty() { return; }
                    let ip_str = match ip_s {
                        Scalar::Ip(a) => arkimedb_core::value::ip_to_string(*a),
                        Scalar::Str(s) => s.to_string(),
                        _ => format!("{:?}", ip_s),
                    };
                    for (port_s, port_vb) in &port_map {
                        let both = &ip_hits & port_vb;
                        if both.is_empty() { continue; }
                        let port_str = match port_s {
                            Scalar::I64(n) => n.to_string(),
                            Scalar::U64(n) => n.to_string(),
                            Scalar::F64(n) => (*n as i64).to_string(),
                            Scalar::Str(s) => s.to_string(),
                            _ => format!("{:?}", port_s),
                        };
                        let sep = if sep_arg == "_" { "_" } else if ip_str.contains('.') && !ip_str.contains(':') { ":" } else { "." };
                        let key = format!("{}{}{}", ip_str, sep, port_str);
                        let n = both.len();
                        let entry = merged.entry(key.clone()).or_insert_with(|| (Scalar::Str(key.clone().into()), 0, Vec::new()));
                        entry.1 += n;
                        entry.2.push((col.clone(), both));
                    }
                });
            }
            let mut items: Vec<(Scalar, u64, Vec<(Arc<Collection>, roaring::RoaringBitmap)>)> =
                merged.into_iter().map(|(_k, v)| v).collect();
            items.sort_by(|a, b| b.1.cmp(&a.1));
            let total: u64 = items.iter().map(|m| m.1).sum();
            let mut kept: u64 = 0;
            let mut buckets = Vec::new();
            for (s, count, per_col) in items.into_iter().take(*size) {
                let subs_sets: Vec<(Arc<Collection>, roaring::RoaringBitmap)> = sets.iter().map(|(c, _)| {
                    let mut acc = roaring::RoaringBitmap::new();
                    for (cc, bb) in &per_col { if Arc::ptr_eq(cc, c) { acc |= bb; } }
                    (c.clone(), acc)
                }).collect();
                let mut sub_out = AHashMap::new();
                for sub in &spec.subs { sub_out.insert(sub.name.clone(), run_one(&subs_sets, sub)?); }
                kept += count;
                buckets.push(Bucket { key: scalar_to_json(&s), key_as_string: None, doc_count: count, subs: sub_out });
            }
            Ok(AggResult::TermsBuckets { buckets, sum_other_doc_count: total.saturating_sub(kept) })
        }
        AggKind::TermsAllIp { size } => {
            // Union of source.ip and destination.ip values — a doc contributes
            // to both its source and destination IP buckets.
            let mut merged: Vec<(Scalar, u64, Vec<(Arc<Collection>, roaring::RoaringBitmap)>, Vec<u8>)> = Vec::new();
            let mut keyed: AHashMap<Vec<u8>, usize> = AHashMap::new();
            for (col, bm) in sets {
                for field in &["source.ip", "destination.ip"] {
                    col.index.for_each_value(field, |s, vb| {
                        let hits = vb & bm;
                        if hits.is_empty() { return; }
                        let n = hits.len();
                        let k = arkimedb_storage::codec::encode_key("ip", s);
                        match keyed.get(&k).copied() {
                            Some(i) => {
                                merged[i].1 += n;
                                merged[i].2.push((col.clone(), hits));
                            }
                            None => {
                                keyed.insert(k.clone(), merged.len());
                                merged.push((s.clone(), n, vec![(col.clone(), hits)], k));
                            }
                        }
                    });
                }
            }
            merged.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.3.cmp(&b.3)));
            let total: u64 = merged.iter().map(|m| m.1).sum();
            let mut kept: u64 = 0;
            let mut buckets = Vec::new();
            for (s, n, per_col, _) in merged.into_iter().take(*size) {
                let subs_sets: Vec<(Arc<Collection>, roaring::RoaringBitmap)> = sets.iter().map(|(c, _)| {
                    let mut acc = roaring::RoaringBitmap::new();
                    for (cc, bb) in &per_col { if Arc::ptr_eq(cc, c) { acc |= bb; } }
                    (c.clone(), acc)
                }).collect();
                let mut sub_out = AHashMap::new();
                for sub in &spec.subs { sub_out.insert(sub.name.clone(), run_one(&subs_sets, sub)?); }
                kept += n;
                buckets.push(Bucket { key: scalar_to_json(&s), key_as_string: None, doc_count: n, subs: sub_out });
            }
            Ok(AggResult::TermsBuckets { buckets, sum_other_doc_count: total.saturating_sub(kept) })
        }
        AggKind::Histogram { field, interval } => {
            let mut acc: AHashMap<i64, u64> = AHashMap::new();
            // Per-bucket row bitmaps, scoped to each collection, so sub-aggs
            // get the right restriction in multi-index queries.
            let mut bucket_sets: AHashMap<i64, Vec<(Arc<Collection>, roaring::RoaringBitmap)>> = AHashMap::new();
            for (col, bm) in sets {
                let mut local: AHashMap<i64, roaring::RoaringBitmap> = AHashMap::new();
                // Probe the main field first.
                let mut saw_any = false;
                col.index.for_each_value(field, |_s, _vb| { saw_any = true; });
                if saw_any {
                    col.index.for_each_value(field, |s, vb| {
                        let Some(v) = scalar_to_f64(s) else { return; };
                        // Bucket start value (aligned to interval).
                        let key = ((v / interval).floor() as i64).saturating_mul(*interval as i64);
                        let hits = vb & bm;
                        if hits.is_empty() { return; }
                        *acc.entry(key).or_default() += hits.len();
                        *local.entry(key).or_default() |= hits;
                    });
                } else {
                    // Range-typed field (e.g., packetRange): flattened as
                    // "<field>.gte" / "<field>.lte". A doc contributes to
                    // every bucket its [gte, lte] span overlaps.
                    let gte_f = format!("{}.gte", field);
                    let lte_f = format!("{}.lte", field);
                    let mut start_of: AHashMap<u32, i64> = AHashMap::new();
                    col.index.for_each_value(&gte_f, |s, vb| {
                        if let Some(ms) = scalar_to_i64_ms(s) {
                            for r in (vb & bm).iter() { start_of.insert(r, ms); }
                        }
                    });
                    let iv = *interval as i64;
                    if iv > 0 {
                        col.index.for_each_value(&lte_f, |s, vb| {
                            if let Some(end_ms) = scalar_to_i64_ms(s) {
                                for r in (vb & bm).iter() {
                                    let Some(start_ms) = start_of.get(&r).copied() else { continue; };
                                    let (lo, hi) = (start_ms.min(end_ms), start_ms.max(end_ms));
                                    let first = (lo / iv) * iv;
                                    let mut k = first;
                                    while k <= hi {
                                        *acc.entry(k).or_default() += 1;
                                        local.entry(k).or_default().insert(r);
                                        k += iv;
                                    }
                                }
                            }
                        });
                    }
                }
                for (k, rb) in local {
                    bucket_sets.entry(k).or_default().push((col.clone(), rb));
                }
            }
            let mut keys: Vec<i64> = acc.keys().copied().collect(); keys.sort();
            let mut buckets = Vec::new();
            for k in keys {
                let count = acc[&k];
                let per_col = bucket_sets.remove(&k).unwrap_or_default();
                let subs_sets: Vec<(Arc<Collection>, roaring::RoaringBitmap)> = sets.iter().map(|(c, _)| {
                    let rb = per_col.iter().find(|(cc, _)| Arc::ptr_eq(cc, c))
                        .map(|(_, b)| b.clone())
                        .unwrap_or_default();
                    (c.clone(), rb)
                }).collect();
                let mut sub_out = AHashMap::new();
                for sub in &spec.subs { sub_out.insert(sub.name.clone(), run_one(&subs_sets, sub)?); }
                let key_val = serde_json::json!(k);
                buckets.push(Bucket {
                    key: key_val,
                    key_as_string: None,
                    doc_count: count,
                    subs: sub_out,
                });
            }
            Ok(AggResult::Buckets(buckets))
        }
        AggKind::DateHistogram { field, interval_ms, span_end_field } => {
            // Two-field span bucketing: if span_end_field is set, a doc with
            // [start, end] contributes to every bucket its span overlaps.
            // Also maintain per-bucket row bitmaps (per collection) so sub-aggs
            // are computed over only the rows that fall in that bucket.
            let mut acc: AHashMap<i64, u64> = AHashMap::new();
            // bucket_key -> Vec<(col, bitmap)> (one entry per collection; collections without hits are omitted)
            let mut bucket_sets: AHashMap<i64, Vec<(Arc<Collection>, roaring::RoaringBitmap)>> = AHashMap::new();
            for (col, bm) in sets {
                if let Some(end_f) = span_end_field {
                    // row -> start
                    let mut start_of: AHashMap<u32, i64> = AHashMap::new();
                    col.index.for_each_value(field, |s, vb| {
                        if let Some(ms) = scalar_to_i64_ms(s) {
                            for r in (vb & bm).iter() { start_of.insert(r, ms); }
                        }
                    });
                    // per-bucket local bitmap for this collection
                    let mut local: AHashMap<i64, roaring::RoaringBitmap> = AHashMap::new();
                    col.index.for_each_value(end_f, |s, vb| {
                        if let Some(end_ms) = scalar_to_i64_ms(s) {
                            for r in (vb & bm).iter() {
                                let Some(start_ms) = start_of.get(&r).copied() else { continue; };
                                let (lo, hi) = (start_ms.min(end_ms), start_ms.max(end_ms));
                                let first = (lo / interval_ms) * interval_ms;
                                let mut k = first;
                                while k <= hi {
                                    *acc.entry(k).or_default() += 1;
                                    local.entry(k).or_default().insert(r);
                                    k += interval_ms;
                                }
                            }
                        }
                    });
                    for (k, rb) in local {
                        bucket_sets.entry(k).or_default().push((col.clone(), rb));
                    }
                } else {
                    let mut local: AHashMap<i64, roaring::RoaringBitmap> = AHashMap::new();
                    col.index.for_each_value(field, |s, vb| {
                        if let Some(ms) = scalar_to_i64_ms(s) {
                            let key = (ms / interval_ms) * interval_ms;
                            let hits = vb & bm;
                            if hits.is_empty() { return; }
                            *acc.entry(key).or_default() += hits.len();
                            let e = local.entry(key).or_default();
                            *e |= hits;
                        }
                    });
                    for (k, rb) in local {
                        bucket_sets.entry(k).or_default().push((col.clone(), rb));
                    }
                }
            }
            let mut keys: Vec<i64> = acc.keys().copied().collect(); keys.sort();
            let mut buckets = Vec::new();
            for k in keys {
                let n = acc[&k];
                let mut sub_out = AHashMap::new();
                // Restrict sub-aggs to docs in this bucket; include every
                // collection (empty bitmap for those with no hits in bucket).
                let per_col = bucket_sets.remove(&k).unwrap_or_default();
                let subs_sets: Vec<(Arc<Collection>, roaring::RoaringBitmap)> = sets.iter().map(|(c, _)| {
                    let rb = per_col.iter().find(|(cc, _)| Arc::ptr_eq(cc, c))
                        .map(|(_, b)| b.clone())
                        .unwrap_or_default();
                    (c.clone(), rb)
                }).collect();
                for sub in &spec.subs { sub_out.insert(sub.name.clone(), run_one(&subs_sets, sub)?); }
                buckets.push(Bucket {
                    key: serde_json::json!(k),
                    key_as_string: Some(iso_ms(k)),
                    doc_count: n,
                    subs: sub_out,
                });
            }
            Ok(AggResult::Buckets(buckets))
        }
    }
}

fn scalar_to_f64(s: &Scalar) -> Option<f64> {
    Some(match s {
        Scalar::I64(v) => *v as f64,
        Scalar::U64(v) => *v as f64,
        Scalar::F64(v) => *v,
        Scalar::Ts(v)  => *v as f64,
        Scalar::Bool(b) => if *b { 1.0 } else { 0.0 },
        _ => return None,
    })
}
fn scalar_to_i64_ms(s: &Scalar) -> Option<i64> {
    Some(match s {
        Scalar::Ts(v) => *v,
        Scalar::I64(v) => *v,
        Scalar::U64(v) => *v as i64,
        Scalar::F64(v) => *v as i64,
        _ => return None,
    })
}
fn scalar_to_json(s: &Scalar) -> J {
    match s {
        Scalar::Null => J::Null,
        Scalar::Bool(b) => J::Bool(*b),
        Scalar::I64(v) => serde_json::json!(*v),
        Scalar::U64(v) => serde_json::json!(*v),
        Scalar::F64(v) => serde_json::json!(*v),
        Scalar::Str(v) => J::String(v.clone()),
        Scalar::Ip(v)  => J::String(arkimedb_core::value::ip_to_string(*v)),
        Scalar::Ts(v)  => serde_json::json!(*v),
        Scalar::Json(v) => v.clone(),
    }
}
fn iso_ms(ms: i64) -> String {
    // formatted as ISO-8601 UTC
    let secs = ms / 1000; let sub = (ms % 1000).unsigned_abs();
    let odt = time::OffsetDateTime::from_unix_timestamp(secs).unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!("{}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        odt.year(), u8::from(odt.month()), odt.day(),
        odt.hour(), odt.minute(), odt.second(), sub)
}

// Suppress unused warnings while we stabilize.
#[allow(dead_code)]
fn _touch(_s: &Scalar) { let _ = cmp_scalar(&Scalar::Null, &Scalar::Null); let _ = FieldType::Keyword; }
