//! Compile an Elasticsearch query-DSL subset into our internal `Query`, and
//! evaluate it against a `Collection`'s posting index.

use std::sync::Arc;
use roaring::RoaringBitmap;
use serde_json::Value as J;

use arkimedb_core::{Error, Result, Scalar, FieldType};
use arkimedb_storage::Collection;

/// Internal normalized query tree.
#[derive(Clone, Debug)]
pub enum Query {
    MatchAll,
    MatchNone,
    /// equality or IN across alternatives
    Term { field: String, values: Vec<Scalar> },
    Exists { field: String },
    Range  { field: String, lo: Option<Scalar>, hi: Option<Scalar>, lo_inclusive: bool, hi_inclusive: bool },
    /// CIDR for IP fields (v4/v6 in canonical v6 form)
    Cidr   { field: String, net: u128, prefix: u8 },
    Wildcard { field: String, pattern: String },
    Regexp   { field: String, pattern: String },
    /// Match documents by exact `_id` (mirrors ES `ids` query). `_id` is
    /// metadata, not an indexed field, so we resolve via the id→row table.
    Ids(Vec<String>),
    Bool {
        must:     Vec<Query>,
        should:   Vec<Query>,
        must_not: Vec<Query>,
        filter:   Vec<Query>,
        min_should_match: usize,
    },
}

impl Query {
    pub fn eval(&self, col: &Arc<Collection>) -> Result<RoaringBitmap> {
        let all = col.all_live_rows()?;
        self.eval_in(col, &all)
    }

    fn eval_in(&self, col: &Arc<Collection>, universe: &RoaringBitmap) -> Result<RoaringBitmap> {
        Ok(match self {
            Query::MatchAll => universe.clone(),
            Query::MatchNone => RoaringBitmap::new(),
            Query::Exists { field } => &col.index.exists_bm(field) & universe,
            Query::Term { field, values } => {
                let mut out = RoaringBitmap::new();
                for v in values { out |= col.index.get(field, v); }
                &out & universe
            }
            Query::Range { field, lo, hi, lo_inclusive, hi_inclusive } => {
                let mut out = RoaringBitmap::new();
                col.index.for_each_value(field, |s, bm| {
                    if in_range(s, lo.as_ref(), hi.as_ref(), *lo_inclusive, *hi_inclusive) {
                        out |= bm;
                    }
                });
                &out & universe
            }
            Query::Cidr { field, net, prefix } => {
                let mask = ipv6_prefix_mask(*prefix);
                let net_masked = net & mask;
                let mut out = RoaringBitmap::new();
                col.index.for_each_value(field, |s, bm| {
                    if let Scalar::Ip(ip) = s {
                        if (ip & mask) == net_masked { out |= bm; }
                    }
                });
                &out & universe
            }
            Query::Wildcard { field, pattern } => {
                let m = glob::Matcher::compile(pattern);
                let mut out = RoaringBitmap::new();
                col.index.for_each_value(field, |s, bm| {
                    if let Scalar::Str(v) = s {
                        if m.is_match(&v) { out |= bm; }
                    }
                });
                &out & universe
            }
            Query::Regexp { field, pattern } => {
                use regex_automata::meta::Regex;
                let anchored = es_anchor_regex(pattern);
                let re = Regex::new(&anchored)
                    .map_err(|e| Error::BadRequest(format!("bad regex: {e}")))?;
                let mut out = RoaringBitmap::new();
                col.index.for_each_value(field, |s, bm| {
                    if let Scalar::Str(v) = s {
                        if re.is_match(v.as_bytes()) { out |= bm; }
                    }
                });
                &out & universe
            }
            Query::Ids(ids) => {
                let mut out = RoaringBitmap::new();
                for id in ids {
                    if let Ok(Some((row, _, _))) = col.get_by_id(id) {
                        out.insert(row);
                    }
                }
                &out & universe
            }
            Query::Bool { must, should, must_not, filter, min_should_match } => {
                let mut cur = universe.clone();
                for q in must.iter().chain(filter.iter()) {
                    cur = q.eval_in(col, &cur)?;
                    if cur.is_empty() { return Ok(cur); }
                }
                if !should.is_empty() {
                    let min = if *min_should_match == 0 && must.is_empty() && filter.is_empty() {
                        1
                    } else { *min_should_match };
                    if min > 0 {
                        // Need at least `min` of the shoulds to match.
                        let mut hit_counts: ahash::AHashMap<u32, u32> = ahash::AHashMap::new();
                        for q in should {
                            let bm = q.eval_in(col, &cur)?;
                            for r in bm.iter() { *hit_counts.entry(r).or_default() += 1; }
                        }
                        let mut out = RoaringBitmap::new();
                        for (r, c) in hit_counts {
                            if c as usize >= min { out.insert(r); }
                        }
                        cur = out;
                    } else {
                        // zero min — shoulds are merely boosts; behave as OR-inclusive.
                        let mut any = RoaringBitmap::new();
                        for q in should { any |= q.eval_in(col, &cur)?; }
                        if !any.is_empty() { cur &= &any; }
                    }
                }
                for q in must_not {
                    let bm = q.eval_in(col, universe)?;
                    cur -= bm;
                }
                cur
            }
        })
    }
}

fn in_range(v: &Scalar, lo: Option<&Scalar>, hi: Option<&Scalar>, li: bool, hi_inc: bool) -> bool {
    let cmp_lo = lo.map(|l| cmp_scalar(v, l)).unwrap_or(std::cmp::Ordering::Greater);
    let cmp_hi = hi.map(|h| cmp_scalar(v, h)).unwrap_or(std::cmp::Ordering::Less);
    let lo_ok = match cmp_lo {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => li,
        _ => false,
    };
    let hi_ok = match cmp_hi {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => hi_inc,
        _ => false,
    };
    lo_ok && hi_ok
}

pub(crate) fn cmp_scalar(a: &Scalar, b: &Scalar) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Scalar::I64(x),  Scalar::I64(y))  => x.cmp(y),
        (Scalar::U64(x),  Scalar::U64(y))  => x.cmp(y),
        (Scalar::F64(x),  Scalar::F64(y))  => x.partial_cmp(y).unwrap_or(Equal),
        (Scalar::Ts(x),   Scalar::Ts(y))   => x.cmp(y),
        (Scalar::Ip(x),   Scalar::Ip(y))   => x.cmp(y),
        (Scalar::Str(x),  Scalar::Str(y))  => x.cmp(y),
        (Scalar::Bool(x), Scalar::Bool(y)) => x.cmp(y),
        // mixed numeric coercions
        (Scalar::I64(x), Scalar::U64(y)) => (*x as i128).cmp(&(*y as i128)),
        (Scalar::U64(x), Scalar::I64(y)) => (*x as i128).cmp(&(*y as i128)),
        (Scalar::I64(x), Scalar::F64(y)) => (*x as f64).partial_cmp(y).unwrap_or(Equal),
        (Scalar::F64(x), Scalar::I64(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Equal),
        (Scalar::Ts(x),  Scalar::I64(y)) => x.cmp(y),
        (Scalar::I64(x), Scalar::Ts(y))  => x.cmp(y),
        _ => Equal,
    }
}

fn ipv6_prefix_mask(prefix: u8) -> u128 {
    if prefix == 0 { 0 } else if prefix >= 128 { u128::MAX } else {
        (!0u128) << (128 - prefix)
    }
}

fn parse_cidr(s: &str) -> Option<(u128, u8)> {
    let (ip_str, prefix_str) = s.split_once('/')?;
    let prefix: u8 = prefix_str.parse().ok()?;
    let ip = Scalar::parse_ip(ip_str)?;
    // If input was IPv4 (no ':'), prefix is v4-style — convert to v6 prefix.
    let v6_prefix = if ip_str.contains(':') { prefix } else { prefix.saturating_add(96) };
    Some((ip, v6_prefix))
}

/// Parse an ES date string into (epoch_ms, granularity_ms).
/// granularity_ms is the width of the string's implied time window:
///   "YYYY-MM-DD"                  -> 86_400_000
///   "...THH"                      -> 3_600_000
///   "...THH:MM"                   -> 60_000
///   "...THH:MM:SS"                -> 1_000
///   "...THH:MM:SS.SSS"            -> 1
/// Caller adds (granularity - 1) to the low bound only when the range is
/// an upper bound (`lt`/`lte`) per ES rounding rules:
///   gt  uses (ms + granularity) exclusive == ms+granularity-1 exclusive+1, but
///   our compiler uses inclusive/exclusive flags so we return ms_up = ms+gran-1
///   for upper bounds and ms for lower.
#[allow(dead_code)]
fn parse_date_to_ms(s: &str) -> Option<i64> { parse_date_with_gran(s).map(|(ms, _)| ms) }

fn parse_date_with_gran(s: &str) -> Option<(i64, i64)> {
    if let Ok(v) = s.parse::<i64>() { return Some((v, 1)); }

    use time::format_description::FormatItem;
    fn gran_from_len(s: &str) -> i64 {
        // Count how many of [year, month, day, hour, min, sec, frac] are present.
        // Use simple heuristic on presence of separators/digits.
        let has_t = s.contains('T') || s.contains(' ');
        let body = s.trim_start_matches(|c: char| c.is_ascii_digit() || c=='-');
        // Body starts after date part "YYYY-MM-DD"
        if !has_t { return 86_400_000; }
        // time portion
        let time_part = s.split(|c| c=='T' || c==' ').nth(1).unwrap_or("");
        // strip tz
        let tp = time_part.split(|c:char| c=='+' || c=='Z' || (c=='-' && time_part.find(c)!=Some(0))).next().unwrap_or(time_part);
        let tp = tp.trim_end_matches('Z');
        if tp.contains('.') { return 1; }
        let colons = tp.chars().filter(|&c| c==':').count();
        let _ = body; // unused
        match colons {
            0 => 3_600_000,
            1 => 60_000,
            _ => 1_000,
        }
    }
    let gran = gran_from_len(s);

    // Try RFC3339 (with tz).
    if let Ok(d) = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339) {
        let ms = (d.unix_timestamp_nanos() / 1_000_000) as i64;
        return Some((ms, gran));
    }
    // Naive "YYYY-MM-DDTHH:MM:SS" or with space — assume UTC.
    let fmts: &[&[FormatItem]] = &[
        time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]"),
        time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"),
        time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]"),
        time::macros::format_description!("[year]-[month]-[day]T[hour]"),
    ];
    for f in fmts {
        if let Ok(d) = time::PrimitiveDateTime::parse(s, f) {
            let ms = (d.assume_utc().unix_timestamp_nanos() / 1_000_000) as i64;
            return Some((ms, gran));
        }
    }
    // Date only.
    if let Ok(d) = time::Date::parse(s, time::macros::format_description!("[year]-[month]-[day]")) {
        let ms = (d.with_hms(0,0,0).ok()?.assume_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        return Some((ms, 86_400_000));
    }
    None
}

/// ES regexp queries are implicitly anchored — the pattern must match the whole
/// value. Wrap with `\A(?:...)\z` unless already anchored.
fn es_anchor_regex(pattern: &str) -> String {
    let starts = pattern.starts_with('^') || pattern.starts_with("\\A");
    let ends = pattern.ends_with('$') || pattern.ends_with("\\z");
    match (starts, ends) {
        (true, true)   => pattern.to_string(),
        (true, false)  => format!("{pattern}\\z"),
        (false, true)  => format!("\\A{pattern}"),
        (false, false) => format!("\\A(?:{pattern})\\z"),
    }
}

// --- ES query-DSL subset compiler ----------------------------------------

/// Compile an ES `query` JSON object into our internal `Query`.
/// Supported clauses: match_all, match_none, term, terms, match (as term),
/// range, exists, wildcard, regexp, ids, bool.
pub fn compile_es_query(j: &J, col: &Arc<Collection>) -> Result<Query> {
    let Some(obj) = j.as_object() else {
        return Err(Error::BadRequest("query must be an object".into()));
    };
    if obj.is_empty() { return Ok(Query::MatchAll); }
    let (clause, body) = obj.iter().next().unwrap();
    match clause.as_str() {
        "match_all"  => Ok(Query::MatchAll),
        "match_none" => Ok(Query::MatchNone),
        "exists" => {
            let field = body.get("field").and_then(|v| v.as_str())
                .ok_or_else(|| Error::BadRequest("exists.field required".into()))?;
            Ok(Query::Exists { field: field.into() })
        }
        "ids" => {
            // ES `ids` query — match by document `_id`.
            let values = body.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let ids: Vec<String> = values.iter().filter_map(|v| v.as_str().map(String::from)).collect();
            Ok(Query::Ids(ids))
        }
        "term" | "match" | "match_phrase" => {
            let (field, val) = single_field_body(body)?;
            let v = if let Some(o) = val.as_object() {
                o.get("value").or(o.get("query")).cloned().unwrap_or(J::Null)
            } else { val.clone() };
            if field == "_id" {
                let ids: Vec<String> = v.as_str().map(|s| vec![s.to_string()]).unwrap_or_default();
                return Ok(Query::Ids(ids));
            }
            let ft = col.index.field_type(&field).or_else(|| {
                col.schema.read().fields.get(&field).copied()
            }).unwrap_or(FieldType::Keyword);
            if ft == FieldType::Ip {
                if let Some(s) = v.as_str() {
                    if s.contains('/') {
                        if let Some((net, prefix)) = parse_cidr(s) {
                            return Ok(Query::Cidr { field, net, prefix });
                        }
                    }
                }
            }
            // Tokenized text fields: split query the same way as indexing and AND.
            if ft == FieldType::Text {
                if let Some(s) = v.as_str() {
                    let toks = arkimedb_core::tokenize_text(s);
                    if toks.is_empty() {
                        return Ok(Query::MatchNone);
                    }
                    if toks.len() == 1 {
                        return Ok(Query::Term {
                            field,
                            values: vec![Scalar::Str(toks.into_iter().next().unwrap())],
                        });
                    }
                    let must: Vec<Query> = toks.into_iter()
                        .map(|t| Query::Term { field: field.clone(), values: vec![Scalar::Str(t)] })
                        .collect();
                    return Ok(Query::Bool {
                        must, should: vec![], must_not: vec![], filter: vec![],
                        min_should_match: 0,
                    });
                }
            }
            let scal = arkimedb_core::value::coerce(&v, ft)
                .ok_or_else(|| Error::BadRequest(format!("term value not coercible: field={field} value={v}")))?;
            Ok(Query::Term { field, values: vec![scal] })
        }
        "terms" => {
            let (field, val) = single_field_body(body)?;
            let arr = val.as_array().ok_or_else(|| Error::BadRequest("terms value must be array".into()))?;
            if field == "_id" {
                let ids: Vec<String> = arr.iter().filter_map(|v| v.as_str().map(String::from)).collect();
                return Ok(Query::Ids(ids));
            }
            let ft = col.index.field_type(&field).unwrap_or(FieldType::Keyword);
            let scalars: Vec<Scalar> = arr.iter().filter_map(|v| arkimedb_core::value::coerce(v, ft)).collect();
            Ok(Query::Term { field, values: scalars })
        }
        "range" => {
            let (field, val) = single_field_body(body)?;
            let ft = col.index.field_type(&field).unwrap_or(FieldType::I64);
            let o = val.as_object().ok_or_else(|| Error::BadRequest("range body must be object".into()))?;
            // is_upper: true for lt/lte (rounds up); false for gt/gte (rounds down).
            let coerce_bound = |v: &J, is_upper: bool| -> Option<Scalar> {
                if let Some(s) = v.as_str() {
                    if matches!(ft, FieldType::I64 | FieldType::U64 | FieldType::Timestamp) {
                        if let Some((mut ms, gran)) = parse_date_with_gran(s) {
                            if is_upper && gran > 1 { ms += gran - 1; }
                            return Some(match ft {
                                FieldType::Timestamp => Scalar::Ts(ms),
                                FieldType::U64 if ms >= 0 => Scalar::U64(ms as u64),
                                _ => Scalar::I64(ms),
                            });
                        }
                    }
                }
                arkimedb_core::value::coerce(v, ft)
            };
            let mut lo = None; let mut hi = None;
            let mut li = true; let mut hi_inc = true;
            if let Some(v) = o.get("gt")  { lo = coerce_bound(v, false); li = false; }
            if let Some(v) = o.get("gte") { lo = coerce_bound(v, false); li = true; }
            if let Some(v) = o.get("lt")  { hi = coerce_bound(v, true);  hi_inc = false; }
            if let Some(v) = o.get("lte") { hi = coerce_bound(v, true);  hi_inc = true; }
            Ok(Query::Range { field, lo, hi, lo_inclusive: li, hi_inclusive: hi_inc })
        }
        "wildcard" => {
            let (field, val) = single_field_body(body)?;
            let pat = val.as_str().map(|s| s.to_string()).or_else(|| {
                val.as_object().and_then(|o| o.get("value")).and_then(|v| v.as_str()).map(|s| s.to_string())
            }).ok_or_else(|| Error::BadRequest("wildcard value required".into()))?;
            Ok(Query::Wildcard { field, pattern: pat })
        }
        "regexp" | "regex" => {
            let (field, val) = single_field_body(body)?;
            let pat = val.as_str().map(|s| s.to_string()).or_else(|| {
                val.as_object().and_then(|o| o.get("value")).and_then(|v| v.as_str()).map(|s| s.to_string())
            }).ok_or_else(|| Error::BadRequest("regexp value required".into()))?;
            Ok(Query::Regexp { field, pattern: pat })
        }
        "prefix" => {
            let (field, val) = single_field_body(body)?;
            let pre = val.as_str().map(|s| s.to_string()).or_else(|| {
                val.as_object().and_then(|o| o.get("value")).and_then(|v| v.as_str()).map(|s| s.to_string())
            }).ok_or_else(|| Error::BadRequest("prefix value required".into()))?;
            let mut esc = String::with_capacity(pre.len() + 1);
            for c in pre.chars() {
                if c == '*' || c == '?' || c == '\\' { esc.push('\\'); }
                esc.push(c);
            }
            esc.push('*');
            Ok(Query::Wildcard { field, pattern: esc })
        }
        "bool" => {
            let o = body.as_object().ok_or_else(|| Error::BadRequest("bool body must be object".into()))?;
            let mut must = vec![]; let mut should = vec![]; let mut must_not = vec![]; let mut filter = vec![];
            let mut min_should_match = 0;
            for (k, v) in o {
                let list = match k.as_str() {
                    "must" => &mut must, "should" => &mut should,
                    "must_not" => &mut must_not, "filter" => &mut filter,
                    "minimum_should_match" => {
                        min_should_match = v.as_u64().unwrap_or(0) as usize; continue;
                    }
                    _ => continue,
                };
                if let Some(arr) = v.as_array() {
                    for c in arr { list.push(compile_es_query(c, col)?); }
                } else { list.push(compile_es_query(v, col)?); }
            }
            Ok(Query::Bool { must, should, must_not, filter, min_should_match })
        }
        "constant_score" | "filter" => {
            let inner = body.get("filter").unwrap_or(body);
            compile_es_query(inner, col)
        }
        other => Err(Error::BadRequest(format!("unsupported query clause: {other}"))),
    }
}

fn single_field_body(body: &J) -> Result<(String, J)> {
    let o = body.as_object().ok_or_else(|| Error::BadRequest("expected object".into()))?;
    let (k, v) = o.iter().next().ok_or_else(|| Error::BadRequest("empty predicate".into()))?;
    Ok((k.clone(), v.clone()))
}

mod glob {
    pub struct Matcher { parts: Vec<Part> }
    enum Part { Star, Any, Lit(String) }
    impl Matcher {
        pub fn compile(pat: &str) -> Self {
            let mut parts = Vec::new();
            let mut buf = String::new();
            for ch in pat.chars() {
                match ch {
                    '*' => { if !buf.is_empty() { parts.push(Part::Lit(std::mem::take(&mut buf))); } parts.push(Part::Star); }
                    '?' => { if !buf.is_empty() { parts.push(Part::Lit(std::mem::take(&mut buf))); } parts.push(Part::Any); }
                    c   => buf.push(c),
                }
            }
            if !buf.is_empty() { parts.push(Part::Lit(buf)); }
            Self { parts }
        }
        pub fn is_match(&self, s: &str) -> bool { m(&self.parts, s) }
    }
    fn m(parts: &[Part], s: &str) -> bool {
        match parts.first() {
            None => s.is_empty(),
            Some(Part::Lit(l)) => s.strip_prefix(l.as_str()).map(|r| m(&parts[1..], r)).unwrap_or(false),
            Some(Part::Any)   => !s.is_empty() && m(&parts[1..], &s[s.chars().next().unwrap().len_utf8()..]),
            Some(Part::Star)  => {
                let mut cur = s;
                loop {
                    if m(&parts[1..], cur) { return true; }
                    if cur.is_empty() { return false; }
                    cur = &cur[cur.chars().next().unwrap().len_utf8()..];
                }
            }
        }
    }
}
