//! Schema: field types, dynamic mapping inference, per-collection catalogs.

use std::collections::BTreeMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    Keyword,
    Ip,
    I64,
    U64,
    F64,
    Bool,
    Timestamp,
    /// Opaque JSON object (not queryable, stored verbatim).
    Json,
    /// Tokenized text (split on \W+, lowercased). Indexed per-token as keyword.
    Text,
}

impl Default for FieldType {
    fn default() -> Self { FieldType::Keyword }
}

/// Name-based dynamic mapping: `*Ip / *.ip → Ip`, `*Tokens → Text` (tokenized),
/// otherwise infer from the JSON value type.
pub fn infer_type_from_name(name: &str, sample: Option<&serde_json::Value>) -> FieldType {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with("tokens") || lower.ends_with(".tokens") {
        return FieldType::Text;
    }
    if lower.ends_with("ip")
        || lower.ends_with(".ip")
        || lower == "ip"
        || lower.ends_with("_ip")
        || lower.ends_with("ipv4")
        || lower.ends_with("ipv6")
    {
        return FieldType::Ip;
    }
    if lower.ends_with("timestamp") || lower.ends_with(".time")
        || lower == "firstpacket" || lower == "lastpacket"
        || lower == "created" || lower == "lastused"
        || lower.ends_with("_at")
    {
        return FieldType::Timestamp;
    }
    match sample {
        Some(serde_json::Value::Bool(_)) => FieldType::Bool,
        Some(serde_json::Value::Number(n)) => {
            if n.is_i64() { FieldType::I64 }
            else if n.is_u64() { FieldType::U64 }
            else { FieldType::F64 }
        }
        Some(serde_json::Value::Object(_)) => FieldType::Json,
        Some(serde_json::Value::Array(a)) => a
            .iter()
            .find_map(|v| if !v.is_null() { Some(infer_type_from_name(name, Some(v))) } else { None })
            .unwrap_or(FieldType::Keyword),
        _ => FieldType::Keyword,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DynamicTemplate {
    /// Glob on field NAME (ES `match` / `path_match`). e.g. "*Tokens", "*.ip".
    #[serde(default)]
    pub match_glob: Option<String>,
    /// Glob on full DOTTED path (ES `path_match`). Same syntax as `match_glob`.
    #[serde(default)]
    pub path_match: Option<String>,
    /// Match by JSON value type: "string" | "long" | "double" | "boolean" | "date" | "object".
    #[serde(default)]
    pub match_mapping_type: Option<String>,
    /// Target ES field type from `mapping.type`.
    pub field_type: FieldType,
    /// `copy_to` from the dynamic_template's mapping body.
    #[serde(default)]
    pub copy_to: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CollectionSchema {
    pub version: u32,
    pub fields: BTreeMap<String, FieldType>,
    /// Arbitrary opaque properties (Arkime stores schema version, dbVersion, etc.)
    pub metadata: serde_json::Map<String, serde_json::Value>,
    /// Map: source field → list of target field names to also-index (ES `copy_to`).
    #[serde(default)]
    pub copy_to: BTreeMap<String, Vec<String>>,
    /// Dynamic templates evaluated when a new (uninferred) field is observed.
    #[serde(default)]
    pub dynamic_templates: Vec<DynamicTemplate>,
}

/// Match a field name against a single ES-style glob ("*Tokens", "foo.*", "ip").
/// `*` matches any chars (including dots — ES does the same for path_match).
/// Match a field/index name against a single ES-style glob ("*Tokens", "foo*").
/// `*` matches zero or more chars (including dots — ES uses the same for path_match).
pub fn schema_glob_match(pat: &str, name: &str) -> bool { glob_match(pat, name) }

fn glob_match(pat: &str, name: &str) -> bool {
    fn m(p: &[u8], n: &[u8]) -> bool {
        if p.is_empty() { return n.is_empty(); }
        if p[0] == b'*' {
            // '*' matches zero or more
            for i in 0..=n.len() {
                if m(&p[1..], &n[i..]) { return true; }
            }
            false
        } else if !n.is_empty() && (p[0] == b'?' || p[0] == n[0]) {
            m(&p[1..], &n[1..])
        } else { false }
    }
    m(pat.as_bytes(), name.as_bytes())
}

/// Resolve (FieldType, copy_to) for a brand-new field by walking
/// `dynamic_templates` in declaration order. Returns None if no template matches.
pub fn match_dynamic_template(
    templates: &[DynamicTemplate],
    full_path: &str,
    sample: Option<&serde_json::Value>,
) -> Option<(FieldType, Vec<String>)> {
    let leaf = full_path.rsplit('.').next().unwrap_or(full_path);
    let mapping_type = sample.map(|v| match v {
        serde_json::Value::String(_) => "string",
        serde_json::Value::Bool(_)   => "boolean",
        serde_json::Value::Number(n) => if n.is_f64() { "double" } else { "long" },
        serde_json::Value::Object(_) => "object",
        serde_json::Value::Array(_)  => "array",
        serde_json::Value::Null      => "null",
    });
    for t in templates {
        let mut ok = false;
        if let Some(p) = &t.match_glob { if glob_match(p, leaf) { ok = true; } }
        if let Some(p) = &t.path_match { if glob_match(p, full_path) { ok = true; } }
        if let Some(mt) = &t.match_mapping_type {
            if mapping_type.map(|x| x == mt).unwrap_or(false) { ok = true; }
        }
        if ok {
            return Some((t.field_type, t.copy_to.clone()));
        }
    }
    None
}

/// Tokenize a string the way Arkime's `wordSplit` analyzer does: split on
/// non-word characters (`\W+`) and lowercase each token. Empty tokens dropped.
pub fn tokenize_text(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in s.chars() {
        if c.is_alphanumeric() || c == '_' {
            for lc in c.to_lowercase() { cur.push(lc); }
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() { out.push(cur); }
    out
}

/// In-memory field catalog for a running node. Authoritative state is persisted
/// in the global redb catalog; this is the runtime view.
#[derive(Debug, Default)]
pub struct FieldCatalog {
    inner: RwLock<BTreeMap<String, CollectionSchema>>,
}

impl FieldCatalog {
    pub fn new() -> Self { Self::default() }

    pub fn get(&self, col: &str) -> Option<CollectionSchema> {
        self.inner.read().get(col).cloned()
    }

    pub fn replace(&self, col: &str, schema: CollectionSchema) {
        self.inner.write().insert(col.to_string(), schema);
    }

    pub fn ensure_collection(&self, col: &str) {
        let mut g = self.inner.write();
        g.entry(col.to_string()).or_default();
    }

    /// Merge new field → type mappings inferred from an incoming record.
    /// Returns `true` if the schema changed.
    pub fn merge_from_record(&self, col: &str, record: &serde_json::Value) -> bool {
        let mut g = self.inner.write();
        let schema = g.entry(col.to_string()).or_default();
        let mut changed = false;
        if let serde_json::Value::Object(map) = record {
            for (k, v) in map {
                if schema.fields.contains_key(k) { continue; }
                let ft = infer_type_from_name(k, Some(v));
                schema.fields.insert(k.clone(), ft);
                changed = true;
            }
            if changed { schema.version += 1; }
        }
        changed
    }

    pub fn all_collections(&self) -> Vec<String> {
        self.inner.read().keys().cloned().collect()
    }
}
