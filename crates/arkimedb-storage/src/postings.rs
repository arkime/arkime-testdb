//! In-memory + persisted posting lists (roaring bitmaps per (field, value) key).

use ahash::AHashMap;
use parking_lot::RwLock;
use roaring::RoaringBitmap;

use arkimedb_core::{Scalar, FieldType};
use crate::codec;

#[derive(Default)]
pub struct PostingsIndex {
    /// key bytes (from `codec::encode_key`) -> bitmap of row ids
    pub(crate) map: RwLock<AHashMap<Vec<u8>, RoaringBitmap>>,
    /// `field name -> bitmap of row ids where field exists`
    pub(crate) exists: RwLock<AHashMap<String, RoaringBitmap>>,
    /// `(field, FieldType)` observed at ingest (for term iteration per field).
    pub(crate) fields: RwLock<AHashMap<String, FieldType>>,
    /// Per-field values list (kept only for keyword-ish fields to support wildcard/regex/terms).
    /// Map: field -> (scalar -> bitmap). Keyword/IP/timestamp/int stored here.
    pub(crate) per_field: RwLock<AHashMap<String, Vec<Scalar>>>,
}

impl PostingsIndex {
    pub fn new() -> Self { Self::default() }

    pub fn insert(&self, field: &str, ft: FieldType, values: &[Scalar], row_id: u32) {
        if values.is_empty() { return; }
        self.fields.write().insert(field.to_string(), ft);
        {
            let mut ex = self.exists.write();
            ex.entry(field.to_string()).or_default().insert(row_id);
        }
        let mut map = self.map.write();
        let mut pf = self.per_field.write();
        let pf_entry = pf.entry(field.to_string()).or_default();
        for s in values {
            let k = codec::encode_key(field, s);
            let entry = map.entry(k).or_default();
            if entry.is_empty() { pf_entry.push(s.clone()); }
            entry.insert(row_id);
        }
    }

    pub fn get(&self, field: &str, s: &Scalar) -> RoaringBitmap {
        let k = codec::encode_key(field, s);
        self.map.read().get(&k).cloned().unwrap_or_default()
    }

    pub fn exists_bm(&self, field: &str) -> RoaringBitmap {
        self.exists.read().get(field).cloned().unwrap_or_default()
    }

    pub fn mark_exists(&self, field: &str, row_id: u32) {
        let mut ex = self.exists.write();
        ex.entry(field.to_string()).or_default().insert(row_id);
    }

    pub fn field_type(&self, field: &str) -> Option<FieldType> {
        self.fields.read().get(field).copied()
    }

    /// Stream (value, bitmap) pairs for a field — used by terms aggregation,
    /// wildcard/regex matching, range scans.
    pub fn for_each_value<F: FnMut(&Scalar, &RoaringBitmap)>(&self, field: &str, mut f: F) {
        let pf = self.per_field.read();
        let Some(values) = pf.get(field) else { return; };
        let map = self.map.read();
        for s in values {
            let k = codec::encode_key(field, s);
            if let Some(bm) = map.get(&k) { f(s, bm); }
        }
    }

    pub fn all_fields(&self) -> Vec<(String, FieldType)> {
        self.fields.read().iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    /// Remove `row_id` from all posting lists / exists bitmaps.
    pub fn remove_row(&self, row_id: u32) {
        let mut map = self.map.write();
        for bm in map.values_mut() { bm.remove(row_id); }
        let mut ex = self.exists.write();
        for bm in ex.values_mut() { bm.remove(row_id); }
    }
}
