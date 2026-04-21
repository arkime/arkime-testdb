//! The per-collection engine and top-level orchestrator.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use ahash::AHashMap;
use parking_lot::RwLock;
use redb::{Database, TableDefinition, ReadableTable};
use roaring::RoaringBitmap;

use arkimedb_core::{
    config::{Config, StorageConfig},
    Error, Result, FieldCatalog, CollectionSchema,
};

use crate::catalog::GlobalCatalog;
use crate::postings::PostingsIndex;
use crate::codec;

const DOCS: TableDefinition<u32, &[u8]>       = TableDefinition::new("docs");        // row_id -> zstd(json)
const ID2ROW: TableDefinition<&str, u32>      = TableDefinition::new("id2row");      // doc_id -> row_id
const ROW2ID: TableDefinition<u32, &str>      = TableDefinition::new("row2id");      // row_id -> doc_id
const VERSIONS: TableDefinition<u32, u64>     = TableDefinition::new("versions");    // row_id -> version
const META: TableDefinition<&str, u64>        = TableDefinition::new("meta");        // scalar counters
const TOMBSTONES: TableDefinition<u32, u8>    = TableDefinition::new("tombstones");  // row_id -> 1 if deleted

#[derive(Clone, Copy, Debug)]
pub struct DocHit {
    pub row_id: u32,
    pub version: u64,
}

pub struct Collection {
    pub name: String,
    pub path: PathBuf,
    pub(crate) db: Arc<Database>,
    pub index: Arc<PostingsIndex>,
    pub schema: RwLock<CollectionSchema>,
    /// Tombstoned row ids (not yet GC'd). Also stored in `TOMBSTONES`.
    pub tombstones: RwLock<RoaringBitmap>,
    pub(crate) storage_cfg: StorageConfig,
}

impl Collection {
    pub fn row_count(&self) -> Result<u64> {
        let r = self.db.begin_read()?;
        let t = r.open_table(META)?;
        Ok(t.get("doc_count")?.map(|v| v.value()).unwrap_or(0))
    }

    /// Highest assigned `row_id + 1`. Cheap (single META lookup), used by
    /// `all_live_rows` to avoid scanning the full DOCS B-tree.
    pub fn next_row_id_value(&self) -> Result<u32> {
        let r = self.db.begin_read()?;
        let t = r.open_table(META)?;
        Ok(t.get("next_row_id")?.map(|v| v.value()).unwrap_or(0) as u32)
    }

    /// Batch-load `(doc_id, decompressed_source)` for the given rows, reusing
    /// a single read transaction. This is the search hydrate hot path; opening
    /// a tx + tables per hit was 2× slower than ES.
    pub fn hydrate_rows(&self, rows: &[u32], want_id: bool, want_raw: bool)
        -> Result<Vec<(Option<String>, Option<Vec<u8>>)>>
    {
        let r = self.db.begin_read()?;
        let row2id = if want_id  { Some(r.open_table(ROW2ID)?) } else { None };
        let docs   = if want_raw { Some(r.open_table(DOCS)?)   } else { None };
        let mut out = Vec::with_capacity(rows.len());
        for &row_id in rows {
            let id = if let Some(t) = &row2id {
                t.get(row_id)?.map(|v| v.value().to_string())
            } else { None };
            let raw = if let Some(t) = &docs {
                match t.get(row_id)? {
                    Some(v) => Some(codec::decompress(v.value())?),
                    None => None,
                }
            } else { None };
            out.push((id, raw));
        }
        Ok(out)
    }

    pub fn next_row_id(&self, w: &redb::WriteTransaction) -> Result<u32> {
        let mut t = w.open_table(META)?;
        let cur = t.get("next_row_id")?.map(|v| v.value()).unwrap_or(0);
        let next = cur + 1;
        t.insert("next_row_id", next)?;
        Ok(cur as u32)
    }

    pub fn get_by_id(&self, id: &str) -> Result<Option<(u32, u64, Vec<u8>)>> {
        let r = self.db.begin_read()?;
        let id2row = r.open_table(ID2ROW)?;
        let Some(row_id) = id2row.get(id)?.map(|v| v.value()) else { return Ok(None); };
        if self.tombstones.read().contains(row_id) { return Ok(None); }
        let docs = r.open_table(DOCS)?;
        let ver_t = r.open_table(VERSIONS)?;
        let Some(raw) = docs.get(row_id)?.map(|v| v.value().to_vec()) else { return Ok(None); };
        let version = ver_t.get(row_id)?.map(|v| v.value()).unwrap_or(1);
        let json_bytes = codec::decompress(&raw)?;
        Ok(Some((row_id, version, json_bytes)))
    }

    /// Returns true if present.
    pub fn has_id(&self, id: &str) -> Result<bool> {
        let r = self.db.begin_read()?;
        let id2row = r.open_table(ID2ROW)?;
        match id2row.get(id)? {
            Some(v) => Ok(!self.tombstones.read().contains(v.value())),
            None => Ok(false),
        }
    }

    pub fn get_raw_by_row(&self, row_id: u32) -> Result<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        let docs = r.open_table(DOCS)?;
        match docs.get(row_id)? {
            Some(v) => Ok(Some(codec::decompress(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn doc_id_of(&self, row_id: u32) -> Result<Option<String>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(ROW2ID)?;
        Ok(t.get(row_id)?.map(|v| v.value().to_string()))
    }

    /// Live (non-tombstoned) row ids. Computed as `(0..next_row_id) - tombstones`
    /// rather than scanning the full DOCS B-tree, which used to dominate query
    /// time on indices with millions of docs.
    pub fn all_live_rows(&self) -> Result<RoaringBitmap> {
        let n = self.next_row_id_value()?;
        let mut bm = RoaringBitmap::new();
        if n > 0 { bm.insert_range(0..n); }
        // Borrow the tombstones read-guard; avoid cloning the whole bitmap.
        let tomb = self.tombstones.read();
        Ok(&bm - &*tomb)
    }

    fn hydrate_index(&self) -> Result<()> {
        // Rebuild in-memory posting lists and field catalog from persisted docs.
        let r = self.db.begin_read()?;
        let docs = r.open_table(DOCS)?;
        let tomb = r.open_table(TOMBSTONES)?;
        let mut tomb_bm = RoaringBitmap::new();
        for row in tomb.iter()? {
            let (k, _) = row?;
            tomb_bm.insert(k.value());
        }
        *self.tombstones.write() = tomb_bm.clone();

        let mut schema_guard = self.schema.write();
        for row in docs.iter()? {
            let (k, v) = row?;
            let row_id = k.value();
            if tomb_bm.contains(row_id) { continue; }
            let bytes = codec::decompress(v.value())?;
            let json: serde_json::Value = serde_json::from_slice(&bytes)?;
            index_one(&self.index, &mut *schema_guard, row_id, &json);
        }
        Ok(())
    }
}

fn index_one(
    index: &PostingsIndex,
    schema: &mut CollectionSchema,
    row_id: u32,
    json: &serde_json::Value,
) {
    let serde_json::Value::Object(map) = json else { return; };
    for (field, val) in map {
        index_value(index, schema, row_id, field, val);
    }
}

fn resolve_field_type(
    schema: &mut CollectionSchema,
    path: &str,
    sample: Option<&serde_json::Value>,
) -> arkimedb_core::FieldType {
    if let Some(ft) = schema.fields.get(path).copied() { return ft; }
    if let Some((ft, copy_to)) = arkimedb_core::match_dynamic_template(&schema.dynamic_templates, path, sample) {
        schema.fields.insert(path.to_string(), ft);
        if !copy_to.is_empty() {
            schema.copy_to.entry(path.to_string()).or_default().extend(copy_to);
        }
        return ft;
    }
    let ft = arkimedb_core::infer_type_from_name(path, sample);
    schema.fields.insert(path.to_string(), ft);
    ft
}

fn index_value(
    index: &PostingsIndex,
    schema: &mut CollectionSchema,
    row_id: u32,
    path: &str,
    val: &serde_json::Value,
) {
    match val {
        serde_json::Value::Null => {}
        serde_json::Value::Array(arr) => {
            if arr.is_empty() { return; }
            let mut scalar_vals: Vec<&serde_json::Value> = Vec::new();
            for x in arr {
                match x {
                    serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                        index_value(index, schema, row_id, path, x);
                    }
                    serde_json::Value::Null => {}
                    _ => scalar_vals.push(x),
                }
            }
            if !scalar_vals.is_empty() {
                let ft = resolve_field_type(schema, path, Some(scalar_vals[0]));
                index_scalars(index, schema, row_id, path, ft, &scalar_vals);
            }
        }
        serde_json::Value::Object(sub) => {
            for (k, v) in sub {
                let child = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                index_value(index, schema, row_id, &child, v);
            }
        }
        _ => {
            let ft = resolve_field_type(schema, path, Some(val));
            index_scalars(index, schema, row_id, path, ft, &[val]);
        }
    }
}

/// Insert a slice of JSON scalar values for `path` (already known field type).
/// For Text fields: tokenize each string. For any field with `copy_to` targets:
/// also tokenize the source string(s) and index under each target as Text.
fn index_scalars(
    index: &PostingsIndex,
    schema: &mut CollectionSchema,
    row_id: u32,
    path: &str,
    ft: arkimedb_core::FieldType,
    vals: &[&serde_json::Value],
) {
    use arkimedb_core::{FieldType, Scalar};
    let mut scalars: Vec<Scalar> = Vec::new();
    if ft == FieldType::Text {
        for v in vals {
            if let Some(s) = v.as_str() {
                for tok in arkimedb_core::tokenize_text(s) {
                    scalars.push(Scalar::Str(tok));
                }
            }
        }
    } else {
        for sv in vals { codec::scalars_of(sv, ft, &mut scalars); }
    }
    if !scalars.is_empty() {
        index.insert(path, ft, &scalars, row_id);
        mark_ancestors_exists(index, path, row_id);
    }
    // Honor copy_to: tokenize source string(s) and index into each target as Text.
    let targets = schema.copy_to.get(path).cloned().unwrap_or_default();
    for target in &targets {
        let mut toks: Vec<Scalar> = Vec::new();
        for v in vals {
            if let Some(s) = v.as_str() {
                for tok in arkimedb_core::tokenize_text(s) {
                    toks.push(Scalar::Str(tok));
                }
            }
        }
        if !toks.is_empty() {
            schema.fields.entry(target.clone()).or_insert(FieldType::Text);
            index.insert(target, FieldType::Text, &toks, row_id);
            mark_ancestors_exists(index, target, row_id);
        }
    }
}

fn mark_ancestors_exists(index: &PostingsIndex, path: &str, row_id: u32) {
    let mut p = path;
    while let Some(i) = p.rfind('.') {
        p = &p[..i];
        index.mark_exists(p, row_id);
    }
}

// --- engine --------------------------------------------------------------

pub struct Engine {
    pub data_dir: PathBuf,
    pub catalog: Arc<GlobalCatalog>,
    pub field_catalog: Arc<FieldCatalog>,
    pub config: Config,
    collections: RwLock<AHashMap<String, Arc<Collection>>>,
}

impl Engine {
    pub fn open(config: Config) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.data_dir)?;
        let catalog = Arc::new(GlobalCatalog::open(&config.data_dir)?);
        let field_catalog = Arc::new(FieldCatalog::new());

        // Preload all known collections in parallel. Each collection opens a
        // redb file and hydrates its postings index from compressed docs;
        // these are independent so rayon gives near-linear speedup on startup.
        let names = catalog.list_collections()?;
        let total = names.len();
        let t_all = std::time::Instant::now();
        eprintln!("[engine] opening {} collections from {}", total, config.data_dir.display());

        let valid: Vec<String> = names.into_iter().filter(|n| {
            if n.len() > 200 || n.contains(',') || n.contains('*') || n.contains('?') {
                eprintln!("[engine] removing invalid collection entry: {:?}", n);
                let _ = catalog.unregister_collection(n);
                false
            } else { true }
        }).collect();

        use rayon::prelude::*;
        let loaded: Vec<Result<(String, Arc<Collection>)>> = valid
            .par_iter()
            .map(|name| -> Result<(String, Arc<Collection>)> {
                let schema = catalog.get_schema(name)?.unwrap_or_default();
                field_catalog.replace(name, schema.clone());
                let col = Arc::new(open_collection(
                    &config.data_dir, name, schema, config.storage.clone()
                )?);
                col.hydrate_index()?;
                Ok((name.clone(), col))
            })
            .collect();

        let mut collections: AHashMap<String, Arc<Collection>> = AHashMap::new();
        for r in loaded {
            let (name, col) = r?;
            collections.insert(name, col);
        }
        eprintln!("[engine] opened {} collections in {:.2}s", total, t_all.elapsed().as_secs_f32());

        Ok(Arc::new(Self {
            data_dir: config.data_dir.clone(),
            catalog,
            field_catalog,
            config,
            collections: RwLock::new(collections),
        }))
    }

    pub fn has_collection(&self, name: &str) -> bool {
        self.collections.read().contains_key(name)
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }

    pub fn get_collection(&self, name: &str) -> Option<Arc<Collection>> {
        self.collections.read().get(name).cloned()
    }

    pub fn ensure_collection(&self, name: &str) -> Result<Arc<Collection>> {
        // Reject obviously-invalid names (patterns, comma-lists, overlong).
        // These belong in alias/resolve paths, not as physical collections.
        if name.is_empty() || name.len() > 200 || name.contains(',') || name.contains('*') || name.contains('?') {
            return Err(Error::BadRequest(format!("invalid index name: {name:?}")));
        }
        // If `name` is an alias to exactly one target collection, write into the
        // target instead of creating a phantom collection with the alias's name.
        // This matches Elasticsearch semantics where writes to an alias are
        // routed to the aliased index.
        let effective: String = match self.catalog.get_alias(name)? {
            Some(targets) if targets.len() == 1 => targets.into_iter().next().unwrap(),
            _ => name.to_string(),
        };
        if let Some(c) = self.collections.read().get(&effective).cloned() { return Ok(c); }
        let mut g = self.collections.write();
        if let Some(c) = g.get(&effective).cloned() { return Ok(c); }
        let schema = self.catalog.get_schema(&effective)?.unwrap_or_default();
        self.catalog.register_collection(&effective, &schema)?;
        self.field_catalog.replace(&effective, schema.clone());
        let col = Arc::new(open_collection(
            &self.data_dir, &effective, schema, self.config.storage.clone()
        )?);
        col.hydrate_index()?;
        g.insert(effective, col.clone());
        Ok(col)
    }

    /// Resolve a logical target (which may be an alias, a wildcard-free single
    /// name, a comma-separated list, or a simple `foo*` wildcard) into the set
    /// of existing physical collections.
    pub fn resolve(&self, target: &str) -> Result<Vec<Arc<Collection>>> {
        let mut out: Vec<Arc<Collection>> = Vec::new();
        let mut seen: ahash::AHashSet<String> = Default::default();
        let parts: Vec<&str> = target.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        let cols = self.collections.read();
        for p in &parts {
            if let Some(targets) = self.catalog.get_alias(p)? {
                for t in targets {
                    if let Some(c) = cols.get(&t) {
                        if seen.insert(t.clone()) { out.push(c.clone()); }
                    }
                }
                continue;
            }
            if p.contains('*') || p.contains('?') {
                for (name, c) in cols.iter() {
                    if arkimedb_core::schema_glob_match(p, name) && seen.insert(name.clone()) {
                        out.push(c.clone());
                    }
                }
            } else if let Some(c) = cols.get(*p) {
                if seen.insert(p.to_string()) { out.push(c.clone()); }
            }
        }
        Ok(out)
    }

    pub fn bulk_write(&self, default_collection: Option<&str>, ops: Vec<BulkOp>) -> Result<Vec<BulkOutcome>> {
        // Group ops by collection while remembering each op's original index
        // so we can re-emit results in input order. ES `_bulk` response items
        // are ordered positionally — clients index into them by position.
        //
        // Prior version only batched *contiguous* same-collection ops; but
        // Arkime capture bulks frequently interleave 100+ daily sessions3-*
        // collections, yielding one begin_write/commit per op (~35ms each
        // for new-file creation). Grouping non-contiguous is safe as long
        // as within-collection order is preserved (ES last-write-wins for
        // same id holds trivially).
        //
        // Each collection is its own redb file, so commits across
        // collections are independent — run them in parallel. This is a
        // large win for bulks that create/touch many collections at once.
        use rayon::prelude::*;
        let n = ops.len();
        let mut by_coll: ahash::AHashMap<String, Vec<(usize, BulkOp)>> = Default::default();
        let mut fatal: Option<Error> = None;
        for (idx, op) in ops.into_iter().enumerate() {
            let coll = match op.collection.clone()
                .or_else(|| default_collection.map(String::from)) {
                Some(c) => c,
                None => { fatal = Some(Error::BadRequest("bulk op missing _index".into())); break; }
            };
            by_coll.entry(coll).or_default().push((idx, op));
        }
        if let Some(e) = fatal { return Err(e); }

        // Pre-ensure all collections outside the parallel section so the
        // engine's `collections` map is only mutated from this thread.
        let mut coll_groups: Vec<(Arc<Collection>, Vec<(usize, BulkOp)>, String)> = Vec::with_capacity(by_coll.len());
        for (name, group) in by_coll {
            let col = self.ensure_collection(&name)?;
            coll_groups.push((col, group, name));
        }

        // Parallel per-collection commits. Each closure returns
        // Vec<(orig_idx, BulkOutcome)>.
        let fc = self.field_catalog.clone();
        let per_coll: Vec<Vec<(usize, BulkOutcome)>> = coll_groups
            .into_par_iter()
            .map(|(col, group, cname)| {
                let mut out: Vec<(usize, BulkOutcome)> = Vec::with_capacity(group.len());
                // Split the group into runs of Index/Create vs single Delete/Update.
                // Batching runs of Index/Create uses one tx per run; Delete/Update
                // use the single-op helpers (still one tx each, but on same col).
                let mut i = 0;
                while i < group.len() {
                    let is_batchable = |op: &BulkOp| matches!(op.kind, BulkKind::Index{..} | BulkKind::Create{..});
                    if is_batchable(&group[i].1) {
                        let mut j = i;
                        while j < group.len() && is_batchable(&group[j].1) { j += 1; }
                        let refs: Vec<&BulkOp> = (i..j).map(|k| &group[k].1).collect();
                        match write_many_in_one_tx(&col, &fc, &refs) {
                            Ok(batch) => {
                                for (k, r) in batch.into_iter().enumerate() {
                                    let orig = group[i + k].0;
                                    out.push((orig, match r {
                                        Ok(br) => BulkOutcome::ok(&cname, br),
                                        Err(e) => BulkOutcome::err(&cname, e),
                                    }));
                                }
                            }
                            Err(e) => {
                                // tx-level failure: surface for every op in the batch
                                for k in i..j {
                                    let orig = group[k].0;
                                    out.push((orig, BulkOutcome::err(&cname, Error::Io(std::io::Error::other(e.to_string())))));
                                }
                            }
                        }
                        i = j;
                    } else {
                        let (orig, op) = &group[i];
                        let res = match &op.kind {
                            BulkKind::Delete { id } => delete_one(&col, id),
                            BulkKind::Update { id, doc } => update_one(&col, &fc, id, doc.clone()),
                            _ => unreachable!(),
                        };
                        out.push((*orig, match res {
                            Ok(r) => BulkOutcome::ok(&cname, r),
                            Err(e) => BulkOutcome::err(&cname, e),
                        }));
                        i += 1;
                    }
                }
                out
            })
            .collect();

        // Re-emit in original op order.
        let mut placed: Vec<Option<BulkOutcome>> = (0..n).map(|_| None).collect();
        for chunk in per_coll {
            for (orig, bo) in chunk { placed[orig] = Some(bo); }
        }
        Ok(placed.into_iter().map(|o| o.expect("bulk outcome slot")).collect())
    }

    /// Make recent writes visible to search.
    ///
    /// In arkimedb this is effectively a no-op: doc writes and tombstones
    /// are added to the in-memory index *and* the on-disk redb tables
    /// inside the same write transaction, so a document is immediately
    /// searchable as soon as its write returns. There is no buffered
    /// write-ahead segment to promote, the way Elasticsearch / Lucene
    /// have with `refresh_interval`.
    ///
    /// Previously this iterated every collection, opened a write tx, and
    /// re-inserted every in-memory tombstone — an O(collections * tombs)
    /// round trip that added tens of seconds to the Arkime test suite
    /// even though the data was already persisted by `delete_doc_in_tx`.
    pub fn refresh(&self, _target: Option<&str>) -> Result<()> {
        Ok(())
    }

    pub fn delete_collection(&self, name: &str) -> Result<bool> {
        let mut g = self.collections.write();
        if g.remove(name).is_none() { return Ok(false); }
        // remove file
        let p = collection_path(&self.data_dir, name);
        let _ = std::fs::remove_file(&p);
        Ok(true)
    }
}

// --- bulk op model --------------------------------------------------------

#[derive(Debug)]
pub enum BulkKind {
    Index  { id: Option<String>, source: serde_json::Value },
    Create { id: String,          source: serde_json::Value },
    Update { id: String,          doc: serde_json::Value },
    Delete { id: String },
}

#[derive(Debug)]
pub struct BulkOp {
    pub collection: Option<String>,
    pub kind: BulkKind,
}

#[derive(Debug, Clone)]
pub struct BulkResult {
    pub id: String,
    pub version: u64,
    pub action: &'static str,   // "index" | "create" | "update" | "delete"
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct BulkOutcome {
    pub collection: String,
    pub ok: Option<BulkResult>,
    pub error: Option<String>,
}

impl BulkOutcome {
    fn ok(c: &str, r: BulkResult) -> Self { Self { collection: c.to_string(), ok: Some(r), error: None } }
    fn err(c: &str, e: Error) -> Self { Self { collection: c.to_string(), ok: None, error: Some(e.to_string()) } }
}

#[derive(Debug, Clone)]
pub struct GetResult {
    pub collection: String,
    pub id: String,
    pub version: u64,
    pub source: serde_json::Value,
    pub row_id: u32,
}

// --- helpers --------------------------------------------------------------

fn open_collection(root: &Path, name: &str, schema: CollectionSchema, storage_cfg: StorageConfig) -> Result<Collection> {
    let path = collection_path(root, name);
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let db = Database::create(&path).map_err(Error::from)?;
    // create tables
    let mut w = db.begin_write()?;
    w.set_durability(redb::Durability::Eventual);
    {
        let _ = w.open_table(DOCS)?;
        let _ = w.open_table(ID2ROW)?;
        let _ = w.open_table(ROW2ID)?;
        let _ = w.open_table(VERSIONS)?;
        let _ = w.open_table(META)?;
        let _ = w.open_table(TOMBSTONES)?;
    }
    w.commit()?;
    Ok(Collection {
        name: name.to_string(),
        path,
        db: Arc::new(db),
        index: Arc::new(PostingsIndex::new()),
        schema: RwLock::new(schema),
        tombstones: RwLock::new(RoaringBitmap::new()),
        storage_cfg,
    })
}

fn collection_path(root: &Path, name: &str) -> PathBuf {
    // sanitize: disallow path separators
    let safe = name.replace('/', "_").replace('\\', "_");
    root.join("collections").join(format!("{safe}.redb"))
}

// --- tx-scoped doc write helpers ------------------------------------------

/// redb-only portion of an upsert. Returns (row_id, created, version).
/// In-memory index updates must be applied by the caller *after* `w.commit()`.
fn write_doc_in_tx(
    w: &redb::WriteTransaction,
    id: &str,
    compressed: &[u8],
    require_create: bool,
) -> Result<(u32, bool, u64)> {
    let mut id2row = w.open_table(ID2ROW)?;
    let existing = id2row.get(id)?.map(|v| v.value());
    let (row_id, created) = match existing {
        Some(r) => {
            if require_create { return Err(Error::Conflict(format!("document already exists: {id}"))); }
            (r, false)
        }
        None => {
            let mut meta = w.open_table(META)?;
            let cur = meta.get("next_row_id")?.map(|v| v.value()).unwrap_or(0);
            meta.insert("next_row_id", cur + 1)?;
            drop(meta);
            id2row.insert(id, cur as u32)?;
            let mut row2id = w.open_table(ROW2ID)?;
            row2id.insert(cur as u32, id)?;
            (cur as u32, true)
        }
    };
    drop(id2row);
    {
        let mut docs = w.open_table(DOCS)?;
        docs.insert(row_id, compressed)?;
    }
    let version = {
        let mut vt = w.open_table(VERSIONS)?;
        let cur = vt.get(row_id)?.map(|v| v.value()).unwrap_or(0);
        let new = cur + 1;
        vt.insert(row_id, new)?;
        new
    };
    if created {
        let mut meta = w.open_table(META)?;
        let cur = meta.get("doc_count")?.map(|v| v.value()).unwrap_or(0);
        meta.insert("doc_count", cur + 1)?;
    }
    Ok((row_id, created, version))
}

fn delete_doc_in_tx(w: &redb::WriteTransaction, id: &str) -> Result<Option<u32>> {
    let id2row = w.open_table(ID2ROW)?;
    let row_id = id2row.get(id)?.map(|v| v.value());
    drop(id2row);
    let Some(row_id) = row_id else { return Ok(None); };
    {
        let mut tomb = w.open_table(TOMBSTONES)?;
        tomb.insert(row_id, 1u8)?;
    }
    {
        let mut meta = w.open_table(META)?;
        let cur = meta.get("doc_count")?.map(|v| v.value()).unwrap_or(0);
        if cur > 0 { meta.insert("doc_count", cur - 1)?; }
    }
    Ok(Some(row_id))
}


fn write_one(col: &Arc<Collection>, fc: &Arc<FieldCatalog>, id: Option<String>, source: serde_json::Value, require_create: bool) -> Result<BulkResult> {
    if !matches!(source, serde_json::Value::Object(_)) {
        return Err(Error::BadRequest("source must be a JSON object".into()));
    }
    // schema inference (field catalog update). Only sync into the per-collection
    // schema when the catalog actually changed — the previous unconditional
    // `fc.get(...)` clone was O(n_fields) on every single write.
    let schema_changed = fc.merge_from_record(&col.name, &source);
    if schema_changed {
        let mut s = col.schema.write();
        if let Some(cs) = fc.get(&col.name) { *s = cs; }
    }
    let id = id.unwrap_or_else(uuid_like);
    let bytes = serde_json::to_vec(&source)?;
    let compressed = codec::compress(&bytes, col.storage_cfg.zstd_level)?;
    let mut w = col.db.begin_write()?;
    w.set_durability(redb::Durability::Eventual);
    let (row_id, created, version) = write_doc_in_tx(&w, &id, &compressed, require_create)?;
    w.commit()?;
    col.tombstones.write().remove(row_id);
    if !created {
        col.index.remove_row(row_id);
    }
    {
        let mut schema = col.schema.write();
        index_one(&col.index, &mut schema, row_id, &source);
    }
    Ok(BulkResult { id, version, action: if created { "create" } else { "index" }, created })
}

/// Write many Index/Create ops for the same collection in a single redb
/// write transaction. For each op we still update the in-memory posting
/// index individually (preserving per-op ordering), but amortize the
/// `begin_write()/commit()` round-trip — historically the single biggest
/// cost in Arkime bulk ingest (thousands of one-doc commits per run).
///
/// Failures on a single doc (e.g. create on existing id) become per-item
/// errors in the returned Vec; the transaction continues and commits the
/// successful docs, matching how Elasticsearch's `_bulk` behaves.
fn write_many_in_one_tx(
    col: &Arc<Collection>,
    fc: &Arc<FieldCatalog>,
    ops: &[&BulkOp],
) -> Result<Vec<std::result::Result<BulkResult, Error>>> {
    // Precompute per-op schema-merge + compressed body outside the tx so
    // the write transaction holds the global write lock for as little
    // time as possible.
    struct Prep {
        id: Option<String>,
        source: serde_json::Value,
        compressed: Vec<u8>,
        require_create: bool,
        is_create: bool,
    }
    let mut preps: Vec<std::result::Result<Prep, Error>> = Vec::with_capacity(ops.len());
    for op in ops {
        let (id, source, is_create) = match &op.kind {
            BulkKind::Index { id, source } => (id.clone(), source.clone(), false),
            BulkKind::Create { id, source } => (Some(id.clone()), source.clone(), true),
            _ => { preps.push(Err(Error::BadRequest("non-index op in batch".into()))); continue; }
        };
        if !matches!(source, serde_json::Value::Object(_)) {
            preps.push(Err(Error::BadRequest("source must be a JSON object".into())));
            continue;
        }
        let changed = fc.merge_from_record(&col.name, &source);
        if changed {
            let mut s = col.schema.write();
            if let Some(cs) = fc.get(&col.name) { *s = cs; }
        }
        let bytes = match serde_json::to_vec(&source) {
            Ok(b) => b, Err(e) => { preps.push(Err(Error::from(e))); continue; }
        };
        let compressed = match codec::compress(&bytes, col.storage_cfg.zstd_level) {
            Ok(c) => c, Err(e) => { preps.push(Err(Error::Io(e))); continue; }
        };
        preps.push(Ok(Prep { id, source, compressed, require_create: is_create, is_create }));
    }

    let mut w = col.db.begin_write()?;
    w.set_durability(redb::Durability::Eventual);

    // per-op in-tx writes; collect (prep-index, row_id, created, version)
    let mut in_tx_results: Vec<std::result::Result<(usize, String, u32, bool, u64), Error>> =
        Vec::with_capacity(preps.len());
    for (pi, p) in preps.iter().enumerate() {
        match p {
            Err(_) => { /* carried through below */ }
            Ok(p) => {
                let id = p.id.clone().unwrap_or_else(uuid_like);
                match write_doc_in_tx(&w, &id, &p.compressed, p.require_create) {
                    Ok((row_id, created, version)) => {
                        in_tx_results.push(Ok((pi, id, row_id, created, version)));
                    }
                    Err(e) => in_tx_results.push(Err(e)),
                }
            }
        }
    }
    w.commit()?;

    // Now apply in-memory index updates (postings + tombstone clear) in
    // op-order. Match each success to its prep to get source back.
    let mut results: Vec<std::result::Result<BulkResult, Error>> =
        (0..preps.len()).map(|_| Err(Error::BadRequest("unset".into()))).collect();
    // First, write back per-prep prep errors.
    for (pi, p) in preps.iter().enumerate() {
        if let Err(e) = p {
            results[pi] = Err(Error::BadRequest(e.to_string()));
        }
    }
    let mut tx_iter = in_tx_results.into_iter();
    for (pi, p) in preps.iter().enumerate() {
        if p.is_err() { continue; }
        let Some(r) = tx_iter.next() else { break; };
        match r {
            Err(e) => results[pi] = Err(e),
            Ok((_, id, row_id, created, version)) => {
                col.tombstones.write().remove(row_id);
                if !created { col.index.remove_row(row_id); }
                {
                    let mut schema = col.schema.write();
                    let src = &preps[pi].as_ref().unwrap().source;
                    index_one(&col.index, &mut schema, row_id, src);
                }
                let is_create = preps[pi].as_ref().unwrap().is_create;
                results[pi] = Ok(BulkResult {
                    id, version,
                    action: if is_create { "create" } else { "index" },
                    created,
                });
            }
        }
    }
    Ok(results)
}

fn delete_one(col: &Arc<Collection>, id: &str) -> Result<BulkResult> {
    let mut w = col.db.begin_write()?;
    w.set_durability(redb::Durability::Eventual);
    let row_id = match delete_doc_in_tx(&w, id)? {
        Some(r) => r,
        None => { w.commit()?; return Err(Error::NotFound(format!("doc {id} not found"))); }
    };
    w.commit()?;
    col.tombstones.write().insert(row_id);
    Ok(BulkResult { id: id.to_string(), version: 0, action: "delete", created: false })
}

fn update_one(col: &Arc<Collection>, fc: &Arc<FieldCatalog>, id: &str, doc: serde_json::Value) -> Result<BulkResult> {
    let (row_id, _version, cur_bytes) = col.get_by_id(id)?
        .ok_or_else(|| Error::NotFound(format!("doc {id} not found")))?;
    let _ = row_id;
    let mut cur: serde_json::Value = serde_json::from_slice(&cur_bytes)?;
    if let (serde_json::Value::Object(base), serde_json::Value::Object(patch)) = (&mut cur, doc.clone()) {
        for (k, v) in patch { base.insert(k, v); }
    }
    write_one(col, fc, Some(id.to_string()), cur, false).map(|mut r| {
        r.action = "update";
        r
    })
}

fn uuid_like() -> String { uuid_fast() }

// Tiny non-crypto id. Per-thread xorshift seeded once; avoids reseeding from
// the clock on every call (which produced duplicate ids under burst load).
fn uuid_fast() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let h = (now.as_nanos() as u64) ^ std::process::id() as u64;
    format!("{:016x}{:08x}", h, rand_u32())
}

fn rand_u32() -> u32 {
    use std::cell::Cell;
    thread_local! {
        static RNG: Cell<u32> = Cell::new({
            use std::time::{SystemTime, UNIX_EPOCH};
            let s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
            // seed with thread id hash + clock to avoid two threads starting identically
            let tid: u64 = unsafe { std::mem::transmute(std::thread::current().id()) };
            (s ^ (tid as u32) ^ 0x9E37_79B9).max(1)
        });
    }
    RNG.with(|r| {
        let mut x = r.get();
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        r.set(x);
        x
    })
}

