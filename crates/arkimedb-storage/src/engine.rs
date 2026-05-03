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

/// Per-collection set of redb table handles. For collections in a shared
/// DB these names are prefixed with the collection name (e.g.
/// `docs__sessions3-250420`); for solo-db collections they are the plain
/// default names above.
#[derive(Clone, Copy)]
pub struct CollTables {
    pub docs: TableDefinition<'static, u32, &'static [u8]>,
    pub id2row: TableDefinition<'static, &'static str, u32>,
    pub row2id: TableDefinition<'static, u32, &'static str>,
    pub versions: TableDefinition<'static, u32, u64>,
    pub meta: TableDefinition<'static, &'static str, u64>,
    pub tombstones: TableDefinition<'static, u32, u8>,
}

impl CollTables {
    fn default_names() -> Self {
        Self { docs: DOCS, id2row: ID2ROW, row2id: ROW2ID, versions: VERSIONS, meta: META, tombstones: TOMBSTONES }
    }
    fn prefixed(name: &str) -> Self {
        let safe: String = name.chars().map(|c| if c == '/' || c == '\\' { '_' } else { c }).collect();
        let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
        Self {
            docs:       TableDefinition::new(leak(format!("docs__{safe}"))),
            id2row:     TableDefinition::new(leak(format!("id2row__{safe}"))),
            row2id:     TableDefinition::new(leak(format!("row2id__{safe}"))),
            versions:   TableDefinition::new(leak(format!("versions__{safe}"))),
            meta:       TableDefinition::new(leak(format!("meta__{safe}"))),
            tombstones: TableDefinition::new(leak(format!("tombstones__{safe}"))),
        }
    }
}

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
    /// Guards the non-atomic (remove_row + index_one) reindex sequence on
    /// updates. Held exclusively during post-commit reindex; searches take
    /// a read guard so they never observe a row temporarily missing from
    /// postings mid-reindex.
    pub reindex_lock: RwLock<()>,
    /// Lazily-built cache: field name -> (row_id -> Scalar value) used for
    /// sort-without-hydration. Built on first sort of a field; cleared on
    /// any write to the collection.
    pub sort_cache: RwLock<ahash::AHashMap<String, Arc<ahash::AHashMap<u32, arkimedb_core::Scalar>>>>,
    /// Per-field (min, max) of the sort_cache values. Built alongside
    /// sort_cache; cleared on any write. Enables cross-collection early-stop
    /// when processing cols in extremum order for single-key sorts.
    pub sort_range: RwLock<ahash::AHashMap<String, (arkimedb_core::Scalar, arkimedb_core::Scalar)>>,
    pub(crate) storage_cfg: StorageConfig,
    /// True if this collection's Database is shared with other
    /// collections (sessions-family). Writes to shared-DB collections
    /// serialize on the same redb writer mutex.
    pub(crate) shared_db: bool,
    pub(crate) tables: CollTables,
    /// Refresh interval in milliseconds. 0 = refresh on every write
    /// (default, matches original behavior). >0 = defer in-memory
    /// postings updates until this much time passes or the pending
    /// buffer fills. Matches ES `index.refresh_interval` semantics.
    pub refresh_interval_ms: std::sync::atomic::AtomicU64,
    /// Last time pending_reindex was drained.
    pub last_refresh: parking_lot::Mutex<std::time::Instant>,
    /// row_ids whose in-memory postings/schema do not yet reflect the
    /// on-disk state. Drained by refresh() or when the elapsed time
    /// exceeds refresh_interval_ms or buffer exceeds PENDING_MAX.
    pub pending_reindex: parking_lot::Mutex<ahash::AHashMap<u32, PendingEntry>>,
}

/// What's known about a deferred reindex target. `need_remove_row`
/// flips true if the row existed in postings *before* being deferred
/// (overwrite / tombstone resurrection / update / script).
#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub need_remove_row: bool,
}

/// Upper bound on pending reindex entries per collection. Exceeding
/// this forces an immediate drain regardless of refresh_interval —
/// bounds memory and keeps search latency predictable if a client
/// stops hitting `_refresh`.
pub const PENDING_MAX: usize = 50_000;

impl Collection {
    pub fn row_count(&self) -> Result<u64> {
        let r = self.db.begin_read()?;
        let t = r.open_table(self.tables.meta)?;
        Ok(t.get("doc_count")?.map(|v| v.value()).unwrap_or(0))
    }

    /// Highest assigned `row_id + 1`. Cheap (single META lookup), used by
    /// `all_live_rows` to avoid scanning the full DOCS B-tree.
    pub fn next_row_id_value(&self) -> Result<u32> {
        let r = self.db.begin_read()?;
        let t = r.open_table(self.tables.meta)?;
        Ok(t.get("next_row_id")?.map(|v| v.value()).unwrap_or(0) as u32)
    }

    /// Batch-load `(doc_id, decompressed_source)` for the given rows, reusing
    /// a single read transaction. This is the search hydrate hot path; opening
    /// a tx + tables per hit was 2× slower than ES.
    pub fn hydrate_rows(&self, rows: &[u32], want_id: bool, want_raw: bool)
        -> Result<Vec<(Option<String>, Option<Vec<u8>>)>>
    {
        let r = self.db.begin_read()?;
        let row2id = if want_id  { Some(r.open_table(self.tables.row2id)?) } else { None };
        let docs   = if want_raw { Some(r.open_table(self.tables.docs)?)   } else { None };
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

    /// Look up versions for a batch of row ids (single read tx).
    /// Returns 1 for rows with no version row (legacy/missing).
    pub fn versions_for_rows(&self, rows: &[u32]) -> Result<Vec<u64>> {
        let r = self.db.begin_read()?;
        let vt = r.open_table(self.tables.versions)?;
        let mut out = Vec::with_capacity(rows.len());
        for &row_id in rows {
            out.push(vt.get(row_id)?.map(|v| v.value()).unwrap_or(1));
        }
        Ok(out)
    }

    pub fn next_row_id(&self, w: &redb::WriteTransaction) -> Result<u32> {
        let mut t = w.open_table(self.tables.meta)?;
        let cur = t.get("next_row_id")?.map(|v| v.value()).unwrap_or(0);
        let next = cur + 1;
        t.insert("next_row_id", next)?;
        Ok(cur as u32)
    }

    pub fn get_by_id(&self, id: &str) -> Result<Option<(u32, u64, Vec<u8>)>> {
        let r = self.db.begin_read()?;
        let id2row = r.open_table(self.tables.id2row)?;
        let Some(row_id) = id2row.get(id)?.map(|v| v.value()) else { return Ok(None); };
        if self.tombstones.read().contains(row_id) { return Ok(None); }
        let docs = r.open_table(self.tables.docs)?;
        let ver_t = r.open_table(self.tables.versions)?;
        let Some(raw) = docs.get(row_id)?.map(|v| v.value().to_vec()) else { return Ok(None); };
        let version = ver_t.get(row_id)?.map(|v| v.value()).unwrap_or(1);
        let json_bytes = codec::decompress(&raw)?;
        Ok(Some((row_id, version, json_bytes)))
    }

    /// Returns true if present.
    pub fn has_id(&self, id: &str) -> Result<bool> {
        let r = self.db.begin_read()?;
        let id2row = r.open_table(self.tables.id2row)?;
        match id2row.get(id)? {
            Some(v) => Ok(!self.tombstones.read().contains(v.value())),
            None => Ok(false),
        }
    }

    pub fn get_raw_by_row(&self, row_id: u32) -> Result<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        let docs = r.open_table(self.tables.docs)?;
        match docs.get(row_id)? {
            Some(v) => Ok(Some(codec::decompress(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn doc_id_of(&self, row_id: u32) -> Result<Option<String>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(self.tables.row2id)?;
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
        let docs = r.open_table(self.tables.docs)?;
        let tomb = r.open_table(self.tables.tombstones)?;
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
    /// Map of shared-db path -> open Database. Any collection whose name
    /// matches a sharing family (currently: `sessions*`) routes to one of
    /// these shared Databases and uses prefixed table names internally.
    /// This collapses hundreds of daily-index files into a single redb
    /// file, slashing startup file-open count and cross-collection commit
    /// overhead.
    shared_dbs: RwLock<AHashMap<PathBuf, Arc<Database>>>,
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
        let shared_dbs: RwLock<AHashMap<PathBuf, Arc<Database>>> = RwLock::new(AHashMap::new());
        let loaded: Vec<Result<(String, Arc<Collection>)>> = valid
            .par_iter()
            .map(|name| -> Result<(String, Arc<Collection>)> {
                let schema = catalog.get_schema(name)?.unwrap_or_default();
                field_catalog.replace(name, schema.clone());
                let col = Arc::new(open_collection(
                    &config.data_dir, name, schema, config.storage.clone(), &shared_dbs,
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
            shared_dbs,
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
            &self.data_dir, &effective, schema, self.config.storage.clone(), &self.shared_dbs,
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
        // Group by collection (preserving per-collection op order) then group
        // collections by their underlying redb Database. Collections in a
        // shared DB (sessions family) commit together in ONE write tx per DB
        // — huge reduction in commit count for capture bulks that span many
        // daily indices. Non-shared-DB collections each get their own tx
        // (one per collection). We then run the per-DB work in parallel with
        // rayon, since distinct redb Databases have distinct writer mutexes.
        //
        // IMPORTANT: we deliberately do NOT parallelize across collections
        // inside a shared DB — redb serializes writers, so doing so would
        // just block rayon threads on the same mutex and starve the pool.
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

        // Partition by underlying DB identity (Arc::as_ptr).
        let mut by_db: ahash::AHashMap<usize, Vec<(Arc<Collection>, Vec<(usize, BulkOp)>, String)>> = Default::default();
        for entry in coll_groups {
            let key = Arc::as_ptr(&entry.0.db) as usize;
            by_db.entry(key).or_default().push(entry);
        }
        let db_groups: Vec<Vec<(Arc<Collection>, Vec<(usize, BulkOp)>, String)>> = by_db.into_values().collect();

        let fc = self.field_catalog.clone();
        let per_db: Vec<Vec<(usize, BulkOutcome)>> = db_groups
            .into_par_iter()
            .map(|cols| {
                // Each `cols` entry = (col, ops, name) that share a Database.
                bulk_write_one_db(&cols, &fc)
            })
            .collect();

        // Re-emit in original op order.
        let mut placed: Vec<Option<BulkOutcome>> = (0..n).map(|_| None).collect();
        for chunk in per_db {
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
    pub fn refresh(&self, target: Option<&str>) -> Result<()> {
        // Our writes are queryable immediately on commit, so there's no
        // Lucene-style "buffered-but-not-yet-searchable" state to flush.
        // What clients (Arkime) need is a **barrier**: after `_refresh`
        // returns, every write that completed before the refresh call
        // must be visible. Briefly acquiring (and immediately releasing)
        // `reindex_lock.write()` on every matching collection waits for
        // any concurrent bulk holding the lock across commit+postings
        // update to finish, then lets readers see fully-applied state.
        let cols: Vec<Arc<Collection>> = match target {
            Some(t) => self.resolve(t).unwrap_or_default(),
            None => {
                let g = self.collections.read();
                g.values().cloned().collect()
            }
        };
        // Sort by Arc pointer to share lock order with bulk_write_one_db
        // so a concurrent bulk can never AB-BA-deadlock with a refresh.
        let mut uniq: Vec<Arc<Collection>> = Vec::with_capacity(cols.len());
        let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::new();
        for c in cols {
            if seen.insert(Arc::as_ptr(&c) as usize) { uniq.push(c); }
        }
        uniq.sort_by_key(|c| Arc::as_ptr(c) as usize);
        for c in uniq {
            let _g = c.reindex_lock.write();
            // Apply any postings updates that were deferred while
            // refresh_interval_ms > 0. No-op when nothing is pending.
            drain_pending_reindex(&c, &self.field_catalog);
        }
        Ok(())
    }

    pub fn delete_collection(&self, name: &str) -> Result<bool> {
        let mut g = self.collections.write();
        let col = match g.remove(name) { Some(c) => c, None => return Ok(false) };
        drop(g);
        // Remove from persistent catalog & in-memory field catalog so it
        // doesn't reappear on restart or linger in schema lookups.
        let _ = self.catalog.unregister_collection(name);
        self.field_catalog.remove(name);
        if col.shared_db {
            // Shared DB: don't delete the file — just drop this collection's tables.
            let mut w = col.db.begin_write()?;
            w.set_durability(redb::Durability::Eventual);
            let _ = w.delete_table(col.tables.docs);
            let _ = w.delete_table(col.tables.id2row);
            let _ = w.delete_table(col.tables.row2id);
            let _ = w.delete_table(col.tables.versions);
            let _ = w.delete_table(col.tables.meta);
            let _ = w.delete_table(col.tables.tombstones);
            w.commit()?;
        } else {
            // Drop the Arc so redb releases the file handle before removing.
            drop(col);
            let (path, _) = db_path_for(&self.data_dir, name);
            let _ = std::fs::remove_file(&path);
        }
        Ok(true)
    }
}

// --- bulk op model --------------------------------------------------------

pub enum BulkKind {
    Index  { id: Option<String>, source: serde_json::Value, force_version: Option<u64> },
    Create { id: String,          source: serde_json::Value, force_version: Option<u64> },
    Update { id: String,          doc: serde_json::Value },
    Delete { id: String },
    /// Read-modify-write under the write tx + reindex_lock (atomic like ES
    /// `_update` with script). Mutator is applied in-place on the current
    /// source JSON. Must be deterministic / side-effect free.
    Script { id: String, mutator: ScriptMutator },
}

/// Boxed callback that mutates a source document in place. Applied inside
/// the write transaction so the read-modify-write is atomic w.r.t. other
/// writers on the same collection.
pub type ScriptMutator = std::sync::Arc<dyn Fn(&mut serde_json::Value) -> Result<()> + Send + Sync>;

#[derive(Debug)]
pub struct BulkOp {
    pub collection: Option<String>,
    pub kind: BulkKind,
}

impl std::fmt::Debug for BulkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BulkKind::Index { id, .. }  => f.debug_struct("Index").field("id", id).finish(),
            BulkKind::Create { id, .. } => f.debug_struct("Create").field("id", id).finish(),
            BulkKind::Update { id, .. } => f.debug_struct("Update").field("id", id).finish(),
            BulkKind::Delete { id }     => f.debug_struct("Delete").field("id", id).finish(),
            BulkKind::Script { id, .. } => f.debug_struct("Script").field("id", id).finish(),
        }
    }
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

/// Collections whose name starts with `sessions` share a single redb file
/// at `<data>/collections/sessions.redb` with prefixed per-collection
/// tables. Arkime creates one such collection *per day* of captured
/// traffic — potentially hundreds in long-lived deployments — so
/// consolidating them removes hundreds of file-create fsyncs at init
/// time and lets a single redb write tx span many daily indices.
fn db_path_for(root: &Path, name: &str) -> (PathBuf, bool) {
    // All collections live in one of two shared redb files. See module
    // comment above `open_collection` for rationale.
    //   * `sessions.redb` — any name containing "sessions" (large per-
    //     month capture indices; isolated so session writers don't
    //     contend with small-index writers).
    //   * `other.redb`   — everything else (users, views, stats,
    //     fields_v3, configs_v2, cont3xt_*, tagger, …). Tiny, often
    //     single-doc indices; collapsing them removes N fsyncs per
    //     bulk batch and tens of files.
    let file = if name.contains("sessions") { "sessions.redb" } else { "other.redb" };
    (root.join("collections").join(file), true)
}

fn open_collection(
    root: &Path,
    name: &str,
    schema: CollectionSchema,
    storage_cfg: StorageConfig,
    shared_dbs: &RwLock<AHashMap<PathBuf, Arc<Database>>>,
) -> Result<Collection> {
    let (path, shared) = db_path_for(root, name);
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }

    let db: Arc<Database> = if shared {
        // IMPORTANT: bind the read result into a local so the read guard
        // is dropped before we try to acquire the write lock below.
        // `if let Some(d) = shared_dbs.read().get(..).cloned() { .. } else { shared_dbs.write() }`
        // extends the read guard's lifetime into the else branch (Rust
        // temporary-lifetime rule for `if let` scrutinees), deadlocking
        // parking_lot RwLock against the same thread.
        let cached = shared_dbs.read().get(&path).cloned();
        if let Some(d) = cached {
            d
        } else {
            let mut g = shared_dbs.write();
            if let Some(d) = g.get(&path).cloned() {
                d
            } else {
                let d = Arc::new(Database::create(&path).map_err(Error::from)?);
                g.insert(path.clone(), d.clone());
                d
            }
        }
    } else {
        Arc::new(Database::create(&path).map_err(Error::from)?)
    };

    let tables = if shared { CollTables::prefixed(name) } else { CollTables::default_names() };

    // create this collection's tables inside the (possibly-shared) db
    let mut w = db.begin_write()?;
    w.set_durability(redb::Durability::Eventual);
    {
        let _ = w.open_table(tables.docs)?;
        let _ = w.open_table(tables.id2row)?;
        let _ = w.open_table(tables.row2id)?;
        let _ = w.open_table(tables.versions)?;
        let _ = w.open_table(tables.meta)?;
        let _ = w.open_table(tables.tombstones)?;
    }
    w.commit()?;
    Ok(Collection {
        name: name.to_string(),
        path,
        db,
        index: Arc::new(PostingsIndex::new()),
        schema: RwLock::new(schema),
        tombstones: RwLock::new(RoaringBitmap::new()),
        reindex_lock: RwLock::new(()),
        sort_cache: RwLock::new(ahash::AHashMap::new()),
        sort_range: RwLock::new(ahash::AHashMap::new()),
        storage_cfg,
        shared_db: shared,
        tables,
        refresh_interval_ms: std::sync::atomic::AtomicU64::new(30_000),
        last_refresh: parking_lot::Mutex::new(std::time::Instant::now()),
        pending_reindex: parking_lot::Mutex::new(ahash::AHashMap::new()),
    })
}

// --- tx-scoped doc write helpers ------------------------------------------

/// redb-only portion of an upsert. Returns (row_id, created, version).
/// In-memory index updates must be applied by the caller *after* `w.commit()`.
fn write_doc_in_tx(
    w: &redb::WriteTransaction,
    t: &CollTables,
    id: &str,
    compressed: &[u8],
    require_create: bool,
    force_version: Option<u64>,
) -> Result<(u32, bool, u64, bool)> {
    let mut id2row = w.open_table(t.id2row)?;
    let existing = id2row.get(id)?.map(|v| v.value());
    // A prior delete tombstones the row but leaves id2row mapped. Treat a
    // tombstoned id as non-existent on create; reuse the row_id and clear
    // the on-disk tombstone (postings-side tombstone is cleared by caller).
    let tombstoned = match existing {
        Some(r) => {
            let tt = w.open_table(t.tombstones)?;
            let v = tt.get(r)?.is_some();
            drop(tt);
            v
        }
        None => false,
    };
    let (row_id, created, fresh) = match existing {
        Some(r) if !tombstoned => {
            if require_create { return Err(Error::Conflict(format!("document already exists: {id}"))); }
            (r, false, false)
        }
        Some(r) => {
            // Resurrecting a tombstoned row.
            let mut tt = w.open_table(t.tombstones)?;
            tt.remove(r)?;
            (r, true, false)
        }
        None => {
            let mut meta = w.open_table(t.meta)?;
            let cur = meta.get("next_row_id")?.map(|v| v.value()).unwrap_or(0);
            meta.insert("next_row_id", cur + 1)?;
            drop(meta);
            id2row.insert(id, cur as u32)?;
            let mut row2id = w.open_table(t.row2id)?;
            row2id.insert(cur as u32, id)?;
            (cur as u32, true, true)
        }
    };
    drop(id2row);
    {
        let mut docs = w.open_table(t.docs)?;
        docs.insert(row_id, compressed)?;
    }
    let version = {
        let mut vt = w.open_table(t.versions)?;
        // External versioning: only honor force_version when the doc didn't
        // already exist (i.e. created==true). For an update of an existing
        // row we fall back to cur+1 — clients that want strict ES external
        // versioning semantics on update should use a real ES.
        let new = if let Some(v) = force_version.filter(|_| created) {
            v
        } else {
            let cur = vt.get(row_id)?.map(|v| v.value()).unwrap_or(0);
            cur + 1
        };
        vt.insert(row_id, new)?;
        new
    };
    if created {
        let mut meta = w.open_table(t.meta)?;
        let cur = meta.get("doc_count")?.map(|v| v.value()).unwrap_or(0);
        meta.insert("doc_count", cur + 1)?;
    }
    Ok((row_id, created, version, fresh))
}

fn delete_doc_in_tx(w: &redb::WriteTransaction, t: &CollTables, id: &str) -> Result<Option<u32>> {
    let id2row = w.open_table(t.id2row)?;
    let row_id = id2row.get(id)?.map(|v| v.value());
    drop(id2row);
    let Some(row_id) = row_id else { return Ok(None); };
    {
        let mut tomb = w.open_table(t.tombstones)?;
        tomb.insert(row_id, 1u8)?;
    }
    {
        let mut meta = w.open_table(t.meta)?;
        let cur = meta.get("doc_count")?.map(|v| v.value()).unwrap_or(0);
        if cur > 0 { meta.insert("doc_count", cur - 1)?; }
    }
    Ok(Some(row_id))
}


/// Execute the Index/Create/Delete/Update ops for all collections that
/// share a single redb Database in **one** write transaction. Update/Delete
/// of non-existent docs and Create conflicts produce per-item errors in
/// the returned outcomes — the tx still commits.
///
/// This is the core win from consolidating sessions indices: instead of
/// N commits (one per daily index touched by a bulk request) we do 1.
fn bulk_write_one_db(
    cols: &[(Arc<Collection>, Vec<(usize, BulkOp)>, String)],
    fc: &Arc<FieldCatalog>,
) -> Vec<(usize, BulkOutcome)> {
    // Pick the shared db handle from the first collection (all share it).
    let db = cols[0].0.db.clone();

    // Prep: per-op compressed body + schema merge. Done outside the tx
    // so we hold the writer lock for as little time as possible.
    #[derive(Default)]
    struct Prep {
        id: Option<String>,
        source: serde_json::Value,
        compressed: Vec<u8>,
        require_create: bool,
        force_version: Option<u64>,
        error: Option<Error>,
    }
    // Flat list of (orig_idx, coll_index, op_kind, prep).
    // coll_index indexes into `cols`.
    struct Item {
        orig: usize,
        col_i: usize,
        kind: BulkKind,
        prep: Option<Prep>,
    }
    let mut items: Vec<Item> = Vec::new();
    for (ci, (col, group, _cname)) in cols.iter().enumerate() {
        for (orig, op) in group {
            let kind = match &op.kind {
                BulkKind::Index { id, source, force_version } => BulkKind::Index { id: id.clone(), source: source.clone(), force_version: *force_version },
                BulkKind::Create { id, source, force_version } => BulkKind::Create { id: id.clone(), source: source.clone(), force_version: *force_version },
                BulkKind::Delete { id } => BulkKind::Delete { id: id.clone() },
                BulkKind::Update { id, doc } => BulkKind::Update { id: id.clone(), doc: doc.clone() },
                BulkKind::Script { id, mutator } => BulkKind::Script { id: id.clone(), mutator: mutator.clone() },
            };
            let prep = match &op.kind {
                BulkKind::Index { id: idx_id, source, force_version } => {
                    if !matches!(source, serde_json::Value::Object(_)) {
                        Some(Prep { error: Some(Error::BadRequest("source must be a JSON object".into())), ..Default::default() })
                    } else {
                        let changed = fc.merge_from_record(&col.name, source);
                        if changed {
                            let mut s = col.schema.write();
                            if let Some(cs) = fc.get(&col.name) { *s = cs; }
                        }
                        match serde_json::to_vec(source)
                            .map_err(Error::from)
                            .and_then(|b| codec::compress(&b, col.storage_cfg.zstd_level).map_err(Error::Io))
                        {
                            Ok(compressed) => Some(Prep {
                                id: idx_id.clone(),
                                source: source.clone(),
                                compressed,
                                require_create: false,
                                force_version: *force_version,
                                error: None,
                            }),
                            Err(e) => Some(Prep { error: Some(e), ..Default::default() }),
                        }
                    }
                }
                BulkKind::Create { id: cre_id, source, force_version } => {
                    if !matches!(source, serde_json::Value::Object(_)) {
                        Some(Prep { error: Some(Error::BadRequest("source must be a JSON object".into())), ..Default::default() })
                    } else {
                        let changed = fc.merge_from_record(&col.name, source);
                        if changed {
                            let mut s = col.schema.write();
                            if let Some(cs) = fc.get(&col.name) { *s = cs; }
                        }
                        match serde_json::to_vec(source)
                            .map_err(Error::from)
                            .and_then(|b| codec::compress(&b, col.storage_cfg.zstd_level).map_err(Error::Io))
                        {
                            Ok(compressed) => Some(Prep {
                                id: Some(cre_id.clone()),
                                source: source.clone(),
                                compressed,
                                require_create: true,
                                force_version: *force_version,
                                error: None,
                            }),
                            Err(e) => Some(Prep { error: Some(e), ..Default::default() }),
                        }
                    }
                }
                _ => None,
            };
            items.push(Item { orig: *orig, col_i: ci, kind, prep });
        }
    }

    // Single write tx for the whole DB.
    let mut out: Vec<(usize, BulkOutcome)> = Vec::with_capacity(items.len());

    // Acquire reindex_lock.write() on every touched collection BEFORE we
    // commit. Holding it across both the commit and the post-commit in-memory
    // index updates closes the "visible on disk but stale in memory" gap —
    // without this, a reader that enters between commit() and the per-col
    // reindex_lock.write() sees a bitmap that still claims the old row
    // membership (e.g. still tagged MTAGTEST1) even though the doc on disk
    // has been updated. Order by collection-Arc pointer to get a deterministic
    // lock order across threads, avoiding any deadlock if two bulks touch the
    // same collections.
    let mut reindex_guards: Vec<parking_lot::RwLockWriteGuard<()>> = {
        let mut uniq: Vec<Arc<Collection>> = Vec::with_capacity(cols.len());
        let mut seen: ahash::AHashSet<usize> = ahash::AHashSet::new();
        for (c, _, _) in cols {
            if seen.insert(Arc::as_ptr(c) as usize) { uniq.push(c.clone()); }
        }
        uniq.sort_by_key(|c| Arc::as_ptr(c) as usize);
        // Any bulk that touches a collection invalidates its sort_cache;
        // keep it simple and clear under the write lock we already hold.
        for c in &uniq { c.sort_cache.write().clear(); c.sort_range.write().clear(); }
        uniq.into_iter().map(|c| {
            // SAFETY: the guard's lifetime is tied to the Collection which we
            // keep alive via the clones in `cols`. We leak the Arc into the
            // guard's lifetime by using a raw borrow — acceptable because the
            // guard is dropped before this function returns.
            let g: parking_lot::RwLockWriteGuard<'_, ()> = c.reindex_lock.write();
            unsafe { std::mem::transmute::<parking_lot::RwLockWriteGuard<'_, ()>, parking_lot::RwLockWriteGuard<'static, ()>>(g) }
        }).collect()
    };

    let mut w = match db.begin_write() {
        Ok(w) => w,
        Err(e) => {
            // Whole-tx failure: report error for every op.
            for it in &items {
                let cname = &cols[it.col_i].2;
                out.push((it.orig, BulkOutcome::err(cname, Error::Io(std::io::Error::other(e.to_string())))));
            }
            reindex_guards.clear();
            return out;
        }
    };
    w.set_durability(redb::Durability::Eventual);

    // Per-op results inside the tx.
    struct TxRes { row_id: u32, created: bool, version: u64, id: String, fresh: bool }
    let mut tx_results: Vec<std::result::Result<TxRes, Error>> = Vec::with_capacity(items.len());
    for it in &items {
        let col = &cols[it.col_i].0;
        let t = &col.tables;
        match &it.kind {
            BulkKind::Index { .. } | BulkKind::Create { .. } => {
                let prep = it.prep.as_ref().unwrap();
                if let Some(e) = &prep.error {
                    tx_results.push(Err(Error::BadRequest(e.to_string())));
                    continue;
                }
                let id = prep.id.clone().unwrap_or_else(uuid_like);
                match write_doc_in_tx(&w, t, &id, &prep.compressed, prep.require_create, prep.force_version) {
                    Ok((row_id, created, version, fresh)) => tx_results.push(Ok(TxRes { row_id, created, version, id, fresh })),
                    Err(e) => tx_results.push(Err(e)),
                }
            }
            BulkKind::Delete { id } => {
                match delete_doc_in_tx(&w, t, id) {
                    Ok(Some(row_id)) => tx_results.push(Ok(TxRes { row_id, created: false, version: 0, id: id.clone(), fresh: false })),
                    Ok(None) => tx_results.push(Err(Error::NotFound(format!("doc {id} not found")))),
                    Err(e) => tx_results.push(Err(e)),
                }
            }
            BulkKind::Update { id, doc } => {
                // Read current source, merge patch, rewrite via write_doc_in_tx.
                // Uses the in-flight write tx for reads so we see our own
                // earlier writes in the same bulk.
                let id2row = match w.open_table(t.id2row) { Ok(x) => x, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                let row_id_opt = match id2row.get(id.as_str()) { Ok(v) => v.map(|x| x.value()), Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                drop(id2row);
                let Some(_row_id) = row_id_opt else { tx_results.push(Err(Error::NotFound(format!("doc {id} not found")))); continue; };
                let docs = match w.open_table(t.docs) { Ok(x) => x, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                let cur_bytes = match docs.get(_row_id) {
                    Ok(Some(v)) => match codec::decompress(v.value()) { Ok(b) => b, Err(e) => { tx_results.push(Err(Error::Io(e))); continue; } },
                    Ok(None) => { tx_results.push(Err(Error::NotFound(format!("doc {id} not found")))); continue; }
                    Err(e) => { tx_results.push(Err(Error::from(e))); continue; }
                };
                drop(docs);
                let mut cur: serde_json::Value = match serde_json::from_slice(&cur_bytes) { Ok(v) => v, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                if let (serde_json::Value::Object(base), serde_json::Value::Object(patch)) = (&mut cur, doc.clone()) {
                    for (k, v) in patch { base.insert(k, v); }
                }
                let col = &cols[it.col_i].0;
                let changed = fc.merge_from_record(&col.name, &cur);
                if changed {
                    let mut s = col.schema.write();
                    if let Some(cs) = fc.get(&col.name) { *s = cs; }
                }
                let bytes = match serde_json::to_vec(&cur) { Ok(b) => b, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                let compressed = match codec::compress(&bytes, col.storage_cfg.zstd_level) { Ok(c) => c, Err(e) => { tx_results.push(Err(Error::Io(e))); continue; } };
                match write_doc_in_tx(&w, t, id, &compressed, false, None) {
                    Ok((row_id, _created, version, _fresh)) => tx_results.push(Ok(TxRes { row_id, created: false, version, id: id.clone(), fresh: false })),
                    Err(e) => tx_results.push(Err(e)),
                }
            }
            BulkKind::Script { id, mutator } => {
                // Same atomic read-modify-write as Update, but the mutation
                // is a user-provided closure (e.g. painless add/remove tags).
                let id2row = match w.open_table(t.id2row) { Ok(x) => x, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                let row_id_opt = match id2row.get(id.as_str()) { Ok(v) => v.map(|x| x.value()), Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                drop(id2row);
                let Some(row_id_cur) = row_id_opt else { tx_results.push(Err(Error::NotFound(format!("doc {id} not found")))); continue; };
                let docs = match w.open_table(t.docs) { Ok(x) => x, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                let cur_bytes = match docs.get(row_id_cur) {
                    Ok(Some(v)) => match codec::decompress(v.value()) { Ok(b) => b, Err(e) => { tx_results.push(Err(Error::Io(e))); continue; } },
                    Ok(None) => { tx_results.push(Err(Error::NotFound(format!("doc {id} not found")))); continue; }
                    Err(e) => { tx_results.push(Err(Error::from(e))); continue; }
                };
                drop(docs);
                let mut cur: serde_json::Value = match serde_json::from_slice(&cur_bytes) { Ok(v) => v, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                if let Err(e) = (mutator)(&mut cur) { tx_results.push(Err(e)); continue; }
                let col = &cols[it.col_i].0;
                let changed = fc.merge_from_record(&col.name, &cur);
                if changed {
                    let mut s = col.schema.write();
                    if let Some(cs) = fc.get(&col.name) { *s = cs; }
                }
                let bytes = match serde_json::to_vec(&cur) { Ok(b) => b, Err(e) => { tx_results.push(Err(Error::from(e))); continue; } };
                let compressed = match codec::compress(&bytes, col.storage_cfg.zstd_level) { Ok(c) => c, Err(e) => { tx_results.push(Err(Error::Io(e))); continue; } };
                match write_doc_in_tx(&w, t, id, &compressed, false, None) {
                    Ok((row_id, _created, version, _fresh)) => tx_results.push(Ok(TxRes { row_id, created: false, version, id: id.clone(), fresh: false })),
                    Err(e) => tx_results.push(Err(e)),
                }
            }
        }
    }

    if let Err(e) = w.commit() {
        // Commit failure taints every op.
        for it in &items {
            let cname = &cols[it.col_i].2;
            out.push((it.orig, BulkOutcome::err(cname, Error::Io(std::io::Error::other(e.to_string())))));
        }
        reindex_guards.clear();
        return out;
    }

    // Post-commit: update in-memory state (tombstones, postings index).
    // Precompute per-collection deferral decision (refresh_interval).
    // We hold `reindex_guards` across this entire block, so all
    // decisions for a given col see a stable view of pending_reindex.
    for (it, r) in items.iter().zip(tx_results.into_iter()) {
        let col = &cols[it.col_i].0;
        let cname = &cols[it.col_i].2;
        match r {
            Err(e) => {
                out.push((it.orig, BulkOutcome::err(cname, e)))
            },
            Ok(tr) => {
                match &it.kind {
                    BulkKind::Delete { .. } => {
                        col.tombstones.write().insert(tr.row_id);
                        // If this row was pending reindex, it's moot now —
                        // tombstone filters will keep it out of result sets.
                        col.pending_reindex.lock().remove(&tr.row_id);
                        out.push((it.orig, BulkOutcome::ok(cname, BulkResult { id: tr.id, version: tr.version, action: "delete", created: false })));
                    }
                    BulkKind::Index { .. } | BulkKind::Create { .. } => {
                        col.tombstones.write().remove(tr.row_id);
                        let src = &it.prep.as_ref().unwrap().source;
                        if should_defer_reindex(col) {
                            // Defer: remember we need to (re)index this row,
                            // preserving whether an earlier postings entry
                            // exists for it. Schema merge happens at drain
                            // time — avoid per-row write-lock contention here.
                            let mut pend = col.pending_reindex.lock();
                            let entry = pend.entry(tr.row_id).or_insert(PendingEntry { need_remove_row: !tr.fresh });
                            entry.need_remove_row |= !tr.fresh;
                        } else {
                            // Apply any previously-deferred items before this one
                            // so the postings reflect insertion order.
                            drain_pending_reindex(col, fc);
                            if !tr.fresh { col.index.remove_row(tr.row_id); }
                            let mut schema = col.schema.write();
                            index_one(&col.index, &mut schema, tr.row_id, src);
                        }
                        let is_create = matches!(it.kind, BulkKind::Create{..});
                        out.push((it.orig, BulkOutcome::ok(cname, BulkResult {
                            id: tr.id, version: tr.version,
                            action: if is_create { "create" } else { "index" },
                            created: tr.created,
                        })));
                    }
                    BulkKind::Update { .. } | BulkKind::Script { .. } => {
                        col.tombstones.write().remove(tr.row_id);
                        if should_defer_reindex(col) {
                            let mut pend = col.pending_reindex.lock();
                            let entry = pend.entry(tr.row_id).or_insert(PendingEntry { need_remove_row: true });
                            entry.need_remove_row = true;
                        } else {
                            drain_pending_reindex(col, fc);
                            col.index.remove_row(tr.row_id);
                            if let Ok(Some((_, _, bytes))) = col.get_by_id(&tr.id) {
                                if let Ok(src) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                                    let mut schema = col.schema.write();
                                    index_one(&col.index, &mut schema, tr.row_id, &src);
                                }
                            }
                        }
                        out.push((it.orig, BulkOutcome::ok(cname, BulkResult { id: tr.id, version: tr.version, action: "update", created: false })));
                    }
                }
            }
        }
    }
    // Drop reindex guards explicitly *after* all post-commit index work is
    // done so readers cannot see the committed-on-disk-but-stale-in-memory
    // state.
    drop(reindex_guards);
    out
}

fn uuid_like() -> String { uuid_fast() }

/// True iff postings updates for this collection should be deferred.
/// Based on refresh_interval_ms and the pending buffer ceiling.
fn should_defer_reindex(col: &Arc<Collection>) -> bool {
    let interval = col.refresh_interval_ms.load(std::sync::atomic::Ordering::Relaxed);
    if interval == 0 { return false; }
    if col.pending_reindex.lock().len() >= PENDING_MAX { return false; }
    let last = *col.last_refresh.lock();
    if last.elapsed().as_millis() as u64 >= interval { return false; }
    true
}

/// Apply all pending (row_id -> PendingEntry) entries for `col`.
/// Called from `Engine::refresh` and from the bulk path when the
/// deferral window expires or the pending buffer fills.
///
/// Caller must hold `col.reindex_lock` in exclusive mode while this
/// runs so search readers don't observe a row temporarily missing
/// from postings.
pub(crate) fn drain_pending_reindex(col: &Arc<Collection>, fc: &Arc<FieldCatalog>) {
    let mut pend = col.pending_reindex.lock();
    if pend.is_empty() { return; }
    let entries: Vec<(u32, PendingEntry)> = pend.drain().collect();
    drop(pend);
    for (row_id, entry) in entries {
        // Skip tombstoned rows — search already filters them by tombstone
        // set; no need to spend cycles on them.
        if col.tombstones.read().contains(row_id) { continue; }
        // Read the current doc bytes.
        let source = match col.hydrate_rows(&[row_id], false, true) {
            Ok(mut v) => match v.pop() {
                Some((_, Some(bytes))) => match serde_json::from_slice::<serde_json::Value>(&bytes) { Ok(s) => s, Err(_) => continue },
                _ => continue,
            },
            Err(_) => continue,
        };
        if entry.need_remove_row { col.index.remove_row(row_id); }
        let mut schema = col.schema.write();
        fc.merge_from_record(&col.name, &source);
        if let Some(cs) = fc.get(&col.name) { *schema = cs; }
        index_one(&col.index, &mut schema, row_id, &source);
    }
    *col.last_refresh.lock() = std::time::Instant::now();
    col.sort_cache.write().clear();
    col.sort_range.write().clear();
}

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

