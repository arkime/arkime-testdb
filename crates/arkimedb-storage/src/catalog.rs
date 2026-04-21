//! Global catalog — a single redb database holding cluster-wide state:
//! aliases, collection registry, atomic counters, external version tokens.

use std::path::Path;
use std::sync::Arc;
use parking_lot::Mutex;
use redb::{Database, TableDefinition, ReadableTable};

use arkimedb_core::{Error, Result};

const COLLECTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("collections");
const ALIASES: TableDefinition<&str, &[u8]> = TableDefinition::new("aliases");   // alias -> json array of collection names
const COUNTERS: TableDefinition<&str, u64> = TableDefinition::new("counters");   // atomic counters
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");         // misc
const TEMPLATES: TableDefinition<&str, &[u8]> = TableDefinition::new("templates"); // index templates (raw JSON body)

pub struct GlobalCatalog {
    db: Arc<Database>,
    _lock: Mutex<()>,
}

impl GlobalCatalog {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("catalog.redb");
        let db = Database::create(&path).map_err(Error::from)?;
        // ensure tables exist
        let w = db.begin_write()?;
        {
            let _ = w.open_table(COLLECTIONS)?;
            let _ = w.open_table(ALIASES)?;
            let _ = w.open_table(COUNTERS)?;
            let _ = w.open_table(META)?;
            let _ = w.open_table(TEMPLATES)?;
        }
        w.commit()?;
        Ok(Self { db: Arc::new(db), _lock: Mutex::new(()) })
    }

    pub fn register_collection(&self, name: &str, schema: &arkimedb_core::CollectionSchema) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(COLLECTIONS)?;
            let bytes = serde_json::to_vec(schema)?;
            t.insert(name, bytes.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn unregister_collection(&self, name: &str) -> Result<bool> {
        let w = self.db.begin_write()?;
        let removed = {
            let mut t = w.open_table(COLLECTIONS)?;
            let x = t.remove(name)?.is_some();
            x
        };
        w.commit()?;
        Ok(removed)
    }

    pub fn get_schema(&self, name: &str) -> Result<Option<arkimedb_core::CollectionSchema>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(COLLECTIONS)?;
        match t.get(name)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn list_collections(&self) -> Result<Vec<String>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(COLLECTIONS)?;
        let mut out = Vec::new();
        for row in t.iter()? {
            let (k, _v) = row?;
            out.push(k.value().to_string());
        }
        Ok(out)
    }

    pub fn set_alias(&self, alias: &str, targets: &[String]) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(ALIASES)?;
            let bytes = serde_json::to_vec(targets)?;
            t.insert(alias, bytes.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn get_alias(&self, alias: &str) -> Result<Option<Vec<String>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(ALIASES)?;
        match t.get(alias)? {
            Some(v) => Ok(Some(serde_json::from_slice(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn list_aliases(&self) -> Result<Vec<(String, Vec<String>)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(ALIASES)?;
        let mut out = Vec::new();
        for row in t.iter()? {
            let (k, v) = row?;
            let targets: Vec<String> = serde_json::from_slice(v.value())?;
            out.push((k.value().to_string(), targets));
        }
        Ok(out)
    }

    /// Atomically increment a counter and return the new value.
    pub fn incr_counter(&self, key: &str, by: u64) -> Result<u64> {
        let w = self.db.begin_write()?;
        let new = {
            let mut t = w.open_table(COUNTERS)?;
            let cur = t.get(key)?.map(|v| v.value()).unwrap_or(0);
            let new = cur.saturating_add(by);
            t.insert(key, new)?;
            new
        };
        w.commit()?;
        Ok(new)
    }

    pub fn get_counter(&self, key: &str) -> Result<u64> {
        let r = self.db.begin_read()?;
        let t = r.open_table(COUNTERS)?;
        Ok(t.get(key)?.map(|v| v.value()).unwrap_or(0))
    }

    /// Conditional write — only succeeds if current value equals `expect`.
    pub fn cas_counter(&self, key: &str, expect: u64, new_val: u64) -> Result<bool> {
        let w = self.db.begin_write()?;
        let ok = {
            let mut t = w.open_table(COUNTERS)?;
            let cur = t.get(key)?.map(|v| v.value()).unwrap_or(0);
            if cur != expect { false } else {
                t.insert(key, new_val)?;
                true
            }
        };
        w.commit()?;
        Ok(ok)
    }

    pub fn put_template(&self, name: &str, body: &[u8]) -> Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(TEMPLATES)?;
            t.insert(name, body)?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn get_template(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(TEMPLATES)?;
        Ok(t.get(name)?.map(|v| v.value().to_vec()))
    }

    pub fn delete_template(&self, name: &str) -> Result<bool> {
        let w = self.db.begin_write()?;
        let existed = {
            let mut t = w.open_table(TEMPLATES)?;
            let x = t.remove(name)?.is_some();
            x
        };
        w.commit()?;
        Ok(existed)
    }

    pub fn list_templates(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(TEMPLATES)?;
        let mut out = Vec::new();
        for row in t.iter()? {
            let (k, v) = row?;
            out.push((k.value().to_string(), v.value().to_vec()));
        }
        Ok(out)
    }
}
