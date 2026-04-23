//! arkimedb-storage — on-disk engine.
//!
//! Phase A layout (single-node MVP, optimized for disk-space first):
//!   - One `redb` database per *collection* (Arkime index / time partition).
//!   - Inside each redb:
//!       * `docs`      : u64 row_id -> zstd(serde_json bytes)
//!       * `id_to_row` : String doc_id -> u64 row_id
//!       * `meta`      : key -> value (next_row_id, version counters, stats)
//!       * `postings`  : bincode-less key (field|type|value) -> roaring bitmap bytes
//!   - In-memory postings index is hydrated on open and kept hot; updates are
//!     both applied in memory and persisted on refresh/flush.
//!
//! Phase B will swap `docs` + `postings` for columnar FST/zstd segments while
//! keeping this crate's public API stable.

pub mod engine;
pub mod postings;
pub mod catalog;
pub mod codec;

// Re-export commonly used types at the crate root.
pub use postings::PostingsIndex;

pub use engine::{Engine, Collection, BulkOutcome, BulkResult, GetResult, DocHit, BulkKind, BulkOp, ScriptMutator};
pub use catalog::GlobalCatalog;
