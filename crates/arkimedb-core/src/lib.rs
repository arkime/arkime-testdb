//! arkimedb-core — shared types, config, schema, error handling.

pub mod config;
pub mod error;
pub mod schema;
pub mod value;
pub mod ids;
pub mod time_ms;

pub use error::{Error, Result};
pub use value::{Value, Scalar};
pub use schema::{FieldType, FieldCatalog, CollectionSchema, DynamicTemplate, infer_type_from_name, tokenize_text, match_dynamic_template, schema_glob_match};
pub use ids::DocId;
pub use time_ms::TimestampMs;
