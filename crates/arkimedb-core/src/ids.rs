use serde::{Deserialize, Serialize};

/// Primary key for a record. Strings up to a few hundred bytes (per spec).
#[derive(Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DocId(pub String);

impl DocId {
    pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl From<&str> for DocId { fn from(s: &str) -> Self { Self(s.to_string()) } }
impl From<String> for DocId { fn from(s: String) -> Self { Self(s) } }
impl std::fmt::Display for DocId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
}
