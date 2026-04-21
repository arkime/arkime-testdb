use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde_json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("redb: {0}")]
    Redb(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("schema: {0}")]
    Schema(String),
    #[error("internal: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<redb::Error> for Error { fn from(e: redb::Error) -> Self { Error::Redb(e.to_string()) } }
impl From<redb::DatabaseError> for Error { fn from(e: redb::DatabaseError) -> Self { Error::Redb(e.to_string()) } }
impl From<redb::TransactionError> for Error { fn from(e: redb::TransactionError) -> Self { Error::Redb(e.to_string()) } }
impl From<redb::TableError> for Error { fn from(e: redb::TableError) -> Self { Error::Redb(e.to_string()) } }
impl From<redb::CommitError> for Error { fn from(e: redb::CommitError) -> Self { Error::Redb(e.to_string()) } }
impl From<redb::StorageError> for Error { fn from(e: redb::StorageError) -> Self { Error::Redb(e.to_string()) } }
impl From<anyhow::Error> for Error { fn from(e: anyhow::Error) -> Self { Error::Internal(e.to_string()) } }
