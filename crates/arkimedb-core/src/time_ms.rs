use serde::{Deserialize, Serialize};

/// Millisecond-precision UTC timestamp (signed to allow epoch comparisons).
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TimestampMs(pub i64);

impl TimestampMs {
    pub fn now() -> Self {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self(d.as_millis() as i64)
    }
    pub fn as_i64(&self) -> i64 { self.0 }
}
