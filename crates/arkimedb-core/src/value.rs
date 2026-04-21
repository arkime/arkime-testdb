//! Typed value model.
//!
//! Every field is array-by-default: a single value is stored as a 1-element
//! array internally. Equality matches if any element matches; aggregations
//! count each element.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// A single, typed scalar value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Scalar {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    /// Keyword / exact string.
    Str(String),
    /// IP stored canonically as IPv6 (IPv4 mapped into ::ffff:0:0/96).
    Ip(u128),
    /// Millisecond epoch timestamp.
    Ts(i64),
    /// Opaque JSON object/sub-tree (user settings, column configs, etc.).
    Json(serde_json::Value),
}

/// A field value — always logically a (possibly single-element) array.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Value(pub SmallVec<[Scalar; 1]>);

impl Value {
    pub fn single(s: Scalar) -> Self { Self(SmallVec::from_iter([s])) }
    pub fn empty() -> Self { Self(SmallVec::new()) }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn len(&self) -> usize { self.0.len() }
    pub fn iter(&self) -> impl Iterator<Item = &Scalar> { self.0.iter() }
}

impl Scalar {
    /// Parse an ip string (v4 or v6) into canonical u128 representation.
    pub fn parse_ip(s: &str) -> Option<u128> {
        match s.parse::<IpAddr>().ok()? {
            IpAddr::V4(v4) => Some(ipv4_to_v6(v4).into()),
            IpAddr::V6(v6) => Some(u128::from(v6)),
        }
    }
}

pub fn ipv4_to_v6(v4: Ipv4Addr) -> Ipv6Addr {
    v4.to_ipv6_mapped()
}

pub fn ip_to_string(ip: u128) -> String {
    let v6 = Ipv6Addr::from(ip);
    match v6.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => v6.to_string(),
    }
}

/// Attempt to coerce a serde_json value into a typed Scalar given the hint.
pub fn coerce(json: &serde_json::Value, hint: crate::FieldType) -> Option<Scalar> {
    use crate::FieldType::*;
    match (json, hint) {
        (serde_json::Value::Null, _) => Some(Scalar::Null),
        (serde_json::Value::Bool(b), Bool) => Some(Scalar::Bool(*b)),
        (serde_json::Value::Bool(b), _) => Some(Scalar::Bool(*b)),
        (serde_json::Value::Number(n), Timestamp) => n.as_i64().map(Scalar::Ts),
        (serde_json::Value::Number(n), I64 | U64) => {
            if let Some(i) = n.as_i64() { Some(Scalar::I64(i)) }
            else if let Some(u) = n.as_u64() { Some(Scalar::U64(u)) }
            else { n.as_f64().map(Scalar::F64) }
        }
        (serde_json::Value::Number(n), F64) => n.as_f64().map(Scalar::F64),
        (serde_json::Value::Number(n), _) => {
            if let Some(i) = n.as_i64() { Some(Scalar::I64(i)) }
            else if let Some(u) = n.as_u64() { Some(Scalar::U64(u)) }
            else { n.as_f64().map(Scalar::F64) }
        }
        (serde_json::Value::String(s), I64) => {
            s.parse::<i64>().ok().map(Scalar::I64)
                .or_else(|| s.parse::<u64>().ok().map(Scalar::U64))
                .or_else(|| s.parse::<f64>().ok().map(Scalar::F64))
        }
        (serde_json::Value::String(s), U64) => {
            s.parse::<u64>().ok().map(Scalar::U64)
                .or_else(|| s.parse::<i64>().ok().map(Scalar::I64))
                .or_else(|| s.parse::<f64>().ok().map(Scalar::F64))
        }
        (serde_json::Value::String(s), F64) => s.parse::<f64>().ok().map(Scalar::F64),
        (serde_json::Value::String(s), Bool) => match s.as_str() {
            "true" | "True" | "TRUE" | "1" => Some(Scalar::Bool(true)),
            "false" | "False" | "FALSE" | "0" => Some(Scalar::Bool(false)),
            _ => Some(Scalar::Str(s.clone())),
        },
        (serde_json::Value::String(s), Ip) => Scalar::parse_ip(s).map(Scalar::Ip),
        (serde_json::Value::String(s), Timestamp) => {
            // try integer-ms first, then rfc3339
            if let Ok(v) = s.parse::<i64>() { return Some(Scalar::Ts(v)); }
            time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                .ok()
                .map(|d| Scalar::Ts((d.unix_timestamp_nanos() / 1_000_000) as i64))
        }
        (serde_json::Value::String(s), _) => Some(Scalar::Str(s.clone())),
        (serde_json::Value::Array(_) | serde_json::Value::Object(_), _) => {
            Some(Scalar::Json(json.clone()))
        }
    }
}
