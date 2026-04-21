//! Value encoding for in-redb posting keys and bitmap serialization.

use arkimedb_core::{Scalar, FieldType};
use std::cell::RefCell;
use std::io::Write;

thread_local! {
    // Reused zstd output buffer; avoids reallocating per write.
    static COMPRESS_BUF: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Encode a scalar into a byte key used as a posting-list map key.
/// Format: [type_tag: u8][value bytes...]
pub fn encode_key(field: &str, s: &Scalar) -> Vec<u8> {
    let mut out = Vec::with_capacity(field.len() + 32);
    out.extend_from_slice(field.as_bytes());
    out.push(0x00);
    match s {
        Scalar::Null => out.push(0),
        Scalar::Bool(b) => { out.push(1); out.push(*b as u8); }
        Scalar::I64(v) => { out.push(2); out.extend_from_slice(&v.to_be_bytes()); }
        Scalar::U64(v) => { out.push(3); out.extend_from_slice(&v.to_be_bytes()); }
        Scalar::F64(v) => { out.push(4); out.extend_from_slice(&v.to_be_bytes()); }
        Scalar::Str(v) => { out.push(5); out.extend_from_slice(v.as_bytes()); }
        Scalar::Ip(v)  => { out.push(6); out.extend_from_slice(&v.to_be_bytes()); }
        Scalar::Ts(v)  => { out.push(7); out.extend_from_slice(&v.to_be_bytes()); }
        Scalar::Json(_) => { out.push(8); /* not indexed */ }
    }
    out
}

pub fn compress(data: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    COMPRESS_BUF.with(|b| {
        let mut buf = b.borrow_mut();
        buf.clear();
        {
            let mut enc = zstd::stream::Encoder::new(&mut *buf, level)?;
            enc.write_all(data)?;
            enc.finish()?;
        }
        Ok(buf.clone())
    })
}
pub fn decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    zstd::decode_all(std::io::Cursor::new(data))
}

/// Iterate scalars out of a (possibly nested) JSON value for a given field,
/// coerced to the schema's declared type where possible.
pub fn scalars_of(v: &serde_json::Value, ft: FieldType, out: &mut Vec<Scalar>) {
    match v {
        serde_json::Value::Null => {}
        serde_json::Value::Array(arr) => {
            for x in arr { scalars_of(x, ft, out); }
        }
        _ => {
            if let Some(s) = arkimedb_core::value::coerce(v, ft) {
                if !matches!(s, Scalar::Null) { out.push(s); }
            }
        }
    }
}
