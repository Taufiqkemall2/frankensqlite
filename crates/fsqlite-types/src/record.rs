//! SQLite record format serialization and deserialization.
//!
//! A SQLite record consists of a header followed by data. The header contains
//! the size of the header itself (as a varint) followed by serial type codes
//! (each as a varint) for every column. The data section contains the column
//! values packed sequentially according to their serial types.
//!
//! See: <https://www.sqlite.org/fileformat.html#record_format>

use crate::serial_type::{
    classify_serial_type, read_varint, serial_type_for_blob, serial_type_for_integer,
    serial_type_for_text, serial_type_len, varint_len, write_varint, SerialTypeClass,
};
use crate::value::SqliteValue;

/// Parse a serialized record into a list of `SqliteValue`s.
///
/// The input `data` should be the complete record (header + body).
/// Returns `None` if the record is malformed.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_record(data: &[u8]) -> Option<Vec<SqliteValue>> {
    if data.is_empty() {
        return Some(Vec::new());
    }

    // Read the header size.
    let (header_size_u64, hdr_varint_len) = read_varint(data)?;
    let header_size = header_size_u64 as usize;

    if header_size > data.len() || header_size < hdr_varint_len {
        return None;
    }

    // Parse serial types from the header.
    let mut serial_types = Vec::new();
    let mut offset = hdr_varint_len;
    while offset < header_size {
        let (serial_type, consumed) = read_varint(&data[offset..])?;
        serial_types.push(serial_type);
        offset += consumed;
    }

    // Parse values from the body.
    let mut body_offset = header_size;
    let mut values = Vec::with_capacity(serial_types.len());

    for &st in &serial_types {
        let value_len = serial_type_len(st)? as usize;
        if body_offset + value_len > data.len() {
            return None;
        }

        let value_bytes = &data[body_offset..body_offset + value_len];
        let value = decode_value(st, value_bytes)?;
        values.push(value);
        body_offset += value_len;
    }

    Some(values)
}

/// Serialize a list of `SqliteValue`s into the SQLite record format.
///
/// Returns the complete record bytes (header + body).
pub fn serialize_record(values: &[SqliteValue]) -> Vec<u8> {
    // First pass: compute serial types and header size.
    let serial_types: Vec<u64> = values.iter().map(serial_type_for_value).collect();

    let mut header_content_size: usize = 0;
    for &st in &serial_types {
        header_content_size += varint_len(st);
    }

    // The header size includes the varint encoding of the header size itself.
    // This is a bit circular: the header size varint is part of the header.
    let header_size = compute_header_size(header_content_size);
    let header_size_varint_len = varint_len(header_size as u64);

    // Second pass: compute body size.
    #[allow(clippy::cast_possible_truncation)]
    let body_size: usize = serial_types
        .iter()
        .map(|&st| serial_type_len(st).unwrap_or(0) as usize)
        .sum();

    let total_size = header_size + body_size;
    let mut buf = vec![0u8; total_size];

    // Write header.
    let mut offset = write_varint(&mut buf, header_size as u64);
    debug_assert_eq!(offset, header_size_varint_len);

    for &st in &serial_types {
        offset += write_varint(&mut buf[offset..], st);
    }
    debug_assert_eq!(offset, header_size);

    // Write body.
    #[allow(clippy::cast_possible_truncation)]
    for (value, &st) in values.iter().zip(serial_types.iter()) {
        let value_len = serial_type_len(st).unwrap_or(0) as usize;
        encode_value(value, st, &mut buf[offset..offset + value_len]);
        offset += value_len;
    }

    buf
}

/// Compute the total header size (including the header-size varint itself).
#[allow(clippy::cast_possible_truncation)]
fn compute_header_size(content_size: usize) -> usize {
    // Start with a guess and iterate.
    let mut header_size = content_size + 1; // +1 for the minimum varint
    loop {
        let needed = varint_len(header_size as u64) + content_size;
        if needed <= header_size {
            return header_size;
        }
        header_size = needed;
    }
}

/// Determine the serial type for a `SqliteValue`.
#[allow(clippy::cast_possible_truncation)]
fn serial_type_for_value(value: &SqliteValue) -> u64 {
    match value {
        SqliteValue::Null => 0,
        SqliteValue::Integer(i) => serial_type_for_integer(*i),
        SqliteValue::Float(_) => 7,
        SqliteValue::Text(s) => serial_type_for_text(s.len() as u64),
        SqliteValue::Blob(b) => serial_type_for_blob(b.len() as u64),
    }
}

/// Decode a value from its serial type and raw bytes.
#[allow(clippy::cast_possible_truncation)]
fn decode_value(serial_type: u64, bytes: &[u8]) -> Option<SqliteValue> {
    match classify_serial_type(serial_type) {
        SerialTypeClass::Null => Some(SqliteValue::Null),
        SerialTypeClass::Zero => Some(SqliteValue::Integer(0)),
        SerialTypeClass::One => Some(SqliteValue::Integer(1)),
        SerialTypeClass::Integer => {
            let value = decode_big_endian_signed(bytes);
            Some(SqliteValue::Integer(value))
        }
        SerialTypeClass::Float => {
            if bytes.len() != 8 {
                return None;
            }
            let bits = u64::from_be_bytes(bytes.try_into().ok()?);
            Some(SqliteValue::Float(f64::from_bits(bits)))
        }
        SerialTypeClass::Text => {
            let s = std::str::from_utf8(bytes).ok()?;
            Some(SqliteValue::Text(s.to_owned()))
        }
        SerialTypeClass::Blob => Some(SqliteValue::Blob(bytes.to_vec())),
        SerialTypeClass::Reserved => None,
    }
}

/// Decode a big-endian signed integer of 1-8 bytes.
#[allow(clippy::cast_possible_wrap)]
fn decode_big_endian_signed(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }

    // Sign-extend: if the high bit is set, fill with 0xFF.
    let negative = bytes[0] & 0x80 != 0;
    let mut value: u64 = if negative { u64::MAX } else { 0 };

    for &b in bytes {
        value = (value << 8) | u64::from(b);
    }

    value as i64
}

/// Encode a `SqliteValue` into its serial type byte representation.
fn encode_value(value: &SqliteValue, serial_type: u64, buf: &mut [u8]) {
    match value {
        SqliteValue::Null => {} // serial type 0: no data
        SqliteValue::Integer(i) => {
            if classify_serial_type(serial_type) == SerialTypeClass::Integer {
                let len = buf.len();
                let bytes = i.to_be_bytes();
                // Take the least significant `len` bytes.
                buf.copy_from_slice(&bytes[8 - len..]);
            }
            // Zero and One serial types have no data bytes.
        }
        SqliteValue::Float(f) => {
            let bits = f.to_bits();
            buf.copy_from_slice(&bits.to_be_bytes());
        }
        SqliteValue::Text(s) => {
            buf.copy_from_slice(s.as_bytes());
        }
        SqliteValue::Blob(b) => {
            buf.copy_from_slice(b);
        }
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn empty_record() {
        let data = serialize_record(&[]);
        assert!(!data.is_empty());
        let values = parse_record(&data).unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn null_record() {
        let values = vec![SqliteValue::Null];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].is_null());
    }

    #[test]
    fn integer_zero() {
        let values = vec![SqliteValue::Integer(0)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(0));
    }

    #[test]
    fn integer_one() {
        let values = vec![SqliteValue::Integer(1)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(1));
    }

    #[test]
    fn integer_small() {
        let values = vec![SqliteValue::Integer(42)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(42));
    }

    #[test]
    fn integer_negative() {
        let values = vec![SqliteValue::Integer(-1)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_integer(), Some(-1));
    }

    #[test]
    fn integer_large() {
        for &val in &[i64::MIN, i64::MAX, 0x7FFF_FFFF_FFFF, -0x7FFF_FFFF_FFFF] {
            let values = vec![SqliteValue::Integer(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(parsed[0].as_integer(), Some(val), "roundtrip failed for {val}");
        }
    }

    #[test]
    fn float_value() {
        let values = vec![SqliteValue::Float(3.14)];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_float(), Some(3.14));
    }

    #[test]
    fn float_special_values() {
        for &val in &[0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY] {
            let values = vec![SqliteValue::Float(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(parsed[0].as_float().unwrap().to_bits(), val.to_bits());
        }
    }

    #[test]
    fn text_value() {
        let values = vec![SqliteValue::Text("hello world".to_owned())];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_text(), Some("hello world"));
    }

    #[test]
    fn text_empty() {
        let values = vec![SqliteValue::Text(String::new())];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_text(), Some(""));
    }

    #[test]
    fn blob_value() {
        let values = vec![SqliteValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_blob(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn blob_empty() {
        let values = vec![SqliteValue::Blob(vec![])];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].as_blob(), Some(&[][..]));
    }

    #[test]
    fn mixed_record() {
        let values = vec![
            SqliteValue::Integer(42),
            SqliteValue::Text("hello".to_owned()),
            SqliteValue::Null,
            SqliteValue::Float(2.718),
            SqliteValue::Blob(vec![1, 2, 3]),
            SqliteValue::Integer(0),
            SqliteValue::Integer(1),
        ];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();

        assert_eq!(parsed.len(), 7);
        assert_eq!(parsed[0].as_integer(), Some(42));
        assert_eq!(parsed[1].as_text(), Some("hello"));
        assert!(parsed[2].is_null());
        assert_eq!(parsed[3].as_float(), Some(2.718));
        assert_eq!(parsed[4].as_blob(), Some(&[1, 2, 3][..]));
        assert_eq!(parsed[5].as_integer(), Some(0));
        assert_eq!(parsed[6].as_integer(), Some(1));
    }

    #[test]
    fn integer_size_boundaries() {
        // Verify that integers at size boundaries roundtrip correctly.
        let test_values: &[i64] = &[
            0,
            1,
            -1,
            127,
            -128,
            128,
            -129,
            32767,
            -32768,
            32768,
            8_388_607,
            -8_388_608,
            8_388_608,
            2_147_483_647,
            -2_147_483_648,
            2_147_483_648,
            0x0000_7FFF_FFFF_FFFF,
            -0x0000_8000_0000_0000,
            0x0000_8000_0000_0000,
            i64::MAX,
            i64::MIN,
        ];

        for &val in test_values {
            let values = vec![SqliteValue::Integer(val)];
            let data = serialize_record(&values);
            let parsed = parse_record(&data).unwrap();
            assert_eq!(
                parsed[0].as_integer(),
                Some(val),
                "roundtrip failed for {val}"
            );
        }
    }

    #[test]
    fn decode_big_endian_signed_cases() {
        assert_eq!(decode_big_endian_signed(&[]), 0);
        assert_eq!(decode_big_endian_signed(&[42]), 42);
        assert_eq!(decode_big_endian_signed(&[0xFF]), -1);
        assert_eq!(decode_big_endian_signed(&[0x00, 0x80]), 128);
        assert_eq!(decode_big_endian_signed(&[0xFF, 0x7F]), -129);
    }

    #[test]
    fn malformed_record_too_short() {
        // Header says it's 10 bytes but we only have 2.
        let data = [10, 0];
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn malformed_record_body_truncated() {
        // Header has serial type 6 (8-byte int) but body is empty.
        let data = [2, 6]; // header_size=2, serial_type=6, body missing
        assert!(parse_record(&data).is_none());
    }

    #[test]
    fn large_text_roundtrip() {
        let big_text = "x".repeat(10_000);
        let values = vec![SqliteValue::Text(big_text.clone())];
        let data = serialize_record(&values);
        let parsed = parse_record(&data).unwrap();
        assert_eq!(parsed[0].as_text(), Some(big_text.as_str()));
    }

    #[test]
    fn header_size_computation() {
        // For small records, header size varint is 1 byte.
        assert_eq!(compute_header_size(0), 1);
        assert_eq!(compute_header_size(1), 2);
        assert_eq!(compute_header_size(126), 127);
        // At 127+, the header size varint becomes 2 bytes.
        assert_eq!(compute_header_size(127), 129);
    }

    #[test]
    fn parse_empty_data() {
        let values = parse_record(&[]).unwrap();
        assert!(values.is_empty());
    }
}
