//! ECS (Erasure-Coded Stream) substrate types.
//!
//! This module defines foundational identity primitives for Native mode.
//! Spec: COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md §3.5.1.

use std::fmt;

/// Domain separation prefix for ECS ObjectIds (spec: `"fsqlite:ecs:v1"`).
const ECS_OBJECT_ID_DOMAIN_SEPARATOR: &[u8] = b"fsqlite:ecs:v1";

/// Canonical 32-byte hash of an ECS object's payload.
///
/// The spec refers to this as `payload_hash` in:
/// `ObjectId = Trunc128(BLAKE3("fsqlite:ecs:v1" || canonical_object_header || payload_hash))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct PayloadHash([u8; 32]);

impl PayloadHash {
    /// Construct from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the hash bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hash a payload using BLAKE3-256.
    #[must_use]
    pub fn blake3(payload: &[u8]) -> Self {
        let hash = blake3::hash(payload);
        Self(*hash.as_bytes())
    }
}

/// 16-byte truncated content-addressed identity for an ECS object.
///
/// Spec:
/// `ObjectId = Trunc128(BLAKE3("fsqlite:ecs:v1" || canonical_object_header || payload_hash))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ObjectId([u8; 16]);

impl ObjectId {
    /// ObjectId length in bytes.
    pub const LEN: usize = 16;

    /// Domain separation prefix from the spec.
    pub const DOMAIN_SEPARATOR: &'static [u8] = ECS_OBJECT_ID_DOMAIN_SEPARATOR;

    /// Construct from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Derive an ObjectId from already-canonicalized bytes.
    ///
    /// `canonical_bytes` must be a deterministic, versioned wire-format blob
    /// (spec: "not serde vibes") representing the object's header plus its
    /// `payload_hash`.
    #[must_use]
    pub fn derive_from_canonical_bytes(canonical_bytes: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(Self::DOMAIN_SEPARATOR);
        hasher.update(canonical_bytes);
        let digest = hasher.finalize();

        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&digest.as_bytes()[..Self::LEN]);
        Self(out)
    }

    /// Derive an ObjectId from canonical header bytes and a payload hash.
    #[must_use]
    pub fn derive(canonical_object_header: &[u8], payload_hash: PayloadHash) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(Self::DOMAIN_SEPARATOR);
        hasher.update(canonical_object_header);
        hasher.update(payload_hash.as_bytes());
        let digest = hasher.finalize();

        let mut out = [0u8; Self::LEN];
        out.copy_from_slice(&digest.as_bytes()[..Self::LEN]);
        Self(out)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for ObjectId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 16]> for ObjectId {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_id_blake3_derivation() {
        let header = b"hdr:v1\x00";
        let payload = b"hello world";
        let payload_hash = PayloadHash::blake3(payload);

        let derived = ObjectId::derive(header, payload_hash);

        let mut canonical = Vec::new();
        canonical.extend_from_slice(header);
        canonical.extend_from_slice(payload_hash.as_bytes());
        let derived2 = ObjectId::derive_from_canonical_bytes(&canonical);

        assert_eq!(derived, derived2);

        let mut hasher = blake3::Hasher::new();
        hasher.update(ObjectId::DOMAIN_SEPARATOR);
        hasher.update(&canonical);
        let digest = hasher.finalize();
        let mut expected = [0u8; 16];
        expected.copy_from_slice(&digest.as_bytes()[..16]);

        assert_eq!(derived.as_bytes(), &expected);
    }

    #[test]
    fn test_object_id_collision_resistance() {
        let header = b"hdr:v1\x00";
        let payload_a = b"payload-a";
        let payload_b = b"payload-b";
        let id_a = ObjectId::derive(header, PayloadHash::blake3(payload_a));
        let id_b = ObjectId::derive(header, PayloadHash::blake3(payload_b));
        assert_ne!(id_a, id_b);
    }

    #[test]
    fn test_object_id_deterministic() {
        let header = b"hdr:v1\x00";
        let payload = b"payload";
        let hash = PayloadHash::blake3(payload);
        let id1 = ObjectId::derive(header, hash);
        let id2 = ObjectId::derive(header, hash);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_object_id_display_hex() {
        let id = ObjectId::from_bytes([0u8; 16]);
        let s = id.to_string();
        assert_eq!(s.len(), 32);
        assert!(s.chars().all(|ch| matches!(ch, '0'..='9' | 'a'..='f')));

        // A stable known-value check (16 zero bytes => 32 zero hex chars).
        assert_eq!(s, "00000000000000000000000000000000");
    }
}
