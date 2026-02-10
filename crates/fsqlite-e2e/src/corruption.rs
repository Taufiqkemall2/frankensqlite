//! Corruption injection utilities for resilience and recovery testing.

use std::path::Path;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::{E2eError, E2eResult};
use fsqlite_vfs::host_fs;

/// Strategy for injecting corruption into a database file.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum CorruptionStrategy {
    /// Flip random bits in the file.
    RandomBitFlip {
        /// Number of bits to flip.
        count: usize,
    },
    /// Zero out a range of bytes at the given offset.
    ZeroRange {
        /// Byte offset into the file.
        offset: usize,
        /// Number of bytes to zero.
        length: usize,
    },
    /// Corrupt an entire page (4096-byte aligned).
    PageCorrupt {
        /// Zero-indexed page number.
        page_number: u32,
    },
}

/// Apply a corruption strategy to a database file.
///
/// # Errors
///
/// Returns `E2eError::Io` if the file cannot be read or written.
#[allow(clippy::cast_possible_truncation)]
pub fn inject_corruption(path: &Path, strategy: CorruptionStrategy, seed: u64) -> E2eResult<()> {
    let mut data = host_fs::read(path).map_err(|e| std::io::Error::other(e.to_string()))?;
    if data.is_empty() {
        return Err(E2eError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty file",
        )));
    }

    let mut rng = StdRng::seed_from_u64(seed);

    match strategy {
        CorruptionStrategy::RandomBitFlip { count } => {
            for _ in 0..count {
                let byte_idx = rng.gen_range(0..data.len());
                let bit_idx = rng.gen_range(0..8u8);
                data[byte_idx] ^= 1 << bit_idx;
            }
        }
        CorruptionStrategy::ZeroRange { offset, length } => {
            let end = (offset + length).min(data.len());
            let start = offset.min(data.len());
            for byte in &mut data[start..end] {
                *byte = 0;
            }
        }
        CorruptionStrategy::PageCorrupt { page_number } => {
            let page_size = 4096usize;
            let start = page_number as usize * page_size;
            let end = (start + page_size).min(data.len());
            if start < data.len() {
                for byte in &mut data[start..end] {
                    *byte = rng.r#gen();
                }
            }
        }
    }

    host_fs::write(path, &data).map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_bit_flip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let original = vec![0u8; 4096];
        std::fs::write(&path, &original).unwrap();

        inject_corruption(&path, CorruptionStrategy::RandomBitFlip { count: 10 }, 42).unwrap();

        let corrupted = std::fs::read(&path).unwrap();
        assert_ne!(original, corrupted, "corruption should modify the file");
        assert_eq!(original.len(), corrupted.len(), "size should be unchanged");
    }

    #[test]
    fn test_zero_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, [0xFF_u8; 1024]).unwrap();

        inject_corruption(
            &path,
            CorruptionStrategy::ZeroRange {
                offset: 100,
                length: 50,
            },
            0,
        )
        .unwrap();

        let data = std::fs::read(&path).unwrap();
        assert!(data[100..150].iter().all(|&b| b == 0));
        assert!(data[0..100].iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_page_corrupt_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, [0u8; 8192]).unwrap();

        inject_corruption(
            &path,
            CorruptionStrategy::PageCorrupt { page_number: 1 },
            99,
        )
        .unwrap();
        let c1 = std::fs::read(&path).unwrap();

        // Re-create and corrupt again with same seed
        std::fs::write(&path, [0u8; 8192]).unwrap();
        inject_corruption(
            &path,
            CorruptionStrategy::PageCorrupt { page_number: 1 },
            99,
        )
        .unwrap();
        let c2 = std::fs::read(&path).unwrap();

        assert_eq!(c1, c2, "same seed must produce identical corruption");
    }
}
