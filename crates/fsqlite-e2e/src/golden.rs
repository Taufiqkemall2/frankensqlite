//! Golden copy management — load, hash, and compare database snapshots.

use std::path::{Path, PathBuf};

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use crate::{E2eError, E2eResult};

/// Metadata about a golden database file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DbMetadata {
    /// Number of tables in the database.
    pub table_count: usize,
    /// Total number of rows across all tables.
    pub row_count: usize,
    /// SQLite page size.
    pub page_size: u32,
}

/// A golden database snapshot used as a reference during testing.
#[derive(Debug, Clone)]
pub struct GoldenCopy {
    /// Human-readable name for this golden copy.
    pub name: String,
    /// Path to the golden database file.
    pub path: PathBuf,
    /// Expected SHA-256 hex digest.
    pub sha256: String,
    /// Structural metadata.
    pub metadata: DbMetadata,
}

impl GoldenCopy {
    /// Compute the SHA-256 hex digest of a file at `path`.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::Io` if the file cannot be read.
    pub fn hash_file(path: &Path) -> E2eResult<String> {
        let bytes = std::fs::read(path)?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        let mut hex = String::with_capacity(64);
        for byte in digest {
            let _ = write!(hex, "{byte:02x}");
        }
        Ok(hex)
    }

    /// Verify that the file at `path` matches the expected hash.
    ///
    /// # Errors
    ///
    /// Returns `E2eError::HashMismatch` on mismatch, or `E2eError::Io` on
    /// read failure.
    pub fn verify_hash(&self, path: &Path) -> E2eResult<()> {
        let actual = Self::hash_file(path)?;
        if actual == self.sha256 {
            Ok(())
        } else {
            Err(E2eError::HashMismatch {
                expected: self.sha256.clone(),
                actual,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_file_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        std::fs::write(&path, b"hello world").unwrap();

        let h1 = GoldenCopy::hash_file(&path).unwrap();
        let h2 = GoldenCopy::hash_file(&path).unwrap();
        assert_eq!(h1, h2, "hashing the same file must be deterministic");
        // Known SHA-256 of "hello world"
        assert_eq!(
            h1,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }
}
