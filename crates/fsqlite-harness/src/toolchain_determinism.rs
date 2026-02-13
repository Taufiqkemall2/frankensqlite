//! Toolchain determinism matrix and acceptance boundaries (bd-mblr.7.8.1).
//!
//! Defines the matrix of supported toolchains, platforms, and compiler
//! configurations, along with acceptance criteria for deterministic
//! replay across environments.  The executor (bd-mblr.7.8.2) consumes
//! this matrix to run cross-toolchain determinism checks.
//!
//! # Design
//!
//! A [`ToolchainEntry`] describes one compiler/platform combination.
//! A [`DeterminismProbe`] describes a specific reproducibility test.
//! A [`DeterminismMatrix`] ties entries to probes with acceptance
//! thresholds and seed contracts.
//!
//! # Determinism Guarantee
//!
//! FrankenSQLite uses `#[forbid(unsafe_code)]`, so all non-determinism
//! comes from floating-point ordering, HashMap iteration order, or
//! platform-specific behaviour.  The matrix captures which operations
//! are required to be bit-exact vs. semantically equivalent.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

/// Bead identifier for log correlation.
#[allow(dead_code)]
const BEAD_ID: &str = "bd-mblr.7.8.1";

/// Domain tag for determinism seed derivation.
const SEED_DOMAIN: &[u8] = b"toolchain_determinism";

// ---------------------------------------------------------------------------
// Toolchain description
// ---------------------------------------------------------------------------

/// Target operating system family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OsFamily {
    Linux,
    MacOs,
    Windows,
}

impl OsFamily {
    /// All supported OS families.
    pub const ALL: &[Self] = &[Self::Linux, Self::MacOs, Self::Windows];
}

impl fmt::Display for OsFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Linux => write!(f, "linux"),
            Self::MacOs => write!(f, "macos"),
            Self::Windows => write!(f, "windows"),
        }
    }
}

/// CPU architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Architecture {
    X86_64,
    Aarch64,
}

impl Architecture {
    /// All supported architectures.
    pub const ALL: &[Self] = &[Self::X86_64, Self::Aarch64];
}

impl fmt::Display for Architecture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::X86_64 => write!(f, "x86_64"),
            Self::Aarch64 => write!(f, "aarch64"),
        }
    }
}

/// Rust toolchain channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RustChannel {
    /// Nightly (required for FrankenSQLite edition 2024).
    Nightly,
    /// Beta (for forward-compatibility testing).
    Beta,
}

impl fmt::Display for RustChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nightly => write!(f, "nightly"),
            Self::Beta => write!(f, "beta"),
        }
    }
}

/// Compiler optimization level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OptLevel {
    /// Debug mode (opt-level=0).
    Debug,
    /// Release mode (opt-level=3, per workspace Cargo.toml).
    Release,
}

impl fmt::Display for OptLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Debug => write!(f, "debug"),
            Self::Release => write!(f, "release"),
        }
    }
}

/// A specific toolchain/platform combination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ToolchainEntry {
    /// Unique identifier (e.g., "linux-x86_64-nightly-release").
    pub id: String,

    /// Operating system.
    pub os: OsFamily,

    /// CPU architecture.
    pub arch: Architecture,

    /// Rust channel.
    pub channel: RustChannel,

    /// Optimization level.
    pub opt_level: OptLevel,

    /// Whether this is a primary (CI-required) or secondary (best-effort) target.
    pub primary: bool,

    /// Human-readable notes.
    pub notes: String,
}

impl ToolchainEntry {
    /// Build the canonical toolchain ID from components.
    #[must_use]
    pub fn canonical_id(
        os: OsFamily,
        arch: Architecture,
        channel: RustChannel,
        opt: OptLevel,
    ) -> String {
        format!("{os}-{arch}-{channel}-{opt}")
    }
}

impl fmt::Display for ToolchainEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let primary_tag = if self.primary { " [PRIMARY]" } else { "" };
        write!(f, "{}{primary_tag}", self.id)
    }
}

// ---------------------------------------------------------------------------
// Determinism probes
// ---------------------------------------------------------------------------

/// What kind of determinism is being tested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DeterminismKind {
    /// Bit-exact: output bytes must be identical across environments.
    BitExact,
    /// Semantic: outputs must be logically equivalent (e.g., same rows, possibly different order).
    Semantic,
    /// Statistical: outputs must be within configured epsilon bounds.
    Statistical,
}

impl fmt::Display for DeterminismKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BitExact => write!(f, "bit-exact"),
            Self::Semantic => write!(f, "semantic"),
            Self::Statistical => write!(f, "statistical"),
        }
    }
}

/// A specific determinism test probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismProbe {
    /// Unique probe identifier (e.g., "DPROBE-001").
    pub id: String,

    /// Human-readable name.
    pub name: String,

    /// What this probe tests.
    pub description: String,

    /// The kind of determinism required.
    pub kind: DeterminismKind,

    /// Subsystem being tested.
    pub subsystem: Subsystem,

    /// Root seed for this probe (deterministic).
    pub seed: u64,

    /// Acceptance threshold (interpretation depends on kind).
    ///
    /// - `BitExact`: must be 1.0 (100% match).
    /// - `Semantic`: fraction of test cases that must match (e.g., 0.99).
    /// - `Statistical`: maximum allowed divergence epsilon.
    pub acceptance_threshold: f64,

    /// Maximum allowed wall-clock variance ratio between fastest and slowest toolchain.
    /// For example, 3.0 means the slowest must be no more than 3x the fastest.
    pub max_timing_ratio: f64,
}

impl fmt::Display for DeterminismProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} ({}, {}, threshold={:.2})",
            self.id, self.name, self.kind, self.subsystem, self.acceptance_threshold
        )
    }
}

/// Subsystem under determinism testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Subsystem {
    /// Seed derivation (xxh3_64-based).
    SeedDerivation,
    /// Page serialisation (btree page layout).
    PageSerialization,
    /// SQL parsing and AST generation.
    SqlParsing,
    /// Query planning (plan shape).
    QueryPlanning,
    /// VDBE bytecode generation.
    VdbeBytecode,
    /// MVCC version chain operations.
    MvccVersioning,
    /// WAL format and checkpointing.
    WalFormat,
    /// Encryption (XChaCha20-Poly1305).
    Encryption,
    /// Hash computations (blake3, crc32c).
    Hashing,
    /// Full end-to-end query result.
    EndToEnd,
}

impl Subsystem {
    /// All subsystems in canonical order.
    pub const ALL: &[Self] = &[
        Self::SeedDerivation,
        Self::PageSerialization,
        Self::SqlParsing,
        Self::QueryPlanning,
        Self::VdbeBytecode,
        Self::MvccVersioning,
        Self::WalFormat,
        Self::Encryption,
        Self::Hashing,
        Self::EndToEnd,
    ];
}

impl fmt::Display for Subsystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SeedDerivation => write!(f, "seed"),
            Self::PageSerialization => write!(f, "page"),
            Self::SqlParsing => write!(f, "parser"),
            Self::QueryPlanning => write!(f, "planner"),
            Self::VdbeBytecode => write!(f, "vdbe"),
            Self::MvccVersioning => write!(f, "mvcc"),
            Self::WalFormat => write!(f, "wal"),
            Self::Encryption => write!(f, "encryption"),
            Self::Hashing => write!(f, "hashing"),
            Self::EndToEnd => write!(f, "e2e"),
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical probes
// ---------------------------------------------------------------------------

/// Build the canonical set of determinism probes.
#[must_use]
pub fn canonical_probes(root_seed: u64) -> Vec<DeterminismProbe> {
    let derive = |probe_id: &str| -> u64 {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(&root_seed.to_le_bytes());
        buf.extend_from_slice(SEED_DOMAIN);
        buf.extend_from_slice(probe_id.as_bytes());
        xxh3_64(&buf)
    };

    vec![
        DeterminismProbe {
            id: "DPROBE-001".to_owned(),
            name: "seed_derivation_exact".to_owned(),
            description: "xxh3_64 seed derivation produces identical values across all toolchains".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::SeedDerivation,
            seed: derive("DPROBE-001"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-002".to_owned(),
            name: "page_serialization_exact".to_owned(),
            description: "B-tree page serialization produces identical bytes".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::PageSerialization,
            seed: derive("DPROBE-002"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-003".to_owned(),
            name: "sql_parse_ast_exact".to_owned(),
            description: "SQL parser produces identical AST for the same input".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::SqlParsing,
            seed: derive("DPROBE-003"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-004".to_owned(),
            name: "query_plan_semantic".to_owned(),
            description: "Query planner produces semantically equivalent plans (join order may vary)".to_owned(),
            kind: DeterminismKind::Semantic,
            subsystem: Subsystem::QueryPlanning,
            seed: derive("DPROBE-004"),
            acceptance_threshold: 0.95,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-005".to_owned(),
            name: "vdbe_bytecode_exact".to_owned(),
            description: "VDBE bytecode generation is identical for the same plan".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::VdbeBytecode,
            seed: derive("DPROBE-005"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-006".to_owned(),
            name: "mvcc_version_chain_semantic".to_owned(),
            description: "MVCC operations produce equivalent version chains (timing may differ)".to_owned(),
            kind: DeterminismKind::Semantic,
            subsystem: Subsystem::MvccVersioning,
            seed: derive("DPROBE-006"),
            acceptance_threshold: 0.99,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-007".to_owned(),
            name: "wal_format_exact".to_owned(),
            description: "WAL frame format and checksums are bit-identical".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::WalFormat,
            seed: derive("DPROBE-007"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-008".to_owned(),
            name: "encryption_exact".to_owned(),
            description: "XChaCha20-Poly1305 encryption produces identical ciphertext".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::Encryption,
            seed: derive("DPROBE-008"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 5.0,
        },
        DeterminismProbe {
            id: "DPROBE-009".to_owned(),
            name: "hash_exact".to_owned(),
            description: "blake3 and crc32c produce identical hashes".to_owned(),
            kind: DeterminismKind::BitExact,
            subsystem: Subsystem::Hashing,
            seed: derive("DPROBE-009"),
            acceptance_threshold: 1.0,
            max_timing_ratio: 3.0,
        },
        DeterminismProbe {
            id: "DPROBE-010".to_owned(),
            name: "e2e_query_result_semantic".to_owned(),
            description: "End-to-end query results are semantically equivalent (row order may differ for unordered queries)".to_owned(),
            kind: DeterminismKind::Semantic,
            subsystem: Subsystem::EndToEnd,
            seed: derive("DPROBE-010"),
            acceptance_threshold: 0.99,
            max_timing_ratio: 5.0,
        },
    ]
}

// ---------------------------------------------------------------------------
// Canonical toolchain matrix
// ---------------------------------------------------------------------------

/// Build the canonical toolchain matrix.
#[must_use]
pub fn canonical_toolchains() -> Vec<ToolchainEntry> {
    vec![
        // Primary targets (CI-required)
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::Linux,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: true,
            notes: "Primary CI target".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Debug,
            ),
            os: OsFamily::Linux,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Debug,
            primary: true,
            notes: "Debug builds for assertion coverage".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::Aarch64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::Linux,
            arch: Architecture::Aarch64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: true,
            notes: "ARM64 cross-compilation target".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::MacOs,
                Architecture::Aarch64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::MacOs,
            arch: Architecture::Aarch64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: true,
            notes: "Apple Silicon target".to_owned(),
        },
        // Secondary targets (best-effort)
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::MacOs,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::MacOs,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: false,
            notes: "Intel Mac (Rosetta)".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Windows,
                Architecture::X86_64,
                RustChannel::Nightly,
                OptLevel::Release,
            ),
            os: OsFamily::Windows,
            arch: Architecture::X86_64,
            channel: RustChannel::Nightly,
            opt_level: OptLevel::Release,
            primary: false,
            notes: "Windows x86_64 target".to_owned(),
        },
        ToolchainEntry {
            id: ToolchainEntry::canonical_id(
                OsFamily::Linux,
                Architecture::X86_64,
                RustChannel::Beta,
                OptLevel::Release,
            ),
            os: OsFamily::Linux,
            arch: Architecture::X86_64,
            channel: RustChannel::Beta,
            opt_level: OptLevel::Release,
            primary: false,
            notes: "Forward-compatibility with beta channel".to_owned(),
        },
    ]
}

// ---------------------------------------------------------------------------
// Determinism matrix
// ---------------------------------------------------------------------------

/// The full determinism matrix combining toolchains, probes, and thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismMatrix {
    /// Root seed for all probe derivations.
    pub root_seed: u64,

    /// Toolchain entries.
    pub toolchains: Vec<ToolchainEntry>,

    /// Determinism probes.
    pub probes: Vec<DeterminismProbe>,

    /// Reference toolchain ID (all comparisons are against this).
    pub reference_toolchain: String,
}

impl DeterminismMatrix {
    /// Build the canonical matrix from defaults.
    #[must_use]
    pub fn canonical(root_seed: u64) -> Self {
        let toolchains = canonical_toolchains();
        let reference = toolchains
            .iter()
            .find(|t| {
                t.primary
                    && t.os == OsFamily::Linux
                    && t.arch == Architecture::X86_64
                    && t.opt_level == OptLevel::Release
            })
            .map_or_else(|| toolchains[0].id.clone(), |t| t.id.clone());

        Self {
            root_seed,
            toolchains,
            probes: canonical_probes(root_seed),
            reference_toolchain: reference,
        }
    }

    /// Validate the matrix for internal consistency.
    #[must_use]
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.toolchains.is_empty() {
            errors.push("no toolchain entries defined".to_owned());
        }
        if self.probes.is_empty() {
            errors.push("no determinism probes defined".to_owned());
        }

        // Check for duplicate toolchain IDs.
        let tc_ids: std::collections::BTreeSet<&str> =
            self.toolchains.iter().map(|t| t.id.as_str()).collect();
        if tc_ids.len() != self.toolchains.len() {
            errors.push("duplicate toolchain IDs".to_owned());
        }

        // Check for duplicate probe IDs.
        let probe_ids: std::collections::BTreeSet<&str> =
            self.probes.iter().map(|p| p.id.as_str()).collect();
        if probe_ids.len() != self.probes.len() {
            errors.push("duplicate probe IDs".to_owned());
        }

        // Reference toolchain must exist.
        if !tc_ids.contains(self.reference_toolchain.as_str()) {
            errors.push(format!(
                "reference toolchain '{}' not in matrix",
                self.reference_toolchain
            ));
        }

        // Must have at least one primary toolchain.
        let primary_count = self.toolchains.iter().filter(|t| t.primary).count();
        if primary_count == 0 {
            errors.push("no primary toolchain entries".to_owned());
        }

        // All bit-exact probes must have threshold 1.0.
        for probe in &self.probes {
            if probe.kind == DeterminismKind::BitExact
                && (probe.acceptance_threshold - 1.0).abs() > f64::EPSILON
            {
                errors.push(format!(
                    "probe {} is BitExact but threshold is {} (must be 1.0)",
                    probe.id, probe.acceptance_threshold
                ));
            }
        }

        errors
    }

    /// Total number of test combinations (toolchains x probes).
    #[must_use]
    pub fn total_combinations(&self) -> usize {
        self.toolchains.len() * self.probes.len()
    }

    /// Serialize to deterministic JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserialize from JSON.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the JSON is malformed.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

// ---------------------------------------------------------------------------
// Probe result
// ---------------------------------------------------------------------------

/// Result of running a single probe on a single toolchain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    /// Probe ID.
    pub probe_id: String,

    /// Toolchain ID.
    pub toolchain_id: String,

    /// Whether the probe passed the acceptance threshold.
    pub passed: bool,

    /// Observed match ratio (1.0 = perfect match).
    pub match_ratio: f64,

    /// Wall-clock time in microseconds.
    pub duration_us: u64,

    /// Output hash for bit-exact comparison (hex string).
    pub output_hash: String,

    /// Human-readable notes on divergence, if any.
    pub divergence_notes: Option<String>,
}

/// Aggregate results across all toolchains for a single probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeAggregateResult {
    /// Probe ID.
    pub probe_id: String,

    /// Results per toolchain.
    pub results: Vec<ProbeResult>,

    /// Whether all toolchains passed.
    pub all_passed: bool,

    /// Minimum match ratio across toolchains.
    pub min_match_ratio: f64,

    /// Maximum timing ratio (slowest / fastest).
    pub timing_ratio: f64,

    /// Whether the timing ratio exceeds the probe's limit.
    pub timing_exceeded: bool,
}

// ---------------------------------------------------------------------------
// Coverage metrics
// ---------------------------------------------------------------------------

/// Coverage report for the determinism matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismCoverage {
    /// Total toolchains.
    pub toolchain_count: usize,

    /// Primary toolchains.
    pub primary_toolchain_count: usize,

    /// Total probes.
    pub probe_count: usize,

    /// Probes by kind.
    pub by_kind: BTreeMap<String, usize>,

    /// Probes by subsystem.
    pub by_subsystem: BTreeMap<String, usize>,

    /// Total test combinations.
    pub total_combinations: usize,

    /// Subsystems covered.
    pub subsystems_covered: Vec<String>,
}

/// Compute coverage metrics for a determinism matrix.
#[must_use]
pub fn compute_determinism_coverage(matrix: &DeterminismMatrix) -> DeterminismCoverage {
    let mut by_kind: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_subsystem: BTreeMap<String, usize> = BTreeMap::new();

    for probe in &matrix.probes {
        *by_kind.entry(format!("{}", probe.kind)).or_insert(0) += 1;
        *by_subsystem
            .entry(format!("{}", probe.subsystem))
            .or_insert(0) += 1;
    }

    let subsystems: Vec<String> = by_subsystem.keys().cloned().collect();

    DeterminismCoverage {
        toolchain_count: matrix.toolchains.len(),
        primary_toolchain_count: matrix.toolchains.iter().filter(|t| t.primary).count(),
        probe_count: matrix.probes.len(),
        by_kind,
        by_subsystem,
        total_combinations: matrix.total_combinations(),
        subsystems_covered: subsystems,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Toolchain matrix ---

    #[test]
    fn test_canonical_toolchains_non_empty() {
        let tcs = canonical_toolchains();
        assert!(
            tcs.len() >= 4,
            "expected >= 4 toolchains, got {}",
            tcs.len()
        );
    }

    #[test]
    fn test_canonical_toolchain_ids_unique() {
        let tcs = canonical_toolchains();
        let ids: std::collections::BTreeSet<&str> = tcs.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids.len(), tcs.len(), "duplicate toolchain IDs");
    }

    #[test]
    fn test_canonical_toolchains_have_primary() {
        let tcs = canonical_toolchains();
        let primary = tcs.iter().filter(|t| t.primary).count();
        assert!(
            primary >= 3,
            "expected >= 3 primary toolchains, got {primary}"
        );
    }

    #[test]
    fn test_canonical_id_format() {
        let id = ToolchainEntry::canonical_id(
            OsFamily::Linux,
            Architecture::X86_64,
            RustChannel::Nightly,
            OptLevel::Release,
        );
        assert_eq!(id, "linux-x86_64-nightly-release");
    }

    #[test]
    fn test_toolchain_display() {
        let tcs = canonical_toolchains();
        let primary = &tcs[0];
        let s = primary.to_string();
        assert!(s.contains("[PRIMARY]"), "primary should be tagged: {s}");
    }

    // --- OS/Arch/Channel enums ---

    #[test]
    fn test_os_all_variants() {
        assert_eq!(OsFamily::ALL.len(), 3);
    }

    #[test]
    fn test_arch_all_variants() {
        assert_eq!(Architecture::ALL.len(), 2);
    }

    #[test]
    fn test_subsystem_all_variants() {
        assert_eq!(Subsystem::ALL.len(), 10);
    }

    // --- Determinism probes ---

    #[test]
    fn test_canonical_probes_non_empty() {
        let probes = canonical_probes(42);
        assert!(
            probes.len() >= 10,
            "expected >= 10 probes, got {}",
            probes.len()
        );
    }

    #[test]
    fn test_canonical_probe_ids_unique() {
        let probes = canonical_probes(42);
        let ids: std::collections::BTreeSet<&str> = probes.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids.len(), probes.len(), "duplicate probe IDs");
    }

    #[test]
    fn test_bit_exact_probes_threshold_1() {
        let probes = canonical_probes(42);
        for p in &probes {
            if p.kind == DeterminismKind::BitExact {
                assert!(
                    (p.acceptance_threshold - 1.0).abs() < f64::EPSILON,
                    "BitExact probe {} has threshold {} != 1.0",
                    p.id,
                    p.acceptance_threshold
                );
            }
        }
    }

    #[test]
    fn test_probe_seeds_deterministic() {
        let p1 = canonical_probes(42);
        let p2 = canonical_probes(42);
        for (a, b) in p1.iter().zip(p2.iter()) {
            assert_eq!(
                a.seed, b.seed,
                "probe {} seed should be deterministic",
                a.id
            );
        }
    }

    #[test]
    fn test_probe_seeds_differ_with_root() {
        let p1 = canonical_probes(42);
        let p2 = canonical_probes(99);
        let differ = p1.iter().zip(p2.iter()).any(|(a, b)| a.seed != b.seed);
        assert!(
            differ,
            "different root seeds should produce different probe seeds"
        );
    }

    #[test]
    fn test_probes_cover_all_subsystems() {
        let probes = canonical_probes(42);
        let subsystems: std::collections::BTreeSet<Subsystem> =
            probes.iter().map(|p| p.subsystem).collect();
        for expected in Subsystem::ALL {
            assert!(
                subsystems.contains(expected),
                "missing subsystem: {expected}"
            );
        }
    }

    #[test]
    fn test_probes_have_all_kinds() {
        let probes = canonical_probes(42);
        let kinds: std::collections::BTreeSet<DeterminismKind> =
            probes.iter().map(|p| p.kind).collect();
        assert!(kinds.contains(&DeterminismKind::BitExact));
        assert!(kinds.contains(&DeterminismKind::Semantic));
    }

    #[test]
    fn test_probe_display() {
        let probes = canonical_probes(42);
        let s = probes[0].to_string();
        assert!(s.contains("DPROBE-001"));
        assert!(s.contains("bit-exact"));
    }

    // --- Matrix construction and validation ---

    #[test]
    fn test_canonical_matrix_valid() {
        let matrix = DeterminismMatrix::canonical(42);
        let errors = matrix.validate();
        assert!(
            errors.is_empty(),
            "canonical matrix has validation errors: {errors:?}"
        );
    }

    #[test]
    fn test_matrix_total_combinations() {
        let matrix = DeterminismMatrix::canonical(42);
        assert_eq!(
            matrix.total_combinations(),
            matrix.toolchains.len() * matrix.probes.len()
        );
    }

    #[test]
    fn test_matrix_reference_toolchain_is_primary() {
        let matrix = DeterminismMatrix::canonical(42);
        let ref_tc = matrix
            .toolchains
            .iter()
            .find(|t| t.id == matrix.reference_toolchain);
        assert!(ref_tc.is_some(), "reference toolchain not found");
        assert!(
            ref_tc.unwrap().primary,
            "reference toolchain should be primary"
        );
    }

    #[test]
    fn test_matrix_json_roundtrip() {
        let matrix = DeterminismMatrix::canonical(42);
        let json = matrix.to_json().expect("serialize");
        let restored = DeterminismMatrix::from_json(&json).expect("deserialize");

        assert_eq!(restored.root_seed, matrix.root_seed);
        assert_eq!(restored.toolchains.len(), matrix.toolchains.len());
        assert_eq!(restored.probes.len(), matrix.probes.len());
        assert_eq!(restored.reference_toolchain, matrix.reference_toolchain);
    }

    #[test]
    fn test_matrix_validation_catches_empty_toolchains() {
        let mut matrix = DeterminismMatrix::canonical(42);
        matrix.toolchains.clear();
        let errors = matrix.validate();
        assert!(
            errors.iter().any(|e| e.contains("no toolchain")),
            "should catch empty toolchains: {errors:?}"
        );
    }

    #[test]
    fn test_matrix_validation_catches_invalid_reference() {
        let mut matrix = DeterminismMatrix::canonical(42);
        matrix.reference_toolchain = "nonexistent".to_owned();
        let errors = matrix.validate();
        assert!(
            errors.iter().any(|e| e.contains("reference toolchain")),
            "should catch invalid reference: {errors:?}"
        );
    }

    // --- Coverage ---

    #[test]
    fn test_determinism_coverage() {
        let matrix = DeterminismMatrix::canonical(42);
        let cov = compute_determinism_coverage(&matrix);

        assert_eq!(cov.toolchain_count, matrix.toolchains.len());
        assert_eq!(cov.probe_count, matrix.probes.len());
        assert!(cov.primary_toolchain_count >= 3);
        assert!(cov.by_kind.len() >= 2); // BitExact + Semantic
        assert!(cov.by_subsystem.len() >= 10);
        assert_eq!(cov.total_combinations, matrix.total_combinations());
    }

    // --- Enum Display ---

    #[test]
    fn test_enum_displays() {
        assert_eq!(OsFamily::Linux.to_string(), "linux");
        assert_eq!(Architecture::Aarch64.to_string(), "aarch64");
        assert_eq!(RustChannel::Nightly.to_string(), "nightly");
        assert_eq!(OptLevel::Release.to_string(), "release");
        assert_eq!(DeterminismKind::BitExact.to_string(), "bit-exact");
        assert_eq!(Subsystem::Encryption.to_string(), "encryption");
    }
}
