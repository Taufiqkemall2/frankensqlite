//! §3.5.8 Decode Proofs (Auditable Repair) + §3.5.9 Deterministic Encoding.
//!
//! Every decode that repairs corruption MUST produce a proof artifact
//! (a mathematical witness that the fix is correct). In replication,
//! a replica MAY demand proof artifacts for suspicious objects.
//!
//! This module provides the FrankenSQLite-side `EcsDecodeProof` type that
//! wraps asupersync's `DecodeProof` with ECS-specific metadata.

use fsqlite_types::ObjectId;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// ECS Decode Proof (§3.5.8)
// ---------------------------------------------------------------------------

/// A decode proof recording the outcome and metadata of an ECS decode
/// operation (§3.5.8).
///
/// This is the FrankenSQLite-side proof artifact. It captures the fields
/// specified by the spec (`object_id`, `k_source`, received ESIs, source
/// vs repair partitions, success/failure, intermediate rank, timing, seed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsDecodeProof {
    /// The object being decoded.
    pub object_id: ObjectId,
    /// Number of source symbols (K).
    pub k_source: u32,
    /// ESIs of all symbols fed to the decoder.
    pub symbols_received: Vec<u32>,
    /// Subset of received ESIs that were source symbols.
    pub source_esis: Vec<u32>,
    /// Subset of received ESIs that were repair symbols.
    pub repair_esis: Vec<u32>,
    /// Whether the decode succeeded.
    pub decode_success: bool,
    /// Decoder matrix rank at success/failure (if available).
    pub intermediate_rank: Option<u32>,
    /// Timing: wall-clock nanoseconds or virtual time under `LabRuntime`.
    pub timing_ns: u64,
    /// RaptorQ seed used for encoding.
    pub seed: u64,
}

impl EcsDecodeProof {
    /// Create a proof for a successful decode operation.
    #[must_use]
    pub fn success(
        object_id: ObjectId,
        k_source: u32,
        symbols_received: Vec<u32>,
        source_esis: Vec<u32>,
        repair_esis: Vec<u32>,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        info!(
            bead_id = "bd-1hi.28",
            object_id = ?object_id,
            k_source,
            received = symbols_received.len(),
            source = source_esis.len(),
            repair = repair_esis.len(),
            timing_ns,
            "decode proof: SUCCESS"
        );
        Self {
            object_id,
            k_source,
            symbols_received,
            source_esis,
            repair_esis,
            decode_success: true,
            intermediate_rank,
            timing_ns,
            seed,
        }
    }

    /// Create a proof for a failed decode operation.
    #[must_use]
    pub fn failure(
        object_id: ObjectId,
        k_source: u32,
        symbols_received: Vec<u32>,
        source_esis: Vec<u32>,
        repair_esis: Vec<u32>,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        warn!(
            bead_id = "bd-1hi.28",
            object_id = ?object_id,
            k_source,
            received = symbols_received.len(),
            intermediate_rank,
            timing_ns,
            "decode proof: FAILURE"
        );
        Self {
            object_id,
            k_source,
            symbols_received,
            source_esis,
            repair_esis,
            decode_success: false,
            intermediate_rank,
            timing_ns,
            seed,
        }
    }

    /// Build an `EcsDecodeProof` from raw received-symbol ESIs.
    ///
    /// Partitions received ESIs into source (< k_source) and repair (>= k_source).
    #[must_use]
    pub fn from_esis(
        object_id: ObjectId,
        k_source: u32,
        all_esis: &[u32],
        decode_success: bool,
        intermediate_rank: Option<u32>,
        timing_ns: u64,
        seed: u64,
    ) -> Self {
        let mut source_esis = Vec::new();
        let mut repair_esis = Vec::new();
        for &esi in all_esis {
            if esi < k_source {
                source_esis.push(esi);
            } else {
                repair_esis.push(esi);
            }
        }
        source_esis.sort_unstable();
        repair_esis.sort_unstable();
        let symbols_received = {
            let mut v = all_esis.to_vec();
            v.sort_unstable();
            v
        };

        debug!(
            bead_id = "bd-1hi.28",
            source_count = source_esis.len(),
            repair_count = repair_esis.len(),
            "partitioned received ESIs into source/repair"
        );

        Self {
            object_id,
            k_source,
            symbols_received,
            source_esis,
            repair_esis,
            decode_success,
            intermediate_rank,
            timing_ns,
            seed,
        }
    }

    /// Whether this proof records a repair operation (i.e., repair symbols used).
    #[must_use]
    pub fn is_repair(&self) -> bool {
        !self.repair_esis.is_empty()
    }

    /// Whether the decode used the minimum possible symbols (fragile recovery).
    #[must_use]
    pub fn is_minimum_decode(&self) -> bool {
        #[allow(clippy::cast_possible_truncation)]
        let received = self.symbols_received.len() as u32;
        received == self.k_source
    }

    /// Verify that this proof is internally consistent.
    ///
    /// Returns `true` if source_esis + repair_esis == symbols_received and
    /// all ESI partitions are correct.
    #[must_use]
    pub fn is_consistent(&self) -> bool {
        let total = self.source_esis.len() + self.repair_esis.len();
        if total != self.symbols_received.len() {
            return false;
        }
        // Source ESIs must all be < k_source
        if self.source_esis.iter().any(|&e| e >= self.k_source) {
            return false;
        }
        // Repair ESIs must all be >= k_source
        if self.repair_esis.iter().any(|&e| e < self.k_source) {
            return false;
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Decode Audit Trail
// ---------------------------------------------------------------------------

/// An entry in the decode audit trail (§3.5.8, lab runtime integration).
///
/// In lab runtime, every repair decode produces a proof attached to the
/// test trace. This struct groups the proof with its trace context.
#[derive(Debug, Clone)]
pub struct DecodeAuditEntry {
    /// The decode proof artifact.
    pub proof: EcsDecodeProof,
    /// Monotonic sequence number within the audit trail.
    pub seq: u64,
    /// Whether this was produced under lab runtime (deterministic virtual time).
    pub lab_mode: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_object_id(seed: u64) -> ObjectId {
        ObjectId::derive_from_canonical_bytes(&seed.to_le_bytes())
    }

    // -- §3.5.8 test 1: Decode proof creation --

    #[test]
    fn test_decode_proof_creation() {
        let oid = test_object_id(0x1234);
        let k_source = 10;
        let all_esis: Vec<u32> = (0..12).collect(); // 10 source + 2 repair

        let proof = EcsDecodeProof::from_esis(oid, k_source, &all_esis, true, Some(10), 5000, 42);

        assert!(proof.decode_success);
        assert_eq!(proof.source_esis, (0..10).collect::<Vec<u32>>());
        assert_eq!(proof.repair_esis, vec![10, 11]);
        assert_eq!(proof.symbols_received.len(), 12);
        assert!(proof.is_repair());
        assert!(proof.is_consistent());
    }

    // -- §3.5.8 test 2: Lab mode timing --

    #[test]
    fn test_decode_proof_lab_mode() {
        let oid = test_object_id(0x5678);
        let lab_timing_ns = 1_000_000; // deterministic virtual time

        let proof = EcsDecodeProof::success(
            oid,
            8,
            (0..10).collect(),
            (0..8).collect(),
            vec![8, 9],
            Some(8),
            lab_timing_ns,
            99,
        );

        let entry = DecodeAuditEntry {
            proof,
            seq: 1,
            lab_mode: true,
        };

        assert!(entry.lab_mode);
        assert_eq!(entry.proof.timing_ns, lab_timing_ns);
        assert_eq!(entry.seq, 1);
    }

    // -- §3.5.8 test 3: Failure case --

    #[test]
    fn test_decode_proof_failure_case() {
        let oid = test_object_id(0xABCD);
        let k_source = 16;
        // Only 10 symbols received (insufficient for K=16)
        let all_esis: Vec<u32> = (0..10).collect();

        let proof = EcsDecodeProof::from_esis(oid, k_source, &all_esis, false, Some(10), 3000, 77);

        assert!(!proof.decode_success);
        assert_eq!(proof.intermediate_rank, Some(10));
        assert_eq!(proof.source_esis.len(), 10);
        assert!(proof.repair_esis.is_empty());
        assert!(proof.is_consistent());
    }

    // -- §3.5.8 test 4: Auditable (reproducible given same seed and ESIs) --

    #[test]
    fn test_decode_proof_auditable() {
        let oid = test_object_id(0xFEED);
        let k_source = 8;
        let esis: Vec<u32> = (0..10).collect();

        let proof_a = EcsDecodeProof::from_esis(oid, k_source, &esis, true, Some(8), 100, 42);
        let proof_b = EcsDecodeProof::from_esis(oid, k_source, &esis, true, Some(8), 100, 42);

        assert_eq!(
            proof_a, proof_b,
            "same inputs must produce identical proofs"
        );
    }

    // -- §3.5.8 test 5: Attached to trace --

    #[test]
    fn test_decode_proof_attached_to_trace() {
        let oid = test_object_id(0xCAFE);
        let proof = EcsDecodeProof::success(
            oid,
            8,
            (0..10).collect(),
            (0..8).collect(),
            vec![8, 9],
            Some(8),
            500,
            42,
        );

        // In lab runtime, every repair decode produces a proof attached to trace.
        let mut trace: Vec<DecodeAuditEntry> = Vec::new();
        if proof.is_repair() {
            trace.push(DecodeAuditEntry {
                proof,
                seq: 0,
                lab_mode: true,
            });
        }

        assert_eq!(trace.len(), 1, "repair decode must produce audit entry");
        assert!(trace[0].lab_mode);
    }

    // -- §3.5.9 test 8: Deterministic repair generation --

    #[test]
    fn test_deterministic_repair_generation() {
        // Same ObjectId + same config → same seed → same repair symbols.
        // Different ObjectId → different seed.
        let oid_a = test_object_id(0x1111);
        let oid_b = test_object_id(0x2222);

        let seed_a = crate::repair_symbols::derive_repair_seed(&oid_a);
        let seed_b = crate::repair_symbols::derive_repair_seed(&oid_a);
        let seed_c = crate::repair_symbols::derive_repair_seed(&oid_b);

        assert_eq!(
            seed_a, seed_b,
            "same ObjectId must produce same repair seed"
        );
        assert_ne!(
            seed_a, seed_c,
            "different ObjectIds must produce different seeds"
        );
    }

    // -- §3.5.9 test 9: Cross-replica determinism --

    #[test]
    fn test_cross_replica_determinism() {
        // Two independent "replicas" derive seeds from the same ObjectId.
        // Both must get the same seed, proving any replica can regenerate
        // repair symbols independently.
        let payload = b"commit_capsule_payload_12345";
        let oid = ObjectId::derive_from_canonical_bytes(payload);

        // Replica 1
        let seed_r1 = crate::repair_symbols::derive_repair_seed(&oid);
        let budget_r1 = crate::repair_symbols::compute_repair_budget(
            100,
            &crate::repair_symbols::RepairConfig::new(),
        );

        // Replica 2 (same inputs, independent derivation)
        let seed_r2 = crate::repair_symbols::derive_repair_seed(&oid);
        let budget_r2 = crate::repair_symbols::compute_repair_budget(
            100,
            &crate::repair_symbols::RepairConfig::new(),
        );

        assert_eq!(seed_r1, seed_r2, "cross-replica seed derivation must match");
        assert_eq!(
            budget_r1, budget_r2,
            "cross-replica repair budgets must match"
        );
    }

    // -- §3.5.8 property: consistency invariant --

    #[test]
    fn prop_proof_consistency_invariant() {
        for k in [1_u32, 4, 8, 16, 100] {
            for extra in [0_u32, 1, 2, 5, 10] {
                let total = k + extra;
                let esis: Vec<u32> = (0..total).collect();
                let oid = test_object_id(u64::from(k) * 1000 + u64::from(extra));
                let proof = EcsDecodeProof::from_esis(oid, k, &esis, true, None, 0, 0);
                assert!(
                    proof.is_consistent(),
                    "proof must be consistent for k={k}, extra={extra}"
                );
                assert_eq!(
                    proof.source_esis.len(),
                    k as usize,
                    "source count must equal k"
                );
                assert_eq!(
                    proof.repair_esis.len(),
                    extra as usize,
                    "repair count must equal extra"
                );
            }
        }
    }

    // -- §3.5.9 property: seed no collision --

    #[test]
    fn prop_seed_no_collision() {
        use std::collections::HashSet;

        let mut seeds = HashSet::new();
        for i in 0..100_000_u64 {
            let oid = ObjectId::derive_from_canonical_bytes(&i.to_le_bytes());
            let seed = crate::repair_symbols::derive_repair_seed(&oid);
            seeds.insert(seed);
        }

        // With 64-bit seeds and 100k entries, collision probability is ~2.7e-10.
        // We allow at most 1 collision for robustness.
        assert!(
            seeds.len() >= 99_999,
            "expected at most 1 collision in 100k seeds, got {} unique out of 100000",
            seeds.len()
        );
    }

    // -- §3.5.8 test: minimum decode detection --

    #[test]
    fn test_minimum_decode_detection() {
        let oid = test_object_id(0xBEEF);
        let k_source = 10;

        // Exactly K symbols (minimum decode)
        let esis_min: Vec<u32> = (0..10).collect();
        let proof_min =
            EcsDecodeProof::from_esis(oid, k_source, &esis_min, true, Some(10), 100, 42);
        assert!(
            proof_min.is_minimum_decode(),
            "K=10 received=10 should be minimum decode"
        );

        // K+2 symbols (not minimum)
        let esis_extra: Vec<u32> = (0..12).collect();
        let proof_extra =
            EcsDecodeProof::from_esis(oid, k_source, &esis_extra, true, Some(10), 100, 42);
        assert!(
            !proof_extra.is_minimum_decode(),
            "K=10 received=12 should not be minimum decode"
        );
    }
}
