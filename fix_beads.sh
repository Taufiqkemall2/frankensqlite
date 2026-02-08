#!/usr/bin/env bash
set -e

# ==============================================================================
# Bead Refactoring Script (Robust Version)
# Generated based on audit of COMPREHENSIVE_SPEC_FOR_FRANKENSQLITE_V1.md
# ==============================================================================

echo "Refactoring §5.10 beads to eliminate overlaps..."

# 1. Refine bd-2blq to focus on §5.10.1 and §5.10.1.1
br update bd-2blq --title "§5.10.1 Intent Logs & RowId Allocation" --description "Implement IntentLog structure, IntentOp kinds, and the global RowId allocator (§5.10.1 + §5.10.1.1). Includes IntentFootprint, SemanticKeyRef, and the 7-bit StructuralEffects flags. Covers the snapshot-independent RowId allocator for concurrent inserts."

# 2. Refine bd-1h3b to focus on §5.10.2 and §5.10.2.1
br update bd-1h3b --title "§5.10.2 Deterministic Rebase & Index Regeneration" --description "Implement the deterministic rebase algorithm (§5.10.2) and index regeneration (§5.10.2.1). Includes schema epoch checks, base drift detection, rebase safety rules (blocking reads), and UpdateExpression replay logic. Handles re-evaluation of constraints and secondary index updates."

# 3. Refine bd-3dv4 to focus on §5.10.3-5
br update bd-3dv4 --title "§5.10.3-5 Physical Merge & Safety Ladder" --description "Implement structured page patch merge (§5.10.3), the commit-time merge policy ladder (§5.10.4), and safety proofs (§5.10.5). Enforces the ban on raw XOR merges for structured pages. Implements the parse->merge->repack pipeline for B-tree leaves."

# 4. Update bd-c6tx title for clarity
br update bd-c6tx --title "§5.10.6-8 History Compression & Merge Certificates" --description "Implement PageHistory objects for MVCC compression (§5.10.6), intent commutativity rules (trace-normalized merge, §5.10.7), and MergeCertificate generation/verification (§5.10.8). Includes the independent-op definition."

# 5. Close redundant bead bd-13b7
# Best-effort comment (ignore failure if syntax varies)
br comments bd-13b7 add "Redundant: superseded by refined bd-2blq (§5.10.1) and bd-1h3b (§5.10.2)" || true
br update bd-13b7 --status closed

echo "Splitting dense §4 beads..."

# 6. Split bd-3go.9 (Regions/Cancel/Obligations) -> Update original to just §4.11
br update bd-3go.9 --title "§4.11 Structured Concurrency (Regions)" --description "Implement the region tree lifetime model (§4.11). Every task/actor must be region-owned. Enforce INV-REGION-QUIESCENCE: no region closes until all children complete and finalizers run. Database::close() must await quiescence."

# Create new for §4.12
br create --title "§4.12 Cancellation Protocol (Checkpoints + Masking)" --description "Implement the cancellation state machine, checkpoints, and masking (§4.12). Includes cx.checkpoint() yield points, INV-CANCEL-PROPAGATES, INV-CANCEL-IDEMPOTENT, and bounded masking (MAX_MASK_DEPTH) for atomic publication sections." --priority 1 --parent bd-3go

# Create new for §4.13
br create --title "§4.13 Obligations (Linear Resources)" --description "Implement obligation tracking (§4.13) to enforce INV-NO-OBLIGATION-LEAKS. Every reserved obligation (SendPermit, TxnSlot lease, etc.) must reach Committed or Aborted state. Includes TrackedSender for safety-critical channels and lab-mode fail-fast logic." --priority 1 --parent bd-3go

# 7. Split bd-3go.12 (Epochs/Remote) -> Update original to just §4.18
br update bd-3go.12 --title "§4.18 Epochs (Validity Windows + Coordination)" --description "Implement EpochClock and epoch coordination (§4.18). Includes SymbolValidityWindow, epoch-scoped symbol auth key derivation (§4.18.2), and the Epoch Transition Barrier (§4.18.4) for quiescent configuration changes."

# Create new for §4.19
br create --title "§4.19 Remote Effects (Named Computations + Sagas)" --description "Implement the remote effects contract (§4.19). Includes RemoteCap requirement, Named Computations (no closures), Lease-backed liveness, IdempotencyKey deduction, and the Saga discipline for multi-step remote workflows." --priority 2 --parent bd-3go

echo "Bead refactoring complete."