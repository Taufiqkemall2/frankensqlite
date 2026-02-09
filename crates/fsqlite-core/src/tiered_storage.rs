//! Tiered storage controls for Native mode (§3.5.11, `bd-1hi.29`).
//!
//! This module models a three-tier object lifecycle:
//! - L1: in-memory decoded bytes (returned by `fetch_object`)
//! - L2: local append-only symbol segments (`l2_segments`)
//! - L3: remote symbol store (`RemoteTier`)
//!
//! The implementation focuses on the normative safety rails:
//! - remote I/O requires a `RemoteCap` token
//! - `durability=local` performs no remote writes on commit
//! - `durability=quorum(M/N)` requires remote ACK quorum before success
//! - segment eviction is cancel-safe and precondition-checked
//! - fetch path prefers systematic symbols then falls back to decode

use std::collections::{BTreeMap, BTreeSet};

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::cx::{Cx, cap};
use fsqlite_types::{
    IdempotencyKey, ObjectId, Oti, RemoteCap, Saga, SymbolReadPath, SymbolRecord,
    SystematicLayoutError, recover_object_with_fallback, source_symbol_count,
};
use tracing::{debug, info, warn};

const BEAD_ID: &str = "bd-1hi.29";
const FETCH_SYMBOLS_COMPUTATION: &str = "fsqlite:tiered:fetch_symbols:v1";
const UPLOAD_SEGMENT_COMPUTATION: &str = "fsqlite:tiered:upload_segment:v1";
const DEFAULT_WRITE_BACK_SEGMENT_ID: u64 = u64::MAX - 1;

/// Native-mode durability policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// `PRAGMA durability = local`
    Local,
    /// `PRAGMA durability = quorum(M/N)`
    Quorum { required: u32, total: u32 },
}

impl DurabilityMode {
    /// Local-only durability policy.
    #[must_use]
    pub const fn local() -> Self {
        Self::Local
    }

    /// Construct a quorum policy.
    pub fn quorum(required: u32, total: u32) -> Result<Self> {
        if required == 0 || required > total {
            return Err(FrankenError::OutOfRange {
                what: "durability quorum".to_owned(),
                value: format!("required={required}, total={total}"),
            });
        }
        Ok(Self::Quorum { required, total })
    }

    #[must_use]
    pub const fn requires_remote(self) -> bool {
        matches!(self, Self::Quorum { .. })
    }

    #[must_use]
    pub const fn quorum_satisfied(self, acked_stores: u32) -> bool {
        match self {
            Self::Local => true,
            Self::Quorum { required, .. } => acked_stores >= required,
        }
    }
}

/// Request for a remote fetch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchSymbolsRequest {
    pub object_id: ObjectId,
    pub preferred_esis: Vec<u32>,
    pub max_symbols: usize,
    pub idempotency_key: IdempotencyKey,
    pub ecs_epoch: u64,
    pub remote_cap: RemoteCap,
    pub computation: &'static str,
}

/// Request for a remote upload operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSegmentRequest {
    pub segment_id: u64,
    pub records: Vec<SymbolRecord>,
    pub idempotency_key: IdempotencyKey,
    pub saga: Saga,
    pub ecs_epoch: u64,
    pub remote_cap: RemoteCap,
    pub computation: &'static str,
}

/// Remote upload result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadSegmentReceipt {
    pub acked_stores: u32,
    pub deduplicated: bool,
}

/// Minimal remote tier contract used by tiered storage control logic.
pub trait RemoteTier {
    /// Fetch symbols for one object.
    fn fetch_symbols(&mut self, request: &FetchSymbolsRequest) -> Result<Vec<SymbolRecord>>;

    /// Upload one rotated segment.
    fn upload_segment(&mut self, request: &UploadSegmentRequest) -> Result<UploadSegmentReceipt>;

    /// Check whether every object in a segment is remotely recoverable.
    fn segment_recoverable(&self, segment_id: u64, min_symbols_per_object: usize) -> bool;
}

/// Commit request for one L2 segment rotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub segment_id: u64,
    pub records: Vec<SymbolRecord>,
    pub idempotency_key: IdempotencyKey,
    pub saga: Saga,
    pub ecs_epoch: u64,
}

impl CommitRequest {
    /// Build a deterministic commit request from segment + symbol records.
    #[must_use]
    pub fn new(segment_id: u64, records: Vec<SymbolRecord>, ecs_epoch: u64) -> Self {
        let mut request_bytes = Vec::with_capacity(24);
        request_bytes.extend_from_slice(&segment_id.to_le_bytes());
        request_bytes.extend_from_slice(
            &u64::try_from(records.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        let idempotency_key = IdempotencyKey::derive(ecs_epoch, &request_bytes);
        let saga = Saga::new(idempotency_key);
        Self {
            segment_id,
            records,
            idempotency_key,
            saga,
            ecs_epoch,
        }
    }
}

/// Commit result summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitOutcome {
    pub remote_io: bool,
    pub upload_receipt: Option<UploadSegmentReceipt>,
}

/// Fetch result summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOutcome {
    pub bytes: Vec<u8>,
    pub read_path: SymbolReadPath,
    pub remote_used: bool,
    pub write_back_count: usize,
}

/// Eviction saga phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPhase {
    Uploaded,
    CompensatedCancelled,
    CompensatedPrecondition,
    Retired,
}

/// Segment-eviction result summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvictionOutcome {
    pub phase: EvictionPhase,
    pub evicted: bool,
    pub local_retained: bool,
    pub upload_receipt: UploadSegmentReceipt,
}

/// Tiered storage control plane state.
#[derive(Debug)]
pub struct TieredStorage {
    durability_mode: DurabilityMode,
    write_back_segment_id: u64,
    l2_segments: BTreeMap<u64, Vec<SymbolRecord>>,
}

impl Default for TieredStorage {
    fn default() -> Self {
        Self::new(DurabilityMode::Local)
    }
}

impl TieredStorage {
    /// Create a tiered-storage controller.
    #[must_use]
    pub fn new(durability_mode: DurabilityMode) -> Self {
        Self {
            durability_mode,
            write_back_segment_id: DEFAULT_WRITE_BACK_SEGMENT_ID,
            l2_segments: BTreeMap::new(),
        }
    }

    /// Current durability mode.
    #[must_use]
    pub const fn durability_mode(&self) -> DurabilityMode {
        self.durability_mode
    }

    /// Update durability mode.
    pub fn set_durability_mode(&mut self, mode: DurabilityMode) {
        self.durability_mode = mode;
    }

    /// Segment id used for self-healing write-back.
    #[must_use]
    pub const fn write_back_segment_id(&self) -> u64 {
        self.write_back_segment_id
    }

    /// Insert or replace one L2 segment.
    pub fn insert_l2_segment(&mut self, segment_id: u64, records: Vec<SymbolRecord>) {
        self.l2_segments.insert(segment_id, records);
    }

    /// Number of L2 segments currently retained.
    #[must_use]
    pub fn l2_segment_count(&self) -> usize {
        self.l2_segments.len()
    }

    /// Whether the L2 segment exists.
    #[must_use]
    pub fn l2_segment_exists(&self, segment_id: u64) -> bool {
        self.l2_segments.contains_key(&segment_id)
    }

    /// Collect all L2 records for one object, deduplicated by ESI.
    #[must_use]
    pub fn l2_records_for_object(&self, object_id: ObjectId) -> Vec<SymbolRecord> {
        let mut by_esi = BTreeMap::<u32, SymbolRecord>::new();
        for segment in self.l2_segments.values() {
            for record in segment {
                if record.object_id == object_id {
                    by_esi.entry(record.esi).or_insert_with(|| record.clone());
                }
            }
        }
        by_esi.into_values().collect()
    }

    /// Commit one rotated segment under the configured durability policy.
    ///
    /// Local symbols are staged first; remote durability is then enforced when
    /// `durability=quorum`.
    pub fn commit_segment<Caps, R>(
        &mut self,
        cx: &Cx<Caps>,
        request: CommitRequest,
        remote: Option<&mut R>,
        remote_cap: Option<RemoteCap>,
    ) -> Result<CommitOutcome>
    where
        Caps: cap::SubsetOf<cap::All>,
        R: RemoteTier,
    {
        self.insert_l2_segment(request.segment_id, request.records.clone());

        if !self.durability_mode.requires_remote() {
            info!(
                bead_id = BEAD_ID,
                segment_id = request.segment_id,
                mode = "local",
                "commit satisfied by L2 only"
            );
            return Ok(CommitOutcome {
                remote_io: false,
                upload_receipt: None,
            });
        }

        let cap = remote_cap.ok_or(FrankenError::AuthDenied)?;
        let remote_store = remote.ok_or(FrankenError::AuthDenied)?;
        cx.checkpoint().map_err(|_| FrankenError::Busy)?;

        let upload_request = UploadSegmentRequest {
            segment_id: request.segment_id,
            records: request.records,
            idempotency_key: request.idempotency_key,
            saga: request.saga,
            ecs_epoch: request.ecs_epoch,
            remote_cap: cap,
            computation: UPLOAD_SEGMENT_COMPUTATION,
        };
        let receipt = remote_store.upload_segment(&upload_request)?;
        if !self.durability_mode.quorum_satisfied(receipt.acked_stores) {
            warn!(
                bead_id = BEAD_ID,
                segment_id = request.segment_id,
                acked_stores = receipt.acked_stores,
                "quorum durability not yet satisfied"
            );
            return Err(FrankenError::Busy);
        }

        Ok(CommitOutcome {
            remote_io: true,
            upload_receipt: Some(receipt),
        })
    }

    /// Fetch one object through tiered storage (L2 fast path, then L3 fallback).
    pub fn fetch_object<Caps, R>(
        &mut self,
        cx: &Cx<Caps>,
        object_id: ObjectId,
        ecs_epoch: u64,
        remote: Option<&mut R>,
        remote_cap: Option<RemoteCap>,
    ) -> Result<FetchOutcome>
    where
        Caps: cap::SubsetOf<cap::All>,
        R: RemoteTier,
    {
        let local_records = self.l2_records_for_object(object_id);
        if !local_records.is_empty() {
            if let Ok((bytes, read_path)) =
                recover_object_with_fallback(&local_records, fallback_decode_records)
            {
                return Ok(FetchOutcome {
                    bytes,
                    read_path,
                    remote_used: false,
                    write_back_count: 0,
                });
            }
        }

        let cap = remote_cap.ok_or(FrankenError::AuthDenied)?;
        let remote_store = remote.ok_or(FrankenError::AuthDenied)?;
        cx.checkpoint().map_err(|_| FrankenError::Busy)?;

        let preferred_esis = preferred_source_esis(local_records.first().map(|record| record.oti));
        let idempotency_key = derive_fetch_key(object_id, &preferred_esis, ecs_epoch);
        let fetch_request = FetchSymbolsRequest {
            object_id,
            preferred_esis,
            max_symbols: usize::MAX,
            idempotency_key,
            ecs_epoch,
            remote_cap: cap,
            computation: FETCH_SYMBOLS_COMPUTATION,
        };
        let fetched = remote_store.fetch_symbols(&fetch_request)?;
        if fetched.is_empty() {
            return Err(FrankenError::Internal(format!(
                "remote tier returned no symbols for object {object_id}"
            )));
        }

        let merged = merge_symbol_sets(&local_records, &fetched);
        let (bytes, read_path) = recover_object_with_fallback(&merged, fallback_decode_records)
            .map_err(|error| FrankenError::DatabaseCorrupt {
                detail: format!("unable to recover object {object_id}: {error}"),
            })?;
        let write_back_count = self.write_back_missing(&local_records, &fetched);

        Ok(FetchOutcome {
            bytes,
            read_path,
            remote_used: true,
            write_back_count,
        })
    }

    /// Evict one rotated segment from L2 to L3 using a cancel-safe saga.
    ///
    /// The local segment is removed only when:
    /// 1. remote upload succeeds, and
    /// 2. cancellation is not requested, and
    /// 3. remote recoverability preconditions are met.
    pub fn evict_segment<Caps, R>(
        &mut self,
        cx: &Cx<Caps>,
        segment_id: u64,
        min_symbols_per_object: usize,
        ecs_epoch: u64,
        remote: &mut R,
        remote_cap: Option<RemoteCap>,
    ) -> Result<EvictionOutcome>
    where
        Caps: cap::SubsetOf<cap::All>,
        R: RemoteTier,
    {
        let cap = remote_cap.ok_or(FrankenError::AuthDenied)?;
        let records =
            self.l2_segments.get(&segment_id).cloned().ok_or_else(|| {
                FrankenError::Internal(format!("unknown L2 segment {segment_id}"))
            })?;

        let key = derive_evict_key(segment_id, ecs_epoch);
        let upload_request = UploadSegmentRequest {
            segment_id,
            records,
            idempotency_key: key,
            saga: Saga::new(key),
            ecs_epoch,
            remote_cap: cap,
            computation: UPLOAD_SEGMENT_COMPUTATION,
        };
        let receipt = remote.upload_segment(&upload_request)?;
        debug!(
            bead_id = BEAD_ID,
            segment_id,
            acked_stores = receipt.acked_stores,
            "segment uploaded to L3"
        );

        if cx.is_cancel_requested() || cx.checkpoint().is_err() {
            warn!(
                bead_id = BEAD_ID,
                segment_id, "eviction cancelled; retaining local segment"
            );
            return Ok(EvictionOutcome {
                phase: EvictionPhase::CompensatedCancelled,
                evicted: false,
                local_retained: true,
                upload_receipt: receipt,
            });
        }

        if !remote.segment_recoverable(segment_id, min_symbols_per_object) {
            warn!(
                bead_id = BEAD_ID,
                segment_id,
                min_symbols_per_object,
                "eviction precondition failed; retaining local segment"
            );
            return Ok(EvictionOutcome {
                phase: EvictionPhase::CompensatedPrecondition,
                evicted: false,
                local_retained: true,
                upload_receipt: receipt,
            });
        }

        let _removed = self.l2_segments.remove(&segment_id);
        info!(bead_id = BEAD_ID, segment_id, "segment evicted from L2");
        Ok(EvictionOutcome {
            phase: EvictionPhase::Retired,
            evicted: true,
            local_retained: false,
            upload_receipt: receipt,
        })
    }

    fn write_back_missing(&mut self, local: &[SymbolRecord], fetched: &[SymbolRecord]) -> usize {
        let known_esi: BTreeSet<u32> = local.iter().map(|record| record.esi).collect();
        let mut missing_by_esi = BTreeMap::<u32, SymbolRecord>::new();
        for record in fetched {
            if !known_esi.contains(&record.esi) {
                missing_by_esi
                    .entry(record.esi)
                    .or_insert_with(|| record.clone());
            }
        }
        let missing: Vec<SymbolRecord> = missing_by_esi.into_values().collect();
        if missing.is_empty() {
            return 0;
        }
        let added = missing.len();
        let segment = self
            .l2_segments
            .entry(self.write_back_segment_id)
            .or_default();
        segment.extend(missing);
        segment.sort_by_key(|record| record.esi);
        segment.dedup_by_key(|record| record.esi);
        added
    }
}

fn preferred_source_esis(oti: Option<Oti>) -> Vec<u32> {
    let Some(oti) = oti else {
        return Vec::new();
    };
    let Ok(source_symbols) = source_symbol_count(oti) else {
        return Vec::new();
    };
    let max_u32 = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
    let capped = source_symbols.min(max_u32);
    let mut esis = Vec::with_capacity(capped);
    for idx in 0..capped {
        if let Ok(esi) = u32::try_from(idx) {
            esis.push(esi);
        }
    }
    esis
}

fn derive_fetch_key(object_id: ObjectId, preferred_esis: &[u32], ecs_epoch: u64) -> IdempotencyKey {
    let mut bytes = Vec::with_capacity(16 + preferred_esis.len() * 4);
    bytes.extend_from_slice(object_id.as_bytes());
    for esi in preferred_esis {
        bytes.extend_from_slice(&esi.to_le_bytes());
    }
    IdempotencyKey::derive(ecs_epoch, &bytes)
}

fn derive_evict_key(segment_id: u64, ecs_epoch: u64) -> IdempotencyKey {
    IdempotencyKey::derive(ecs_epoch, &segment_id.to_le_bytes())
}

fn merge_symbol_sets(local: &[SymbolRecord], fetched: &[SymbolRecord]) -> Vec<SymbolRecord> {
    let mut by_esi = BTreeMap::<u32, SymbolRecord>::new();
    for record in local {
        by_esi.entry(record.esi).or_insert_with(|| record.clone());
    }
    for record in fetched {
        by_esi.entry(record.esi).or_insert_with(|| record.clone());
    }
    by_esi.into_values().collect()
}

fn fallback_decode_records(
    records: &[SymbolRecord],
) -> std::result::Result<Vec<u8>, SystematicLayoutError> {
    let first = records
        .first()
        .ok_or(SystematicLayoutError::EmptySymbolSet)?;
    let source_symbols = source_symbol_count(first.oti)?;
    if records.len() < source_symbols {
        return Err(SystematicLayoutError::MissingSystematicSymbol { expected_esi: 0 });
    }

    let symbol_size =
        usize::try_from(first.oti.t).map_err(|_| SystematicLayoutError::ZeroSymbolSize)?;
    let transfer_len = usize::try_from(first.oti.f).map_err(|_| {
        SystematicLayoutError::TransferLengthTooLarge {
            transfer_length: first.oti.f,
        }
    })?;

    let mut sorted = records.to_vec();
    sorted.sort_by_key(|record| record.esi);
    let mut out = Vec::with_capacity(source_symbols.saturating_mul(symbol_size));
    for record in sorted.iter().take(source_symbols) {
        out.extend_from_slice(&record.symbol_data);
    }
    out.truncate(transfer_len);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use fsqlite_types::cx::{Cx, cap};
    use fsqlite_types::{ObjectId, Oti, SymbolRecordFlags};

    use super::*;

    #[derive(Debug, Default)]
    struct MockRemoteTier {
        object_symbols: HashMap<ObjectId, Vec<SymbolRecord>>,
        segment_symbols: HashMap<u64, Vec<SymbolRecord>>,
        upload_receipts: HashMap<(u64, IdempotencyKey), UploadSegmentReceipt>,
        segment_recoverability_overrides: HashMap<u64, bool>,
        upload_calls: usize,
        fetch_calls: usize,
        configured_acks: u32,
        cancel_after_upload: Option<Cx<cap::All>>,
        last_fetch_preferred: Vec<u32>,
    }

    impl MockRemoteTier {
        fn set_object_symbols(&mut self, object_id: ObjectId, records: Vec<SymbolRecord>) {
            self.object_symbols.insert(object_id, records);
        }

        fn set_acked_stores(&mut self, acked_stores: u32) {
            self.configured_acks = acked_stores;
        }

        fn set_segment_recoverable(&mut self, segment_id: u64, recoverable: bool) {
            self.segment_recoverability_overrides
                .insert(segment_id, recoverable);
        }

        fn set_cancel_after_upload(&mut self, cx: Cx<cap::All>) {
            self.cancel_after_upload = Some(cx);
        }

        fn upload_calls(&self) -> usize {
            self.upload_calls
        }

        fn fetch_calls(&self) -> usize {
            self.fetch_calls
        }
    }

    impl RemoteTier for MockRemoteTier {
        fn fetch_symbols(&mut self, request: &FetchSymbolsRequest) -> Result<Vec<SymbolRecord>> {
            self.fetch_calls = self.fetch_calls.saturating_add(1);
            self.last_fetch_preferred = request.preferred_esis.clone();
            let Some(records) = self.object_symbols.get(&request.object_id) else {
                return Ok(Vec::new());
            };

            let preferred: BTreeSet<u32> = request.preferred_esis.iter().copied().collect();
            let mut ordered = records.clone();
            ordered.sort_by_key(|record| (!preferred.contains(&record.esi), record.esi));
            ordered.truncate(request.max_symbols);
            Ok(ordered)
        }

        fn upload_segment(
            &mut self,
            request: &UploadSegmentRequest,
        ) -> Result<UploadSegmentReceipt> {
            let key = (request.segment_id, request.idempotency_key);
            if let Some(existing) = self.upload_receipts.get(&key).copied() {
                return Ok(UploadSegmentReceipt {
                    deduplicated: true,
                    ..existing
                });
            }

            self.upload_calls = self.upload_calls.saturating_add(1);
            self.segment_symbols
                .insert(request.segment_id, request.records.clone());

            for record in &request.records {
                let entry = self.object_symbols.entry(record.object_id).or_default();
                if entry.iter().all(|existing| existing.esi != record.esi) {
                    entry.push(record.clone());
                }
                entry.sort_by_key(|existing| existing.esi);
            }

            let receipt = UploadSegmentReceipt {
                acked_stores: self.configured_acks,
                deduplicated: false,
            };
            self.upload_receipts.insert(key, receipt);

            if let Some(cx) = self.cancel_after_upload.take() {
                cx.cancel();
            }

            Ok(receipt)
        }

        fn segment_recoverable(&self, segment_id: u64, min_symbols_per_object: usize) -> bool {
            if let Some(override_value) = self.segment_recoverability_overrides.get(&segment_id) {
                return *override_value;
            }
            let Some(records) = self.segment_symbols.get(&segment_id) else {
                return false;
            };
            let mut per_object = HashMap::<ObjectId, usize>::new();
            for record in records {
                let entry = per_object.entry(record.object_id).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            per_object
                .values()
                .all(|count| *count >= min_symbols_per_object)
        }
    }

    fn object_id_from_u64(raw: u64) -> ObjectId {
        let mut bytes = [0_u8; 16];
        bytes[0..8].copy_from_slice(&raw.to_le_bytes());
        bytes[8..16].copy_from_slice(&raw.to_le_bytes());
        ObjectId::from_bytes(bytes)
    }

    fn remote_cap(seed: u8) -> RemoteCap {
        RemoteCap::from_bytes([seed; 16])
    }

    fn make_symbol_records(
        object_id: ObjectId,
        payload: &[u8],
        symbol_size: usize,
        repair_symbols: usize,
    ) -> Vec<SymbolRecord> {
        let symbol_size_u32 = u32::try_from(symbol_size).expect("symbol_size fits u32");
        let transfer_len_u64 = u64::try_from(payload.len()).expect("payload len fits u64");
        let oti = Oti {
            f: transfer_len_u64,
            al: 1,
            t: symbol_size_u32,
            z: 1,
            n: 1,
        };

        let source_symbols = payload.len().div_ceil(symbol_size);
        let mut out = Vec::new();
        for idx in 0..source_symbols {
            let start = idx * symbol_size;
            let end = (start + symbol_size).min(payload.len());
            let mut symbol = vec![0_u8; symbol_size];
            symbol[..end - start].copy_from_slice(&payload[start..end]);
            let esi = u32::try_from(idx).expect("source esi fits u32");
            let flags = if idx == 0 {
                SymbolRecordFlags::SYSTEMATIC_RUN_START
            } else {
                SymbolRecordFlags::empty()
            };
            out.push(SymbolRecord::new(object_id, oti, esi, symbol, flags));
        }

        for repair_idx in 0..repair_symbols {
            let repair_esi_usize = source_symbols.saturating_add(repair_idx);
            let esi = u32::try_from(repair_esi_usize).expect("repair esi fits u32");
            let mut symbol = vec![0_u8; symbol_size];
            let esi_low = u8::try_from(esi & 0xFF).expect("masked to u8");
            for (offset, byte) in symbol.iter_mut().enumerate() {
                let offset_low = u8::try_from(offset & 0xFF).expect("masked to u8");
                *byte = esi_low ^ offset_low;
            }
            out.push(SymbolRecord::new(
                object_id,
                oti,
                esi,
                symbol,
                SymbolRecordFlags::empty(),
            ));
        }

        out
    }

    #[test]
    fn test_l3_fetch_requires_remote_cap() {
        let object_id = object_id_from_u64(1);
        let payload = b"tiered-fetch-requires-cap";

        let mut local = make_symbol_records(object_id, payload, 8, 0);
        local.retain(|record| record.esi != 1);

        let mut storage = TieredStorage::new(DurabilityMode::local());
        storage.insert_l2_segment(1, local);

        let mut remote = MockRemoteTier::default();
        remote.set_object_symbols(object_id, make_symbol_records(object_id, payload, 8, 1));

        let cx = Cx::<cap::All>::new();
        let result = storage.fetch_object(&cx, object_id, 7, Some(&mut remote), None);
        assert!(matches!(result, Err(FrankenError::AuthDenied)));
        assert_eq!(remote.fetch_calls(), 0);
    }

    #[test]
    fn test_l3_upload_idempotency_key() {
        let object_id = object_id_from_u64(2);
        let payload = b"idempotent-upload";
        let records = make_symbol_records(object_id, payload, 8, 1);

        let mut storage = TieredStorage::new(DurabilityMode::quorum(1, 3).expect("valid quorum"));
        let mut remote = MockRemoteTier::default();
        remote.set_acked_stores(2);
        let cx = Cx::<cap::All>::new();
        let cap = Some(remote_cap(9));

        let request = CommitRequest::new(10, records, 11);
        let first = storage
            .commit_segment(&cx, request.clone(), Some(&mut remote), cap)
            .expect("first upload succeeds");
        let second = storage
            .commit_segment(&cx, request, Some(&mut remote), cap)
            .expect("second upload returns idempotent result");

        assert_eq!(remote.upload_calls(), 1);
        let first_receipt = first
            .upload_receipt
            .expect("first commit has upload receipt");
        let second_receipt = second
            .upload_receipt
            .expect("second commit has upload receipt");
        assert!(!first_receipt.deduplicated);
        assert!(second_receipt.deduplicated);
    }

    #[test]
    fn test_eviction_cancel_safety() {
        let object_id = object_id_from_u64(3);
        let payload = b"eviction-cancel-safety";
        let records = make_symbol_records(object_id, payload, 8, 1);

        let mut storage = TieredStorage::new(DurabilityMode::local());
        storage.insert_l2_segment(20, records);

        let cx = Cx::<cap::All>::new();
        let mut remote = MockRemoteTier::default();
        remote.set_acked_stores(3);
        remote.set_cancel_after_upload(cx.clone());

        let outcome = storage
            .evict_segment(&cx, 20, 1, 50, &mut remote, Some(remote_cap(7)))
            .expect("eviction call succeeds");

        assert_eq!(outcome.phase, EvictionPhase::CompensatedCancelled);
        assert!(!outcome.evicted);
        assert!(outcome.local_retained);
        assert!(storage.l2_segment_exists(20));
    }

    #[test]
    fn test_eviction_precondition_check() {
        let object_id = object_id_from_u64(4);
        let payload = b"eviction-precondition-check";
        let records = make_symbol_records(object_id, payload, 8, 1);

        let mut storage = TieredStorage::new(DurabilityMode::local());
        storage.insert_l2_segment(30, records);

        let cx = Cx::<cap::All>::new();
        let mut remote = MockRemoteTier::default();
        remote.set_acked_stores(3);
        remote.set_segment_recoverable(30, false);

        let outcome = storage
            .evict_segment(&cx, 30, 2, 51, &mut remote, Some(remote_cap(8)))
            .expect("eviction call succeeds");

        assert_eq!(outcome.phase, EvictionPhase::CompensatedPrecondition);
        assert!(!outcome.evicted);
        assert!(outcome.local_retained);
        assert!(storage.l2_segment_exists(30));
    }

    #[test]
    fn test_fetch_on_demand_systematic_fast_path() {
        let object_id = object_id_from_u64(5);
        let payload = b"systematic-fast-path";
        let records = make_symbol_records(object_id, payload, 8, 1);

        let mut storage = TieredStorage::new(DurabilityMode::local());
        storage.insert_l2_segment(40, records);

        let cx = Cx::<cap::All>::new();
        let outcome = storage
            .fetch_object(
                &cx,
                object_id,
                52,
                Option::<&mut MockRemoteTier>::None,
                None,
            )
            .expect("local fast-path fetch succeeds");

        assert_eq!(outcome.bytes, payload);
        assert!(matches!(
            outcome.read_path,
            SymbolReadPath::SystematicFastPath
        ));
        assert!(!outcome.remote_used);
        assert_eq!(outcome.write_back_count, 0);
    }

    #[test]
    fn test_fetch_on_demand_repair_fallback() {
        let object_id = object_id_from_u64(6);
        let payload = b"repair-fallback-path";
        let full = make_symbol_records(object_id, payload, 8, 3);

        let mut local_partial = full.clone();
        local_partial.retain(|record| record.esi == 0 || record.esi == 2);

        let mut remote_repairs = full;
        remote_repairs.retain(|record| record.esi >= 3);

        let mut storage = TieredStorage::new(DurabilityMode::local());
        storage.insert_l2_segment(41, local_partial);

        let mut remote = MockRemoteTier::default();
        remote.set_object_symbols(object_id, remote_repairs);
        let cx = Cx::<cap::All>::new();

        let outcome = storage
            .fetch_object(&cx, object_id, 53, Some(&mut remote), Some(remote_cap(5)))
            .expect("fallback fetch succeeds");

        assert!(matches!(
            outcome.read_path,
            SymbolReadPath::FullDecodeFallback { .. }
        ));
        assert!(outcome.remote_used);
        assert!(outcome.write_back_count > 0);
        assert_eq!(remote.last_fetch_preferred, vec![0, 1, 2]);
        assert!(storage.l2_segment_exists(storage.write_back_segment_id()));
    }

    #[test]
    fn test_durability_mode_local_no_remote() {
        let object_id = object_id_from_u64(7);
        let payload = b"local-durability-no-remote";
        let records = make_symbol_records(object_id, payload, 8, 1);

        let mut storage = TieredStorage::new(DurabilityMode::local());
        let mut remote = MockRemoteTier::default();
        remote.set_acked_stores(3);
        let cx = Cx::<cap::All>::new();
        let request = CommitRequest::new(50, records, 60);

        let outcome = storage
            .commit_segment(&cx, request, Some(&mut remote), Some(remote_cap(4)))
            .expect("local durability commit succeeds");

        assert!(!outcome.remote_io);
        assert_eq!(remote.upload_calls(), 0);
        assert!(storage.l2_segment_exists(50));
    }

    #[test]
    fn test_durability_mode_quorum_requires_ack() {
        let object_id = object_id_from_u64(8);
        let payload = b"quorum-durability";
        let records = make_symbol_records(object_id, payload, 8, 1);

        let mut storage = TieredStorage::new(DurabilityMode::quorum(2, 3).expect("valid quorum"));
        let mut remote = MockRemoteTier::default();
        let cx = Cx::<cap::All>::new();
        let cap = Some(remote_cap(10));

        remote.set_acked_stores(1);
        let req_fail = CommitRequest::new(60, records.clone(), 61);
        let fail = storage.commit_segment(&cx, req_fail, Some(&mut remote), cap);
        assert!(matches!(fail, Err(FrankenError::Busy)));
        assert!(storage.l2_segment_exists(60));

        remote.set_acked_stores(2);
        let req_ok = CommitRequest::new(61, records, 62);
        let ok = storage
            .commit_segment(&cx, req_ok, Some(&mut remote), cap)
            .expect("quorum commit succeeds after sufficient ACKs");
        assert!(ok.remote_io);
        assert!(storage.l2_segment_exists(61));
    }

    #[test]
    fn test_e2e_tiered_storage_evict_and_fetch() {
        let mut storage = TieredStorage::new(DurabilityMode::local());
        let mut remote = MockRemoteTier::default();
        remote.set_acked_stores(3);
        let cx = Cx::<cap::All>::new();
        let cap = Some(remote_cap(11));

        let mut expected = HashMap::<ObjectId, Vec<u8>>::new();
        for idx in 0_u64..500_u64 {
            let segment_id = idx + 1;
            let object_id = object_id_from_u64(10_000 + idx);
            let payload = format!("commit-{segment_id:04}-payload").into_bytes();
            let records = make_symbol_records(object_id, &payload, 16, 2);
            storage.insert_l2_segment(segment_id, records);
            expected.insert(object_id, payload);
        }

        for segment_id in 1_u64..=500_u64 {
            let outcome = storage
                .evict_segment(&cx, segment_id, 1, 70, &mut remote, cap)
                .expect("eviction succeeds");
            assert_eq!(outcome.phase, EvictionPhase::Retired);
            assert!(outcome.evicted);
        }
        assert_eq!(storage.l2_segment_count(), 0);

        let target_object = object_id_from_u64(10_321);
        let outcome = storage
            .fetch_object(&cx, target_object, 71, Some(&mut remote), cap)
            .expect("remote fetch after eviction succeeds");
        assert_eq!(
            outcome.bytes,
            expected
                .get(&target_object)
                .expect("target payload available")
                .clone()
        );
        assert!(outcome.remote_used);
        assert!(storage.l2_segment_exists(storage.write_back_segment_id()));
        assert!(outcome.write_back_count > 0);
    }
}
