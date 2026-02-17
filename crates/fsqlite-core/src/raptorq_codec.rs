//! Production `SymbolCodec` implementation backed by asupersync RaptorQ (bd-3sj9w).
//!
//! This module lifts the test-only `AsupersyncCodec` from `raptorq_integration`
//! into a public, production-ready codec.  The codec wraps asupersync's
//! `RaptorQSenderBuilder` / `RaptorQReceiverBuilder` behind the
//! `SymbolCodec` trait, translating between FrankenSQLite's packed symbol
//! key format and asupersync's `Symbol` / `SymbolId` types.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use asupersync::error::ErrorKind as AsErrorKind;
use asupersync::raptorq::{RaptorQReceiverBuilder, RaptorQSenderBuilder};
use asupersync::security::AuthenticationTag;
use asupersync::security::authenticated::AuthenticatedSymbol;
use asupersync::transport::error::{SinkError, StreamError};
use asupersync::transport::sink::SymbolSink;
use asupersync::transport::stream::SymbolStream;
use asupersync::types::{ObjectId as AsObjectId, ObjectParams, Symbol, SymbolId, SymbolKind};
use asupersync::{Cx as AsCx, RaptorQConfig};

use fsqlite_error::{FrankenError, Result};

use crate::raptorq_integration::{
    CodecDecodeResult, CodecEncodeResult, DecodeFailureReason, SymbolCodec,
};

const BEAD_ID: &str = "bd-3sj9w";

/// Fixed object ID for production codec operations.  The object ID is not
/// semantically meaningful for FrankenSQLite's page-level FEC — each WAL
/// commit group is a standalone encode/decode unit — so we use a constant
/// derived from the bead lineage.
const PRODUCTION_OBJECT_ID: u64 = 0xF5_3D9A_0001;

// ---------------------------------------------------------------------------
// Packed symbol key format
// ---------------------------------------------------------------------------
//
// 32-bit key layout:
//   [31]     = kind (0 = source, 1 = repair)
//   [30..23] = source block number (SBN, 8 bits)
//   [22..0]  = encoding symbol ID (ESI, 23 bits)

const PACKED_KIND_REPAIR_BIT: u32 = 1_u32 << 31;
const PACKED_SBN_SHIFT: u32 = 23;
const PACKED_SBN_MASK: u32 = 0xFF;
const PACKED_ESI_MASK: u32 = 0x7F_FFFF;

/// Pack a `(kind, sbn, esi)` triple into a 32-bit key.
///
/// Returns an error if `esi` exceeds 23 bits.
pub fn pack_symbol_key(kind: SymbolKind, sbn: u8, esi: u32) -> Result<u32> {
    if esi > PACKED_ESI_MASK {
        return Err(FrankenError::OutOfRange {
            what: "packed symbol esi (must fit 23 bits)".to_owned(),
            value: esi.to_string(),
        });
    }

    let kind_bit = if kind.is_repair() {
        PACKED_KIND_REPAIR_BIT
    } else {
        0
    };
    Ok(kind_bit | (u32::from(sbn) << PACKED_SBN_SHIFT) | esi)
}

/// Unpack a 32-bit key into `(kind, sbn, esi)`.
#[must_use]
pub fn unpack_symbol_key(packed: u32) -> (SymbolKind, u8, u32) {
    let kind = if packed & PACKED_KIND_REPAIR_BIT == 0 {
        SymbolKind::Source
    } else {
        SymbolKind::Repair
    };
    #[allow(clippy::cast_possible_truncation)]
    let sbn = ((packed >> PACKED_SBN_SHIFT) & PACKED_SBN_MASK) as u8;
    let esi = packed & PACKED_ESI_MASK;
    (kind, sbn, esi)
}

// ---------------------------------------------------------------------------
// In-memory transport adapters
// ---------------------------------------------------------------------------

/// In-memory symbol sink that collects symbols into a `Vec`.
#[derive(Debug)]
struct VecTransportSink {
    symbols: Vec<Symbol>,
}

impl VecTransportSink {
    fn new() -> Self {
        Self {
            symbols: Vec::new(),
        }
    }
}

impl SymbolSink for VecTransportSink {
    fn poll_send(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        symbol: AuthenticatedSymbol,
    ) -> Poll<std::result::Result<(), SinkError>> {
        self.symbols.push(symbol.into_symbol());
        Poll::Ready(Ok(()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(Ok(()))
    }
}

/// In-memory symbol stream that drains from a `VecDeque`.
#[derive(Debug)]
struct VecTransportStream {
    symbols: VecDeque<AuthenticatedSymbol>,
}

impl VecTransportStream {
    fn new(symbols: Vec<Symbol>) -> Self {
        let symbols = symbols
            .into_iter()
            .map(|symbol| AuthenticatedSymbol::new_verified(symbol, AuthenticationTag::zero()))
            .collect();
        Self { symbols }
    }
}

impl SymbolStream for VecTransportStream {
    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<AuthenticatedSymbol, StreamError>>> {
        match self.symbols.pop_front() {
            Some(symbol) => Poll::Ready(Some(Ok(symbol))),
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.symbols.len(), Some(self.symbols.len()))
    }

    fn is_exhausted(&self) -> bool {
        self.symbols.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Production SymbolCodec
// ---------------------------------------------------------------------------

/// Production [`SymbolCodec`] backed by asupersync's RaptorQ encoder/decoder.
///
/// Wraps `RaptorQSenderBuilder` for encode and `RaptorQReceiverBuilder` for
/// decode, using in-memory transports.  The codec is stateless and can be
/// shared across threads (`Send + Sync`).
///
/// # Configuration
///
/// - `max_block_size`: Maximum source block size in bytes (default: 64 KiB).
///   This controls how asupersync partitions large objects into source blocks.
///   For page-level FEC where each encode call handles a single commit group
///   (typically a few pages), the default is sufficient.
#[derive(Debug, Clone)]
pub struct AsupersyncCodec {
    /// Maximum source block size in bytes.
    max_block_size: usize,
}

impl AsupersyncCodec {
    /// Create a codec with the given maximum block size.
    #[must_use]
    pub const fn new(max_block_size: usize) -> Self {
        Self { max_block_size }
    }
}

impl Default for AsupersyncCodec {
    fn default() -> Self {
        Self::new(64 * 1024)
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
impl SymbolCodec for AsupersyncCodec {
    fn encode(
        &self,
        source_data: &[u8],
        symbol_size: u32,
        repair_overhead: f64,
    ) -> Result<CodecEncodeResult> {
        let mut config = RaptorQConfig::default();
        config.encoding.symbol_size = symbol_size as u16;
        config.encoding.max_block_size = self.max_block_size;
        config.encoding.repair_overhead = repair_overhead;

        let cx = AsCx::for_testing();
        let object_id = AsObjectId::new_for_test(PRODUCTION_OBJECT_ID);
        let mut sender = RaptorQSenderBuilder::new()
            .config(config)
            .transport(VecTransportSink::new())
            .build()
            .map_err(|e| FrankenError::Internal(format!("{BEAD_ID}: sender build: {e}")))?;

        let outcome = sender
            .send_object(&cx, object_id, source_data)
            .map_err(|e| FrankenError::Internal(format!("{BEAD_ID}: send_object: {e}")))?;

        let symbols = std::mem::take(&mut sender.transport_mut().symbols);
        let k = outcome.source_symbols as u32;

        let mut source_symbols = Vec::new();
        let mut repair_symbols = Vec::new();
        for s in &symbols {
            let packed_key = pack_symbol_key(s.kind(), s.sbn(), s.esi())?;
            if s.kind().is_source() {
                source_symbols.push((packed_key, s.data().to_vec()));
            } else {
                repair_symbols.push((packed_key, s.data().to_vec()));
            }
        }

        Ok(CodecEncodeResult {
            source_symbols,
            repair_symbols,
            k_source: k,
        })
    }

    fn decode(
        &self,
        symbols: &[(u32, Vec<u8>)],
        k_source: u32,
        symbol_size: u32,
    ) -> Result<CodecDecodeResult> {
        if symbols.is_empty() {
            return Ok(CodecDecodeResult::Failure {
                reason: DecodeFailureReason::InsufficientSymbols,
                symbols_received: 0,
                k_required: k_source,
            });
        }

        let object_id = AsObjectId::new_for_test(PRODUCTION_OBJECT_ID);
        let mut config = RaptorQConfig::default();
        config.encoding.symbol_size = symbol_size as u16;
        config.encoding.max_block_size = self.max_block_size;

        let symbol_size_usize =
            usize::try_from(symbol_size).map_err(|_| FrankenError::OutOfRange {
                what: "symbol_size as usize".to_owned(),
                value: symbol_size.to_string(),
            })?;
        let symbols_per_block = u32::try_from((self.max_block_size / symbol_size_usize).max(1))
            .map_err(|_| FrankenError::OutOfRange {
                what: "symbols_per_block as u32".to_owned(),
                value: (self.max_block_size / symbol_size_usize).to_string(),
            })?;
        let source_blocks = k_source.div_ceil(symbols_per_block).max(1);
        let object_size = u64::from(k_source)
            .checked_mul(u64::from(symbol_size))
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "object_size for decode params".to_owned(),
                value: format!("{k_source}*{symbol_size}"),
            })?;
        let params = ObjectParams::new(
            object_id,
            object_size,
            u16::try_from(symbol_size).map_err(|_| FrankenError::OutOfRange {
                what: "symbol_size as u16".to_owned(),
                value: symbol_size.to_string(),
            })?,
            u8::try_from(source_blocks).map_err(|_| FrankenError::OutOfRange {
                what: "source_blocks as u8".to_owned(),
                value: source_blocks.to_string(),
            })?,
            u16::try_from(symbols_per_block).map_err(|_| FrankenError::OutOfRange {
                what: "symbols_per_block as u16".to_owned(),
                value: symbols_per_block.to_string(),
            })?,
        );

        let mut rebuilt = Vec::with_capacity(symbols.len());
        for (packed, data) in symbols {
            let (kind, sbn, esi) = unpack_symbol_key(*packed);
            rebuilt.push(Symbol::new(
                SymbolId::new(object_id, sbn, esi),
                data.clone(),
                kind,
            ));
        }

        let cx = AsCx::for_testing();
        let mut receiver = RaptorQReceiverBuilder::new()
            .config(config)
            .source(VecTransportStream::new(rebuilt))
            .build()
            .map_err(|e| FrankenError::Internal(format!("{BEAD_ID}: receiver build: {e}")))?;

        match receiver.receive_object(&cx, &params) {
            Ok(outcome) => Ok(CodecDecodeResult::Success {
                data: outcome.data,
                symbols_used: outcome.symbols_received as u32,
                peeled_count: 0,
                inactivated_count: 0,
            }),
            Err(err) => {
                let reason = match err.kind() {
                    AsErrorKind::InsufficientSymbols => DecodeFailureReason::InsufficientSymbols,
                    _ => DecodeFailureReason::SingularMatrix,
                };
                Ok(CodecDecodeResult::Failure {
                    reason,
                    symbols_received: symbols.len() as u32,
                    k_required: k_source,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_source_symbol() {
        let packed = pack_symbol_key(SymbolKind::Source, 0, 42).unwrap();
        let (kind, sbn, esi) = unpack_symbol_key(packed);
        assert_eq!(kind, SymbolKind::Source);
        assert_eq!(sbn, 0);
        assert_eq!(esi, 42);
    }

    #[test]
    fn test_pack_unpack_repair_symbol() {
        let packed = pack_symbol_key(SymbolKind::Repair, 3, 100).unwrap();
        let (kind, sbn, esi) = unpack_symbol_key(packed);
        assert_eq!(kind, SymbolKind::Repair);
        assert_eq!(sbn, 3);
        assert_eq!(esi, 100);
    }

    #[test]
    fn test_pack_esi_overflow() {
        let result = pack_symbol_key(SymbolKind::Source, 0, PACKED_ESI_MASK + 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_max_esi() {
        let packed = pack_symbol_key(SymbolKind::Source, 0, PACKED_ESI_MASK).unwrap();
        let (_, _, esi) = unpack_symbol_key(packed);
        assert_eq!(esi, PACKED_ESI_MASK);
    }

    #[test]
    fn test_pack_max_sbn() {
        let packed = pack_symbol_key(SymbolKind::Repair, 255, 0).unwrap();
        let (kind, sbn, esi) = unpack_symbol_key(packed);
        assert_eq!(kind, SymbolKind::Repair);
        assert_eq!(sbn, 255);
        assert_eq!(esi, 0);
    }

    #[test]
    fn test_codec_encode_decode_roundtrip() {
        let codec = AsupersyncCodec::default();
        let data = vec![0xAB_u8; 4096];
        let symbol_size = 512_u32;
        let repair_overhead = 1.25;

        let encoded = codec.encode(&data, symbol_size, repair_overhead).unwrap();
        assert!(encoded.k_source > 0);
        assert!(!encoded.source_symbols.is_empty());
        assert!(!encoded.repair_symbols.is_empty());

        // Decode with all symbols (source + repair).
        let mut all_symbols: Vec<(u32, Vec<u8>)> = encoded.source_symbols.clone();
        all_symbols.extend(encoded.repair_symbols.clone());

        let decoded = codec
            .decode(&all_symbols, encoded.k_source, symbol_size)
            .unwrap();
        match decoded {
            CodecDecodeResult::Success {
                data: recovered, ..
            } => {
                assert_eq!(recovered, data);
            }
            CodecDecodeResult::Failure { reason, .. } => {
                panic!("decode failed: {reason:?}");
            }
        }
    }

    #[test]
    fn test_codec_decode_source_only() {
        let codec = AsupersyncCodec::default();
        let data = vec![0xCD_u8; 2048];
        let symbol_size = 512_u32;

        let encoded = codec.encode(&data, symbol_size, 1.25).unwrap();

        // Decode with source symbols only (no repair needed).
        let decoded = codec
            .decode(&encoded.source_symbols, encoded.k_source, symbol_size)
            .unwrap();
        match decoded {
            CodecDecodeResult::Success {
                data: recovered, ..
            } => {
                assert_eq!(recovered, data);
            }
            CodecDecodeResult::Failure { reason, .. } => {
                panic!("source-only decode failed: {reason:?}");
            }
        }
    }

    #[test]
    fn test_codec_decode_with_erasures() {
        let codec = AsupersyncCodec::default();
        let data = vec![0xEF_u8; 4096];
        let symbol_size = 512_u32;

        let encoded = codec.encode(&data, symbol_size, 1.5).unwrap();
        let k = encoded.k_source as usize;

        // Drop first source symbol, replace with repair symbols.
        let mut symbols: Vec<(u32, Vec<u8>)> = encoded.source_symbols[1..].to_vec();
        symbols.extend(encoded.repair_symbols.iter().take(2).cloned());

        assert!(symbols.len() >= k, "need at least K symbols");

        let decoded = codec
            .decode(&symbols, encoded.k_source, symbol_size)
            .unwrap();
        match decoded {
            CodecDecodeResult::Success {
                data: recovered, ..
            } => {
                assert_eq!(recovered, data);
            }
            CodecDecodeResult::Failure { reason, .. } => {
                panic!("erasure decode failed: {reason:?}");
            }
        }
    }

    #[test]
    fn test_codec_decode_empty() {
        let codec = AsupersyncCodec::default();
        let result = codec.decode(&[], 4, 512).unwrap();
        assert!(matches!(
            result,
            CodecDecodeResult::Failure {
                reason: DecodeFailureReason::InsufficientSymbols,
                ..
            }
        ));
    }

    #[test]
    fn test_codec_default_max_block_size() {
        let codec = AsupersyncCodec::default();
        assert_eq!(codec.max_block_size, 64 * 1024);
    }

    #[test]
    fn test_codec_custom_max_block_size() {
        let codec = AsupersyncCodec::new(128 * 1024);
        assert_eq!(codec.max_block_size, 128 * 1024);

        // Should still encode/decode correctly.
        let data = vec![0x42_u8; 2048];
        let encoded = codec.encode(&data, 512, 1.25).unwrap();
        let decoded = codec
            .decode(&encoded.source_symbols, encoded.k_source, 512)
            .unwrap();
        assert!(matches!(decoded, CodecDecodeResult::Success { .. }));
    }

    #[test]
    fn test_codec_send_sync() {
        // SymbolCodec requires Send + Sync.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AsupersyncCodec>();
    }

    #[test]
    fn test_codec_large_data_4096_page() {
        let codec = AsupersyncCodec::default();
        // 4 pages of 4096 bytes each.
        let data = vec![0x77_u8; 4 * 4096];
        let encoded = codec.encode(&data, 4096, 1.25).unwrap();
        assert!(encoded.k_source >= 4);

        let decoded = codec
            .decode(&encoded.source_symbols, encoded.k_source, 4096)
            .unwrap();
        match decoded {
            CodecDecodeResult::Success {
                data: recovered, ..
            } => {
                assert_eq!(recovered, data);
            }
            CodecDecodeResult::Failure { reason, .. } => {
                panic!("large page decode failed: {reason:?}");
            }
        }
    }

    #[test]
    fn test_codec_repair_symbol_count_scales_with_overhead() {
        let codec = AsupersyncCodec::default();
        let data = vec![0x55_u8; 8192];

        let low = codec.encode(&data, 512, 1.1).unwrap();
        let high = codec.encode(&data, 512, 2.0).unwrap();

        // Higher overhead should produce more repair symbols.
        assert!(
            high.repair_symbols.len() > low.repair_symbols.len(),
            "2.0x overhead ({}) should produce more repairs than 1.1x ({})",
            high.repair_symbols.len(),
            low.repair_symbols.len()
        );
    }

    #[test]
    fn test_pack_all_bits_combined() {
        // Test with all bit fields populated.
        let packed = pack_symbol_key(SymbolKind::Repair, 127, 0x3F_FFFF).unwrap();
        let (kind, sbn, esi) = unpack_symbol_key(packed);
        assert_eq!(kind, SymbolKind::Repair);
        assert_eq!(sbn, 127);
        assert_eq!(esi, 0x3F_FFFF);
    }
}
