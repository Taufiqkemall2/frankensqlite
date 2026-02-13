//! Trie-shaped relation storage primitives for Leapfrog-style join execution.
//!
//! This module provides the first `bd-2qr3a.1` prototype:
//! - a cache-friendly arena layout (`Vec<TrieNode>`) with range-based links,
//! - deterministic construction from lexicographically sorted join keys,
//! - a cursor API with `open`, `seek`, `next`, `at_end`, and `open_child`.
//!
//! ## Node Format
//! Each node stores:
//! - `key`: join-key value at this depth,
//! - `depth`: zero-based depth in the key tuple,
//! - `row_range`: contiguous slice of input rows sharing this prefix,
//! - `child_range`: contiguous node range for the next depth.
//!
//! Sibling nodes are contiguous in `TrieRelation::nodes`; no pointer chasing is
//! required for sibling scans or binary seek.
//!
//! ## Memory Layout
//! The layout is an arena of plain values:
//! - `TrieRelation::nodes: Vec<TrieNode>`
//! - `TrieRelation::rows: Vec<TrieRow>`
//! - parent/child and sibling relations are represented by `Range<usize>`
//!   indices into `nodes`.
//!
//! This keeps metadata compact, supports predictable cache behavior, and avoids
//! fragmentation from per-node allocations.
//!
//! ## Prefix Compression
//! The prototype stores full `SqliteValue` keys per node and does not yet apply
//! prefix compression. The range-based layout is compatible with a later
//! dictionary/prefix encoding pass without changing cursor semantics.
//!
//! ## MVCC Iterator Semantics
//! `TrieRelation` is immutable after construction. Cursors keep only indices
//! into the immutable arena and therefore remain stable for the lifetime of the
//! relation snapshot. Under MVCC, relations should be materialized from a single
//! snapshot (or transaction-local batch); cursor invalidation happens only when
//! that snapshot object is dropped.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;

use fsqlite_types::value::SqliteValue;

/// A single sorted input row for trie construction.
#[derive(Debug, Clone, PartialEq)]
pub struct TrieRow {
    /// Join-key tuple for this row.
    pub key: Vec<SqliteValue>,
    /// Stable payload reference (e.g. row ordinal in a materialized batch).
    pub payload_row_index: usize,
}

impl TrieRow {
    /// Build a trie row from key columns and payload row index.
    #[must_use]
    pub fn new(key: Vec<SqliteValue>, payload_row_index: usize) -> Self {
        Self {
            key,
            payload_row_index,
        }
    }
}

/// A trie node stored in the arena.
#[derive(Debug, Clone, PartialEq)]
pub struct TrieNode {
    /// Key value represented by this node.
    pub key: SqliteValue,
    /// Zero-based key depth.
    pub depth: u16,
    /// Input-row range sharing this prefix.
    pub row_range: Range<usize>,
    /// Child-node range (next key depth), if any.
    pub child_range: Option<Range<usize>>,
}

/// Errors that can occur while constructing a trie relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrieBuildError {
    EmptyKey {
        row_index: usize,
    },
    InconsistentArity {
        expected: usize,
        found: usize,
        row_index: usize,
    },
    UnsortedInput {
        previous_row_index: usize,
        row_index: usize,
    },
    NonComparableKey {
        row_index: usize,
        depth: usize,
    },
    ArityTooLarge {
        arity: usize,
    },
}

impl fmt::Display for TrieBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey { row_index } => {
                write!(f, "row {row_index} has an empty join key")
            }
            Self::InconsistentArity {
                expected,
                found,
                row_index,
            } => write!(
                f,
                "row {row_index} has key arity {found}, expected {expected}",
            ),
            Self::UnsortedInput {
                previous_row_index,
                row_index,
            } => write!(
                f,
                "input rows are not sorted at indices {previous_row_index} and {row_index}",
            ),
            Self::NonComparableKey { row_index, depth } => {
                write!(f, "row {row_index} has non-comparable key at depth {depth}",)
            }
            Self::ArityTooLarge { arity } => write!(f, "key arity {arity} exceeds u16::MAX"),
        }
    }
}

impl std::error::Error for TrieBuildError {}

/// Errors that can occur during cursor seek.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrieSeekError {
    NonComparableTarget { node_index: usize, depth: usize },
}

impl fmt::Display for TrieSeekError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonComparableTarget { node_index, depth } => write!(
                f,
                "target key is not comparable with node {node_index} at depth {depth}",
            ),
        }
    }
}

impl std::error::Error for TrieSeekError {}

/// Immutable trie relation for Leapfrog-style join probes.
#[derive(Debug, Clone, PartialEq)]
pub struct TrieRelation {
    arity: usize,
    rows: Vec<TrieRow>,
    nodes: Vec<TrieNode>,
    root_range: Option<Range<usize>>,
}

impl TrieRelation {
    /// Build a trie from lexicographically sorted rows.
    ///
    /// Rows must be sorted by their full key tuple according to SQLite value
    /// ordering (`SqliteValue::partial_cmp`).
    pub fn from_sorted_rows(rows: Vec<TrieRow>) -> Result<Self, TrieBuildError> {
        if rows.is_empty() {
            return Ok(Self {
                arity: 0,
                rows,
                nodes: Vec::new(),
                root_range: None,
            });
        }

        let arity = rows[0].key.len();
        if arity == 0 {
            return Err(TrieBuildError::EmptyKey { row_index: 0 });
        }
        if arity > usize::from(u16::MAX) {
            return Err(TrieBuildError::ArityTooLarge { arity });
        }

        validate_sorted_rows(&rows, arity)?;
        let mut nodes = Vec::new();
        let root_range = build_level(&rows, 0, 0, arity, &mut nodes)?;

        Ok(Self {
            arity,
            rows,
            nodes,
            root_range,
        })
    }

    /// Key arity for this relation.
    #[must_use]
    pub const fn arity(&self) -> usize {
        self.arity
    }

    /// Total input rows represented by this trie.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Total node count in the trie arena.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Root sibling range, if present.
    #[must_use]
    pub fn root_range(&self) -> Option<Range<usize>> {
        self.root_range.clone()
    }

    /// Open a cursor at the first root-level node.
    #[must_use]
    pub fn open_root_cursor(&self) -> TrieCursor<'_> {
        let sibling_range = self.root_range.clone().unwrap_or(0..0);
        TrieCursor::new(self, sibling_range)
    }

    fn node(&self, index: usize) -> Option<&TrieNode> {
        self.nodes.get(index)
    }
}

/// Cursor over contiguous siblings at a trie depth.
#[derive(Debug, Clone)]
pub struct TrieCursor<'a> {
    relation: &'a TrieRelation,
    sibling_range: Range<usize>,
    position: usize,
}

impl<'a> TrieCursor<'a> {
    fn new(relation: &'a TrieRelation, sibling_range: Range<usize>) -> Self {
        let position = sibling_range.start;
        Self {
            relation,
            sibling_range,
            position,
        }
    }

    /// Return true if the cursor has reached the end of the sibling range.
    #[must_use]
    pub fn at_end(&self) -> bool {
        self.position >= self.sibling_range.end
    }

    /// Current key at cursor position.
    #[must_use]
    pub fn current_key(&self) -> Option<&SqliteValue> {
        self.current_node().map(|node| &node.key)
    }

    /// Advance to the next sibling.
    pub fn next(&mut self) {
        if !self.at_end() {
            self.position = self.position.saturating_add(1);
        }
    }

    /// Seek to `target` or the next greater key using galloping + binary search.
    ///
    /// Returns `Ok(true)` when exact key equality is reached.
    /// Returns `Ok(false)` when positioned at the next greater key or end.
    pub fn seek(&mut self, target: &SqliteValue) -> Result<bool, TrieSeekError> {
        if self.at_end() {
            return Ok(false);
        }

        let current_cmp = self.compare_at(self.position, target)?;
        if current_cmp == Ordering::Equal {
            return Ok(true);
        }
        if current_cmp == Ordering::Greater {
            return Ok(false);
        }

        let mut low = self.position;
        let mut high = self.sibling_range.end;

        let mut step = 1usize;
        let mut probe = self.position.saturating_add(step);
        while probe < self.sibling_range.end {
            match self.compare_at(probe, target)? {
                Ordering::Less => {
                    low = probe.saturating_add(1);
                    step = step.saturating_mul(2);
                    probe = probe.saturating_add(step);
                }
                Ordering::Equal => {
                    self.position = probe;
                    return Ok(true);
                }
                Ordering::Greater => {
                    high = probe.saturating_add(1);
                    break;
                }
            }
        }

        self.position = lower_bound(self.relation, low, high, target)?;
        if self.at_end() {
            return Ok(false);
        }
        Ok(self.compare_at(self.position, target)? == Ordering::Equal)
    }

    /// Open a cursor for the child level of the current key, if present.
    #[must_use]
    pub fn open_child(&self) -> Option<Self> {
        let range = self.current_node()?.child_range.clone()?;
        Some(Self::new(self.relation, range))
    }

    fn current_node(&self) -> Option<&TrieNode> {
        if self.at_end() {
            return None;
        }
        self.relation.node(self.position)
    }

    fn compare_at(
        &self,
        node_index: usize,
        target: &SqliteValue,
    ) -> Result<Ordering, TrieSeekError> {
        let node = self
            .relation
            .node(node_index)
            .ok_or(TrieSeekError::NonComparableTarget {
                node_index,
                depth: 0,
            })?;
        node.key
            .partial_cmp(target)
            .ok_or_else(|| TrieSeekError::NonComparableTarget {
                node_index,
                depth: usize::from(node.depth),
            })
    }
}

fn validate_sorted_rows(rows: &[TrieRow], arity: usize) -> Result<(), TrieBuildError> {
    for (row_index, row) in rows.iter().enumerate() {
        let key_len = row.key.len();
        if key_len == 0 {
            return Err(TrieBuildError::EmptyKey { row_index });
        }
        if key_len != arity {
            return Err(TrieBuildError::InconsistentArity {
                expected: arity,
                found: key_len,
                row_index,
            });
        }
        if row_index > 0 {
            let previous = &rows[row_index - 1];
            let ordering = compare_key_slices(&previous.key, &row.key).ok_or(
                TrieBuildError::NonComparableKey {
                    row_index,
                    depth: 0,
                },
            )?;
            if ordering == Ordering::Greater {
                return Err(TrieBuildError::UnsortedInput {
                    previous_row_index: row_index - 1,
                    row_index,
                });
            }
        }
    }
    Ok(())
}

fn compare_key_slices(left: &[SqliteValue], right: &[SqliteValue]) -> Option<Ordering> {
    for (depth, (left_value, right_value)) in left.iter().zip(right).enumerate() {
        let ordering = left_value.partial_cmp(right_value)?;
        if ordering != Ordering::Equal {
            return Some(ordering);
        }
        if depth == left.len().saturating_sub(1) {
            return Some(Ordering::Equal);
        }
    }
    Some(Ordering::Equal)
}

fn build_level(
    rows: &[TrieRow],
    depth: usize,
    base_row_offset: usize,
    arity: usize,
    nodes: &mut Vec<TrieNode>,
) -> Result<Option<Range<usize>>, TrieBuildError> {
    if rows.is_empty() || depth >= arity {
        return Ok(None);
    }

    let mut groups = Vec::new();
    let mut group_start = 0usize;
    while group_start < rows.len() {
        let current_key = rows[group_start]
            .key
            .get(depth)
            .ok_or(TrieBuildError::InconsistentArity {
                expected: arity,
                found: rows[group_start].key.len(),
                row_index: base_row_offset + group_start,
            })?
            .clone();
        let mut group_end = group_start + 1;
        while group_end < rows.len() {
            let next_key =
                rows[group_end]
                    .key
                    .get(depth)
                    .ok_or(TrieBuildError::InconsistentArity {
                        expected: arity,
                        found: rows[group_end].key.len(),
                        row_index: base_row_offset + group_end,
                    })?;
            if *next_key != current_key {
                break;
            }
            group_end = group_end.saturating_add(1);
        }
        groups.push((current_key, group_start, group_end));
        group_start = group_end;
    }

    let range_start = nodes.len();
    let depth_u16 = u16::try_from(depth).map_err(|_| TrieBuildError::ArityTooLarge { arity })?;
    for (key, local_start, local_end) in &groups {
        nodes.push(TrieNode {
            key: key.clone(),
            depth: depth_u16,
            row_range: (base_row_offset + *local_start)..(base_row_offset + *local_end),
            child_range: None,
        });
    }
    let range_end = nodes.len();

    for (offset, (_, local_start, local_end)) in groups.iter().enumerate() {
        let child_rows = &rows[*local_start..*local_end];
        let child_base = base_row_offset + *local_start;
        nodes[range_start + offset].child_range =
            build_level(child_rows, depth + 1, child_base, arity, nodes)?;
    }

    Ok(Some(range_start..range_end))
}

fn lower_bound(
    relation: &TrieRelation,
    mut low: usize,
    mut high: usize,
    target: &SqliteValue,
) -> Result<usize, TrieSeekError> {
    while low < high {
        let mid = low + ((high - low) / 2);
        let node = relation
            .node(mid)
            .ok_or(TrieSeekError::NonComparableTarget {
                node_index: mid,
                depth: 0,
            })?;
        let ordering =
            node.key
                .partial_cmp(target)
                .ok_or_else(|| TrieSeekError::NonComparableTarget {
                    node_index: mid,
                    depth: usize::from(node.depth),
                })?;
        if ordering == Ordering::Less {
            low = mid.saturating_add(1);
        } else {
            high = mid;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use super::{TrieBuildError, TrieRelation, TrieRow};
    use fsqlite_types::value::SqliteValue;

    fn sample_rows() -> Vec<TrieRow> {
        vec![
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(2)], 1),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 2),
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(3)], 3),
        ]
    }

    #[test]
    fn builds_trie_from_sorted_rows() {
        let trie = TrieRelation::from_sorted_rows(sample_rows()).expect("build should succeed");
        assert_eq!(trie.arity(), 2);
        assert_eq!(trie.row_count(), 4);
        assert_eq!(trie.root_range(), Some(0..2));
        assert_eq!(trie.node_count(), 6);
    }

    #[test]
    fn rejects_unsorted_rows() {
        let unsorted = vec![
            TrieRow::new(vec![SqliteValue::Integer(2), SqliteValue::Integer(1)], 0),
            TrieRow::new(vec![SqliteValue::Integer(1), SqliteValue::Integer(1)], 1),
        ];
        let err = TrieRelation::from_sorted_rows(unsorted).expect_err("must reject unsorted rows");
        assert_eq!(
            err,
            TrieBuildError::UnsortedInput {
                previous_row_index: 0,
                row_index: 1
            }
        );
    }

    #[test]
    fn cursor_seek_and_child_navigation() {
        let trie = TrieRelation::from_sorted_rows(sample_rows()).expect("build should succeed");
        let mut root = trie.open_root_cursor();

        assert!(!root.at_end());
        assert_eq!(root.current_key(), Some(&SqliteValue::Integer(1)));

        let exact = root
            .seek(&SqliteValue::Integer(2))
            .expect("seek should succeed");
        assert!(exact);
        assert_eq!(root.current_key(), Some(&SqliteValue::Integer(2)));

        let mut child = root.open_child().expect("child cursor should exist");
        assert_eq!(child.current_key(), Some(&SqliteValue::Integer(1)));
        child.next();
        assert_eq!(child.current_key(), Some(&SqliteValue::Integer(3)));

        let exact_child = child
            .seek(&SqliteValue::Integer(2))
            .expect("seek should succeed");
        assert!(!exact_child);
        assert_eq!(child.current_key(), Some(&SqliteValue::Integer(3)));
    }
}
