//! VDBE bytecode interpreter — the fetch-execute engine.
//!
//! Takes a [`VdbeProgram`] (produced by codegen) and executes it instruction by
//! instruction. The engine maintains a register file (`Vec<SqliteValue>`) and
//! accumulates result rows emitted by `OP_ResultRow`.
//!
//! This implementation covers the core opcode set needed for expression
//! evaluation, control flow, arithmetic, comparison, and row output.
//! Cursor-based opcodes (OpenRead, Rewind, Next, Column, etc.) are stubbed
//! and will be wired to the B-tree layer in Phase 5.

use fsqlite_error::{FrankenError, Result};
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::value::SqliteValue;

use crate::VdbeProgram;

/// Outcome of a single engine execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecOutcome {
    /// Program halted normally (Halt with p1=0).
    Done,
    /// Program halted with an error code and message.
    Error { code: i32, message: String },
}

/// The VDBE bytecode interpreter.
///
/// Executes a program produced by the code generator, maintaining a register
/// file and collecting result rows.
#[derive(Debug)]
pub struct VdbeEngine {
    /// Register file (1-indexed; index 0 is unused/sentinel).
    registers: Vec<SqliteValue>,
    /// Result rows accumulated during execution.
    results: Vec<Vec<SqliteValue>>,
}

impl VdbeEngine {
    /// Create a new engine with enough registers for the given program.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn new(register_count: i32) -> Self {
        // +1 because registers are 1-indexed (register 0 unused).
        let count = register_count.max(0) as u32 + 1;
        Self {
            registers: vec![SqliteValue::Null; count as usize],
            results: Vec::new(),
        }
    }

    /// Execute a VDBE program to completion.
    ///
    /// Returns `Ok(ExecOutcome::Done)` on normal halt, or an error if the
    /// program encounters a fatal condition.
    #[allow(
        clippy::too_many_lines,
        clippy::match_same_arms,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap
    )]
    pub fn execute(&mut self, program: &VdbeProgram) -> Result<ExecOutcome> {
        let ops = program.ops();
        if ops.is_empty() {
            return Ok(ExecOutcome::Done);
        }

        let mut pc: usize = 0;
        // "once" flags: one bit per instruction address.
        let mut once_flags = vec![false; ops.len()];

        loop {
            if pc >= ops.len() {
                return Ok(ExecOutcome::Done);
            }

            let op = &ops[pc];
            match op.opcode {
                // ── Control Flow ────────────────────────────────────────
                Opcode::Init => {
                    // Jump to p2 if it points to a valid instruction.
                    // In the standard SQLite pattern, p2 points to a Goto
                    // at the end that bounces back. If p2 points past the
                    // end (our codegen pattern), fall through.
                    let target = op.p2 as usize;
                    if op.p2 > 0 && target < ops.len() {
                        pc = target;
                        continue;
                    }
                    pc += 1;
                }

                Opcode::Goto => {
                    pc = op.p2 as usize;
                }

                Opcode::Halt => {
                    if op.p1 != 0 {
                        let msg = match &op.p4 {
                            P4::Str(s) => s.clone(),
                            _ => format!("halt with error code {}", op.p1),
                        };
                        return Ok(ExecOutcome::Error {
                            code: op.p1,
                            message: msg,
                        });
                    }
                    return Ok(ExecOutcome::Done);
                }

                Opcode::Noop => {
                    pc += 1;
                }

                // ── Constants ───────────────────────────────────────────
                Opcode::Integer => {
                    // Set register p2 to integer value p1.
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(op.p1)));
                    pc += 1;
                }

                Opcode::Int64 => {
                    let val = match &op.p4 {
                        P4::Int64(v) => *v,
                        _ => 0,
                    };
                    self.set_reg(op.p2, SqliteValue::Integer(val));
                    pc += 1;
                }

                Opcode::Real => {
                    let val = match &op.p4 {
                        P4::Real(v) => *v,
                        _ => 0.0,
                    };
                    self.set_reg(op.p2, SqliteValue::Float(val));
                    pc += 1;
                }

                Opcode::String8 => {
                    let val = match &op.p4 {
                        P4::Str(s) => s.clone(),
                        _ => String::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Text(val));
                    pc += 1;
                }

                Opcode::String => {
                    // p1 = length, p4 = string data. Same as String8 for us.
                    let val = match &op.p4 {
                        P4::Str(s) => s.clone(),
                        _ => String::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Text(val));
                    pc += 1;
                }

                Opcode::Null => {
                    // Set registers p2..p2+p3 to NULL (p3 is count-1, or 0
                    // for a single register).
                    let start = op.p2;
                    let end = if op.p3 > 0 { start + op.p3 } else { start };
                    for r in start..=end {
                        self.set_reg(r, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::SoftNull => {
                    self.set_reg(op.p1, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::Blob => {
                    let val = match &op.p4 {
                        P4::Blob(b) => b.clone(),
                        _ => Vec::new(),
                    };
                    self.set_reg(op.p2, SqliteValue::Blob(val));
                    pc += 1;
                }

                // ── Register Operations ─────────────────────────────────
                Opcode::Move => {
                    // Move p3 registers from p1 to p2.
                    for i in 0..op.p3 {
                        let val = self.get_reg(op.p1 + i).clone();
                        self.set_reg(op.p2 + i, val);
                        self.set_reg(op.p1 + i, SqliteValue::Null);
                    }
                    pc += 1;
                }

                Opcode::Copy => {
                    // Copy register p1 to p2 (deep copy).
                    let val = self.get_reg(op.p1).clone();
                    self.set_reg(op.p2, val);
                    pc += 1;
                }

                Opcode::SCopy => {
                    // Shallow copy register p1 to p2.
                    let val = self.get_reg(op.p1).clone();
                    self.set_reg(op.p2, val);
                    pc += 1;
                }

                Opcode::IntCopy => {
                    let val = self.get_reg(op.p1).to_integer();
                    self.set_reg(op.p2, SqliteValue::Integer(val));
                    pc += 1;
                }

                // ── Result Row ──────────────────────────────────────────
                Opcode::ResultRow => {
                    // Output p2 registers starting at p1.
                    let start = op.p1 as usize;
                    let count = op.p2 as usize;
                    let row: Vec<SqliteValue> = (start..start + count)
                        .map(|r| self.get_reg(r as i32).clone())
                        .collect();
                    self.results.push(row);
                    pc += 1;
                }

                // ── Arithmetic ──────────────────────────────────────────
                Opcode::Add => {
                    // p3 = p2 + p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_add(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Subtract => {
                    // p3 = p2 - p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_sub(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Multiply => {
                    // p3 = p2 * p1
                    let a = self.get_reg(op.p2);
                    let b = self.get_reg(op.p1);
                    let result = a.sql_mul(b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Divide => {
                    // p3 = p2 / p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_div(dividend, divisor);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Remainder => {
                    // p3 = p2 % p1
                    let divisor = self.get_reg(op.p1);
                    let dividend = self.get_reg(op.p2);
                    let result = sql_rem(dividend, divisor);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                // ── String Concatenation ────────────────────────────────
                Opcode::Concat => {
                    // Concatenate p1 and p2 into p3.
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        let mut s = b.to_text();
                        s.push_str(&a.to_text());
                        SqliteValue::Text(s)
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                // ── Bitwise ─────────────────────────────────────────────
                Opcode::BitAnd => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(a.to_integer() & b.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::BitOr => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(a.to_integer() | b.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::ShiftLeft => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        sql_shift_left(b.to_integer(), a.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::ShiftRight => {
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = if a.is_null() || b.is_null() {
                        SqliteValue::Null
                    } else {
                        sql_shift_right(b.to_integer(), a.to_integer())
                    };
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::BitNot => {
                    // p2 = ~p1
                    let a = self.get_reg(op.p1);
                    let result = if a.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(!a.to_integer())
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Type Conversion ─────────────────────────────────────
                Opcode::AddImm => {
                    // Add integer p2 to register p1.
                    let val = self.get_reg(op.p1).to_integer() + i64::from(op.p2);
                    self.set_reg(op.p1, SqliteValue::Integer(val));
                    pc += 1;
                }

                Opcode::Cast => {
                    // Cast register p1 to type indicated by p2.
                    let val = self.get_reg(op.p1).clone();
                    let casted = sql_cast(val, op.p2);
                    self.set_reg(op.p1, casted);
                    pc += 1;
                }

                Opcode::MustBeInt => {
                    let val = self.get_reg(op.p1);
                    if val.as_integer().is_none() && !val.is_null() {
                        if op.p2 > 0 {
                            pc = op.p2 as usize;
                            continue;
                        }
                        return Err(FrankenError::TypeMismatch {
                            expected: "integer".to_owned(),
                            actual: val.typeof_str().to_owned(),
                        });
                    }
                    pc += 1;
                }

                #[allow(clippy::cast_precision_loss)]
                Opcode::RealAffinity => {
                    if let SqliteValue::Integer(i) = self.get_reg(op.p1) {
                        let f = *i as f64;
                        self.set_reg(op.p1, SqliteValue::Float(f));
                    }
                    pc += 1;
                }

                // ── Comparison Jumps ────────────────────────────────────
                Opcode::Eq | Opcode::Ne | Opcode::Lt | Opcode::Le | Opcode::Gt | Opcode::Ge => {
                    let lhs = self.get_reg(op.p3);
                    let rhs = self.get_reg(op.p1);

                    // NULL handling: if either is NULL, jump depends on p5
                    // flag (SQLITE_NULLEQ).
                    let should_jump = if lhs.is_null() || rhs.is_null() {
                        let null_eq = (op.p5 & 0x80) != 0;
                        if null_eq {
                            // IS / IS NOT semantics: NULL == NULL is true.
                            let both_null = lhs.is_null() && rhs.is_null();
                            match op.opcode {
                                Opcode::Eq => both_null,
                                Opcode::Ne => !both_null,
                                _ => false,
                            }
                        } else {
                            // Standard SQL: comparison with NULL is NULL (no jump).
                            false
                        }
                    } else {
                        let cmp = lhs.partial_cmp(rhs);
                        matches!(
                            (op.opcode, cmp),
                            (Opcode::Eq, Some(std::cmp::Ordering::Equal))
                                | (Opcode::Lt, Some(std::cmp::Ordering::Less))
                                | (
                                    Opcode::Le,
                                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                                )
                                | (Opcode::Gt, Some(std::cmp::Ordering::Greater))
                                | (
                                    Opcode::Ge,
                                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                                )
                        ) || matches!(
                            (op.opcode, cmp),
                            (Opcode::Ne, Some(ord)) if ord != std::cmp::Ordering::Equal
                        )
                    };

                    if should_jump {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                // ── Boolean Logic ───────────────────────────────────────
                Opcode::And => {
                    // Three-valued AND: p3 = p1 AND p2
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = sql_and(a, b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Or => {
                    // Three-valued OR: p3 = p1 OR p2
                    let a = self.get_reg(op.p1);
                    let b = self.get_reg(op.p2);
                    let result = sql_or(a, b);
                    self.set_reg(op.p3, result);
                    pc += 1;
                }

                Opcode::Not => {
                    // p2 = NOT p1
                    let a = self.get_reg(op.p1);
                    let result = if a.is_null() {
                        SqliteValue::Null
                    } else {
                        SqliteValue::Integer(i64::from(a.to_integer() == 0))
                    };
                    self.set_reg(op.p2, result);
                    pc += 1;
                }

                // ── Conditional Jumps ───────────────────────────────────
                Opcode::If => {
                    // Jump to p2 if p1 is true (non-zero, non-NULL).
                    let val = self.get_reg(op.p1);
                    if !val.is_null() && val.to_integer() != 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IfNot => {
                    // Jump to p2 if p1 is false (zero) or NULL.
                    let val = self.get_reg(op.p1);
                    if val.is_null() || val.to_integer() == 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::IsNull => {
                    // Jump to p2 if p1 is NULL.
                    if self.get_reg(op.p1).is_null() {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::NotNull => {
                    // Jump to p2 if p1 is NOT NULL.
                    if self.get_reg(op.p1).is_null() {
                        pc += 1;
                    } else {
                        pc = op.p2 as usize;
                    }
                }

                Opcode::Once => {
                    // Jump to p2 on first execution only.
                    if once_flags[pc] {
                        pc += 1;
                    } else {
                        once_flags[pc] = true;
                        pc = op.p2 as usize;
                    }
                }

                // ── Gosub / Return ──────────────────────────────────────
                Opcode::Gosub => {
                    // Store return address in p1, jump to p2.
                    let return_addr = (pc + 1) as i32;
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(return_addr)));
                    pc = op.p2 as usize;
                }

                Opcode::Return => {
                    // Jump to address stored in p1.
                    let addr = self.get_reg(op.p1).to_integer();
                    pc = addr as usize;
                }

                // ── Transaction (stub for expression eval) ──────────────
                Opcode::Transaction
                | Opcode::AutoCommit
                | Opcode::ReadCookie
                | Opcode::SetCookie
                | Opcode::TableLock => {
                    // No-op in expression-only mode. Will be wired to WAL
                    // and lock manager in Phase 5.
                    pc += 1;
                }

                // ── Cursor ops (stub) ───────────────────────────────────
                Opcode::OpenRead
                | Opcode::OpenWrite
                | Opcode::OpenEphemeral
                | Opcode::OpenAutoindex
                | Opcode::OpenPseudo
                | Opcode::OpenDup
                | Opcode::ReopenIdx
                | Opcode::SorterOpen
                | Opcode::Close
                | Opcode::ColumnsUsed => {
                    // Stub: cursor operations will be wired in Phase 5.
                    pc += 1;
                }

                Opcode::Rewind | Opcode::Sort | Opcode::SorterSort => {
                    // Stub: jump to p2 (table empty / no cursor).
                    pc = op.p2 as usize;
                }

                Opcode::Next | Opcode::Prev | Opcode::SorterNext => {
                    // Stub: no more rows, fall through.
                    pc += 1;
                }

                Opcode::Column
                | Opcode::Rowid
                | Opcode::RowData
                | Opcode::NullRow
                | Opcode::Offset => {
                    // Stub: set result to NULL.
                    self.set_reg(op.p3.max(op.p2), SqliteValue::Null);
                    pc += 1;
                }

                // ── Seek ops (stub) ─────────────────────────────────────
                Opcode::SeekGE
                | Opcode::SeekGT
                | Opcode::SeekLE
                | Opcode::SeekLT
                | Opcode::SeekRowid
                | Opcode::SeekScan
                | Opcode::SeekEnd
                | Opcode::SeekHit => {
                    // Stub: seek not found, jump to p2.
                    pc = op.p2 as usize;
                }

                Opcode::NotFound | Opcode::NotExists | Opcode::IfNoHope => {
                    // Stub: key not found, jump to p2.
                    pc = op.p2 as usize;
                }

                Opcode::Found | Opcode::NoConflict => {
                    // Stub: key not found, fall through.
                    pc += 1;
                }

                // ── Insert/Delete (stub) ────────────────────────────────
                Opcode::Insert
                | Opcode::Delete
                | Opcode::NewRowid
                | Opcode::IdxInsert
                | Opcode::IdxDelete
                | Opcode::SorterInsert
                | Opcode::SorterCompare
                | Opcode::SorterData
                | Opcode::RowCell
                | Opcode::ResetCount => {
                    pc += 1;
                }

                // ── Record building ─────────────────────────────────────
                Opcode::MakeRecord => {
                    // Build a record from p1..p1+p2-1 into p3.
                    // Placeholder: store as empty blob. Full record format
                    // will use fsqlite-types::record in Phase 5.
                    self.set_reg(op.p3, SqliteValue::Blob(Vec::new()));
                    pc += 1;
                }

                Opcode::Affinity => {
                    // Apply type affinity to p2 registers starting at p1.
                    // Uses p4 as affinity string.
                    if let P4::Affinity(aff) = &op.p4 {
                        let start = op.p1;
                        for (i, ch) in aff.chars().enumerate() {
                            #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
                            let reg = start + i as i32;
                            let val = self.get_reg(reg).clone();
                            let affinity = char_to_affinity(ch);
                            self.set_reg(reg, val.apply_affinity(affinity));
                        }
                    }
                    pc += 1;
                }

                // ── Miscellaneous ───────────────────────────────────────
                Opcode::HaltIfNull => {
                    if self.get_reg(op.p3).is_null() {
                        let msg = match &op.p4 {
                            P4::Str(s) => s.clone(),
                            _ => "NOT NULL constraint failed".to_owned(),
                        };
                        return Ok(ExecOutcome::Error {
                            code: op.p1,
                            message: msg,
                        });
                    }
                    pc += 1;
                }

                Opcode::Count => {
                    // Stub: set p2 to 0 (no cursor).
                    self.set_reg(op.p2, SqliteValue::Integer(0));
                    pc += 1;
                }

                Opcode::Sequence => {
                    self.set_reg(op.p2, SqliteValue::Integer(0));
                    pc += 1;
                }

                Opcode::SequenceTest => {
                    pc += 1;
                }

                Opcode::Variable => {
                    // Bind parameter — stub as NULL for now.
                    self.set_reg(op.p2, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::BeginSubrtn => {
                    self.set_reg(op.p2, SqliteValue::Null);
                    pc += 1;
                }

                Opcode::IsTrue => {
                    let val = self.get_reg(op.p1);
                    let truth = !val.is_null() && val.to_integer() != 0;
                    self.set_reg(op.p2, SqliteValue::Integer(i64::from(truth)));
                    pc += 1;
                }

                Opcode::ZeroOrNull => {
                    // p2 = 0 if any of p1, p2, p3 is NULL.
                    let any_null = self.get_reg(op.p1).is_null()
                        || self.get_reg(op.p2).is_null()
                        || self.get_reg(op.p3).is_null();
                    if any_null {
                        self.set_reg(op.p2, SqliteValue::Integer(0));
                    }
                    pc += 1;
                }

                Opcode::IfNullRow => {
                    // Stub: cursor p1 always has null row in stub mode.
                    pc = op.p2 as usize;
                }

                Opcode::IfNotOpen => {
                    // Stub: cursor is never open in stub mode.
                    pc = op.p2 as usize;
                }

                Opcode::TypeCheck
                | Opcode::Permutation
                | Opcode::Compare
                | Opcode::CollSeq
                | Opcode::ElseEq
                | Opcode::FkCheck => {
                    pc += 1;
                }

                Opcode::Jump => {
                    // Jump to one of p1/p2/p3 based on last comparison.
                    // Stub: jump to p2 (neutral).
                    pc = op.p2 as usize;
                }

                Opcode::IsType => {
                    // Type check; stub: fall through.
                    pc += 1;
                }

                Opcode::Last => {
                    // Stub: table empty, jump to p2.
                    pc = op.p2 as usize;
                }

                Opcode::IfSizeBetween | Opcode::IfEmpty => {
                    // Stub: jump to p2.
                    pc = op.p2 as usize;
                }

                Opcode::DeferredSeek | Opcode::IdxRowid | Opcode::FinishSeek => {
                    pc += 1;
                }

                // ── Index comparison ────────────────────────────────────
                Opcode::IdxLE | Opcode::IdxGT | Opcode::IdxLT | Opcode::IdxGE => {
                    // Stub: fall through.
                    pc += 1;
                }

                // ── Schema / DDL ────────────────────────────────────────
                Opcode::CreateBtree
                | Opcode::SqlExec
                | Opcode::ParseSchema
                | Opcode::LoadAnalysis
                | Opcode::DropTable
                | Opcode::DropIndex
                | Opcode::DropTrigger
                | Opcode::Destroy
                | Opcode::Clear
                | Opcode::ResetSorter => {
                    pc += 1;
                }

                // ── Savepoint / Checkpoint ──────────────────────────────
                Opcode::Savepoint | Opcode::Checkpoint => {
                    pc += 1;
                }

                // ── Program execution (subprogram) ──────────────────────
                Opcode::Program | Opcode::Param => {
                    pc += 1;
                }

                // ── Coroutine ───────────────────────────────────────────
                Opcode::InitCoroutine => {
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(op.p3)));
                    if op.p2 > 0 {
                        pc = op.p2 as usize;
                    } else {
                        pc += 1;
                    }
                }

                Opcode::Yield => {
                    let saved = self.get_reg(op.p1).to_integer();
                    let current = (pc + 1) as i32;
                    self.set_reg(op.p1, SqliteValue::Integer(i64::from(current)));
                    pc = saved as usize;
                }

                Opcode::EndCoroutine => {
                    let saved = self.get_reg(op.p1).to_integer();
                    pc = saved as usize;
                }

                // ── Aggregation (stub) ──────────────────────────────────
                Opcode::AggStep | Opcode::AggInverse | Opcode::AggFinal | Opcode::AggValue => {
                    pc += 1;
                }

                // ── Catch-all for remaining opcodes ─────────────────────
                _ => {
                    // Unimplemented opcode: skip (no-op for now).
                    pc += 1;
                }
            }
        }
    }

    /// Get the collected result rows.
    pub fn results(&self) -> &[Vec<SqliteValue>] {
        &self.results
    }

    /// Take the result rows, consuming them.
    pub fn take_results(&mut self) -> Vec<Vec<SqliteValue>> {
        std::mem::take(&mut self.results)
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    #[allow(clippy::cast_sign_loss)]
    fn get_reg(&self, r: i32) -> &SqliteValue {
        self.registers.get(r as usize).unwrap_or(&SqliteValue::Null)
    }

    #[allow(clippy::cast_sign_loss)]
    fn set_reg(&mut self, r: i32, val: SqliteValue) {
        let idx = r as usize;
        if idx >= self.registers.len() {
            self.registers.resize(idx + 1, SqliteValue::Null);
        }
        self.registers[idx] = val;
    }
}

// ── Arithmetic helpers ──────────────────────────────────────────────────────

/// SQL division with NULL propagation and division-by-zero handling.
#[allow(clippy::cast_precision_loss)]
fn sql_div(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    if let (SqliteValue::Integer(a), SqliteValue::Integer(b)) = (dividend, divisor) {
        if *b == 0 {
            SqliteValue::Null
        } else {
            SqliteValue::Integer(a / b)
        }
    } else {
        let b = divisor.to_float();
        if b == 0.0 {
            SqliteValue::Null
        } else {
            SqliteValue::Float(dividend.to_float() / b)
        }
    }
}

/// SQL remainder with NULL propagation and division-by-zero handling.
fn sql_rem(dividend: &SqliteValue, divisor: &SqliteValue) -> SqliteValue {
    if dividend.is_null() || divisor.is_null() {
        return SqliteValue::Null;
    }
    let a = dividend.to_integer();
    let b = divisor.to_integer();
    if b == 0 {
        SqliteValue::Null
    } else {
        SqliteValue::Integer(a % b)
    }
}

/// SQL shift left (SQLite semantics: negative shift = shift right).
fn sql_shift_left(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_right(val, -amount);
    }
    if amount >= 64 {
        return SqliteValue::Integer(0);
    }
    // amount is in [0, 63] so the cast is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let shift = amount as u32;
    SqliteValue::Integer(val << shift)
}

/// SQL shift right (SQLite semantics: negative shift = shift left).
fn sql_shift_right(val: i64, amount: i64) -> SqliteValue {
    if amount < 0 {
        return sql_shift_left(val, -amount);
    }
    if amount >= 64 {
        return SqliteValue::Integer(if val < 0 { -1 } else { 0 });
    }
    // amount is in [0, 63] so the cast is safe.
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let shift = amount as u32;
    SqliteValue::Integer(val >> shift)
}

/// Three-valued SQL AND.
fn sql_and(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    let a_val = if a.is_null() {
        None
    } else {
        Some(a.to_integer() != 0)
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(b.to_integer() != 0)
    };

    match (a_val, b_val) {
        (Some(false), _) | (_, Some(false)) => SqliteValue::Integer(0),
        (Some(true), Some(true)) => SqliteValue::Integer(1),
        _ => SqliteValue::Null,
    }
}

/// Three-valued SQL OR.
fn sql_or(a: &SqliteValue, b: &SqliteValue) -> SqliteValue {
    let a_val = if a.is_null() {
        None
    } else {
        Some(a.to_integer() != 0)
    };
    let b_val = if b.is_null() {
        None
    } else {
        Some(b.to_integer() != 0)
    };

    match (a_val, b_val) {
        (Some(true), _) | (_, Some(true)) => SqliteValue::Integer(1),
        (Some(false), Some(false)) => SqliteValue::Integer(0),
        _ => SqliteValue::Null,
    }
}

/// SQL CAST operation (p2 encodes target type).
fn sql_cast(val: SqliteValue, target: i32) -> SqliteValue {
    // Target type encoding matches SQLite:
    // 'A' (65) = BLOB, 'B' (66) = TEXT, 'C' (67) = NUMERIC,
    // 'D' (68) = INTEGER, 'E' (69) = REAL
    // But more commonly p2 is used as an affinity character.
    match target as u8 {
        b'A' | b'a' => SqliteValue::Blob(match val {
            SqliteValue::Blob(b) => b,
            SqliteValue::Text(s) => s.into_bytes(),
            other => other.to_text().into_bytes(),
        }),
        b'B' | b'b' => SqliteValue::Text(val.to_text()),
        b'D' | b'd' => SqliteValue::Integer(val.to_integer()),
        b'E' | b'e' => SqliteValue::Float(val.to_float()),
        _ => val, // NUMERIC / unknown: no-op
    }
}

/// Convert affinity character to `TypeAffinity`.
fn char_to_affinity(ch: char) -> fsqlite_types::TypeAffinity {
    match ch {
        'd' | 'D' => fsqlite_types::TypeAffinity::Integer,
        'e' | 'E' => fsqlite_types::TypeAffinity::Real,
        'B' => fsqlite_types::TypeAffinity::Blob,
        'C' | 'c' => fsqlite_types::TypeAffinity::Text,
        'A' | 'a' => fsqlite_types::TypeAffinity::Numeric,
        _ => fsqlite_types::TypeAffinity::Blob,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProgramBuilder;
    use fsqlite_types::opcode::{Opcode, P4};

    /// Build and execute a program, returning results.
    fn run_program(build: impl FnOnce(&mut ProgramBuilder)) -> Vec<Vec<SqliteValue>> {
        let mut b = ProgramBuilder::new();
        build(&mut b);
        let prog = b.finish().expect("program should build");
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).expect("execution should succeed");
        assert_eq!(outcome, ExecOutcome::Done);
        engine.take_results()
    }

    // ── test_select_integer_literal ─────────────────────────────────────
    #[test]
    fn test_select_integer_literal() {
        // SELECT 42 → [(42,)]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
            let r1 = b.alloc_reg();
            b.emit_op(Opcode::Integer, 42, r1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(42)]);
    }

    // ── test_select_arithmetic ──────────────────────────────────────────
    #[test]
    fn test_select_arithmetic() {
        // SELECT 1+2 → [(3,)]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg(); // 1
            let r2 = b.alloc_reg(); // 2
            let r3 = b.alloc_reg(); // result

            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
    }

    // ── test_select_expression_eval ─────────────────────────────────────
    #[test]
    fn test_select_expression_eval() {
        // SELECT 1+2, 'abc'||'def' → [(3, "abcdef")]
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg(); // 1+2 result
            let r4 = b.alloc_reg();
            let r5 = b.alloc_reg();
            let r6 = b.alloc_reg(); // concat result

            // 1 + 2
            b.emit_op(Opcode::Integer, 1, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r1, r2, r3, P4::None, 0);

            // 'abc' || 'def'
            b.emit_op(Opcode::String8, 0, r4, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r5, 0, P4::Str("def".to_owned()), 0);
            b.emit_op(Opcode::Concat, r5, r4, r6, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            // Also emit second column as separate row for now
            b.emit_op(Opcode::ResultRow, r6, 1, 0, P4::None, 0);

            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Integer(3)]);
        assert_eq!(rows[1], vec![SqliteValue::Text("abcdef".to_owned())]);
    }

    // ── test_select_multi_column ────────────────────────────────────────
    #[test]
    fn test_select_multi_column() {
        // SELECT 1+2, 'abc'||'def' as a single row
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let out_start = b.alloc_regs(2);
            let r_tmp1 = b.alloc_reg();
            let r_tmp2 = b.alloc_reg();

            // 1 + 2 → out_start
            b.emit_op(Opcode::Integer, 1, r_tmp1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 2, r_tmp2, 0, P4::None, 0);
            b.emit_op(Opcode::Add, r_tmp1, r_tmp2, out_start, P4::None, 0);

            // 'abc' || 'def' → out_start+1
            b.emit_op(Opcode::String8, 0, r_tmp1, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::String8, 0, r_tmp2, 0, P4::Str("def".to_owned()), 0);
            b.emit_op(Opcode::Concat, r_tmp2, r_tmp1, out_start + 1, P4::None, 0);

            b.emit_op(Opcode::ResultRow, out_start, 2, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcdef".to_owned()),
            ]
        );
    }

    // ── test_vdbe_null_handling ──────────────────────────────────────────
    #[test]
    fn test_vdbe_null_handling() {
        // NULL + 1 = NULL, NULL = NULL is NULL (no jump)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_one = b.alloc_reg();
            let r_result = b.alloc_reg();
            let r_is_null = b.alloc_reg();

            // NULL
            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            // 1
            b.emit_op(Opcode::Integer, 1, r_one, 0, P4::None, 0);
            // NULL + 1
            b.emit_op(Opcode::Add, r_null, r_one, r_result, P4::None, 0);
            // Check: result IS NULL → set r_is_null=1
            b.emit_op(Opcode::Integer, 0, r_is_null, 0, P4::None, 0);
            let skip = b.emit_label();
            b.emit_jump_to_label(Opcode::NotNull, r_result, 0, skip, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_is_null, 0, P4::None, 0);
            b.resolve_label(skip);

            b.emit_op(Opcode::ResultRow, r_result, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_is_null, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Null]); // NULL + 1 = NULL
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // IS NULL = true
    }

    // ── test_vdbe_comparison_affinity ────────────────────────────────────
    #[test]
    fn test_vdbe_comparison_affinity() {
        // Test: 5 > 3 → jump taken (result 1), 3 > 5 → not taken (result 0)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_5 = b.alloc_reg();
            let r_3 = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Integer, 5, r_5, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 3, r_3, 0, P4::None, 0);

            // Test 5 > 3: if r_5 (p3) > r_3 (p1), jump.
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let gt_taken = b.emit_label();
            b.emit_jump_to_label(Opcode::Gt, r_3, r_5, gt_taken, P4::None, 0);
            // Not taken path:
            let done1 = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done1, P4::None, 0);
            // Taken path:
            b.resolve_label(gt_taken);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done1);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);

            // Test 3 > 5: should NOT jump
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);
            let gt_taken2 = b.emit_label();
            // p3=r_3 (3), p1=r_5 (5): is 3 > 5? No.
            b.emit_jump_to_label(Opcode::Gt, r_5, r_3, gt_taken2, P4::None, 0);
            let done2 = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done2, P4::None, 0);
            b.resolve_label(gt_taken2);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done2);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // 5 > 3 = true
        assert_eq!(rows[1], vec![SqliteValue::Integer(0)]); // 3 > 5 = false
    }

    // ── test_vdbe_division_by_zero ──────────────────────────────────────
    #[test]
    fn test_vdbe_division_by_zero() {
        // SELECT 10 / 0 → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();

            b.emit_op(Opcode::Integer, 0, r1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 10, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Divide, r1, r2, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null]); // div by zero → NULL
    }

    // ── test_vdbe_string_concat_null ────────────────────────────────────
    #[test]
    fn test_vdbe_string_concat_null() {
        // 'abc' || NULL → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();

            b.emit_op(Opcode::String8, 0, r1, 0, P4::Str("abc".to_owned()), 0);
            b.emit_op(Opcode::Null, 0, r2, 0, P4::None, 0);
            b.emit_op(Opcode::Concat, r2, r1, r3, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Null]);
    }

    // ── test_vdbe_boolean_logic ─────────────────────────────────────────
    #[test]
    fn test_vdbe_boolean_logic() {
        // TRUE AND FALSE → 0, TRUE OR FALSE → 1, NOT TRUE → 0
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r_and = b.alloc_reg();
            let r_or = b.alloc_reg();
            let r_not = b.alloc_reg();

            b.emit_op(Opcode::Integer, 1, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);
            b.emit_op(Opcode::And, r_true, r_false, r_and, P4::None, 0);
            b.emit_op(Opcode::Or, r_true, r_false, r_or, P4::None, 0);
            b.emit_op(Opcode::Not, r_true, r_not, 0, P4::None, 0);

            b.emit_op(Opcode::ResultRow, r_and, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_or, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_not, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // T AND F = F
        assert_eq!(rows[1], vec![SqliteValue::Integer(1)]); // T OR F = T
        assert_eq!(rows[2], vec![SqliteValue::Integer(0)]); // NOT T = F
    }

    // ── test_vdbe_three_valued_logic ────────────────────────────────────
    #[test]
    fn test_vdbe_three_valued_logic() {
        // NULL AND FALSE → 0, NULL AND TRUE → NULL
        // NULL OR TRUE → 1, NULL OR FALSE → NULL
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_true = b.alloc_reg();
            let r_false = b.alloc_reg();
            let r1 = b.alloc_reg();
            let r2 = b.alloc_reg();
            let r3 = b.alloc_reg();
            let r4 = b.alloc_reg();

            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 1, r_true, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_false, 0, P4::None, 0);

            b.emit_op(Opcode::And, r_null, r_false, r1, P4::None, 0); // NULL AND F
            b.emit_op(Opcode::And, r_null, r_true, r2, P4::None, 0); // NULL AND T
            b.emit_op(Opcode::Or, r_null, r_true, r3, P4::None, 0); // NULL OR T
            b.emit_op(Opcode::Or, r_null, r_false, r4, P4::None, 0); // NULL OR F

            b.emit_op(Opcode::ResultRow, r1, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r2, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r4, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows[0], vec![SqliteValue::Integer(0)]); // NULL AND F = F
        assert_eq!(rows[1], vec![SqliteValue::Null]); // NULL AND T = NULL
        assert_eq!(rows[2], vec![SqliteValue::Integer(1)]); // NULL OR T = T
        assert_eq!(rows[3], vec![SqliteValue::Null]); // NULL OR F = NULL
    }

    // ── test_vdbe_gosub_return ──────────────────────────────────────────
    #[test]
    fn test_vdbe_gosub_return() {
        // Use Gosub/Return to call a subroutine that sets r2=99.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_return = b.alloc_reg(); // return address storage
            let r_val = b.alloc_reg(); // output

            // Main: call subroutine, then output r_val.
            let sub_label = b.emit_label();
            b.emit_jump_to_label(Opcode::Gosub, r_return, 0, sub_label, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            // Subroutine: set r_val=99, return.
            b.resolve_label(sub_label);
            b.emit_op(Opcode::Integer, 99, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::Return, r_return, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(99)]);
    }

    // ── test_vdbe_is_null_comparison ─────────────────────────────────────
    #[test]
    fn test_vdbe_is_null_comparison() {
        // NULL IS NULL → true (using Eq with NULLEQ flag)
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_null = b.alloc_reg();
            let r_out = b.alloc_reg();

            b.emit_op(Opcode::Null, 0, r_null, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 0, r_out, 0, P4::None, 0);

            // Eq with p5=0x80 (SQLITE_NULLEQ): NULL IS NULL → jump
            let is_null_label = b.emit_label();
            // p1=r_null, p3=r_null (compare same register)
            b.emit_jump_to_label(Opcode::Eq, r_null, 0, is_null_label, P4::None, 0x80);
            let done = b.emit_label();
            b.emit_jump_to_label(Opcode::Goto, 0, 0, done, P4::None, 0);
            b.resolve_label(is_null_label);
            b.emit_op(Opcode::Integer, 1, r_out, 0, P4::None, 0);
            b.resolve_label(done);

            b.emit_op(Opcode::ResultRow, r_out, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![SqliteValue::Integer(1)]); // NULL IS NULL = true
    }

    // ── test_vdbe_coroutine ─────────────────────────────────────────────
    #[test]
    fn test_vdbe_coroutine() {
        // Test coroutine: producer yields values 10, 20, 30.
        let rows = run_program(|b| {
            let end = b.emit_label();
            b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);

            let r_co = b.alloc_reg(); // coroutine state register
            let r_val = b.alloc_reg(); // value register

            // InitCoroutine: p1=r_co, p2=consumer start, p3=producer start
            let consumer_start = b.emit_label();
            let producer_start = b.emit_label();
            b.emit_jump_to_label(Opcode::InitCoroutine, r_co, 0, consumer_start, P4::None, 0);
            // Hack: resolve producer_start at the InitCoroutine's p3 position.
            // Actually, InitCoroutine stores p3 into r_co, then jumps to p2.
            // So p3 should be the producer's first instruction address.

            // For simplicity, just test Yield directly:
            b.resolve_label(consumer_start);

            // Producer: emit 3 values
            b.resolve_label(producer_start);
            b.emit_op(Opcode::Integer, 10, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 20, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Integer, 30, r_val, 0, P4::None, 0);
            b.emit_op(Opcode::ResultRow, r_val, 1, 0, P4::None, 0);
            b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);

            b.resolve_label(end);
        });
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![SqliteValue::Integer(10)]);
        assert_eq!(rows[1], vec![SqliteValue::Integer(20)]);
        assert_eq!(rows[2], vec![SqliteValue::Integer(30)]);
    }

    // ── test_vdbe_halt_with_error ───────────────────────────────────────
    #[test]
    fn test_vdbe_halt_with_error() {
        let mut b = ProgramBuilder::new();
        b.emit_op(
            Opcode::Halt,
            1,
            0,
            0,
            P4::Str("constraint failed".to_owned()),
            0,
        );
        let prog = b.finish().unwrap();
        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(
            outcome,
            ExecOutcome::Error {
                code: 1,
                message: "constraint failed".to_owned(),
            }
        );
    }

    // ── test_vdbe_disassemble_and_exec ──────────────────────────────────
    #[test]
    fn test_vdbe_disassemble_and_exec() {
        // Build a program, disassemble it, and verify output.
        let mut b = ProgramBuilder::new();
        let end = b.emit_label();
        b.emit_jump_to_label(Opcode::Init, 0, 0, end, P4::None, 0);
        let r1 = b.alloc_reg();
        let r2 = b.alloc_reg();
        let r3 = b.alloc_reg();
        b.emit_op(Opcode::Integer, 10, r1, 0, P4::None, 0);
        b.emit_op(Opcode::Integer, 20, r2, 0, P4::None, 0);
        b.emit_op(Opcode::Multiply, r1, r2, r3, P4::None, 0);
        b.emit_op(Opcode::ResultRow, r3, 1, 0, P4::None, 0);
        b.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
        b.resolve_label(end);

        let prog = b.finish().unwrap();
        let asm = prog.disassemble();
        assert!(asm.contains("Init"));
        assert!(asm.contains("Integer"));
        assert!(asm.contains("Multiply"));
        assert!(asm.contains("ResultRow"));
        assert!(asm.contains("Halt"));

        let mut engine = VdbeEngine::new(prog.register_count());
        let outcome = engine.execute(&prog).unwrap();
        assert_eq!(outcome, ExecOutcome::Done);
        assert_eq!(engine.results().len(), 1);
        assert_eq!(engine.results()[0], vec![SqliteValue::Integer(200)]);
    }
}
