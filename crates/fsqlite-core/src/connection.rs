//! Minimal SQL connection API for the Phase 4 query pipeline.
//!
//! This module intentionally starts with a narrow execution path:
//! expression-only `SELECT` statements (no `FROM`) compiled to VDBE bytecode
//! and executed by the VDBE engine.

use fsqlite_ast::{
    BinaryOp, Distinctness, Expr, FunctionArgs, Literal, PlaceholderType, ResultColumn, SelectCore,
    SelectStatement, Statement, UnaryOp,
};
use fsqlite_error::{FrankenError, Result};
use fsqlite_parser::Parser;
use fsqlite_types::opcode::{Opcode, P4};
use fsqlite_types::value::SqliteValue;
use fsqlite_vdbe::engine::{ExecOutcome, VdbeEngine};
use fsqlite_vdbe::{ProgramBuilder, VdbeProgram};

/// Map a SQL type name to its SQLite affinity byte (§3.1 Type Affinity Rules).
fn type_name_to_affinity(name: &str) -> u8 {
    let upper = name.to_uppercase();
    if upper.contains("INT") {
        b'D' // INTEGER affinity
    } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
        b'C' // TEXT affinity
    } else if upper.contains("BLOB") || upper.is_empty() {
        b'A' // BLOB affinity
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        b'E' // REAL affinity
    } else {
        b'B' // NUMERIC affinity
    }
}

/// A database row produced by a query.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<SqliteValue>,
}

impl Row {
    /// Returns all values in this row.
    pub fn values(&self) -> &[SqliteValue] {
        &self.values
    }

    /// Returns the value at `index`, if present.
    pub fn get(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }
}

/// A prepared SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedStatement {
    program: VdbeProgram,
}

impl PreparedStatement {
    /// Execute as a query and return all result rows.
    pub fn query(&self) -> Result<Vec<Row>> {
        execute_program(&self.program, None)
    }

    /// Execute as a query with bound SQL parameters (`?1`, `?2`, ...).
    pub fn query_with_params(&self, params: &[SqliteValue]) -> Result<Vec<Row>> {
        execute_program(&self.program, Some(params))
    }

    /// Execute and return affected/output row count.
    pub fn execute(&self) -> Result<usize> {
        Ok(self.query()?.len())
    }

    /// Execute with bound SQL parameters and return affected/output row count.
    pub fn execute_with_params(&self, params: &[SqliteValue]) -> Result<usize> {
        Ok(self.query_with_params(params)?.len())
    }

    /// Return an EXPLAIN-style disassembly for the compiled program.
    pub fn explain(&self) -> String {
        self.program.disassemble()
    }
}

/// A lightweight connection façade over the parser + VDBE path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    path: String,
}

impl Connection {
    /// Open a connection.
    ///
    /// At this stage, the path is tracked but storage-backed statements are
    /// not implemented yet. Expression-only SELECT is supported.
    pub fn open(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        if path.is_empty() {
            return Err(FrankenError::CannotOpen {
                path: std::path::PathBuf::from(path),
            });
        }
        Ok(Self { path })
    }

    /// Returns the configured database path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Prepare SQL into a statement.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let statement = parse_single_statement(sql)?;
        let program = compile_statement(&statement)?;
        Ok(PreparedStatement { program })
    }

    /// Prepare and execute SQL as a query.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>> {
        self.prepare(sql)?.query()
    }

    /// Prepare and execute SQL as a query with bound SQL parameters.
    pub fn query_with_params(&self, sql: &str, params: &[SqliteValue]) -> Result<Vec<Row>> {
        self.prepare(sql)?.query_with_params(params)
    }

    /// Prepare and execute SQL, returning output/affected row count.
    pub fn execute(&self, sql: &str) -> Result<usize> {
        self.prepare(sql)?.execute()
    }

    /// Prepare and execute SQL with bound SQL parameters.
    pub fn execute_with_params(&self, sql: &str, params: &[SqliteValue]) -> Result<usize> {
        self.prepare(sql)?.execute_with_params(params)
    }
}

fn execute_program(program: &VdbeProgram, params: Option<&[SqliteValue]>) -> Result<Vec<Row>> {
    let mut engine = VdbeEngine::new(program.register_count());
    if let Some(params) = params {
        validate_bound_parameters(program, params)?;
        engine.set_bindings(params.to_vec());
    }

    match engine.execute(program)? {
        ExecOutcome::Done => Ok(engine
            .take_results()
            .into_iter()
            .map(|values| Row { values })
            .collect()),
        ExecOutcome::Error { code, message } => Err(FrankenError::Internal(format!(
            "VDBE halted with code {code}: {message}",
        ))),
    }
}

fn validate_bound_parameters(program: &VdbeProgram, params: &[SqliteValue]) -> Result<()> {
    let mut max_required: usize = 0;
    for op in program.ops() {
        if op.opcode != Opcode::Variable {
            continue;
        }
        let one_based = usize::try_from(op.p1).map_err(|_| FrankenError::OutOfRange {
            what: "bind parameter index".to_owned(),
            value: op.p1.to_string(),
        })?;
        if one_based == 0 {
            return Err(FrankenError::OutOfRange {
                what: "bind parameter index".to_owned(),
                value: op.p1.to_string(),
            });
        }
        max_required = max_required.max(one_based);
    }

    if max_required > params.len() {
        return Err(FrankenError::OutOfRange {
            what: "bind parameter index".to_owned(),
            value: max_required.to_string(),
        });
    }
    Ok(())
}

fn parse_single_statement(sql: &str) -> Result<Statement> {
    let mut parser = Parser::from_sql(sql);
    let (statements, errors) = parser.parse_all();

    if let Some(parse_error) = errors.first() {
        return Err(FrankenError::ParseError {
            #[allow(clippy::cast_sign_loss)]
            offset: parse_error.span.start as usize,
            detail: parse_error.message.clone(),
        });
    }

    let mut iter = statements.into_iter();
    let statement = iter.next().ok_or_else(|| FrankenError::ParseError {
        offset: 0,
        detail: "no SQL statement provided".to_owned(),
    })?;

    if iter.next().is_some() {
        return Err(FrankenError::NotImplemented(
            "multiple statements are not supported yet".to_owned(),
        ));
    }

    Ok(statement)
}

fn compile_statement(statement: &Statement) -> Result<VdbeProgram> {
    match statement {
        Statement::Select(select) => compile_expression_select(select),
        _ => Err(FrankenError::NotImplemented(
            "only expression-only SELECT statements are supported".to_owned(),
        )),
    }
}

#[allow(clippy::too_many_lines)]
fn compile_expression_select(select: &SelectStatement) -> Result<VdbeProgram> {
    if select.with.is_some() {
        return Err(FrankenError::NotImplemented(
            "WITH is not supported in this connection path".to_owned(),
        ));
    }
    if !select.order_by.is_empty() {
        return Err(FrankenError::NotImplemented(
            "ORDER BY is not supported in this connection path".to_owned(),
        ));
    }
    if select.limit.is_some() {
        return Err(FrankenError::NotImplemented(
            "LIMIT is not supported in this connection path".to_owned(),
        ));
    }
    if !select.body.compounds.is_empty() {
        return Err(FrankenError::NotImplemented(
            "compound SELECT is not supported in this connection path".to_owned(),
        ));
    }

    let mut builder = ProgramBuilder::new();
    let mut bind_state = BindParamState::default();
    let init_target = builder.emit_label();
    builder.emit_jump_to_label(Opcode::Init, 0, 0, init_target, P4::None, 0);

    match &select.body.select {
        SelectCore::Select {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            windows,
        } => {
            if *distinct != Distinctness::All {
                return Err(FrankenError::NotImplemented(
                    "DISTINCT is not supported in this connection path".to_owned(),
                ));
            }
            if from.is_some() {
                return Err(FrankenError::NotImplemented(
                    "SELECT ... FROM is not supported in this connection path".to_owned(),
                ));
            }
            if !group_by.is_empty() {
                return Err(FrankenError::NotImplemented(
                    "GROUP BY is not supported in this connection path".to_owned(),
                ));
            }
            if having.is_some() {
                return Err(FrankenError::NotImplemented(
                    "HAVING is not supported in this connection path".to_owned(),
                ));
            }
            if !windows.is_empty() {
                return Err(FrankenError::NotImplemented(
                    "WINDOW is not supported in this connection path".to_owned(),
                ));
            }
            if columns.is_empty() {
                return Err(FrankenError::ParseError {
                    offset: 0,
                    detail: "SELECT must include at least one result column".to_owned(),
                });
            }

            let out_count =
                i32::try_from(columns.len()).map_err(|_| FrankenError::TooManyColumns {
                    count: columns.len(),
                    max: i32::MAX as usize,
                })?;
            let out_first_reg = builder.alloc_regs(out_count);
            let skip_row_label = if let Some(predicate) = where_clause.as_ref() {
                let predicate_reg = builder.alloc_temp();
                emit_expr(&mut builder, predicate, predicate_reg, &mut bind_state)?;
                let skip_label = builder.emit_label();
                builder.emit_jump_to_label(
                    Opcode::IfNot,
                    predicate_reg,
                    0,
                    skip_label,
                    P4::None,
                    0,
                );
                builder.free_temp(predicate_reg);
                Some(skip_label)
            } else {
                None
            };

            for (idx, column) in columns.iter().enumerate() {
                let expr = match column {
                    ResultColumn::Expr { expr, .. } => expr,
                    ResultColumn::Star | ResultColumn::TableStar(_) => {
                        return Err(FrankenError::NotImplemented(
                            "star expansion requires name resolution and FROM sources".to_owned(),
                        ));
                    }
                };

                let idx_i32 = i32::try_from(idx).map_err(|_| FrankenError::OutOfRange {
                    what: "result column index".to_owned(),
                    value: idx.to_string(),
                })?;
                let output_reg = out_first_reg + idx_i32;
                emit_expr(&mut builder, expr, output_reg, &mut bind_state)?;
            }

            builder.emit_op(Opcode::ResultRow, out_first_reg, out_count, 0, P4::None, 0);
            if let Some(skip_label) = skip_row_label {
                builder.resolve_label(skip_label);
            }
        }
        SelectCore::Values(rows) => {
            if rows.is_empty() {
                return Err(FrankenError::ParseError {
                    offset: 0,
                    detail: "VALUES must include at least one row".to_owned(),
                });
            }
            let first_row_len = rows[0].len();
            if first_row_len == 0 {
                return Err(FrankenError::ParseError {
                    offset: 0,
                    detail: "VALUES row must include at least one expression".to_owned(),
                });
            }
            for row in rows {
                if row.len() != first_row_len {
                    return Err(FrankenError::ParseError {
                        offset: 0,
                        detail: "VALUES rows must have matching column counts".to_owned(),
                    });
                }
            }

            let out_count =
                i32::try_from(first_row_len).map_err(|_| FrankenError::TooManyColumns {
                    count: first_row_len,
                    max: i32::MAX as usize,
                })?;
            let out_first_reg = builder.alloc_regs(out_count);

            for row in rows {
                for (idx, expr) in row.iter().enumerate() {
                    let idx_i32 = i32::try_from(idx).map_err(|_| FrankenError::OutOfRange {
                        what: "VALUES column index".to_owned(),
                        value: idx.to_string(),
                    })?;
                    emit_expr(&mut builder, expr, out_first_reg + idx_i32, &mut bind_state)?;
                }
                builder.emit_op(Opcode::ResultRow, out_first_reg, out_count, 0, P4::None, 0);
            }
        }
    }

    builder.emit_op(Opcode::Halt, 0, 0, 0, P4::None, 0);
    builder.resolve_label(init_target);
    builder.finish()
}

fn emit_expr(
    builder: &mut ProgramBuilder,
    expr: &Expr,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    match expr {
        Expr::Literal(literal, _) => {
            emit_literal(builder, literal, target_reg);
            Ok(())
        }
        Expr::BinaryOp {
            left, op, right, ..
        } => emit_binary_expr(builder, left, *op, right, target_reg, bind_state),
        Expr::UnaryOp { op, expr, .. } => {
            emit_unary_expr(builder, *op, expr, target_reg, bind_state)
        }
        Expr::FunctionCall {
            name,
            args,
            distinct,
            filter,
            over,
            ..
        } => emit_function_call(
            builder,
            name,
            args,
            *distinct,
            filter.is_some(),
            over.is_some(),
            target_reg,
        ),
        Expr::Placeholder(placeholder, _) => {
            let param_index = placeholder_to_index(placeholder, bind_state)?;
            builder.emit_op(Opcode::Variable, param_index, target_reg, 0, P4::None, 0);
            Ok(())
        }
        Expr::Case {
            operand,
            whens,
            else_expr,
            ..
        } => emit_case_expr(
            builder,
            operand.as_deref(),
            whens,
            else_expr.as_deref(),
            target_reg,
            bind_state,
        ),
        Expr::Cast {
            expr: inner,
            type_name,
            ..
        } => {
            emit_expr(builder, inner, target_reg, bind_state)?;
            let affinity = type_name_to_affinity(&type_name.name);
            builder.emit_op(
                Opcode::Cast,
                target_reg,
                i32::from(affinity),
                0,
                P4::None,
                0,
            );
            Ok(())
        }
        Expr::IsNull {
            expr: inner, not, ..
        } => {
            emit_expr(builder, inner, target_reg, bind_state)?;
            let lbl_null = builder.emit_label();
            let lbl_done = builder.emit_label();
            builder.emit_jump_to_label(Opcode::IsNull, target_reg, 0, lbl_null, P4::None, 0);
            let val_not_null = i32::from(*not);
            let val_null = i32::from(!*not);
            builder.emit_op(Opcode::Integer, val_not_null, target_reg, 0, P4::None, 0);
            builder.emit_jump_to_label(Opcode::Goto, 0, 0, lbl_done, P4::None, 0);
            builder.resolve_label(lbl_null);
            builder.emit_op(Opcode::Integer, val_null, target_reg, 0, P4::None, 0);
            builder.resolve_label(lbl_done);
            Ok(())
        }
        _ => Err(FrankenError::NotImplemented(format!(
            "expression form is not supported in this connection path: {expr:?}",
        ))),
    }
}

fn emit_binary_expr(
    builder: &mut ProgramBuilder,
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let left_reg = builder.alloc_temp();
    let right_reg = builder.alloc_temp();
    emit_expr(builder, left, left_reg, bind_state)?;
    emit_expr(builder, right, right_reg, bind_state)?;

    let opcode = match op {
        BinaryOp::Add => Opcode::Add,
        BinaryOp::Subtract => Opcode::Subtract,
        BinaryOp::Multiply => Opcode::Multiply,
        BinaryOp::Divide => Opcode::Divide,
        BinaryOp::Modulo => Opcode::Remainder,
        BinaryOp::Concat => Opcode::Concat,
        BinaryOp::BitAnd => Opcode::BitAnd,
        BinaryOp::BitOr => Opcode::BitOr,
        BinaryOp::ShiftLeft => Opcode::ShiftLeft,
        BinaryOp::ShiftRight => Opcode::ShiftRight,
        BinaryOp::And => Opcode::And,
        BinaryOp::Or => Opcode::Or,
        // Comparison operators produce 0/1 via conditional jumps.
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            let cmp_opcode = match op {
                BinaryOp::Eq => Opcode::Eq,
                BinaryOp::Ne => Opcode::Ne,
                BinaryOp::Lt => Opcode::Lt,
                BinaryOp::Le => Opcode::Le,
                BinaryOp::Gt => Opcode::Gt,
                BinaryOp::Ge => Opcode::Ge,
                _ => unreachable!(),
            };
            let true_label = builder.emit_label();
            let done_label = builder.emit_label();
            builder.emit_jump_to_label(cmp_opcode, right_reg, left_reg, true_label, P4::None, 0);
            builder.emit_op(Opcode::Integer, 0, target_reg, 0, P4::None, 0);
            builder.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            builder.resolve_label(true_label);
            builder.emit_op(Opcode::Integer, 1, target_reg, 0, P4::None, 0);
            builder.resolve_label(done_label);
            builder.free_temp(right_reg);
            builder.free_temp(left_reg);
            return Ok(());
        }
        BinaryOp::Is | BinaryOp::IsNot => {
            let (cmp_opcode, nulleq_flag) = match op {
                BinaryOp::Is => (Opcode::Eq, 0x80_u16),
                BinaryOp::IsNot => (Opcode::Ne, 0x80_u16),
                _ => unreachable!(),
            };
            let true_label = builder.emit_label();
            let done_label = builder.emit_label();
            builder.emit_jump_to_label(
                cmp_opcode,
                right_reg,
                left_reg,
                true_label,
                P4::None,
                nulleq_flag,
            );
            builder.emit_op(Opcode::Integer, 0, target_reg, 0, P4::None, 0);
            builder.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
            builder.resolve_label(true_label);
            builder.emit_op(Opcode::Integer, 1, target_reg, 0, P4::None, 0);
            builder.resolve_label(done_label);
            builder.free_temp(right_reg);
            builder.free_temp(left_reg);
            return Ok(());
        }
    };

    // Engine semantics for these opcodes consume p2 (left) and p1 (right).
    builder.emit_op(opcode, right_reg, left_reg, target_reg, P4::None, 0);
    builder.free_temp(right_reg);
    builder.free_temp(left_reg);
    Ok(())
}

fn emit_unary_expr(
    builder: &mut ProgramBuilder,
    op: UnaryOp,
    expr: &Expr,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    match op {
        UnaryOp::Plus => emit_expr(builder, expr, target_reg, bind_state),
        UnaryOp::BitNot => {
            let source_reg = builder.alloc_temp();
            emit_expr(builder, expr, source_reg, bind_state)?;
            builder.emit_op(Opcode::BitNot, source_reg, target_reg, 0, P4::None, 0);
            builder.free_temp(source_reg);
            Ok(())
        }
        UnaryOp::Not => {
            let source_reg = builder.alloc_temp();
            emit_expr(builder, expr, source_reg, bind_state)?;
            builder.emit_op(Opcode::Not, source_reg, target_reg, 0, P4::None, 0);
            builder.free_temp(source_reg);
            Ok(())
        }
        UnaryOp::Negate => {
            let source_reg = builder.alloc_temp();
            let zero_reg = builder.alloc_temp();
            emit_expr(builder, expr, source_reg, bind_state)?;
            builder.emit_op(Opcode::Integer, 0, zero_reg, 0, P4::None, 0);
            // target = 0 - source
            builder.emit_op(
                Opcode::Subtract,
                source_reg,
                zero_reg,
                target_reg,
                P4::None,
                0,
            );
            builder.free_temp(zero_reg);
            builder.free_temp(source_reg);
            Ok(())
        }
    }
}

fn emit_function_call(
    builder: &mut ProgramBuilder,
    name: &str,
    args: &FunctionArgs,
    distinct: bool,
    has_filter: bool,
    has_over: bool,
    target_reg: i32,
) -> Result<()> {
    if distinct || has_filter || has_over {
        return Err(FrankenError::NotImplemented(
            "function modifiers (DISTINCT/FILTER/OVER) are not supported".to_owned(),
        ));
    }

    if !name.eq_ignore_ascii_case("typeof") {
        return Err(FrankenError::NotImplemented(format!(
            "function is not supported in this connection path: {name}",
        )));
    }

    let arguments = match args {
        FunctionArgs::List(arguments) => arguments,
        FunctionArgs::Star => {
            return Err(FrankenError::NotImplemented(
                "typeof(*) is not supported".to_owned(),
            ));
        }
    };

    if arguments.len() != 1 {
        return Err(FrankenError::TooManyArguments {
            name: "typeof".to_owned(),
        });
    }

    match &arguments[0] {
        Expr::Literal(literal, _) => {
            builder.emit_op(
                Opcode::String8,
                0,
                target_reg,
                0,
                P4::Str(literal_typeof(literal).to_owned()),
                0,
            );
            Ok(())
        }
        _ => Err(FrankenError::NotImplemented(
            "typeof() currently supports literal arguments only".to_owned(),
        )),
    }
}

fn emit_literal(builder: &mut ProgramBuilder, literal: &Literal, target_reg: i32) {
    match literal {
        Literal::Integer(value) => {
            #[allow(clippy::cast_possible_truncation)]
            builder.emit_op(Opcode::Integer, *value as i32, target_reg, 0, P4::None, 0);
        }
        Literal::Float(value) => {
            builder.emit_op(Opcode::Real, 0, target_reg, 0, P4::Real(*value), 0);
        }
        Literal::String(value) => {
            builder.emit_op(Opcode::String8, 0, target_reg, 0, P4::Str(value.clone()), 0);
        }
        Literal::Null | Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
            builder.emit_op(Opcode::Null, 0, target_reg, 0, P4::None, 0);
        }
        Literal::True => {
            builder.emit_op(Opcode::Integer, 1, target_reg, 0, P4::None, 0);
        }
        Literal::False => {
            builder.emit_op(Opcode::Integer, 0, target_reg, 0, P4::None, 0);
        }
        Literal::Blob(value) => {
            builder.emit_op(Opcode::Blob, 0, target_reg, 0, P4::Blob(value.clone()), 0);
        }
    }
}

fn literal_typeof(literal: &Literal) -> &'static str {
    match literal {
        Literal::Null => "null",
        Literal::Integer(_) | Literal::True | Literal::False => "integer",
        Literal::Float(_) => "real",
        Literal::String(_)
        | Literal::CurrentTime
        | Literal::CurrentDate
        | Literal::CurrentTimestamp => "text",
        Literal::Blob(_) => "blob",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BindParamState {
    next_index: i32,
}

impl Default for BindParamState {
    fn default() -> Self {
        Self { next_index: 1 }
    }
}

impl BindParamState {
    fn claim_anonymous(&mut self) -> Result<i32> {
        let index = self.next_index;
        self.next_index =
            self.next_index
                .checked_add(1)
                .ok_or_else(|| FrankenError::OutOfRange {
                    what: "placeholder index".to_owned(),
                    value: index.to_string(),
                })?;
        Ok(index)
    }

    fn register_numbered(&mut self, index: i32) -> Result<i32> {
        let next = index
            .checked_add(1)
            .ok_or_else(|| FrankenError::OutOfRange {
                what: "placeholder index".to_owned(),
                value: index.to_string(),
            })?;
        self.next_index = self.next_index.max(next);
        Ok(index)
    }
}

fn placeholder_to_index(
    placeholder: &PlaceholderType,
    bind_state: &mut BindParamState,
) -> Result<i32> {
    match placeholder {
        PlaceholderType::Anonymous => bind_state.claim_anonymous(),
        PlaceholderType::Numbered(index) => {
            let index = i32::try_from(*index).map_err(|_| FrankenError::OutOfRange {
                what: "placeholder index".to_owned(),
                value: index.to_string(),
            })?;
            bind_state.register_numbered(index)
        }
        PlaceholderType::ColonNamed(name)
        | PlaceholderType::AtNamed(name)
        | PlaceholderType::DollarNamed(name) => Err(FrankenError::NotImplemented(format!(
            "named placeholder not supported: {name}",
        ))),
    }
}

fn emit_case_expr(
    builder: &mut ProgramBuilder,
    operand: Option<&Expr>,
    whens: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    target_reg: i32,
    bind_state: &mut BindParamState,
) -> Result<()> {
    let done_label = builder.emit_label();
    let r_operand = if let Some(op_expr) = operand {
        let r = builder.alloc_temp();
        emit_expr(builder, op_expr, r, bind_state)?;
        Some(r)
    } else {
        None
    };

    for (when_expr, then_expr) in whens {
        let next_when = builder.emit_label();

        if let Some(r_op) = r_operand {
            let r_when = builder.alloc_temp();
            emit_expr(builder, when_expr, r_when, bind_state)?;
            builder.emit_jump_to_label(Opcode::Ne, r_when, r_op, next_when, P4::None, 0);
            builder.free_temp(r_when);
        } else {
            emit_expr(builder, when_expr, target_reg, bind_state)?;
            builder.emit_jump_to_label(Opcode::IfNot, target_reg, 1, next_when, P4::None, 0);
        }

        emit_expr(builder, then_expr, target_reg, bind_state)?;
        builder.emit_jump_to_label(Opcode::Goto, 0, 0, done_label, P4::None, 0);
        builder.resolve_label(next_when);
    }

    if let Some(el) = else_expr {
        emit_expr(builder, el, target_reg, bind_state)?;
    } else {
        builder.emit_op(Opcode::Null, 0, target_reg, 0, P4::None, 0);
    }

    builder.resolve_label(done_label);

    if let Some(r_op) = r_operand {
        builder.free_temp(r_op);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Connection, Row};
    use fsqlite_error::FrankenError;
    use fsqlite_types::value::SqliteValue;

    fn row_values(row: &Row) -> Vec<SqliteValue> {
        row.values().to_vec()
    }

    #[test]
    fn test_query_expression_eval_pipeline() {
        let connection = Connection::open(":memory:").expect("in-memory path should open");
        let rows = connection
            .query("SELECT 1+2, 'abc'||'def', typeof(3.14);")
            .expect("expression SELECT should execute");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(3),
                SqliteValue::Text("abcdef".to_owned()),
                SqliteValue::Text("real".to_owned()),
            ],
        );
    }

    #[test]
    fn test_reject_select_with_from() {
        let connection = Connection::open(":memory:").expect("in-memory path should open");
        let error = connection
            .query("SELECT a FROM t;")
            .expect_err("SELECT with FROM must be rejected in this narrow path");
        assert!(matches!(error, FrankenError::NotImplemented(_)));
    }

    #[test]
    fn test_query_comparison_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 3 > 2, 1 = 1, 5 < 3;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(1),
                SqliteValue::Integer(1),
                SqliteValue::Integer(0),
            ],
        );
    }

    #[test]
    fn test_query_case_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query("SELECT CASE WHEN 1 THEN 'yes' ELSE 'no' END;")
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Text("yes".to_owned())],
        );
    }

    #[test]
    fn test_query_negation_expression() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT -42;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(-42)]);
    }

    #[test]
    fn test_query_null_is_null() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT NULL IS NULL;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(1)]);
    }

    #[test]
    fn test_query_where_true_returns_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 41 + 1 WHERE 1;").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(42)]);
    }

    #[test]
    fn test_query_where_false_filters_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 1 WHERE 0;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_query_where_null_filters_row() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("SELECT 1 WHERE NULL;").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_values_multiple_rows() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn.query("VALUES (1, 'a'), (2, 'b');").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(1), SqliteValue::Text("a".to_owned())]
        );
        assert_eq!(
            row_values(&rows[1]),
            vec![SqliteValue::Integer(2), SqliteValue::Text("b".to_owned())]
        );
    }

    #[test]
    fn test_values_mismatched_column_count_rejected() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query("VALUES (1), (2, 3);")
            .expect_err("mismatched VALUES row widths must fail");
        assert!(matches!(error, FrankenError::ParseError { .. }));
    }

    #[test]
    fn test_query_with_params_numbered_placeholders() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT ?1 + ?2, ?3 WHERE ?4;",
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(5),
                    SqliteValue::Text("ok".to_owned()),
                    SqliteValue::Integer(1),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(7), SqliteValue::Text("ok".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_missing_required_param_rejected() {
        let conn = Connection::open(":memory:").unwrap();
        let error = conn
            .query_with_params("SELECT ?1 + ?2;", &[SqliteValue::Integer(1)])
            .expect_err("missing bind param should fail");
        assert!(matches!(error, FrankenError::OutOfRange { .. }));
    }

    #[test]
    fn test_prepared_statement_query_with_params() {
        let conn = Connection::open(":memory:").unwrap();
        let stmt = conn.prepare("SELECT ?1 + 1;").unwrap();
        let rows = stmt.query_with_params(&[SqliteValue::Integer(9)]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(row_values(&rows[0]), vec![SqliteValue::Integer(10)]);
    }

    #[test]
    fn test_query_with_params_anonymous_placeholders_are_distinct() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT ? + ?, ?;",
                &[
                    SqliteValue::Integer(2),
                    SqliteValue::Integer(5),
                    SqliteValue::Text("ok".to_owned()),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![SqliteValue::Integer(7), SqliteValue::Text("ok".to_owned())]
        );
    }

    #[test]
    fn test_query_with_params_mixed_numbered_and_anonymous() {
        let conn = Connection::open(":memory:").unwrap();
        let rows = conn
            .query_with_params(
                "SELECT ?2, ?, ?3, ?;",
                &[
                    SqliteValue::Integer(10),
                    SqliteValue::Integer(20),
                    SqliteValue::Integer(30),
                    SqliteValue::Integer(40),
                ],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            row_values(&rows[0]),
            vec![
                SqliteValue::Integer(20),
                SqliteValue::Integer(30),
                SqliteValue::Integer(30),
                SqliteValue::Integer(40),
            ]
        );
    }
}
