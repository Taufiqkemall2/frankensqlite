#![no_main]

//! Fuzz the expression parser (Pratt precedence climber) with arbitrary input.
//!
//! Expression parsing is the most complex part of the parser due to operator
//! precedence, prefix/infix/postfix handling, and nested subexpressions.
//! This target focuses on exercising those paths.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };

    if sql.len() > 2048 {
        return;
    }

    // Wrap in SELECT so the expression parser is invoked.
    let wrapped = format!("SELECT {sql}");
    let mut parser = fsqlite_parser::Parser::from_sql(&wrapped);
    let _ = parser.parse_all();

    // Also try as a WHERE clause.
    let where_wrapped = format!("SELECT 1 WHERE {sql}");
    let mut parser2 = fsqlite_parser::Parser::from_sql(&where_wrapped);
    let _ = parser2.parse_all();
});
