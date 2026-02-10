#![no_main]

//! Fuzz the SQL parser with arbitrary byte input.
//!
//! The parser must never panic on any input. It may return errors, but it
//! must do so gracefully. This catches panics in the recursive descent
//! parser, Pratt expression parser, and error-recovery synchronization.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 — the parser expects &str input.
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };

    // Limit input size to avoid excessive runtime on deep recursion.
    if sql.len() > 4096 {
        return;
    }

    // The parser must not panic on any SQL string.
    let mut parser = fsqlite_parser::Parser::from_sql(sql);
    let (_stmts, _errors) = parser.parse_all();
});
