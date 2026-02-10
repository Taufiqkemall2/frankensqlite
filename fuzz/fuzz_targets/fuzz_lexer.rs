#![no_main]

//! Fuzz the SQL lexer/tokenizer with arbitrary byte input.
//!
//! The lexer must never panic. It should gracefully handle any input
//! including malformed Unicode, unterminated strings, invalid escape
//! sequences, and extremely long tokens.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };

    if input.len() > 8192 {
        return;
    }

    // Tokenize must never panic.
    let tokens = fsqlite_parser::Lexer::tokenize(input);

    // Every token's kind must be inspectable without panic.
    for token in &tokens {
        let _ = format!("{:?}", token.kind);
        let _ = token.kind.to_sql();
    }
});
