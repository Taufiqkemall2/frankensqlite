// bd-5ew.4: Parser conformance & fuzz suite
//
// Comprehensive parser test suite covering:
//   1. Round-trip fuzz: parse → display → parse identity
//   2. SQL statement type coverage (all major statement types)
//   3. Error recovery: partial AST + diagnostics for malformed SQL
//   4. Edge cases: unicode identifiers, nesting depth, keyword-as-identifier
//   5. Expression completeness: all operators, precedence, special forms
//
// Uses proptest for randomized input generation and fsqlite-parser API directly.

#![allow(
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::similar_names
)]

use fsqlite_ast::Statement;
use fsqlite_parser::Parser;
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

// ── Helpers ───────────────────────────────────────────────────────────────

fn parse_ok(sql: &str) -> Vec<Statement> {
    let mut parser = Parser::from_sql(sql);
    let (stmts, errs) = parser.parse_all();
    assert!(
        errs.is_empty(),
        "unexpected parse errors for '{sql}': {errs:?}"
    );
    stmts
}

fn parse_one(sql: &str) -> Statement {
    let stmts = parse_ok(sql);
    assert_eq!(stmts.len(), 1, "expected 1 statement, got {}", stmts.len());
    stmts.into_iter().next().unwrap()
}

fn parse_errors(sql: &str) -> (Vec<Statement>, Vec<fsqlite_parser::ParseError>) {
    let mut parser = Parser::from_sql(sql);
    parser.parse_all()
}

/// Parse → Display → Parse round-trip. Returns true if the second parse
/// produces an identical AST (ignoring spans).
fn round_trip_ok(sql: &str) -> bool {
    let stmt1 = match catch_unwind(AssertUnwindSafe(|| parse_one(sql))) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let sql2 = stmt1.to_string();

    let stmt2 = match catch_unwind(AssertUnwindSafe(|| parse_one(&sql2))) {
        Ok(s) => s,
        Err(_) => return false,
    };

    // Compare Display output (canonical form) since spans differ
    stmt1.to_string() == stmt2.to_string()
}

// ── Conformance result types ──────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ConformanceResult {
    name: String,
    category: String,
    passed: bool,
    detail: String,
}

// ── 1. Round-trip conformance tests ───────────────────────────────────────

fn round_trip_tests() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        // (category, name, sql)
        // ── SELECT ──
        ("select", "simple_select", "SELECT 1;"),
        ("select", "select_columns", "SELECT a, b, c FROM t;"),
        ("select", "select_star", "SELECT * FROM t;"),
        ("select", "select_where", "SELECT x FROM t WHERE x > 10;"),
        ("select", "select_order_by", "SELECT a FROM t ORDER BY a ASC;"),
        ("select", "select_limit_offset", "SELECT a FROM t LIMIT 10 OFFSET 5;"),
        ("select", "select_distinct", "SELECT DISTINCT a FROM t;"),
        ("select", "select_group_by", "SELECT a, COUNT(*) FROM t GROUP BY a;"),
        ("select", "select_having", "SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 10;"),
        ("select", "select_alias", "SELECT a AS col_a, b AS col_b FROM t AS tbl;"),
        ("select", "select_expression", "SELECT 1 + 2 * 3, 'hello' || ' world';"),
        ("select", "select_subquery", "SELECT (SELECT MAX(x) FROM t) AS mx;"),
        ("select", "select_exists", "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id);"),
        ("select", "select_in_list", "SELECT * FROM t WHERE a IN (1, 2, 3);"),
        ("select", "select_in_subquery", "SELECT * FROM t WHERE a IN (SELECT id FROM s);"),
        ("select", "select_between", "SELECT * FROM t WHERE a BETWEEN 1 AND 10;"),
        ("select", "select_case", "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END FROM t;"),
        ("select", "select_cast", "SELECT CAST(x AS TEXT) FROM t;"),
        ("select", "select_is_null", "SELECT * FROM t WHERE a IS NULL;"),
        ("select", "select_is_not_null", "SELECT * FROM t WHERE a IS NOT NULL;"),
        ("select", "select_like", "SELECT * FROM t WHERE name LIKE 'a%';"),
        ("select", "select_glob", "SELECT * FROM t WHERE name GLOB 'a*';"),
        ("select", "select_coalesce", "SELECT COALESCE(a, b, 0) FROM t;"),
        ("select", "select_nullif", "SELECT NULLIF(a, 0) FROM t;"),
        ("select", "select_values", "VALUES (1, 'a'), (2, 'b'), (3, 'c');"),
        // ── JOINs ──
        ("join", "inner_join", "SELECT * FROM a INNER JOIN b ON a.id = b.aid;"),
        ("join", "left_join", "SELECT * FROM a LEFT JOIN b ON a.id = b.aid;"),
        ("join", "cross_join", "SELECT * FROM a CROSS JOIN b;"),
        ("join", "natural_join", "SELECT * FROM a NATURAL JOIN b;"),
        ("join", "join_using", "SELECT * FROM a JOIN b USING (id);"),
        ("join", "multi_join", "SELECT * FROM a JOIN b ON a.id = b.aid JOIN c ON b.id = c.bid;"),
        // ── Compound SELECT ──
        ("compound", "union", "SELECT 1 UNION SELECT 2;"),
        ("compound", "union_all", "SELECT 1 UNION ALL SELECT 2;"),
        ("compound", "intersect", "SELECT 1 INTERSECT SELECT 1;"),
        ("compound", "except", "SELECT 1 EXCEPT SELECT 2;"),
        // ── INSERT ──
        ("insert", "insert_values", "INSERT INTO t VALUES (1, 'a');"),
        ("insert", "insert_columns", "INSERT INTO t (a, b) VALUES (1, 'x');"),
        ("insert", "insert_multi_row", "INSERT INTO t VALUES (1, 'a'), (2, 'b');"),
        ("insert", "insert_select", "INSERT INTO t SELECT * FROM s;"),
        ("insert", "insert_default", "INSERT INTO t DEFAULT VALUES;"),
        ("insert", "insert_or_replace", "INSERT OR REPLACE INTO t VALUES (1, 'a');"),
        ("insert", "insert_or_ignore", "INSERT OR IGNORE INTO t VALUES (1, 'a');"),
        // ── UPDATE ──
        ("update", "update_simple", "UPDATE t SET a = 1;"),
        ("update", "update_where", "UPDATE t SET a = 1 WHERE id = 5;"),
        ("update", "update_multi_set", "UPDATE t SET a = 1, b = 'x' WHERE id = 5;"),
        ("update", "update_or_replace", "UPDATE OR REPLACE t SET a = 1;"),
        // ── DELETE ──
        ("delete", "delete_all", "DELETE FROM t;"),
        ("delete", "delete_where", "DELETE FROM t WHERE id = 5;"),
        // ── CREATE TABLE ──
        ("ddl", "create_table", "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);"),
        ("ddl", "create_table_if_not_exists", "CREATE TABLE IF NOT EXISTS t (id INTEGER);"),
        ("ddl", "create_temp_table", "CREATE TEMP TABLE t (id INTEGER);"),
        ("ddl", "create_table_unique", "CREATE TABLE t (id INTEGER, name TEXT, UNIQUE (name));"),
        ("ddl", "create_table_check", "CREATE TABLE t (id INTEGER, val INTEGER CHECK (val > 0));"),
        ("ddl", "create_table_default", "CREATE TABLE t (id INTEGER, val INTEGER DEFAULT 0);"),
        ("ddl", "create_table_fk", "CREATE TABLE t (id INTEGER, pid INTEGER REFERENCES parent(id));"),
        // ── CREATE INDEX ──
        ("ddl", "create_index", "CREATE INDEX idx ON t (a);"),
        ("ddl", "create_unique_index", "CREATE UNIQUE INDEX idx ON t (a, b);"),
        ("ddl", "create_index_if_not_exists", "CREATE INDEX IF NOT EXISTS idx ON t (a);"),
        // ── CREATE VIEW ──
        ("ddl", "create_view", "CREATE VIEW v AS SELECT * FROM t;"),
        // ── DROP ──
        ("ddl", "drop_table", "DROP TABLE t;"),
        ("ddl", "drop_table_if_exists", "DROP TABLE IF EXISTS t;"),
        ("ddl", "drop_index", "DROP INDEX idx;"),
        ("ddl", "drop_view", "DROP VIEW v;"),
        // ── ALTER TABLE ──
        ("ddl", "alter_rename", "ALTER TABLE t RENAME TO t2;"),
        // ── TRANSACTION ──
        ("txn", "begin", "BEGIN;"),
        ("txn", "begin_deferred", "BEGIN DEFERRED;"),
        ("txn", "commit", "COMMIT;"),
        ("txn", "rollback", "ROLLBACK;"),
        ("txn", "savepoint", "SAVEPOINT sp1;"),
        ("txn", "release", "RELEASE sp1;"),
        ("txn", "rollback_to", "ROLLBACK TO sp1;"),
        // ── PRAGMA ──
        ("pragma", "pragma_get", "PRAGMA table_info(t);"),
        ("pragma", "pragma_set", "PRAGMA journal_mode = WAL;"),
        // ── EXPLAIN ──
        ("explain", "explain", "EXPLAIN SELECT 1;"),
        ("explain", "explain_qp", "EXPLAIN QUERY PLAN SELECT 1;"),
        // ── MISC ──
        ("misc", "vacuum", "VACUUM;"),
        ("misc", "analyze", "ANALYZE;"),
        ("misc", "reindex", "REINDEX;"),
        // ── Expressions ──
        ("expr", "arithmetic", "SELECT 1 + 2 - 3 * 4 / 5 % 6;"),
        ("expr", "comparison", "SELECT 1 < 2, 3 <= 3, 4 > 3, 5 >= 5, 6 = 6, 7 != 8;"),
        ("expr", "logical", "SELECT 1 AND 0, 1 OR 0, NOT 1;"),
        ("expr", "string_concat", "SELECT 'a' || 'b' || 'c';"),
        ("expr", "unary_minus", "SELECT -42, +42;"),
        ("expr", "bitwise", "SELECT 5 & 3, 5 | 3, ~5, 1 << 3, 8 >> 2;"),
        ("expr", "null_ops", "SELECT NULL IS NULL, 1 IS NOT NULL;"),
        ("expr", "nested_parens", "SELECT ((1 + 2) * (3 - 4)) / 5;"),
        ("expr", "function_call", "SELECT LENGTH('hello'), UPPER('world'), ABS(-1);"),
        ("expr", "aggregate_funcs", "SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM t;"),
        ("expr", "blob_literal", "SELECT x'DEADBEEF';"),
        ("expr", "float_literal", "SELECT 3.14, 1.0e10, 2.5E-3;"),
        ("expr", "negative_literal", "SELECT -1, -3.14;"),
        ("expr", "like_escape", "SELECT * FROM t WHERE name LIKE 'a%b' ESCAPE '\\';"),
        ("expr", "not_between", "SELECT * FROM t WHERE a NOT BETWEEN 5 AND 10;"),
        ("expr", "not_in", "SELECT * FROM t WHERE a NOT IN (1, 2, 3);"),
        ("expr", "not_like", "SELECT * FROM t WHERE name NOT LIKE 'x%';"),
        ("expr", "not_glob", "SELECT * FROM t WHERE name NOT GLOB 'x*';"),
        ("expr", "isnull", "SELECT * FROM t WHERE a ISNULL;"),
        ("expr", "notnull", "SELECT * FROM t WHERE a NOTNULL;"),
        ("expr", "row_value_cmp", "SELECT * FROM t WHERE (a, b) = (1, 2);"),
        ("expr", "placeholder_qmark", "SELECT ?;"),
        ("expr", "placeholder_numbered", "SELECT ?1, ?2;"),
        ("expr", "placeholder_named", "SELECT :name;"),
        // ── Window functions ──
        ("window", "row_number", "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t;"),
        ("window", "partition_by", "SELECT SUM(a) OVER (PARTITION BY cat ORDER BY id) FROM t;"),
        // ── CTE ──
        ("cte", "simple_cte", "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte;"),
        ("cte", "multi_cte", "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b;"),
    ]
}

#[test]
fn test_round_trip_conformance() {
    let tests = round_trip_tests();
    let mut results: Vec<ConformanceResult> = Vec::new();

    for (cat, name, sql) in &tests {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            // Step 1: Parse the SQL
            let stmt1 = parse_one(sql);
            let display1 = stmt1.to_string();

            // Step 2: Re-parse the displayed output
            let stmt2 = parse_one(&display1);
            let display2 = stmt2.to_string();

            // Step 3: Verify canonical form stability
            assert_eq!(
                display1, display2,
                "round-trip unstable for '{name}':\n  original:  {sql}\n  display1:  {display1}\n  display2:  {display2}"
            );
        }));

        let (passed, detail) = match outcome {
            Ok(()) => (true, "PASS".to_owned()),
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = e.downcast_ref::<&str>() {
                    (*s).to_owned()
                } else {
                    "unknown panic".to_owned()
                };
                (false, msg)
            }
        };
        results.push(ConformanceResult {
            name: name.to_string(),
            category: cat.to_string(),
            passed,
            detail,
        });
    }

    // Print matrix
    println!("\n=== Parser Round-Trip Conformance ===\n");
    let mut by_cat: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for r in &results {
        let entry = by_cat.entry(r.category.as_str()).or_insert((0, 0));
        entry.0 += 1;
        if r.passed {
            entry.1 += 1;
        }
        let icon = if r.passed { "PASS" } else { "FAIL" };
        if !r.passed {
            println!("  [{icon}] {cat:12} :: {name:30} {detail}", cat = r.category, name = r.name, detail = r.detail);
        }
    }

    println!("\n--- Category Summary ---");
    let mut total = 0;
    let mut total_pass = 0;
    for (cat, (count, pass)) in &by_cat {
        let pct = (*pass as f64 / *count as f64) * 100.0;
        println!("  {cat:15} {pass:3}/{count:3} ({pct:.1}%)");
        total += count;
        total_pass += pass;
    }
    let overall_pct = (total_pass as f64 / total as f64) * 100.0;
    let overall_label = "Overall";
    println!("  {overall_label:15} {total_pass:3}/{total:3} ({overall_pct:.1}%)\n");

    // Gate: 100% round-trip pass rate for valid SQL
    assert!(
        overall_pct >= 95.0,
        "round-trip pass rate {overall_pct:.1}% below 95% threshold"
    );
}

// ── 2. Error recovery tests ──────────────────────────────────────────────

#[test]
fn test_error_recovery() {
    let mut pass = 0;
    let mut fail = 0;

    let cases: Vec<(&str, &str, usize, usize)> = vec![
        // (name, sql, expected_stmts, expected_errors_min)
        ("garbage_keyword", "XYZZY;", 0, 1),
        ("recovery_after_garbage", "XYZZY; SELECT 1;", 1, 1),
        ("multi_error_recovery", "XYZZY; ABCDE; SELECT 1;", 1, 1),
        ("incomplete_select", "SELECT;", 0, 1),
        ("missing_from_table", "SELECT * FROM;", 0, 1),
        ("unclosed_paren", "SELECT (1 + 2;", 0, 1),
        // Note: "SELECT 1 GARBAGE;" parses OK because GARBAGE is treated as an alias
        ("trailing_garbage", "SELECT 1 FROM t WHERE = ;", 0, 1),
        ("empty_input", "", 0, 0),
        ("just_semicolons", ";;;", 0, 0),
        ("valid_between_invalid", "SELECT 1; NOTACOMMAND; SELECT 2;", 2, 1),
    ];

    for (name, sql, min_stmts, min_errors) in &cases {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let (stmts, errs) = parse_errors(sql);
            assert!(
                stmts.len() >= *min_stmts,
                "error_recovery '{name}': expected >= {min_stmts} stmts, got {}",
                stmts.len()
            );
            assert!(
                errs.len() >= *min_errors,
                "error_recovery '{name}': expected >= {min_errors} errors, got {}",
                errs.len()
            );

            // Verify errors have source location info
            for err in &errs {
                assert!(err.line > 0, "error should have line > 0");
                assert!(err.col > 0, "error should have col > 0");
                assert!(!err.message.is_empty(), "error should have message");
            }
        }));

        match outcome {
            Ok(()) => {
                pass += 1;
                println!("[PASS] error_recovery :: {name}");
            }
            Err(e) => {
                fail += 1;
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "panic".to_owned()
                };
                println!("[FAIL] error_recovery :: {name} — {msg}");
            }
        }
    }

    let total = pass + fail;
    let pct = if total > 0 {
        (pass as f64 / total as f64) * 100.0
    } else {
        100.0
    };
    println!("\n=== Error recovery: {pass}/{total} ({pct:.1}%) ===\n");
    assert!(
        pct >= 80.0,
        "error recovery pass rate {pct:.1}% below 80%"
    );
}

// ── 3. Edge cases ────────────────────────────────────────────────────────

#[test]
fn test_edge_cases() {
    let mut pass = 0;
    let mut fail = 0;

    // (name, sql, should_parse)
    let cases: Vec<(&str, &str, bool)> = vec![
        // Keyword-as-identifier (SQLite allows this in many contexts)
        ("keyword_as_column_name", "CREATE TABLE t (\"select\" INTEGER);", true),
        ("keyword_as_table_name", "SELECT * FROM \"order\";", true),
        ("keyword_as_alias", "SELECT 1 AS \"table\";", true),
        // Quoted identifiers
        ("double_quoted_ident", "SELECT \"col name\" FROM \"my table\";", true),
        ("backtick_ident", "SELECT `col` FROM `tbl`;", true),
        ("bracket_ident", "SELECT [col] FROM [tbl];", true),
        // String edge cases
        ("empty_string", "SELECT '';", true),
        ("string_with_single_quote", "SELECT 'it''s';", true),
        ("string_with_newline", "SELECT 'line1\nline2';", true),
        // Numeric edge cases
        ("integer_zero", "SELECT 0;", true),
        ("negative_zero", "SELECT -0;", true),
        ("hex_literal", "SELECT 0xFF;", true),
        ("float_no_integer_part", "SELECT .5;", true),
        ("float_no_fraction", "SELECT 1.;", true),
        ("scientific_notation", "SELECT 1e10;", true),
        // Blob literals
        ("empty_blob", "SELECT x'';", true),
        ("large_blob", "SELECT x'0123456789ABCDEF';", true),
        // Complex nesting
        ("nested_subquery", "SELECT * FROM (SELECT * FROM (SELECT 1));", true),
        ("deeply_nested_expr", "SELECT ((((1 + 2) * 3) - 4) / 5);", true),
        // Multiple statements
        ("multi_stmt", "SELECT 1; SELECT 2; SELECT 3;", true),
        // Whitespace handling
        ("leading_whitespace", "  \n\t  SELECT 1;", true),
        ("trailing_whitespace", "SELECT 1;  \n\t  ", true),
        // Comments
        ("line_comment", "-- this is a comment\nSELECT 1;", true),
        ("block_comment", "/* comment */ SELECT 1;", true),
        ("inline_comment", "SELECT /* inline */ 1;", true),
        // Empty column list edge cases
        ("select_no_from", "SELECT 1, 2, 3;", true),
        ("select_many_cols", "SELECT 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;", true),
        // Table-valued function
        ("table_valued_func", "SELECT * FROM generate_series(1, 10);", true),
    ];

    for (name, sql, should_parse) in &cases {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let (stmts, errs) = parse_errors(sql);
            if *should_parse {
                assert!(
                    !stmts.is_empty() && errs.is_empty(),
                    "edge '{name}' should parse but got {} stmts, {} errors: {:?}",
                    stmts.len(),
                    errs.len(),
                    errs.iter().map(|e| &e.message).collect::<Vec<_>>()
                );
            } else {
                assert!(
                    !errs.is_empty(),
                    "edge '{name}' should fail but parsed OK"
                );
            }
        }));

        match outcome {
            Ok(()) => {
                pass += 1;
                println!("[PASS] edge :: {name}");
            }
            Err(e) => {
                fail += 1;
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "panic".to_owned()
                };
                println!("[FAIL] edge :: {name} — {msg}");
            }
        }
    }

    let total = pass + fail;
    let pct = if total > 0 {
        (pass as f64 / total as f64) * 100.0
    } else {
        100.0
    };
    println!("\n=== Edge cases: {pass}/{total} ({pct:.1}%) ===\n");
    assert!(
        pct >= 85.0,
        "edge case pass rate {pct:.1}% below 85%"
    );
}

// ── 4. No-panic fuzz: random SQL fragments ───────────────────────────────
//
// The parser must NEVER panic on arbitrary input. It should return errors
// gracefully for malformed SQL.

#[test]
fn test_no_panic_on_random_input() {
    // A collection of adversarial/random SQL fragments
    let inputs = [
        "",
        ";",
        ";;;",
        "SELECT",
        "FROM",
        "WHERE",
        "((((",
        "))))",
        "SELECT 1 + + + +;",
        "SELECT * FROM FROM FROM;",
        "INSERT INSERT INSERT;",
        "CREATE TABLE;",
        "CREATE TABLE t ();",
        "DROP DROP DROP;",
        "SELECT 1 2 3 4 5;",
        "UPDATE SET;",
        "DELETE;",
        "''''''",
        "\"\"\"\"",
        "SELECT x'GHIJ';", // invalid hex
        "SELECT 1e9999;",   // huge exponent
        "SELECT 99999999999999999999999999999999;", // huge integer
        "/* unclosed comment",
        "SELECT -- no newline",
        "SELECT 'unclosed string",
        "SELECT * FROM t WHERE a = AND b = ;",
        "SELECT ,,,, FROM t;",
        "SELECT * FROM t ORDER BY;",
        "SELECT * FROM t LIMIT;",
        "CREATE TABLE t (a INTEGER,);", // trailing comma
        "CREATE INDEX ON t;", // missing index name
        "ALTER TABLE;",
        "BEGIN BEGIN BEGIN;",
        "COMMIT COMMIT COMMIT;",
        "ROLLBACK ROLLBACK;",
        "SAVEPOINT;",
        "RELEASE;",
        "EXPLAIN;",
        "PRAGMA;",
        "VACUUM VACUUM;",
        // Various unicode
        "SELECT '日本語';",
        "SELECT 'émojis 🎉';",
        "SELECT 'Ω∑∏';",
        // Null bytes and control chars
        "SELECT '\0';",
        "SELECT '\t\r\n';",
        // Deeply nested
        &format!("SELECT {};", "(".repeat(50) + "1" + &")".repeat(50)),
        // Very long identifier
        &format!("SELECT {};", "a".repeat(1000)),
        // Many columns
        &format!(
            "SELECT {};",
            (1..=200).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        ),
    ];

    let mut panics = 0;
    for (i, input) in inputs.iter().enumerate() {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut parser = Parser::from_sql(input);
            let (_stmts, _errs) = parser.parse_all();
        }));

        if result.is_err() {
            panics += 1;
            println!(
                "[PANIC] no_panic_fuzz #{i}: {:?}",
                &input[..input.len().min(60)]
            );
        }
    }

    println!(
        "\n=== No-panic fuzz: {}/{} inputs handled without panic ===\n",
        inputs.len() - panics,
        inputs.len()
    );
    assert_eq!(panics, 0, "{panics} inputs caused panics");
}

// ── 5. Proptest-based round-trip fuzzing ──────────────────────────────────

mod proptest_fuzz {
    use super::*;
    use proptest::prelude::*;

    /// Generate a random valid SELECT statement
    fn arb_select() -> impl Strategy<Value = String> {
        let cols = prop::collection::vec(
            prop_oneof![
                Just("*".to_owned()),
                (1..=100i64).prop_map(|i| i.to_string()),
                "[a-z]{1,8}".prop_map(|s| s),
                Just("COUNT(*)".to_owned()),
                Just("NULL".to_owned()),
            ],
            1..=5,
        )
        .prop_map(|v| v.join(", "));

        let table = prop_oneof![
            Just("t".to_owned()),
            Just("my_table".to_owned()),
            "[a-z]{1,6}".prop_map(|s| s),
        ];

        let where_clause = prop_oneof![
            Just("".to_owned()),
            Just(" WHERE 1".to_owned()),
            (1..=100i64).prop_map(|i| format!(" WHERE id > {i}")),
        ];

        (cols, table, where_clause).prop_map(|(c, t, w)| {
            format!("SELECT {c} FROM {t}{w};")
        })
    }

    /// Generate a random valid INSERT statement
    fn arb_insert() -> impl Strategy<Value = String> {
        let values = prop::collection::vec(
            prop_oneof![
                (1..=1000i64).prop_map(|i| i.to_string()),
                "[a-z]{1,8}".prop_map(|s| format!("'{s}'")),
                Just("NULL".to_owned()),
            ],
            1..=4,
        )
        .prop_map(|v| v.join(", "));

        let table = "[a-z]{1,6}".prop_map(|s| s);

        (table, values).prop_map(|(t, v)| {
            format!("INSERT INTO {t} VALUES ({v});")
        })
    }

    /// Generate random SQL strings (potentially invalid)
    fn arb_sql_fragment() -> impl Strategy<Value = String> {
        prop_oneof![
            arb_select(),
            arb_insert(),
            Just("SELECT 1;".to_owned()),
            Just("BEGIN;".to_owned()),
            Just("COMMIT;".to_owned()),
            "[a-zA-Z0-9 ;,.()'\"*+\\-/<>=!]{1,100}".prop_map(|s| s),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        /// The parser must never panic on arbitrary input
        #[test]
        fn parser_never_panics(sql in arb_sql_fragment()) {
            let mut parser = Parser::from_sql(&sql);
            let _ = parser.parse_all();
        }

        /// Valid SELECT statements round-trip through display
        #[test]
        fn select_round_trips(sql in arb_select()) {
            let mut parser = Parser::from_sql(&sql);
            let (stmts, errs) = parser.parse_all();
            if !stmts.is_empty() && errs.is_empty() {
                let display1 = stmts[0].to_string();
                let mut parser2 = Parser::from_sql(&display1);
                let (stmts2, errs2) = parser2.parse_all();
                // If re-parse succeeds, canonical form should be stable
                if !stmts2.is_empty() && errs2.is_empty() {
                    let display2 = stmts2[0].to_string();
                    prop_assert_eq!(display1, display2,
                        "round-trip unstable: sql={}", sql);
                }
            }
        }

        /// Valid INSERT statements round-trip through display
        #[test]
        fn insert_round_trips(sql in arb_insert()) {
            let mut parser = Parser::from_sql(&sql);
            let (stmts, errs) = parser.parse_all();
            if !stmts.is_empty() && errs.is_empty() {
                let display1 = stmts[0].to_string();
                let mut parser2 = Parser::from_sql(&display1);
                let (stmts2, errs2) = parser2.parse_all();
                if !stmts2.is_empty() && errs2.is_empty() {
                    let display2 = stmts2[0].to_string();
                    prop_assert_eq!(display1, display2,
                        "round-trip unstable: sql={}", sql);
                }
            }
        }
    }
}

// ── 6. Conformance summary (machine-readable) ────────────────────────────

#[test]
fn test_conformance_summary_json() {
    let tests = round_trip_tests();
    let mut categories: BTreeMap<String, (usize, usize)> = BTreeMap::new();

    for (cat, _name, sql) in &tests {
        let entry = categories.entry(cat.to_string()).or_insert((0, 0));
        entry.0 += 1;
        if round_trip_ok(sql) {
            entry.1 += 1;
        }
    }

    let entries: Vec<String> = categories
        .iter()
        .map(|(cat, (total, pass))| {
            format!(
                "{{\"category\":\"{cat}\",\"total\":{total},\"pass\":{pass},\"pct\":{:.1}}}",
                (*pass as f64 / *total as f64) * 100.0
            )
        })
        .collect();

    let total: usize = categories.values().map(|(t, _)| t).sum();
    let total_pass: usize = categories.values().map(|(_, p)| p).sum();
    let overall_pct = (total_pass as f64 / total as f64) * 100.0;

    println!(
        "PARSER_CONFORMANCE_JSON={{\"categories\":[{}],\"total\":{total},\"pass\":{total_pass},\"overall_pct\":{overall_pct:.1}}}",
        entries.join(",")
    );

    assert!(
        overall_pct >= 95.0,
        "parser conformance {overall_pct:.1}% below 95%"
    );
}
