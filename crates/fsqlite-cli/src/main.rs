use std::ffi::OsString;
use std::io::{self, BufRead, Write};

use fsqlite::{Connection, Row};

const DEFAULT_DB_PATH: &str = ":memory:";
const PROMPT_PRIMARY: &str = "fsqlite> ";
const PROMPT_CONTINUATION: &str = "   ...> ";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliOptions {
    db_path: String,
    command: Option<String>,
    show_help: bool,
}

fn main() {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut stdout = io::stdout();
    let mut stderr = io::stderr();

    let exit_code = run(std::env::args_os(), &mut input, &mut stdout, &mut stderr);
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run<I, R, W, E>(args: I, input: &mut R, out: &mut W, err: &mut E) -> i32
where
    I: IntoIterator<Item = OsString>,
    R: BufRead,
    W: Write,
    E: Write,
{
    let options = match parse_args(args) {
        Ok(options) => options,
        Err(message) => {
            let _ = writeln!(err, "error: {message}");
            let _ = write_usage(err);
            return 2;
        }
    };

    if options.show_help {
        if write_usage(out).is_err() {
            return 1;
        }
        return 0;
    }

    let connection = match Connection::open(&options.db_path) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = writeln!(err, "error: {error}");
            return 1;
        }
    };

    if let Some(command) = options.command {
        return run_command(&connection, &command, out, err);
    }

    run_repl(&connection, input, out, err)
}

fn parse_args<I>(args: I) -> Result<CliOptions, String>
where
    I: IntoIterator<Item = OsString>,
{
    let mut iter = args.into_iter();
    let _argv0 = iter.next();

    let mut db_path = String::from(DEFAULT_DB_PATH);
    let mut has_path = false;
    let mut command: Option<String> = None;
    let mut show_help = false;

    while let Some(argument) = iter.next() {
        let arg = argument.to_string_lossy();
        let arg_str = arg.as_ref();

        match arg_str {
            "-h" | "--help" => {
                show_help = true;
            }
            "-c" | "--command" => {
                if command.is_some() {
                    return Err(String::from("`-c/--command` may only be provided once"));
                }
                let next = iter
                    .next()
                    .ok_or_else(|| String::from("missing SQL argument for `-c/--command`"))?;
                command = Some(next.to_string_lossy().into_owned());
            }
            _ => {
                if let Some(value) = arg_str.strip_prefix("-c=") {
                    if command.is_some() {
                        return Err(String::from("`-c/--command` may only be provided once"));
                    }
                    command = Some(value.to_owned());
                    continue;
                }

                if let Some(value) = arg_str.strip_prefix("--command=") {
                    if command.is_some() {
                        return Err(String::from("`-c/--command` may only be provided once"));
                    }
                    command = Some(value.to_owned());
                    continue;
                }

                if arg_str.starts_with('-') {
                    return Err(format!("unknown option `{arg_str}`"));
                }

                if has_path {
                    return Err(String::from(
                        "too many positional arguments; expected at most one DB path",
                    ));
                }

                db_path = arg_str.to_owned();
                has_path = true;
            }
        }
    }

    Ok(CliOptions {
        db_path,
        command,
        show_help,
    })
}

fn run_command<W, E>(connection: &Connection, command: &str, out: &mut W, err: &mut E) -> i32
where
    W: Write,
    E: Write,
{
    if execute_sql(connection, command, out, err) {
        0
    } else {
        1
    }
}

fn run_repl<R, W, E>(connection: &Connection, input: &mut R, out: &mut W, err: &mut E) -> i32
where
    R: BufRead,
    W: Write,
    E: Write,
{
    let mut pending_sql = String::new();
    let mut line_buffer = String::new();

    loop {
        let prompt = if pending_sql.trim().is_empty() {
            PROMPT_PRIMARY
        } else {
            PROMPT_CONTINUATION
        };

        if write!(out, "{prompt}").and_then(|_| out.flush()).is_err() {
            return 1;
        }

        line_buffer.clear();
        let bytes_read = match input.read_line(&mut line_buffer) {
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                let _ = writeln!(err, "error: {error}");
                return 1;
            }
        };

        if bytes_read == 0 {
            if !pending_sql.trim().is_empty() {
                let _ = execute_sql(connection, pending_sql.trim(), out, err);
            }
            return 0;
        }

        let line = line_buffer.trim_end_matches(['\n', '\r']);
        let trimmed = line.trim();

        if pending_sql.trim().is_empty() {
            if matches!(trimmed, ".exit" | ".quit") {
                return 0;
            }

            if trimmed == ".help" {
                if write_repl_help(out).is_err() {
                    return 1;
                }
                continue;
            }

            if trimmed.is_empty() {
                continue;
            }
        }

        if !pending_sql.is_empty() {
            pending_sql.push('\n');
        }
        pending_sql.push_str(line);

        if statement_complete(&pending_sql) {
            let success = execute_sql(connection, pending_sql.trim(), out, err);
            pending_sql.clear();
            if !success {
                continue;
            }
        }
    }
}

fn execute_sql<W, E>(connection: &Connection, sql: &str, out: &mut W, err: &mut E) -> bool
where
    W: Write,
    E: Write,
{
    match connection.query(sql) {
        Ok(rows) => {
            if write_rows(&rows, out).is_err() {
                let _ = writeln!(err, "error: failed writing query results");
                return false;
            }
            true
        }
        Err(error) => {
            let _ = writeln!(err, "error: {error}");
            false
        }
    }
}

fn write_rows<W>(rows: &[Row], out: &mut W) -> io::Result<()>
where
    W: Write,
{
    for row in rows {
        writeln!(out, "{}", format_row(row))?;
    }
    Ok(())
}

fn format_row(row: &Row) -> String {
    row.values()
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ")
}

fn statement_complete(buffer: &str) -> bool {
    buffer.trim_end().ends_with(';')
}

fn write_usage<W>(out: &mut W) -> io::Result<()>
where
    W: Write,
{
    writeln!(
        out,
        "Usage: fsqlite [DB_PATH] [-c|--command SQL]\n\
         \n\
         Examples:\n\
         \n\
         fsqlite\n\
         fsqlite app.db\n\
         fsqlite -c \"SELECT 1 + 2;\"\n\
         fsqlite app.db --command \"SELECT * FROM users;\"\n",
    )
}

fn write_repl_help<W>(out: &mut W) -> io::Result<()>
where
    W: Write,
{
    writeln!(
        out,
        "Dot commands:\n\
         \n\
         .help      Show this help\n\
         .quit      Exit the shell\n\
         .exit      Exit the shell\n\
         \n\
         Enter SQL statements terminated by `;`.\n",
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::io::Cursor;

    use super::{format_row, parse_args, run, statement_complete};

    fn parse_from(args: &[&str]) -> Result<super::CliOptions, String> {
        let os_args: Vec<OsString> = args.iter().map(OsString::from).collect();
        parse_args(os_args)
    }

    #[test]
    fn test_parse_defaults() {
        let options = parse_from(&["fsqlite"]).expect("default args should parse");
        assert_eq!(options.db_path, ":memory:");
        assert_eq!(options.command, None);
        assert!(!options.show_help);
    }

    #[test]
    fn test_parse_db_path_and_command() {
        let options =
            parse_from(&["fsqlite", "demo.db", "-c", "SELECT 1;"]).expect("args should parse");
        assert_eq!(options.db_path, "demo.db");
        assert_eq!(options.command.as_deref(), Some("SELECT 1;"));
    }

    #[test]
    fn test_parse_command_equals_form() {
        let options = parse_from(&["fsqlite", "--command=SELECT 2;"]).expect("args should parse");
        assert_eq!(options.command.as_deref(), Some("SELECT 2;"));
    }

    #[test]
    fn test_parse_unknown_option_fails() {
        let error = parse_from(&["fsqlite", "--wat"]).expect_err("unknown option should fail");
        assert!(error.contains("unknown option"));
    }

    #[test]
    fn test_parse_multiple_paths_fails() {
        let error = parse_from(&["fsqlite", "a.db", "b.db"])
            .expect_err("multiple positional args should fail");
        assert!(error.contains("too many positional arguments"));
    }

    #[test]
    fn test_statement_complete_requires_trailing_semicolon() {
        assert!(statement_complete("SELECT 1;"));
        assert!(statement_complete("SELECT 1;\n"));
        assert!(!statement_complete("SELECT 1"));
    }

    #[test]
    fn test_format_row_joins_with_pipes() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![
            OsString::from("fsqlite"),
            OsString::from("-c"),
            OsString::from("SELECT 1, 'x';"),
        ];
        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);

        let stdout = String::from_utf8(out).expect("output should be utf-8");
        assert!(
            stdout.contains("1 | 'x'"),
            "expected rendered row in output, got: {stdout}",
        );
    }

    #[test]
    fn test_repl_quit_command_exits_cleanly() {
        let mut input = Cursor::new(b".quit\n".to_vec());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);
    }

    #[test]
    fn test_repl_executes_statement_then_quits() {
        let mut input = Cursor::new(b"SELECT 7;\n.quit\n".to_vec());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![OsString::from("fsqlite")];

        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);
        assert!(err.is_empty(), "unexpected stderr: {:?}", err);

        let stdout = String::from_utf8(out).expect("output should be utf-8");
        assert!(stdout.contains("7"), "expected query result in output");
    }

    #[test]
    fn test_format_row_helper_with_connection_row() {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut out = Vec::new();
        let mut err = Vec::new();
        let args = vec![
            OsString::from("fsqlite"),
            OsString::from("-c"),
            OsString::from("SELECT NULL;"),
        ];
        let exit_code = run(args, &mut input, &mut out, &mut err);
        assert_eq!(exit_code, 0);

        // Also directly exercise `format_row` on a real row.
        let conn = fsqlite::Connection::open(":memory:").expect("connection should open");
        let row = conn
            .query_row("SELECT 10, 'abc', NULL;")
            .expect("query_row should succeed");
        let rendered = format_row(&row);
        assert_eq!(rendered, "10 | 'abc' | NULL");
    }
}
