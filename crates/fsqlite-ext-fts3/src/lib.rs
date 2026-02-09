const MATCHINFO_ALLOWED_CHARS: [char; 7] = ['p', 'c', 'n', 'a', 'l', 's', 'x'];

#[must_use]
pub const fn extension_name() -> &'static str {
    "fts3"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtsDialect {
    Fts3,
    Fts4,
}

#[must_use]
pub const fn supports_dialect(dialect: FtsDialect) -> bool {
    matches!(dialect, FtsDialect::Fts3 | FtsDialect::Fts4)
}

#[must_use]
pub const fn supports_column_level_match() -> bool {
    true
}

#[must_use]
pub const fn supports_unary_not() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTokenKind {
    Term,
    Phrase,
    And,
    Or,
    Not,
    Near,
    LParen,
    RParen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryToken {
    kind: QueryTokenKind,
    lexeme: String,
}

impl QueryToken {
    #[must_use]
    pub const fn kind(&self) -> QueryTokenKind {
        self.kind
    }

    #[must_use]
    pub fn lexeme(&self) -> &str {
        &self.lexeme
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryValidationError {
    EmptyQuery,
    UnclosedPhrase,
    UnbalancedParentheses,
    ImplicitAnd { left: String, right: String },
}

pub fn parse_query(query: &str) -> Result<Vec<QueryToken>, QueryValidationError> {
    let tokens = tokenize_query(query)?;
    validate_parentheses(&tokens)?;
    validate_explicit_and(&tokens)?;
    Ok(tokens)
}

fn tokenize_query(query: &str) -> Result<Vec<QueryToken>, QueryValidationError> {
    let mut chars = query.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_whitespace() {
            let _ = chars.next();
            continue;
        }

        if ch == '(' {
            let _ = chars.next();
            tokens.push(QueryToken {
                kind: QueryTokenKind::LParen,
                lexeme: "(".to_owned(),
            });
            continue;
        }

        if ch == ')' {
            let _ = chars.next();
            tokens.push(QueryToken {
                kind: QueryTokenKind::RParen,
                lexeme: ")".to_owned(),
            });
            continue;
        }

        if ch == '"' {
            let _ = chars.next();
            let mut phrase = String::new();
            let mut closed = false;

            for phrase_ch in chars.by_ref() {
                if phrase_ch == '"' {
                    closed = true;
                    break;
                }
                phrase.push(phrase_ch);
            }

            if !closed {
                return Err(QueryValidationError::UnclosedPhrase);
            }
            if !phrase.is_empty() {
                tokens.push(QueryToken {
                    kind: QueryTokenKind::Phrase,
                    lexeme: phrase,
                });
            }
            continue;
        }

        let mut word = String::new();
        while let Some(word_ch) = chars.peek().copied() {
            if word_ch.is_ascii_whitespace() || matches!(word_ch, '(' | ')' | '"') {
                break;
            }
            let _ = chars.next();
            word.push(word_ch);
        }

        if word.is_empty() {
            continue;
        }

        let upper = word.to_ascii_uppercase();
        let kind = match upper.as_str() {
            "AND" => QueryTokenKind::And,
            "OR" => QueryTokenKind::Or,
            "NOT" => QueryTokenKind::Not,
            "NEAR" => QueryTokenKind::Near,
            _ => QueryTokenKind::Term,
        };

        tokens.push(QueryToken { kind, lexeme: word });
    }

    if tokens.is_empty() {
        return Err(QueryValidationError::EmptyQuery);
    }
    Ok(tokens)
}

fn validate_parentheses(tokens: &[QueryToken]) -> Result<(), QueryValidationError> {
    let mut depth = 0u32;
    for token in tokens {
        match token.kind {
            QueryTokenKind::LParen => {
                depth = depth.saturating_add(1);
            }
            QueryTokenKind::RParen => {
                if depth == 0 {
                    return Err(QueryValidationError::UnbalancedParentheses);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(QueryValidationError::UnbalancedParentheses);
    }
    Ok(())
}

fn validate_explicit_and(tokens: &[QueryToken]) -> Result<(), QueryValidationError> {
    for pair in tokens.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if left_ends_expression(left.kind) && right_starts_expression(right.kind) {
            return Err(QueryValidationError::ImplicitAnd {
                left: left.lexeme.clone(),
                right: right.lexeme.clone(),
            });
        }
    }
    Ok(())
}

const fn left_ends_expression(kind: QueryTokenKind) -> bool {
    matches!(
        kind,
        QueryTokenKind::Term | QueryTokenKind::Phrase | QueryTokenKind::RParen
    )
}

const fn right_starts_expression(kind: QueryTokenKind) -> bool {
    matches!(
        kind,
        QueryTokenKind::Term | QueryTokenKind::Phrase | QueryTokenKind::LParen
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchinfoFormatError {
    EmptyFormat,
    InvalidChar(char),
    ArithmeticOverflow,
}

#[must_use]
pub const fn is_matchinfo_format_char(ch: char) -> bool {
    matches!(ch, 'p' | 'c' | 'n' | 'a' | 'l' | 's' | 'x')
}

pub fn validate_matchinfo_format(format: &str) -> Result<(), MatchinfoFormatError> {
    if format.is_empty() {
        return Err(MatchinfoFormatError::EmptyFormat);
    }

    for ch in format.chars() {
        if !MATCHINFO_ALLOWED_CHARS.contains(&ch) {
            return Err(MatchinfoFormatError::InvalidChar(ch));
        }
    }
    Ok(())
}

pub fn matchinfo_u32_width(
    format: &str,
    phrase_count: u32,
    column_count: u32,
) -> Result<u32, MatchinfoFormatError> {
    validate_matchinfo_format(format)?;

    let mut width = 0u32;
    for ch in format.chars() {
        let addend = match ch {
            'p' | 'c' | 'n' => 1,
            'a' | 'l' | 's' => column_count,
            'x' => 3u32
                .checked_mul(phrase_count)
                .and_then(|value| value.checked_mul(column_count))
                .ok_or(MatchinfoFormatError::ArithmeticOverflow)?,
            _ => return Err(MatchinfoFormatError::InvalidChar(ch)),
        };
        width = width
            .checked_add(addend)
            .ok_or(MatchinfoFormatError::ArithmeticOverflow)?;
    }

    Ok(width)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetEntry {
    pub column: u32,
    pub term: u32,
    pub byte_offset: u32,
    pub byte_length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetsParseError {
    InvalidFieldCount(usize),
    InvalidInteger(String),
}

pub fn parse_offsets(payload: &str) -> Result<Vec<OffsetEntry>, OffsetsParseError> {
    let trimmed = payload.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let values = trimmed
        .split_whitespace()
        .map(|piece| {
            piece
                .parse::<u32>()
                .map_err(|_error| OffsetsParseError::InvalidInteger(piece.to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if values.len() % 4 != 0 {
        return Err(OffsetsParseError::InvalidFieldCount(values.len()));
    }

    let entries = values
        .chunks_exact(4)
        .map(|chunk| OffsetEntry {
            column: chunk[0],
            term: chunk[1],
            byte_offset: chunk[2],
            byte_length: chunk[3],
        })
        .collect::<Vec<_>>();

    Ok(entries)
}

#[must_use]
pub fn format_offsets(entries: &[OffsetEntry]) -> String {
    entries
        .iter()
        .flat_map(|entry| {
            [
                entry.column,
                entry.term,
                entry.byte_offset,
                entry.byte_length,
            ]
        })
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{
        FtsDialect, MatchinfoFormatError, OffsetsParseError, QueryTokenKind, QueryValidationError,
        extension_name, format_offsets, matchinfo_u32_width, parse_offsets, parse_query,
        supports_column_level_match, supports_dialect, supports_unary_not,
        validate_matchinfo_format,
    };

    #[test]
    fn test_extension_name_matches_crate_suffix() {
        let expected = env!("CARGO_PKG_NAME")
            .strip_prefix("fsqlite-ext-")
            .expect("extension crates should use fsqlite-ext-* naming");
        assert_eq!(extension_name(), expected);
    }

    #[test]
    fn test_legacy_dialect_capabilities() {
        assert!(supports_dialect(FtsDialect::Fts3));
        assert!(supports_dialect(FtsDialect::Fts4));
        assert!(supports_column_level_match());
        assert!(supports_unary_not());
    }

    #[test]
    fn test_parse_query_rejects_implicit_and() {
        assert_eq!(
            parse_query("alpha beta"),
            Err(QueryValidationError::ImplicitAnd {
                left: "alpha".to_owned(),
                right: "beta".to_owned(),
            }),
        );
    }

    #[test]
    fn test_parse_query_accepts_explicit_boolean_operators() {
        let parsed =
            parse_query(r#"alpha AND "exact phrase" OR NOT gamma"#).expect("query should parse");
        let kinds = parsed
            .into_iter()
            .map(|token| token.kind())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                QueryTokenKind::Term,
                QueryTokenKind::And,
                QueryTokenKind::Phrase,
                QueryTokenKind::Or,
                QueryTokenKind::Not,
                QueryTokenKind::Term,
            ]
        );
    }

    #[test]
    fn test_parse_query_accepts_unary_not() {
        let parsed = parse_query("NOT archive").expect("unary NOT should be accepted");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].kind(), QueryTokenKind::Not);
        assert_eq!(parsed[1].kind(), QueryTokenKind::Term);
    }

    #[test]
    fn test_parse_query_rejects_unbalanced_parentheses() {
        assert_eq!(
            parse_query("(alpha OR beta"),
            Err(QueryValidationError::UnbalancedParentheses)
        );
    }

    #[test]
    fn test_validate_matchinfo_format_allowed_set() {
        assert_eq!(validate_matchinfo_format("pcnalsx"), Ok(()));
        assert_eq!(
            validate_matchinfo_format("pcz"),
            Err(MatchinfoFormatError::InvalidChar('z')),
        );
    }

    #[test]
    fn test_matchinfo_u32_width_for_x_triplets() {
        let width = matchinfo_u32_width("pcx", 2, 3).expect("width should compute");
        assert_eq!(width, 20);
    }

    #[test]
    fn test_parse_offsets_roundtrip() {
        let payload = "0 1 15 4 1 0 22 5";
        let parsed = parse_offsets(payload).expect("payload should parse");
        assert_eq!(parsed.len(), 2);
        assert_eq!(format_offsets(&parsed), payload);
    }

    #[test]
    fn test_parse_offsets_rejects_invalid_shape() {
        assert_eq!(
            parse_offsets("0 1 15"),
            Err(OffsetsParseError::InvalidFieldCount(3))
        );
    }

    #[test]
    fn test_parse_offsets_rejects_non_numeric() {
        assert_eq!(
            parse_offsets("0 1 NaN 4"),
            Err(OffsetsParseError::InvalidInteger("NaN".to_owned()))
        );
    }
}
