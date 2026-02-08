use std::cmp::Ordering;
use std::fmt;

use crate::TypeAffinity;

/// A dynamically-typed SQLite value.
///
/// Corresponds to C SQLite's `sqlite3_value` / `Mem` type. SQLite has five
/// fundamental storage classes: NULL, INTEGER, REAL, TEXT, and BLOB.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum SqliteValue {
    /// SQL NULL.
    Null,
    /// A 64-bit signed integer.
    Integer(i64),
    /// A 64-bit IEEE 754 floating-point number.
    Float(f64),
    /// A UTF-8 text string.
    Text(String),
    /// A binary large object.
    Blob(Vec<u8>),
}

impl SqliteValue {
    /// Returns the type affinity that best describes this value.
    pub const fn affinity(&self) -> TypeAffinity {
        match self {
            Self::Null | Self::Blob(_) => TypeAffinity::Blob,
            Self::Integer(_) => TypeAffinity::Integer,
            Self::Float(_) => TypeAffinity::Real,
            Self::Text(_) => TypeAffinity::Text,
        }
    }

    /// Returns true if this is a NULL value.
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to extract an integer value.
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract a float value.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Try to extract a text reference.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract a blob reference.
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    /// Convert to an integer following SQLite's type coercion rules.
    ///
    /// - NULL -> 0
    /// - Integer -> itself
    /// - Float -> truncated to i64
    /// - Text -> attempt to parse, 0 on failure
    /// - Blob -> 0
    #[allow(clippy::cast_possible_truncation)]
    pub fn to_integer(&self) -> i64 {
        match self {
            Self::Null | Self::Blob(_) => 0,
            Self::Integer(i) => *i,
            Self::Float(f) => *f as i64,
            Self::Text(s) => s.parse::<i64>().unwrap_or_else(|_| {
                // Try parsing as float first, then truncate
                s.parse::<f64>().map_or(0, |f| f as i64)
            }),
        }
    }

    /// Convert to a float following SQLite's type coercion rules.
    ///
    /// - NULL -> 0.0
    /// - Integer -> as f64
    /// - Float -> itself
    /// - Text -> attempt to parse, 0.0 on failure
    /// - Blob -> 0.0
    #[allow(clippy::cast_precision_loss)]
    pub fn to_float(&self) -> f64 {
        match self {
            Self::Null | Self::Blob(_) => 0.0,
            Self::Integer(i) => *i as f64,
            Self::Float(f) => *f,
            Self::Text(s) => s.parse::<f64>().unwrap_or(0.0),
        }
    }

    /// Convert to text following SQLite's type coercion rules.
    pub fn to_text(&self) -> String {
        match self {
            Self::Null => String::new(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => format!("{f}"),
            Self::Text(s) => s.clone(),
            Self::Blob(b) => {
                use std::fmt::Write;
                let mut hex = String::with_capacity(2 + b.len() * 2);
                hex.push_str("X'");
                for byte in b {
                    let _ = write!(hex, "{byte:02X}");
                }
                hex.push('\'');
                hex
            }
        }
    }

    /// The sort order key for NULL values (SQLite sorts NULLs first).
    const fn sort_class(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Integer(_) | Self::Float(_) => 1,
            Self::Text(_) => 2,
            Self::Blob(_) => 3,
        }
    }
}

impl fmt::Display for SqliteValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "'{s}'"),
            Self::Blob(b) => {
                f.write_str("X'")?;
                for byte in b {
                    write!(f, "{byte:02X}")?;
                }
                f.write_str("'")
            }
        }
    }
}

impl PartialEq for SqliteValue {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.partial_cmp(other), Some(Ordering::Equal))
    }
}

impl PartialOrd for SqliteValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // SQLite sort order: NULL < numeric < text < blob
        let class_a = self.sort_class();
        let class_b = other.sort_class();

        if class_a != class_b {
            return Some(class_a.cmp(&class_b));
        }

        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Integer(a), Self::Integer(b)) => Some(a.cmp(b)),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            #[allow(clippy::cast_precision_loss)]
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            #[allow(clippy::cast_precision_loss)]
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Text(a), Self::Text(b)) => Some(a.cmp(b)),
            (Self::Blob(a), Self::Blob(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

impl From<i64> for SqliteValue {
    fn from(i: i64) -> Self {
        Self::Integer(i)
    }
}

impl From<i32> for SqliteValue {
    fn from(i: i32) -> Self {
        Self::Integer(i64::from(i))
    }
}

impl From<f64> for SqliteValue {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<String> for SqliteValue {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for SqliteValue {
    fn from(s: &str) -> Self {
        Self::Text(s.to_owned())
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(b: Vec<u8>) -> Self {
        Self::Blob(b)
    }
}

impl From<&[u8]> for SqliteValue {
    fn from(b: &[u8]) -> Self {
        Self::Blob(b.to_vec())
    }
}

impl<T: Into<Self>> From<Option<T>> for SqliteValue {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn null_properties() {
        let v = SqliteValue::Null;
        assert!(v.is_null());
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
        assert_eq!(v.to_text(), "");
        assert_eq!(v.to_string(), "NULL");
    }

    #[test]
    fn integer_properties() {
        let v = SqliteValue::Integer(42);
        assert!(!v.is_null());
        assert_eq!(v.as_integer(), Some(42));
        assert_eq!(v.to_integer(), 42);
        assert_eq!(v.to_float(), 42.0);
        assert_eq!(v.to_text(), "42");
    }

    #[test]
    fn float_properties() {
        let v = SqliteValue::Float(3.14);
        assert_eq!(v.as_float(), Some(3.14));
        assert_eq!(v.to_integer(), 3);
        assert_eq!(v.to_text(), "3.14");
    }

    #[test]
    fn text_properties() {
        let v = SqliteValue::Text("hello".to_owned());
        assert_eq!(v.as_text(), Some("hello"));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
    }

    #[test]
    fn text_numeric_coercion() {
        let v = SqliteValue::Text("123".to_owned());
        assert_eq!(v.to_integer(), 123);
        assert_eq!(v.to_float(), 123.0);

        let v = SqliteValue::Text("3.14".to_owned());
        assert_eq!(v.to_integer(), 3);
        assert_eq!(v.to_float(), 3.14);
    }

    #[test]
    fn blob_properties() {
        let v = SqliteValue::Blob(vec![0xDE, 0xAD]);
        assert_eq!(v.as_blob(), Some(&[0xDE, 0xAD][..]));
        assert_eq!(v.to_integer(), 0);
        assert_eq!(v.to_float(), 0.0);
        assert_eq!(v.to_text(), "X'DEAD'");
    }

    #[test]
    fn display_formatting() {
        assert_eq!(SqliteValue::Null.to_string(), "NULL");
        assert_eq!(SqliteValue::Integer(42).to_string(), "42");
        assert_eq!(SqliteValue::Integer(-1).to_string(), "-1");
        assert_eq!(SqliteValue::Float(1.5).to_string(), "1.5");
        assert_eq!(SqliteValue::Text("hi".to_owned()).to_string(), "'hi'");
        assert_eq!(SqliteValue::Blob(vec![0xCA, 0xFE]).to_string(), "X'CAFE'");
    }

    #[test]
    fn sort_order_null_first() {
        let null = SqliteValue::Null;
        let int = SqliteValue::Integer(0);
        let text = SqliteValue::Text(String::new());
        let blob = SqliteValue::Blob(vec![]);

        assert!(null < int);
        assert!(int < text);
        assert!(text < blob);
    }

    #[test]
    fn sort_order_integers() {
        let a = SqliteValue::Integer(1);
        let b = SqliteValue::Integer(2);
        assert!(a < b);
        assert_eq!(a.partial_cmp(&a), Some(Ordering::Equal));
    }

    #[test]
    fn sort_order_mixed_numeric() {
        let int = SqliteValue::Integer(1);
        let float = SqliteValue::Float(1.5);
        assert!(int < float);

        let int = SqliteValue::Integer(2);
        assert!(int > float);
    }

    #[test]
    fn from_conversions() {
        assert_eq!(SqliteValue::from(42i64).as_integer(), Some(42));
        assert_eq!(SqliteValue::from(42i32).as_integer(), Some(42));
        assert_eq!(SqliteValue::from(1.5f64).as_float(), Some(1.5));
        assert_eq!(SqliteValue::from("hello").as_text(), Some("hello"));
        assert_eq!(
            SqliteValue::from(String::from("world")).as_text(),
            Some("world")
        );
        assert_eq!(SqliteValue::from(vec![1u8, 2]).as_blob(), Some(&[1, 2][..]));
        assert!(SqliteValue::from(None::<i64>).is_null());
        assert_eq!(SqliteValue::from(Some(42i64)).as_integer(), Some(42));
    }

    #[test]
    fn affinity() {
        assert_eq!(SqliteValue::Null.affinity(), TypeAffinity::Blob);
        assert_eq!(SqliteValue::Integer(0).affinity(), TypeAffinity::Integer);
        assert_eq!(SqliteValue::Float(0.0).affinity(), TypeAffinity::Real);
        assert_eq!(
            SqliteValue::Text(String::new()).affinity(),
            TypeAffinity::Text
        );
        assert_eq!(SqliteValue::Blob(vec![]).affinity(), TypeAffinity::Blob);
    }

    #[test]
    fn null_equality() {
        // In SQLite, NULL == NULL is false, but for sorting they are equal
        let a = SqliteValue::Null;
        let b = SqliteValue::Null;
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
    }
}
