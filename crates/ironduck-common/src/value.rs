//! Runtime value representation for IronDuck
//!
//! The `Value` enum represents any value that can be stored or computed
//! in IronDuck. It's used for:
//! - Constant expressions
//! - Parameters
//! - Function results
//! - Row values during non-vectorized operations

use crate::types::LogicalType;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::fmt;
use uuid::Uuid;

/// An interval value representing a duration
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Interval {
    /// Number of months
    pub months: i32,
    /// Number of days
    pub days: i32,
    /// Number of microseconds
    pub micros: i64,
}

impl Interval {
    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Interval {
            months,
            days,
            micros,
        }
    }

    pub fn zero() -> Self {
        Interval {
            months: 0,
            days: 0,
            micros: 0,
        }
    }
}

/// Runtime value representation
#[derive(Debug, Clone)]
pub enum Value {
    /// NULL value
    Null,

    /// Boolean value
    Boolean(bool),

    /// 8-bit signed integer
    TinyInt(i8),
    /// 16-bit signed integer
    SmallInt(i16),
    /// 32-bit signed integer
    Integer(i32),
    /// 64-bit signed integer
    BigInt(i64),
    /// 128-bit signed integer
    HugeInt(i128),

    /// Unsigned 8-bit integer
    UTinyInt(u8),
    /// Unsigned 16-bit integer
    USmallInt(u16),
    /// Unsigned 32-bit integer
    UInteger(u32),
    /// Unsigned 64-bit integer
    UBigInt(u64),
    /// Unsigned 128-bit integer
    UHugeInt(u128),

    /// 32-bit floating point
    Float(f32),
    /// 64-bit floating point
    Double(f64),

    /// Fixed-point decimal
    Decimal(Decimal),

    /// Variable-length string
    Varchar(String),
    /// Binary data
    Blob(Vec<u8>),

    /// Date value
    Date(NaiveDate),
    /// Time value
    Time(NaiveTime),
    /// Timestamp without timezone
    Timestamp(NaiveDateTime),
    /// Timestamp with timezone (stored as UTC)
    TimestampTz(DateTime<Utc>),
    /// Interval
    Interval(Interval),

    /// UUID
    Uuid(Uuid),

    /// List of values
    List(Vec<Value>),
    /// Struct with named fields
    Struct(Vec<(String, Value)>),
    /// Map of key-value pairs
    Map(Vec<(Value, Value)>),
}

impl Value {
    /// Returns the logical type of this value
    pub fn logical_type(&self) -> LogicalType {
        match self {
            Value::Null => LogicalType::Null,
            Value::Boolean(_) => LogicalType::Boolean,
            Value::TinyInt(_) => LogicalType::TinyInt,
            Value::SmallInt(_) => LogicalType::SmallInt,
            Value::Integer(_) => LogicalType::Integer,
            Value::BigInt(_) => LogicalType::BigInt,
            Value::HugeInt(_) => LogicalType::HugeInt,
            Value::UTinyInt(_) => LogicalType::UTinyInt,
            Value::USmallInt(_) => LogicalType::USmallInt,
            Value::UInteger(_) => LogicalType::UInteger,
            Value::UBigInt(_) => LogicalType::UBigInt,
            Value::UHugeInt(_) => LogicalType::UHugeInt,
            Value::Float(_) => LogicalType::Float,
            Value::Double(_) => LogicalType::Double,
            Value::Decimal(_) => LogicalType::Decimal {
                width: 38,
                scale: 0,
            },
            Value::Varchar(_) => LogicalType::Varchar,
            Value::Blob(_) => LogicalType::Blob,
            Value::Date(_) => LogicalType::Date,
            Value::Time(_) => LogicalType::Time,
            Value::Timestamp(_) => LogicalType::Timestamp,
            Value::TimestampTz(_) => LogicalType::TimestampTz,
            Value::Interval(_) => LogicalType::Interval,
            Value::Uuid(_) => LogicalType::Uuid,
            Value::List(values) => {
                let element_type = values
                    .first()
                    .map(|v| v.logical_type())
                    .unwrap_or(LogicalType::Null);
                LogicalType::List(Box::new(element_type))
            }
            Value::Struct(fields) => {
                let field_types: Vec<(String, LogicalType)> = fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.logical_type()))
                    .collect();
                LogicalType::Struct(field_types)
            }
            Value::Map(pairs) => {
                let (key_type, value_type) = pairs
                    .first()
                    .map(|(k, v)| (k.logical_type(), v.logical_type()))
                    .unwrap_or((LogicalType::Null, LogicalType::Null));
                LogicalType::Map {
                    key: Box::new(key_type),
                    value: Box::new(value_type),
                }
            }
        }
    }

    /// Returns true if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to convert this value to a boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::TinyInt(i) => Some(*i != 0),
            Value::SmallInt(i) => Some(*i != 0),
            Value::Integer(i) => Some(*i != 0),
            Value::BigInt(i) => Some(*i != 0),
            _ => None,
        }
    }

    /// Try to convert this value to an i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::TinyInt(i) => Some(*i as i64),
            Value::SmallInt(i) => Some(*i as i64),
            Value::Integer(i) => Some(*i as i64),
            Value::BigInt(i) => Some(*i),
            Value::UTinyInt(i) => Some(*i as i64),
            Value::USmallInt(i) => Some(*i as i64),
            Value::UInteger(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Try to convert this value to an f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f as f64),
            Value::Double(f) => Some(*f),
            Value::TinyInt(i) => Some(*i as f64),
            Value::SmallInt(i) => Some(*i as f64),
            Value::Integer(i) => Some(*i as f64),
            Value::BigInt(i) => Some(*i as f64),
            Value::Decimal(d) => d.to_string().parse().ok(),
            _ => None,
        }
    }

    /// Try to convert this value to a string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Varchar(s) => Some(s),
            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::TinyInt(a), Value::TinyInt(b)) => a == b,
            (Value::SmallInt(a), Value::SmallInt(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::HugeInt(a), Value::HugeInt(b)) => a == b,
            (Value::UTinyInt(a), Value::UTinyInt(b)) => a == b,
            (Value::USmallInt(a), Value::USmallInt(b)) => a == b,
            (Value::UInteger(a), Value::UInteger(b)) => a == b,
            (Value::UBigInt(a), Value::UBigInt(b)) => a == b,
            (Value::UHugeInt(a), Value::UHugeInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Varchar(a), Value::Varchar(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a == b,
            (Value::Interval(a), Value::Interval(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            (Value::Struct(a), Value::Struct(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // NULL comparisons
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            // Same-type comparisons
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::TinyInt(a), Value::TinyInt(b)) => a.partial_cmp(b),
            (Value::SmallInt(a), Value::SmallInt(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::HugeInt(a), Value::HugeInt(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b),
            (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
            (Value::Varchar(a), Value::Varchar(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::TimestampTz(a), Value::TimestampTz(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),

            // Cross-type numeric comparisons (promote to larger type)
            (Value::Integer(a), Value::BigInt(b)) => (*a as i64).partial_cmp(b),
            (Value::BigInt(a), Value::Integer(b)) => a.partial_cmp(&(*b as i64)),

            // Integer to Float/Double comparisons
            (Value::Integer(a), Value::Double(b)) => (*a as f64).partial_cmp(b),
            (Value::Double(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Integer(a), Value::Float(b)) => (*a as f32).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f32)),

            // BigInt to Float/Double comparisons
            (Value::BigInt(a), Value::Double(b)) => (*a as f64).partial_cmp(b),
            (Value::Double(a), Value::BigInt(b)) => a.partial_cmp(&(*b as f64)),
            (Value::BigInt(a), Value::Float(b)) => (*a as f32).partial_cmp(b),
            (Value::Float(a), Value::BigInt(b)) => a.partial_cmp(&(*b as f32)),

            // Float to Double comparisons
            (Value::Float(a), Value::Double(b)) => (*a as f64).partial_cmp(b),
            (Value::Double(a), Value::Float(b)) => a.partial_cmp(&(*b as f64)),

            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::TinyInt(i) => write!(f, "{}", i),
            Value::SmallInt(i) => write!(f, "{}", i),
            Value::Integer(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            Value::HugeInt(i) => write!(f, "{}", i),
            Value::UTinyInt(i) => write!(f, "{}", i),
            Value::USmallInt(i) => write!(f, "{}", i),
            Value::UInteger(i) => write!(f, "{}", i),
            Value::UBigInt(i) => write!(f, "{}", i),
            Value::UHugeInt(i) => write!(f, "{}", i),
            Value::Float(n) => write!(f, "{}", n),
            Value::Double(n) => write!(f, "{}", n),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::Varchar(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "<blob {} bytes>", b.len()),
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
            Value::Timestamp(ts) => write!(f, "{}", ts),
            Value::TimestampTz(ts) => write!(f, "{}", ts),
            Value::Interval(i) => write!(f, "{} months {} days {} us", i.months, i.days, i.micros),
            Value::Uuid(u) => write!(f, "{}", u),
            Value::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "{{")?;
                for (i, (name, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, value)?;
                }
                write!(f, "}}")
            }
            Value::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (key, value)) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} => {}", key, value)?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

// Convenient From implementations
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::BigInt(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Double(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Varchar(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Varchar(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type() {
        assert_eq!(Value::Integer(42).logical_type(), LogicalType::Integer);
        assert_eq!(
            Value::Varchar("hello".to_string()).logical_type(),
            LogicalType::Varchar
        );
        assert_eq!(Value::Null.logical_type(), LogicalType::Null);
    }

    #[test]
    fn test_value_comparison() {
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Varchar("a".to_string()) < Value::Varchar("b".to_string()));
        assert_eq!(Value::Integer(42), Value::Integer(42));
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Varchar("hello".to_string()).to_string(), "hello");
        assert_eq!(Value::Boolean(true).to_string(), "true");
    }

    #[test]
    fn test_from_conversions() {
        let v: Value = 42.into();
        assert_eq!(v, Value::Integer(42));

        let v: Value = "hello".into();
        assert_eq!(v, Value::Varchar("hello".to_string()));
    }
}
