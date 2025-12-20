//! Logical type system for IronDuck
//!
//! This module defines the type system that matches DuckDB's semantics.
//! Every value in IronDuck has a LogicalType that determines how it's stored,
//! compared, and operated upon.

use std::fmt;

/// The logical type of a value in IronDuck.
/// This matches DuckDB's type system for compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalType {
    // ============================================
    // Null type
    // ============================================
    /// The NULL type - represents absence of value
    Null,

    // ============================================
    // Boolean
    // ============================================
    /// Boolean (true/false)
    Boolean,

    // ============================================
    // Numeric types
    // ============================================
    /// 8-bit signed integer (-128 to 127)
    TinyInt,
    /// 16-bit signed integer (-32768 to 32767)
    SmallInt,
    /// 32-bit signed integer
    Integer,
    /// 64-bit signed integer
    BigInt,
    /// 128-bit signed integer
    HugeInt,
    /// Unsigned 8-bit integer (0 to 255)
    UTinyInt,
    /// Unsigned 16-bit integer (0 to 65535)
    USmallInt,
    /// Unsigned 32-bit integer
    UInteger,
    /// Unsigned 64-bit integer
    UBigInt,
    /// Unsigned 128-bit integer
    UHugeInt,

    // ============================================
    // Floating point
    // ============================================
    /// 32-bit IEEE 754 floating point
    Float,
    /// 64-bit IEEE 754 floating point
    Double,

    // ============================================
    // Fixed-point decimal
    // ============================================
    /// Fixed-point decimal with specified width and scale
    /// - width: total number of digits (1-38)
    /// - scale: digits after decimal point
    Decimal { width: u8, scale: u8 },

    // ============================================
    // String types
    // ============================================
    /// Variable-length string (UTF-8)
    Varchar,
    /// Binary large object
    Blob,

    // ============================================
    // Date/Time types
    // ============================================
    /// Date (year, month, day)
    Date,
    /// Time of day (hour, minute, second, microsecond)
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone (stored as UTC)
    TimestampTz,
    /// Time interval
    Interval,

    // ============================================
    // Other scalar types
    // ============================================
    /// UUID (128-bit universally unique identifier)
    Uuid,

    // ============================================
    // Nested types
    // ============================================
    /// Variable-length list of elements of the same type
    List(Box<LogicalType>),

    /// Fixed-size array
    Array {
        element_type: Box<LogicalType>,
        size: u64,
    },

    /// Struct with named fields
    Struct(Vec<(String, LogicalType)>),

    /// Map from key type to value type
    Map {
        key: Box<LogicalType>,
        value: Box<LogicalType>,
    },

    /// Union of multiple types
    Union(Vec<LogicalType>),

    /// Enum with named values
    Enum(Vec<String>),

    // ============================================
    // Special types
    // ============================================
    /// Any type - used in function signatures for polymorphic functions
    Any,

    /// Unknown type - used during type inference before resolution
    Unknown,
}

impl LogicalType {
    /// Returns true if this type is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal { .. }
        )
    }

    /// Returns true if this type is an integer type
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::UTinyInt
                | LogicalType::USmallInt
                | LogicalType::UInteger
                | LogicalType::UBigInt
                | LogicalType::UHugeInt
        )
    }

    /// Returns true if this type is a floating point type
    pub fn is_floating_point(&self) -> bool {
        matches!(self, LogicalType::Float | LogicalType::Double)
    }

    /// Returns true if this type is a signed type
    pub fn is_signed(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal { .. }
        )
    }

    /// Returns true if this type is a nested type (list, struct, map, etc.)
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            LogicalType::List(_)
                | LogicalType::Array { .. }
                | LogicalType::Struct(_)
                | LogicalType::Map { .. }
                | LogicalType::Union(_)
        )
    }

    /// Returns true if this type is a temporal type
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            LogicalType::Date
                | LogicalType::Time
                | LogicalType::Timestamp
                | LogicalType::TimestampTz
                | LogicalType::Interval
        )
    }

    /// Returns the size in bytes for fixed-size types, None for variable-size
    pub fn physical_size(&self) -> Option<usize> {
        match self {
            LogicalType::Boolean => Some(1),
            LogicalType::TinyInt | LogicalType::UTinyInt => Some(1),
            LogicalType::SmallInt | LogicalType::USmallInt => Some(2),
            LogicalType::Integer | LogicalType::UInteger | LogicalType::Float => Some(4),
            LogicalType::BigInt | LogicalType::UBigInt | LogicalType::Double => Some(8),
            LogicalType::HugeInt | LogicalType::UHugeInt | LogicalType::Uuid => Some(16),
            LogicalType::Date => Some(4),
            LogicalType::Time => Some(8),
            LogicalType::Timestamp | LogicalType::TimestampTz => Some(8),
            LogicalType::Interval => Some(16),
            LogicalType::Decimal { width, .. } => {
                if *width <= 9 {
                    Some(4)
                } else if *width <= 18 {
                    Some(8)
                } else {
                    Some(16)
                }
            }
            // Variable-size types
            LogicalType::Varchar | LogicalType::Blob => None,
            LogicalType::List(_)
            | LogicalType::Array { .. }
            | LogicalType::Struct(_)
            | LogicalType::Map { .. }
            | LogicalType::Union(_)
            | LogicalType::Enum(_) => None,
            // Special types
            LogicalType::Null | LogicalType::Any | LogicalType::Unknown => None,
        }
    }

    /// Get the default value for this type (used for NULL coalescing, etc.)
    pub fn default_value(&self) -> Option<super::Value> {
        use super::Value;
        match self {
            LogicalType::Boolean => Some(Value::Boolean(false)),
            LogicalType::TinyInt => Some(Value::TinyInt(0)),
            LogicalType::SmallInt => Some(Value::SmallInt(0)),
            LogicalType::Integer => Some(Value::Integer(0)),
            LogicalType::BigInt => Some(Value::BigInt(0)),
            LogicalType::Float => Some(Value::Float(0.0)),
            LogicalType::Double => Some(Value::Double(0.0)),
            LogicalType::Varchar => Some(Value::Varchar(String::new())),
            _ => None,
        }
    }

    /// Returns the maximum precision for this type if applicable
    pub fn max_precision(&self) -> Option<u8> {
        match self {
            LogicalType::Decimal { width, .. } => Some(*width),
            _ => None,
        }
    }

    /// Try to find a common supertype for two types
    pub fn common_supertype(&self, other: &LogicalType) -> Option<LogicalType> {
        if self == other {
            return Some(self.clone());
        }

        // NULL can be cast to any type
        if *self == LogicalType::Null {
            return Some(other.clone());
        }
        if *other == LogicalType::Null {
            return Some(self.clone());
        }

        // Numeric type promotion
        if self.is_numeric() && other.is_numeric() {
            return Some(self.promote_numeric(other));
        }

        None
    }

    /// Promote two numeric types to a common type
    fn promote_numeric(&self, other: &LogicalType) -> LogicalType {
        // If either is floating point, result is floating point
        if self.is_floating_point() || other.is_floating_point() {
            return LogicalType::Double;
        }

        // For integers, use the larger type
        let self_size = self.physical_size().unwrap_or(0);
        let other_size = other.physical_size().unwrap_or(0);

        if self_size >= other_size {
            self.clone()
        } else {
            other.clone()
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Null => write!(f, "NULL"),
            LogicalType::Boolean => write!(f, "BOOLEAN"),
            LogicalType::TinyInt => write!(f, "TINYINT"),
            LogicalType::SmallInt => write!(f, "SMALLINT"),
            LogicalType::Integer => write!(f, "INTEGER"),
            LogicalType::BigInt => write!(f, "BIGINT"),
            LogicalType::HugeInt => write!(f, "HUGEINT"),
            LogicalType::UTinyInt => write!(f, "UTINYINT"),
            LogicalType::USmallInt => write!(f, "USMALLINT"),
            LogicalType::UInteger => write!(f, "UINTEGER"),
            LogicalType::UBigInt => write!(f, "UBIGINT"),
            LogicalType::UHugeInt => write!(f, "UHUGEINT"),
            LogicalType::Float => write!(f, "FLOAT"),
            LogicalType::Double => write!(f, "DOUBLE"),
            LogicalType::Decimal { width, scale } => write!(f, "DECIMAL({},{})", width, scale),
            LogicalType::Varchar => write!(f, "VARCHAR"),
            LogicalType::Blob => write!(f, "BLOB"),
            LogicalType::Date => write!(f, "DATE"),
            LogicalType::Time => write!(f, "TIME"),
            LogicalType::Timestamp => write!(f, "TIMESTAMP"),
            LogicalType::TimestampTz => write!(f, "TIMESTAMPTZ"),
            LogicalType::Interval => write!(f, "INTERVAL"),
            LogicalType::Uuid => write!(f, "UUID"),
            LogicalType::List(inner) => write!(f, "{}[]", inner),
            LogicalType::Array { element_type, size } => write!(f, "{}[{}]", element_type, size),
            LogicalType::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, (name, ty)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", name, ty)?;
                }
                write!(f, ")")
            }
            LogicalType::Map { key, value } => write!(f, "MAP({}, {})", key, value),
            LogicalType::Union(types) => {
                write!(f, "UNION(")?;
                for (i, ty) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", ty)?;
                }
                write!(f, ")")
            }
            LogicalType::Enum(values) => {
                write!(f, "ENUM(")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}'", v)?;
                }
                write!(f, ")")
            }
            LogicalType::Any => write!(f, "ANY"),
            LogicalType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl Default for LogicalType {
    fn default() -> Self {
        LogicalType::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_numeric() {
        assert!(LogicalType::Integer.is_numeric());
        assert!(LogicalType::Double.is_numeric());
        assert!(LogicalType::Decimal { width: 10, scale: 2 }.is_numeric());
        assert!(!LogicalType::Varchar.is_numeric());
        assert!(!LogicalType::Boolean.is_numeric());
    }

    #[test]
    fn test_physical_size() {
        assert_eq!(LogicalType::Boolean.physical_size(), Some(1));
        assert_eq!(LogicalType::Integer.physical_size(), Some(4));
        assert_eq!(LogicalType::BigInt.physical_size(), Some(8));
        assert_eq!(LogicalType::Varchar.physical_size(), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(LogicalType::Integer.to_string(), "INTEGER");
        assert_eq!(
            LogicalType::Decimal { width: 10, scale: 2 }.to_string(),
            "DECIMAL(10,2)"
        );
        assert_eq!(
            LogicalType::List(Box::new(LogicalType::Integer)).to_string(),
            "INTEGER[]"
        );
    }
}
