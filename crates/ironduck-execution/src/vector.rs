//! Vector - The core unit of vectorized execution
//!
//! A Vector holds up to VECTOR_SIZE values of the same type.
//! This is the fundamental data structure for columnar processing.

use ironduck_common::{LogicalType, Value};

/// Standard vector size (matches DuckDB)
pub const VECTOR_SIZE: usize = 2048;

/// A validity mask indicating which rows are NULL
#[derive(Debug, Clone)]
pub struct ValidityMask {
    /// Bit vector: 1 = valid, 0 = null
    /// None means all values are valid
    mask: Option<Vec<u64>>,
}

impl ValidityMask {
    /// Create a validity mask where all values are valid
    pub fn all_valid() -> Self {
        ValidityMask { mask: None }
    }

    /// Create a validity mask where all values are NULL
    pub fn all_null(count: usize) -> Self {
        let num_words = (count + 63) / 64;
        ValidityMask {
            mask: Some(vec![0; num_words]),
        }
    }

    /// Check if a specific row is valid (not NULL)
    pub fn is_valid(&self, idx: usize) -> bool {
        match &self.mask {
            None => true,
            Some(mask) => {
                let word_idx = idx / 64;
                let bit_idx = idx % 64;
                if word_idx >= mask.len() {
                    return true;
                }
                (mask[word_idx] >> bit_idx) & 1 == 1
            }
        }
    }

    /// Set a row as valid or NULL
    pub fn set(&mut self, idx: usize, valid: bool) {
        if self.mask.is_none() && !valid {
            // Need to allocate mask
            let num_words = (idx / 64) + 1;
            self.mask = Some(vec![u64::MAX; num_words]);
        }

        if let Some(mask) = &mut self.mask {
            let word_idx = idx / 64;
            let bit_idx = idx % 64;

            // Extend if needed
            while word_idx >= mask.len() {
                mask.push(u64::MAX);
            }

            if valid {
                mask[word_idx] |= 1 << bit_idx;
            } else {
                mask[word_idx] &= !(1 << bit_idx);
            }
        }
    }

    /// Returns true if all values are valid
    pub fn all_valid_flag(&self) -> bool {
        self.mask.is_none()
    }
}

impl Default for ValidityMask {
    fn default() -> Self {
        Self::all_valid()
    }
}

/// Value-based vector (stores Values directly - simpler but less efficient)
#[derive(Debug, Clone)]
pub struct ValueVector {
    pub values: Vec<Value>,
}

/// The type of vector storage
#[derive(Debug, Clone)]
pub enum VectorData {
    /// Flat array of values (raw bytes)
    Flat(FlatVector),
    /// Dictionary-encoded values
    Dictionary(DictionaryVector),
    /// Constant value repeated for all rows
    Constant(ConstantVector),
    /// Value-based storage (simpler, less efficient)
    Values(ValueVector),
}

/// Flat vector with concrete storage
#[derive(Debug, Clone)]
pub struct FlatVector {
    /// Raw bytes storage
    pub data: Vec<u8>,
}

impl FlatVector {
    pub fn new(capacity_bytes: usize) -> Self {
        FlatVector {
            data: vec![0; capacity_bytes],
        }
    }
}

/// Dictionary-encoded vector
#[derive(Debug, Clone)]
pub struct DictionaryVector {
    /// Indices into the dictionary
    pub indices: Vec<u32>,
    /// The dictionary (unique values)
    pub dictionary: Box<Vector>,
}

/// Constant vector (same value for all rows)
#[derive(Debug, Clone)]
pub struct ConstantVector {
    /// The constant value
    pub value: ironduck_common::Value,
}

/// A vector of values of the same type
#[derive(Debug, Clone)]
pub struct Vector {
    /// The logical type of values in this vector
    pub logical_type: LogicalType,
    /// Validity mask for NULL handling
    pub validity: ValidityMask,
    /// The actual data
    pub data: VectorData,
}

impl Vector {
    /// Create a new flat vector with the given type
    pub fn new_flat(logical_type: LogicalType, capacity: usize) -> Self {
        let bytes_per_value = logical_type.physical_size().unwrap_or(16);
        Vector {
            logical_type,
            validity: ValidityMask::all_valid(),
            data: VectorData::Flat(FlatVector::new(capacity * bytes_per_value)),
        }
    }

    /// Create a constant vector
    pub fn new_constant(value: ironduck_common::Value) -> Self {
        let logical_type = value.logical_type();
        Vector {
            logical_type,
            validity: if value.is_null() {
                ValidityMask::all_null(1)
            } else {
                ValidityMask::all_valid()
            },
            data: VectorData::Constant(ConstantVector { value }),
        }
    }

    /// Check if this vector is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self.data, VectorData::Constant(_))
    }

    /// Get the constant value if this is a constant vector
    pub fn get_constant(&self) -> Option<&Value> {
        match &self.data {
            VectorData::Constant(c) => Some(&c.value),
            _ => None,
        }
    }

    /// Create a vector from a slice of Values
    pub fn from_values(values: &[Value], logical_type: LogicalType) -> Self {
        let mut vector = Vector {
            logical_type,
            validity: ValidityMask::all_valid(),
            data: VectorData::Flat(FlatVector {
                data: Vec::new(), // We'll store values differently
            }),
        };

        // Store as a value vector for now (simpler, works with any type)
        vector.data = VectorData::Values(ValueVector {
            values: values.to_vec(),
        });

        // Set null bits
        for (i, v) in values.iter().enumerate() {
            if v.is_null() {
                vector.validity.set(i, false);
            }
        }

        vector
    }

    /// Get value at index
    pub fn get_value(&self, idx: usize) -> Value {
        if !self.validity.is_valid(idx) {
            return Value::Null;
        }

        match &self.data {
            VectorData::Constant(c) => c.value.clone(),
            VectorData::Values(v) => v.values.get(idx).cloned().unwrap_or(Value::Null),
            VectorData::Flat(_) => Value::Null, // TODO: decode from bytes
            VectorData::Dictionary(d) => {
                let dict_idx = d.indices.get(idx).copied().unwrap_or(0) as usize;
                d.dictionary.get_value(dict_idx)
            }
        }
    }

    /// Set value at index
    pub fn set_value(&mut self, idx: usize, value: Value) {
        // Ensure we have a Values storage
        if !matches!(self.data, VectorData::Values(_)) {
            self.data = VectorData::Values(ValueVector { values: Vec::new() });
        }

        if let VectorData::Values(v) = &mut self.data {
            // Extend if needed
            while v.values.len() <= idx {
                v.values.push(Value::Null);
            }

            if value.is_null() {
                self.validity.set(idx, false);
            } else {
                self.validity.set(idx, true);
            }
            v.values[idx] = value;
        }
    }

    /// Get all values as a Vec
    pub fn to_values(&self, count: usize) -> Vec<Value> {
        (0..count).map(|i| self.get_value(i)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validity_mask() {
        let mut mask = ValidityMask::all_valid();
        assert!(mask.is_valid(0));
        assert!(mask.is_valid(100));

        mask.set(5, false);
        assert!(mask.is_valid(0));
        assert!(!mask.is_valid(5));
        assert!(mask.is_valid(6));
    }

    #[test]
    fn test_constant_vector() {
        let v = Vector::new_constant(ironduck_common::Value::Integer(42));
        assert!(v.is_constant());
        assert_eq!(
            v.get_constant(),
            Some(&ironduck_common::Value::Integer(42))
        );
    }
}
