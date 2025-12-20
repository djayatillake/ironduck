//! DataChunk - A batch of vectors for processing

use super::vector::{Vector, VECTOR_SIZE};
use ironduck_common::{LogicalType, Value};

/// A batch of column vectors
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// The column vectors
    pub vectors: Vec<Vector>,
    /// Number of valid rows in this chunk (up to VECTOR_SIZE)
    pub count: usize,
}

impl DataChunk {
    /// The standard vector size
    pub const STANDARD_VECTOR_SIZE: usize = VECTOR_SIZE;

    /// Create a new data chunk with the given column types
    pub fn new(types: &[LogicalType]) -> Self {
        let vectors = types
            .iter()
            .map(|t| Vector::new_flat(t.clone(), VECTOR_SIZE))
            .collect();

        DataChunk { vectors, count: 0 }
    }

    /// Create an empty data chunk with no columns
    pub fn empty() -> Self {
        DataChunk {
            vectors: Vec::new(),
            count: 0,
        }
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.vectors.len()
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.count
    }

    /// Check if this chunk is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Set the row count
    pub fn set_count(&mut self, count: usize) {
        assert!(count <= VECTOR_SIZE);
        self.count = count;
    }

    /// Get a reference to a column vector
    pub fn column(&self, idx: usize) -> &Vector {
        &self.vectors[idx]
    }

    /// Get a mutable reference to a column vector
    pub fn column_mut(&mut self, idx: usize) -> &mut Vector {
        &mut self.vectors[idx]
    }

    /// Reset this chunk for reuse
    pub fn reset(&mut self) {
        self.count = 0;
    }

    /// Create a DataChunk from row-based data
    pub fn from_rows(rows: &[Vec<Value>], types: &[LogicalType]) -> Self {
        if rows.is_empty() {
            return DataChunk::new(types);
        }

        let num_columns = types.len();
        let num_rows = rows.len().min(VECTOR_SIZE);

        let mut vectors: Vec<Vector> = types
            .iter()
            .map(|t| {
                let mut v = Vector::new_flat(t.clone(), num_rows);
                // Convert to values-based storage for simplicity
                v.data = super::vector::VectorData::Values(super::vector::ValueVector {
                    values: Vec::with_capacity(num_rows),
                });
                v
            })
            .collect();

        // Fill columns from rows
        for (row_idx, row) in rows.iter().take(num_rows).enumerate() {
            for (col_idx, value) in row.iter().enumerate() {
                if col_idx < num_columns {
                    vectors[col_idx].set_value(row_idx, value.clone());
                }
            }
        }

        DataChunk {
            vectors,
            count: num_rows,
        }
    }

    /// Convert to row-based representation
    pub fn to_rows(&self) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(self.count);
        for row_idx in 0..self.count {
            let row: Vec<Value> = self
                .vectors
                .iter()
                .map(|v| v.get_value(row_idx))
                .collect();
            rows.push(row);
        }
        rows
    }

    /// Get a single row
    pub fn get_row(&self, row_idx: usize) -> Vec<Value> {
        self.vectors
            .iter()
            .map(|v| v.get_value(row_idx))
            .collect()
    }

    /// Set a single row
    pub fn set_row(&mut self, row_idx: usize, values: &[Value]) {
        for (col_idx, value) in values.iter().enumerate() {
            if col_idx < self.vectors.len() {
                self.vectors[col_idx].set_value(row_idx, value.clone());
            }
        }
    }

    /// Append a row to the chunk, returns true if successful
    pub fn append_row(&mut self, values: &[Value]) -> bool {
        if self.count >= VECTOR_SIZE {
            return false;
        }
        self.set_row(self.count, values);
        self.count += 1;
        true
    }

    /// Get types of all columns
    pub fn types(&self) -> Vec<LogicalType> {
        self.vectors.iter().map(|v| v.logical_type.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_chunk_new() {
        let types = vec![LogicalType::Integer, LogicalType::Varchar];
        let chunk = DataChunk::new(&types);
        assert_eq!(chunk.column_count(), 2);
        assert_eq!(chunk.row_count(), 0);
    }

    #[test]
    fn test_data_chunk_set_count() {
        let types = vec![LogicalType::Integer];
        let mut chunk = DataChunk::new(&types);
        chunk.set_count(100);
        assert_eq!(chunk.row_count(), 100);
    }
}
