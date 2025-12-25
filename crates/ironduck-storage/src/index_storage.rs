//! B-tree index storage for efficient lookups

use ironduck_common::Value;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Storage for all indexes in memory
pub struct IndexStorage {
    /// Index name -> index data (schema.index_name -> BTreeIndex)
    indexes: RwLock<HashMap<String, Arc<BTreeIndex>>>,
}

impl IndexStorage {
    pub fn new() -> Self {
        IndexStorage {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Create a normalized key
    fn make_key(schema: &str, index_name: &str) -> String {
        format!("{}.{}", schema.to_lowercase(), index_name.to_lowercase())
    }

    /// Create a new index
    pub fn create_index(&self, schema: &str, index_name: &str, unique: bool) -> Arc<BTreeIndex> {
        let key = Self::make_key(schema, index_name);
        let mut indexes = self.indexes.write();
        let index = Arc::new(BTreeIndex::new(unique));
        indexes.insert(key, index.clone());
        index
    }

    /// Get an index
    pub fn get(&self, schema: &str, index_name: &str) -> Option<Arc<BTreeIndex>> {
        let key = Self::make_key(schema, index_name);
        self.indexes.read().get(&key).cloned()
    }

    /// Drop an index
    pub fn drop_index(&self, schema: &str, index_name: &str) -> bool {
        let key = Self::make_key(schema, index_name);
        self.indexes.write().remove(&key).is_some()
    }
}

impl Default for IndexStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// A B-tree index for efficient lookups
/// Maps key values to row IDs (positions in the table)
pub struct BTreeIndex {
    /// Whether this is a unique index
    unique: bool,
    /// The B-tree mapping: key -> set of row IDs
    /// For single-column indexes, key is the column value
    /// For multi-column indexes, key is a tuple of values
    tree: RwLock<BTreeMap<IndexKey, Vec<usize>>>,
}

/// Key for B-tree index (supports single and multi-column keys)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexKey(Vec<ComparableValue>);

/// A Value wrapper that implements Ord for B-tree storage
#[derive(Debug, Clone)]
pub struct ComparableValue(Value);

impl PartialEq for ComparableValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for ComparableValue {}

impl PartialOrd for ComparableValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Get a numeric type order for cross-type comparison
fn type_order(v: &Value) -> u8 {
    use Value::*;
    match v {
        Null => 0,
        Boolean(_) => 1,
        TinyInt(_) => 2,
        SmallInt(_) => 3,
        Integer(_) => 4,
        BigInt(_) => 5,
        HugeInt(_) => 6,
        UTinyInt(_) => 7,
        USmallInt(_) => 8,
        UInteger(_) => 9,
        UBigInt(_) => 10,
        UHugeInt(_) => 11,
        Float(_) => 12,
        Double(_) => 13,
        Varchar(_) => 14,
        Date(_) => 15,
        Time(_) => 16,
        Timestamp(_) => 17,
        TimestampTz(_) => 18,
        Interval(_) => 19,
        Uuid(_) => 20,
        Blob(_) => 21,
        List(_) => 22,
        Struct(_) => 23,
        Map(_) => 24,
        Decimal(_) => 25,
        TimeTz(_, _) => 26,
    }
}

impl Ord for ComparableValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use Value::*;

        match (&self.0, &other.0) {
            // Nulls sort first
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            // Booleans
            (Boolean(a), Boolean(b)) => a.cmp(b),

            // Integers - compare as i128 for uniformity
            (TinyInt(a), TinyInt(b)) => a.cmp(b),
            (SmallInt(a), SmallInt(b)) => a.cmp(b),
            (Integer(a), Integer(b)) => a.cmp(b),
            (BigInt(a), BigInt(b)) => a.cmp(b),
            (HugeInt(a), HugeInt(b)) => a.cmp(b),

            // Cross-type integer comparison
            (Integer(a), BigInt(b)) => (*a as i64).cmp(b),
            (BigInt(a), Integer(b)) => a.cmp(&(*b as i64)),

            // Unsigned integers
            (UTinyInt(a), UTinyInt(b)) => a.cmp(b),
            (USmallInt(a), USmallInt(b)) => a.cmp(b),
            (UInteger(a), UInteger(b)) => a.cmp(b),
            (UBigInt(a), UBigInt(b)) => a.cmp(b),
            (UHugeInt(a), UHugeInt(b)) => a.cmp(b),

            // Floats
            (Float(a), Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Double(a), Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),

            // Strings
            (Varchar(a), Varchar(b)) => a.cmp(b),

            // Dates and times
            (Date(a), Date(b)) => a.cmp(b),
            (Time(a), Time(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),

            // UUIDs
            (Uuid(a), Uuid(b)) => a.cmp(b),

            // Different types - compare by type index
            _ => type_order(&self.0).cmp(&type_order(&other.0)),
        }
    }
}

impl IndexKey {
    /// Create a key from a single value
    pub fn single(value: Value) -> Self {
        IndexKey(vec![ComparableValue(value)])
    }

    /// Create a key from multiple values
    pub fn multi(values: Vec<Value>) -> Self {
        IndexKey(values.into_iter().map(ComparableValue).collect())
    }
}

impl BTreeIndex {
    pub fn new(unique: bool) -> Self {
        BTreeIndex {
            unique,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    /// Insert a key-rowid pair
    pub fn insert(&self, key: IndexKey, row_id: usize) -> Result<(), String> {
        let mut tree = self.tree.write();

        if self.unique {
            if let Some(existing) = tree.get(&key) {
                if !existing.is_empty() {
                    return Err("Duplicate key violation".to_string());
                }
            }
        }

        tree.entry(key).or_default().push(row_id);
        Ok(())
    }

    /// Remove a key-rowid pair
    pub fn remove(&self, key: &IndexKey, row_id: usize) {
        let mut tree = self.tree.write();
        if let Some(row_ids) = tree.get_mut(key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                tree.remove(key);
            }
        }
    }

    /// Look up exact match
    pub fn lookup(&self, key: &IndexKey) -> Vec<usize> {
        self.tree.read().get(key).cloned().unwrap_or_default()
    }

    /// Range scan: get all row IDs where key >= start and key < end
    pub fn range_scan(&self, start: Option<&IndexKey>, end: Option<&IndexKey>) -> Vec<usize> {
        let tree = self.tree.read();
        let range = match (start, end) {
            (Some(s), Some(e)) => tree.range(s.clone()..e.clone()),
            (Some(s), None) => tree.range(s.clone()..),
            (None, Some(e)) => tree.range(..e.clone()),
            (None, None) => tree.range(..),
        };

        range.flat_map(|(_, ids)| ids.iter().copied()).collect()
    }

    /// Get all entries (for rebuilding after updates)
    pub fn scan_all(&self) -> Vec<(IndexKey, Vec<usize>)> {
        self.tree.read().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.tree.write().clear();
    }

    /// Rebuild the index from table data
    pub fn rebuild(&self, column_indices: &[usize], rows: &[Vec<Value>]) -> Result<(), String> {
        self.clear();
        for (row_id, row) in rows.iter().enumerate() {
            let key_values: Vec<Value> = column_indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let key = if key_values.len() == 1 {
                IndexKey::single(key_values.into_iter().next().unwrap())
            } else {
                IndexKey::multi(key_values)
            };
            self.insert(key, row_id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_index_basic() {
        let index = BTreeIndex::new(false);

        index.insert(IndexKey::single(Value::Integer(1)), 0).unwrap();
        index.insert(IndexKey::single(Value::Integer(2)), 1).unwrap();
        index.insert(IndexKey::single(Value::Integer(1)), 2).unwrap();

        let result = index.lookup(&IndexKey::single(Value::Integer(1)));
        assert_eq!(result, vec![0, 2]);

        let result = index.lookup(&IndexKey::single(Value::Integer(2)));
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_unique_index() {
        let index = BTreeIndex::new(true);

        index.insert(IndexKey::single(Value::Integer(1)), 0).unwrap();
        let result = index.insert(IndexKey::single(Value::Integer(1)), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_range_scan() {
        let index = BTreeIndex::new(false);

        for i in 0..10 {
            index.insert(IndexKey::single(Value::Integer(i)), i as usize).unwrap();
        }

        let start = IndexKey::single(Value::Integer(3));
        let end = IndexKey::single(Value::Integer(7));
        let result = index.range_scan(Some(&start), Some(&end));

        assert_eq!(result, vec![3, 4, 5, 6]);
    }
}
