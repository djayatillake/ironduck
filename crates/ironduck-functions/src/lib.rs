//! IronDuck Functions - Built-in scalar, aggregate, and window functions

pub mod aggregate;
pub mod scalar;

// TODO: Implement 400+ built-in functions
// Priority order:
// 1. Aggregate: SUM, COUNT, AVG, MIN, MAX
// 2. String: CONCAT, SUBSTRING, REPLACE, TRIM, LOWER, UPPER
// 3. Date/Time: DATE_TRUNC, DATE_PART, NOW, EXTRACT
// 4. Comparison: COALESCE, NULLIF, GREATEST, LEAST
