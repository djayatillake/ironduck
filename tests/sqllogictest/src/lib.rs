//! SQLLogicTest runner for IronDuck
//!
//! This crate provides a test runner that can execute DuckDB's
//! extensive test suite against IronDuck.

use ironduck::Database;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TestError {
    #[error("Query execution failed: {0}")]
    ExecutionError(String),

    #[error("Result mismatch: expected {expected}, got {actual}")]
    ResultMismatch { expected: String, actual: String },

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type TestResult<T> = Result<T, TestError>;

/// A test runner for SQLLogicTest format
pub struct TestRunner {
    db: Database,
    verbose: bool,
}

impl TestRunner {
    pub fn new() -> Self {
        TestRunner {
            db: Database::new(),
            verbose: false,
        }
    }

    pub fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    /// Reset the database for a new test
    pub fn reset(&mut self) {
        self.db = Database::new();
    }

    /// Run a single test file
    pub fn run_file(&mut self, path: &str) -> TestResult<TestReport> {
        let content = fs::read_to_string(path)?;
        self.run_tests(&content)
    }

    /// Run tests from a string
    pub fn run_tests(&mut self, content: &str) -> TestResult<TestReport> {
        let mut report = TestReport::default();
        let tests = parse_slt(content)?;

        for test in tests {
            match self.run_test(&test) {
                Ok(passed) => {
                    if passed {
                        report.passed += 1;
                    } else {
                        report.failed += 1;
                    }
                }
                Err(e) => {
                    if self.verbose {
                        eprintln!("Test error: {}", e);
                    }
                    report.failed += 1;
                }
            }
        }

        Ok(report)
    }

    /// Run a single test case
    fn run_test(&mut self, test: &TestCase) -> TestResult<bool> {
        match test {
            TestCase::Statement { sql, expected_error } => {
                let result = self.db.execute(sql);
                match (result, expected_error) {
                    (Ok(_), false) => Ok(true),
                    (Err(_), true) => Ok(true),
                    (Ok(_), true) => {
                        if self.verbose {
                            eprintln!("Expected error but got success for: {}", sql);
                        }
                        Ok(false)
                    }
                    (Err(e), false) => {
                        if self.verbose {
                            eprintln!("Unexpected error for '{}': {}", sql, e);
                        }
                        Ok(false)
                    }
                }
            }
            TestCase::Query {
                sql,
                expected_results,
                sort_mode,
            } => {
                match self.db.execute(sql) {
                    Ok(result) => {
                        let mut actual: Vec<String> = result
                            .rows
                            .iter()
                            .map(|row| {
                                row.iter()
                                    .map(|v| v.to_string())
                                    .collect::<Vec<_>>()
                                    .join("\t")
                            })
                            .collect();

                        let mut expected = expected_results.clone();

                        // Apply sorting if needed
                        if *sort_mode == SortMode::RowSort {
                            actual.sort();
                            expected.sort();
                        }

                        if actual == expected {
                            Ok(true)
                        } else {
                            if self.verbose {
                                eprintln!("Query result mismatch for: {}", sql);
                                eprintln!("Expected:\n{}", expected.join("\n"));
                                eprintln!("Actual:\n{}", actual.join("\n"));
                            }
                            Ok(false)
                        }
                    }
                    Err(e) => {
                        if self.verbose {
                            eprintln!("Query error for '{}': {}", sql, e);
                        }
                        Ok(false)
                    }
                }
            }
            TestCase::Halt => {
                // Stop processing
                Ok(true)
            }
            TestCase::Skip => {
                // Skipped test
                Ok(true)
            }
        }
    }

    /// Run all test files in a directory
    pub fn run_directory(&mut self, path: &str) -> TestResult<TestReport> {
        let mut total_report = TestReport::default();

        fn visit_dir(runner: &mut TestRunner, dir: &Path, report: &mut TestReport) -> TestResult<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        visit_dir(runner, &path, report)?;
                    } else if path.extension().map_or(false, |e| e == "slt" || e == "test") {
                        runner.reset();
                        if let Ok(file_report) = runner.run_file(path.to_str().unwrap_or("")) {
                            report.passed += file_report.passed;
                            report.failed += file_report.failed;
                            report.skipped += file_report.skipped;
                        }
                    }
                }
            }
            Ok(())
        }

        visit_dir(self, Path::new(path), &mut total_report)?;
        Ok(total_report)
    }
}

impl Default for TestRunner {
    fn default() -> Self {
        Self::new()
    }
}

/// Report from running tests
#[derive(Debug, Default)]
pub struct TestReport {
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
}

impl TestReport {
    pub fn total(&self) -> usize {
        self.passed + self.failed + self.skipped
    }

    pub fn pass_rate(&self) -> f64 {
        if self.total() == 0 {
            0.0
        } else {
            self.passed as f64 / self.total() as f64
        }
    }
}

/// Sort mode for query results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    NoSort,
    RowSort,
    ValueSort,
}

/// A single test case from an SLT file
#[derive(Debug)]
pub enum TestCase {
    Statement {
        sql: String,
        expected_error: bool,
    },
    Query {
        sql: String,
        expected_results: Vec<String>,
        sort_mode: SortMode,
    },
    Halt,
    Skip,
}

/// Parse SQLLogicTest format
fn parse_slt(content: &str) -> TestResult<Vec<TestCase>> {
    let mut tests = Vec::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let line = line.trim();

        // Skip comments and empty lines
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Parse directives
        if line.starts_with("statement") {
            let expected_error = line.contains("error");
            let mut sql_lines = Vec::new();

            while let Some(sql_line) = lines.peek() {
                if sql_line.trim().is_empty() {
                    break;
                }
                sql_lines.push(lines.next().unwrap().to_string());
            }

            if !sql_lines.is_empty() {
                tests.push(TestCase::Statement {
                    sql: sql_lines.join("\n"),
                    expected_error,
                });
            }
        } else if line.starts_with("query") {
            // Parse query directive: query <types> <sort_mode> <label>
            let parts: Vec<&str> = line.split_whitespace().collect();
            let sort_mode = if parts.len() > 2 {
                match parts[2] {
                    "rowsort" => SortMode::RowSort,
                    "valuesort" => SortMode::ValueSort,
                    _ => SortMode::NoSort,
                }
            } else {
                SortMode::NoSort
            };

            // Read SQL (until ---- separator)
            let mut sql_lines = Vec::new();
            while let Some(sql_line) = lines.peek() {
                if sql_line.trim() == "----" {
                    lines.next(); // consume the ----
                    break;
                }
                if sql_line.trim().is_empty() && sql_lines.is_empty() {
                    lines.next();
                    continue;
                }
                sql_lines.push(lines.next().unwrap().to_string());
            }

            // Read expected results (until empty line or next directive)
            let mut results = Vec::new();
            while let Some(result_line) = lines.peek() {
                let trimmed = result_line.trim();
                if trimmed.is_empty()
                    || trimmed.starts_with("statement")
                    || trimmed.starts_with("query")
                    || trimmed.starts_with("halt")
                    || trimmed.starts_with("hash-threshold")
                {
                    break;
                }
                results.push(lines.next().unwrap().to_string());
            }

            if !sql_lines.is_empty() {
                tests.push(TestCase::Query {
                    sql: sql_lines.join("\n"),
                    expected_results: results,
                    sort_mode,
                });
            }
        } else if line.starts_with("halt") {
            tests.push(TestCase::Halt);
            break;
        } else if line.starts_with("skipif") || line.starts_with("onlyif") {
            // Skip conditional tests for now - skip the next statement/query
            while let Some(next) = lines.peek() {
                if next.trim().is_empty() {
                    break;
                }
                lines.next();
            }
        } else if line.starts_with("hash-threshold") {
            // Ignore hash-threshold directives
            continue;
        }
    }

    Ok(tests)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_statement() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
SELECT 1
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_simple_query() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
query I nosort
SELECT 1
----
1
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_query_with_expression() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
query I nosort
SELECT 1 + 2
----
3
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_multiple_tests() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
SELECT 1

query I nosort
SELECT 2 * 3
----
6

query T nosort
SELECT 'hello'
----
hello
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 3);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_create_and_select() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE t1 (a INTEGER, b VARCHAR)

statement ok
INSERT INTO t1 VALUES (1, 'hello')

statement ok
INSERT INTO t1 VALUES (2, 'world')

query IT nosort
SELECT a, b FROM t1
----
1	hello
2	world
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 4);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_expected_error() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement error
SELECT * FROM nonexistent_table
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 1);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_insert_select() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE source (a INTEGER, b VARCHAR)

statement ok
INSERT INTO source VALUES (1, 'one')

statement ok
INSERT INTO source VALUES (2, 'two')

statement ok
INSERT INTO source VALUES (3, 'three')

statement ok
CREATE TABLE target (x INTEGER, y VARCHAR)

statement ok
INSERT INTO target SELECT a, b FROM source

query IT nosort
SELECT x, y FROM target
----
1	one
2	two
3	three
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_insert_select_with_filter() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE numbers (n INTEGER)

statement ok
INSERT INTO numbers VALUES (1)

statement ok
INSERT INTO numbers VALUES (2)

statement ok
INSERT INTO numbers VALUES (3)

statement ok
INSERT INTO numbers VALUES (4)

statement ok
INSERT INTO numbers VALUES (5)

statement ok
CREATE TABLE evens (n INTEGER)

statement ok
INSERT INTO evens SELECT n FROM numbers WHERE n % 2 = 0

query I nosort
SELECT n FROM evens
----
2
4
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 9);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_delete() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE items (id INTEGER, name VARCHAR)

statement ok
INSERT INTO items VALUES (1, 'apple')

statement ok
INSERT INTO items VALUES (2, 'banana')

statement ok
INSERT INTO items VALUES (3, 'cherry')

statement ok
DELETE FROM items WHERE id = 2

query IT nosort
SELECT id, name FROM items
----
1	apple
3	cherry
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_delete_all() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE temp (x INTEGER)

statement ok
INSERT INTO temp VALUES (1)

statement ok
INSERT INTO temp VALUES (2)

statement ok
DELETE FROM temp

query I nosort
SELECT x FROM temp
----
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_update() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE products (id INTEGER, price INTEGER)

statement ok
INSERT INTO products VALUES (1, 100)

statement ok
INSERT INTO products VALUES (2, 200)

statement ok
INSERT INTO products VALUES (3, 300)

statement ok
UPDATE products SET price = 150 WHERE id = 1

query II nosort
SELECT id, price FROM products
----
1	150
2	200
3	300
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_update_all() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE scores (val INTEGER)

statement ok
INSERT INTO scores VALUES (10)

statement ok
INSERT INTO scores VALUES (20)

statement ok
UPDATE scores SET val = val * 2

query I rowsort
SELECT val FROM scores
----
20
40
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_left_join() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE employees (id INTEGER, name VARCHAR)

statement ok
INSERT INTO employees VALUES (1, 'Alice')

statement ok
INSERT INTO employees VALUES (2, 'Bob')

statement ok
INSERT INTO employees VALUES (3, 'Charlie')

statement ok
CREATE TABLE departments (emp_id INTEGER, dept VARCHAR)

statement ok
INSERT INTO departments VALUES (1, 'Engineering')

statement ok
INSERT INTO departments VALUES (2, 'Sales')

query ITT nosort
SELECT e.id, e.name, d.dept FROM employees e LEFT JOIN departments d ON e.id = d.emp_id
----
1	Alice	Engineering
2	Bob	Sales
3	Charlie	NULL
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_right_join() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE orders (id INTEGER, product VARCHAR)

statement ok
INSERT INTO orders VALUES (1, 'Widget')

statement ok
INSERT INTO orders VALUES (2, 'Gadget')

statement ok
CREATE TABLE customers (order_id INTEGER, customer VARCHAR)

statement ok
INSERT INTO customers VALUES (1, 'John')

statement ok
INSERT INTO customers VALUES (3, 'Jane')

query ITT nosort
SELECT o.id, o.product, c.customer FROM orders o RIGHT JOIN customers c ON o.id = c.order_id
----
1	Widget	John
NULL	NULL	Jane
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_count_distinct() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE colors (name VARCHAR, category VARCHAR)

statement ok
INSERT INTO colors VALUES ('red', 'warm')

statement ok
INSERT INTO colors VALUES ('blue', 'cool')

statement ok
INSERT INTO colors VALUES ('orange', 'warm')

statement ok
INSERT INTO colors VALUES ('green', 'cool')

statement ok
INSERT INTO colors VALUES ('yellow', 'warm')

query I nosort
SELECT COUNT(DISTINCT category) FROM colors
----
2

query I nosort
SELECT COUNT(category) FROM colors
----
5

query I nosort
SELECT COUNT(*) FROM colors
----
5
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 9);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_string_agg() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE fruits (name VARCHAR, color VARCHAR)

statement ok
INSERT INTO fruits VALUES ('apple', 'red')

statement ok
INSERT INTO fruits VALUES ('banana', 'yellow')

statement ok
INSERT INTO fruits VALUES ('cherry', 'red')

statement ok
INSERT INTO fruits VALUES ('grape', 'purple')

query T nosort
SELECT STRING_AGG(name, ', ') FROM fruits
----
apple, banana, cherry, grape

query TT rowsort
SELECT color, STRING_AGG(name, '-') FROM fruits GROUP BY color
----
purple	grape
red	apple-cherry
yellow	banana
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_count_distinct_with_group_by() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE sales (region VARCHAR, product VARCHAR)

statement ok
INSERT INTO sales VALUES ('North', 'Widget')

statement ok
INSERT INTO sales VALUES ('North', 'Gadget')

statement ok
INSERT INTO sales VALUES ('North', 'Widget')

statement ok
INSERT INTO sales VALUES ('South', 'Widget')

statement ok
INSERT INTO sales VALUES ('South', 'Widget')

query TI rowsort
SELECT region, COUNT(DISTINCT product) FROM sales GROUP BY region
----
North	2
South	1

query TI rowsort
SELECT region, COUNT(product) FROM sales GROUP BY region
----
North	3
South	2
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);  // 6 statements + 2 queries
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_union() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE t1 (x INTEGER)

statement ok
INSERT INTO t1 VALUES (1)

statement ok
INSERT INTO t1 VALUES (2)

statement ok
INSERT INTO t1 VALUES (3)

statement ok
CREATE TABLE t2 (y INTEGER)

statement ok
INSERT INTO t2 VALUES (2)

statement ok
INSERT INTO t2 VALUES (3)

statement ok
INSERT INTO t2 VALUES (4)

query I rowsort
SELECT x FROM t1 UNION SELECT y FROM t2
----
1
2
3
4

query I rowsort
SELECT x FROM t1 UNION ALL SELECT y FROM t2
----
1
2
2
3
3
4
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 10);  // 8 statements + 2 queries
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_intersect() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE a (val INTEGER)

statement ok
INSERT INTO a VALUES (1)

statement ok
INSERT INTO a VALUES (2)

statement ok
INSERT INTO a VALUES (3)

statement ok
CREATE TABLE b (val INTEGER)

statement ok
INSERT INTO b VALUES (2)

statement ok
INSERT INTO b VALUES (3)

statement ok
INSERT INTO b VALUES (4)

query I rowsort
SELECT val FROM a INTERSECT SELECT val FROM b
----
2
3
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 9);  // 8 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_except() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE first (n INTEGER)

statement ok
INSERT INTO first VALUES (1)

statement ok
INSERT INTO first VALUES (2)

statement ok
INSERT INTO first VALUES (3)

statement ok
CREATE TABLE second (n INTEGER)

statement ok
INSERT INTO second VALUES (2)

statement ok
INSERT INTO second VALUES (4)

query I rowsort
SELECT n FROM first EXCEPT SELECT n FROM second
----
1
3
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);  // 7 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_in_subquery() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE products (id INTEGER, name VARCHAR)

statement ok
INSERT INTO products VALUES (1, 'Widget')

statement ok
INSERT INTO products VALUES (2, 'Gadget')

statement ok
INSERT INTO products VALUES (3, 'Gizmo')

statement ok
CREATE TABLE popular (product_id INTEGER)

statement ok
INSERT INTO popular VALUES (1)

statement ok
INSERT INTO popular VALUES (3)

query IT rowsort
SELECT id, name FROM products WHERE id IN (SELECT product_id FROM popular)
----
1	Widget
3	Gizmo
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);  // 7 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_not_in_subquery() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE items (id INTEGER)

statement ok
INSERT INTO items VALUES (1)

statement ok
INSERT INTO items VALUES (2)

statement ok
INSERT INTO items VALUES (3)

statement ok
CREATE TABLE excluded (item_id INTEGER)

statement ok
INSERT INTO excluded VALUES (2)

query I nosort
SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM excluded)
----
1
3
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);  // 6 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_cte_basic() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE employees (id INTEGER, name VARCHAR, dept_id INTEGER)

statement ok
INSERT INTO employees VALUES (1, 'Alice', 10)

statement ok
INSERT INTO employees VALUES (2, 'Bob', 10)

statement ok
INSERT INTO employees VALUES (3, 'Charlie', 20)

statement ok
INSERT INTO employees VALUES (4, 'Diana', 20)

query IT rowsort
WITH dept10 AS (SELECT id, name FROM employees WHERE dept_id = 10)
SELECT id, name FROM dept10
----
1	Alice
2	Bob
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_cte_multiple() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE orders (id INTEGER, amount INTEGER, customer_id INTEGER)

statement ok
INSERT INTO orders VALUES (1, 100, 1)

statement ok
INSERT INTO orders VALUES (2, 200, 1)

statement ok
INSERT INTO orders VALUES (3, 150, 2)

statement ok
INSERT INTO orders VALUES (4, 300, 2)

query II rowsort
WITH
  large_orders AS (SELECT id, customer_id FROM orders WHERE amount > 150),
  customer1_orders AS (SELECT id FROM orders WHERE customer_id = 1)
SELECT id, customer_id FROM large_orders
----
2	1
4	2
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_row_number() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE scores (name VARCHAR, score INTEGER)

statement ok
INSERT INTO scores VALUES ('Alice', 100)

statement ok
INSERT INTO scores VALUES ('Bob', 85)

statement ok
INSERT INTO scores VALUES ('Charlie', 95)

statement ok
INSERT INTO scores VALUES ('Diana', 95)

query TII nosort
SELECT name, score, ROW_NUMBER() OVER (ORDER BY score DESC) as rn FROM scores
----
Alice	100	1
Bob	85	4
Charlie	95	2
Diana	95	3
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_rank() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE students (name VARCHAR, grade INTEGER)

statement ok
INSERT INTO students VALUES ('Alice', 90)

statement ok
INSERT INTO students VALUES ('Bob', 85)

statement ok
INSERT INTO students VALUES ('Charlie', 90)

statement ok
INSERT INTO students VALUES ('Diana', 80)

query TII nosort
SELECT name, grade, RANK() OVER (ORDER BY grade DESC) as rnk FROM students
----
Alice	90	1
Bob	85	3
Charlie	90	1
Diana	80	4
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_dense_rank() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE players (name VARCHAR, points INTEGER)

statement ok
INSERT INTO players VALUES ('Alice', 100)

statement ok
INSERT INTO players VALUES ('Bob', 85)

statement ok
INSERT INTO players VALUES ('Charlie', 100)

statement ok
INSERT INTO players VALUES ('Diana', 90)

query TII nosort
SELECT name, points, DENSE_RANK() OVER (ORDER BY points DESC) as drnk FROM players
----
Alice	100	1
Bob	85	3
Charlie	100	1
Diana	90	2
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_row_number_partition() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INTEGER)

statement ok
INSERT INTO sales VALUES ('North', 'Widget', 100)

statement ok
INSERT INTO sales VALUES ('North', 'Gadget', 150)

statement ok
INSERT INTO sales VALUES ('South', 'Widget', 80)

statement ok
INSERT INTO sales VALUES ('South', 'Gadget', 120)

statement ok
INSERT INTO sales VALUES ('North', 'Gizmo', 200)

query TTII nosort
SELECT region, product, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn FROM sales
----
North	Widget	100	3
North	Gadget	150	2
South	Widget	80	2
South	Gadget	120	1
North	Gizmo	200	1
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);  // 6 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_lag() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE stock_prices (day INTEGER, price INTEGER)

statement ok
INSERT INTO stock_prices VALUES (1, 100)

statement ok
INSERT INTO stock_prices VALUES (2, 105)

statement ok
INSERT INTO stock_prices VALUES (3, 102)

statement ok
INSERT INTO stock_prices VALUES (4, 110)

query III nosort
SELECT day, price, LAG(price) OVER (ORDER BY day) as prev_price FROM stock_prices
----
1	100	NULL
2	105	100
3	102	105
4	110	102
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_lead() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE temperatures (day INTEGER, temp INTEGER)

statement ok
INSERT INTO temperatures VALUES (1, 72)

statement ok
INSERT INTO temperatures VALUES (2, 75)

statement ok
INSERT INTO temperatures VALUES (3, 68)

statement ok
INSERT INTO temperatures VALUES (4, 70)

query III nosort
SELECT day, temp, LEAD(temp) OVER (ORDER BY day) as next_temp FROM temperatures
----
1	72	75
2	75	68
3	68	70
4	70	NULL
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);  // 5 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_like() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE products (name VARCHAR, category VARCHAR)

statement ok
INSERT INTO products VALUES ('Apple iPhone', 'Electronics')

statement ok
INSERT INTO products VALUES ('Apple MacBook', 'Electronics')

statement ok
INSERT INTO products VALUES ('Samsung Galaxy', 'Electronics')

statement ok
INSERT INTO products VALUES ('Apple Pie', 'Food')

query TT rowsort
SELECT name, category FROM products WHERE name LIKE 'Apple%'
----
Apple MacBook	Electronics
Apple Pie	Food
Apple iPhone	Electronics

query TT rowsort
SELECT name, category FROM products WHERE name LIKE '%Book'
----
Apple MacBook	Electronics

query TT rowsort
SELECT name, category FROM products WHERE name LIKE '%a%'
----
Apple MacBook	Electronics
Samsung Galaxy	Electronics
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);  // 5 statements + 3 queries
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_ilike() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE users (username VARCHAR)

statement ok
INSERT INTO users VALUES ('JohnDoe')

statement ok
INSERT INTO users VALUES ('janedoe')

statement ok
INSERT INTO users VALUES ('BobSmith')

query T rowsort
SELECT username FROM users WHERE username ILIKE '%doe%'
----
JohnDoe
janedoe
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);  // 4 statements + 1 query
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_string_functions() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
query T
SELECT CONCAT('Hello', ' ', 'World')
----
Hello World

query T
SELECT SUBSTRING('Hello World', 1, 5)
----
Hello

query T
SELECT SUBSTRING('Hello World', 7)
----
World

query T
SELECT LEFT('Hello World', 5)
----
Hello

query T
SELECT RIGHT('Hello World', 5)
----
World

query T
SELECT UPPER('hello')
----
HELLO

query T
SELECT LOWER('HELLO')
----
hello

query T
SELECT TRIM('  hello  ')
----
hello

query T
SELECT LTRIM('  hello')
----
hello

query T
SELECT RTRIM('hello  ')
----
hello

query I
SELECT LENGTH('hello')
----
5

query T
SELECT REPLACE('hello world', 'world', 'there')
----
hello there

query T
SELECT REVERSE('hello')
----
olleh

query I
SELECT POSITION('o' IN 'hello')
----
5

query T
SELECT LPAD('42', 5, '0')
----
00042

query T
SELECT RPAD('42', 5, '0')
----
42000

query T
SELECT SPLIT_PART('a,b,c', ',', 2)
----
b
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 17);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_null_functions() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
query I
SELECT COALESCE(NULL, NULL, 42)
----
42

query I
SELECT COALESCE(1, 2, 3)
----
1

query I
SELECT NULLIF(5, 5)
----
NULL

query I
SELECT NULLIF(5, 3)
----
5

query I
SELECT IFNULL(NULL, 100)
----
100

query I
SELECT IFNULL(50, 100)
----
50

query I
SELECT NVL(NULL, 200)
----
200

query I
SELECT GREATEST(3, 1, 4, 1, 5)
----
5

query I
SELECT LEAST(3, 1, 4, 1, 5)
----
1

query I
SELECT GREATEST(NULL, 10, 5)
----
10

query I
SELECT LEAST(NULL, 10, 5)
----
5
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 11);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_nulls_first() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE scores (name VARCHAR, score INTEGER)

statement ok
INSERT INTO scores VALUES ('Alice', 90)

statement ok
INSERT INTO scores VALUES ('Bob', NULL)

statement ok
INSERT INTO scores VALUES ('Charlie', 85)

statement ok
INSERT INTO scores VALUES ('Diana', NULL)

statement ok
INSERT INTO scores VALUES ('Eve', 95)

query TI
SELECT name, score FROM scores ORDER BY score NULLS FIRST
----
Bob	NULL
Diana	NULL
Charlie	85
Alice	90
Eve	95

query TI
SELECT name, score FROM scores ORDER BY score ASC NULLS FIRST
----
Bob	NULL
Diana	NULL
Charlie	85
Alice	90
Eve	95
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);  // 6 statements + 2 queries
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_nulls_last() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE scores (name VARCHAR, score INTEGER)

statement ok
INSERT INTO scores VALUES ('Alice', 90)

statement ok
INSERT INTO scores VALUES ('Bob', NULL)

statement ok
INSERT INTO scores VALUES ('Charlie', 85)

statement ok
INSERT INTO scores VALUES ('Diana', NULL)

statement ok
INSERT INTO scores VALUES ('Eve', 95)

query TI
SELECT name, score FROM scores ORDER BY score NULLS LAST
----
Charlie	85
Alice	90
Eve	95
Bob	NULL
Diana	NULL

query TI
SELECT name, score FROM scores ORDER BY score DESC NULLS LAST
----
Eve	95
Alice	90
Charlie	85
Bob	NULL
Diana	NULL
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 8);  // 6 statements + 2 queries
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_between() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
statement ok
CREATE TABLE numbers (id INTEGER, value INTEGER)

statement ok
INSERT INTO numbers VALUES (1, 10)

statement ok
INSERT INTO numbers VALUES (2, 25)

statement ok
INSERT INTO numbers VALUES (3, 50)

statement ok
INSERT INTO numbers VALUES (4, 75)

statement ok
INSERT INTO numbers VALUES (5, 100)

query II rowsort
SELECT id, value FROM numbers WHERE value BETWEEN 20 AND 60
----
2	25
3	50

query II rowsort
SELECT id, value FROM numbers WHERE value NOT BETWEEN 20 AND 60
----
1	10
4	75
5	100

query I
SELECT 50 BETWEEN 1 AND 100
----
true

query I
SELECT 150 BETWEEN 1 AND 100
----
false
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 10);  // 6 statements + 4 queries
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_datetime_functions() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
query I
SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-06-15 14:30:00')
----
2024

query I
SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 14:30:00')
----
14

query I
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2024-06-15 14:30:45')
----
30

query B
SELECT NOW() IS NOT NULL
----
true

query B
SELECT CURRENT_DATE IS NOT NULL
----
true

query B
SELECT CURRENT_TIMESTAMP IS NOT NULL
----
true
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 6);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_math_functions() {
        let mut runner = TestRunner::new().with_verbose(true);
        let content = r#"
query I
SELECT ABS(-42)
----
42

query R
SELECT CEIL(3.2)
----
4

query R
SELECT FLOOR(3.8)
----
3

query R
SELECT ROUND(3.567, 2)
----
3.57

query I
SELECT ROUND(3.5)
----
4

query I
SELECT SQRT(16.0)
----
4

query I
SELECT POWER(2, 10)
----
1024

query I
SELECT MOD(17, 5)
----
2

query R
SELECT LN(2.718281828459045)
----
1

query R
SELECT EXP(1.0)
----
2.718281828459045
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 10);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_distinct_on() {
        let mut runner = TestRunner::new();
        let content = r#"
statement ok
CREATE TABLE products (category VARCHAR, name VARCHAR, price INT)

statement ok
INSERT INTO products VALUES
    ('Electronics', 'Phone', 800),
    ('Electronics', 'Laptop', 1200),
    ('Electronics', 'Tablet', 500),
    ('Clothing', 'Shirt', 50),
    ('Clothing', 'Pants', 80),
    ('Books', 'Novel', 15),
    ('Books', 'Textbook', 100)

# DISTINCT ON returns first row per category (based on insertion order)
query TT
SELECT DISTINCT ON (category) category, name FROM products
----
Electronics	Phone
Clothing	Shirt
Books	Novel

# DISTINCT ON with ORDER BY to control which row is returned
# ORDER BY category, price DESC - so we get the most expensive item per category
query TTI
SELECT DISTINCT ON (category) category, name, price FROM products ORDER BY category, price DESC
----
Books	Textbook	100
Clothing	Pants	80
Electronics	Laptop	1200

# Plain DISTINCT still works
query T
SELECT DISTINCT category FROM products ORDER BY category
----
Books
Clothing
Electronics
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_having() {
        let mut runner = TestRunner::new();
        let content = r#"
statement ok
CREATE TABLE sales (product VARCHAR, region VARCHAR, amount INT)

statement ok
INSERT INTO sales VALUES
    ('Phone', 'East', 500),
    ('Phone', 'West', 400),
    ('Phone', 'East', 300),
    ('Laptop', 'East', 1200),
    ('Laptop', 'West', 900),
    ('Tablet', 'East', 200)

# HAVING with COUNT aggregate
query TI
SELECT product, COUNT(*) as cnt FROM sales GROUP BY product HAVING COUNT(*) > 1 ORDER BY product
----
Laptop	2
Phone	3

# HAVING with SUM aggregate
query TI
SELECT product, SUM(amount) as total FROM sales GROUP BY product HAVING SUM(amount) > 500 ORDER BY product
----
Laptop	2100
Phone	1200

# HAVING with AVG aggregate (Phone AVG=400, not > 400 so filtered out)
query TR
SELECT product, AVG(amount) as avg_amt FROM sales GROUP BY product HAVING AVG(amount) > 400.0 ORDER BY product
----
Laptop	1050

# HAVING with multiple conditions
query TI
SELECT product, SUM(amount) as total FROM sales GROUP BY product HAVING SUM(amount) > 500 AND COUNT(*) >= 2 ORDER BY product
----
Laptop	2100
Phone	1200

# HAVING referencing GROUP BY column (less common but valid)
query TI
SELECT product, SUM(amount) as total FROM sales GROUP BY product HAVING product != 'Tablet' ORDER BY product
----
Laptop	2100
Phone	1200
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_exists() {
        let mut runner = TestRunner::new();
        let content = r#"
statement ok
CREATE TABLE departments (id INT, name VARCHAR)

statement ok
INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'HR')

statement ok
CREATE TABLE employees (id INT, name VARCHAR, dept_id INT)

statement ok
INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2)

# EXISTS with always true subquery (has employees)
query IT
SELECT id, name FROM departments WHERE EXISTS (SELECT 1 FROM employees) ORDER BY id
----
1	Engineering
2	Marketing
3	HR

# NOT EXISTS with always true subquery (has employees)
query IT
SELECT id, name FROM departments WHERE NOT EXISTS (SELECT 1 FROM employees) ORDER BY id
----

# EXISTS with simple subquery
query IT
SELECT id, name FROM departments WHERE EXISTS (SELECT 1) ORDER BY id
----
1	Engineering
2	Marketing
3	HR
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 7);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_full_outer_join() {
        let mut runner = TestRunner::new();
        let content = r#"
statement ok
CREATE TABLE left_tbl (id INT, val VARCHAR)

statement ok
INSERT INTO left_tbl VALUES (1, 'A'), (2, 'B'), (3, 'C')

statement ok
CREATE TABLE right_tbl (id INT, val VARCHAR)

statement ok
INSERT INTO right_tbl VALUES (2, 'X'), (3, 'Y'), (4, 'Z')

# FULL OUTER JOIN - all rows from both sides
query ITIT
SELECT l.id, l.val, r.id, r.val FROM left_tbl l FULL OUTER JOIN right_tbl r ON l.id = r.id ORDER BY COALESCE(l.id, r.id)
----
1	A	NULL	NULL
2	B	2	X
3	C	3	Y
NULL	NULL	4	Z
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_scalar_subquery() {
        let mut runner = TestRunner::new();
        let content = r#"
statement ok
CREATE TABLE products (id INT, name VARCHAR, price INT)

statement ok
INSERT INTO products VALUES (1, 'Phone', 500), (2, 'Laptop', 1200), (3, 'Tablet', 300)

# Scalar subquery in SELECT
query ITI
SELECT id, name, (SELECT MAX(price) FROM products) as max_price FROM products ORDER BY id
----
1	Phone	1200
2	Laptop	1200
3	Tablet	1200

# Scalar subquery with column reference in WHERE
query IT
SELECT id, name FROM products WHERE price = (SELECT MAX(price) FROM products)
----
2	Laptop

# Scalar subquery comparing to aggregate
query IT
SELECT id, name FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY id
----
2	Laptop
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);
        assert_eq!(report.failed, 0);
    }

    #[test]
    fn test_cast() {
        let mut runner = TestRunner::new();
        let content = r#"
# CAST integer to varchar
query T
SELECT CAST(123 AS VARCHAR)
----
123

# CAST varchar to integer
query I
SELECT CAST('456' AS INTEGER)
----
456

# CAST double to integer (truncates)
query I
SELECT CAST(3.7 AS INTEGER)
----
3

# CAST integer to double
query R
SELECT CAST(42 AS DOUBLE)
----
42

# CAST in expressions
query I
SELECT CAST('10' AS INTEGER) + CAST('20' AS INTEGER)
----
30
"#;
        let report = runner.run_tests(content).unwrap();
        assert_eq!(report.passed, 5);
        assert_eq!(report.failed, 0);
    }
}
