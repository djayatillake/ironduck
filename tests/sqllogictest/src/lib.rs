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
                        // Flatten results: each value on its own line
                        // Empty strings are displayed as "(empty)" in sqllogictest format
                        let mut actual: Vec<String> = result
                            .rows
                            .iter()
                            .flat_map(|row| {
                                row.iter().map(|v| {
                                    let s = v.to_string();
                                    if s.is_empty() {
                                        "(empty)".to_string()
                                    } else {
                                        s
                                    }
                                })
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
    let mut skip_until_empty = false;
    let mut in_loop = false;
    let mut requires_extension = false;

    while let Some(line) = lines.next() {
        let line = line.trim();

        // Skip comments and empty lines
        if line.is_empty() {
            skip_until_empty = false;
            continue;
        }
        if line.starts_with('#') {
            continue;
        }

        // If we're skipping, continue until empty line
        if skip_until_empty {
            continue;
        }

        // Handle require directive - skip entire file if extension required
        if line.starts_with("require ") {
            let ext = line.strip_prefix("require ").unwrap_or("").trim();
            // Skip files that require extensions we don't have
            if !matches!(ext, "vector_size" | "skip_reload" | "no_extension_autoloading") {
                requires_extension = true;
            }
            continue;
        }

        // Skip file if it requires an extension
        if requires_extension {
            continue;
        }

        // Handle mode directive
        if line.starts_with("mode ") {
            continue;
        }

        // Handle loop constructs (skip them for now)
        if line.starts_with("foreach") || line.starts_with("loop") {
            in_loop = true;
            continue;
        }
        if line.starts_with("endloop") {
            in_loop = false;
            continue;
        }
        if in_loop {
            continue;
        }

        // Handle load directive
        if line.starts_with("load ") {
            continue;
        }

        // Parse directives
        if line.starts_with("statement") {
            let expected_error = line.contains("error");
            let maybe_error = line.contains("maybe");
            let mut sql_lines = Vec::new();

            while let Some(sql_line) = lines.peek() {
                if sql_line.trim().is_empty() {
                    break;
                }
                // Skip result expectations for statement error
                if sql_line.trim() == "----" {
                    lines.next();
                    // Skip error message lines
                    while let Some(err_line) = lines.peek() {
                        if err_line.trim().is_empty()
                            || err_line.trim().starts_with("statement")
                            || err_line.trim().starts_with("query") {
                            break;
                        }
                        lines.next();
                    }
                    break;
                }
                sql_lines.push(lines.next().unwrap().to_string());
            }

            if !sql_lines.is_empty() {
                // For 'maybe' errors, we accept both success and error
                tests.push(TestCase::Statement {
                    sql: sql_lines.join("\n"),
                    expected_error: if maybe_error { false } else { expected_error },
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
            // SQLLogicTest format: each line has tab-separated values
            // We flatten to individual values (one per line)
            let mut results = Vec::new();
            while let Some(result_line) = lines.peek() {
                let trimmed = result_line.trim();
                if trimmed.is_empty()
                    || trimmed.starts_with("statement")
                    || trimmed.starts_with("query")
                    || trimmed.starts_with("halt")
                    || trimmed.starts_with("hash-threshold")
                    || trimmed.starts_with("require")
                    || trimmed.starts_with("mode")
                    || trimmed.starts_with("loop")
                    || trimmed.starts_with("foreach")
                    || trimmed.starts_with("endloop")
                    || trimmed.starts_with("load")
                {
                    break;
                }
                let line = lines.next().unwrap();
                // Split each line by tabs to get individual values
                for value in line.split('\t') {
                    results.push(value.to_string());
                }
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
            // Check for DuckDB-specific skipif/onlyif
            if line.contains("duckdb") {
                // For "onlyif duckdb", we run the test
                // For "skipif duckdb", we skip the test
                if line.starts_with("skipif") {
                    skip_until_empty = true;
                }
            } else {
                // For other databases, inverse logic
                if line.starts_with("onlyif") {
                    skip_until_empty = true;
                }
            }
        } else if line.starts_with("hash-threshold") {
            // Ignore hash-threshold directives
            continue;
        }
    }

    Ok(tests)
}
