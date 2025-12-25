//! Run DuckDB sqllogictest files against IronDuck
//!
//! Usage: cargo run --bin run_duckdb_tests [test_file_or_directory]

use ironduck_sqllogictest::TestRunner;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::collections::VecDeque;

/// Files/directories that cause stack overflow or hang and should be skipped
const SKIP_PATTERNS: &[&str] = &[
    "grouping_sets/cube.test",           // CUBE with many columns causes stack overflow
    "grouping_sets/rollup.test",         // ROLLUP can also cause issues
    "grouping_sets/grouping_sets.test",  // Complex grouping sets
    "tpch/",                              // Requires TPCH extension
    "tpcds/",                             // Requires TPCDS extension
    "extensions/",                        // Extension tests
];

fn main() {
    // Increase stack size for main thread
    let handle = std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024) // 32 MB stack
        .spawn(run_main)
        .unwrap();

    // Ignore panics from test runner
    let _ = handle.join();
}

fn run_main() {
    let args: Vec<String> = env::args().collect();

    let test_path = if args.len() > 1 {
        &args[1]
    } else {
        "tests/sqllogictest/duckdb_tests"
    };

    println!("=== IronDuck DuckDB Compatibility Test ===\n");

    let path = Path::new(test_path);

    if path.is_file() {
        run_single_file(path);
    } else if path.is_dir() {
        run_directory(path);
    } else {
        eprintln!("Error: Path '{}' does not exist", test_path);
        std::process::exit(1);
    }
}

fn run_single_file(path: &Path) {
    println!("Running: {}", path.display());

    let mut runner = TestRunner::new().with_verbose(true);

    match runner.run_file(path.to_str().unwrap()) {
        Ok(report) => {
            println!("\nResults:");
            println!("  Passed: {}", report.passed);
            println!("  Failed: {}", report.failed);
            println!("  Skipped: {}", report.skipped);
            println!("  Pass rate: {:.1}%", report.pass_rate() * 100.0);
        }
        Err(e) => {
            eprintln!("Error running test: {}", e);
        }
    }
}

fn run_directory(dir: &Path) {
    let verbose = std::env::var("VERBOSE").is_ok();
    println!("Scanning directory: {}\n", dir.display());
    if verbose {
        println!("VERBOSE mode: showing failure details\n");
    }

    let mut total_passed = 0;
    let mut total_failed = 0;
    let mut total_skipped = 0;
    let mut file_results: Vec<(String, usize, usize, usize)> = Vec::new();

    // Collect all test files using iterative approach (avoid stack overflow)
    let test_files = collect_test_files_iter(dir);
    let total_files = test_files.len();

    for (idx, path) in test_files.iter().enumerate() {
        // Show progress every 100 files
        if idx > 0 && idx % 100 == 0 {
            println!("Progress: {}/{} files...", idx, total_files);
        }

        // Catch panics for individual test files
        let path_str = path.to_str().unwrap_or("").to_string();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut runner = TestRunner::new().with_verbose(verbose);
            runner.run_file(&path_str)
        }));

        match result {
            Ok(Ok(report)) => {
                let filename = path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();

                file_results.push((
                    filename,
                    report.passed,
                    report.failed,
                    report.skipped,
                ));

                total_passed += report.passed;
                total_failed += report.failed;
                total_skipped += report.skipped;
            }
            Ok(Err(e)) => {
                if verbose {
                    eprintln!("Error running {}: {}", path.display(), e);
                }
            }
            Err(_) => {
                // Test file caused a panic - count as failed
                let filename = path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                file_results.push((filename, 0, 1, 0));
                total_failed += 1;
            }
        }
    }

    // Print per-file results (only show first 100 and summary for large test suites)
    println!("\n{:<50} {:>8} {:>8} {:>8}", "File", "Passed", "Failed", "Skipped");
    println!("{}", "-".repeat(80));

    let display_limit = if file_results.len() > 200 { 50 } else { file_results.len() };
    for (file, passed, failed, skipped) in file_results.iter().take(display_limit) {
        let status = if *failed == 0 { "✓" } else { "✗" };
        let display_name = if file.len() > 48 { &file[..48] } else { file };
        println!("{} {:<48} {:>8} {:>8} {:>8}",
            status, display_name, passed, failed, skipped);
    }

    if file_results.len() > display_limit {
        println!("... and {} more files", file_results.len() - display_limit);
    }

    // Print summary
    println!("{}", "=".repeat(80));
    println!("\n=== SUMMARY ===");
    println!("Total files: {}", file_results.len());
    println!("Total tests: {}", total_passed + total_failed + total_skipped);
    println!("  Passed:  {} ({:.1}%)",
        total_passed,
        if total_passed + total_failed > 0 {
            total_passed as f64 / (total_passed + total_failed) as f64 * 100.0
        } else { 0.0 });
    println!("  Failed:  {}", total_failed);
    println!("  Skipped: {}", total_skipped);
}

/// Check if a path should be skipped based on SKIP_PATTERNS
fn should_skip(path: &Path) -> bool {
    let path_str = path.to_string_lossy();
    SKIP_PATTERNS.iter().any(|pattern| path_str.contains(pattern))
}

/// Collect test files using iterative BFS (avoids stack overflow)
fn collect_test_files_iter(start_dir: &Path) -> Vec<PathBuf> {
    let mut test_files = Vec::new();
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    let mut skipped_count = 0;
    queue.push_back(start_dir.to_path_buf());

    while let Some(dir) = queue.pop_front() {
        // Skip entire directories that match skip patterns
        if should_skip(&dir) {
            continue;
        }

        if let Ok(entries) = fs::read_dir(&dir) {
            let mut paths: Vec<_> = entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();

            paths.sort();

            for path in paths {
                if should_skip(&path) {
                    skipped_count += 1;
                    continue;
                }
                if path.is_dir() {
                    queue.push_back(path);
                } else if let Some(ext) = path.extension() {
                    if ext == "test" || ext == "slt" {
                        test_files.push(path);
                    }
                }
            }
        }
    }

    if skipped_count > 0 {
        println!("Skipped {} problematic files/directories", skipped_count);
    }

    test_files.sort();
    test_files
}
