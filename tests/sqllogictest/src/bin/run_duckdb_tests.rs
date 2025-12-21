//! Run DuckDB sqllogictest files against IronDuck
//!
//! Usage: cargo run --bin run_duckdb_tests [test_file_or_directory]

use ironduck_sqllogictest::TestRunner;
use std::env;
use std::fs;
use std::path::Path;

fn main() {
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

    collect_test_files(dir, &mut |path| {
        let mut runner = TestRunner::new().with_verbose(verbose);

        match runner.run_file(path.to_str().unwrap()) {
            Ok(report) => {
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
            Err(e) => {
                eprintln!("Error running {}: {}", path.display(), e);
            }
        }
    });

    // Print per-file results
    println!("{:<40} {:>8} {:>8} {:>8}", "File", "Passed", "Failed", "Skipped");
    println!("{}", "-".repeat(68));

    for (file, passed, failed, skipped) in &file_results {
        let status = if *failed == 0 { "✓" } else { "✗" };
        println!("{} {:<38} {:>8} {:>8} {:>8}",
            status, file, passed, failed, skipped);
    }

    // Print summary
    println!("{}", "=".repeat(68));
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

fn collect_test_files<F>(dir: &Path, callback: &mut F)
where
    F: FnMut(&Path),
{
    if let Ok(entries) = fs::read_dir(dir) {
        let mut paths: Vec<_> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();

        paths.sort();

        for path in paths {
            if path.is_dir() {
                collect_test_files(&path, callback);
            } else if let Some(ext) = path.extension() {
                if ext == "test" || ext == "slt" {
                    callback(&path);
                }
            }
        }
    }
}
