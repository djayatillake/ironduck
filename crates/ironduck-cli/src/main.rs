//! IronDuck CLI - Command-line interface
//!
//! A REPL for interacting with IronDuck databases.

use ironduck::Database;
use std::io::{self, BufRead, Write};
use std::time::Instant;

fn main() {
    println!("IronDuck v0.1.0");
    println!("A pure Rust analytical database, DuckDB compatible");
    println!("Type .help for help, .quit to exit");
    println!();

    let db = Database::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut multi_line_buffer = String::new();

    loop {
        // Show different prompt for multi-line input
        if multi_line_buffer.is_empty() {
            print!("ironduck> ");
        } else {
            print!("       -> ");
        }
        stdout.flush().unwrap();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                continue;
            }
        }

        let line = line.trim_end();

        // Handle multi-line input
        multi_line_buffer.push_str(line);

        // Check if statement is complete (ends with semicolon)
        if !multi_line_buffer.trim().ends_with(';') && !multi_line_buffer.trim().starts_with('.') {
            multi_line_buffer.push(' ');
            continue;
        }

        let input = multi_line_buffer.trim().to_string();
        multi_line_buffer.clear();

        if input.is_empty() {
            continue;
        }

        // Handle dot commands
        if input.starts_with('.') {
            handle_dot_command(&db, &input);
            continue;
        }

        // Remove trailing semicolon for execution
        let sql = input.trim_end_matches(';').trim();

        // Execute SQL
        let start = Instant::now();
        match db.execute(sql) {
            Ok(result) => {
                let elapsed = start.elapsed();

                if result.row_count() > 0 {
                    println!("{}", result.to_table_string());
                    println!(
                        "{} row(s) in {:.3}s",
                        result.row_count(),
                        elapsed.as_secs_f64()
                    );
                } else if !result.columns.is_empty() {
                    println!("OK (no rows returned)");
                } else {
                    println!("OK in {:.3}s", elapsed.as_secs_f64());
                }
                println!();
            }
            Err(e) => {
                eprintln!("Error: {}", e);
                eprintln!();
            }
        }
    }

    println!("Goodbye!");
}

fn handle_dot_command(db: &Database, input: &str) {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let command = parts.first().map(|s| *s).unwrap_or("");

    match command {
        ".quit" | ".exit" | ".q" => {
            println!("Goodbye!");
            std::process::exit(0);
        }

        ".help" | ".h" => {
            println!("Available commands:");
            println!("  .help          Show this help message");
            println!("  .quit          Exit the CLI");
            println!("  .tables        List all tables in the current schema");
            println!("  .schemas       List all schemas");
            println!("  .describe <t>  Describe table structure");
            println!("  .timer on|off  Toggle query timing");
            println!();
        }

        ".tables" => {
            let schema = db.catalog().default_schema();
            let tables = schema.list_tables();
            if tables.is_empty() {
                println!("No tables found.");
            } else {
                for table in tables {
                    println!("  {}", table);
                }
            }
            println!();
        }

        ".schemas" => {
            let schemas = db.catalog().list_schemas();
            for schema in schemas {
                println!("  {}", schema);
            }
            println!();
        }

        ".describe" | ".d" => {
            if parts.len() < 2 {
                eprintln!("Usage: .describe <table_name>");
                return;
            }
            let table_name = parts[1];
            match db.catalog().get_table("main", table_name) {
                Some(table) => {
                    println!("Table: {}", table.name);
                    println!("{:-<50}", "");
                    for col in &table.columns {
                        let nullable = if col.nullable { "NULL" } else { "NOT NULL" };
                        println!("  {} {} {}", col.name, col.logical_type, nullable);
                    }
                    println!();
                }
                None => {
                    eprintln!("Table '{}' not found.", table_name);
                }
            }
        }

        ".timer" => {
            if parts.len() < 2 {
                println!("Usage: .timer on|off");
            } else {
                println!("Timer: {}", parts[1]);
            }
        }

        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Type .help for available commands.");
        }
    }
}
