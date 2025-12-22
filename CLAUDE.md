# IronDuck Project Guide

## Project Goal

IronDuck is a **complete replication of DuckDB written in pure Rust**. The goal is exact functional parity with DuckDB - every SQL feature, function, and behavior should work identically.

## Why Rust?

- **Memory safety by design**: No segfaults, no leaks, no use-after-free
- **Fearless concurrency**: Data races prevented at compile time
- **Modern tooling**: Cargo, clippy, rustfmt
- **Performance**: Zero-cost abstractions, no GC pauses

## Development Strategy

### Phase 1: DuckDB Compatibility Tests (Current)

We validate correctness by running DuckDB's official SQLLogicTest suite against IronDuck.

**Current Status**: 100% (122/122 tests passing)

#### What are SQLLogicTests?

SQLLogicTest is a testing framework that executes SQL statements and compares results against expected output. Test files use this format:

```
# Comments start with #
statement ok
CREATE TABLE t1 (a INTEGER, b TEXT);

statement ok
INSERT INTO t1 VALUES (1, 'hello'), (2, 'world');

query IT
SELECT * FROM t1 ORDER BY a;
----
1
hello
2
world

statement error
SELECT * FROM nonexistent_table;
----
```

- `statement ok` - SQL should execute without error
- `statement error` - SQL should fail
- `query <types>` - SQL should return results matching the expected output
  - Type hints: `I`=Integer, `T`=Text, `R`=Real

#### Test Location

Tests are in `tests/sqllogictest/duckdb_tests/`. Run them with:

```bash
cargo run --bin run_duckdb_tests
```

### Phase 2: Full Functional Replication

After passing compatibility tests, we expand to support all DuckDB features:

#### SQL Functions to Implement

DuckDB has 400+ built-in functions. Categories include:

- **Aggregate**: SUM, AVG, COUNT, MIN, MAX, STRING_AGG, ARRAY_AGG, etc.
- **Scalar**: ABS, ROUND, CONCAT, SUBSTRING, COALESCE, etc.
- **Date/Time**: DATE_PART, DATE_TRUNC, STRFTIME, NOW, etc.
- **String**: UPPER, LOWER, TRIM, REPLACE, REGEXP_MATCHES, etc.
- **List/Array**: LIST_AGG, UNNEST, ARRAY_SLICE, etc.
- **Window**: ROW_NUMBER, RANK, LAG, LEAD, NTILE, etc.
- **Table Functions**: RANGE, GENERATE_SERIES, READ_CSV, READ_PARQUET, etc.

#### Features to Implement

- [ ] All aggregate functions with DISTINCT, ORDER BY, FILTER
- [ ] Window functions with PARTITION BY, ORDER BY, frame clauses
- [ ] Common Table Expressions (CTEs), including recursive
- [ ] Correlated subqueries
- [ ] All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI)
- [ ] SET operations (UNION, INTERSECT, EXCEPT)
- [ ] Complex types (LIST, STRUCT, MAP)
- [ ] Sequences (CREATE SEQUENCE, NEXTVAL)
- [ ] Views (CREATE VIEW, materialized views)
- [ ] Indexes
- [ ] Transactions (BEGIN, COMMIT, ROLLBACK)
- [ ] External file formats (Parquet, CSV, JSON)

## Crate Architecture

```
ironduck/
├── crates/
│   ├── ironduck/              # Main facade crate
│   ├── ironduck-common/       # Types, Value, Error
│   ├── ironduck-parser/       # SQL parsing (sqlparser-rs)
│   ├── ironduck-binder/       # Schema binding, type resolution
│   ├── ironduck-catalog/      # Tables, schemas, functions
│   ├── ironduck-planner/      # Logical query plans
│   ├── ironduck-optimizer/    # Cost-based optimization
│   ├── ironduck-execution/    # Vectorized execution engine
│   ├── ironduck-storage/      # Buffer pool, columnar storage
│   └── ironduck-cli/          # Command-line interface
└── tests/
    └── sqllogictest/          # DuckDB test suite runner
```

## Current Limitations

Features not yet implemented:

- **Recursive CTEs**: WITH RECURSIVE queries
- **Some INTERVAL syntax**: `INTERVAL 7 MINUTES` (use `INTERVAL '7' MINUTE`)
- **Persistent storage**: Currently in-memory only
- **Transactions**: BEGIN, COMMIT, ROLLBACK

## Contributing

1. Pick a failing test from `cargo run --bin run_duckdb_tests`
2. Implement the missing feature
3. Ensure the test passes
4. Run full test suite to check for regressions
