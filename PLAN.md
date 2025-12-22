# IronDuck: Pure Rust DuckDB Rewrite

## Current Progress (December 2025)

**DuckDB Compatibility: 98.4% (120/122 tests passing)**

### Completed Features
- [x] Core SQL parsing via sqlparser-rs
- [x] Schema binding and type resolution
- [x] In-memory catalog (schemas, tables, views, sequences)
- [x] Row-based execution engine
- [x] All basic aggregates: SUM, COUNT, AVG, MIN, MAX, STRING_AGG, ARRAY_AGG
- [x] Statistical aggregates: STDDEV, VARIANCE, CORR, COVAR
- [x] Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
- [x] All JOIN types: INNER, LEFT, RIGHT, FULL, CROSS with ON/USING
- [x] Subqueries (scalar, EXISTS, IN, correlated)
- [x] Common Table Expressions (WITH clauses)
- [x] UNION/INTERSECT/EXCEPT operations
- [x] Complex types: LIST, intervals
- [x] Date/Time functions: DATE_PART, DATE_TRUNC, STRFTIME, range() with intervals
- [x] Type casting for all basic types including TIMESTAMPTZ, TIMETZ
- [x] CREATE TABLE, INSERT, UPDATE, DELETE, DROP TABLE
- [x] CREATE VIEW, DROP VIEW
- [x] CREATE SEQUENCE, NEXTVAL, CURRVAL
- [x] TIMETZ type (time with timezone)
- [x] EXPLAIN query plans

### Not Yet Implemented
- [ ] Ordered aggregates (SUM(x ORDER BY y) syntax)
- [ ] Recursive CTEs
- [ ] Persistent storage (currently in-memory only)
- [ ] Transactions (BEGIN, COMMIT, ROLLBACK)
- [ ] External file formats (Parquet, CSV, JSON)
- [ ] Vectorized execution (currently row-based)

### How to Run Tests
```bash
cargo run --bin run_duckdb_tests
```

---

## Honest Assessment

| Metric | Reality |
|--------|---------|
| DuckDB size | ~500,000 lines of C++ |
| Development time | 7 years, 66,531 commits |
| Contributors | 500+ developers |
| Your situation | Solo developer, full compatibility target |

**Is this too hard?** For a solo developer aiming for full SQL compatibility, this is a multi-year undertaking. Realistic timeline: **24+ months** to reach meaningful parity. However, it's achievable by:

1. Leveraging existing Rust libraries (sqlparser, arrow-rs)
2. Using DuckDB's extensive test suite to validate correctness
3. Building incrementally with clear milestones
4. Rust's safety guarantees eliminate entire classes of bugs

**What you gain**: Memory safety by design (no leaks, no segfaults), modern language tooling, and full control over the implementation.

---

## Crate Architecture

```
ironduck/
├── Cargo.toml                 # Workspace root
├── crates/
│   ├── ironduck/              # Main facade crate
│   ├── ironduck-common/       # Types, Value, Error
│   ├── ironduck-parser/       # SQL parsing (extends sqlparser-rs)
│   ├── ironduck-binder/       # Schema binding, type resolution
│   ├── ironduck-catalog/      # Tables, schemas, functions
│   ├── ironduck-planner/      # Logical query plans
│   ├── ironduck-optimizer/    # Cost-based optimization
│   ├── ironduck-execution/    # Vectorized execution engine
│   ├── ironduck-storage/      # Buffer pool, columnar storage
│   ├── ironduck-transaction/  # MVCC, ACID transactions
│   ├── ironduck-functions/    # 400+ built-in functions
│   └── ironduck-cli/          # Command-line interface
└── tests/
    └── sqllogictest/          # DuckDB test suite runner
```

---

## Key Dependencies

```toml
[workspace.dependencies]
# Core
sqlparser = "0.53"        # SQL parsing foundation
arrow = "53"              # Columnar memory format, SIMD ops
tokio = "1"               # Async runtime for parallel execution

# Memory & Concurrency
bumpalo = "3"             # Arena allocation for query lifetime
parking_lot = "0.12"      # Fast locks
crossbeam = "0.8"         # Lock-free data structures
hashbrown = "0.15"        # Fast hash maps

# Storage
memmap2 = "0.9"           # Memory-mapped I/O
lz4_flex = "0.11"         # Fast compression
zstd = "0.13"             # High-ratio compression

# Testing
sqllogictest = "0.28"     # DuckDB test format runner
criterion = "0.5"         # Benchmarking
```

---

## Implementation Phases

### Phase 0: Foundation (Months 1-2)
**Goal**: Parse and bind `SELECT 1`

- [ ] Initialize Cargo workspace with all crate stubs
- [ ] Set up CI/CD (GitHub Actions)
- [ ] Implement core types (`LogicalType`, `Value`)
- [ ] Integrate sqlparser-rs with DuckDB dialect extensions
- [ ] Basic catalog (in-memory schemas, tables)
- [ ] Minimal binder for column/table resolution

**Milestone**: `SELECT 1 + 1` parses, binds, returns logical plan

### Phase 1: Core Execution (Months 3-5)
**Goal**: Execute SELECT with filtering, projection, aggregation

- [ ] Implement `Vector` and `DataChunk` (2048-tuple batches)
- [ ] Physical operators: `TableScan`, `Filter`, `Projection`
- [ ] Push-based pipeline execution model
- [ ] Hash aggregation with `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`
- [ ] `GROUP BY` support
- [ ] First sqllogictests passing

**Key file**: `crates/ironduck-execution/src/vector.rs`

```rust
pub struct DataChunk {
    pub vectors: Vec<Vector>,
    pub count: usize,  // Up to 2048
}

pub struct Vector {
    pub logical_type: LogicalType,
    pub validity: ValidityMask,
    pub data: VectorData,
}
```

### Phase 2: Storage Engine (Months 6-8)
**Goal**: Persistent storage with columnar format

- [ ] Buffer pool manager (256KB pages, LRU eviction)
- [ ] RAII `PageGuard` for safe page access
- [ ] Row groups (122,880 rows) with column segments
- [ ] Compression: RLE, dictionary, bitpacking
- [ ] Zonemaps (min/max per segment)
- [ ] Write-ahead logging (WAL)
- [ ] Checkpoint mechanism

**Key file**: `crates/ironduck-storage/src/buffer_pool.rs`

### Phase 3: Transactions (Months 9-10)
**Goal**: ACID compliance with MVCC

- [ ] Version chains for multi-version concurrency
- [ ] Snapshot isolation
- [ ] Commit/rollback with undo buffers
- [ ] Transaction visibility rules
- [ ] Conflict detection

**Key file**: `crates/ironduck-transaction/src/mvcc.rs`

### Phase 4: Query Optimization (Months 11-13)
**Goal**: Cost-based optimization for complex queries

- [ ] Filter pushdown
- [ ] Projection pushdown
- [ ] Expression simplification & constant folding
- [ ] DPccp algorithm for join ordering
- [ ] Cardinality estimation with histograms
- [ ] Physical plan selection (hash join vs nested loop)

**Key file**: `crates/ironduck-optimizer/src/rules/join_order.rs`

**Milestone**: TPC-H queries Q1-Q6 executing correctly

### Phase 5: Advanced SQL (Months 14-18)
**Goal**: Full SQL compatibility

- [ ] Window functions (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`)
- [ ] Correlated subqueries
- [ ] Common Table Expressions (CTEs)
- [ ] Recursive CTEs
- [ ] `LIST`, `STRUCT`, `MAP` types
- [ ] All 400+ DuckDB functions

**Milestone**: 80%+ of DuckDB sqllogictests passing

### Phase 6: External & Ecosystem (Months 19-24)
**Goal**: Full ecosystem compatibility

- [ ] Parquet read/write (via arrow-rs)
- [ ] CSV parsing
- [ ] PostgreSQL wire protocol
- [ ] Python bindings (PyO3)
- [ ] C API for language bindings
- [ ] Extension loading mechanism

**Milestone**: DuckDB v1.4.3 API compatibility

---

## Test Strategy

### DuckDB Test Suite Integration

DuckDB uses SQLLogicTest format. Run their tests against IronDuck:

```rust
// tests/sqllogictest/src/runner.rs
use sqllogictest::{AsyncDB, DBOutput};

pub struct IronDuckRunner {
    db: Database,
}

#[async_trait]
impl AsyncDB for IronDuckRunner {
    async fn run(&mut self, sql: &str) -> Result<DBOutput, Error> {
        let result = self.db.execute(sql).await?;
        // Convert to DBOutput format
    }
}
```

### Test Coverage Targets

| Phase | Target Pass Rate |
|-------|-----------------|
| Phase 1 | 10% of basic tests |
| Phase 3 | 30% of tests |
| Phase 5 | 80% of tests |
| Phase 6 | 95%+ of tests |

---

## Critical Implementation Details

### Vectorized Execution

```rust
// SIMD-optimized filter operation
pub fn filter_chunk(chunk: &DataChunk, predicate: &Vector) -> DataChunk {
    let mask = predicate.to_boolean_array();
    chunk.vectors.iter()
        .map(|v| arrow::compute::filter(&v.to_arrow(), &mask))
        .collect()
}
```

### Buffer Pool with RAII Safety

```rust
// PageGuard automatically unpins on drop - no leaks possible
pub struct PageGuard<'a> {
    pool: &'a BufferPool,
    frame_id: FrameId,
}

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        self.pool.unpin_page(self.frame_id);  // Always called
    }
}
```

### MVCC Visibility

```rust
impl Transaction {
    pub fn is_visible(&self, version: &RowVersion) -> bool {
        let begin_ts = version.begin_ts.load(Ordering::Acquire);
        let end_ts = version.end_ts.load(Ordering::Acquire);

        begin_ts != 0 && begin_ts <= self.start_ts &&
        (end_ts == u64::MAX || end_ts > self.start_ts)
    }
}
```

---

## Files to Create (Initial Setup)

1. `/Users/admin/ironduck/Cargo.toml` - Workspace manifest
2. `/Users/admin/ironduck/crates/ironduck-common/src/lib.rs` - Core types
3. `/Users/admin/ironduck/crates/ironduck-common/src/types.rs` - LogicalType enum
4. `/Users/admin/ironduck/crates/ironduck-common/src/value.rs` - Value enum
5. `/Users/admin/ironduck/crates/ironduck-common/src/error.rs` - Error types
6. `/Users/admin/ironduck/crates/ironduck-parser/src/lib.rs` - Parser wrapper
7. `/Users/admin/ironduck/crates/ironduck-execution/src/vector.rs` - Vector/DataChunk
8. `/Users/admin/ironduck/crates/ironduck-cli/src/main.rs` - CLI entry point
9. `/Users/admin/ironduck/tests/sqllogictest/src/lib.rs` - Test runner
10. `/Users/admin/ironduck/.github/workflows/ci.yml` - CI configuration

---

## Rust's Memory Safety Advantage

DuckDB's C++ codebase occasionally has:
- Memory leaks from missed deallocation
- Segfaults from use-after-free or null pointers
- Data races in concurrent code

**Rust eliminates these by design:**

| Issue | How Rust Prevents |
|-------|------------------|
| Memory leaks | Ownership + RAII (Drop trait) |
| Use-after-free | Borrow checker prevents |
| Null pointers | `Option<T>` instead of null |
| Data races | `Send`/`Sync` traits + ownership |
| Buffer overflows | Bounds checking by default |

This is the core value proposition of IronDuck.

---

## Summary

This is an ambitious but achievable project. The key success factors:

1. **Leverage existing Rust libraries** - Don't rebuild Arrow or sqlparser
2. **Test-driven development** - DuckDB's test suite is your north star
3. **Incremental milestones** - Ship working features, not vaporware
4. **Rust's guarantees** - Memory safety eliminates debugging entire bug classes

Timeline: ~24 months for meaningful v1.0 parity as a solo developer.

The hardest parts: query optimization (join ordering), window functions, and achieving performance parity with hand-optimized C++.

The easiest wins: memory safety, no segfaults, no leaks - you get these for free from Rust.
