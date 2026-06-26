# Evaluation: Fault-Tolerance Tombstones & Crash Recovery

Scope: the `EpochTombstone` / `recover_partial_state()` work merged in PR #5
(`f4f54af`, `86c8073`). Evaluated on a single 56-core / 251 GB Linux host,
RocksDB backend, `cargo --release`, rustc 1.96.

## What was evaluated

1. **Correctness** of recovery (existing integration tests).
2. **Fault-recovery latency** as a function of completed-epoch count — a new
   in-process benchmark, `recovery-bench`.

## 1. Correctness — PASS

The shipped integration tests in `aggregator/host/tests/tombstone_recovery.rs`
exercise the recovery invariant against a real in-process RocksDB:

| Test | Asserts | Result |
|------|---------|--------|
| `recovery_deletes_orphaned_partial_writes` | a partial epoch (rows present, no tombstone) is cleaned | ok |
| `recovery_preserves_completed_epochs` | a tombstoned epoch survives recovery untouched | ok |
| `recovery_mixed_scenario` | one complete + one partial → only the partial is cleaned | ok |

```
cargo test --release -p zktelemetry-risc0-aggr-host --test tombstone_recovery
test result: ok. 3 passed; 0 failed
```

The single-source-of-truth design is sound: each completed epoch writes its
`EpochTombstone` in the **same atomic `WriteBatch`** as `agg_epoch` / per-mode
struct / `agg_epoch_meta` / `agg_epoch_proof`, so a crash can never leave a
tombstone without its epoch rows. Recovery treats *tombstone present* as "done"
and deletes any epoch rows in the scan window that lack one.

> Caveat on test realism (acknowledged in the test's own header): these are
> in-process store-level tests, not real `kill -9` crash injection, and they
> cover only the **RocksDB** backend. The mirrored **FoundationDB** tombstone
> path (`common/src/fdb_store.rs`) has no equivalent test.

## 2. Fault-recovery latency — `recovery-bench`

New binary: `aggregator/host/src/bin/recovery_bench.rs`. It populates a
RocksDB with `N` completed epochs plus a few orphaned partial writes just past
the last tombstone, then times `recover_partial_state()` (median of 5, fresh DB
each run). It separately isolates `max_epoch_tombstone_seq()`.

```
target/release/recovery-bench --epochs-sweep 1,10,100,1000,10000,50000 --orphans 3 --repeat 5
```

| Completed epochs `N` | `max_epoch_tombstone_seq` (ms) | **`recover_partial_state` (ms)** | orphans cleaned |
|---------------------:|-------------------------------:|---------------------------------:|----------------:|
| 1      | 0.017  | **0.137** | 3 |
| 10     | 0.021  | **0.155** | 3 |
| 100    | 0.055  | **0.200** | 3 |
| 1,000  | 0.394  | **0.561** | 3 |
| 10,000 | 1.865  | **1.952** | 3 |
| 50,000 | 11.477 | **9.725** | 3 |

### Findings

- **Recovery is cheap in absolute terms** — sub-millisecond up to ~1k epochs,
  ~2 ms at 10k, ~10 ms at 50k. The orphan-cleanup work itself is constant
  (the scan window is a fixed 11 sequences around `max_done`), and it always
  recovers the correct number of orphans.
- **But recovery scales O(N) with completed-epoch count, and the cost is
  dominated by `max_epoch_tombstone_seq()`**, which does a *full forward scan*
  of the tombstone keyspace (`common/src/rocksdb_store.rs:1054`). At N=50k the
  scan (11.5 ms) is the entire recovery cost. This is avoidable: tombstone keys
  are big-endian `sequence`, so they sort ascending — a single reverse seek to
  the last key would make `max_epoch_tombstone_seq` **O(1)** instead of O(N),
  flattening the curve above. Recommended follow-up fix.

### Reproduce

```
cargo build --release -p zktelemetry-risc0-aggr-host --bin recovery-bench
target/release/recovery-bench            # default sweep
```
