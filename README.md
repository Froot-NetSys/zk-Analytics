# zk-Analytics

`zk-Analytics` is a distributed, end-to-end **verifiable, privacy-preserving cloud
analytics** system. It augments an analytics pipeline with lightweight append-only
log commitments and zero-knowledge proofs of correct aggregation and query
execution, so an external verifier can check reported results **without** access to
raw logs or the provider's infrastructure. Proofs are generated with the
[RISC Zero](https://risczero.com) zkVM. This repository is the implementation
described in the paper *"Zero-Knowledge Cloud Analytics."*

## Architecture

The pipeline has three logically separated stages (paper §4–5):

1. **Online commitment — `data_source/`.** Each data source emits time-series
   `(timestamp, key, value)` log entries and incrementally commits to them with a
   lightweight **SHA-256 hash chain** (one constant-size update per log). Committed
   batches are streamed to the provider through a Kafka dispatcher, and each source
   may publish hash-chain checkpoints to a public transparency log (Trillian — see
   `deploy/trillian/` and `docs/transparency_log.md`).
2. **Offline distributed aggregation — `aggregator/`.** Shared-nothing aggregators
   consume committed batches from Kafka into a local RocksDB buffer, group them into
   fixed-size epochs, and run a RISC Zero **guest** that verifies the source
   hash-chain and aggregates the epoch into a query-ready summary (per-key samples,
   histograms, or a Count-Min sketch). Each epoch is chained to the previous one and
   persisted; its aggregate state is stored in RocksDB or FoundationDB.
3. **Offline query + verification — `querier/`.** An HTTP service answers analytics
   queries over a time window, returning the **answer plus a RISC Zero proof** that
   the window's epochs are authentic (epoch-chain + commitment checks) and that the
   query was executed correctly. Verifiers validate the proof offline, without raw
   logs or provider infrastructure.

### Crates

| Crate (package) | Path | Role |
|-----------------|------|------|
| `data_source` | `data_source/` | log generation + SHA-256 commitment; Kafka producer; Trillian checkpoints |
| `aggregator` | `aggregator/host/` | epoch aggregation + RISC Zero proving; Kafka consumer; resharding tools |
| `aggregator-core` | `aggregator/core/` | `no_std` aggregation logic shared by host + guests |
| `aggr_samples` / `aggr_cm` / `aggr_histogram` | `aggregator/methods/guest*` | RISC Zero aggregation guests: samples / Count-Min / histogram |
| `querier` | `querier/server/` | HTTP query service + RISC Zero proving |
| `querier-core` (+ guests) | `querier/{core,methods}/` | query logic + RISC Zero query guests |
| `common` | `common/` | RocksDB / FoundationDB stores, epoch types, differential privacy |
| `zkvm-common` | `zkvm-common/` | shared `no_std` zkVM types (`Event`, hash-chain) |
| `query-checker` | `query_checker/` | query allow/block-list access control (§5.4) |
| `cf_detector` | `cf_detector/` | control-flow / output leakage detector for query guests (§5.4) |
| `native-baseline` | `native_baseline/` | non-ZK baseline running the same analytics natively (evaluation) |

## Build

```bash
# RocksDB bindings need clang/libclang:
sudo apt-get update
sudo apt-get install -y clang libclang-dev

# Optional features: Kafka (rdkafka, cmake-build) and Trillian (protoc):
sudo apt-get install -y cmake libssl-dev pkg-config protobuf-compiler

# RISC Zero toolchain (guest compiler + r0vm):
curl -L https://risczero.com/install | bash && rzup install

cargo build --release        # host crates + RISC Zero guest ELFs
```

Proof **generation** uses AVX-512 for performance; proof **verification** does not.
FoundationDB 7.1 is required only for the FDB-backed (`--features fdb`) path.

## Run

Each service is a Cargo binary. End-to-end, committed batches flow
`data_source → Kafka → aggregator → RocksDB/FoundationDB → querier`.

```bash
# Aggregator: consume a Kafka topic into a local RocksDB buffer and prove epochs
# of type samples | histogram | cm (add --features fdb to store aggregates in FDB).
cargo run -p aggregator --release --features kafka -- --mode samples

# Data source: stream events as a Kafka producer (per-source SHA-256 hash chain).
cargo run -p data_source --bin kafka-producer --release --features kafka -- \
  --events 100000 --batch-size 100

# Querier: HTTP query service (default HTTP_LISTEN=0.0.0.0:8082).
cargo run -p querier --release
```

For a full local run (Kafka + FoundationDB via Docker, orchestrated in tmux):

```bash
./scripts/setup_local_e2e.sh --all   # install deps, Kafka/FDB, RISC Zero toolchain
./scripts/run_local_e2e.sh start      # data_source -> Kafka -> aggregator -> FDB -> querier
./scripts/run_local_e2e.sh status
```

Additional binaries: `aggregator/host` also builds `kafka-consumer`,
`reshard-controller`, `chain-inspector`, `recovery-bench`, `reshard-bench`, and
`handoff-sync`; `data_source` builds `kafka-producer` and `trillian-smoke`;
`querier/host` builds `bench_queries`. Reset local state with
`./scripts/reset_rocksdb.sh` (and `./scripts/reset_fdb.sh` for FoundationDB).

The default `data_source` binary is a standalone SHA-256 hash-chain microbenchmark
(`BENCH_INPUT`, `PARALLEL_CHAINS`, `VALUE_ZIPF_S`, `TS_MODE`), independent of Kafka.

## RocksDB

All services use the same data directory:

- `ROCKSDB_PATH` (default: `/mydata/rocksdb`)

### Logical tables (stored as key-value records)

- `aggregator`:
  - `agg_mode_state` (mode-level chain tip for aggregated epochs)
  - `agg_epochs` (aggregated epochs + merge proof fields + `result_commit`)
  - `agg_cm_struct` (structured CM counts/heap + `result_commit`)
  - `agg_hist_struct` (structured histogram totals/table + `result_commit`)
  - `verified_epoch_frames` (verified-only storage for `samples_epoch`)
  - `verified_samples_struct` (structured per-epoch totals + `result_commit` for `samples_epoch`)
  - `bad_epoch_frames` (quarantine for frames that fail verification)

Reset RocksDB storage:

```bash
ROCKSDB_PATH=/mydata/rocksdb ./scripts/reset_rocksdb.sh
```

## Aggregator

### What it does

It polls `epoch_frames`, verifies each per-source RISC Zero proof (host-side), and:

- For `cm_epoch (epoch_type=cm_epoch)`:
  - merges **CM array** by element-wise sum
  - merges **topk heap** by key (sum counts for matching keys)
  - produces a RISC Zero merge proof (the `aggr_cm` guest)
  - stores structured CM into `agg_cm_struct` and the SHA-256 `result_commit` into `agg_epochs`
  - stores the aggregate into `agg_epochs` and deletes the original per-source rows for that `(epoch_type, sequence)`
- For `histogram_epoch (epoch_type=histogram_epoch)`:
  - merges bucket counts by bucket (sum)
  - produces a RISC Zero merge proof (the `aggr_histogram` guest)
  - stores structured histogram into `agg_hist_struct` and the SHA-256 `result_commit` into `agg_epochs`
  - stores the aggregate into `agg_epochs` and deletes the original per-source rows for that `(epoch_type, sequence)`
- For `samples_epoch (epoch_type=samples_epoch)`:
  - verify-only: moves rows from `epoch_frames` into `verified_epoch_frames`
  - extracts `(out_commit,total_count,total_sum)` from the verified proof output, computes a SHA-256 `result_commit`, and stores into `verified_samples_struct` (the stored samples table is `(key,key_chain_tip,len,sum,occ)`; `key_chain_tip` is a SHA-256 chain over that key’s values, preserving per-key order only)
  - the per-source `chain_hash` is computed by the zkVM guest as `SHA-256(SHA-256(chain_prev || TAG_FINALIZE) || out_commit)` where `out_commit` is a commutative sum of SHA-256 digests over `(key,key_chain_tip,len,sum)` for occupied slots (so cross-key reordering does not change the commitment)

### Config

- `ROCKSDB_PATH`
- `INIT_DB=1` to initialize RocksDB (no-op)
- `POLL_MS` (default: `500`)
- `MODES` (default: `1,2,3`)
- `MIN_SOURCES` (default: `1`) for `cm/histogram` aggregation trigger
- `ALLOW_GAPS` (default: `0`) if `0`, only aggregates `sequence = last_seq+1` per mode
- `PROOF_COMPRESS` (default: `0`) controls whether aggregator-generated proofs are compressed
- `VERIFY_ONLY_BATCH` (default: `100`) batch size for `samples_epoch` verify-only

### Run

```bash
cd zk-Analytics
ROCKSDB_PATH=/mydata/rocksdb INIT_DB=1 cargo run -p aggregator
```

### Recovery semantics

Each completed epoch writes an `EpochTombstone` row (keyed by `(epoch_type,
aggregator_id, sequence)`) inside the same atomic RocksDB `WriteBatch` as the
rest of the epoch's rows (`agg_epoch`, the per-mode struct, `agg_epoch_meta`,
`agg_epoch_proof`). The tombstone's presence is the single source of truth for
"this epoch is fully done" — `has_agg_epoch` is no longer authoritative,
because a crash mid-WriteBatch can leave any subset of the per-epoch rows on
disk.

On startup, `recover_partial_state()` runs once before the polling loop. It
looks up the highest tombstoned sequence (`max_done`) for the current
`(epoch_type, aggregator_id)` pair and scans a small window of `[max_done-5,
max_done+5]`. For each sequence in the window with no tombstone but any of the
related rows present, it deletes those orphaned rows in a single `WriteBatch`
and logs `[native-aggr][recover] max_done=… orphans_cleaned=…`. The polling
loop then re-processes those sequences from scratch.

The cross-epoch chain hash is unaffected because each subsequent epoch's
`prev_chain_hash` is read from the previous epoch's `final_chain_hash`, which
is only persisted as part of the per-mode struct row — and that row only
survives if its accompanying tombstone was written.

### Online resharding

Moves a `source_id` from one aggregator to another at an epoch boundary while
preserving its per-source SHA-256 chain. The data plane is implemented and
verified end-to-end for arbitrary X→Y reshards (scale up or down), including a
real zkVM proof that verifies across a handoff — see
`docs/EVALUATION_ONLINE_RESHARDING.md`.

- Three record types in `common/src/rocksdb_store.rs` (mirrored in
  `common/src/fdb_store.rs`):
  - `OwnershipEpoch { epoch_seq, assignments: Vec<(source_id, aggregator_id)>, installed_at_ms }`
    becomes active at `epoch_seq` and stays active until a higher-seq row
    overrides it.
  - `Handoff { source_id, at_epoch, from_aggregator, to_aggregator, chain_tip, last_seq, published_at_ms }`
    records a source's ownership transition (for audit).
  - `AggSourceTip { source_id, last_seq, chain_tip, owner, updated_at_epoch }`
    is the durable per-source chain tip — persisted atomically with each epoch
    and inherited by a source's new owner so its chain continues across the
    handoff (see the Security note below).
- A pure read function `current_owner_for_source(db, source_id, epoch_seq, default_aggregator)`
  walks the OwnershipEpoch rows and returns the assigned aggregator, or
  the default if none. Unit-tested via `ownership_lookup_basic`.
- A one-shot manual control-plane tool, `reshard-controller`
  (`aggregator/host/src/bin/reshard_controller.rs`), takes
  `--rocksdb-path`, `--at-epoch N`, `--map "src:agg,..."`, validates
  that `N` is strictly in the future relative to
  `EpochBatcherState.next_epoch_seq`, and writes one OwnershipEpoch row.

Aggregators opt in with `--use-online-ownership` (or `USE_ONLINE_OWNERSHIP=1`).
When unset, behaviour is identical to existing static partitioning.

Reshards are triggered manually via `reshard-controller`; an automatic
controller-driven rebalancer (a polling controller that decides *when* and
*how* to rebalance) is **future work**.

Security: the SHA-256 per-source chain is preserved across a handoff. Each
aggregator durably persists the tip of every source it owns as an
`AggSourceTip` row (committed atomically with the epoch); when a source is
reassigned, the new owner inherits that tip — propagated through the
coordination store, with the `Handoff` row recording the transition for audit —
and continues the chain from it instead of restarting from zero. The new
owner's first post-handoff batch is verified to chain from the inherited tip:
on the real ingest path the aggregator refuses to extend (and the zkVM proof
fails to verify) if the tip does not match, so a buggy or malicious controller
cannot silently re-seat a source onto a fork. This holds for an arbitrary X→Y
reshard (scale up or down); see `docs/EVALUATION_ONLINE_RESHARDING.md` for the
end-to-end verification (including a real zkVM proof that verifies across a
handoff).

Evaluation harnesses: `scripts/bench_resharding_xy.sh` (general X→Y reshard,
scale up & down), `scripts/bench_resharding_real.sh` (real raw_db path, chain
verified), `scripts/prove_handoff_demo.sh` (real zkVM proof across a handoff).
See `docs/EVALUATION_ONLINE_RESHARDING.md`.

## Querier

### What it does

`querier` serves `POST /query`:

- Loads epochs in a time window from aggregator tables (`agg_epochs` / `agg_*` / `verified_samples_struct`)
- Builds a RISC Zero proof that checks:
  - each epoch struct matches its `epoch_commit` (SHA-256 commitment recomputed in-circuit)
  - commitments are bound into a window digest (SHA-256 fold)
  - window merge + query result are correct
  - (optional) if `SAMPLES_BIND_RAW=1`, also recomputes `samples_epoch` per-key chains from `sample_events` (ordered by `idx`) and rejects epochs whose `out_commit` / table do not match

### Config

- `HTTP_LISTEN` (default: `${QUERIER_IP}:8082`)
- `ROCKSDB_PATH`
- `PROOF_COMPRESS=1` to return compressed RISC Zero (Groth16) proofs (default `0` = recursive receipts)

### Run

```bash
cd zk-Analytics
ROCKSDB_PATH=/mydata/rocksdb cargo run -p querier
```

### API examples

CM estimate:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"cm_estimate","window":"1h","key":123}'
```

CM top-k:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"cm_topk","window":"5m","limit":20}'
```

Histogram bucket:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"histogram_bucket","window":"1d","bucket":42}'
```

Histogram all:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"histogram_all","window":"1h"}'
```

Samples average:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_avg","window":"1h"}'
```

Samples sum:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_sum","window":"1h"}'
```

Samples average by key:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_avg_key","window":"1h","key":123}'
```

Samples sum by key:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_sum_key","window":"1h","key":123}'
```

Samples average by key prefix/suffix (bitmask match):

```bash
# Example: suffix match on low 16 bits (mask = 0xffff)
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_avg_key","window":"1h","key":123,"mask":65535}'
```

Samples sum by key prefix/suffix (bitmask match):

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_sum_key","window":"1h","key":123,"mask":65535}'
```

Samples raw max by key prefix/suffix (bitmask match, raw events as private witness):

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_raw_max_key","window":"1h","key":123,"mask":65535}'
```

Samples raw stats by key prefix/suffix (bitmask match, raw events as private witness):

Returns `count,sum,sumsq` (as `sumsq_lo/sumsq_hi` u64 limbs) so the client can compute variance/stddev.

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_raw_stats_key","window":"1h","key":123,"mask":65535}'
```

Samples raw histogram bucket by key prefix/suffix (bitmask match, raw events as private witness):

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_raw_histogram_bucket_key","window":"1h","key":123,"mask":65535,"bucket":10}'
```

Samples raw Count-Min estimate over values by key prefix/suffix (bitmask match, raw events as private witness):

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_raw_cm_estimate_key","window":"1h","key":123,"mask":65535,"value":42}'
```

Per-key histogram bucket (from `histogram_epoch_per_key` sharded frames; note: currently returns an empty proof bundle):

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"series_histogram_bucket_key","window":"1h","key":123,"mask":65535,"bucket":10}'
```

Per-key CM estimate (from `cm_epoch_per_key` sharded frames; note: currently returns an empty proof bundle):

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"series_cm_estimate_key","window":"1h","key":123,"mask":65535,"value":42}'
```

## Benchmarks

### `aggregator`

Use scripts under `zk-Analytics/aggregator/scripts/` (they print `*_ms` fields and write CSVs).

### Aggregator

Bench one unit of work (requires data already in Postgres):

```bash
cd zk-Analytics
MODE=3 ./scripts/bench_aggregator_once.sh
MODE=1 MIN_SOURCES=2 ./scripts/bench_aggregator_once.sh
MODE=2 MIN_SOURCES=2 ./scripts/bench_aggregator_once.sh
```

Key variables:
- `MODE`: `1|2|3` (cm/hist/samples verify-only)
- `PROOF_COMPRESS`: `0|1` (affects merge + agg-chain proof)
- `MIN_SOURCES`: frames needed to aggregate a sequence (mode 1/2)
- `VERIFY_ONLY_BATCH`: rows per verify-only run (mode 3)
- `TIMEOUT_S`: stop if there’s no work

Outputs:
- `bench_csv/bench_aggregator_once.csv`
- `bench_logs/`

Run a small suite (one run per mode):

```bash
cd zk-Analytics
./scripts/bench_aggregator_all.sh
```

### Querier


## Non-ZK Native Baseline (SIGCOMM camera-ready)

Isolates the cost of **zkVM proof generation** from the cost of the analytics
architecture itself, by running the *same* aggregation/query logic
(`process_*_aggr`, `run_*_query`) natively on the host CPU with **no zkVM and no
proofs**, on the same machine / input / epoch+batch sizes / aggregator counts /
matched CPU cores as the zkVM experiments.

Components (all additive; the default proving path is unchanged):

- `native_baseline/` — standalone native measurement binary (depends only
  on the `*-core` crates, so it does **not** build any guest ELF).
- `aggregator/host` `--native` (or `NATIVE=1`) — opt-in flag that runs the
  aggregation analytics natively (no proof) through the real data loaders
  (synthetic / Google `tsv` / CAIDA), used for the real-dataset e2e baseline.
- `scripts/` + `Makefile`:
  - `make eval-non-zk-baseline` — native aggregation + query micro-baselines and
    the merged CSVs/plots/summary (seconds). Core deliverable.
  - `make eval-zkvm-aggr-56` — re-prove aggregation at 56 threads (hours) to
    match the paper's all-cores setup.
  - `make eval-zkvm-query-proofs` — real zkVM query proofs at 1/2/4 epochs.
  - `make eval-non-zk-e2e` — native e2e on the real Google/CAIDA traces.
  - `scripts/run_non_zk_phase2.sh` — runs the CPU-heavy steps (query proofs,
    CAIDA prep, e2e) in sequence on a quiet machine, then re-merges.

Outputs (`results/`): `non_zk_aggregation_baseline.csv`,
`non_zk_query_baseline.csv`, `zk_cost_breakdown.csv`, `non_zk_e2e_baseline.csv`,
`non_zk_baseline_summary.md`; plots in `plots/`.

Build dep: `clang`/`libclang-dev` (RocksDB bindings); FoundationDB 7.1 only for
the FDB-backed querier e2e.
