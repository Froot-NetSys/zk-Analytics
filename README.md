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

### Hardware requirements

- **CPU**: x86-64 with **AVX-512**; many cores recommended (the zkVM prover
  scales with core count).
- **RAM**: **≥ 64 GB** — the zkVM prover peaks around 9–10 GB per
  aggregation/query node.
- **Storage**: NVMe SSD (RocksDB / FoundationDB backing store).

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
./scripts/setup/setup_local_e2e.sh --all   # install deps, Kafka/FDB, RISC Zero toolchain
./scripts/eval/run_local_e2e.sh start      # data_source -> Kafka -> aggregator -> FDB -> querier
./scripts/eval/run_local_e2e.sh status
```


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
ROCKSDB_PATH=/mydata/rocksdb ./scripts/setup/reset_rocksdb.sh
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

> Recovery semantics and online resharding internals are documented in [docs/INTERNALS.md](docs/INTERNALS.md).

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

Samples sum:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_sum","window":"1h"}'
```

Histogram bucket:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"histogram_bucket","window":"1d","bucket":42}'
```

CM top-k:

```bash
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"cm_topk","window":"5m","limit":20}'
```

Samples sum by key prefix/suffix (bitmask match):

```bash
# Example: suffix match on low 16 bits (mask = 0xffff)
curl -sS localhost:8082/query \
  -H 'content-type: application/json' \
  -d '{"type":"samples_sum_key","window":"1h","key":123,"mask":65535}'
```

> Benchmarking and the non-ZK native baseline are documented in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).
