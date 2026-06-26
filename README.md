# zk-Analytics

`zk-Analytics` is a Rust workspace with 2 long-running services:

- `aggregator`: consumes events from Kafka, batches them into epochs (by `EPOCH_LENGTH` or timeout), and generates ZK proofs:
  - `cm_epoch (epoch_type=cm_epoch)`: merges across `source_id` for the same `sequence`, generates a **CM-merge proof** and stores an aggregate epoch.
  - `histogram_epoch (epoch_type=histogram_epoch)`: merges across `source_id` for the same `sequence`, generates a **histogram-merge proof** and stores an aggregate epoch.
  - `samples_epoch (epoch_type=samples_epoch)`: **verify-only**; stores verified frames and does not generate a new hash chain.
- `querier`: HTTP API that queries epochs over a time window and returns **(answer + proof)** for the window merge + query.

It also includes:

- `aggregator` receiver mode: an HTTP receiver that accepts raw `(key_id,value)` events, verifies a Poseidon hash-chain over the events (like `poseidon_bytes` mode’s hash function), and feeds events into one of the per-epoch collector modes.
- `data_source`: a simple event generator that streams `(key_id,value)` events to the collector receiver.

## Requirements
CPU types support AVX-512. 

Build dependencies (Ubuntu/Debian):

```bash
sudo apt-get update
sudo apt-get install -y clang libclang-dev
```

## Streaming Event Receiver (collector) + Data Source

### Wire format

`POST /event` JSON body:

- `seq: u64` (must be contiguous starting from 0)
- `ts_ns: u64`
- `key_id: u64`
- `value: u32`
- `chain_hash_hex: String` (32-byte hex, Poseidon hash-chain tip after applying this event)
- (optional) `proof_kind: u8` (0 = recursive, 1 = compressed)
- (optional) `proof_b64: String` (base64 of a bincode-serialized Nova proof for a single event-step)

The event bytes for hashing are `ts_ns (BE u64) || key_id (BE u64) || value (BE u32)`, padded with the same `poseidon_bytes` padding and hashed with `Poseidon(h_prev, chunk31)` (1 step per event).

### Run collector receiver

```bash
cd zk-Analytics/aggregator
SERVICE_MODE=receiver RECEIVER_ADDR=${COLLECTOR_IP}:9000 EPOCH_TYPE=samples_epoch EPOCH_EVENTS=1000 cargo run --release -q
```

Or from the workspace root:

```bash
cd zk-Analytics
EPOCH_TYPE=samples_epoch EPOCH_EVENTS=1000 ./scripts/run_collector_receiver.sh
```

Supported `EPOCH_TYPE`: `samples_epoch`, `histogram_epoch_per_key`, `cm_epoch_per_key`, `histogram_epoch`, `cm_epoch`.

If the data source sends per-sample Nova proofs, the receiver can verify them (default on):

```bash
RECEIVER_VERIFY_PROOF=1 VERIFY_PROGRESS_EVERY=100
```

### Run data_source

```bash
cd zk-Analytics
RECEIVER_URL=http://${COLLECTOR_IP}:9000/event EVENTS=10000 PROGRESS_EVERY=100 PROOF_COMPRESS=0 ./scripts/run_data_source_http.sh
```

### Modes overview (data_source / aggregator / querier)

data_source (`MODE`):
- `stream`: synthetic events to `RECEIVER_URL` (hash-chain only, no per-event proofs).
- `stream_prove`: synthetic events + per-event Nova proofs (slow).
- `stream_tsv`: replay TSV traces (timestamps re-based), no per-event proofs.
- `stream_tsv_prove`: TSV replay + per-event proofs (slow).
- `bench`: local Poseidon hash-chain benchmark (no HTTP unless `BENCH_HTTP=1`).
- `bench_prove`: local per-event Nova proving benchmark.

aggregator (`SERVICE_MODE` + `EPOCH_TYPE`):
- `SERVICE_MODE=receiver`: HTTP receiver that accepts events; set `EPOCH_TYPE` to choose the epoch type produced.
- `SERVICE_MODE=local`: local proving/bench mode; set `EPOCH_TYPE` to the epoch type to prove.

aggregator receiver (`EPOCH_TYPE`, only when `SERVICE_MODE=receiver`):
- `samples_epoch`, `histogram_epoch`, `cm_epoch`, `histogram_epoch_per_key`, `cm_epoch_per_key`

querier (behavior toggles):
- `DIRECT_FROM_INGESTOR=1`: read/verify `epoch_frames` directly; `0` uses aggregate tables.
- `VERIFY_CHAIN_CHECKPOINT=1`: verify hash-chain continuity (with checkpoints).
- `CHAIN_VERIFY_DEBUG=1`: verbose chain-verify logs.
- `PROOF_COMPRESS=1`: return compressed proofs in responses.
- `DP_ENABLED=1`: enable differential privacy offsets in responses.

### Benchmark data_source Poseidon chain (per-event)

This benchmarks the data_source’s per-event Poseidon chain update (input bytes = 20) and optionally the HTTP POST overhead:

```bash
cd zk-Analytics/data_source
EVENTS=100000 PROGRESS_EVERY=10000 BENCH_HTTP=0 ./scripts/bench_poseidon_sample20.sh
```

To include receiver roundtrip time:

```bash
cd zk-Analytics/data_source
RECEIVER_URL=http://${COLLECTOR_IP}:9000/event BENCH_HTTP=1 EVENTS=100000 PROGRESS_EVERY=10000 ./scripts/bench_poseidon_sample20.sh
```

### Benchmark data_source Nova proof (Poseidon chain)

This generates a Nova recursive proof that the Poseidon hash-chain over `EVENTS` samples is correct.

```bash
cd zk-Analytics/data_source
EVENTS=10000 PROGRESS_EVERY=1000 PROOF_COMPRESS=0 ./scripts/bench_poseidon_sample20_prove.sh
```

Notes:
- These scripts read configuration from environment variables; don’t pass `EVENTS=...` as a positional argument to `bash ...`.
- `bench_poseidon_sample20_prove.sh` can be very slow for large `EVENTS`; try a small run first, e.g. `EVENTS=200 PROGRESS_EVERY=50`.

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

It polls `epoch_frames`, verifies each per-source Nova proof (host-side), and:

- For `cm_epoch (epoch_type=cm_epoch)`:
  - merges **CM array** by element-wise sum
  - merges **topk heap** by key (sum counts for matching keys)
  - produces a Nova merge proof (`CmMergeStep`)
  - stores structured CM into `agg_cm_struct` and the Poseidon `result_commit` into `agg_epochs`
  - stores the aggregate into `agg_epochs` and deletes the original per-source rows for that `(epoch_type, sequence)`
- For `histogram_epoch (epoch_type=histogram_epoch)`:
  - merges bucket counts by bucket (sum)
  - produces a Nova merge proof (`HistogramMergeStep`)
  - stores structured histogram into `agg_hist_struct` and the Poseidon `result_commit` into `agg_epochs`
  - stores the aggregate into `agg_epochs` and deletes the original per-source rows for that `(epoch_type, sequence)`
- For `samples_epoch (epoch_type=samples_epoch)`:
  - verify-only: moves rows from `epoch_frames` into `verified_epoch_frames`
  - extracts `(out_commit,total_count,total_sum)` from the verified proof output, computes a Poseidon `result_commit`, and stores into `verified_samples_struct` (the stored samples table is `(key,key_chain_tip,len,sum,occ)`; `key_chain_tip` is a Poseidon chain over that key’s values, preserving per-key order only)
  - the per-source `chain_hash` is computed by the Nova circuit as `Poseidon(Poseidon(chain_prev, TAG_FINALIZE), out_commit)` where `out_commit` is a commutative sum of Poseidon digests over `(key,key_chain_tip,len,sum)` for occupied slots (so cross-key reordering does not change the commitment)

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
- Builds a Nova proof that checks:
  - each epoch struct matches its `epoch_commit` (Poseidon commitment recomputed in-circuit)
  - commitments are bound into a window digest (Poseidon fold)
  - window merge + query result are correct
  - (optional) if `SAMPLES_BIND_RAW=1`, also recomputes `samples_epoch` per-key chains from `sample_events` (ordered by `idx`) and rejects epochs whose `out_commit` / table do not match

### Config

- `HTTP_LISTEN` (default: `${QUERIER_IP}:8082`)
- `ROCKSDB_PATH`
- `PROOF_COMPRESS=1` to return compressed Nova proofs (default `0` = recursive)

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
