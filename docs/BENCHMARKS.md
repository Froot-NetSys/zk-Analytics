# Benchmarks

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
