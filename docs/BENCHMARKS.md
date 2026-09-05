# Benchmarks

The maintained, end-to-end benchmark and paper-reproduction guide is
[`ARTIFACT-EVALUATION.md`](ARTIFACT-EVALUATION.md): it maps every figure and
table (Figures 4–7, Tables 1–3) to a concrete command. The main entry points
(each prints `*_ms` timing fields and writes CSVs under `results/`):

- `make eval-non-zk-baseline` — local native (non-ZK) aggregation/query
  functional baseline (see below).
- `make eval-zkvm-dev-mode`, `make eval-zkvm-query-proofs`,
  `make eval-zkvm-aggr-56` — zkVM proving benchmarks (execution-only, query
  proofs, and the 56-thread aggregation re-anchor).
- `make eval-fig4-vehicle-dev`, `make eval-fig4-vehicle-zk` — vehicle-emissions
  end-to-end pipeline (Figure 4), in functional dev mode or real-ZK mode.
- `make eval-fig5-table3-dev`, `make eval-fig5-table3-zk` — distributed
  pipeline through Kafka, RocksDB, and FoundationDB (Figure 5 and Table 3),
  in functional dev mode or real-ZK mode.
- `make eval-fig6-aggregator-dev`, `make eval-fig6-aggregator-zk` — standalone
  zkVM aggregator benchmark (Figure 6).
- `make eval-fig7-query-dev`, `make eval-fig7-query-zk` — standalone zkVM query
  benchmark (Figure 7).
- `make eval-table2-native` — the 8/8/4-node Google, CAIDA, and Vehicle
  Vanilla pipelines through Kafka, RocksDB, and FoundationDB (Table 2).

## Non-ZK Native Baseline (SIGCOMM camera-ready)

Isolates the cost of **zkVM proof generation** from the cost of the analytics
architecture itself, by running the *same* aggregation/query logic
(`process_*_aggr`, `run_*_query`) natively on the host CPU with **no zkVM and no
proofs**, on the same machine / input / epoch+batch sizes / matched CPU cores as
the zkVM experiments. The quick baseline measures one local aggregator; it does
not infer distributed scaling.

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
  - `make eval-non-zk-e2e` — optional legacy native diagnostic for separately
    obtained Google/CAIDA traces; it is not the primary Figure 4 command.
  - `scripts/eval/run_non_zk_phase2.sh` — runs the CPU-heavy steps (query proofs,
    CAIDA prep, e2e) in sequence on a quiet machine, then re-merges.

Outputs (`results/`): `non_zk_aggregation_baseline.csv`,
`non_zk_query_baseline.csv`, `zk_cost_breakdown.csv`, `non_zk_e2e_baseline.csv`,
`non_zk_baseline_summary.md`; plots in `plots/`.

Build dep: `clang`/`libclang-dev` (RocksDB bindings); FoundationDB 7.1 only for
the FDB-backed querier e2e.
