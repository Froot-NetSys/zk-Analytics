# Scripts

Evaluation, setup, and orchestration scripts, grouped by purpose. Run them from
the repository root (each script resolves the repo root from its own location).

## Layout

| Directory | What's in it |
|-----------|--------------|
| `setup/` | Environment setup and reset: `setup_local_e2e.sh`, `setup_remote_e2e.sh`, `kafka-setup.sh`, `prep_caida.sh`, `reset_fdb.sh`, `reset_rocksdb.sh`, `docker_no_sudo.sh` |
| `eval/` | Paper figure/table reproduction: `run_fig4_e2e.sh`, `run_table2_native.sh`, `run_figures_zk.sh`, `run_zkvm_*.sh`, `run_zkvm_distributed_sweep.sh`, `run_non_zk_*.sh`, `run_e2e_native_*.sh`, and baseline runners. |
| `distributed/` | Multi-node cluster orchestration: `run_distributed_*.sh`, `bench_distributed_aggregators.sh`, `example_distributed_setup.sh` |
| `bench/` | Micro-benchmarks: `bench_resharding_*.sh`, `prove_handoff_demo.sh` |
| `lib/` | Shared config (`common.sh`, sourced for the structural params) and internal helpers invoked by the above: log parsers (`_parse_*.py`), table/plot builders (`build_*.py`, `_build_*.py`), `mem_trace.py` |
| `util/` | Standalone utilities: `kill_bench_*.sh`, `debug_aggregator_consumption.sh`, `trillian_smoke.sh` |

Shared config files live at the top level (`scripts/`): `ip_defaults.sh`,
`docker-compose-kafka.yml`, `distributed_e2e_config*.sh`, `requirements.txt`.

## Entry points

The paper experiments are driven through the `Makefile` targets and the
`run_figures_*` / `run_distributed_baseline.sh` scripts:

| Make target | What it runs |
|-------------|--------------|
| `eval-kill` | Stops local benchmark processes left by an interrupted evaluation run. |
| `eval-non-zk-baseline` | Native (non-ZK) aggregation/query micro-baseline + merged CSVs (seconds). |
| `eval-zkvm-dev-mode` | Local single-aggregator zkVM guest checks in dev mode (guests executed, no STARK proof) — minutes. |
| `eval-fig4-vehicle-dev` | Figure 4 vehicle-emissions distributed pipeline in zkVM dev mode; no real proof. |
| `eval-fig4-vehicle-zk` | Figure 4 vehicle-emissions distributed pipeline with real proofs. |
| `eval-table2-native` | Table 2 Vanilla pipelines on Google/CAIDA/Vehicle using 8/8/4 real aggregator nodes, Kafka, RocksDB, and FoundationDB. |
| `eval-fig5-table3-zk` | Figure 5/Table 3 distributed real-ZK pipeline through Kafka, RocksDB, and FoundationDB. |
| `eval-fig6-aggregator-dev` / `eval-fig6-aggregator-zk` | Figure 6 standalone aggregator benchmark. |
| `eval-fig7-query-dev` / `eval-fig7-query-zk` | Figure 7 standalone query benchmark. |
| `eval-zkvm-query-proofs` | Real zkVM query proofs at 1/2/4 epochs. |
| `eval-zkvm-aggr-56` | Real zkVM aggregation re-run at 56 threads (hours). |

See [`../docs/ARTIFACT-EVALUATION.md`](../docs/ARTIFACT-EVALUATION.md) for the full
per-figure/table reproduction guide.
