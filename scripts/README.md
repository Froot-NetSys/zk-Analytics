# Scripts

Evaluation, setup, and orchestration scripts, grouped by purpose. Run them from
the repository root (each script resolves the repo root from its own location).

## Layout

| Directory | What's in it |
|-----------|--------------|
| `setup/` | Environment setup and reset: `setup_local_e2e.sh`, `setup_remote_e2e.sh`, `kafka-setup.sh`, `prep_caida.sh`, `reset_fdb.sh`, `reset_rocksdb.sh`, `docker_no_sudo.sh` |
| `eval/` | Paper figure/table reproduction: `run_figures_{native,zk}.sh`, `run_fig7_native.sh`, `run_zkvm_*.sh`, `run_non_zk_*.sh`, `run_e2e_native_*.sh`, `run_table2_sweep.sh`, `run_baseline_*.sh`, `run_local_e2e.sh` |
| `distributed/` | Multi-node cluster orchestration: `run_distributed_*.sh`, `bench_distributed_aggregators.sh`, `example_distributed_setup.sh` |
| `bench/` | Micro-benchmarks: `bench_resharding_*.sh`, `prove_handoff_demo.sh` |
| `lib/` | Internal helpers invoked by the above: log parsers (`_parse_*.py`), table/plot builders (`build_*.py`, `_build_*.py`), `mem_trace.py` |
| `util/` | Standalone utilities: `kill_bench_*.sh`, `debug_aggregator_consumption.sh`, `trillian_smoke.sh` |

Shared config files live at the top level (`scripts/`): `ip_defaults.sh`,
`docker-compose-kafka.yml`, `distributed_e2e_config*.sh`, `requirements.txt`.

## Entry points

The paper experiments are driven through the `Makefile` targets
(`make eval-non-zk-baseline`, `eval-zkvm-dev-mode`, `eval-zkvm-query-proofs`,
`eval-zkvm-aggr-56`, `eval-non-zk-e2e`) and the `run_figures_*` /
`run_distributed_baseline.sh` scripts. See
[`../docs/ARTIFACT-EVALUATION.md`](../docs/ARTIFACT-EVALUATION.md) for the full
per-figure/table reproduction guide.
