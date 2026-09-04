# zk-Analytics camera-ready evaluation targets.

.PHONY: eval-non-zk-baseline eval-non-zk-e2e eval-zkvm-query-proofs \
        eval-zkvm-aggr-56 eval-zkvm-dev-mode eval-dev-zk-e2e eval-non-zk-all \
        eval-kill eval-fig6-aggregator-dev eval-fig6-aggregator-zk \
        eval-fig7-query-dev eval-fig7-query-zk eval-table2-native \
        eval-fig5-table3-zk eval-fig4-vehicle-dev eval-fig4-vehicle-zk

# Stop local processes left by an interrupted evaluation run.
eval-kill:
	./scripts/util/kill_bench_processes.sh

# (1) Non-ZK native baseline + zkVM cost breakdown (the core must-do eval).
# Reruns the native analytics (no zkVM) and regenerates the CSVs, plots, and
# summary under results/ and plots/. Fast (seconds).
eval-non-zk-baseline:
	./scripts/eval/run_non_zk_baseline.sh

# (2) zkVM aggregation re-run at 56 threads (all cores) to match the paper.
# Multi-hour (CM ~3.5 h). Writes results/zkvm_aggregation_56threads.csv.
eval-zkvm-aggr-56:
	./scripts/eval/run_zkvm_aggr_56.sh

# (3) Real zkVM query proofs at 1/2/4 epochs to anchor the query slowdown.
# Writes results/zkvm_query_proofs.csv (consumed by the baseline merge).
eval-zkvm-query-proofs:
	./scripts/eval/run_zkvm_query_proofs.sh

# (3b) Run local aggregation/query guest checks with one aggregator in dev mode
# (RISC0_DEV_MODE=1): guests execute, but no STARK proof is created. Fast
# (minutes). Writes results/zkvm_dev_*.csv.
eval-zkvm-dev-mode:
	./scripts/eval/run_zkvm_dev_mode.sh

# Paper Figure 6: standalone aggregator benchmark (no Kafka/RocksDB/FDB).
eval-fig6-aggregator-dev:
	RUN_AGGREGATION=1 RUN_QUERY=0 ./scripts/eval/run_zkvm_dev_mode.sh

eval-fig6-aggregator-zk: eval-zkvm-aggr-56

# Paper Figure 7: standalone query benchmark (no Kafka/RocksDB/FDB).
eval-fig7-query-dev:
	RUN_AGGREGATION=0 RUN_QUERY=1 ./scripts/eval/run_zkvm_dev_mode.sh

eval-fig7-query-zk: eval-zkvm-query-proofs

# Paper Table 2 Vanilla side: Figure 4's real 8/8/4-node dataset deployments,
# with Kafka/RocksDB/FDB and no ZK.
eval-table2-native:
	./scripts/eval/run_table2_native.sh

# Paper Figure 5 and Table 3: distributed real-ZK pipeline, exactly one
# aggregator process per selected machine.
eval-fig5-table3-zk:
	RISC0_DEV_MODE=0 FIG=5 ./scripts/eval/run_figures_zk.sh

# (3c) Figure 4 vehicle-emissions end-to-end in zkVM dev mode (guests execute,
# STARK proof faked). Set KAFKA_HOST for the four-machine cluster.
eval-dev-zk-e2e:
	./scripts/eval/run_fig4_e2e.sh

eval-fig4-vehicle-dev: eval-dev-zk-e2e

eval-fig4-vehicle-zk:
	RISC0_DEV_MODE=0 ./scripts/eval/run_fig4_e2e.sh

# (4) Native end-to-end baseline on the real Fig.4 datasets (Google/CAIDA),
# no zkVM proof and no data-source hash commitment. Writes
# results/non_zk_e2e_baseline.csv. Run scripts/setup/prep_caida.sh first for CAIDA.
eval-non-zk-e2e:
	./scripts/eval/run_e2e_native_baseline.sh

# Convenience: regenerate the merged CSVs/plots/summary from whatever measured
# inputs are currently present (native + 56-thread agg + query proofs).
eval-non-zk-all: eval-non-zk-baseline
