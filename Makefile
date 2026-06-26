# zk-Analytics camera-ready evaluation targets.

.PHONY: eval-non-zk-baseline eval-non-zk-e2e eval-zkvm-query-proofs \
        eval-zkvm-aggr-56 eval-zkvm-dev-mode eval-non-zk-all

# (1) Non-ZK native baseline + zkVM cost breakdown (the core must-do eval).
# Reruns the native analytics (no zkVM) and regenerates the CSVs, plots, and
# summary under results/ and plots/. Fast (seconds).
eval-non-zk-baseline:
	./scripts/run_non_zk_baseline.sh

# (2) zkVM aggregation re-run at 56 threads (all cores) to match the paper.
# Multi-hour (CM ~3.5 h). Writes results/zkvm_aggregation_56threads.csv, which
# the baseline merge then prefers over the 32-thread CSV.
eval-zkvm-aggr-56:
	./scripts/run_zkvm_aggr_56.sh

# (3) Real zkVM query proofs at 1/2/4 epochs to anchor the query slowdown.
# Writes results/zkvm_query_proofs.csv (consumed by the baseline merge).
eval-zkvm-query-proofs:
	./scripts/run_zkvm_query_proofs.sh

# (3b) Run ALL planned zkVM experiments in dev mode (RISC0_DEV_MODE=1): guest
# executed, no STARK proof. Fast (minutes). Writes results/zkvm_dev_*.csv with
# the zkVM execution / witness-gen times for the cost breakdown.
eval-zkvm-dev-mode:
	./scripts/run_zkvm_dev_mode.sh

# (4) Native end-to-end baseline on the real Fig.4 datasets (Google/CAIDA),
# no zkVM proof and no data-source hash commitment. Writes
# results/non_zk_e2e_baseline.csv. Run scripts/prep_caida.sh first for CAIDA.
eval-non-zk-e2e:
	./scripts/run_e2e_native_baseline.sh

# Convenience: regenerate the merged CSVs/plots/summary from whatever measured
# inputs are currently present (native + 56-thread agg + query proofs).
eval-non-zk-all: eval-non-zk-baseline
