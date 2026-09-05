# Dev-mode AEC walkthrough — 2026-09-05

Scope: execute the guide's local/dev path through `make eval-plots-all`.
This is **not a complete distributed artifact validation**: node0 cannot
authenticate to any of node1–node7 (`Permission denied (publickey)`).
No real-proof performance or paper-level scaling claims were validated.

The starting revision was `c5cd41e`. The allocation already had build caches,
Docker services and FoundationDB data. HTTPS cloning succeeded, but this was
not a clean-OS installation test. Commands initially ran in
`/mydata/zk-Analytics`; fixes and isolated reruns use a separate worktree with
the existing target cache. Logs are retained on the allocation under
`/mydata/zk-analytics-aec-20260905/`, not committed as benchmark data.

## Command coverage

| Guide step / command | Observed result | Log |
|---|---|---|
| HTTPS `git clone` | Passed, separate fresh source clone | Clone at `clone/` |
| `FDB_PUBLIC_ADDRESS=10.10.1.1 .../setup_local_e2e.sh --all` | Rejected existing bridge-network FDB, preserving its data; distributed FDB prerequisite unresolved | `setup-initial.log` |
| `.../setup_local_e2e.sh --all` (local alternative) | Passed on existing allocation | `setup-local.log` |
| `clang --version`, `cargo --version`, `rzup show`, `r0vm --version` | Passed after documented PATH setup | `tool-versions.log` |
| `mkdir -p target/tmp`; `.../setup_local_e2e.sh --build` | Passed | `build.log` |
| `make eval-non-zk-baseline` | Passed; native aggregation and queries through 256 epochs | `native.log` |
| `make eval-zkvm-dev-mode` | Passed: 15 aggregation rows, 25 query rows; no empty fields; proof bytes and verification metrics zero | `dev.log` |
| Documented synthetic commitment command, 1,000,000 events / 4,096 keys | Passed; `serial_ns_per_event=156.237` (one run, not a repeated statistical estimate) | `commitment.log` |
| `make eval-fig6-aggregator-dev` | Passed full default sweep with fixes; 15 complete rows | `fig6-fixed.log` |
| `make eval-fig7-query-dev` | Passed full default sweep with fixes; 25 complete rows | `fig7-fixed.log` |
| Documented eight-node cluster setup with copy-keys/install-deps/deploy | Blocked at SSH key installation; initial 30-second diagnostic timeout occurred while restarting Kafka, retry reached definitive SSH failure | `cluster-setup.log`, `cluster-setup-retry.log` |
| `KAFKA_HOST=node0 make eval-fig4-vehicle-dev` | Failed SSH preflight, before shared-state reset | `fig4-four-node.log` |
| `KAFKA_HOST=node0 TABLE2_SPECS=vehicle:4 make eval-table2-native` | Failed SSH preflight | `table2-four-node.log` |
| `KAFKA_HOST=node0 FIG5_SPECS=histogram:4 make eval-fig5-table3-dev` | Failed SSH preflight; no scaling result | `fig5-four-node.log` |
| `make eval-plots-all` in original results directory | Exited successfully, but reused an old Figure 4 input; **not evidence of this walkthrough passing Figure 4** | `plots-existing.log` |
| `make eval-plots-all` in isolated worktree | Passed: Figure 6/7 PDFs generated and visually inspected; seven unavailable plots explicitly skipped in manifest | `plots-fixed.log` |

Real-ZK commands were intentionally not run. Google/CAIDA preparation was not
run because the external inputs were not supplied. Alternative machine-list
examples and the full Figure 5 sweep remain untested due to the same SSH
blocker; they are not counted as passing based on single-machine runs.
Troubleshooting reset/kill commands are remedies, not mandatory experiments,
and were not indiscriminately executed.

## Reproduced issues and fixes

1. The dev runner returned success after a failed compiler invocation, then
   emitted a CSV row with empty measured values. An isolated fake compiler
   returning 42 reproduced this without touching real measurements. The runner
   now stops on failures and rejects incomplete aggregation/query metrics.
2. Plotting without matplotlib returned success without producing any plots.
   It now fails with an installation hint.
3. A skipped plot could leave a PDF from an earlier run without warning. The
   plotter now warns without deleting user files and writes a per-run
   `plot_manifest.json`. Explicit input/output directory options permit
   isolated plotting. Existing input files still require provenance checks.

Regression checks: `python3 scripts/lib/test_dev_runner.py`,
`python3 scripts/lib/test_plot_all_results.py`, and
`python3 scripts/lib/test_dist_metrics.py`.

## Remaining acceptance work

Restore coordinator-to-worker SSH authorization and provide a FoundationDB
deployment reachable from every worker, then rerun shared setup, four-node
vehicle Figure 4/Table 2 and all Figure 5 points before claiming distributed
success. No SSH policy or existing FoundationDB volume was changed to bypass
these prerequisites. Real proofs, external datasets, independent query-value
checking and the documented Figure 4 paper-panel/time-definition comparison
remain outside the completed dev validation.
