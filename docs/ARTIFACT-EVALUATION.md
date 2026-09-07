# Artifact Evaluation: Zero-Knowledge Cloud Analytics

## Overview

This artifact contains the source code and
instructions needed to reproduce the experiments in *"Zero-Knowledge Cloud
Analytics."* More precisely, it contains:

- instructions to configure a local machine or multi-node cluster;
- instructions to build the data source, aggregation and query components,
  including their RISC Zero zkVM guests;
- scripts to launch the experiments and generate CSV and PDF results; and
- a short functional evaluation that does not require hours of proof generation.

By running the experiments, reviewers can reproduce or validate the results in:

- **Figure 4:** end-to-end performance, with the bundled vehicle-emissions
  workload provided as the initial reproduction path;
- **Table 2:** vanilla non-ZK pipeline costs with Kafka, RocksDB, and
  FoundationDB;
- **Figure 5 and Table 3:** distributed aggregation scalability;
- **Figure 6:** single-machine aggregation time, proof verification, proof size,
  and memory;
- **Figure 7:** query proving and verification as the number of epochs grows;
  and
- **§7.2:** online SHA-256 log-commitment throughput.

## Contents

- [Prerequisites](#prerequisites)
- [Step 0 — Setup and build](#step-0--setup-and-build)
- [Step 1 — Functional checks](#step-1--functional-checks)
- [Step 2 — Standalone experiments](#step-2--standalone-experiments)
- [Step 3 — Distributed experiments](#step-3--distributed-experiments)
- [Step 4 — Plots and output index](#step-4--plots-and-output-index)
- [Troubleshooting](#troubleshooting)

Start with Steps 0 and 1 on the local machine (the coordinator for distributed
runs). Then choose standalone experiments in Step 2 or configure the shared
cluster and run distributed experiments in Step 3. Step 4 collects the output
paths and the common plotting command.

## Prerequisites

### Hardware and runtime

| | Functional check / native baselines | Full ZK reproduction |
|---|---|---|
| CPU | any x86-64 | **AVX-512**, many cores (paper: CloudLab `c6420`, dual 16-core Intel Xeon Gold 6142) |
| RAM | 8 GB | ≥ 64 GB (prover peaks ~9–10 GB/node; parallel proving needs headroom) |
| Disk | **10 GB free** | 50+ GB free (RocksDB/FoundationDB + datasets) |
| Time | minutes | **hours per experiment** |

Paper hardware is CloudLab `c6420`: two 16-core Intel Xeon Gold 6142 CPUs
(32 physical cores total) per machine. Native and local dev-mode checks use
one machine. The bundled vehicle Figure 4/Table 2 experiments use four
machines; the complete Figure 5/Table 3 scaling sweep requires eight.

Real proof generation takes hours per experiment. Larger Figure 4 and Figure 7
points can take days; the guide identifies reduced-scale proof runs.

### Software

The setup script supports Ubuntu/Debian and uses `sudo` to install packages.
`scripts/setup/setup_local_e2e.sh --all` installs `clang`/`libclang` (RocksDB),
the RISC Zero toolchain via `rzup` (guest compiler + `r0vm`), Docker, Kafka, and
FoundationDB 7.1. Docker, Kafka, FoundationDB, and `protoc` are needed only for
the distributed/end-to-end paths. See the README "Build" section for the manual
dependency list.

### Datasets

The vehicle-emissions dataset is bundled. Google Cluster v3 must be downloaded
separately. CAIDA traces require an academic data-sharing agreement and cannot
be redistributed by the authors.

| Dataset | Status | How to obtain |
|---|---|---|
| Vehicle CO2 emissions (Canada) | **bundled** | `testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv` |
| Google Cluster v3 | not bundled | Download the public Google cluster-usage traces (v3) and place the per-machine CSVs under `testdata/google_cluster_data/input/`. |
| CAIDA backbone traces | not bundled | Requires CAIDA's academic **data-sharing agreement** (cannot be redistributed). Obtain a PCAP, then `PCAP=/path/to.pcap.gz ./scripts/setup/prep_caida.sh` → `testdata/caida_pcap/caida_txt/`. |

The synthetic-workload experiments (Figs 5–7, §7.2) need **no external data** —
they are the most self-contained to reproduce.

## Step 0 — Setup and build

```bash
git clone https://github.com/Froot-NetSys/zk-Analytics
cd zk-Analytics
```

### Install all prerequisites (full local environment)

Use the repository setup script on Ubuntu/Debian to install the build tools,
RISC Zero toolchain, Docker, Kafka, and FoundationDB, and to start
Kafka/FoundationDB. Choose the command for your intended setup.
For a local-only run:

```bash
./scripts/setup/setup_local_e2e.sh --all
```

For a new distributed coordinator, use the following command instead, setting
its private IPv4 address when creating FoundationDB so both coordinator and
workers can reach the advertised server:

```bash
FDB_PUBLIC_ADDRESS=10.10.1.1 ./scripts/setup/setup_local_e2e.sh --all
```

Replace `10.10.1.1` with your coordinator's private IPv4 address. This selects
host networking for a newly created FDB container. Existing FDB containers are
preserved; this command does not migrate their data or change their network
configuration. For an existing deployment, provide a cluster file whose
coordinator and storage-server addresses are reachable from every worker.
If the existing database is disposable, explicitly delete and recreate it at
the correct address (this removes its old container and anonymous data volume):

```bash
FDB_PUBLIC_ADDRESS=10.10.1.1 FDB_RESET=1 ./scripts/setup/setup_local_e2e.sh --fdb
```

Do not keep `FDB_RESET=1` in your shell environment: use it only on the command
that should erase the database.
Deployment and each distributed run check database availability from all nodes
with `fdbcli`; an inaccessible database aborts the run before shared-state reset.

The script invokes `sudo`. If it adds your account to the `docker` group, log
out and back in before running Docker without `sudo`.

Confirm that both the host and RISC Zero tools are available:

```bash
clang --version
cargo --version
rzup show
r0vm --version
```

All four commands must succeed. If `cargo` or `rzup` is not found immediately
after installation, run `source "$HOME/.cargo/env"` and
`export PATH="$HOME/.risc0/bin:$PATH"` (or open a new login shell).

### Build

```bash
mkdir -p target/tmp            # required by .cargo/config.toml (EXDEV workaround)
# Pipeline binaries + zkVM guest ELFs
./scripts/setup/setup_local_e2e.sh --build
```

A successful build compiles the feature-gated Kafka producer/consumer,
FoundationDB-backed aggregator/querier, and the zkVM guest ELFs. Complete
both Step 1 commands as the functional check; building by itself does not
exercise the analytics or proving paths.

## Step 1 — Functional checks

These two runs validate the core native analytics and zkVM execution paths
without hours of proving.

> **What dev mode means.** Setting `RISC0_DEV_MODE=1` makes RISC Zero execute
> the guest program and produce its journal, but return a fake receipt without
> any cryptographic proof. That receipt verifies only while dev mode is also
> enabled and provides no security. The dev and real-ZK CSVs deliberately use
> identical schemas: in a dev CSV, `prove_ms` or `prove_ms_total` measures local
> guest execution rather than proof generation. Because there is no
> cryptographic proof, dev-mode `verify_ms` and `proof_bytes` are both recorded
> as zero and must not be used
> to evaluate proof generation, proof verification, or proof size. Dev mode
> also runs here with one local aggregator, so it does not validate distributed
> scaling. See the RISC Zero
> [`DevModeProver` documentation](https://docs.rs/risc0-zkvm/latest/risc0_zkvm/struct.DevModeProver.html).

```bash
# (a) Native (non-ZK) baseline — runs the exact aggregation/query analytics
#     locally with one aggregator and regenerates CSVs under results/. About two
#     minutes on a 64-core machine after compilation; a cold build takes longer.
make eval-non-zk-baseline

# (b) Local zkVM guest checks in DEV MODE (RISC0_DEV_MODE=1): the aggregation
#     and query guests execute and witnesses are generated, but no SNARK proof
#     is created. Uses one aggregator and writes results/zkvm_dev_*.csv.
make eval-zkvm-dev-mode
```

If both complete and write the expected CSVs under `results/`, the artifact's
native analytics and zkVM guest/witness paths are functional. The dev-mode run
does not validate cryptographic proof generation or verification; use the real
proof runs in Steps 2 and 3 for those claims.

## Step 2 — Standalone experiments

Times are order-of-magnitude on a 56-core AVX-512 node; smaller machines are
proportionally slower. The experiments below are independent unless noted, so
reviewers may select a subset. These experiments use one machine and do not
require the shared cluster setup in Step 3.

### §7.2 — online commitment throughput

This single-machine experiment needs one CPU core and synthetic input. It takes
minutes. Sweep `--key-cardinality` or `--events` to evaluate additional input
shapes.

```bash
BENCH_INPUT=synthetic cargo run -p data_source --bin data_source --release -- \
  --streaming --bench --events 1000000 --key-cardinality 4096
```

Use the reported serial measurement to evaluate the paper's online
commitment-throughput claim:

```text
hash_fn=sha256
key_cardinality=4096
serial_ns_per_event=<measured value>
```

Convert nanoseconds per event to events per second and compare with the §7.2
range of 1.6–6.7 million commitments/s.

### Figure 6 — zkVM aggregator benchmark

Figure 6 is the standalone aggregator benchmark. It uses synthetic epochs and
does not include Kafka, RocksDB, FoundationDB, or network transmission. First,
the following dev-mode command executes each aggregation guest for one
16,384-event epoch at every Figure 6 x-axis point:

```bash
make eval-fig6-aggregator-dev
```

Expected output: `results/zkvm_dev_aggregation.csv`. Its `unique_keys` column
contains 256, 512, 1024, 2048, and 4096 for every aggregation mode; the epoch
always contains 16,384 events, so `events_per_key` is respectively 64, 32, 16,
8, and 4. It has exactly the same columns and stdout table layout as the real-ZK
CSV shown below. In the dev output, `prove_ms_total` is guest execution time,
`max_rss_kb` is peak RSS reported by `/usr/bin/time -v` (Linux reports
this in KiB). The former column name was `time_max_rss_kb`; `time` identified
the measurement tool, not a duration. There is no real prover process in dev mode. `verify_ms` and `proof_bytes` are therefore zero; the underlying
insecure dev receipt is deliberately not reported as a proof.

For measured cryptographic proofs, run:

```bash
make eval-fig6-aggregator-zk
```

This requires AVX-512 and many cores and takes hours. Expected output:
`results/zkvm_aggregation_56threads.csv`, with the same 15 directly measured
`mode × unique_keys` points:

```text
mode,unique_keys,events_per_key,threads,epoch_events,prove_ms_total,verify_ms_total,proc_hwm_kb,max_rss_kb,proof_bytes,journal_bytes
```

Compare proving time, verification time, and peak RSS with Figure 6. The command
runs the aggregator benchmark directly; storage and transmission costs belong
to Table 2 and the end-to-end experiments, not this figure.

For a reduced real-proof check, select points explicitly, for example:

```bash
FIG6_MODES=histogram FIG6_KEYS=256 make eval-fig6-aggregator-zk
```

Valid `FIG6_MODES` values are `samples`, `histogram`, and `cm`. Both
`FIG6_MODES` and `FIG6_KEYS` accept space-separated selections, for example:

```bash
FIG6_MODES="samples cm" FIG6_KEYS="256 4096" \
  make eval-fig6-aggregator-zk
```

### Figure 7 — zkVM query benchmark

Figure 7 is the standalone query benchmark over in-memory synthetic epochs. It
does not require Kafka, RocksDB, or FoundationDB. Run the dev-mode shapes first:

```bash
make eval-fig7-query-dev
```

Expected output: `results/zkvm_dev_query.csv`, with 1, 2, 4, 8, and 16 queried
epochs. Its columns and stdout table layout are identical to the real-ZK query
output shown below. In this dev CSV, `prove_ms` is guest execution time and
`max_rss_kb` is the measured dev-process peak RSS. `verify_ms` and `proof_bytes`
are zero because dev mode performs no cryptographic proof or verification.

For real proofs at the reviewer-feasible 1/2/4-epoch points, run:

```bash
make eval-fig7-query-zk
```

Expected output: `results/zkvm_query_proofs.csv`:

```text
epoch_type,query,num_epochs,events_per_epoch,keys,prove_ms,verify_ms,max_rss_kb,proof_bytes
```

There should be rows for 1, 2, and 4 epochs. Compare `prove_ms`, `verify_ms`,
and `proof_bytes` with the corresponding small-epoch points in Figure 7.
This target runs 15 independent proofs, not three process invocations. A
cost-limited partial run on the Xeon Gold 6142/RISC Zero 3.0.6 node completed
the two 1-epoch samples receipts:

```text
epoch_type,query,num_epochs,events_per_epoch,keys,prove_ms,verify_ms,max_rss_kb,proof_bytes
samples,samples_sum,1,8192,1024,698549,32,9618860,224050
samples,samples_sum_topk,1,8192,1024,782927,33,9618860,224202
```

These rows support the constant, millisecond-scale verification and compact
proof-size claims. They do not by themselves establish the 2/4-epoch scaling
claim; a complete CSV must contain all 15 rows.

## Step 3 — Distributed experiments

Complete Step 0 on the coordinator and configure the shared cluster below
before running Figure 4, Table 2, or Figure 5/Table 3.

### Shared cluster setup

Kafka defaults to `node0`. All nodes must resolve that hostname to the
coordinator; use `KAFKA_HOST` or `--kafka-host` to override it on other clusters.

All distributed artifact experiments share the ordered machine pool in
`.artifact-cluster.env`. Configure passwordless SSH, validate Kafka reachability,
and optionally deploy the worker binaries with:

```bash
./scripts/setup/setup_artifact_cluster.sh \
  --machines "node0 <node1-ip> <node2-ip> <node3-ip> <node4-ip> <node5-ip> <node6-ip> <node7-ip>" \
  --ssh-user <username> \
  --kafka-host node0 \
  --fdb-cluster-file /etc/foundationdb/fdb.cluster \
  --copy-keys --install-deps --deploy
```

For a CloudLab allocation named `node0` through `node7`, the equivalent
copy-ready command is:

```bash
./scripts/setup/setup_artifact_cluster.sh \
  --machines "node0 node1 node2 node3 node4 node5 node6 node7" \
  --ssh-user "$USER" \
  --kafka-host node0 \
  --fdb-cluster-file /etc/foundationdb/fdb.cluster \
  --copy-keys --install-deps --deploy
```

The first entry is the local coordinator and is not contacted over SSH.
If Step 0 created a local-only, disposable FDB database, add `--reset-fdb` once
to the deployment command to delete and recreate it at the IPv4 address resolved
from `--kafka-host` (or set `FDB_PUBLIC_ADDRESS` explicitly). Worker database
connectivity is checked before building the binaries. Omit `--reset-fdb` on
subsequent deployments to retain the data.
`--copy-keys` may ask for each worker password once; subsequent evaluation runs
are non-interactive. The generated `.artifact-cluster.env` is ignored by Git
and is loaded automatically by Figure 4, Table 2, Figure 5, and Table 3.
Complete Step 0 on the coordinator first. `--install-deps` installs the system
packages, Rust/RISC Zero toolchain, and FoundationDB client on every worker;
workers connect to the shared Kafka and FoundationDB services and do not start
their own servers. It requires passwordless `sudo` on the workers (the default
on CloudLab nodes). `--deploy` then copies `kafka-producer`, `kafka-consumer`,
`aggregator`, `querier`, the memory tracer, and the FDB cluster file to every
worker, and verifies that each deployed file is usable. Thus, Step 0 starts
Kafka and FoundationDB once on the coordinator, while this command installs all
worker-side dependencies.

> **Kafka/FDB endpoints.** Table 2, Figure 4, Figure 5, and Table 3 use the
> storage-backed `run_distributed_baseline.sh` pipeline, which connects to Kafka
> and FoundationDB. Use `setup_artifact_cluster.sh` above to store the shared
> coordinator address, FDB cluster file, machine pool, and worker deployment.
> Kafka must advertise an address reachable from every worker; `localhost` is
> valid only for a one-machine diagnostic, not a distributed run.

### Figure 4 — vehicle-emissions end-to-end pipeline

Figure 4 measures the end-to-end zk-Analytics pipeline. The initial artifact
path uses only the bundled vehicle-emissions dataset and four real aggregators.
It includes commitment generation, Kafka, RocksDB, aggregation, FoundationDB,
and the query. From `node0`, with three SSH-reachable workers, run the functional
dev-mode version:

```bash
KAFKA_HOST=node0 make eval-fig4-vehicle-dev
```

Expected output: `results/e2e_dev_zk/vehicle_dev_zk.jsonl`. This validates the
distributed execution checks below but does not produce a cryptographic proof. For the
real-ZK run on the same topology:

```bash
KAFKA_HOST=node0 make eval-fig4-vehicle-zk
```

Expected output: `results/e2e_real_zk/vehicle_real_zk.jsonl`. Only this second
command may be used for proof-generation and proof-verification performance.

Real proofs can take hours on CPU. Real-ZK runs wait without a time limit for
aggregator completion (`AGG_MAX_WAIT=0`; a positive value imposes a limit).
Queries run locally through `BENCH_REQUEST`, without HTTP or a query timeout,
and produce three JSON results using the same query handler and proof code.
Database, merge, proof, verification, and memory measurements are retained.
Query progress is recorded in
`/mydata/zk-analytics-runs/dist_run/vehicle_histogram_n4_zk/logs/querier.log`,
with prover CPU and memory samples in the adjacent `../mem_query.csv`.
These paths use the default `RUN_ROOT`.

The Figure 5/Table 3 and Table 2 pipeline targets share this local query path.
The single-machine e2e baseline also runs three local queries without a time
limit and fails the run if a query fails. Figure 6/7 standalone benchmarks
already wait for proof completion without a deadline. The standalone
distributed zkVM sweep now waits for all completion markers before collecting
results. In the general distributed e2e runner, waiting for the first proved
epochs is also unlimited by default (`EPOCH_WAIT_TIMEOUT_SEC=0`).
Service connection/readiness checks and epoch flush/idle timers retain their
limits; they do not impose a deadline on an active proof.

#### Validate the run

A successful run requires zero exit codes from every aggregator, nonempty
epoch timing records on every selected node, the requested number of ingested
logs, and three successful JSON query responses. The result records
`ingested_logs` and `epochs_per_node`. Missing timing records cause failure
instead of becoming zero-valued measurements. In real-ZK mode, receipt
verification is performed by the pipeline and proof records must be present.
These checks do not replace an independent comparison of query values against
a reference computation over the input dataset.

`total_time_s` in the aggregation record is the slowest node's sum of epoch
times; it excludes ingestion and orchestration. `agg_wall_clock_s` includes
launching, polling and shutdown. Neither field alone is full pipeline latency.
The current Figure 4 PDF labels aggregation and query times separately. It is
a diagnostic plot; matching the paper's Figure 4 panels and time definitions
requires checking the published figure before interpreting it as reproduction.

### Table 2 — vanilla baseline on the end-to-end setups

Table 2 uses the same real datasets and distributed topologies as Figure 4:
Google Cluster with eight aggregators, CAIDA with eight aggregators, and Vehicle
Emissions with four aggregators. It is not a single-machine synthetic sweep.
The pipeline includes log commitment, Kafka ingestion, RocksDB raw storage,
parallel native aggregation, FoundationDB storage, and the native query.

Using the bundled vehicle-emissions data, measure the Vehicle column with four
machines:

```bash
KAFKA_HOST=node0 TABLE2_SPECS=vehicle:4 \
  make eval-table2-native
```

Expected output: `results/table2_native.csv` and
`results/table2_native/vehicle_native.jsonl`. The row is measured using four
actual aggregator machines; no multi-node values are inferred. The CSV is
organized around the Vanilla columns in the paper table:

```text
dataset,num_aggregators,epochs_on_critical_node,rocksdb_insert_ms_per_epoch,rocksdb_read_ms_per_epoch,fdb_write_ms_per_epoch,fdb_read_ms_per_query,aggregation_compute_ms_per_epoch,query_ms_per_query,agg_peak_rss_mb_per_node,query_peak_rss_mb
```

`aggregation_compute_ms_per_epoch` corresponds to Table 2's “Total Aggregation
time (in parallel).” Kafka transmission is not added to this value; RocksDB and
FoundationDB costs are reported in their own columns. The ZK columns in Table 2
come from the matching real-ZK Figure 4 runs, not from this Vanilla command.

Native processes are sampled every 10 ms, with GNU `time` maximum RSS as a
fallback. Press Ctrl-C once to terminate the current cell. If an abnormal
termination leaves benchmark processes behind, run `make eval-kill` locally or
`./scripts/util/kill_bench_processes.sh --all-machines` for the cluster.

### Figure 5 and Table 3 — distributed aggregation

These experiments require up to eight SSH-reachable machines plus Kafka and
FoundationDB. These are distributed zk-Analytics experiments, not extrapolated
single-machine rows and not a native scaling sweep.

#### Select machines and aggregator counts

`ARTIFACT_MACHINES` is the resulting ordered pool. A point requesting `N`
aggregators uses its first `N` entries. For example, run every mode on exactly
four machines with:

```bash
KAFKA_HOST=node0 \
ARTIFACT_MACHINES="node0 node1 node2 node3" \
FIG5_NUM_AGGREGATORS=4 \
make eval-fig5-table3-zk
```

Or run only one point on four chosen machines:

```bash
KAFKA_HOST=node0 \
ARTIFACT_MACHINES="node0 node2 node5 node7" \
FIG5_SPECS="histogram:4" \
make eval-fig5-table3-zk
```

The first `ARTIFACT_MACHINES` entry is the coordinator running locally;
subsequent entries may be SSH aliases or `user@host` targets and must support
passwordless SSH. The command rejects a requested aggregator count larger than
the configured machine pool. The two Make targets force `RISC0_DEV_MODE=1` and
`RISC0_DEV_MODE=0`, respectively, regardless of the caller's environment.

#### Run the full sweep

After completing the shared cluster setup above, run the functional dev-mode
sweep with:

```bash
KAFKA_HOST=node0 make eval-fig5-table3-dev
```

Then start the real-proof sweep with:

```bash
KAFKA_HOST=node0 make eval-fig5-table3-zk
```

Expected outputs are `results/fig5_dev.csv` and `results/fig5_zk.csv`. Both
commands directly measure all 12 combinations of three aggregation modes and
1, 2, 4, or 8 aggregators by default. Override `FIG5_SPECS` only for a reduced
smoke test. For example, `FIG5_SPECS="samples:1,2,4,8"` runs the four listed
aggregator counts for samples, while `FIG5_SPECS="samples:8"` runs only the
eight-aggregator samples point. Multiple selections can be combined, such as
`FIG5_SPECS="samples:1,8 histogram:4,8"`. Alternatively,
`FIG5_NUM_AGGREGATORS="2,4,8"` applies the same count list to all three modes.

#### Run selected points

Run selected aggregator counts for one aggregation mode:

```bash
# Samples with 1, 2, 4, and 8 aggregators
KAFKA_HOST=node0 \
  FIG5_SPECS="samples:1,2,4,8" \
  make eval-fig5-table3-dev

# Samples with only 8 aggregators
KAFKA_HOST=node0 \
  FIG5_SPECS="samples:8" \
  make eval-fig5-table3-dev
```

Apply the same aggregator-count list to all three modes (`samples`,
`histogram`, and `cm`):

```bash
KAFKA_HOST=node0 \
  FIG5_NUM_AGGREGATORS="1,2,4,8" \
  make eval-fig5-table3-dev
```

#### Workload and metrics

The fixed paper workload contains 16,384 logs total, divided into
eight epochs of 2,048 logs. Each Kafka commit batch contains eight logs, so an
epoch contains 256 commit batches. Every row is produced by the requested
number of real machines.
The runner starts exactly one aggregator on each selected machine; it never
places multiple aggregators on one machine. The CSV reports aggregation
prove/verify time, Kafka/RocksDB/FDB components, host and prover RSS, proof
size, and query costs. Dev mode executes the same zkVM guests but does not
create real STARK proofs; use the ZK output for proof size and real proving
costs. Compare aggregation time and speedup with Figure 5, and the component
measurements with Table 3. Expect the real-ZK run to take many hours.
The runner prints the CSV header before the sweep and labels each subsequent
line as a result row, so terminal output has the same field ordering as the
saved CSV.

## Step 4 — Plots and output index

### Generate plots

After running any desired subset of experiments, generate every plot whose
measured input is available with:

```bash
make eval-plots-all
```

This command does not rerun experiments or invent missing values. It prints a
`skipped` message for every plot whose input CSV/JSONL is absent. Compare the
regenerated numbers and plots to the corresponding paper Figure/Table — exact
wall-clock will vary with hardware, but the **trends** (near-linear aggregation
speedup, constant verification, compact proofs, aggregation-dominated latency)
are the claims under evaluation.

### Experiment output index

The following table is the complete output index. Result files under `results/`
contain the measured values; PDFs under `plots/` are derived visualizations.
Both directories are git-ignored and created on demand.

| Experiment | Command | Measured result files | Plot or summary |
|---|---|---|---|
| §7.2 online commitment throughput | `BENCH_INPUT=synthetic cargo run ... --streaming --bench` | Terminal output (`serial_ns_per_event`); no result file | No plot |
| Native/ZK comparison inputs | `make eval-non-zk-baseline` or `make eval-non-zk-all` | `results/non_zk_aggregation_baseline.csv`, `results/non_zk_query_baseline.csv`, `results/zk_cost_breakdown.csv` | `results/non_zk_baseline_summary.md` when measured ZK inputs exist; `plots/non_zk_vs_zk_aggregation.pdf`, `plots/non_zk_vs_zk_query.pdf`, `plots/zk_cost_breakdown.pdf` |
| Local zkVM dev checks | `make eval-zkvm-dev-mode` | `results/zkvm_dev_aggregation.csv`, `results/zkvm_dev_query.csv` | Inputs to Figure 6/7 plots |
| Figure 4 vehicle, dev | `KAFKA_HOST=node0 make eval-fig4-vehicle-dev` | `results/e2e_dev_zk/vehicle_dev_zk.jsonl` | `plots/fig4_e2e.pdf` |
| Figure 4 vehicle, real ZK | `KAFKA_HOST=node0 make eval-fig4-vehicle-zk` | `results/e2e_real_zk/vehicle_real_zk.jsonl` | `plots/fig4_e2e.pdf` |
| Figure 5 / Table 3, dev | `KAFKA_HOST=node0 make eval-fig5-table3-dev` | `results/fig5_dev.csv` | `plots/fig5_scaling.pdf`, `plots/table3_cost_components.pdf` |
| Figure 5 / Table 3, real ZK | `KAFKA_HOST=node0 make eval-fig5-table3-zk` | `results/fig5_zk.csv` | `plots/fig5_scaling.pdf`, `plots/table3_cost_components.pdf` |
| Figure 6 aggregator, dev | `make eval-fig6-aggregator-dev` | `results/zkvm_dev_aggregation.csv` | `plots/fig6_aggregation.pdf` |
| Figure 6 aggregator, real ZK | `make eval-fig6-aggregator-zk` | `results/zkvm_aggregation_56threads.csv` | `plots/fig6_aggregation.pdf` |
| Figure 7 query, dev | `make eval-fig7-query-dev` | `results/zkvm_dev_query.csv` | `plots/fig7_query.pdf` |
| Figure 7 query, real ZK | `make eval-fig7-query-zk` | `results/zkvm_query_proofs.csv` | `plots/fig7_query.pdf` |
| Table 2 native vehicle pipeline | `KAFKA_HOST=node0 TABLE2_SPECS=vehicle:4 make eval-table2-native` | `results/table2_native.csv`, `results/table2_native/vehicle_native.jsonl` | `plots/table2_native.pdf` |
| Native Google/CAIDA end-to-end baseline | `make eval-non-zk-e2e` | `results/non_zk_e2e_baseline.csv` | Included in the non-ZK summary when present |

## Troubleshooting

- `Invalid cross-device link (EXDEV)` during build → ensure `mkdir -p target/tmp`
  (referenced by `.cargo/config.toml`).
- `cargo:warning ... curl/curl.h` when building `--features kafka` → install
  `libcurl4-openssl-dev libsasl2-dev zlib1g-dev`.
- zkVM proving OOMs or is extremely slow → you are likely without AVX-512 or with
  too little RAM; use `make eval-zkvm-dev-mode` for functional validation
  instead, and reduce `SYNTH_KEYS` / epoch sizes.
- Reset local state between runs: `./scripts/setup/reset_rocksdb.sh`,
  `./scripts/setup/reset_fdb.sh`.
- `make eval-non-zk-baseline` on a fresh clone produces **native-only** CSVs
  under `results/` (the repo ships no measured-ZK data); the ZK-comparison
  columns/summary populate after you run `make eval-zkvm-aggr-56` (real proofs)
  or `make eval-zkvm-dev-mode` (fast). This is expected, not an error.
- PDF plots need matplotlib (`pip install matplotlib`); without it the run
  still completes and just skips the plots with a warning.
