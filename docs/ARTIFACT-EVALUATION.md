# Artifact Evaluation: Zero-Knowledge Cloud Analytics

## Artifact description

This artifact contains the source code, datasets that may be redistributed, and
instructions needed to reproduce the experiments in *"Zero-Knowledge Cloud
Analytics."* More precisely, it contains:

- instructions to configure a local machine or multi-node cluster;
- instructions to build the data source, aggregation and query components,
  including their RISC Zero zkVM guests;
- scripts to launch the experiments and generate CSV and PDF results; and
- a short functional evaluation that does not require hours of proof generation.

By running the experiments, reviewers can reproduce or validate the results in:

- **Figure 4 and Tables 1–2:** end-to-end performance on Google cluster, CAIDA,
  and vehicle-emissions workloads;
- **Figure 5 and Table 3:** distributed aggregation scalability;
- **Figure 6:** single-machine aggregation time, proof verification, proof size,
  and memory;
- **Figure 7:** query proving and verification as the number of epochs grows;
  and
- **§7.2:** online SHA-256 log-commitment throughput.

The repository contains no precomputed result files. Each experiment creates
its own `results/` and `plots/` outputs locally. Reviewers should compare these
outputs with the corresponding paper figure or table; exact wall-clock values
vary with hardware.

## Artifact location

<https://github.com/Froot-NetSys/zk-Analytics>

## Artifact commit/tag/version

Use the commit or release tag supplied in the SIGCOMM 2026 artifact submission.
During kick-the-tires, the tip of `main` may contain documentation or portability
fixes reported by reviewers. The final evaluated version will be preserved as an
immutable release/archive.

## Hardware requirements

**Requires specific hardware for full reproduction.** The short functional
evaluation runs on a single x86-64 machine with 8 GB RAM and 10 GB free disk.
Efficient real proof generation requires AVX-512, at least 64 GB RAM, and many
CPU cores. The paper used CloudLab `c6420` machines, each with two 16-core
Intel Xeon Gold 6142 CPUs (32 physical cores total).
Distributed Figure 4/5 and Table 3 experiments require up to eight machines
reachable over SSH, plus Kafka and FoundationDB. Native and dev-mode checks do
not require this cluster.

## Comments for the AEC

- Real proof generation takes hours per experiment. Larger Figure 4 and Figure
  7 points can take days; the guide identifies reduced-scale proof runs.
- The vehicle-emissions dataset is bundled. Google Cluster v3 must be downloaded
  separately. CAIDA traces require an academic data-sharing agreement and cannot
  be redistributed by the authors.
- Only one evaluator should use a shared Kafka/FoundationDB deployment at a
  time. Reset commands are listed under Troubleshooting.
- Please report setup or execution problems through the artifact-submission
  discussion channel so the instructions can be corrected during kick-the-tires.

## Claims under evaluation

1. **Functional** — the system builds and runs the three pipeline stages
   (data-source SHA-256 log commitment → distributed RISC Zero aggregation →
   verifiable RISC Zero query engine), and produces verifiable proofs.
2. **Low online + verification overhead** — log commitment and proof
   *verification* are cheap (sub-100 ms verification; millions of commits/s).
3. **Aggregation dominates, and scales** — proof *generation* is the dominant
   cost and parallelizes near-linearly across aggregators (Fig 5).
4. **Practical at moderate scale** — end-to-end proof generation completes in
   hours, proofs stay compact, verification stays constant (Figs 4, 6, 7).

## Detailed instructions

### Requirements

| | Functional check / native baselines | Full ZK reproduction |
|---|---|---|
| CPU | any x86-64 | **AVX-512**, many cores (paper: CloudLab `c6420`, dual 16-core Intel Xeon Gold 6142) |
| RAM | 8 GB | ≥ 64 GB (prover peaks ~9–10 GB/node; parallel proving needs headroom) |
| Disk | **10 GB free** | 50+ GB free (RocksDB/FoundationDB + datasets) |
| Time | minutes | **hours per experiment** (see table below) |

The setup script supports Ubuntu/Debian and uses `sudo` to install packages.
`scripts/setup/setup_local_e2e.sh --all` installs `clang`/`libclang` (RocksDB),
the RISC Zero toolchain via `rzup` (guest compiler + `r0vm`), Docker, Kafka, and
FoundationDB 7.1. Docker, Kafka, FoundationDB, and `protoc` are needed only for
the distributed/end-to-end paths. See the README "Build" section for the manual
dependency list.

### Step 0 — Setup and build

```bash
git clone https://github.com/Froot-NetSys/zk-Analytics
cd zk-Analytics
```

#### Option A — install all prerequisites (full local environment)

Use the repository setup script on Ubuntu/Debian to install the build tools,
RISC Zero toolchain, Docker, Kafka, and FoundationDB, and to start the local
Kafka/FoundationDB containers:

```bash
./scripts/setup/setup_local_e2e.sh --all
```

The script invokes `sudo`. If it adds your account to the `docker` group, log
out and back in before running Docker without `sudo`.

#### Option B — install only the quick-start prerequisites

Use this smaller installation when evaluating only the build, native baseline,
and zkVM dev-mode targets:

```bash
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev cmake \
  libssl-dev pkg-config python3 python3-matplotlib

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
  sh -s -- -y
source "$HOME/.cargo/env"

curl -fsSL https://risczero.com/install | bash
export PATH="$HOME/.risc0/bin:$PATH"
rzup install
```

Confirm that both the host and RISC Zero tools are available:

```bash
clang --version
cargo --version
rzup show
r0vm --version
```

All four commands must succeed. The RISC Zero installer requires `rustc`, so the
Rust installation above must come first on a clean machine. If `cargo` or `rzup`
is not found immediately after installation, source `$HOME/.cargo/env` and add
`$HOME/.risc0/bin` to `PATH` as shown above (or open a new login shell).

#### Build

```bash
mkdir -p target/tmp            # required by .cargo/config.toml (EXDEV workaround)
cargo build --release          # host crates + RISC Zero guest ELFs
```

A successful build compiles all host crates and the zkVM guest ELFs. Complete
both Step 1 commands as the functional check; building by itself does not
exercise the analytics or proving paths.

### Step 1 — Kick the tires (minutes)

These two runs validate the core native analytics and zkVM execution paths
without hours of proving.

```bash
# (a) Native (non-ZK) baseline — runs the exact aggregation/query analytics
#     natively and regenerates CSVs under results/. About two minutes on a
#     64-core machine after compilation; a cold build takes longer.
make eval-non-zk-baseline

# (b) zkVM pipeline in DEV MODE (RISC0_DEV_MODE=1): guests are executed and the
#     witness is generated, but no STARK proof. Exercises the real proving path
#     end-to-end in minutes. Writes results/zkvm_dev_*.csv.
make eval-zkvm-dev-mode
```

If both complete and write the expected CSVs under `results/`, the artifact's
native analytics and zkVM guest/witness paths are functional. The dev-mode run
does not validate cryptographic proof generation or verification; use the real
proof rows in Step 2 for those claims.

### Datasets

| Dataset | Status | How to obtain |
|---|---|---|
| Vehicle CO2 emissions (Canada) | **bundled** | `testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv` |
| Google Cluster v3 | not bundled | Download the public Google cluster-usage traces (v3) and place the per-machine CSVs under `testdata/google_cluster_data/input/`. |
| CAIDA backbone traces | not bundled | Requires CAIDA's academic **data-sharing agreement** (cannot be redistributed). Obtain a PCAP, then `PCAP=/path/to.pcap.gz ./scripts/setup/prep_caida.sh` → `testdata/caida_pcap/caida_txt/`. |

The synthetic-workload experiments (Figs 5–7, §7.2) need **no external data** —
they are the most self-contained to reproduce.

### Step 2 — Reproduce the experiments

Times are order-of-magnitude on a 56-core AVX-512 node; smaller machines are
proportionally slower. The experiments below are independent unless noted, so
reviewers may select a subset.

#### §7.2 — online commitment throughput

This single-machine experiment needs one CPU core and synthetic input. It takes
minutes. Sweep `--key-mod` or `--events` to evaluate additional input shapes.

```bash
BENCH_INPUT=synthetic cargo run -p data_source --bin data_source --release -- \
  --streaming --bench --events 1000000 --key-mod 4096
```

Use the serial measurement to evaluate the paper's online commitment-throughput
claim. For example, a CloudLab `c6420` node produced:

```text
hash_fn=sha256
serial_ns_per_event=148.059
```

Convert nanoseconds per event to events per second and compare with the §7.2
range of 1.6–6.7 million commitments/s. The example corresponds to 6.754
million commitments/s, which meets (and slightly exceeds) the paper's reported
range.

#### Figure 6 — single-machine native aggregation

This run needs local Kafka and FoundationDB and takes approximately 30 minutes
on the paper machine.

```bash
KAFKA_HOST=localhost FIG=6 ./scripts/eval/run_figures_native.sh
```

Expected output: `results/fig6_native.csv`. It contains one row for every
`var` (number of keys) and aggregation `mode`, with the following header:

```text
var,mode,agg_total_s,kafka_recv_s,rocksdb_raw_insert_s,rocksdb_raw_read_s,aggr_compute_s,fdb_write_s,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s,agg_per_node_host_rss_mb,agg_cluster_host_rss_mb,agg_prover_rss_mb,query_rss_mb,query_prover_rss_mb
```

Compare the native timing and memory columns with the native series in Figure
6. A 2026-09-03 run on a 32-core/64-thread Xeon Gold 6142 produced all 15
rows. Representative rows (seconds and MB) were:

```text
var,mode,agg_total_s,aggr_compute_s,fdb_write_s,agg_cluster_host_rss_mb,query_total_s
256,samples,0.062357,0.028523,0.012718,32.73,0.006
1024,histogram,0.072858,0.031743,0.020035,33.88,0.000
4096,cm,0.098553,0.043971,0.034385,32.57,0.000
```

Sub-millisecond query components round to `0.0` in this CSV. An aggregation
row with `agg_total_s=0` is invalid; the runner now stops instead of silently
accepting incomplete metrics.

#### Figure 6 — single-machine ZK aggregation

This run generates real proofs. It requires AVX-512 and many cores and takes
hours (roughly 28 minutes for histogram, 86 minutes for samples, and 185 minutes
for Count-Min on the paper machine).

```bash
KAFKA_HOST=localhost FIG=6 SYNTH_KEYS=1024 \
  ./scripts/eval/run_figures_zk.sh
```

Expected output: `results/fig6_zk.csv`, with one row for each of `histogram`,
`samples`, and `cm`. Its header is:

```text
var,mode,agg_total_s,prove_s,verify_s,kafka_recv_s,rocksdb_raw_insert_s,fdb_write_s,agg_host_rss_mb,agg_prover_rss_mb,agg_cluster_rss_mb,proof_bytes,journal_bytes,query_total_s,query_prove_s,query_verify_s,query_fdb_lookup_s,query_host_rss_mb,query_prover_rss_mb
```

Compare `prove_s`, `verify_s`, `proof_bytes`, and the RSS columns with Figure 6.
Do not use the paper-machine estimates above as watchdog timeouts: proving is
hardware- and RISC Zero-version-dependent. For example, the same Xeon Gold
6142 node with RISC Zero 3.0.6 produced this successfully verified histogram
row on 2026-09-04:

```text
var,mode,agg_total_s,prove_s,verify_s,agg_cluster_rss_mb,proof_bytes,journal_bytes,query_total_s,query_prove_s,query_verify_s
1024,histogram,7567.390,7567.235,0.034,9493.1,229544,793,960.269,960.233,0.034
```

#### Figure 7 — native query scaling

This run uses local Kafka and FoundationDB and takes approximately 30 minutes.

```bash
KAFKA_HOST=localhost ./scripts/eval/run_fig7_native.sh
```

Expected output: `results/fig7_native.csv`. It contains query measurements for
increasing `queried_epochs` and has this header:

```text
epoch_type,query,queried_epochs,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s
```

Compare `query_total_s` and its component columns with the native curves in
Figure 7. The Xeon Gold 6142 reference run above produced all 27 rows. The
endpoints were:

```text
epoch_type,query,queried_epochs,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s
samples,samples_sum,1,0.344000,0.344,0.0,0.0
samples,samples_sum,256,0.327000,0.327,0.0,0.0
histogram,histogram_p90,1,0.167000,0.167,0.0,0.0
histogram,histogram_p90,256,0.229000,0.175,0.054,0.0
cm,cm_topk,1,0.044000,0.043,0.001,0.0
cm,cm_topk,256,0.071000,0.059,0.012,0.0
```

The histogram and CM merge components increase with the number of queried
epochs; the samples query is dominated by a roughly constant FDB lookup in
this run. Millisecond instrumentation rounds smaller components to `0.0`.

#### Figure 7 — ZK query proofs

This reduced 1/2/4-epoch run produces real proofs, requires AVX-512, and takes
approximately 1–2 hours on the paper machine.

```bash
make eval-zkvm-query-proofs
```

Expected output: `results/zkvm_query_proofs.csv`, containing:

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
samples,global_sum,1,8192,1024,698549,32,9618860,224050
samples,topk_hash,1,8192,1024,782927,33,9618860,224202
```

These rows support the constant, millisecond-scale verification and compact
proof-size claims. They do not by themselves establish the 2/4-epoch scaling
claim; a complete CSV must contain all 15 rows.

#### Figure 5 and Table 3 — distributed aggregation

These experiments require up to eight SSH-reachable machines plus Kafka and
FoundationDB. Run the native sweep first:

```bash
KAFKA_HOST=<coordinator-private-address> FIG=5 \
  ./scripts/eval/run_figures_native.sh
```

Run this on `node0`, with passwordless SSH to `node1` through `node7` and the
same hostnames used by the script. `KAFKA_HOST` must be reachable from every
worker; do not use `localhost` for an eight-node run. The driver now checks the
broker and every worker before cleaning state, and exits with a diagnostic
instead of waiting indefinitely for Kafka drain.

To smoke-test only the coordinator before reserving all eight nodes, run:

```bash
KAFKA_HOST=localhost FIG=5 FIG5_SPECS=samples:1 \
  ./scripts/eval/run_figures_native.sh
```

Expected output: `results/fig5_native.csv`, with rows for 1, 2, 4, and 8
aggregators for each aggregation mode. It uses the same native CSV header shown
for Figure 6; here `var` is the number of aggregators.

Then run the real-proof subset:

```bash
KAFKA_HOST=<coordinator-private-address> FIG=5 \
  ./scripts/eval/run_figures_zk.sh
```

Expected output: `results/fig5_zk.csv`. By default it contains
`histogram:1`, `histogram:8`, `samples:8`, and `cm:8`; override `FIG5_SPECS` to
run more cells. It uses the same ZK CSV header shown for Figure 6. Compare
aggregation time and speedup with Figure 5, and the component measurements with
Table 3. Expect this run to take many hours.

#### Figure 4 and Tables 1–2 — native end-to-end workloads

Download the Google data and prepare the access-controlled CAIDA trace as
described under Datasets. Then run:

```bash
PCAP=/path/to/trace.pcap.gz ./scripts/setup/prep_caida.sh
make eval-non-zk-e2e
```

Expected output: `results/non_zk_e2e_baseline.csv`. A dataset is skipped with an
explicit `[e2e] SKIP ...` message if its input is unavailable. Successful rows
have this header:

```text
dataset,mode,bench_input,epochs,epoch_logs,total_logs,native_ms_total,native_rss_mb,zkvm_dev_exec_ms,zk_agg_proofgen_s,zk_query_proofgen_s,slowdown_native_vs_proof,zk_provenance
```

Compare the native columns with Figure 4 and the non-ZK columns in Table 2.
Without the two external datasets, the Xeon Gold 6142 synthetic control run
produced:

```text
dataset,mode,native_ms_total,native_rss_mb,zkvm_dev_exec_ms
synthetic,samples,64.923,24.6,40980
synthetic,histogram,65.456,18.9,42774
synthetic,cm,152.678,18.9,86750
```

Synthetic rows have no paper real-proof anchor, so
`slowdown_native_vs_proof` is empty and the terminal displays `n/a`. They must
not be interpreted as a `0x` slowdown claim. Google and CAIDA are explicitly
reported as `SKIP` until their external files are installed.
Depending on dataset size, this takes approximately 1–4 hours.

#### Figure 4 / Table 2 — 56-thread ZK aggregation anchor

This single-node real-proof run requires AVX-512 and 56 cores. Count-Min takes
approximately 3.5 hours on the paper machine.

```bash
make eval-zkvm-aggr-56
```

Expected output: `results/zkvm_aggregation_56threads.csv`, with one row for each
aggregation mode and this header:

```text
mode,threads,series,samples_per_series,epoch_events,prove_ms_total,verify_ms_total,proc_hwm_kb,time_max_rss_kb
```

Use `prove_ms_total`, `verify_ms_total`, and the RSS columns to compare with the
ZK aggregation values in Figure 4 and Table 2.

> **Kafka/FDB endpoints.** Every row that drives the real pipeline (the Fig 6/7
> native runs and all distributed cells go through
> `run_distributed_baseline.sh`) connects to Kafka and FoundationDB. The shipped
> scripts use RFC 5737 **placeholder IPs** (e.g. `192.0.2.1`), so point them at
> your setup first: after `setup_local_e2e.sh --all` use `KAFKA_HOST=localhost`
> (single machine), or set `KAFKA_HOST`/`KAFKA_BROKERS` + `FDB_CLUSTER_FILE` and
> the node IPs in `scripts/ip_defaults.sh` for a cluster.

Merge the measured CSVs into the comparison tables/plots with:

```bash
make eval-non-zk-all
```

Expected outputs include `results/non_zk_aggregation_baseline.csv`,
`results/non_zk_query_baseline.csv`, and `results/zk_cost_breakdown.csv`.
When measured ZK inputs are present, it also creates
`results/non_zk_baseline_summary.md` and PDFs under `plots/`.

### Distributed experiments (Fig 5, Table 3, Fig 4 distributed)

These need multiple machines reachable over SSH. Copy
`scripts/distributed_e2e_config.example.sh`, set `SSH_USER`, the node IPs
(`scripts/ip_defaults.sh`), and `KAFKA_BROKERS`/`FDB_*`, then drive the runs with
`scripts/distributed/run_distributed_baseline.sh` / `run_table2_sweep.sh`. See
`docs/DISTRIBUTED_SETUP.md` and `docs/DISTRIBUTED_E2E_GUIDE.md`. On a
single machine you can still reproduce the **native** distributed cells and all
single-machine ZK results above.

## Cost-limited claims (read before reproducing)

Some paper points are too expensive to re-run in full and are validated by
proxy; this is stated so reviewers know what to expect:

- **Fig 4 ZK end-to-end** and **Fig 7 ZK at ≥ 16 queried epochs** would take days
  of proving per dataset. They are reproduced at reduced scale (dev-mode for the
  pipeline; `eval-zkvm-aggr-56` and 1/2/4-epoch query proofs for the proving
  cost), and the larger points are compared against the paper's reported values.
- **Verification cost** (the cheap, reviewer-friendly claim) reproduces fully and
  quickly at every scale.

## Outputs and comparison

All experiments write to `results/` (CSVs, a `*_summary.md`) and `plots/` (PDFs);
both are git-ignored and created on demand. Compare your regenerated numbers and
plots to the corresponding paper Figure/Table — exact wall-clock will vary with
hardware, but the **trends** (near-linear aggregation speedup, constant
verification, compact proofs, aggregation-dominated latency) are the claims under
evaluation.

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
