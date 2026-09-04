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

- **Short functional evaluation:** one x86-64 machine with 8 GB RAM and 10 GB
  free disk. Native and dev-mode checks run on one machine, but do not validate
  distributed scaling.
- **Real proof generation:** an AVX-512 CPU, at least 64 GB RAM, and many CPU
  cores.
- **Paper hardware:** CloudLab `c6420` machines, each with two 16-core Intel
  Xeon Gold 6142 CPUs (32 physical cores total).
- **Distributed experiments:** fully validating aggregator scaling in Figure 5
  and Table 3, or all three Table 2 dataset columns, requires eight
  SSH-reachable CloudLab `c6420` machines (or equivalent), plus Kafka and
  FoundationDB. The bundled vehicle-emissions Figure 4/Table 2 run uses four
  of these machines.

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
#     and query guests execute and witnesses are generated, but no STARK proof
#     is created. Uses one aggregator and writes results/zkvm_dev_*.csv.
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

#### Figure 6 — zkVM aggregator benchmark

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
`time_max_rss_kb` is the measured dev-process peak RSS, and there is no real
prover process. `verify_ms` and `proof_bytes` are therefore zero; the underlying
insecure dev receipt is deliberately not reported as a proof.

For measured cryptographic proofs, run:

```bash
make eval-fig6-aggregator-zk
```

This requires AVX-512 and many cores and takes hours. Expected output:
`results/zkvm_aggregation_56threads.csv`, with the same 15 directly measured
`mode × unique_keys` points:

```text
mode,unique_keys,events_per_key,threads,epoch_events,prove_ms_total,verify_ms_total,proc_hwm_kb,time_max_rss_kb,proof_bytes,journal_bytes
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

#### Figure 7 — zkVM query benchmark

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
samples,global_sum,1,8192,1024,698549,32,9618860,224050
samples,topk_hash,1,8192,1024,782927,33,9618860,224202
```

These rows support the constant, millisecond-scale verification and compact
proof-size claims. They do not by themselves establish the 2/4-epoch scaling
claim; a complete CSV must contain all 15 rows.

#### Figure 5 and Table 3 — distributed aggregation

These experiments require up to eight SSH-reachable machines plus Kafka and
FoundationDB. These are distributed zk-Analytics experiments, not extrapolated
single-machine rows and not a native scaling sweep. Run on `node0`, with
passwordless SSH to `node1` through `node7`:

```bash
KAFKA_HOST=<coordinator-private-address> make eval-fig5-table3-zk
```

Expected output: `results/fig5_zk.csv`. By default it directly measures all 12
combinations of three aggregation modes and 1, 2, 4, or 8 aggregators. Override
`FIG5_SPECS` only for a reduced smoke test. Every row is produced by the
requested number of real machines.
The CSV reports aggregation prove/verify time, Kafka/RocksDB/FDB components,
host and prover RSS, proof size, and query costs. Compare aggregation time and
speedup with Figure 5, and the component measurements with Table 3. Expect this
run to take many hours.

#### Figure 4 — vehicle-emissions end-to-end pipeline

Figure 4 measures the end-to-end zk-Analytics pipeline. The initial artifact
path uses only the bundled vehicle-emissions dataset and four real aggregators.
It includes commitment generation, Kafka, RocksDB, aggregation, FoundationDB,
and the query. From `node0`, with three SSH-reachable workers, run the functional
dev-mode version:

```bash
KAFKA_HOST=<coordinator-private-address> make eval-fig4-vehicle-dev
```

Expected output: `results/e2e_dev_zk/vehicle_dev_zk.jsonl`. This validates the
complete distributed path but does not produce a cryptographic proof. For the
real-ZK run on the same topology:

```bash
KAFKA_HOST=<coordinator-private-address> make eval-fig4-vehicle-zk
```

Expected output: `results/e2e_real_zk/vehicle_real_zk.jsonl`. Only this second
command may be used for proof-generation and proof-verification performance.

#### Table 2 — vanilla baseline on the end-to-end setups

Table 2 uses the same real datasets and distributed topologies as Figure 4:
Google Cluster with eight aggregators, CAIDA with eight aggregators, and Vehicle
Emissions with four aggregators. It is not a single-machine synthetic sweep.
All three runs include log commitment, Kafka ingestion, RocksDB raw storage,
parallel native aggregation, FoundationDB storage, and the native query.

After installing the Google and CAIDA inputs and configuring passwordless SSH
from `node0` to `node1` through `node7`, run the Vanilla side of Table 2 with:

```bash
KAFKA_HOST=<coordinator-private-address> make eval-table2-native
```

Expected output: `results/table2_native.csv`, plus one raw JSONL file per
dataset under `results/table2_native/`. Every row is measured using the actual
number of aggregator machines; no multi-node values are inferred. The CSV is
organized around the Vanilla columns in the paper table:

```text
dataset,num_aggregators,epochs_on_critical_node,rocksdb_insert_ms_per_epoch,rocksdb_read_ms_per_epoch,fdb_write_ms_per_epoch,fdb_read_ms_per_query,aggregation_compute_ms_per_epoch,query_ms_per_query,agg_peak_rss_mb_per_node,query_peak_rss_mb
```

`aggregation_compute_ms_per_epoch` corresponds to Table 2's “Total Aggregation
time (in parallel).” Kafka transmission is not added to this value; RocksDB and
FoundationDB costs are reported in their own columns. The ZK columns in Table 2
come from the matching real-ZK Figure 4 runs, not from this Vanilla command.

If only the bundled vehicle-emissions data is available, directly measure just
that Table 2 column with four machines:

```bash
KAFKA_HOST=<coordinator-private-address> TABLE2_SPECS=vehicle:4 \
  make eval-table2-native
```

Native processes are sampled every 10 ms, with GNU `time` maximum RSS as a
fallback. Press Ctrl-C once to terminate the current cell. If an abnormal
termination leaves benchmark processes behind, run `make eval-kill` locally or
`./scripts/util/kill_bench_processes.sh --all-machines` for the cluster.

> **Kafka/FDB endpoints.** Table 2, Figure 4, Figure 5, and Table 3 use the
> storage-backed `run_distributed_baseline.sh` pipeline, which connects to Kafka
> and FoundationDB. The shipped
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
`scripts/distributed/run_distributed_baseline.sh`. See
`docs/DISTRIBUTED_SETUP.md` and `docs/DISTRIBUTED_E2E_GUIDE.md`. A single
machine can run the standalone Figure 6/7 benchmarks, but cannot validate the
distributed Figure 4, Table 2, Figure 5, or Table 3 behavior.

## Cost-limited claims (read before reproducing)

Some paper points are too expensive to re-run in full and are validated by
proxy; this is stated so reviewers know what to expect:

- **Fig 4 ZK end-to-end** and **Fig 7 ZK at ≥ 16 queried epochs** can take days
  to prove. They are reproduced at reduced scale (dev mode for the
  pipeline; `eval-zkvm-aggr-56` and 1/2/4-epoch query proofs for the proving
  cost), and the larger points are compared against the paper's reported values.
- **Verification cost** is directly measured for every real proof generated by
  the selected real-ZK commands.

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
