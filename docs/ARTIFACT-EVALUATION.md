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
CPU cores. The paper used Intel Xeon Gold 5512U machines with 56 cores.
Distributed Figure 4/5 and Table 3 experiments require up to eight machines
reachable over SSH, plus Kafka and FoundationDB. Native and dev-mode checks do
not require this cluster.

## Comments for the AEC

- Start with **Getting started instructions** below. This checks the native
  analytics and zkVM guest/witness paths without running expensive proofs.
- Real proof generation takes hours per experiment. Larger Figure 4 and Figure
  7 points can take days; the guide identifies reduced-scale proof runs.
- The vehicle-emissions dataset is bundled. Google Cluster v3 must be downloaded
  separately. CAIDA traces require an academic data-sharing agreement and cannot
  be redistributed by the authors.
- Only one evaluator should use a shared Kafka/FoundationDB deployment at a
  time. Reset commands are listed under Troubleshooting.
- Please report setup or execution problems through the artifact-submission
  discussion channel so the instructions can be corrected during kick-the-tires.

## Getting started instructions

On a clean Ubuntu/Debian x86-64 machine, complete the dependency installation in
Step 0, then run:

```bash
mkdir -p target/tmp
cargo build --release
make eval-non-zk-baseline
make eval-zkvm-dev-mode
```

Success means all commands exit with status 0 and the following outputs exist:

| Check | What it validates | Expected output |
|---|---|---|
| `cargo build --release` | Host crates and RISC Zero guest ELFs compile | release artifacts under `target/` |
| `make eval-non-zk-baseline` | Native aggregation and query analytics run | `results/non_zk_aggregation_baseline.csv`, `results/non_zk_query_baseline.csv`, and `results/zk_cost_breakdown.csv`; comparison summary/plots are added when measured ZK inputs exist |
| `make eval-zkvm-dev-mode` | The real zkVM guests execute and witnesses are generated; cryptographic proving is skipped | `results/zkvm_dev_*.csv` |

This is the intended functional evaluation. `RISC0_DEV_MODE=1` is deliberately
used only by the dev-mode target; it does **not** produce a proof and must not be
used to evaluate proof-generation or verification performance. For real proofs,
run the applicable Step 2 rows.

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
| CPU | any x86-64 | **AVX-512**, many cores (paper: Xeon Gold 5512U, 56 cores) |
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

curl -fsSL https://risczero.com/install | bash
source "$HOME/.bashrc"
rzup install
```

Confirm that both the host and RISC Zero tools are available:

```bash
clang --version
cargo --version
rzup show
r0vm --version
```

All four commands must succeed. If `rzup` is not found immediately after the
installer finishes, open a new shell or run `source "$HOME/.bashrc"` again.

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
proportionally slower. Commands in this table are independent unless their
"Needs" column says otherwise, so reviewers may select a subset. Compare each
regenerated output against the cited paper item.

| Paper item | Command | Approx time | Needs | Compare against |
|---|---|---|---|---|
| §7.2 online commitment throughput | `BENCH_INPUT=synthetic cargo run -p data_source --release -- --streaming --bench --events 1000000 --key-mod 4096` (sweep `--key-mod` / `--events`); reports `hash_fn=sha256`, `serial_ns_per_event`, `parallel_ns_per_event` | minutes | 1 core | §7.2 (1.6–6.7 M commits/s) |
| Fig 6 — single-machine aggregation, native | `FIG=6 ./scripts/eval/run_figures_native.sh` | ~30 min | 56 cores | Fig 6 (native columns) |
| Fig 6 — single-machine aggregation, ZK | `FIG=6 SYNTH_KEYS=1024 ./scripts/eval/run_figures_zk.sh` | hours | AVX-512, 56 cores | Fig 6 (proof gen/verify/size/mem) |
| Fig 7 — query, native | `./scripts/eval/run_fig7_native.sh` | ~30 min | local Kafka+FDB | Fig 7 (native query times) |
| Fig 7 — query, ZK (1/2/4 epochs) | `make eval-zkvm-query-proofs` | ~1–2 h | AVX-512 | Fig 7 (prove/verify/size at small epoch counts) |
| Fig 5 + Table 3 — distributed aggregation (1/2/4/8) | `FIG=5 ./scripts/eval/run_figures_zk.sh` (and `run_figures_native.sh`) | many hours | **8-node SSH cluster** (see below) | Fig 5, Table 3 |
| Fig 4 + Tables 1–2 — end-to-end, native | `./scripts/setup/prep_caida.sh` then `make eval-non-zk-e2e` | ~1–4 h | Google+CAIDA data | Fig 4, Table 2 (non-ZK columns) |
| Aggregation re-anchor at 56 threads | `make eval-zkvm-aggr-56` | ~3.5 h (CM) | AVX-512, 56 cores | Table 2 / Fig 4 (ZK aggregation) |

> **Kafka/FDB endpoints.** Every row that drives the real pipeline (the Fig 6/7
> native runs and all distributed cells go through
> `run_distributed_baseline.sh`) connects to Kafka and FoundationDB. The shipped
> scripts use RFC 5737 **placeholder IPs** (e.g. `192.0.2.1`), so point them at
> your setup first: after `setup_local_e2e.sh --all` use `KAFKA_HOST=localhost`
> (single machine), or set `KAFKA_HOST`/`KAFKA_BROKERS` + `FDB_CLUSTER_FILE` and
> the node IPs in `scripts/ip_defaults.sh` for a cluster.

Merge the measured CSVs into the comparison tables/plots with:

```bash
make eval-non-zk-all          # regenerates results/*.csv, plots/*.pdf, summary.md
```

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
