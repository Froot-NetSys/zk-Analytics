# Artifact Evaluation Guide

This guide walks a reviewer from a clean clone to reproducing the experiments in
*"Zero-Knowledge Cloud Analytics."* It is organized so you can validate the
artifact **functionally in minutes** and then reproduce individual paper results
as time and hardware allow.

The repository ships **no precomputed results**: every experiment regenerates its
own `results/` and `plots/` locally, and you compare those against the numbers
and figures reported in the paper (Figures 4–7, Tables 1–3). Nothing here needs
to be diffed against bundled reference data.

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

## Requirements

| | Functional check / native baselines | Full ZK reproduction |
|---|---|---|
| CPU | any x86-64 | **AVX-512**, many cores (paper: Xeon Gold 5512U, 56 cores) |
| RAM | 8 GB | ≥ 64 GB (prover peaks ~9–10 GB/node; parallel proving needs headroom) |
| Disk | 10 GB | 50+ GB (RocksDB/FoundationDB + datasets) |
| Time | minutes | **hours per experiment** (see table below) |

Software (installed by `scripts/setup_local_e2e.sh --all`): `clang`/`libclang`
(RocksDB), the RISC Zero toolchain via `rzup` (guest compiler + `r0vm`), and —
for the distributed/e2e paths — Docker (Kafka), FoundationDB 7.1, `protoc`
(Trillian). See the README "Build" section for the manual dependency list.

## Step 0 — Setup and build

```bash
git clone https://github.com/Froot-NetSys/zk-Analytics
cd zk-Analytics

# One-shot environment (deps + Kafka/FDB via Docker + RISC Zero toolchain):
./scripts/setup_local_e2e.sh --all
# or just the build deps:
sudo apt-get install -y clang libclang-dev
curl -L https://risczero.com/install | bash && rzup install

mkdir -p target/tmp            # required by .cargo/config.toml (EXDEV workaround)
cargo build --release          # host crates + RISC Zero guest ELFs
```

A successful `cargo build --release` is the **Functional** badge: it compiles
all host crates *and* the zkVM guest ELFs.

## Step 1 — Kick the tires (minutes)

These two runs validate the full pipeline without hours of proving.

```bash
# (a) Native (non-ZK) baseline — runs the exact aggregation/query analytics
#     natively, regenerates results/ + plots/. Seconds.
make eval-non-zk-baseline

# (b) zkVM pipeline in DEV MODE (RISC0_DEV_MODE=1): guests are executed and the
#     witness is generated, but no STARK proof. Exercises the real proving path
#     end-to-end in minutes. Writes results/zkvm_dev_*.csv.
make eval-zkvm-dev-mode
```

If both complete and write CSVs under `results/`, the artifact is functional and
the proving path works; the remaining steps differ only in producing *real*
(slow) STARK proofs and scaling up input sizes.

## Datasets

| Dataset | Status | How to obtain |
|---|---|---|
| Vehicle CO2 emissions (Canada) | **bundled** | `testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv` |
| Google Cluster v3 | not bundled | Download the public Google cluster-usage traces (v3) and place the per-machine CSVs under `testdata/google_cluster_data/input/`. |
| CAIDA backbone traces | not bundled | Requires CAIDA's academic **data-sharing agreement** (cannot be redistributed). Obtain a PCAP, then `PCAP=/path/to.pcap.gz ./scripts/prep_caida.sh` → `testdata/caida_pcap/caida_txt/`. |

The synthetic-workload experiments (Figs 5–7, §7.2) need **no external data** —
they are the most self-contained to reproduce.

## Step 2 — Reproduce the experiments

Times are order-of-magnitude on a 56-core AVX-512 node; smaller machines are
proportionally slower. Compare regenerated outputs against the cited paper item.

| Paper item | Command | Approx time | Needs | Compare against |
|---|---|---|---|---|
| §7.2 online commitment throughput | `BENCH_INPUT=<trace> PARALLEL_CHAINS=1 cargo run -p data_source --release` (sweep batch size in the trace); reports `serial_ns_per_event` / `hash_fn=sha256` | minutes | 1 core | §7.2 (1.6–6.7 M commits/s) |
| Fig 6 — single-machine aggregation, native | `FIG=6 ./scripts/run_figures_native.sh` | ~30 min | 56 cores | Fig 6 (native columns) |
| Fig 6 — single-machine aggregation, ZK | `FIG=6 SYNTH_KEYS=1024 ./scripts/run_figures_zk.sh` | hours | AVX-512, 56 cores | Fig 6 (proof gen/verify/size/mem) |
| Fig 7 — query, native | `./scripts/run_fig7_native.sh` | ~30 min | local Kafka+FDB | Fig 7 (native query times) |
| Fig 7 — query, ZK (1/2/4 epochs) | `make eval-zkvm-query-proofs` | ~1–2 h | AVX-512 | Fig 7 (prove/verify/size at small epoch counts) |
| Fig 5 + Table 3 — distributed aggregation (1/2/4/8) | `FIG=5 ./scripts/run_figures_zk.sh` (and `run_figures_native.sh`) | many hours | **8-node SSH cluster** (see below) | Fig 5, Table 3 |
| Fig 4 + Tables 1–2 — end-to-end, native | `./scripts/prep_caida.sh` then `make eval-non-zk-e2e` | ~1–4 h | Google+CAIDA data | Fig 4, Table 2 (non-ZK columns) |
| Aggregation re-anchor at 56 threads | `make eval-zkvm-aggr-56` | ~3.5 h (CM) | AVX-512, 56 cores | Table 2 / Fig 4 (ZK aggregation) |

Merge the measured CSVs into the comparison tables/plots with:

```bash
make eval-non-zk-all          # regenerates results/*.csv, plots/*.pdf, summary.md
```

### Distributed experiments (Fig 5, Table 3, Fig 4 distributed)

These need multiple machines reachable over SSH. Copy
`scripts/distributed_e2e_config.example.sh`, set `SSH_USER`, the node IPs
(`scripts/ip_defaults.sh`), and `KAFKA_BROKERS`/`FDB_*`, then drive the runs with
`scripts/run_distributed_baseline.sh` / `run_table2_sweep.sh`. See
`scripts/DISTRIBUTED_SETUP.md` and `scripts/DISTRIBUTED_E2E_GUIDE.md`. On a
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
- Reset local state between runs: `./scripts/reset_rocksdb.sh`,
  `./scripts/reset_fdb.sh`.
- `make eval-non-zk-baseline` on a fresh clone produces **native-only** CSVs
  under `results/` (the repo ships no measured-ZK data); the ZK-comparison
  columns/summary populate after you run `make eval-zkvm-aggr-56` (real proofs)
  or `make eval-zkvm-dev-mode` (fast). This is expected, not an error.
- PDF plots need matplotlib (`pip install matplotlib`); without it the run
  still completes and just skips the plots with a warning.
