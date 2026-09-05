# zk-Analytics

**Verifiable, privacy-preserving cloud analytics with zero-knowledge proofs.**

zk-Analytics combines lightweight log commitments with proofs of correct
aggregation and query execution. An external verifier can check analytics
results without access to raw logs or the cloud provider's infrastructure.
The system uses SHA-256 hash chains for online commitments and the
[RISC Zero zkVM](https://risczero.com) for offline proving.

This repository accompanies [*Zero-Knowledge Cloud Analytics* (SIGCOMM 2026)](https://dl.acm.org/doi/10.1145/3789240.3829157).

[Artifact evaluation](docs/ARTIFACT-EVALUATION.md) ·
[Benchmarks](docs/BENCHMARKS.md) ·
[Distributed setup](docs/DISTRIBUTED_SETUP.md) ·
[Citation](CITATION.cff)

## SIGCOMM 2026 Artifact Evaluation

**SIGCOMM 2026 artifact evaluation reviewers: please start with the
[Artifact Evaluation Guide](docs/ARTIFACT-EVALUATION.md).**
The guide provides hardware and software requirements, setup instructions,
functional checks, and step-by-step commands to reproduce the paper's figures
and tables, including expected outputs and troubleshooting.

## Overview

The pipeline separates online ingestion from offline aggregation and queries:

```text
Data sources → Kafka → Kafka consumers → Aggregators → Querier → Answer + proof
                            │                 │            ↑
                       RocksDB buffer         └─ Epoch store ─┘
                                              (RocksDB / FoundationDB)
```

1. **Commit logs.** Data sources emit `(timestamp, key, value)` events, update
   per-source SHA-256 hash chains, and stream committed batches through Kafka.
   Optional [Trillian checkpoints](docs/transparency_log.md) publish commitments
   to a transparency log.
2. **Aggregate epochs.** Kafka consumers buffer batches in RocksDB. Aggregators
   verify commitments and compute per-key samples, histograms, or Count-Min
   sketches inside zkVM guests. Epoch summaries, chain metadata, and proofs are
   persisted for subsequent queries.
3. **Query and verify.** The HTTP querier loads epochs for a time window and
   produces an answer with a proof of the query computation and commitment
   checks. The verifier checks the proof without reading the underlying logs.

The repository also includes query access control, query-guest leakage analysis,
crash recovery, online resharding, and native baselines for evaluation.

## Build

### Prerequisites

The setup scripts support **Ubuntu/Debian**. Host code uses Rust; zkVM guests
require the RISC Zero toolchain. RocksDB bindings require Clang/libclang.
Kafka builds additionally use CMake; the optional Trillian integration requires
`protoc`. The FoundationDB backend uses **FoundationDB 7.1**.

For full proof experiments, the artifact guide specifies an **AVX-512 x86-64
CPU**, **at least 64 GB RAM**, and **50+ GB free disk space**. Native and
functional checks have lower requirements. See the
[hardware and runtime requirements](docs/ARTIFACT-EVALUATION.md#prerequisites)
for sizing and expected runtimes.

### Install and build

```bash
git clone https://github.com/Froot-NetSys/zk-Analytics.git
cd zk-Analytics

# Keep compiler temporary files on the same filesystem as build outputs.
mkdir -p target/tmp

# Install dependencies and the RISC Zero toolchain, start Kafka/FoundationDB,
# and build the pipeline binaries and zkVM guests. Uses sudo.
./scripts/setup/setup_local_e2e.sh --all
```

If installation adds your account to the Docker group, log out and back in
before using Docker without `sudo`. If the newly installed tools are not on
`PATH`, load them into the current shell:

```bash
source "$HOME/.cargo/env"
export PATH="$HOME/.risc0/bin:$PATH"
```

For an already configured environment, rebuild with:

```bash
./scripts/setup/setup_local_e2e.sh --build
```

For a manual build, `cargo build --release` builds the default workspace
features. Kafka binaries and FoundationDB support must be enabled explicitly:

```bash
cargo build --release -p data_source --bin kafka-producer --features kafka
cargo build --release -p aggregator --features 'kafka fdb'
cargo build --release -p querier --features fdb
```

## Quick start

After setup, run the native analytics and local zkVM functional checks:

```bash
# Native aggregation/query baseline; writes CSVs under results/.
make eval-non-zk-baseline

# Execute aggregation/query guests locally without generating real proofs.
make eval-zkvm-dev-mode
```

**Dev mode does not produce cryptographic proofs.** It executes the guest and
returns a fake receipt. Use it to check guest execution; use the real-proof
experiments below to evaluate proving, verification, and proof size.

For a complete deployment, follow the
[artifact evaluation guide](docs/ARTIFACT-EVALUATION.md#step-3--distributed-experiments),
which covers cluster configuration, worker builds, datasets, and experiment
commands. The distributed pipeline runs the Kafka consumer and aggregator as
separate processes, with local RocksDB buffers and FoundationDB as the shared
epoch store.

## Query API

The querier serves `POST /query` and listens on `0.0.0.0:8082` by default
(override with `HTTP_LISTEN`). Run these examples against a deployed pipeline
after it has ingested and aggregated data of the corresponding epoch type.

```bash
# Sum sample values in the last hour.
curl -sS http://localhost:8082/query \
  -H 'Content-Type: application/json' \
  -d '{"type":"samples_sum","window":"1h"}'

# Read histogram bucket 42 over the last day.
curl -sS http://localhost:8082/query \
  -H 'Content-Type: application/json' \
  -d '{"type":"histogram_bucket","window":"1d","bucket":42}'

# Return the top 20 Count-Min sketch entries over the last five minutes.
curl -sS http://localhost:8082/query \
  -H 'Content-Type: application/json' \
  -d '{"type":"cm_topk","window":"5m","limit":20}'

# Sum samples whose numeric key matches 123 in its low 16 bits.
curl -sS http://localhost:8082/query \
  -H 'Content-Type: application/json' \
  -d '{"type":"samples_sum_key","window":"1h","key":123,"mask":65535}'
```

The [request and response types](querier/server/src/main_parts/common.rs)
define the HTTP schema, including additional query types and key filters.
Deployment scripts configure the storage paths and FoundationDB connection;
see the [distributed setup guide](docs/DISTRIBUTED_SETUP.md) for configuration.

## Reproduce the paper

**Reviewers should start with the [Artifact Evaluation Guide](docs/ARTIFACT-EVALUATION.md).**
It provides the experiment prerequisites, reduced-scale runs, expected outputs,
and mappings to paper figures and tables.

| Experiment | Make target |
|---|---|
| Native analytics baseline | `eval-non-zk-baseline` |
| Local zkVM functional checks (no real proofs) | `eval-zkvm-dev-mode` |
| Figure 4: vehicle-emissions pipeline with real proofs | `eval-fig4-vehicle-zk` |
| Table 2: native distributed pipelines | `eval-table2-native` |
| Figure 5 / Table 3: distributed aggregation with real proofs | `eval-fig5-table3-zk` |
| Figure 6: standalone aggregation with real proofs | `eval-fig6-aggregator-zk` |
| Figure 7: query proofs | `eval-fig7-query-zk` |
| Plot available experiment results | `eval-plots-all` |

Invoke a target with `make <target>` from the repository root. Real-proof runs
can take hours or days; distributed runs require the cluster setup in the guide.
The vehicle-emissions dataset is bundled; Google Cluster v3 and CAIDA traces
must be obtained separately. Synthetic experiments need no external datasets.

## Repository layout

| Path | Purpose |
|---|---|
| [`data_source/`](data_source/) | Event generation, SHA-256 commitments, Kafka producer, optional Trillian checkpoints |
| [`aggregator/`](aggregator/) | Kafka consumer, aggregation host, shared aggregation logic, and zkVM guests |
| [`querier/`](querier/) | HTTP service, query hosts, shared query logic, and zkVM guests |
| [`common/`](common/) | RocksDB/FoundationDB storage, epoch types, and differential privacy utilities |
| [`zkvm-common/`](zkvm-common/) | Shared `no_std` event and hash-chain types |
| [`query_checker/`](query_checker/) | Query allow/block-list access control |
| [`cf_detector/`](cf_detector/) | Control-flow and output leakage analysis for query guests |
| [`native_baseline/`](native_baseline/) | Native analytics for non-ZK performance comparisons |
| [`scripts/`](scripts/README.md) | Setup, orchestration, benchmarks, and plotting |
| [`testdata/`](testdata/) | Bundled data and input locations for external datasets |

## Further documentation

- [Distributed end-to-end evaluation](docs/DISTRIBUTED_E2E_GUIDE.md)
- [Benchmark details](docs/BENCHMARKS.md) and [results collection](docs/RESULTS_COLLECTION_GUIDE.md)
- [Recovery and resharding internals](docs/INTERNALS.md)
- [Fault recovery evaluation](docs/EVALUATION_FAULT_RECOVERY.md)
- [Online resharding evaluation](docs/EVALUATION_ONLINE_RESHARDING.md)
- [Transparency log integration](docs/transparency_log.md)
- [Query access control](query_checker/README.md) and [leakage detector](cf_detector/README.md)

## Citation and license

If you use zk-Analytics in your research, please cite *Zero-Knowledge Cloud
Analytics* (SIGCOMM 2026). Author and publication metadata are available in
[`CITATION.cff`](CITATION.cff).

zk-Analytics is released under the [MIT License](LICENSE).
