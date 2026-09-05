# zk-Analytics

**Verifiable, privacy-preserving cloud analytics with zero-knowledge proofs.**

zk-Analytics combines lightweight log commitments with proofs of correct
aggregation and query execution. An external verifier can check analytics
results without access to raw logs or the cloud provider's infrastructure.
The system uses SHA-256 hash chains for online commitments and the
[RISC Zero zkVM](https://risczero.com) for offline proving.

This repository accompanies [*Zero-Knowledge Cloud Analytics* (SIGCOMM 2026)](https://dl.acm.org/doi/10.1145/3789240.3829157).

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
mkdir -p target/tmp

# Install system dependencies and the RISC Zero toolchain (uses sudo).
./scripts/setup/setup_local_e2e.sh --deps
./scripts/setup/setup_local_e2e.sh --risc0
source "$HOME/.cargo/env"
export PATH="$HOME/.risc0/bin:$PATH"

# Build the Aggregator, Querier, and their zkVM guests for the local example.
cargo build --release -p aggregator --bin aggregator -p querier --bin querier
```

The local example below uses RocksDB. For a Kafka/FoundationDB deployment,
follow the setup instructions in the
[Artifact Evaluation Guide](docs/ARTIFACT-EVALUATION.md).

## E2E example

This single-machine example generates 16 synthetic sample events with valid
per-source hash chains, aggregates one epoch into RocksDB, and queries its sum
through the HTTP query engine. It uses the Aggregator's input generator to
prepare the batches that a Kafka consumer would normally store.

Run the commands from the repository root. The example uses **dev mode** for a
quick functional run: the zkVM guests execute, but the returned receipts are
fake and provide no cryptographic proof. To generate real proofs, set
`RISC0_DEV_MODE=0` in both the Aggregator and Querier commands and repeat the
example with a fresh data directory; proving takes longer.

### 1. Prepare sample data

In terminal 1, create a fresh directory and generate one epoch of committed
sample batches:

```bash
DEMO_DIR=$(mktemp -d "$PWD/target/tmp/zk-analytics-demo.XXXXXX")

target/release/aggregator --gen-raw-epochs --mode samples \
  --raw-rocksdb-path "$DEMO_DIR/raw" \
  --start-seq 0 --end-seq 0 \
  --series 4 --samples-per-series 4 --commit-batch-size 4 --seed 1
```

### 2. Run the Aggregator

In the same terminal, process the input batches and persist the sample summary:

```bash
env -u FDB_CLUSTER_FILE RISC0_DEV_MODE=1 AGGR_IDLE_TIMEOUT_SECS=1 \
  target/release/aggregator --rocksdb --mode samples \
  --raw-rocksdb-path "$DEMO_DIR/raw" \
  --agg-rocksdb-path "$DEMO_DIR/agg"
```

Wait for `DONE: epochs_proved=1`. The Aggregator exits after processing the
epoch and one second of inactivity, releasing the database for the Querier.

### 3. Start the query engine (Querier)

Still in terminal 1, start the HTTP service against the aggregated data.
`DP_ENABLED=0` disables differential privacy noise for this example so the
query returns the exact sum.

```bash
env -u FDB_CLUSTER_FILE RISC0_DEV_MODE=1 DP_ENABLED=0 \
  AGG_ROCKSDB_PATH="$DEMO_DIR/agg" HTTP_LISTEN=127.0.0.1:8082 \
  target/release/querier
```

Leave it running. Once it prints `listening on http://127.0.0.1:8082/query`,
it is ready to accept queries.

### 4. Send a query and inspect the result

In terminal 2, from the repository root, request the sum over the last hour
and save the full JSON response, including the proof bundle:

```bash
curl --fail-with-body -sS http://127.0.0.1:8082/query \
  -H 'Content-Type: application/json' \
  -d '{"type":"samples_sum","window":"1h"}' \
  -o target/readme-query.json

# Display the answer fields; the full proof bundle remains in the saved JSON.
python3 - <<'PYTHON'
import json
from pathlib import Path

response = json.loads(Path("target/readme-query.json").read_text())
print(json.dumps({k: v for k, v in response.items() if k != "proof"}, indent=2))
PYTHON
```

With the sample parameters above and default data-generation settings, the
answer is:

```json
{
  "type": "samples_sum",
  "sum": 1065257,
  "dp_offset_sum": 0,
  "suppressed": false
}
```

The saved response also contains a `proof` object with the receipt and digest.
In dev mode, this is a fake receipt. Run the query within an hour of aggregation
so the example epoch falls inside the requested window. Press `Ctrl+C` in
terminal 1 to stop the Querier when finished.

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

## Citation and license

If you use zk-Analytics in your research, please cite *Zero-Knowledge Cloud
Analytics* (SIGCOMM 2026). Author and publication metadata are available in
[`CITATION.cff`](CITATION.cff).

zk-Analytics is released under the [MIT License](LICENSE).
