# Data-source checkpoints → public transparency log (Trillian)

This implements the `\change` block of the **Log Commitment (data source)**
algorithm: the data source maintains a per-source hash chain
`h_i = CommitHash(h_{i-1}, buffer)` over committed log batches, and **every `P`
batches it publishes a checkpoint `(i, h_i)` to a public append-only registry**
(Google [Trillian](https://github.com/google/trillian)). An auditor can then
check the source's chain against an externally-witnessed log rather than
trusting the source.

## Where it lives

- `data_source/src/transparency.rs` — the `TransparencyLog` backend
  (`Trillian` gRPC client + `Noop` fallback) and the `Checkpoint` leaf encoding.
- `data_source/src/kafka_producer.rs` — `EventBatchProducer` publishes
  a checkpoint right after each batch's Kafka send, gated by `P`.
- `data_source/proto/trillian_log.proto` — minimal, wire-compatible
  subset of Trillian's `TrillianLog.QueueLeaf` API (field numbers/service match
  upstream, so it talks to an unmodified Trillian server).

## Configuration (environment)

| Var | Meaning | Default |
|-----|---------|---------|
| `CHECKPOINT_INTERVAL` | `P` — publish every P-th batch per source (`0` disables) | `0` |
| `TRILLIAN_ADDR` | Trillian log gRPC endpoint, e.g. `http://127.0.0.1:8090`. Presence selects the Trillian backend | unset → no-op |
| `TRILLIAN_LOG_ID` | Target Trillian log/tree id (`i64`) | `0` |

## Backends

- **No-op (default build / unconfigured):** checkpoints are logged to stderr
  (`[transparency][noop] checkpoint source_id=… index=… h_i=…`). The pipeline
  compiles and runs with no tonic/protoc dependency and no live Trillian.
- **Trillian:** build with the opt-in feature and point at a server:

  ```bash
  cargo build -p zktelemetry-risc0-data-source-host --features kafka,trillian
  CHECKPOINT_INTERVAL=128 TRILLIAN_ADDR=http://127.0.0.1:8090 TRILLIAN_LOG_ID=$LOG_ID \
    ./kafka-producer --events 131072 --commit-batch-size 8
  ```

  Each checkpoint is appended via `QueueLeaf`. The leaf value is the canonical
  encoding `"ZKTLM_CHECKPOINT_V1" || source_id(BE u32) || index(BE u64) ||
  chain_hash(32B)`, with `leaf_identity_hash = SHA256(leaf_value)` for
  server-side de-duplication.

Publication is **best-effort**: a Trillian connect or `QueueLeaf` failure is
logged and ingestion continues — the transparency log is an auxiliary audit
trail, not on the data path.

## Local stack + smoke test

A self-contained Trillian stack and an end-to-end smoke test are provided:

- `deploy/trillian/docker-compose.yml` — MySQL (with Trillian's schema), a log
  server (gRPC `:8090`), and a log signer.
- `deploy/trillian/storage.sql` — Trillian's MySQL schema (vendored, Apache-2.0).
- `scripts/util/trillian_smoke.sh` — brings up the stack, creates a log (tree) via
  `createtree`, then appends checkpoints through the real `QueueLeaf` path and
  asserts success.
- `data_source/src/bin/trillian_smoke.rs` — the `trillian-smoke`
  binary it runs (fails loudly if the client silently falls back to no-op).

```bash
scripts/util/trillian_smoke.sh          # up -> create tree -> smoke -> down
KEEP=1 scripts/util/trillian_smoke.sh   # leave the stack running
```

Verified round-trip: client `QueueLeaf` → log server → signer → MySQL, with the
stored `LeafValue` matching the canonical checkpoint encoding and the signer
sequencing the leaves into the Merkle tree.
