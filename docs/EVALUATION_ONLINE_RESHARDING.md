# Evaluation: Online Resharding (OwnershipEpoch / Handoff)

Scope: the online-resharding scaffolding merged in PR #6 (`18bf2e3`,
`8517510`) — `OwnershipEpoch` / `Handoff` records, `current_owner_for_source`,
the `reshard-controller` tool, and the aggregator `--use-online-ownership`
opt-in. Single 56-core / 251 GB Linux host, RocksDB backend, `cargo --release`.

## What was evaluated

1. **Functional**: does ownership filtering actually move sources between
   aggregators at an epoch boundary, on scale-up (1→2) and scale-down (2→1)?
2. **Correctness**: is the per-source SHA-256 hash chain (and therefore the
   proofs that attest it) *continuous* across a handoff?
3. **Performance**: control-plane install latency and the per-source owner
   lookup the aggregator runs every epoch.

New tooling added for this evaluation:
- `chain-inspector` (`src/bin/chain_inspector.rs`) — reads aggregator DBs and
  renders a chain-continuity verdict over all `Handoff` rows.
- `reshard-bench` (`src/bin/reshard_bench.rs`) — in-process install + lookup
  latency micro-benchmark.
- `handoff-sync` (`src/bin/handoff_sync.rs`) — replicates published `Handoff`
  rows between per-aggregator stores (single-machine stand-in for the shared
  coordination store); used by the continuity fix in §2b.
- `scripts/bench_resharding_perf.sh` — corrected end-to-end scale-up/down
  harness (replaces the now-removed existence-proof `bench_resharding.sh`).
- `scripts/bench_resharding_handoff.sh` — focused continuity demo asserting
  `chain-inspector --strict` PASS after the §2b fix.
- `scripts/bench_resharding_xy.sh` — general X→Y reshard sweep (§2c): coverage
  + continuity for arbitrary aggregator counts, scale up and down.
- `scripts/bench_resharding_real.sh` — general X→Y on the REAL raw_db path
  (§2d): aggregators read real `epoch_batches`; the core cryptographically
  verifies each moved source's chain (a wrong inherited tip panics).
- `--gen-raw-epochs` / `--max-process-seq` / `--keep-raw-batches` — aggregator
  flags supporting the shared-raw_db real-path harness.
- `chain-inspector --check-coverage` — verifies the post-reshard map partitions
  N sources across exactly Y aggregators.
- `tests/handoff_inheritance.rs` — store-level inheritance unit tests.

## 0. The shipped existence-proof bench did not exercise resharding (removed)

> The original `scripts/bench_resharding.sh` has been **removed**; this section
> records why (it was the starting point for everything below).

`scripts/bench_resharding.sh` passed `--fake-epochs --fake-raw-shards
--fake-series-hist`. With `--fake-series-hist`, `main.rs` takes an early
`return Ok(())` (line ~1228) after writing a single fixed shard, so the
aggregator **never reaches the poll loop where the ownership filter lives**.
The script's only *hard* assertion is that the `"[ownership] enabled"` log line
is present — and that line is printed *before* the early return. The per-source
skip / handoff checks are soft `warn`s. Net effect: the bench prints
`PASS: online resharding scaffolding works end-to-end` while exercising none of
the resharding data path.

The correct invocation is `--fake-epochs` **alone** (`--fake-raw-shards` is
rejected by the poll loop at `main.rs:1303`), and the generator emits
**0-based** `source_id`s (`generate_epoch_batches`, line 232), so the ownership
map must use `0..N-1`, not `1..N`. The shipped bench's map (`1:0,2:1,3:0,4:1`)
also misses source 0 and never matches a generated source. `bench_resharding_perf.sh`
fixes both.

## 1. Functional — PASS (load splitting works)

Driving the real path (4 sources `0..3`, map `0→0,1→1,2→0,3→1` installed at the
epoch-3 boundary):

```
[native-aggr][ownership] seq=3 source_id=1 skipped (owner=1)   # agg0 drops odd sources
[native-aggr][ownership] seq=3 source_id=3 skipped (owner=1)
[native-aggr][ownership] seq=3 source_id=0 skipped (owner=0)   # agg1 drops even sources
[native-aggr][ownership] seq=3 source_id=2 skipped (owner=0)
```

- **Scale-up (1→2)**: at the boundary agg0 keeps `{0,2}`, agg1 keeps `{1,3}`. ✔
- **Scale-down (2→1)**: install `0→0,1→0,2→0,3→0`; agg0 absorbs all sources,
  agg1 skips all and idles. ✔
- The `reshard-controller`'s future-epoch validation and diff logging work as
  documented (refuses retroactive installs; prints the per-source owner diff).

Convergence is **immediate** — exactly one epoch boundary — because there is no
state-transfer step. That speed is the direct consequence of finding #2.

## 2. Correctness — chain continuity across a handoff

> **Status: was BROKEN in the merged preview; FIXED in this branch.** The
> original finding (2a) is kept for context; the fix and its verification are
> in 2b.

### 2a. Original finding (merged preview) — BROKEN

`chain-inspector` over both aggregator DBs after scale-up **and** scale-down:

```
RESULT_SUMMARY dbs=2 handoffs=4 continuous=0 bootstrap_zero_tip=4 unknown_prev_owner=4 all_continuous=false
VERDICT: BROKEN CONTINUITY — 4/4 handoff(s) are bootstrap-from-zero ...
  src=0 at_epoch=3 UNKNOWN(u32::MAX)->0 last_seq=-1 tip=ZERO [BOOTSTRAP]
```

Every `Handoff` row written at runtime is a **bootstrap**: `chain_tip = [0;32]`,
`from_aggregator = u32::MAX`, `last_seq = -1`. Reading `main.rs`:

- The **incoming** path (a source the aggregator now owns but didn't last epoch)
  *always* writes a zero-tip / sentinel-owner handoff and seeds the source's
  chain from zero — it never looks up the prior owner's published tip
  (`main.rs:1519-1538`).
- The **outgoing** path that *would* carry the real `chain_tip` is in practice
  never taken at the boundary: each aggregator starts the post-boundary run
  with an **empty** `prev_owned_sources`, so it has no memory that it previously
  owned the moved source.
- Crucially, **no code anywhere reads `Handoff.chain_tip` back**. `handoffs()` /
  `handoffs_for_epoch()` exist in the store but have zero non-test callers
  (`grep` confirms). The field is write-only.

**Consequence.** The README's security claim —

> "the receiving aggregator inherits the `chain_tip` from the `Handoff` row …
> a buggy or malicious controller cannot silently re-seat a source onto a fork"

— is **aspirational, not implemented**. After a handoff the moved source's
per-source SHA-256 chain restarts from zero on the new owner, so cross-handoff
chain continuity is broken, and any verifier that checks per-source chain
linkage across the boundary (i.e. the property the proofs are meant to attest)
would reject the join. The current preview trades the continuity guarantee for
zero-cost, instantaneous migration.

### 2b. Fix implemented in this branch — PASS

The handoff key is `(at_epoch, source_id)`, so there is exactly **one
authoritative `Handoff` row per source per boundary**. The fix makes the
*losing* owner the sole author of that row and the *gaining* owner a reader:

1. **Incoming inheritance** (`main.rs`): the zero-bootstrap write is removed.
   When an aggregator newly owns a source it calls the new point-get
   `RocksDb::handoff_at(at_epoch, source_id)`; if a real-tip handoff exists it
   seeds `prev_source_chain_tips` / `prev_source_batch_seqs` from
   `(chain_tip, last_seq)` and logs `incoming: inherited chain_tip from
   handoff`. If none exists it is a genuine cold start (no fork created).
2. **Outgoing** keeps publishing `Handoff{from = self, chain_tip = real,
   last_seq}` for a released source (fires when the losing owner runs across the
   boundary).
3. **`handoff-sync`** (new tool) replicates published handoffs from the losing
   owner's store into the gaining owner's store — the single-machine stand-in
   for the shared coordination store (mirrors how `reshard-controller` installs
   the ownership map on both DBs).

Store-level proof — `tests/handoff_inheritance.rs` (3 tests, all pass):
`handoff_at` round-trips the published tip; a missing row is a cold start; a
zero-tip/sentinel row is rejected by the inheritance predicate.

End-to-end — `scripts/bench_resharding_handoff.sh`:

```
[native-aggr][ownership] seq=3 source_id=1 outgoing (to next_owner=1)        # agg0 publishes real tip
[handoff_sync] ... copied=2 skipped_zero_tip=0                               # replicated to agg1
[native-aggr][ownership] seq=3 source_id=1 incoming: inherited chain_tip from handoff (from_aggregator=0, last_seq=23)
RESULT_SUMMARY dbs=2 handoffs=4 continuous=4 bootstrap_zero_tip=0 unknown_prev_owner=0 all_continuous=true
VERDICT: PASS — all 4 handoff(s) carry a real chain_tip; per-source chain stitches across the boundary.
```

`chain-inspector --strict` now exits 0. The README's "receiving aggregator
inherits the `chain_tip`" guarantee is now actually implemented.

### 2c. General X → Y resharding (arbitrary aggregator counts, up & down)

The §2b demo moved single sources between two aggregators. The real definition
of resharding is **X aggregators → Y aggregators** for arbitrary X, Y (scale up
*or* down), where an aggregator may simultaneously keep some sources, shed
others, and gain others. Two extensions make this work and are verifiable:

- **Durable per-source tips** (`AggSourceTip`, keyed by `source_id`): each
  aggregator persists the chain tip of every source it owns *into the epoch's
  atomic `WriteBatch`* (alongside the tombstone). The tip records its `owner`,
  so a reader distinguishes a **kept** source (`owner == self`, reloaded across
  a restart) from a **moved** one (`owner != self`, inherited + audited). This
  also fixes the §2b "restart doesn't reload kept tips" limitation.
- **`handoff-sync --from … --from … --to …`** now replicates both handoffs and
  per-source tips, and takes multiple sources — so an X→Y reshard merges every
  old owner's tips into each new owner's coordination view.
- The incoming path resolves a source's tip from the durable record (fallback:
  explicit `Handoff`); on a real move it seeds the chain (real `raw_db` path)
  and writes an auditable `Handoff{from: prev_owner, to: self}`.

A new coverage check — `chain-inspector --check-coverage --sources N
--aggregators Y --at-epoch B` — asserts the post-reshard map partitions all N
sources across exactly Y aggregators (each owned once, none dropped).

`scripts/bench_resharding_xy.sh` runs the full protocol (install both maps →
old owners build+persist tips → replicate → new owners inherit → verify) over a
sweep. Result (N=12 sources, boundary epoch 3) — **every config PASS**:

| reshard | movers | handoffs | coverage (load) | verdict |
|--------:|-------:|---------:|-----------------|:-------:|
| 1→2 | 6  | 6  | 2 aggs, 6/6 | ✅ |
| 1→3 | 8  | 8  | 3 aggs, 4/4 | ✅ |
| 2→1 | 6  | 6  | 1 agg, 12   | ✅ |
| 3→1 | 8  | 8  | 1 agg, 12   | ✅ |
| 2→4 | 6  | 6  | 4 aggs, 3/3 | ✅ |
| 4→2 | 6  | 6  | 2 aggs, 6/6 | ✅ |
| 3→5 | 9  | 9  | 5 aggs, 2–3 | ✅ |
| 5→3 | 9  | 9  | 3 aggs, 4/4 | ✅ |
| 2→3 | 8  | 8  | 3 aggs, 4/4 | ✅ |
| 3→2 | 8  | 8  | 2 aggs, 6/6 | ✅ |
| 1→6 | 10 | 10 | 6 aggs, 2/2 | ✅ |
| 6→1 | 10 | 10 | 1 agg, 12   | ✅ |

For each config: coverage is exact (every source owned by exactly one of the Y
aggregators) and the number of continuous handoffs equals the number of sources
whose owner actually changed (`s%X != s%Y`) — i.e. every mover's chain stitched,
no mover dropped, no spurious handoff. `bench_resharding_xy.sh` exits 0.

### 2d. Real raw_db path — cryptographic chain verification (not just metadata)

§2c runs in `--fake-epochs` mode, which inherits at the coordination layer only
(the synthetic generator owns its per-source batch numbering). To exercise the
**real path** — where aggregators read actual `epoch_batches` from a raw RocksDB
and the core *verifies* each batch's SHA-256 chain against the per-source tip —
three small pieces were added:

- **`--gen-raw-epochs`**: writes real, continuously-chained `epoch_batches` for
  all N sources across the seq range into one **shared** raw RocksDB (the
  durable source of truth the Kafka consumer would otherwise populate).
- **`--max-process-seq`**: caps the poll loop's range so pre-boundary
  aggregators stop exactly at the boundary (the new owners then process the
  boundary epoch with inherited tips).
- **`--keep-raw-batches`** + tombstone-on-skip: lets several aggregators share
  one raw_db (each reads only the sources it owns; an aggregator that owns
  nothing at an epoch tombstones it instead of leaving it to be re-scanned —
  without this the kept raw_db re-surfaces the epoch every poll pass, replaying
  the ownership filter with stale state).

`scripts/bench_resharding_real.sh`: gen shared raw → old owners process
`[0,B-1]` and persist tips → replicate tips → new owners process `[B,B+1]`,
each reading the **real** boundary batch and verifying it chains from the
inherited tip. Because the core panics on a chain/sequence mismatch, a clean run
is cryptographic proof of continuity — not bookkeeping. Result (N=12, B=3):

| reshard | movers | handoffs | proc. epoch B | panics | verdict |
|--------:|-------:|---------:|:-------------:|:------:|:-------:|
| 1→2 | 6 | 6 | 2/2 | 0 | ✅ |
| 2→1 | 6 | 6 | 1/1 | 0 | ✅ |
| 2→4 | 6 | 6 | 4/4 | 0 | ✅ |
| 4→2 | 6 | 6 | 2/2 | 0 | ✅ |
| 3→5 | 9 | 9 | 5/5 | 0 | ✅ |
| 5→3 | 9 | 9 | 3/3 | 0 | ✅ |
| 2→3 | 8 | 8 | 3/3 | 0 | ✅ |
| 3→2 | 8 | 8 | 2/2 | 0 | ✅ |

Every new owner processed the boundary epoch from the shared raw_db, **zero
chain/sequence panics**, and #handoffs == #movers. `bench_resharding_real.sh`
exits 0 — the real-path counterpart of the §2c metadata result: the moved
sources' chains are verified to continue across the reshard.

### 2e. Real zkVM proof across a handoff

§2a–2d run `--no-zkvm-proof` (they verify the SHA-256 chain in the core, which
is what the proof attests). As a final check, `scripts/prove_handoff_demo.sh`
generates and **verifies actual RISC0 receipts** across a 1→2 handoff (one tiny
epoch each side; real proving is slow here — no AVX-512 — ~4 min/epoch):

```
[phaseA] agg0 proving epoch 0 ...   [samples] seq=0 prove_ms=236202 verify_ms=35 proof_bytes=230664
[sync]   source_tips_copied=2
[phaseB] agg1 proving boundary epoch 1 for moved source ...
  seq=1 source_id=1 incoming: inherited chain_tip (from_aggregator=0, last_seq=0)
  [samples] seq=1 prove_ms=225903 verify_ms=35 proof_bytes=229544        # receipt VERIFIED
VERDICT: PASS — per-source chain stitches across the boundary.
```

The new owner (agg1) inherited the moved source's tip and produced a **verified**
receipt for the boundary epoch, with no chain/sequence panic. Because the guest
checks the per-source chain in-circuit (the host asserts `out == expected` and
calls `receipt.verify`), a verified receipt means the zkVM circuit accepted the
inherited tip as the chain predecessor — i.e. continuity holds at the **proof**
level, not just the hash-chain or metadata level.

## 3. Performance — `reshard-bench`

In-process timing on an already-open DB (isolates the data structures from
process-startup / RocksDB-open noise), 20k iterations.

### Control-plane install + owner-lookup vs. source count

| Sources `S` | install (`put_ownership_epoch`) ms | owner lookup µs/op |
|------------:|-----------------------------------:|-------------------:|
| 2     | 0.008 | 3.60 |
| 8     | 0.009 | 3.65 |
| 32    | 0.010 | 3.74 |
| 128   | 0.014 | 4.10 |
| 512   | 0.039 | 5.58 |
| 2,048 | 0.119 | 12.34 |

Installing a reshard map is **sub-millisecond even at 2,048 sources** — the
control plane is effectively free. (The 380 ms "install" printed by the
end-to-end shell harness is process-startup + 2× RocksDB-open overhead, *not*
the install itself; the in-process number above is the real cost.) Owner lookup
grows mildly with `S` because each call deserializes the whole assignment vector.

### Owner-lookup vs. reshard-history depth `H` (32 sources)

**Finding (was a scalability concern; FIXED).** `current_owner_for_source` →
`ownership_epoch_at` originally **forward-scanned every `OwnershipEpoch` row ≤
the query epoch**, so lookup latency was **linear in the number of reshards ever
performed** — and the aggregator runs one lookup *per source per epoch* (at 4,096
reshards a single lookup cost ~1.3 ms, ~42 ms for one 32-source epoch).

The fix: a single **reverse seek** (`seek_for_prev`). Keys are
`PREFIX || epoch_seq` (BE, `epoch_seq >= 0`), so they sort ascending and the
active row is the last one ≤ target — found in O(log n) instead of an O(H) scan.

| Installed OwnershipEpoch rows `H` | before (µs/op) | **after (µs/op)** |
|----------------------------------:|---------------:|------------------:|
| 1      | 2.5     | **2.7** |
| 8      | 5.2     | **3.4** |
| 64     | 21.4    | **3.4** |
| 512    | 165.5   | **4.0** |
| 4,096  | 1,302.2 | **2.6** |
| 16,384 | (≈5,000)| **2.3** |

After the fix latency is **flat** in `H` (~2–4 µs regardless of reshard history;
~490× faster at H=4,096). Verified by `ownership_epoch_at_reverse_seek` in
`common/src/rocksdb_store.rs` (empty store / before-first / exact / between /
past-last). (The FDB mirror `fdb_store::ownership_epoch_at` is off this hot path
— the aggregator's filter always resolves ownership against the local RocksDB —
and could get the same treatment via a reverse range read if it ever becomes
hot.)

## Summary

| Property | Verdict |
|----------|---------|
| Ownership filtering / load splitting (up & down) | ✅ works |
| `reshard-controller` install + validation | ✅ works, sub-ms |
| Per-source hash-chain continuity across handoff | ✅ fixed in this branch (durable `AggSourceTip` + inheritance; inspector `--strict` PASS). Was ❌ in the merged preview. |
| **General X→Y reshard (arbitrary counts, up & down)** | ✅ 12/12 configs PASS — exact coverage + every mover's chain continuous (`bench_resharding_xy.sh`) |
| **General X→Y on the real raw_db path (chain cryptographically verified)** | ✅ 8/8 configs PASS, 0 chain/seq panics (`bench_resharding_real.sh`, §2d) |
| **Real zkVM proof verifies across a handoff** | ✅ boundary-epoch RISC0 receipt verified on the new owner with the inherited tip (`prove_handoff_demo.sh`, §2e) |
| Restart reloads kept-source tips | ✅ via durable `AggSourceTip` (was a §2b limitation) |
| Owner-lookup scaling vs reshard history | ✅ O(log n) reverse seek — flat ~2–4 µs in `H` (was O(H): ~1.3 ms at H=4096) |
| Controller-driven auto-rebalancer | ⛔ not present (README: future work) |

### Reproduce

```
cargo build --release -p zktelemetry-risc0-aggr-host \
  --bin zktelemetry-risc0-aggr-host --bin reshard-controller \
  --bin chain-inspector --bin reshard-bench --bin handoff-sync
cargo test --release -p zktelemetry-risc0-aggr-host --test handoff_inheritance
./scripts/bench_resharding_handoff.sh        # continuity fix demo -> VERDICT: PASS (exit 0)
./scripts/bench_resharding_xy.sh             # general X->Y sweep (up & down) -> ALL CONFIGS PASS
./scripts/bench_resharding_real.sh           # general X->Y on REAL raw_db path (chain verified) -> ALL PASS
./scripts/prove_handoff_demo.sh              # real zkVM receipt VERIFIED across a handoff (~10 min)
./scripts/bench_resharding_perf.sh           # e2e + microbench
target/release/chain-inspector --rocksdb-path DB1 --rocksdb-path DB2 [--strict]
target/release/chain-inspector --check-coverage --rocksdb-path DB --sources N --aggregators Y --at-epoch B
```
