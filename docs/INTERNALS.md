# Internal design notes

Aggregator-internal mechanics: crash recovery and online resharding.

## Recovery semantics

Each completed epoch writes an `EpochTombstone` row (keyed by `(epoch_type,
aggregator_id, sequence)`) inside the same atomic RocksDB `WriteBatch` as the
rest of the epoch's rows (`agg_epoch`, the per-mode struct, `agg_epoch_meta`,
`agg_epoch_proof`). The tombstone's presence is the single source of truth for
"this epoch is fully done" — `has_agg_epoch` is no longer authoritative,
because a crash mid-WriteBatch can leave any subset of the per-epoch rows on
disk.

On startup, `recover_partial_state()` runs once before the polling loop. It
looks up the highest tombstoned sequence (`max_done`) for the current
`(epoch_type, aggregator_id)` pair and scans a small window of `[max_done-5,
max_done+5]`. For each sequence in the window with no tombstone but any of the
related rows present, it deletes those orphaned rows in a single `WriteBatch`
and logs `[native-aggr][recover] max_done=… orphans_cleaned=…`. The polling
loop then re-processes those sequences from scratch.

The cross-epoch chain hash is unaffected because each subsequent epoch's
`prev_chain_hash` is read from the previous epoch's `final_chain_hash`, which
is only persisted as part of the per-mode struct row — and that row only
survives if its accompanying tombstone was written.

## Online resharding

Moves a `source_id` from one aggregator to another at an epoch boundary while
preserving its per-source SHA-256 chain. The data plane is implemented and
verified end-to-end for arbitrary X→Y reshards (scale up or down), including a
real zkVM proof that verifies across a handoff — see
`EVALUATION_ONLINE_RESHARDING.md`.

- Three record types in `common/src/rocksdb_store.rs` (mirrored in
  `common/src/fdb_store.rs`):
  - `OwnershipEpoch { epoch_seq, assignments: Vec<(source_id, aggregator_id)>, installed_at_ms }`
    becomes active at `epoch_seq` and stays active until a higher-seq row
    overrides it.
  - `Handoff { source_id, at_epoch, from_aggregator, to_aggregator, chain_tip, last_seq, published_at_ms }`
    records a source's ownership transition (for audit).
  - `AggSourceTip { source_id, last_seq, chain_tip, owner, updated_at_epoch }`
    is the durable per-source chain tip — persisted atomically with each epoch
    and inherited by a source's new owner so its chain continues across the
    handoff (see the Security note below).
- A pure read function `current_owner_for_source(db, source_id, epoch_seq, default_aggregator)`
  walks the OwnershipEpoch rows and returns the assigned aggregator, or
  the default if none. Unit-tested via `ownership_lookup_basic`.
- A one-shot manual control-plane tool, `reshard-controller`
  (`aggregator/host/src/bin/reshard_controller.rs`), takes
  `--rocksdb-path`, `--at-epoch N`, `--map "src:agg,..."`, validates
  that `N` is strictly in the future relative to
  `EpochBatcherState.next_epoch_seq`, and writes one OwnershipEpoch row.

Aggregators opt in with `--use-online-ownership` (or `USE_ONLINE_OWNERSHIP=1`).
When unset, behaviour is identical to existing static partitioning.

Reshards are triggered manually via `reshard-controller`; an automatic
controller-driven rebalancer (a polling controller that decides *when* and
*how* to rebalance) is **future work**.

Security: the SHA-256 per-source chain is preserved across a handoff. Each
aggregator durably persists the tip of every source it owns as an
`AggSourceTip` row (committed atomically with the epoch); when a source is
reassigned, the new owner inherits that tip — propagated through the
coordination store, with the `Handoff` row recording the transition for audit —
and continues the chain from it instead of restarting from zero. The new
owner's first post-handoff batch is verified to chain from the inherited tip:
on the real ingest path the aggregator refuses to extend (and the zkVM proof
fails to verify) if the tip does not match, so a buggy or malicious controller
cannot silently re-seat a source onto a fork. This holds for an arbitrary X→Y
reshard (scale up or down); see `EVALUATION_ONLINE_RESHARDING.md` for the
end-to-end verification (including a real zkVM proof that verifies across a
handoff).

Evaluation harnesses: `scripts/bench/bench_resharding_xy.sh` (general X→Y reshard,
scale up & down), `scripts/bench/bench_resharding_real.sh` (real raw_db path, chain
verified), `scripts/bench/prove_handoff_demo.sh` (real zkVM proof across a handoff).
See `EVALUATION_ONLINE_RESHARDING.md`.
