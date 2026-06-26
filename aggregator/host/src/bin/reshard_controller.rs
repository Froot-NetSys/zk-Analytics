//! reshard-controller — manual control-plane tool that installs an
//! `OwnershipEpoch` row into the aggregator's RocksDB.
//!
//! This is the SIGCOMM-rebuttal companion to the OwnershipEpoch / Handoff data
//! structures landed in `common/src/rocksdb_store.rs`. The full controller-driven
//! online rebalancer is future work; this binary is a one-shot tool that lets
//! an operator install a new ownership map for a future epoch boundary.
//!
//! Usage:
//!
//!     reshard-controller \
//!         --rocksdb-path /mydata/rocksdb_agg \
//!         --at-epoch 42 \
//!         --map "1:0,2:1,3:0,4:1"
//!
//! Semantics:
//! * `--at-epoch N` is the epoch_seq from which the new map is active.
//! * `--map "src:agg,..."` is the ownership assignment (src = source_id, agg = aggregator_id).
//! * The tool refuses to install a retroactive map: `N` must be strictly
//!   greater than the `EpochBatcherState.next_epoch_seq` (or, if that record
//!   is absent, strictly greater than the highest existing OwnershipEpoch).
//! * No `Handoff` rows are written by this tool — that is the aggregator's
//!   responsibility at runtime when it observes the transition (incoming or
//!   outgoing) for sources it currently owns.

use anyhow::{anyhow, Context, Result};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use common::rocksdb_store::{OwnershipEpoch, RocksDb};

fn parse_arg(name: &str) -> Option<String> {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next();
        }
        if let Some(rest) = arg.strip_prefix(&format!("{}=", name)) {
            return Some(rest.to_string());
        }
    }
    None
}

fn parse_map(map_str: &str) -> Result<Vec<(u32, u32)>> {
    let mut out: BTreeMap<u32, u32> = BTreeMap::new();
    for piece in map_str.split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let (l, r) = piece
            .split_once(':')
            .ok_or_else(|| anyhow!("expected src:agg pair, got {:?}", piece))?;
        let src: u32 = l
            .trim()
            .parse()
            .with_context(|| format!("parse source_id from {:?}", l))?;
        let agg: u32 = r
            .trim()
            .parse()
            .with_context(|| format!("parse aggregator_id from {:?}", r))?;
        if let Some(prev) = out.insert(src, agg) {
            return Err(anyhow!(
                "source_id {} listed twice in --map (was {}, now {})",
                src,
                prev,
                agg
            ));
        }
    }
    if out.is_empty() {
        return Err(anyhow!("--map produced an empty assignment list"));
    }
    Ok(out.into_iter().collect())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn main() -> Result<()> {
    let path = parse_arg("--rocksdb-path")
        .ok_or_else(|| anyhow!("missing required --rocksdb-path"))?;
    let at_epoch: i64 = parse_arg("--at-epoch")
        .ok_or_else(|| anyhow!("missing required --at-epoch"))?
        .parse()
        .context("parse --at-epoch as i64")?;
    let map_str = parse_arg("--map").ok_or_else(|| anyhow!("missing required --map"))?;
    let assignments = parse_map(&map_str)?;

    let db = RocksDb::open(&path).with_context(|| format!("open rocksdb at {}", path))?;

    // Validation: --at-epoch must be a future epoch.
    //
    // Primary check: against EpochBatcherState.next_epoch_seq if it exists.
    // Fallback (e.g. before any epoch has been minted): require at_epoch >
    // the highest existing OwnershipEpoch (or >= 0 if none exist).
    if let Some(state) = db
        .get_epoch_batcher_state()
        .context("read EpochBatcherState")?
    {
        if at_epoch <= state.next_epoch_seq {
            return Err(anyhow!(
                "refusing to install OwnershipEpoch retroactively: --at-epoch={} <= next_epoch_seq={}",
                at_epoch,
                state.next_epoch_seq
            ));
        }
    } else {
        let existing = db.ownership_epochs().context("list existing OwnershipEpochs")?;
        if let Some(last) = existing.last() {
            if at_epoch <= last.epoch_seq {
                return Err(anyhow!(
                    "refusing to install OwnershipEpoch retroactively: --at-epoch={} <= last installed OwnershipEpoch.epoch_seq={}",
                    at_epoch,
                    last.epoch_seq
                ));
            }
        }
        eprintln!(
            "[reshard-controller] note: no EpochBatcherState found (cold DB); validating against installed OwnershipEpoch rows only"
        );
    }

    // Compute the diff against the *previously active* OwnershipEpoch (the row
    // active at at_epoch-1). Sources whose owner changes will see a Handoff
    // row written at runtime by the aggregators themselves.
    let prev_active = db
        .ownership_epoch_at(at_epoch.saturating_sub(1))
        .context("look up active OwnershipEpoch at at_epoch-1")?;
    let prev_map: BTreeMap<u32, u32> = prev_active
        .as_ref()
        .map(|oe| oe.assignments.iter().copied().collect())
        .unwrap_or_default();
    let new_map: BTreeMap<u32, u32> = assignments.iter().copied().collect();

    let mut changes: Vec<(u32, Option<u32>, u32)> = Vec::new();
    let mut all_sources: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    all_sources.extend(prev_map.keys().copied());
    all_sources.extend(new_map.keys().copied());
    for src in &all_sources {
        let prev = prev_map.get(src).copied();
        let next = new_map.get(src).copied();
        match (prev, next) {
            (Some(p), Some(n)) if p != n => changes.push((*src, Some(p), n)),
            (None, Some(n)) => changes.push((*src, None, n)),
            // Source removed from the new map -> falls back to default at
            // runtime; we still log it as a change.
            (Some(_p), None) => {
                eprintln!(
                    "[reshard-controller] warn: source_id={} present in previous map but absent from new --map; runtime will fall back to default aggregator",
                    src
                );
            }
            _ => {}
        }
    }

    eprintln!(
        "[reshard-controller] target: --at-epoch={} --rocksdb-path={} new_map_size={} prev_map_size={}",
        at_epoch,
        path,
        new_map.len(),
        prev_map.len()
    );
    if changes.is_empty() {
        eprintln!("[reshard-controller] diff: no owner changes (new map matches active map)");
    } else {
        eprintln!("[reshard-controller] diff: {} source(s) change owner", changes.len());
        for (src, from, to) in &changes {
            match from {
                Some(f) => eprintln!(
                    "[reshard-controller]   source_id={:>5} {} -> {}",
                    src, f, to
                ),
                None => eprintln!(
                    "[reshard-controller]   source_id={:>5} (new) -> {}",
                    src, to
                ),
            }
        }
    }

    let oe = OwnershipEpoch {
        epoch_seq: at_epoch,
        assignments,
        installed_at_ms: now_ms(),
    };
    db.put_ownership_epoch(&oe)
        .context("install OwnershipEpoch")?;
    eprintln!(
        "[reshard-controller] OK: installed OwnershipEpoch at epoch_seq={} ({} assignments)",
        oe.epoch_seq,
        oe.assignments.len()
    );
    Ok(())
}
