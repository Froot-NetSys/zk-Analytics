//! handoff_sync — replicate online-resharding coordination state (`Handoff`
//! rows AND durable per-source chain tips) from one or more aggregator stores
//! into another.
//!
//! Online resharding's `Handoff` rows and `AggSourceTip` rows are *shared
//! coordination state*: the aggregator that owns a source advances and persists
//! its per-source chain tip; the aggregator that GAINS the source after a
//! reshard must see that tip to inherit it (see the incoming-inheritance path
//! in `main.rs`). In a real deployment that state lives in one coordination
//! store (e.g. FoundationDB). On a single machine each aggregator carries its
//! own RocksDB, so this tool replicates the state, just as `reshard-controller`
//! installs the OwnershipEpoch map on every store.
//!
//! For a general X->Y reshard, pass every source store via repeated `--from`
//! and the destination via `--to`; the union of real-tip rows is written to the
//! destination. Per-source tips are keyed by `source_id` (one current tip per
//! source), so merging across stores is unambiguous: only a source's owner
//! persists a tip for it.
//!
//! Usage:
//!   handoff_sync --from SRC1 [--from SRC2 ...] --to DST [--at-epoch N] [--dry-run]
//!
//! Lines beginning `RESULT ` are machine-readable.

use anyhow::{anyhow, Context, Result};
use common::rocksdb_store::RocksDb;

fn parse_multi(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == name {
            if let Some(v) = it.next() {
                out.push(v);
            }
        } else if let Some(rest) = a.strip_prefix(&format!("{}=", name)) {
            out.push(rest.to_string());
        }
    }
    out
}

fn parse_arg(name: &str) -> Option<String> {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == name {
            return it.next();
        }
        if let Some(rest) = a.strip_prefix(&format!("{}=", name)) {
            return Some(rest.to_string());
        }
    }
    None
}

fn has_flag(name: &str) -> bool {
    std::env::args().skip(1).any(|a| a == name)
}

fn main() -> Result<()> {
    let froms = parse_multi("--from");
    if froms.is_empty() {
        return Err(anyhow!("missing required --from (repeatable)"));
    }
    let to = parse_arg("--to").ok_or_else(|| anyhow!("missing required --to"))?;
    let at_epoch: Option<i64> = parse_arg("--at-epoch").map(|s| s.parse()).transpose()?;
    let dry_run = has_flag("--dry-run");

    let dst = RocksDb::open(&to).with_context(|| format!("open dest rocksdb {}", to))?;

    let mut handoffs_copied = 0usize;
    let mut tips_copied = 0usize;
    let mut handoffs_skipped = 0usize;

    for from in &froms {
        if *from == to {
            continue; // nothing to copy from a store into itself
        }
        let src = RocksDb::open(from).with_context(|| format!("open source rocksdb {}", from))?;

        // 1) Handoff rows (authoritative, real-tip only).
        let handoffs = match at_epoch {
            Some(e) => src.handoffs_for_epoch(e).context("read handoffs_for_epoch")?,
            None => src.handoffs().context("read handoffs")?,
        };
        for h in &handoffs {
            if h.chain_tip == [0u8; 32] || h.last_seq < 0 {
                handoffs_skipped += 1;
                continue;
            }
            if !dry_run {
                dst.put_handoff(h).context("put_handoff into dest")?;
            }
            handoffs_copied += 1;
        }

        // 2) Durable per-source chain tips (real-tip only).
        for t in &src.source_tips().context("read source_tips")? {
            if t.chain_tip == [0u8; 32] || t.last_seq < 0 {
                continue;
            }
            eprintln!(
                "[handoff_sync] {} -> {}: source_tip src={} owner={} last_seq={} {}",
                from,
                to,
                t.source_id,
                t.owner,
                t.last_seq,
                if dry_run { "(dry-run)" } else { "copy" }
            );
            if !dry_run {
                dst.put_source_tip_now(t).context("put_source_tip into dest")?;
            }
            tips_copied += 1;
        }
    }

    println!(
        "RESULT froms={} to={} handoffs_copied={} handoffs_skipped_zero_tip={} source_tips_copied={} dry_run={}",
        froms.len(),
        to,
        handoffs_copied,
        handoffs_skipped,
        tips_copied,
        dry_run
    );
    Ok(())
}
