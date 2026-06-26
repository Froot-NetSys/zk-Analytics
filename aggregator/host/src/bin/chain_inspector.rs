//! chain_inspector — read-only auditor for the online-resharding data plane.
//!
//! Opens one or more aggregator RocksDBs and reports, for each:
//!   * the installed `OwnershipEpoch` rows (the ownership timeline), and
//!   * the `Handoff` rows written by the aggregator at runtime,
//! then renders a *chain-continuity verdict* for every handoff.
//!
//! Motivation: the original existence-proof bench only checked that ownership
//! log lines were emitted and deferred the actual chain-continuity comparison
//! "to a follow-up Rust tool". This is that tool (see the X→Y harnesses in
//! `scripts/bench_resharding_*.sh`).
//!
//! Continuity criterion. A handoff preserves the per-source SHA-256 chain iff
//! the receiving aggregator continues the moved source's chain from the tip
//! the previous owner published — i.e. the `Handoff` row must carry:
//!   * a non-zero `chain_tip` (the real tip, not a bootstrap-from-zero), and
//!   * a known `from_aggregator` (not the `u32::MAX` "unknown prior owner"
//!     sentinel), and
//!   * a `last_seq >= 0`.
//! A handoff that is zero-tip / sentinel-owner is a *bootstrap*: the new owner
//! restarts the chain from scratch, so continuity across the boundary is NOT
//! established by the row.
//!
//! Usage:
//!   chain_inspector --rocksdb-path DB [--rocksdb-path DB2 ...] [--strict]
//!
//! `--strict` makes the process exit 1 if any handoff fails the continuity
//! criterion (useful as a CI gate); without it the tool always exits 0 and
//! just prints the verdict. Lines beginning `RESULT ` are machine-readable
//! key=value records for harness consumption.

use anyhow::{anyhow, Context, Result};
use zktelemetry_common::rocksdb_store::{Handoff, OwnershipEpoch, RocksDb};

fn parse_multi(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            if let Some(v) = it.next() {
                out.push(v);
            }
        } else if let Some(rest) = arg.strip_prefix(&format!("{}=", name)) {
            out.push(rest.to_string());
        }
    }
    out
}

fn has_flag(name: &str) -> bool {
    std::env::args().skip(1).any(|a| a == name)
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

/// `--check-coverage`: verify the OwnershipEpoch map active at `--at-epoch`
/// partitions `--sources` sources across exactly `--aggregators` aggregators —
/// every source assigned to exactly one aggregator in `[0, Y)`, none dropped.
/// This is the correctness check for an X->Y reshard's ownership layer.
fn check_coverage() -> Result<()> {
    use zktelemetry_common::rocksdb_store::current_owner_for_source;
    let path = parse_arg("--rocksdb-path")
        .ok_or_else(|| anyhow!("--check-coverage requires --rocksdb-path"))?;
    let n: u32 = parse_arg("--sources")
        .ok_or_else(|| anyhow!("--check-coverage requires --sources"))?
        .parse()?;
    let y: u32 = parse_arg("--aggregators")
        .ok_or_else(|| anyhow!("--check-coverage requires --aggregators"))?
        .parse()?;
    let at_epoch: i64 = parse_arg("--at-epoch")
        .ok_or_else(|| anyhow!("--check-coverage requires --at-epoch"))?
        .parse()?;
    let strict = has_flag("--strict");

    let db = RocksDb::open(&path).with_context(|| format!("open rocksdb at {}", path))?;
    let mut load = vec![0u32; y as usize];
    let mut uncovered = 0u32;
    let mut out_of_range = 0u32;
    for sid in 0..n {
        // default sentinel u32::MAX => "no map entry" (would fall to static default).
        let owner = current_owner_for_source(&db, sid, at_epoch, u32::MAX)?;
        if owner == u32::MAX {
            uncovered += 1;
        } else if owner >= y {
            out_of_range += 1;
        } else {
            load[owner as usize] += 1;
        }
    }
    let assigned: u32 = load.iter().sum();
    let loads: Vec<String> = load
        .iter()
        .enumerate()
        .map(|(a, c)| format!("{}:{}", a, c))
        .collect();
    println!(
        "RESULT_COVERAGE at_epoch={} sources={} aggregators={} assigned={} uncovered={} out_of_range={} per_aggregator=[{}]",
        at_epoch, n, y, assigned, uncovered, out_of_range, loads.join(",")
    );
    let ok = uncovered == 0 && out_of_range == 0 && assigned == n && load.iter().all(|&c| c > 0);
    if ok {
        println!(
            "VERDICT: COVERAGE OK — {} sources partitioned across all {} aggregators (every source owned exactly once; min/max load {}/{}).",
            n,
            y,
            load.iter().min().unwrap_or(&0),
            load.iter().max().unwrap_or(&0)
        );
    } else {
        println!(
            "VERDICT: COVERAGE FAIL — uncovered={} out_of_range={} idle_aggregators={}",
            uncovered,
            out_of_range,
            load.iter().filter(|&&c| c == 0).count()
        );
    }
    if strict && !ok {
        std::process::exit(1);
    }
    Ok(())
}

fn is_zero(tip: &[u8; 32]) -> bool {
    tip.iter().all(|&b| b == 0)
}

fn short_hex(tip: &[u8; 32]) -> String {
    let mut s = String::with_capacity(16);
    for b in &tip[..8] {
        s.push_str(&format!("{:02x}", b));
    }
    s.push('…');
    s
}

struct DbVerdict {
    path: String,
    ownership_rows: usize,
    handoffs_total: usize,
    handoffs_continuous: usize,
    handoffs_bootstrap_zero_tip: usize,
    handoffs_unknown_prev_owner: usize,
}

fn inspect_one(path: &str) -> Result<DbVerdict> {
    let db = RocksDb::open(path).with_context(|| format!("open rocksdb at {}", path))?;

    let ownership: Vec<OwnershipEpoch> =
        db.ownership_epochs().context("read ownership epochs")?;
    let handoffs: Vec<Handoff> = db.handoffs().context("read handoffs")?;

    println!("== {} ==", path);
    println!("  OwnershipEpoch rows: {}", ownership.len());
    for oe in &ownership {
        let mut a = oe.assignments.clone();
        a.sort();
        let pretty: Vec<String> = a.iter().map(|(s, g)| format!("{}->{}", s, g)).collect();
        println!(
            "    epoch_seq={:>4}  installed_at_ms={}  map=[{}]",
            oe.epoch_seq,
            oe.installed_at_ms,
            pretty.join(", ")
        );
    }

    let mut continuous = 0usize;
    let mut bootstrap_zero = 0usize;
    let mut unknown_prev = 0usize;

    println!("  Handoff rows: {}", handoffs.len());
    for h in &handoffs {
        let zero_tip = is_zero(&h.chain_tip);
        let unknown_owner = h.from_aggregator == u32::MAX;
        let ok = !zero_tip && !unknown_owner && h.last_seq >= 0;
        if ok {
            continuous += 1;
        }
        if zero_tip {
            bootstrap_zero += 1;
        }
        if unknown_owner {
            unknown_prev += 1;
        }
        let verdict = if ok { "CONTINUOUS" } else { "BOOTSTRAP" };
        let from = if unknown_owner {
            "UNKNOWN(u32::MAX)".to_string()
        } else {
            h.from_aggregator.to_string()
        };
        println!(
            "    src={:>4} at_epoch={:>4} {}->{} last_seq={:>4} tip={} [{}]",
            h.source_id,
            h.at_epoch,
            from,
            h.to_aggregator,
            h.last_seq,
            if zero_tip {
                "ZERO".to_string()
            } else {
                short_hex(&h.chain_tip)
            },
            verdict
        );
    }

    Ok(DbVerdict {
        path: path.to_string(),
        ownership_rows: ownership.len(),
        handoffs_total: handoffs.len(),
        handoffs_continuous: continuous,
        handoffs_bootstrap_zero_tip: bootstrap_zero,
        handoffs_unknown_prev_owner: unknown_prev,
    })
}

fn main() -> Result<()> {
    if has_flag("--check-coverage") {
        return check_coverage();
    }
    let paths = parse_multi("--rocksdb-path");
    if paths.is_empty() {
        return Err(anyhow!(
            "usage: chain_inspector --rocksdb-path DB [--rocksdb-path DB2 ...] [--strict]"
        ));
    }
    let strict = has_flag("--strict");

    let mut total_handoffs = 0usize;
    let mut total_continuous = 0usize;
    let mut total_bootstrap = 0usize;
    let mut total_unknown = 0usize;

    for p in &paths {
        let v = inspect_one(p)?;
        total_handoffs += v.handoffs_total;
        total_continuous += v.handoffs_continuous;
        total_bootstrap += v.handoffs_bootstrap_zero_tip;
        total_unknown += v.handoffs_unknown_prev_owner;
        // Per-DB machine-readable record.
        println!(
            "RESULT db={} ownership_rows={} handoffs={} continuous={} bootstrap_zero_tip={} unknown_prev_owner={}",
            v.path,
            v.ownership_rows,
            v.handoffs_total,
            v.handoffs_continuous,
            v.handoffs_bootstrap_zero_tip,
            v.handoffs_unknown_prev_owner
        );
    }

    let all_continuous = total_handoffs > 0 && total_continuous == total_handoffs;
    println!(
        "RESULT_SUMMARY dbs={} handoffs={} continuous={} bootstrap_zero_tip={} unknown_prev_owner={} all_continuous={}",
        paths.len(),
        total_handoffs,
        total_continuous,
        total_bootstrap,
        total_unknown,
        all_continuous
    );

    if total_handoffs == 0 {
        println!("VERDICT: no handoffs observed (no ownership transitions crossed a boundary).");
    } else if all_continuous {
        println!(
            "VERDICT: PASS — all {} handoff(s) carry a real chain_tip; per-source chain stitches across the boundary.",
            total_handoffs
        );
    } else {
        println!(
            "VERDICT: BROKEN CONTINUITY — {}/{} handoff(s) are bootstrap-from-zero (zero_tip={}, unknown_prev_owner={}). \
The receiving aggregator restarts the moved source's chain instead of inheriting the previous owner's tip.",
            total_handoffs - total_continuous,
            total_handoffs,
            total_bootstrap,
            total_unknown
        );
    }

    if strict && !all_continuous {
        std::process::exit(1);
    }
    Ok(())
}
