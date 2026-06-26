//! reshard_bench — in-process performance benchmark for the online-resharding
//! data structures (`OwnershipEpoch` install + `current_owner_for_source`
//! lookup).
//!
//! This measures the *control-plane* and *data-plane* hot paths that scale
//! when you reshard, isolated from process-startup / RocksDB-open noise:
//!
//!   1. install_ms       — `put_ownership_epoch()` for a map of `S` sources.
//!                          This is what the reshard-controller does; it is the
//!                          "time to finish installing a reshard".
//!   2. lookup_us         — `current_owner_for_source()` per source, the
//!                          per-(source,epoch) filter the aggregator runs every
//!                          epoch under `--use-online-ownership`. Reported as
//!                          microseconds/lookup, swept over source count `S`.
//!   3. lookup_vs_history — the same lookup as a function of reshard-history
//!                          depth `H` (number of installed OwnershipEpoch rows),
//!                          because `ownership_epoch_at()` forward-scans the
//!                          OwnershipEpoch keyspace.
//!
//! All timings are wall-clock around in-process calls on an already-open DB.
//! Lines beginning `RESULT ` are machine-readable key=value records.
//!
//! Usage:
//!   reshard_bench [--sources-sweep 2,8,32,128,512]
//!                 [--history-sweep 1,8,64,512]
//!                 [--iters 5000] [--workdir /tmp/zkt_reshard_bench]

use anyhow::{Context, Result};
use std::time::Instant;
use zktelemetry_common::rocksdb_store::{current_owner_for_source, OwnershipEpoch, RocksDb};

fn parse_str(name: &str, default: &str) -> String {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == name {
            return it.next().unwrap_or_else(|| default.to_string());
        }
        if let Some(rest) = a.strip_prefix(&format!("{}=", name)) {
            return rest.to_string();
        }
    }
    default.to_string()
}

fn parse_list(name: &str, default: &str) -> Vec<usize> {
    parse_str(name, default)
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect()
}

fn parse_usize(name: &str, default: usize) -> usize {
    parse_str(name, &default.to_string())
        .parse()
        .unwrap_or(default)
}

/// Build an ownership map of `s` sources round-robin across `aggs` aggregators.
fn build_map(s: usize, aggs: u32) -> Vec<(u32, u32)> {
    (0..s as u32).map(|src| (src, src % aggs.max(1))).collect()
}

fn main() -> Result<()> {
    let sources_sweep = parse_list("--sources-sweep", "2,8,32,128,512");
    let history_sweep = parse_list("--history-sweep", "1,8,64,512");
    let iters = parse_usize("--iters", 5000).max(1);
    let workdir = parse_str("--workdir", "/tmp/zkt_reshard_bench");

    std::fs::create_dir_all(&workdir).context("create workdir")?;
    eprintln!(
        "[reshard_bench] sources_sweep={:?} history_sweep={:?} iters={} workdir={}",
        sources_sweep, history_sweep, iters, workdir
    );

    // ---- Part 1+2: install latency + lookup latency vs source count ----
    // One OwnershipEpoch installed at epoch 1; lookups at epoch 2.
    for &s in &sources_sweep {
        let db_path = format!("{}/sources_{}", workdir, s);
        let _ = std::fs::remove_dir_all(&db_path);
        let db = RocksDb::open(&db_path).context("open db")?;

        let map = build_map(s, 2);
        let oe = OwnershipEpoch {
            epoch_seq: 1,
            assignments: map.clone(),
            installed_at_ms: 0,
        };

        // Install latency: median over a few installs (idempotent overwrite).
        let mut install_samples = Vec::new();
        for _ in 0..20 {
            let t = Instant::now();
            db.put_ownership_epoch(&oe).context("put ownership epoch")?;
            install_samples.push(t.elapsed().as_secs_f64() * 1e3);
        }
        install_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let install_ms = install_samples[install_samples.len() / 2];

        // Lookup latency: cycle over all sources, `iters` total lookups.
        let t = Instant::now();
        let mut sink = 0u64;
        for i in 0..iters {
            let src = (i % s.max(1)) as u32;
            let owner = current_owner_for_source(&db, src, 2, u32::MAX)?;
            sink = sink.wrapping_add(owner as u64);
        }
        let total = t.elapsed().as_secs_f64();
        let lookup_us = total / iters as f64 * 1e6;

        println!(
            "RESULT phase=sources sources={} install_ms_median={:.4} lookup_us={:.3} throughput_lookups_per_s={:.0} sink={}",
            s,
            install_ms,
            lookup_us,
            iters as f64 / total,
            sink
        );
        eprintln!(
            "[reshard_bench] S={:>4}  install={:.4} ms  lookup={:.3} us/op  ({:.0} lookups/s)",
            s, install_ms, lookup_us, iters as f64 / total
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&db_path);
    }

    // ---- Part 3: lookup latency vs reshard-history depth ----
    // Install H OwnershipEpoch rows (epochs 1..=H); lookup at epoch H+1 so the
    // scan in `ownership_epoch_at` traverses the whole history.
    let fixed_sources = 32usize;
    for &h in &history_sweep {
        let db_path = format!("{}/history_{}", workdir, h);
        let _ = std::fs::remove_dir_all(&db_path);
        let db = RocksDb::open(&db_path).context("open db")?;

        let map = build_map(fixed_sources, 2);
        for e in 1..=h as i64 {
            db.put_ownership_epoch(&OwnershipEpoch {
                epoch_seq: e,
                assignments: map.clone(),
                installed_at_ms: 0,
            })?;
        }

        let lookup_epoch = h as i64 + 1;
        let t = Instant::now();
        let mut sink = 0u64;
        for i in 0..iters {
            let src = (i % fixed_sources) as u32;
            let owner = current_owner_for_source(&db, src, lookup_epoch, u32::MAX)?;
            sink = sink.wrapping_add(owner as u64);
        }
        let total = t.elapsed().as_secs_f64();
        let lookup_us = total / iters as f64 * 1e6;

        println!(
            "RESULT phase=history history_depth={} sources={} lookup_us={:.3} throughput_lookups_per_s={:.0} sink={}",
            h,
            fixed_sources,
            lookup_us,
            iters as f64 / total,
            sink
        );
        eprintln!(
            "[reshard_bench] H={:>4}  lookup={:.3} us/op  ({:.0} lookups/s)",
            h, lookup_us, iters as f64 / total
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&db_path);
    }

    Ok(())
}
