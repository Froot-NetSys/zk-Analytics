//! recovery_bench — measures aggregator fault-recovery time.
//!
//! Fault recovery is the `recover_partial_state()` pass that runs once at
//! startup (see `src/recovery.rs`): it finds the highest tombstoned sequence
//! and cleans orphaned partial writes left by a crash mid-`WriteBatch`. This
//! bench populates a RocksDB with `N` completed epochs (each = the full atomic
//! row set: agg_epoch + per-mode struct + meta + proof + tombstone) plus `K`
//! orphaned partial writes just past the last tombstone, then times recovery.
//!
//! It isolates two costs:
//!   * `max_seq_ms`  — time for `max_epoch_tombstone_seq()` alone, which today
//!     does a full forward scan of the tombstone keyspace (so it grows with N).
//!   * `recover_ms`  — total `recover_partial_state()` time (scan + the fixed
//!     11-seq cleanup window WriteBatch).
//!
//! Usage:
//!   recovery_bench [--epochs-sweep 1,10,100,1000,10000] [--orphans 3]
//!                  [--repeat 5] [--workdir /tmp/zkt_recovery_bench]
//!
//! Lines beginning `RESULT ` are machine-readable key=value records.

use anyhow::{Context, Result};
use rocksdb::WriteBatch;
use std::time::Instant;
use common::epoch::EpochType;
use common::rocksdb_store::{
    AggEpoch, AggEpochMeta, AggEpochProof, AggHistStruct, EpochTombstone, RocksDb,
};
use aggregator::recovery::recover_partial_state;

const AGG_ID: u32 = 0;
const EPOCH_TYPE: EpochType = EpochType::HistogramEpoch;
const MODE: &str = "histogram";

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

fn parse_usize(name: &str, default: usize) -> usize {
    parse_str(name, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn write_complete_epoch(db: &RocksDb, seq: i64) -> Result<()> {
    let mut batch = WriteBatch::default();
    db.put_agg_epoch(
        &mut batch,
        &AggEpoch {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            ingest_time_ms: 2000,
            result_commit: vec![5, 6, 7, 8],
            aggregator_id: AGG_ID,
            min_ts: 0,
            max_ts: 10,
        },
    )?;
    db.put_agg_hist_struct(
        &mut batch,
        &AggHistStruct {
            sequence: seq,
            total_count: 5,
            total_sum: 99,
            table_fixed: vec![0u8; 8],
            prev_chain_hash: vec![0u8; 32],
            events_commit: vec![0u8; 32],
            out_commit: vec![0u8; 32],
            final_chain_hash: vec![0u8; 32],
        },
    )?;
    db.put_agg_epoch_meta(
        &mut batch,
        &AggEpochMeta {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            ingest_time_ms: 2000,
            n_events: 5,
            aggregator_id: AGG_ID,
        },
    )?;
    db.put_agg_epoch_proof(
        &mut batch,
        &AggEpochProof {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            receipt_words: vec![0u32; 4],
            aggregator_id: AGG_ID,
        },
    )?;
    db.put_epoch_tombstone(
        &mut batch,
        &EpochTombstone {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            aggregator_id: AGG_ID,
            completed_at_ms: 2000,
        },
    )?;
    db.write_batch(batch).context("write complete epoch")
}

/// A partial write = agg_epoch + per-mode struct, NO tombstone (crash mid-batch).
fn write_partial_epoch(db: &RocksDb, seq: i64) -> Result<()> {
    let mut batch = WriteBatch::default();
    db.put_agg_epoch(
        &mut batch,
        &AggEpoch {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            ingest_time_ms: 1000,
            result_commit: vec![1, 2, 3, 4],
            aggregator_id: AGG_ID,
            min_ts: 0,
            max_ts: 10,
        },
    )?;
    db.put_agg_hist_struct(
        &mut batch,
        &AggHistStruct {
            sequence: seq,
            total_count: 5,
            total_sum: 99,
            table_fixed: vec![0u8; 8],
            prev_chain_hash: vec![0u8; 32],
            events_commit: vec![0u8; 32],
            out_commit: vec![0u8; 32],
            final_chain_hash: vec![0u8; 32],
        },
    )?;
    db.write_batch(batch).context("write partial epoch")
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn main() -> Result<()> {
    let sweep_str = parse_str("--epochs-sweep", "1,10,100,1000,10000");
    let orphans = parse_usize("--orphans", 3);
    let repeat = parse_usize("--repeat", 5).max(1);
    let workdir = parse_str("--workdir", "/tmp/zkt_recovery_bench");

    let sweep: Vec<usize> = sweep_str
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect();

    std::fs::create_dir_all(&workdir).context("create workdir")?;
    eprintln!(
        "[recovery_bench] sweep={:?} orphans={} repeat={} workdir={}",
        sweep, orphans, repeat, workdir
    );

    for &n in &sweep {
        let mut recover_samples = Vec::with_capacity(repeat);
        let mut maxseq_samples = Vec::with_capacity(repeat);
        let mut cleaned_last = 0usize;

        for r in 0..repeat {
            let db_path = format!("{}/db_n{}_r{}", workdir, n, r);
            let _ = std::fs::remove_dir_all(&db_path);
            let db = RocksDb::open(&db_path).context("open bench db")?;

            // N completed epochs: seq 0..N-1.
            for seq in 0..n as i64 {
                write_complete_epoch(&db, seq)?;
            }
            // K orphaned partial writes just past max_done (= N-1), inside the
            // recovery window [max_done-5, max_done+5] so they are reachable.
            // Place them at N, N+1, ... (capped so they stay in-window).
            let in_window_orphans = orphans.min(5);
            for j in 0..in_window_orphans {
                write_partial_epoch(&db, n as i64 + j as i64)?;
            }

            // Isolate the max-seq scan cost.
            let t0 = Instant::now();
            let _ = db.max_epoch_tombstone_seq(EPOCH_TYPE, AGG_ID)?;
            maxseq_samples.push(t0.elapsed().as_secs_f64() * 1e3);

            // Full recovery pass (re-runs the scan internally + cleanup).
            let t1 = Instant::now();
            let cleaned = recover_partial_state(&db, MODE, EPOCH_TYPE, AGG_ID)?;
            recover_samples.push(t1.elapsed().as_secs_f64() * 1e3);
            cleaned_last = cleaned;

            drop(db);
            let _ = std::fs::remove_dir_all(&db_path);
        }

        let rec_med = median(recover_samples.clone());
        let rec_min = recover_samples.iter().cloned().fold(f64::INFINITY, f64::min);
        let ms_med = median(maxseq_samples.clone());

        println!(
            "RESULT epochs={} orphans_cleaned={} max_seq_ms_median={:.3} recover_ms_median={:.3} recover_ms_min={:.3}",
            n,
            cleaned_last,
            ms_med,
            rec_med,
            rec_min
        );
        eprintln!(
            "[recovery_bench] N={:>6}  recover median={:.3} ms (min {:.3})  max_seq_scan median={:.3} ms  orphans_cleaned={}",
            n, rec_med, rec_min, ms_med, cleaned_last
        );
    }

    Ok(())
}
