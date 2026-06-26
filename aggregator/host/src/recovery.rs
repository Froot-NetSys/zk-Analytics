//! Startup recovery for the aggregator: scans a small window of epoch sequences
//! around the highest tombstoned sequence and deletes orphaned partial writes
//! left over from a crash mid-WriteBatch.
//!
//! The single source of truth for "this epoch is fully done" is the
//! `EpochTombstone` written atomically alongside the rest of the epoch's rows.
//! If a tombstone is missing but any of the related rows
//! (`agg_epoch`, the per-mode struct, `agg_epoch_meta`, `agg_epoch_proof`)
//! exist, those are partial writes and are cleaned up here.

use anyhow::{Context, Result};
use rocksdb::WriteBatch;
use zktelemetry_common::epoch::EpochType;
use zktelemetry_common::rocksdb_store::RocksDb;

/// Scan a small window around the highest tombstoned sequence for orphaned partial
/// writes. For each sequence in the window that has *no* tombstone but *does* have
/// any of the related rows, delete those rows in a single atomic WriteBatch.
///
/// Returns the number of orphaned epochs cleaned up. Logs
/// `[native-aggr][recover] max_done={} orphans_cleaned={}`.
///
/// `mode` is one of `"samples"` | `"histogram"` | `"cm"`.
pub fn recover_partial_state(
    agg_db: &RocksDb,
    mode: &str,
    epoch_type: EpochType,
    aggregator_id: u32,
) -> Result<usize> {
    let max_done = agg_db
        .max_epoch_tombstone_seq(epoch_type, aggregator_id)
        .unwrap_or(None);

    // Window: 5 seqs on either side of max_done (or -1 if no tombstones yet).
    let center = max_done.unwrap_or(-1);
    let lo = center.saturating_sub(5);
    let hi = center.saturating_add(5);

    let mut batch = WriteBatch::default();
    let mut orphans_cleaned: usize = 0;

    for seq in lo..=hi {
        if seq < 0 {
            continue;
        }
        // Skip already-tombstoned seqs.
        if agg_db
            .has_epoch_tombstone(epoch_type, seq, aggregator_id)
            .unwrap_or(false)
        {
            continue;
        }

        // Per-mode struct existence check.
        let has_mode_struct = match mode {
            "samples" => agg_db.has_verified_samples_struct(seq).unwrap_or(false),
            "histogram" => agg_db.has_agg_hist_struct(seq).unwrap_or(false),
            "cm" => agg_db.has_agg_cm_struct(seq).unwrap_or(false),
            _ => false,
        };
        let has_epoch = agg_db.has_agg_epoch(epoch_type, seq).unwrap_or(false);
        let has_meta = agg_db.has_agg_epoch_meta(epoch_type, seq).unwrap_or(false);
        let has_proof = agg_db.has_agg_epoch_proof(epoch_type, seq).unwrap_or(false);

        if has_mode_struct || has_epoch || has_meta || has_proof {
            eprintln!(
                "[native-aggr][recover] orphaned partial write at seq={} (epoch={} struct={} meta={} proof={}); cleaning up",
                seq, has_epoch, has_mode_struct, has_meta, has_proof
            );
            if has_epoch {
                agg_db.delete_agg_epoch(&mut batch, epoch_type, seq)?;
            }
            if has_meta {
                agg_db.delete_agg_epoch_meta(&mut batch, epoch_type, seq)?;
            }
            if has_proof {
                agg_db.delete_agg_epoch_proof(&mut batch, epoch_type, seq)?;
            }
            match mode {
                "samples" if has_mode_struct => {
                    agg_db.delete_verified_samples_struct(&mut batch, seq)?;
                }
                "histogram" if has_mode_struct => {
                    agg_db.delete_agg_hist_struct(&mut batch, seq)?;
                }
                "cm" if has_mode_struct => {
                    agg_db.delete_agg_cm_struct(&mut batch, seq)?;
                }
                _ => {}
            }
            orphans_cleaned += 1;
        }
    }

    if orphans_cleaned > 0 {
        agg_db
            .write_batch(batch)
            .context("write recovery cleanup batch")?;
    }

    eprintln!(
        "[native-aggr][recover] max_done={} orphans_cleaned={}",
        max_done
            .map(|v| v.to_string())
            .unwrap_or_else(|| "None".to_string()),
        orphans_cleaned
    );

    Ok(orphans_cleaned)
}
