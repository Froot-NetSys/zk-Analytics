//! Integration test for `recover_partial_state()`.
//!
//! Simulates a crash mid-WriteBatch by writing partial epoch rows (an
//! `agg_epoch` + a per-mode struct row, with NO tombstone), then asserts
//! `recover_partial_state()` cleans them up. Then writes a complete epoch
//! including a tombstone and asserts the rows survive a second recovery pass.
//!
//! This is a unit test against the in-process RocksDB store — not a real
//! crash-injection test (which is out of scope for this branch).

use rocksdb::WriteBatch;
use tempfile::tempdir;
use common::epoch::EpochType;
use common::rocksdb_store::{
    AggEpoch, AggEpochMeta, AggEpochProof, AggHistStruct, EpochTombstone, RocksDb,
};
use aggregator::recovery::recover_partial_state;

const AGG_ID: u32 = 7;
const EPOCH_TYPE: EpochType = EpochType::HistogramEpoch;
const MODE: &str = "histogram";

fn write_partial_epoch(db: &RocksDb, seq: i64) {
    // Mimic a crash mid-batch: agg_epoch + per-mode struct exist, but NO tombstone.
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
    )
    .unwrap();
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
    )
    .unwrap();
    db.write_batch(batch).unwrap();
}

fn write_complete_epoch(db: &RocksDb, seq: i64) {
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
    )
    .unwrap();
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
    )
    .unwrap();
    db.put_agg_epoch_meta(
        &mut batch,
        &AggEpochMeta {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            ingest_time_ms: 2000,
            n_events: 5,
            aggregator_id: AGG_ID,
        },
    )
    .unwrap();
    db.put_agg_epoch_proof(
        &mut batch,
        &AggEpochProof {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            receipt_words: vec![0u32; 4],
            aggregator_id: AGG_ID,
        },
    )
    .unwrap();
    db.put_epoch_tombstone(
        &mut batch,
        &EpochTombstone {
            epoch_type: EPOCH_TYPE,
            sequence: seq,
            aggregator_id: AGG_ID,
            completed_at_ms: 2000,
        },
    )
    .unwrap();
    db.write_batch(batch).unwrap();
}

#[test]
fn recovery_deletes_orphaned_partial_writes() {
    let tmp = tempdir().unwrap();
    let db = RocksDb::open(tmp.path()).unwrap();

    // 1. Simulate a partial-write crash: rows present, no tombstone.
    let seq: i64 = 0;
    write_partial_epoch(&db, seq);

    assert!(db.has_agg_epoch(EPOCH_TYPE, seq).unwrap());
    assert!(db.has_agg_hist_struct(seq).unwrap());
    assert!(!db.has_epoch_tombstone(EPOCH_TYPE, seq, AGG_ID).unwrap());

    // 2. Recovery should clean up the orphaned rows.
    let cleaned = recover_partial_state(&db, MODE, EPOCH_TYPE, AGG_ID).unwrap();
    assert_eq!(cleaned, 1, "recovery should clean exactly 1 orphaned epoch");
    assert!(
        !db.has_agg_epoch(EPOCH_TYPE, seq).unwrap(),
        "agg_epoch row should be deleted"
    );
    assert!(
        !db.has_agg_hist_struct(seq).unwrap(),
        "agg_hist_struct row should be deleted"
    );
}

#[test]
fn recovery_preserves_completed_epochs() {
    let tmp = tempdir().unwrap();
    let db = RocksDb::open(tmp.path()).unwrap();

    // Write a complete epoch (with tombstone), then recover.
    let seq: i64 = 3;
    write_complete_epoch(&db, seq);

    assert!(db.has_epoch_tombstone(EPOCH_TYPE, seq, AGG_ID).unwrap());
    let max_done = db
        .max_epoch_tombstone_seq(EPOCH_TYPE, AGG_ID)
        .unwrap();
    assert_eq!(max_done, Some(seq));

    let cleaned = recover_partial_state(&db, MODE, EPOCH_TYPE, AGG_ID).unwrap();
    assert_eq!(cleaned, 0, "recovery should not touch completed epochs");

    // All rows should still be present.
    assert!(db.has_agg_epoch(EPOCH_TYPE, seq).unwrap());
    assert!(db.has_agg_hist_struct(seq).unwrap());
    assert!(db.has_agg_epoch_meta(EPOCH_TYPE, seq).unwrap());
    assert!(db.has_agg_epoch_proof(EPOCH_TYPE, seq).unwrap());
    assert!(db.has_epoch_tombstone(EPOCH_TYPE, seq, AGG_ID).unwrap());
}

#[test]
fn recovery_mixed_scenario() {
    // Combined: one completed epoch and one orphaned partial write side-by-side.
    let tmp = tempdir().unwrap();
    let db = RocksDb::open(tmp.path()).unwrap();

    write_complete_epoch(&db, 1);
    // Partial write at seq=2 (within the +/-5 window around max_done=1).
    write_partial_epoch(&db, 2);

    let cleaned = recover_partial_state(&db, MODE, EPOCH_TYPE, AGG_ID).unwrap();
    assert_eq!(cleaned, 1);

    // Completed epoch is intact.
    assert!(db.has_agg_epoch(EPOCH_TYPE, 1).unwrap());
    assert!(db.has_epoch_tombstone(EPOCH_TYPE, 1, AGG_ID).unwrap());

    // Orphan was wiped.
    assert!(!db.has_agg_epoch(EPOCH_TYPE, 2).unwrap());
    assert!(!db.has_agg_hist_struct(2).unwrap());
    assert!(!db.has_epoch_tombstone(EPOCH_TYPE, 2, AGG_ID).unwrap());
}
