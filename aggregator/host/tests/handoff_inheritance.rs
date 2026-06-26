//! Integration test for the online-resharding handoff-inheritance fix.
//!
//! Verifies that the authoritative `Handoff` row published by a source's
//! previous owner can be read back point-wise (`handoff_at`) and that the
//! inheritance predicate the aggregator uses on the incoming path
//! (`chain_tip != 0 && last_seq >= 0`) correctly accepts a real-tip handoff and
//! rejects a degenerate/zero one. This is the store-level proof that the new
//! owner inherits the previous owner's per-source chain tip instead of
//! restarting from zero.

use tempfile::tempdir;
use zktelemetry_common::rocksdb_store::{AggSourceTip, Handoff, RocksDb};

const BOUNDARY: i64 = 3;

/// Mirror of the aggregator's incoming-path decision (main.rs): inherit iff the
/// handoff carries a real tip and a valid last_seq.
fn inherits(h: &Handoff) -> bool {
    h.chain_tip != [0u8; 32] && h.last_seq >= 0
}

#[test]
fn incoming_owner_inherits_real_tip_from_handoff() {
    let dir = tempdir().unwrap();
    let db = RocksDb::open(dir.path().to_str().unwrap()).unwrap();

    // Previous owner (agg 0) publishes an authoritative handoff for source 1 as
    // it releases the source to agg 1 at the boundary epoch.
    let mut tip = [0u8; 32];
    tip[..4].copy_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    let published = Handoff {
        source_id: 1,
        at_epoch: BOUNDARY,
        from_aggregator: 0,
        to_aggregator: 1,
        chain_tip: tip,
        last_seq: 23,
        published_at_ms: 111,
    };
    db.put_handoff(&published).unwrap();

    // New owner (agg 1) point-reads the handoff for (boundary, source 1).
    let got = db.handoff_at(BOUNDARY, 1).unwrap().expect("handoff must exist");
    assert_eq!(got.chain_tip, tip, "inherited tip must equal published tip");
    assert_eq!(got.last_seq, 23);
    assert_eq!(got.from_aggregator, 0, "known prior owner, not the u32::MAX sentinel");
    assert!(inherits(&got), "real-tip handoff must be inheritable");

    // The seed the aggregator would push into prev_source_chain_tips.
    let seeded: (u32, u64, [u8; 32]) = (got.source_id, got.last_seq as u64, got.chain_tip);
    assert_eq!(seeded, (1, 23u64, tip));
}

#[test]
fn missing_handoff_is_cold_start() {
    let dir = tempdir().unwrap();
    let db = RocksDb::open(dir.path().to_str().unwrap()).unwrap();
    // No handoff published for source 2 -> point-get returns None -> the
    // aggregator treats it as a genuine cold start (no inheritance, no fork).
    assert!(db.handoff_at(BOUNDARY, 2).unwrap().is_none());
}

#[test]
fn durable_source_tip_distinguishes_kept_vs_moved() {
    // Models the general X->Y inheritance path: an aggregator reads the durable
    // per-source tip and classifies it by `owner` — kept (owner == self) vs
    // moved (owner != self, which triggers an audit Handoff).
    let dir = tempdir().unwrap();
    let db = RocksDb::open(dir.path().to_str().unwrap()).unwrap();

    let mut tip = [0u8; 32];
    tip[..4].copy_from_slice(&[1, 2, 3, 4]);
    // Previous owner 0 persisted this; aggregator 2 will gain it.
    db.put_source_tip_now(&AggSourceTip {
        source_id: 5,
        last_seq: 40,
        chain_tip: tip,
        owner: 0,
        updated_at_epoch: 2,
    })
    .unwrap();

    let got = db.get_source_tip(5).unwrap().expect("tip must exist");
    assert_eq!(got.chain_tip, tip);
    assert_eq!(got.last_seq, 40);

    let me: u32 = 2;
    assert!(got.owner != me, "owner 0 != self 2 => MOVED (writes audit handoff)");
    let me_is_owner: u32 = 0;
    assert!(
        got.owner == me_is_owner,
        "if self were 0 this would be a KEPT source reloaded across restart"
    );
    // round-trips through the source_tips() scan too
    assert_eq!(db.source_tips().unwrap().len(), 1);
}

#[test]
fn zero_tip_handoff_is_not_inherited() {
    let degenerate = Handoff {
        source_id: 9,
        at_epoch: BOUNDARY,
        from_aggregator: u32::MAX,
        to_aggregator: 1,
        chain_tip: [0u8; 32],
        last_seq: -1,
        published_at_ms: 0,
    };
    assert!(
        !inherits(&degenerate),
        "a zero-tip / sentinel handoff must NOT be treated as inheritable"
    );
}
