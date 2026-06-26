use crate::epoch::EpochType;
use anyhow::{Context, Result};
use rocksdb::{Direction, FlushOptions, IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

/// Key size in bytes (15 bytes = 120 bits)
pub const KEY_BYTES_LEN: usize = 15;
/// Value size in bytes (4 bytes = 32 bits)
pub const VALUE_BYTES_LEN: usize = 4;
/// Timestamp size in bytes (4 bytes = 32 bits)
pub const TS_BYTES_LEN: usize = 4;

const PREFIX_META_NEXT_SOURCE_ID: &[u8] = b"meta:next_source_id";
const PREFIX_SOURCES_BY_ID: &[u8] = b"sources:id:";
const PREFIX_SOURCES_BY_KEY: &[u8] = b"sources:key:";
const PREFIX_EPOCH_FRAMES: &[u8] = b"epoch_frames:";
const PREFIX_CHAIN_CHECKPOINTS: &[u8] = b"chain_checkpoints:";
const PREFIX_SAMPLE_SHARD_FRAMES: &[u8] = b"sample_shard_frames:";
const PREFIX_SERIES_SHARD_FRAMES: &[u8] = b"series_shard_frames:";
const PREFIX_SAMPLE_EVENTS: &[u8] = b"sample_events:";
const PREFIX_AGG_EPOCHS: &[u8] = b"agg_epochs:";
const PREFIX_AGG_CM_STRUCT: &[u8] = b"agg_cm_struct:";
const PREFIX_AGG_HIST_STRUCT: &[u8] = b"agg_hist_struct:";
const PREFIX_VERIFIED_SAMPLES_STRUCT: &[u8] = b"verified_samples_struct:";
const PREFIX_AGG_EPOCH_META: &[u8] = b"agg_epoch_meta:";
const PREFIX_AGG_EPOCH_PROOFS: &[u8] = b"agg_epoch_proofs:";
const PREFIX_EPOCH_TOMBSTONE: &[u8] = b"epoch_tombstone:";

// Epoch batching: EventBatches indexed by (source_id, batch_seq) for batch-level storage
const PREFIX_EVENT_BATCHES: &[u8] = b"event_batches:";
// Epoch-indexed EventBatches: indexed by (epoch_seq) for ZK aggregator
const PREFIX_EPOCH_BATCHES: &[u8] = b"epoch_batches:";
// Per-source state: tracks batch sequence progress per source
const PREFIX_SOURCE_STATE: &[u8] = b"source_state:";
// Global epoch batcher state
const KEY_EPOCH_BATCHER_STATE: &[u8] = b"epoch_batcher_state";

// ---- Online resharding (preview) ----
// OwnershipEpoch rows (one per epoch boundary at which ownership changes).
// Key layout: PREFIX_OWNERSHIP_EPOCH || epoch_seq.to_be_bytes() (i64 BE)
const PREFIX_OWNERSHIP_EPOCH: &[u8] = b"ownership_epoch:";
// Handoff rows (one per (source_id, at_epoch) pair when ownership transitions).
// Key layout: PREFIX_HANDOFF || at_epoch.to_be_bytes() (i64 BE) || source_id.to_be_bytes() (u32 BE)
const PREFIX_HANDOFF: &[u8] = b"handoff:";
const PREFIX_SOURCE_TIP: &[u8] = b"agg_source_tip:";

/// Convert 15-byte key to u64 for database indexing (uses FNV-1a hash of all bytes)
pub fn key_to_u64(key: &[u8; KEY_BYTES_LEN]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
    for &byte in key.iter() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }
    hash
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceRecord {
    pub source_key: String,
    pub chain_tip: [u8; 32],
    pub last_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochFrame {
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub payload: Vec<u8>,
    pub chain_prev: [u8; 32],
    pub chain_hash: [u8; 32],
    pub epoch_type: EpochType,
    pub proof_kind: i16,
    pub num_steps: i32,
    pub proof: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainCheckpoint {
    pub epoch_type: EpochType,
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub chain_hash: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleFrame {
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub payload: Vec<u8>,
    pub chain_prev: [u8; 32],
    pub chain_hash: [u8; 32],
    pub proof_kind: i16,
    pub num_steps: i32,
    pub proof: Vec<u8>,
}

pub type SampleShardFrame = SampleFrame;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeriesFrame {
    pub sequence: i64,
    pub epoch_type: EpochType,
    pub ingest_time_ms: i64,
    pub payload: Vec<u8>,
    pub chain_prev: [u8; 32],
    pub chain_hash: [u8; 32],
    pub proof_kind: i16,
    pub num_steps: i32,
    pub proof: Vec<u8>,
}

pub type SeriesShardFrame = SeriesFrame;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleEvent {
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub idx: i32,
    pub key_id: [u8; KEY_BYTES_LEN],
    pub value: u32,   // 32-bit value
    pub ts: u32,      // 32-bit timestamp (seconds)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggEpoch {
    pub epoch_type: EpochType,
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub result_commit: Vec<u8>,
    /// Aggregator ID for distributed deployments (prevents FDB key collisions)
    #[serde(default)]
    pub aggregator_id: u32,
    /// Minimum timestamp of events in this epoch (seconds)
    #[serde(default)]
    pub min_ts: u32,
    /// Maximum timestamp of events in this epoch (seconds)
    #[serde(default)]
    pub max_ts: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggEpochMeta {
    pub epoch_type: EpochType,
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub n_events: u64,
    /// Aggregator ID for distributed deployments (prevents FDB key collisions)
    #[serde(default)]
    pub aggregator_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggEpochProof {
    pub epoch_type: EpochType,
    pub sequence: i64,
    pub receipt_words: Vec<u32>,
    /// Aggregator ID for distributed deployments (prevents FDB key collisions)
    #[serde(default)]
    pub aggregator_id: u32,
}

/// Explicit "this epoch is fully done" tombstone written atomically alongside
/// the rest of the epoch's rows. Its presence is the authoritative signal
/// that an epoch's WriteBatch was committed; absence means a partial write
/// from a crash that should be cleaned up on startup.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochTombstone {
    pub epoch_type: EpochType,
    pub sequence: i64,
    pub aggregator_id: u32,
    pub completed_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggCmStruct {
    pub sequence: i64,
    pub counts_u32: Vec<u8>,
    pub heap_fixed: Vec<u8>,
    /// Total sum for CM state commit verification
    #[serde(default)]
    pub total_sum: u64,
    /// Hash chain fields for epoch verification
    #[serde(default)]
    pub prev_chain_hash: Vec<u8>,
    #[serde(default)]
    pub events_commit: Vec<u8>,
    #[serde(default)]
    pub out_commit: Vec<u8>,
    #[serde(default)]
    pub final_chain_hash: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggHistStruct {
    pub sequence: i64,
    pub total_count: u64,
    pub total_sum: u64,
    pub table_fixed: Vec<u8>,
    /// Hash chain fields for epoch verification
    #[serde(default)]
    pub prev_chain_hash: Vec<u8>,
    #[serde(default)]
    pub events_commit: Vec<u8>,
    #[serde(default)]
    pub out_commit: Vec<u8>,
    #[serde(default)]
    pub final_chain_hash: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerifiedSamplesStruct {
    pub sequence: i64,
    pub ingest_time_ms: i64,
    pub result_commit: Vec<u8>, // final_chain_hash
    pub out_commit: Vec<u8>,
    pub total_count: u64,
    pub total_sum: u64,
    pub table_fixed: Option<Vec<u8>>,
    /// Hash chain fields for epoch verification
    #[serde(default)]
    pub prev_chain_hash: Vec<u8>,
    #[serde(default)]
    pub events_commit: Vec<u8>,
    /// Aggregator ID for distributed deployments (prevents FDB key collisions)
    #[serde(default)]
    pub aggregator_id: u32,
}

/// Single event within a batch (for storage).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchEvent {
    /// Key identifier (15 bytes) - each event has its own key
    pub key_id: [u8; KEY_BYTES_LEN],
    /// Event value (32 bits)
    pub value: u32,
    /// Event timestamp (32-bit seconds)
    pub ts: u32,
}

/// EventBatch stored for epoch batching (indexed by source_id and batch sequence).
/// Each batch contains multiple events from a single source, with a batch-level hash.
/// batch_hash = SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
/// The chain is per-source: all batches from a source chain together.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredEventBatch {
    /// Batch sequence number for this source (0, 1, 2, ...)
    pub batch_seq: u64,
    /// Source identifier for per-source chain verification and indexing
    pub source_id: u32,
    /// Per-source batch sequence for chain ordering (from producer)
    #[serde(default)]
    pub source_batch_seq: u64,
    /// Ingest timestamp in milliseconds
    pub ingest_time_ms: i64,
    /// Events in this batch
    pub events: Vec<BatchEvent>,
    /// Batch-level chain hash (from producer)
    pub batch_hash: [u8; 32],
}

/// Per-key state for epoch batching (persisted for restart recovery).
/// Tracks batch sequences (not individual event sequences).
/// Per-source batch tracking state for epoch batching.
/// Note: Chain verification happens in ZK guest, not host.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SourceState {
    /// Source ID for restart recovery
    pub source_id: u32,
    /// Last batch_seq that was included in an epoch for this source
    pub last_processed_batch_seq: u64,
    /// Max batch_seq received for this source (for restart recovery)
    pub max_received_batch_seq: u64,
}

/// Global epoch batcher state.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EpochBatcherState {
    /// Next epoch sequence number to assign
    pub next_epoch_seq: i64,
    /// Timestamp of last epoch flush (for timeout-based flushing)
    pub last_flush_time_ms: i64,
    /// Timeout in milliseconds to force epoch flush
    pub epoch_timeout_ms: u64,
}

/// An OwnershipEpoch row records, for a given epoch boundary, the
/// (source_id -> aggregator_id) ownership map that becomes active at that epoch.
///
/// At lookup time the row with the largest `epoch_seq` that is `<= the
/// requested epoch_seq` is the active map. This is the data-plane structure
/// that the (future) controller writes to perform an online reshard at an
/// epoch boundary; the data plane only reads it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnershipEpoch {
    pub epoch_seq: i64,
    /// (source_id -> aggregator_id) for this epoch onward, until a higher-seq OwnershipEpoch overrides.
    pub assignments: Vec<(u32, u32)>,
    pub installed_at_ms: i64,
}

/// A Handoff row records that ownership of a single source_id moved from
/// `from_aggregator` to `to_aggregator` at epoch `at_epoch`. The recipient
/// aggregator inherits the per-source `chain_tip` from this row, which is
/// what makes the SHA-256 chain refuse to extend if the wrong tip is presented.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Handoff {
    pub source_id: u32,
    pub at_epoch: i64,
    pub from_aggregator: u32,
    pub to_aggregator: u32,
    pub chain_tip: [u8; 32],
    pub last_seq: i64,
    pub published_at_ms: i64,
}

/// Durable per-source chain tip for online resharding. Keyed by `source_id`
/// (one current tip per source), it lets an aggregator (a) reload the tips for
/// sources it *keeps* across a restart and (b) — once replicated into the
/// coordination view — inherit the tip for a source it *gains* from a previous
/// owner. `owner` records who last advanced the tip, so a reader can tell a
/// kept source (`owner == self`) from a moved one (`owner != self`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggSourceTip {
    pub source_id: u32,
    pub last_seq: i64,
    pub chain_tip: [u8; 32],
    pub owner: u32,
    pub updated_at_epoch: i64,
}

pub struct RocksDb {
    db: DB,
    is_secondary: bool,
}

impl RocksDb {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path).context("open rocksdb")?;
        Ok(Self {
            db,
            is_secondary: false,
        })
    }

    pub fn open_secondary(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(false);
        let db = DB::open_as_secondary(&options, primary_path.as_ref(), secondary_path.as_ref())
            .context("open rocksdb secondary")?;
        Ok(Self {
            db,
            is_secondary: true,
        })
    }

    pub fn catch_up_if_secondary(&self) -> Result<()> {
        if self.is_secondary {
            let stable = std::env::var("ROCKSDB_CATCHUP_STABLE")
                .ok()
                .as_deref()
                .map(|v| v != "0")
                .unwrap_or(true);
            if !stable {
                self.db
                    .try_catch_up_with_primary()
                    .context("catch up with primary")?;
                return Ok(());
            }

            let stable_rounds: u32 = std::env::var("ROCKSDB_CATCHUP_STABLE_ROUNDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2)
                .max(1);
            let max_iters: u32 = std::env::var("ROCKSDB_CATCHUP_MAX_ITERS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(200)
                .max(1);
            let sleep_ms: u64 = std::env::var("ROCKSDB_CATCHUP_SLEEP_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5);

            let mut last_max: Option<i64> = None;
            let mut same_rounds: u32 = 0;
            for _ in 0..max_iters {
                self.db
                    .try_catch_up_with_primary()
                    .context("catch up with primary")?;
                let max_seq = self.max_source_last_seq().unwrap_or(-1);
                if last_max == Some(max_seq) {
                    same_rounds = same_rounds.saturating_add(1);
                } else {
                    same_rounds = 0;
                    last_max = Some(max_seq);
                }
                // We count "same" transitions. For stable_rounds=2, we need one same transition.
                if same_rounds.saturating_add(1) >= stable_rounds {
                    return Ok(());
                }
                if sleep_ms > 0 {
                    std::thread::sleep(Duration::from_millis(sleep_ms));
                }
            }

            anyhow::bail!(
                "rocksdb secondary catch-up did not stabilize (max_source_last_seq={})",
                last_max.unwrap_or(-1)
            );
        }
        Ok(())
    }

    pub fn max_source_last_seq(&self) -> Result<i64> {
        let mut max_seq: i64 = -1;
        let mode = IteratorMode::From(PREFIX_SOURCES_BY_ID, Direction::Forward);
        for item in self.db.iterator(mode) {
            let (k, v) = item.context("rocksdb iterator")?;
            if !k.starts_with(PREFIX_SOURCES_BY_ID) {
                break;
            }
            let record: SourceRecord = bincode::deserialize(&v).context("decode SourceRecord")?;
            max_seq = max_seq.max(record.last_seq);
        }
        Ok(max_seq)
    }

    pub fn register_source(&self, source_key: &str) -> Result<(u32, [u8; 32], i64)> {
        if let Some(source_id) = self.source_id_by_key(source_key)? {
            let record = self
                .source_by_id(source_id)?
                .context("source id missing for key")?;
            return Ok((source_id, record.chain_tip, record.last_seq));
        }

        let next_id = self.next_source_id()?;
        let record = SourceRecord {
            source_key: source_key.to_string(),
            chain_tip: [0u8; 32],
            last_seq: -1,
        };
        let mut batch = WriteBatch::default();
        batch.put(key_source_id(next_id), bincode::serialize(&record)?);
        batch.put(key_source_key(source_key), next_id.to_be_bytes());
        batch.put(PREFIX_META_NEXT_SOURCE_ID, (next_id + 1).to_be_bytes());
        self.db.write(batch).context("register source write")?;
        Ok((next_id, record.chain_tip, record.last_seq))
    }

    pub fn source_by_id(&self, source_id: u32) -> Result<Option<SourceRecord>> {
        match self
            .db
            .get(key_source_id(source_id))
            .context("get source by id")?
        {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn source_id_by_key(&self, source_key: &str) -> Result<Option<u32>> {
        match self
            .db
            .get(key_source_key(source_key))
            .context("get source by key")?
        {
            Some(bytes) => Ok(Some(u32::from_be_bytes(
                bytes.as_slice().try_into().context("source_id bytes")?,
            ))),
            None => Ok(None),
        }
    }

    pub fn put_source(
        &self,
        batch: &mut WriteBatch,
        source_id: u32,
        record: &SourceRecord,
    ) -> Result<()> {
        batch.put(key_source_id(source_id), bincode::serialize(record)?);
        batch.put(key_source_key(&record.source_key), source_id.to_be_bytes());
        Ok(())
    }

    pub fn put_epoch_frame(&self, batch: &mut WriteBatch, frame: &EpochFrame) -> Result<()> {
        let key = key_epoch_frame(frame.sequence);
        batch.put(key, bincode::serialize(frame)?);
        Ok(())
    }

    pub fn put_sample_shard_frame(
        &self,
        batch: &mut WriteBatch,
        frame: &SampleShardFrame,
    ) -> Result<()> {
        let key = key_sample_shard_frame(frame.sequence);
        batch.put(key, bincode::serialize(frame)?);
        Ok(())
    }

    pub fn put_series_shard_frame(
        &self,
        batch: &mut WriteBatch,
        frame: &SeriesShardFrame,
    ) -> Result<()> {
        let key = key_series_shard_frame(frame.sequence, frame.epoch_type);
        batch.put(key, bincode::serialize(frame)?);
        Ok(())
    }

    pub fn put_chain_checkpoint(
        &self,
        batch: &mut WriteBatch,
        checkpoint: &ChainCheckpoint,
    ) -> Result<()> {
        let key = key_chain_checkpoint(checkpoint.epoch_type, checkpoint.sequence);
        batch.put(key, bincode::serialize(checkpoint)?);
        Ok(())
    }

    pub fn put_sample_event(&self, batch: &mut WriteBatch, event: &SampleEvent) -> Result<()> {
        let key = key_sample_event(event.sequence, event.idx);
        batch.put(key, bincode::serialize(event)?);
        Ok(())
    }

    pub fn delete_sample_events_for_frame(
        &self,
        batch: &mut WriteBatch,
        sequence: i64,
    ) -> Result<()> {
        let prefix = key_sample_event_prefix(sequence);
        for key in self.scan_keys_with_prefix(&prefix)? {
            batch.delete(key);
        }
        Ok(())
    }

    // ============ Epoch Batching Methods ============

    /// Store an EventBatch for epoch batching (indexed by source_id and batch_seq).
    pub fn put_event_batch(&self, batch: &mut WriteBatch, event_batch: &StoredEventBatch) -> Result<()> {
        let key = key_event_batch(event_batch.source_id, event_batch.batch_seq);
        // Verbose logging controlled by ROCKSDB_TRACE env var
        if std::env::var("ROCKSDB_TRACE").map(|v| v == "1").unwrap_or(false) {
            eprintln!(
                "[rocksdb][PUT] event_batch source_id={} batch_seq={} events={}",
                event_batch.source_id, event_batch.batch_seq, event_batch.events.len()
            );
        }
        batch.put(key, bincode::serialize(event_batch)?);
        Ok(())
    }

    /// Get all EventBatches for a specific source, ordered by batch_seq.
    pub fn event_batches_for_source(&self, source_id: u32) -> Result<Vec<StoredEventBatch>> {
        let prefix = key_event_batch_prefix_source(source_id);
        self.scan_values_with_prefix::<StoredEventBatch>(&prefix)
    }

    /// Get EventBatches for a source within a batch sequence range [start_seq, end_seq].
    pub fn event_batches_for_source_range(
        &self,
        source_id: u32,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<Vec<StoredEventBatch>> {
        let prefix = key_event_batch_prefix_source(source_id);
        let mut batches = Vec::new();
        for kv in self.db.prefix_iterator(&prefix) {
            let (k, v) = kv.context("iterate event batches")?;
            // CRITICAL: prefix_iterator does NOT stop at prefix boundary!
            // It continues iterating into other source_ids. We MUST check
            // that the key still starts with our source's prefix.
            if !k.starts_with(&prefix) {
                break; // Moved past our source's keys
            }
            if k.len() >= prefix.len() + 8 {
                let seq_bytes: [u8; 8] = k[prefix.len()..prefix.len() + 8]
                    .try_into()
                    .unwrap_or([0; 8]);
                let seq = u64::from_be_bytes(seq_bytes);
                if seq >= start_seq && seq <= end_seq {
                    let batch: StoredEventBatch = bincode::deserialize(&v)?;
                    batches.push(batch);
                } else if seq > end_seq {
                    break;
                }
            }
        }
        Ok(batches)
    }

    /// Delete EventBatches for a source up to (and including) a batch sequence.
    pub fn delete_event_batches_up_to(
        &self,
        batch: &mut WriteBatch,
        source_id: u32,
        up_to_seq: u64,
    ) -> Result<u64> {
        let prefix = key_event_batch_prefix_source(source_id);
        let mut deleted = 0u64;
        let mut first_deleted_seq: Option<u64> = None;
        let mut last_deleted_seq: u64 = 0;
        for kv in self.db.prefix_iterator(&prefix) {
            let (k, _v) = kv.context("iterate event batches for delete")?;
            // CRITICAL: prefix_iterator does NOT stop at prefix boundary!
            // It continues iterating into other source_ids. We MUST check
            // that the key still starts with our source's prefix.
            if !k.starts_with(&prefix) {
                break; // Moved past our source's keys
            }
            if k.len() >= prefix.len() + 8 {
                let seq_bytes: [u8; 8] = k[prefix.len()..prefix.len() + 8]
                    .try_into()
                    .unwrap_or([0; 8]);
                let seq = u64::from_be_bytes(seq_bytes);
                if seq <= up_to_seq {
                    batch.delete(&k);
                    if first_deleted_seq.is_none() {
                        first_deleted_seq = Some(seq);
                    }
                    last_deleted_seq = seq;
                    deleted += 1;
                } else {
                    break;
                }
            }
        }
        if deleted > 0 {
            eprintln!(
                "[rocksdb][DELETE] event_batches source_id={} up_to_seq={} deleted={} range=[{},{}]",
                source_id, up_to_seq, deleted, first_deleted_seq.unwrap_or(0), last_deleted_seq
            );
        }
        Ok(deleted)
    }

    // ============ Epoch Batches Methods (epoch-indexed storage) ============

    /// Store all EventBatches for an epoch.
    /// Called when epoch is created - moves batches from per-key storage to epoch storage.
    pub fn put_epoch_batches(
        &self,
        batch: &mut WriteBatch,
        epoch_seq: i64,
        batches: &[StoredEventBatch],
    ) -> Result<()> {
        let key = key_epoch_batches(epoch_seq);
        batch.put(key, bincode::serialize(batches)?);
        Ok(())
    }

    /// Get all EventBatches for an epoch.
    /// Used by ZK aggregator to read batches for processing.
    pub fn get_epoch_batches(&self, epoch_seq: i64) -> Result<Option<Vec<StoredEventBatch>>> {
        let key = key_epoch_batches(epoch_seq);
        match self.db.get(&key).context("get epoch batches")? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Check if epoch batches exist for a given sequence.
    pub fn has_epoch_batches(&self, epoch_seq: i64) -> Result<bool> {
        let key = key_epoch_batches(epoch_seq);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Delete all EventBatches for an epoch (after ZK processing).
    pub fn delete_epoch_batches(&self, batch: &mut WriteBatch, epoch_seq: i64) -> Result<()> {
        let key = key_epoch_batches(epoch_seq);
        batch.delete(key);
        Ok(())
    }

    /// Find the next unprocessed epoch sequence.
    /// Scans epoch_batches prefix to find the minimum epoch_seq.
    pub fn next_epoch_seq(&self) -> Result<Option<i64>> {
        let mode = IteratorMode::From(PREFIX_EPOCH_BATCHES, Direction::Forward);
        for kv in self.db.iterator(mode) {
            let (k, _v) = kv.context("iterate epoch batches")?;
            if !k.starts_with(PREFIX_EPOCH_BATCHES) {
                break;
            }
            if k.len() >= PREFIX_EPOCH_BATCHES.len() + 8 {
                let seq_bytes: [u8; 8] = k[PREFIX_EPOCH_BATCHES.len()..PREFIX_EPOCH_BATCHES.len() + 8]
                    .try_into()
                    .unwrap_or([0; 8]);
                return Ok(Some(i64::from_be_bytes(seq_bytes)));
            }
        }
        Ok(None)
    }

    /// Discover the range of available epoch_batches sequences.
    /// Returns (min_seq, max_seq) if any epochs exist, None otherwise.
    pub fn epoch_batches_seq_range(&self) -> Result<Option<(i64, i64)>> {
        let prefix = PREFIX_EPOCH_BATCHES;
        let prefix_len = prefix.len();
        let mut min_seq: Option<i64> = None;
        let mut max_seq: Option<i64> = None;

        let mode = IteratorMode::From(prefix, Direction::Forward);
        for item in self.db.iterator(mode) {
            let (k, _) = item.context("rocksdb iterator")?;
            if !k.starts_with(prefix) {
                break;
            }
            if k.len() >= prefix_len + 8 {
                let seq_bytes: [u8; 8] = k[prefix_len..prefix_len + 8]
                    .try_into()
                    .unwrap_or([0; 8]);
                let seq = i64::from_be_bytes(seq_bytes);
                if min_seq.is_none() {
                    min_seq = Some(seq);
                }
                max_seq = Some(seq);
            }
        }

        match (min_seq, max_seq) {
            (Some(min), Some(max)) => Ok(Some((min, max))),
            _ => Ok(None),
        }
    }

    // ============ Per-Source State Methods ============

    /// Update per-source state.
    pub fn put_source_state(&self, batch: &mut WriteBatch, source_id: u32, state: &SourceState) -> Result<()> {
        batch.put(key_source_state(source_id), bincode::serialize(state)?);
        Ok(())
    }

    /// Get all source states (for scanning all sources).
    pub fn all_source_states(&self) -> Result<Vec<(u32, SourceState)>> {
        let mut result = Vec::new();
        for kv in self.db.prefix_iterator(PREFIX_SOURCE_STATE) {
            let (k, v) = kv.context("iterate source states")?;
            if k.len() >= PREFIX_SOURCE_STATE.len() + 4 {
                let state: SourceState = bincode::deserialize(&v)?;
                result.push((state.source_id, state));
            }
        }
        Ok(result)
    }

    /// Get epoch batcher state.
    pub fn get_epoch_batcher_state(&self) -> Result<Option<EpochBatcherState>> {
        match self.db.get(KEY_EPOCH_BATCHER_STATE).context("get epoch batcher state")? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Update epoch batcher state.
    pub fn put_epoch_batcher_state(&self, batch: &mut WriteBatch, state: &EpochBatcherState) -> Result<()> {
        batch.put(KEY_EPOCH_BATCHER_STATE, bincode::serialize(state)?);
        Ok(())
    }

    // ============ End Epoch Batching Methods ============

    // ============ Online Resharding (preview) ============

    /// Persist an `OwnershipEpoch` row directly (control-plane action — not part
    /// of any data-plane WriteBatch). This is a one-shot write performed by the
    /// reshard controller; the aggregator only ever reads ownership rows.
    pub fn put_ownership_epoch(&self, oe: &OwnershipEpoch) -> Result<()> {
        let key = key_ownership_epoch(oe.epoch_seq);
        self.db
            .put(&key, bincode::serialize(oe)?)
            .context("rocksdb put ownership_epoch")?;
        Ok(())
    }

    /// Return the active `OwnershipEpoch` at `epoch_seq` (i.e. the row with the
    /// highest `epoch_seq` that is `<= requested`). Returns `None` if no
    /// OwnershipEpoch has been installed at or before the requested seq.
    pub fn ownership_epoch_at(&self, epoch_seq: i64) -> Result<Option<OwnershipEpoch>> {
        // The active row is the highest-seq `OwnershipEpoch` with
        // `epoch_seq <= epoch_seq`. Keys are `PREFIX || epoch_seq.to_be_bytes()`;
        // since `epoch_seq >= 0`, big-endian bytes sort ascending, so a single
        // reverse seek (`seek_for_prev`) lands directly on the active row in
        // O(log n) — instead of a forward scan of every row up to the target,
        // which was O(reshard-history) and dominated the per-source-per-epoch
        // owner lookup the aggregator runs under `--use-online-ownership`.
        let seek_key = key_ownership_epoch(epoch_seq);
        let mode = IteratorMode::From(&seek_key, Direction::Reverse);
        if let Some(item) = self.db.iterator(mode).next() {
            let (k, v) = item.context("seek ownership_epoch")?;
            // The reverse seek may land on a key from an earlier prefix when no
            // OwnershipEpoch <= epoch_seq exists; guard with the prefix + length.
            if k.starts_with(PREFIX_OWNERSHIP_EPOCH)
                && k.len() >= PREFIX_OWNERSHIP_EPOCH.len() + 8
            {
                let oe: OwnershipEpoch =
                    bincode::deserialize(&v).context("decode OwnershipEpoch")?;
                return Ok(Some(oe));
            }
        }
        Ok(None)
    }

    /// All `OwnershipEpoch` rows, sorted ascending by `epoch_seq`.
    pub fn ownership_epochs(&self) -> Result<Vec<OwnershipEpoch>> {
        let mut out: Vec<OwnershipEpoch> = self.scan_values::<OwnershipEpoch>(PREFIX_OWNERSHIP_EPOCH)?;
        out.sort_by_key(|oe| oe.epoch_seq);
        Ok(out)
    }

    /// Persist a `Handoff` row directly. Data-plane action: the aggregator
    /// writes one of these when it observes an ownership transition for a
    /// source it owns (incoming or outgoing) at an epoch boundary.
    pub fn put_handoff(&self, h: &Handoff) -> Result<()> {
        let key = key_handoff(h.at_epoch, h.source_id);
        self.db
            .put(&key, bincode::serialize(h)?)
            .context("rocksdb put handoff")?;
        Ok(())
    }

    /// All `Handoff` rows for a given epoch.
    pub fn handoffs_for_epoch(&self, at_epoch: i64) -> Result<Vec<Handoff>> {
        let prefix = key_handoff_prefix(at_epoch);
        self.scan_values_with_prefix::<Handoff>(&prefix)
    }

    /// Point-get the authoritative `Handoff` row for `(at_epoch, source_id)`, if
    /// one was published (by the source's previous owner). The handoff key is
    /// `(at_epoch, source_id)`, so there is at most one row per source per epoch
    /// boundary — the new owner reads it to inherit the per-source chain tip.
    pub fn handoff_at(&self, at_epoch: i64, source_id: u32) -> Result<Option<Handoff>> {
        let key = key_handoff(at_epoch, source_id);
        match self.db.get(&key).context("rocksdb get handoff")? {
            Some(v) => Ok(Some(
                bincode::deserialize(&v).context("decode Handoff")?,
            )),
            None => Ok(None),
        }
    }

    /// All `Handoff` rows, in (at_epoch, source_id) order.
    pub fn handoffs(&self) -> Result<Vec<Handoff>> {
        self.scan_values::<Handoff>(PREFIX_HANDOFF)
    }

    /// Persist a per-source chain tip (into the epoch's atomic `WriteBatch`, so
    /// it commits together with the epoch rows and tombstone).
    pub fn put_source_tip(&self, batch: &mut WriteBatch, t: &AggSourceTip) -> Result<()> {
        let key = key_source_tip(t.source_id);
        batch.put(key, bincode::serialize(t)?);
        Ok(())
    }

    /// Persist a per-source chain tip immediately (its own committed write).
    /// Convenience for tools replicating coordination state.
    pub fn put_source_tip_now(&self, t: &AggSourceTip) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.put_source_tip(&mut batch, t)?;
        self.write_batch(batch)
    }

    /// Read the durable per-source chain tip for `source_id`, if any.
    pub fn get_source_tip(&self, source_id: u32) -> Result<Option<AggSourceTip>> {
        let key = key_source_tip(source_id);
        match self.db.get(&key).context("rocksdb get source_tip")? {
            Some(v) => Ok(Some(
                bincode::deserialize(&v).context("decode AggSourceTip")?,
            )),
            None => Ok(None),
        }
    }

    /// All durable per-source chain tips, in `source_id` order.
    pub fn source_tips(&self) -> Result<Vec<AggSourceTip>> {
        self.scan_values::<AggSourceTip>(PREFIX_SOURCE_TIP)
    }

    // ============ End Online Resharding ============

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch).context("rocksdb write batch")?;
        Ok(())
    }

    pub fn maybe_flush_after_epoch(&self, sequence: i64) -> Result<()> {
        if self.is_secondary {
            return Ok(());
        }
        let flush_every: i64 = 256;
        if (sequence + 1) % flush_every == 0 {
            let mut opts = FlushOptions::default();
            opts.set_wait(true);
            self.db.flush_opt(&opts).context("rocksdb flush")?;
        }
        Ok(())
    }

    /// Explicitly flush all memtables to disk.
    pub fn flush(&self) -> Result<()> {
        if self.is_secondary {
            return Ok(());
        }
        let mut opts = FlushOptions::default();
        opts.set_wait(true);
        self.db.flush_opt(&opts).context("rocksdb flush")?;
        Ok(())
    }

    pub fn epoch_frames(&self) -> Result<Vec<EpochFrame>> {
        self.scan_values::<EpochFrame>(PREFIX_EPOCH_FRAMES)
    }

    pub fn chain_checkpoints(&self) -> Result<Vec<ChainCheckpoint>> {
        self.scan_values::<ChainCheckpoint>(PREFIX_CHAIN_CHECKPOINTS)
    }

    pub fn sample_shard_frames(&self) -> Result<Vec<SampleShardFrame>> {
        self.scan_values::<SampleShardFrame>(PREFIX_SAMPLE_SHARD_FRAMES)
    }

    /// Discover the range of available sample_shard_frame sequences.
    /// Returns (min_seq, max_seq) if any frames exist, None otherwise.
    /// This scans the keys only (not values) for efficiency.
    pub fn sample_shard_frame_seq_range(&self) -> Result<Option<(i64, i64)>> {
        let prefix = PREFIX_SAMPLE_SHARD_FRAMES;
        let prefix_len = prefix.len();
        let mut min_seq: Option<i64> = None;
        let mut max_seq: Option<i64> = None;

        let mode = IteratorMode::From(prefix, Direction::Forward);
        for item in self.db.iterator(mode) {
            let (k, _) = item.context("rocksdb iterator")?;
            if !k.starts_with(prefix) {
                break;
            }
            // Extract sequence from key: prefix + 8 bytes (i64 big-endian)
            if k.len() >= prefix_len + 8 {
                let seq_bytes: [u8; 8] = k[prefix_len..prefix_len + 8]
                    .try_into()
                    .context("seq bytes")?;
                let seq = i64::from_be_bytes(seq_bytes);
                min_seq = Some(min_seq.map_or(seq, |m| m.min(seq)));
                max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            }
        }

        match (min_seq, max_seq) {
            (Some(min), Some(max)) => Ok(Some((min, max))),
            _ => Ok(None),
        }
    }

    /// Discover the range of available sample_event sequences (epochs).
    /// This is used by the aggregator to find epochs written by kafka-consumer.
    /// Returns (min_seq, max_seq) if any events exist, None otherwise.
    pub fn sample_event_seq_range(&self) -> Result<Option<(i64, i64)>> {
        let prefix = PREFIX_SAMPLE_EVENTS;
        let prefix_len = prefix.len();
        let mut min_seq: Option<i64> = None;
        let mut max_seq: Option<i64> = None;

        let mode = IteratorMode::From(prefix, Direction::Forward);
        for item in self.db.iterator(mode) {
            let (k, _) = item.context("rocksdb iterator")?;
            if !k.starts_with(prefix) {
                break;
            }
            // Key format: prefix + sequence (8 bytes) + idx (4 bytes)
            if k.len() >= prefix_len + 8 {
                let seq_bytes: [u8; 8] = k[prefix_len..prefix_len + 8]
                    .try_into()
                    .context("seq bytes")?;
                let seq = i64::from_be_bytes(seq_bytes);
                min_seq = Some(min_seq.map_or(seq, |m| m.min(seq)));
                max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            }
        }

        match (min_seq, max_seq) {
            (Some(min), Some(max)) => Ok(Some((min, max))),
            _ => Ok(None),
        }
    }

    pub fn series_shard_frames(&self) -> Result<Vec<SeriesShardFrame>> {
        self.scan_values::<SeriesShardFrame>(PREFIX_SERIES_SHARD_FRAMES)
    }

    pub fn sample_events(&self) -> Result<Vec<SampleEvent>> {
        self.scan_values::<SampleEvent>(PREFIX_SAMPLE_EVENTS)
    }

    pub fn sample_events_for_frame(&self, sequence: i64) -> Result<Vec<SampleEvent>> {
        let prefix = key_sample_event_prefix(sequence);
        self.scan_values_with_prefix::<SampleEvent>(&prefix)
    }

    pub fn sample_shard_frame(&self, sequence: i64) -> Result<Option<SampleShardFrame>> {
        match self
            .db
            .get(key_sample_shard_frame(sequence))
            .context("get sample_shard_frame")?
        {
            Some(bytes) => Ok(Some(
                bincode::deserialize(&bytes).context("decode SampleShardFrame")?,
            )),
            None => Ok(None),
        }
    }

    pub fn series_shard_frame(
        &self,
        sequence: i64,
        epoch_type: EpochType,
    ) -> Result<Option<SeriesShardFrame>> {
        match self
            .db
            .get(key_series_shard_frame(sequence, epoch_type))
            .context("get series_shard_frame")?
        {
            Some(bytes) => Ok(Some(
                bincode::deserialize(&bytes).context("decode SeriesShardFrame")?,
            )),
            None => Ok(None),
        }
    }

    pub fn agg_epochs(&self) -> Result<Vec<AggEpoch>> {
        self.scan_values::<AggEpoch>(PREFIX_AGG_EPOCHS)
    }

    pub fn agg_epoch_meta(&self) -> Result<Vec<AggEpochMeta>> {
        self.scan_values::<AggEpochMeta>(PREFIX_AGG_EPOCH_META)
    }

    pub fn agg_epoch_proofs(&self) -> Result<Vec<AggEpochProof>> {
        self.scan_values::<AggEpochProof>(PREFIX_AGG_EPOCH_PROOFS)
    }

    pub fn agg_cm_structs(&self) -> Result<Vec<AggCmStruct>> {
        self.scan_values::<AggCmStruct>(PREFIX_AGG_CM_STRUCT)
    }

    pub fn agg_hist_structs(&self) -> Result<Vec<AggHistStruct>> {
        self.scan_values::<AggHistStruct>(PREFIX_AGG_HIST_STRUCT)
    }

    pub fn verified_samples_structs(&self) -> Result<Vec<VerifiedSamplesStruct>> {
        self.scan_values::<VerifiedSamplesStruct>(PREFIX_VERIFIED_SAMPLES_STRUCT)
    }

    pub fn put_agg_epoch(&self, batch: &mut WriteBatch, epoch: &AggEpoch) -> Result<()> {
        let key = key_agg_epoch(epoch.epoch_type, epoch.sequence);
        batch.put(key, bincode::serialize(epoch)?);
        Ok(())
    }

    pub fn put_agg_epoch_meta(&self, batch: &mut WriteBatch, meta: &AggEpochMeta) -> Result<()> {
        let key = key_agg_epoch_meta(meta.epoch_type, meta.sequence);
        batch.put(key, bincode::serialize(meta)?);
        Ok(())
    }

    pub fn put_agg_epoch_proof(
        &self,
        batch: &mut WriteBatch,
        proof: &AggEpochProof,
    ) -> Result<()> {
        let key = key_agg_epoch_proof(proof.epoch_type, proof.sequence);
        batch.put(key, bincode::serialize(proof)?);
        Ok(())
    }

    /// Check if an epoch has been processed (exists in agg_db).
    pub fn has_agg_epoch(&self, epoch_type: EpochType, sequence: i64) -> Result<bool> {
        let key = key_agg_epoch(epoch_type, sequence);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Write an explicit "this epoch is fully done" tombstone. Must be added to
    /// the same WriteBatch as the rest of the epoch's rows so it commits atomically.
    pub fn put_epoch_tombstone(
        &self,
        batch: &mut WriteBatch,
        t: &EpochTombstone,
    ) -> Result<()> {
        let key = key_epoch_tombstone(t.epoch_type, t.sequence, t.aggregator_id);
        batch.put(key, bincode::serialize(t)?);
        Ok(())
    }

    /// Whether a tombstone exists for the given (epoch_type, sequence, aggregator_id).
    pub fn has_epoch_tombstone(
        &self,
        epoch_type: EpochType,
        sequence: i64,
        aggregator_id: u32,
    ) -> Result<bool> {
        let key = key_epoch_tombstone(epoch_type, sequence, aggregator_id);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Highest tombstoned sequence for the given (epoch_type, aggregator_id), if any.
    pub fn max_epoch_tombstone_seq(
        &self,
        epoch_type: EpochType,
        aggregator_id: u32,
    ) -> Result<Option<i64>> {
        let prefix = key_epoch_tombstone_prefix(epoch_type, aggregator_id);
        let mut max_seq: Option<i64> = None;
        let mode = IteratorMode::From(&prefix, Direction::Forward);
        for item in self.db.iterator(mode) {
            let (k, _) = item.context("rocksdb iterator")?;
            if !k.starts_with(&prefix) {
                break;
            }
            if k.len() >= prefix.len() + 8 {
                let seq_bytes: [u8; 8] = k[prefix.len()..prefix.len() + 8]
                    .try_into()
                    .context("tombstone seq bytes")?;
                let seq = i64::from_be_bytes(seq_bytes);
                max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
            }
        }
        Ok(max_seq)
    }

    /// Read all tombstones (across all epoch types and aggregators).
    pub fn epoch_tombstones(&self) -> Result<Vec<EpochTombstone>> {
        self.scan_values::<EpochTombstone>(PREFIX_EPOCH_TOMBSTONE)
    }

    /// Existence check for agg_epoch_meta row (used by recovery).
    pub fn has_agg_epoch_meta(&self, epoch_type: EpochType, sequence: i64) -> Result<bool> {
        let key = key_agg_epoch_meta(epoch_type, sequence);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Existence check for agg_epoch_proof row (used by recovery).
    pub fn has_agg_epoch_proof(&self, epoch_type: EpochType, sequence: i64) -> Result<bool> {
        let key = key_agg_epoch_proof(epoch_type, sequence);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Existence check for agg_cm_struct row at sequence.
    pub fn has_agg_cm_struct(&self, sequence: i64) -> Result<bool> {
        let key = key_agg_cm_struct(sequence);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Existence check for agg_hist_struct row at sequence.
    pub fn has_agg_hist_struct(&self, sequence: i64) -> Result<bool> {
        let key = key_agg_hist_struct(sequence);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Existence check for verified_samples_struct row at sequence.
    pub fn has_verified_samples_struct(&self, sequence: i64) -> Result<bool> {
        let key = key_verified_samples_struct(sequence);
        Ok(self.db.get(&key)?.is_some())
    }

    /// Stage deletion of an agg_epoch row.
    pub fn delete_agg_epoch(
        &self,
        batch: &mut WriteBatch,
        epoch_type: EpochType,
        sequence: i64,
    ) -> Result<()> {
        batch.delete(key_agg_epoch(epoch_type, sequence));
        Ok(())
    }

    /// Stage deletion of an agg_epoch_meta row.
    pub fn delete_agg_epoch_meta(
        &self,
        batch: &mut WriteBatch,
        epoch_type: EpochType,
        sequence: i64,
    ) -> Result<()> {
        batch.delete(key_agg_epoch_meta(epoch_type, sequence));
        Ok(())
    }

    /// Stage deletion of an agg_epoch_proof row.
    pub fn delete_agg_epoch_proof(
        &self,
        batch: &mut WriteBatch,
        epoch_type: EpochType,
        sequence: i64,
    ) -> Result<()> {
        batch.delete(key_agg_epoch_proof(epoch_type, sequence));
        Ok(())
    }

    /// Stage deletion of an agg_cm_struct row.
    pub fn delete_agg_cm_struct(&self, batch: &mut WriteBatch, sequence: i64) -> Result<()> {
        batch.delete(key_agg_cm_struct(sequence));
        Ok(())
    }

    /// Stage deletion of an agg_hist_struct row.
    pub fn delete_agg_hist_struct(&self, batch: &mut WriteBatch, sequence: i64) -> Result<()> {
        batch.delete(key_agg_hist_struct(sequence));
        Ok(())
    }

    /// Stage deletion of a verified_samples_struct row.
    pub fn delete_verified_samples_struct(
        &self,
        batch: &mut WriteBatch,
        sequence: i64,
    ) -> Result<()> {
        batch.delete(key_verified_samples_struct(sequence));
        Ok(())
    }

    pub fn put_agg_cm_struct(&self, batch: &mut WriteBatch, item: &AggCmStruct) -> Result<()> {
        let key = key_agg_cm_struct(item.sequence);
        batch.put(key, bincode::serialize(item)?);
        Ok(())
    }

    pub fn put_agg_hist_struct(&self, batch: &mut WriteBatch, item: &AggHistStruct) -> Result<()> {
        let key = key_agg_hist_struct(item.sequence);
        batch.put(key, bincode::serialize(item)?);
        Ok(())
    }

    pub fn put_verified_samples_struct(
        &self,
        batch: &mut WriteBatch,
        item: &VerifiedSamplesStruct,
    ) -> Result<()> {
        let key = key_verified_samples_struct(item.sequence);
        batch.put(key, bincode::serialize(item)?);
        Ok(())
    }

    fn scan_values<T: for<'a> Deserialize<'a>>(&self, prefix: &[u8]) -> Result<Vec<T>> {
        self.scan_values_with_prefix(prefix)
    }

    fn scan_values_with_prefix<T: for<'a> Deserialize<'a>>(&self, prefix: &[u8]) -> Result<Vec<T>> {
        let mut out = Vec::new();
        for (_, v) in self.scan_kv_with_prefix(prefix)? {
            out.push(bincode::deserialize(&v)?);
        }
        Ok(out)
    }

    fn scan_keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for (k, _) in self.scan_kv_with_prefix(prefix)? {
            keys.push(k);
        }
        Ok(keys)
    }

    fn scan_kv_with_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let mode = IteratorMode::From(prefix, Direction::Forward);
        for item in self.db.iterator(mode) {
            let (k, v) = item.context("rocksdb iterator")?;
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn next_source_id(&self) -> Result<u32> {
        match self.db.get(PREFIX_META_NEXT_SOURCE_ID)? {
            Some(bytes) => Ok(u32::from_be_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .context("next_source_id bytes")?,
            )),
            None => Ok(1),
        }
    }
}

fn key_source_id(source_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SOURCES_BY_ID.len() + 4);
    key.extend_from_slice(PREFIX_SOURCES_BY_ID);
    key.extend_from_slice(&source_id.to_be_bytes());
    key
}

fn key_source_key(source_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SOURCES_BY_KEY.len() + source_key.len());
    key.extend_from_slice(PREFIX_SOURCES_BY_KEY);
    key.extend_from_slice(source_key.as_bytes());
    key
}

fn key_epoch_frame(sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_EPOCH_FRAMES.len() + 8);
    key.extend_from_slice(PREFIX_EPOCH_FRAMES);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_chain_checkpoint(epoch_type: EpochType, sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_CHAIN_CHECKPOINTS.len() + 10);
    key.extend_from_slice(PREFIX_CHAIN_CHECKPOINTS);
    push_epoch_type(&mut key, epoch_type);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_sample_shard_frame(sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SAMPLE_SHARD_FRAMES.len() + 8);
    key.extend_from_slice(PREFIX_SAMPLE_SHARD_FRAMES);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_series_shard_frame(sequence: i64, epoch_type: EpochType) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SERIES_SHARD_FRAMES.len() + 10);
    key.extend_from_slice(PREFIX_SERIES_SHARD_FRAMES);
    key.extend_from_slice(&sequence.to_be_bytes());
    push_epoch_type(&mut key, epoch_type);
    key
}

fn key_sample_event(sequence: i64, idx: i32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SAMPLE_EVENTS.len() + 12);
    key.extend_from_slice(PREFIX_SAMPLE_EVENTS);
    key.extend_from_slice(&sequence.to_be_bytes());
    key.extend_from_slice(&idx.to_be_bytes());
    key
}

fn key_sample_event_prefix(sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SAMPLE_EVENTS.len() + 8);
    key.extend_from_slice(PREFIX_SAMPLE_EVENTS);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_agg_epoch(epoch_type: EpochType, sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_AGG_EPOCHS.len() + 10);
    key.extend_from_slice(PREFIX_AGG_EPOCHS);
    push_epoch_type(&mut key, epoch_type);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_agg_epoch_meta(epoch_type: EpochType, sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_AGG_EPOCH_META.len() + 10);
    key.extend_from_slice(PREFIX_AGG_EPOCH_META);
    push_epoch_type(&mut key, epoch_type);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_agg_epoch_proof(epoch_type: EpochType, sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_AGG_EPOCH_PROOFS.len() + 10);
    key.extend_from_slice(PREFIX_AGG_EPOCH_PROOFS);
    push_epoch_type(&mut key, epoch_type);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Tombstone key: epoch_tombstone:{epoch_type}{sequence:i64-be}{aggregator_id:u32-be}
/// The epoch_type and aggregator_id are placed so that scanning by
/// (epoch_type, aggregator_id) prefix yields all tombstones for that pair,
/// ordered by sequence.
fn key_epoch_tombstone(epoch_type: EpochType, sequence: i64, aggregator_id: u32) -> Vec<u8> {
    let mut key = key_epoch_tombstone_prefix(epoch_type, aggregator_id);
    // NOTE: the ordering is (epoch_type, aggregator_id, sequence) so we scan by
    // pair-prefix; sequence is appended last for forward-scan ordering.
    // We rebuild the key here to put sequence after the prefix.
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Prefix for scanning all tombstones for a given (epoch_type, aggregator_id):
/// epoch_tombstone:{epoch_type}{aggregator_id:u32-be}
fn key_epoch_tombstone_prefix(epoch_type: EpochType, aggregator_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_EPOCH_TOMBSTONE.len() + 1 + 16 + 4);
    key.extend_from_slice(PREFIX_EPOCH_TOMBSTONE);
    push_epoch_type(&mut key, epoch_type);
    key.extend_from_slice(&aggregator_id.to_be_bytes());
    key
}

fn key_agg_cm_struct(sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_AGG_CM_STRUCT.len() + 8);
    key.extend_from_slice(PREFIX_AGG_CM_STRUCT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_agg_hist_struct(sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_AGG_HIST_STRUCT.len() + 8);
    key.extend_from_slice(PREFIX_AGG_HIST_STRUCT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn key_verified_samples_struct(sequence: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_VERIFIED_SAMPLES_STRUCT.len() + 8);
    key.extend_from_slice(PREFIX_VERIFIED_SAMPLES_STRUCT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

// Epoch batching key functions

/// Key for EventBatch: event_batches:{source_id}:{batch_seq}
/// Indexed by source_id (not key_id) for per-source batch tracking.
fn key_event_batch(source_id: u32, batch_seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_EVENT_BATCHES.len() + 12);
    key.extend_from_slice(PREFIX_EVENT_BATCHES);
    key.extend_from_slice(&source_id.to_be_bytes());
    key.extend_from_slice(&batch_seq.to_be_bytes());
    key
}

/// Prefix for scanning all EventBatches for a source: event_batches:{source_id}:
fn key_event_batch_prefix_source(source_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_EVENT_BATCHES.len() + 4);
    key.extend_from_slice(PREFIX_EVENT_BATCHES);
    key.extend_from_slice(&source_id.to_be_bytes());
    key
}

/// Key for per-source state: source_state:{source_id}
fn key_source_state(source_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SOURCE_STATE.len() + 4);
    key.extend_from_slice(PREFIX_SOURCE_STATE);
    key.extend_from_slice(&source_id.to_be_bytes());
    key
}

/// Key for epoch batches: epoch_batches:{epoch_seq}
/// Stores Vec<StoredEventBatch> - all batches belonging to this epoch
fn key_epoch_batches(epoch_seq: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_EPOCH_BATCHES.len() + 8);
    key.extend_from_slice(PREFIX_EPOCH_BATCHES);
    key.extend_from_slice(&epoch_seq.to_be_bytes());
    key
}

fn push_epoch_type(key: &mut Vec<u8>, epoch_type: EpochType) {
    let s = epoch_type.as_str();
    let len = u8::try_from(s.len()).unwrap_or(0);
    key.push(len);
    key.extend_from_slice(s.as_bytes());
}

/// Key for OwnershipEpoch rows: ownership_epoch:{epoch_seq i64 BE}.
fn key_ownership_epoch(epoch_seq: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_OWNERSHIP_EPOCH.len() + 8);
    key.extend_from_slice(PREFIX_OWNERSHIP_EPOCH);
    key.extend_from_slice(&epoch_seq.to_be_bytes());
    key
}

/// Key for Handoff rows: handoff:{at_epoch i64 BE}{source_id u32 BE}.
fn key_source_tip(source_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_SOURCE_TIP.len() + 4);
    key.extend_from_slice(PREFIX_SOURCE_TIP);
    key.extend_from_slice(&source_id.to_be_bytes());
    key
}

fn key_handoff(at_epoch: i64, source_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_HANDOFF.len() + 12);
    key.extend_from_slice(PREFIX_HANDOFF);
    key.extend_from_slice(&at_epoch.to_be_bytes());
    key.extend_from_slice(&source_id.to_be_bytes());
    key
}

/// Prefix for scanning all Handoff rows for a single epoch.
fn key_handoff_prefix(at_epoch: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_HANDOFF.len() + 8);
    key.extend_from_slice(PREFIX_HANDOFF);
    key.extend_from_slice(&at_epoch.to_be_bytes());
    key
}

/// Look up the aggregator that owns `source_id` at `epoch_seq`.
///
/// This is a pure read function exposed as the API a future controller-driven
/// rebalancer would call. It walks the OwnershipEpoch rows and returns the
/// aggregator assigned to `source_id` in the active map for `epoch_seq`.
///
/// Falls back to `default_aggregator` when (a) no OwnershipEpoch has been
/// installed at or before `epoch_seq`, or (b) `source_id` is not present in
/// the active map (i.e. the controller did not name it explicitly).
pub fn current_owner_for_source(
    db: &RocksDb,
    source_id: u32,
    epoch_seq: i64,
    default_aggregator: u32,
) -> Result<u32> {
    match db.ownership_epoch_at(epoch_seq)? {
        Some(oe) => {
            for (sid, agg) in oe.assignments.iter() {
                if *sid == source_id {
                    return Ok(*agg);
                }
            }
            Ok(default_aggregator)
        }
        None => Ok(default_aggregator),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_tmp_dir(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let pid = std::process::id();
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("zktel_rdb_{}_{}_{}_{}", tag, pid, nanos, n))
    }

    #[test]
    fn ownership_epoch_at_reverse_seek() {
        let dir = unique_tmp_dir("ownership_revseek");
        let db = RocksDb::open(&dir).expect("open rocksdb");

        // Empty store -> no active row at any epoch.
        assert!(db.ownership_epoch_at(0).unwrap().is_none());
        assert!(db.ownership_epoch_at(1_000).unwrap().is_none());

        // Install several reshards across the timeline.
        for s in [5i64, 10, 25, 40, 100] {
            db.put_ownership_epoch(&OwnershipEpoch {
                epoch_seq: s,
                assignments: vec![(0, (s % 7) as u32)],
                installed_at_ms: s,
            })
            .expect("put oe");
        }

        // Before the first row -> still none (reverse seek must land before the
        // ownership prefix and return None, not the lowest row).
        assert!(db.ownership_epoch_at(0).unwrap().is_none());
        assert!(db.ownership_epoch_at(4).unwrap().is_none());

        // Exact hits.
        assert_eq!(db.ownership_epoch_at(5).unwrap().unwrap().epoch_seq, 5);
        assert_eq!(db.ownership_epoch_at(40).unwrap().unwrap().epoch_seq, 40);
        assert_eq!(db.ownership_epoch_at(100).unwrap().unwrap().epoch_seq, 100);

        // Between rows -> the most recent row at or before the query.
        assert_eq!(db.ownership_epoch_at(9).unwrap().unwrap().epoch_seq, 5);
        assert_eq!(db.ownership_epoch_at(24).unwrap().unwrap().epoch_seq, 10);
        assert_eq!(db.ownership_epoch_at(39).unwrap().unwrap().epoch_seq, 25);
        assert_eq!(db.ownership_epoch_at(99).unwrap().unwrap().epoch_seq, 40);

        // Past the last row -> the last row stays active.
        assert_eq!(db.ownership_epoch_at(10_000).unwrap().unwrap().epoch_seq, 100);

        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ownership_lookup_basic() {
        let dir = unique_tmp_dir("ownership_lookup");
        let db = RocksDb::open(&dir).expect("open rocksdb");

        // No OwnershipEpoch installed yet -> returns default.
        let owner = current_owner_for_source(&db, 7, 0, 99).expect("default");
        assert_eq!(owner, 99);

        // Install OE at epoch 10: source 7 -> aggregator 1, source 8 -> aggregator 2.
        let oe10 = OwnershipEpoch {
            epoch_seq: 10,
            assignments: vec![(7, 1), (8, 2)],
            installed_at_ms: 1_000,
        };
        db.put_ownership_epoch(&oe10).expect("put oe10");

        // Before epoch 10 the OE is not yet active -> default.
        assert_eq!(current_owner_for_source(&db, 7, 9, 99).unwrap(), 99);
        // At/after epoch 10 the explicit assignments win.
        assert_eq!(current_owner_for_source(&db, 7, 10, 99).unwrap(), 1);
        assert_eq!(current_owner_for_source(&db, 8, 11, 99).unwrap(), 2);
        // Unnamed source falls back to default.
        assert_eq!(current_owner_for_source(&db, 42, 11, 99).unwrap(), 99);

        // Install OE at epoch 20: move source 7 to aggregator 3.
        let oe20 = OwnershipEpoch {
            epoch_seq: 20,
            assignments: vec![(7, 3), (8, 2)],
            installed_at_ms: 2_000,
        };
        db.put_ownership_epoch(&oe20).expect("put oe20");

        // Between [10,19] the older map still applies.
        assert_eq!(current_owner_for_source(&db, 7, 19, 99).unwrap(), 1);
        // From 20 onward the new map applies.
        assert_eq!(current_owner_for_source(&db, 7, 20, 99).unwrap(), 3);
        assert_eq!(current_owner_for_source(&db, 7, 100, 99).unwrap(), 3);

        // ownership_epochs() returns rows sorted ascending.
        let all = db.ownership_epochs().expect("list");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].epoch_seq, 10);
        assert_eq!(all[1].epoch_seq, 20);

        // Handoff round-trip.
        let h = Handoff {
            source_id: 7,
            at_epoch: 20,
            from_aggregator: 1,
            to_aggregator: 3,
            chain_tip: [1u8; 32],
            last_seq: 42,
            published_at_ms: 1_500,
        };
        db.put_handoff(&h).expect("put_handoff");
        let fetched = db.handoffs_for_epoch(20).expect("read handoffs");
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].source_id, 7);
        assert_eq!(fetched[0].to_aggregator, 3);
        assert_eq!(fetched[0].chain_tip, [1u8; 32]);

        // Drop the DB before removing the directory so RocksDB releases its lock.
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Sharded RocksDB wrapper for reading from multiple aggregator shards.
///
/// Each shard is a separate RocksDB instance, typically one per aggregator.
/// The querier uses this to read and merge results from all shards.
///
/// # Environment Variables
/// - `AGG_ROCKSDB_SHARDS`: Comma-separated list of shard paths
/// - `AGG_ROCKSDB_PATH`: Single path (backward compatible, creates 1 shard)
///
/// # Example
/// ```bash
/// AGG_ROCKSDB_SHARDS=/data/agg0,/data/agg1,/data/agg2 cargo run --bin querier
/// ```
pub struct ShardedRocksDb {
    shards: Vec<RocksDb>,
}

impl ShardedRocksDb {
    /// Open multiple shards from a comma-separated list of paths.
    pub fn open_shards(paths: &[impl AsRef<Path>]) -> Result<Self> {
        anyhow::ensure!(!paths.is_empty(), "at least one shard path required");
        let mut shards = Vec::with_capacity(paths.len());
        for (i, path) in paths.iter().enumerate() {
            let db = RocksDb::open(path.as_ref())
                .with_context(|| format!("open shard {} at {:?}", i, path.as_ref()))?;
            shards.push(db);
        }
        eprintln!("[sharded-rocksdb] opened {} shards", shards.len());
        Ok(Self { shards })
    }

    /// Open multiple shards in secondary (read-only) mode.
    pub fn open_shards_secondary(
        primary_paths: &[impl AsRef<Path>],
        secondary_paths: &[impl AsRef<Path>],
    ) -> Result<Self> {
        anyhow::ensure!(!primary_paths.is_empty(), "at least one shard path required");
        anyhow::ensure!(
            primary_paths.len() == secondary_paths.len(),
            "primary and secondary path counts must match"
        );
        let mut shards = Vec::with_capacity(primary_paths.len());
        for (i, (primary, secondary)) in primary_paths.iter().zip(secondary_paths.iter()).enumerate()
        {
            let db = RocksDb::open_secondary(primary.as_ref(), secondary.as_ref())
                .with_context(|| format!("open secondary shard {} at {:?}", i, primary.as_ref()))?;
            shards.push(db);
        }
        eprintln!("[sharded-rocksdb] opened {} secondary shards", shards.len());
        Ok(Self { shards })
    }

    /// Parse shard paths from environment variable.
    /// Returns paths from `AGG_ROCKSDB_SHARDS` (comma-separated) or falls back to `AGG_ROCKSDB_PATH`.
    pub fn paths_from_env() -> Result<Vec<String>> {
        if let Ok(shards) = std::env::var("AGG_ROCKSDB_SHARDS") {
            let paths: Vec<String> = shards
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !paths.is_empty() {
                return Ok(paths);
            }
        }
        // Fallback to single path
        let path = std::env::var("AGG_ROCKSDB_PATH")
            .or_else(|_| std::env::var("ROCKSDB_PATH"))
            .unwrap_or_else(|_| "/mydata/rocksdb".to_string());
        Ok(vec![path])
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Catch up all secondary shards with their primaries.
    pub fn catch_up_if_secondary(&self) -> Result<()> {
        for (i, shard) in self.shards.iter().enumerate() {
            shard
                .catch_up_if_secondary()
                .with_context(|| format!("catch up shard {}", i))?;
        }
        Ok(())
    }

    /// Get aggregation epochs from all shards, merged and deduplicated by (epoch_type, sequence).
    pub fn agg_epochs(&self) -> Result<Vec<AggEpoch>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.agg_epochs()?);
        }
        // Deduplicate by (epoch_type, sequence) - keep first occurrence
        let mut seen = std::collections::HashSet::new();
        all.retain(|e| seen.insert((e.epoch_type, e.sequence)));
        // Sort by sequence for consistent ordering
        all.sort_by_key(|e| (e.epoch_type, e.sequence));
        Ok(all)
    }

    /// Get aggregation epoch metadata from all shards.
    pub fn agg_epoch_meta(&self) -> Result<Vec<AggEpochMeta>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.agg_epoch_meta()?);
        }
        let mut seen = std::collections::HashSet::new();
        all.retain(|e| seen.insert((e.epoch_type, e.sequence)));
        all.sort_by_key(|e| (e.epoch_type, e.sequence));
        Ok(all)
    }

    /// Get Count-Min structs from all shards.
    /// Note: CM sketches from different shards should be merged (counts summed).
    pub fn agg_cm_structs(&self) -> Result<Vec<AggCmStruct>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.agg_cm_structs()?);
        }
        all.sort_by_key(|e| e.sequence);
        Ok(all)
    }

    /// Get histogram structs from all shards.
    /// Note: Histograms from different shards should be merged (counts summed).
    pub fn agg_hist_structs(&self) -> Result<Vec<AggHistStruct>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.agg_hist_structs()?);
        }
        all.sort_by_key(|e| e.sequence);
        Ok(all)
    }

    /// Get verified samples structs from all shards.
    pub fn verified_samples_structs(&self) -> Result<Vec<VerifiedSamplesStruct>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.verified_samples_structs()?);
        }
        all.sort_by_key(|e| e.sequence);
        Ok(all)
    }

    /// Get aggregation epoch proofs from all shards.
    pub fn agg_epoch_proofs(&self) -> Result<Vec<AggEpochProof>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.agg_epoch_proofs()?);
        }
        let mut seen = std::collections::HashSet::new();
        all.retain(|e| seen.insert((e.epoch_type, e.sequence)));
        all.sort_by_key(|e| (e.epoch_type, e.sequence));
        Ok(all)
    }

    /// Get sample shard frames from all shards.
    pub fn sample_shard_frames(&self) -> Result<Vec<SampleShardFrame>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.sample_shard_frames()?);
        }
        all.sort_by_key(|e| e.sequence);
        Ok(all)
    }

    /// Get series shard frames from all shards.
    pub fn series_shard_frames(&self) -> Result<Vec<SeriesShardFrame>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.series_shard_frames()?);
        }
        all.sort_by_key(|f| (f.epoch_type, f.sequence));
        Ok(all)
    }

    /// Get sample events from all shards.
    pub fn sample_events(&self) -> Result<Vec<SampleEvent>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.sample_events()?);
        }
        all.sort_by_key(|e| (e.sequence, e.idx));
        Ok(all)
    }

    /// Get sample events for a specific sequence from all DB shards.
    pub fn sample_events_for_frame(&self, sequence: i64) -> Result<Vec<SampleEvent>> {
        let mut all = Vec::new();
        for shard in &self.shards {
            all.extend(shard.sample_events_for_frame(sequence)?);
        }
        all.sort_by_key(|e| e.idx);
        Ok(all)
    }
}
