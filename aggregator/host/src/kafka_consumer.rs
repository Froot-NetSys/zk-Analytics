//! Kafka consumer for ingesting EventBatches into RocksDB with epoch batching.
//!
//! EventBatches are received from data sources, each containing multiple events
//! for a single key with a batch-level chain hash.
//!
//! Epochs are created when:
//! - **Threshold reached**: When `EPOCH_BATCH_THRESHOLD` batches are received.
//! - **Timeout**: When `EPOCH_TIMEOUT_MS` elapses with pending batches.
//!
//! Each epoch contains at most `EPOCH_BATCH_THRESHOLD` batches (limit always applies).
//!
//! # Usage
//! ```bash
//! KAFKA_BROKERS=kafka:9092 \
//! KAFKA_TOPIC=raw_events \
//! KAFKA_GROUP_ID=aggregators \
//! RAW_DB_PATH=/data/raw \
//! EPOCH_TIMEOUT_MS=60000 \
//! EPOCH_BATCH_THRESHOLD=16 \
//! cargo run --bin kafka-consumer
//! ```

use anyhow::{Context, Result};
use rayon::prelude::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use zktelemetry_common::epoch::EpochType;
use zktelemetry_common::rocksdb_store::{
    BatchEvent, EpochBatcherState, SourceState, StoredEventBatch, RocksDb, KEY_BYTES_LEN,
};

/// Default epoch timeout in milliseconds (30 seconds)
pub const DEFAULT_EPOCH_TIMEOUT_MS: u64 = 30_000;

/// Default epoch batch threshold (total batches received to trigger epoch)
pub const DEFAULT_EPOCH_BATCH_THRESHOLD: u64 = 16;

/// Cleanup check interval in milliseconds (5 seconds)
pub const CLEANUP_CHECK_INTERVAL_MS: u64 = 5_000;

/// Signals for controlling the kafka consumer from external sources.
#[derive(Debug)]
pub struct ConsumerSignals {
    /// When true, triggers graceful shutdown with final epoch flush
    pub shutdown: AtomicBool,
    /// When true, forces an epoch flush with all pending batches (without shutdown)
    pub force_flush: AtomicBool,
}

impl ConsumerSignals {
    pub fn new() -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            force_flush: AtomicBool::new(false),
        }
    }
}

impl Default for ConsumerSignals {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw event from Kafka (matches data source format).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawEvent {
    pub key_id: [u8; KEY_BYTES_LEN],
    #[serde(default)]  // For backward compatibility with old messages
    pub seq: u64,      // Deprecated: kept for deserialization only
    pub value: u32,    // 32-bit value
    pub ts: u32,       // 32-bit timestamp (seconds)
}

/// Batch of events from a single Kafka message.
/// Each batch has a single batch_hash that covers all events.
/// batch_hash = SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
/// where events_commit = SHA256(key_id || value || ts for each event)
/// The chain is per-source (all batches from a source chain together).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventBatch {
    pub ingest_time_ms: i64,
    #[serde(default)]
    pub source_id: u32,  // Source identifier for per-source chain verification
    #[serde(default)]
    pub source_batch_seq: u64,  // Per-source batch sequence for chain ordering
    pub events: Vec<RawEvent>,
    pub batch_hash: [u8; 32],  // chain commitment for this batch
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub struct KafkaConsumerConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: String,
    pub epoch_timeout_ms: u64,
    /// Total batches received threshold to trigger epoch creation
    pub epoch_batch_threshold: u64,
    /// Path to agg_db for checking processed epochs (optional, for cleanup)
    pub agg_db_path: Option<String>,
    /// Path for agg_db secondary instance
    pub agg_db_secondary_path: Option<String>,
    /// Specific partition to consume from (for deterministic key→aggregator mapping).
    /// If set, uses manual partition assignment instead of consumer group auto-assignment.
    /// This ensures partition N goes to aggregator N (when KAFKA_PARTITIONS = NUM_AGGREGATORS).
    pub partition_id: Option<i32>,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            brokers: std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            topic: std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "raw_events".into()),
            group_id: std::env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "aggregators".into()),
            auto_offset_reset: std::env::var("KAFKA_AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| "earliest".into()),
            epoch_timeout_ms: std::env::var("EPOCH_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_EPOCH_TIMEOUT_MS),
            epoch_batch_threshold: std::env::var("EPOCH_BATCH_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_EPOCH_BATCH_THRESHOLD),
            agg_db_path: std::env::var("AGG_ROCKSDB_PATH").ok().filter(|s| !s.is_empty()),
            agg_db_secondary_path: std::env::var("AGG_ROCKSDB_SECONDARY_PATH").ok().filter(|s| !s.is_empty()),
            // Manual partition assignment for deterministic key→aggregator mapping
            // Set KAFKA_PARTITION_ID=N to consume only from partition N
            partition_id: std::env::var("KAFKA_PARTITION_ID")
                .ok()
                .and_then(|v| v.parse().ok()),
        }
    }
}

/// Create a Kafka consumer with the given configuration.
///
/// If `partition_id` is set, uses manual partition assignment (for deterministic key→aggregator mapping).
/// Otherwise, uses consumer group auto-assignment.
pub fn create_consumer(config: &KafkaConsumerConfig) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false") // Manual commit after RocksDB write
        .set("auto.offset.reset", &config.auto_offset_reset)
        .set("session.timeout.ms", "30000")
        .set("heartbeat.interval.ms", "10000")
        .create()
        .context("failed to create Kafka consumer")?;

    if let Some(partition_id) = config.partition_id {
        // Manual partition assignment for deterministic key→aggregator mapping
        // This ensures aggregator N consumes only from partition N
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&config.topic, partition_id);
        consumer
            .assign(&tpl)
            .context("failed to assign partition")?;
        eprintln!(
            "[kafka-consumer] manually assigned to partition {} (deterministic mode)",
            partition_id
        );
    } else {
        // Consumer group auto-assignment (legacy mode)
        consumer
            .subscribe(&[&config.topic])
            .context("failed to subscribe to topic")?;
    }

    Ok(consumer)
}

/// Per-source batch sequence tracking for epoch readiness detection.
#[derive(Clone, Debug)]
struct SourceBatchState {
    /// Highest batch_seq received for this source (= source_batch_seq from producer)
    max_received_batch_seq: u64,
    /// Lowest batch_seq received for this source (for debugging out-of-order delivery)
    min_received_batch_seq: u64,
    /// Where the next epoch should start from (last_processed_batch_seq + 1)
    /// Initialized to 0 since producer's source_batch_seq starts at 0
    next_epoch_start_batch_seq: u64,
}

impl Default for SourceBatchState {
    fn default() -> Self {
        Self {
            max_received_batch_seq: 0,
            min_received_batch_seq: u64::MAX,  // Will be set on first batch
            // Start at 0 to match producer's source_batch_seq which starts at 0
            next_epoch_start_batch_seq: 0,
        }
    }
}

/// Epoch batcher state (in-memory, synced with RocksDB).
struct EpochBatcher {
    /// Per-source batch sequence tracking (in-memory cache)
    source_batch_states: HashMap<u32, SourceBatchState>,
    /// Next epoch sequence number (assigned by aggregator)
    next_epoch_seq: i64,
    /// Last epoch flush time
    last_flush_time: Instant,
    epoch_timeout_ms: u64,
    /// Total batches received threshold to trigger epoch creation
    epoch_batch_threshold: u64,
    /// Total batches received since last epoch
    total_batches_received: u64,
}

impl EpochBatcher {
    fn new(epoch_timeout_ms: u64, epoch_batch_threshold: u64) -> Self {
        Self {
            source_batch_states: HashMap::new(),
            next_epoch_seq: 0,
            last_flush_time: Instant::now(),
            epoch_timeout_ms,
            epoch_batch_threshold,
            total_batches_received: 0,
        }
    }

    /// Load state from RocksDB (for restart recovery).
    fn load_from_db(&mut self, raw_db: &RocksDb) -> Result<()> {
        // Load epoch batcher state
        if let Some(state) = raw_db.get_epoch_batcher_state()? {
            self.next_epoch_seq = state.next_epoch_seq;
        }

        // Load per-source states and count existing pending batches
        let mut pending_count: u64 = 0;
        for (source_id, state) in raw_db.all_source_states()? {
            let next_start = state.last_processed_batch_seq + 1;
            // Count pending batches for this source
            if state.max_received_batch_seq >= next_start {
                pending_count += state.max_received_batch_seq - next_start + 1;
            }
            self.source_batch_states.insert(source_id, SourceBatchState {
                max_received_batch_seq: state.max_received_batch_seq,
                min_received_batch_seq: 0,  // Unknown from persisted state, set to 0
                next_epoch_start_batch_seq: next_start,
            });
        }

        // Initialize total_batches_received to existing pending count
        // This ensures epoch threshold accounts for pre-existing batches
        self.total_batches_received = pending_count;
        if pending_count > 0 {
            eprintln!(
                "[kafka-consumer] loaded {} pending batches from {} sources",
                pending_count,
                self.source_batch_states.len()
            );
        }

        Ok(())
    }

    /// Update max received batch sequence for a source and increment total batches counter.
    fn update_received_batch_seq(&mut self, source_id: u32, batch_seq: u64) {
        let state = self.source_batch_states.entry(source_id).or_default();

        // Detect producer restart: if we receive a batch_seq lower than expected,
        // the producer likely restarted and reset its sequence counter to 0.
        if batch_seq < state.next_epoch_start_batch_seq && state.next_epoch_start_batch_seq > 0 {
            eprintln!(
                "[kafka-consumer][PRODUCER_RESTART] source_id={}: received batch_seq={} but expected >= {}. \
                Producer likely restarted and reset sequence counter.",
                source_id, batch_seq, state.next_epoch_start_batch_seq
            );
        }

        // Track min_received for debugging out-of-order delivery
        if batch_seq < state.min_received_batch_seq {
            state.min_received_batch_seq = batch_seq;
        }

        if batch_seq > state.max_received_batch_seq {
            // Detect gaps in received sequences (batches arriving out of order or with missing seqs)
            let expected_next = state.max_received_batch_seq + 1;
            if batch_seq > expected_next && state.max_received_batch_seq > 0 {
                eprintln!(
                    "[kafka-consumer][SEQUENCE_GAP] source_id={}: received batch_seq={} but expected {}. \
                    Gap of {} batches (may arrive later, out-of-order).",
                    source_id, batch_seq, expected_next, batch_seq - expected_next
                );
            }
            state.max_received_batch_seq = batch_seq;
            // Increment total batches received counter for threshold check
            self.total_batches_received += 1;
        }
    }

    /// Check if we should create a new epoch.
    /// Epoch is created when:
    /// - Total batches received reaches epoch_batch_threshold, OR
    /// - Timeout (epoch_timeout_ms) elapses with pending batches
    fn should_create_epoch(&self) -> bool {
        // Need at least one source to create an epoch
        if self.source_batch_states.is_empty() {
            return false;
        }

        // Check if total batches received reaches threshold
        if self.total_batches_received >= self.epoch_batch_threshold {
            eprintln!(
                "[kafka-consumer][debug] should_create_epoch: total_batches_received={} >= epoch_batch_threshold={}",
                self.total_batches_received,
                self.epoch_batch_threshold
            );
            return true;
        }

        // Check timeout (if there are any pending batches)
        let has_pending = self.source_batch_states.values().any(|s| s.max_received_batch_seq >= s.next_epoch_start_batch_seq);
        if has_pending && self.last_flush_time.elapsed().as_millis() >= self.epoch_timeout_ms as u128 {
            return true;
        }

        false
    }

    /// Mark epoch as processed for a source.
    fn mark_epoch_processed(&mut self, source_id: u32, last_processed_batch_seq: u64) {
        if let Some(state) = self.source_batch_states.get_mut(&source_id) {
            state.next_epoch_start_batch_seq = last_processed_batch_seq + 1;
        }
    }

    /// Check if there are any pending batches across all sources.
    fn has_pending_batches(&self) -> bool {
        self.source_batch_states.values().any(|s| s.max_received_batch_seq >= s.next_epoch_start_batch_seq)
    }

    /// Get count of sources with pending batches.
    fn pending_sources_count(&self) -> usize {
        self.source_batch_states.values().filter(|s| s.max_received_batch_seq >= s.next_epoch_start_batch_seq).count()
    }
}

/// Process a single Kafka message: store entire EventBatch with batch sequence tracking.
/// Each EventBatch contains events for a single source with a batch-level chain hash.
/// Result of processing a single message: contains batch info for deferred state updates.
/// The (source_id, batch_seq) is returned so that max_received can be updated AFTER
/// the WriteBatch is committed to RocksDB (fixing race condition).
struct ProcessedBatchInfo {
    source_id: u32,
    batch_seq: u64,
    event_count: usize,
}

fn process_message_for_batching(
    raw_db: &RocksDb,
    wb: &mut WriteBatch,
    payload: &[u8],
) -> Result<Option<ProcessedBatchInfo>> {
    let batch: EventBatch = serde_json::from_slice(payload).context("deserialize event batch")?;
    let event_count = batch.events.len();

    if batch.events.is_empty() {
        return Ok(None);
    }

    let source_id = batch.source_id;
    let source_batch_seq = batch.source_batch_seq;

    // Convert events to BatchEvent format for storage
    let stored_events: Vec<BatchEvent> = batch.events.iter().map(|e| BatchEvent {
        key_id: e.key_id,
        value: e.value,
        ts: e.ts,
    }).collect();

    // Store the entire EventBatch indexed by (source_id, source_batch_seq)
    // Using producer's source_batch_seq ensures correct chain ordering.
    // If producer restarts and sends duplicate seq, it overwrites the old batch.
    let stored_batch = StoredEventBatch {
        batch_seq: source_batch_seq,  // Use producer's sequence for indexing
        source_id,
        source_batch_seq,
        ingest_time_ms: batch.ingest_time_ms,
        events: stored_events.clone(),
        batch_hash: batch.batch_hash,
    };

    // Debug: log received batch hash for chain verification debugging (enable with VERBOSE_HASH_LOGGING=1)
    if std::env::var("VERBOSE_HASH_LOGGING").map(|v| v == "1").unwrap_or(false) {
        eprintln!(
            "[CONSUMER_HASH] source_id={} source_batch_seq={} batch_hash={:?} num_events={}",
            source_id, source_batch_seq, batch.batch_hash, event_count
        );
        // Log first few events for debugging
        for (i, ev) in stored_events.iter().take(3).enumerate() {
            eprintln!(
                "[CONSUMER_HASH] source_id={} seq={} event[{}]: key_id={:?} value={} ts={}",
                source_id, source_batch_seq, i, ev.key_id, ev.value, ev.ts
            );
        }
        if stored_events.len() > 3 {
            eprintln!(
                "[CONSUMER_HASH] source_id={} seq={} ... and {} more events",
                source_id, source_batch_seq, stored_events.len() - 3
            );
        }
    }

    raw_db.put_event_batch(wb, &stored_batch)?;

    // Return batch info for deferred state update (after WriteBatch commit)
    // DO NOT update batcher.max_received here - that causes a race condition
    // where max_received > what's actually in RocksDB
    Ok(Some(ProcessedBatchInfo {
        source_id,
        batch_seq: source_batch_seq,
        event_count,
    }))
}

/// Result of reading batches for a single source (used for parallel processing).
struct SourceBatchReadResult {
    source_id: u32,
    batches: Vec<StoredEventBatch>,
    last_processed_batch_seq: u64,
    max_received_batch_seq: u64,
}

/// Create an epoch from pending batches.
/// Triggered when total batches reaches EPOCH_BATCH_THRESHOLD or timeout with pending batches.
///
/// Each epoch contains at most `epoch_batch_threshold` batches:
/// - **Threshold reached**: Exactly `epoch_batch_threshold` batches.
/// - **Timeout**: Up to `epoch_batch_threshold` batches (may be fewer if less available).
///
/// Uses parallel reads from RocksDB for improved performance.
fn create_epoch(
    raw_db: &RocksDb,
    wb: &mut WriteBatch,
    batcher: &mut EpochBatcher,
    force: bool,
) -> Result<Option<i64>> {
    let is_timeout = batcher.last_flush_time.elapsed().as_millis() >= batcher.epoch_timeout_ms as u128;
    let threshold_reached = batcher.total_batches_received >= batcher.epoch_batch_threshold;
    let has_pending = batcher.source_batch_states.values().any(|s| s.max_received_batch_seq >= s.next_epoch_start_batch_seq);

    // Skip threshold/timeout check if force=true (SIGUSR1 or shutdown)
    if !force && !threshold_reached && !(is_timeout && has_pending) {
        return Ok(None);
    }

    // Even with force=true, we need pending batches to create an epoch
    if !has_pending {
        return Ok(None);
    }

    // Always limit to epoch_batch_threshold batches per epoch
    // (both threshold-triggered and timeout-triggered epochs respect this limit)
    let batch_limit = batcher.epoch_batch_threshold;

    // Get sources that have pending batches with their batch states
    // If batch_limit is set, we'll limit the total batches collected
    // Tuple: (source_id, start_seq, end_seq, max_received, min_received)
    let mut source_queries: Vec<(u32, u64, u64, u64, u64)> = Vec::new();
    let mut batches_to_collect: u64 = 0;

    for (&source_id, state) in batcher.source_batch_states.iter() {
        if state.max_received_batch_seq < state.next_epoch_start_batch_seq {
            continue; // No pending batches for this source
        }

        let start_seq = state.next_epoch_start_batch_seq;
        let mut end_seq = state.max_received_batch_seq;
        let pending_for_source = end_seq - start_seq + 1;

        let remaining = batch_limit.saturating_sub(batches_to_collect);
        if remaining == 0 {
            break; // Already have enough batches
        }
        // Limit this source's contribution to not exceed the total limit
        if pending_for_source > remaining {
            end_seq = start_seq + remaining - 1;
        }

        let batches_from_source = end_seq - start_seq + 1;
        batches_to_collect += batches_from_source;
        source_queries.push((source_id, start_seq, end_seq, state.max_received_batch_seq, state.min_received_batch_seq));
    }

    if source_queries.is_empty() {
        return Ok(None);
    }

    // Log the query ranges for debugging batch sequence gaps
    eprintln!(
        "[kafka-consumer][epoch-create] querying {} sources for epoch:",
        source_queries.len()
    );
    for (source_id, start_seq, end_seq, max_received, min_received) in &source_queries {
        let min_str = if *min_received == u64::MAX { "N/A".to_string() } else { min_received.to_string() };
        eprintln!(
            "  source_id={}: query_range=[{}, {}], min_received={}, max_received={}",
            source_id, start_seq, end_seq, min_str, max_received
        );
    }

    // PARALLEL: Read batches from RocksDB for all sources in parallel
    let read_results: Vec<Result<SourceBatchReadResult>> = source_queries
        .par_iter()
        .map(|(source_id, start_seq, end_seq, max_received, _min_received)| {
            let batches = raw_db.event_batches_for_source_range(*source_id, *start_seq, *end_seq)?;
            if batches.is_empty() {
                eprintln!(
                    "[kafka-consumer][epoch-create] source_id={}: NO BATCHES in RocksDB for range [{},{}]. \
                    max_received={} but nothing stored. Possible: batches not yet written or consumer lag.",
                    source_id, start_seq, end_seq, max_received
                );
                return Ok(SourceBatchReadResult {
                    source_id: *source_id,
                    batches: vec![],
                    last_processed_batch_seq: *start_seq,
                    max_received_batch_seq: *max_received,
                });
            }
            let mut sorted_batches = batches;
            sorted_batches.sort_by_key(|b| b.batch_seq);
            // DEDUPLICATION: Kafka guarantees at-least-once delivery (no loss, but may have
            // duplicates). Duplicates can occur from: producer retries on network issues,
            // consumer restarts before offset commit, or producer restarts resending same seqs.
            // We deduplicate by batch_seq to ensure each sequence number appears only once.
            sorted_batches.dedup_by_key(|b| b.batch_seq);

            // CONTIGUITY CHECK: Only include batches that start at expected seq and are contiguous.
            // If batches arrive out of order (e.g., we expect seq=109 but only have 135+),
            // we must wait for the missing batches to arrive before processing.
            let first_batch_seq = sorted_batches.first().map(|b| b.batch_seq).unwrap_or(0);
            if first_batch_seq != *start_seq {
                // Gap at start: first available batch doesn't match expected start_seq.
                // Skip this source for now - batches haven't arrived yet.
                // Log all batch seqs in RocksDB for debugging
                let all_seqs: Vec<u64> = sorted_batches.iter().map(|b| b.batch_seq).collect();
                let seqs_preview: String = if all_seqs.len() <= 10 {
                    format!("{:?}", all_seqs)
                } else {
                    format!("{:?}...(total {})", &all_seqs[..10], all_seqs.len())
                };
                eprintln!(
                    "[kafka-consumer][epoch-create] source_id={}: GAP detected! expected_start={} but first_in_rocksdb={}. \
                    Batches in RocksDB for range [{},{}]: {}. Missing seqs [{},{}). Skipping source.",
                    source_id, start_seq, first_batch_seq,
                    start_seq, end_seq, seqs_preview,
                    start_seq, first_batch_seq
                );
                return Ok(SourceBatchReadResult {
                    source_id: *source_id,
                    batches: vec![],
                    last_processed_batch_seq: *start_seq,
                    max_received_batch_seq: *max_received,
                });
            }

            // Keep only contiguous batches (each batch_seq = previous + 1)
            let mut contiguous_batches: Vec<StoredEventBatch> = Vec::with_capacity(sorted_batches.len());
            let mut expected_seq = *start_seq;
            for batch in sorted_batches {
                if batch.batch_seq == expected_seq {
                    expected_seq += 1;
                    contiguous_batches.push(batch);
                } else {
                    // Gap found in the middle - stop here
                    eprintln!(
                        "[kafka-consumer][epoch-create] source_id={}: gap in middle, expected seq={} but got={}. Taking {} contiguous batches.",
                        source_id, expected_seq, batch.batch_seq, contiguous_batches.len()
                    );
                    break;
                }
            }

            let last_processed = contiguous_batches.last().map(|b| b.batch_seq).unwrap_or(*start_seq);
            Ok(SourceBatchReadResult {
                source_id: *source_id,
                batches: contiguous_batches,
                last_processed_batch_seq: last_processed,
                max_received_batch_seq: *max_received,
            })
        })
        .collect();

    // SEQUENTIAL: Process results and collect batches for epoch
    // Only collect up to batch_limit batches, and only delete what we actually use
    let epoch_seq = batcher.next_epoch_seq;
    let mut total_events = 0u64;
    let mut epoch_batches: Vec<StoredEventBatch> = Vec::new();
    let mut unique_sources = std::collections::HashSet::new();
    let mut batches_collected = 0u64;

    for result in read_results {
        let result = result?;
        if result.batches.is_empty() {
            continue;
        }

        // Stop if we've already collected enough batches
        if batches_collected >= batch_limit {
            break;
        }

        // Calculate how many batches we can take from this source
        let remaining_capacity = (batch_limit - batches_collected) as usize;
        let batches_to_take = result.batches.len().min(remaining_capacity);

        // Collect only up to remaining capacity
        let mut actual_last_processed_seq = result.last_processed_batch_seq;
        for (i, batch) in result.batches.iter().enumerate() {
            if i >= batches_to_take {
                break;
            }
            total_events += batch.events.len() as u64;
            unique_sources.insert(batch.source_id);
            actual_last_processed_seq = batch.batch_seq;
        }
        epoch_batches.extend(result.batches.into_iter().take(batches_to_take));
        batches_collected += batches_to_take as u64;

        // Update per-source state in RocksDB (only up to what we actually used)
        let new_source_state = SourceState {
            source_id: result.source_id,
            last_processed_batch_seq: actual_last_processed_seq,
            max_received_batch_seq: result.max_received_batch_seq,
        };
        raw_db.put_source_state(wb, result.source_id, &new_source_state)?;

        // Update in-memory batcher state
        batcher.mark_epoch_processed(result.source_id, actual_last_processed_seq);

        // Delete only the batches we actually used from per-source storage
        raw_db.delete_event_batches_up_to(wb, result.source_id, actual_last_processed_seq)?;
    }

    if total_events == 0 {
        return Ok(None);
    }

    // Sort batches by (source_id, source_batch_seq) to preserve per-source chain ordering.
    // Since batch_seq == source_batch_seq (producer's sequence), this ensures correct chain order.
    // Sorting is still needed because batches are collected in parallel from multiple sources.
    epoch_batches.sort_by(|a, b| {
        a.source_id.cmp(&b.source_id)
            .then_with(|| a.batch_seq.cmp(&b.batch_seq))
    });

    // Sanity check: we should never exceed batch_limit since we stop collecting early
    debug_assert!(
        epoch_batches.len() as u64 <= batch_limit,
        "BUG: collected {} batches but limit is {}",
        epoch_batches.len(),
        batch_limit
    );

    // Store all batches for this epoch (single write)
    raw_db.put_epoch_batches(wb, epoch_seq, &epoch_batches)?;

    // Update batcher state
    batcher.next_epoch_seq += 1;
    batcher.last_flush_time = Instant::now();
    // Subtract the batches we actually used from the counter
    // (For timeout, this effectively resets to 0 since we take all available)
    let batches_used = epoch_batches.len() as u64;
    batcher.total_batches_received = batcher.total_batches_received.saturating_sub(batches_used);

    // Persist batcher state
    let state = EpochBatcherState {
        next_epoch_seq: batcher.next_epoch_seq,
        last_flush_time_ms: now_ms(),
        epoch_timeout_ms: batcher.epoch_timeout_ms,
    };
    raw_db.put_epoch_batcher_state(wb, &state)?;

    eprintln!(
        "[kafka-consumer] created epoch seq={} batches={} events={} sources={}",
        epoch_seq,
        epoch_batches.len(),
        total_events,
        unique_sources.len()
    );

    Ok(Some(epoch_seq))
}

/// Run the Kafka consumer loop with epoch batching.
pub async fn run_consumer(
    config: KafkaConsumerConfig,
    raw_db: Arc<RocksDb>,
    signals: Arc<ConsumerSignals>,
) -> Result<()> {
    const MAX_BATCH_SIZE: usize = 100;
    const BATCH_TIMEOUT_MS: u64 = 100;

    let consumer = create_consumer(&config)?;

    eprintln!(
        "[kafka-consumer] started brokers={} topic={} group={} epoch_timeout_ms={} epoch_batch_threshold={}",
        config.brokers, config.topic, config.group_id, config.epoch_timeout_ms, config.epoch_batch_threshold
    );

    let mut batcher = EpochBatcher::new(config.epoch_timeout_ms, config.epoch_batch_threshold);
    batcher.load_from_db(&raw_db)?;

    // Initialize cleanup state for deleting processed epochs
    let mut cleanup_state = CleanupState::new(
        config.agg_db_path.as_deref(),
        config.agg_db_secondary_path.as_deref(),
    );

    let mut pending_messages = Vec::new();
    let mut last_flush = Instant::now();
    let mut last_status_log = Instant::now();
    const STATUS_LOG_INTERVAL_SEC: u64 = 30;

    // E2E component timing (camera-ready baseline): accumulate wall-clock spent
    // waiting on Kafka receive vs. inserting raw batches into RocksDB. Emitted
    // once on shutdown when E2E_TIMING=1. Cheap counters; no-op accounting only.
    let e2e_timing = std::env::var("E2E_TIMING").map(|v| v != "0" && !v.is_empty()).unwrap_or(false);
    let mut kafka_recv_ms = 0f64;
    let mut rocksdb_raw_insert_ms = 0f64;

    loop {
        // Periodic status logging for debugging batch progress
        if last_status_log.elapsed().as_secs() >= STATUS_LOG_INTERVAL_SEC {
            eprintln!(
                "[kafka-consumer][status] sources={} total_batches_received={} pending={}",
                batcher.source_batch_states.len(),
                batcher.total_batches_received,
                batcher.pending_sources_count()
            );
            // Log first few sources with potential issues (where max_received > next_start by a lot)
            let sources_with_pending: Vec<_> = batcher.source_batch_states.iter()
                .filter(|(_, s)| s.max_received_batch_seq >= s.next_epoch_start_batch_seq)
                .take(5)
                .collect();
            for (sid, state) in sources_with_pending {
                eprintln!(
                    "[kafka-consumer][status]   source_id={}: next_start={}, max_received={}",
                    sid, state.next_epoch_start_batch_seq, state.max_received_batch_seq
                );
            }
            last_status_log = Instant::now();
        }

        // Handle force flush signal (SIGUSR1) - flush all pending batches as epoch without shutdown
        if signals.force_flush.swap(false, Ordering::Relaxed) {
            eprintln!("[kafka-consumer] force flush requested via SIGUSR1");
            // First flush any pending Kafka messages to RocksDB
            if !pending_messages.is_empty() {
                flush_batch(&raw_db, &consumer, &mut pending_messages, &mut batcher)?;
            }
            // Create epoch with ALL pending batches (regardless of threshold)
            if batcher.has_pending_batches() {
                let mut wb = WriteBatch::default();
                if let Some(epoch_seq) = create_epoch(&raw_db, &mut wb, &mut batcher, true)? {
                    raw_db.write_batch(wb)?;
                    raw_db.flush()?;
                    eprintln!("[kafka-consumer] force-flushed epoch {} to disk", epoch_seq);
                }
            } else {
                eprintln!("[kafka-consumer] no pending batches to flush");
            }
        }

        if signals.shutdown.load(Ordering::Relaxed) {
            // Flush pending messages before shutdown
            if !pending_messages.is_empty() {
                flush_batch(&raw_db, &consumer, &mut pending_messages, &mut batcher)?;
            }
            // Create final epoch if there are pending batches
            if batcher.has_pending_batches() {
                let mut wb = WriteBatch::default();
                create_epoch(&raw_db, &mut wb, &mut batcher, true)?;
                raw_db.write_batch(wb)?;
                raw_db.flush()?;
            }
            // Log batch statistics per source for debugging
            eprintln!("[kafka-consumer] === Shutdown summary ===");
            eprintln!("[kafka-consumer] Total sources tracked: {}", batcher.source_batch_states.len());
            for (source_id, state) in batcher.source_batch_states.iter() {
                let pending = if state.max_received_batch_seq >= state.next_epoch_start_batch_seq {
                    state.max_received_batch_seq - state.next_epoch_start_batch_seq + 1
                } else {
                    0
                };
                eprintln!(
                    "[kafka-consumer]   source_id={}: max_received={}, next_epoch_start={}, pending={}",
                    source_id, state.max_received_batch_seq, state.next_epoch_start_batch_seq, pending
                );
            }
            if e2e_timing {
                println!(
                    "[e2e-timing][kafka-consumer] kafka_recv_ms={:.3} rocksdb_raw_insert_ms={:.3} total_batches_received={}",
                    kafka_recv_ms, rocksdb_raw_insert_ms, batcher.total_batches_received
                );
                use std::io::Write as _;
                let _ = std::io::stdout().flush();
            }
            eprintln!("[kafka-consumer] shutdown requested");
            break;
        }

        // Check if we should flush Kafka messages
        let should_flush = pending_messages.len() >= MAX_BATCH_SIZE
            || (!pending_messages.is_empty()
                && last_flush.elapsed().as_millis() >= BATCH_TIMEOUT_MS as u128);

        if should_flush {
            let _ins0 = Instant::now();
            flush_batch(&raw_db, &consumer, &mut pending_messages, &mut batcher)?;
            if e2e_timing {
                rocksdb_raw_insert_ms += _ins0.elapsed().as_secs_f64() * 1000.0;
            }
            last_flush = Instant::now();
        }

        // Check if we should create an epoch
        if batcher.should_create_epoch() {
            let mut wb = WriteBatch::default();
            if let Some(epoch_seq) = create_epoch(&raw_db, &mut wb, &mut batcher, false)? {
                raw_db.write_batch(wb)?;
                raw_db.flush()?;
                eprintln!("[kafka-consumer] epoch {} flushed to disk", epoch_seq);
            }
        }

        // Periodically cleanup processed epochs
        if cleanup_state.should_check_cleanup() {
            if let Err(e) = cleanup_state.cleanup_processed_epochs(&raw_db) {
                eprintln!("[kafka-consumer] cleanup error: {e}");
            }
        }

        // Poll for next message
        let _recv0 = Instant::now();
        let recv_result = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            consumer.recv(),
        )
        .await;
        match recv_result
        {
            Ok(Ok(msg)) => {
                // Count only recv calls that returned a message, so idle 10ms
                // poll timeouts after the producer finishes don't inflate the
                // "Kafka receive" component.
                if e2e_timing {
                    kafka_recv_ms += _recv0.elapsed().as_secs_f64() * 1000.0;
                }
                pending_messages.push(msg);
            }
            Ok(Err(e)) => {
                eprintln!("[kafka-consumer] recv error: {e}");
            }
            Err(_) => {
                // Timeout - continue loop
            }
        }
    }

    Ok(())
}

/// Flush a batch of pending Kafka messages to RocksDB.
fn flush_batch(
    raw_db: &RocksDb,
    consumer: &StreamConsumer,
    pending_messages: &mut Vec<rdkafka::message::BorrowedMessage>,
    batcher: &mut EpochBatcher,
) -> Result<()> {
    if pending_messages.is_empty() {
        return Ok(());
    }

    let mut wb = WriteBatch::default();
    let mut total_batches = 0usize;
    let mut total_events = 0usize;
    let mut failed = false;

    // Collect batch info for deferred state updates (fixes race condition)
    // We only update max_received AFTER WriteBatch is committed to RocksDB
    let mut committed_batches: Vec<ProcessedBatchInfo> = Vec::new();

    for msg in pending_messages.iter() {
        if let Some(payload) = msg.payload() {
            match process_message_for_batching(raw_db, &mut wb, payload) {
                Ok(Some(info)) => {
                    total_batches += 1;
                    total_events += info.event_count;
                    committed_batches.push(info);
                }
                Ok(None) => {
                    // Empty batch, nothing to do
                }
                Err(e) => {
                    eprintln!(
                        "[kafka-consumer] process failed partition={} offset={}: {e}",
                        msg.partition(),
                        msg.offset()
                    );
                    failed = true;
                    break;
                }
            }
        }
    }

    if !failed {
        // Commit WriteBatch to RocksDB FIRST
        raw_db.write_batch(wb)?;
        raw_db.flush()?;

        // NOW update batcher state - only after batches are confirmed in RocksDB
        // This fixes the race condition where max_received > what's in RocksDB
        for info in committed_batches {
            batcher.update_received_batch_seq(info.source_id, info.batch_seq);
        }

        for msg in pending_messages.iter() {
            if let Err(e) = consumer.commit_message(msg, CommitMode::Async) {
                eprintln!("[kafka-consumer] commit failed: {e}");
            }
        }

        eprintln!(
            "[kafka-consumer] ingested batches={} events={} pending_sources={}",
            total_batches,
            total_events,
            batcher.pending_sources_count()
        );
    }

    pending_messages.clear();
    Ok(())
}

/// Cleanup state for tracking processed epochs.
struct CleanupState {
    /// agg_db opened as secondary (for checking processed epochs)
    agg_db: Option<RocksDb>,
    /// Last epoch sequence we cleaned up
    last_cleaned_seq: i64,
    /// Last cleanup check time
    last_check_time: Instant,
    /// Epoch type to check (defaults to Samples)
    epoch_type: EpochType,
}

impl CleanupState {
    fn new(agg_db_path: Option<&str>, agg_db_secondary_path: Option<&str>) -> Self {
        let agg_db = match (agg_db_path, agg_db_secondary_path) {
            (Some(primary), Some(secondary)) => {
                match RocksDb::open_secondary(primary, secondary) {
                    Ok(db) => {
                        eprintln!("[kafka-consumer] opened agg_db as secondary for cleanup");
                        Some(db)
                    }
                    Err(e) => {
                        eprintln!("[kafka-consumer] failed to open agg_db secondary for cleanup: {e}");
                        None
                    }
                }
            }
            _ => None,
        };

        Self {
            agg_db,
            last_cleaned_seq: -1,
            last_check_time: Instant::now(),
            epoch_type: EpochType::SamplesEpoch,
        }
    }

    /// Check and cleanup processed epochs.
    /// Deletes epoch_batches from raw_db for epochs that have been processed by ZK aggregator.
    fn cleanup_processed_epochs(&mut self, raw_db: &RocksDb) -> Result<u64> {
        let agg_db = match self.agg_db.as_ref() {
            Some(db) => db,
            None => return Ok(0), // No agg_db configured, skip cleanup
        };

        // Catch up secondary to see latest processed epochs
        agg_db.catch_up_if_secondary().ok();

        // Find epoch_batches that have been processed
        let mut deleted_count = 0u64;
        let mut wb = WriteBatch::default();

        // Get the range of epoch_batches in raw_db
        let range = match raw_db.epoch_batches_seq_range()? {
            Some((min, max)) => (min, max),
            None => return Ok(0), // No epochs to clean
        };

        // Check each epoch from last_cleaned + 1 to max
        let start_check = (self.last_cleaned_seq + 1).max(range.0);
        for seq in start_check..=range.1 {
            // Check if this epoch has been processed by ZK aggregator
            if agg_db.has_agg_epoch(self.epoch_type, seq)? {
                // Delete the epoch_batches
                raw_db.delete_epoch_batches(&mut wb, seq)?;
                deleted_count += 1;
                self.last_cleaned_seq = seq;
            } else {
                // Stop at first unprocessed epoch (assume sequential processing)
                break;
            }
        }

        if deleted_count > 0 {
            raw_db.write_batch(wb)?;
            eprintln!(
                "[kafka-consumer] cleaned up {} processed epochs (last_cleaned={})",
                deleted_count, self.last_cleaned_seq
            );
        }

        self.last_check_time = Instant::now();
        Ok(deleted_count)
    }

    /// Check if it's time to run cleanup.
    fn should_check_cleanup(&self) -> bool {
        self.agg_db.is_some()
            && self.last_check_time.elapsed().as_millis() >= CLEANUP_CHECK_INTERVAL_MS as u128
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_key_id(val: u8) -> [u8; KEY_BYTES_LEN] {
        let mut key = [0u8; KEY_BYTES_LEN];
        key[KEY_BYTES_LEN - 1] = val;
        key
    }

    #[test]
    fn test_raw_event_struct() {
        let key_id = make_test_key_id(100);
        let event = RawEvent {
            key_id,
            seq: 0,
            value: 42,
            ts: 1234567890,
        };
        assert_eq!(event.value, 42);
        assert_eq!(event.ts, 1234567890);
        assert_eq!(event.key_id[KEY_BYTES_LEN - 1], 100);
    }
}
