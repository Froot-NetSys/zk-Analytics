//! Kafka producer for sending event batches to aggregators.
//!
//! # Environment Variables
//! - `KAFKA_BROKERS`: Kafka broker addresses (default: localhost:9092)
//! - `KAFKA_TOPIC`: Topic to produce to (default: raw_events)
//!
//! # Example
//! ```bash
//! KAFKA_BROKERS=kafka:9092 \
//! KAFKA_TOPIC=raw_events \
//! cargo run --bin kafka-producer -- --events 10000 --batch-size 100
//! ```

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::time::Duration;
use zktelemetry_risc0_common::KEY_BYTES_LEN;

const TAG_SHARD_CHAIN: &[u8] = b"ZKTLM_SHARD_CHAIN_V1";

/// Raw event matching consumer format.
/// Wire format: (key_id, value, ts)
/// - key_id: 15-byte key identifier
/// - value: 32-bit value
/// - ts: 32-bit timestamp (seconds)
/// Note: chain_hash is computed at batch level to amortize commitment overhead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawEvent {
    pub key_id: [u8; KEY_BYTES_LEN],
    pub value: u32,   // 32-bit value
    pub ts: u32,      // 32-bit timestamp (seconds)
}

/// Batch of events for a single Kafka message.
/// Each batch has a single batch_hash that covers all events, amortizing
/// the commitment overhead (32 bytes per batch instead of per event).
/// batch_hash = SHA256(TAG || chain_prev || events_commit)
/// Note: chain_prev is not transmitted (sequential verification).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventBatch {
    pub ingest_time_ms: i64,
    pub source_id: u32,  // Source identifier for per-source chain verification
    #[serde(default)]
    pub source_batch_seq: u64,  // Per-source batch sequence for chain ordering
    pub events: Vec<RawEvent>,
    pub batch_hash: [u8; 32],  // chain commitment for this batch
}

/// Simple event for input (without chain fields).
#[derive(Clone, Debug)]
pub struct SimpleEvent {
    pub ts: u32,      // 32-bit timestamp (seconds)
    pub key_id: [u8; KEY_BYTES_LEN],
    pub value: u32,   // 32-bit value
}

fn sha256_bytes(chunks: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for c in chunks {
        hasher.update(c);
    }
    let out = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

/// Compute commitment of events: SHA256(key_id || value || ts) for each event
/// Timestamp is included in the commitment for integrity verification
fn events_commit(events: &[RawEvent]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for ev in events {
        hasher.update(&ev.key_id);               // 15 bytes key_id
        hasher.update(&ev.value.to_be_bytes());  // 4 bytes
        hasher.update(&ev.ts.to_be_bytes());     // 4 bytes - timestamp in commitment
    }
    let out = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

fn compute_chain_hash(prev: [u8; 32], events: &[RawEvent]) -> [u8; 32] {
    let commit = events_commit(events);
    sha256_bytes(&[TAG_SHARD_CHAIN, &prev, &commit])
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Clone)]
pub struct KafkaProducerConfig {
    pub brokers: String,
    pub topic: String,
    pub acks: String,
    pub timeout_ms: u64,
    /// Number of Kafka partitions (= number of aggregator consumers).
    /// partition = source_id % num_aggregators
    pub num_aggregators: u32,
    /// Number of logical sources for source_id computation.
    /// source_id = hash(natural_key) % num_sources
    /// where natural_key is machine_id (Google Cluster) or ip_pair (CAIDA).
    /// Defaults to num_aggregators if not set.
    pub num_sources: u32,
    /// Source ID for synthetic data mode (single source).
    /// Used directly when use_configured_source_id is true.
    /// Ignored when loading from datasets (source_id is computed from data).
    pub source_id: u32,
    /// When true, use config.source_id directly for all events (synthetic mode).
    /// When false, compute source_id = hash(natural_key) % num_sources (dataset mode).
    /// Default: false (dataset mode with hash-based source_id)
    pub use_configured_source_id: bool,
    /// Number of events per EventBatch (commit batch size).
    /// Controls how many events are covered by one batch_hash.
    /// Default: 8 events per batch_hash
    pub commit_batch_size: u64,
    /// Number of commit batches per Kafka send call.
    /// Default: 1 (one commit batch per send)
    pub kafka_batch_size: u64,
    /// Deprecated: Ignored. Source-based partitioning is always used.
    /// source_id is computed from the event data (machine_id or ip_pair).
    #[deprecated(note = "Ignored - source-based partitioning is always used")]
    pub partition_by_key: bool,
    /// When true, distribute events evenly across source_ids using round-robin
    /// instead of hash(natural_key) % num_sources. This ensures balanced load
    /// across partitions even when the input data has skewed key distributions.
    /// Default: false (use hash-based distribution)
    pub distribute_evenly: bool,
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        let num_aggregators = std::env::var("NUM_AGGREGATORS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        // NUM_SOURCES defaults to NUM_AGGREGATORS if not set
        let num_sources = std::env::var("NUM_SOURCES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(num_aggregators);
        Self {
            brokers: std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            topic: std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "raw_events".into()),
            acks: std::env::var("KAFKA_ACKS").unwrap_or_else(|_| "all".into()),
            timeout_ms: std::env::var("KAFKA_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
            num_aggregators,
            num_sources,
            source_id: std::env::var("SOURCE_ID")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            use_configured_source_id: std::env::var("USE_CONFIGURED_SOURCE_ID")
                .map(|v| v == "1" || v.to_lowercase() == "true")
                .unwrap_or(false),
            commit_batch_size: std::env::var("COMMIT_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8),  // 8 events per batch_hash
            kafka_batch_size: std::env::var("KAFKA_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),  // 1 commit batch per key per send
            #[allow(deprecated)]
            partition_by_key: std::env::var("PARTITION_BY_KEY")
                .map(|v| v == "1" || v.to_lowercase() == "true")
                .unwrap_or(false),
            distribute_evenly: std::env::var("DISTRIBUTE_EVENLY")
                .map(|v| v == "1" || v.to_lowercase() == "true")
                .unwrap_or(false),
        }
    }
}

/// Create a Kafka producer with the given configuration.
pub fn create_producer(config: &KafkaProducerConfig) -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("acks", &config.acks)
        .set("message.timeout.ms", config.timeout_ms.to_string())
        .set("queue.buffering.max.messages", "100000")
        .set("queue.buffering.max.kbytes", "1048576") // 1GB
        .set("batch.num.messages", "1000")
        .set("linger.ms", "5")
        .create()
        .context("failed to create Kafka producer")?;

    Ok(producer)
}

/// Per-partition state for chain tracking when partition_by_key is enabled.
#[derive(Clone, Debug)]
pub struct PartitionState {
    pub chain_hash: [u8; 32],
    pub batch_seq: u64,
}

impl Default for PartitionState {
    fn default() -> Self {
        Self {
            chain_hash: [0u8; 32],
            batch_seq: 0,
        }
    }
}

/// Event batch producer that maintains per-source chain state.
/// All keys from the same source share one hash chain for commitment.
/// This ensures each aggregator receives a complete, verifiable chain.
///
/// When partition_by_key is enabled, each partition has its own chain hash,
/// allowing events to be distributed across aggregators by machine_id.
pub struct EventBatchProducer {
    producer: FutureProducer,
    config: KafkaProducerConfig,
    /// Source-level chain hash (shared across all keys from this source)
    /// Used when partition_by_key is false.
    source_chain_hash: [u8; 32],
    /// Source-level batch sequence (for chain ordering during epoch creation)
    /// Used when partition_by_key is false.
    source_batch_seq: u64,
    /// Per-partition chain state (used when partition_by_key is true)
    partition_states: std::collections::HashMap<u32, PartitionState>,
    /// Total events sent
    total_events: u64,
    batch_number: i64,    // Batch counter for logging
    /// Round-robin counter for even distribution mode
    round_robin_counter: u64,
    /// Transparency-log publisher for periodic checkpoints (Algorithm:
    /// "Log Commitment", `\change` block). No-op unless TRILLIAN_ADDR is set.
    tlog: crate::transparency::TransparencyLog,
    /// Publication interval `P`: publish a checkpoint every P-th committed batch
    /// per source (from CHECKPOINT_INTERVAL; 0 disables publication).
    checkpoint_interval: u64,
}

impl EventBatchProducer {
    pub async fn new(
        config: KafkaProducerConfig,
        initial_chain_hash: [u8; 32],  // Initial source chain hash
    ) -> Result<Self> {
        let producer = create_producer(&config)?;
        // Publication interval P for transparency-log checkpoints (0 = disabled).
        let checkpoint_interval = std::env::var("CHECKPOINT_INTERVAL")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let tlog = crate::transparency::TransparencyLog::from_env().await;
        Ok(Self {
            producer,
            config,
            source_chain_hash: initial_chain_hash,
            source_batch_seq: 0,
            partition_states: std::collections::HashMap::new(),
            total_events: 0,
            batch_number: 0,
            round_robin_counter: 0,
            tlog,
            checkpoint_interval,
        })
    }

    /// Extract the natural key from key_id bytes (last 8 bytes).
    /// - For Google Cluster data: this is machine_id
    /// - For CAIDA data: this is (src_ip << 32) | dst_ip
    fn extract_natural_key(key_id: &[u8; KEY_BYTES_LEN]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key_id[KEY_BYTES_LEN - 8..]);
        u64::from_be_bytes(bytes)
    }

    /// Compute source_id from natural key using a simple hash.
    /// source_id = hash(natural_key) % num_sources
    /// This ensures all events from the same machine/ip_pair go to the same source.
    fn compute_source_id(natural_key: u64, num_sources: u32) -> u32 {
        // Use a simple but effective hash mixing function
        // FNV-1a inspired mixing
        let mut h = natural_key;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        (h % num_sources as u64) as u32
    }

    /// Send a batch of events to Kafka.
    ///
    /// Source ID determination:
    /// - Synthetic mode (use_configured_source_id=true): use config.source_id directly
    /// - Dataset mode (use_configured_source_id=false): source_id = hash(natural_key) % num_sources
    ///   This ensures all events from the same machine (Google Cluster) or IP pair (CAIDA)
    ///   go to the same source, and each source has contiguous batch sequences.
    ///
    /// Partitioning: partition = source_id % num_aggregators
    ///
    /// Batch-level hashing: One SHA256 hash per commit batch (amortized commitment).
    /// batch_hash = SHA256(TAG || chain_prev || events_commit)
    /// where events_commit = SHA256(all event data in batch)
    ///
    /// The commit_batch_size config controls how many events go into each
    /// EventBatch (and thus each batch_hash).
    pub async fn send_batch(&mut self, events: Vec<SimpleEvent>) -> Result<[u8; 32]> {
        if events.is_empty() {
            return Ok([0u8; 32]);
        }

        let ingest_time_ms = now_ms();
        let commit_batch_size = self.config.commit_batch_size;

        // Group events by source_id
        let mut events_by_source: std::collections::HashMap<u32, Vec<&SimpleEvent>> =
            std::collections::HashMap::new();

        if self.config.use_configured_source_id {
            // Synthetic mode: use configured source_id directly for all events
            // All events go to the same source (single producer per source)
            let source_id = self.config.source_id;
            events_by_source.insert(source_id, events.iter().collect());
        } else if self.config.distribute_evenly {
            // Even distribution mode: round-robin across source_ids
            // This ensures balanced load even when input data has skewed key distributions
            for event in &events {
                let source_id = (self.round_robin_counter % self.config.num_sources as u64) as u32;
                self.round_robin_counter = self.round_robin_counter.wrapping_add(1);
                events_by_source.entry(source_id).or_default().push(event);
            }
        } else {
            // Dataset mode: compute source_id = hash(natural_key) % num_sources
            // This ensures all events from the same machine/ip_pair go to the same source
            for event in &events {
                let natural_key = Self::extract_natural_key(&event.key_id);
                let source_id = Self::compute_source_id(natural_key, self.config.num_sources);
                events_by_source.entry(source_id).or_default().push(event);
            }
        }

        let mut last_chain_hash = [0u8; 32];
        let num_sources_used = events_by_source.len();
        let mut partitions_used: std::collections::HashSet<u32> = std::collections::HashSet::new();

        // Send Kafka messages per source_id, splitting by commit_batch_size
        for (source_id, source_events) in events_by_source {
            // Partition = source_id % num_aggregators
            // Multiple sources can map to the same partition if num_sources > num_aggregators
            let partition_idx = source_id % self.config.num_aggregators;
            partitions_used.insert(partition_idx);

            // Partition key for Kafka (used for logging/debugging)
            let partition_key = format!("src{}", source_id);

            // Split events into chunks based on commit_batch_size
            // 0 = unlimited (all events in one batch)
            let chunk_size = if commit_batch_size == 0 {
                source_events.len()
            } else {
                commit_batch_size as usize
            };

            for chunk in source_events.chunks(chunk_size) {
                // Get per-source chain state
                let state = self.partition_states
                    .entry(source_id)
                    .or_default();
                let chain_prev = state.chain_hash;
                let batch_seq = state.batch_seq;

                // Convert to RawEvents
                let mut raw_events = Vec::with_capacity(chunk.len());
                for event in chunk {
                    let raw_event = RawEvent {
                        key_id: event.key_id,
                        value: event.value,
                        ts: event.ts,
                    };
                    raw_events.push(raw_event);
                }

                // Compute batch-level commitment
                // batch_hash = SHA256(TAG || chain_prev || events_commit(&raw_events))
                let ev_commit = events_commit(&raw_events);
                let batch_hash = compute_chain_hash(chain_prev, &raw_events);

                // Debug: log hash inputs for chain verification debugging (enable with VERBOSE_HASH_LOGGING=1)
                if std::env::var("VERBOSE_HASH_LOGGING").map(|v| v == "1").unwrap_or(false) {
                    eprintln!(
                        "[PRODUCER_HASH] source_id={} batch_seq={} partition={} chain_prev={:?} events_commit={:?} batch_hash={:?} num_events={}",
                        source_id,
                        batch_seq,
                        partition_idx,
                        chain_prev,
                        ev_commit,
                        batch_hash,
                        raw_events.len()
                    );
                    // Log first few events for debugging
                    for (i, ev) in raw_events.iter().take(3).enumerate() {
                        eprintln!(
                            "[PRODUCER_HASH] source_id={} seq={} event[{}]: key_id={:?} value={} ts={}",
                            source_id, batch_seq, i, ev.key_id, ev.value, ev.ts
                        );
                    }
                    if raw_events.len() > 3 {
                        eprintln!("[PRODUCER_HASH] source_id={} seq={} ... and {} more events",
                            source_id, batch_seq, raw_events.len() - 3);
                    }
                }

                // Update per-source chain state
                let state = self.partition_states.get_mut(&source_id).unwrap();
                state.chain_hash = batch_hash;
                state.batch_seq += 1;
                last_chain_hash = batch_hash;

                // Create EventBatch with batch-level chain hash
                let batch = EventBatch {
                    ingest_time_ms,
                    source_id,
                    source_batch_seq: batch_seq,
                    events: raw_events,
                    batch_hash,
                };

                // Serialize to JSON
                let payload = serde_json::to_vec(&batch).context("serialize event batch")?;

                // Send message to Kafka with explicit partition assignment
                let record = FutureRecord::to(&self.config.topic)
                    .partition(partition_idx as i32)
                    .key(&partition_key)
                    .payload(&payload);

                self.producer
                    .send(record, Duration::from_millis(self.config.timeout_ms))
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!("kafka send failed: {}", e))?;

                // Publish a checkpoint (i, h_i) to the transparency log every P
                // committed batches for this source. `index` is the 1-based batch
                // count (batch_seq is 0-based). Best-effort: a publication failure
                // is logged but must not stall ingestion.
                let index = batch_seq + 1;
                if self.checkpoint_interval > 0 && index.is_multiple_of(self.checkpoint_interval) {
                    let cp = crate::transparency::Checkpoint {
                        source_id,
                        index,
                        chain_hash: batch_hash,
                    };
                    if let Err(e) = self.tlog.publish(cp).await {
                        eprintln!(
                            "[transparency] WARN: checkpoint publish failed (source_id={} index={}): {:#}",
                            source_id, index, e
                        );
                    }
                }
            }
        }

        self.batch_number += 1;
        self.total_events += events.len() as u64;

        let partitions_str = format!("{:?}", partitions_used);

        eprintln!(
            "[kafka-producer] sent batch_num={} events={} sources={} partitions={}",
            self.batch_number,
            events.len(),
            num_sources_used,
            partitions_str,
        );

        Ok(last_chain_hash)
    }

    /// Get current batch number (number of send_batch calls).
    pub fn sequence(&self) -> i64 {
        self.batch_number
    }

    /// Get total event count.
    pub fn event_sequence(&self) -> i64 {
        self.total_events as i64
    }

    /// Get the chain hash for a specific source_id.
    /// Returns zero hash if source has no batches.
    pub fn chain_hash_for_source(&self, source_id: u32) -> [u8; 32] {
        self.partition_states
            .get(&source_id)
            .map(|s| s.chain_hash)
            .unwrap_or([0u8; 32])
    }

    /// Get legacy source chain hash (for backward compatibility).
    /// Returns the hash for the configured source_id, or zero if no batches.
    pub fn chain_hash(&self) -> [u8; 32] {
        // For single-source mode, return the configured source_id's hash
        // For multi-source mode (dataset), return zero (use source_chain_hashes instead)
        if self.partition_states.len() == 1 {
            self.partition_states.values().next().map(|s| s.chain_hash).unwrap_or([0u8; 32])
        } else {
            self.source_chain_hash
        }
    }

    /// Get per-source chain hashes.
    /// Each source_id (computed from hash(natural_key) % num_aggregators) has its own chain.
    pub fn source_chain_hashes(&self) -> &std::collections::HashMap<u32, PartitionState> {
        &self.partition_states
    }

    /// Get per-partition chain hashes (alias for source_chain_hashes for compatibility).
    pub fn partition_chain_hashes(&self) -> &std::collections::HashMap<u32, PartitionState> {
        &self.partition_states
    }

    /// Get configured source ID (for single-source synthetic mode).
    pub fn source_id(&self) -> u32 {
        self.config.source_id
    }

    /// Check if using multi-source mode (dataset with computed source_ids).
    /// Returns true if multiple sources were used during sending.
    pub fn is_multi_source(&self) -> bool {
        self.partition_states.len() > 1
    }

    /// Backward compatibility: alias for is_multi_source
    pub fn is_partition_by_key(&self) -> bool {
        self.is_multi_source()
    }

    /// Flush pending messages.
    pub fn flush(&self, timeout: Duration) -> Result<()> {
        self.producer.flush(timeout)?;
        Ok(())
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
    fn test_chain_hash_computation() {
        let prev = [0u8; 32];
        let events = vec![RawEvent {
            key_id: make_test_key_id(100),
            value: 42,
            ts: 0,
        }];

        let hash = compute_chain_hash(prev, &events);
        assert_ne!(hash, [0u8; 32]);

        // Same input should produce same hash
        let hash2 = compute_chain_hash(prev, &events);
        assert_eq!(hash, hash2);

        // Different prev should produce different hash
        let hash3 = compute_chain_hash([1u8; 32], &events);
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_source_chain_covers_multiple_keys() {
        // Source chain commits to events from different keys
        let prev = [0u8; 32];

        // Batch 1: key A
        let events1 = vec![RawEvent {
            key_id: make_test_key_id(1),
            value: 100,
            ts: 0,
        }];
        let hash1 = compute_chain_hash(prev, &events1);

        // Batch 2: key B (chained from batch 1)
        let events2 = vec![RawEvent {
            key_id: make_test_key_id(2),
            value: 200,
            ts: 0,
        }];
        let hash2 = compute_chain_hash(hash1, &events2);

        // Chain should progress
        assert_ne!(hash1, hash2);
        assert_ne!(hash2, prev);

        // Changing order changes the final hash
        let alt_hash1 = compute_chain_hash(prev, &events2);
        let alt_hash2 = compute_chain_hash(alt_hash1, &events1);
        assert_ne!(hash2, alt_hash2, "Order of keys in chain matters");
    }
}
