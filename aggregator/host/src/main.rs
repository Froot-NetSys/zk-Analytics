use anyhow::Context;
use rand::{RngCore, SeedableRng};
use risc0_zkvm::serde::to_vec;
use risc0_zkvm::{default_prover, ExecutorEnv, ProverOpts};
use rocksdb::WriteBatch;
use sha2::{Digest as _, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use aggregator_core::{
    histogram_bucket_index,
    process_histogram_aggr_with_state, process_samples_aggr_with_state, process_cm_aggr_with_state,
    compute_samples_out_commit, compute_histogram_out_commit, compute_cm_out_commit,
    BatchInput, BucketEntry, SamplesState, CmState,
    CmAggrInput, CmAggrOutput, HistogramAggrInput, HistogramAggrOutput, KeyHistogram,
    SamplesAggrInput, SamplesAggrOutput, CM_COLS, CM_ROWS, CM_TOPK_SLOTS, HISTOGRAM_SLOTS,
};
use aggregator_methods::{
    AGGR_CM_ELF, AGGR_CM_ID, AGGR_HISTOGRAM_ELF, AGGR_HISTOGRAM_ID, AGGR_SAMPLES_ELF,
    AGGR_SAMPLES_ID,
};
use common::epoch::EpochType;
use common::rocksdb_store::{
    current_owner_for_source, AggCmStruct, AggEpoch, AggEpochMeta, AggEpochProof, AggHistStruct,
    AggSourceTip, BatchEvent, EpochTombstone, Handoff, RocksDb, SampleEvent, SampleShardFrame,
    SeriesShardFrame, StoredEventBatch, VerifiedSamplesStruct,
};
use aggregator::recovery::recover_partial_state;
#[cfg(feature = "fdb")]
use common::fdb_store::FdbStore;
use zkvm_common::{Event, KEY_BYTES_LEN};

/// Per-epoch end-to-end timing accumulator for the camera-ready non-ZK / zk
/// baseline. Accumulates wall-clock milliseconds per pipeline component so the
/// e2e driver can break down where time goes (RocksDB raw read, native/zk
/// aggregation compute, proving, verification, RocksDB+FDB agg write). Enabled
/// only when `E2E_TIMING=1`; otherwise every call is a cheap no-op.
///
/// One `[e2e-timing]` key=value line is emitted per processed epoch on stdout
/// and parsed by `scripts/run_baseline_e2e.sh`.
pub(crate) mod etime {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::time::Instant;

    thread_local! {
        static ACC: RefCell<BTreeMap<&'static str, f64>> = RefCell::new(BTreeMap::new());
        static ENABLED: bool = std::env::var("E2E_TIMING").map(|v| v != "0" && !v.is_empty()).unwrap_or(false);
    }

    pub fn enabled() -> bool {
        ENABLED.with(|e| *e)
    }

    /// Add `ms` milliseconds to the named component's running total.
    pub fn add_ms(key: &'static str, ms: f64) {
        if !enabled() {
            return;
        }
        ACC.with(|a| *a.borrow_mut().entry(key).or_insert(0.0) += ms);
    }

    pub fn get(key: &str) -> f64 {
        ACC.with(|a| a.borrow().get(key).copied().unwrap_or(0.0))
    }

    pub fn reset() {
        ACC.with(|a| a.borrow_mut().clear());
    }

    /// RAII guard: accumulates elapsed time into `key` on drop (also on `?`
    /// early-return, since Drop still runs as the scope unwinds normally).
    pub struct Guard {
        key: &'static str,
        start: Instant,
    }
    impl Guard {
        pub fn new(key: &'static str) -> Self {
            Self { key, start: Instant::now() }
        }
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            add_ms(self.key, self.start.elapsed().as_secs_f64() * 1000.0);
        }
    }
}

#[allow(dead_code)]
const U48_BYTES: usize = 6;
#[allow(dead_code)]
const U48_MAX_PLUS_ONE: u64 = 1u64 << 48;
const TAG_SHARD_CHAIN: &[u8] = b"ZKTLM_SHARD_CHAIN_V1";

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

fn hash_u64_to_u64(v: u64) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(&v.to_be_bytes());
    let out = hasher.finalize();
    let mut b = [0u8; 8];
    b.copy_from_slice(&out[..8]);
    u64::from_be_bytes(b)
}

fn partition_id_for_key(key_id: &[u8; KEY_BYTES_LEN], partitions: u64) -> i16 {
    use aggregator_core::key_to_u64;
    if partitions <= 1 {
        return 0;
    }
    let h = hash_u64_to_u64(key_to_u64(key_id));
    let pid = h % partitions;
    pid as i16
}

/// Extract u64 from key_id (stored in lower 8 bytes), add offset, and convert back
#[allow(dead_code)]
fn key_id_add_offset(key_id: &[u8; KEY_BYTES_LEN], offset: u64) -> [u8; KEY_BYTES_LEN] {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key_id[KEY_BYTES_LEN - 8..]);
    let val = u64::from_be_bytes(bytes);
    Event::key_id_from_u64(val.wrapping_add(offset))
}

fn shard_chain_hash(prev: [u8; 32], events_commit: [u8; 32]) -> [u8; 32] {
    sha256_bytes(&[TAG_SHARD_CHAIN, &prev, &events_commit])
}

/// Compute events commit hash: SHA256(key_id || value || ts for each event).
/// Matches batch_events_commit format for consistency.
fn events_commit(events: &[Event]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for ev in events {
        hasher.update(&ev.key_id);                // 15 bytes key_id
        hasher.update(&ev.value.to_be_bytes());   // 4 bytes value
        hasher.update(&ev.ts.to_be_bytes());      // 4 bytes timestamp
    }
    let out = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

/// Read current process memory usage (VmRSS) from /proc/self/status in MB.
fn get_memory_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|kb| kb.parse::<u64>().ok())
                        .map(|kb| kb / 1024) // Convert KB to MB
                })
        })
        .unwrap_or(0)
}

fn log_epoch_details_batches(
    mode: &str,
    seq: i64,
    batches: &[BatchInput],
    input_bytes: usize,
) {
    use std::collections::HashSet;

    // Count unique source_ids and total events from batches
    let unique_sources: HashSet<u32> = batches.iter().map(|b| b.source_id).collect();
    let num_sources = unique_sources.len();
    let num_batches = batches.len();
    let total_events: usize = batches.iter().map(|b| b.events.len()).sum();

    eprintln!(
        "[risc0-aggr][{}] seq={} sources={} batches={} events={} input_bytes={} ({:.2} KB)",
        mode, seq, num_sources, num_batches, total_events, input_bytes, input_bytes as f64 / 1024.0
    );
}

/// Compute events_commit for a batch: SHA256(key_id || value || ts for each event)
/// Matches data_source and kafka_producer format.
/// Compute events commit hash: SHA256(key_id || value || ts for each event).
/// Each event uses its own key_id, supporting mixed keys per batch.
fn batch_events_commit(events: &[BatchEvent]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for ev in events {
        hasher.update(&ev.key_id);                // 15 bytes key_id
        hasher.update(&ev.value.to_be_bytes());   // 4 bytes value
        hasher.update(&ev.ts.to_be_bytes());      // 4 bytes timestamp
    }
    let out = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

/// Compute batch chain hash: SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
/// Matches data_source and kafka_producer format.
fn compute_batch_chain_hash(prev: [u8; 32], events: &[BatchEvent]) -> [u8; 32] {
    let commit = batch_events_commit(events);
    sha256_bytes(&[TAG_SHARD_CHAIN, &prev, &commit])
}

/// Compute events commit hash for Event type (used by ZK core).
fn events_commit_from_events(events: &[Event]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for ev in events {
        hasher.update(&ev.key_id);                // 15 bytes key_id
        hasher.update(&ev.value.to_be_bytes());   // 4 bytes value
        hasher.update(&ev.ts.to_be_bytes());      // 4 bytes timestamp
    }
    let out = hasher.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

/// Compute batch chain hash for Event type: SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
fn compute_batch_chain_hash_from_events(prev: [u8; 32], events: &[Event]) -> [u8; 32] {
    let commit = events_commit_from_events(events);
    sha256_bytes(&[TAG_SHARD_CHAIN, &prev, &commit])
}

/// Generate synthetic StoredEventBatch data for one epoch with multi-source support.
/// Returns (Vec<StoredEventBatch>, updated chain tips per source).
///
/// When num_sources > 1:
/// - Generates batches for source_ids 0..num_sources-1
/// - Each source gets its own distinct key range (source_id * series .. (source_id + 1) * series)
/// - Each source gets its own chain
/// - This exercises the core's verify_and_compute_chain multi-source path
///
/// Matches Kafka producer batching logic (EventBatchProducer::send_batch):
/// 1. Generate all events per key (same value/ts generation logic)
/// 2. Group by key_id
/// 3. For each key (in sorted key_id order for determinism), chunk into groups of commit_batch_size
/// 4. Each chunk becomes one StoredEventBatch with single-key events
/// 5. All batches chain sequentially on the source-level chain (per source)
fn generate_epoch_batches(
    mode: &str,
    series: u64,
    samples_per_series: u64,
    commit_batch_size: u64,
    num_sources: u32,
    seed: u64,
    prev_source_chain_tips: &BTreeMap<u32, [u8; 32]>,
    prev_source_batch_seqs: &BTreeMap<u32, u64>,
) -> (Vec<StoredEventBatch>, BTreeMap<u32, [u8; 32]>, BTreeMap<u32, u64>) {
    use rand::Rng;
    let series = series.max(1);
    let samples_per_series = samples_per_series.max(1);
    let commit_batch_size = commit_batch_size.max(1);
    let num_sources = num_sources.max(1);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let default_value_mod = match mode {
        "cm" => 1_000u64,
        "histogram" => 10_000u64,
        "samples" => 1_000_000u64,
        _ => 10_000u64,
    };
    let value_mod = env_u64("VALUE_MOD", default_value_mod).max(1);
    let zipf = build_zipf_value(value_mod).expect("zipf sampler");

    let ingest_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let mut all_batches: Vec<StoredEventBatch> = Vec::new();
    let mut new_source_chain_tips: BTreeMap<u32, [u8; 32]> = BTreeMap::new();
    let mut new_source_batch_seqs: BTreeMap<u32, u64> = BTreeMap::new();

    // Generate batches for each source
    for source_id in 0..num_sources {
        // Get previous chain tip for this source (or start from [0;32])
        let mut source_chain_tip = prev_source_chain_tips.get(&source_id).copied().unwrap_or([0u8; 32]);
        // Continue batch sequence from where previous epoch left off
        let mut source_batch_seq: u64 = prev_source_batch_seqs.get(&source_id).copied().unwrap_or(0);

        // Step 1: Generate all events per key for this source
        // Use BTreeMap for sorted key_id order (deterministic iteration)
        let mut events_by_key: BTreeMap<[u8; KEY_BYTES_LEN], Vec<BatchEvent>> = BTreeMap::new();

        // Check if KEY_ZIPF_S is set for Zipf key distribution
        let key_zipf_opt = env_string("KEY_ZIPF_S")
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|&v| v > 0.0)
            .and_then(|s| ZipfSampler::new(series as usize, s).ok());

        if let Some(ref key_zipf) = key_zipf_opt {
            // Zipf key distribution: sample keys with skew
            let total_events = series.saturating_mul(samples_per_series);
            for _ in 0..total_events {
                let key_index = key_zipf.sample_u64(&mut rng) % series;
                let key_id = Event::make_key_id(source_id, key_index);
                let value = (zipf.sample_u64(&mut rng) + 1) as u32;
                let ts = rng.gen_range(0u32..1_000_000u32);
                events_by_key.entry(key_id).or_insert_with(Vec::new).push(BatchEvent {
                    key_id,
                    value,
                    ts,
                });
            }
        } else {
            // Uniform key distribution: each key gets exactly samples_per_series events
            for key_index in 0..series {
                let key_id = Event::make_key_id(source_id, key_index);
                let mut key_events = Vec::with_capacity(samples_per_series as usize);

                for _ in 0..samples_per_series {
                    let value = (zipf.sample_u64(&mut rng) + 1) as u32;
                    let ts = rng.gen_range(0u32..1_000_000u32);
                    key_events.push(BatchEvent {
                        key_id,
                        value,
                        ts,
                    });
                }
                events_by_key.insert(key_id, key_events);
            }
        }

        // Step 2-4: For each key (in sorted order), chunk into batches and chain sequentially
        // BTreeMap iterates in sorted key order (deterministic)
        for (_key_id, key_events) in events_by_key {
            // Chunk this key's events by commit_batch_size
            for chunk in key_events.chunks(commit_batch_size as usize) {
                let batch_events: Vec<BatchEvent> = chunk.to_vec();

                // Compute batch chain hash using source chain (shared across all keys from this source)
                let batch_hash = compute_batch_chain_hash(source_chain_tip, &batch_events);
                source_chain_tip = batch_hash;

                all_batches.push(StoredEventBatch {
                    batch_seq: source_batch_seq,
                    source_id,
                    source_batch_seq,
                    ingest_time_ms,
                    events: batch_events,
                    batch_hash,
                });

                source_batch_seq += 1;
            }
        }

        // Store final chain tip and next batch sequence for this source
        new_source_chain_tips.insert(source_id, source_chain_tip);
        new_source_batch_seqs.insert(source_id, source_batch_seq);
    }

    (all_batches, new_source_chain_tips, new_source_batch_seqs)
}

/// Flatten StoredEventBatch to Vec<Event> for ZK input.
/// Matches the production pipeline's flattening logic.
#[allow(dead_code)]
fn flatten_batches_to_events(batches: &[StoredEventBatch]) -> Vec<Event> {
    let mut events = Vec::new();
    for batch in batches {
        for ev in &batch.events {
            events.push(Event {
                ts: ev.ts,
                key_id: ev.key_id,
                value: ev.value,
            });
        }
    }
    events
}

/// Convert StoredEventBatch to BatchInput for ZK verification.
/// The ZK guest will:
/// 1. Use chain_prev from aggregator's stored prev_source_chain_tips (trusted)
/// 2. Recompute batch_hash from events
/// 3. Verify recomputed hash matches sent_batch_hash
///
/// Returns: (Vec<BatchInput>, final chain tips per source)
fn build_batch_inputs(
    batches: &[StoredEventBatch],
    prev_source_chain_tips: &BTreeMap<u32, [u8; 32]>,
) -> (Vec<BatchInput>, BTreeMap<u32, [u8; 32]>) {
    let mut batch_inputs: Vec<BatchInput> = Vec::new();
    // Track final chain tips per source (for next epoch)
    let mut source_chain_state: BTreeMap<u32, [u8; 32]> = prev_source_chain_tips.clone();

    // Sort batches by (source_id, source_batch_seq) to ensure correct chain verification order.
    // DEDUPLICATION: Kafka guarantees at-least-once delivery (no message loss, but duplicates
    // are possible). Duplicates can occur from producer retries, consumer restarts before
    // offset commit, or producer restarts resending the same sequence numbers. We deduplicate
    // by (source_id, source_batch_seq) to ensure each sequence appears only once per source.
    let mut sorted_batches = batches.to_vec();
    sorted_batches.sort_by_key(|b| (b.source_id, b.source_batch_seq));
    sorted_batches.dedup_by_key(|b| (b.source_id, b.source_batch_seq));

    for batch in &sorted_batches {
        let source_id = batch.source_id;

        // Convert BatchEvent to Event
        let events: Vec<Event> = batch.events.iter().map(|ev| Event {
            ts: ev.ts,
            key_id: ev.key_id,
            value: ev.value,
        }).collect();

        // Create BatchInput with producer's claimed batch_hash
        // The ZK guest will verify this by recomputing from events
        batch_inputs.push(BatchInput {
            source_id,
            source_batch_seq: batch.source_batch_seq,
            events: events.clone(),
            sent_batch_hash: batch.batch_hash,
        });

        // Debug: log batch info being sent to ZK verification (enable with VERBOSE_HASH_LOGGING=1)
        if std::env::var("VERBOSE_HASH_LOGGING").map(|v| v == "1").unwrap_or(false) {
            eprintln!(
                "[AGGREGATOR_ZK_INPUT] source_id={} source_batch_seq={} sent_batch_hash={:?} num_events={}",
                source_id, batch.source_batch_seq, batch.batch_hash, events.len()
            );
            // Log first few events for debugging
            for (i, ev) in events.iter().take(3).enumerate() {
                eprintln!(
                    "[AGGREGATOR_ZK_INPUT] source_id={} seq={} event[{}]: key_id={:?} value={} ts={}",
                    source_id, batch.source_batch_seq, i, ev.key_id, ev.value, ev.ts
                );
            }
            if events.len() > 3 {
                eprintln!(
                    "[AGGREGATOR_ZK_INPUT] source_id={} seq={} ... and {} more events",
                    source_id, batch.source_batch_seq, events.len() - 3
                );
            }
        }

        // Update final chain tip per source (for return value)
        source_chain_state.insert(source_id, batch.batch_hash);
    }

    (batch_inputs, source_chain_state)
}

fn proc_status_kb(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let line = line.strip_prefix(field)?;
        let kb = line
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<u64>().ok())?;
        return Some(kb);
    }
    None
}

fn parse_arg_u64(name: &str, default: u64) -> u64 {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            if let Some(v) = it.next() {
                return v.parse::<u64>().unwrap_or(default);
            }
        }
    }
    default
}

fn parse_arg_opt_u64(name: &str) -> Option<u64> {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next().and_then(|v| v.parse::<u64>().ok());
        }
    }
    None
}

fn parse_arg_str(name: &str, default: &str) -> String {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            if let Some(v) = it.next() {
                return v;
            }
        }
    }
    default.to_string()
}

fn has_flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

fn parse_arg_string(name: &str) -> Option<String> {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next();
        }
    }
    None
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_string(name).map(PathBuf::from)
}

#[allow(dead_code)]
fn now_ts() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0)
}

fn default_google_cluster_dir() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("testdata/google_cluster_data/input"),
        PathBuf::from("../testdata/google_cluster_data/input"),
    ];
    candidates.into_iter().find(|p| p.is_dir())
}

fn default_caida_dir() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("testdata/caida_pcap/caida_txt"),
        PathBuf::from("../testdata/caida_pcap/caida_txt"),
    ];
    candidates.into_iter().find(|p| p.is_dir())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn parse_arg_opt_i64(name: &str) -> Option<i64> {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next().and_then(|v| v.parse::<i64>().ok());
        }
    }
    None
}

fn parse_csv_u32_list(s: &str) -> anyhow::Result<Vec<u32>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let v: u32 = part.parse().with_context(|| format!("parse u32 '{part}'"))?;
        out.push(v);
    }
    Ok(out)
}

#[allow(dead_code)]
fn parse_csv_i16_list(s: &str) -> anyhow::Result<Vec<i16>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let v: i16 = part.parse().with_context(|| format!("parse i16 '{part}'"))?;
        out.push(v);
    }
    Ok(out)
}

#[allow(dead_code)]
fn pack_u48_le(v: u64) -> anyhow::Result<[u8; U48_BYTES]> {
    anyhow::ensure!(v < U48_MAX_PLUS_ONE, "u48 overflow: {}", v);
    let bytes = v.to_le_bytes();
    let mut out = [0u8; U48_BYTES];
    out.copy_from_slice(&bytes[..U48_BYTES]);
    Ok(out)
}

/// Pack CM counts from state.table as u32 (4 bytes each, little-endian).
/// Direct storage without wasteful u48 encoding - saves 50% space and eliminates conversion bugs.
fn pack_cm_counts_u32(state: &CmState) -> Vec<u8> {
    let mut out = Vec::with_capacity(CM_ROWS * CM_COLS * 4);
    for r in 0..CM_ROWS {
        for c in 0..CM_COLS {
            out.extend_from_slice(&state.table[r][c].to_le_bytes());
        }
    }
    out
}

fn pack_cm_heap_fixed(
    heap_keys: &[[u8; KEY_BYTES_LEN]; CM_TOPK_SLOTS],
    heap_vals: &[u64; CM_TOPK_SLOTS],
    heap_occ: &[u8; CM_TOPK_SLOTS],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CM_TOPK_SLOTS * (KEY_BYTES_LEN + 8 + 1));
    for i in 0..CM_TOPK_SLOTS {
        out.extend_from_slice(&heap_keys[i]);
        out.extend_from_slice(&heap_vals[i].to_be_bytes());
        out.push(heap_occ[i]);
    }
    out
}

/// Pack per-key histograms into compact binary format
/// Format: [num_keys: u32] followed by variable-length per-key entries
/// Each entry: [key_id: 15 bytes][num_buckets: u16][bucket_data...][count: u64][sum: u64]
/// bucket_data: for each non-zero bucket: [bucket_idx: u16][count: u64]
fn pack_hist_per_key_table(per_key_histograms: &[KeyHistogram]) -> Vec<u8> {
    let mut out = Vec::new();

    // Write number of keys
    out.extend_from_slice(&(per_key_histograms.len() as u32).to_be_bytes());

    // Pack each key's histogram
    for kh in per_key_histograms {
        // Write key_id (15 bytes - KEY_BYTES_LEN)
        out.extend_from_slice(&kh.key_id);

        // Count non-zero buckets
        let mut non_zero_buckets: Vec<(u16, u32)> = Vec::new();
        for (idx, &count) in kh.bucket_counts.iter().enumerate() {
            if count > 0 {
                non_zero_buckets.push((idx as u16, count));
            }
        }

        // Write number of non-zero buckets
        out.extend_from_slice(&(non_zero_buckets.len() as u16).to_be_bytes());

        // Write each non-zero bucket
        for (bucket_idx, count) in non_zero_buckets {
            out.extend_from_slice(&bucket_idx.to_be_bytes());
            out.extend_from_slice(&count.to_be_bytes());
        }

        // Write total count and sum for this key
        out.extend_from_slice(&kh.count.to_be_bytes());
        out.extend_from_slice(&kh.sum.to_be_bytes());
    }

    out
}

/// Pack samples table from SamplesState - dynamic size based on actual entries.
/// No longer truncates to a fixed slot count or re-derives state from events.
fn pack_samples_table(state: &SamplesState) -> Vec<u8> {
    let mut sorted: Vec<&BucketEntry> = state.entries.values().collect();
    sorted.sort_by_key(|e| e.key_id);

    let mut out = Vec::new();
    out.extend_from_slice(&(sorted.len() as u32).to_be_bytes());
    for entry in sorted {
        out.extend_from_slice(&entry.key_id);
        out.extend_from_slice(&entry.key_chain_tip);
        out.extend_from_slice(&entry.count.to_be_bytes());
        out.extend_from_slice(&entry.sum.to_be_bytes());
    }
    out
}

fn build_series_hist_payload_v4(events: &[Event]) -> Vec<u8> {
    let series_ht_buckets = env_usize("SERIES_HT_BUCKETS", 16);
    let series_ht_bucket_cap = env_usize("SERIES_HT_BUCKET_CAP", 2);

    let mut per_key: BTreeMap<[u8; KEY_BYTES_LEN], ([u64; HISTOGRAM_SLOTS], u64, u64)> = BTreeMap::new();
    for ev in events {
        let bucket = histogram_bucket_index(ev.value as u64) as usize;
        let entry = per_key.entry(ev.key_id).or_insert(([0u64; HISTOGRAM_SLOTS], 0, 0));
        if bucket < HISTOGRAM_SLOTS {
            entry.0[bucket] = entry.0[bucket].saturating_add(1);
        }
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.saturating_add(ev.value as u64);
    }

    let mut payload = Vec::new();
    payload.push(4u8); // version
    let events_commit = events_commit(events);
    payload.extend_from_slice(&events_commit);
    let out_commit = sha256_bytes(&[b"ZKTLM_SERIES_HIST_OUT_V4", &events_commit]);
    payload.extend_from_slice(&out_commit);

    let total_slots = series_ht_buckets * series_ht_bucket_cap;
    let mut it = per_key.into_iter();
    for _ in 0..total_slots {
        if let Some((k, (buckets, total_count, total_sum))) = it.next() {
            payload.push(1u8); // occ
            payload.extend_from_slice(&k);  // Full 15-byte key
            payload.extend_from_slice(&total_count.to_be_bytes());
            payload.extend_from_slice(&total_sum.to_be_bytes());
            for i in 0..HISTOGRAM_SLOTS {
                payload.extend_from_slice(&buckets[i].to_be_bytes());
            }
        } else {
            payload.push(0u8);
            payload.extend_from_slice(&[0u8; KEY_BYTES_LEN]);  // Empty 15-byte key
            payload.extend_from_slice(&0u64.to_be_bytes());
            payload.extend_from_slice(&0u64.to_be_bytes());
            for _ in 0..HISTOGRAM_SLOTS {
                payload.extend_from_slice(&0u64.to_be_bytes());
            }
        }
    }
    payload
}

#[allow(dead_code)]
fn write_fake_raw_shards(
    raw_db: &RocksDb,
    source_ids: &[u32],
    start_seq: i64,
    end_seq: i64,
    mode: &str,
    series: u64,
    samples_per_series: u64,
    series_per_shard: u64,
    seed: u64,
    _distinct_keys_per_source: bool,
) -> anyhow::Result<()> {
    let partitions = (series + series_per_shard - 1) / series_per_shard;
    anyhow::ensure!(partitions >= 1, "partitions must be >= 1");
    anyhow::ensure!(partitions <= (i16::MAX as u64), "too many partitions for i16 shard_id");

    let mut shard_prev: std::collections::HashMap<i16, [u8; 32]> =
        std::collections::HashMap::new();
    if start_seq > 0 {
        let prev_seq = start_seq - 1;
        if let Some(f) = raw_db.sample_shard_frame(prev_seq)? {
            // For now, store under shard_id 0
            shard_prev.insert(0, f.chain_hash);
        }
    }

    for seq in start_seq..=end_seq {
        let ingest_time_ms = now_ms();
        let mut batch = WriteBatch::default();

        for (src_idx, &_source_id) in source_ids.iter().enumerate() {
            let events = generate_epoch_events(
                mode,
                src_idx as u32,
                series,
                samples_per_series,
                seed
                    .saturating_add(seq as u64)
                    .saturating_add(src_idx as u64),
            );

            let mut per_shard: Vec<Vec<Event>> = (0..partitions).map(|_| Vec::new()).collect();
            for ev in events {
                let shard_id = partition_id_for_key(&ev.key_id, partitions) as usize;
                per_shard[shard_id].push(ev);
            }

            for shard_u in 0..partitions {
                let shard_id = shard_u as i16;
                let events = &per_shard[shard_u as usize];
                let mut idx_local: i32 = 0;
                for ev in events {
                    let event = SampleEvent {
                        sequence: seq,
                        ingest_time_ms,
                        idx: idx_local,
                        key_id: ev.key_id,
                        value: ev.value,
                        ts: ev.ts,
                    };
                    idx_local = idx_local.saturating_add(1);
                    raw_db.put_sample_event(&mut batch, &event)?;
                }

                let prev = shard_prev.get(&shard_id).copied().unwrap_or([0u8; 32]);
                let events_commit = events_commit(events);
                let chain_hash = shard_chain_hash(prev, events_commit);
                shard_prev.insert(shard_id, chain_hash);

                let frame = SampleShardFrame {
                    sequence: seq,
                    ingest_time_ms,
                    payload: Vec::new(),
                    chain_prev: prev,
                    chain_hash,
                    proof_kind: 0,
                    num_steps: 0,
                    proof: Vec::new(),
                };
                raw_db.put_sample_shard_frame(&mut batch, &frame)?;
            }
        }
        raw_db.write_batch(batch)?;
    }
    Ok(())
}

fn write_fake_series_hist_shards(
    db: &RocksDb,
    source_ids: &[u32],
    start_seq: i64,
    end_seq: i64,
    series: u64,
    samples_per_series: u64,
    series_per_shard: u64,
    seed: u64,
    _distinct_keys_per_source: bool,
) -> anyhow::Result<()> {
    let partitions = (series + series_per_shard - 1) / series_per_shard;
    anyhow::ensure!(partitions >= 1, "partitions must be >= 1");
    anyhow::ensure!(partitions <= (i16::MAX as u64), "too many partitions for i16 shard_id");

    let mut shard_prev: std::collections::HashMap<i16, [u8; 32]> =
        std::collections::HashMap::new();
    if start_seq > 0 {
        let prev_seq = start_seq - 1;
        if let Some(f) = db.series_shard_frame(prev_seq, EpochType::HistogramEpoch)? {
            // For now, store under shard_id 0
            shard_prev.insert(0, f.chain_hash);
        }
    }

    for seq in start_seq..=end_seq {
        let ingest_time_ms = now_ms();
        let mut batch = WriteBatch::default();

        for (src_idx, &_source_id) in source_ids.iter().enumerate() {
            let events = generate_epoch_events(
                "histogram",
                src_idx as u32,
                series,
                samples_per_series,
                seed
                    .saturating_add(seq as u64)
                    .saturating_add(src_idx as u64),
            );

            let mut per_shard: Vec<Vec<Event>> = (0..partitions).map(|_| Vec::new()).collect();
            for ev in events {
                let shard_id = partition_id_for_key(&ev.key_id, partitions) as usize;
                per_shard[shard_id].push(ev);
            }

            for shard_u in 0..partitions {
                let shard_id = shard_u as i16;
                let events = &per_shard[shard_u as usize];
                let payload = build_series_hist_payload_v4(&events);
                let prev = shard_prev.get(&shard_id).copied().unwrap_or([0u8; 32]);
                let events_commit = events_commit(events);
                let chain_hash = shard_chain_hash(prev, events_commit);
                shard_prev.insert(shard_id, chain_hash);

                let frame = SeriesShardFrame {
                    sequence: seq,
                    epoch_type: EpochType::HistogramEpoch,
                    ingest_time_ms,
                    payload,
                    chain_prev: prev,
                    chain_hash,
                    proof_kind: 0,
                    num_steps: 0,
                    proof: Vec::new(),
                };
                db.put_series_shard_frame(&mut batch, &frame)?;
            }
        }
        db.write_batch(batch)?;
    }
    Ok(())
}

/// Generate real `epoch_batches` rows into a (shared) raw RocksDB, with valid
/// per-source SHA-256 chains threaded continuously across the whole seq range —
/// the same layout the Kafka consumer would produce. This is the durable,
/// shared source of truth the real (non-`--fake-epochs`) aggregator path reads
/// via `get_epoch_batches`, so an online reshard against this store exercises
/// genuine cross-aggregator chain verification (not the in-process synthetic
/// generator).
fn run_gen_raw_epochs() -> anyhow::Result<()> {
    let raw_db_path = parse_arg_string("--raw-rocksdb-path")
        .or_else(|| std::env::var("RAW_ROCKSDB_PATH").ok())
        .context("--gen-raw-epochs requires --raw-rocksdb-path")?;
    let mode = parse_arg_str("--mode", "samples");
    let start_seq = parse_arg_opt_i64("--start-seq").unwrap_or(0);
    let end_seq = parse_arg_opt_i64("--end-seq").unwrap_or(start_seq);
    anyhow::ensure!(end_seq >= start_seq, "--end-seq must be >= --start-seq");
    let source_ids_str = parse_arg_string("--source-ids")
        .or_else(|| env_string("SOURCE_IDS"))
        .unwrap_or_else(|| "1".to_string());
    let source_ids = parse_csv_u32_list(&source_ids_str).context("parse --source-ids")?;
    let num_sources = source_ids.len() as u32;
    let series = parse_arg_u64("--series", 8).max(1);
    let samples_per_series = parse_arg_u64("--samples-per-series", 4).max(1);
    let commit_batch_size = parse_arg_u64("--commit-batch-size", 8).max(1);
    let seed = parse_arg_u64("--seed", 1);

    if let Some(parent) = std::path::Path::new(&raw_db_path).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let raw_db = RocksDb::open(&raw_db_path).context("open raw rocksdb for gen-raw-epochs")?;

    let mut prev_tips: BTreeMap<u32, [u8; 32]> = BTreeMap::new();
    let mut prev_batch_seqs: BTreeMap<u32, u64> = BTreeMap::new();
    let mut total_batches = 0usize;
    for seq in start_seq..=end_seq {
        let (batches, new_tips, new_batch_seqs) = generate_epoch_batches(
            &mode,
            series,
            samples_per_series,
            commit_batch_size,
            num_sources,
            seed.saturating_add(seq as u64),
            &prev_tips,
            &prev_batch_seqs,
        );
        let mut wb = WriteBatch::default();
        raw_db.put_epoch_batches(&mut wb, seq, &batches)?;
        raw_db.write_batch(wb)?;
        total_batches += batches.len();
        prev_tips = new_tips;
        prev_batch_seqs = new_batch_seqs;
        eprintln!(
            "[gen-raw-epochs] seq={} sources={} batches={}",
            seq, num_sources, batches.len()
        );
    }
    eprintln!(
        "[gen-raw-epochs] done: raw_db={} seqs=[{},{}] sources={} total_batches={}",
        raw_db_path, start_seq, end_seq, num_sources, total_batches
    );
    Ok(())
}

fn run_gen_raw_shards() -> anyhow::Result<()> {
    // Generate a raw-dispatch RocksDB layout:
    // - sample_events keyed by (source_id, seq, shard_id)
    // - sample_shard_frames keyed by (source_id, seq, shard_id), with a simple hash-chain
    //
    // Shard assignment:
    // - keys are (optionally) offset per source, then assigned via `partition_id_for_key(hash(key_id) % P)`
    // - P = ceil(SERIES / SERIES_PER_AGG) (fixed for the run)
    //
    // This models a dispatcher that routes committed raw events to per-(source,partition) aggregators.
    let raw_db_path = parse_arg_string("--raw-rocksdb-path")
        .or_else(|| std::env::var("RAW_ROCKSDB_PATH").ok())
        .unwrap_or_else(|| "/mydata/rocksdb_raw".to_string());
    let start_seq = parse_arg_opt_i64("--start-seq")
        .or_else(|| env_string("START_SEQ").and_then(|v| v.parse::<i64>().ok()))
        .unwrap_or(0);
    let end_seq = parse_arg_opt_i64("--end-seq")
        .or_else(|| env_string("END_SEQ").and_then(|v| v.parse::<i64>().ok()))
        .unwrap_or(start_seq);
    anyhow::ensure!(end_seq >= start_seq, "--end-seq must be >= --start-seq");

    let source_ids = parse_arg_string("--source-ids")
        .or_else(|| env_string("SOURCE_IDS"))
        .unwrap_or_else(|| "1".to_string());
    let source_ids = parse_csv_u32_list(&source_ids).context("parse --source-ids/SOURCE_IDS")?;
    anyhow::ensure!(!source_ids.is_empty(), "no source_ids provided");

    let mode = parse_arg_str("--mode", "samples");
    let series = parse_arg_u64("--series", 1024).max(1);
    let samples_per_series = parse_arg_u64("--samples-per-series", 8).max(1);
    let series_per_agg = parse_arg_u64("--series-per-agg", 1024).max(1);
    let seed = parse_arg_u64("--seed", 1);
    let distinct_keys_per_source = has_flag("--distinct-keys-per-source")
        || env_u8("DISTINCT_KEYS_PER_SOURCE", 1) != 0;

    let partitions = (series.saturating_add(series_per_agg - 1)) / series_per_agg;
    anyhow::ensure!(partitions <= (i16::MAX as u64), "too many partitions: {}", partitions);

    if let Some(parent) = std::path::Path::new(&raw_db_path).parent() {
        std::fs::create_dir_all(parent).context("create RAW_ROCKSDB_PATH parent")?;
    }
    let db = RocksDb::open(&raw_db_path).context("open RAW_ROCKSDB_PATH")?;

    // Track last chain hash per shard so we can link frames across seq.
    let mut last_hash: std::collections::HashMap<i16, [u8; 32]> =
        std::collections::HashMap::new();
    if start_seq > 0 {
        let prev_seq = start_seq - 1;
        if let Some(f) = db
            .sample_shard_frame(prev_seq)
            .with_context(|| format!("read sample_shard_frame for chain seed seq={prev_seq}"))?
        {
            // Store under shard_id 0
            last_hash.insert(0, f.chain_hash);
        }
    }

    // Global zipf: when enabled, zipf distribution spans all num_sources * series keys
    // instead of being per-source. This creates truly global hot keys.
    let global_key_zipf = has_flag("--global-key-zipf") || env_u8("GLOBAL_KEY_ZIPF", 0) != 0;
    let num_sources = source_ids.len() as u64;

    for seq in start_seq..=end_seq {
        let ingest_time_ms = now_ms();
        let mut batch = WriteBatch::default();

        // When global_key_zipf is enabled and KEY_ZIPF_S is set, generate all events
        // with keys using global zipf distribution across all sources.
        // Keys are converted to (source_id, key_index) pairs via make_key_id.
        let global_events: Option<Vec<Event>> = if global_key_zipf && env_string("KEY_ZIPF_S").is_some() {
            let total_samples = num_sources.saturating_mul(series).saturating_mul(samples_per_series);
            Some(generate_epoch_events_global_zipf(
                &mode,
                num_sources,
                series,
                total_samples,
                seed.saturating_add(seq as u64),
            ))
        } else {
            None
        };

        for (src_idx, &source_id) in source_ids.iter().enumerate() {
            let _key_base = if distinct_keys_per_source {
                (src_idx as u64).saturating_mul(series)
            } else {
                0u64
            };

            // Generate events for this source
            let events: Vec<Event> = if let Some(ref all_events) = global_events {
                // Global zipf: filter events belonging to this source by extracting source_id
                all_events
                    .iter()
                    .filter(|ev| {
                        Event::extract_source_id(&ev.key_id) == src_idx as u32
                    })
                    .cloned()
                    .collect()
            } else {
                // Per-source generation (original behavior)
                generate_epoch_events(
                    &mode,
                    src_idx as u32,
                    series,
                    samples_per_series,
                    seed
                        .saturating_add(seq as u64)
                        .saturating_add(source_id as u64),
                )
            };

            let mut per_shard: Vec<Vec<Event>> = (0..partitions)
                .map(|_| Vec::new())
                .collect();
            for ev in events {
                let shard = partition_id_for_key(&ev.key_id, partitions) as usize;
                per_shard[shard].push(ev);
            }

            for shard_u in 0..partitions {
                let shard_id = shard_u as i16;
                let prev = *last_hash.get(&shard_id).unwrap_or(&[0u8; 32]);
                let shard_events = &per_shard[shard_u as usize];

                // Persist sample_events for this shard.
                for (i, ev) in shard_events.iter().enumerate() {
                    let idx_unique: i32 = (shard_id as i32)
                        .saturating_mul(1_000_000)
                        .saturating_add(i as i32);
                    db.put_sample_event(
                        &mut batch,
                        &SampleEvent {
                            sequence: seq,
                            ingest_time_ms,
                            idx: idx_unique,
                            key_id: ev.key_id,
                            value: ev.value,
                            ts: ev.ts,
                        },
                    )?;
                }

                // Persist a shard frame with a lightweight commitment chain.
                let events_commit = events_commit(shard_events);
                let chain_hash = shard_chain_hash(prev, events_commit);
                last_hash.insert(shard_id, chain_hash);
                db.put_sample_shard_frame(
                    &mut batch,
                    &SampleShardFrame {
                        sequence: seq,
                        ingest_time_ms,
                        payload: Vec::new(),
                        chain_prev: prev,
                        chain_hash,
                        proof_kind: 0,
                        num_steps: 0,
                        proof: Vec::new(),
                    },
                )?;
            }
        }
        db.write_batch(batch).context("write raw shard batch")?;
        eprintln!(
            "[risc0-aggr][gen-raw] seq={} sources={} series={} samples_per_series={} series_per_agg={} partitions_per_source={}",
            seq,
            source_ids.len(),
            series,
            samples_per_series,
            series_per_agg,
            partitions
        );
    }
    Ok(())
}

#[allow(dead_code)]
fn verify_sample_shard_chain(
    raw_db: &RocksDb,
    start_seq: i64,
    end_seq: i64,
) -> anyhow::Result<()> {
    let mut last_hash: Option<[u8; 32]> = None;

    for seq in start_seq..=end_seq {
        let Some(frame) = raw_db
            .sample_shard_frame(seq)
            .with_context(|| format!("read sample_shard_frame seq={seq}"))?
        else {
            // Skip missing frames (distributed mode may not have all sequences)
            continue;
        };

        let expected_prev = if let Some(prev) = last_hash {
            Some(prev)
        } else if seq > 0 {
            raw_db
                .sample_shard_frame(seq - 1)?
                .map(|f| f.chain_hash)
        } else {
            None
        };

        if let Some(prev) = expected_prev {
            anyhow::ensure!(
                frame.chain_prev == prev,
                "hash-chain link mismatch seq={} (chain_prev != prior chain_hash)",
                seq
            );
        }
        last_hash = Some(frame.chain_hash);
    }
    Ok(())
}

fn inspect_raw_db(raw_db: &RocksDb, filter_sources: &[u32]) -> anyhow::Result<()> {
    
    eprintln!("[risc0-aggr][inspect-raw] scanning sample_shard_frames + sample_events...");

    let frames = raw_db.sample_shard_frames().context("scan sample_shard_frames")?;
    let events = raw_db.sample_events().context("scan sample_events")?;

    let want_all = filter_sources.is_empty();
    let _want = |src: u32| want_all || filter_sources.contains(&src);

    #[derive(Clone, Copy, Debug, Default)]
    struct Stats {
        count: u64,
        min_seq: i64,
        max_seq: i64,
    }
    impl Stats {
        fn new(seq: i64) -> Self {
            Self {
                count: 1,
                min_seq: seq,
                max_seq: seq,
            }
        }
        fn add(&mut self, seq: i64) {
            self.count = self.count.saturating_add(1);
            self.min_seq = self.min_seq.min(seq);
            self.max_seq = self.max_seq.max(seq);
        }
    }

    let mut frame_stats: Stats = Stats::new(0);
    for f in frames {
        frame_stats.add(f.sequence);
    }

    let mut event_stats: Stats = Stats::new(0);
    for e in events {
        event_stats.add(e.sequence);
    }

    eprintln!("[risc0-aggr][inspect-raw] sample_shard_frames:");
    if frame_stats.count == 0 {
        eprintln!("  (none)");
    } else {
        eprintln!(
            "  frames={} seq_range=[{},{}]",
            frame_stats.count, frame_stats.min_seq, frame_stats.max_seq
        );
    }

    eprintln!("[risc0-aggr][inspect-raw] sample_events:");
    if event_stats.count == 0 {
        eprintln!("  (none)");
    } else {
        eprintln!(
            "  events={} seq_range=[{},{}]",
            event_stats.count, event_stats.min_seq, event_stats.max_seq
        );
    }

    Ok(())
}

#[allow(dead_code)]
fn shard_has_any_frame(
    raw_db: &RocksDb,
    _source_id: u32,
    _shard_id: i16,
    start_seq: i64,
    end_seq: i64,
) -> anyhow::Result<bool> {
    for seq in start_seq..=end_seq {
        if raw_db.sample_shard_frame(seq)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn run_rocksdb_pipeline(mode: &str, skip_receipt_verify: bool) -> anyhow::Result<()> {
    let no_zkvm_proof =
        has_flag("--no-zkvm-proof") || env_u8("NO_ZKVM_PROOF", 0) != 0;
    let fake_epochs =
        has_flag("--fake-epochs") || env_u8("FAKE_EPOCHS", 0) != 0;
    let fake_raw_shards =
        has_flag("--fake-raw-shards") || env_u8("FAKE_RAW_SHARDS", 0) != 0;
    let fake_series_hist =
        has_flag("--fake-series-hist") || env_u8("FAKE_SERIES_HIST", 0) != 0;
    let raw_db_path = parse_arg_string("--raw-rocksdb-path")
        .or_else(|| std::env::var("RAW_ROCKSDB_PATH").ok())
        .unwrap_or_else(|| "/mydata/rocksdb".to_string());
    let agg_db_path = parse_arg_string("--agg-rocksdb-path")
        .or_else(|| std::env::var("AGG_ROCKSDB_PATH").ok())
        .unwrap_or_else(|| "/mydata/rocksdb_agg".to_string());

    // Aggregator ID for distributed deployments (prevents FDB key collisions)
    // Each aggregator should have a unique ID (typically matches KAFKA_PARTITION_ID)
    let aggregator_id: u32 = parse_arg_string("--aggregator-id")
        .or_else(|| std::env::var("AGGREGATOR_ID").ok())
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);

    // Online resharding (preview) opt-in. When set, the aggregator filters
    // each per-epoch (source_id, batches) pair through `current_owner_for_source`
    // and skips sources it does not currently own. When unset (the default),
    // behaviour is identical to the existing static-partitioning path — this
    // branch must NOT regress existing deployments.
    let use_online_ownership: bool =
        has_flag("--use-online-ownership") || env_u8("USE_ONLINE_OWNERSHIP", 0) != 0;
    if use_online_ownership {
        eprintln!(
            "[native-aggr][ownership] enabled: aggregator_id={} (sources not owned by this aggregator at the current epoch will be skipped)",
            aggregator_id
        );
    }

    let limit_events: Option<u64> = parse_arg_string("--limit-events")
        .or_else(|| env_string("LIMIT_EVENTS"))
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0);

    if let Some(parent) = std::path::Path::new(&raw_db_path).parent() {
        std::fs::create_dir_all(parent).context("create RAW_ROCKSDB_PATH parent")?;
    }
    if let Some(parent) = std::path::Path::new(&agg_db_path).parent() {
        std::fs::create_dir_all(parent).context("create AGG_ROCKSDB_PATH parent")?;
    }

    let agg_db = RocksDb::open(&agg_db_path).context("open agg rocksdb")?;

    // Optionally open FDB for distributed writes
    #[cfg(feature = "fdb")]
    let (fdb_store, fdb_runtime): (Option<FdbStore>, Option<tokio::runtime::Runtime>) = if std::env::var("FDB_CLUSTER_FILE").is_ok() {
        let rt = tokio::runtime::Runtime::new().context("create tokio runtime for FDB")?;
        let store = rt.block_on(FdbStore::open()).context("open FDB store")?;
        (Some(store), Some(rt))
    } else {
        (None, None)
    };

    #[cfg(not(feature = "fdb"))]
    let _fdb_store: Option<()> = None;

    if fake_epochs && fake_series_hist {
        // Fake data generation - single epoch (0)
        let series = parse_arg_u64("--series", 32).max(1);
        let samples_per_series = parse_arg_u64("--samples-per-series", 32).max(1);
        let series_per_shard = parse_arg_u64("--series-per-shard", series).max(1);
        let seed = parse_arg_u64("--seed", 0xA66A_1E);
        let distinct_keys_per_source =
            has_flag("--distinct-keys-per-source") || env_u8("DISTINCT_KEYS_PER_SOURCE", 1) != 0;
        write_fake_series_hist_shards(
            &agg_db,
            &[1],
            0, // start_seq
            0, // end_seq
            series,
            samples_per_series,
            series_per_shard,
            seed,
            distinct_keys_per_source,
        )?;
        eprintln!(
            "[risc0-aggr][rocksdb] fake_series_hist=1 wrote series_shard_frames epoch_type=histogram_epoch_per_key seq=[0,0] sources=1 shards_per_source={}",
            (series + series_per_shard - 1) / series_per_shard
        );
        return Ok(());
    }

    let raw_db = if !fake_epochs || fake_raw_shards {
        if let Ok(sec) = std::env::var("RAW_ROCKSDB_SECONDARY_PATH") {
            if !sec.trim().is_empty() {
                let db = RocksDb::open_secondary(&raw_db_path, &sec).context("open raw rocksdb secondary")?;
                eprintln!("[risc0-aggr] opened raw rocksdb as secondary, catching up...");
                db.catch_up_if_secondary().context("catch up secondary")?;
                eprintln!("[risc0-aggr] secondary caught up successfully");
                Some(db)
            } else {
                Some(RocksDb::open(&raw_db_path).context("open raw rocksdb")?)
            }
        } else {
            Some(RocksDb::open(&raw_db_path).context("open raw rocksdb")?)
        }
    } else {
        None
    };

    // Parse epoch generation parameters (used for fake_epochs mode)
    let start_seq = parse_arg_opt_i64("--start-seq")
        .or_else(|| env_string("START_SEQ").and_then(|v| v.parse::<i64>().ok()))
        .unwrap_or(0);
    let end_seq = parse_arg_opt_i64("--end-seq")
        .or_else(|| env_string("END_SEQ").and_then(|v| v.parse::<i64>().ok()))
        .unwrap_or(start_seq);
    // Optional cap on the highest epoch the real-mode poll loop will process
    // (the raw_db-derived range otherwise covers everything ingested). Used to
    // stop pre-boundary aggregators exactly at a reshard boundary so the new
    // owners process the boundary epoch with inherited tips.
    let max_process_seq: Option<i64> = parse_arg_opt_i64("--max-process-seq")
        .or_else(|| env_string("MAX_PROCESS_SEQ").and_then(|v| v.parse::<i64>().ok()));
    // Keep processed epoch_batches in raw_db instead of deleting them. Required
    // when several aggregators share one raw_db (each processes only the sources
    // it owns, so no single aggregator may consume the epoch for the others).
    let keep_raw_batches =
        has_flag("--keep-raw-batches") || env_u8("KEEP_RAW_BATCHES", 0) != 0;
    let source_ids_str = parse_arg_string("--source-ids")
        .or_else(|| env_string("SOURCE_IDS"))
        .unwrap_or_else(|| "1".to_string());
    let source_ids = parse_csv_u32_list(&source_ids_str).context("parse --source-ids/SOURCE_IDS")?;
    let series = parse_arg_u64("--series", 1024).max(1);
    let samples_per_series = parse_arg_u64("--samples-per-series", 8).max(1);
    let commit_batch_size = parse_arg_u64("--commit-batch-size", 8).max(1);
    let seed = parse_arg_u64("--seed", 1);

    // Idle timeout for poll-based epoch processing (default: 5 minutes)
    let idle_timeout_secs: u64 = std::env::var("AGGR_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let idle_timeout = std::time::Duration::from_secs(idle_timeout_secs);

    if has_flag("--inspect-raw") {
        let raw_db = raw_db.as_ref().context("--inspect-raw requires RAW_ROCKSDB_PATH (disable --fake-epochs)")?;
        inspect_raw_db(raw_db, &[])?;
        return Ok(());
    }


    // Note: fake_raw_shards mode is not supported with poll-based processing
    if fake_raw_shards {
        anyhow::bail!("--fake-raw-shards is not supported with poll-based epoch processing");
    }

    eprintln!(
        "[risc0-aggr][rocksdb] poll-based epoch processing enabled, idle_timeout={}s",
        idle_timeout_secs
    );

    // Track epochs proved for final summary
    let mut epochs_proved: usize = 0;
    let pipeline_start = std::time::Instant::now();

    let prover = if no_zkvm_proof { None } else { Some(default_prover()) };
    let mut prev_chain_hash = [0u8; 32];
    // Track per-source chain tips across epochs for cross-epoch verification
    // Format: (source_id, last_processed_seq, chain_tip)
    let mut prev_source_chain_tips: Vec<(u32, u64, [u8; 32])> = Vec::new();
    // Track per-source batch sequences for fake epochs (continuous across epochs)
    let mut prev_source_batch_seqs: BTreeMap<u32, u64> = BTreeMap::new();
    // Online-ownership scaffolding: track which sources THIS aggregator owned
    // in the previous epoch, so we can detect incoming/outgoing transitions.
    let mut prev_owned_sources: std::collections::BTreeSet<u32> =
        std::collections::BTreeSet::new();
    let (elf, image_id, out_epoch_type) = match mode {
        "samples" => (AGGR_SAMPLES_ELF, AGGR_SAMPLES_ID, EpochType::SamplesEpoch),
        "histogram" => (
            AGGR_HISTOGRAM_ELF,
            AGGR_HISTOGRAM_ID,
            EpochType::HistogramEpoch,
        ),
        "cm" => (AGGR_CM_ELF, AGGR_CM_ID, EpochType::CmEpoch),
        other => anyhow::bail!("unsupported --mode {other}; expected samples|histogram|cm"),
    };

    // Recover from any partial WriteBatch left over from a crash. Must run once
    // *before* the polling loop so subsequent epochs see a clean tombstone set.
    if let Err(e) = recover_partial_state(&agg_db, mode, out_epoch_type, aggregator_id) {
        eprintln!("[native-aggr][recover] WARNING: recovery scan failed: {e:#}");
    }

    // Poll-based epoch processing: find unprocessed epochs and process them one at a time
    // Exit after idle_timeout with no new epochs to process
    let mut last_work_time = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(500);

    loop {
        // Catch up secondary RocksDB if opened as secondary
        if let Some(ref raw_db) = raw_db {
            raw_db.catch_up_if_secondary().ok();
        }

        // Discover available epochs from raw_db (epoch_batches storage)
        let (min_seq, max_seq) = if let Some(ref raw_db) = raw_db {
            match raw_db.epoch_batches_seq_range()? {
                Some((min, max)) => (min, max),
                None => {
                    // No epochs in raw DB yet, wait and retry
                    if last_work_time.elapsed() >= idle_timeout {
                        eprintln!("[risc0-aggr] idle timeout reached ({}s), no epochs found in raw DB, exiting", idle_timeout.as_secs());
                        break;
                    }
                    std::thread::sleep(poll_interval);
                    continue;
                }
            }
        } else if fake_epochs {
            // In fake_epochs mode without raw_db, use explicit epoch range from command line
            anyhow::ensure!(start_seq <= end_seq, "--fake-epochs requires --start-seq and --end-seq");
            (start_seq, end_seq)
        } else {
            // No raw_db and not fake_epochs - nothing to do
            eprintln!("[risc0-aggr] no RAW_ROCKSDB_PATH set, exiting");
            break;
        };
        // Apply the optional processing cap (e.g. stop at a reshard boundary).
        let max_seq = match max_process_seq {
            Some(cap) => max_seq.min(cap),
            None => max_seq,
        };
        if max_seq < min_seq {
            // Everything in range is above the cap; nothing to do here.
            if last_work_time.elapsed() >= idle_timeout {
                eprintln!(
                    "[risc0-aggr] idle timeout reached ({}s), no epochs <= max-process-seq cap, exiting",
                    idle_timeout.as_secs()
                );
                break;
            }
            std::thread::sleep(poll_interval);
            continue;
        }

        // Find the first unprocessed epoch in the range
        let mut found_work = false;
        let mut seq = min_seq;

        // Log available vs processed epochs at discovery time
        // "Processed" is now defined by the explicit EpochTombstone, not by the
        // mere presence of agg_epoch (which can exist after a partial-write crash).
        {
            let mut already_processed = Vec::new();
            let mut unprocessed = Vec::new();
            for s in min_seq..=max_seq {
                if agg_db
                    .has_epoch_tombstone(out_epoch_type, s, aggregator_id)
                    .unwrap_or(false)
                {
                    already_processed.push(s);
                } else {
                    unprocessed.push(s);
                }
            }
            eprintln!(
                "[risc0-aggr][poll] raw_db epoch range=[{},{}] | already_processed={:?} | unprocessed={:?}",
                min_seq, max_seq, already_processed, unprocessed
            );
        }

        while seq <= max_seq {
            // Check if this epoch is already processed. Use the explicit tombstone:
            // an epoch is "done" iff its tombstone exists (i.e. the entire WriteBatch
            // committed atomically).
            if agg_db.has_epoch_tombstone(out_epoch_type, seq, aggregator_id)? {
                seq += 1;
                continue;
            }

            // Found unprocessed epoch
            found_work = true;
            last_work_time = std::time::Instant::now();

            eprintln!("[risc0-aggr] processing epoch seq={} (range=[{},{}])", seq, min_seq, max_seq);

            // E2E component timing: reset per-epoch accumulator, start total timer.
            etime::reset();
            let epoch_t0 = std::time::Instant::now();

            // (2) Fetch raw datapoints for this epoch from epoch_batches.
            let mut events: Vec<Event> = Vec::new();
            let mut ingest_time_ms_max: i64 = 0;
            let _raw_read_t0 = std::time::Instant::now();
            let epoch_batches: Vec<StoredEventBatch> = if let Some(raw_db) = raw_db.as_ref() {
                match raw_db.get_epoch_batches(seq)? {
                    Some(batches) => {
                        for batch in &batches {
                            ingest_time_ms_max = ingest_time_ms_max.max(batch.ingest_time_ms);
                            for ev in &batch.events {
                                events.push(Event {
                                    ts: ev.ts,
                                    key_id: ev.key_id,
                                    value: ev.value,
                                });
                            }
                        }
                        batches
                    }
                    None => Vec::new(),
                }
            } else if fake_epochs {
                // Generate fake epoch batches (no RocksDB needed)
                // Convert prev_source_chain_tips Vec to BTreeMap for generate_epoch_batches
                let prev_tips_map: BTreeMap<u32, [u8; 32]> = prev_source_chain_tips
                    .iter()
                    .map(|(source_id, _seq, tip)| (*source_id, *tip))
                    .collect();

                let (batches, _new_tips, new_batch_seqs) = generate_epoch_batches(
                    mode,
                    series,
                    samples_per_series,
                    commit_batch_size,
                    source_ids.len() as u32,
                    seed.saturating_add(seq as u64),
                    &prev_tips_map,
                    &prev_source_batch_seqs,
                );

                // Update batch sequences for next epoch
                prev_source_batch_seqs = new_batch_seqs;

                // Extract events for downstream processing
                for batch in &batches {
                    ingest_time_ms_max = ingest_time_ms_max.max(batch.ingest_time_ms);
                    for ev in &batch.events {
                        events.push(Event {
                            ts: ev.ts,
                            key_id: ev.key_id,
                            value: ev.value,
                        });
                    }
                }

                batches
            } else {
                Vec::new()
            };
            // RocksDB raw-batch read time for this epoch (Kafka -> RocksDB buffer
            // is done by the kafka-consumer process; this is the aggregator's read).
            etime::add_ms("rocksdb_raw_read", _raw_read_t0.elapsed().as_secs_f64() * 1000.0);

            // Online resharding (preview): filter (source_id, batches) by the
            // active OwnershipEpoch at this seq. Sources not owned by this
            // aggregator are skipped for the epoch; their batches will be
            // claimed by the new owner. This is intentionally a *post-load*
            // filter so the existing static-partitioning code paths are
            // bit-for-bit unchanged when --use-online-ownership is unset.
            let mut owned_sources_this_epoch: std::collections::BTreeSet<u32> =
                std::collections::BTreeSet::new();
            let epoch_batches: Vec<StoredEventBatch> = if use_online_ownership {
                let mut sources_seen: std::collections::BTreeSet<u32> =
                    std::collections::BTreeSet::new();
                for b in &epoch_batches {
                    sources_seen.insert(b.source_id);
                }
                let mut owner_for: std::collections::BTreeMap<u32, u32> =
                    std::collections::BTreeMap::new();
                for sid in &sources_seen {
                    let owner = current_owner_for_source(&agg_db, *sid, seq, aggregator_id)
                        .unwrap_or(aggregator_id);
                    owner_for.insert(*sid, owner);
                    if owner == aggregator_id {
                        owned_sources_this_epoch.insert(*sid);
                    } else {
                        eprintln!(
                            "[native-aggr][ownership] seq={} source_id={} skipped (owner={})",
                            seq, sid, owner
                        );
                    }
                }
                // Incoming sources: ones we own this epoch but did NOT own last
                // epoch. Rather than writing a zero "bootstrap" handoff (which
                // would silently fork the per-source chain), INHERIT the chain
                // tip the previous owner published in the authoritative Handoff
                // row at (seq, source_id). The handoff key is (at_epoch,
                // source_id), so there is exactly one authoritative row per
                // source per boundary, written by the losing owner. If none
                // exists this is a genuine cold start (no prior owner).
                for sid in &owned_sources_this_epoch {
                    if !prev_owned_sources.contains(sid) {
                        // Resolve the source's durable chain tip. Prefer the
                        // per-source tip record (general X->Y: works for any
                        // previous owner and for a kept source after a restart);
                        // fall back to an explicit Handoff row. Returns
                        // (previous_owner, last_seq, chain_tip).
                        let inherited: Option<(u32, i64, [u8; 32])> =
                            match agg_db.get_source_tip(*sid) {
                                Ok(Some(t)) if t.chain_tip != [0u8; 32] && t.last_seq >= 0 => {
                                    Some((t.owner, t.last_seq, t.chain_tip))
                                }
                                _ => match agg_db.handoff_at(seq, *sid) {
                                    Ok(Some(h)) if h.chain_tip != [0u8; 32] && h.last_seq >= 0 => {
                                        Some((h.from_aggregator, h.last_seq, h.chain_tip))
                                    }
                                    _ => None,
                                },
                            };
                        match inherited {
                            Some((from, last_seq, tip)) => {
                                // Seed the chain tip so the new owner's first
                                // batch is verified to chain from it instead of
                                // restarting from zero. Real (raw_db) path only:
                                // the `--fake-epochs` generator owns its own
                                // self-contained per-source batch numbering, so
                                // seeding it would desync — there we inherit at
                                // the coordination layer only.
                                if !fake_epochs {
                                    prev_source_chain_tips.retain(|(s, _, _)| s != sid);
                                    prev_source_chain_tips.push((*sid, last_seq as u64, tip));
                                }
                                if from != aggregator_id {
                                    // Genuine cross-aggregator move: record an
                                    // auditable Handoff so chain-inspector can
                                    // verify continuity across the boundary.
                                    let h = Handoff {
                                        source_id: *sid,
                                        at_epoch: seq,
                                        from_aggregator: from,
                                        to_aggregator: aggregator_id,
                                        chain_tip: tip,
                                        last_seq,
                                        published_at_ms: now_ms(),
                                    };
                                    if let Err(e) = agg_db.put_handoff(&h) {
                                        eprintln!(
                                            "[native-aggr][ownership] seq={} source_id={} put_handoff(audit) error: {:?}",
                                            seq, sid, e
                                        );
                                    }
                                    eprintln!(
                                        "[native-aggr][ownership] seq={} source_id={} incoming: inherited chain_tip (from_aggregator={}, last_seq={}){}",
                                        seq, sid, from, last_seq,
                                        if fake_epochs { " [coordination-only]" } else { "" }
                                    );
                                } else {
                                    // Kept source reloaded across a restart — not
                                    // a handoff.
                                    eprintln!(
                                        "[native-aggr][ownership] seq={} source_id={} reloaded own chain_tip (kept, last_seq={})",
                                        seq, sid, last_seq
                                    );
                                }
                            }
                            None => {
                                eprintln!(
                                    "[native-aggr][ownership] seq={} source_id={} incoming: cold start (no tip to inherit)",
                                    seq, sid
                                );
                            }
                        }
                    }
                }
                // Detect outgoing handoffs: sources we owned last epoch but
                // do NOT own this epoch. Look up the next owner from the
                // OwnershipEpoch active at `seq`.
                for sid in &prev_owned_sources {
                    if !owned_sources_this_epoch.contains(sid) {
                        let next_owner = current_owner_for_source(&agg_db, *sid, seq, u32::MAX)
                            .unwrap_or(u32::MAX);
                        eprintln!(
                            "[native-aggr][ownership] seq={} source_id={} outgoing (to next_owner={})",
                            seq, sid, next_owner
                        );
                        // Recover chain_tip from the per-source state we just
                        // emitted at the end of the previous epoch.
                        let (last_seq, tip) = prev_source_chain_tips
                            .iter()
                            .find(|(s, _, _)| s == sid)
                            .map(|(_, ls, t)| (*ls as i64, *t))
                            .unwrap_or((-1, [0u8; 32]));
                        let h = Handoff {
                            source_id: *sid,
                            at_epoch: seq,
                            from_aggregator: aggregator_id,
                            to_aggregator: next_owner,
                            chain_tip: tip,
                            last_seq,
                            published_at_ms: now_ms(),
                        };
                        if let Err(e) = agg_db.put_handoff(&h) {
                            eprintln!(
                                "[native-aggr][ownership] seq={} source_id={} put_handoff(outgoing) error: {:?}",
                                seq, sid, e
                            );
                        }
                    }
                }

                // The authoritative filter is on epoch_batches (which carry
                // source_id directly). We rebuild flat `events` below from
                // the kept batches so downstream code is consistent.
                let owned = owned_sources_this_epoch.clone();
                let kept: Vec<StoredEventBatch> = epoch_batches
                    .into_iter()
                    .filter(|b| owned.contains(&b.source_id))
                    .collect();
                // Rebuild flat `events` from kept batches (mirrors the loader).
                events.clear();
                for batch in &kept {
                    for ev in &batch.events {
                        events.push(Event {
                            ts: ev.ts,
                            key_id: ev.key_id,
                            value: ev.value,
                        });
                    }
                }
                kept
            } else {
                epoch_batches
            };

            if let Some(max) = limit_events {
                if (events.len() as u64) > max {
                    events.truncate(max as usize);
                }
            }
            if events.is_empty() {
                if use_online_ownership {
                    eprintln!(
                        "[native-aggr][ownership] seq={} no events for this aggregator after ownership filter; skipping",
                        seq
                    );
                    // Tombstone the skipped epoch so it is recorded as "done"
                    // (this aggregator owns nothing here). Without it, a shared
                    // raw_db (kept, not consumed) would re-surface this epoch on
                    // every poll pass — re-running the ownership filter with a
                    // stale prev_owned_sources and emitting spurious outgoing
                    // handoffs, and never making progress.
                    let mut tomb = WriteBatch::default();
                    if let Err(e) = agg_db.put_epoch_tombstone(
                        &mut tomb,
                        &EpochTombstone {
                            epoch_type: out_epoch_type,
                            sequence: seq,
                            aggregator_id,
                            completed_at_ms: now_ms(),
                        },
                    ) {
                        eprintln!("[native-aggr][ownership] seq={} tombstone(skip) error: {:?}", seq, e);
                    } else {
                        let _ = agg_db.write_batch(tomb);
                    }
                    // Roll the owned-sources tracker so outgoing detection still works.
                    prev_owned_sources = owned_sources_this_epoch;
                } else {
                    eprintln!("[risc0-aggr][rocksdb] seq={} no events; skipping", seq);
                }
                seq += 1;
                continue;
            }
            let ingest_time_ms = if ingest_time_ms_max > 0 {
                ingest_time_ms_max
            } else {
                now_ms()
            };

            // Compute min/max timestamps from events for epoch indexing
            let (min_ts, max_ts) = if events.is_empty() {
                (0u32, 0u32)
            } else {
                let min = events.iter().map(|e| e.ts).min().unwrap_or(0);
                let max = events.iter().map(|e| e.ts).max().unwrap_or(0);
                (min, max)
            };

            let prove_start = std::time::Instant::now();
            let mut batch = WriteBatch::default();
            let mut receipt_words: Vec<u32> = Vec::new();

            // Collect items for FDB write (when enabled)
            #[cfg(feature = "fdb")]
            let mut fdb_epoch: Option<AggEpoch> = None;
            #[cfg(feature = "fdb")]
            let mut fdb_epoch_meta: Option<AggEpochMeta> = None;
            #[cfg(feature = "fdb")]
            let mut fdb_epoch_proof: Option<AggEpochProof> = None;
            #[cfg(feature = "fdb")]
            let mut fdb_cm_struct: Option<AggCmStruct> = None;
            #[cfg(feature = "fdb")]
            let mut fdb_hist_struct: Option<AggHistStruct> = None;
            #[cfg(feature = "fdb")]
            let mut fdb_verified_samples: Option<VerifiedSamplesStruct> = None;
            #[cfg(feature = "fdb")]
            let mut fdb_tombstone: Option<EpochTombstone> = None;

            // Build batch inputs for ZK verification with proper hash verification
            // Uses build_batch_inputs which converts StoredEventBatch to BatchInput
            // Extract just (source_id, tip) for build_batch_inputs (seq is only used in ZK guest)
            let prev_source_tips_map: BTreeMap<u32, [u8; 32]> = prev_source_chain_tips
                .iter()
                .map(|(source_id, _seq, tip)| (*source_id, *tip))
                .collect();
            let (batch_inputs, _new_source_chain_tips_map) = build_batch_inputs(&epoch_batches, &prev_source_tips_map);

            // Log epoch summary before guest processing
            {
                use std::collections::HashSet;
                let unique_sources: HashSet<u32> = batch_inputs.iter().map(|b| b.source_id).collect();
                eprintln!(
                    "[risc0-aggr] === Epoch seq={} mode={} sources={} batches={} events={} ===",
                    seq, mode, unique_sources.len(), batch_inputs.len(), events.len()
                );
            }

            // === Pre-processing source status: expected vs incoming batch sequences ===
            {
                use std::collections::BTreeMap;
                // Build map of expected_next_seq per source (from previous epoch state)
                let expected_seqs: BTreeMap<u32, u64> = prev_source_chain_tips
                    .iter()
                    .map(|(sid, last_seq, _)| (*sid, *last_seq + 1))
                    .collect();
                // Collect incoming batch seq ranges per source
                let mut incoming_stats: BTreeMap<u32, (u64, u64, usize)> = BTreeMap::new(); // (min_seq, max_seq, count)
                for b in &batch_inputs {
                    incoming_stats
                        .entry(b.source_id)
                        .and_modify(|(min, max, cnt)| {
                            *min = (*min).min(b.source_batch_seq);
                            *max = (*max).max(b.source_batch_seq);
                            *cnt += 1;
                        })
                        .or_insert((b.source_batch_seq, b.source_batch_seq, 1));
                }
                eprintln!("[risc0-aggr][pre-check] --- Epoch seq={} incoming batch status ---", seq);
                for (source_id, (min_seq, max_seq, count)) in &incoming_stats {
                    let expected = expected_seqs.get(source_id).copied().unwrap_or(0);
                    let status = if *min_seq == expected {
                        "OK"
                    } else if *min_seq > expected {
                        "GAP!"
                    } else {
                        "OVERLAP"
                    };
                    eprintln!(
                        "[risc0-aggr][pre-check]   source_id={:3} | expected_seq={:4} | incoming_range=[{},{}] ({} batches) | {}",
                        source_id, expected, min_seq, max_seq, count, status
                    );
                }
                // Warn about sources with prior state but no incoming batches
                for (source_id, expected) in &expected_seqs {
                    if !incoming_stats.contains_key(source_id) {
                        eprintln!(
                            "[risc0-aggr][pre-check]   source_id={:3} | expected_seq={:4} | NO INCOMING BATCHES",
                            source_id, expected
                        );
                    }
                }
            }

        match mode {
            "samples" => {
                let input = SamplesAggrInput {
                    prev_chain_hash,
                    batches: batch_inputs.clone(),
                    prev_source_chain_tips: prev_source_chain_tips.clone(),
                };
                let (state, expected) = {
                    let _g = etime::Guard::new("aggr_compute");
                    process_samples_aggr_with_state(&input)
                };
                let out: SamplesAggrOutput = if let Some(prover) = prover.as_ref() {
                    let input_bytes = to_vec(&input)?.len();
                    log_epoch_details_batches("samples", seq, &input.batches, input_bytes);
                    let env = ExecutorEnv::builder().write(&input)?.build()?;
                    let prove_start = std::time::Instant::now();
                    let prove_info = prover.prove_with_opts(env, elf, &ProverOpts::succinct())?;
                    let prove_ms = prove_start.elapsed().as_millis();
                    let verify_ms = if !skip_receipt_verify {
                        let verify_start = std::time::Instant::now();
                        prove_info
                            .receipt
                            .verify(risc0_zkvm::sha::Digest::from(image_id))?;
                        verify_start.elapsed().as_millis()
                    } else {
                        0
                    };
                    let out: SamplesAggrOutput = prove_info.receipt.journal.decode()?;
                    anyhow::ensure!(out == expected, "guest output mismatch (seq {})", seq);
                    receipt_words = to_vec(&prove_info.receipt)?;
                    let proof_bytes = receipt_words.len() * 4; // u32 words to bytes
                    let journal_bytes = to_vec(&prove_info.receipt.journal).map(|v| v.len()).unwrap_or(0);
                    let memory_mb = get_memory_mb();
                    eprintln!(
                        "[risc0-aggr][samples] seq={} prove_ms={} verify_ms={} proof_bytes={} journal_bytes={} memory_mb={}",
                        seq, prove_ms, verify_ms, proof_bytes, journal_bytes, memory_mb
                    );
                    etime::add_ms("prove", prove_ms as f64);
                    etime::add_ms("verify", verify_ms as f64);
                    out
                } else {
                    expected
                };
                prev_chain_hash = out.epoch_chain_link.final_chain_hash;
                // Update per-source chain tips from the ZK journal output (verified chain tips)
                // This ensures we use the cryptographically verified result, not host-computed value
                prev_source_chain_tips = out.final_source_chain_tips.clone();

                let result_commit = out.epoch_chain_link.final_chain_hash.to_vec();
                // Pack samples table using state directly (no redundant reprocessing)
                let table_fixed = pack_samples_table(&state);
                // Flatten events from batches for events_commit computation
                let flat_events: Vec<Event> = input.batches.iter()
                    .flat_map(|b| b.events.iter().cloned())
                    .collect();
                let events_commit = events_commit_from_events(&flat_events);

                // Clone table_fixed and receipt_words if FDB is enabled
                #[cfg(feature = "fdb")]
                let table_fixed_for_fdb = table_fixed.clone();
                #[cfg(feature = "fdb")]
                let receipt_words_for_fdb = receipt_words.clone();

                // IMPORTANT: put_agg_epoch is required for has_agg_epoch() to detect processed epochs
                agg_db.put_agg_epoch(
                    &mut batch,
                    &AggEpoch {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        aggregator_id,
                        min_ts,
                        max_ts,
                    },
                )?;
                // Compute out_commit locally (it's no longer in journal to reduce size)
                let out_commit = compute_samples_out_commit(
                    &out.buckets_root,
                    state.total_count,
                    state.total_sum,
                    out.n_events,
                    &out.final_source_chain_tips,
                );
                agg_db.put_verified_samples_struct(
                    &mut batch,
                    &VerifiedSamplesStruct {
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        out_commit: out_commit.to_vec(),
                        total_count: state.total_count,
                        total_sum: state.total_sum,
                        table_fixed: Some(table_fixed),
                        prev_chain_hash: input.prev_chain_hash.to_vec(),
                        events_commit: events_commit.to_vec(),
                        aggregator_id,
                    },
                )?;
                agg_db.put_agg_epoch_meta(
                    &mut batch,
                    &AggEpochMeta {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        n_events: out.n_events,
                        aggregator_id,
                    },
                )?;
                agg_db.put_agg_epoch_proof(
                    &mut batch,
                    &AggEpochProof {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        receipt_words,
                        aggregator_id,
                    },
                )?;
                // Atomic tombstone: only readable if the entire epoch's WriteBatch committed.
                agg_db.put_epoch_tombstone(
                    &mut batch,
                    &EpochTombstone {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        aggregator_id,
                        completed_at_ms: now_ms(),
                    },
                )?;
                #[cfg(feature = "fdb")]
                {
                    fdb_tombstone = Some(EpochTombstone {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        aggregator_id,
                        completed_at_ms: now_ms(),
                    });
                }
                eprintln!(
                    "[risc0-aggr][rocksdb] mode=samples seq={} n_events={} prove_ms={}",
                    seq,
                    out.n_events,
                    if prover.is_some() { prove_start.elapsed().as_millis() } else { 0 }
                );

                // Collect for FDB
                #[cfg(feature = "fdb")]
                {
                    fdb_verified_samples = Some(VerifiedSamplesStruct {
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        out_commit: out_commit.to_vec(),
                        total_count: state.total_count,
                        total_sum: state.total_sum,
                        table_fixed: Some(table_fixed_for_fdb),
                        prev_chain_hash: input.prev_chain_hash.to_vec(),
                        events_commit: events_commit.to_vec(),
                        aggregator_id,
                    });
                    fdb_epoch_meta = Some(AggEpochMeta {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        n_events: out.n_events,
                        aggregator_id,
                    });
                    fdb_epoch_proof = Some(AggEpochProof {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        receipt_words: receipt_words_for_fdb,
                        aggregator_id,
                    });
                }
            }
            "histogram" => {
                let input = HistogramAggrInput {
                    prev_chain_hash,
                    batches: batch_inputs.clone(),
                    prev_source_chain_tips: prev_source_chain_tips.clone(),
                };
                let (state, expected) = {
                    let _g = etime::Guard::new("aggr_compute");
                    process_histogram_aggr_with_state(&input)
                };
                let out: HistogramAggrOutput = if let Some(prover) = prover.as_ref() {
                    let input_bytes = to_vec(&input)?.len();
                    log_epoch_details_batches("histogram", seq, &input.batches, input_bytes);
                    let env = ExecutorEnv::builder().write(&input)?.build()?;
                    let prove_start = std::time::Instant::now();
                    let prove_info = prover.prove_with_opts(env, elf, &ProverOpts::succinct())?;
                    let prove_ms = prove_start.elapsed().as_millis();
                    let verify_ms = if !skip_receipt_verify {
                        let verify_start = std::time::Instant::now();
                        prove_info
                            .receipt
                            .verify(risc0_zkvm::sha::Digest::from(image_id))?;
                        verify_start.elapsed().as_millis()
                    } else {
                        0
                    };
                    let out: HistogramAggrOutput = prove_info.receipt.journal.decode()?;
                    anyhow::ensure!(out == expected, "guest output mismatch (seq {})", seq);
                    receipt_words = to_vec(&prove_info.receipt)?;
                    let proof_bytes = receipt_words.len() * 4; // u32 words to bytes
                    let journal_bytes = to_vec(&prove_info.receipt.journal).map(|v| v.len()).unwrap_or(0);
                    let memory_mb = get_memory_mb();
                    eprintln!(
                        "[risc0-aggr][histogram] seq={} prove_ms={} verify_ms={} proof_bytes={} journal_bytes={} memory_mb={}",
                        seq, prove_ms, verify_ms, proof_bytes, journal_bytes, memory_mb
                    );
                    etime::add_ms("prove", prove_ms as f64);
                    etime::add_ms("verify", verify_ms as f64);
                    out
                } else {
                    expected
                };
                prev_chain_hash = out.epoch_chain_link.final_chain_hash;
                // Update per-source chain tips from the ZK journal output (verified chain tips)
                // This ensures we use the cryptographically verified result, not host-computed value
                prev_source_chain_tips = out.final_source_chain_tips.clone();

                let result_commit = out.epoch_chain_link.final_chain_hash.to_vec();
                // Use state.per_key_histograms since it's no longer in journal
                let table_fixed = pack_hist_per_key_table(&state.per_key_histograms);
                // Flatten events from batches for events_commit computation
                let flat_events: Vec<Event> = input.batches.iter()
                    .flat_map(|b| b.events.iter().cloned())
                    .collect();
                let events_commit = events_commit_from_events(&flat_events);

                // Clone for FDB if enabled
                #[cfg(feature = "fdb")]
                let table_fixed_for_fdb = table_fixed.clone();
                #[cfg(feature = "fdb")]
                let receipt_words_for_fdb = receipt_words.clone();

                agg_db.put_agg_epoch(
                    &mut batch,
                    &AggEpoch {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        aggregator_id,
                        min_ts,
                        max_ts,
                    },
                )?;
                // Compute out_commit locally (it's no longer in journal to reduce size)
                let out_commit = compute_histogram_out_commit(
                    &out.buckets_root,
                    state.total_count,
                    state.total_sum,
                    out.n_events,
                    &out.final_source_chain_tips,
                );
                agg_db.put_agg_hist_struct(
                    &mut batch,
                    &AggHistStruct {
                        sequence: seq,
                        total_count: state.total_count,
                        total_sum: state.total_sum,
                        table_fixed,
                        prev_chain_hash: input.prev_chain_hash.to_vec(),
                        events_commit: events_commit.to_vec(),
                        out_commit: out_commit.to_vec(),
                        final_chain_hash: out.epoch_chain_link.final_chain_hash.to_vec(),
                    },
                )?;
                agg_db.put_agg_epoch_meta(
                    &mut batch,
                    &AggEpochMeta {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        n_events: out.n_events,
                        aggregator_id,
                    },
                )?;
                agg_db.put_agg_epoch_proof(
                    &mut batch,
                    &AggEpochProof {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        receipt_words,
                        aggregator_id,
                    },
                )?;
                // Atomic tombstone: only readable if the entire epoch's WriteBatch committed.
                agg_db.put_epoch_tombstone(
                    &mut batch,
                    &EpochTombstone {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        aggregator_id,
                        completed_at_ms: now_ms(),
                    },
                )?;
                #[cfg(feature = "fdb")]
                {
                    fdb_tombstone = Some(EpochTombstone {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        aggregator_id,
                        completed_at_ms: now_ms(),
                    });
                }
                eprintln!(
                    "[risc0-aggr][rocksdb] mode=histogram seq={} n_events={} prove_ms={}",
                    seq,
                    out.n_events,
                    if prover.is_some() { prove_start.elapsed().as_millis() } else { 0 }
                );

                // Collect for FDB
                #[cfg(feature = "fdb")]
                {
                    fdb_epoch = Some(AggEpoch {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        aggregator_id,
                        min_ts,
                        max_ts,
                    });
                    fdb_hist_struct = Some(AggHistStruct {
                        sequence: seq,
                        total_count: state.total_count,
                        total_sum: state.total_sum,
                        table_fixed: table_fixed_for_fdb,
                        prev_chain_hash: input.prev_chain_hash.to_vec(),
                        events_commit: events_commit.to_vec(),
                        out_commit: out_commit.to_vec(),
                        final_chain_hash: out.epoch_chain_link.final_chain_hash.to_vec(),
                    });
                    fdb_epoch_meta = Some(AggEpochMeta {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        n_events: out.n_events,
                        aggregator_id,
                    });
                    fdb_epoch_proof = Some(AggEpochProof {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        receipt_words: receipt_words_for_fdb,
                        aggregator_id,
                    });
                }
            }
            "cm" => {
                let input = CmAggrInput {
                    prev_chain_hash,
                    batches: batch_inputs.clone(),
                    prev_source_chain_tips: prev_source_chain_tips.clone(),
                };
                let (state, expected) = {
                    let _g = etime::Guard::new("aggr_compute");
                    process_cm_aggr_with_state(&input)
                };
                let out: CmAggrOutput = if let Some(prover) = prover.as_ref() {
                    let input_bytes = to_vec(&input)?.len();
                    log_epoch_details_batches("cm", seq, &input.batches, input_bytes);
                    let env = ExecutorEnv::builder().write(&input)?.build()?;
                    let prove_start = std::time::Instant::now();
                    let prove_info = prover.prove_with_opts(env, elf, &ProverOpts::succinct())?;
                    let prove_ms = prove_start.elapsed().as_millis();
                    let verify_ms = if !skip_receipt_verify {
                        let verify_start = std::time::Instant::now();
                        prove_info
                            .receipt
                            .verify(risc0_zkvm::sha::Digest::from(image_id))?;
                        verify_start.elapsed().as_millis()
                    } else {
                        0
                    };
                    let out: CmAggrOutput = prove_info.receipt.journal.decode()?;
                    anyhow::ensure!(out == expected, "guest output mismatch (seq {})", seq);
                    receipt_words = to_vec(&prove_info.receipt)?;
                    let proof_bytes = receipt_words.len() * 4; // u32 words to bytes
                    let journal_bytes = to_vec(&prove_info.receipt.journal).map(|v| v.len()).unwrap_or(0);
                    let memory_mb = get_memory_mb();
                    eprintln!(
                        "[risc0-aggr][cm] seq={} prove_ms={} verify_ms={} proof_bytes={} journal_bytes={} memory_mb={}",
                        seq, prove_ms, verify_ms, proof_bytes, journal_bytes, memory_mb
                    );
                    etime::add_ms("prove", prove_ms as f64);
                    etime::add_ms("verify", verify_ms as f64);
                    out
                } else {
                    expected
                };
                prev_chain_hash = out.epoch_chain_link.final_chain_hash;
                // Update per-source chain tips from the ZK journal output (verified chain tips)
                // This ensures we use the cryptographically verified result, not host-computed value
                prev_source_chain_tips = out.final_source_chain_tips.clone();

                let result_commit = out.epoch_chain_link.final_chain_hash.to_vec();
                // Flatten events from batches for events_commit computation
                let flat_events: Vec<Event> = input.batches.iter()
                    .flat_map(|b| b.events.iter().cloned())
                    .collect();
                let events_commit = events_commit_from_events(&flat_events);
                // Pack CM counts from state.table as u32 (no wasteful u48 conversion)
                // This ensures the stored data matches what was used to compute state_commit
                let counts_u32 = pack_cm_counts_u32(&state);
                // Use state values since they're no longer in journal
                let heap_fixed = pack_cm_heap_fixed(&state.heap_keys, &state.heap_vals, &state.heap_occ);

                // Diagnostic: Log state_commit and final_chain_hash being stored
                eprintln!("[AGGREGATOR] CM Epoch {}: Storing to DB", seq);
                eprintln!("  state_commit: {}", hex::encode(&out.state_commit));
                eprintln!("  final_chain_hash: {}", hex::encode(&out.epoch_chain_link.final_chain_hash));
                eprintln!("  total_sum: {}", state.total_sum);
                eprintln!("  counts_u32 bytes: {}", counts_u32.len());
                eprintln!("  sample counts[0..5]: {:?}", &state.table[0][0..5]);

                // Clone for FDB if enabled
                #[cfg(feature = "fdb")]
                let counts_u32_for_fdb = counts_u32.clone();
                #[cfg(feature = "fdb")]
                let heap_fixed_for_fdb = heap_fixed.clone();
                #[cfg(feature = "fdb")]
                let receipt_words_for_fdb = receipt_words.clone();

                agg_db.put_agg_epoch(
                    &mut batch,
                    &AggEpoch {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        aggregator_id,
                        min_ts,
                        max_ts,
                    },
                )?;
                // Compute out_commit locally (it's no longer in journal to reduce size)
                let out_commit = compute_cm_out_commit(
                    &out.cm_root,
                    &out.heap_root,
                    state.total_sum,
                    out.n_events,
                    &out.final_source_chain_tips,
                );
                agg_db.put_agg_cm_struct(
                    &mut batch,
                    &AggCmStruct {
                        sequence: seq,
                        counts_u32,
                        heap_fixed,
                        total_sum: state.total_sum,
                        prev_chain_hash: input.prev_chain_hash.to_vec(),
                        events_commit: events_commit.to_vec(),
                        out_commit: out_commit.to_vec(),
                        final_chain_hash: out.epoch_chain_link.final_chain_hash.to_vec(),
                    },
                )?;
                agg_db.put_agg_epoch_meta(
                    &mut batch,
                    &AggEpochMeta {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        n_events: out.n_events,
                        aggregator_id,
                    },
                )?;
                agg_db.put_agg_epoch_proof(
                    &mut batch,
                    &AggEpochProof {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        receipt_words,
                        aggregator_id,
                    },
                )?;
                // Atomic tombstone: only readable if the entire epoch's WriteBatch committed.
                agg_db.put_epoch_tombstone(
                    &mut batch,
                    &EpochTombstone {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        aggregator_id,
                        completed_at_ms: now_ms(),
                    },
                )?;
                #[cfg(feature = "fdb")]
                {
                    fdb_tombstone = Some(EpochTombstone {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        aggregator_id,
                        completed_at_ms: now_ms(),
                    });
                }
                eprintln!(
                    "[risc0-aggr][rocksdb] mode=cm seq={} n_events={} prove_ms={}",
                    seq,
                    out.n_events,
                    if prover.is_some() { prove_start.elapsed().as_millis() } else { 0 }
                );

                // Collect for FDB
                #[cfg(feature = "fdb")]
                {
                    fdb_epoch = Some(AggEpoch {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        result_commit: result_commit.clone(),
                        aggregator_id,
                        min_ts,
                        max_ts,
                    });
                    fdb_cm_struct = Some(AggCmStruct {
                        sequence: seq,
                        counts_u32: counts_u32_for_fdb,
                        heap_fixed: heap_fixed_for_fdb,
                        total_sum: state.total_sum,
                        prev_chain_hash: input.prev_chain_hash.to_vec(),
                        events_commit: events_commit.to_vec(),
                        out_commit: out_commit.to_vec(),
                        final_chain_hash: out.epoch_chain_link.final_chain_hash.to_vec(),
                    });
                    fdb_epoch_meta = Some(AggEpochMeta {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        ingest_time_ms,
                        n_events: out.n_events,
                        aggregator_id,
                    });
                    fdb_epoch_proof = Some(AggEpochProof {
                        epoch_type: out_epoch_type,
                        sequence: seq,
                        receipt_words: receipt_words_for_fdb,
                        aggregator_id,
                    });
                }
            }
            _ => unreachable!(),
        }

        // Online resharding: durably persist this aggregator's per-source chain
        // tips into the epoch's atomic WriteBatch (so they commit together with
        // the epoch rows + tombstone). These let the source's chain be reloaded
        // after a restart and inherited by a future owner after a reshard.
        // prev_source_chain_tips was just reassigned to the verified
        // out.final_source_chain_tips, which only contains sources processed
        // (owned) this epoch.
        if use_online_ownership {
            for (sid, last_seq, tip) in &prev_source_chain_tips {
                if let Err(e) = agg_db.put_source_tip(
                    &mut batch,
                    &AggSourceTip {
                        source_id: *sid,
                        last_seq: *last_seq as i64,
                        chain_tip: *tip,
                        owner: aggregator_id,
                        updated_at_epoch: seq,
                    },
                ) {
                    eprintln!(
                        "[native-aggr][ownership] seq={} source_id={} put_source_tip error: {:?}",
                        seq, sid, e
                    );
                }
            }
        }

        // Write to FDB if enabled, otherwise write to RocksDB
        #[cfg(feature = "fdb")]
        let use_fdb = fdb_store.is_some();
        #[cfg(not(feature = "fdb"))]
        let use_fdb = false;

        if use_fdb {
            #[cfg(feature = "fdb")]
            if let Some(ref fdb) = fdb_store {
                use common::fdb_store::FdbWriteBatch;

                let rt = fdb_runtime.as_ref().context("FDB runtime not available")?;

                let mut fdb_batch = FdbWriteBatch::new();

                if let Some(epoch) = fdb_epoch {
                    fdb_batch.put_agg_epoch(epoch);
                }
                if let Some(meta) = fdb_epoch_meta {
                    fdb_batch.put_agg_epoch_meta(meta);
                }
                if let Some(proof) = fdb_epoch_proof {
                    fdb_batch.put_agg_epoch_proof(proof);
                }
                if let Some(cm) = fdb_cm_struct {
                    fdb_batch.put_agg_cm_struct(cm);
                }
                if let Some(hist) = fdb_hist_struct {
                    fdb_batch.put_agg_hist_struct(hist);
                }
                if let Some(vs) = fdb_verified_samples {
                    fdb_batch.put_verified_samples_struct(vs);
                }
                if let Some(t) = fdb_tombstone {
                    fdb_batch.put_epoch_tombstone(t);
                }

                let _fdb_w0 = std::time::Instant::now();
                rt.block_on(async {
                    fdb.write_batch(fdb_batch).await
                })
                .context("write FDB batch")?;
                etime::add_ms("fdb_write", _fdb_w0.elapsed().as_secs_f64() * 1000.0);

                if epochs_proved == 0 {
                    eprintln!("[risc0-aggr] FDB mode - writing to FoundationDB only (no RocksDB)");
                }
            }
        } else {
            // RocksDB-only mode - write full data to local RocksDB
            let _agg_w0 = std::time::Instant::now();
            agg_db.write_batch(batch).context("write agg rocksdb batch")?;
            agg_db.maybe_flush_after_epoch(seq).ok();
            etime::add_ms("rocksdb_agg_write", _agg_w0.elapsed().as_secs_f64() * 1000.0);
        }

        // Always mark epoch as processed in agg_db (even in FDB mode)
        // This prevents duplicate processing when polling for new epochs
        if use_fdb {
            let mut marker_batch = WriteBatch::default();
            agg_db.put_agg_epoch(
                &mut marker_batch,
                &AggEpoch {
                    epoch_type: out_epoch_type,
                    sequence: seq,
                    ingest_time_ms,
                    result_commit: vec![], // Minimal marker - full data is in FDB
                    aggregator_id,
                    min_ts,
                    max_ts,
                },
            )?;
            // Mirror the tombstone locally so the local skip-check (has_epoch_tombstone)
            // sees this epoch as fully done in FDB mode too.
            agg_db.put_epoch_tombstone(
                &mut marker_batch,
                &EpochTombstone {
                    epoch_type: out_epoch_type,
                    sequence: seq,
                    aggregator_id,
                    completed_at_ms: now_ms(),
                },
            )?;
            agg_db.write_batch(marker_batch).context("write epoch marker to agg_db")?;
        }

        // Emit one machine-readable per-epoch component-timing line for the e2e
        // baseline driver. `epoch_total_ms` spans raw read -> compute -> (prove) ->
        // store write for this epoch; the sum of components plus untimed glue.
        if etime::enabled() {
            let total_ms = epoch_t0.elapsed().as_secs_f64() * 1000.0;
            println!(
                "[e2e-timing] seq={} mode={} rocksdb_raw_read_ms={:.3} aggr_compute_ms={:.3} prove_ms={:.3} verify_ms={:.3} rocksdb_agg_write_ms={:.3} fdb_write_ms={:.3} epoch_total_ms={:.3}",
                seq,
                mode,
                etime::get("rocksdb_raw_read"),
                etime::get("aggr_compute"),
                etime::get("prove"),
                etime::get("verify"),
                etime::get("rocksdb_agg_write"),
                etime::get("fdb_write"),
                total_ms,
            );
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
        }

        // Delete exactly the processed batches from raw_db (only if opened as primary, not secondary)
        // The epoch_batches:{seq} blob contains all batches that were loaded via get_epoch_batches(seq),
        // which were then deduplicated into batch_inputs. Deleting this blob removes exactly:
        // - All batches that were processed (batch_inputs)
        // - Any duplicates that were filtered out during deduplication
        // This ensures we don't delete more (other epochs) or less (leave stale batches).
        // NOTE: Skip deletion if raw_db was opened as secondary (read-only mode).
        let is_secondary_mode = std::env::var("RAW_ROCKSDB_SECONDARY_PATH")
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        if let Some(ref raw_db) = raw_db {
            if keep_raw_batches {
                // Shared-raw_db mode: leave the epoch_batches in place so the
                // other aggregators sharing this raw_db can read their sources.
            } else if is_secondary_mode {
                // In secondary mode, we can't delete directly. The primary (kafka-consumer)
                // handles deletion via its cleanup loop. Force a sync to see any deletions
                // the primary may have done.
                raw_db.catch_up_if_secondary().ok();
                eprintln!(
                    "[risc0-aggr][cleanup] skipping deletion of epoch_batches seq={} (secondary mode); synced with primary",
                    seq
                );
            } else {
                let mut delete_batch = WriteBatch::default();
                raw_db.delete_epoch_batches(&mut delete_batch, seq)?;
                raw_db.write_batch(delete_batch).context("delete processed epoch_batches from raw_db")?;
                eprintln!(
                    "[risc0-aggr][cleanup] deleted epoch_batches seq={} from raw_db ({} batches processed)",
                    seq, batch_inputs.len()
                );
            }
        }

        epochs_proved += 1;

        // === Per-source status logging after epoch completion ===
        {
            use std::collections::BTreeMap;
            // Collect per-source stats from batch_inputs processed this epoch
            let mut source_stats: BTreeMap<u32, (u64, u64, usize)> = BTreeMap::new(); // (min_seq, max_seq, count)
            for b in &batch_inputs {
                source_stats
                    .entry(b.source_id)
                    .and_modify(|(min, max, cnt)| {
                        *min = (*min).min(b.source_batch_seq);
                        *max = (*max).max(b.source_batch_seq);
                        *cnt += 1;
                    })
                    .or_insert((b.source_batch_seq, b.source_batch_seq, 1));
            }
            eprintln!("[risc0-aggr][source-status] === Epoch seq={} completed, {} sources ===", seq, source_stats.len());
            for (source_id, (min_seq, max_seq, batch_count)) in &source_stats {
                // Find expected_next_seq from prev_source_chain_tips (state AFTER this epoch)
                let (last_processed_seq, _tip) = prev_source_chain_tips
                    .iter()
                    .find(|(sid, _, _)| *sid == *source_id)
                    .map(|(_, seq, tip)| (*seq, *tip))
                    .unwrap_or((0, [0u8; 32]));
                let expected_next_seq = last_processed_seq + 1;
                eprintln!(
                    "[risc0-aggr][source-status]   source_id={:3} | processed_seq_range=[{},{}] ({} batches) | next_expected_seq={}",
                    source_id, min_seq, max_seq, batch_count, expected_next_seq
                );
            }
            // Also log sources that had prior state but no batches this epoch
            for (source_id, last_seq, tip) in &prev_source_chain_tips {
                if !source_stats.contains_key(source_id) {
                    eprintln!(
                        "[risc0-aggr][source-status]   source_id={:3} | NO BATCHES THIS EPOCH | last_seq={} next_expected_seq={} tip={:?}",
                        source_id, last_seq, last_seq + 1, &tip[..8]
                    );
                }
            }
        }

        // === Log RocksDB state after epoch completion ===
        if let Some(ref raw_db) = raw_db {
            raw_db.catch_up_if_secondary().ok();
            match raw_db.epoch_batches_seq_range() {
                Ok(Some((remaining_min, remaining_max))) => {
                    eprintln!(
                        "[risc0-aggr][rocksdb-state] after epoch seq={}: epoch_batches in raw_db range=[{},{}] ({} epochs available)",
                        seq, remaining_min, remaining_max, remaining_max - remaining_min + 1
                    );
                }
                Ok(None) => {
                    eprintln!(
                        "[risc0-aggr][rocksdb-state] after epoch seq={}: NO epoch_batches remaining in raw_db",
                        seq
                    );
                }
                Err(e) => {
                    eprintln!(
                        "[risc0-aggr][rocksdb-state] after epoch seq={}: error querying epoch_batches: {}",
                        seq, e
                    );
                }
            }
        }

        // Online resharding (preview): roll forward the "what we owned this
        // epoch" tracker so that the *next* epoch can detect outgoing
        // transitions for sources we own now but no longer own.
        if use_online_ownership {
            prev_owned_sources = owned_sources_this_epoch.clone();
        }

        // After processing one epoch, break inner loop to re-poll for more work
        break;
        }  // end inner while seq <= max_seq

        // If no work found in this poll cycle, check idle timeout
        if !found_work {
            if last_work_time.elapsed() >= idle_timeout {
                // Log final RocksDB state before exiting
                if let Some(ref raw_db) = raw_db {
                    raw_db.catch_up_if_secondary().ok();
                    match raw_db.epoch_batches_seq_range() {
                        Ok(Some((remaining_min, remaining_max))) => {
                            eprintln!(
                                "[risc0-aggr][rocksdb-state] BEFORE EXIT: epoch_batches in raw_db range=[{},{}] ({} epochs still available but idle timeout reached)",
                                remaining_min, remaining_max, remaining_max - remaining_min + 1
                            );
                        }
                        Ok(None) => {
                            eprintln!("[risc0-aggr][rocksdb-state] BEFORE EXIT: NO epoch_batches remaining in raw_db (all processed)");
                        }
                        Err(e) => {
                            eprintln!("[risc0-aggr][rocksdb-state] BEFORE EXIT: error querying epoch_batches: {}", e);
                        }
                    }
                }
                eprintln!("[risc0-aggr] idle timeout reached ({}s), no unprocessed epochs, exiting", idle_timeout.as_secs());
                break;
            }
            std::thread::sleep(poll_interval);
        }
    }  // end outer poll loop

    // Final summary: report total epochs proved by this aggregator
    let pipeline_elapsed_ms = pipeline_start.elapsed().as_millis();
    eprintln!(
        "[risc0-aggr][rocksdb] DONE: epochs_proved={} mode={} total_ms={}",
        epochs_proved, mode, pipeline_elapsed_ms
    );

    Ok(())
}

struct ZipfSampler {
    cdf: Vec<f64>,
}

impl ZipfSampler {
    fn new(n: usize, s: f64) -> anyhow::Result<Self> {
        anyhow::ensure!(n >= 1, "Zipf n must be >= 1");
        anyhow::ensure!(s > 0.0, "VALUE_ZIPF_S must be > 0 (got {s})");
        let mut cdf: Vec<f64> = Vec::with_capacity(n);
        let mut sum = 0.0f64;
        for k in 1..=n {
            sum += (k as f64).powf(-s);
            cdf.push(sum);
        }
        for x in &mut cdf {
            *x /= sum;
        }
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }
        Ok(Self { cdf })
    }

    fn sample_u64<R: RngCore>(&self, rng: &mut R) -> u64 {
        let u = ((rng.next_u64() >> 11) as f64) / ((1u64 << 53) as f64);
        let idx = match self
            .cdf
            .binary_search_by(|p| p.partial_cmp(&u).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(i) => i,
            Err(i) => i,
        };
        idx as u64
    }
}

fn build_zipf_value(value_mod: u64) -> anyhow::Result<ZipfSampler> {
    anyhow::ensure!(value_mod > 0, "VALUE_MOD must be > 0 for zipf");
    let n = value_mod as usize;
    let max_n = env_u64("VALUE_ZIPF_MAX_N", 2_000_000) as usize;
    anyhow::ensure!(
        n <= max_n,
        "VALUE_MOD={} too large for Zipf precompute; increase VALUE_ZIPF_MAX_N (current {})",
        n,
        max_n
    );
    let s = env_f64("VALUE_ZIPF_S", 1.2);
    ZipfSampler::new(n, s)
}


fn generate_epoch_events(mode: &str, source_id: u32, series: u64, samples_per_series: u64, seed: u64) -> Vec<Event> {
    // Generate events with distinct keys.
    // Each key embeds source_id and local key_index, matching querier benchmark.
    // Supports optional Zipf key distribution via KEY_ZIPF_S environment variable.
    use rand::Rng;
    let series = series.max(1);
    let samples_per_series = samples_per_series.max(1);
    let total = series.saturating_mul(samples_per_series);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    let default_value_mod = match mode {
        "cm" => 1_000u64,
        "histogram" => 10_000u64,
        "samples" => 1_000_000u64,
        _ => 10_000u64,
    };
    let value_mod = env_u64("VALUE_MOD", default_value_mod).max(1);
    let zipf = build_zipf_value(value_mod).expect("zipf sampler");

    let mut events = Vec::with_capacity(total as usize);

    // Check if KEY_ZIPF_S is set for Zipf key distribution
    let key_zipf_opt = env_string("KEY_ZIPF_S")
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&v| v > 0.0)
        .and_then(|s| ZipfSampler::new(series as usize, s).ok());

    if let Some(ref key_zipf) = key_zipf_opt {
        // Zipf key distribution: sample keys with skew
        for _ in 0..total {
            let key_idx = key_zipf.sample_u64(&mut rng) % series;
            let key_id = Event::make_key_id(source_id, key_idx);
            let value = (zipf.sample_u64(&mut rng) + 1) as u32;
            let ts = rng.gen_range(0u32..1_000_000u32);
            events.push(Event { ts, key_id, value });
        }
    } else {
        // Uniform key distribution: round-robin through keys
        // Each key gets samples_per_series events total, interleaved
        let mut key_remaining: Vec<u64> = (0..series).map(|_| samples_per_series).collect();
        let mut key_idx = 0u64;

        for _ in 0..total {
            // Find next key with remaining samples (round-robin with skip)
            let start_idx = key_idx;
            loop {
                if key_remaining[key_idx as usize] > 0 {
                    break;
                }
                key_idx = (key_idx + 1) % series;
                if key_idx == start_idx {
                    break;
                }
            }

            let key_id = Event::make_key_id(source_id, key_idx);
            let value = (zipf.sample_u64(&mut rng) + 1) as u32;
            let ts = rng.gen_range(0u32..1_000_000u32);
            events.push(Event { ts, key_id, value });

            key_remaining[key_idx as usize] -= 1;
            key_idx = (key_idx + 1) % series;
        }
    }
    events
}

/// Generate events with global zipf distribution across all keys.
/// Keys are sampled from Zipf distribution over [0, num_sources * series_per_source),
/// then converted to (source_id, key_index) pairs using make_key_id.
/// This creates truly global hot keys across all sources.
fn generate_epoch_events_global_zipf(
    mode: &str,
    num_sources: u64,
    series_per_source: u64,
    total_samples: u64,
    seed: u64,
) -> Vec<Event> {
    use rand::Rng;
    let num_sources = num_sources.max(1);
    let series_per_source = series_per_source.max(1);
    let total_keys = num_sources.saturating_mul(series_per_source);
    let total_samples = total_samples.max(1);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

    // Value distribution
    let default_value_mod = match mode {
        "cm" => 1_000u64,
        "histogram" => 10_000u64,
        "samples" => 1_000_000u64,
        _ => 10_000u64,
    };
    let value_mod = env_u64("VALUE_MOD", default_value_mod).max(1);
    let value_zipf = build_zipf_value(value_mod).expect("value zipf sampler");

    // Key distribution: global zipf over [0, total_keys)
    let key_zipf_s = env_string("KEY_ZIPF_S")
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(1.2);
    let key_zipf = ZipfSampler::new(total_keys as usize, key_zipf_s).expect("key zipf sampler");

    let mut events = Vec::with_capacity(total_samples as usize);
    for _ in 0..total_samples {
        let key_num = key_zipf.sample_u64(&mut rng) % total_keys;
        // Convert global key number to (source_id, key_index)
        let source_id = (key_num / series_per_source) as u32;
        let key_index = key_num % series_per_source;
        let key_id = Event::make_key_id(source_id, key_index);
        let value = (value_zipf.sample_u64(&mut rng) + 1) as u32;
        let ts = rng.gen_range(0u32..1_000_000u32);
        events.push(Event { ts, key_id, value });
    }
    events
}

#[derive(Copy, Clone, Debug)]
enum BenchInputKind {
    Synthetic,
    Tsv,
    Caida,
}

fn bench_input_kind() -> anyhow::Result<BenchInputKind> {
    let raw = parse_arg_string("--bench-input")
        .or_else(|| env_string("BENCH_INPUT"))
        .unwrap_or_else(|| "synthetic".to_string());
    match raw.as_str() {
        "synthetic" => Ok(BenchInputKind::Synthetic),
        "tsv" | "google" | "google_cluster" | "google_cluster_data" => Ok(BenchInputKind::Tsv),
        "caida" | "caida_txt" => Ok(BenchInputKind::Caida),
        other => anyhow::bail!(
            "unsupported BENCH_INPUT={other}; expected synthetic, tsv/google, or caida"
        ),
    }
}

fn parse_tsv_line(line: &str) -> Option<(u64, u64)> {
    // Expected: timestamp<TAB>value_u64
    let s = line.trim();
    if s.is_empty() {
        return None;
    }
    let mut it = s.split('\t');
    let t = it.next()?.trim().parse::<u64>().ok()?;
    let v = it.next()?.trim().parse::<u64>().ok()?;
    Some((t, v))
}

fn trim_quotes(s: &str) -> &str {
    let s = s.trim();
    let s = s.strip_prefix('"').unwrap_or(s);
    s.strip_suffix('"').unwrap_or(s)
}

fn metric_id_from_csv_header(line: &str) -> Option<u8> {
    let mut it = line.split(',');
    let first = it.next()?;
    let name = trim_quotes(first).to_ascii_lowercase();
    if name.contains("cpu") {
        Some(1)
    } else if name.contains("mem") {
        Some(2)
    } else {
        None
    }
}

fn parse_csv_line(line: &str, value_scale: f64) -> Option<(u64, u64, u64)> {
    // Expected: value_f64,machine_id,end_time
    let s = line.trim();
    if s.is_empty() {
        return None;
    }
    let mut it = s.split(',');
    let value_raw = trim_quotes(it.next()?);
    let machine_raw = trim_quotes(it.next()?);
    let end_time_raw = trim_quotes(it.next()?);

    let value_f64 = value_raw.parse::<f64>().ok()?;
    let machine_id = match machine_raw.parse::<i64>().ok()? {
        -1 => (u32::MAX as u64).saturating_sub(1),
        v if v >= 0 => v as u64,
        _ => return None,
    };
    let end_time = end_time_raw.parse::<u64>().ok()?;

    let scaled = (value_f64 * value_scale).round();
    if !scaled.is_finite() || scaled < 0.0 {
        return None;
    }
    let value_u64 = scaled as u64;
    Some((end_time, value_u64, machine_id))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn extract_machine_id_from_filename(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_string_lossy().to_string();
    let name_lc = name.to_ascii_lowercase();
    if let Some(rest) = name_lc.strip_prefix("avg_cpu_machine_id_") {
        return rest.trim_end_matches(".txt").parse::<u64>().ok();
    }
    if let Some(rest) = name_lc.strip_prefix("avg_mem_machine_id_") {
        return rest.trim_end_matches(".txt").parse::<u64>().ok();
    }
    if let Some(rest) = name_lc.strip_prefix("machine_") {
        let (mid, _rest) = rest.split_once("__")?;
        return mid.parse::<u64>().ok();
    }
    None
}

fn metric_id_from_filename(path: &Path) -> u8 {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if name.contains("cpu") {
        1
    } else if name.contains("mem") {
        2
    } else {
        255
    }
}

fn encode_key_id(metric_id: u8, machine_id: u64) -> [u8; KEY_BYTES_LEN] {
    // Layout: 15 bytes with metric_id in first byte, machine_id in last 8 bytes (big-endian)
    let mut key = [0u8; KEY_BYTES_LEN];
    key[0] = metric_id;
    key[KEY_BYTES_LEN - 8..].copy_from_slice(&machine_id.to_be_bytes());
    key
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum GoogleDataFileFormat {
    Tsv,
    Csv,
}

struct GoogleDataFile {
    key_id: [u8; KEY_BYTES_LEN],
    format: GoogleDataFileFormat,
    csv_value_scale: f64,
    reader: BufReader<fs::File>,
    path: PathBuf,
    pending: Option<(u64, u64)>,
}

impl GoogleDataFile {
    fn open(path: PathBuf) -> anyhow::Result<Self> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let format = if ext == "csv" {
            GoogleDataFileFormat::Csv
        } else {
            GoogleDataFileFormat::Tsv
        };

        let f = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut reader = BufReader::new(f);

        let (key_id, pending, csv_value_scale) = match format {
            GoogleDataFileFormat::Tsv => {
                let metric_id = metric_id_from_filename(&path);
                anyhow::ensure!(
                    metric_id != 255,
                    "metric id not found in filename: {}",
                    path.display()
                );
                let machine_id = extract_machine_id_from_filename(&path)
                    .with_context(|| format!("parse machine_id from filename: {}", path.display()))?;
                (encode_key_id(metric_id, machine_id), None, 1.0)
            }
            GoogleDataFileFormat::Csv => {
                let csv_value_scale = env_f64("CSV_VALUE_SCALE", 1_000_000.0);
                anyhow::ensure!(
                    csv_value_scale.is_finite() && csv_value_scale > 0.0,
                    "CSV_VALUE_SCALE must be a finite positive number"
                );

                let mut line = String::new();
                let mut metric_id = env_u8("CSV_METRIC_ID", 2);
                let mut first_row: Option<(u64, u64, u64)> = None;

                loop {
                    line.clear();
                    let n = reader
                        .read_line(&mut line)
                        .with_context(|| format!("read {}", path.display()))?;
                    if n == 0 {
                        break;
                    }
                    if let Some(mid) = metric_id_from_csv_header(&line) {
                        metric_id = mid;
                        continue;
                    }
                    if let Some(row) = parse_csv_line(&line, csv_value_scale) {
                        first_row = Some(row);
                        break;
                    }
                }

                let (t, v, machine_id) = first_row
                    .with_context(|| format!("no data rows in {}", path.display()))?;
                (
                    encode_key_id(metric_id, machine_id),
                    Some((t, v)),
                    csv_value_scale,
                )
            }
        };

        Ok(Self {
            key_id,
            format,
            csv_value_scale,
            reader,
            path,
            pending,
        })
    }

    fn next_row(&mut self) -> anyhow::Result<Option<(u64, u64)>> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        let mut line = String::new();
        let mut rewound = false;
        loop {
            line.clear();
            let n = self
                .reader
                .read_line(&mut line)
                .with_context(|| format!("read {}", self.path.display()))?;
            if n == 0 {
                if rewound {
                    return Ok(None);
                }
                let f = fs::File::open(&self.path)
                    .with_context(|| format!("reopen {}", self.path.display()))?;
                self.reader = BufReader::new(f);
                rewound = true;
                continue;
            }
            match self.format {
                GoogleDataFileFormat::Tsv => {
                    if let Some((t, v)) = parse_tsv_line(&line) {
                        return Ok(Some((t, v)));
                    }
                }
                GoogleDataFileFormat::Csv => {
                    if let Some((t, v, _mid)) = parse_csv_line(&line, self.csv_value_scale) {
                        return Ok(Some((t, v)));
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct GoogleHeapItem {
    start_time: u64,
    value: u64,
    file_idx: usize,
    key_id: [u8; KEY_BYTES_LEN],
}

impl PartialEq for GoogleHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.start_time == other.start_time
            && self.key_id == other.key_id
            && self.file_idx == other.file_idx
            && self.value == other.value
    }
}
impl Eq for GoogleHeapItem {}
impl PartialOrd for GoogleHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GoogleHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start_time
            .cmp(&other.start_time)
            .then_with(|| self.key_id.cmp(&other.key_id))
            .then_with(|| self.file_idx.cmp(&other.file_idx))
            .then_with(|| self.value.cmp(&other.value))
    }
}

fn collect_google_cluster_files(tsv_dir: &Path, max_files: usize) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(tsv_dir).with_context(|| format!("read_dir {}", tsv_dir.display()))? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "tsv" && ext != "txt" && ext != "csv" {
            continue;
        }
        if ext != "csv" {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let name_lc = name.to_ascii_lowercase();
            if !name_lc.starts_with("machine_")
                && !name_lc.starts_with("avg_cpu_machine_id_")
                && !name_lc.starts_with("avg_mem_machine_id_")
            {
                continue;
            }
            if extract_machine_id_from_filename(&p).is_none() {
                continue;
            }
            if metric_id_from_filename(&p) == 255 {
                continue;
            }
        }
        paths.push(p);
    }
    paths.sort();
    if max_files > 0 && paths.len() > max_files {
        paths.truncate(max_files);
    }
    anyhow::ensure!(!paths.is_empty(), "no input files found in {}", tsv_dir.display());
    Ok(paths)
}

struct GoogleEventSource {
    files: Vec<GoogleDataFile>,
    heap: BinaryHeap<Reverse<GoogleHeapItem>>,
}

impl GoogleEventSource {
    fn new(tsv_dir: &Path, max_files: usize) -> anyhow::Result<Self> {
        let mut files: Vec<GoogleDataFile> = collect_google_cluster_files(tsv_dir, max_files)?
            .into_iter()
            .map(GoogleDataFile::open)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut heap: BinaryHeap<Reverse<GoogleHeapItem>> = BinaryHeap::new();
        for (idx, f) in files.iter_mut().enumerate() {
            if let Some((t, v)) = f.next_row()? {
                heap.push(Reverse(GoogleHeapItem {
                    start_time: t,
                    value: v,
                    file_idx: idx,
                    key_id: f.key_id,
                }));
            }
        }
        anyhow::ensure!(
            !heap.is_empty(),
            "no rows found in input files under {}",
            tsv_dir.display()
        );
        Ok(Self { files, heap })
    }

    fn next_event(&mut self) -> anyhow::Result<Option<BenchInputEvent>> {
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };
        let ts = item.start_time as u32;  // Truncate to u32
        let key_id = item.key_id;
        let value = item.value as u32;    // Truncate to u32

        let idx = item.file_idx;
        if let Some((t2, v2)) = self.files[idx].next_row()? {
            self.heap.push(Reverse(GoogleHeapItem {
                start_time: t2,
                value: v2,
                file_idx: idx,
                key_id,
            }));
        }

        Ok(Some(BenchInputEvent { ts, key_id, value }))
    }
}

#[derive(Clone, Debug)]
struct BenchInputEvent {
    ts: u32,
    key_id: [u8; KEY_BYTES_LEN],
    value: u32,
}

fn load_google_cluster_epochs(
    epochs: u64,
    series: u64,
    samples_per_series: u64,
) -> anyhow::Result<Vec<Vec<Event>>> {
    let epoch_events = (series as usize).saturating_mul(samples_per_series as usize);
    if epoch_events == 0 || epochs == 0 {
        return Ok(Vec::new());
    }

    let total_events = (epoch_events as u64).saturating_mul(epochs);
    let dir = parse_arg_string("--tsv-dir")
        .map(PathBuf::from)
        .or_else(|| env_path("TSV_DIR"))
        .or_else(|| env_path("GOOGLE_CLUSTER_DIR"))
        .or_else(|| env_path("GOOGLE_CLUSTER_INPUT_DIR"))
        .or_else(default_google_cluster_dir)
        .context("TSV_DIR is required for BENCH_INPUT=tsv/google")?;
    let max_files = env_usize("TSV_MAX_FILES", 64);

    let target_per_key = if series > 0 {
        env_usize(
            "GOOGLE_CLUSTER_EVENTS_PER_KEY",
            (samples_per_series as usize).saturating_mul(epochs as usize),
        )
        .max(1)
    } else {
        0
    };
    let mut allowed_keys: BTreeSet<[u8; KEY_BYTES_LEN]> = BTreeSet::new();
    let mut per_key: BTreeMap<[u8; KEY_BYTES_LEN], Vec<BenchInputEvent>> = BTreeMap::new();
    let mut satisfied_keys = 0usize;

    let mut source = GoogleEventSource::new(&dir, max_files)?;
    while let Some(ev) = source.next_event()? {
        if series > 0 {
            if allowed_keys.len() < series as usize {
                allowed_keys.insert(ev.key_id);
            }
            if !allowed_keys.contains(&ev.key_id) {
                continue;
            }
        }
        let entry = per_key.entry(ev.key_id).or_default();
        if target_per_key == 0 || entry.len() < target_per_key {
            entry.push(ev);
            if target_per_key > 0 && entry.len() == target_per_key {
                satisfied_keys = satisfied_keys.saturating_add(1);
                if allowed_keys.len() == series as usize && satisfied_keys >= series as usize {
                    break;
                }
            }
        }
    }

    if series > 0 && allowed_keys.len() < series as usize {
        anyhow::bail!(
            "not enough Google cluster series: need {} distinct keys, got {}; try smaller SERIES or increase TSV_MAX_FILES",
            series,
            allowed_keys.len()
        );
    }
    for key in &allowed_keys {
        let n = per_key.get(key).map(|v| v.len()).unwrap_or(0);
        anyhow::ensure!(
            n > 0,
            "no Google cluster events for key_id={}; try smaller SERIES or increase TSV_MAX_FILES",
            hex::encode(key)
        );
    }

    let keys: Vec<[u8; KEY_BYTES_LEN]> = allowed_keys.iter().copied().collect();
    let mut cursors: BTreeMap<[u8; KEY_BYTES_LEN], usize> = keys.iter().map(|k| (*k, 0usize)).collect();
    let mut out: Vec<Vec<Event>> = Vec::with_capacity(epochs as usize);

    let mut remaining = total_events;
    for _epoch in 0..epochs {
        let mut one: Vec<Event> = Vec::with_capacity(epoch_events);
        for _sample in 0..samples_per_series {
            for key in &keys {
                if remaining == 0 {
                    break;
                }
                let list = per_key.get(key).expect("key list");
                let idx = cursors.get_mut(key).expect("cursor");
                let ev = list[*idx % list.len()].clone();
                one.push(Event {
                    ts: ev.ts,
                    key_id: ev.key_id,
                    value: ev.value,
                });
                *idx += 1;
                remaining = remaining.saturating_sub(1);
            }
        }
        out.push(one);
    }
    Ok(out)
}

struct CaidaEventSource {
    files: Vec<CaidaFile>,
    file_idx: usize,
    next_ts: u64,
}

impl CaidaEventSource {
    fn new(caida_dir: &Path, max_files: usize) -> anyhow::Result<Self> {
        let paths = collect_caida_txt_files(caida_dir, max_files)?;
        let files = paths
            .into_iter()
            .map(CaidaFile::open)
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !files.is_empty(),
            "no CAIDA txt files found in {}",
            caida_dir.display()
        );
        Ok(Self {
            files,
            file_idx: 0,
            next_ts: 0,
        })
    }

    fn next_event(&mut self) -> anyhow::Result<Option<BenchInputEvent>> {
        loop {
            if self.file_idx >= self.files.len() {
                return Ok(None);
            }
            let f = &mut self.files[self.file_idx];
            if let Some((src_ip, dst_ip, pkt_len)) = f.next_row()? {
                let ts = self.next_ts as u32;
                self.next_ts = self.next_ts.saturating_add(1);
                // Encode src_ip and dst_ip into 15-byte key
                let key_num = ((src_ip as u64) << 32) | (dst_ip as u64);
                let key_id = Event::key_id_from_u64(key_num);
                return Ok(Some(BenchInputEvent {
                    ts,
                    key_id,
                    value: pkt_len,
                }));
            }
            self.file_idx = self.file_idx.saturating_add(1);
        }
    }
}

struct CaidaFile {
    path: PathBuf,
    r: BufReader<fs::File>,
    line_no: u64,
}

impl CaidaFile {
    fn open(path: PathBuf) -> anyhow::Result<Self> {
        let f = fs::File::open(&path)
            .with_context(|| format!("open CAIDA txt {}", path.display()))?;
        Ok(Self {
            path,
            r: BufReader::new(f),
            line_no: 0,
        })
    }

    fn next_row(&mut self) -> anyhow::Result<Option<(u32, u32, u32)>> {
        let mut line = String::new();
        loop {
            line.clear();
            let n = self
                .r
                .read_line(&mut line)
                .with_context(|| format!("read CAIDA txt {}", self.path.display()))?;
            if n == 0 {
                return Ok(None);
            }
            self.line_no = self.line_no.saturating_add(1);
            let s = line.trim();
            if s.is_empty() {
                continue;
            }
            let mut it = s.split_whitespace();
            let src = it.next();
            let dst = it.next();
            let len = it.next();
            if src.is_none() || dst.is_none() || len.is_none() || it.next().is_some() {
                anyhow::bail!(
                    "invalid CAIDA row (expected: src_ip dst_ip pkt_len) at {}:{}: {}",
                    self.path.display(),
                    self.line_no,
                    s
                );
            }
            let src_ip: u32 = src
                .unwrap()
                .parse()
                .with_context(|| format!("parse src_ip at {}:{}", self.path.display(), self.line_no))?;
            let dst_ip: u32 = dst
                .unwrap()
                .parse()
                .with_context(|| format!("parse dst_ip at {}:{}", self.path.display(), self.line_no))?;
            let pkt_len: u32 = len
                .unwrap()
                .parse()
                .with_context(|| format!("parse pkt_len at {}:{}", self.path.display(), self.line_no))?;
            return Ok(Some((src_ip, dst_ip, pkt_len)));
        }
    }
}

fn collect_caida_txt_files(caida_dir: &Path, max_files: usize) -> anyhow::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    for ent in fs::read_dir(caida_dir)
        .with_context(|| format!("read CAIDA_DIR {}", caida_dir.display()))?
    {
        let ent = ent?;
        let p = ent.path();
        if !p.is_file() {
            continue;
        }
        if p.extension().and_then(|s| s.to_str()) != Some("txt") {
            continue;
        }
        out.push(p);
    }
    let sort_by_size = env_u8("CAIDA_SORT_BY_SIZE", 1) != 0;
    if sort_by_size {
        let mut with_sizes: Vec<(PathBuf, u64)> = Vec::with_capacity(out.len());
        for p in out {
            let len = fs::metadata(&p)
                .with_context(|| format!("stat CAIDA txt {}", p.display()))?
                .len();
            with_sizes.push((p, len));
        }
        with_sizes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out = with_sizes.into_iter().map(|(p, _)| p).collect();
    } else {
        out.sort();
    }
    if max_files > 0 && out.len() > max_files {
        out.truncate(max_files);
    }
    anyhow::ensure!(
        !out.is_empty(),
        "no CAIDA txt files found in {}",
        caida_dir.display()
    );
    Ok(out)
}

fn load_caida_epochs(epochs: u64, series: u64, samples_per_series: u64) -> anyhow::Result<Vec<Vec<Event>>> {
    let epoch_events = (series as usize).saturating_mul(samples_per_series as usize);
    if epoch_events == 0 || epochs == 0 {
        return Ok(Vec::new());
    }
    let total_events = (epoch_events as u64).saturating_mul(epochs);

    let dir = parse_arg_string("--caida-dir")
        .map(PathBuf::from)
        .or_else(|| env_path("CAIDA_DIR"))
        .or_else(default_caida_dir)
        .context("CAIDA_DIR is required for BENCH_INPUT=caida")?;
    let max_files = env_usize("CAIDA_MAX_FILES", 64);

    let target_per_key = if series > 0 {
        env_usize(
            "CAIDA_EVENTS_PER_KEY",
            (samples_per_series as usize).saturating_mul(epochs as usize),
        )
        .max(1)
    } else {
        0
    };
    let mut allowed_keys: BTreeSet<[u8; KEY_BYTES_LEN]> = BTreeSet::new();
    let mut per_key: BTreeMap<[u8; KEY_BYTES_LEN], Vec<BenchInputEvent>> = BTreeMap::new();
    let mut satisfied_keys = 0usize;

    let mut source = CaidaEventSource::new(&dir, max_files)?;
    while let Some(ev) = source.next_event()? {
        if series > 0 {
            if allowed_keys.len() < series as usize {
                allowed_keys.insert(ev.key_id);
            }
            if !allowed_keys.contains(&ev.key_id) {
                continue;
            }
        }
        let entry = per_key.entry(ev.key_id).or_default();
        if target_per_key == 0 || entry.len() < target_per_key {
            entry.push(ev);
            if target_per_key > 0 && entry.len() == target_per_key {
                satisfied_keys = satisfied_keys.saturating_add(1);
                if allowed_keys.len() == series as usize && satisfied_keys >= series as usize {
                    break;
                }
            }
        }
    }

    if series > 0 && allowed_keys.len() < series as usize {
        anyhow::bail!(
            "not enough CAIDA series: need {} distinct keys, got {}; try smaller SERIES or increase CAIDA_MAX_FILES",
            series,
            allowed_keys.len()
        );
    }
    for key in &allowed_keys {
        let n = per_key.get(key).map(|v| v.len()).unwrap_or(0);
        anyhow::ensure!(
            n > 0,
            "no CAIDA events for key_id={}; try smaller SERIES or increase CAIDA_MAX_FILES",
            hex::encode(key)
        );
    }

    let keys: Vec<[u8; KEY_BYTES_LEN]> = allowed_keys.iter().copied().collect();
    let mut cursors: BTreeMap<[u8; KEY_BYTES_LEN], usize> = keys.iter().map(|k| (*k, 0usize)).collect();
    let mut out: Vec<Vec<Event>> = Vec::with_capacity(epochs as usize);

    let mut remaining = total_events;
    for _epoch in 0..epochs {
        let mut one: Vec<Event> = Vec::with_capacity(epoch_events);
        for _sample in 0..samples_per_series {
            for key in &keys {
                if remaining == 0 {
                    break;
                }
                let list = per_key.get(key).expect("key list");
                let idx = cursors.get_mut(key).expect("cursor");
                let ev = list[*idx % list.len()].clone();
                one.push(Event {
                    ts: ev.ts,
                    key_id: ev.key_id,
                    value: ev.value,
                });
                *idx += 1;
                remaining = remaining.saturating_sub(1);
            }
        }
        out.push(one);
    }
    Ok(out)
}

fn main() -> anyhow::Result<()> {
    let bench = has_flag("--bench");
    let skip_verify = has_flag("--skip-verify");
    // Non-ZK native baseline: run the SAME aggregation logic (process_*_aggr)
    // natively with no zkVM proof. Used for the camera-ready non-ZK baseline so
    // the analytics cost can be measured on the same real datasets / loaders.
    // Purely additive and opt-in; does not affect the default proving path.
    let native = has_flag("--native") || env_u8("NATIVE_ONLY", 0) != 0;
    if let Some(n) = parse_arg_opt_u64("--threads") {
        if n > 0 {
            // Used by the RISC0 prover implementation(s) when they use Rayon internally (including
            // the `r0vm` subprocess, which inherits environment variables).
            std::env::set_var("RAYON_NUM_THREADS", n.to_string());
        }
    }
    let mode = parse_arg_str("--mode", "samples");
    if has_flag("--gen-raw-shards") || env_string("GEN_RAW_SHARDS").as_deref() == Some("1") {
        return run_gen_raw_shards();
    }
    if has_flag("--gen-raw-epochs") || env_string("GEN_RAW_EPOCHS").as_deref() == Some("1") {
        return run_gen_raw_epochs();
    }
    if has_flag("--rocksdb") || env_string("AGGR_PIPELINE").as_deref() == Some("rocksdb") {
        return run_rocksdb_pipeline(&mode, skip_verify);
    }
    let epochs = parse_arg_u64("--epochs", 1).max(1);
    let series = parse_arg_u64("--series", 32).max(1);
    let samples_per_series = parse_arg_u64("--samples-per-series", 32).max(1);
    let commit_batch_size = parse_arg_u64("--commit-batch-size", 8).max(1);
    let num_sources = parse_arg_u64("--num-sources", 1).max(1) as u32;
    let seed = parse_arg_u64("--seed", 0xA66A_1E);
    let mut prev_chain_hash = [0u8; 32];

    let input_kind = bench_input_kind()?;
    let events_per_epoch = series.saturating_mul(samples_per_series);
    anyhow::ensure!(events_per_epoch > 0, "events_per_epoch must be > 0");

    // Generate batch inputs per epoch (matching Kafka production pipeline)
    // Each batch contains commit_batch_size events for a single key
    // Chain is per-source: all batches from a source chain together
    let mut epoch_batch_inputs: Vec<Vec<BatchInput>> = Vec::with_capacity(epochs as usize);
    let mut prev_source_chain_tips: BTreeMap<u32, [u8; 32]> = BTreeMap::new();
    let mut prev_source_batch_seqs: BTreeMap<u32, u64> = BTreeMap::new();

    match input_kind {
        BenchInputKind::Synthetic => {
            for i in 0..epochs {
                // Generate batches for this epoch (handles all sources)
                let (batches, new_chain_tips, new_batch_seqs) = generate_epoch_batches(
                    &mode,
                    series,
                    samples_per_series,
                    commit_batch_size,
                    num_sources,
                    seed.saturating_add(i),
                    &prev_source_chain_tips,
                    &prev_source_batch_seqs,
                );

                // Build batch inputs from StoredEventBatch (for ZK verification)
                let (batch_inputs, _) = build_batch_inputs(&batches, &prev_source_chain_tips);

                epoch_batch_inputs.push(batch_inputs);

                // Update source chain tips and batch sequences for next epoch
                prev_source_chain_tips = new_chain_tips;
                prev_source_batch_seqs = new_batch_seqs;
            }
        }
        BenchInputKind::Tsv => {
            // Legacy path: load events directly (no batch structure)
            // Wrap each epoch's events in a single batch with computed hash
            let loaded_events = load_google_cluster_epochs(epochs, series, samples_per_series)?;
            let mut prev_tip = [0u8; 32];
            let mut batch_seq = 0u64;
            for events in loaded_events {
                let batch_hash = compute_batch_chain_hash_from_events(prev_tip, &events);
                let batch = BatchInput {
                    source_id: 0,
                    source_batch_seq: batch_seq,
                    events: events.clone(),
                    sent_batch_hash: batch_hash,
                };
                epoch_batch_inputs.push(vec![batch]);
                prev_tip = batch_hash;
                batch_seq += 1;
            }
        }
        BenchInputKind::Caida => {
            // Legacy path: load events directly (no batch structure)
            // Wrap each epoch's events in a single batch with computed hash
            let loaded_events = load_caida_epochs(epochs, series, samples_per_series)?;
            let mut prev_tip = [0u8; 32];
            let mut batch_seq = 0u64;
            for events in loaded_events {
                let batch_hash = compute_batch_chain_hash_from_events(prev_tip, &events);
                let batch = BatchInput {
                    source_id: 0,
                    source_batch_seq: batch_seq,
                    events: events.clone(),
                    sent_batch_hash: batch_hash,
                };
                epoch_batch_inputs.push(vec![batch]);
                prev_tip = batch_hash;
                batch_seq += 1;
            }
        }
    }

    let epoch_events_u64 = epoch_batch_inputs
        .first()
        .map(|batches| batches.iter().map(|b| b.events.len() as u64).sum())
        .unwrap_or(0);

    let prover = default_prover();
    let proc_rss_kb_start = proc_status_kb("VmRSS:");

    let mut prove_ms_total: u128 = 0;
    let mut verify_ms_total: u128 = 0;
    let mut native_us_total: u128 = 0;
    let mut input_bytes_last: Option<u64> = None;
    let mut total_input_bytes: u64 = 0;
    let mut proof_bytes_last: Option<u64> = None;
    let mut proof_bytes_max: Option<u64> = None;
    let mut journal_bytes_last: Option<u64> = None;
    let mut events_commit_last: Option<[u8; 32]> = None;
    let mut out_commit_last: Option<[u8; 32]> = None;
    let mut epochs_done: u64 = 0;

    // Reset chain tips for ZK processing (cross-epoch verification uses prev_source_chain_tips)
    // Format: (source_id, last_processed_seq, chain_tip)
    let mut prev_source_chain_tips_for_zk: Vec<(u32, u64, [u8; 32])> = Vec::new();

    for (i, batch_inputs) in epoch_batch_inputs.into_iter().enumerate() {
        let epoch_no = (i as u64).saturating_add(1);

        match mode.as_str() {
            "samples" => {
                let input = SamplesAggrInput {
                    prev_chain_hash,
                    batches: batch_inputs,
                    prev_source_chain_tips: prev_source_chain_tips_for_zk.clone(),
                };
                let nat_start = std::time::Instant::now();
                let (state, expected) = process_samples_aggr_with_state(&input);
                native_us_total = native_us_total.saturating_add(nat_start.elapsed().as_micros());
                let input_bytes = to_vec(&input).map(|v| v.len() as u64).ok();
                if let Some(n) = input_bytes {
                    input_bytes_last = Some(n);
                    total_input_bytes = total_input_bytes.saturating_add(n);
                }

                let out: SamplesAggrOutput = if native {
                    expected.clone()
                } else {
                    let env = ExecutorEnv::builder()
                        .write(&input)
                        .context("zkvm write input")?
                        .build()
                        .context("build executor env")?;

                    let prove_start = std::time::Instant::now();
                    let opts = ProverOpts::succinct();
                    let prove_info = prover.prove_with_opts(env, AGGR_SAMPLES_ELF, &opts)?;
                    let prove_ms = prove_start.elapsed().as_millis() as u128;
                    prove_ms_total = prove_ms_total.saturating_add(prove_ms);

                    let verify_start = std::time::Instant::now();
                    if !skip_verify {
                        prove_info.receipt.verify(AGGR_SAMPLES_ID)?;
                    }
                    let verify_ms = verify_start.elapsed().as_millis() as u128;
                    verify_ms_total = verify_ms_total.saturating_add(verify_ms);

                    let decoded: SamplesAggrOutput = prove_info
                        .receipt
                        .journal
                        .decode()
                        .context("decode journal")?;
                    anyhow::ensure!(
                        decoded == expected,
                        "guest output mismatch vs host recompute (epoch {})",
                        epoch_no
                    );
                    let proof_bytes = to_vec(&prove_info.receipt).map(|v| v.len() as u64).ok();
                    let journal_bytes = to_vec(&prove_info.receipt.journal)
                        .map(|v| v.len() as u64)
                        .ok();
                    if let Some(n) = proof_bytes {
                        proof_bytes_last = Some(n);
                        proof_bytes_max = Some(proof_bytes_max.unwrap_or(0).max(n));
                    }
                    if let Some(n) = journal_bytes {
                        journal_bytes_last = Some(n);
                    }
                    decoded
                };
                prev_chain_hash = out.epoch_chain_link.final_chain_hash;
                // Flatten events from batches for events_commit computation
                let flat_events: Vec<Event> = input.batches.iter()
                    .flat_map(|b| b.events.iter().cloned())
                    .collect();
                let events_commit = events_commit_from_events(&flat_events);
                events_commit_last = Some(events_commit);
                // Compute out_commit locally since it's no longer in journal
                let out_commit = compute_samples_out_commit(
                    &out.buckets_root,
                    state.total_count,
                    state.total_sum,
                    out.n_events,
                    &out.final_source_chain_tips,
                );
                out_commit_last = Some(out_commit);

                epochs_done = epochs_done.saturating_add(1);

                // Update per-source chain tips for next epoch's cross-epoch verification
                prev_source_chain_tips_for_zk = out.final_source_chain_tips.clone();

                if !bench && epoch_no == epochs {
                    println!("mode=samples n_events={}", out.n_events);
                    println!("final_chain_hash_hex={}", hex::encode(out.epoch_chain_link.final_chain_hash));
                    println!("events_commit_hex={}", hex::encode(events_commit));
                    println!("out_commit_hex={}", hex::encode(out_commit));
                    println!("buckets_root_hex={}", hex::encode(out.buckets_root));
                    println!("total_count={}", state.total_count);
                    println!("total_sum={}", state.total_sum);
                }
            }
            "histogram" => {
                let input = HistogramAggrInput {
                    prev_chain_hash,
                    batches: batch_inputs.clone(),
                    prev_source_chain_tips: prev_source_chain_tips_for_zk.clone(),
                };
                let nat_start = std::time::Instant::now();
                let (state, expected) = process_histogram_aggr_with_state(&input);
                native_us_total = native_us_total.saturating_add(nat_start.elapsed().as_micros());
                let input_bytes = to_vec(&input).map(|v| v.len() as u64).ok();
                if let Some(n) = input_bytes {
                    input_bytes_last = Some(n);
                    total_input_bytes = total_input_bytes.saturating_add(n);
                }

                let out: HistogramAggrOutput = if native {
                    expected.clone()
                } else {
                    let env = ExecutorEnv::builder()
                        .write(&input)
                        .context("zkvm write input")?
                        .build()
                        .context("build executor env")?;

                    let prove_start = std::time::Instant::now();
                    let opts = ProverOpts::succinct();
                    let prove_info = prover.prove_with_opts(env, AGGR_HISTOGRAM_ELF, &opts)?;
                    let prove_ms = prove_start.elapsed().as_millis() as u128;
                    prove_ms_total = prove_ms_total.saturating_add(prove_ms);

                    let verify_start = std::time::Instant::now();
                    if !skip_verify {
                        prove_info.receipt.verify(AGGR_HISTOGRAM_ID)?;
                    }
                    let verify_ms = verify_start.elapsed().as_millis() as u128;
                    verify_ms_total = verify_ms_total.saturating_add(verify_ms);

                    let decoded: HistogramAggrOutput = prove_info
                        .receipt
                        .journal
                        .decode()
                        .context("decode journal")?;
                    anyhow::ensure!(
                        decoded == expected,
                        "guest output mismatch vs host recompute (epoch {})",
                        epoch_no
                    );
                    let proof_bytes = to_vec(&prove_info.receipt).map(|v| v.len() as u64).ok();
                    let journal_bytes = to_vec(&prove_info.receipt.journal)
                        .map(|v| v.len() as u64)
                        .ok();
                    if let Some(n) = proof_bytes {
                        proof_bytes_last = Some(n);
                        proof_bytes_max = Some(proof_bytes_max.unwrap_or(0).max(n));
                    }
                    if let Some(n) = journal_bytes {
                        journal_bytes_last = Some(n);
                    }
                    decoded
                };
                prev_chain_hash = out.epoch_chain_link.final_chain_hash;
                // Flatten events from batches for events_commit computation
                let flat_events: Vec<Event> = input.batches.iter()
                    .flat_map(|b| b.events.iter().cloned())
                    .collect();
                let events_commit = events_commit_from_events(&flat_events);
                events_commit_last = Some(events_commit);
                // Compute out_commit locally since it's no longer in journal
                let out_commit = compute_histogram_out_commit(
                    &out.buckets_root,
                    state.total_count,
                    state.total_sum,
                    out.n_events,
                    &out.final_source_chain_tips,
                );
                out_commit_last = Some(out_commit);

                epochs_done = epochs_done.saturating_add(1);

                // Update per-source chain tips for next epoch's cross-epoch verification
                prev_source_chain_tips_for_zk = out.final_source_chain_tips.clone();

                if !bench && epoch_no == epochs {
                    println!("mode=histogram n_events={}", out.n_events);
                    println!("final_chain_hash_hex={}", hex::encode(out.epoch_chain_link.final_chain_hash));
                    println!("events_commit_hex={}", hex::encode(events_commit));
                    println!("out_commit_hex={}", hex::encode(out_commit));
                    println!("buckets_root_hex={}", hex::encode(out.buckets_root));
                    println!("total_count={}", state.total_count);
                    println!("total_sum={}", state.total_sum);
                    println!("state_commit_hex={}", hex::encode(out.state_commit));
                }
            }
            "cm" => {
                let input = CmAggrInput {
                    prev_chain_hash,
                    batches: batch_inputs.clone(),
                    prev_source_chain_tips: prev_source_chain_tips_for_zk.clone(),
                };
                let nat_start = std::time::Instant::now();
                let (state, expected) = process_cm_aggr_with_state(&input);
                native_us_total = native_us_total.saturating_add(nat_start.elapsed().as_micros());
                let input_bytes = to_vec(&input).map(|v| v.len() as u64).ok();
                if let Some(n) = input_bytes {
                    input_bytes_last = Some(n);
                    total_input_bytes = total_input_bytes.saturating_add(n);
                }

                let out: CmAggrOutput = if native {
                    expected.clone()
                } else {
                    let env = ExecutorEnv::builder()
                        .write(&input)
                        .context("zkvm write input")?
                        .build()
                        .context("build executor env")?;

                    let prove_start = std::time::Instant::now();
                    let opts = ProverOpts::succinct();
                    let prove_info = prover.prove_with_opts(env, AGGR_CM_ELF, &opts)?;
                    let prove_ms = prove_start.elapsed().as_millis() as u128;
                    prove_ms_total = prove_ms_total.saturating_add(prove_ms);

                    let verify_start = std::time::Instant::now();
                    if !skip_verify {
                        prove_info.receipt.verify(AGGR_CM_ID)?;
                    }
                    let verify_ms = verify_start.elapsed().as_millis() as u128;
                    verify_ms_total = verify_ms_total.saturating_add(verify_ms);

                    let decoded: CmAggrOutput = prove_info
                        .receipt
                        .journal
                        .decode()
                        .context("decode journal")?;
                    anyhow::ensure!(
                        decoded == expected,
                        "guest output mismatch vs host recompute (epoch {})",
                        epoch_no
                    );
                    let proof_bytes = to_vec(&prove_info.receipt).map(|v| v.len() as u64).ok();
                    let journal_bytes = to_vec(&prove_info.receipt.journal)
                        .map(|v| v.len() as u64)
                        .ok();
                    if let Some(n) = proof_bytes {
                        proof_bytes_last = Some(n);
                        proof_bytes_max = Some(proof_bytes_max.unwrap_or(0).max(n));
                    }
                    if let Some(n) = journal_bytes {
                        journal_bytes_last = Some(n);
                    }
                    decoded
                };
                prev_chain_hash = out.epoch_chain_link.final_chain_hash;
                // Flatten events from batches for events_commit computation
                let flat_events: Vec<Event> = input.batches.iter()
                    .flat_map(|b| b.events.iter().cloned())
                    .collect();
                let events_commit = events_commit_from_events(&flat_events);
                events_commit_last = Some(events_commit);
                // Compute out_commit locally since it's no longer in journal
                let out_commit = compute_cm_out_commit(
                    &out.cm_root,
                    &out.heap_root,
                    state.total_sum,
                    out.n_events,
                    &out.final_source_chain_tips,
                );
                out_commit_last = Some(out_commit);

                epochs_done = epochs_done.saturating_add(1);

                // Update per-source chain tips for next epoch's cross-epoch verification
                prev_source_chain_tips_for_zk = out.final_source_chain_tips.clone();

                if !bench && epoch_no == epochs {
                    println!("mode=cm");
                    println!("final_chain_hash_hex={}", hex::encode(out.epoch_chain_link.final_chain_hash));
                    if let Some(ev) = events_commit_last {
                        println!("events_commit_hex={}", hex::encode(ev));
                    }
                    if let Some(oc) = out_commit_last {
                        println!("out_commit_hex={}", hex::encode(oc));
                    }
                    println!("cm_root_hex={}", hex::encode(out.cm_root));
                    println!("cm_topk_heap_root_hex={}", hex::encode(out.heap_root));
                    println!("row0_root_hex={}", hex::encode(out.row_roots[0]));
                    println!("row1_root_hex={}", hex::encode(out.row_roots[1]));
                    println!("row2_root_hex={}", hex::encode(out.row_roots[2]));
                    println!("total_sum={}", state.total_sum);

                    let mut shown = 0usize;
                    for i in 0..state.heap_occ.len() {
                        if state.heap_occ[i] == 0 {
                            continue;
                        }
                        println!(
                            "topk_slot_{}=key_id:{} score:{}",
                            i, hex::encode(state.heap_keys[i]), state.heap_vals[i]
                        );
                        shown += 1;
                        if shown >= 10 {
                            break;
                        }
                    }
                }
            }
            other => anyhow::bail!(
                "unsupported --mode {}; expected samples|histogram|cm",
                other
            ),
        }
    }

    let proc_rss_kb_end = proc_status_kb("VmRSS:");
    let proc_hwm_kb = proc_status_kb("VmHWM:");
    if bench {
        println!("bench=1");
        println!("mode={}", mode);
        println!("epochs={}", epochs_done);
        println!("series={}", series);
        println!("samples_per_series={}", samples_per_series);
        println!("commit_batch_size={}", commit_batch_size);
        println!("epoch_events={}", epoch_events_u64);
        if let Some(n) = input_bytes_last {
            println!("input_bytes={}", n);
        }
        println!("total_input_bytes={}", total_input_bytes);
        println!("native={}", if native { 1 } else { 0 });
        println!("native_us_total={}", native_us_total);
        println!("native_ms_total={}", native_us_total as f64 / 1000.0);
        println!("prove_ms_total={}", prove_ms_total);
        println!(
            "prove_ms_per_epoch={}",
            if epochs_done > 0 {
                prove_ms_total / epochs_done as u128
            } else {
                0
            }
        );
        println!("verify_ms_total={}", verify_ms_total);
        println!(
            "verify_ms_per_epoch={}",
            if epochs_done > 0 {
                verify_ms_total / epochs_done as u128
            } else {
                0
            }
        );
        if let Some(kb) = proc_rss_kb_start {
            println!("proc_rss_kb_start={}", kb);
        }
        if let Some(kb) = proc_rss_kb_end {
            println!("proc_rss_kb_end={}", kb);
        }
        if let Some(kb) = proc_hwm_kb {
            println!("proc_hwm_kb={}", kb);
        }
        if let Some(n) = proof_bytes_last {
            println!("proof_bytes_last={}", n);
        }
        if let Some(n) = proof_bytes_max {
            println!("proof_bytes_max={}", n);
        }
        if let Some(n) = journal_bytes_last {
            println!("journal_bytes_last={}", n);
        }
        if let Some(ev) = events_commit_last {
            println!("events_commit_hex={}", hex::encode(ev));
        }
        if let Some(oc) = out_commit_last {
            println!("out_commit_hex={}", hex::encode(oc));
        }
    }
    Ok(())
}
