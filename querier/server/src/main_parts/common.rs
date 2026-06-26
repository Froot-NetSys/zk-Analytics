use anyhow::{Context, Result};
use axum::{extract::State, routing::post, Json, Router};
use rand::SeedableRng;
use risc0_zkvm::{default_prover, ExecutorEnv, ProverOpts};
use risc0_zkvm::serde::to_vec;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use common::dp;
use common::epoch::EpochType;
use common::rocksdb_store::{
    AggCmStruct, AggEpoch, AggEpochMeta, AggEpochProof, AggHistStruct,
    SampleEvent, SampleShardFrame, SeriesShardFrame, ShardedRocksDb, VerifiedSamplesStruct,
};
#[cfg(feature = "fdb")]
use common::fdb_store::{FdbStore, FdbStoreSync};
use aggregator_core as acore;
use querier_core as qcore;

/// Abstraction over data store backends (ShardedRocksDb or FdbStoreSync)
enum DataStore {
    RocksDb(ShardedRocksDb),
    #[cfg(feature = "fdb")]
    Fdb(FdbStoreSync),
}

impl DataStore {
    fn agg_epochs(&self) -> Result<Vec<AggEpoch>> {
        match self {
            DataStore::RocksDb(db) => db.agg_epochs(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(fdb) => fdb.agg_epochs(),
        }
    }

    #[allow(dead_code)]
    fn agg_epoch_meta(&self) -> Result<Vec<AggEpochMeta>> {
        match self {
            DataStore::RocksDb(db) => db.agg_epoch_meta(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(fdb) => fdb.agg_epoch_meta(),
        }
    }

    #[allow(dead_code)]
    fn agg_epoch_proofs(&self) -> Result<Vec<AggEpochProof>> {
        match self {
            DataStore::RocksDb(db) => db.agg_epoch_proofs(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(fdb) => fdb.agg_epoch_proofs(),
        }
    }

    fn agg_cm_structs(&self) -> Result<Vec<AggCmStruct>> {
        match self {
            DataStore::RocksDb(db) => db.agg_cm_structs(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(fdb) => fdb.agg_cm_structs(),
        }
    }

    fn agg_hist_structs(&self) -> Result<Vec<AggHistStruct>> {
        match self {
            DataStore::RocksDb(db) => db.agg_hist_structs(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(fdb) => fdb.agg_hist_structs(),
        }
    }

    fn verified_samples_structs(&self) -> Result<Vec<VerifiedSamplesStruct>> {
        match self {
            DataStore::RocksDb(db) => db.verified_samples_structs(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(fdb) => fdb.verified_samples_structs(),
        }
    }

    // Raw data methods - only available with RocksDb backend
    #[allow(dead_code)]
    fn sample_shard_frames(&self) -> Result<Vec<SampleShardFrame>> {
        match self {
            DataStore::RocksDb(db) => db.sample_shard_frames(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(_) => anyhow::bail!("sample_shard_frames not available in FDB mode"),
        }
    }

    fn sample_events(&self) -> Result<Vec<SampleEvent>> {
        match self {
            DataStore::RocksDb(db) => db.sample_events(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(_) => anyhow::bail!("sample_events not available in FDB mode"),
        }
    }

    #[allow(dead_code)]
    fn series_shard_frames(&self) -> Result<Vec<SeriesShardFrame>> {
        match self {
            DataStore::RocksDb(db) => db.series_shard_frames(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(_) => anyhow::bail!("series_shard_frames not available in FDB mode"),
        }
    }

    #[allow(dead_code)]
    fn catch_up_if_secondary(&self) -> Result<()> {
        match self {
            DataStore::RocksDb(db) => db.catch_up_if_secondary(),
            #[cfg(feature = "fdb")]
            DataStore::Fdb(_) => Ok(()), // No-op for FDB
        }
    }
}

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<DataStore>>,
    /// Access-control policy, built once at startup (see `build_query_policy`).
    /// `None` means enforcement is disabled. `Arc` keeps per-request `AppState`
    /// clones cheap.
    pub policy: std::sync::Arc<Option<query_checker::QueryPolicy>>,
}

#[allow(dead_code)]
fn proof_compress_enabled() -> bool {
    // Keep the knob for script compatibility, even though we currently return empty proofs.
    std::env::var("PROOF_COMPRESS").ok().as_deref() != Some("0")
}

#[derive(serde::Deserialize)]
struct TimeWindow {
    /// Window like `"5m"`, `"1h"`, `"1d"`, `"30s"`.
    window: Option<String>,
    /// Optional end time (ms since unix epoch). Defaults to "now".
    end_ms: Option<i64>,
    /// Optional number of latest epochs to select (overrides window).
    epochs: Option<usize>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum QueryRequest {
    CmEstimate {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
    },
    CmTopk {
        #[serde(flatten)]
        tw: TimeWindow,
        limit: Option<usize>,
    },
    HistogramBucket {
        #[serde(flatten)]
        tw: TimeWindow,
        bucket: u16,
    },
    HistogramAll {
        #[serde(flatten)]
        tw: TimeWindow,
    },
    /// Histogram bucket distribution filtered by a 15-byte key pattern.
    /// Pattern is a hex string (up to 30 hex chars) with '?' wildcards for don't-care nibbles.
    /// E.g. "????????aabbccdd??????????????" matches bytes 4-7 = aabbccdd (Model hash).
    HistogramAllKey {
        #[serde(flatten)]
        tw: TimeWindow,
        /// Hex pattern for the 15-byte key (up to 30 hex chars, '?'/'*' = wildcard nibble).
        pattern: String,
    },
    HistogramP90 {
        #[serde(flatten)]
        tw: TimeWindow,
    },
    SamplesAvg {
        #[serde(flatten)]
        tw: TimeWindow,
    },
    SamplesAvgKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
        /// Optional bitmask for matching: include a slot if `(slot_key & mask) == (key & mask)`.
        /// For exact match, omit or use `mask = 0xffff_ffff_ffff_ffff`.
        mask: Option<u64>,
    },
    SamplesSum {
        #[serde(flatten)]
        tw: TimeWindow,
    },
    SamplesSumExactKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
    },
    SamplesSumKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
        /// Optional bitmask for matching: include a slot if `(slot_key & mask) == (key & mask)`.
        /// For exact match, omit or use `mask = 0xffff_ffff_ffff_ffff`.
        mask: Option<u64>,
    },
    SamplesSumKeyPattern {
        #[serde(flatten)]
        tw: TimeWindow,
    /// Hex pattern (up to 16 nibbles, optional 0x prefix), or binary pattern (up to 64 bits, "b" or "0b" prefix).
    /// Use '?' or '*' for a wildcard nibble/bit. Shorter patterns are treated as prefix and right-padded with '?'.
    pattern: String,
    },
    SamplesRawMaxKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
        /// Bitmask for matching: include an event if `(key_id & mask) == (key & mask)`.
        mask: u64,
    },
    SamplesRawHistogramBucketKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
        /// Bitmask for matching: include an event if `(key_id & mask) == (key & mask)`.
        mask: u64,
        /// Histogram bucket index (same log2/msb bucket definition as `histogram_epoch`).
        bucket: u16,
    },
    SamplesRawCmEstimateKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
        /// Bitmask for matching: include an event if `(key_id & mask) == (key & mask)`.
        mask: u64,
        /// Value whose frequency is estimated (Count-Min over values within the selected series).
        value: u64,
    },
    SamplesRawStatsKey {
        #[serde(flatten)]
        tw: TimeWindow,
        key: u64,
        /// Bitmask for matching: include an event if `(key_id & mask) == (key & mask)`.
        mask: u64,
    },
    SamplesSumTopk {
        #[serde(flatten)]
        tw: TimeWindow,
        limit: Option<usize>,
    },
}

#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum QueryResponse {
    CmEstimate { estimate: u64, dp_offset: u64, suppressed: bool, proof: ProofBundle },
    CmTopk { items: Vec<TopKItem>, dp_offset_count: u64, suppressed: bool, proof: ProofBundle },
    HistogramBucket { bucket: u16, count: u64, dp_offset: u64, suppressed: bool, proof: ProofBundle },
    HistogramAll {
        total_count: u64,
        total_sum: u64,
        buckets: Vec<HistogramBucketItem>,
        dp_offset_total_count: u64,
        dp_offset_total_sum: u64,
        dp_offset_bucket_count: u64,
        suppressed: bool,
        proof: ProofBundle,
    },
    HistogramAllKey {
        pattern: String,
        total_count: u64,
        total_sum: u64,
        buckets: Vec<HistogramBucketItem>,
        dp_offset_total_count: u64,
        dp_offset_total_sum: u64,
        dp_offset_bucket_count: u64,
        suppressed: bool,
        proof: ProofBundle,
    },
    HistogramP90 {
        p90_value: u64,
        total_count: u64,
        suppressed: bool,
        proof: ProofBundle,
    },
    SamplesAvg { avg: u64, dp_offset_sum: u64, dp_offset_count: u64, suppressed: bool, proof: ProofBundle },
    SamplesAvgKey { key: u64, avg: u64, dp_offset_sum: u64, dp_offset_count: u64, suppressed: bool, proof: ProofBundle },
    SamplesSum { sum: u64, dp_offset_sum: u64, suppressed: bool, proof: ProofBundle },
    SamplesSumExactKey { key: u64, sum: u64, dp_offset_sum: u64, suppressed: bool, proof: ProofBundle },
    SamplesSumKey { key: u64, sum: u64, dp_offset_sum: u64, suppressed: bool, proof: ProofBundle },
    SamplesSumKeyPattern { pattern: String, sum_keys: u64, dp_offset_sum: u64, suppressed: bool, proof: ProofBundle },
    SamplesRawMaxKey { items: Vec<SamplesRawMaxKeyItem>, dp_offset: u64, proof: ProofBundle },
    SamplesRawHistogramBucketKey {
        key: u64,
        bucket: u16,
        count: u64,
        dp_offset: u64,
        proof: ProofBundle,
    },
    SamplesRawCmEstimateKey {
        key: u64,
        value: u64,
        estimate: u64,
        dp_offset: u64,
        proof: ProofBundle,
    },
    SamplesRawStatsKey {
        key: u64,
        sum: u64,
        dp_offset_sum: u64,
        proof: ProofBundle,
    },
    SamplesSumTopk {
        items: Vec<SamplesSumTopkItem>,
        dp_offset_sum: u64,
        suppressed: bool,
        proof: ProofBundle,
    },
}

#[derive(serde::Serialize)]
struct ProofBundle {
    // Keep compatible with Nova schema: 0=RecursiveSNARK, 1=CompressedSNARK.
    // We return empty proofs for now, so this is always 0.
    proof_kind: u16,
    num_steps: u32,
    digest_hex: String,
    proof_hex: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    prove_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    proof_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    journal_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    proof_file: Option<String>,
}

#[derive(serde::Serialize)]
struct TopKItem {
    #[serde(serialize_with = "serialize_key_hex")]
    key: [u8; 15],
    count: u64,
}

fn serialize_key_hex<S>(key: &[u8; 15], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&hex::encode(key))
}

#[derive(serde::Serialize)]
struct SamplesSumTopkItem {
    sum: u64,
}

#[derive(serde::Serialize)]
struct SamplesRawMaxKeyItem {
    key: u64,
    max: u64,
}

#[derive(serde::Serialize)]
struct HistogramBucketItem {
    bucket: u16,
    count: u64,
}

#[allow(dead_code)]
const U48_BYTES: usize = 6;
#[allow(dead_code)]
const U48_MAX_PLUS_ONE: u64 = 1u64 << 48;
const SAMPLES_RAW_QUERY_MAX_EVENTS: usize = 1_000_000;
const CM_SEEDS_U32: [u32; acore::CM_ROWS] = [0x6d0f27bd, 0x9e3779b9, 0x94d049bb];
const CM_LEAVES: usize = acore::CM_COLS;  // CM_COLS = 1024, which was 1 << CM_MERKLE_DEPTH

fn dp_enabled() -> bool {
    match std::env::var("DP_ENABLED") {
        Err(_) => true,
        Ok(s) => {
            let s = s.trim();
            !(s == "0" || s.eq_ignore_ascii_case("false"))
        }
    }
}

/// Generate randomized Laplace noise for differential privacy.
/// Returns the noise value (can be negative) that should be added to the result.
#[allow(dead_code)]
fn dp_noise(cfg: dp::DpConfig) -> i64 {
    if !dp_enabled() {
        return 0;
    }
    // Use thread-local RNG seeded from system entropy
    let mut rng = rand::rngs::StdRng::from_entropy();
    cfg.laplace_noise(&mut rng).round() as i64
}

/// Apply DP noise to a u64 value, clamping to non-negative.
/// Returns (noisy_value, noise_applied).
#[allow(dead_code)]
fn dp_apply_noise_u64(cfg: dp::DpConfig, value: u64) -> (u64, i64) {
    if !dp_enabled() {
        return (value, 0);
    }
    let mut rng = rand::rngs::StdRng::from_entropy();
    cfg.apply_noise_u64(value, &mut rng)
}

/// Legacy function for backwards compatibility - returns the fixed offset.
fn dp_offset(cfg: dp::DpConfig) -> u64 {
    if dp_enabled() { cfg.b } else { 0 }
}

#[allow(dead_code)]
fn maybe_omit_proof(mut proof: ProofBundle, omit: bool) -> ProofBundle {
    if omit {
        proof.proof_hex.clear();
    }
    proof
}

fn bench_print_enabled() -> bool {
    std::env::var("BENCH_PRINT").ok().as_deref() == Some("1")
}

fn bench_query_log(
    query_kind: &str,
    epoch_kind: &str,
    epochs: usize,
    rows: usize,
    db_ms: u64,
    merge_ms: u64,
    use_direct: bool,
) {
    if bench_print_enabled() {
        eprintln!(
            "bench query={} epoch_kind={} epochs={} epochs_total={} rows={} merge_cnt={} direct={} db_ms={} merge_ms={}",
            query_kind,
            epoch_kind,
            epochs,
            epochs,
            rows,
            rows,
            if use_direct { 1 } else { 0 },
            db_ms,
            merge_ms
        );
    }
}

fn bench_match_log(match_keys: u64) {
    if bench_print_enabled() {
        eprintln!("bench match_keys={}", match_keys);
    }
}

fn bench_proof_log(
    kind: &str,
    steps: u64,
    setup_ms: u64,
    prove_ms: u64,
    verify_ms: u64,
    proof_bytes: u64,
    journal_bytes: u64,
) {
    if bench_print_enabled() {
        let total_ms = prove_ms.saturating_add(verify_ms);
        eprintln!(
            "bench kind={} steps={} circuits=0 setup_ms={} prove_ms={} verify_ms={} compress_ms=0 verify_compressed_ms=0 proof_bytes={} journal_bytes={} total_ms={}",
            kind, steps, setup_ms, prove_ms, verify_ms, proof_bytes, journal_bytes, total_ms
        );
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn parse_window_ms(spec: &str) -> Result<i64> {
    let s = spec.trim();
    anyhow::ensure!(!s.is_empty(), "empty window");

    // Support: Ns, Nm, Nh, Nd.
    let (num_part, unit) = s.split_at(s.len().saturating_sub(1));
    let n: i64 = num_part
        .parse()
        .with_context(|| format!("invalid window number '{num_part}'"))?;
    anyhow::ensure!(n > 0, "window must be > 0");
    let ms = match unit {
        "s" => n.checked_mul(1_000).context("window overflow")?,
        "m" => n.checked_mul(60_000).context("window overflow")?,
        "h" => n.checked_mul(3_600_000).context("window overflow")?,
        "d" => n.checked_mul(86_400_000).context("window overflow")?,
        _ => anyhow::bail!("unsupported window unit '{unit}', use s/m/h/d"),
    };
    Ok(ms)
}

fn resolve_time_window(tw: &TimeWindow) -> Result<(i64, i64)> {
    let window = tw.window.as_deref().context("missing window")?;
    let end_ms = tw.end_ms.unwrap_or_else(now_ms);
    let win_ms = parse_window_ms(window)?;
    let start_ms = end_ms.checked_sub(win_ms).context("time underflow")?;
    Ok((start_ms, end_ms))
}

fn cm_bucket_index(key: u64, row: usize) -> u16 {
    // Keep in sync with `zkTelemetry/aggregator/src/epoch.rs`.
    const CM_COLS: usize = acore::CM_COLS;
    let seed = CM_SEEDS_U32[row];
    let mut x = key ^ (((seed as u64) << 32) | seed as u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    ((x as usize) % CM_COLS) as u16
}

#[allow(dead_code)]
fn unpack_u48_le(b: &[u8]) -> Result<u64> {
    anyhow::ensure!(b.len() == U48_BYTES, "u48 requires 6 bytes");
    let mut tmp = [0u8; 8];
    tmp[..U48_BYTES].copy_from_slice(b);
    Ok(u64::from_le_bytes(tmp))
}

/// Unpack CM counts from u32 storage (4 bytes each, little-endian).
/// Direct unpacking without wasteful u48 intermediate - clean and fast.
fn unpack_cm_counts_u32(bytes: &[u8]) -> Result<Vec<u32>> {
    let want = acore::CM_ROWS * acore::CM_COLS;
    anyhow::ensure!(
        bytes.len() == want * 4,
        "cm counts_u32 length mismatch: expected {} bytes, got {}",
        want * 4,
        bytes.len()
    );
    let mut out = Vec::with_capacity(want);
    for chunk in bytes.chunks_exact(4) {
        let v = u32::from_le_bytes(chunk.try_into().unwrap());
        out.push(v);
    }
    Ok(out)
}

fn unpack_cm_heap_fixed(bytes: &[u8]) -> Result<Vec<([u8; 15], u64, u8)>> {
    // Stored as (key_id[15], value[8], occ[1]) per slot = 24 bytes
    let want = acore::CM_TOPK_SLOTS * (15 + 8 + 1);
    anyhow::ensure!(bytes.len() == want, "cm heap_fixed length mismatch");
    let mut out = Vec::with_capacity(acore::CM_TOPK_SLOTS);
    let mut i = 0usize;
    while i < bytes.len() {
        // Read full 15-byte key_id directly - no conversion, no data loss!
        let mut k = [0u8; 15];
        k.copy_from_slice(&bytes[i..i + 15]);
        let v = u64::from_be_bytes(bytes[i + 15..i + 23].try_into().unwrap());
        let o = bytes[i + 23];
        anyhow::ensure!(o == 0 || o == 1, "heap occ must be 0/1");
        out.push((k, v, o));
        i += 24;
    }
    Ok(out)
}

#[allow(dead_code)]
fn unpack_hist_table_fixed(bytes: &[u8]) -> Result<Vec<(u16, u64, u8)>> {
    let want = acore::HISTOGRAM_SLOTS * (2 + 8 + 1);
    anyhow::ensure!(bytes.len() == want, "hist table_fixed length mismatch");
    let mut out = Vec::with_capacity(acore::HISTOGRAM_SLOTS);
    let mut i = 0usize;
    while i < bytes.len() {
        let b = u16::from_be_bytes(bytes[i..i + 2].try_into().unwrap());
        let c = u64::from_be_bytes(bytes[i + 2..i + 10].try_into().unwrap());
        let o = bytes[i + 10];
        anyhow::ensure!(o == 0 || o == 1, "hist occ must be 0/1");
        out.push((b, c, o));
        i += 11;
    }
    Ok(out)
}

#[allow(dead_code)]
fn unpack_hist_counts_fixed(bytes: &[u8]) -> Result<Vec<u64>> {
    let table = unpack_hist_table_fixed(bytes)?;
    let mut counts = vec![0u64; acore::HISTOGRAM_SLOTS];
    for (b, c, o) in table {
        let idx = b as usize;
        anyhow::ensure!(idx < acore::HISTOGRAM_SLOTS, "hist bucket out of range");
        if o == 1 {
            counts[idx] = c;
        }
    }
    Ok(counts)
}

/// Unpack per-key histogram data from table_fixed bytes
/// Returns Vec of (key_id, bucket_counts, total_count, total_sum)
fn unpack_hist_per_key_table_fixed(bytes: &[u8]) -> Result<Vec<([u8; 15], Vec<u32>, u64, u64)>> {
    anyhow::ensure!(bytes.len() >= 4, "insufficient data for num_keys");

    let mut offset = 0;
    let num_keys = u32::from_be_bytes(bytes[offset..offset+4].try_into().unwrap()) as usize;
    offset += 4;

    let mut result = Vec::with_capacity(num_keys);

    for _ in 0..num_keys {
        anyhow::ensure!(offset + 15 <= bytes.len(), "insufficient data for key_id");

        // Read key_id (15 bytes - KEY_BYTES_LEN)
        let mut key_id = [0u8; 15];
        key_id.copy_from_slice(&bytes[offset..offset+15]);
        offset += 15;

        anyhow::ensure!(offset + 2 <= bytes.len(), "insufficient data for num_buckets");

        // Read number of non-zero buckets
        let num_buckets = u16::from_be_bytes(bytes[offset..offset+2].try_into().unwrap()) as usize;
        offset += 2;

        // Initialize bucket counts
        let mut bucket_counts = vec![0u32; acore::HISTOGRAM_SLOTS];

        // Read each non-zero bucket
        for _ in 0..num_buckets {
            anyhow::ensure!(offset + 2 + 4 <= bytes.len(), "insufficient data for bucket entry");

            let bucket_idx = u16::from_be_bytes(bytes[offset..offset+2].try_into().unwrap()) as usize;
            offset += 2;

            let count = u32::from_be_bytes(bytes[offset..offset+4].try_into().unwrap());
            offset += 4;

            anyhow::ensure!(bucket_idx < acore::HISTOGRAM_SLOTS, "bucket index out of range");
            bucket_counts[bucket_idx] = count;
        }

        anyhow::ensure!(offset + 4 + 8 <= bytes.len(), "insufficient data for count and sum");

        // Read total count (u32) and sum (u64)
        let total_count = u32::from_be_bytes(bytes[offset..offset+4].try_into().unwrap()) as u64;
        offset += 4;

        let total_sum = u64::from_be_bytes(bytes[offset..offset+8].try_into().unwrap());
        offset += 8;

        result.push((key_id, bucket_counts, total_count, total_sum));
    }

    Ok(result)
}

/// Build CmEpochState from AggCmStruct data
fn build_cm_epoch_state(s: &AggCmStruct) -> Result<acore::CmEpochState> {
    // Direct u32 unpacking - no conversion needed!
    let counts = unpack_cm_counts_u32(&s.counts_u32)?;

    eprintln!("[DEBUG] Building CmEpochState: seq={} num_counts={} total_sum={} prev_chain_hash_len={}",
        s.sequence, counts.len(), s.total_sum, s.prev_chain_hash.len());

    // Log sample of counts for debugging
    if counts.len() >= 10 {
        eprintln!("[DEBUG]   counts[0..10]: {:?}", &counts[0..10]);
        eprintln!("[DEBUG]   counts sum (first 100): {}", counts.iter().take(100).map(|&c| c as u64).sum::<u64>());
    }

    // Unpack heap data
    let heap_data = unpack_cm_heap_fixed(&s.heap_fixed)?;
    let mut heap_keys: Vec<[u8; 15]> = Vec::with_capacity(acore::CM_TOPK_SLOTS);
    let mut heap_vals: Vec<u64> = Vec::with_capacity(acore::CM_TOPK_SLOTS);
    let mut heap_occ: Vec<u8> = Vec::with_capacity(acore::CM_TOPK_SLOTS);

    for (k, v, o) in heap_data {
        // Use full 15-byte key directly - matches what aggregator stored!
        heap_keys.push(k);
        heap_vals.push(v);
        heap_occ.push(o);
    }

    // Log heap data for debugging
    let occupied_heap = heap_occ.iter().filter(|&&o| o != 0).count();
    eprintln!("[DEBUG]   heap: occupied_slots={} first_3_vals={:?}",
        occupied_heap,
        heap_vals.iter().take(3).collect::<Vec<_>>()
    );

    Ok(acore::CmEpochState {
        counts,
        heap_keys,
        heap_vals,
        heap_occ,
        total_sum: s.total_sum,
    })
}

/// Build HistogramEpochState from AggHistStruct data
fn build_histogram_epoch_state(s: &AggHistStruct) -> Result<acore::HistogramEpochState> {
    let per_key_data = unpack_hist_per_key_table_fixed(&s.table_fixed)?;
    let mut per_key_histograms: Vec<acore::KeyHistogram> = Vec::with_capacity(per_key_data.len());

    eprintln!("[DEBUG] Building HistogramEpochState: seq={} num_keys={} total_count={} total_sum={} prev_chain_hash_len={}",
        s.sequence, per_key_data.len(), s.total_count, s.total_sum, s.prev_chain_hash.len());

    for (key_id, bucket_counts, total_count, total_sum) in per_key_data {
        let mut bucket_counts_array = [0u32; acore::HISTOGRAM_SLOTS];
        bucket_counts_array.copy_from_slice(&bucket_counts);
        per_key_histograms.push(acore::KeyHistogram {
            key_id,
            bucket_counts: bucket_counts_array,
            count: total_count as u32,
            sum: total_sum,
        });
    }

    // Sort by key_id for determinism
    per_key_histograms.sort_by_key(|h| h.key_id);

    Ok(acore::HistogramEpochState {
        total_count: s.total_count,
        total_sum: s.total_sum,
        per_key_histograms,
    })
}

/// Build SamplesEpochState from VerifiedSamplesStruct data
fn build_samples_epoch_state(s: &VerifiedSamplesStruct) -> Result<acore::SamplesEpochState> {
    let table_fixed = s.table_fixed.as_ref()
        .ok_or_else(|| anyhow::anyhow!("VerifiedSamplesStruct missing table_fixed"))?;

    // Unpack table_fixed using aggregator's format: length[4] + (key_id[15] + tip[32] + count[4] + sum[8]) per entry
    anyhow::ensure!(table_fixed.len() >= 4, "table_fixed too short for length prefix");

    let num_entries = u32::from_be_bytes(table_fixed[0..4].try_into().unwrap()) as usize;
    let expected_len = 4 + num_entries * 59; // 4 byte header + 59 bytes per entry
    anyhow::ensure!(
        table_fixed.len() == expected_len,
        "samples table_fixed length mismatch: got {} expected {} for {} entries",
        table_fixed.len(), expected_len, num_entries
    );

    eprintln!("[DEBUG] Building SamplesEpochState: seq={} num_entries={} total_count={} total_sum={} prev_chain_hash_len={}",
        s.sequence, num_entries, s.total_count, s.total_sum, s.prev_chain_hash.len());

    let mut per_key: Vec<acore::BucketEntry> = Vec::with_capacity(num_entries);
    let mut offset = 4; // Skip length prefix

    for _ in 0..num_entries {
        // Read key_id (15 bytes)
        let mut key_id = [0u8; 15];
        key_id.copy_from_slice(&table_fixed[offset..offset + 15]);
        offset += 15;

        // Read key_chain_tip (32 bytes)
        let mut key_chain_tip = [0u8; 32];
        key_chain_tip.copy_from_slice(&table_fixed[offset..offset + 32]);
        offset += 32;

        // Read count (4 bytes, u32)
        let count = u32::from_be_bytes(table_fixed[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Read sum (8 bytes, u64)
        let sum = u64::from_be_bytes(table_fixed[offset..offset + 8].try_into().unwrap());
        offset += 8;

        per_key.push(acore::BucketEntry {
            occupied: 1, // All entries in the table are occupied
            key_id,
            key_chain_tip,
            sum,
            count,
        });
    }

    // Sort by key_id for determinism
    per_key.sort_by_key(|e| e.key_id);

    Ok(acore::SamplesEpochState {
        total_count: s.total_count,
        total_sum: s.total_sum,
        chain_hash: {
            let mut arr = [0u8; 32];
            if s.prev_chain_hash.len() >= 32 {
                arr.copy_from_slice(&s.prev_chain_hash[..32]);
            }
            // If empty or too short, remains [0u8; 32] (correct for epoch 0)
            arr
        },
        per_key,
    })
}

fn parse_key_pattern(pattern: &str) -> Result<(u64, u64)> {
    let mut s = pattern.trim();
    if let Some(rest) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        s = rest;
        anyhow::ensure!(!s.is_empty(), "pattern must not be empty");
        anyhow::ensure!(s.len() <= 64, "pattern too long (max 64 bits)");
        let mut padded = String::with_capacity(64);
        padded.push_str(s);
        while padded.len() < 64 {
            padded.push('?');
        }
        let mut key: u64 = 0;
        let mut mask: u64 = 0;
        for ch in padded.chars() {
            key <<= 1;
            mask <<= 1;
            match ch {
                '0' => {
                    mask |= 1;
                }
                '1' => {
                    key |= 1;
                    mask |= 1;
                }
                '?' | '*' | '_' => {}
                _ => anyhow::bail!("invalid pattern bit: {}", ch),
            }
        }
        return Ok((key, mask));
    }
    if let Some(rest) = s.strip_prefix("b").or_else(|| s.strip_prefix("B")) {
        s = rest;
        anyhow::ensure!(!s.is_empty(), "pattern must not be empty");
        anyhow::ensure!(s.len() <= 64, "pattern too long (max 64 bits)");
        let mut padded = String::with_capacity(64);
        padded.push_str(s);
        while padded.len() < 64 {
            padded.push('?');
        }
        let mut key: u64 = 0;
        let mut mask: u64 = 0;
        for ch in padded.chars() {
            key <<= 1;
            mask <<= 1;
            match ch {
                '0' => {
                    mask |= 1;
                }
                '1' => {
                    key |= 1;
                    mask |= 1;
                }
                '?' | '*' | '_' => {}
                _ => anyhow::bail!("invalid pattern bit: {}", ch),
            }
        }
        return Ok((key, mask));
    }
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        s = rest;
    }
    anyhow::ensure!(!s.is_empty(), "pattern must not be empty");
    anyhow::ensure!(s.len() <= 16, "pattern too long (max 16 hex nibbles)");
    let mut padded = String::with_capacity(16);
    padded.push_str(s);
    while padded.len() < 16 {
        padded.push('?');
    }
    let mut key: u64 = 0;
    let mut mask: u64 = 0;
    for ch in padded.chars() {
        key <<= 4;
        mask <<= 4;
        match ch {
            '0'..='9' | 'a'..='f' | 'A'..='F' => {
                let v = ch.to_digit(16).unwrap() as u64;
                key |= v;
                mask |= 0xF;
            }
            '?' | '*' | '_' => {}
            _ => anyhow::bail!("invalid pattern nibble: {}", ch),
        }
    }
    Ok((key, mask))
}

/// Parse a hex pattern for a 15-byte key into (key, mask) byte arrays.
///
/// Pattern: up to 30 hex nibbles (optional "0x" prefix). `?`/`*`/`_` = wildcard nibble.
/// Shorter patterns are right-padded with `?` (don't-care).
///
/// Example: "????????aabbccdd??????????????" matches bytes 4-7 against 0xaabbccdd.
fn parse_key_pattern_15(pattern: &str) -> Result<([u8; 15], [u8; 15])> {
    let mut s = pattern.trim();
    if let Some(rest) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        s = rest;
    }
    anyhow::ensure!(!s.is_empty(), "key pattern must not be empty");
    anyhow::ensure!(s.len() <= 30, "key pattern too long (max 30 hex nibbles for 15 bytes)");

    let mut padded = String::with_capacity(30);
    padded.push_str(s);
    while padded.len() < 30 {
        padded.push('?');
    }

    let mut key_bytes = [0u8; 15];
    let mut mask_bytes = [0u8; 15];

    for (i, pair) in padded.as_bytes().chunks(2).enumerate() {
        let (hi_ch, lo_ch) = (pair[0] as char, pair[1] as char);

        let (hi_val, hi_mask) = match hi_ch {
            '0'..='9' | 'a'..='f' | 'A'..='F' => {
                let v = hi_ch.to_digit(16).unwrap() as u8;
                (v << 4, 0xF0u8)
            }
            '?' | '*' | '_' => (0u8, 0u8),
            _ => anyhow::bail!("invalid pattern nibble: {}", hi_ch),
        };
        let (lo_val, lo_mask) = match lo_ch {
            '0'..='9' | 'a'..='f' | 'A'..='F' => {
                let v = lo_ch.to_digit(16).unwrap() as u8;
                (v, 0x0Fu8)
            }
            '?' | '*' | '_' => (0u8, 0u8),
            _ => anyhow::bail!("invalid pattern nibble: {}", lo_ch),
        };

        key_bytes[i] = hi_val | lo_val;
        mask_bytes[i] = hi_mask | lo_mask;
    }

    Ok((key_bytes, mask_bytes))
}

fn raw_hist_bucket(value: u64) -> u16 {
    // Same definition as the circuits: floor(log2(max(value,1))).
    let v = if value == 0 { 1 } else { value };
    (63u16).saturating_sub(v.leading_zeros() as u16)
}

fn proof_bundle_from_receipt(
    label: &str,
    receipt: &risc0_zkvm::Receipt,
    setup_ms: u64,
    prove_ms: u64,
    verify_ms: u64,
    omit_proof_hex: bool,
) -> Result<ProofBundle> {
    use sha2::Digest as _;
    // `risc0_zkvm::serde::to_vec` returns word-aligned data (u32 words).
    // Convert to bytes for digest/hex/size reporting.
    let proof_words: Vec<u32> = to_vec(receipt).context("serialize receipt")?;
    let proof_bin: &[u8] = bytemuck::cast_slice(&proof_words);
    let proof_bytes = proof_bin.len() as u64;
    let journal_bytes = receipt.journal.bytes.len() as u64;
    let digest = sha2::Sha256::digest(proof_bin);
    let mut digest32 = [0u8; 32];
    digest32.copy_from_slice(&digest[..32]);
    bench_proof_log("risc0", 1, setup_ms, prove_ms, verify_ms, proof_bytes, journal_bytes);
    Ok(ProofBundle {
        proof_kind: 2, // RISC0 receipt
        num_steps: 1,
        digest_hex: hex::encode(digest32),
        proof_hex: if omit_proof_hex { String::new() } else { hex::encode(proof_bin) },
        prove_ms: Some(prove_ms),
        proof_bytes: Some(proof_bytes),
        journal_bytes: Some(journal_bytes),
        proof_file: Some(label.to_string()),
    })
}

fn prove_risc0<I: serde::Serialize, O: serde::de::DeserializeOwned + PartialEq>(
    label: &str,
    input: &I,
    expected: &O,
    elf: &[u8],
    id: risc0_zkvm::sha::Digest,
    omit_proof_hex: bool,
) -> Result<ProofBundle> {
    // Native (non-ZK) query baseline: skip zkVM proving entirely. The query
    // result returned to the client is computed natively in the handler; here we
    // just emit zero prove/verify so the e2e driver can record native query time
    // = FDB lookup (db_ms) + deserialize/merge (merge_ms), with no proving cost.
    // Enabled by QUERY_NO_PROVE=1 (set by scripts/run_baseline_e2e.sh native pass).
    if std::env::var("QUERY_NO_PROVE").map(|v| v != "0" && !v.is_empty()).unwrap_or(false) {
        bench_proof_log("native", 1, 0, 0, 0, 0, 0);
        return Ok(ProofBundle {
            proof_kind: 0,
            num_steps: 0,
            digest_hex: String::new(),
            proof_hex: String::new(),
            prove_ms: Some(0),
            proof_bytes: Some(0),
            journal_bytes: Some(0),
            proof_file: None,
        });
    }

    let setup_start = std::time::Instant::now();
    let env = ExecutorEnv::builder()
        .write(input)
        .context("zkvm write input")?
        .build()
        .context("build executor env")?;
    let setup_ms = setup_start.elapsed().as_millis() as u64;

    let prover = default_prover();
    let opts = ProverOpts::succinct();
    let prove_start = std::time::Instant::now();
    let prove_info = prover.prove_with_opts(env, elf, &opts)
        .with_context(|| format!("failed to generate proof for {}", label))?;
    let prove_ms = prove_start.elapsed().as_millis() as u64;

    let verify_start = std::time::Instant::now();
    prove_info.receipt.verify(id).context("verify receipt")?;
    let verify_ms = verify_start.elapsed().as_millis() as u64;

    let out: O = prove_info.receipt.journal.decode().context("decode journal")?;
    anyhow::ensure!(out == *expected, "guest output mismatch for {label}");

    proof_bundle_from_receipt(label, &prove_info.receipt, setup_ms, prove_ms, verify_ms, omit_proof_hex)
}

/// Check if epoch chain verification is enabled via environment variable
/// Default: ENABLED (set VERIFY_EPOCH_CHAIN=0 to disable)
fn epoch_chain_verification_enabled() -> bool {
    std::env::var("VERIFY_EPOCH_CHAIN")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1)  // Default to enabled
        != 0
}

/// Build epoch chain links from VerifiedSamplesStruct and computed state commits
fn build_epoch_chain_links_samples(structs: &[VerifiedSamplesStruct], state_commits: &[[u8; 32]]) -> Vec<qcore::EpochChainLink> {
    structs
        .iter()
        .zip(state_commits.iter())
        .enumerate()
        .map(|(i, (s, state_commit))| {
            let prev_chain_hash = if s.prev_chain_hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&s.prev_chain_hash);
                arr
            } else {
                eprintln!("[DEBUG] Epoch {}: prev_chain_hash missing or too short (len={}), using zeros", i, s.prev_chain_hash.len());
                [0u8; 32]
            };
            let final_chain_hash = if s.result_commit.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&s.result_commit);
                arr
            } else {
                [0u8; 32]
            };

            // Diagnostic: Compute what final_chain_hash SHOULD be based on recomputed state_commit
            const TAG_EPOCH_CHAIN: &[u8] = b"ZKTLM_EPOCH_CHAIN_V1";
            let expected_final_hash = {
                use sha2::{Digest as _, Sha256};
                let mut sha = Sha256::new();
                sha.update(TAG_EPOCH_CHAIN);
                sha.update(&prev_chain_hash);
                sha.update(state_commit);
                let result: [u8; 32] = sha.finalize().into();
                result
            };

            if expected_final_hash != final_chain_hash {
                eprintln!("[DEBUG] Epoch {}: Hash mismatch detected!", i);
                eprintln!("  prev_chain_hash: {}", hex::encode(prev_chain_hash));
                eprintln!("  recomputed state_commit: {}", hex::encode(state_commit));
                eprintln!("  expected final_hash: {}", hex::encode(expected_final_hash));
                eprintln!("  stored final_hash: {}", hex::encode(final_chain_hash));
                eprintln!("  DB total_count: {}, total_sum: {}", s.total_count, s.total_sum);
                eprintln!("  DB prev_chain_hash len: {}", s.prev_chain_hash.len());
            }

            qcore::EpochChainLink {
                prev_chain_hash,
                state_commit: *state_commit,
                final_chain_hash,
            }
        })
        .collect()
}

/// Build epoch chain links from AggHistStruct and computed state commits
fn build_epoch_chain_links_hist(structs: &[AggHistStruct], state_commits: &[[u8; 32]]) -> Vec<qcore::EpochChainLink> {
    structs
        .iter()
        .zip(state_commits.iter())
        .enumerate()
        .map(|(i, (s, state_commit))| {
            let prev_chain_hash = if s.prev_chain_hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&s.prev_chain_hash);
                arr
            } else {
                eprintln!("[DEBUG] Histogram Epoch {}: prev_chain_hash missing or too short (len={}), using zeros", i, s.prev_chain_hash.len());
                [0u8; 32]
            };
            let final_chain_hash = if s.final_chain_hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&s.final_chain_hash);
                arr
            } else {
                [0u8; 32]
            };

            // Diagnostic: Compute what final_chain_hash SHOULD be
            const TAG_EPOCH_CHAIN: &[u8] = b"ZKTLM_EPOCH_CHAIN_V1";
            let expected_final_hash = {
                use sha2::{Digest as _, Sha256};
                let mut sha = Sha256::new();
                sha.update(TAG_EPOCH_CHAIN);
                sha.update(&prev_chain_hash);
                sha.update(state_commit);
                let result: [u8; 32] = sha.finalize().into();
                result
            };

            if expected_final_hash != final_chain_hash {
                eprintln!("[DEBUG] Histogram Epoch {}: Hash mismatch detected!", i);
                eprintln!("  prev_chain_hash: {}", hex::encode(prev_chain_hash));
                eprintln!("  recomputed state_commit: {}", hex::encode(state_commit));
                eprintln!("  expected final_hash: {}", hex::encode(expected_final_hash));
                eprintln!("  stored final_hash: {}", hex::encode(final_chain_hash));
                eprintln!("  DB total_count: {}, total_sum: {}", s.total_count, s.total_sum);
            }

            qcore::EpochChainLink {
                prev_chain_hash,
                state_commit: *state_commit,
                final_chain_hash,
            }
        })
        .collect()
}

/// Build epoch chain links from AggCmStruct and computed state commits
fn build_epoch_chain_links_cm(structs: &[AggCmStruct], state_commits: &[[u8; 32]]) -> Vec<qcore::EpochChainLink> {
    structs
        .iter()
        .zip(state_commits.iter())
        .enumerate()
        .map(|(i, (s, state_commit))| {
            let prev_chain_hash = if s.prev_chain_hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&s.prev_chain_hash);
                arr
            } else {
                eprintln!("[DEBUG] CM Epoch {}: prev_chain_hash missing or too short (len={}), using zeros", i, s.prev_chain_hash.len());
                [0u8; 32]
            };
            let final_chain_hash = if s.final_chain_hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&s.final_chain_hash);
                arr
            } else {
                [0u8; 32]
            };

            // Diagnostic: Compute what final_chain_hash SHOULD be
            const TAG_EPOCH_CHAIN: &[u8] = b"ZKTLM_EPOCH_CHAIN_V1";
            let expected_final_hash = {
                use sha2::{Digest as _, Sha256};
                let mut sha = Sha256::new();
                sha.update(TAG_EPOCH_CHAIN);
                sha.update(&prev_chain_hash);
                sha.update(state_commit);
                let result: [u8; 32] = sha.finalize().into();
                result
            };

            if expected_final_hash != final_chain_hash {
                eprintln!("[DEBUG] CM Epoch {}: Hash mismatch detected!", i);
                eprintln!("  prev_chain_hash: {}", hex::encode(prev_chain_hash));
                eprintln!("  recomputed state_commit: {}", hex::encode(state_commit));
                eprintln!("  expected final_hash: {}", hex::encode(expected_final_hash));
                eprintln!("  stored final_hash: {}", hex::encode(final_chain_hash));
                eprintln!("  DB total_sum: {}", s.total_sum);
                eprintln!("  ---");
                eprintln!("  The aggregator must have computed a DIFFERENT state_commit than {}", hex::encode(state_commit));
                eprintln!("  This means the CmEpochState data doesn't match what aggregator used.");
                eprintln!("  Check: counts array, heap data, total_sum, or data packing format.");
            }

            qcore::EpochChainLink {
                prev_chain_hash,
                state_commit: *state_commit,
                final_chain_hash,
            }
        })
        .collect()
}
