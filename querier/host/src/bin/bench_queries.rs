//! Comprehensive benchmark for epoch-based query types with multi-source support.
//!
//! This binary generates synthetic epoch states and benchmarks all query types
//! through the zkVM prover, reporting performance metrics.
//!
//! Usage:
//!   cargo run --release --bin bench_queries -- [OPTIONS]
//!
//! Options:
//!   --epochs N                 Number of epochs to generate (default: 3)
//!   --num-sources N            Number of data sources (default: 4)
//!   --keys-per-source N        Keys per source (default: 25)
//!   --sources-per-epoch N      Sources appearing in each epoch (default: 2)
//!   --events-per-key N         Number of events/samples per key per epoch (default: 50)
//!   --samples-per-key N        Alias for --events-per-key
//!   --seed N                   Random seed (default: 0xBEEF)
//!   --skip-histogram           Skip all histogram queries
//!   --skip-histogram-bucket    Skip histogram/bucket query
//!   --skip-histogram-all       Skip histogram/all query
//!   --skip-histogram-p90       Skip histogram/p90 query
//!   --skip-cm                  Skip CM sketch queries
//!   --skip-samples             Skip all samples queries
//!   --skip-samples-sum         Skip samples/sum query
//!   --skip-samples-sum-key     Skip samples/sum_key query
//!   --skip-samples-sum-topk    Skip samples/sum_topk query
//!   --skip-raw                 Skip raw event queries
//!   --keys N                   (Legacy) Total keys using single source
//!   --num-aggregators N        Number of aggregators, each with separate chain (default: 1)
//!   --dp-enabled               Enable differential privacy offsets (default)
//!   --dp-disabled              Disable differential privacy offsets

use anyhow::{Context, Result};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use risc0_zkvm::{default_prover, ExecutorEnv, ProverOpts};
use sha2::{Digest as _, Sha256};
use std::time::Instant;
use common::dp;
use aggregator_core::{
    histogram_bucket_index, histogram_epoch_state_commit, samples_epoch_state_commit,
    cm_epoch_state_commit, BucketEntry, CmEpochState, EpochChainLink, HistogramEpochState,
    KeyHistogram, SamplesEpochState, CM_COLS, CM_ROWS, CM_SEEDS, CM_TOPK_SLOTS,
};
use zkvm_common::KEY_BYTES_LEN;
use querier_core::{
    CmQuery, CmQueryInput, CmQueryOutput, HistogramQuery, HistogramQueryInput,
    HistogramQueryOutput, RawEvent, RawQuery, RawQueryInput, RawQueryOutput,
    SamplesQuery, SamplesQueryInput, SamplesQueryOutput,
};
use querier_methods::{
    QUERIER_GUEST_CM_ELF as QUERIER_CM_ELF,
    QUERIER_GUEST_CM_ID as QUERIER_CM_ID,
    QUERIER_GUEST_HISTOGRAM_ELF as QUERIER_HISTOGRAM_ELF,
    QUERIER_GUEST_HISTOGRAM_ID as QUERIER_HISTOGRAM_ID,
    QUERIER_GUEST_RAW_ELF as QUERIER_RAW_ELF,
    QUERIER_GUEST_RAW_ID as QUERIER_RAW_ID,
    QUERIER_GUEST_SAMPLES_ELF as QUERIER_SAMPLES_ELF,
    QUERIER_GUEST_SAMPLES_ID as QUERIER_SAMPLES_ID,
};

const TAG_EPOCH_CHAIN: &[u8] = b"ZKTLM_EPOCH_CHAIN_V1";

/// Parse command line argument as u64
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

/// Check if a flag is present
fn has_flag(name: &str) -> bool {
    std::env::args().any(|arg| arg == name)
}

/// Parse command line argument as u32
fn parse_arg_u32(name: &str, default: u32) -> u32 {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            if let Some(v) = it.next() {
                return v.parse::<u32>().unwrap_or(default);
            }
        }
    }
    default
}

/// Create a 15-byte key_id with source_id embedded
fn make_key_id(source_id: u32, key_index: u64) -> [u8; KEY_BYTES_LEN] {
    let mut key_id = [0u8; KEY_BYTES_LEN];
    key_id[3..7].copy_from_slice(&source_id.to_be_bytes());
    key_id[15 - 8..].copy_from_slice(&key_index.to_be_bytes());
    key_id
}

/// Select which sources appear in a given epoch.
/// Uses deterministic randomness based on epoch_idx and seed.
/// Selection is WITHOUT replacement (each source appears at most once).
///
/// # Arguments
/// * `epoch_idx` - Zero-based epoch index for deterministic variation
/// * `num_sources` - Total number of available sources (0..num_sources)
/// * `sources_per_epoch` - How many sources to select (must be <= num_sources)
/// * `rng` - Random number generator (will be reseeded based on epoch_idx)
///
/// # Returns
/// Sorted vector of source IDs selected for this epoch
fn select_epoch_sources(
    epoch_idx: u64,
    num_sources: u32,
    sources_per_epoch: u32,
    rng: &mut rand::rngs::StdRng,
) -> Vec<u32> {
    use rand::seq::SliceRandom;

    // Create deterministic sub-seed from epoch_idx
    // Use the RNG to generate a sub-seed, then reseed for this epoch
    let epoch_seed = rng.next_u64().wrapping_add(epoch_idx);
    let mut epoch_rng = rand::rngs::StdRng::seed_from_u64(epoch_seed);

    // Create pool of all source IDs
    let mut all_sources: Vec<u32> = (0..num_sources).collect();

    // Shuffle and take first sources_per_epoch elements
    all_sources.shuffle(&mut epoch_rng);
    let mut selected: Vec<u32> = all_sources
        .into_iter()
        .take(sources_per_epoch as usize)
        .collect();

    // Sort for deterministic ordering
    selected.sort_unstable();
    selected
}

/// SHA256 helper for multiple parts
fn sha256_bytes(parts: &[&[u8]]) -> [u8; 32] {
    let mut sha = Sha256::new();
    for p in parts {
        sha.update(p);
    }
    sha.finalize().into()
}

/// CM sketch bucket index computation
fn cm_bucket_index(key: &[u8; KEY_BYTES_LEN], row: usize) -> usize {
    fn key_to_u64(key: &[u8; KEY_BYTES_LEN]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in key.iter() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    let seed = CM_SEEDS[row];
    let mut x = key_to_u64(key) ^ (((seed as u64) << 32) | seed as u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x as usize) % CM_COLS
}

// ============================================================================
// Synthetic Epoch State Generation
// ============================================================================

/// Generate a synthetic histogram epoch state with multiple sources.
/// Creates keys_per_source keys for each source in source_ids.
/// Results are sorted by key_id for deterministic ordering.
fn generate_histogram_epoch_multi_source(
    source_ids: &[u32],
    keys_per_source: u64,
    events_per_key: u32,
    value_mod: u32,
    rng: &mut rand::rngs::StdRng,
) -> HistogramEpochState {
    let mut per_key = Vec::new();
    let mut total_count = 0u64;
    let mut total_sum = 0u64;

    // Generate keys for each source
    for &source_id in source_ids {
        for key_idx in 0..keys_per_source {
            let key_id = make_key_id(source_id, key_idx);
            let mut hist = KeyHistogram::new(key_id);

            for _ in 0..events_per_key {
                let value = (rng.next_u32() % value_mod) as u64;
                let bucket = histogram_bucket_index(value);
                hist.count = hist.count.saturating_add(1);
                hist.sum = hist.sum.saturating_add(value);
                hist.bucket_counts[bucket] = hist.bucket_counts[bucket].saturating_add(1);
                total_count += 1;
                total_sum += value;
            }
            per_key.push(hist);
        }
    }

    // Sort by key_id for deterministic ordering
    per_key.sort_by(|a, b| a.key_id.cmp(&b.key_id));

    HistogramEpochState {
        total_count,
        total_sum,
        per_key_histograms: per_key,
    }
}

/// Generate a synthetic samples epoch state with multiple sources.
/// Creates keys_per_source keys for each source in source_ids.
/// Results are sorted by key_id for deterministic ordering.
fn generate_samples_epoch_multi_source(
    source_ids: &[u32],
    keys_per_source: u64,
    events_per_key: u32,
    value_mod: u32,
    rng: &mut rand::rngs::StdRng,
) -> SamplesEpochState {
    let mut per_key = Vec::new();
    let mut total_count = 0u64;
    let mut total_sum = 0u64;

    // Generate keys for each source
    for &source_id in source_ids {
        for key_idx in 0..keys_per_source {
            let key_id = make_key_id(source_id, key_idx);
            let mut sum = 0u64;
            let mut count = 0u32;

            for _ in 0..events_per_key {
                let value = (rng.next_u32() % value_mod) as u64;
                sum = sum.saturating_add(value);
                count = count.saturating_add(1);
                total_count += 1;
                total_sum += value;
            }

            per_key.push(BucketEntry {
                occupied: 1,
                key_id,
                key_chain_tip: [0u8; 32], // Simplified for benchmark
                sum,
                count,
            });
        }
    }

    // Sort by key_id for deterministic ordering
    per_key.sort_by(|a, b| a.key_id.cmp(&b.key_id));

    SamplesEpochState {
        total_count,
        total_sum,
        chain_hash: [0u8; 32], // Simplified for benchmark
        per_key,
    }
}

/// Generate a synthetic CM sketch epoch state with multiple sources.
/// Creates keys_per_source keys for each source in source_ids.
/// Updates CM sketch and tracks top-k across all keys.
fn generate_cm_epoch_multi_source(
    source_ids: &[u32],
    keys_per_source: u64,
    events_per_key: u32,
    value_mod: u32,
    rng: &mut rand::rngs::StdRng,
) -> CmEpochState {
    let mut counts = vec![0u32; CM_ROWS * CM_COLS];
    let mut total_sum = 0u64;

    // Track top-k candidates across all sources
    let mut key_counts: std::collections::HashMap<[u8; KEY_BYTES_LEN], u64> =
        std::collections::HashMap::new();

    // Generate keys for each source
    for &source_id in source_ids {
        for key_idx in 0..keys_per_source {
            let key_id = make_key_id(source_id, key_idx);
            let mut key_total = 0u64;

            for _ in 0..events_per_key {
                let value = (rng.next_u32() % value_mod) as u32;

                // Update CM sketch counts
                for r in 0..CM_ROWS {
                    let col = cm_bucket_index(&key_id, r);
                    let idx = r * CM_COLS + col;
                    counts[idx] = counts[idx].saturating_add(value);
                }

                key_total += value as u64;
                total_sum += value as u64;
            }

            *key_counts.entry(key_id).or_insert(0) += key_total;
        }
    }

    // Build top-k heap from key_counts
    let mut sorted_keys: Vec<_> = key_counts.into_iter().collect();
    sorted_keys.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending

    let mut heap_keys = vec![[0u8; KEY_BYTES_LEN]; CM_TOPK_SLOTS];
    let mut heap_vals = vec![0u64; CM_TOPK_SLOTS];
    let mut heap_occ = vec![0u8; CM_TOPK_SLOTS];

    for (i, (key, count)) in sorted_keys.into_iter().take(CM_TOPK_SLOTS).enumerate() {
        heap_keys[i] = key;
        heap_vals[i] = count;
        heap_occ[i] = 1;
    }

    CmEpochState {
        counts,
        heap_keys,
        heap_vals,
        heap_occ,
        total_sum,
    }
}

// ============================================================================
// Chain Link Generation
// ============================================================================

/// Build epoch chain links with correct state commits and linkage
fn build_chain_links<S, F>(states: &[S], state_commit_fn: F) -> Vec<EpochChainLink>
where
    F: Fn(&S) -> [u8; 32],
{
    let mut links = Vec::new();
    let mut prev = [0u8; 32]; // genesis

    for state in states {
        let sc = state_commit_fn(state);
        let final_hash = sha256_bytes(&[TAG_EPOCH_CHAIN, &prev, &sc]);
        links.push(EpochChainLink {
            prev_chain_hash: prev,
            state_commit: sc,
            final_chain_hash: final_hash,
        });
        prev = final_hash;
    }
    links
}

// ============================================================================
// Benchmark Result Types
// ============================================================================

struct BenchResult {
    query_type: String,
    epochs: u64,
    keys: u64,
    proof_time_ms: u128,
    verify_time_ms: u128,
    proof_size_bytes: usize,
    journal_size_bytes: usize,
    dp_offset: u64,
}

impl BenchResult {
    fn format_time(ms: u128) -> String {
        if ms >= 1000 {
            format!("{:.1}s", ms as f64 / 1000.0)
        } else {
            format!("{}ms", ms)
        }
    }

    fn format_size(bytes: usize) -> String {
        if bytes >= 1024 * 1024 {
            format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes >= 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else {
            format!("{} B", bytes)
        }
    }
}

// ============================================================================
// Benchmark Runners
// ============================================================================

fn run_histogram_benchmark(
    query_name: &str,
    query: HistogramQuery,
    epoch_states: Vec<HistogramEpochState>,
    epoch_chain_links: Vec<EpochChainLink>,
    n_epochs: u64,
    n_keys: u64,
    dp_enabled: bool,
) -> Result<BenchResult> {
    // Determine DP offset based on query type (using randomized Laplace noise)
    // Must be done before query is moved into input
    let dp_offset = if dp_enabled {
        let mut rng = StdRng::from_entropy();
        let cfg = match &query {
            HistogramQuery::Bucket { .. } => dp::DP_HIST_BUCKET,
            HistogramQuery::All => dp::DP_HIST_TOTAL_COUNT, // Use total_count as representative
            HistogramQuery::P90 => dp::DP_HIST_BUCKET_COUNT,
            HistogramQuery::AllKey { .. } => dp::DP_HIST_BUCKET_COUNT, // Uses bucket counts like P90
        };
        cfg.laplace_noise(&mut rng).round().abs() as u64
    } else {
        0
    };

    let input = HistogramQueryInput {
        query,
        epoch_states,
        epoch_chain_links,
    };

    let env = ExecutorEnv::builder()
        .write(&input)
        .context("zkvm write input")?
        .build()
        .context("build executor env")?;

    let prover = default_prover();
    let opts = ProverOpts::succinct();

    let start = Instant::now();
    let prove_info = prover.prove_with_opts(env, QUERIER_HISTOGRAM_ELF, &opts)?;
    let proof_time_ms = start.elapsed().as_millis();

    let start = Instant::now();
    prove_info.receipt.verify(QUERIER_HISTOGRAM_ID)?;
    let verify_time_ms = start.elapsed().as_millis();

    let proof_size_bytes = bincode::serialize(&prove_info.receipt)?.len();
    let journal_size_bytes = bincode::serialize(&prove_info.receipt.journal)?.len();

    // Decode and validate output
    let _output: HistogramQueryOutput = prove_info
        .receipt
        .journal
        .decode()
        .context("decode journal")?;

    Ok(BenchResult {
        query_type: format!("histogram/{}", query_name),
        epochs: n_epochs,
        keys: n_keys,
        proof_time_ms,
        verify_time_ms,
        proof_size_bytes,
        journal_size_bytes,
        dp_offset,
    })
}

fn run_cm_benchmark(
    query_name: &str,
    query: CmQuery,
    epoch_states: Vec<CmEpochState>,
    epoch_chain_links: Vec<EpochChainLink>,
    n_epochs: u64,
    n_keys: u64,
    dp_enabled: bool,
) -> Result<BenchResult> {
    // Determine DP offset based on query type (using randomized Laplace noise)
    // Must be done before query is moved into input
    let dp_offset = if dp_enabled {
        let mut rng = StdRng::from_entropy();
        let cfg = match &query {
            CmQuery::Estimate { .. } => dp::DP_CM_ESTIMATE,
            CmQuery::Topk { .. } => dp::DP_CM_TOPK,
        };
        cfg.laplace_noise(&mut rng).round().abs() as u64
    } else {
        0
    };

    let input = CmQueryInput {
        query,
        epoch_states,
        epoch_chain_links,
    };

    let env = ExecutorEnv::builder()
        .write(&input)
        .context("zkvm write input")?
        .build()
        .context("build executor env")?;

    let prover = default_prover();
    let opts = ProverOpts::succinct();

    let start = Instant::now();
    let prove_info = prover.prove_with_opts(env, QUERIER_CM_ELF, &opts)?;
    let proof_time_ms = start.elapsed().as_millis();

    let start = Instant::now();
    prove_info.receipt.verify(QUERIER_CM_ID)?;
    let verify_time_ms = start.elapsed().as_millis();

    let proof_size_bytes = bincode::serialize(&prove_info.receipt)?.len();
    let journal_size_bytes = bincode::serialize(&prove_info.receipt.journal)?.len();

    // Decode and validate output
    let _output: CmQueryOutput = prove_info
        .receipt
        .journal
        .decode()
        .context("decode journal")?;

    Ok(BenchResult {
        query_type: format!("cm/{}", query_name),
        epochs: n_epochs,
        keys: n_keys,
        proof_time_ms,
        verify_time_ms,
        proof_size_bytes,
        journal_size_bytes,
        dp_offset,
    })
}

fn run_samples_benchmark(
    query_name: &str,
    query: SamplesQuery,
    epoch_states: Vec<SamplesEpochState>,
    epoch_chain_links: Vec<EpochChainLink>,
    n_epochs: u64,
    n_keys: u64,
    dp_enabled: bool,
) -> Result<BenchResult> {
    // Determine DP offset based on query type (using randomized Laplace noise)
    // Must be done before query is moved into input
    let dp_offset = if dp_enabled {
        let mut rng = StdRng::from_entropy();
        let cfg = match &query {
            SamplesQuery::Sum => dp::DP_SAMPLES_SUM,
            SamplesQuery::Avg => dp::DP_SAMPLES_AVG,
            SamplesQuery::SumKey { .. } => dp::DP_SAMPLES_SUM_KEY,
            SamplesQuery::AvgKey { .. } => dp::DP_SAMPLES_AVG,
            SamplesQuery::SumExactKey { .. } => dp::DP_SAMPLES_SUM_KEY,
            SamplesQuery::SumKeyIds { .. } => dp::DP_SAMPLES_SUM_KEY,
            SamplesQuery::SumTopk { .. } => dp::DP_CM_TOPK, // Uses same config as CM topk
            SamplesQuery::MaxKey { .. } => dp::DP_SAMPLES_SUM_KEY, // Uses same config as SumKey
        };
        cfg.laplace_noise(&mut rng).round().abs() as u64
    } else {
        0
    };

    let input = SamplesQueryInput {
        query,
        epoch_states,
        epoch_chain_links,
    };

    let env = ExecutorEnv::builder()
        .write(&input)
        .context("zkvm write input")?
        .build()
        .context("build executor env")?;

    let prover = default_prover();
    let opts = ProverOpts::succinct();

    let start = Instant::now();
    let prove_info = prover.prove_with_opts(env, QUERIER_SAMPLES_ELF, &opts)?;
    let proof_time_ms = start.elapsed().as_millis();

    let start = Instant::now();
    prove_info.receipt.verify(QUERIER_SAMPLES_ID)?;
    let verify_time_ms = start.elapsed().as_millis();

    let proof_size_bytes = bincode::serialize(&prove_info.receipt)?.len();
    let journal_size_bytes = bincode::serialize(&prove_info.receipt.journal)?.len();

    // Decode and validate output
    let _output: SamplesQueryOutput = prove_info
        .receipt
        .journal
        .decode()
        .context("decode journal")?;

    Ok(BenchResult {
        query_type: format!("samples/{}", query_name),
        epochs: n_epochs,
        keys: n_keys,
        proof_time_ms,
        verify_time_ms,
        proof_size_bytes,
        journal_size_bytes,
        dp_offset,
    })
}

fn run_raw_benchmark(
    query_name: &str,
    query: RawQuery,
    events: Vec<RawEvent>,
    n_events: u64,
    dp_enabled: bool,
) -> Result<BenchResult> {
    let dp_offset = if dp_enabled {
        let mut rng = StdRng::from_entropy();
        let cfg = match &query {
            RawQuery::MaxKey { .. } => dp::DP_RAW_MAX,
            RawQuery::StatsKey { .. } => dp::DP_RAW_STATS_SUM,
            RawQuery::HistBucketKey { .. } => dp::DP_RAW_HIST_BUCKET,
            RawQuery::CmEstimateKey { .. } => dp::DP_RAW_CM_ESTIMATE,
        };
        cfg.laplace_noise(&mut rng).round().abs() as u64
    } else {
        0
    };

    let input = RawQueryInput {
        query,
        events,
    };

    let env = ExecutorEnv::builder()
        .write(&input)
        .context("zkvm write input")?
        .build()
        .context("build executor env")?;

    let prover = default_prover();
    let opts = ProverOpts::succinct();

    let start = Instant::now();
    let prove_info = prover.prove_with_opts(env, QUERIER_RAW_ELF, &opts)?;
    let proof_time_ms = start.elapsed().as_millis();

    let start = Instant::now();
    prove_info.receipt.verify(QUERIER_RAW_ID)?;
    let verify_time_ms = start.elapsed().as_millis();

    let proof_size_bytes = bincode::serialize(&prove_info.receipt)?.len();
    let journal_size_bytes = bincode::serialize(&prove_info.receipt.journal)?.len();

    let _output: RawQueryOutput = prove_info
        .receipt
        .journal
        .decode()
        .context("decode journal")?;

    Ok(BenchResult {
        query_type: format!("raw/{}", query_name),
        epochs: 0,
        keys: n_events,
        proof_time_ms,
        verify_time_ms,
        proof_size_bytes,
        journal_size_bytes,
        dp_offset,
    })
}

// ============================================================================
// Main
// ============================================================================

fn main() -> Result<()> {
    // Parse arguments
    let n_epochs = parse_arg_u64("--epochs", 3);
    // --samples-per-key is an alias for --events-per-key (samples per key per epoch)
    let events_per_key = if std::env::args().any(|a| a == "--samples-per-key") {
        parse_arg_u64("--samples-per-key", 50) as u32
    } else {
        parse_arg_u64("--events-per-key", 50) as u32
    };
    let seed = parse_arg_u64("--seed", 0xBEEF);
    let value_mod = 10_000u32;

    // Parse new multi-source parameters
    let num_sources = parse_arg_u32("--num-sources", 4);
    let keys_per_source = parse_arg_u64("--keys-per-source", 25);
    let sources_per_epoch = parse_arg_u32("--sources-per-epoch", 2);
    let num_aggregators = parse_arg_u32("--num-aggregators", 1);

    // Backward compatibility: if --keys is provided without --num-sources
    let n_keys = parse_arg_u64("--keys", 0); // 0 = not provided
    let (num_sources, keys_per_source) = if n_keys > 0
        && !std::env::args().any(|a| a == "--num-sources")
    {
        // Legacy mode: treat as single source with n_keys
        (1u32, n_keys)
    } else {
        (num_sources, keys_per_source)
    };

    // Validate num_aggregators
    let num_aggregators = if num_aggregators == 0 {
        eprintln!("Warning: --num-aggregators must be >= 1, setting to 1");
        1
    } else if num_aggregators > num_sources {
        eprintln!(
            "Warning: --num-aggregators ({}) exceeds --num-sources ({}), clamping to {}",
            num_aggregators, num_sources, num_sources
        );
        num_sources
    } else if num_sources % num_aggregators != 0 {
        eprintln!(
            "Warning: --num-sources ({}) not evenly divisible by --num-aggregators ({})",
            num_sources, num_aggregators
        );
        num_aggregators
    } else {
        num_aggregators
    };

    // Calculate sources per aggregator
    let sources_per_aggregator = num_sources / num_aggregators;

    // Validate and clamp sources_per_epoch (must not exceed sources per aggregator)
    let sources_per_epoch = if sources_per_epoch > sources_per_aggregator {
        eprintln!(
            "Warning: --sources-per-epoch ({}) exceeds sources per aggregator ({}), clamping to {}",
            sources_per_epoch, sources_per_aggregator, sources_per_aggregator
        );
        sources_per_aggregator
    } else {
        sources_per_epoch
    };

    // Calculate epochs per aggregator (distribute evenly)
    let epochs_per_aggregator = if num_aggregators > 1 {
        (n_epochs + num_aggregators as u64 - 1) / num_aggregators as u64
    } else {
        n_epochs
    };

    let total_unique_keys = (num_sources as u64) * keys_per_source;

    let skip_histogram = has_flag("--skip-histogram");
    let skip_cm = has_flag("--skip-cm");
    let skip_samples = has_flag("--skip-samples");
    let skip_raw = has_flag("--skip-raw");

    // Fine-grained histogram query control
    let skip_histogram_bucket = has_flag("--skip-histogram-bucket");
    let skip_histogram_all = has_flag("--skip-histogram-all");
    let skip_histogram_p90 = has_flag("--skip-histogram-p90");

    // Fine-grained samples query control
    let skip_samples_sum = has_flag("--skip-samples-sum");
    let skip_samples_sum_key = has_flag("--skip-samples-sum-key");
    let skip_samples_sum_topk = has_flag("--skip-samples-sum-topk");

    // DP (differential privacy) control - enabled by default
    let dp_enabled = !has_flag("--dp-disabled");

    println!("=== Querier Benchmark (Multi-Aggregator) ===");
    println!("Total epochs to query: {}", n_epochs);
    println!("Num aggregators: {}", num_aggregators);
    println!("Epochs per aggregator: {}", epochs_per_aggregator);
    println!("Num sources (total): {}", num_sources);
    println!("Sources per aggregator: {}", sources_per_aggregator);
    println!("Keys per source: {}", keys_per_source);
    println!("Total unique keys: {}", total_unique_keys);
    println!("Sources per epoch: {}", sources_per_epoch);
    println!("Samples per key (per epoch): {}", events_per_key);
    println!("Seed: 0x{:X}", seed);
    println!("DP enabled: {}", dp_enabled);
    println!();

    let mut results: Vec<BenchResult> = Vec::new();

    // ========================================================================
    // Pre-generate source assignments for all epochs across all aggregators
    // ========================================================================
    println!("Generating epoch source assignments across {} aggregators...", num_aggregators);

    // Each aggregator gets a fixed partition of sources
    // Aggregator i owns sources: [i * sources_per_aggregator, (i+1) * sources_per_aggregator)
    let mut master_rng = rand::rngs::StdRng::seed_from_u64(seed);

    // Structure: aggregator_epochs[agg_idx] = vec of (global_epoch_idx, source_ids)
    let mut aggregator_epochs: Vec<Vec<(u64, Vec<u32>)>> = Vec::new();

    for agg_idx in 0..num_aggregators {
        let agg_source_start = agg_idx * sources_per_aggregator;
        let agg_source_end = agg_source_start + sources_per_aggregator;
        let agg_sources: Vec<u32> = (agg_source_start..agg_source_end).collect();

        println!("  Aggregator {}: sources {:?}", agg_idx, agg_sources);

        // Generate epochs for this aggregator
        let mut agg_epoch_assignments: Vec<(u64, Vec<u32>)> = Vec::new();
        for epoch_idx in 0..epochs_per_aggregator {
            // Select sources_per_epoch from this aggregator's source pool
            let selected = select_epoch_sources(
                epoch_idx,
                sources_per_aggregator,  // Select from this aggregator's pool
                sources_per_epoch,
                &mut master_rng,
            );
            // Map local source indices to global source IDs
            let global_sources: Vec<u32> = selected
                .iter()
                .map(|&local_idx| agg_source_start + local_idx)
                .collect();
            let global_epoch_idx = agg_idx as u64 * epochs_per_aggregator + epoch_idx;
            agg_epoch_assignments.push((global_epoch_idx, global_sources));
        }
        aggregator_epochs.push(agg_epoch_assignments);
    }

    // Interleave epochs from aggregators to maximize key coverage when querying N epochs
    // Pattern: take 1 epoch from each aggregator in round-robin fashion
    let mut epoch_source_assignments: Vec<Vec<u32>> = Vec::new();
    let mut epoch_aggregator_ids: Vec<u32> = Vec::new();  // Track which aggregator each epoch came from

    let max_epochs_per_agg = epochs_per_aggregator as usize;
    for round in 0..max_epochs_per_agg {
        for agg_idx in 0..num_aggregators as usize {
            if round < aggregator_epochs[agg_idx].len() {
                let (_global_idx, sources) = &aggregator_epochs[agg_idx][round];
                epoch_source_assignments.push(sources.clone());
                epoch_aggregator_ids.push(agg_idx as u32);
            }
        }
    }

    // Trim to requested number of epochs
    epoch_source_assignments.truncate(n_epochs as usize);
    epoch_aggregator_ids.truncate(n_epochs as usize);

    // Log final epoch assignments
    println!("\nFinal epoch assignments (interleaved for key coverage):");
    for (epoch_idx, (sources, agg_id)) in epoch_source_assignments.iter().zip(epoch_aggregator_ids.iter()).enumerate() {
        let num_keys = sources.len() as u64 * keys_per_source;
        println!(
            "  Epoch {}: aggregator {}, sources {:?} ({} keys)",
            epoch_idx, agg_id, sources, num_keys
        );
    }

    // Calculate and report key coverage
    let mut all_sources: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for sources in &epoch_source_assignments {
        all_sources.extend(sources.iter());
    }
    let covered_keys = all_sources.len() as u64 * keys_per_source;
    println!(
        "\nKey coverage: {} / {} unique keys ({:.1}%)",
        covered_keys, total_unique_keys,
        (covered_keys as f64 / total_unique_keys as f64) * 100.0
    );
    println!();

    // ========================================================================
    // Histogram Benchmarks
    // ========================================================================
    if !skip_histogram {
        println!("Generating histogram epoch states...");
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let histogram_epochs: Vec<HistogramEpochState> = epoch_source_assignments
            .iter()
            .enumerate()
            .map(|(epoch_idx, source_ids)| {
                let state = generate_histogram_epoch_multi_source(
                    source_ids,
                    keys_per_source,
                    events_per_key,
                    value_mod,
                    &mut rng,
                );
                let num_keys = source_ids.len() as u64 * keys_per_source;
                println!(
                    "  Epoch {}: {} keys, total_count={}, total_sum={}",
                    epoch_idx, num_keys, state.total_count, state.total_sum
                );
                state
            })
            .collect();
        let histogram_links = build_chain_links(&histogram_epochs, histogram_epoch_state_commit);

        if !skip_histogram_bucket {
            println!("Running histogram/bucket benchmark...");
            match run_histogram_benchmark(
                "bucket",
                HistogramQuery::Bucket { bucket: 0 },
                histogram_epochs.clone(),
                histogram_links.clone(),
                n_epochs,
                total_unique_keys,
                dp_enabled,
            ) {
                Ok(r) => results.push(r),
                Err(e) => eprintln!("histogram/bucket failed: {}", e),
            }
        }

        if !skip_histogram_all {
            println!("Running histogram/all benchmark...");
            match run_histogram_benchmark(
                "all",
                HistogramQuery::All,
                histogram_epochs.clone(),
                histogram_links.clone(),
                n_epochs,
                total_unique_keys,
                dp_enabled,
            ) {
                Ok(r) => results.push(r),
                Err(e) => eprintln!("histogram/all failed: {}", e),
            }
        }

        if !skip_histogram_p90 {
            println!("Running histogram/p90 benchmark...");
            match run_histogram_benchmark(
                "p90",
                HistogramQuery::P90,
                histogram_epochs,
                histogram_links,
                n_epochs,
                total_unique_keys,
                dp_enabled,
            ) {
                Ok(r) => results.push(r),
                Err(e) => eprintln!("histogram/p90 failed: {}", e),
            }
        }
    }

    // ========================================================================
    // CM Sketch Benchmarks
    // ========================================================================
    if !skip_cm {
        println!("Generating CM sketch epoch states...");
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let cm_epochs: Vec<CmEpochState> = epoch_source_assignments
            .iter()
            .enumerate()
            .map(|(epoch_idx, source_ids)| {
                let state = generate_cm_epoch_multi_source(
                    source_ids,
                    keys_per_source,
                    events_per_key,
                    value_mod,
                    &mut rng,
                );
                let num_keys = source_ids.len() as u64 * keys_per_source;
                println!(
                    "  Epoch {}: {} keys, total_sum={}",
                    epoch_idx, num_keys, state.total_sum
                );
                state
            })
            .collect();
        let cm_links = build_chain_links(&cm_epochs, cm_epoch_state_commit);

        // Get first key from heap for estimate query (fallback to first source's first key)
        let first_key = if !cm_epochs.is_empty() && cm_epochs[0].heap_occ[0] != 0 {
            cm_epochs[0].heap_keys[0]
        } else {
            let first_sources = &epoch_source_assignments[0];
            make_key_id(first_sources[0], 0)
        };

        println!("Running cm/estimate benchmark...");
        match run_cm_benchmark(
            "estimate",
            CmQuery::Estimate { key: first_key },
            cm_epochs.clone(),
            cm_links.clone(),
            n_epochs,
            total_unique_keys,
            dp_enabled,
        ) {
            Ok(r) => results.push(r),
            Err(e) => eprintln!("cm/estimate failed: {}", e),
        }

        println!("Running cm/topk benchmark...");
        match run_cm_benchmark(
            "topk",
            CmQuery::Topk { limit: 10 },
            cm_epochs,
            cm_links,
            n_epochs,
            total_unique_keys,
            dp_enabled,
        ) {
            Ok(r) => results.push(r),
            Err(e) => eprintln!("cm/topk failed: {}", e),
        }
    }

    // ========================================================================
    // Samples Benchmarks
    // ========================================================================
    if !skip_samples {
        println!("Generating samples epoch states...");
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let samples_epochs: Vec<SamplesEpochState> = epoch_source_assignments
            .iter()
            .enumerate()
            .map(|(epoch_idx, source_ids)| {
                let state = generate_samples_epoch_multi_source(
                    source_ids,
                    keys_per_source,
                    events_per_key,
                    value_mod,
                    &mut rng,
                );
                let num_keys = source_ids.len() as u64 * keys_per_source;
                println!(
                    "  Epoch {}: {} keys, total_count={}, total_sum={}",
                    epoch_idx, num_keys, state.total_count, state.total_sum
                );
                state
            })
            .collect();
        let samples_links = build_chain_links(&samples_epochs, samples_epoch_state_commit);

        // Get first source for pattern matching query
        let first_sources = &epoch_source_assignments[0];
        let first_source_id = first_sources[0];

        // Create key and mask to match all keys from first source
        // Key: source_id in bytes 3-6, zeros elsewhere
        // Mask: 0xFF for bytes 0-6 (match source_id), 0x00 for bytes 7-14 (ignore key_index)
        let query_key = make_key_id(first_source_id, 0);
        let mut query_mask = [0u8; KEY_BYTES_LEN];
        query_mask[0..7].fill(0xFF); // Match source_id (bytes 3-6) and leading zeros
        // bytes 7-14 remain 0x00, ignoring key_index

        if !skip_samples_sum {
            println!("Running samples/sum benchmark...");
            match run_samples_benchmark(
                "sum",
                SamplesQuery::Sum,
                samples_epochs.clone(),
                samples_links.clone(),
                n_epochs,
                total_unique_keys,
                dp_enabled,
            ) {
                Ok(r) => results.push(r),
                Err(e) => eprintln!("samples/sum failed: {}", e),
            }
        }

        if !skip_samples_sum_key {
            println!("Running samples/sum_key benchmark (matching source {})...", first_source_id);
            match run_samples_benchmark(
                "sum_key",
                SamplesQuery::SumKey { key: query_key, mask: query_mask },
                samples_epochs.clone(),
                samples_links.clone(),
                n_epochs,
                total_unique_keys,
                dp_enabled,
            ) {
                Ok(r) => results.push(r),
                Err(e) => eprintln!("samples/sum_key failed: {}", e),
            }
        }

        if !skip_samples_sum_topk {
            println!("Running samples/sum_topk benchmark...");
            match run_samples_benchmark(
                "sum_topk",
                SamplesQuery::SumTopk { limit: 10 },
                samples_epochs,
                samples_links,
                n_epochs,
                total_unique_keys,
                dp_enabled,
            ) {
                Ok(r) => results.push(r),
                Err(e) => eprintln!("samples/sum_topk failed: {}", e),
            }
        }
    }

    // ========================================================================
    // Raw Event Benchmarks
    // ========================================================================
    if !skip_raw {
        println!("Generating raw events for raw/max_key benchmark...");
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        // Generate raw events across all epoch source assignments
        let mut raw_events: Vec<RawEvent> = Vec::new();
        for source_ids in &epoch_source_assignments {
            for &source_id in source_ids {
                for key_idx in 0..keys_per_source {
                    let key_id = make_key_id(source_id, key_idx);
                    for _ in 0..events_per_key {
                        let value = (rng.next_u32() % value_mod) as u64;
                        raw_events.push(RawEvent { key_id, value });
                    }
                }
            }
        }
        let n_events = raw_events.len() as u64;
        println!("  Generated {} raw events", n_events);

        // Use same query_key/query_mask as samples benchmarks (match all keys from first source)
        let first_sources = &epoch_source_assignments[0];
        let first_source_id = first_sources[0];
        let query_key = make_key_id(first_source_id, 0);
        let mut query_mask = [0u8; KEY_BYTES_LEN];
        query_mask[0..7].fill(0xFF);

        println!("Running raw/max_key benchmark (matching source {})...", first_source_id);
        match run_raw_benchmark(
            "max_key",
            RawQuery::MaxKey { key: query_key, mask: query_mask },
            raw_events,
            n_events,
            dp_enabled,
        ) {
            Ok(r) => results.push(r),
            Err(e) => eprintln!("raw/max_key failed: {}", e),
        }
    }

    // ========================================================================
    // Print Results Table
    // ========================================================================
    println!();
    println!("=== Benchmark Results ===");
    println!();

    // Header
    println!(
        "{:<25} {:>8} {:>8} {:>12} {:>12} {:>12} {:>12} {:>10}",
        "Query Type", "Epochs", "Keys", "Proof Time", "Verify Time", "Proof Size", "Journal Size", "DP Offset"
    );
    println!("{}", "-".repeat(104));

    // Rows
    for r in &results {
        println!(
            "{:<25} {:>8} {:>8} {:>12} {:>12} {:>12} {:>12} {:>10}",
            r.query_type,
            r.epochs,
            r.keys,
            BenchResult::format_time(r.proof_time_ms),
            BenchResult::format_time(r.verify_time_ms),
            BenchResult::format_size(r.proof_size_bytes),
            BenchResult::format_size(r.journal_size_bytes),
            r.dp_offset,
        );
    }

    // Machine-readable rows for the non-ZK baseline merge (stable, easy to grep).
    for r in &results {
        println!(
            "CSVROW,{},{},{},{},{},{}",
            r.query_type, r.epochs, r.keys,
            r.proof_time_ms, r.verify_time_ms, r.proof_size_bytes
        );
    }

    println!();

    // TODO: Add peak memory usage tracking
    // On Linux, could read /proc/self/status VmPeak before and after proving

    Ok(())
}
