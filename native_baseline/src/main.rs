//! Native (non-ZK) baseline measurement harness for zk-Analytics.
//!
//! Runs the same aggregation / query analytics logic as the RISC Zero guests,
//! but natively on the host CPU with no zkVM and no proof generation. Reports
//! wall-clock runtime (single-thread and multi-core) and peak process RSS.
//!
//! One invocation == one measurement task. A driver script
//! (`scripts/run_non_zk_baseline.sh`) loops over the configurations and merges
//! these native numbers with the existing measured zkVM numbers.
//!
//! CLI (all `--name value`):
//!   --task aggregation|query
//!   --mode / --epoch-type   samples|histogram|cm
//!   --series N --samples-per-series N   (series*sps == events per epoch)
//!   --epochs N              (aggregation: epochs to process)
//!   --num-epochs N          (query: number of queried epochs)
//!   --query KIND            (query: global_sum|per_key_sum|topk_hash|
//!                            cm_topk|cm_estimate|hist_percentile)
//!   --batch N               (commit batch size, default 8)
//!   --threads N             (rayon pool size for the multi-core run)
//!   --reps N                (timing repetitions; min is reported)
//!   --seed N                (base RNG seed)
//!
//! Output: `key=value` lines on stdout (consumed by the driver script).

use std::collections::BTreeMap;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

use aggregator_core as aggr;
use zkvm_common::{Event, KEY_BYTES_LEN};

use aggr::{
    batch_chain_hash, process_cm_aggr_with_state, process_histogram_aggr_with_state,
    process_samples_aggr_with_state, BatchInput, CmAggrInput, CmEpochState, EpochChainLink,
    HistogramAggrInput, HistogramEpochState, SamplesAggrInput, SamplesEpochState,
};
use querier_core as q;

// ---------------------------------------------------------------------------
// Zipf value sampler (copied verbatim from the aggregator host bench so the
// synthetic workload is identical to the one used for the zkVM measurements).
// ---------------------------------------------------------------------------
struct ZipfSampler {
    cdf: Vec<f64>,
}

impl ZipfSampler {
    fn new(n: usize, s: f64) -> Self {
        assert!(n >= 1, "Zipf n must be >= 1");
        assert!(s > 0.0, "Zipf s must be > 0");
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
        Self { cdf }
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

fn value_mod_for_mode(mode: &str) -> u64 {
    match mode {
        "cm" => 1_000,
        "histogram" => 10_000,
        "samples" => 1_000_000,
        _ => 10_000,
    }
}

/// One epoch's ZK input pieces, fully chained, ready to feed to the core
/// `process_*_aggr` functions. Mirrors the host bench pipeline exactly.
struct EpochInput {
    batches: Vec<BatchInput>,
    prev_chain_hash: [u8; 32],
    prev_source_chain_tips: Vec<(u32, u64, [u8; 32])>,
}

/// Generate `epochs` epochs of synthetic, per-source-chained batch inputs.
///
/// Replicates `generate_epoch_batches` + the cross-epoch chain bookkeeping from
/// `risc0/aggregator/host/src/main.rs`, using the shared `batch_chain_hash` so
/// the produced `sent_batch_hash` values pass the core's in-circuit chain check.
fn generate_epochs(
    mode: &str,
    series: u64,
    samples_per_series: u64,
    commit_batch_size: u64,
    num_sources: u32,
    epochs: u64,
    base_seed: u64,
    key_zipf_s: Option<f64>,
) -> Vec<EpochInput> {
    let value_mod = value_mod_for_mode(mode);
    let value_zipf = ZipfSampler::new(value_mod as usize, 1.2);
    let key_zipf = key_zipf_s.map(|s| ZipfSampler::new(series.max(1) as usize, s));

    // Rolling per-source generation state: source_id -> (next_batch_seq, chain_tip).
    let mut gen_state: BTreeMap<u32, (u64, [u8; 32])> = BTreeMap::new();
    // Rolling per-source ZK tips fed as `prev_source_chain_tips`: (source, last_seq, tip).
    let mut prev_tips: Vec<(u32, u64, [u8; 32])> = Vec::new();
    let mut prev_chain_hash = [0u8; 32];

    let mut out = Vec::with_capacity(epochs as usize);

    for e in 0..epochs {
        let mut rng = StdRng::seed_from_u64(base_seed.wrapping_add(e));
        let mut batches: Vec<BatchInput> = Vec::new();

        for source_id in 0..num_sources {
            let (mut next_seq, mut chain_tip) =
                gen_state.get(&source_id).copied().unwrap_or((0, [0u8; 32]));

            // Step 1: events grouped by key (BTreeMap -> sorted, deterministic).
            let mut events_by_key: BTreeMap<[u8; KEY_BYTES_LEN], Vec<Event>> = BTreeMap::new();
            if let Some(ref kz) = key_zipf {
                let total_events = series.saturating_mul(samples_per_series);
                for _ in 0..total_events {
                    let key_index = kz.sample_u64(&mut rng) % series;
                    let key_id = Event::make_key_id(source_id, key_index);
                    let value = (value_zipf.sample_u64(&mut rng) + 1) as u32;
                    let ts = (rng.next_u32_in(1_000_000)) as u32;
                    events_by_key
                        .entry(key_id)
                        .or_default()
                        .push(Event { ts, key_id, value });
                }
            } else {
                for key_index in 0..series {
                    let key_id = Event::make_key_id(source_id, key_index);
                    let mut evs = Vec::with_capacity(samples_per_series as usize);
                    for _ in 0..samples_per_series {
                        let value = (value_zipf.sample_u64(&mut rng) + 1) as u32;
                        let ts = (rng.next_u32_in(1_000_000)) as u32;
                        evs.push(Event { ts, key_id, value });
                    }
                    events_by_key.insert(key_id, evs);
                }
            }

            // Step 2-4: chunk each key's events by commit_batch_size, chaining.
            for (_key_id, key_events) in events_by_key {
                for chunk in key_events.chunks(commit_batch_size as usize) {
                    let events: Vec<Event> = chunk.to_vec();
                    let batch_hash = batch_chain_hash(chain_tip, &events);
                    chain_tip = batch_hash;
                    batches.push(BatchInput {
                        source_id,
                        source_batch_seq: next_seq,
                        events,
                        sent_batch_hash: batch_hash,
                    });
                    next_seq += 1;
                }
            }

            gen_state.insert(source_id, (next_seq, chain_tip));
        }

        out.push(EpochInput {
            batches,
            prev_chain_hash,
            prev_source_chain_tips: prev_tips.clone(),
        });

        // Advance cross-epoch chain state by actually running the (cheap) core
        // once so the next epoch's prev_* fields match exactly what the guest
        // would carry forward. This is identical to the host bench loop.
        let last = out.last().unwrap();
        let (final_chain_hash, final_tips) = match mode {
            "samples" => {
                let input = SamplesAggrInput {
                    prev_chain_hash: last.prev_chain_hash,
                    batches: last.batches.clone(),
                    prev_source_chain_tips: last.prev_source_chain_tips.clone(),
                };
                let (_s, o) = process_samples_aggr_with_state(&input);
                (o.epoch_chain_link.final_chain_hash, o.final_source_chain_tips)
            }
            "histogram" => {
                let input = HistogramAggrInput {
                    prev_chain_hash: last.prev_chain_hash,
                    batches: last.batches.clone(),
                    prev_source_chain_tips: last.prev_source_chain_tips.clone(),
                };
                let (_s, o) = process_histogram_aggr_with_state(&input);
                (o.epoch_chain_link.final_chain_hash, o.final_source_chain_tips)
            }
            _ => {
                let input = CmAggrInput {
                    prev_chain_hash: last.prev_chain_hash,
                    batches: last.batches.clone(),
                    prev_source_chain_tips: last.prev_source_chain_tips.clone(),
                };
                let (_s, o) = process_cm_aggr_with_state(&input);
                (o.epoch_chain_link.final_chain_hash, o.final_source_chain_tips)
            }
        };
        prev_chain_hash = final_chain_hash;
        prev_tips = final_tips;
    }

    out
}

// Small helper: uniform u32 in [0, n) using rng.next_u32 (avoids pulling in
// rand::Rng's gen_range trait bounds; distribution is not analytically critical
// for timestamps which are not part of the aggregation arithmetic).
trait NextU32In {
    fn next_u32_in(&mut self, n: u32) -> u32;
}
impl<T: RngCore> NextU32In for T {
    fn next_u32_in(&mut self, n: u32) -> u32 {
        if n == 0 {
            0
        } else {
            (self.next_u32() % n) as u32
        }
    }
}

// ---------------------------------------------------------------------------
// Measurement helpers
// ---------------------------------------------------------------------------

/// Peak resident set size of this process (kB), from /proc/self/status VmHWM.
fn peak_rss_kb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            for tok in rest.split_whitespace() {
                if let Ok(v) = tok.parse::<u64>() {
                    return v;
                }
            }
        }
    }
    0
}

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == name {
            if let Some(v) = args.get(i + 1) {
                return v.clone();
            }
        }
    }
    default.to_string()
}

fn argu64(name: &str, default: u64) -> u64 {
    let s = arg(name, &default.to_string());
    let s = s.trim();
    if let Some(hex) = s.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).unwrap_or(default)
    } else {
        s.parse().unwrap_or(default)
    }
}

// ---------------------------------------------------------------------------
// Aggregation task
// ---------------------------------------------------------------------------
fn run_aggregation(mode: &str) {
    let series = argu64("--series", 128);
    let sps = argu64("--samples-per-series", 128);
    let batch = argu64("--batch", 8);
    let epochs = argu64("--epochs", 8);
    let threads = argu64("--threads", 32) as usize;
    let reps = argu64("--reps", 5).max(1);
    let seed = argu64("--seed", 0xA66A_1E);
    let key_zipf = std::env::var("KEY_ZIPF_S").ok().and_then(|s| s.parse::<f64>().ok());

    let epoch_inputs = generate_epochs(mode, series, sps, batch, 1, epochs, seed, key_zipf);
    let events_per_epoch: u64 = epoch_inputs
        .first()
        .map(|e| e.batches.iter().map(|b| b.events.len() as u64).sum())
        .unwrap_or(0);

    // Closure that processes ONE epoch natively (the analytics logic only).
    let run_one = |ei: &EpochInput| match mode {
        "samples" => {
            let input = SamplesAggrInput {
                prev_chain_hash: ei.prev_chain_hash,
                batches: ei.batches.clone(),
                prev_source_chain_tips: ei.prev_source_chain_tips.clone(),
            };
            let (_s, o) = process_samples_aggr_with_state(&input);
            o.n_events
        }
        "histogram" => {
            let input = HistogramAggrInput {
                prev_chain_hash: ei.prev_chain_hash,
                batches: ei.batches.clone(),
                prev_source_chain_tips: ei.prev_source_chain_tips.clone(),
            };
            let (_s, o) = process_histogram_aggr_with_state(&input);
            o.n_events
        }
        _ => {
            let input = CmAggrInput {
                prev_chain_hash: ei.prev_chain_hash,
                batches: ei.batches.clone(),
                prev_source_chain_tips: ei.prev_source_chain_tips.clone(),
            };
            let (_s, o) = process_cm_aggr_with_state(&input);
            o.n_events
        }
    };

    // --- Single-thread runtime: process all epochs sequentially. ---
    let mut best_seq_ns = u128::MAX;
    let mut checksum = 0u64;
    for _ in 0..reps {
        let t = Instant::now();
        for ei in &epoch_inputs {
            checksum = checksum.wrapping_add(run_one(ei));
        }
        best_seq_ns = best_seq_ns.min(t.elapsed().as_nanos());
    }

    // --- Multi-core runtime: process epochs in parallel across `threads`. ---
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .expect("rayon pool");
    let mut best_par_ns = u128::MAX;
    for _ in 0..reps {
        let t = Instant::now();
        let sum: u64 = pool.install(|| {
            use rayon::prelude::*;
            epoch_inputs.par_iter().map(|ei| run_one(ei)).sum()
        });
        checksum = checksum.wrapping_add(sum);
        best_par_ns = best_par_ns.min(t.elapsed().as_nanos());
    }

    let seq_ms = best_seq_ns as f64 / 1e6;
    let par_ms = best_par_ns as f64 / 1e6;
    println!("task=aggregation");
    println!("mode={}", mode);
    println!("series={}", series);
    println!("samples_per_series={}", sps);
    println!("batch={}", batch);
    println!("epochs={}", epochs);
    println!("events_per_epoch={}", events_per_epoch);
    println!("total_events={}", events_per_epoch * epochs);
    println!("threads={}", threads);
    println!("reps={}", reps);
    println!("native_single_thread_ms={:.4}", seq_ms);
    println!("native_max_core_ms={:.4}", par_ms);
    println!("native_per_epoch_single_ms={:.4}", seq_ms / epochs as f64);
    println!("peak_rss_kb={}", peak_rss_kb());
    println!("checksum={}", checksum);
}

// ---------------------------------------------------------------------------
// Query task
// ---------------------------------------------------------------------------
fn build_chain_link(prev: [u8; 32], state_commit: [u8; 32]) -> EpochChainLink {
    EpochChainLink::new(prev, state_commit)
}

fn run_query(epoch_type: &str) {
    let series = argu64("--series", if epoch_type == "cm" { 8192 } else { 1024 });
    let sps = argu64(
        "--samples-per-series",
        if epoch_type == "cm" { 1 } else { 8 },
    );
    let batch = argu64("--batch", 8);
    let num_epochs = argu64("--num-epochs", 16);
    let reps = argu64("--reps", 5).max(1);
    let seed = argu64("--seed", 0xA66A_1E);
    let query_kind = arg("--query", "global_sum");
    let key_zipf = std::env::var("KEY_ZIPF_S").ok().and_then(|s| s.parse::<f64>().ok());

    let epoch_inputs = generate_epochs(epoch_type, series, sps, batch, 1, num_epochs, seed, key_zipf);
    let events_per_epoch: u64 = epoch_inputs
        .first()
        .map(|e| e.batches.iter().map(|b| b.events.len() as u64).sum())
        .unwrap_or(0);

    // Build typed epoch states + chain links (this is "LoadEpochs"; it is the
    // query setup, NOT part of the timed query logic).
    let key0 = Event::make_key_id(0, 1);
    let zero_mask = [0u8; KEY_BYTES_LEN];

    let timed_ms: f64 = match epoch_type {
        "samples" => {
            let mut states: Vec<SamplesEpochState> = Vec::new();
            let mut links: Vec<EpochChainLink> = Vec::new();
            for ei in &epoch_inputs {
                let input = SamplesAggrInput {
                    prev_chain_hash: ei.prev_chain_hash,
                    batches: ei.batches.clone(),
                    prev_source_chain_tips: ei.prev_source_chain_tips.clone(),
                };
                let (s, o) = process_samples_aggr_with_state(&input);
                states.push(SamplesEpochState::from(&s));
                links.push(build_chain_link(o.epoch_chain_link.prev_chain_hash, o.state_commit));
            }
            let query = match query_kind.as_str() {
                "global_sum" => q::SamplesQuery::Sum,
                "per_key_sum" => q::SamplesQuery::SumExactKey { key: key0 },
                "topk_hash" => q::SamplesQuery::SumTopk { limit: 10 },
                other => panic!("unsupported samples query {other}"),
            };
            let input = q::SamplesQueryInput {
                query,
                epoch_states: states,
                epoch_chain_links: links,
            };
            let mut best = u128::MAX;
            let mut acc = 0u64;
            for _ in 0..reps {
                let t = Instant::now();
                let out = q::run_samples_query(&input);
                best = best.min(t.elapsed().as_nanos());
                acc = acc.wrapping_add(out.state_commits.len() as u64);
            }
            std::hint::black_box(acc);
            best as f64 / 1e6
        }
        "histogram" => {
            let mut states: Vec<HistogramEpochState> = Vec::new();
            let mut links: Vec<EpochChainLink> = Vec::new();
            for ei in &epoch_inputs {
                let input = HistogramAggrInput {
                    prev_chain_hash: ei.prev_chain_hash,
                    batches: ei.batches.clone(),
                    prev_source_chain_tips: ei.prev_source_chain_tips.clone(),
                };
                let (s, o) = process_histogram_aggr_with_state(&input);
                states.push(s);
                links.push(build_chain_link(o.epoch_chain_link.prev_chain_hash, o.state_commit));
            }
            let query = match query_kind.as_str() {
                "hist_percentile" => q::HistogramQuery::P90,
                "global_sum" => q::HistogramQuery::All,
                other => panic!("unsupported histogram query {other}"),
            };
            let input = q::HistogramQueryInput {
                query,
                epoch_states: states,
                epoch_chain_links: links,
            };
            let mut best = u128::MAX;
            let mut acc = 0u64;
            for _ in 0..reps {
                let t = Instant::now();
                let out = q::run_histogram_query(&input);
                best = best.min(t.elapsed().as_nanos());
                acc = acc.wrapping_add(out.state_commits.len() as u64);
            }
            std::hint::black_box(acc);
            best as f64 / 1e6
        }
        _ => {
            // cm
            let mut states: Vec<CmEpochState> = Vec::new();
            let mut links: Vec<EpochChainLink> = Vec::new();
            for ei in &epoch_inputs {
                let input = CmAggrInput {
                    prev_chain_hash: ei.prev_chain_hash,
                    batches: ei.batches.clone(),
                    prev_source_chain_tips: ei.prev_source_chain_tips.clone(),
                };
                let (s, o) = process_cm_aggr_with_state(&input);
                states.push(CmEpochState::from(&s));
                links.push(build_chain_link(o.epoch_chain_link.prev_chain_hash, o.state_commit));
            }
            let query = match query_kind.as_str() {
                "cm_topk" => q::CmQuery::Topk { limit: 10 },
                "cm_estimate" => q::CmQuery::Estimate { key: key0 },
                other => panic!("unsupported cm query {other}"),
            };
            let _ = zero_mask;
            let input = q::CmQueryInput {
                query,
                epoch_states: states,
                epoch_chain_links: links,
            };
            let mut best = u128::MAX;
            let mut acc = 0u64;
            for _ in 0..reps {
                let t = Instant::now();
                let out = q::run_cm_query(&input);
                best = best.min(t.elapsed().as_nanos());
                acc = acc.wrapping_add(out.state_commits.len() as u64);
            }
            std::hint::black_box(acc);
            best as f64 / 1e6
        }
    };

    println!("task=query");
    println!("epoch_type={}", epoch_type);
    println!("query={}", query_kind);
    println!("series={}", series);
    println!("samples_per_series={}", sps);
    println!("events_per_epoch={}", events_per_epoch);
    println!("num_epochs={}", num_epochs);
    println!("reps={}", reps);
    println!("native_query_ms={:.6}", timed_ms);
    println!("peak_rss_kb={}", peak_rss_kb());
}

fn main() {
    let task = arg("--task", "aggregation");
    let mode = arg("--mode", &arg("--epoch-type", "samples"));
    match task.as_str() {
        "aggregation" => run_aggregation(&mode),
        "query" => run_query(&mode),
        other => {
            eprintln!("unknown --task {other}");
            std::process::exit(2);
        }
    }
}
