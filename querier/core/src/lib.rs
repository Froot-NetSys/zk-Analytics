#![no_std]

// Security note: This codebase uses the word "commitment" when it means "hash".
// i.e. commitments in this code are deterministic and unblinded. This is fine for 
// our use case (high entropy network logs), but may not be fine in general.

extern crate alloc;

pub mod merkle;

use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};
use zktelemetry_risc0_aggr_core::{
    key_to_u64,
    histogram_epoch_state_commit, samples_epoch_state_commit, cm_epoch_state_commit,
    CM_COLS, CM_ROWS, CM_SEEDS, CM_TOPK_SLOTS, HISTOGRAM_SLOTS,
};
use zktelemetry_risc0_common::KEY_BYTES_LEN;

/// LOW-SUPPORT privacy threshold ("suppressed sentinel").
///
/// An aggregate query whose number of contributing records/observations
/// (`support`) is below this value MUST NOT reveal its real result. Instead of
/// failing the proof (which leaked a fault/exit-status side channel), the guest
/// ALWAYS produces a valid proof but commits a fixed placeholder: every numeric
/// result field is zeroed, `support` is set to 0, and `suppressed` is set to
/// `true`. This is an in-band, constant-shape suppression signal — the only
/// information revealed about a low-support aggregate is the single `suppressed`
/// bit. See `*::suppress_if_low_support`.
///
/// This constant is compiled into every guest and therefore becomes part of
/// the guest image ID — changing it changes the audited image.
pub const MIN_SUPPORT: u64 = 10;

#[cfg(target_os = "zkvm")]
use risc0_zkvm::sha::rust_crypto::{Digest as _, Sha256};
#[cfg(not(target_os = "zkvm"))]
use rust_crypto::{Digest as _, Sha256};

const TAG_EPOCH_CHAIN: &[u8] = b"ZKTLM_EPOCH_CHAIN_V1";

fn sha256_bytes(parts: &[&[u8]]) -> [u8; 32] {
    let mut sha = Sha256::new();
    for p in parts {
        sha.update(p);
    }
    sha.finalize().into()
}

// Re-export EpochChainLink and epoch state types from aggregator for convenience
pub use zktelemetry_risc0_aggr_core::EpochChainLink;
pub use zktelemetry_risc0_aggr_core::{HistogramEpochState, SamplesEpochState, CmEpochState};

// Design Note: HashMap vs Vec in Epoch States
//
// The aggregator uses HashMap internally for efficient event aggregation during epoch processing.
// However, epoch states serialize to deterministic sorted Vecs for cryptographic commitments:
// - SamplesEpochState.per_key: Vec<BucketEntry> (sorted by key_id)
// - HistogramEpochState.per_key_histograms: Vec<KeyHistogram> (sorted by key_id)
// - CmEpochState.counts: Vec<u32> (flattened 2D array as counts[r * CM_COLS + c])
//
// This querier works directly with these Vec-based structures from committed epoch states.
// The Vec representation ensures deterministic hashing and enables efficient ZK proofs,
// while the aggregator's internal HashMap usage remains an implementation detail.

/// Global scalar values common to all epoch state types.
/// Used with MerkleProof mode for per-key queries.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GlobalScalars {
    pub total_count: u64,
    pub total_sum: u64,
}

/// Epoch data can be provided in full or as Merkle proofs for selective queries.
///
/// For the initial implementation, only Full mode is used.
/// MerkleProof mode can be added later as an optimization for per-key queries
/// where query cost inside ZK scales with query selectivity, not total key count.
///
/// TODO: Implement MerkleProof mode for per-key queries:
/// - Global queries (total sum, total count): use Full mode with verified global fields
/// - Per-key queries: host provides Merkle proof, guest verifies inclusion against root
/// - All-keys queries: host provides full sorted Vec, guest recomputes Merkle root
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EpochData<S> {
    /// Full state for all-keys queries or queries that need all data
    Full(S),
    // TODO: Add MerkleProof variant for selective per-key queries:
    // MerkleProof {
    //     global_scalars: GlobalScalars,
    //     entries: Vec<KeyEntry>,           // only the relevant entries
    //     siblings: Vec<Vec<[u8; 32]>>,     // Merkle siblings per entry
    //     indices: Vec<usize>,              // leaf indices per entry
    // },
}

/// Verify epoch hash chain integrity inside ZK proof.
/// This function is called inside the guest, so any assertion failure
/// will cause the proof to fail - providing cryptographic guarantees.
///
/// Verifies:
/// 1. Each epoch's final_chain_hash is computed correctly: hash(TAG || prev || state_commit)
/// 2. Chain is linked: prev_chain_hash[n] == final_chain_hash[n-1]
fn verify_epoch_chain_integrity(chain_links: &[EpochChainLink]) {
    for (i, link) in chain_links.iter().enumerate() {
        // Verify epoch hash computation: final_chain_hash = hash(TAG || prev || state_commit)
        let computed_hash = sha256_bytes(&[
            TAG_EPOCH_CHAIN,
            &link.prev_chain_hash,
            &link.state_commit,
        ]);
        assert!(
            computed_hash == link.final_chain_hash,
            "epoch {} hash mismatch: computed != expected",
            i
        );

        // Verify chain linkage (except for first epoch)
        if i > 0 {
            let prev_link = &chain_links[i - 1];
            assert!(
                link.prev_chain_hash == prev_link.final_chain_hash,
                "epoch {} chain broken: prev_hash != prev_epoch_final_hash",
                i
            );
        }
    }
}

// Helper function to apply mask to key (used by various query functions)
fn key_mask_fn(k: &[u8; KEY_BYTES_LEN], mask: &[u8; KEY_BYTES_LEN]) -> [u8; KEY_BYTES_LEN] {
    let mut result = [0u8; KEY_BYTES_LEN];
    for i in 0..KEY_BYTES_LEN {
        result[i] = k[i] & mask[i];
    }
    result
}

fn cm_bucket_index(key: &[u8; KEY_BYTES_LEN], row: usize) -> usize {
    let seed = CM_SEEDS[row];
    let mut x = key_to_u64(key) ^ (((seed as u64) << 32) | seed as u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x as usize) % CM_COLS
}

fn cm_bucket_index_u64(key: u64, row: usize) -> usize {
    let seed = CM_SEEDS[row];
    let mut x = key ^ (((seed as u64) << 32) | seed as u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x as usize) % CM_COLS
}

/// Top-k item containing only the count value, not the key.
/// This preserves privacy by revealing only the distribution of top values,
/// not which specific keys have those values.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmTopkItem {
    /// Count estimate (u64 for cross-epoch aggregation)
    pub count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CmQuery {
    Estimate { key: [u8; KEY_BYTES_LEN] },
    /// Top-k query returning the k highest count estimates (without keys for privacy).
    /// Recommended default: limit = 10
    Topk { limit: u16 },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmQueryInput {
    pub query: CmQuery,
    /// Typed epoch states from the aggregator, verified against chain links.
    pub epoch_states: Vec<CmEpochState>,
    /// Epoch chain links for cryptographic verification of hash chain integrity.
    /// Each link contains prev_chain_hash, state_commit, final_chain_hash.
    /// The guest verifies the hash chain and that epoch_states[i] hashes to epoch_chain_links[i].state_commit.
    #[serde(default)]
    pub epoch_chain_links: Vec<EpochChainLink>,
}

/// The actual query result (unchanged enum for backwards compatibility)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CmQueryResult {
    Estimate { estimate: u64 },
    Topk { items: Vec<CmTopkItem> },
}

/// Full query output with chain binding for journal verification.
/// Includes chain boundaries so the journal binds the result to a verified chain.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmQueryOutput {
    /// Chain hash at start of query range: epoch_chain_links[0].prev_chain_hash
    /// If epoch_chain_links is empty, this is [0u8; 32].
    pub chain_prev_hash: [u8; 32],
    /// Chain hash at end of query range: epoch_chain_links.last().final_chain_hash
    /// If epoch_chain_links is empty, this is [0u8; 32].
    pub chain_final_hash: [u8; 32],
    /// State commits from input, returned in public output for external verifier to match.
    pub state_commits: Vec<[u8; 32]>,
    /// The actual query result.
    pub result: CmQueryResult,
    /// Number of contributing observations the aggregate is computed over.
    /// PROXY: the count-min sketch does not track exact support, so this uses the
    /// estimate value itself (the merged CM count for the queried key, or the max
    /// top-k estimate). It is a lower-bound-ish proxy, not an exact record count.
    pub support: u64,
    /// Suppressed-sentinel flag. `false` for a normal result. Set to `true` by
    /// `suppress_if_low_support` when `support < MIN_SUPPORT`, in which case all
    /// numeric result fields and `support` are zeroed.
    pub suppressed: bool,
}

impl CmQueryOutput {
    /// If `support < MIN_SUPPORT`, replace the real result with a fixed
    /// placeholder: zero every numeric result field (estimate / top-k counts),
    /// zero `support`, and set `suppressed = true`. Otherwise return `self`
    /// unchanged. Pure transformation; chain boundaries and state commits are
    /// preserved so the journal stays bound to the verified chain.
    pub fn suppress_if_low_support(mut self) -> Self {
        if self.support < MIN_SUPPORT {
            self.result = match self.result {
                CmQueryResult::Estimate { .. } => CmQueryResult::Estimate { estimate: 0 },
                CmQueryResult::Topk { .. } => CmQueryResult::Topk { items: Vec::new() },
            };
            self.support = 0;
            self.suppressed = true;
        }
        self
    }
}

pub fn run_cm_query(input: &CmQueryInput) -> CmQueryOutput {
    let epochs = input.epoch_states.len();

    // Verify epoch hash chain inside the proof (cryptographically proven)
    if !input.epoch_chain_links.is_empty() {
        verify_epoch_chain_integrity(&input.epoch_chain_links);
        assert!(
            input.epoch_chain_links.len() == epochs,
            "epoch_chain_links length != epoch_states length"
        );

        // Verify each epoch state hashes to the state_commit in the chain link
        for (i, (state, link)) in input.epoch_states.iter().zip(input.epoch_chain_links.iter()).enumerate() {
            let computed_commit = cm_epoch_state_commit(state);
            assert!(
                computed_commit == link.state_commit,
                "epoch {} state_commit mismatch: computed != chain_link",
                i
            );
        }
    }

    // Extract chain boundaries for output
    let (chain_prev_hash, chain_final_hash) = if input.epoch_chain_links.is_empty() {
        ([0u8; 32], [0u8; 32])
    } else {
        (
            input.epoch_chain_links[0].prev_chain_hash,
            input.epoch_chain_links.last().unwrap().final_chain_hash,
        )
    };

    // Collect state_commits for output
    let state_commits: Vec<[u8; 32]> = input.epoch_chain_links.iter()
        .map(|link| link.state_commit)
        .collect();

    // Sum CM counts across all epochs from typed epoch states
    // CmEpochState.counts is Vec<u32>, sum_counts is Vec<u64>
    let mut sum_counts = alloc::vec![0u64; CM_ROWS * CM_COLS];
    for state in &input.epoch_states {
        for r in 0..CM_ROWS {
            for c in 0..CM_COLS {
                let idx = r * CM_COLS + c;
                sum_counts[idx] = sum_counts[idx].wrapping_add(state.counts[idx] as u64);
            }
        }
    }

    let result = match input.query {
        CmQuery::Estimate { ref key } => {
            let mut est = u64::MAX;
            for r in 0..CM_ROWS {
                let pos = cm_bucket_index(key, r);
                est = est.min(sum_counts[r * CM_COLS + pos]);
            }
            CmQueryResult::Estimate {
                estimate: if est == u64::MAX { 0 } else { est },
            }
        }
        CmQuery::Topk { limit } => {
            let limit = (limit as usize).min(CM_TOPK_SLOTS);

            // Collect candidate keys from all epoch heaps
            let mut candidates: Vec<[u8; KEY_BYTES_LEN]> = Vec::new();
            let zero_key = [0u8; KEY_BYTES_LEN];
            for state in &input.epoch_states {
                for i in 0..state.heap_keys.len() {
                    if state.heap_occ[i] != 0 && state.heap_keys[i] != zero_key {
                        candidates.push(state.heap_keys[i]);
                    }
                }
            }

            if candidates.is_empty() {
                return CmQueryOutput {
                    chain_prev_hash,
                    chain_final_hash,
                    state_commits,
                    result: CmQueryResult::Topk { items: Vec::new() },
                    // PROXY: empty top-k => zero support (no observations).
                    support: 0,
                    suppressed: false,
                };
            }
            candidates.sort_unstable();
            candidates.dedup();

            // Re-estimate counts for each candidate using sum_counts
            let mut items: Vec<CmTopkItem> = Vec::with_capacity(candidates.len());
            for k in candidates {
                let mut est = u64::MAX;
                for r in 0..CM_ROWS {
                    let pos = cm_bucket_index(&k, r);
                    est = est.min(sum_counts[r * CM_COLS + pos]);
                }
                let count = if est == u64::MAX { 0 } else { est };
                items.push(CmTopkItem { count });
            }
            // Sort by count descending (keys not included for privacy)
            items.sort_by(|a, b| b.count.cmp(&a.count));
            items.truncate(limit);
            CmQueryResult::Topk { items }
        }
    };

    // PROXY: the count-min sketch does not track exact support. Use the estimate
    // value as the support proxy: for Estimate that is the merged count for the
    // queried key; for Topk it is the largest (head) estimate among returned items.
    let support = match &result {
        CmQueryResult::Estimate { estimate } => *estimate,
        CmQueryResult::Topk { items } => items.first().map(|it| it.count).unwrap_or(0),
    };

    CmQueryOutput {
        chain_prev_hash,
        chain_final_hash,
        state_commits,
        result,
        support,
        suppressed: false,
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramBucketItem {
    pub bucket: u16,
    pub count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HistogramQuery {
    Bucket { bucket: u16 },
    All,
    P90,
    /// All buckets filtered to keys matching (key_id & mask) == (key & mask) byte-by-byte.
    AllKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramQueryInput {
    pub query: HistogramQuery,
    /// Typed epoch states from the aggregator, verified against chain links.
    pub epoch_states: Vec<HistogramEpochState>,
    /// Epoch chain links for cryptographic verification of hash chain integrity.
    /// Each link contains prev_chain_hash, state_commit, final_chain_hash.
    /// The guest verifies the hash chain and that epoch_states[i] hashes to epoch_chain_links[i].state_commit.
    #[serde(default)]
    pub epoch_chain_links: Vec<EpochChainLink>,
}

/// The actual query result (unchanged enum for backwards compatibility)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HistogramQueryResult {
    Bucket { bucket: u16, count: u64 },
    All {
        total_count: u64,
        total_sum: u64,
        buckets: Vec<HistogramBucketItem>,
    },
    P90 {
        p90_value: u64,
        total_count: u64,
    },
    AllKey {
        key: [u8; KEY_BYTES_LEN],
        mask: [u8; KEY_BYTES_LEN],
        total_count: u64,
        total_sum: u64,
        buckets: Vec<HistogramBucketItem>,
    },
}

/// Full query output with chain binding for journal verification.
/// Includes chain boundaries so the journal binds the result to a verified chain.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramQueryOutput {
    /// Chain hash at start of query range: epoch_chain_links[0].prev_chain_hash
    pub chain_prev_hash: [u8; 32],
    /// Chain hash at end of query range: epoch_chain_links.last().final_chain_hash
    pub chain_final_hash: [u8; 32],
    /// State commits from input, returned in public output for external verifier to match.
    pub state_commits: Vec<[u8; 32]>,
    /// The actual query result.
    pub result: HistogramQueryResult,
    /// Number of contributing observations the aggregate is computed over.
    /// For global queries (Bucket/All/P90) this is the aggregated total_count.
    /// For AllKey this is the masked-key filtered total_count.
    pub support: u64,
    /// Suppressed-sentinel flag. `false` for a normal result. Set to `true` by
    /// `suppress_if_low_support` when `support < MIN_SUPPORT`, in which case all
    /// numeric result fields and `support` are zeroed.
    pub suppressed: bool,
}

impl HistogramQueryOutput {
    /// If `support < MIN_SUPPORT`, replace the real result with a fixed
    /// placeholder: zero every numeric result field (bucket count, totals, and
    /// the per-bucket items list), zero `support`, and set `suppressed = true`.
    /// Key/mask selectors are preserved (they are query inputs, not results).
    /// Otherwise return `self` unchanged. Pure transformation.
    pub fn suppress_if_low_support(mut self) -> Self {
        if self.support < MIN_SUPPORT {
            self.result = match self.result {
                HistogramQueryResult::Bucket { bucket, .. } => {
                    HistogramQueryResult::Bucket { bucket, count: 0 }
                }
                HistogramQueryResult::All { .. } => HistogramQueryResult::All {
                    total_count: 0,
                    total_sum: 0,
                    buckets: Vec::new(),
                },
                HistogramQueryResult::P90 { .. } => HistogramQueryResult::P90 {
                    p90_value: 0,
                    total_count: 0,
                },
                HistogramQueryResult::AllKey { key, mask, .. } => {
                    HistogramQueryResult::AllKey {
                        key,
                        mask,
                        total_count: 0,
                        total_sum: 0,
                        buckets: Vec::new(),
                    }
                }
            };
            self.support = 0;
            self.suppressed = true;
        }
        self
    }
}

pub fn run_histogram_query(input: &HistogramQueryInput) -> HistogramQueryOutput {
    let epochs = input.epoch_states.len();

    // Verify epoch hash chain inside the proof (cryptographically proven)
    if !input.epoch_chain_links.is_empty() {
        verify_epoch_chain_integrity(&input.epoch_chain_links);
        assert!(
            input.epoch_chain_links.len() == epochs,
            "epoch_chain_links length != epoch_states length"
        );

        // Verify each epoch state hashes to the state_commit in the chain link
        for (i, (state, link)) in input.epoch_states.iter().zip(input.epoch_chain_links.iter()).enumerate() {
            let computed_commit = histogram_epoch_state_commit(state);
            assert!(
                computed_commit == link.state_commit,
                "epoch {} state_commit mismatch: computed != chain_link",
                i
            );
        }
    }

    // Extract chain boundaries for output
    let (chain_prev_hash, chain_final_hash) = if input.epoch_chain_links.is_empty() {
        ([0u8; 32], [0u8; 32])
    } else {
        (
            input.epoch_chain_links[0].prev_chain_hash,
            input.epoch_chain_links.last().unwrap().final_chain_hash,
        )
    };

    // Collect state_commits for output
    let state_commits: Vec<[u8; 32]> = input.epoch_chain_links.iter()
        .map(|link| link.state_commit)
        .collect();

    // Aggregate totals and bucket counts from typed epoch states
    let mut total_count = 0u64;
    let mut total_sum = 0u64;
    let mut counts = [0u64; HISTOGRAM_SLOTS];

    for state in &input.epoch_states {
        total_count = total_count.wrapping_add(state.total_count);
        total_sum = total_sum.wrapping_add(state.total_sum);

        // Compute global bucket counts by summing per-key histograms
        for entry in &state.per_key_histograms {
            for (b, &c) in entry.bucket_counts.iter().enumerate() {
                counts[b] = counts[b].wrapping_add(c as u64);
            }
        }
    }

    let result = match input.query {
        HistogramQuery::Bucket { bucket } => {
            let idx = bucket as usize;
            let count = if idx < HISTOGRAM_SLOTS { counts[idx] } else { 0 };
            HistogramQueryResult::Bucket { bucket, count }
        }
        HistogramQuery::All => {
            let mut out = Vec::new();
            for (i, c) in counts.iter().enumerate() {
                if *c != 0 {
                    out.push(HistogramBucketItem {
                        bucket: i as u16,
                        count: *c,
                    });
                }
            }
            HistogramQueryResult::All {
                total_count,
                total_sum,
                buckets: out,
            }
        }
        HistogramQuery::P90 => {
            // Calculate P90: find bucket where cumulative count reaches 90% of total
            let target_count = (total_count as u128 * 90 / 100) as u64;
            let mut cumulative_count = 0u64;
            let mut p90_value = 0u64;

            for (bucket_idx, &count) in counts.iter().enumerate() {
                cumulative_count = cumulative_count.saturating_add(count);
                if cumulative_count >= target_count {
                    // Estimate value within bucket: use lower bound of bucket range
                    // Bucket i contains values in range [2^(i-1), 2^i) for i >= 1
                    // Bucket 0 contains value 0
                    p90_value = match bucket_idx {
                        0 => 0,
                        1 => 1,
                        _ => 1u64 << (bucket_idx - 1), // 2^(bucket_idx - 1)
                    };
                    break;
                }
            }

            HistogramQueryResult::P90 {
                p90_value,
                total_count,
            }
        }
        HistogramQuery::AllKey { ref key, ref mask } => {
            // Filter per-key histograms by (key_id & mask) == (key & mask) byte-by-byte
            let mut filtered_count = 0u64;
            let mut filtered_sum = 0u64;
            let mut filtered_counts = [0u64; HISTOGRAM_SLOTS];

            for state in &input.epoch_states {
                for entry in &state.per_key_histograms {
                    let mut matches = true;
                    for i in 0..KEY_BYTES_LEN {
                        if (entry.key_id[i] & mask[i]) != (key[i] & mask[i]) {
                            matches = false;
                            break;
                        }
                    }
                    if matches {
                        filtered_count = filtered_count.wrapping_add(entry.count as u64);
                        filtered_sum = filtered_sum.wrapping_add(entry.sum);
                        for (b, &c) in entry.bucket_counts.iter().enumerate() {
                            filtered_counts[b] = filtered_counts[b].wrapping_add(c as u64);
                        }
                    }
                }
            }

            let mut out = Vec::new();
            for (i, c) in filtered_counts.iter().enumerate() {
                if *c != 0 {
                    out.push(HistogramBucketItem { bucket: i as u16, count: *c });
                }
            }
            HistogramQueryResult::AllKey {
                key: *key,
                mask: *mask,
                total_count: filtered_count,
                total_sum: filtered_sum,
                buckets: out,
            }
        }
    };

    // Support = number of contributing observations behind the aggregate.
    // Bucket: the global aggregated total_count. All/P90: their own total_count.
    // AllKey: the masked-key filtered total_count.
    let support = match &result {
        HistogramQueryResult::Bucket { .. } => total_count,
        HistogramQueryResult::All { total_count, .. } => *total_count,
        HistogramQueryResult::P90 { total_count, .. } => *total_count,
        HistogramQueryResult::AllKey { total_count, .. } => *total_count,
    };

    HistogramQueryOutput {
        chain_prev_hash,
        chain_final_hash,
        state_commits,
        result,
        support,
        suppressed: false,
    }
}

/// Top-k item containing only the sum value, not the keys.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplesSumTopkItem {
    pub sum: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SamplesQuery {
    Sum,
    Avg,
    SumKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
    AvgKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
    SumExactKey { key: [u8; KEY_BYTES_LEN] },
    SumKeyIds { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
    /// Top-k query returning the k highest sum values (without keys for privacy).
    /// Recommended default: limit = 10
    SumTopk { limit: u16 },
    MaxKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplesQueryInput {
    pub query: SamplesQuery,
    /// Typed epoch states from the aggregator, verified against chain links.
    pub epoch_states: Vec<SamplesEpochState>,
    /// Epoch chain links for cryptographic verification of hash chain integrity.
    /// Each link contains prev_chain_hash, state_commit, final_chain_hash.
    /// The guest verifies the hash chain and that epoch_states[i] hashes to epoch_chain_links[i].state_commit.
    #[serde(default)]
    pub epoch_chain_links: Vec<EpochChainLink>,
}

/// The actual query result.
/// Note: Key fields removed from per-key query results - the verifier already knows
/// which key was queried from the input, so returning it is unnecessary.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SamplesQueryResult {
    Sum { sum: u64 },
    Avg { avg: u64 },
    SumKey { sum: u64 },
    AvgKey { avg: u64 },
    SumExactKey { sum: u64 },
    SumKeyIds { sum_keys: u64 },
    SumTopk { items: Vec<SamplesSumTopkItem> },
    MaxKey { max: u64 },
}

/// Full query output with chain binding for journal verification.
/// Includes chain boundaries so the journal binds the result to a verified chain.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplesQueryOutput {
    /// Chain hash at start of query range: epoch_chain_links[0].prev_chain_hash
    pub chain_prev_hash: [u8; 32],
    /// Chain hash at end of query range: epoch_chain_links.last().final_chain_hash
    pub chain_final_hash: [u8; 32],
    /// State commits from input, returned in public output for external verifier to match.
    pub state_commits: Vec<[u8; 32]>,
    /// The actual query result.
    pub result: SamplesQueryResult,
    /// Number of contributing sample records the aggregate is computed over.
    /// Global Sum/Avg: aggregated total_count. Per-key (SumKey/AvgKey/MaxKey/
    /// SumExactKey/SumKeyIds): count of matching contributing records. SumTopk:
    /// total contributing count across all occupied per-key entries.
    pub support: u64,
    /// Suppressed-sentinel flag. `false` for a normal result. Set to `true` by
    /// `suppress_if_low_support` when `support < MIN_SUPPORT`, in which case all
    /// numeric result fields and `support` are zeroed.
    pub suppressed: bool,
}

impl SamplesQueryOutput {
    /// If `support < MIN_SUPPORT`, replace the real result with a fixed
    /// placeholder: zero every numeric result field (sum / avg / max / sum_keys
    /// / top-k sums), zero `support`, and set `suppressed = true`. Otherwise
    /// return `self` unchanged. Pure transformation.
    pub fn suppress_if_low_support(mut self) -> Self {
        if self.support < MIN_SUPPORT {
            self.result = match self.result {
                SamplesQueryResult::Sum { .. } => SamplesQueryResult::Sum { sum: 0 },
                SamplesQueryResult::Avg { .. } => SamplesQueryResult::Avg { avg: 0 },
                SamplesQueryResult::SumKey { .. } => SamplesQueryResult::SumKey { sum: 0 },
                SamplesQueryResult::AvgKey { .. } => SamplesQueryResult::AvgKey { avg: 0 },
                SamplesQueryResult::SumExactKey { .. } => {
                    SamplesQueryResult::SumExactKey { sum: 0 }
                }
                SamplesQueryResult::SumKeyIds { .. } => {
                    SamplesQueryResult::SumKeyIds { sum_keys: 0 }
                }
                SamplesQueryResult::SumTopk { .. } => {
                    SamplesQueryResult::SumTopk { items: Vec::new() }
                }
                SamplesQueryResult::MaxKey { .. } => SamplesQueryResult::MaxKey { max: 0 },
            };
            self.support = 0;
            self.suppressed = true;
        }
        self
    }
}

pub fn run_samples_query(input: &SamplesQueryInput) -> SamplesQueryOutput {
    let epochs = input.epoch_states.len();

    // Verify epoch hash chain inside the proof
    if !input.epoch_chain_links.is_empty() {
        verify_epoch_chain_integrity(&input.epoch_chain_links);
        assert!(
            input.epoch_chain_links.len() == epochs,
            "epoch_chain_links length != epoch_states length"
        );

        // Verify each epoch state hashes to the state_commit in the chain link
        for (i, (state, link)) in input.epoch_states.iter().zip(input.epoch_chain_links.iter()).enumerate() {
            let computed_commit = samples_epoch_state_commit(state);
            assert!(
                computed_commit == link.state_commit,
                "epoch {} state_commit mismatch: computed != chain_link",
                i
            );
        }
    }

    // Extract chain boundaries for output
    let (chain_prev_hash, chain_final_hash) = if input.epoch_chain_links.is_empty() {
        ([0u8; 32], [0u8; 32])
    } else {
        (
            input.epoch_chain_links[0].prev_chain_hash,
            input.epoch_chain_links.last().unwrap().final_chain_hash,
        )
    };

    // Collect state_commits for output
    let state_commits: Vec<[u8; 32]> = input.epoch_chain_links.iter()
        .map(|link| link.state_commit)
        .collect();

    // Aggregate totals from typed epoch states
    let mut total_count = 0u64;
    let mut total_sum = 0u64;
    for state in &input.epoch_states {
        total_count = total_count.wrapping_add(state.total_count);
        total_sum = total_sum.wrapping_add(state.total_sum);
    }

    // Helper to apply mask to key
    fn key_mask(k: &[u8; KEY_BYTES_LEN], mask: &[u8; KEY_BYTES_LEN]) -> [u8; KEY_BYTES_LEN] {
        let mut result = [0u8; KEY_BYTES_LEN];
        for i in 0..KEY_BYTES_LEN {
            result[i] = k[i] & mask[i];
        }
        result
    }

    let zero_key = [0u8; KEY_BYTES_LEN];

    let (result, support): (SamplesQueryResult, u64) = match input.query {
        // Support = total contributing sample records across all epochs.
        SamplesQuery::Sum => (SamplesQueryResult::Sum { sum: total_sum }, total_count),
        SamplesQuery::Avg => {
            let avg = if total_count == 0 { 0 } else { total_sum / total_count };
            (SamplesQueryResult::Avg { avg }, total_count)
        }
        SamplesQuery::SumExactKey { ref key } => {
            let mut sum = 0u64;
            // Support = count of contributing records for the exact key.
            let mut cnt = 0u64;
            for state in &input.epoch_states {
                for entry in &state.per_key {
                    if entry.occupied != 0 && entry.key_id == *key {
                        sum = sum.wrapping_add(entry.sum);
                        cnt = cnt.wrapping_add(entry.count as u64);
                    }
                }
            }
            (SamplesQueryResult::SumExactKey { sum }, cnt)
        }
        SamplesQuery::SumKeyIds { ref key, ref mask } => {
            let key_m = key_mask(key, mask);
            let mut sum_keys = 0u64;
            // Support = count of contributing records behind the matched keys.
            let mut cnt = 0u64;
            for state in &input.epoch_states {
                for entry in &state.per_key {
                    if entry.occupied != 0 && key_mask(&entry.key_id, mask) == key_m {
                        sum_keys = sum_keys.wrapping_add(key_to_u64(&entry.key_id));
                        cnt = cnt.wrapping_add(entry.count as u64);
                    }
                }
            }
            (SamplesQueryResult::SumKeyIds { sum_keys }, cnt)
        }
        SamplesQuery::SumKey { ref key, ref mask } => {
            let key_m = key_mask(key, mask);
            let mut sum = 0u64;
            // Support = count of contributing records matching the masked key.
            let mut cnt = 0u64;
            for state in &input.epoch_states {
                for entry in &state.per_key {
                    if entry.occupied != 0 && key_mask(&entry.key_id, mask) == key_m {
                        sum = sum.wrapping_add(entry.sum);
                        cnt = cnt.wrapping_add(entry.count as u64);
                    }
                }
            }
            (SamplesQueryResult::SumKey { sum }, cnt)
        }
        SamplesQuery::AvgKey { ref key, ref mask } => {
            let key_m = key_mask(key, mask);
            let mut sum = 0u64;
            let mut cnt = 0u64;
            for state in &input.epoch_states {
                for entry in &state.per_key {
                    if entry.occupied != 0 && key_mask(&entry.key_id, mask) == key_m {
                        sum = sum.wrapping_add(entry.sum);
                        cnt = cnt.wrapping_add(entry.count as u64);
                    }
                }
            }
            let avg = if cnt == 0 { 0 } else { sum / cnt };
            // Support = count of contributing records matching the masked key.
            (SamplesQueryResult::AvgKey { avg }, cnt)
        }
        SamplesQuery::SumTopk { limit } => {
            let limit = (limit as usize).min(CM_TOPK_SLOTS);
            let mut pairs: Vec<([u8; KEY_BYTES_LEN], u64)> = Vec::new(); // (key,sum)
            // Support = total contributing records across all occupied per-key entries.
            let mut topk_support = 0u64;
            for state in &input.epoch_states {
                for entry in &state.per_key {
                    if entry.occupied != 0 && entry.key_id != zero_key {
                        pairs.push((entry.key_id, entry.sum));
                        topk_support = topk_support.wrapping_add(entry.count as u64);
                    }
                }
            }
            if pairs.is_empty() {
                return SamplesQueryOutput {
                    chain_prev_hash,
                    chain_final_hash,
                    state_commits,
                    result: SamplesQueryResult::SumTopk { items: Vec::new() },
                    support: 0,
                    suppressed: false,
                };
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            // Aggregate by key in-place to get per-key sums
            let mut agg: Vec<([u8; KEY_BYTES_LEN], u64)> = Vec::new();
            let mut cur_k = pairs[0].0;
            let mut cur_s = 0u64;
            for (k, s) in pairs {
                if k != cur_k {
                    agg.push((cur_k, cur_s));
                    cur_k = k;
                    cur_s = 0;
                }
                cur_s = cur_s.wrapping_add(s);
            }
            agg.push((cur_k, cur_s));
            // Sort by sum descending (keys not included in output for privacy)
            agg.sort_by(|a, b| b.1.cmp(&a.1));
            agg.truncate(limit);
            let items = agg
                .into_iter()
                .map(|(_k, s)| SamplesSumTopkItem { sum: s })
                .collect();
            (SamplesQueryResult::SumTopk { items }, topk_support)
        }
        SamplesQuery::MaxKey { ref key, ref mask } => {
            let key_m = key_mask(key, mask);
            let mut max = 0u64;
            // Support = count of contributing records matching the masked key.
            let mut cnt = 0u64;
            for state in &input.epoch_states {
                for entry in &state.per_key {
                    if entry.occupied != 0 && key_mask(&entry.key_id, mask) == key_m {
                        max = max.max(entry.sum);
                        cnt = cnt.wrapping_add(entry.count as u64);
                    }
                }
            }
            (SamplesQueryResult::MaxKey { max }, cnt)
        }
    };

    SamplesQueryOutput {
        chain_prev_hash,
        chain_final_hash,
        state_commits,
        result,
        support,
        suppressed: false,
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawMaxKeyItem {
    pub key_id: [u8; KEY_BYTES_LEN],
    pub max: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawEvent {
    pub key_id: [u8; KEY_BYTES_LEN],
    pub value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RawQuery {
    MaxKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
    StatsKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN] },
    HistBucketKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN], bucket: u16 },
    CmEstimateKey { key: [u8; KEY_BYTES_LEN], mask: [u8; KEY_BYTES_LEN], value: u64 },
}

/// Input for raw event queries.
///
/// NOTE: This query type operates on raw events rather than epoch states.
/// It does NOT currently support epoch chain verification.
///
/// Chain verification status:
/// - These queries are designed for ad-hoc analysis of raw event data
/// - The events are assumed to be provided by a trusted host or verified
///   through a separate mechanism (e.g., batch hash verification)
/// - If run inside a ZK guest, the events should be verified against a
///   commitment (e.g., events_commit) before use
/// - Consider adding epoch_chain_links if these queries need to bind
///   results to a verified epoch chain
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawQueryInput {
    pub query: RawQuery,
    pub events: Vec<RawEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RawQueryOutput {
    MaxKey { items: Vec<RawMaxKeyItem>, match_keys: u64 },
    StatsKey { sum: u64, match_keys: u64 },
    HistBucketKey { bucket: u16, count: u64, match_keys: u64 },
    CmEstimateKey { value: u64, estimate: u64, match_keys: u64 },
}

fn raw_hist_bucket(value: u64) -> u16 {
    let v = if value == 0 { 1 } else { value };
    63u16.saturating_sub(v.leading_zeros() as u16)
}

pub fn run_raw_query(input: &RawQueryInput) -> RawQueryOutput {
    match input.query {
        RawQuery::MaxKey { ref key, ref mask } => {
            let key_m = key_mask_fn(key, mask);
            let mut match_keys = 0u64;
            let mut per_key_max: BTreeMap<[u8; KEY_BYTES_LEN], u64> = BTreeMap::new();
            for e in &input.events {
                if key_mask_fn(&e.key_id, mask) == key_m {
                    match_keys += 1;
                    let entry = per_key_max.entry(e.key_id).or_insert(0);
                    *entry = (*entry).max(e.value);
                }
            }
            let items: Vec<RawMaxKeyItem> = per_key_max
                .into_iter()
                .map(|(key_id, max)| RawMaxKeyItem { key_id, max })
                .collect();
            RawQueryOutput::MaxKey { items, match_keys }
        }
        RawQuery::StatsKey { ref key, ref mask } => {
            let key_m = key_mask_fn(key, mask);
            let mut sum = 0u64;
            let mut match_keys = 0u64;
            for e in &input.events {
                if key_mask_fn(&e.key_id, mask) == key_m {
                    match_keys += 1;
                    sum = sum.wrapping_add(e.value);
                }
            }
            RawQueryOutput::StatsKey { sum, match_keys }
        }
        RawQuery::HistBucketKey { ref key, ref mask, bucket } => {
            let key_m = key_mask_fn(key, mask);
            let mut count = 0u64;
            let mut match_keys = 0u64;
            for e in &input.events {
                if key_mask_fn(&e.key_id, mask) == key_m {
                    match_keys += 1;
                    if raw_hist_bucket(e.value) == bucket {
                        count += 1;
                    }
                }
            }
            RawQueryOutput::HistBucketKey {
                bucket,
                count,
                match_keys,
            }
        }
        RawQuery::CmEstimateKey { ref key, ref mask, value } => {
            let key_m = key_mask_fn(key, mask);
            let mut counts = alloc::vec![0u64; CM_ROWS * CM_COLS];
            let mut match_keys = 0u64;
            for e in &input.events {
                if key_mask_fn(&e.key_id, mask) == key_m {
                    match_keys += 1;
                    for r in 0..CM_ROWS {
                        let pos = cm_bucket_index_u64(e.value, r);
                        counts[r * CM_COLS + pos] = counts[r * CM_COLS + pos].wrapping_add(1);
                    }
                }
            }
            let mut est = u64::MAX;
            for r in 0..CM_ROWS {
                let pos = cm_bucket_index_u64(value, r);
                est = est.min(counts[r * CM_COLS + pos]);
            }
            RawQueryOutput::CmEstimateKey {
                value,
                estimate: if est == u64::MAX { 0 } else { est },
                match_keys,
            }
        }
    }
}
