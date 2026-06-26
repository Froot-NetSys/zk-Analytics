#![no_std]

// Security note: This codebase uses the word "commitment" when it means "hash".
// i.e. commitments in this code are deterministic and unblinded. This is fine for 
// our use case (high entropy network logs), but may not be fine in general.

extern crate alloc;

pub mod merkle;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use hashbrown::HashMap;
#[cfg(target_os = "zkvm")]
use risc0_zkvm::sha::rust_crypto::{Digest as _, Sha256};
#[cfg(not(target_os = "zkvm"))]
use rust_crypto::{Digest as _, Sha256};
use serde::{Deserialize, Serialize};
use zkvm_common::{Event, HASH_BYTES_LEN, KEY_BYTES_LEN};

use merkle::{MerkleTree, Proof};
use merkle_light::proof;
use risc0_zkvm::sha::Digest;

// Constants for use in aggregation code. Variable name abbreviation guide:
// HT: "Hash Table"
// CM: "Count-Min" as in "Count-Min Sketch"
// TAG: "Domain Separator". These domain separators are used in computing various hash chains so 
//   that the different hashes cannot be used in place of each other.
pub const HISTOGRAM_SLOTS: usize = parse_usize(env!("HISTOGRAM_SLOTS"));

// Note: SAMPLES_HT_BUCKETS must be a power of 2. Explicitly check this.
pub const SAMPLES_HT_BUCKETS: usize = parse_usize(env!("SAMPLES_HT_BUCKETS"));
const _: () = assert!(SAMPLES_HT_BUCKETS.is_power_of_two(), "SAMPLES_HT_BUCKETS must be a power of 2");
pub const SAMPLES_HT_BUCKET_CAP: usize = parse_usize(env!("SAMPLES_HT_BUCKET_CAP"));

pub const CM_ROWS: usize = 3;
pub const CM_COLS: usize = 1024;
pub const CM_SEEDS: [u32; CM_ROWS] = [0x6d0f27bd, 0x9e3779b9, 0x94d049bb];
pub const CM_TOPK_SLOTS: usize = parse_usize(env!("CM_TOPK_SLOTS"));

const TAG_BUCKET_LEAF: &[u8] = b"ZKTLM_BUCKET_V1";

const TAG_HIST_STATE_COMMIT: &[u8] = b"ZKTLM_HIST_STATE_COMMIT_V1";
const TAG_SAMPLES_STATE_COMMIT: &[u8] = b"ZKTLM_SAMPLES_STATE_COMMIT_V1";
const TAG_CM_STATE_COMMIT: &[u8] = b"ZKTLM_CM_STATE_COMMIT_V1";

const TAG_CM_LEAF: &[u8] = b"ZKTLM_CM_LEAF_V1";
const TAG_CM_HEAP_LEAF: &[u8] = b"ZKTLM_CM_HEAP_LEAF_V1";

/// Tag for epoch chain hash computation: hash(TAG || prev_chain_hash || state_commit)
const TAG_EPOCH_CHAIN: &[u8] = b"ZKTLM_EPOCH_CHAIN_V1";

/// Tag for batch-level shard chain (matches data_source and kafka_producer)
const TAG_SHARD_CHAIN: &[u8] = b"ZKTLM_SHARD_CHAIN_V1";

const TAG_SOURCE_CHAIN_TIPS: &[u8] = b"ZKTLM_SOURCE_CHAIN_TIPS_V1";

/// Tags for computing out_commit values (deterministic commitment of output fields)
pub const TAG_OUT_COMMIT_SAMPLES: &[u8] = b"ZKTLM_OUT_COMMIT_SAMPLES_V1";
pub const TAG_OUT_COMMIT_HISTOGRAM: &[u8] = b"ZKTLM_OUT_COMMIT_HISTOGRAM_V1";
pub const TAG_OUT_COMMIT_CM: &[u8] = b"ZKTLM_OUT_COMMIT_CM_V1";


const fn parse_usize(s: &str) -> usize {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut acc: usize = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b < b'0' || b > b'9' {
            panic!("invalid usize");
        }
        acc = acc * 10 + (b - b'0') as usize;
        i += 1;
    }
    acc
}

const fn next_pow2_ge(value: usize) -> usize {
    if value <= 1 {
        return 1;
    }
    let mut v = 1usize;
    while v < value {
        v <<= 1;
    }
    v
}

fn sha256_bytes(parts: &[&[u8]]) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    for p in parts {
        sha.update(p);
    }
    sha.finalize().into()
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketEntry {
    pub occupied: u8,
    pub key_id: [u8; KEY_BYTES_LEN],
    pub key_chain_tip: [u8; HASH_BYTES_LEN],
    pub sum: u64,
    pub count: u32,
}

impl BucketEntry {
    pub const fn empty() -> Self {
        Self {
            occupied: 0,
            key_id: [0u8; KEY_BYTES_LEN],
            key_chain_tip: [0u8; HASH_BYTES_LEN],
            sum: 0,
            count: 0,
        }
    }

    /// Helper to get key_id as u64 (using all 15 bytes via FNV-1a mixing)
    pub fn key_id_u64(&self) -> u64 {
        key_to_u64(&self.key_id)
    }
}

/// Epoch chain link for hash chain verification inside ZK proof.
/// final_chain_hash = SHA256(TAG_EPOCH_CHAIN || prev_chain_hash || state_commit)
///
/// The chain forms over successive epochs (genesis = [0u8; 32]):
///   Epoch 0: SHA256(TAG || genesis      || state_commit_0) → chain_hash_0
///   Epoch 1: SHA256(TAG || chain_hash_0 || state_commit_1) → chain_hash_1
///   Epoch 2: SHA256(TAG || chain_hash_1 || state_commit_2) → chain_hash_2
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochChainLink {
    pub prev_chain_hash: [u8; HASH_BYTES_LEN],
    pub state_commit: [u8; HASH_BYTES_LEN],
    pub final_chain_hash: [u8; HASH_BYTES_LEN],
}

impl EpochChainLink {
    /// Compute final_chain_hash from prev_chain_hash and state_commit
    pub fn compute_final_hash(prev_chain_hash: [u8; HASH_BYTES_LEN], state_commit: [u8; HASH_BYTES_LEN]) -> [u8; HASH_BYTES_LEN] {
        sha256_bytes(&[TAG_EPOCH_CHAIN, &prev_chain_hash, &state_commit])
    }

    /// Create a new EpochChainLink, computing final_chain_hash automatically
    pub fn new(prev_chain_hash: [u8; HASH_BYTES_LEN], state_commit: [u8; HASH_BYTES_LEN]) -> Self {
        let final_chain_hash = Self::compute_final_hash(prev_chain_hash, state_commit);
        Self {
            prev_chain_hash,
            state_commit,
            final_chain_hash,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SamplesState {
    pub entries: HashMap<[u8; KEY_BYTES_LEN], BucketEntry>,
    pub total_count: u64,
    pub total_sum: u64,
    pub chain_hash: [u8; HASH_BYTES_LEN],
}

impl SamplesState {
    pub fn new(prev_chain_hash: [u8; HASH_BYTES_LEN]) -> Self {
        Self {
            entries: HashMap::new(),
            total_count: 0,
            total_sum: 0,
            chain_hash: prev_chain_hash,
        }
    }
}

/// Serializable epoch state for samples aggregation.
/// Used for host storage and querier input. Converts from SamplesState.
/// Uses Vec sorted by key_id for deterministic serialization.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplesEpochState {
    pub total_count: u64,
    pub total_sum: u64,
    pub chain_hash: [u8; HASH_BYTES_LEN],
    /// Per-key entries sorted by key_id for determinism
    pub per_key: Vec<BucketEntry>,
}

impl From<&SamplesState> for SamplesEpochState {
    fn from(state: &SamplesState) -> Self {
        let mut per_key: Vec<BucketEntry> = state.entries.values().copied().collect();
        per_key.sort_by_key(|e| e.key_id);
        Self {
            total_count: state.total_count,
            total_sum: state.total_sum,
            chain_hash: state.chain_hash,
            per_key,
        }
    }
}

/// Compute state commitment from SamplesEpochState.
/// Produces the same hash as samples_state_commit(SamplesState).
pub fn samples_epoch_state_commit(state: &SamplesEpochState) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_SAMPLES_STATE_COMMIT);
    sha.update(&state.total_count.to_be_bytes());
    sha.update(&state.total_sum.to_be_bytes());
    sha.update(&state.chain_hash);

    // per_key is already sorted by key_id
    for entry in &state.per_key {
        sha.update(&[entry.occupied]);
        sha.update(&entry.key_id);
        sha.update(&entry.key_chain_tip);
        sha.update(&entry.sum.to_be_bytes());
        sha.update(&entry.count.to_be_bytes());
    }

    sha.finalize().into()
}

/// Batch-level chain link for sequence and chain verification inside ZK proof.
/// Each link represents one batch's chain state for a specific source_id.
///
/// Chain hash formula (matches data_source and kafka_producer):
///   batch_hash = SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
///   events_commit = SHA256(key_id || value || ts for each event in batch)
///
/// The chain is per-source: all batches from a source chain together.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceChainLink {
    /// Source identifier for per-source chain verification
    pub source_id: u32,
    /// Batch sequence number within this source
    pub sequence: i64,
    /// Expected chain hash before this batch
    pub expected_chain_prev: [u8; 32],
    /// Expected chain hash after this batch (computed from batch events)
    pub expected_chain_tip: [u8; 32],
}

/// Input for a single batch in ZK verification.
/// Contains events and the producer's claimed batch_hash for verification.
/// The ZK guest will:
/// 1. Use chain_prev from aggregator's stored state (not from producer)
/// 2. Recompute batch_hash from events
/// 3. Verify recomputed hash matches sent_batch_hash
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchInput {
    /// Source identifier for per-source chain verification
    pub source_id: u32,
    /// Batch sequence number from producer (for debugging chain verification)
    pub source_batch_seq: u64,
    /// Events in this batch
    pub events: Vec<Event>,
    /// Producer's claimed batch_hash - will be VERIFIED against recomputed hash
    pub sent_batch_hash: [u8; HASH_BYTES_LEN],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplesAggrInput {
    pub prev_chain_hash: [u8; HASH_BYTES_LEN],
    /// Batches with events grouped (not flattened) for per-batch hash verification.
    /// Each batch contains events and the producer's claimed batch_hash.
    /// The ZK guest recomputes and verifies each batch_hash.
    pub batches: Vec<BatchInput>,
    /// Previous epoch's final chain tips per source_id for cross-epoch verification.
    /// These come from the aggregator's stored state (trusted).
    /// Format: Vec<(source_id, last_processed_seq, chain_tip)>
    #[serde(default)]
    pub prev_source_chain_tips: Vec<(u32, u64, [u8; HASH_BYTES_LEN])>,
}

/// Public journal output for samples aggregation (only commitments)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SamplesAggrOutput {
    /// The epoch chain link containing prev_chain_hash, state_commit, and final_chain_hash.
    /// final_chain_hash = hash(TAG_EPOCH_CHAIN || prev_chain_hash || state_commit)
    pub epoch_chain_link: EpochChainLink,
    /// Commitment to the epoch's computed state (hash over full SamplesState).
    /// Also present inside epoch_chain_link, but kept at top level for convenience.
    /// The host stores the full state privately; query guests verify it matches this commit.
    pub state_commit: [u8; HASH_BYTES_LEN],
    /// Merkle root of samples buckets
    pub buckets_root: [u8; HASH_BYTES_LEN],
    /// Number of events processed in this epoch
    pub n_events: u64,
    /// Per-source final chain tips committed in the journal.
    /// These are verified chain tips that can be used for cross-epoch continuity.
    /// Format: Vec<(source_id, last_processed_seq, chain_tip)> sorted by source_id for determinism.
    pub final_source_chain_tips: Vec<(u32, u64, [u8; HASH_BYTES_LEN])>,
}

/// Compute commitment of events: SHA256(key_id || value || ts) for each event
/// Matches data_source and kafka_producer format (no TAG prefix)
fn events_commit(events: &[Event]) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    for ev in events {
        sha.update(&ev.key_id);                // 15 bytes key
        sha.update(&ev.value.to_be_bytes());   // 4 bytes value
        sha.update(&ev.ts.to_be_bytes());      // 4 bytes timestamp
    }
    sha.finalize().into()
}

/// Batch-level chain hash: SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
/// Matches data_source and kafka_producer format.
/// When batch_size=1, this is equivalent to per-event chaining.
pub fn batch_chain_hash(prev: [u8; HASH_BYTES_LEN], events: &[Event]) -> [u8; HASH_BYTES_LEN] {
    let commit = events_commit(events);
    sha256_bytes(&[TAG_SHARD_CHAIN, &prev, &commit])
}

/// Compute a deterministic hash of per-source chain tips.
/// The tips are sorted by source_id for determinism.
pub fn source_chain_tips_commit(tips: &[(u32, u64, [u8; HASH_BYTES_LEN])]) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_SOURCE_CHAIN_TIPS);
    // Tips should already be sorted, but sort again for safety
    let mut sorted_tips = tips.to_vec();
    sorted_tips.sort_by_key(|(source_id, _, _)| *source_id);
    for (source_id, last_seq, chain_tip) in sorted_tips {
        sha.update(&source_id.to_be_bytes());
        sha.update(&last_seq.to_be_bytes());
        sha.update(&chain_tip);
    }
    sha.finalize().into()
}

/// Helper to convert HashMap chain tips to sorted Vec
fn chain_tips_to_sorted_vec(tips: HashMap<u32, (u64, [u8; HASH_BYTES_LEN])>) -> Vec<(u32, u64, [u8; HASH_BYTES_LEN])> {
    let mut vec: Vec<(u32, u64, [u8; HASH_BYTES_LEN])> = tips
        .into_iter()
        .map(|(source_id, (seq, tip))| (source_id, seq, tip))
        .collect();
    vec.sort_by_key(|(source_id, _, _)| *source_id);
    vec
}

/// Convert 15-byte key to u64 for hashing (uses all 15 bytes via mixing)
pub fn key_to_u64(key: &[u8; KEY_BYTES_LEN]) -> u64 {
    // Use FNV-1a style mixing to combine all 15 bytes into a u64
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
    for &byte in key.iter() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }
    hash
}

pub fn samples_bucket_index(key_id: &[u8; KEY_BYTES_LEN]) -> usize {
    // Deterministic mixing then mod 64.
    let mut x = key_to_u64(key_id);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    (x as usize) & (SAMPLES_HT_BUCKETS - 1)
}

pub fn samples_bucket_leaf_hash(
    entries: &[BucketEntry],
) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_BUCKET_LEAF);
    // Hash entries in sorted order by key_id for determinism
    let mut sorted_entries = entries.to_vec();
    sorted_entries.sort_by_key(|e| e.key_id);
    for e in sorted_entries {
        sha.update(&[e.occupied]);
        sha.update(&e.key_id);
        sha.update(&e.key_chain_tip);
        sha.update(&e.sum.to_be_bytes());
        sha.update(&e.count.to_be_bytes());
    }
    sha.finalize().into()
}

pub fn samples_buckets_root_from_leaves(
    leaves: &[[u8; HASH_BYTES_LEN]],
) -> [u8; HASH_BYTES_LEN] {
    // Convert to Digest type for merkle_light
    let leaf_digests: Vec<Digest> = leaves.iter()
        .map(|&leaf| Digest::try_from(&leaf[..]).unwrap())
        .collect();

    let tree = MerkleTree::new(leaf_digests);
    tree.root().into()
}

pub fn samples_buckets_leaves(state: &SamplesState) -> Vec<[u8; HASH_BYTES_LEN]> {
    // Find actual number of buckets needed (max bucket index + 1, rounded to pow2)
    let max_bucket = state.entries.values()
        .map(|entry| samples_bucket_index(&entry.key_id))
        .max()
        .unwrap_or(0);
    let num_buckets = next_pow2_ge(max_bucket + 1);

    let mut leaves = vec![[0u8; HASH_BYTES_LEN]; num_buckets];

    // Group entries by bucket index
    let mut bucket_entries: Vec<Vec<BucketEntry>> = (0..num_buckets)
        .map(|_| Vec::new())
        .collect();

    for (_, entry) in state.entries.iter() {
        let bucket_idx = samples_bucket_index(&entry.key_id);
        bucket_entries[bucket_idx].push(*entry);
    }

    // Compute leaf hash for each bucket
    for i in 0..num_buckets {
        leaves[i] = samples_bucket_leaf_hash(&bucket_entries[i]);
    }
    leaves
}

pub fn samples_buckets_root(state: &SamplesState) -> [u8; HASH_BYTES_LEN] {
    samples_buckets_root_from_leaves(&samples_buckets_leaves(state))
}

/// Compute a deterministic hash over the full SamplesState.
/// This captures all state needed for query verification:
/// - total_count, total_sum, chain_hash
/// - entries (via sorted key_id order)
///
/// The host stores the full state privately; only this hash is public in the journal.
/// Query guests verify that the host-provided epoch data hashes to this commit.
pub fn samples_state_commit(state: &SamplesState) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_SAMPLES_STATE_COMMIT);
    sha.update(&state.total_count.to_be_bytes());
    sha.update(&state.total_sum.to_be_bytes());
    sha.update(&state.chain_hash);

    // Hash entries in sorted order by key_id for determinism
    let mut sorted_entries: Vec<&BucketEntry> = state.entries.values().collect();
    sorted_entries.sort_by_key(|e| e.key_id);
    for entry in sorted_entries {
        sha.update(&[entry.occupied]);
        sha.update(&entry.key_id);
        sha.update(&entry.key_chain_tip);
        sha.update(&entry.sum.to_be_bytes());
        sha.update(&entry.count.to_be_bytes());
    }

    sha.finalize().into()
}

pub fn samples_merkle_proof_for_bucket(
    leaves: &[[u8; HASH_BYTES_LEN]],
    bucket_index: usize,
) -> Vec<[u8; HASH_BYTES_LEN]> {
    // Convert to Digest type for merkle_light
    let leaf_digests: Vec<Digest> = leaves.iter()
        .map(|&leaf| Digest::try_from(&leaf[..]).unwrap())
        .collect();

    let tree = MerkleTree::new(leaf_digests);
    let proof: Proof<Digest> = tree.prove(bucket_index);

    // Extract sibling hashes from the proof
    let lemma = proof.lemma();
    let siblings: Vec<[u8; HASH_BYTES_LEN]> = lemma.iter()
        .map(|sibling| (*sibling).into())
        .collect();
    siblings
}

pub fn samples_merkle_root_from_leaf_and_siblings(
    leaf_hash: [u8; HASH_BYTES_LEN],
    siblings: &[[u8; HASH_BYTES_LEN]],
    bucket_index: usize,
) -> [u8; HASH_BYTES_LEN] {
    // Convert siblings to Vec<Digest> and compute path from bucket_index
    let lemma: Vec<Digest> = siblings.iter()
        .map(|&sib| Digest::try_from(&sib[..]).unwrap())
        .collect();

    // Compute path bits from bucket_index (depth inferred from siblings length)
    let mut path = Vec::new();
    let mut idx = bucket_index;
    for _ in 0..siblings.len() {
        path.push((idx & 1) == 0); // true if left, false if right
        idx >>= 1;
    }

    // Create proof from lemma and path
    let inner_proof = proof::Proof::new(lemma, path);
    let proof: Proof<Digest> = inner_proof.into();

    // Compute root from leaf and proof
    let leaf_digest = Digest::try_from(&leaf_hash[..]).unwrap();
    proof.verified_root(&leaf_digest).unwrap_or([0u8; HASH_BYTES_LEN].into()).into()
}

/// Verify and compute batch-level per-source chain integrity inside ZK proof.
/// This function is called inside the guest, so any assertion failure
/// will cause the proof to fail - providing cryptographic guarantees.
///
/// Chain hash formula (matches data_source and kafka_producer):
///   batch_hash = SHA256(TAG_SHARD_CHAIN || chain_prev || events_commit)
///   events_commit = SHA256(key_id || value || ts for each event in batch)
///
/// Security model:
/// 1. chain_prev comes from aggregator's stored state (trusted), NOT from producer
/// 2. batch_hash is RECOMPUTED from events and VERIFIED against producer's claim
/// 3. Cross-epoch continuity uses aggregator's stored tips
///
/// This prevents a malicious producer from:
/// - Sending fake batch_hash values
/// - Claiming wrong chain_prev values
/// - Including events that don't match the claimed hash
///
/// Arguments:
/// - batches: Batch inputs with events and producer's claimed batch_hash
/// - prev_source_chain_tips: Previous epoch's final chain tips per source_id (from aggregator)
///   Format: (source_id, last_processed_seq, chain_tip)
///
/// Returns: HashMap of source_id -> (last_processed_seq, final_chain_tip) after verification
fn verify_and_compute_chain(
    batches: &[BatchInput],
    prev_source_chain_tips: &[(u32, u64, [u8; HASH_BYTES_LEN])],
) -> HashMap<u32, (u64, [u8; HASH_BYTES_LEN])> {
    // Initialize chain state from aggregator's stored state: (last_seq, tip)
    let mut chain_state: HashMap<u32, (u64, [u8; HASH_BYTES_LEN])> = prev_source_chain_tips
        .iter()
        .map(|(source_id, seq, tip)| (*source_id, (*seq, *tip)))
        .collect();

    for batch in batches {
        let source_id = batch.source_id;
        let source_batch_seq = batch.source_batch_seq;

        // Determine expected chain_prev based on source_batch_seq and stored state
        let chain_prev = if source_batch_seq == 0 {
            // seq=0: chain always starts with [0;32] (producer fresh start)
            [0u8; HASH_BYTES_LEN]
        } else if let Some(&(last_seq, stored_tip)) = chain_state.get(&source_id) {
            // Validate sequence continuity: incoming seq must be last_seq + 1
            if source_batch_seq != last_seq + 1 {
                panic!(
                    "batch sequence gap for source_id={}: expected seq={} but got seq={}\n\
                     (last_seq={}, stored_tip={:?})",
                    source_id, last_seq + 1, source_batch_seq,
                    last_seq, stored_tip
                );
            }
            // Use stored chain tip from previous batch
            stored_tip
        } else {
            // First batch from this source in this epoch, seq must be 0
            if source_batch_seq != 0 {
                panic!(
                    "first batch from source_id={} has seq={} but expected seq=0 (no prior state)",
                    source_id, source_batch_seq
                );
            }
            [0u8; HASH_BYTES_LEN]
        };

        // Compute events_commit separately for debugging
        let ev_commit = events_commit(&batch.events);

        // RECOMPUTE batch_hash from events
        let computed_hash = batch_chain_hash(chain_prev, &batch.events);

        // VERIFY producer's hash matches recomputed hash
        if computed_hash != batch.sent_batch_hash {
            // Build detailed debug message for hash mismatch
            let mut msg = format!(
                "batch_hash mismatch for source_id={} source_batch_seq={}\n\
                 chain_prev={:?}\n\
                 events_commit={:?}\n\
                 TAG_SHARD_CHAIN={:?}\n\
                 num_events={}\n",
                source_id, source_batch_seq, chain_prev, ev_commit, TAG_SHARD_CHAIN, batch.events.len()
            );
            // Log first few events for debugging
            for (i, ev) in batch.events.iter().take(3).enumerate() {
                msg.push_str(&format!(
                    "event[{}]: key_id={:?} value={} ts={}\n",
                    i, ev.key_id, ev.value, ev.ts
                ));
            }
            if batch.events.len() > 3 {
                msg.push_str(&format!("... and {} more events\n", batch.events.len() - 3));
            }
            msg.push_str(&format!(
                "producer_hash={:?}\ncomputed_hash={:?}",
                batch.sent_batch_hash, computed_hash
            ));
            panic!("{}", msg);
        }

        // Update chain state (seq, tip) for next batch
        chain_state.insert(source_id, (source_batch_seq, computed_hash));
    }

    chain_state
}

pub fn process_samples_aggr(input: &SamplesAggrInput) -> SamplesAggrOutput {
    // Chain verification now happens inside process_samples_aggr_with_state
    let (_, out) = process_samples_aggr_with_state(input);
    out
}

pub fn process_samples_aggr_with_state(
    input: &SamplesAggrInput,
) -> (SamplesState, SamplesAggrOutput) {
    let mut st = SamplesState::new(input.prev_chain_hash);

    // Verify and compute chain from batches:
    // 1. Recompute batch_hash from events for each batch
    // 2. Verify it matches producer's claimed sent_batch_hash
    // 3. Use aggregator's stored prev_source_chain_tips as chain_prev (trusted)
    let source_chain_tips = if !input.batches.is_empty() {
        Some(verify_and_compute_chain(
            &input.batches,
            &input.prev_source_chain_tips,
        ))
    } else {
        None
    };

    // Get the final source chain tip (for single-source scenarios, use source_id=0)
    // Extract just the hash, not the seq, for key_chain_tip
    let final_source_chain_tip = source_chain_tips
        .as_ref()
        .and_then(|tips| tips.get(&0).map(|(_seq, tip)| *tip))
        .unwrap_or([0u8; HASH_BYTES_LEN]);

    // Flatten batches to events for processing
    let mut all_events: Vec<Event> = Vec::new();
    for batch in &input.batches {
        all_events.extend(batch.events.iter().cloned());
    }

    for ev in &all_events {
        st.total_count = st.total_count.saturating_add(1);
        st.total_sum = st.total_sum.saturating_add(ev.value as u64);

        st.entries.entry(ev.key_id)
            .and_modify(|entry| {
                // Only update sum and count; chain_tip set from source chain
                entry.sum = entry.sum.saturating_add(ev.value as u64);
                entry.count = entry.count.saturating_add(1);
            })
            .or_insert_with(|| {
                // Use source chain tip for all keys in this source
                BucketEntry {
                    occupied: 1,
                    key_id: ev.key_id,
                    key_chain_tip: final_source_chain_tip,
                    sum: ev.value as u64,
                    count: 1,
                }
            });
    }

    // Convert chain tips to sorted Vec for deterministic output
    let final_source_chain_tips = source_chain_tips
        .map(chain_tips_to_sorted_vec)
        .unwrap_or_default();

    let buckets_root = samples_buckets_root(&st);

    // Compute state commitment for the epoch's state
    let state_commit = samples_state_commit(&st);

    // Build epoch chain link: final_chain_hash = hash(TAG || prev || state_commit)
    let epoch_chain_link = EpochChainLink::new(input.prev_chain_hash, state_commit);

    let out = SamplesAggrOutput {
        epoch_chain_link,
        state_commit,
        buckets_root,
        n_events: all_events.len() as u64,
        final_source_chain_tips,
    };
    (st, out)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramAggrInput {
    pub prev_chain_hash: [u8; HASH_BYTES_LEN],
    /// Batches with events grouped (not flattened) for per-batch hash verification.
    pub batches: Vec<BatchInput>,
    /// Previous epoch's final chain tips per source_id for cross-epoch verification.
    /// These come from the aggregator's stored state (trusted).
    /// Format: Vec<(source_id, last_processed_seq, chain_tip)>
    #[serde(default)]
    pub prev_source_chain_tips: Vec<(u32, u64, [u8; HASH_BYTES_LEN])>,
}

/// Per-key histogram data: stores the histogram bucket counts for a single key
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyHistogram {
    pub key_id: [u8; KEY_BYTES_LEN],
    /// Per-bucket counts (u32 to save memory; sufficient for most use cases)
    pub bucket_counts: [u32; HISTOGRAM_SLOTS],
    /// Event count for this key (u32 sufficient per-epoch; cast to u64 for cross-epoch)
    pub count: u32,
    /// Sum of values for this key (u64 to handle large sums)
    pub sum: u64,
}

impl KeyHistogram {
    pub fn new(key_id: [u8; KEY_BYTES_LEN]) -> Self {
        Self {
            key_id,
            bucket_counts: [0u32; HISTOGRAM_SLOTS],
            count: 0,
            sum: 0,
        }
    }
}

/// Private state for histogram aggregation (not included in public journal).
/// The host stores this privately for query guests to verify against state_commit.
/// Each epoch is processed independently — there is NO incremental accumulation.
/// Global bucket_counts is NOT stored - it's derivable by summing per_key_histograms.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramState {
    pub total_count: u64,
    pub total_sum: u64,
    /// Per-key histograms (sorted by key_id for determinism)
    pub per_key_histograms: Vec<KeyHistogram>,
}

/// Type alias for histogram epoch state (same as HistogramState).
/// Used for clarity in querier input types.
pub type HistogramEpochState = HistogramState;

/// Compute state commitment from HistogramEpochState.
/// Same as histogram_state_commit but named consistently with other epoch state types.
pub fn histogram_epoch_state_commit(state: &HistogramEpochState) -> [u8; HASH_BYTES_LEN] {
    histogram_state_commit(state)
}

/// Public journal output for histogram aggregation (only commitments)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistogramAggrOutput {
    /// The epoch chain link containing prev_chain_hash, state_commit, and final_chain_hash.
    /// final_chain_hash = hash(TAG_EPOCH_CHAIN || prev_chain_hash || state_commit)
    pub epoch_chain_link: EpochChainLink,
    /// Commitment to the epoch's computed state (hash over full HistogramState).
    /// Also present inside epoch_chain_link, but kept at top level for convenience.
    /// The host stores the full state privately; query guests verify it matches this commit.
    pub state_commit: [u8; HASH_BYTES_LEN],
    /// Merkle root of per-key histograms
    pub buckets_root: [u8; HASH_BYTES_LEN],
    /// Number of events processed in this epoch
    pub n_events: u64,
    /// Per-source final chain tips committed in the journal.
    /// These are verified chain tips that can be used for cross-epoch continuity.
    /// Format: Vec<(source_id, last_processed_seq, chain_tip)> sorted by source_id for determinism.
    pub final_source_chain_tips: Vec<(u32, u64, [u8; HASH_BYTES_LEN])>,
}

pub fn histogram_bucket_index(value: u64) -> usize {
    if value == 0 {
        return 0;
    }
    63usize.saturating_sub(value.leading_zeros() as usize)
}

const TAG_HIST_KEY_LEAF: &[u8] = b"ZKTLM_HIST_KEY_LEAF_V1";

/// Compute leaf hash for a single key's histogram
fn hist_key_leaf_hash(key_id: &[u8; KEY_BYTES_LEN], histogram: &KeyHistogram) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_HIST_KEY_LEAF);
    sha.update(key_id);
    sha.update(&histogram.count.to_be_bytes());
    sha.update(&histogram.sum.to_be_bytes());
    for &count in histogram.bucket_counts.iter() {
        sha.update(&count.to_be_bytes());
    }
    sha.finalize().into()
}

/// Compute Merkle root for per-key histograms
fn hist_per_key_root(per_key_histograms: &HashMap<[u8; KEY_BYTES_LEN], KeyHistogram>) -> [u8; HASH_BYTES_LEN] {
    if per_key_histograms.is_empty() {
        // Return zero hash for empty histogram map
        return [0u8; HASH_BYTES_LEN];
    }

    // Sort keys for deterministic ordering
    let mut keys: Vec<&[u8; KEY_BYTES_LEN]> = per_key_histograms.keys().collect();
    keys.sort();

    // Compute leaf hashes for each key's histogram
    let leaf_hashes: Vec<Digest> = keys
        .iter()
        .map(|&key_id| {
            let histogram = per_key_histograms.get(key_id).unwrap();
            let hash = hist_key_leaf_hash(key_id, histogram);
            Digest::try_from(&hash[..]).unwrap()
        })
        .collect();

    // Pad to next power of 2 if needed
    let num_leaves = next_pow2_ge(leaf_hashes.len());
    let mut padded_leaves = leaf_hashes;
    let zero_hash = [0u8; HASH_BYTES_LEN];
    while padded_leaves.len() < num_leaves {
        padded_leaves.push(Digest::try_from(&zero_hash[..]).unwrap());
    }

    // Build Merkle tree and return root
    let tree = MerkleTree::new(padded_leaves);
    tree.root().into()
}

/// Compute a deterministic hash over the full HistogramState.
/// This captures all state needed for cross-epoch verification:
/// - total_count, total_sum
/// - per_key_histograms (via their sorted key_id order and per-key bucket_counts)
///
/// Note: Global bucket_counts is NOT included in the commitment - it's derivable by
/// summing per-key bucket_counts across all KeyHistogram entries.
///
/// The host stores the full state privately; only this hash is public in the journal.
/// On the next epoch, the guest verifies that the provided prev_state hashes to prev_state_commit.
pub fn histogram_state_commit(state: &HistogramState) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_HIST_STATE_COMMIT);
    sha.update(&state.total_count.to_be_bytes());
    sha.update(&state.total_sum.to_be_bytes());

    // Hash per-key histograms in sorted order by key_id for determinism
    let mut sorted_histograms = state.per_key_histograms.clone();
    sorted_histograms.sort_by(|a, b| a.key_id.cmp(&b.key_id));
    for hist in &sorted_histograms {
        sha.update(&hist.key_id);
        sha.update(&hist.count.to_be_bytes());
        sha.update(&hist.sum.to_be_bytes());
        for &count in hist.bucket_counts.iter() {
            sha.update(&count.to_be_bytes());
        }
    }

    sha.finalize().into()
}

pub fn process_histogram_aggr(input: &HistogramAggrInput) -> HistogramAggrOutput {
    let (_, out) = process_histogram_aggr_with_state(input);
    out
}

pub fn process_histogram_aggr_with_state(
    input: &HistogramAggrInput,
) -> (HistogramState, HistogramAggrOutput) {
    // Each epoch is processed independently — NO incremental accumulation across epochs.
    // The aggregator PRODUCES commitments, the query guest CONSUMES and VERIFIES them.
    let mut total_count = 0u64;
    let mut total_sum = 0u64;
    let mut per_key_histograms: HashMap<[u8; KEY_BYTES_LEN], KeyHistogram> = HashMap::new();

    // Verify and compute chain from batches:
    // 1. Recompute batch_hash from events for each batch
    // 2. Verify it matches producer's claimed sent_batch_hash
    // 3. Use aggregator's stored prev_source_chain_tips as chain_prev (trusted)
    let source_chain_tips = if !input.batches.is_empty() {
        Some(verify_and_compute_chain(
            &input.batches,
            &input.prev_source_chain_tips,
        ))
    } else {
        None
    };

    // Convert chain tips to sorted Vec for deterministic output
    let final_source_chain_tips = source_chain_tips
        .map(chain_tips_to_sorted_vec)
        .unwrap_or_default();

    // Flatten batches to events for processing
    let mut all_events: Vec<Event> = Vec::new();
    for batch in &input.batches {
        all_events.extend(batch.events.iter().cloned());
    }

    // Process events for this epoch
    for ev in &all_events {
        let value = ev.value as u64;
        total_count = total_count.saturating_add(1);
        total_sum = total_sum.saturating_add(value);

        let bucket_idx = histogram_bucket_index(value);

        // Update per-key histogram
        per_key_histograms
            .entry(ev.key_id)
            .and_modify(|hist| {
                hist.count = hist.count.saturating_add(1);
                hist.sum = hist.sum.saturating_add(value);
                hist.bucket_counts[bucket_idx] = hist.bucket_counts[bucket_idx].saturating_add(1);
            })
            .or_insert_with(|| {
                let mut hist = KeyHistogram::new(ev.key_id);
                hist.count = 1;
                hist.sum = value;
                hist.bucket_counts[bucket_idx] = 1;
                hist
            });
    }

    // Compute Merkle root from per-key histograms (new structure)
    let buckets_root = hist_per_key_root(&per_key_histograms);

    // Convert HashMap to sorted Vec for deterministic serialization
    let mut per_key_histograms_vec: Vec<KeyHistogram> = per_key_histograms.into_values().collect();
    per_key_histograms_vec.sort_by(|a, b| a.key_id.cmp(&b.key_id));

    // Build final state (global bucket_counts not stored - derivable from per_key_histograms)
    let state = HistogramState {
        total_count,
        total_sum,
        per_key_histograms: per_key_histograms_vec,
    };

    // Compute state commitment for the final state
    let state_commit = histogram_state_commit(&state);

    // Build epoch chain link: final_chain_hash = hash(TAG || prev || state_commit)
    let epoch_chain_link = EpochChainLink::new(input.prev_chain_hash, state_commit);

    let out = HistogramAggrOutput {
        epoch_chain_link,
        state_commit,
        buckets_root,
        n_events: all_events.len() as u64,
        final_source_chain_tips,
    };

    (state, out)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmAggrInput {
    pub prev_chain_hash: [u8; HASH_BYTES_LEN],
    /// Batches with events grouped (not flattened) for per-batch hash verification.
    pub batches: Vec<BatchInput>,
    /// Previous epoch's final chain tips per source_id for cross-epoch verification.
    /// These come from the aggregator's stored state (trusted).
    /// Format: Vec<(source_id, last_processed_seq, chain_tip)>
    #[serde(default)]
    pub prev_source_chain_tips: Vec<(u32, u64, [u8; HASH_BYTES_LEN])>,
}

/// Private state for Count-Min sketch aggregation (not included in public journal).
/// This struct uses Box for efficiency during aggregation but isn't serializable.
/// Uses u32 for count entries to save memory (sufficient for most use cases).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CmState {
    pub table: Box<[[u32; CM_COLS]; CM_ROWS]>,
    pub heap_keys: [[u8; KEY_BYTES_LEN]; CM_TOPK_SLOTS],
    pub heap_vals: [u64; CM_TOPK_SLOTS],
    pub heap_occ: [u8; CM_TOPK_SLOTS],
    pub total_sum: u64,
}

/// Serializable epoch state for Count-Min sketch.
/// Used for host storage and querier input. Converts from CmState.
/// Uses u32 for count entries to match CmState.
/// Note: This actually implements a sorted linked list because the data structure is small
/// in all benchmarks and it's kind of hard to serialize the heap state in between executions,
/// so it doesn't save a lot of work.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmEpochState {
    /// Flattened CM table: counts[r * CM_COLS + c] = table[r][c]
    pub counts: Vec<u32>,
    /// Heap keys for top-k tracking
    pub heap_keys: Vec<[u8; KEY_BYTES_LEN]>,
    /// Heap values (scores) for top-k tracking
    pub heap_vals: Vec<u64>,
    /// Heap occupancy flags
    pub heap_occ: Vec<u8>,
    /// Total sum of all values
    pub total_sum: u64,
}

impl From<&CmState> for CmEpochState {
    fn from(state: &CmState) -> Self {
        let mut counts = Vec::with_capacity(CM_ROWS * CM_COLS);
        for r in 0..CM_ROWS {
            for c in 0..CM_COLS {
                counts.push(state.table[r][c]);
            }
        }
        Self {
            counts,
            heap_keys: state.heap_keys.to_vec(),
            heap_vals: state.heap_vals.to_vec(),
            heap_occ: state.heap_occ.to_vec(),
            total_sum: state.total_sum,
        }
    }
}

/// Compute state commitment from CmEpochState.
/// Produces the same hash as cm_state_commit(CmState).
pub fn cm_epoch_state_commit(state: &CmEpochState) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_CM_STATE_COMMIT);
    sha.update(&state.total_sum.to_be_bytes());

    // Hash CM table (row by row, column by column) - u32 values
    for r in 0..CM_ROWS {
        for c in 0..CM_COLS {
            sha.update(&state.counts[r * CM_COLS + c].to_be_bytes());
        }
    }

    // Hash heap entries
    for i in 0..CM_TOPK_SLOTS {
        sha.update(&state.heap_keys[i]);
        sha.update(&state.heap_vals[i].to_be_bytes());
        sha.update(&[state.heap_occ[i]]);
    }

    sha.finalize().into()
}

/// Public journal output for Count-Min sketch aggregation (only commitments)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CmAggrOutput {
    /// The epoch chain link containing prev_chain_hash, state_commit, and final_chain_hash.
    /// final_chain_hash = hash(TAG_EPOCH_CHAIN || prev_chain_hash || state_commit)
    pub epoch_chain_link: EpochChainLink,
    /// Commitment to the epoch's computed state (hash over full CmState).
    /// Also present inside epoch_chain_link, but kept at top level for convenience.
    /// The host stores the full state privately; query guests verify it matches this commit.
    pub state_commit: [u8; HASH_BYTES_LEN],
    /// Merkle roots for each CM row
    pub row_roots: [[u8; HASH_BYTES_LEN]; CM_ROWS],
    /// Combined Merkle root of all rows
    pub cm_root: [u8; HASH_BYTES_LEN],
    /// Merkle root of top-k heap
    pub heap_root: [u8; HASH_BYTES_LEN],
    /// Number of events processed in this epoch
    pub n_events: u64,
    /// Per-source final chain tips committed in the journal.
    /// These are verified chain tips that can be used for cross-epoch continuity.
    /// Format: Vec<(source_id, last_processed_seq, chain_tip)> sorted by source_id for determinism.
    pub final_source_chain_tips: Vec<(u32, u64, [u8; HASH_BYTES_LEN])>,
}

pub fn cm_bucket_index(key_id: &[u8; KEY_BYTES_LEN], row: usize) -> u16 {
    let seed = CM_SEEDS[row];
    let mut x = key_to_u64(key_id) ^ (((seed as u64) << 32) | seed as u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    ((x as usize) % CM_COLS) as u16
}

fn cm_leaf_hash(row: u8, col: u16, count: u32) -> [u8; HASH_BYTES_LEN] {
    sha256_bytes(&[
        TAG_CM_LEAF,
        &[row],
        &col.to_be_bytes(),
        &count.to_be_bytes(),
    ])
}

fn cm_row_root(row: u8, counts: &[u32]) -> [u8; HASH_BYTES_LEN] {
    let num_cols = counts.len();
    // Compute leaf hashes
    let leaf_hashes: Vec<Digest> = (0..num_cols)
        .map(|col| {
            let hash = cm_leaf_hash(row, col as u16, counts[col]);
            Digest::try_from(&hash[..]).unwrap()
        })
        .collect();

    // Build Merkle tree and return root
    let tree = MerkleTree::new(leaf_hashes);
    tree.root().into()
}

fn cm_estimate_count(table: &[[u32; CM_COLS]; CM_ROWS], key_id: &[u8; KEY_BYTES_LEN]) -> u64 {
    let mut est = u64::MAX;
    for r in 0..CM_ROWS {
        let idx = cm_bucket_index(key_id, r) as usize;
        est = est.min(table[r][idx] as u64);
    }
    if est == u64::MAX {
        0
    } else {
        est
    }
}

fn cm_heap_update(
    update_key: &[u8; KEY_BYTES_LEN],
    score: u64,
    heap_keys: &mut [[u8; KEY_BYTES_LEN]; CM_TOPK_SLOTS],
    heap_vals: &mut [u64; CM_TOPK_SLOTS],
    heap_occ: &mut [u8; CM_TOPK_SLOTS],
) -> Option<usize> {
    for i in 0..CM_TOPK_SLOTS {
        if heap_occ[i] == 1 && heap_keys[i] == *update_key {
            if score > heap_vals[i] {
                heap_vals[i] = score;
            }
            return Some(i);
        }
    }
    for i in 0..CM_TOPK_SLOTS {
        if heap_occ[i] == 0 {
            heap_keys[i] = *update_key;
            heap_vals[i] = score;
            heap_occ[i] = 1;
            return Some(i);
        }
    }
    let mut worst_idx = 0usize;
    let mut worst_val = heap_vals[0];
    for i in 1..CM_TOPK_SLOTS {
        if heap_vals[i] < worst_val {
            worst_val = heap_vals[i];
            worst_idx = i;
        }
    }
    if score > worst_val {
        heap_keys[worst_idx] = *update_key;
        heap_vals[worst_idx] = score;
        heap_occ[worst_idx] = 1;
        return Some(worst_idx);
    }
    None
}

fn cm_heap_leaf_hash(slot_idx: u16, key: &[u8; KEY_BYTES_LEN], val: u64, occ: u8) -> [u8; HASH_BYTES_LEN] {
    sha256_bytes(&[
        TAG_CM_HEAP_LEAF,
        &slot_idx.to_be_bytes(),
        key,
        &val.to_be_bytes(),
        &[occ],
    ])
}

fn cm_heap_root(
    heap_keys: &[[u8; KEY_BYTES_LEN]],
    heap_vals: &[u64],
    heap_occ: &[u8],
) -> [u8; HASH_BYTES_LEN] {
    debug_assert_eq!(heap_keys.len(), heap_vals.len());
    debug_assert_eq!(heap_keys.len(), heap_occ.len());

    let actual_slots = heap_keys.len();
    let num_leaves = next_pow2_ge(actual_slots);  // Dynamic padding to pow2
    let zero_key = [0u8; KEY_BYTES_LEN];

    // Compute leaf hashes (padding to next power of 2)
    let leaf_hashes: Vec<Digest> = (0..num_leaves)
        .map(|i| {
            let hash = if i < actual_slots {
                cm_heap_leaf_hash(i as u16, &heap_keys[i], heap_vals[i], heap_occ[i])
            } else {
                cm_heap_leaf_hash(i as u16, &zero_key, 0, 0)
            };
            Digest::try_from(&hash[..]).unwrap()
        })
        .collect();

    // Build Merkle tree and return root
    let tree = MerkleTree::new(leaf_hashes);
    tree.root().into()
}

/// Compute a deterministic hash over the full CmState.
/// This captures all state needed for query verification:
/// - table (CM counts), heap_keys, heap_vals, heap_occ, total_sum
///
/// The host stores the full state privately; only this hash is public in the journal.
/// Query guests verify that the host-provided epoch data hashes to this commit.
pub fn cm_state_commit(state: &CmState) -> [u8; HASH_BYTES_LEN] {
    let mut sha = Sha256::new();
    sha.update(TAG_CM_STATE_COMMIT);
    sha.update(&state.total_sum.to_be_bytes());

    // Hash CM table (row by row, column by column) - u32 values
    for r in 0..CM_ROWS {
        for c in 0..CM_COLS {
            sha.update(&state.table[r][c].to_be_bytes());
        }
    }

    // Hash heap entries
    for i in 0..CM_TOPK_SLOTS {
        sha.update(&state.heap_keys[i]);
        sha.update(&state.heap_vals[i].to_be_bytes());
        sha.update(&[state.heap_occ[i]]);
    }

    sha.finalize().into()
}

pub fn process_cm_aggr(input: &CmAggrInput) -> CmAggrOutput {
    let (_, out) = process_cm_aggr_with_state(input);
    out
}

pub fn process_cm_aggr_with_state(input: &CmAggrInput) -> (CmState, CmAggrOutput) {
    // Verify and compute chain from batches:
    // 1. Recompute batch_hash from events for each batch
    // 2. Verify it matches producer's claimed sent_batch_hash
    // 3. Use aggregator's stored prev_source_chain_tips as chain_prev (trusted)
    let source_chain_tips = if !input.batches.is_empty() {
        Some(verify_and_compute_chain(
            &input.batches,
            &input.prev_source_chain_tips,
        ))
    } else {
        None
    };

    // Convert chain tips to sorted Vec for deterministic output
    let final_source_chain_tips = source_chain_tips
        .map(chain_tips_to_sorted_vec)
        .unwrap_or_default();

    // Flatten batches to events for processing
    let mut all_events: Vec<Event> = Vec::new();
    for batch in &input.batches {
        all_events.extend(batch.events.iter().cloned());
    }

    let mut total_sum = 0u64;
    let mut table: Box<[[u32; CM_COLS]; CM_ROWS]> = Box::new([[0u32; CM_COLS]; CM_ROWS]);
    let mut heap_keys = [[0u8; KEY_BYTES_LEN]; CM_TOPK_SLOTS];
    let mut heap_vals = [0u64; CM_TOPK_SLOTS];
    let mut heap_occ = [0u8; CM_TOPK_SLOTS];

    for ev in &all_events {
        total_sum = total_sum.saturating_add(ev.value as u64);
        for r in 0..CM_ROWS {
            let idx = cm_bucket_index(&ev.key_id, r) as usize;
            table[r][idx] = table[r][idx].saturating_add(ev.value as u32);
        }
        let score = cm_estimate_count(&table, &ev.key_id);
        let _ = cm_heap_update(
            &ev.key_id,
            score,
            &mut heap_keys,
            &mut heap_vals,
            &mut heap_occ,
        );
    }

    let mut row_roots = [[0u8; HASH_BYTES_LEN]; CM_ROWS];
    for r in 0..CM_ROWS {
        row_roots[r] = cm_row_root(r as u8, &table[r]);
    }

    // Build Merkle tree from row roots (will pad to next power of 2)
    let row_root_digests: Vec<Digest> = row_roots.iter()
        .map(|&root| Digest::try_from(&root[..]).unwrap())
        .collect();
    let row_tree = MerkleTree::new(row_root_digests);
    let cm_root: [u8; HASH_BYTES_LEN] = row_tree.root().into();

    let heap_root = cm_heap_root(&heap_keys, &heap_vals, &heap_occ);

    let state = CmState {
        table,
        heap_keys,
        heap_vals,
        heap_occ,
        total_sum,
    };

    // Compute state commitment for the epoch's state
    let state_commit = cm_state_commit(&state);

    // Build epoch chain link: final_chain_hash = hash(TAG || prev || state_commit)
    let epoch_chain_link = EpochChainLink::new(input.prev_chain_hash, state_commit);

    let out = CmAggrOutput {
        epoch_chain_link,
        state_commit,
        row_roots,
        cm_root,
        heap_root,
        n_events: all_events.len() as u64,
        final_source_chain_tips,
    };

    (state, out)
}

// ===========================================================================
// Out-commit computation functions
// ===========================================================================

/// Compute samples out_commit from output and state values.
/// This provides a deterministic commitment to the full output fields.
pub fn compute_samples_out_commit(
    buckets_root: &[u8; HASH_BYTES_LEN],
    total_count: u64,
    total_sum: u64,
    n_events: u64,
    final_source_chain_tips: &[(u32, u64, [u8; HASH_BYTES_LEN])],
) -> [u8; HASH_BYTES_LEN] {
    let chain_tips_commit = source_chain_tips_commit(final_source_chain_tips);
    sha256_bytes(&[
        TAG_OUT_COMMIT_SAMPLES,
        buckets_root,
        &total_count.to_be_bytes(),
        &total_sum.to_be_bytes(),
        &n_events.to_be_bytes(),
        &chain_tips_commit,
    ])
}

/// Compute histogram out_commit from output and state values.
/// This provides a deterministic commitment to the full output fields.
pub fn compute_histogram_out_commit(
    buckets_root: &[u8; HASH_BYTES_LEN],
    total_count: u64,
    total_sum: u64,
    n_events: u64,
    final_source_chain_tips: &[(u32, u64, [u8; HASH_BYTES_LEN])],
) -> [u8; HASH_BYTES_LEN] {
    let chain_tips_commit = source_chain_tips_commit(final_source_chain_tips);
    sha256_bytes(&[
        TAG_OUT_COMMIT_HISTOGRAM,
        buckets_root,
        &total_count.to_be_bytes(),
        &total_sum.to_be_bytes(),
        &n_events.to_be_bytes(),
        &chain_tips_commit,
    ])
}

/// Compute cm out_commit from output and state values.
/// This provides a deterministic commitment to the full output fields.
pub fn compute_cm_out_commit(
    cm_root: &[u8; HASH_BYTES_LEN],
    heap_root: &[u8; HASH_BYTES_LEN],
    total_sum: u64,
    n_events: u64,
    final_source_chain_tips: &[(u32, u64, [u8; HASH_BYTES_LEN])],
) -> [u8; HASH_BYTES_LEN] {
    let chain_tips_commit = source_chain_tips_commit(final_source_chain_tips);
    sha256_bytes(&[
        TAG_OUT_COMMIT_CM,
        cm_root,
        heap_root,
        &total_sum.to_be_bytes(),
        &n_events.to_be_bytes(),
        &chain_tips_commit,
    ])
}
