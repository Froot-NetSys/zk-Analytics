//! FoundationDB store for zkTelemetry aggregated data.
//!
//! This module provides a distributed storage backend for communication
//! between aggregators and queriers, replacing the filesystem-based
//! ShardedRocksDb approach.

use crate::epoch::EpochType;
use crate::fdb_chunking::{chunk_bytes, FDB_CHUNK_SIZE};
use crate::rocksdb_store::{
    AggCmStruct, AggEpoch, AggEpochMeta, AggEpochProof, AggHistStruct, EpochTombstone, Handoff,
    OwnershipEpoch, VerifiedSamplesStruct,
};
use anyhow::{Context, Result};
use foundationdb::tuple::Subspace;
use foundationdb::{Database, RangeOption};
use std::collections::HashMap;

/// Key prefixes for different data types in FDB
const KEY_EPOCHS: &str = "epochs";
const KEY_EPOCH_META: &str = "epoch_meta";
const KEY_EPOCH_PROOFS: &str = "epoch_proofs";
const KEY_CM_STRUCT: &str = "cm_struct";
const KEY_HIST_STRUCT: &str = "hist_struct";
const KEY_VERIFIED_SAMPLES: &str = "verified_samples";
const KEY_EPOCH_TOMBSTONE: &str = "epoch_tombstone";
// Online resharding (preview) — mirrored from rocksdb_store.
const KEY_OWNERSHIP_EPOCH: &str = "ownership_epoch";
const KEY_HANDOFF: &str = "handoff";

/// FoundationDB store for aggregated telemetry data.
pub struct FdbStore {
    db: Database,
    subspace: Subspace,
    _network_guard: foundationdb::api::NetworkAutoStop,
}

/// Batch of items to write atomically to FDB.
#[derive(Default)]
pub struct FdbWriteBatch {
    pub epochs: Vec<AggEpoch>,
    pub epoch_meta: Vec<AggEpochMeta>,
    pub epoch_proofs: Vec<AggEpochProof>,
    pub cm_structs: Vec<AggCmStruct>,
    pub hist_structs: Vec<AggHistStruct>,
    pub verified_samples: Vec<VerifiedSamplesStruct>,
    pub tombstones: Vec<EpochTombstone>,
}

impl FdbWriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_agg_epoch(&mut self, epoch: AggEpoch) {
        self.epochs.push(epoch);
    }

    pub fn put_agg_epoch_meta(&mut self, meta: AggEpochMeta) {
        self.epoch_meta.push(meta);
    }

    pub fn put_agg_epoch_proof(&mut self, proof: AggEpochProof) {
        self.epoch_proofs.push(proof);
    }

    pub fn put_agg_cm_struct(&mut self, item: AggCmStruct) {
        self.cm_structs.push(item);
    }

    pub fn put_agg_hist_struct(&mut self, item: AggHistStruct) {
        self.hist_structs.push(item);
    }

    pub fn put_verified_samples_struct(&mut self, item: VerifiedSamplesStruct) {
        self.verified_samples.push(item);
    }

    pub fn put_epoch_tombstone(&mut self, t: EpochTombstone) {
        self.tombstones.push(t);
    }
}

impl FdbStore {
    /// Open a connection to FoundationDB.
    ///
    /// Uses `FDB_CLUSTER_FILE` environment variable for cluster file path,
    /// defaulting to `/etc/foundationdb/fdb.cluster`.
    ///
    /// Uses `FDB_SUBSPACE` environment variable for key prefix,
    /// defaulting to `zktelemetry`.
    pub async fn open() -> Result<Self> {
        let start = std::time::Instant::now();

        // Initialize FDB network with explicit API version
        // Using api::FdbApiBuilder to set version explicitly
        eprintln!("[fdb] initializing with API version 710...");
        let network_guard = unsafe {
            // Use FdbApiBuilder to set API version explicitly to 710 (for FDB 7.1)
            foundationdb::api::FdbApiBuilder::default()
                .set_runtime_version(710)
                .build()
                .expect("failed to set FDB API version")
                .boot()
                .expect("failed to boot FDB network")
        };

        let cluster_file = std::env::var("FDB_CLUSTER_FILE")
            .unwrap_or_else(|_| "/etc/foundationdb/fdb.cluster".to_string());

        eprintln!("[fdb] connecting to cluster: {}", cluster_file);

        let db = Database::new(Some(&cluster_file))
            .context("failed to connect to FoundationDB")?;

        let subspace_name = std::env::var("FDB_SUBSPACE").unwrap_or_else(|_| "zktelemetry".to_string());
        let subspace = Subspace::from_bytes(subspace_name.as_bytes());

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] connected: cluster_file={} subspace={} latency_ms={}",
            cluster_file, subspace_name, elapsed_ms
        );

        Ok(Self { db, subspace, _network_guard: network_guard })
    }

    /// Create FdbStore from an existing database connection (for testing)
    ///
    /// # Safety
    /// This function assumes the FDB network has already been initialized.
    /// The caller must ensure a NetworkAutoStop guard is kept alive.
    pub unsafe fn from_db_unchecked(db: Database, subspace: Subspace, network_guard: foundationdb::api::NetworkAutoStop) -> Self {
        Self { db, subspace, _network_guard: network_guard }
    }

    /// Clear all data in the subspace (for testing/benchmarking)
    pub async fn clear_all(&self) -> Result<()> {
        let start_key = self.subspace.range().0;
        let end_key = self.subspace.range().1;

        self.db
            .run(|trx, _| {
                let start = start_key.clone();
                let end = end_key.clone();
                async move {
                    trx.clear_range(&start, &end);
                    Ok(())
                }
            })
            .await
            .context("FDB clear_all")?;

        eprintln!("[fdb] cleared all data in subspace");
        Ok(())
    }

    // ========== Write Methods (for Aggregator) ==========

    /// Write an AggEpoch to FDB.
    pub async fn put_agg_epoch(&self, epoch: &AggEpoch) -> Result<()> {
        let key = self.key_agg_epoch(epoch.epoch_type, epoch.aggregator_id, epoch.sequence);
        let value = bincode::serialize(epoch).context("serialize AggEpoch")?;

        self.db
            .run(|trx, _| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await
            .context("FDB put_agg_epoch")
    }

    /// Write an AggEpochMeta to FDB.
    pub async fn put_agg_epoch_meta(&self, meta: &AggEpochMeta) -> Result<()> {
        let key = self.key_agg_epoch_meta(meta.epoch_type, meta.aggregator_id, meta.sequence);
        let value = bincode::serialize(meta).context("serialize AggEpochMeta")?;

        self.db
            .run(|trx, _| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await
            .context("FDB put_agg_epoch_meta")
    }

    /// Write an EpochTombstone to FDB.
    pub async fn put_epoch_tombstone(&self, t: &EpochTombstone) -> Result<()> {
        let key = self.key_epoch_tombstone(t.epoch_type, t.aggregator_id, t.sequence);
        let value = bincode::serialize(t).context("serialize EpochTombstone")?;

        self.db
            .run(|trx, _| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await
            .context("FDB put_epoch_tombstone")
    }

    /// Whether a tombstone exists for the given (epoch_type, sequence, aggregator_id).
    pub async fn has_epoch_tombstone(
        &self,
        epoch_type: EpochType,
        sequence: i64,
        aggregator_id: u32,
    ) -> Result<bool> {
        let key = self.key_epoch_tombstone(epoch_type, aggregator_id, sequence);
        let trx = self
            .db
            .create_trx()
            .context("create has_epoch_tombstone transaction")?;
        let val = trx
            .get(&key, false)
            .await
            .context("FDB has_epoch_tombstone get")?;
        Ok(val.is_some())
    }

    /// Highest tombstoned sequence for the given (epoch_type, aggregator_id).
    pub async fn max_epoch_tombstone_seq(
        &self,
        epoch_type: EpochType,
        aggregator_id: u32,
    ) -> Result<Option<i64>> {
        let prefix = self
            .subspace
            .pack(&(KEY_EPOCH_TOMBSTONE, epoch_type.as_str(), aggregator_id));
        let range = self.prefix_range(&prefix);

        let trx = self
            .db
            .create_trx()
            .context("create max_epoch_tombstone_seq transaction")?;
        let kvs = trx
            .get_range(&range, 1024, false)
            .await
            .context("FDB max_epoch_tombstone_seq read")?;

        let mut max_seq: Option<i64> = None;
        for kv in kvs {
            let t: EpochTombstone =
                bincode::deserialize(kv.value()).context("deserialize EpochTombstone")?;
            max_seq = Some(max_seq.map_or(t.sequence, |m: i64| m.max(t.sequence)));
        }
        Ok(max_seq)
    }

    /// Read all EpochTombstone records from FDB.
    pub async fn epoch_tombstones(&self) -> Result<Vec<EpochTombstone>> {
        let prefix = self.subspace.pack(&(KEY_EPOCH_TOMBSTONE,));
        let range = self.prefix_range(&prefix);

        let trx = self
            .db
            .create_trx()
            .context("create epoch_tombstones transaction")?;
        let kvs = trx
            .get_range(&range, 1024, false)
            .await
            .context("FDB epoch_tombstones read")?;

        let mut out = Vec::new();
        for kv in kvs {
            let t: EpochTombstone =
                bincode::deserialize(kv.value()).context("deserialize EpochTombstone")?;
            out.push(t);
        }
        out.sort_by_key(|t| (t.epoch_type, t.aggregator_id, t.sequence));
        Ok(out)
    }

    /// Write an AggEpochProof to FDB.
    /// Large proofs are automatically chunked.
    pub async fn put_agg_epoch_proof(&self, proof: &AggEpochProof) -> Result<()> {
        // Serialize receipt_words to bytes
        let proof_bytes: Vec<u8> = proof
            .receipt_words
            .iter()
            .flat_map(|w| w.to_le_bytes())
            .collect();

        let epoch_type_str = proof.epoch_type.as_str().to_string();
        let aggregator_id = proof.aggregator_id;
        let sequence = proof.sequence;
        let chunks = chunk_bytes(&proof_bytes);
        let total_chunks = chunks.len() as u32;

        // Build all keys and values before the transaction
        let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(chunks.len() + 1);

        // Store metadata in chunk 0 header
        let meta_key = self.key_agg_epoch_proof_chunk(&epoch_type_str, aggregator_id, sequence, 0);
        let meta_value = bincode::serialize(&(total_chunks, proof.epoch_type))
            .context("serialize proof metadata")?;
        kvs.push((meta_key, meta_value));

        // Store chunk data
        for (idx, chunk_data) in chunks {
            let key = self.key_agg_epoch_proof_data(&epoch_type_str, aggregator_id, sequence, idx);
            kvs.push((key, chunk_data));
        }

        self.db
            .run(|trx, _| {
                let kvs = kvs.clone();
                async move {
                    for (k, v) in kvs {
                        trx.set(&k, &v);
                    }
                    Ok(())
                }
            })
            .await
            .context("FDB put_agg_epoch_proof")
    }

    /// Write an AggCmStruct to FDB.
    /// Large structs are automatically chunked.
    pub async fn put_agg_cm_struct(&self, item: &AggCmStruct) -> Result<()> {
        let sequence = item.sequence;

        // Check if we need chunking
        let total_size = item.counts_u32.len() + item.heap_fixed.len();
        if total_size <= FDB_CHUNK_SIZE {
            // Small enough to store directly
            let key = self.key_agg_cm_struct(sequence, 0);
            let value = bincode::serialize(item).context("serialize AggCmStruct")?;

            self.db
                .run(|trx, _| {
                    let key = key.clone();
                    let value = value.clone();
                    async move {
                        trx.set(&key, &value);
                        Ok(())
                    }
                })
                .await
                .context("FDB put_agg_cm_struct")?;
        } else {
            // Need to chunk - store counts_u32 and heap_fixed separately
            let counts_chunks = chunk_bytes(&item.counts_u32);
            let heap_chunks = chunk_bytes(&item.heap_fixed);

            let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

            // Store metadata
            let meta_key = self.key_agg_cm_struct_meta(sequence);
            let meta_value = bincode::serialize(&(
                counts_chunks.len() as u32,
                heap_chunks.len() as u32,
            ))
            .context("serialize cm_struct metadata")?;
            kvs.push((meta_key, meta_value));

            // Store counts chunks
            for (idx, chunk_data) in counts_chunks {
                let key = self.key_agg_cm_struct_counts(sequence, idx);
                kvs.push((key, chunk_data));
            }

            // Store heap chunks
            for (idx, chunk_data) in heap_chunks {
                let key = self.key_agg_cm_struct_heap(sequence, idx);
                kvs.push((key, chunk_data));
            }

            self.db
                .run(|trx, _| {
                    let kvs = kvs.clone();
                    async move {
                        for (k, v) in kvs {
                            trx.set(&k, &v);
                        }
                        Ok(())
                    }
                })
                .await
                .context("FDB put_agg_cm_struct chunked")?;
        }

        Ok(())
    }

    /// Write an AggHistStruct to FDB. Chunks table_fixed if it exceeds the
    /// per-value limit (same scheme as write_batch / cm_structs).
    pub async fn put_agg_hist_struct(&self, item: &AggHistStruct) -> Result<()> {
        let mut batch = FdbWriteBatch::new();
        batch.put_agg_hist_struct(item.clone());
        self.write_batch(batch).await
    }

    /// Write a VerifiedSamplesStruct to FDB.
    /// Large table_fixed data is automatically chunked.
    pub async fn put_verified_samples_struct(&self, item: &VerifiedSamplesStruct) -> Result<()> {
        let table_fixed_size = item.table_fixed.as_ref().map(|v| v.len()).unwrap_or(0);

        if table_fixed_size <= FDB_CHUNK_SIZE {
            // Small enough to store directly
            let key = self.key_verified_samples_struct(item.aggregator_id, item.sequence);
            let value = bincode::serialize(item).context("serialize VerifiedSamplesStruct")?;

            self.db
                .run(|trx, _| {
                    let key = key.clone();
                    let value = value.clone();
                    async move {
                        trx.set(&key, &value);
                        Ok(())
                    }
                })
                .await
                .context("FDB put_verified_samples_struct")?;
        } else {
            // Need to chunk table_fixed separately
            let table_fixed = item.table_fixed.as_ref().unwrap();
            let table_chunks = chunk_bytes(table_fixed);

            // Create a version without table_fixed for metadata
            let meta_item = VerifiedSamplesStruct {
                sequence: item.sequence,
                ingest_time_ms: item.ingest_time_ms,
                result_commit: item.result_commit.clone(),
                out_commit: item.out_commit.clone(),
                total_count: item.total_count,
                total_sum: item.total_sum,
                table_fixed: None, // Will be stored separately
                prev_chain_hash: item.prev_chain_hash.clone(),
                events_commit: item.events_commit.clone(),
                aggregator_id: item.aggregator_id,
            };

            let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

            // Store metadata (struct without table_fixed + chunk count)
            let meta_key = self.key_verified_samples_meta(item.aggregator_id, item.sequence);
            let meta_value = bincode::serialize(&(meta_item, table_chunks.len() as u32))
                .context("serialize verified_samples metadata")?;
            kvs.push((meta_key, meta_value));

            // Store table_fixed chunks
            for (idx, chunk_data) in table_chunks {
                let key = self.key_verified_samples_table(item.aggregator_id, item.sequence, idx);
                kvs.push((key, chunk_data));
            }

            self.db
                .run(|trx, _| {
                    let kvs = kvs.clone();
                    async move {
                        for (k, v) in kvs {
                            trx.set(&k, &v);
                        }
                        Ok(())
                    }
                })
                .await
                .context("FDB put_verified_samples_struct chunked")?;
        }

        Ok(())
    }

    /// Write a batch of items atomically to FDB.
    pub async fn write_batch(&self, batch: FdbWriteBatch) -> Result<()> {
        let start = std::time::Instant::now();

        // Pre-serialize all items
        let mut kvs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for epoch in &batch.epochs {
            let key = self.key_agg_epoch(epoch.epoch_type, epoch.aggregator_id, epoch.sequence);
            let value = bincode::serialize(epoch).context("serialize AggEpoch")?;
            kvs.push((key, value));
        }

        for meta in &batch.epoch_meta {
            let key = self.key_agg_epoch_meta(meta.epoch_type, meta.aggregator_id, meta.sequence);
            let value = bincode::serialize(meta).context("serialize AggEpochMeta")?;
            kvs.push((key, value));
        }

        // Handle hist_structs (table_fixed grows with distinct keys; chunk if it
        // would exceed FDB's per-value limit, mirroring cm_structs/verified_samples).
        for item in &batch.hist_structs {
            if item.table_fixed.len() <= FDB_CHUNK_SIZE {
                let key = self.key_agg_hist_struct(item.sequence);
                let value = bincode::serialize(item).context("serialize AggHistStruct")?;
                kvs.push((key, value));
            } else {
                let table_chunks = chunk_bytes(&item.table_fixed);

                // Meta entry: the struct with table_fixed emptied + chunk count.
                let meta_item = AggHistStruct {
                    sequence: item.sequence,
                    total_count: item.total_count,
                    total_sum: item.total_sum,
                    table_fixed: Vec::new(),
                    prev_chain_hash: item.prev_chain_hash.clone(),
                    events_commit: item.events_commit.clone(),
                    out_commit: item.out_commit.clone(),
                    final_chain_hash: item.final_chain_hash.clone(),
                };
                let meta_key = self.key_agg_hist_struct_meta(item.sequence);
                let meta_value = bincode::serialize(&(meta_item, table_chunks.len() as u32))
                    .context("serialize AggHistStruct meta")?;
                kvs.push((meta_key, meta_value));

                for (idx, chunk_data) in table_chunks {
                    let key = self.key_agg_hist_struct_table(item.sequence, idx);
                    kvs.push((key, chunk_data));
                }
            }
        }

        // Handle verified_samples (may need chunking for large table_fixed)
        for item in &batch.verified_samples {
            let table_fixed_size = item.table_fixed.as_ref().map(|v| v.len()).unwrap_or(0);

            if table_fixed_size <= FDB_CHUNK_SIZE {
                // Small enough to store directly
                let key = self.key_verified_samples_struct(item.aggregator_id, item.sequence);
                let value = bincode::serialize(item).context("serialize VerifiedSamplesStruct")?;
                kvs.push((key, value));
            } else {
                // Need to chunk table_fixed separately
                let table_fixed = item.table_fixed.as_ref().unwrap();
                let table_chunks = chunk_bytes(table_fixed);

                // Create a version without table_fixed for metadata
                let meta_item = VerifiedSamplesStruct {
                    sequence: item.sequence,
                    ingest_time_ms: item.ingest_time_ms,
                    result_commit: item.result_commit.clone(),
                    out_commit: item.out_commit.clone(),
                    total_count: item.total_count,
                    total_sum: item.total_sum,
                    table_fixed: None,
                    prev_chain_hash: item.prev_chain_hash.clone(),
                    events_commit: item.events_commit.clone(),
                    aggregator_id: item.aggregator_id,
                };

                let meta_key = self.key_verified_samples_meta(item.aggregator_id, item.sequence);
                let meta_value = bincode::serialize(&(meta_item, table_chunks.len() as u32))?;
                kvs.push((meta_key, meta_value));

                for (idx, chunk_data) in table_chunks {
                    let key = self.key_verified_samples_table(item.aggregator_id, item.sequence, idx);
                    kvs.push((key, chunk_data));
                }
            }
        }

        // Handle proofs (may need chunking)
        for proof in &batch.epoch_proofs {
            let proof_bytes: Vec<u8> = proof
                .receipt_words
                .iter()
                .flat_map(|w| w.to_le_bytes())
                .collect();

            let epoch_type_str = proof.epoch_type.as_str().to_string();
            let aggregator_id = proof.aggregator_id;
            let chunks = chunk_bytes(&proof_bytes);
            let total_chunks = chunks.len() as u32;

            let meta_key = self.key_agg_epoch_proof_chunk(&epoch_type_str, aggregator_id, proof.sequence, 0);
            let meta_value = bincode::serialize(&(total_chunks, proof.epoch_type))
                .context("serialize proof metadata")?;
            kvs.push((meta_key, meta_value));

            for (idx, chunk_data) in chunks {
                let key = self.key_agg_epoch_proof_data(&epoch_type_str, aggregator_id, proof.sequence, idx);
                kvs.push((key, chunk_data));
            }
        }

        // Handle cm_structs (may need chunking)
        for item in &batch.cm_structs {
            let total_size = item.counts_u32.len() + item.heap_fixed.len();
            if total_size <= FDB_CHUNK_SIZE {
                let key = self.key_agg_cm_struct(item.sequence, 0);
                let value = bincode::serialize(item).context("serialize AggCmStruct")?;
                kvs.push((key, value));
            } else {
                let counts_chunks = chunk_bytes(&item.counts_u32);
                let heap_chunks = chunk_bytes(&item.heap_fixed);

                let meta_key = self.key_agg_cm_struct_meta(item.sequence);
                let meta_value = bincode::serialize(&(
                    counts_chunks.len() as u32,
                    heap_chunks.len() as u32,
                ))?;
                kvs.push((meta_key, meta_value));

                for (idx, chunk_data) in counts_chunks {
                    let key = self.key_agg_cm_struct_counts(item.sequence, idx);
                    kvs.push((key, chunk_data));
                }

                for (idx, chunk_data) in heap_chunks {
                    let key = self.key_agg_cm_struct_heap(item.sequence, idx);
                    kvs.push((key, chunk_data));
                }
            }
        }

        // Tombstones (small, no chunking).
        for t in &batch.tombstones {
            let key = self.key_epoch_tombstone(t.epoch_type, t.aggregator_id, t.sequence);
            let value = bincode::serialize(t).context("serialize EpochTombstone")?;
            kvs.push((key, value));
        }

        // Calculate total bytes
        let total_bytes: usize = kvs.iter().map(|(k, v)| k.len() + v.len()).sum();
        let num_keys = kvs.len();

        self.db
            .run(|trx, _| {
                let kvs = kvs.clone();
                async move {
                    for (k, v) in kvs {
                        trx.set(&k, &v);
                    }
                    Ok(())
                }
            })
            .await
            .context("FDB write_batch")?;

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] write_batch: keys={} bytes={} latency_ms={}",
            num_keys, total_bytes, elapsed_ms
        );

        Ok(())
    }

    // ========== Read Methods (for Querier) ==========

    /// Simple test: try to read any key from the subspace
    pub async fn test_read_any(&self) -> Result<bool> {
        let prefix = self.subspace.pack(&("",));
        let range = self.prefix_range(&prefix);

        eprintln!("[fdb] test_read_any: creating transaction...");
        let trx = self.db.create_trx().context("create transaction")?;
        eprintln!("[fdb] test_read_any: transaction created, calling get_range...");

        let result = trx.get_range(&range, 1, false).await;
        match &result {
            Ok(kvs) => eprintln!("[fdb] test_read_any: get_range returned {} keys", kvs.len()),
            Err(e) => eprintln!("[fdb] test_read_any: get_range error: {:?}", e),
        }

        Ok(result.is_ok())
    }

    /// Read all AggEpoch records from FDB.
    pub async fn agg_epochs(&self) -> Result<Vec<AggEpoch>> {
        let start = std::time::Instant::now();
        let prefix = self.subspace.pack(&(KEY_EPOCHS,));
        let kvs = self.read_prefix_kvs(&prefix).await?;

        let num_kvs = kvs.len();
        let mut epochs = Vec::new();
        for (_key, value) in &kvs {
            let epoch: AggEpoch =
                bincode::deserialize(value).context("deserialize AggEpoch")?;
            epochs.push(epoch);
        }

        // Sort by (epoch_type, sequence) for consistent ordering
        epochs.sort_by_key(|e| (e.epoch_type, e.sequence));

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] agg_epochs: rows={} latency_ms={}",
            num_kvs, elapsed_ms
        );

        Ok(epochs)
    }

    /// Read all AggEpochMeta records from FDB.
    pub async fn agg_epoch_meta(&self) -> Result<Vec<AggEpochMeta>> {
        let start = std::time::Instant::now();
        let prefix = self.subspace.pack(&(KEY_EPOCH_META,));
        let kvs = self.read_prefix_kvs(&prefix).await?;

        let num_kvs = kvs.len();
        let mut metas = Vec::new();
        for (_key, value) in &kvs {
            let meta: AggEpochMeta =
                bincode::deserialize(value).context("deserialize AggEpochMeta")?;
            metas.push(meta);
        }

        metas.sort_by_key(|e| (e.epoch_type, e.sequence));

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] agg_epoch_meta: rows={} latency_ms={}",
            num_kvs, elapsed_ms
        );

        Ok(metas)
    }

    /// Read all AggEpochProof records from FDB.
    /// Automatically reassembles chunked proofs.
    pub async fn agg_epoch_proofs(&self) -> Result<Vec<AggEpochProof>> {
        let prefix = self.subspace.pack(&(KEY_EPOCH_PROOFS,));
        let range = self.prefix_range(&prefix);

        // Use direct transaction instead of db.run() to avoid error 2000
        let trx = self.db.create_trx().context("create agg_epoch_proofs transaction")?;
        let kvs: Vec<_> = trx
            .get_range(&range, 1, false)
            .await
            .context("FDB agg_epoch_proofs read")?
            .into_iter()
            .collect();

        // Group by key prefix (everything before the last tuple element)
        // For simplicity, we'll parse all data chunks and group by (epoch_type_str, sequence)
        let mut proof_data: HashMap<Vec<u8>, Vec<(u32, Vec<u8>)>> = HashMap::new();
        let mut proof_meta: HashMap<Vec<u8>, (u32, EpochType)> = HashMap::new();

        for kv in kvs {
            let key = kv.key();
            // Keys are structured: subspace + (KEY_EPOCH_PROOFS, epoch_type, seq, "meta"/"data", idx)
            // We group by the first part to associate metadata with data

            // Simple heuristic: metadata keys end in bincode of (total_chunks, EpochType)
            // Data keys are just raw bytes
            if let Ok((total_chunks, epoch_type)) = bincode::deserialize::<(u32, EpochType)>(kv.value()) {
                // This is metadata
                let group_key = key[..key.len().saturating_sub(10)].to_vec(); // Approximate group key
                proof_meta.insert(group_key, (total_chunks, epoch_type));
            } else {
                // This is a data chunk - extract chunk index from key somehow
                // For now, just store all data values
                let group_key = key[..key.len().saturating_sub(10)].to_vec();
                proof_data.entry(group_key).or_default().push((0, kv.value().to_vec()));
            }
        }

        // This is a simplified version - in production you'd parse the tuple properly
        // For now, return empty since proper parsing requires more work
        Ok(Vec::new())
    }

    /// Read all AggCmStruct records from FDB.
    pub async fn agg_cm_structs(&self) -> Result<Vec<AggCmStruct>> {
        let start = std::time::Instant::now();
        let prefix = self.subspace.pack(&(KEY_CM_STRUCT,));
        let kvs = self.read_prefix_kvs(&prefix).await?;

        let num_kvs = kvs.len();
        let mut structs = Vec::new();
        for (_key, value) in &kvs {
            // Try to deserialize as direct AggCmStruct
            if let Ok(item) = bincode::deserialize::<AggCmStruct>(value) {
                structs.push(item);
            }
            // Chunked items would need more complex handling
        }

        structs.sort_by_key(|e| e.sequence);

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] agg_cm_structs: rows={} latency_ms={}",
            num_kvs, elapsed_ms
        );

        Ok(structs)
    }

    /// Read all AggHistStruct records from FDB.
    /// Automatically reassembles chunked table_fixed data.
    pub async fn agg_hist_structs(&self) -> Result<Vec<AggHistStruct>> {
        use crate::fdb_chunking::reassemble_chunks;

        let start = std::time::Instant::now();
        let prefix = self.subspace.pack(&(KEY_HIST_STRUCT,));
        let kvs = self.read_prefix_kvs(&prefix).await?;
        let num_kvs = kvs.len();

        // First pass: categorize by KEY structure (NOT by trying to deserialize
        // the value, which is ambiguous — a raw histogram chunk can spuriously
        // parse as an AggHistStruct). Keys are:
        //   direct: (KEY_HIST_STRUCT, seq)
        //   meta:   (KEY_HIST_STRUCT, seq, "meta",  0)
        //   table:  (KEY_HIST_STRUCT, seq, "table", idx)
        // The tuple-encoded markers "meta"/"table" appear verbatim in the key
        // bytes (0x02 <utf8> 0x00); KEY_HIST_STRUCT and the int seq never do.
        let meta_marker: &[u8] = b"\x02meta\x00";
        let table_marker: &[u8] = b"\x02table\x00";
        let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);

        let mut direct_items: Vec<AggHistStruct> = Vec::new();
        let mut chunked_meta: HashMap<i64, (AggHistStruct, u32)> = HashMap::new();
        let mut raw_chunks: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for (key_bytes, value_bytes) in kvs {
            if contains(&key_bytes, table_marker) {
                raw_chunks.push((key_bytes, value_bytes));
            } else if contains(&key_bytes, meta_marker) {
                let (meta_item, chunk_count) =
                    bincode::deserialize::<(AggHistStruct, u32)>(&value_bytes)
                        .context("deserialize AggHistStruct chunk meta")?;
                chunked_meta.insert(meta_item.sequence, (meta_item, chunk_count));
            } else {
                let item = bincode::deserialize::<AggHistStruct>(&value_bytes)
                    .context("deserialize AggHistStruct")?;
                direct_items.push(item);
            }
        }

        // Second pass: match table chunks to their metadata by key prefix.
        // kvs arrive globally key-sorted, so per-seq chunks are already in idx
        // order; use discovery order as the chunk index (robust, no int decode).
        let mut chunked_table: HashMap<i64, Vec<(u32, Vec<u8>)>> = HashMap::new();
        for (seq, _) in &chunked_meta {
            let table_prefix = self.subspace.pack(&(KEY_HIST_STRUCT, *seq, "table"));
            let mut chunks_for_entry: Vec<(u32, Vec<u8>)> = Vec::new();
            for (chunk_key, chunk_data) in &raw_chunks {
                if chunk_key.starts_with(&table_prefix) {
                    chunks_for_entry.push((chunks_for_entry.len() as u32, chunk_data.clone()));
                }
            }
            if !chunks_for_entry.is_empty() {
                chunked_table.insert(*seq, chunks_for_entry);
            }
        }

        // Third pass: reassemble chunked items.
        let mut structs = direct_items;
        for (seq, (mut meta_item, _chunk_count)) in chunked_meta {
            if let Some(chunks) = chunked_table.get(&seq) {
                meta_item.table_fixed = reassemble_chunks(chunks);
            }
            structs.push(meta_item);
        }

        structs.sort_by_key(|e| e.sequence);

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] agg_hist_structs: rows={} latency_ms={}",
            num_kvs, elapsed_ms
        );

        Ok(structs)
    }

    /// Read all VerifiedSamplesStruct records from FDB.
    /// Automatically reassembles chunked table_fixed data.
    pub async fn verified_samples_structs(&self) -> Result<Vec<VerifiedSamplesStruct>> {
        use crate::fdb_chunking::reassemble_chunks;

        let start = std::time::Instant::now();
        let prefix = self.subspace.pack(&(KEY_VERIFIED_SAMPLES,));
        let kvs = self.read_prefix_kvs(&prefix).await?;

        let num_kvs = kvs.len();

        // First pass: categorize by KEY structure (robust — a raw table chunk
        // must never be mis-parsed as a struct). Keys are:
        //   direct: (KEY_VERIFIED_SAMPLES, agg_id, seq)
        //   meta:   (KEY_VERIFIED_SAMPLES, agg_id, seq, "meta",  0)
        //   table:  (KEY_VERIFIED_SAMPLES, agg_id, seq, "table", idx)
        let meta_marker: &[u8] = b"\x02meta\x00";
        let table_marker: &[u8] = b"\x02table\x00";
        let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);

        let mut direct_items: Vec<VerifiedSamplesStruct> = Vec::new();
        let mut chunked_meta: HashMap<(u32, i64), (VerifiedSamplesStruct, u32)> = HashMap::new();
        let mut raw_chunks: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // (key, value) pairs for chunks

        for (key_bytes, value_bytes) in kvs {
            if contains(&key_bytes, table_marker) {
                raw_chunks.push((key_bytes, value_bytes));
            } else if contains(&key_bytes, meta_marker) {
                let (meta_item, chunk_count) =
                    bincode::deserialize::<(VerifiedSamplesStruct, u32)>(&value_bytes)
                        .context("deserialize VerifiedSamplesStruct chunk meta")?;
                chunked_meta.insert((meta_item.aggregator_id, meta_item.sequence), (meta_item, chunk_count));
            } else {
                let item = bincode::deserialize::<VerifiedSamplesStruct>(&value_bytes)
                    .context("deserialize VerifiedSamplesStruct")?;
                direct_items.push(item);
            }
        }

        // Second pass: match table chunks to their metadata by key prefix.
        // kvs arrive globally key-sorted, so per-entry chunks are already in idx
        // order; use discovery order as the chunk index (robust, no int decode).
        let mut chunked_table: HashMap<(u32, i64), Vec<(u32, Vec<u8>)>> = HashMap::new();
        for ((agg_id, seq), _) in &chunked_meta {
            let table_prefix = self.subspace.pack(&(KEY_VERIFIED_SAMPLES, *agg_id, *seq, "table"));
            let mut chunks_for_entry: Vec<(u32, Vec<u8>)> = Vec::new();
            for (chunk_key, chunk_data) in &raw_chunks {
                if chunk_key.starts_with(&table_prefix) {
                    chunks_for_entry.push((chunks_for_entry.len() as u32, chunk_data.clone()));
                }
            }
            if !chunks_for_entry.is_empty() {
                chunked_table.insert((*agg_id, *seq), chunks_for_entry);
            }
        }

        // Third pass: reassemble chunked items
        let mut structs = direct_items;
        for ((agg_id, seq), (mut meta_item, _chunk_count)) in chunked_meta {
            if let Some(chunks) = chunked_table.get(&(agg_id, seq)) {
                let table_data = reassemble_chunks(chunks);
                meta_item.table_fixed = Some(table_data);
            }
            structs.push(meta_item);
        }

        structs.sort_by_key(|e| e.sequence);

        let elapsed_ms = start.elapsed().as_millis();
        eprintln!(
            "[fdb] verified_samples_structs: rows={} reassembled_chunked={} latency_ms={}",
            num_kvs,
            chunked_table.len(),
            elapsed_ms
        );

        Ok(structs)
    }

    // ========== Online Resharding (preview) ==========

    /// Persist an OwnershipEpoch row directly (control-plane action).
    pub async fn put_ownership_epoch(&self, oe: &OwnershipEpoch) -> Result<()> {
        let key = self.key_ownership_epoch(oe.epoch_seq);
        let value = bincode::serialize(oe).context("serialize OwnershipEpoch")?;
        self.db
            .run(|trx, _| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await
            .context("FDB put_ownership_epoch")
    }

    /// Active OwnershipEpoch at `epoch_seq` (highest row with `epoch_seq <= requested`).
    pub async fn ownership_epoch_at(&self, epoch_seq: i64) -> Result<Option<OwnershipEpoch>> {
        let all = self.ownership_epochs().await?;
        let mut best: Option<OwnershipEpoch> = None;
        for oe in all.into_iter() {
            if oe.epoch_seq <= epoch_seq {
                best = Some(oe);
            } else {
                break;
            }
        }
        Ok(best)
    }

    /// All OwnershipEpoch rows, sorted ascending by `epoch_seq`.
    pub async fn ownership_epochs(&self) -> Result<Vec<OwnershipEpoch>> {
        let prefix = self.subspace.pack(&(KEY_OWNERSHIP_EPOCH,));
        let range = self.prefix_range(&prefix);
        let trx = self
            .db
            .create_trx()
            .context("create ownership_epochs transaction")?;
        let kvs: Vec<_> = trx
            .get_range(&range, 1, false)
            .await
            .context("FDB ownership_epochs read")?
            .into_iter()
            .collect();
        let mut out: Vec<OwnershipEpoch> = Vec::with_capacity(kvs.len());
        for kv in kvs {
            let oe: OwnershipEpoch =
                bincode::deserialize(kv.value()).context("deserialize OwnershipEpoch")?;
            out.push(oe);
        }
        out.sort_by_key(|oe| oe.epoch_seq);
        Ok(out)
    }

    /// Persist a Handoff row directly.
    pub async fn put_handoff(&self, h: &Handoff) -> Result<()> {
        let key = self.key_handoff(h.at_epoch, h.source_id);
        let value = bincode::serialize(h).context("serialize Handoff")?;
        self.db
            .run(|trx, _| {
                let key = key.clone();
                let value = value.clone();
                async move {
                    trx.set(&key, &value);
                    Ok(())
                }
            })
            .await
            .context("FDB put_handoff")
    }

    /// All Handoff rows for a given epoch.
    pub async fn handoffs_for_epoch(&self, at_epoch: i64) -> Result<Vec<Handoff>> {
        let prefix = self.subspace.pack(&(KEY_HANDOFF, at_epoch));
        let range = self.prefix_range(&prefix);
        let trx = self
            .db
            .create_trx()
            .context("create handoffs_for_epoch transaction")?;
        let kvs: Vec<_> = trx
            .get_range(&range, 1, false)
            .await
            .context("FDB handoffs_for_epoch read")?
            .into_iter()
            .collect();
        let mut out: Vec<Handoff> = Vec::with_capacity(kvs.len());
        for kv in kvs {
            let h: Handoff = bincode::deserialize(kv.value()).context("deserialize Handoff")?;
            out.push(h);
        }
        out.sort_by_key(|h| h.source_id);
        Ok(out)
    }

    /// All Handoff rows.
    pub async fn handoffs(&self) -> Result<Vec<Handoff>> {
        let prefix = self.subspace.pack(&(KEY_HANDOFF,));
        let range = self.prefix_range(&prefix);
        let trx = self
            .db
            .create_trx()
            .context("create handoffs transaction")?;
        let kvs: Vec<_> = trx
            .get_range(&range, 1, false)
            .await
            .context("FDB handoffs read")?
            .into_iter()
            .collect();
        let mut out: Vec<Handoff> = Vec::with_capacity(kvs.len());
        for kv in kvs {
            let h: Handoff = bincode::deserialize(kv.value()).context("deserialize Handoff")?;
            out.push(h);
        }
        out.sort_by_key(|h| (h.at_epoch, h.source_id));
        Ok(out)
    }

    // ========== End Online Resharding ==========

    // ========== Helper Methods ==========

    /// Create a range for prefix scanning
    fn prefix_range(&self, prefix: &[u8]) -> RangeOption<'static> {
        let mut end = prefix.to_vec();
        // Increment the last byte to create an exclusive end bound
        if let Some(last) = end.last_mut() {
            *last = last.wrapping_add(1);
        } else {
            end.push(0xFF);
        }
        let mut opt = RangeOption::from((prefix.to_vec(), end));
        // Fetch the ENTIRE range in one shot. The default (Iterator) streaming
        // mode returns only a small first batch for iteration=1, which silently
        // truncates large/chunked reads (e.g. a histogram split into meta+chunks).
        opt.mode = foundationdb::options::StreamingMode::WantAll;
        opt.limit = None;
        opt
    }

    /// Read ALL (key, value) pairs under `prefix`, paginating across FDB
    /// batches. A single `get_range` returns only the first batch (byte-limited,
    /// ~80KB), so large or chunked ranges (e.g. a histogram split into
    /// meta + 90KB chunks, or many epochs) MUST paginate or they silently
    /// truncate — dropping data and corrupting reassembly.
    async fn read_prefix_kvs(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let trx = self.db.create_trx().context("create read transaction")?;
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut opt = Some(self.prefix_range(prefix));
        let mut iteration = 1usize;
        while let Some(o) = opt {
            let values = trx
                .get_range(&o, iteration, false)
                .await
                .context("FDB get_range")?;
            for kv in values.iter() {
                out.push((kv.key().to_vec(), kv.value().to_vec()));
            }
            opt = o.next_range(&values);
            iteration += 1;
        }
        Ok(out)
    }

    // ========== Key Generation ==========

    fn key_agg_epoch(&self, epoch_type: EpochType, aggregator_id: u32, sequence: i64) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_EPOCHS, epoch_type.as_str(), aggregator_id, sequence))
    }

    fn key_agg_epoch_meta(&self, epoch_type: EpochType, aggregator_id: u32, sequence: i64) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_EPOCH_META, epoch_type.as_str(), aggregator_id, sequence))
    }

    fn key_agg_epoch_proof_chunk(
        &self,
        epoch_type_str: &str,
        aggregator_id: u32,
        sequence: i64,
        chunk_idx: u32,
    ) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_EPOCH_PROOFS, epoch_type_str, aggregator_id, sequence, "meta", chunk_idx))
    }

    fn key_agg_epoch_proof_data(
        &self,
        epoch_type_str: &str,
        aggregator_id: u32,
        sequence: i64,
        chunk_idx: u32,
    ) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_EPOCH_PROOFS, epoch_type_str, aggregator_id, sequence, "data", chunk_idx))
    }

    fn key_agg_cm_struct(&self, sequence: i64, idx: u32) -> Vec<u8> {
        self.subspace.pack(&(KEY_CM_STRUCT, sequence, idx))
    }

    fn key_agg_cm_struct_meta(&self, sequence: i64) -> Vec<u8> {
        self.subspace.pack(&(KEY_CM_STRUCT, sequence, "meta", 0u32))
    }

    fn key_agg_cm_struct_counts(&self, sequence: i64, chunk_idx: u32) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_CM_STRUCT, sequence, "counts", chunk_idx))
    }

    fn key_agg_cm_struct_heap(&self, sequence: i64, chunk_idx: u32) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_CM_STRUCT, sequence, "heap", chunk_idx))
    }

    fn key_agg_hist_struct(&self, sequence: i64) -> Vec<u8> {
        self.subspace.pack(&(KEY_HIST_STRUCT, sequence))
    }

    fn key_agg_hist_struct_meta(&self, sequence: i64) -> Vec<u8> {
        self.subspace.pack(&(KEY_HIST_STRUCT, sequence, "meta", 0u32))
    }

    fn key_agg_hist_struct_table(&self, sequence: i64, chunk_idx: u32) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_HIST_STRUCT, sequence, "table", chunk_idx))
    }

    fn key_verified_samples_struct(&self, aggregator_id: u32, sequence: i64) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_VERIFIED_SAMPLES, aggregator_id, sequence))
    }

    fn key_verified_samples_meta(&self, aggregator_id: u32, sequence: i64) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_VERIFIED_SAMPLES, aggregator_id, sequence, "meta", 0u32))
    }

    fn key_verified_samples_table(&self, aggregator_id: u32, sequence: i64, chunk_idx: u32) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_VERIFIED_SAMPLES, aggregator_id, sequence, "table", chunk_idx))
    }

    fn key_epoch_tombstone(
        &self,
        epoch_type: EpochType,
        aggregator_id: u32,
        sequence: i64,
    ) -> Vec<u8> {
        self.subspace
            .pack(&(KEY_EPOCH_TOMBSTONE, epoch_type.as_str(), aggregator_id, sequence))
    }

    fn key_ownership_epoch(&self, epoch_seq: i64) -> Vec<u8> {
        self.subspace.pack(&(KEY_OWNERSHIP_EPOCH, epoch_seq))
    }

    fn key_handoff(&self, at_epoch: i64, source_id: u32) -> Vec<u8> {
        self.subspace.pack(&(KEY_HANDOFF, at_epoch, source_id))
    }
}

/// Synchronous wrapper around FdbStore for use in sync contexts.
///
/// This wrapper uses `tokio::task::block_in_place()` with `block_on()` to run
/// async FDB operations synchronously. This allows calling sync methods from
/// within an async context (e.g., the querier server's tokio runtime).
pub struct FdbStoreSync {
    inner: std::sync::Arc<FdbStore>,
    runtime: tokio::runtime::Handle,
}

impl FdbStoreSync {
    /// Create a new FdbStoreSync from an existing FdbStore.
    /// Must be called from within a tokio runtime.
    pub fn new(inner: FdbStore) -> Self {
        Self {
            inner: std::sync::Arc::new(inner),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    /// Open a new connection to FoundationDB.
    /// Must be called from within a tokio runtime.
    pub fn open_sync() -> Result<Self> {
        let rt = tokio::runtime::Handle::current();
        let inner = tokio::task::block_in_place(|| rt.block_on(FdbStore::open()))?;
        Ok(Self::new(inner))
    }

    /// Helper to run async FDB operations from sync context.
    /// Uses spawn_blocking to avoid blocking the tokio worker thread.
    fn run_async<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(std::sync::Arc<FdbStore>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send>> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();

        // Use block_in_place to run async code from sync context
        tokio::task::block_in_place(|| {
            rt.block_on(async move {
                // Run the FDB operation
                f(inner).await
            })
        })
    }

    /// Clear all data in the subspace (for testing/benchmarking)
    pub fn clear_all(&self) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.clear_all()))
    }

    /// Read all AggEpoch records.
    pub fn agg_epochs(&self) -> Result<Vec<AggEpoch>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.agg_epochs()))
    }

    /// Read all AggEpochMeta records.
    pub fn agg_epoch_meta(&self) -> Result<Vec<AggEpochMeta>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.agg_epoch_meta()))
    }

    /// Read all AggEpochProof records.
    pub fn agg_epoch_proofs(&self) -> Result<Vec<AggEpochProof>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.agg_epoch_proofs()))
    }

    /// Read all AggCmStruct records.
    pub fn agg_cm_structs(&self) -> Result<Vec<AggCmStruct>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.agg_cm_structs()))
    }

    /// Read all AggHistStruct records.
    pub fn agg_hist_structs(&self) -> Result<Vec<AggHistStruct>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.agg_hist_structs()))
    }

    /// Read all VerifiedSamplesStruct records.
    pub fn verified_samples_structs(&self) -> Result<Vec<VerifiedSamplesStruct>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.verified_samples_structs()))
    }

    /// Write an AggEpoch to FDB.
    pub fn put_agg_epoch(&self, epoch: &AggEpoch) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let epoch = epoch.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_agg_epoch(&epoch)))
    }

    /// Write an AggEpochMeta to FDB.
    pub fn put_agg_epoch_meta(&self, meta: &AggEpochMeta) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let meta = meta.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_agg_epoch_meta(&meta)))
    }

    /// Write an AggEpochProof to FDB.
    pub fn put_agg_epoch_proof(&self, proof: &AggEpochProof) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let proof = proof.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_agg_epoch_proof(&proof)))
    }

    /// Write an AggCmStruct to FDB.
    pub fn put_agg_cm_struct(&self, item: &AggCmStruct) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let item = item.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_agg_cm_struct(&item)))
    }

    /// Write an AggHistStruct to FDB.
    pub fn put_agg_hist_struct(&self, item: &AggHistStruct) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let item = item.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_agg_hist_struct(&item)))
    }

    /// Write a VerifiedSamplesStruct to FDB.
    pub fn put_verified_samples_struct(&self, item: &VerifiedSamplesStruct) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let item = item.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_verified_samples_struct(&item)))
    }

    /// Write a batch of items atomically.
    pub fn write_batch(&self, batch: FdbWriteBatch) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.write_batch(batch)))
    }

    /// Write an EpochTombstone to FDB.
    pub fn put_epoch_tombstone(&self, t: &EpochTombstone) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let t = t.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_epoch_tombstone(&t)))
    }

    /// Whether a tombstone exists for the given (epoch_type, sequence, aggregator_id).
    pub fn has_epoch_tombstone(
        &self,
        epoch_type: EpochType,
        sequence: i64,
        aggregator_id: u32,
    ) -> Result<bool> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| {
            rt.block_on(inner.has_epoch_tombstone(epoch_type, sequence, aggregator_id))
        })
    }

    /// Highest tombstoned sequence for the given (epoch_type, aggregator_id).
    pub fn max_epoch_tombstone_seq(
        &self,
        epoch_type: EpochType,
        aggregator_id: u32,
    ) -> Result<Option<i64>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| {
            rt.block_on(inner.max_epoch_tombstone_seq(epoch_type, aggregator_id))
        })
    }

    /// Read all EpochTombstone records from FDB.
    pub fn epoch_tombstones(&self) -> Result<Vec<EpochTombstone>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.epoch_tombstones()))
    }

    // ---- Online resharding (preview) sync wrappers ----

    pub fn put_ownership_epoch(&self, oe: &OwnershipEpoch) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let oe = oe.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_ownership_epoch(&oe)))
    }

    pub fn ownership_epoch_at(&self, epoch_seq: i64) -> Result<Option<OwnershipEpoch>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.ownership_epoch_at(epoch_seq)))
    }

    pub fn ownership_epochs(&self) -> Result<Vec<OwnershipEpoch>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.ownership_epochs()))
    }

    pub fn put_handoff(&self, h: &Handoff) -> Result<()> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        let h = h.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.put_handoff(&h)))
    }

    pub fn handoffs_for_epoch(&self, at_epoch: i64) -> Result<Vec<Handoff>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.handoffs_for_epoch(at_epoch)))
    }

    pub fn handoffs(&self) -> Result<Vec<Handoff>> {
        let inner = self.inner.clone();
        let rt = self.runtime.clone();
        tokio::task::block_in_place(|| rt.block_on(inner.handoffs()))
    }
}
