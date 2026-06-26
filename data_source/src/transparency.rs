//! Pluggable transparency-log publisher for data-source checkpoints.
//!
//! This implements the `\change` block of Algorithm "Log Commitment (data
//! source)": every `P` committed batches the data source publishes a checkpoint
//! `(i, h_i)` — the per-source batch index `i` and the chain hash
//! `h_i = CommitHash(h_{i-1}, buffer)` — to a public append-only registry. An
//! auditor can then verify the data source's hash chain against an
//! externally-witnessed log instead of trusting the source.
//!
//! Two backends, selected at runtime:
//!   * [`TransparencyLog::Trillian`] — appends each checkpoint to a Google
//!     Trillian verifiable log via the gRPC `QueueLeaf` RPC. Compiled in only
//!     with the `trillian` cargo feature and used when `TRILLIAN_ADDR` is set.
//!   * [`TransparencyLog::Noop`] — the default fallback. Logs each checkpoint to
//!     stderr so the pipeline compiles and runs without a live Trillian server.
//!
//! Configuration (all via environment):
//!   * `CHECKPOINT_INTERVAL` — `P`, publish every P-th batch (0 = disabled).
//!     Read by the producer, not here.
//!   * `TRILLIAN_ADDR`       — gRPC endpoint, e.g. `http://127.0.0.1:8090`.
//!     Presence selects the Trillian backend.
//!   * `TRILLIAN_LOG_ID`     — target Trillian log/tree id (i64, default 0).

use anyhow::Result;

/// Domain-separation tag for the canonical checkpoint leaf encoding.
const CHECKPOINT_TAG: &[u8] = b"ZKTLM_CHECKPOINT_V1";

#[cfg(feature = "trillian")]
mod trillian_proto {
    // Generated from proto/trillian_log.proto (package `trillian`).
    tonic::include_proto!("trillian");
}

/// A checkpoint published to the transparency log.
#[derive(Clone, Copy, Debug)]
pub struct Checkpoint {
    /// Per-source identifier whose hash chain this checkpoint pins.
    pub source_id: u32,
    /// Batch index `i` (1-based count of committed batches for this source).
    pub index: u64,
    /// Chain hash `h_i = CommitHash(h_{i-1}, buffer)` after batch `i`.
    pub chain_hash: [u8; 32],
}

impl Checkpoint {
    /// Canonical, self-describing leaf encoding appended to the log:
    /// `TAG || source_id (BE u32) || index (BE u64) || chain_hash (32 bytes)`.
    pub fn leaf_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(CHECKPOINT_TAG.len() + 4 + 8 + 32);
        v.extend_from_slice(CHECKPOINT_TAG);
        v.extend_from_slice(&self.source_id.to_be_bytes());
        v.extend_from_slice(&self.index.to_be_bytes());
        v.extend_from_slice(&self.chain_hash);
        v
    }
}

/// Runtime-selected transparency-log backend.
pub enum TransparencyLog {
    /// No external registry; checkpoints are logged to stderr only.
    Noop,
    // Boxed so the enum stays small when the (data-less) Noop variant is used.
    #[cfg(feature = "trillian")]
    Trillian(Box<TrillianPublisher>),
}

impl TransparencyLog {
    /// Construct from the environment. Returns [`TransparencyLog::Trillian`]
    /// when `TRILLIAN_ADDR` is set and the `trillian` feature is enabled and the
    /// connection succeeds; otherwise [`TransparencyLog::Noop`]. Never fails —
    /// transparency publication is best-effort and must not block ingestion.
    pub async fn from_env() -> Self {
        let addr = std::env::var("TRILLIAN_ADDR")
            .ok()
            .filter(|s| !s.trim().is_empty());
        match addr {
            None => TransparencyLog::Noop,
            #[cfg(feature = "trillian")]
            Some(addr) => {
                let log_id = std::env::var("TRILLIAN_LOG_ID")
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    .unwrap_or(0);
                match TrillianPublisher::connect(addr.clone(), log_id).await {
                    Ok(p) => {
                        eprintln!(
                            "[transparency] Trillian publisher connected addr={addr} log_id={log_id}"
                        );
                        TransparencyLog::Trillian(Box::new(p))
                    }
                    Err(e) => {
                        eprintln!(
                            "[transparency] WARN: Trillian connect failed ({e:#}); using no-op fallback"
                        );
                        TransparencyLog::Noop
                    }
                }
            }
            #[cfg(not(feature = "trillian"))]
            Some(addr) => {
                eprintln!(
                    "[transparency] WARN: TRILLIAN_ADDR={addr} set but binary built without the \
                     `trillian` feature; using no-op fallback"
                );
                TransparencyLog::Noop
            }
        }
    }

    /// Publish a single checkpoint. Errors are returned to the caller, which is
    /// expected to log and continue rather than abort the data pipeline.
    pub async fn publish(&self, cp: Checkpoint) -> Result<()> {
        match self {
            TransparencyLog::Noop => {
                eprintln!(
                    "[transparency][noop] checkpoint source_id={} index={} h_i={}",
                    cp.source_id,
                    cp.index,
                    hex::encode(cp.chain_hash)
                );
                Ok(())
            }
            #[cfg(feature = "trillian")]
            TransparencyLog::Trillian(p) => p.publish(cp).await,
        }
    }
}

/// Trillian-backed publisher: appends each checkpoint as a log leaf via the
/// `TrillianLog.QueueLeaf` RPC. The channel is cloneable and the client is cheap
/// to share; a mutex serializes the few publish calls per epoch.
#[cfg(feature = "trillian")]
pub struct TrillianPublisher {
    client: tokio::sync::Mutex<
        trillian_proto::trillian_log_client::TrillianLogClient<tonic::transport::Channel>,
    >,
    log_id: i64,
}

#[cfg(feature = "trillian")]
impl TrillianPublisher {
    /// Connect to a Trillian log server at `addr` (e.g. `http://127.0.0.1:8090`).
    pub async fn connect(addr: String, log_id: i64) -> Result<Self> {
        use anyhow::Context;
        let client =
            trillian_proto::trillian_log_client::TrillianLogClient::connect(addr)
                .await
                .context("connect to Trillian log server")?;
        Ok(Self {
            client: tokio::sync::Mutex::new(client),
            log_id,
        })
    }

    async fn publish(&self, cp: Checkpoint) -> Result<()> {
        use anyhow::Context;
        use sha2::{Digest, Sha256};

        let leaf_value = cp.leaf_bytes();
        // leaf_identity_hash de-duplicates re-published checkpoints server-side.
        let leaf_identity_hash = Sha256::digest(&leaf_value).to_vec();
        let leaf = trillian_proto::LogLeaf {
            merkle_leaf_hash: Vec::new(),
            leaf_value,
            extra_data: Vec::new(),
            leaf_index: 0,
            leaf_identity_hash,
        };
        let request = trillian_proto::QueueLeafRequest {
            log_id: self.log_id,
            leaf: Some(leaf),
        };
        let mut client = self.client.lock().await;
        client
            .queue_leaf(request)
            .await
            .context("Trillian QueueLeaf RPC")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_encoding_is_canonical() {
        let cp = Checkpoint {
            source_id: 0x01020304,
            index: 0x0a0b0c0d0e0f1011,
            chain_hash: [0xab; 32],
        };
        let bytes = cp.leaf_bytes();
        assert_eq!(&bytes[..CHECKPOINT_TAG.len()], CHECKPOINT_TAG);
        let mut off = CHECKPOINT_TAG.len();
        assert_eq!(&bytes[off..off + 4], &[0x01, 0x02, 0x03, 0x04]);
        off += 4;
        assert_eq!(
            &bytes[off..off + 8],
            &[0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11]
        );
        off += 8;
        assert_eq!(&bytes[off..], &[0xab; 32]);
    }

    #[tokio::test]
    async fn noop_when_unconfigured() {
        std::env::remove_var("TRILLIAN_ADDR");
        let log = TransparencyLog::from_env().await;
        assert!(matches!(log, TransparencyLog::Noop));
        // Publishing on the no-op backend always succeeds.
        log.publish(Checkpoint {
            source_id: 1,
            index: 8,
            chain_hash: [0u8; 32],
        })
        .await
        .unwrap();
    }
}
