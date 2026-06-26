//! Smoke test for the Trillian transparency-log backend.
//!
//! Connects to a running Trillian log server (see deploy/trillian/), appends a
//! few data-source checkpoints via the real `QueueLeaf` gRPC path, and fails
//! loudly if the connection silently degrades to the no-op fallback. This
//! exercises end-to-end wire compatibility of proto/trillian_log.proto against
//! an unmodified Trillian server.
//!
//! Usage:
//!   TRILLIAN_ADDR=http://127.0.0.1:8090 TRILLIAN_LOG_ID=<tree id> \
//!     cargo run -p zktelemetry-risc0-data-source-host \
//!       --features trillian --bin trillian-smoke

use anyhow::{ensure, Context, Result};
use data_source::transparency::{Checkpoint, TransparencyLog};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("TRILLIAN_ADDR").context("TRILLIAN_ADDR must be set")?;
    let log_id = std::env::var("TRILLIAN_LOG_ID")
        .context("TRILLIAN_LOG_ID must be set")?
        .trim()
        .parse::<i64>()
        .context("TRILLIAN_LOG_ID must be an i64")?;

    println!("[smoke] connecting to Trillian addr={addr} log_id={log_id}");
    let log = TransparencyLog::from_env().await;
    ensure!(
        matches!(log, TransparencyLog::Trillian(_)),
        "expected the Trillian backend, but got the no-op fallback — connection to {addr} failed \
         (is the log server up and TRILLIAN_LOG_ID a valid, initialized tree?)"
    );

    const N: u64 = 3;
    for index in 1..=N {
        let cp = Checkpoint {
            source_id: 42,
            index,
            chain_hash: [index as u8; 32],
        };
        log.publish(cp)
            .await
            .with_context(|| format!("QueueLeaf failed for checkpoint index={index}"))?;
        println!(
            "[smoke] appended checkpoint source_id={} index={} h_i={}",
            cp.source_id,
            cp.index,
            hex::encode(cp.chain_hash)
        );
    }

    println!("[smoke] OK — appended {N} checkpoints via Trillian QueueLeaf");
    Ok(())
}
