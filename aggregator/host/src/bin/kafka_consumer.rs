//! Kafka consumer binary for ingesting raw events into RocksDB.
//!
//! # Environment Variables
//! - `KAFKA_BROKERS`: Kafka broker addresses (default: localhost:9092)
//! - `KAFKA_TOPIC`: Topic to consume from (default: raw_events)
//! - `KAFKA_GROUP_ID`: Consumer group ID (default: aggregators)
//! - `KAFKA_AUTO_OFFSET_RESET`: Offset reset policy (default: earliest)
//! - `RAW_DB_PATH`: Path to RocksDB for raw event storage (required)
//!
//! # Example
//! ```bash
//! KAFKA_BROKERS=kafka:9092 \
//! KAFKA_TOPIC=raw_events \
//! KAFKA_GROUP_ID=aggregators \
//! RAW_DB_PATH=/data/raw \
//! cargo run --bin kafka-consumer
//! ```

use anyhow::{Context, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use zktelemetry_common::rocksdb_store::RocksDb;
use aggregator::kafka_consumer::{run_consumer, KafkaConsumerConfig, ConsumerSignals};

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

#[tokio::main]
async fn main() -> Result<()> {
    let raw_db_path = env_string("RAW_DB_PATH")
        .context("RAW_DB_PATH environment variable is required")?;

    eprintln!("[kafka-consumer] opening RocksDB at: {raw_db_path}");

    let raw_db = RocksDb::open(&raw_db_path)
        .with_context(|| format!("failed to open RocksDB at {raw_db_path}"))?;
    let raw_db = Arc::new(raw_db);

    eprintln!("[kafka-consumer] RocksDB opened successfully at: {raw_db_path}");

    let config = KafkaConsumerConfig::default();
    let signals = Arc::new(ConsumerSignals::new());

    // Handle SIGINT (Ctrl-C) for graceful shutdown
    let signals_sigint = signals.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        eprintln!("[kafka-consumer] received SIGINT");
        signals_sigint.shutdown.store(true, Ordering::Relaxed);
    });

    // Handle SIGTERM for graceful shutdown
    let signals_sigterm = signals.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate()
        ).expect("failed to setup SIGTERM handler");
        sigterm.recv().await;
        eprintln!("[kafka-consumer] received SIGTERM");
        signals_sigterm.shutdown.store(true, Ordering::Relaxed);
    });

    // Handle SIGUSR1 for forced epoch flush (without shutdown)
    let signals_sigusr1 = signals.clone();
    tokio::spawn(async move {
        let mut sigusr1 = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::user_defined1()
        ).expect("failed to setup SIGUSR1 handler");
        loop {
            sigusr1.recv().await;
            eprintln!("[kafka-consumer] received SIGUSR1 - triggering forced epoch flush");
            signals_sigusr1.force_flush.store(true, Ordering::Relaxed);
        }
    });

    let raw_db_clone = raw_db.clone();
    run_consumer(config, raw_db, signals).await?;

    // Explicitly flush RocksDB to ensure data is persisted to disk
    eprintln!("[kafka-consumer] flushing RocksDB before exit...");
    raw_db_clone.flush()
        .context("failed to flush RocksDB before shutdown")?;
    eprintln!("[kafka-consumer] RocksDB flushed successfully");

    eprintln!("[kafka-consumer] shutdown complete");
    Ok(())
}
