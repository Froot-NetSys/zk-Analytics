//! Kafka producer binary for sending event batches to aggregators.
//!
//! # Partitioning
//! Events are partitioned by source_id, with different modes:
//!
//! ## Synthetic Mode (BENCH_INPUT=synthetic)
//! - source_id = configured SOURCE_ID (use_configured_source_id=true)
//! - Each producer instance handles exactly one source_id
//! - Run NUM_SOURCES parallel producers (one per source_id) for full parallelism
//!
//! ## Dataset Mode (BENCH_INPUT=tsv/caida/car_emission)
//! - source_id = hash(natural_key) % NUM_SOURCES
//! - Google Cluster (TSV): natural_key = machine_id
//! - CAIDA: natural_key = (src_ip << 32) | dst_ip
//! - Car Emission: natural_key = last 8 bytes of encoded key (model hash + attributes)
//! - Single producer instance handles all sources (auto-partitioned by data)
//!
//! Then Kafka partition is computed as: partition = source_id % NUM_AGGREGATORS
//!
//! This ensures all events from the same machine/ip_pair go to the same consumer,
//! and each source has contiguous batch sequences (0, 1, 2, ...).
//!
//! # Environment Variables
//! - `KAFKA_BROKERS`: Kafka broker addresses (default: localhost:9092)
//! - `KAFKA_TOPIC`: Topic to produce to (default: raw_events)
//! - `COMMIT_BATCH_SIZE`: Events per batch_hash (default: 8)
//! - `KAFKA_BATCH_SIZE`: Number of commit batches per send (default: 1)
//! - `NUM_AGGREGATORS`: Number of Kafka partitions/consumers (default: 1)
//! - `NUM_SOURCES`: Number of logical sources for source_id (default: NUM_AGGREGATORS)
//! - `SOURCE_ID`: Source ID for synthetic mode (default: 0)
//! - `USE_CONFIGURED_SOURCE_ID`: Use SOURCE_ID directly (set automatically for synthetic)
//! - `BENCH_INPUT`: Data source type: synthetic, tsv/google, caida, car_emission (default: synthetic)
//! - `TSV_DIR`: Directory with TSV/CSV files (required for BENCH_INPUT=tsv)
//! - `TSV_MAX_FILES`: Max files to load for TSV (default: 64)
//! - `CAIDA_DIR`: Directory with CAIDA txt files (required for BENCH_INPUT=caida)
//! - `CAIDA_MAX_FILES`: Max files to load for CAIDA (default: 64)
//! - `EMISSION_CSV`: Path to car emission CSV file (required for BENCH_INPUT=car_emission)
//! - `EMISSION_VALUE_SCALE`: Scale factor for CO2 values (default: 1.0)
//!
//! # Command Line Arguments
//! - `--events N`: Total number of events to generate (default: 1000)
//! - `--kafka-batch-size N`: Number of commit batches per send_batch call (default: 1)
//! - `--commit-batch-size N`: Events per key per batch_hash (default: 8)
//! - `--series N`: Number of distinct keys per source (default: 1000)
//! - `--samples-per-series N`: Samples per key (used with --series to compute total events)
//! - `--source-id N`: Source identifier for synthetic data (default: 0)
//! - `--key-mod N`: Maximum number of unique keys per source (default: 1000)
//! - `--value-mod N`: Value modulo for synthetic events (default: 10000)
//! - `--seed N`: Random seed (default: 0x5EED)
//! - `--rate N`: Events per second rate limit (0 = unlimited)
//! - `--parallel-producers N`: Number of parallel producer tasks for dataset mode (default: 1)
//!
//! # Example
//! ```bash
//! # Synthetic data
//! KAFKA_BROKERS=kafka:9092 cargo run --bin kafka-producer -- --events 10000
//!
//! # Google Cluster data (auto-partitioned by machine_id)
//! BENCH_INPUT=tsv TSV_DIR=/path/to/data NUM_AGGREGATORS=4 NUM_SOURCES=64 \
//! cargo run --bin kafka-producer -- --events 100000
//! ```

use anyhow::{Context, Result};
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use data_source::event_sources::{BenchEvent, EventSource};
use data_source::kafka_producer::{
    EventBatchProducer, KafkaProducerConfig, SimpleEvent,
};

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

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let kafka_batch_size = parse_arg_u64("--kafka-batch-size", 0);
    let commit_batch_size = parse_arg_u64("--commit-batch-size", 0);
    let value_mod = parse_arg_u64("--value-mod", 10000).max(1) as u32;
    let seed = parse_arg_u64("--seed", 0x5EED);
    let rate_limit = parse_arg_u64("--rate", 0);
    let parallel_producers = parse_arg_u64("--parallel-producers", env_u64("PARALLEL_PRODUCERS", 1)).max(1) as u32;

    // Support both --series (alias) and --key-mod
    let series = parse_arg_u64("--series", 0);
    let key_mod = if series > 0 {
        series
    } else {
        parse_arg_u64("--key-mod", 1000).max(1)
    };

    // Support --samples-per-series to compute total events
    let samples_per_series = parse_arg_u64("--samples-per-series", 0);
    let total_events = if samples_per_series > 0 && series > 0 {
        series * samples_per_series
    } else {
        parse_arg_u64("--events", env_u64("EVENTS", 1000))
    };

    // Source ID for partitioning
    let source_id = parse_arg_u64("--source-id", env_u64("SOURCE_ID", 0)) as u32;

    // Configure producer
    let mut config = KafkaProducerConfig::default();
    config.source_id = source_id;
    if commit_batch_size > 0 || std::env::args().any(|a| a == "--commit-batch-size") {
        config.commit_batch_size = commit_batch_size;
    }
    if kafka_batch_size > 0 || std::env::args().any(|a| a == "--kafka-batch-size") {
        config.kafka_batch_size = kafka_batch_size;
    }

    let commit_batch_desc = if config.commit_batch_size == 0 {
        "unlimited".to_string()
    } else {
        config.commit_batch_size.to_string()
    };

    // Calculate events per send
    let events_per_send = if config.commit_batch_size == 0 {
        config.kafka_batch_size.max(1)
    } else {
        config.kafka_batch_size.max(1) * config.commit_batch_size
    };

    // Determine data source type
    let bench_input = std::env::var("BENCH_INPUT").unwrap_or_else(|_| "synthetic".to_string());

    // For synthetic mode, use configured source_id directly (single source per producer)
    // For dataset mode (tsv/caida), compute source_id from hash of natural key
    let is_synthetic = bench_input == "synthetic";
    if is_synthetic {
        config.use_configured_source_id = true;
    }

    // Check if even distribution mode is enabled
    let distribute_evenly = config.distribute_evenly;

    eprintln!(
        "[kafka-producer] starting brokers={} topic={}",
        config.brokers, config.topic
    );
    eprintln!(
        "[kafka-producer] bench_input={} events={} kafka_batch_size={} commit_batch_size={} events_per_send={}",
        bench_input, total_events, config.kafka_batch_size, commit_batch_desc, events_per_send
    );
    if is_synthetic {
        eprintln!(
            "[kafka-producer] synthetic mode: source_id={} (direct), partition = source_id %% {} = {}",
            config.source_id, config.num_aggregators, config.source_id % config.num_aggregators
        );
    } else if distribute_evenly {
        eprintln!(
            "[kafka-producer] dataset mode (EVEN DISTRIBUTION): num_aggregators={} num_sources={} (source_id = round_robin %% num_sources, partition = source_id %% num_aggregators)",
            config.num_aggregators, config.num_sources
        );
    } else {
        eprintln!(
            "[kafka-producer] dataset mode: num_aggregators={} num_sources={} (source_id = hash(key) %% num_sources, partition = source_id %% num_aggregators)",
            config.num_aggregators, config.num_sources
        );
    }

    let mut producer = EventBatchProducer::new(config.clone(), [0u8; 32])
        .await
        .context("create producer")?;

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut events_sent: u64 = 0;
    let start_time = std::time::Instant::now();
    let mut last_rate_check = std::time::Instant::now();
    let mut events_since_check: u64 = 0;

    // Create event source based on BENCH_INPUT
    match bench_input.as_str() {
        "synthetic" => {
            eprintln!(
                "[kafka-producer] synthetic mode: keys_per_source={} value_mod={}",
                key_mod, value_mod
            );

            // Synthetic mode: generate events with source_id in key
            let cbs = config.commit_batch_size.max(1);
            let mut current_key: u64 = 0;
            let mut events_for_current_key: u64 = 0;

            while events_sent < total_events {
                let remaining = total_events - events_sent;
                let batch_count = remaining.min(events_per_send);

                let mut batch: Vec<SimpleEvent> = Vec::with_capacity(batch_count as usize);
                for _ in 0..batch_count {
                    // Generate 15-byte key_id unique per source
                    let mut key_id = [0u8; 15];
                    key_id[3..7].copy_from_slice(&source_id.to_be_bytes());
                    key_id[15 - 8..].copy_from_slice(&current_key.to_be_bytes());

                    batch.push(SimpleEvent {
                        ts: data_source::event_sources::now_ts(),
                        key_id,
                        value: rng.gen::<u32>() % value_mod,
                    });

                    events_for_current_key += 1;
                    if events_for_current_key >= cbs {
                        events_for_current_key = 0;
                        current_key = (current_key + 1) % key_mod;
                    }
                }

                producer.send_batch(batch).await?;
                events_sent += batch_count;
                events_since_check += batch_count;

                apply_rate_limit(rate_limit, &mut last_rate_check, &mut events_since_check).await;
            }
        }
        "tsv" | "google" | "google_cluster" | "google_cluster_data" | "car_emission" | "emission" => {
            // TSV/Google Cluster/Car Emission mode: load from files
            let mut source = EventSource::from_env(total_events)?;
            eprintln!(
                "[kafka-producer] {} mode: loading events from files (parallel_producers={})",
                source.type_name(), parallel_producers
            );

            if parallel_producers > 1 {
                // Parallel mode: pre-load all events, group by source_id, spawn N tasks
                eprintln!("[kafka-producer] pre-loading events into memory...");
                let load_start = std::time::Instant::now();

                let mut events_by_source: HashMap<u32, Vec<SimpleEvent>> = HashMap::new();
                let mut loaded_count: u64 = 0;

                loop {
                    if loaded_count >= total_events {
                        break;
                    }

                    let event = match &mut source {
                        EventSource::Tsv(tsv) => tsv.next_event()?,
                        EventSource::Caida(caida) => caida.next_event()?,
                        EventSource::CarEmission(em) => em.next_event()?,
                        EventSource::Synthetic(_) => unreachable!(),
                    };

                    let Some(ev) = event else {
                        break;
                    };

                    let simple_ev = bench_event_to_simple(ev);
                    let natural_key = extract_natural_key(&simple_ev.key_id);
                    let source_id = compute_source_id(natural_key, config.num_sources);
                    events_by_source.entry(source_id).or_default().push(simple_ev);
                    loaded_count += 1;

                    if loaded_count % 500000 == 0 {
                        eprintln!("[kafka-producer] loaded {} events...", loaded_count);
                    }
                }

                let load_elapsed = load_start.elapsed();
                eprintln!(
                    "[kafka-producer] loaded {} events into {} sources in {:.2}s",
                    loaded_count, events_by_source.len(), load_elapsed.as_secs_f64()
                );

                // Wrap in Arc for sharing across tasks
                let events_by_source = Arc::new(events_by_source);

                // Spawn parallel producer tasks
                let mut handles = Vec::new();
                for task_idx in 0..parallel_producers {
                    let events_clone = Arc::clone(&events_by_source);
                    let config_clone = config.clone();
                    let handle = tokio::spawn(async move {
                        run_producer_task(
                            task_idx,
                            parallel_producers,
                            events_clone,
                            config_clone,
                            events_per_send,
                            rate_limit,
                        ).await
                    });
                    handles.push(handle);
                }

                // Wait for all tasks and collect results
                let mut total_events_sent: u64 = 0;
                let mut total_batches: i64 = 0;
                for handle in handles {
                    match handle.await {
                        Ok(Ok((sent, batches))) => {
                            total_events_sent += sent;
                            total_batches += batches;
                        }
                        Ok(Err(e)) => {
                            eprintln!("[kafka-producer] task error: {}", e);
                            return Err(e);
                        }
                        Err(e) => {
                            eprintln!("[kafka-producer] task join error: {}", e);
                            return Err(anyhow::anyhow!("task join error: {}", e));
                        }
                    }
                }
                events_sent = total_events_sent;

                let total_elapsed = start_time.elapsed();
                let rate = events_sent as f64 / total_elapsed.as_secs_f64();
                eprintln!(
                    "[kafka-producer] parallel done: events={} batches={} elapsed={:.2}s rate={:.0} events/s",
                    events_sent, total_batches, total_elapsed.as_secs_f64(), rate
                );
            } else {
                // Single producer mode (original behavior)
                let mut batch: Vec<SimpleEvent> = Vec::with_capacity(events_per_send as usize);

                loop {
                    if events_sent >= total_events {
                        break;
                    }

                    let event = match &mut source {
                        EventSource::Tsv(tsv) => tsv.next_event()?,
                        EventSource::Caida(caida) => caida.next_event()?,
                        EventSource::CarEmission(em) => em.next_event()?,
                        EventSource::Synthetic(_) => unreachable!(),
                    };

                    let Some(ev) = event else {
                        break;
                    };

                    batch.push(bench_event_to_simple(ev));

                    if batch.len() >= events_per_send as usize {
                        let batch_size = batch.len() as u64;
                        producer.send_batch(batch).await?;
                        events_sent += batch_size;
                        events_since_check += batch_size;
                        batch = Vec::with_capacity(events_per_send as usize);

                        apply_rate_limit(rate_limit, &mut last_rate_check, &mut events_since_check).await;

                        if events_sent % 100000 == 0 {
                            let elapsed = start_time.elapsed();
                            let rate = events_sent as f64 / elapsed.as_secs_f64();
                            eprintln!(
                                "[kafka-producer] progress: {} events sent ({:.0} events/s)",
                                events_sent, rate
                            );
                        }
                    }
                }

                if !batch.is_empty() {
                    let batch_size = batch.len() as u64;
                    producer.send_batch(batch).await?;
                    events_sent += batch_size;
                }
            }
        }
        "caida" | "caida_txt" => {
            // CAIDA mode: load from files
            let mut source = EventSource::from_env(total_events)?;
            eprintln!(
                "[kafka-producer] {} mode: loading events from files (parallel_producers={})",
                source.type_name(), parallel_producers
            );

            if parallel_producers > 1 {
                // Parallel mode: pre-load all events, group by source_id, spawn N tasks
                eprintln!("[kafka-producer] pre-loading events into memory...");
                let load_start = std::time::Instant::now();

                let mut events_by_source: HashMap<u32, Vec<SimpleEvent>> = HashMap::new();
                let mut loaded_count: u64 = 0;

                loop {
                    if loaded_count >= total_events {
                        break;
                    }

                    let event = match &mut source {
                        EventSource::Caida(caida) => caida.next_event()?,
                        _ => unreachable!(),
                    };

                    let Some(ev) = event else {
                        break;
                    };

                    let simple_ev = bench_event_to_simple(ev);
                    let natural_key = extract_natural_key(&simple_ev.key_id);
                    let source_id = compute_source_id(natural_key, config.num_sources);
                    events_by_source.entry(source_id).or_default().push(simple_ev);
                    loaded_count += 1;

                    if loaded_count % 500000 == 0 {
                        eprintln!("[kafka-producer] loaded {} events...", loaded_count);
                    }
                }

                let load_elapsed = load_start.elapsed();
                eprintln!(
                    "[kafka-producer] loaded {} events into {} sources in {:.2}s",
                    loaded_count, events_by_source.len(), load_elapsed.as_secs_f64()
                );

                let events_by_source = Arc::new(events_by_source);

                let mut handles = Vec::new();
                for task_idx in 0..parallel_producers {
                    let events_clone = Arc::clone(&events_by_source);
                    let config_clone = config.clone();
                    let handle = tokio::spawn(async move {
                        run_producer_task(
                            task_idx,
                            parallel_producers,
                            events_clone,
                            config_clone,
                            events_per_send,
                            rate_limit,
                        ).await
                    });
                    handles.push(handle);
                }

                let mut total_events_sent: u64 = 0;
                let mut total_batches: i64 = 0;
                for handle in handles {
                    match handle.await {
                        Ok(Ok((sent, batches))) => {
                            total_events_sent += sent;
                            total_batches += batches;
                        }
                        Ok(Err(e)) => {
                            eprintln!("[kafka-producer] task error: {}", e);
                            return Err(e);
                        }
                        Err(e) => {
                            eprintln!("[kafka-producer] task join error: {}", e);
                            return Err(anyhow::anyhow!("task join error: {}", e));
                        }
                    }
                }
                events_sent = total_events_sent;

                let total_elapsed = start_time.elapsed();
                let rate = events_sent as f64 / total_elapsed.as_secs_f64();
                eprintln!(
                    "[kafka-producer] parallel done: events={} batches={} elapsed={:.2}s rate={:.0} events/s",
                    events_sent, total_batches, total_elapsed.as_secs_f64(), rate
                );
            } else {
                // Single producer mode (original behavior)
                let mut batch: Vec<SimpleEvent> = Vec::with_capacity(events_per_send as usize);

                loop {
                    if events_sent >= total_events {
                        break;
                    }

                    let event = match &mut source {
                        EventSource::Caida(caida) => caida.next_event()?,
                        _ => unreachable!(),
                    };

                    let Some(ev) = event else {
                        break;
                    };

                    batch.push(bench_event_to_simple(ev));

                    if batch.len() >= events_per_send as usize {
                        let batch_size = batch.len() as u64;
                        producer.send_batch(batch).await?;
                        events_sent += batch_size;
                        events_since_check += batch_size;
                        batch = Vec::with_capacity(events_per_send as usize);

                        apply_rate_limit(rate_limit, &mut last_rate_check, &mut events_since_check).await;

                        if events_sent % 100000 == 0 {
                            let elapsed = start_time.elapsed();
                            let rate = events_sent as f64 / elapsed.as_secs_f64();
                            eprintln!(
                                "[kafka-producer] progress: {} events sent ({:.0} events/s)",
                                events_sent, rate
                            );
                        }
                    }
                }

                if !batch.is_empty() {
                    let batch_size = batch.len() as u64;
                    producer.send_batch(batch).await?;
                    events_sent += batch_size;
                }
            }
        }
        other => {
            anyhow::bail!(
                "unsupported BENCH_INPUT={other}; expected synthetic, tsv/google, caida, or car_emission"
            );
        }
    }

    // Flush remaining messages
    producer.flush(Duration::from_secs(30))?;

    let elapsed = start_time.elapsed();
    let rate = events_sent as f64 / elapsed.as_secs_f64();

    eprintln!(
        "[kafka-producer] done events={} batches={} elapsed={:.2}s rate={:.0} events/s",
        events_sent,
        producer.sequence(),
        elapsed.as_secs_f64(),
        rate
    );

    // Log per-source chain hashes
    let source_hashes = producer.source_chain_hashes();
    if source_hashes.len() > 1 {
        eprintln!(
            "[kafka-producer] multi-source mode: {} sources used",
            source_hashes.len()
        );
        for (source_id, state) in source_hashes {
            eprintln!(
                "[kafka-producer]   source_id={} chain_hash={} batch_seq={}",
                source_id,
                hex::encode(state.chain_hash),
                state.batch_seq
            );
        }
    } else if source_hashes.len() == 1 {
        let (source_id, state) = source_hashes.iter().next().unwrap();
        eprintln!(
            "[kafka-producer] single source_id={} chain_hash={} batch_seq={}",
            source_id,
            hex::encode(state.chain_hash),
            state.batch_seq
        );
    } else {
        eprintln!(
            "[kafka-producer] final_chain_hash={}",
            hex::encode(producer.chain_hash())
        );
    }

    Ok(())
}

fn bench_event_to_simple(ev: BenchEvent) -> SimpleEvent {
    SimpleEvent {
        ts: ev.ts,
        key_id: ev.key_id,
        value: ev.value,
    }
}

/// Extract natural key from key_id (last 8 bytes) - same as in kafka_producer.rs
fn extract_natural_key(key_id: &[u8; 15]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key_id[15 - 8..]);
    u64::from_be_bytes(bytes)
}

/// Compute source_id from natural key - same hash as in kafka_producer.rs
fn compute_source_id(natural_key: u64, num_sources: u32) -> u32 {
    let mut h = natural_key;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    (h % num_sources as u64) as u32
}

/// Run a single producer task for a subset of source_ids
async fn run_producer_task(
    task_index: u32,
    num_tasks: u32,
    events_by_source: Arc<HashMap<u32, Vec<SimpleEvent>>>,
    config: KafkaProducerConfig,
    events_per_send: u64,
    rate_limit: u64,
) -> Result<(u64, i64)> {
    let mut producer = EventBatchProducer::new(config.clone(), [0u8; 32])
        .await
        .context("create producer")?;

    let mut events_sent: u64 = 0;
    let start_time = std::time::Instant::now();
    let mut last_rate_check = std::time::Instant::now();
    let mut events_since_check: u64 = 0;

    // Process only source_ids where source_id % num_tasks == task_index
    for (&source_id, events) in events_by_source.iter() {
        if source_id % num_tasks != task_index {
            continue;
        }

        // Send events in batches
        for chunk in events.chunks(events_per_send as usize) {
            let batch: Vec<SimpleEvent> = chunk.to_vec();
            let batch_size = batch.len() as u64;
            producer.send_batch(batch).await?;
            events_sent += batch_size;
            events_since_check += batch_size;

            apply_rate_limit(rate_limit, &mut last_rate_check, &mut events_since_check).await;

            // Progress logging
            if events_sent % 100000 == 0 {
                let elapsed = start_time.elapsed();
                let rate = events_sent as f64 / elapsed.as_secs_f64();
                eprintln!(
                    "[kafka-producer-{}] progress: {} events sent ({:.0} events/s)",
                    task_index, events_sent, rate
                );
            }
        }
    }

    // Flush
    producer.flush(Duration::from_secs(30))?;

    let elapsed = start_time.elapsed();
    let rate = events_sent as f64 / elapsed.as_secs_f64();
    eprintln!(
        "[kafka-producer-{}] done events={} batches={} elapsed={:.2}s rate={:.0} events/s",
        task_index, events_sent, producer.sequence(), elapsed.as_secs_f64(), rate
    );

    Ok((events_sent, producer.sequence()))
}

async fn apply_rate_limit(
    rate_limit: u64,
    last_rate_check: &mut std::time::Instant,
    events_since_check: &mut u64,
) {
    if rate_limit > 0 {
        let elapsed = last_rate_check.elapsed();
        let expected_time =
            Duration::from_secs_f64(*events_since_check as f64 / rate_limit as f64);
        if expected_time > elapsed {
            tokio::time::sleep(expected_time - elapsed).await;
        }
        if elapsed >= Duration::from_secs(1) {
            *last_rate_check = std::time::Instant::now();
            *events_since_check = 0;
        }
    }
}
