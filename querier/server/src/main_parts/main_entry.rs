#[tokio::main]
async fn main() -> Result<()> {
    let listen: SocketAddr = std::env::var("HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8082".to_string())
        .parse()
        .context("parse HTTP_LISTEN")?;

    // Check if FDB is configured
    #[cfg(feature = "fdb")]
    let use_fdb = std::env::var("FDB_CLUSTER_FILE").is_ok();
    #[cfg(not(feature = "fdb"))]
    let use_fdb = false;

    let data_store = if use_fdb {
        #[cfg(feature = "fdb")]
        {
            eprintln!("[risc0-querier] using FoundationDB backend...");
            let fdb_store = FdbStore::open().await.context("open FDB store")?;

            // Test: verify FDB reads work
            eprintln!("[risc0-querier] testing FDB read (agg_epoch_meta)...");
            match fdb_store.agg_epoch_meta().await {
                Ok(meta) => eprintln!("[risc0-querier] FDB agg_epoch_meta OK: {} records", meta.len()),
                Err(e) => eprintln!("[risc0-querier] FDB agg_epoch_meta ERROR: {e:?}"),
            }

            eprintln!("[risc0-querier] testing FDB read (verified_samples_structs)...");
            match fdb_store.verified_samples_structs().await {
                Ok(structs) => eprintln!("[risc0-querier] FDB verified_samples_structs OK: {} structs", structs.len()),
                Err(e) => eprintln!("[risc0-querier] FDB verified_samples_structs ERROR: {e:?}"),
            }

            let fdb_store_sync = FdbStoreSync::new(fdb_store);
            DataStore::Fdb(fdb_store_sync)
        }
        #[cfg(not(feature = "fdb"))]
        {
            unreachable!()
        }
    } else {
        // Get shard paths from environment
        let primary_paths = ShardedRocksDb::paths_from_env().context("get shard paths")?;
        eprintln!(
            "[risc0-querier] opening {} RocksDB shard(s)...",
            primary_paths.len()
        );

        // Create parent directories for all shard paths
        for path in &primary_paths {
            if let Some(parent) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create parent for {}", path))?;
            }
        }

        // Check for secondary mode (comma-separated paths matching primary count)
        let secondary_paths: Option<Vec<String>> = std::env::var("ROCKSDB_SECONDARY_PATHS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            });

        let db_client = if let Some(sec_paths) = secondary_paths {
            anyhow::ensure!(
                sec_paths.len() == primary_paths.len(),
                "ROCKSDB_SECONDARY_PATHS count ({}) must match shard count ({})",
                sec_paths.len(),
                primary_paths.len()
            );
            for path in &sec_paths {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("create secondary parent for {}", path))?;
                }
            }
            eprintln!(
                "[risc0-querier] using secondary mode with {} shards",
                sec_paths.len()
            );
            ShardedRocksDb::open_shards_secondary(&primary_paths, &sec_paths)
                .context("open sharded RocksDB secondary")?
        } else {
            ShardedRocksDb::open_shards(&primary_paths).context("open sharded RocksDB")?
        };
        DataStore::RocksDb(db_client)
    };

    let state = AppState {
        db: Arc::new(Mutex::new(data_store)),
        policy: std::sync::Arc::new(build_query_policy()),
    };

    // Bench mode: run a single request without binding a socket (mirrors Nova querier).
    if let Ok(req_json) = std::env::var("BENCH_REQUEST") {
        let req: QueryRequest = serde_json::from_str(&req_json).context("parse BENCH_REQUEST")?;
        let print_resp = std::env::var("BENCH_PRINT_RESPONSE")
            .ok()
            .as_deref()
            .map(|v| v != "0")
            .unwrap_or(true);
        match query(State(state), axum::http::HeaderMap::new(), Json(req)).await {
            Ok(Json(resp)) => {
                if print_resp {
                    println!("{}", serde_json::to_string(&resp)?);
                }
                return Ok(());
            }
            Err((_code, msg)) => anyhow::bail!("{msg}"),
        }
    }

    let app = Router::new().route("/query", post(query)).with_state(state);

    println!("listening on http://{listen}/query");
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .context("bind HTTP_LISTEN")?;
    axum::serve(listener, app).await?;
    Ok(())
}
