use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use zkvm_common::{Event, KEY_BYTES_LEN};
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

/// Canonical `type` tag for the rule-based access-control screen. Mirrors the
/// `#[serde(rename_all = "snake_case")]` wire name of each `QueryRequest`
/// variant so the policy's per-kind rules line up with the request types.
fn policy_kind(req: &QueryRequest) -> &'static str {
    match req {
        QueryRequest::CmEstimate { .. } => "cm_estimate",
        QueryRequest::CmTopk { .. } => "cm_topk",
        QueryRequest::HistogramBucket { .. } => "histogram_bucket",
        QueryRequest::HistogramAll { .. } => "histogram_all",
        QueryRequest::HistogramAllKey { .. } => "histogram_all_key",
        QueryRequest::HistogramP90 { .. } => "histogram_p90",
        QueryRequest::SamplesAvg { .. } => "samples_avg",
        QueryRequest::SamplesAvgKey { .. } => "samples_avg_key",
        QueryRequest::SamplesSum { .. } => "samples_sum",
        QueryRequest::SamplesSumExactKey { .. } => "samples_sum_exact_key",
        QueryRequest::SamplesSumKey { .. } => "samples_sum_key",
        QueryRequest::SamplesSumKeyPattern { .. } => "samples_sum_key_pattern",
        QueryRequest::SamplesRawMaxKey { .. } => "samples_raw_max_key",
        QueryRequest::SamplesRawHistogramBucketKey { .. } => "samples_raw_histogram_bucket_key",
        QueryRequest::SamplesRawCmEstimateKey { .. } => "samples_raw_cm_estimate_key",
        QueryRequest::SamplesRawStatsKey { .. } => "samples_raw_stats_key",
        QueryRequest::SamplesSumTopk { .. } => "samples_sum_topk",
    }
}

/// Rule-based access-control (query blacklist) screen, applied before any query
/// executes. The whitelist is already enforced structurally by the typed
/// `QueryRequest` enum (only pre-approved guest programs are reachable); this
/// adds the complementary blacklist of membership / PII / group-by rules.
///
/// Enforcement is ON by default; set `QUERY_POLICY_ENFORCE=0` to disable it
/// (e.g. for the per-key micro-benchmarks, which intentionally issue queries the
/// default policy would reject). The deployed policy is provider-config:
/// `QUERY_POLICY_MIN_ANONYMITY_BITS` (membership $k$-anonymity threshold) and
/// `QUERY_POLICY_ALLOW_ANON_IDS=1` tune it.
///
/// Note: the low-support rule is enforced at runtime *inside the guest* (see
/// `MIN_SUPPORT`); here `support` is `None`, so this static screen only checks
/// the membership / PII-output / group-by rules.
/// Build the access-control policy ONCE at startup from the environment, so we
/// don't re-read env vars and rebuild the policy (allocating a BTreeSet) on
/// every request. Returns `None` when enforcement is disabled (the cached
/// `enforce_query_policy` short-circuits to `Ok(())` in that case).
///
/// Preserves the exact prior semantics: enforcement is ON by default
/// (`QUERY_POLICY_ENFORCE=0` or empty => disabled => `None`);
/// `QUERY_POLICY_MIN_ANONYMITY_BITS` (parsed u32) overrides the default;
/// `QUERY_POLICY_ALLOW_ANON_IDS=1` enables anonymized-id outputs;
/// `allow_anonymized_ids` defaults to false otherwise.
pub(crate) fn build_query_policy() -> Option<query_checker::QueryPolicy> {
    let enforce = std::env::var("QUERY_POLICY_ENFORCE")
        .map(|v| v != "0" && !v.is_empty())
        .unwrap_or(true);
    if !enforce {
        return None;
    }

    let mut policy = query_checker::QueryPolicy::default();
    // The k-anonymity threshold for membership detection (rule 1).
    if let Ok(k) = std::env::var("QUERY_POLICY_MIN_ANONYMITY_BITS")
        .unwrap_or_default()
        .parse::<u32>()
    {
        policy.min_anonymity_bits = k;
    }
    if std::env::var("QUERY_POLICY_ALLOW_ANON_IDS").map(|v| v == "1").unwrap_or(false) {
        policy.allow_anonymized_ids = true;
    }
    Some(policy)
}

fn enforce_query_policy(
    req: &QueryRequest,
    policy: &Option<query_checker::QueryPolicy>,
) -> Result<(), (axum::http::StatusCode, String)> {
    // Enforcement disabled (QUERY_POLICY_ENFORCE=0/empty at startup).
    let policy = match policy {
        Some(p) => p,
        None => return Ok(()),
    };

    // Note: low-support (rule 2) is NOT screened here. It is enforced inside the
    // guest via the compiled-in `MIN_SUPPORT`, so `support` is left `None` below
    // and the static low-support rule never fires. We therefore deliberately do
    // not expose a server-side min-support knob (it would be a no-op).

    // Extract the key-predicate parameters so the policy can bound selectivity
    // (membership detection). Mask defaults to all-ones (exact key) when absent.
    let mask = match req {
        QueryRequest::SamplesAvgKey { mask, .. } | QueryRequest::SamplesSumKey { mask, .. } => {
            mask.unwrap_or(u64::MAX)
        }
        QueryRequest::SamplesRawMaxKey { mask, .. }
        | QueryRequest::SamplesRawHistogramBucketKey { mask, .. }
        | QueryRequest::SamplesRawCmEstimateKey { mask, .. }
        | QueryRequest::SamplesRawStatsKey { mask, .. } => *mask,
        _ => u64::MAX,
    };
    let pattern = match req {
        QueryRequest::HistogramAllKey { pattern, .. }
        | QueryRequest::SamplesSumKeyPattern { pattern, .. } => Some(pattern.clone()),
        _ => None,
    };

    let creq = query_checker::QueryRequest {
        kind: policy_kind(req).to_string(),
        key: 0,
        mask,
        bucket: 0,
        value: 0,
        group_by: Vec::new(),
        support: None,
        pattern,
    };
    let report = query_checker::check_query_request(&creq, policy);
    if !report.ok {
        let detail = report
            .violations
            .iter()
            .map(|v| format!("{}: {}", v.kind.as_str(), v.detail))
            .collect::<Vec<_>>()
            .join("; ");
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            format!("query rejected by access-control policy: {detail}"),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct DbRow {
    values: Vec<DbValue>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum DbValue {
    I64(i64),
    I32(i32),
    I16(i16),
    Bytes(Vec<u8>),
    Epoch(EpochType),
}

trait FromDbValue: Sized {
    fn from_db_value(v: &DbValue) -> Self;
}

impl FromDbValue for i64 {
    fn from_db_value(v: &DbValue) -> Self {
        match v {
            DbValue::I64(x) => *x,
            _ => panic!("DbValue type mismatch for i64"),
        }
    }
}

impl FromDbValue for i32 {
    fn from_db_value(v: &DbValue) -> Self {
        match v {
            DbValue::I32(x) => *x,
            _ => panic!("DbValue type mismatch for i32"),
        }
    }
}

impl FromDbValue for i16 {
    fn from_db_value(v: &DbValue) -> Self {
        match v {
            DbValue::I16(x) => *x,
            _ => panic!("DbValue type mismatch for i16"),
        }
    }
}

impl FromDbValue for Vec<u8> {
    fn from_db_value(v: &DbValue) -> Self {
        match v {
            DbValue::Bytes(x) => x.clone(),
            _ => panic!("DbValue type mismatch for Vec<u8>"),
        }
    }
}

impl FromDbValue for EpochType {
    fn from_db_value(v: &DbValue) -> Self {
        match v {
            DbValue::Epoch(x) => *x,
            _ => panic!("DbValue type mismatch for EpochType"),
        }
    }
}

impl DbRow {
    fn new(values: Vec<DbValue>) -> Self {
        Self { values }
    }

    fn get<T: FromDbValue>(&self, idx: usize) -> T {
        T::from_db_value(&self.values[idx])
    }
}

enum EpochSelect {
    TimeWindow { start_ms: i64, end_ms: i64 },
    Latest { epochs: i64 },
}

/// Convert u64 to [u8; KEY_BYTES_LEN] key representation
fn u64_to_key(val: u64) -> [u8; KEY_BYTES_LEN] {
    Event::key_id_from_u64(val)
}

/// Convert optional u64 mask to key representation (default: all 1s for exact match)
#[allow(dead_code)]
fn u64_to_mask(val: Option<u64>) -> [u8; KEY_BYTES_LEN] {
    Event::key_id_from_u64(val.unwrap_or(u64::MAX))
}

/// Extract u64 from [u8; KEY_BYTES_LEN] key (lower 8 bytes, big-endian)
fn key_to_u64(key_id: &[u8; KEY_BYTES_LEN]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key_id[KEY_BYTES_LEN - 8..]);
    u64::from_be_bytes(bytes)
}

fn epoch_select(tw: &TimeWindow) -> anyhow::Result<EpochSelect> {
    if let Some(epochs) = tw.epochs {
        anyhow::ensure!(epochs > 0, "epochs must be > 0");
        return Ok(EpochSelect::Latest {
            epochs: epochs as i64,
        });
    }
    let (start_ms, end_ms) = resolve_time_window(tw)?;
    Ok(EpochSelect::TimeWindow { start_ms, end_ms })
}

async fn latest_sequences_agg_epochs(
    db: &DataStore,
    epoch_type: EpochType,
    epochs: i64,
) -> Result<Vec<i64>, anyhow::Error> {
    let mut sequences: Vec<i64> = db
        .agg_epochs()?
        .into_iter()
        .filter(|e| e.epoch_type == epoch_type)
        .map(|e| e.sequence)
        .collect();
    sequences.sort_unstable();
    sequences.dedup();
    sequences.sort_by(|a, b| b.cmp(a));
    sequences.truncate(epochs.max(0) as usize);
    Ok(sequences)
}

async fn latest_sequences_verified_samples(
    db: &DataStore,
    epochs: i64,
) -> Result<Vec<i64>, anyhow::Error> {
    let mut sequences: Vec<i64> = db
        .verified_samples_structs()?
        .into_iter()
        .map(|v| v.sequence)
        .collect();
    sequences.sort_unstable();
    sequences.dedup();
    sequences.sort_by(|a, b| b.cmp(a));
    sequences.truncate(epochs.max(0) as usize);
    Ok(sequences)
}

async fn latest_sequences_sample_events(
    db: &DataStore,
    epochs: i64,
) -> Result<Vec<i64>, anyhow::Error> {
    let mut sequences: Vec<i64> = db.sample_events()?.into_iter().map(|e| e.sequence).collect();
    sequences.sort_unstable();
    sequences.dedup();
    sequences.sort_by(|a, b| b.cmp(a));
    sequences.truncate(epochs.max(0) as usize);
    Ok(sequences)
}

fn rows_sample_events_by_time(
    db: &DataStore,
    start_ms: i64,
    end_ms: i64,
    limit: i64,
) -> Result<Vec<DbRow>, anyhow::Error> {
    let mut rows: Vec<(i64, i64, i32, DbRow)> = db
        .sample_events()?
        .into_iter()
        .filter(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms)
        .map(|e| {
            let row = DbRow::new(vec![DbValue::I64(key_to_u64(&e.key_id) as i64), DbValue::I32(e.value as i32)]);
            (e.ingest_time_ms, e.sequence, e.idx, row)
        })
        .collect();
    rows.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    let mut out: Vec<DbRow> = rows.into_iter().map(|(_, _, _, row)| row).collect();
    if limit >= 0 {
        out.truncate(limit as usize);
    }
    Ok(out)
}

fn rows_sample_events_by_sequences(
    db: &DataStore,
    sequences: &[i64],
    limit: i64,
) -> Result<Vec<DbRow>, anyhow::Error> {
    let mut rows: Vec<(i64, i64, i32, DbRow)> = db
        .sample_events()?
        .into_iter()
        .filter(|e| sequences.contains(&e.sequence))
        .map(|e| {
            let row = DbRow::new(vec![DbValue::I64(key_to_u64(&e.key_id) as i64), DbValue::I32(e.value as i32)]);
            (e.ingest_time_ms, e.sequence, e.idx, row)
        })
        .collect();
    rows.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    let mut out: Vec<DbRow> = rows.into_iter().map(|(_, _, _, row)| row).collect();
    if limit >= 0 {
        out.truncate(limit as usize);
    }
    Ok(out)
}

#[allow(dead_code)]
fn rows_agg_cm_by_time(
    db: &DataStore,
    epoch_type: EpochType,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<DbRow>, anyhow::Error> {
    let structs = db
        .agg_cm_structs()?
        .into_iter()
        .map(|s| (s.sequence, s))
        .collect::<HashMap<_, _>>();
    let mut rows: Vec<(i64, DbRow)> = db
        .agg_epochs()?
        .into_iter()
        .filter(|e| e.epoch_type == epoch_type && e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms)
        .filter_map(|e| {
            let s = structs.get(&e.sequence)?;
            let row = DbRow::new(vec![
                DbValue::Bytes(e.result_commit.clone()),
                DbValue::Bytes(s.counts_u32.clone()),
                DbValue::Bytes(s.heap_fixed.clone()),
            ]);
            Some((e.sequence, row))
        })
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

#[allow(dead_code)]
fn rows_agg_cm_by_sequences(
    db: &DataStore,
    epoch_type: EpochType,
    sequences: &[i64],
) -> Result<Vec<DbRow>, anyhow::Error> {
    let structs = db
        .agg_cm_structs()?
        .into_iter()
        .map(|s| (s.sequence, s))
        .collect::<HashMap<_, _>>();
    let mut rows: Vec<(i64, DbRow)> = db
        .agg_epochs()?
        .into_iter()
        .filter(|e| e.epoch_type == epoch_type && sequences.contains(&e.sequence))
        .filter_map(|e| {
            let s = structs.get(&e.sequence)?;
            let row = DbRow::new(vec![
                DbValue::Bytes(e.result_commit.clone()),
                DbValue::Bytes(s.counts_u32.clone()),
                DbValue::Bytes(s.heap_fixed.clone()),
            ]);
            Some((e.sequence, row))
        })
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

#[allow(dead_code)]
fn rows_agg_hist_by_time(
    db: &DataStore,
    epoch_type: EpochType,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<DbRow>, anyhow::Error> {
    let structs = db
        .agg_hist_structs()?
        .into_iter()
        .map(|s| (s.sequence, s))
        .collect::<HashMap<_, _>>();
    let mut rows: Vec<(i64, DbRow)> = db
        .agg_epochs()?
        .into_iter()
        .filter(|e| e.epoch_type == epoch_type && e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms)
        .filter_map(|e| {
            let s = structs.get(&e.sequence)?;
            let row = DbRow::new(vec![
                DbValue::Bytes(e.result_commit.clone()),
                DbValue::I64(s.total_count as i64),
                DbValue::I64(s.total_sum as i64),
                DbValue::Bytes(s.table_fixed.clone()),
            ]);
            Some((e.sequence, row))
        })
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

#[allow(dead_code)]
fn rows_agg_hist_by_sequences(
    db: &DataStore,
    epoch_type: EpochType,
    sequences: &[i64],
) -> Result<Vec<DbRow>, anyhow::Error> {
    let structs = db
        .agg_hist_structs()?
        .into_iter()
        .map(|s| (s.sequence, s))
        .collect::<HashMap<_, _>>();
    let mut rows: Vec<(i64, DbRow)> = db
        .agg_epochs()?
        .into_iter()
        .filter(|e| e.epoch_type == epoch_type && sequences.contains(&e.sequence))
        .filter_map(|e| {
            let s = structs.get(&e.sequence)?;
            let row = DbRow::new(vec![
                DbValue::Bytes(e.result_commit.clone()),
                DbValue::I64(s.total_count as i64),
                DbValue::I64(s.total_sum as i64),
                DbValue::Bytes(s.table_fixed.clone()),
            ]);
            Some((e.sequence, row))
        })
        .collect();
    rows.sort_by_key(|(seq, _)| *seq);
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

#[allow(dead_code)]
fn rows_verified_samples_by_time(
    db: &DataStore,
    start_ms: i64,
    end_ms: i64,
    include_table_fixed: bool,
) -> Result<Vec<DbRow>, anyhow::Error> {
    let mut rows: Vec<DbRow> = db
        .verified_samples_structs()?
        .into_iter()
        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
        .map(|v| {
            let mut values = vec![
                DbValue::I64(v.sequence),
                DbValue::I64(v.total_count as i64),
                DbValue::I64(v.total_sum as i64),
            ];
            if include_table_fixed {
                values.push(DbValue::Bytes(v.table_fixed.unwrap_or_default()));
            }
            DbRow::new(values)
        })
        .collect();
    rows.sort_by(|a, b| {
        let seq_a: i64 = a.get(0);
        let seq_b: i64 = b.get(0);
        seq_a.cmp(&seq_b)
    });
    Ok(rows)
}

#[allow(dead_code)]
fn rows_verified_samples_by_sequences(
    db: &DataStore,
    sequences: &[i64],
    include_table_fixed: bool,
) -> Result<Vec<DbRow>, anyhow::Error> {
    let mut rows: Vec<DbRow> = db
        .verified_samples_structs()?
        .into_iter()
        .filter(|v| sequences.contains(&v.sequence))
        .map(|v| {
            let mut values = vec![
                DbValue::I64(v.sequence),
                DbValue::I64(v.total_count as i64),
                DbValue::I64(v.total_sum as i64),
            ];
            if include_table_fixed {
                values.push(DbValue::Bytes(v.table_fixed.unwrap_or_default()));
            }
            DbRow::new(values)
        })
        .collect();
    rows.sort_by(|a, b| {
        let seq_a: i64 = a.get(0);
        let seq_b: i64 = b.get(0);
        seq_a.cmp(&seq_b)
    });
    Ok(rows)
}

fn get_verified_samples_structs_by_sequences(
    db: &DataStore,
    sequences: &[i64],
) -> Result<Vec<VerifiedSamplesStruct>, anyhow::Error> {
    let mut structs: Vec<VerifiedSamplesStruct> = db
        .verified_samples_structs()?
        .into_iter()
        .filter(|v| sequences.contains(&v.sequence))
        .collect();
    // Distributed: epochs come from multiple independent per-aggregator chains,
    // each rooted at the genesis hash. Order by (aggregator_id, sequence) so each
    // chain is contiguous and in sequence order for chain-linkage verification.
    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
    Ok(structs)
}

fn get_agg_hist_structs_by_sequences(
    db: &DataStore,
    sequences: &[i64],
) -> Result<Vec<AggHistStruct>, anyhow::Error> {
    let mut structs: Vec<AggHistStruct> = db
        .agg_hist_structs()?
        .into_iter()
        .filter(|h| sequences.contains(&h.sequence))
        .collect();
    structs.sort_by_key(|h| h.sequence);
    Ok(structs)
}

fn get_agg_cm_structs_by_sequences(
    db: &DataStore,
    sequences: &[i64],
) -> Result<Vec<AggCmStruct>, anyhow::Error> {
    let mut structs: Vec<AggCmStruct> = db
        .agg_cm_structs()?
        .into_iter()
        .filter(|c| sequences.contains(&c.sequence))
        .collect();
    structs.sort_by_key(|c| c.sequence);
    Ok(structs)
}

#[allow(dead_code)]
fn count_distinct_seq(rows: &[DbRow]) -> usize {
    let mut last: Option<i64> = None;
    let mut n = 0usize;
    for r in rows {
        let seq: i64 = r.get(0);
        if last != Some(seq) {
            n += 1;
            last = Some(seq);
        }
    }
    n
}

async fn query(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (axum::http::StatusCode, String)> {
    let omit_proof_hex = headers
        .get("x-no-proof")
        .and_then(|v| v.to_str().ok())
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let omit_proof_hex = omit_proof_hex
        || std::env::var("OMIT_PROOF_HEX").ok().as_deref() == Some("1");

    // Rule-based access-control screen (opt-in via QUERY_POLICY_ENFORCE=1),
    // applied before the query executes.
    enforce_query_policy(&req, &state.policy)?;

    // For now we only implement the "aggregated DB" paths. Proofs are empty.
    let use_direct = false;

    match req {
        QueryRequest::CmEstimate { tw, key } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<AggCmStruct> = db
                        .agg_cm_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|c| {
                            let epoch = db.agg_epochs().ok().and_then(|epochs| {
                                epochs.into_iter().find(|e| e.epoch_type == EpochType::CmEpoch && e.sequence == c.sequence)
                            });
                            epoch.map(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms).unwrap_or(false)
                        })
                        .collect();
                    structs.sort_by_key(|c| c.sequence);
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_agg_epochs(&db, EpochType::CmEpoch, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_agg_cm_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let pos: [usize; acore::CM_ROWS] =
                std::array::from_fn(|r| cm_bucket_index(key, r) as usize);
            let mut sums = [0u64; acore::CM_ROWS];
            for s in &structs {
                let counts = unpack_cm_counts_u32(&s.counts_u32).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
                for rr in 0..acore::CM_ROWS {
                    sums[rr] = sums[rr].saturating_add(counts[rr * CM_LEAVES + pos[rr]] as u64);
                }
            }
            let estimate = *sums.iter().min().unwrap_or(&0);
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("cm_estimate", "cm_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::CmEpochState> = structs.iter()
                .map(|s| build_cm_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::cm_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_cm(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::CmQueryInput {
                query: qcore::CmQuery::Estimate { key: u64_to_key(key) },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            // Derive expected from the SAME core function the guest runs on the
            // SAME input, guaranteeing journal == expected by construction.
            let expected = qcore::run_cm_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "cm_estimate",
                &proof_in,
                &expected,
                QUERIER_CM_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_CM_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = estimate;
            let estimate = match &expected.result {
                qcore::CmQueryResult::Estimate { estimate } => *estimate,
                _ => unreachable!("cm_estimate query produced non-Estimate result"),
            };
            Ok(Json(QueryResponse::CmEstimate {
                estimate,
                dp_offset: dp_offset(dp::DP_CM_ESTIMATE),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::CmTopk { tw, limit } => {
            let limit = limit
                .unwrap_or(acore::CM_TOPK_SLOTS)
                .min(acore::CM_TOPK_SLOTS);
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<AggCmStruct> = db
                        .agg_cm_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|c| {
                            let epoch = db.agg_epochs().ok().and_then(|epochs| {
                                epochs.into_iter().find(|e| e.epoch_type == EpochType::CmEpoch && e.sequence == c.sequence)
                            });
                            epoch.map(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms).unwrap_or(false)
                        })
                        .collect();
                    structs.sort_by_key(|c| c.sequence);
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_agg_epochs(&db, EpochType::CmEpoch, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_agg_cm_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let want = acore::CM_ROWS * CM_LEAVES;
            let mut sum_counts = vec![0u64; want];
            let mut candidates: HashSet<[u8; 15]> = HashSet::new();
            for s in &structs {
                let counts = unpack_cm_counts_u32(&s.counts_u32).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
                for (i, v) in counts.into_iter().enumerate() {
                    sum_counts[i] = sum_counts[i].saturating_add(v as u64);
                }
                let heap = unpack_cm_heap_fixed(&s.heap_fixed).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
                for (k, _v, o) in heap {
                    const ZERO_KEY: [u8; 15] = [0u8; 15];
                    if o == 1 && k != ZERO_KEY {
                        candidates.insert(k);
                    }
                }
            }

            let mut items: Vec<TopKItem> = candidates
                .into_par_iter()
                .map(|k| {
                    let pos: [usize; acore::CM_ROWS] =
                        std::array::from_fn(|r| acore::cm_bucket_index(&k, r) as usize);
                    let mut min_v = u64::MAX;
                    for rr in 0..acore::CM_ROWS {
                        let v = sum_counts[rr * CM_LEAVES + pos[rr]];
                        min_v = min_v.min(v);
                    }
                    TopKItem { key: k, count: if min_v == u64::MAX { 0 } else { min_v } }
                })
                .collect();
            items.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.key.cmp(&b.key)));
            items.truncate(limit);

            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("cm_topk", "cm_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::CmEpochState> = structs.iter()
                .map(|s| build_cm_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::cm_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_cm(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::CmQueryInput {
                query: qcore::CmQuery::Topk { limit: limit as u16 },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_cm_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "cm_topk",
                &proof_in,
                &expected,
                QUERIER_CM_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_CM_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Source counts from the proven result (sorted desc, same as `items`);
            // on suppression the proven list is empty. Keys come from the server
            // side (the proof omits keys for privacy); zip preserves the
            // count-descending order so non-suppressed JSON is unchanged.
            let items: Vec<TopKItem> = match &expected.result {
                qcore::CmQueryResult::Topk { items: proven } => items
                    .into_iter()
                    .zip(proven.iter())
                    .map(|(it, p)| TopKItem { key: it.key, count: p.count })
                    .collect(),
                _ => unreachable!("cm_topk query produced non-Topk result"),
            };
            Ok(Json(QueryResponse::CmTopk {
                items,
                dp_offset_count: dp_offset(dp::DP_CM_TOPK),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::HistogramBucket { tw, bucket } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<AggHistStruct> = db
                        .agg_hist_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|h| {
                            let epoch = db.agg_epochs().ok().and_then(|epochs| {
                                epochs.into_iter().find(|e| e.epoch_type == EpochType::HistogramEpoch && e.sequence == h.sequence)
                            });
                            epoch.map(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms).unwrap_or(false)
                        })
                        .collect();
                    structs.sort_by_key(|h| h.sequence);
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_agg_epochs(&db, EpochType::HistogramEpoch, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_agg_hist_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let mut count: u64 = 0;
            for s in &structs {
                // Unpack per-key histograms and aggregate bucket counts across all keys
                let per_key_data = unpack_hist_per_key_table_fixed(&s.table_fixed)
                    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

                for (_key_id, bucket_counts, _total_count, _total_sum) in per_key_data {
                    let idx = bucket as usize;
                    if idx < bucket_counts.len() {
                        count = count.saturating_add(bucket_counts[idx] as u64);
                    }
                }
            }
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("histogram_bucket", "histogram_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::HistogramEpochState> = structs.iter()
                .map(|s| build_histogram_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::histogram_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_hist(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::HistogramQueryInput {
                query: qcore::HistogramQuery::Bucket { bucket },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_histogram_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "histogram_bucket",
                &proof_in,
                &expected,
                QUERIER_HISTOGRAM_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_HISTOGRAM_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = count;
            let count = match &expected.result {
                qcore::HistogramQueryResult::Bucket { count, .. } => *count,
                _ => unreachable!("histogram_bucket query produced non-Bucket result"),
            };
            Ok(Json(QueryResponse::HistogramBucket {
                bucket,
                count,
                dp_offset: dp_offset(dp::DP_HIST_BUCKET),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::HistogramAll { tw } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<AggHistStruct> = db
                        .agg_hist_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|h| {
                            let epoch = db.agg_epochs().ok().and_then(|epochs| {
                                epochs.into_iter().find(|e| e.epoch_type == EpochType::HistogramEpoch && e.sequence == h.sequence)
                            });
                            epoch.map(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms).unwrap_or(false)
                        })
                        .collect();
                    structs.sort_by_key(|h| h.sequence);
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_agg_epochs(&db, EpochType::HistogramEpoch, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_agg_hist_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let mut total_count: u64 = 0;
            let mut total_sum: u64 = 0;
            let mut buckets_acc = vec![0u64; acore::HISTOGRAM_SLOTS];
            for s in &structs {
                // Unpack per-key histograms and aggregate across all keys
                let per_key_data = unpack_hist_per_key_table_fixed(&s.table_fixed)
                    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

                for (_key_id, bucket_counts, key_count, key_sum) in per_key_data {
                    total_count = total_count.saturating_add(key_count);
                    total_sum = total_sum.saturating_add(key_sum);
                    for (i, c) in bucket_counts.into_iter().enumerate() {
                        buckets_acc[i] = buckets_acc[i].saturating_add(c as u64);
                    }
                }
            }
            let buckets: Vec<HistogramBucketItem> = buckets_acc
                .into_iter()
                .enumerate()
                .filter(|(_i, c)| *c > 0)
                .map(|(i, c)| HistogramBucketItem { bucket: i as u16, count: c })
                .collect();
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("histogram_all", "histogram_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::HistogramEpochState> = structs.iter()
                .map(|s| build_histogram_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::histogram_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_hist(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::HistogramQueryInput {
                query: qcore::HistogramQuery::All,
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_histogram_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "histogram_all",
                &proof_in,
                &expected,
                QUERIER_HISTOGRAM_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_HISTOGRAM_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = (&buckets, total_count, total_sum);
            let (total_count, total_sum, buckets) = match &expected.result {
                qcore::HistogramQueryResult::All { total_count, total_sum, buckets } => (
                    *total_count,
                    *total_sum,
                    buckets
                        .iter()
                        .map(|b| HistogramBucketItem { bucket: b.bucket, count: b.count })
                        .collect::<Vec<_>>(),
                ),
                _ => unreachable!("histogram_all query produced non-All result"),
            };
            Ok(Json(QueryResponse::HistogramAll {
                total_count,
                total_sum,
                buckets,
                dp_offset_total_count: dp_offset(dp::DP_HIST_TOTAL_COUNT),
                dp_offset_total_sum: dp_offset(dp::DP_HIST_TOTAL_SUM),
                dp_offset_bucket_count: dp_offset(dp::DP_HIST_BUCKET_COUNT),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::HistogramAllKey { tw, ref pattern } => {
            let (key_pat, mask_pat) = parse_key_pattern_15(pattern)
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, format!("bad key pattern: {e}")))?;
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<AggHistStruct> = db
                        .agg_hist_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|h| {
                            let epoch = db.agg_epochs().ok().and_then(|epochs| {
                                epochs.into_iter().find(|e| e.epoch_type == EpochType::HistogramEpoch && e.sequence == h.sequence)
                            });
                            epoch.map(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms).unwrap_or(false)
                        })
                        .collect();
                    structs.sort_by_key(|h| h.sequence);
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_agg_epochs(&db, EpochType::HistogramEpoch, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_agg_hist_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let mut total_count: u64 = 0;
            let mut total_sum: u64 = 0;
            let mut buckets_acc = vec![0u64; acore::HISTOGRAM_SLOTS];
            for s in &structs {
                let per_key_data = unpack_hist_per_key_table_fixed(&s.table_fixed)
                    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

                for (key_id, bucket_counts, key_count, key_sum) in per_key_data {
                    // Filter by key pattern: (key_id[i] & mask[i]) == (key[i] & mask[i])
                    let mut matches = true;
                    for i in 0..15 {
                        if (key_id[i] & mask_pat[i]) != (key_pat[i] & mask_pat[i]) {
                            matches = false;
                            break;
                        }
                    }
                    if !matches {
                        continue;
                    }
                    total_count = total_count.saturating_add(key_count);
                    total_sum = total_sum.saturating_add(key_sum);
                    for (i, c) in bucket_counts.into_iter().enumerate() {
                        buckets_acc[i] = buckets_acc[i].saturating_add(c as u64);
                    }
                }
            }
            let buckets: Vec<HistogramBucketItem> = buckets_acc
                .into_iter()
                .enumerate()
                .filter(|(_i, c)| *c > 0)
                .map(|(i, c)| HistogramBucketItem { bucket: i as u16, count: c })
                .collect();
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("histogram_all_key", "histogram_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::HistogramEpochState> = structs.iter()
                .map(|s| build_histogram_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::histogram_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_hist(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::HistogramQueryInput {
                query: qcore::HistogramQuery::AllKey { key: key_pat, mask: mask_pat },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_histogram_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "histogram_all_key",
                &proof_in,
                &expected,
                QUERIER_HISTOGRAM_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_HISTOGRAM_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = (&buckets, total_count, total_sum);
            let (total_count, total_sum, buckets) = match &expected.result {
                qcore::HistogramQueryResult::AllKey { total_count, total_sum, buckets, .. } => (
                    *total_count,
                    *total_sum,
                    buckets
                        .iter()
                        .map(|b| HistogramBucketItem { bucket: b.bucket, count: b.count })
                        .collect::<Vec<_>>(),
                ),
                _ => unreachable!("histogram_all_key query produced non-AllKey result"),
            };
            Ok(Json(QueryResponse::HistogramAllKey {
                pattern: pattern.clone(),
                total_count,
                total_sum,
                buckets,
                dp_offset_total_count: dp_offset(dp::DP_HIST_TOTAL_COUNT),
                dp_offset_total_sum: dp_offset(dp::DP_HIST_TOTAL_SUM),
                dp_offset_bucket_count: dp_offset(dp::DP_HIST_BUCKET_COUNT),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::HistogramP90 { tw } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch histogram structs
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<AggHistStruct> = db
                        .agg_hist_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|h| {
                            let epoch = db.agg_epochs().ok().and_then(|epochs| {
                                epochs.into_iter().find(|e| e.epoch_type == EpochType::HistogramEpoch && e.sequence == h.sequence)
                            });
                            epoch.map(|e| e.ingest_time_ms >= start_ms && e.ingest_time_ms <= end_ms).unwrap_or(false)
                        })
                        .collect();
                    structs.sort_by_key(|h| h.sequence);
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_agg_epochs(&db, EpochType::HistogramEpoch, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_agg_hist_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();

            // Aggregate all histogram buckets from per-key histograms
            let mut total_count: u64 = 0;
            let mut buckets_acc = vec![0u64; acore::HISTOGRAM_SLOTS];
            for s in &structs {
                // Unpack per-key histograms and aggregate across all keys
                let per_key_data = unpack_hist_per_key_table_fixed(&s.table_fixed)
                    .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

                for (_key_id, bucket_counts, key_count, _key_sum) in per_key_data {
                    total_count = total_count.saturating_add(key_count);
                    for (i, c) in bucket_counts.into_iter().enumerate() {
                        buckets_acc[i] = buckets_acc[i].saturating_add(c as u64);
                    }
                }
            }

            // Calculate P90 from histogram buckets
            let target_count = (total_count as f64 * 0.90) as u64;
            let mut cumulative_count = 0u64;
            let mut p90_value = 0u64;

            for (bucket_idx, &count) in buckets_acc.iter().enumerate() {
                cumulative_count = cumulative_count.saturating_add(count);
                if cumulative_count >= target_count {
                    // Estimate value within bucket: use lower bound of bucket range
                    // Bucket i contains values in range [2^(i-1), 2^i) for i >= 1
                    // Bucket 0 contains value 0
                    if bucket_idx == 0 {
                        p90_value = 0;
                    } else if bucket_idx == 1 {
                        p90_value = 1;
                    } else {
                        // Use lower bound: 2^(bucket_idx - 1)
                        p90_value = 1u64 << (bucket_idx - 1);
                    }
                    break;
                }
            }

            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("histogram_p90", "histogram_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::HistogramEpochState> = structs.iter()
                .map(|s| build_histogram_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::histogram_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_hist(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::HistogramQueryInput {
                query: qcore::HistogramQuery::P90,
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_histogram_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "histogram_p90",
                &proof_in,
                &expected,
                QUERIER_HISTOGRAM_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_HISTOGRAM_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = (p90_value, total_count);
            let (p90_value, total_count) = match &expected.result {
                qcore::HistogramQueryResult::P90 { p90_value, total_count } => {
                    (*p90_value, *total_count)
                }
                _ => unreachable!("histogram_p90 query produced non-P90 result"),
            };
            Ok(Json(QueryResponse::HistogramP90 {
                p90_value,
                total_count,
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesAvg { ref tw } | QueryRequest::SamplesSum { ref tw } => {
            let is_sum = matches!(&req, QueryRequest::SamplesSum { .. });
            let query_kind = if is_sum { "samples_sum" } else { "samples_avg" };
            let select = epoch_select(tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<VerifiedSamplesStruct> = db
                        .verified_samples_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
                        .collect();
                    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_verified_samples(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_verified_samples_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let mut total_count: u64 = 0;
            let mut total_sum: u64 = 0;

            for s in &structs {
                total_count = total_count.saturating_add(s.total_count);
                total_sum = total_sum.saturating_add(s.total_sum);
            }
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log(query_kind, "samples_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build complete epoch states from DB for cryptographic integrity verification
            let epoch_states: Vec<acore::SamplesEpochState> = structs.iter()
                .map(|s| build_samples_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits from rebuilt states and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::samples_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_samples(&structs, &state_commits)
            } else {
                Vec::new()
            };


            if is_sum {
                let proof_in = qcore::SamplesQueryInput {
                    query: qcore::SamplesQuery::Sum,
                    epoch_states: epoch_states.clone(),
                    epoch_chain_links: epoch_chain_links.clone(),
                };
                let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
                let proof = prove_risc0(
                    "samples_sum",
                    &proof_in,
                    &expected,
                    QUERIER_SAMPLES_ELF,
                    risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                    omit_proof_hex,
                )
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

                let sum = match &expected.result {
                    qcore::SamplesQueryResult::Sum { sum } => *sum,
                    _ => unreachable!("samples_sum query produced non-Sum result"),
                };
                return Ok(Json(QueryResponse::SamplesSum {
                    sum,
                    dp_offset_sum: dp_offset(dp::DP_SAMPLES_SUM),
                    suppressed: expected.suppressed,
                    proof,
                }));
            }
            let avg = if total_count == 0 { 0 } else { total_sum / total_count };
            let proof_in = qcore::SamplesQueryInput {
                query: qcore::SamplesQuery::Avg,
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "samples_avg",
                &proof_in,
                &expected,
                QUERIER_SAMPLES_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = avg;
            let avg = match &expected.result {
                qcore::SamplesQueryResult::Avg { avg } => *avg,
                _ => unreachable!("samples_avg query produced non-Avg result"),
            };
            Ok(Json(QueryResponse::SamplesAvg {
                avg,
                dp_offset_sum: dp_offset(dp::DP_SAMPLES_AVG),
                dp_offset_count: dp_offset(dp::DP_SAMPLES_COUNT),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesAvgKey { ref tw, key, mask } => {
            let query_kind = "samples_avg_key";
            let mask = mask.unwrap_or(u64::MAX);
            let select = epoch_select(tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<VerifiedSamplesStruct> = db
                        .verified_samples_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
                        .collect();
                    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_verified_samples(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_verified_samples_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let query_masked = key & mask;
            let mut match_keys_total: u64 = 0;
            let mut sum_total: u64 = 0;
            let mut count_total: u64 = 0;
            for s in &structs {
                let table_fixed = match &s.table_fixed {
                    Some(tf) => tf.as_slice(),
                    None => &[],
                };
                if table_fixed.is_empty() {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "verified_samples_struct missing table_fixed (needed for per-key queries)".to_string()));
                }
                // Inline unpack: (key_id[15], tip32[32], count[8], sum[8], occ[1]) per slot = 64 bytes
                if table_fixed.len() % 64 != 0 {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "samples table_fixed length not multiple of 64".to_string()));
                }
                let mut i = 0usize;
                while i < table_fixed.len() {
                    let slot_key = u64::from_be_bytes(table_fixed[i + 7..i + 15].try_into().unwrap());
                    let len = u64::from_be_bytes(table_fixed[i + 47..i + 55].try_into().unwrap());
                    let sum = u64::from_be_bytes(table_fixed[i + 55..i + 63].try_into().unwrap());
                    let occ = table_fixed[i + 63];
                    if occ != 0 && occ != 1 {
                        return Err((axum::http::StatusCode::BAD_REQUEST, "samples occ must be 0/1".to_string()));
                    }
                    i += 64;
                    if occ == 0 {
                        continue;
                    }
                    if (slot_key & mask) == query_masked {
                        match_keys_total += 1;
                        sum_total = sum_total.saturating_add(sum);
                        count_total = count_total.saturating_add(len);
                    }
                }
            }
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log(query_kind, "samples_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            let avg = if count_total == 0 { 0 } else { sum_total / count_total };

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::SamplesEpochState> = structs.iter()
                .map(|s| build_samples_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::samples_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_samples(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::SamplesQueryInput {
                query: qcore::SamplesQuery::AvgKey { key: u64_to_key(key), mask: u64_to_key(mask) },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "samples_avg_key",
                &proof_in,
                &expected,
                QUERIER_SAMPLES_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let _ = avg;
            let avg = match &expected.result {
                qcore::SamplesQueryResult::AvgKey { avg } => *avg,
                _ => unreachable!("samples_avg_key query produced non-AvgKey result"),
            };
            Ok(Json(QueryResponse::SamplesAvgKey {
                key,
                avg,
                dp_offset_sum: dp_offset(dp::DP_SAMPLES_AVG),
                dp_offset_count: dp_offset(dp::DP_SAMPLES_COUNT_KEY),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesSumKey { ref tw, key, mask } => {
            let query_kind = "samples_sum_key";
            let mask = mask.unwrap_or(u64::MAX);
            let select = epoch_select(tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<VerifiedSamplesStruct> = db
                        .verified_samples_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
                        .collect();
                    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_verified_samples(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_verified_samples_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let query_masked = key & mask;
            let mut match_keys_total: u64 = 0;
            let mut sum_total: u64 = 0;
            // Support = count of contributing records (sum of per-key `count`) over matches.
            let mut count_total: u64 = 0;
            for s in &structs {
                let table_fixed = match &s.table_fixed {
                    Some(tf) => tf.as_slice(),
                    None => &[],
                };
                if table_fixed.is_empty() {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "verified_samples_struct missing table_fixed (needed for per-key queries)".to_string()));
                }
                // Inline unpack: (key_id[15], tip32[32], count[8], sum[8], occ[1]) per slot = 64 bytes
                if table_fixed.len() % 64 != 0 {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "samples table_fixed length not multiple of 64".to_string()));
                }
                let mut i = 0usize;
                while i < table_fixed.len() {
                    let slot_key = u64::from_be_bytes(table_fixed[i + 7..i + 15].try_into().unwrap());
                    let len = u64::from_be_bytes(table_fixed[i + 47..i + 55].try_into().unwrap());
                    let sum = u64::from_be_bytes(table_fixed[i + 55..i + 63].try_into().unwrap());
                    let occ = table_fixed[i + 63];
                    if occ != 0 && occ != 1 {
                        return Err((axum::http::StatusCode::BAD_REQUEST, "samples occ must be 0/1".to_string()));
                    }
                    i += 64;
                    if occ == 0 {
                        continue;
                    }
                    if (slot_key & mask) == query_masked {
                        match_keys_total += 1;
                        sum_total = sum_total.saturating_add(sum);
                        count_total = count_total.saturating_add(len);
                    }
                }
            }
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log(query_kind, "samples_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::SamplesEpochState> = structs.iter()
                .map(|s| build_samples_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::samples_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_samples(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::SamplesQueryInput {
                query: qcore::SamplesQuery::SumKey { key: u64_to_key(key), mask: u64_to_key(mask) },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "samples_sum_key",
                &proof_in,
                &expected,
                QUERIER_SAMPLES_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let sum = match &expected.result {
                qcore::SamplesQueryResult::SumKey { sum } => *sum,
                _ => unreachable!("samples_sum_key query produced non-SumKey result"),
            };
            let _ = sum_total;
            Ok(Json(QueryResponse::SamplesSumKey {
                key,
                sum,
                dp_offset_sum: dp_offset(dp::DP_SAMPLES_SUM_KEY),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesSumExactKey { ref tw, key } => {
            let query_kind = "samples_sum_exact_key";
            let mask = u64::MAX;
            let select = epoch_select(tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<VerifiedSamplesStruct> = db
                        .verified_samples_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
                        .collect();
                    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_verified_samples(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_verified_samples_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let query_masked = key & mask;
            let mut match_keys_total: u64 = 0;
            let mut sum_total: u64 = 0;
            // Support = count of contributing records (sum of per-key `count`) for the key.
            let mut count_total: u64 = 0;
            for s in &structs {
                let table_fixed = match &s.table_fixed {
                    Some(tf) => tf.as_slice(),
                    None => &[],
                };
                if table_fixed.is_empty() {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "verified_samples_struct missing table_fixed (needed for per-key queries)".to_string()));
                }
                // Inline unpack: (key_id[15], tip32[32], count[8], sum[8], occ[1]) per slot = 64 bytes
                if table_fixed.len() % 64 != 0 {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "samples table_fixed length not multiple of 64".to_string()));
                }
                let mut i = 0usize;
                while i < table_fixed.len() {
                    let slot_key = u64::from_be_bytes(table_fixed[i + 7..i + 15].try_into().unwrap());
                    let len = u64::from_be_bytes(table_fixed[i + 47..i + 55].try_into().unwrap());
                    let sum = u64::from_be_bytes(table_fixed[i + 55..i + 63].try_into().unwrap());
                    let occ = table_fixed[i + 63];
                    if occ != 0 && occ != 1 {
                        return Err((axum::http::StatusCode::BAD_REQUEST, "samples occ must be 0/1".to_string()));
                    }
                    i += 64;
                    if occ == 0 {
                        continue;
                    }
                    if (slot_key & mask) == query_masked {
                        match_keys_total += 1;
                        sum_total = sum_total.saturating_add(sum);
                        count_total = count_total.saturating_add(len);
                    }
                }
            }
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log(query_kind, "samples_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::SamplesEpochState> = structs.iter()
                .map(|s| build_samples_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::samples_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_samples(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::SamplesQueryInput {
                query: qcore::SamplesQuery::SumExactKey { key: u64_to_key(key) },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "samples_sum_exact_key",
                &proof_in,
                &expected,
                QUERIER_SAMPLES_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let sum = match &expected.result {
                qcore::SamplesQueryResult::SumExactKey { sum } => *sum,
                _ => unreachable!("samples_sum_exact_key query produced non-SumExactKey result"),
            };
            let _ = sum_total;
            Ok(Json(QueryResponse::SamplesSumExactKey {
                key,
                sum,
                dp_offset_sum: dp_offset(dp::DP_SAMPLES_SUM_KEY),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesSumKeyPattern { ref tw, ref pattern } => {
            let query_kind = "samples_sum_key_pattern";
            let (key, mask) = parse_key_pattern(pattern)
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let select = epoch_select(tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<VerifiedSamplesStruct> = db
                        .verified_samples_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
                        .collect();
                    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_verified_samples(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_verified_samples_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let query_masked = key & mask;
            let mut match_keys_total: u64 = 0;
            let mut sum_keys_total: u64 = 0;
            // Support = count of contributing records (sum of per-key `count`) behind matches.
            let mut count_total: u64 = 0;
            for s in &structs {
                let table_fixed = match &s.table_fixed {
                    Some(tf) => tf.as_slice(),
                    None => &[],
                };
                if table_fixed.is_empty() {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "verified_samples_struct missing table_fixed (needed for per-key queries)".to_string()));
                }
                // Inline unpack: (key_id[15], tip32[32], count[8], sum[8], occ[1]) per slot = 64 bytes
                if table_fixed.len() % 64 != 0 {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "samples table_fixed length not multiple of 64".to_string()));
                }
                let mut i = 0usize;
                while i < table_fixed.len() {
                    let slot_key = u64::from_be_bytes(table_fixed[i + 7..i + 15].try_into().unwrap());
                    let len = u64::from_be_bytes(table_fixed[i + 47..i + 55].try_into().unwrap());
                    let occ = table_fixed[i + 63];
                    if occ != 0 && occ != 1 {
                        return Err((axum::http::StatusCode::BAD_REQUEST, "samples occ must be 0/1".to_string()));
                    }
                    i += 64;
                    if occ == 0 {
                        continue;
                    }
                    if (slot_key & mask) == query_masked {
                        match_keys_total += 1;
                        sum_keys_total = sum_keys_total.saturating_add(slot_key);
                        count_total = count_total.saturating_add(len);
                    }
                }
            }
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log(query_kind, "samples_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::SamplesEpochState> = structs.iter()
                .map(|s| build_samples_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::samples_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_samples(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::SamplesQueryInput {
                query: qcore::SamplesQuery::SumKeyIds { key: u64_to_key(key), mask: u64_to_key(mask) },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "samples_sum_key_pattern",
                &proof_in,
                &expected,
                QUERIER_SAMPLES_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            let sum_keys = match &expected.result {
                qcore::SamplesQueryResult::SumKeyIds { sum_keys } => *sum_keys,
                _ => unreachable!("samples_sum_key_pattern query produced non-SumKeyIds result"),
            };
            let _ = sum_keys_total;
            Ok(Json(QueryResponse::SamplesSumKeyPattern {
                pattern: pattern.clone(),
                sum_keys,
                dp_offset_sum: dp_offset(dp::DP_SAMPLES_SUM_KEY),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesSumTopk { tw, limit } => {
            let limit = limit
                .unwrap_or(acore::CM_TOPK_SLOTS)
                .min(acore::CM_TOPK_SLOTS);
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();

            // Fetch full structs for chain verification
            let structs = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => {
                    let mut structs: Vec<VerifiedSamplesStruct> = db
                        .verified_samples_structs()
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                        .into_iter()
                        .filter(|v| v.ingest_time_ms >= start_ms && v.ingest_time_ms <= end_ms)
                        .collect();
                    structs.sort_by_key(|v| (v.aggregator_id, v.sequence));
                    structs
                }
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_verified_samples(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() {
                        Vec::new()
                    } else {
                        get_verified_samples_structs_by_sequences(&db, &seqs)
                            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?
                    }
                }
            };

            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = structs.len();

            let merge_start = std::time::Instant::now();
            let mut sums_by_key: HashMap<u64, u64> = HashMap::new();
            // Support = total contributing records across all occupied per-key entries.
            let mut topk_support: u64 = 0;
            for s in &structs {
                let table_fixed = match &s.table_fixed {
                    Some(tf) => tf.as_slice(),
                    None => &[],
                };
                if table_fixed.is_empty() {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "verified_samples_struct missing table_fixed (needed for topk)".to_string()));
                }
                // Inline unpack: (key_id[15], tip32[32], count[8], sum[8], occ[1]) per slot = 64 bytes
                if table_fixed.len() % 64 != 0 {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "samples table_fixed length not multiple of 64".to_string()));
                }
                let mut i = 0usize;
                while i < table_fixed.len() {
                    let k = u64::from_be_bytes(table_fixed[i + 7..i + 15].try_into().unwrap());
                    let len = u64::from_be_bytes(table_fixed[i + 47..i + 55].try_into().unwrap());
                    let sum = u64::from_be_bytes(table_fixed[i + 55..i + 63].try_into().unwrap());
                    let occ = table_fixed[i + 63];
                    if occ != 0 && occ != 1 {
                        return Err((axum::http::StatusCode::BAD_REQUEST, "samples occ must be 0/1".to_string()));
                    }
                    i += 64;
                    if occ == 1 && k != 0 {
                        *sums_by_key.entry(k).or_insert(0) = sums_by_key.get(&k).copied().unwrap_or(0).saturating_add(sum);
                        topk_support = topk_support.saturating_add(len);
                    }
                }
            }
            let mut sums_list: Vec<(u64, u64)> = sums_by_key
                .into_iter()
                .collect();
            sums_list.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            sums_list.truncate(limit);
            let items: Vec<SamplesSumTopkItem> = sums_list.iter().map(|(_k, sum)| SamplesSumTopkItem { sum: *sum }).collect();

            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_query_log("samples_sum_topk", "samples_epoch", rows_len, rows_len, db_ms, merge_ms, use_direct);

            // Build typed epoch states from DB structs
            let epoch_states: Vec<acore::SamplesEpochState> = structs.iter()
                .map(|s| build_samples_epoch_state(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Compute state commits and build epoch chain links
            let state_commits: Vec<[u8; 32]> = epoch_states.iter()
                .map(|s| acore::samples_epoch_state_commit(s))
                .collect();

            let epoch_chain_links = if epoch_chain_verification_enabled() {
                build_epoch_chain_links_samples(&structs, &state_commits)
            } else {
                Vec::new()
            };


            let proof_in = qcore::SamplesQueryInput {
                query: qcore::SamplesQuery::SumTopk { limit: limit as u16 },
                epoch_states,
                epoch_chain_links: epoch_chain_links.clone(),
            };
            let expected = qcore::run_samples_query(&proof_in).suppress_if_low_support();
            let proof = prove_risc0(
                "samples_sum_topk",
                &proof_in,
                &expected,
                QUERIER_SAMPLES_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_SAMPLES_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

            // Source the sums from the proven result (sorted desc, same order as
            // `items`); on suppression the proven list is empty.
            let items: Vec<SamplesSumTopkItem> = match &expected.result {
                qcore::SamplesQueryResult::SumTopk { items: proven } => {
                    let _ = items;
                    proven.iter().map(|p| SamplesSumTopkItem { sum: p.sum }).collect()
                }
                _ => unreachable!("samples_sum_topk query produced non-SumTopk result"),
            };
            Ok(Json(QueryResponse::SamplesSumTopk {
                items,
                dp_offset_sum: dp_offset(dp::DP_CM_TOPK),
                suppressed: expected.suppressed,
                proof,
            }))
        }
        QueryRequest::SamplesRawMaxKey { tw, key, mask } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();
            let rows = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => rows_sample_events_by_time(&db, start_ms, end_ms, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64),
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_sample_events(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() { Ok(Vec::new()) } else { rows_sample_events_by_sequences(&db, &seqs, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64) }
                }
            }
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = rows.len();
            if rows.len() > SAMPLES_RAW_QUERY_MAX_EVENTS {
                return Err((axum::http::StatusCode::BAD_REQUEST, format!("too many sample_events in window (>{})", SAMPLES_RAW_QUERY_MAX_EVENTS)));
            }

            let merge_start = std::time::Instant::now();
            let events: Vec<(u64, u32)> = rows
                .par_iter()
                .map(|r| -> Result<(u64, u32)> {
                    let key_id_i: i64 = r.get(0);
                    let value_i: i32 = r.get(1);
                    anyhow::ensure!(key_id_i >= 0 && value_i >= 0, "negative sample_events value");
                    Ok((key_id_i as u64, value_i as u32))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let match_keys_total = events.par_iter().filter(|(k, _)| (*k & mask) == (key & mask)).count() as u64;
            let mut per_key_max: HashMap<u64, u64> = HashMap::new();
            for (k, v) in events.iter().filter(|(k, _)| (*k & mask) == (key & mask)) {
                let entry = per_key_max.entry(*k).or_insert(0);
                *entry = (*entry).max(*v as u64);
            }
            let mut items: Vec<(u64, u64)> = per_key_max.into_iter().collect();
            items.sort_by_key(|(k, _)| *k);
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log("samples_raw_max_key", "samples_raw_events", 0, rows_len, db_ms, merge_ms, true);
            let events_in: Vec<qcore::RawEvent> = events
                .iter()
                .map(|(k, v)| qcore::RawEvent {
                    key_id: u64_to_key(*k),
                    value: *v as u64,
                })
                .collect();
            let proof_in = qcore::RawQueryInput {
                query: qcore::RawQuery::MaxKey { key: u64_to_key(key), mask: u64_to_key(mask) },
                events: events_in,
            };
            let expected_items: Vec<qcore::RawMaxKeyItem> = items.iter()
                .map(|(k, m)| qcore::RawMaxKeyItem { key_id: u64_to_key(*k), max: *m })
                .collect();
            let expected = qcore::RawQueryOutput::MaxKey { items: expected_items, match_keys: match_keys_total };
            let proof = prove_risc0(
                "samples_raw_max_key",
                &proof_in,
                &expected,
                QUERIER_RAW_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_RAW_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let response_items: Vec<SamplesRawMaxKeyItem> = items.iter()
                .map(|(k, m)| SamplesRawMaxKeyItem { key: *k, max: *m })
                .collect();
            Ok(Json(QueryResponse::SamplesRawMaxKey {
                items: response_items,
                dp_offset: dp_offset(dp::DP_RAW_MAX),
                proof,
            }))
        }
        QueryRequest::SamplesRawHistogramBucketKey { tw, key, mask, bucket } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();
            let rows = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => rows_sample_events_by_time(&db, start_ms, end_ms, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64),
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_sample_events(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() { Ok(Vec::new()) } else { rows_sample_events_by_sequences(&db, &seqs, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64) }
                }
            }
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = rows.len();
            if rows.len() > SAMPLES_RAW_QUERY_MAX_EVENTS {
                return Err((axum::http::StatusCode::BAD_REQUEST, format!("too many sample_events in window (>{})", SAMPLES_RAW_QUERY_MAX_EVENTS)));
            }

            let merge_start = std::time::Instant::now();
            let events: Vec<(u64, u32)> = rows
                .par_iter()
                .map(|r| -> Result<(u64, u32)> {
                    let key_id_i: i64 = r.get(0);
                    let value_i: i32 = r.get(1);
                    anyhow::ensure!(key_id_i >= 0 && value_i >= 0, "negative sample_events value");
                    Ok((key_id_i as u64, value_i as u32))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let match_keys_total = events.par_iter().filter(|(k, _)| (*k & mask) == (key & mask)).count() as u64;
            let count = events
                .par_iter()
                .filter(|(k, _)| (*k & mask) == (key & mask))
                .filter(|(_, v)| raw_hist_bucket(*v as u64) == bucket)
                .count() as u64;
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log("samples_raw_histogram_bucket_key", "samples_raw_events", 0, rows_len, db_ms, merge_ms, true);
            let events_in: Vec<qcore::RawEvent> = events
                .iter()
                .map(|(k, v)| qcore::RawEvent {
                    key_id: u64_to_key(*k),
                    value: *v as u64,
                })
                .collect();
            let proof_in = qcore::RawQueryInput {
                query: qcore::RawQuery::HistBucketKey { key: u64_to_key(key), mask: u64_to_key(mask), bucket },
                events: events_in,
            };
            let expected = qcore::RawQueryOutput::HistBucketKey { bucket, count, match_keys: match_keys_total };
            let proof = prove_risc0(
                "samples_raw_histogram_bucket_key",
                &proof_in,
                &expected,
                QUERIER_RAW_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_RAW_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            Ok(Json(QueryResponse::SamplesRawHistogramBucketKey {
                key,
                bucket,
                count,
                dp_offset: dp_offset(dp::DP_RAW_HIST_BUCKET),
                proof,
            }))
        }
        QueryRequest::SamplesRawCmEstimateKey { tw, key, mask, value } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();
            let rows = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => rows_sample_events_by_time(&db, start_ms, end_ms, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64),
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_sample_events(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() { Ok(Vec::new()) } else { rows_sample_events_by_sequences(&db, &seqs, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64) }
                }
            }
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = rows.len();
            if rows.len() > SAMPLES_RAW_QUERY_MAX_EVENTS {
                return Err((axum::http::StatusCode::BAD_REQUEST, format!("too many sample_events in window (>{})", SAMPLES_RAW_QUERY_MAX_EVENTS)));
            }

            let merge_start = std::time::Instant::now();
            let events: Vec<(u64, u32)> = rows
                .par_iter()
                .map(|r| -> Result<(u64, u32)> {
                    let key_id_i: i64 = r.get(0);
                    let value_i: i32 = r.get(1);
                    anyhow::ensure!(key_id_i >= 0 && value_i >= 0, "negative sample_events value");
                    Ok((key_id_i as u64, value_i as u32))
                })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let match_keys_total = events.par_iter().filter(|(k, _)| (*k & mask) == (key & mask)).count() as u64;
            let mut counts = vec![0u64; acore::CM_ROWS * CM_LEAVES];
            for (_k, v) in events.iter().filter(|(k, _)| (*k & mask) == (key & mask)) {
                for rr in 0..acore::CM_ROWS {
                    let pos = cm_bucket_index(*v as u64, rr) as usize;
                    counts[rr * CM_LEAVES + pos] =
                        counts[rr * CM_LEAVES + pos].saturating_add(1);
                }
            }
            let pos_q: [usize; acore::CM_ROWS] =
                std::array::from_fn(|rr| cm_bucket_index(value as u64, rr) as usize);
            let mut est = u64::MAX;
            for rr in 0..acore::CM_ROWS {
                est = est.min(counts[rr * CM_LEAVES + pos_q[rr]]);
            }
            let estimate = if est == u64::MAX { 0 } else { est };
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log("samples_raw_cm_estimate_key", "samples_raw_events", 0, rows_len, db_ms, merge_ms, true);
            let events_in: Vec<qcore::RawEvent> = events
                .iter()
                .map(|(k, v)| qcore::RawEvent {
                    key_id: u64_to_key(*k),
                    value: *v as u64,
                })
                .collect();
            let proof_in = qcore::RawQueryInput {
                query: qcore::RawQuery::CmEstimateKey { key: u64_to_key(key), mask: u64_to_key(mask), value },
                events: events_in,
            };
            let expected = qcore::RawQueryOutput::CmEstimateKey { value, estimate, match_keys: match_keys_total };
            let proof = prove_risc0(
                "samples_raw_cm_estimate_key",
                &proof_in,
                &expected,
                QUERIER_RAW_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_RAW_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            Ok(Json(QueryResponse::SamplesRawCmEstimateKey {
                key,
                value,
                estimate,
                dp_offset: dp_offset(dp::DP_RAW_CM_ESTIMATE),
                proof,
            }))
        }
        QueryRequest::SamplesRawStatsKey { tw, key, mask } => {
            let select = epoch_select(&tw).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let db = state.db.lock().await;
            let db_start = std::time::Instant::now();
            let rows = match select {
                EpochSelect::TimeWindow { start_ms, end_ms } => rows_sample_events_by_time(&db, start_ms, end_ms, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64),
                EpochSelect::Latest { epochs } => {
                    let seqs = latest_sequences_sample_events(&db, epochs)
                        .await
                        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
                    if seqs.is_empty() { Ok(Vec::new()) } else { rows_sample_events_by_sequences(&db, &seqs, (SAMPLES_RAW_QUERY_MAX_EVENTS + 1) as i64) }
                }
            }
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e:?}")))?;
            let db_ms = db_start.elapsed().as_millis() as u64;
            let rows_len = rows.len();
            if rows.len() > SAMPLES_RAW_QUERY_MAX_EVENTS {
                return Err((axum::http::StatusCode::BAD_REQUEST, format!("too many sample_events in window (>{})", SAMPLES_RAW_QUERY_MAX_EVENTS)));
            }

            let merge_start = std::time::Instant::now();
            let events: Vec<(u64, u32)> = rows
                .par_iter()
                .map(|r| -> Result<(u64, u32)> {
                    let key_id_i: i64 = r.get(0);
                    let value_i: i32 = r.get(1);
                    anyhow::ensure!(key_id_i >= 0 && value_i >= 0, "negative sample_events value");
                    Ok((key_id_i as u64, value_i as u32))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            let match_keys_total = events.par_iter().filter(|(k, _)| (*k & mask) == (key & mask)).count() as u64;
            let sum: u64 = events
                .par_iter()
                .filter(|(k, _)| (*k & mask) == (key & mask))
                .map(|(_, v)| *v as u64)
                .sum();
            let merge_ms = merge_start.elapsed().as_millis() as u64;
            bench_match_log(match_keys_total);
            bench_query_log("samples_raw_stats_key", "samples_raw_events", 0, rows_len, db_ms, merge_ms, true);
            let events_in: Vec<qcore::RawEvent> = events
                .iter()
                .map(|(k, v)| qcore::RawEvent {
                    key_id: u64_to_key(*k),
                    value: *v as u64,
                })
                .collect();
            let proof_in = qcore::RawQueryInput {
                query: qcore::RawQuery::StatsKey { key: u64_to_key(key), mask: u64_to_key(mask) },
                events: events_in,
            };
            let expected = qcore::RawQueryOutput::StatsKey { sum, match_keys: match_keys_total };
            let proof = prove_risc0(
                "samples_raw_stats_key",
                &proof_in,
                &expected,
                QUERIER_RAW_ELF,
                risc0_zkvm::sha::Digest::from(QUERIER_RAW_ID),
                omit_proof_hex,
            )
            .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
            Ok(Json(QueryResponse::SamplesRawStatsKey {
                key,
                sum,
                dp_offset_sum: dp_offset(dp::DP_RAW_STATS_SUM),
                proof,
            }))
        }
    }
}
