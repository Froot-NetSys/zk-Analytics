use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Deserialize)]
pub struct QueryRequest {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub key: u64,
    #[serde(default = "default_mask")]
    pub mask: u64,
    #[serde(default)]
    pub bucket: u16,
    #[serde(default)]
    pub value: u64,
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Number of contributing records. In the real system this is committed by
    /// the guest to the journal at proving time, so at static-check time it may
    /// be absent.
    #[serde(default)]
    pub support: Option<u64>,
    /// Hex/binary key pattern with `?`/`*` wildcards, for the pattern-filtered
    /// query kinds. Used to bound the predicate's selectivity (k-anonymity).
    #[serde(default)]
    pub pattern: Option<String>,
}

#[derive(Clone, Debug)]
pub struct QueryPolicy {
    /// Lowercase, canonical group-by dimensions allowed by policy.
    pub allow_group_by: BTreeSet<String>,
    /// When true, fields like `key`/`key_id` are treated as anonymized IDs.
    pub allow_anonymized_ids: bool,
    /// Minimum number of contributing records (support) for a result to be released.
    pub min_support: u64,
    /// Minimum predicate anonymity, in bits: a query whose key predicate can match
    /// fewer than `2^min_anonymity_bits` distinct keys is treated as a membership /
    /// single-record test. (`0` => never reject on selectivity.)
    pub min_anonymity_bits: u32,
}

#[derive(Clone, Debug)]
pub enum QueryRuleViolationKind {
    PiiOutput,
    NonAggregateQuery,
    GroupByNotAllowed,
    LowSupport,
}

#[derive(Clone, Debug)]
pub struct QueryRuleViolation {
    pub kind: QueryRuleViolationKind,
    pub detail: String,
}

#[derive(Clone, Debug)]
pub struct QueryRuleReport {
    pub ok: bool,
    pub violations: Vec<QueryRuleViolation>,
}

impl QueryRequest {
    pub fn from_json(s: &str) -> Result<Self> {
        let v: QueryRequest = serde_json::from_str(s).map_err(|e| anyhow!("parse JSON: {e}"))?;
        Ok(v)
    }
}

impl Default for QueryPolicy {
    fn default() -> Self {
        let mut allow_group_by = BTreeSet::new();
        allow_group_by.insert("anonymized_id".to_string());
        allow_group_by.insert("anonymized_ip".to_string());
        allow_group_by.insert("subnet_prefix".to_string());
        allow_group_by.insert("app_class".to_string());
        allow_group_by.insert("bucket".to_string());
        Self {
            allow_group_by,
            allow_anonymized_ids: false,
            min_support: 10,
            min_anonymity_bits: 16,
        }
    }
}

impl QueryRuleViolationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryRuleViolationKind::PiiOutput => "pii_output",
            QueryRuleViolationKind::NonAggregateQuery => "non_aggregate_query",
            QueryRuleViolationKind::GroupByNotAllowed => "group_by_not_allowed",
            QueryRuleViolationKind::LowSupport => "low_support",
        }
    }
}

pub fn check_query_request(req: &QueryRequest, policy: &QueryPolicy) -> QueryRuleReport {
    let mut violations = Vec::new();
    let outputs = outputs_for_kind(&req.kind);
    for field in outputs {
        if is_pii_output(field, policy) {
            violations.push(QueryRuleViolation {
                kind: QueryRuleViolationKind::PiiOutput,
                detail: format!("output field `{field}` is treated as PII"),
            });
        }
    }

    if let Some(free) = predicate_freedom_bits(req) {
        if free < policy.min_anonymity_bits {
            violations.push(QueryRuleViolation {
                kind: QueryRuleViolationKind::NonAggregateQuery,
                detail: format!(
                    "membership/single-record test: predicate `{}` pins down at most 2^{} keys (< 2^{} required)",
                    req.kind, free, policy.min_anonymity_bits
                ),
            });
        }
    }

    let mut dims: Vec<String> = if req.group_by.is_empty() {
        group_by_for_kind(&req.kind)
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        req.group_by.clone()
    };
    for dim in dims.drain(..) {
        let canonical = normalize_group_by_dim(&dim, policy);
        if is_raw_pii_dimension(&canonical) {
            violations.push(QueryRuleViolation {
                kind: QueryRuleViolationKind::GroupByNotAllowed,
                detail: format!("group by `{}` is a raw PII dimension", dim),
            });
            continue;
        }
        if !policy.allow_group_by.contains(&canonical) {
            violations.push(QueryRuleViolation {
                kind: QueryRuleViolationKind::GroupByNotAllowed,
                detail: format!("group by `{}` is not an approved dimension", dim),
            });
        }
    }

    // Low-support rule: only a runtime rule when `support` is provided. At static-check
    // time `support` may be absent (committed by the guest at proving time), so skip it.
    if let Some(s) = req.support {
        if s < policy.min_support {
            violations.push(QueryRuleViolation {
                kind: QueryRuleViolationKind::LowSupport,
                detail: format!("support {s} < min_support {}", policy.min_support),
            });
        }
    }

    QueryRuleReport {
        ok: violations.is_empty(),
        violations,
    }
}

fn default_mask() -> u64 {
    u64::MAX
}

fn outputs_for_kind(kind: &str) -> &'static [&'static str] {
    match kind {
        "cm_estimate" => &["estimate"],
        "cm_topk" => &["key", "count"],
        "histogram_bucket" => &["bucket", "count"],
        "histogram_all" => &["bucket", "count", "total_count", "total_sum"],
        "samples_avg" => &["avg"],
        "samples_avg_key" => &["key", "avg"],
        "samples_sum" => &["sum"],
        "samples_sum_key" => &["key", "sum"],
        "samples_raw_max_key" => &["key", "max"],
        "samples_raw_histogram_bucket_key" => &["key", "bucket", "count"],
        "samples_raw_cm_estimate_key" => &["key", "value", "estimate"],
        "samples_raw_stats_key" => &["key", "count", "sum", "sumsq_lo", "sumsq_hi"],
        "histogram_epoch_per_key_bucket_key" | "series_histogram_bucket_key" => {
            &["key", "bucket", "count"]
        }
        "cm_epoch_per_key_estimate_key" | "series_cm_estimate_key" => &["key", "value", "estimate"],
        _ => &[],
    }
}

fn group_by_for_kind(kind: &str) -> &'static [&'static str] {
    match kind {
        "cm_topk" => &["key_id"],
        "histogram_bucket" | "histogram_all" => &["bucket"],
        "samples_avg_key" | "samples_sum_key" => &["key_id"],
        "samples_raw_max_key"
        | "samples_raw_histogram_bucket_key"
        | "samples_raw_cm_estimate_key"
        | "samples_raw_stats_key"
        | "histogram_epoch_per_key_bucket_key"
        | "series_histogram_bucket_key"
        | "cm_epoch_per_key_estimate_key"
        | "series_cm_estimate_key" => &["key_id"],
        _ => &[],
    }
}

/// Key encoding (mirrors `zkvm_common::KEY_BYTES_LEN`): keys are
/// 15-byte arrays matched byte-by-byte at the guest level.
const KEY_BYTES_LEN: u32 = 15;
/// Full key width in hex nibbles (used by full-key pattern queries).
const FULL_KEY_NIBBLES: u32 = KEY_BYTES_LEN * 2; // 30
/// The `u64` `key`/`mask` API addresses only the low 8 bytes of the key (the
/// `key_index`, via `Event::key_id_from_u64`); its upper 7 bytes are zero / don't
/// care. So the addressable identity domain for u64-keyed queries is 64 bits.
const KEY_INDEX_BITS: u32 = 64;
const KEY_INDEX_NIBBLES: u32 = KEY_INDEX_BITS / 4; // 16

/// Data-independent upper bound, in bits, on how many distinct keys a query's
/// predicate can match: `log2(|anonymity set of the predicate|)`. `None` means
/// the query carries no key predicate and aggregates over the whole population.
///
/// This generalizes the old name-based allowlist: instead of asking "is this
/// query kind a per-key lookup?", it asks "how selective is the predicate?",
/// so a `samples_sum_key` with a full mask (exact key) is caught while the same
/// kind with a broad subnet mask is treated as an aggregate. The actual number
/// of *matching records* is a separate, data-dependent concern enforced by the
/// in-guest low-support rule.
pub fn predicate_freedom_bits(req: &QueryRequest) -> Option<u32> {
    match req.kind.as_str() {
        // Population aggregates: no per-entity predicate.
        "cm_topk" | "histogram_bucket" | "histogram_all" | "histogram_p90"
        | "samples_avg" | "samples_sum" | "samples_sum_topk" => None,

        // Exact single key (no mask) => 0 free bits => pins exactly one identity.
        "cm_estimate" | "samples_sum_exact_key" => Some(0),

        // Key + mask: free bits = domain width - the bits the mask fixes.
        // (An absent mask defaults to all-ones => exact key => 0 free bits.)
        // The `*_epoch_per_key_*` / `series_*` kinds are the per-series query
        // path (the `guest_series` program); they are screened here for
        // forward-compatibility even though they are not yet surfaced in the
        // server's typed `QueryRequest` enum.
        "samples_avg_key" | "samples_sum_key" | "samples_raw_max_key"
        | "samples_raw_histogram_bucket_key" | "samples_raw_cm_estimate_key"
        | "samples_raw_stats_key" | "histogram_epoch_per_key_bucket_key"
        | "series_histogram_bucket_key" | "cm_epoch_per_key_estimate_key"
        | "series_cm_estimate_key" => {
            // The mask is a u64 over the key_index; popcount(mask) bits are fixed.
            Some(KEY_INDEX_BITS.saturating_sub(req.mask.count_ones()))
        }

        // Pattern-filtered kinds: freedom from the wildcard positions. The key
        // domain differs by kind: the samples pattern is over the u64 key_index
        // (16 nibbles), while the histogram pattern is over the full 15-byte key
        // (30 nibbles).
        "samples_sum_key_pattern" => Some(pattern_freedom_bits(
            req.pattern.as_deref().unwrap_or(""),
            KEY_INDEX_NIBBLES,
        )),
        "histogram_all_key" => Some(pattern_freedom_bits(
            req.pattern.as_deref().unwrap_or(""),
            FULL_KEY_NIBBLES,
        )),

        // Unknown kinds: the structural whitelist (typed request enum) governs
        // these, so we make no selectivity claim here (treated as an aggregate).
        // NOTE: any NEW per-key/selective query kind MUST be added to an arm
        // above, or it will silently bypass membership detection. Keep this set
        // in sync with the server's `policy_kind` mapping.
        _ => None,
    }
}

/// Free bits implied by a key pattern: `?`/`*` are wildcard positions; positions
/// not present in the (shorter-than-full) pattern are treated as wildcard
/// (prefix match). Hex: each free nibble = 4 bits. Binary (`b`/`0b` prefix):
/// each free position = 1 bit.
fn pattern_freedom_bits(pattern: &str, key_nibbles: u32) -> u32 {
    let p = pattern.trim();
    if let Some(bits) = p.strip_prefix("0b").or_else(|| p.strip_prefix('b')) {
        let specified = bits.chars().count() as u32;
        let wild = bits.chars().filter(|c| *c == '?' || *c == '*').count() as u32;
        let domain = key_nibbles * 4;
        wild + domain.saturating_sub(specified)
    } else {
        let hex = p.strip_prefix("0x").unwrap_or(p);
        let specified = hex.chars().count() as u32;
        let wild = hex.chars().filter(|c| *c == '?' || *c == '*').count() as u32;
        (wild + key_nibbles.saturating_sub(specified)) * 4
    }
}

/// A query is a membership / single-record test if its predicate can match fewer
/// than `2^policy.min_anonymity_bits` distinct keys.
pub fn is_membership_test(req: &QueryRequest, policy: &QueryPolicy) -> bool {
    match predicate_freedom_bits(req) {
        None => false,
        Some(free) => free < policy.min_anonymity_bits,
    }
}

fn is_pii_output(field: &str, policy: &QueryPolicy) -> bool {
    let field = field.to_lowercase();
    match field.as_str() {
        "key" | "key_id" | "user_id" => !policy.allow_anonymized_ids,
        "ip" | "src_ip" | "dst_ip" | "username" | "user" | "port" => true,
        "value" => true,
        _ => false,
    }
}

fn normalize_group_by_dim(dim: &str, policy: &QueryPolicy) -> String {
    let dim = dim.trim().to_lowercase();
    match dim.as_str() {
        "key" | "key_id" | "user_id" if policy.allow_anonymized_ids => "anonymized_id".to_string(),
        _ => dim,
    }
}

fn is_raw_pii_dimension(dim: &str) -> bool {
    matches!(
        dim,
        "ip" | "src_ip" | "dst_ip" | "username" | "user" | "port"
    )
}
