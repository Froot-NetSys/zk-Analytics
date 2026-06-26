use std::env;

fn main() {
    // Keep these in sync with the Nova-side defaults (common/build.rs).
    println!("cargo:rerun-if-env-changed=SERIES_SLOTS_PER_SHARD");
    println!("cargo:rerun-if-env-changed=SERIES_HT_BUCKETS");
    println!("cargo:rerun-if-env-changed=SERIES_HT_BUCKET_CAP");
    println!("cargo:rerun-if-env-changed=HISTOGRAM_SLOTS");
    println!("cargo:rerun-if-env-changed=CM_TOPK_SLOTS");

    let series_slots_per_shard =
        env::var("SERIES_SLOTS_PER_SHARD").unwrap_or_else(|_| "32".to_string());
    let series_ht_buckets = env::var("SERIES_HT_BUCKETS").unwrap_or_else(|_| "16".to_string());
    let series_ht_bucket_cap =
        env::var("SERIES_HT_BUCKET_CAP").unwrap_or_else(|_| "2".to_string());

    // These exist in zktelemetry-risc0-aggr-core too, but we want series payload parsing to stay
    // consistent if the build env overrides them.
    let histogram_slots = env::var("HISTOGRAM_SLOTS").unwrap_or_else(|_| "32".to_string());
    let cm_topk_slots = env::var("CM_TOPK_SLOTS").unwrap_or_else(|_| "100".to_string());

    println!(
        "cargo:rustc-env=SERIES_SLOTS_PER_SHARD={}",
        series_slots_per_shard
    );
    println!("cargo:rustc-env=SERIES_HT_BUCKETS={}", series_ht_buckets);
    println!(
        "cargo:rustc-env=SERIES_HT_BUCKET_CAP={}",
        series_ht_bucket_cap
    );
    println!("cargo:rustc-env=HISTOGRAM_SLOTS={}", histogram_slots);
    println!("cargo:rustc-env=CM_TOPK_SLOTS={}", cm_topk_slots);
}
