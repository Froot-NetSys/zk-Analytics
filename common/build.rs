use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=SERIES_SLOTS_PER_SHARD");
    println!("cargo:rerun-if-env-changed=SAMPLES_HT_BUCKETS");
    println!("cargo:rerun-if-env-changed=SAMPLES_HT_BUCKET_CAP");
    println!("cargo:rerun-if-env-changed=SAMPLES_SHARDS");
    println!("cargo:rerun-if-env-changed=SAMPLES_MAX_VALUES_PER_KEY");
    println!("cargo:rerun-if-env-changed=SERIES_HT_BUCKETS");
    println!("cargo:rerun-if-env-changed=SERIES_HT_BUCKET_CAP");

    let series_slots_per_shard =
        env::var("SERIES_SLOTS_PER_SHARD").unwrap_or_else(|_| "32".to_string());
    let samples_ht_buckets = env::var("SAMPLES_HT_BUCKETS").unwrap_or_else(|_| "64".to_string());
    let samples_ht_bucket_cap =
        env::var("SAMPLES_HT_BUCKET_CAP").unwrap_or_else(|_| "4".to_string());
    let samples_shards = env::var("SAMPLES_SHARDS").unwrap_or_else(|_| "4".to_string());
    let samples_max_values =
        env::var("SAMPLES_MAX_VALUES_PER_KEY").unwrap_or_else(|_| "1000".to_string());

    let buckets = env::var("SERIES_HT_BUCKETS").unwrap_or_else(|_| "16".to_string());
    let cap = env::var("SERIES_HT_BUCKET_CAP").unwrap_or_else(|_| "2".to_string());

    println!(
        "cargo:rustc-env=SERIES_SLOTS_PER_SHARD={}",
        series_slots_per_shard
    );
    println!("cargo:rustc-env=SAMPLES_HT_BUCKETS={}", samples_ht_buckets);
    println!(
        "cargo:rustc-env=SAMPLES_HT_BUCKET_CAP={}",
        samples_ht_bucket_cap
    );
    println!("cargo:rustc-env=SAMPLES_SHARDS={}", samples_shards);
    println!(
        "cargo:rustc-env=SAMPLES_MAX_VALUES_PER_KEY={}",
        samples_max_values
    );

    println!("cargo:rustc-env=SERIES_HT_BUCKETS={}", buckets);
    println!("cargo:rustc-env=SERIES_HT_BUCKET_CAP={}", cap);
}
