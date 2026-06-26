use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=SAMPLES_HT_BUCKETS");
    println!("cargo:rerun-if-env-changed=SAMPLES_HT_BUCKET_CAP");
    println!("cargo:rerun-if-env-changed=HISTOGRAM_SLOTS");
    println!("cargo:rerun-if-env-changed=CM_TOPK_SLOTS");

    let samples_ht_buckets = env::var("SAMPLES_HT_BUCKETS").unwrap_or_else(|_| "1024".to_string());
    let samples_ht_bucket_cap =
        env::var("SAMPLES_HT_BUCKET_CAP").unwrap_or_else(|_| "4".to_string());
    let histogram_slots = env::var("HISTOGRAM_SLOTS").unwrap_or_else(|_| "32".to_string());
    let cm_topk_slots = env::var("CM_TOPK_SLOTS").unwrap_or_else(|_| "100".to_string());

    println!("cargo:rustc-env=SAMPLES_HT_BUCKETS={}", samples_ht_buckets);
    println!(
        "cargo:rustc-env=SAMPLES_HT_BUCKET_CAP={}",
        samples_ht_bucket_cap
    );
    println!("cargo:rustc-env=HISTOGRAM_SLOTS={}", histogram_slots);
    println!("cargo:rustc-env=CM_TOPK_SLOTS={}", cm_topk_slots);
}
