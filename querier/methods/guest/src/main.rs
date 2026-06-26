#![no_main]
#![no_std]

extern crate alloc;

use risc0_zkvm::guest::env;
use zktelemetry_risc0_querier_core::{run_samples_query, SamplesQueryInput};

risc0_zkvm::guest::entry!(main);

fn main() {
    let input: SamplesQueryInput = env::read();
    let out = run_samples_query(&input);
    env::commit(&out);
}
