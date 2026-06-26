#![no_main]
#![no_std]

extern crate alloc;

use risc0_zkvm::guest::env;
use zktelemetry_risc0_aggr_core::{process_samples_aggr, SamplesAggrInput};

risc0_zkvm::guest::entry!(main);

fn main() {
    let input: SamplesAggrInput = env::read();
    let out = process_samples_aggr(&input);
    env::commit(&out);
}
