#![no_main]
#![no_std]

extern crate alloc;

use risc0_zkvm::guest::env;
use zktelemetry_risc0_querier_core::{run_samples_query, SamplesQueryInput, SamplesQueryOutput};

risc0_zkvm::guest::entry!(main);

fn main() {
    let input: SamplesQueryInput = env::read();
    // LOW-SUPPORT privacy rule (suppressed sentinel): a low-support aggregate
    // ALWAYS produces a valid proof but commits a fixed placeholder (zeroed
    // result + suppressed=true) instead of the real value. No fault/exit-status
    // side channel; the only signal is the in-band, constant-shape `suppressed` bit.
    let out: SamplesQueryOutput = run_samples_query(&input).suppress_if_low_support();
    env::commit(&out);
}

