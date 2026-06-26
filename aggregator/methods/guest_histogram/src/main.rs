#![no_main]
#![no_std]

extern crate alloc;

use risc0_zkvm::guest::env;
use aggregator_core::{process_histogram_aggr, HistogramAggrInput};

risc0_zkvm::guest::entry!(main);

fn main() {
    let input: HistogramAggrInput = env::read();
    let out = process_histogram_aggr(&input);
    env::commit(&out);
}
