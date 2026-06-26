#![no_main]
#![no_std]

extern crate alloc;

use risc0_zkvm::guest::env;
use aggregator_core::{process_cm_aggr, CmAggrInput};

risc0_zkvm::guest::entry!(main);

fn main() {
    let input: CmAggrInput = env::read();
    let out = process_cm_aggr(&input);
    env::commit(&out);
}
