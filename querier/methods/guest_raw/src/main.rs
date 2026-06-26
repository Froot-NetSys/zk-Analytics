#![no_main]
#![no_std]

extern crate alloc;

use risc0_zkvm::guest::env;
use querier_core::{run_raw_query, RawQueryInput, RawQueryOutput};

risc0_zkvm::guest::entry!(main);

fn main() {
    let input: RawQueryInput = env::read();
    let out: RawQueryOutput = run_raw_query(&input);
    // NOTE: Raw queries are ad-hoc per-key analyses over raw events, not aggregate
    // epoch-state reductions, and RawQueryOutput has no `support` field. Each variant
    // already returns `match_keys` (its own contribution count). No MIN_SUPPORT assert
    // is applied here; if a low-support floor is later desired for raw queries, gate on
    // `match_keys` per variant.
    env::commit(&out);
}

