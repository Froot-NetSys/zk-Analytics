# cf_detector — differential secret-dependent-control-flow / output-leak detector

A small, **execution-only** (no proving) tool that tests whether a zkTelemetry
zkVM **query guest** leaks a *private* input field through its **control flow**
or its **public output structure**. It runs the real query guests in the RISC0
executor (`risc0-zkvm = 3.0.4`, in-process `ExecutorImpl`, no receipt
generation) and diffs observables across input pairs.

## The property under test: 2-safety / non-interference

A query guest should compute an aggregate; the result must not depend on any
single *private* field beyond its intended aggregate contribution. Formally we
want a **non-interference / 2-safety** property:

> For any two guest inputs `a` and `b` that agree on every *aggregate-relevant
> (public)* field and differ only in a *private* field, the guest's observable
> behaviour must be identical.

This is a **relational (2-run)** property — it talks about *pairs* of
executions — so it cannot be decided from a single run. We check it
*differentially*: run the guest on the pair and compare what we can observe.

## Observables (what we read from the executor)

From one execution-only run (`ExecutorImpl::from_elf(env, elf).run()` →
`Session`) we read three observables:

| Observable        | Source field                | What it catches                                  |
|-------------------|-----------------------------|--------------------------------------------------|
| executed cycles   | `Session::user_cycles`      | secret-dependent branches / loop trip counts     |
| journal bytes     | `Session::journal.bytes`    | secret-dependent public output (size or content) |
| exit status       | `Session::exit_code`        | data-dependent `assert!`/panic (one run faults)  |

`total_cycles` is also reported for context but is derived from `user_cycles`
plus continuation/po2 padding, so `user_cycles` is the sensitive signal.

A guest that faults (`assert!`, panic, OOB) on one input but not the other is
**not** a detector crash — the fault is captured as an observable
(`ExitOutcome::Fault` / `ExecutorError`) and a fault-vs-halt pair is itself a
leak (the exit status reveals something about the private field).

### Public API

```rust
pub fn differential_check<T: serde::Serialize>(
    elf: &[u8], input_a: &T, input_b: &T,
) -> CheckResult;     // { verdict: Pass | Flag, diff, run_a, run_b }

pub fn execute_observed<T: serde::Serialize>(
    elf: &[u8], input: &T,
) -> RunObservation;  // { user_cycles, total_cycles, journal, exit }
```

`Flag` = at least one observable differed ⇒ behaviour depends on the differing
(private) field. `Pass` = all observables identical on the tested pair.

## Benchmark (`src/main.rs`)

Each case is a labelled pair of inputs to a real guest
(`zktelemetry-risc0-querier-guest-cm`) differing only in a field we treat as
private. The driver runs every case and prints a confusion matrix of detector
verdict vs. expected label.

| Case                                   | Private field varied                         | Expected | Why                                                                   |
|----------------------------------------|----------------------------------------------|----------|-----------------------------------------------------------------------|
| `topk_unoccupied_heap_value`           | value in an *unoccupied* heap slot           | **Pass** | Topk skips unoccupied slots and re-estimates from the CM table        |
| `estimate_key_changes_output`          | the queried CM key (non-uniform sketch)      | **Flag** | committed estimate (journal) depends on the private key               |
| `estimate_low_support_fault_asymmetry` | the queried CM key (one below `MIN_SUPPORT`) | **Flag** | the `assert!(support >= MIN_SUPPORT)` privacy guard faults on one run  |
| `identical_inputs_control`             | nothing (sanity control)                     | **Pass** | identical inputs must never produce a spurious flag                   |

Observed result (RISC0 3.0.4): **TP=2, TN=2, FP=0, FN=0**. Notably
`estimate_key_changes_output` shows identical cycles but a differing journal —
the journal observable catches a leak the cycle count alone would miss.

Run it:

```bash
cargo run -p cf_detector
```

## This is testing-based evidence, NOT a proof

Like the dynamic constant-time / leakage tools this methodology is borrowed
from, a `Pass` means only "no leak observed on the tested pairs", never "no leak
exists". Related work:

- **dudect** — Reparaz, Balasch, Verbauwhede, *"Dude, is my code constant
  time?"*, DATE 2017. Black-box statistical timing-leak detection.
- **DATA** — Weiser et al., *"DATA: Differential Address Trace Analysis"*,
  USENIX Security 2018. Differential address-trace analysis across
  secret-varying runs.
- **MicroWalk** — Wichelmann et al., ACSAC 2018. Dynamic instruction/memory
  trace leakage analysis via mutual information.

Our diffed "trace" is coarse (cycle count + journal + exit) rather than a
per-instruction address trace, so this is closer to dudect's black-box timing
diff than to DATA/MicroWalk's full trace diff.

A sound, exhaustive guarantee would require static information-flow / relational
verification of the guest binary, e.g. **ct-verif** (Almeida et al., USENIX Sec
2016) or **Binsec/Rel** (Daniel et al., S&P 2020). That is **future work**.

## Limitations

- **Coarse observables.** Two genuinely different control-flow paths that happen
  to use the same cycle count *and* commit identical journals would be missed
  (false negative). A per-instruction or memory-address trace would be stronger.
- **Testing-based.** Only the supplied input pairs are checked; no input-space
  search or fuzzing.
- **Cycle counts are deterministic** for a fixed `(elf, input)` under the RISC0
  executor, so this detects *input-dependent divergence*, not measurement noise;
  no statistical thresholding (unlike dudect) is applied or needed here.
- **Scope.** Only the query guests are wired up. The benchmark currently
  exercises the CM (count-min) guest; the same API works for the histogram /
  samples / raw guests by supplying their ELF and input struct.

## Build notes

- Depends on `risc0-zkvm` with the `prove` feature solely to get the in-process
  `ExecutorImpl` (we never call a prover and never spawn an external `r0vm`).
- Depends on `zktelemetry-risc0-querier-methods` for the embedded guest ELFs and
  on `…-querier-core` / `…-aggr-core` / `…-common` for the input structs and the
  `cm_bucket_index` helper used to construct disjoint demo sketches.
- `cargo build -p cf_detector` compiles cleanly; the guest ELFs are already
  built in the workspace `target/`, so no guest-toolchain rebuild is triggered.
