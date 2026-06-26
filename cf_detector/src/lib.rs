//! Differential secret-dependent-control-flow / output-leak detector for the
//! zkTelemetry zkVM query guests.
//!
//! # Property under test (2-safety / non-interference)
//!
//! A query guest must not let a PRIVATE input field influence its *control
//! flow* or its *public output structure*. Concretely we want a
//! non-interference / 2-safety property: for any two guest inputs `a` and `b`
//! that agree on every *aggregate-relevant* (public) field and differ only in
//! a *private* field, the guest's observable behaviour must be identical.
//!
//! This is a relational (2-run) property, not a property of a single run, so it
//! cannot be checked by looking at one execution. We check it differentially by
//! running the guest twice and comparing observables.
//!
//! # Observables
//!
//! From a single execution-only run (no proving) of the RISC0 executor we read:
//!   * **executed cycles** (`user_cycles`, plus `total_cycles` for context) —
//!     a proxy for the executed instruction trace / control flow. If a private
//!     field steers a branch or changes a loop trip count, the cycle count
//!     moves.
//!   * **committed journal bytes** — the public output. If a private field
//!     changes what (or how much) the guest commits, the journal differs.
//!   * **exit status** (`ExitCode`) — a guest may `assert!` / fault on some
//!     inputs and not others; a pair where one run faults and the other halts
//!     cleanly is itself an observable difference (a leak), not a detector
//!     crash.
//!
//! If, across many input pairs that differ only in a private field, *both*
//! cycles and journal are identical and the exit status matches, the guest
//! passes the test for that field. A difference in *any* observable flags the
//! guest as leaking through control flow / output.
//!
//! # This is testing-based evidence, not a proof
//!
//! Like the dynamic constant-time / leakage tools this methodology is borrowed
//! from, passing only means "no leak observed on the tested pairs", never "no
//! leak exists". Compare:
//!   * **dudect** — Reparaz, Balasch, Verbauwhede, *"Dude, is my code
//!     constant time?"*, DATE 2017. Statistical timing-based leakage detection
//!     from black-box measurements.
//!   * **DATA** — Weiser et al., *"DATA: Differential Address Trace Analysis"*,
//!     USENIX Security 2018. Differential analysis of address traces across
//!     secret-varying runs.
//!   * **MicroWalk** — Wichelmann et al., ACSAC 2018. Dynamic
//!     instruction/memory-trace leakage analysis via mutual information.
//!
//! Here the "trace" we diff is coarse (cycle count + journal) rather than a
//! per-instruction address trace, so this is closer to dudect's black-box
//! timing diff than to DATA/MicroWalk's full trace diff. A sound,
//! exhaustive guarantee would require static information-flow / relational
//! verification of the guest binary (e.g. ct-verif [Almeida et al., USENIX Sec
//! 2016], Binsec/Rel [Daniel et al., S&P 2020]); that is left as future work.
//!
//! # Limitations
//!   * Coarse observables: two genuinely different control-flow paths could, in
//!     principle, take the same number of cycles and commit identical journals
//!     and would then be missed (false negative). A per-instruction or
//!     memory-address trace would be stronger.
//!   * Testing-based: only the supplied input pairs are checked.
//!   * Cycle counts here are deterministic for a fixed (elf, input) under the
//!     RISC0 executor, so this detects *input-dependent* divergence, not
//!     measurement noise; no statistical thresholding is applied.

use anyhow::Result;
use risc0_zkvm::{ExecutorEnv, ExecutorImpl, ExitCode, Session};

/// The observables read from one execution-only run of a guest.
#[derive(Clone, Debug)]
pub struct RunObservation {
    /// User cycles (guest instructions, no continuation/padding overhead).
    pub user_cycles: u64,
    /// Total cycles including continuation / po2 padding overhead.
    pub total_cycles: u64,
    /// Bytes committed to the journal (public output). `None` if the guest
    /// committed nothing or faulted before committing.
    pub journal: Option<Vec<u8>>,
    /// How the session terminated (clean halt vs. fault, etc.).
    pub exit: ExitOutcome,
}

/// A serializable / comparable summary of how a run ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExitOutcome {
    /// Guest halted normally with the given user exit code.
    Halted(u32),
    /// Guest paused with the given user exit code.
    Paused(u32),
    /// Execution hit the session limit.
    SessionLimit,
    /// Guest faulted (e.g. `assert!`, unwrap, out-of-bounds, illegal instr).
    Fault,
    /// The executor itself returned an error trying to run the guest. We treat
    /// this as an observable (a faulting run) rather than crashing the
    /// detector, carrying the error string for the report.
    ExecutorError(String),
}

impl From<ExitCode> for ExitOutcome {
    fn from(code: ExitCode) -> Self {
        match code {
            ExitCode::Halted(c) => ExitOutcome::Halted(c),
            ExitCode::Paused(c) => ExitOutcome::Paused(c),
            ExitCode::SystemSplit => ExitOutcome::Halted(0),
            ExitCode::SessionLimit => ExitOutcome::SessionLimit,
            // `ExitCode` is #[non_exhaustive]; treat any future/other code as a
            // distinct observable so it still participates in the exit diff.
            other => ExitOutcome::ExecutorError(format!("other-exit:{other:?}")),
        }
    }
}

/// Run a guest ELF in the RISC0 executor (execution only, NO proving) on a
/// single serializable input, returning the observables.
///
/// A guest fault (assert/panic) is captured as `ExitOutcome::Fault` /
/// `ExecutorError` rather than propagated, so callers can treat "faults on a
/// vs. not on b" as a leak.
pub fn execute_observed<T: serde::Serialize>(elf: &[u8], input: &T) -> RunObservation {
    match build_and_run(elf, input) {
        Ok(session) => observation_from_session(&session),
        Err(e) => {
            // A faulting guest surfaces here. Distinguish a guest-level fault
            // (the message typically mentions the exit code / "Fatal") from
            // host-side setup errors; either way it is an observable.
            RunObservation {
                user_cycles: 0,
                total_cycles: 0,
                journal: None,
                exit: ExitOutcome::ExecutorError(e.to_string()),
            }
        }
    }
}

fn build_and_run<T: serde::Serialize>(elf: &[u8], input: &T) -> Result<Session> {
    let env = ExecutorEnv::builder().write(input)?.build()?;
    // ExecutorImpl runs entirely in-process (the `prove` feature provides it),
    // so we never depend on an external r0vm subprocess. `.run()` executes
    // without generating a receipt.
    let mut exec = ExecutorImpl::from_elf(env, elf)?;
    let session = exec.run()?;
    Ok(session)
}

fn observation_from_session(session: &Session) -> RunObservation {
    RunObservation {
        user_cycles: session.user_cycles,
        total_cycles: session.total_cycles,
        journal: session.journal.as_ref().map(|j| j.bytes.clone()),
        exit: session.exit_code.into(),
    }
}

/// Which observable(s) differed between the two runs of a pair.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffReport {
    pub cycles_differ: bool,
    pub journal_differs: bool,
    pub exit_differs: bool,
}

impl DiffReport {
    pub fn any(&self) -> bool {
        self.cycles_differ || self.journal_differs || self.exit_differs
    }
}

/// The detector's verdict for one input pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// No observable difference: the private field did not influence behaviour
    /// on this pair (testing-based, not a proof).
    Pass,
    /// At least one observable differed: behaviour depends on the private field
    /// => potential control-flow / output leak.
    Flag,
}

/// Full result of a differential check on one input pair.
#[derive(Clone, Debug)]
pub struct CheckResult {
    pub verdict: Verdict,
    pub diff: DiffReport,
    pub run_a: RunObservation,
    pub run_b: RunObservation,
}

/// Core public API: run `elf` on two inputs that should differ only in a
/// PRIVATE field, compare the observables, and decide Pass / Flag.
///
/// `Flag` means behaviour depends on the differing (private) field.
pub fn differential_check<T: serde::Serialize>(
    elf: &[u8],
    input_a: &T,
    input_b: &T,
) -> CheckResult {
    let run_a = execute_observed(elf, input_a);
    let run_b = execute_observed(elf, input_b);

    // Compare on user_cycles (the control-flow proxy). total_cycles is reported
    // for context but is derived from user_cycles + padding, so user_cycles is
    // the sensitive signal.
    let cycles_differ = run_a.user_cycles != run_b.user_cycles;
    let journal_differs = run_a.journal != run_b.journal;
    let exit_differs = run_a.exit != run_b.exit;

    let diff = DiffReport {
        cycles_differ,
        journal_differs,
        exit_differs,
    };
    let verdict = if diff.any() {
        Verdict::Flag
    } else {
        Verdict::Pass
    };

    CheckResult {
        verdict,
        diff,
        run_a,
        run_b,
    }
}
