//! Benchmark driver for the differential secret-dependent-control-flow /
//! output-leak detector.
//!
//! See `lib.rs` for the methodology (2-safety / non-interference, observables =
//! cycles + journal + exit status, testing-based; cf. dudect, DATA, MicroWalk).
//!
//! Each benchmark case is a pair of inputs to a real query guest that differ
//! ONLY in a field we are treating as "private", together with the expected
//! label (Pass = no leak, Flag = leak). We run all cases through
//! `differential_check` and print a confusion-matrix summary of detector
//! verdict vs. expected label.
//!
//! Coverage: the CM guest (count-min), the HISTOGRAM guest, and the SAMPLES
//! guest, each with at least one Pass and one Flag case. Because the three
//! guests take different (incompatible) input types, each case runs its own
//! `differential_check` at build time and stores the resulting `CheckResult`;
//! the reporting loop is then uniform over guests.

use cf_detector::{differential_check, CheckResult, ExitOutcome, Verdict};

use zktelemetry_risc0_aggr_core::{
    cm_bucket_index, BucketEntry, CmEpochState, HistogramEpochState, KeyHistogram, SamplesEpochState,
    CM_COLS, CM_ROWS, HISTOGRAM_SLOTS,
};
use zktelemetry_risc0_common::{Event, KEY_BYTES_LEN};
use zktelemetry_risc0_querier_core::{
    CmQuery, CmQueryInput, HistogramQuery, HistogramQueryInput, SamplesQuery, SamplesQueryInput,
};
use zktelemetry_risc0_querier_methods::{
    ZKTELEMETRY_RISC0_QUERIER_GUEST_CM_ELF as CM_ELF,
    ZKTELEMETRY_RISC0_QUERIER_GUEST_HISTOGRAM_ELF as HISTOGRAM_ELF,
    ZKTELEMETRY_RISC0_QUERIER_GUEST_SAMPLES_ELF as SAMPLES_ELF,
};

// ===========================================================================
// CM-guest helpers (unchanged)
// ===========================================================================

/// Build a single-epoch CM sketch input whose entire table is filled with
/// `base` and whose `heap_vals` has one extra unoccupied slot we can perturb.
/// No epoch chain links => the guest skips chain verification (we are testing
/// the query path, not chain integrity).
fn cm_input(query: CmQuery, base: u32) -> CmQueryInput {
    let counts = vec![base; CM_ROWS * CM_COLS];
    // One occupied + one unoccupied heap slot, so Topk has a candidate AND we
    // have a "private but ignored" slot to mutate.
    let heap_keys = vec![key_of(1), [0u8; KEY_BYTES_LEN]];
    let heap_vals = vec![base as u64, 0];
    let heap_occ = vec![1u8, 0u8];
    let state = CmEpochState {
        counts,
        heap_keys,
        heap_vals,
        heap_occ,
        total_sum: base as u64,
    };
    CmQueryInput {
        query,
        epoch_states: vec![state],
        epoch_chain_links: Vec::new(),
    }
}

fn key_of(v: u64) -> [u8; KEY_BYTES_LEN] {
    Event::key_id_from_u64(v)
}

/// The set of CM cell indices (`row*CM_COLS + col`) that key `k` touches.
fn key_cells(k: &[u8; KEY_BYTES_LEN]) -> Vec<usize> {
    (0..CM_ROWS)
        .map(|r| r * CM_COLS + cm_bucket_index(k, r) as usize)
        .collect()
}

/// Pick two keys whose CM cells are disjoint, so lowering one key's cells does
/// not change the other key's min estimate. Returns `(hi, lo)`.
fn pick_disjoint_keys(seed: u64) -> ([u8; KEY_BYTES_LEN], [u8; KEY_BYTES_LEN]) {
    let hi = key_of(seed);
    let hi_cells = key_cells(&hi);
    for v in (seed + 1)..(seed + 100_000) {
        let lo = key_of(v);
        if key_cells(&lo).iter().all(|c| !hi_cells.contains(c)) {
            return (hi, lo);
        }
    }
    panic!("could not find a non-colliding key pair");
}

/// Lower every CM cell that key `k` hashes into, down to `to`, in epoch 0.
/// This makes the count-min estimate for `k` equal `to` (the min over its
/// rows) while leaving most of the table untouched.
fn set_key_estimate(input: &mut CmQueryInput, k: &[u8; KEY_BYTES_LEN], to: u32) {
    let counts = &mut input.epoch_states[0].counts;
    for r in 0..CM_ROWS {
        let c = cm_bucket_index(k, r) as usize;
        counts[r * CM_COLS + c] = to;
    }
}

/// Min CM estimate for key `k` over epoch 0 (mirrors the guest's Estimate path
/// for a single epoch). Used only to pick non-colliding demo keys.
fn estimate(input: &CmQueryInput, k: &[u8; KEY_BYTES_LEN]) -> u32 {
    let counts = &input.epoch_states[0].counts;
    (0..CM_ROWS)
        .map(|r| counts[r * CM_COLS + cm_bucket_index(k, r) as usize])
        .min()
        .unwrap_or(0)
}

// ===========================================================================
// Histogram-guest helpers
// ===========================================================================

/// One per-key histogram entry. `bucket` indices and per-bucket `counts` are
/// chosen by the caller; `count` (the per-key support) is the sum of the bucket
/// counts and `sum` is a representative value-sum.
fn key_hist(key: [u8; KEY_BYTES_LEN], buckets: &[(usize, u32)], sum: u64) -> KeyHistogram {
    let mut kh = KeyHistogram::new(key);
    let mut total = 0u32;
    for &(b, c) in buckets {
        assert!(b < HISTOGRAM_SLOTS, "bucket index out of range");
        kh.bucket_counts[b] = c;
        total += c;
    }
    kh.count = total;
    kh.sum = sum;
    kh
}

/// Build a single-epoch histogram input from a set of per-key histograms.
/// `total_count` / `total_sum` are derived from the entries so the global
/// queries are internally consistent. Empty chain links => no chain check.
fn histogram_input(query: HistogramQuery, per_key: Vec<KeyHistogram>) -> HistogramQueryInput {
    let total_count: u64 = per_key.iter().map(|e| e.count as u64).sum();
    let total_sum: u64 = per_key.iter().map(|e| e.sum).sum();
    // per_key_histograms are expected sorted by key_id for determinism.
    let mut per_key = per_key;
    per_key.sort_by_key(|e| e.key_id);
    let state = HistogramEpochState {
        total_count,
        total_sum,
        per_key_histograms: per_key,
    };
    HistogramQueryInput {
        query,
        epoch_states: vec![state],
        epoch_chain_links: Vec::new(),
    }
}

// ===========================================================================
// Samples-guest helpers
// ===========================================================================

/// One occupied per-key sample bucket entry.
fn sample_entry(key: [u8; KEY_BYTES_LEN], count: u32, sum: u64) -> BucketEntry {
    BucketEntry {
        occupied: 1,
        key_id: key,
        key_chain_tip: [0u8; 32],
        sum,
        count,
    }
}

/// Build a single-epoch samples input from a set of per-key entries. Totals
/// are derived from the entries. Empty chain links => no chain check.
fn samples_input(query: SamplesQuery, per_key: Vec<BucketEntry>) -> SamplesQueryInput {
    let total_count: u64 = per_key.iter().map(|e| e.count as u64).sum();
    let total_sum: u64 = per_key.iter().map(|e| e.sum).sum();
    // per_key is expected sorted by key_id for determinism.
    let mut per_key = per_key;
    per_key.sort_by_key(|e| e.key_id);
    let state = SamplesEpochState {
        total_count,
        total_sum,
        chain_hash: [0u8; 32],
        per_key,
    };
    SamplesQueryInput {
        query,
        epoch_states: vec![state],
        epoch_chain_links: Vec::new(),
    }
}

// ===========================================================================
// Case model
// ===========================================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Label {
    Pass,
    Flag,
}

/// A finished case: metadata plus the already-run differential result. We store
/// the `CheckResult` (run at build time) rather than the inputs, so a single
/// homogeneous `Vec<Case>` can hold cases from guests with different input
/// types.
struct Case {
    guest: &'static str,
    name: &'static str,
    /// What private field is varied, and why we expect the label.
    rationale: &'static str,
    expected: Label,
    result: CheckResult,
}

/// Run a differential check and package it as a `Case`.
fn case<T: serde::Serialize>(
    guest: &'static str,
    name: &'static str,
    rationale: &'static str,
    elf: &'static [u8],
    a: &T,
    b: &T,
    expected: Label,
) -> Case {
    let result = differential_check(elf, a, b);
    Case {
        guest,
        name,
        rationale,
        expected,
        result,
    }
}

fn build_cases() -> Vec<Case> {
    let mut cases = Vec::new();

    // =======================================================================
    // CM guest
    // =======================================================================

    // ---- CM Case 1: PASS. Topk query; vary a PRIVATE value in an *unoccupied*
    // heap slot. The guest skips unoccupied slots and re-estimates Topk counts
    // from the CM table, so this field cannot influence output or control flow.
    {
        let base = 50; // head count 50 >= MIN_SUPPORT(10), so the guest commits.
        let a = cm_input(CmQuery::Topk { limit: 5 }, base);
        let mut b = a.clone();
        // Perturb heap_vals[1] (heap_occ[1] == 0): a private, ignored field.
        b.epoch_states[0].heap_vals[1] = 999_999;
        cases.push(case(
            "CM",
            "topk_unoccupied_heap_value",
            "Topk ignores unoccupied heap slots and re-estimates from the CM \
             table; perturbing a private value in an unoccupied slot must not \
             change cycles or journal.",
            CM_ELF,
            &a,
            &b,
            Label::Pass,
        ));
    }

    // ---- CM Case 2: FLAG. Estimate query; vary the PRIVATE queried key so that
    // its count-min estimate differs (both still >= MIN_SUPPORT, so both
    // commit). The committed journal carries the estimate => journal differs
    // and very likely cycles too. The queried key is the sensitive identifier;
    // the guest's output depends on it.
    {
        let base = 50;
        let (key_hi, key_lo) = pick_disjoint_keys(1001);
        let mut a = cm_input(CmQuery::Estimate { key: key_hi }, base);
        set_key_estimate(&mut a, &key_lo, 20);
        let mut b = a.clone();
        b.query = CmQuery::Estimate { key: key_lo };
        assert_eq!(estimate(&a, &key_hi), 50, "key_hi collided; pick another key");
        assert_eq!(estimate(&a, &key_lo), 20, "key_lo not set as expected");
        cases.push(case(
            "CM",
            "estimate_key_changes_output",
            "The queried key is private; with a non-uniform sketch two keys \
             yield different estimates, so the committed journal (and cycles) \
             depend on the private key.",
            CM_ELF,
            &a,
            &b,
            Label::Flag,
        ));
    }

    // ---- CM Case 3: FLAG via low-support suppression asymmetry. Estimate
    // query; one private key estimates >= MIN_SUPPORT (commits the real value),
    // the other estimates < MIN_SUPPORT and is replaced by the suppressed
    // sentinel (zeroed result + suppressed=true). The committed journal differs
    // between the two queried keys => the (private) queried key's low-support
    // status leaks through the public output.
    {
        let base = 50;
        let (key_ok, key_low) = pick_disjoint_keys(3003);
        let mut a = cm_input(CmQuery::Estimate { key: key_ok }, base);
        set_key_estimate(&mut a, &key_low, 3);
        let mut b = a.clone();
        b.query = CmQuery::Estimate { key: key_low };
        assert_eq!(estimate(&a, &key_ok), 50, "key_ok collided; pick another key");
        assert_eq!(estimate(&a, &key_low), 3, "key_low not set as expected");
        cases.push(case(
            "CM",
            "estimate_low_support_suppression_asymmetry",
            "MIN_SUPPORT suppression depends on the private queried key: a key \
             whose estimate is below threshold yields the suppressed sentinel \
             while another commits the real estimate, so the journal reveals \
             whether the queried aggregate is low-support.",
            CM_ELF,
            &a,
            &b,
            Label::Flag,
        ));
    }

    // ---- CM Case 4: PASS (control / sanity). Identical inputs must never flag.
    {
        let a = cm_input(CmQuery::Topk { limit: 5 }, 50);
        let b = a.clone();
        cases.push(case(
            "CM",
            "identical_inputs_control",
            "Identical inputs: the detector must report Pass (no spurious flag).",
            CM_ELF,
            &a,
            &b,
            Label::Pass,
        ));
    }

    // =======================================================================
    // Histogram guest
    // =======================================================================

    // ---- HIST Case 1: PASS. AllKey query selecting key X. We perturb a
    // DIFFERENT, non-selected key Y's private buckets. The masked filter keeps
    // only X's entry, so Y's histogram never enters the result. Both sides keep
    // X's support >= MIN_SUPPORT (12), so no suppression. Output and cycles must
    // not depend on Y.
    {
        let key_x = key_of(100);
        let key_y = key_of(200);
        // Select exactly key_x: full mask, exact match.
        let mask = [0xffu8; KEY_BYTES_LEN];
        let x_entry = key_hist(key_x, &[(2, 7), (3, 5)], 480); // count 12 >= 10
        let y_a = key_hist(key_y, &[(1, 4), (5, 8)], 300); // not selected
        let mut y_b = y_a.clone();
        // PRIVATE perturbation of the non-selected key Y: move counts around and
        // change its value-sum. (Y's count is unchanged so global support, which
        // AllKey does not use anyway, is irrelevant here.)
        y_b.bucket_counts[1] = 9;
        y_b.bucket_counts[5] = 3;
        y_b.sum = 777;
        let a = histogram_input(
            HistogramQuery::AllKey { key: key_x, mask },
            vec![x_entry.clone(), y_a],
        );
        let b = histogram_input(
            HistogramQuery::AllKey { key: key_x, mask },
            vec![x_entry, y_b],
        );
        cases.push(case(
            "HIST",
            "allkey_unselected_key_buckets",
            "AllKey filters to the masked key X; the private bucket distribution \
             and value-sum of a non-selected key Y cannot influence X's filtered \
             result, so cycles and journal must be unchanged.",
            HISTOGRAM_ELF,
            &a,
            &b,
            Label::Pass,
        ));
    }

    // ---- HIST Case 2: FLAG. AllKey query selecting key X; we vary X's OWN
    // private bucket distribution (holding its count, hence support, constant
    // so neither side is suppressed). The committed per-bucket items change =>
    // journal differs. X's distribution is the sensitive payload.
    {
        let key_x = key_of(100);
        let mask = [0xffu8; KEY_BYTES_LEN];
        // Same total count (12 => support stays >= 10) but different bucket shape.
        let x_a = key_hist(key_x, &[(2, 7), (3, 5)], 480);
        let x_b = key_hist(key_x, &[(2, 5), (3, 7)], 480);
        assert_eq!(x_a.count, x_b.count, "support must match so neither suppresses");
        let a = histogram_input(HistogramQuery::AllKey { key: key_x, mask }, vec![x_a]);
        let b = histogram_input(HistogramQuery::AllKey { key: key_x, mask }, vec![x_b]);
        cases.push(case(
            "HIST",
            "allkey_selected_key_distribution",
            "The selected key's bucket distribution is private; reshaping it (at \
             constant support) changes the committed per-bucket items, so the \
             journal depends on the private distribution.",
            HISTOGRAM_ELF,
            &a,
            &b,
            Label::Flag,
        ));
    }

    // ---- HIST Case 3: FLAG via low-support suppression. AllKey selecting key
    // X. On side A, X has count 12 (>= MIN_SUPPORT) => real result; on side B we
    // drop X's matched count below 10 (=> suppressed sentinel). The only varied
    // field is X's private matched count; the journal flips from real to
    // suppressed, leaking the low-support status.
    {
        let key_x = key_of(101);
        let mask = [0xffu8; KEY_BYTES_LEN];
        let x_a = key_hist(key_x, &[(2, 7), (3, 5)], 480); // count 12 -> real
        let x_b = key_hist(key_x, &[(2, 4), (3, 3)], 300); // count 7  -> suppressed
        let a = histogram_input(HistogramQuery::AllKey { key: key_x, mask }, vec![x_a]);
        let b = histogram_input(HistogramQuery::AllKey { key: key_x, mask }, vec![x_b]);
        cases.push(case(
            "HIST",
            "allkey_low_support_suppression",
            "Dropping the selected key's private matched count below MIN_SUPPORT \
             flips the output to the suppressed sentinel, so the journal reveals \
             the low-support status of the private aggregate.",
            HISTOGRAM_ELF,
            &a,
            &b,
            Label::Flag,
        ));
    }

    // =======================================================================
    // Samples guest
    // =======================================================================

    // ---- SAMPLES Case 1: PASS. SumExactKey for key X; we perturb a DIFFERENT
    // key Y's private sum/count. X's matched sum and support are unchanged
    // (support 14 >= MIN_SUPPORT both sides), so output and cycles must not move.
    {
        let key_x = key_of(500);
        let key_y = key_of(600);
        let x_entry = sample_entry(key_x, 14, 1400); // support 14 >= 10
        let y_a = sample_entry(key_y, 11, 900);
        let mut y_b = y_a;
        // PRIVATE perturbation of non-queried key Y's sum and count.
        y_b.sum = 4242;
        y_b.count = 30;
        let a = samples_input(
            SamplesQuery::SumExactKey { key: key_x },
            vec![x_entry, y_a],
        );
        let b = samples_input(
            SamplesQuery::SumExactKey { key: key_x },
            vec![x_entry, y_b],
        );
        cases.push(case(
            "SAMPLES",
            "sumexactkey_other_key_sum",
            "SumExactKey sums only the queried key X; a non-queried key Y's \
             private sum/count cannot influence X's result, so cycles and \
             journal must be unchanged.",
            SAMPLES_ELF,
            &a,
            &b,
            Label::Pass,
        ));
    }

    // ---- SAMPLES Case 2: FLAG. SumKey (masked) for key X; we vary X's OWN
    // private matched sum (holding its count constant so support stays >= 10,
    // no suppression). The committed sum changes => journal differs. The
    // queried key's value-sum is the sensitive aggregate.
    {
        let key_x = key_of(500);
        let mask = [0xffu8; KEY_BYTES_LEN];
        let x_a = sample_entry(key_x, 14, 1400);
        let mut x_b = x_a;
        x_b.sum = 9999; // same count (support) -> not suppressed; sum differs
        assert_eq!(x_a.count, x_b.count, "support must match so neither suppresses");
        let a = samples_input(SamplesQuery::SumKey { key: key_x, mask }, vec![x_a]);
        let b = samples_input(SamplesQuery::SumKey { key: key_x, mask }, vec![x_b]);
        cases.push(case(
            "SAMPLES",
            "sumkey_queried_key_sum",
            "The queried key's value-sum is private; changing it at constant \
             support changes the committed sum, so the journal depends on the \
             private aggregate.",
            SAMPLES_ELF,
            &a,
            &b,
            Label::Flag,
        ));
    }

    // ---- SAMPLES Case 3: FLAG via low-support suppression. SumExactKey for key
    // X. Side A: X's matched count 14 (>= MIN_SUPPORT) => real sum. Side B: drop
    // X's matched count to 6 (< MIN_SUPPORT) => suppressed sentinel. The only
    // varied field is X's private matched count; the journal flips to the
    // suppressed sentinel, leaking low-support status.
    {
        let key_x = key_of(501);
        let x_a = sample_entry(key_x, 14, 1400); // support 14 -> real
        let x_b = sample_entry(key_x, 6, 600); // support 6  -> suppressed
        let a = samples_input(SamplesQuery::SumExactKey { key: key_x }, vec![x_a]);
        let b = samples_input(SamplesQuery::SumExactKey { key: key_x }, vec![x_b]);
        cases.push(case(
            "SAMPLES",
            "sumexactkey_low_support_suppression",
            "Dropping the queried key's private matched count below MIN_SUPPORT \
             flips the output to the suppressed sentinel, so the journal reveals \
             the low-support status of the private aggregate.",
            SAMPLES_ELF,
            &a,
            &b,
            Label::Flag,
        ));
    }

    // =======================================================================
    // Cycle-count (control-flow) divergence coverage
    //
    // TODO: A dedicated case demonstrating a CYCLE-count divergence that is
    // independent of the journal/exit (i.e. a private value steering a branch
    // or loop trip-count while committing the same output) would require a
    // synthetic guest whose work depends on a private value. The existing query
    // guests are written so their per-element work is data-independent over the
    // tested pairs, so we cannot exhibit a pure cycle-only leak without
    // authoring a new control-flow-leak guest + methods crate. Left as future
    // coverage to avoid adding a new guest program. (The FLAG cases above do
    // exercise cycle differences alongside the journal differences they
    // primarily target.)
    // =======================================================================

    cases
}

fn label_of(v: &Verdict) -> Label {
    match v {
        Verdict::Pass => Label::Pass,
        Verdict::Flag => Label::Flag,
    }
}

fn main() {
    println!("cf_detector: differential secret-dependent-control-flow / output-leak detector");
    println!("RISC0 executor, execution only (NO proving). Observables: user_cycles + journal + exit.");
    println!(
        "Methodology: 2-safety / non-interference, testing-based (cf. dudect DATE'17, \
         DATA USENIX'18, MicroWalk ACSAC'18).\n"
    );

    let cases = build_cases();

    // Confusion matrix counts: detector verdict (Flag = "positive") vs. label.
    let mut tp = 0; // expected Flag, detected Flag
    let mut tn = 0; // expected Pass, detected Pass
    let mut fp = 0; // expected Pass, detected Flag
    let mut fn_ = 0; // expected Flag, detected Pass

    for case in &cases {
        let res = &case.result;
        let detected = label_of(&res.verdict);

        println!("=== [{}] case: {} ===", case.guest, case.name);
        println!("  expected : {:?}", case.expected);
        println!("  detected : {:?}", detected);
        println!(
            "  run A    : user_cycles={} total_cycles={} journal={}B exit={}",
            res.run_a.user_cycles,
            res.run_a.total_cycles,
            res.run_a.journal.as_ref().map(|j| j.len()).unwrap_or(0),
            exit_str(&res.run_a.exit),
        );
        println!(
            "  run B    : user_cycles={} total_cycles={} journal={}B exit={}",
            res.run_b.user_cycles,
            res.run_b.total_cycles,
            res.run_b.journal.as_ref().map(|j| j.len()).unwrap_or(0),
            exit_str(&res.run_b.exit),
        );
        println!(
            "  diff     : cycles={} journal={} exit={}",
            res.diff.cycles_differ, res.diff.journal_differs, res.diff.exit_differs
        );
        println!("  why      : {}", case.rationale);
        let agree = detected == case.expected;
        println!("  result   : {}\n", if agree { "OK (matches label)" } else { "MISMATCH" });

        match (case.expected, detected) {
            (Label::Flag, Label::Flag) => tp += 1,
            (Label::Pass, Label::Pass) => tn += 1,
            (Label::Pass, Label::Flag) => fp += 1,
            (Label::Flag, Label::Pass) => fn_ += 1,
        }
    }

    println!("================ confusion matrix (Flag = positive) ================");
    println!("  cases                                : {}", cases.len());
    println!("  TP (leak found, expected leak)       : {tp}");
    println!("  TN (clean,      expected clean)      : {tn}");
    println!("  FP (false alarm on clean guest)      : {fp}");
    println!("  FN (missed leak)                     : {fn_}");
    let total = tp + tn + fp + fn_;
    println!("  accuracy                             : {}/{}", tp + tn, total);
    println!("====================================================================");

    if fp > 0 || fn_ > 0 {
        eprintln!("\nWARNING: detector disagreed with expected labels on {} case(s).", fp + fn_);
        std::process::exit(1);
    }
}

fn exit_str(e: &ExitOutcome) -> String {
    match e {
        ExitOutcome::Halted(c) => format!("Halted({c})"),
        ExitOutcome::Paused(c) => format!("Paused({c})"),
        ExitOutcome::SessionLimit => "SessionLimit".to_string(),
        ExitOutcome::Fault => "Fault".to_string(),
        ExitOutcome::ExecutorError(_) => "Fault(executor-error)".to_string(),
    }
}
