use query_checker::{
    check_query_request, is_membership_test, QueryPolicy, QueryRequest, QueryRuleViolationKind,
};

#[test]
fn allows_simple_aggregate_query() {
    let req = QueryRequest::from_json(r#"{"type":"samples_sum"}"#).unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(report.ok);
}

#[test]
fn blocks_single_key_query() {
    let req = QueryRequest::from_json(r#"{"type":"samples_avg_key","key":123}"#).unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(!report.ok);
    assert!(report
        .violations
        .iter()
        .any(|v| matches!(v.kind, QueryRuleViolationKind::NonAggregateQuery)));
}

#[test]
fn blocks_pii_output_by_default() {
    let req = QueryRequest::from_json(r#"{"type":"cm_topk"}"#).unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(!report.ok);
    assert!(report
        .violations
        .iter()
        .any(|v| matches!(v.kind, QueryRuleViolationKind::PiiOutput)));
}

#[test]
fn blocks_raw_pii_group_by() {
    let req = QueryRequest::from_json(r#"{"type":"samples_sum","group_by":["ip"]}"#).unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(!report.ok);
    assert!(report
        .violations
        .iter()
        .any(|v| matches!(v.kind, QueryRuleViolationKind::GroupByNotAllowed)));
}

#[test]
fn blocks_low_support_query() {
    let req = QueryRequest::from_json(r#"{"type":"samples_sum","support":3}"#).unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(!report.ok);
    assert!(report
        .violations
        .iter()
        .any(|v| matches!(v.kind, QueryRuleViolationKind::LowSupport)));
}

#[test]
fn allows_sufficient_support_query() {
    let req = QueryRequest::from_json(r#"{"type":"samples_sum","support":50}"#).unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(report.ok);
}

#[test]
fn allows_anonymized_ids_when_enabled() {
    let req = QueryRequest::from_json(r#"{"type":"cm_topk"}"#).unwrap();
    let mut policy = QueryPolicy::default();
    policy.allow_anonymized_ids = true;
    let report = check_query_request(&req, &policy);
    assert!(report.ok);
}

#[test]
fn membership_detector_uses_predicate_selectivity() {
    let policy = QueryPolicy::default(); // min_anonymity_bits = 16

    // Exact key (mask defaults to all-ones) pins one identity => membership.
    let exact = QueryRequest::from_json(r#"{"type":"samples_sum_key","key":7}"#).unwrap();
    assert!(is_membership_test(&exact, &policy));

    // Broad /8-style mask (8 bits fixed, 56 free) is a population aggregate.
    let broad = QueryRequest::from_json(r#"{"type":"samples_sum_key","key":7,"mask":255}"#).unwrap();
    assert!(!is_membership_test(&broad, &policy));

    // Pure aggregate has no key predicate at all.
    let agg = QueryRequest::from_json(r#"{"type":"samples_sum"}"#).unwrap();
    assert!(!is_membership_test(&agg, &policy));

    // Exact-key estimate kind => membership.
    let cm = QueryRequest::from_json(r#"{"type":"cm_estimate","key":42}"#).unwrap();
    assert!(is_membership_test(&cm, &policy));
}

#[test]
fn blocks_narrow_pattern() {
    // A fully-specified 16-nibble pattern pins one key => membership.
    let req =
        QueryRequest::from_json(r#"{"type":"samples_sum_key_pattern","pattern":"aabbccddeeff0011"}"#)
            .unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(report
        .violations
        .iter()
        .any(|v| matches!(v.kind, QueryRuleViolationKind::NonAggregateQuery)));
}

#[test]
fn allows_broad_pattern() {
    // Mostly-wildcard pattern matches a large key class => allowed.
    let req =
        QueryRequest::from_json(r#"{"type":"samples_sum_key_pattern","pattern":"a???????????????"}"#)
            .unwrap();
    let report = check_query_request(&req, &QueryPolicy::default());
    assert!(report.ok);
}
