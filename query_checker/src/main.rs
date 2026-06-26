use anyhow::{Context, Result};
use query_checker::{check_query_request, QueryPolicy, QueryRequest, QueryRuleReport};

fn print_query_rule_report(label: &str, report: &QueryRuleReport) {
    if report.ok {
        println!("{label}: OK");
        return;
    }
    println!("{label}: VIOLATION");
    for v in &report.violations {
        println!("- {}: {}", v.kind.as_str(), v.detail);
    }
}

fn main() -> Result<()> {
    let mut querier_req: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--querier-request" => {
                querier_req = Some(
                    args.next()
                        .context("--querier-request needs a JSON string or @file")?,
                )
            }
            "--help" | "-h" => {
                eprintln!(
                    "usage: query-checker --querier-request '<json>'\n\
                           query-checker --querier-request @req.json\n\n\
                     notes:\n  \
                     - Only the `type` field is required; other fields are optional.\n  \
                     - `support` (number of contributing records) is optional; the low-support\n    \
                       rule only fires when it is present.\n\n\
                     examples:\n  \
                     query-checker --querier-request '{{\"type\":\"cm_estimate\"}}'\n  \
                     query-checker --querier-request '{{\"type\":\"samples_sum\",\"support\":3}}'\n"
                );
                return Ok(());
            }
            other => return Err(anyhow::anyhow!("unknown arg: {other}")),
        }
    }

    let s = querier_req.context("missing --querier-request (use --help for usage)")?;
    let json = if let Some(path) = s.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("read {path}"))?
    } else {
        s
    };
    let req = QueryRequest::from_json(&json).context("parse querier request JSON")?;
    let query_policy = QueryPolicy::default();
    let query_report = check_query_request(&req, &query_policy);
    print_query_rule_report("query-policy", &query_report);
    if !query_report.ok {
        return Err(anyhow::anyhow!("query policy violation"));
    }
    Ok(())
}
