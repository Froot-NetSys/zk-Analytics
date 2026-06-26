# Query Checker

Static query-policy checker for zkTelemetry's querier requests.

This tool **does not generate proofs** and does not analyze circuits. It parses a
querier `QueryRequest` (the same JSON shape as querier's `/query` request) and
enforces a query-level policy covering PII outputs, aggregation-only queries,
allowed `GROUP BY` dimensions, and a minimum-support (k-anonymity style) rule.

The real proving system is RISC0 zkVM; this crate is a lightweight, standalone
policy gate with no zkVM/R1CS dependencies.

## Run

This is a standalone crate (it carries its own empty `[workspace]` table), so run
commands from inside `query_checker/`:

- Run tests:
  - `cargo test`
- Check a request:
  - `cargo run --bin query-checker -- --querier-request '{"type":"cm_estimate"}'`
  - Read JSON from a file (prefix the path with `@`):
    `cargo run --bin query-checker -- --querier-request @req.json`

For privacy checking, only the `type` field is required; other fields are
optional and defaulted.

## Query policy rules

- **No raw PII outputs** (e.g., IPs/usernames/ports); only anonymized IDs can be
  exposed (and only when `allow_anonymized_ids` is enabled).
- **Aggregation-only** queries (no single-key/flow lookups).
- **`GROUP BY`** limited to approved dimensions (e.g., anonymized ID, subnet
  prefix, application class, bucket).
- **Minimum support**: if the request carries `support` (the number of
  contributing records), it must be `>= min_support` (default `10`). `support`
  is committed by the guest to the journal at proving time, so at static-check
  time it may be absent — when absent, this rule is skipped.

Output:

- `query-policy: OK`
- `query-policy: VIOLATION` (with each violated rule and a detail string)
