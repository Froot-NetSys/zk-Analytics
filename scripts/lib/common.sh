#!/usr/bin/env bash
# Shared configuration for zk-Analytics evaluation scripts.
#
# Source this AFTER computing the repo root; it only sets environment, so
# behaviour is identical to inlining the exports below:
#
#   ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
#   source "$ROOT_DIR/scripts/lib/common.sh"
#
# Aggregation structural parameters. These MUST match the values baked into the
# zkVM guests and used across every experiment (samples hash-table geometry,
# histogram slot count, Count-Min top-k slots).
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
