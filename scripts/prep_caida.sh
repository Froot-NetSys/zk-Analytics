#!/usr/bin/env bash
set -euo pipefail
# Extract per-key (src_ip,dst_ip,length) txt files from a CAIDA pcap(.gz) for
# the native e2e baseline. Caps to MAX_PACKETS (default 150k; the paper uses
# 131,072 logs) so we don't parse the whole multi-GB trace.
#
# Usage: PCAP=/mydata/equinix-nyc.dirA.20190117-125910.UTC.anon.pcap.gz \
#        scripts/prep_caida.sh
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PCAP="${PCAP:-$(ls -1 /mydata/equinix-*.pcap.gz 2>/dev/null | head -1)}"
MAX_PACKETS="${MAX_PACKETS:-150000}"
JOBS="${JOBS:-8}"
OUT_DIR="$ROOT_DIR/testdata/caida_pcap/caida_txt"

[[ -n "$PCAP" && -f "$PCAP" ]] || { echo "no pcap found (set PCAP=...)"; exit 1; }
mkdir -p "$OUT_DIR"
echo "[caida] parsing $PCAP (max $MAX_PACKETS packets) -> $OUT_DIR"
python3 "$ROOT_DIR/testdata/caida_pcap/pcap_ip_pairs_to_txt.py" \
  --out-dir "$OUT_DIR" --max-packets "$MAX_PACKETS" -j "$JOBS" "$PCAP"
echo "[caida] produced $(ls "$OUT_DIR" | wc -l) key files"
