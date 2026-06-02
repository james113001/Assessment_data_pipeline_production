#!/usr/bin/env bash
# Usage: bash run.sh [input_dir] [output_dir] [contract_path] [start_date] [end_date]
# Dates are optional (YYYY-MM-DD). Omit both to process all files in input_dir.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

INPUT="${1:-$SCRIPT_DIR/data/input}"
OUTPUT="${2:-$SCRIPT_DIR/data/output}"
CONTRACT="${3:-$SCRIPT_DIR/Contract_rules.yaml}"
START_DATE="${4:-}"
END_DATE="${5:-}"

echo "============================================"
echo "  Data Contract Validation Pipeline"
echo "  Input:    $INPUT"
echo "  Output:   $OUTPUT"
echo "  Contract: $CONTRACT"
if [[ -n "$START_DATE" || -n "$END_DATE" ]]; then
  echo "  Dates:    ${START_DATE:-unbounded} → ${END_DATE:-unbounded}"
fi
echo "============================================"

cd "$SCRIPT_DIR/src"
python pipeline.py "$INPUT" "$OUTPUT" "$CONTRACT" "$START_DATE" "$END_DATE"

echo ""
echo "Done. Outputs:"
echo "  Silver    → $OUTPUT/silver"
echo "  Quarantine→ $OUTPUT/quarantine"
echo "  Report    → $OUTPUT/report"
