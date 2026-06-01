#!/usr/bin/env bash
# run.sh – execute the pipeline locally
# Usage: bash run.sh [input_dir] [output_dir] [contract_path]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

INPUT="${1:-$SCRIPT_DIR/data/input}"
OUTPUT="${2:-$SCRIPT_DIR/data/output}"
CONTRACT="${3:-$SCRIPT_DIR/Contract_rules.yaml}"

echo "============================================"
echo "  Data Contract Validation Pipeline"
echo "  Input:    $INPUT"
echo "  Output:   $OUTPUT"
echo "  Contract: $CONTRACT"
echo "============================================"

cd "$SCRIPT_DIR/src"
python pipeline.py "$INPUT" "$OUTPUT" "$CONTRACT"

echo ""
echo "Done. Outputs:"
echo "  Silver    → $OUTPUT/silver"
echo "  Quarantine→ $OUTPUT/quarantine"
echo "  Report    → $OUTPUT/report"
