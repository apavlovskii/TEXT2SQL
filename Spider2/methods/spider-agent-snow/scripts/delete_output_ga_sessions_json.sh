#!/usr/bin/env bash
set -euo pipefail

# One-off cleanup script:
# Delete only files matching:
#   - GA_SESSIONS*.json
#   - EVENTS*.json
#   - snowflake_credential.json
# under spider-agent-snow/output/
# Usage:
#   bash Spider2/methods/spider-agent-snow/scripts/delete_output_ga_sessions_json.sh --dry-run
#   bash Spider2/methods/spider-agent-snow/scripts/delete_output_ga_sessions_json.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="$ROOT_DIR/output"
DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

if [[ ! -d "$OUTPUT_DIR" ]]; then
  echo "Output directory not found: $OUTPUT_DIR"
  exit 1
fi

mapfile -t TARGETS < <(
  find "$OUTPUT_DIR" -type f \( \
    -name 'GA_SESSIONS*.json' -o \
    -name 'EVENTS*.json' -o \
    -name 'snowflake_credential.json' \
  \) | sort
)

echo "Output dir: $OUTPUT_DIR"
echo "Matched files: ${#TARGETS[@]}"

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No matching files found."
  exit 0
fi

for file_path in "${TARGETS[@]}"; do
  rel_path="${file_path#$ROOT_DIR/}"
  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] would delete: $rel_path"
  else
    rm -f -- "$file_path"
    echo "deleted: $rel_path"
  fi
done

if [[ "$DRY_RUN" == true ]]; then
  echo "Dry-run complete. No files were deleted."
else
  echo "Deletion complete."
fi
