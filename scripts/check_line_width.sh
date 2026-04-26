#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAX_WIDTH="${1:-179}"

if ! [[ "$MAX_WIDTH" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 [max_width]" >&2
  exit 2
fi

violations=0

while IFS= read -r -d '' file; do
  line_no=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_no=$((line_no + 1))
    # Ignore package/import declarations for width counting.
    if [[ "$line" =~ ^[[:space:]]*package[[:space:]] || "$line" =~ ^[[:space:]]*import[[:space:]] ]]; then
      continue
    fi
    if (( ${#line} > MAX_WIDTH )); then
      echo "$file:$line_no:${#line} > $MAX_WIDTH"
      violations=$((violations + 1))
    fi
  done < "$file"
done < <(find "$ROOT_DIR" -type f -name '*.java' -print0)

if (( violations > 0 )); then
  echo "Found $violations line-width violations (max=$MAX_WIDTH)."
  exit 1
fi

echo "OK: no line exceeds $MAX_WIDTH chars in Java files."
