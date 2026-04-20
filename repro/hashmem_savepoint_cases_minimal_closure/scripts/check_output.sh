#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${1:?usage: check_output.sh <output-dir>}"

if [[ ! -d "$OUTPUT_DIR" ]]; then
  echo "output dir not found: $OUTPUT_DIR" >&2
  exit 1
fi

files=("$OUTPUT_DIR"/part-*.tsv)
if [[ ! -e "${files[0]}" ]]; then
  echo "no part file found under $OUTPUT_DIR" >&2
  exit 1
fi

wc -l "${files[@]}"
echo "== head =="
head -5 "${files[@]}" || true
