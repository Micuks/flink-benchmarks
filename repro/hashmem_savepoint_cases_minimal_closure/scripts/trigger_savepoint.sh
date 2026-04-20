#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER_REST="${JOBMANAGER_REST:-http://127.0.0.1:8081}"
JOB_ID="${JOB_ID:?JOB_ID is required}"
TARGET_DIR="${TARGET_DIR:?TARGET_DIR is required}"

request_id="$(
  curl -sS -X POST \
    -H "Content-Type: application/json" \
    --data "{\"target-directory\":\"$TARGET_DIR\",\"cancel-job\":false}" \
    "$JOBMANAGER_REST/jobs/$JOB_ID/savepoints" \
  | sed -n 's/.*"request-id":"\\([^"]*\\)".*/\\1/p'
)"

if [[ -z "$request_id" ]]; then
  echo "failed to create savepoint request" >&2
  exit 1
fi

for _ in $(seq 1 120); do
  resp="$(curl -sS "$JOBMANAGER_REST/jobs/$JOB_ID/savepoints/$request_id")"
  status="$(printf '%s\n' "$resp" | sed -n 's/.*"status":{"id":"\\([^"]*\\)".*/\\1/p')"
  if [[ "$status" == "COMPLETED" ]]; then
    savepoint_path="$(printf '%s\n' "$resp" | sed -n 's/.*"location":"\\([^"]*\\)".*/\\1/p')"
    if [[ -z "$savepoint_path" ]]; then
      echo "savepoint completed but location is empty" >&2
      exit 1
    fi
    echo "SAVEPOINT_PATH=$savepoint_path"
    exit 0
  fi
  if printf '%s\n' "$resp" | grep -q '"failure-cause"'; then
    printf '%s\n' "$resp" >&2
    exit 1
  fi
  sleep 2
done

echo "savepoint request timed out: $request_id" >&2
exit 1
