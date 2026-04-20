#!/usr/bin/env bash
set -euo pipefail

CLOSURE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLINK_BIN="${FLINK_BIN:-/opt/flink/bin/flink}"
JAR_PATH="${JAR_PATH:-$CLOSURE_DIR/job/hashmem-savepoint-cases.jar}"

CLASS_NAME="${CLASS_NAME:?CLASS_NAME is required}"
MODE="${MODE:?MODE is required}"
OUTPUT_DIR="${OUTPUT_DIR:?OUTPUT_DIR is required}"

PARALLELISM="${PARALLELISM:-1}"
SOURCE_PARALLELISM="${SOURCE_PARALLELISM:-$PARALLELISM}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-3600000}"
EVENTS_PER_SECOND="${EVENTS_PER_SECOND:-50000}"
SAVEPOINT_PATH="${SAVEPOINT_PATH:-}"

declare -a cmd
cmd=("$FLINK_BIN" run -d)

if [[ -n "$SAVEPOINT_PATH" ]]; then
  cmd+=("-Dexecution.savepoint.path=$SAVEPOINT_PATH")
fi

cmd+=(
  -c "$CLASS_NAME"
  "$JAR_PATH"
  --mode "$MODE"
  --parallelism "$PARALLELISM"
  --sourceParallelism "$SOURCE_PARALLELISM"
  --checkpointIntervalMs "$CHECKPOINT_INTERVAL_MS"
  --eventsPerSecond "$EVENTS_PER_SECOND"
  --output "$OUTPUT_DIR"
)

case "$CLASS_NAME" in
  org.apache.flink.benchmark.HashMemtableSavepointRiskJob)
    cmd+=(--keys "${KEYS:-50000}")
    ;;
  org.apache.flink.benchmark.HashMemtableSavepointRmwRiskJob)
    cmd+=(--keys "${KEYS:-250000}" --updatesPerKey "${UPDATES_PER_KEY:-2}")
    ;;
  org.apache.flink.benchmark.HashMemtableSavepointMapStateRiskJob)
    cmd+=(--stateKeys "${STATE_KEYS:-100000}" --entriesPerKey "${ENTRIES_PER_KEY:-5}")
    ;;
  *)
    echo "unsupported CLASS_NAME: $CLASS_NAME" >&2
    exit 1
    ;;
esac

echo "submitting: ${cmd[*]}" >&2
output="$("${cmd[@]}")"
echo "$output" >&2

job_id="$(printf '%s\n' "$output" | sed -n 's/.*JobID \\([0-9a-f]\\{32\\}\\).*/\\1/p' | tail -1)"
if [[ -z "$job_id" ]]; then
  echo "failed to parse job id" >&2
  exit 1
fi

echo "JOB_ID=$job_id"
