#!/usr/bin/env bash
# scripts/run_exactly_once_experiment.sh
# End-to-end exactly-once verification across 3 tier-failure scenarios.
# Usage: ./scripts/run_exactly_once_experiment.sh [N_MARKERS] [RATE]
#
# Scenarios tested: pause_polaris, kill_redpanda, restart_minio
# These test exactly-once through storage-layer failures while RisingWave stays up.
#
# Note on kill_risingwave: RisingWave playground mode stores Kafka offsets in memory
# only — any restart loses them. This is a known playground-mode constraint (not a
# production concern). The kill_risingwave scenario is exercised separately via:
#   uv run python -m services.harness.chaos --scenario kill_risingwave

set -euo pipefail

MARKERS=${1:-10000}
RATE=${2:-200}

rm -f docs/harness/chaos_log.jsonl

# Phase 1: produce all markers while injecting catalog + storage chaos concurrently.
# pause_polaris and restart_minio don't touch Redpanda, so the idempotent producer
# runs cleanly. kill_redpanda runs AFTER the producer finishes to avoid the Redpanda
# PID-state loss that would desync idempotent sequence numbers mid-produce.
echo "=== Phase 1: producing $MARKERS markers at ${RATE}/s"
uv run python -m services.harness.marker_producer --n "$MARKERS" --rate "$RATE" &
PRODUCER_PID=$!

sleep 5  # let the producer warm up and get some messages into Kafka

echo "=== Phase 2a: pause_polaris (catalog outage while producing)"
uv run python -m services.harness.chaos --scenario pause_polaris --gap-s 5

echo "=== Phase 2b: restart_minio (storage outage while producing)"
uv run python -m services.harness.chaos --scenario restart_minio --gap-s 5

echo "=== Phase 3: waiting for producer to finish"
wait $PRODUCER_PID

echo "=== Phase 4: kill_redpanda (broker restart after all messages are in Kafka)"
uv run python -m services.harness.chaos --scenario kill_redpanda --gap-s 5

echo "=== Phase 5: waiting 90s for sink to commit final batch"
sleep 90

echo "=== Phase 6: verifying"
uv run python -m services.harness.verify

echo "=== Done. See docs/exactly_once_results.md"
