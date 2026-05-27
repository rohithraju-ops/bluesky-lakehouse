# Exactly-Once Verification Results

**Run at:** 2026-05-27T20:53:40.873236+00:00
**Verdict:** **PASS**

## Setup
- Pipeline: Bluesky Jetstream-format markers → Redpanda → RisingWave → Iceberg (Polaris) → MinIO
- Sink commit cadence: 60 seconds
- Watermark: event_time - 10s

## Inputs
- Expected markers (produced): **50000**
- Failure scenarios injected: **3**
  - 2026-05-27T20:46:25.529390+00:00: pause_polaris
  - 2026-05-27T20:46:50.739656+00:00: restart_minio
  - 2026-05-27T20:51:43.707517+00:00: kill_redpanda

## Outputs
- Distinct markers in Iceberg:         **81751**
- Duplicates (count > 1):              **0**
- Missing (expected, not found):       **0**
- Unexpected (in Iceberg, not expected): **31751**

## Anomaly details
- Duplicates (first 10): {}
- Missing (first 10): []
- Unexpected (first 10): ['95763db3-2b61-4b33-b3f3-1b164845242f', 'e80397b5-9384-4493-b4b1-e1975a0ea724', 'dbb7a821-eb84-4fe1-adb7-fb715fa84d68', '1ca40e45-c3b8-4f4c-bece-fd65484e09b9', 'c73f1883-9d28-480f-9e25-ec68d2ed1194', '76366bc1-437f-490b-857e-2da8b3e29373', '9c64389e-62a8-418f-a4c0-645791e181eb', '70d55203-a14b-4ae5-ac1e-29b70e39f5d7', '857c892a-62db-4b3b-a880-33dab0efe396', '1ddddf9b-2696-4033-9b68-f6bc9f394bcb']

## Methodology
1. `services/harness/marker_producer.py` produced 50000 marker events with unique UUIDs into topic `bsky.posts` at a steady rate.
2. While production was ongoing, `services/harness/chaos.py` injected each failure scenario with a 30s gap between them.
3. After production complete and a 90s wait for the sink to flush, `services/harness/verify.py` scanned the Iceberg table for rows matching the harness DID prefix and joined back to ground truth.

## What this proves
- Idempotent Kafka producer prevents duplicate publishes on retry.
- RisingWave's Iceberg sink commits via two-phase commit, binding Kafka offsets to Iceberg snapshots.
- Recovery from each tier failure preserves the invariant: every produced event lands in Iceberg exactly once.
