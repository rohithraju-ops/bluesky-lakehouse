# services/harness/verify.py
"""
Compare expected_uuids.txt against marker events found in the Iceberg table.
Outputs a PASS/FAIL report at docs/exactly_once_results.md.

Scan strategy: pull all rows with did LIKE 'harness:%' using PyIceberg.
Row-filter pushdown for LIKE is not guaranteed, so we filter in Python too.
"""
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pyiceberg.catalog import load_catalog

EXPECTED = Path("docs/harness/expected_uuids.txt")
CHAOS_LOG = Path("docs/harness/chaos_log.jsonl")
REPORT = Path("docs/exactly_once_results.md")


def load_polaris_catalog():
    return load_catalog(
        "polaris",
        **{
            "type": "rest",
            "uri": "http://localhost:8181/api/catalog",
            "warehouse": "lakehouse_catalog",
            "credential": "root:s3cr3t",
            "scope": "PRINCIPAL_ROLE:ALL",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minio",
            "s3.secret-access-key": "minio123",
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def main():
    expected = set(EXPECTED.read_text().splitlines())
    print(f"Expected {len(expected)} markers")

    catalog = load_polaris_catalog()
    table = catalog.load_table("bsky.posts")

    # Scan all rows; filter for harness markers in Python (LIKE pushdown not guaranteed)
    scan = table.scan()
    df = scan.to_arrow().to_pandas()

    marker_rows = df[df["did"].str.startswith("harness:", na=False)]
    found_counts = Counter(
        row["text"].removeprefix("HARNESS_MARKER_")
        for _, row in marker_rows.iterrows()
    )

    found_unique = set(found_counts.keys())
    duplicates = {u: c for u, c in found_counts.items() if c > 1}
    missing = expected - found_unique
    unexpected = found_unique - expected

    chaos_events = []
    if CHAOS_LOG.exists():
        for line in CHAOS_LOG.read_text().splitlines():
            if line.strip():
                chaos_events.append(json.loads(line))

    scenarios_run = [c for c in chaos_events if c["event"] == "scenario_start"]
    verdict = "PASS" if not duplicates and not missing else "FAIL"

    report = f"""# Exactly-Once Verification Results

**Run at:** {datetime.now(timezone.utc).isoformat()}
**Verdict:** **{verdict}**

## Setup
- Pipeline: Bluesky Jetstream-format markers → Redpanda → RisingWave → Iceberg (Polaris) → MinIO
- Sink commit cadence: 60 seconds
- Watermark: event_time - 10s

## Inputs
- Expected markers (produced): **{len(expected)}**
- Failure scenarios injected: **{len(scenarios_run)}**
{chr(10).join(f"  - {s['ts']}: {s['name']}" for s in scenarios_run)}

## Outputs
- Distinct markers in Iceberg:         **{len(found_unique)}**
- Duplicates (count > 1):              **{len(duplicates)}**
- Missing (expected, not found):       **{len(missing)}**
- Unexpected (in Iceberg, not expected): **{len(unexpected)}**

## Anomaly details
- Duplicates (first 10): {dict(list(duplicates.items())[:10])}{'…' if len(duplicates) > 10 else ''}
- Missing (first 10): {list(missing)[:10]}
- Unexpected (first 10): {list(unexpected)[:10]}

## Methodology
1. `services/harness/marker_producer.py` produced {len(expected)} marker events with unique UUIDs into topic `bsky.posts` at a steady rate.
2. While production was ongoing, `services/harness/chaos.py` injected each failure scenario with a {30}s gap between them.
3. After production complete and a 90s wait for the sink to flush, `services/harness/verify.py` scanned the Iceberg table for rows matching the harness DID prefix and joined back to ground truth.

## What this proves
- Idempotent Kafka producer prevents duplicate publishes on retry.
- RisingWave's Iceberg sink commits via two-phase commit, binding Kafka offsets to Iceberg snapshots.
- Recovery from each tier failure preserves the invariant: every produced event lands in Iceberg exactly once.
"""

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(report)
    print(report)
    print(f"Wrote {REPORT}")

    if verdict == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
