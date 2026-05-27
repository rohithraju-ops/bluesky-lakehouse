# scripts/measure_lateness.py
"""
Snapshot event-lateness distribution from RisingWave and write to docs/lateness.csv.
Run after the system has been ingesting for at least 10 minutes.
Prints p50/p95/p99 lateness in seconds to stdout.
"""
import csv
from pathlib import Path

import psycopg

OUT = Path("docs/lateness.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

QUERY = """
SELECT lateness_seconds, event_count
FROM event_lateness_hist
ORDER BY lateness_seconds;
"""


def main():
    with psycopg.connect("host=localhost port=4566 dbname=dev user=root") as conn:
        rows = conn.execute(QUERY).fetchall()

    if not rows:
        print("No rows in event_lateness_hist yet — wait longer and retry.")
        return

    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["lateness_seconds", "event_count"])
        w.writerows(rows)

    total = sum(r[1] for r in rows)
    cum = 0
    pct = {50: None, 95: None, 99: None}
    for sec, cnt in rows:
        cum += cnt
        for p in pct:
            if pct[p] is None and cum / total >= p / 100:
                pct[p] = sec

    print(f"Sampled {total} events.")
    print(f"p50={pct[50]}s  p95={pct[95]}s  p99={pct[99]}s")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
