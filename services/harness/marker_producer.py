# services/harness/marker_producer.py
"""
Emit N marker events to Redpanda topic bsky.posts.
Marker events share the Bluesky message shape but use a special DID prefix
'harness:' and a UUID embedded in text so we can identify them in Iceberg.

Writes the CONFIRMED-DELIVERED UUIDs to docs/harness/expected_uuids.txt after
all messages flush — only UUIDs that Kafka acknowledged are written, so the
ground-truth set is accurate even if the broker restarts mid-run.
"""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from confluent_kafka import Producer, KafkaError

OUT = Path("docs/harness/expected_uuids.txt")
OUT.parent.mkdir(parents=True, exist_ok=True)


def build_event(marker_uuid: str) -> dict:
    now_us = int(time.time() * 1_000_000)
    return {
        "did": f"harness:{marker_uuid}",
        "time_us": now_us,
        "kind": "commit",
        "commit": {
            "rev": "harness",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": marker_uuid[:8],
            "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "text": f"HARNESS_MARKER_{marker_uuid}",
                "langs": ["en"],
            },
            "cid": "harness",
        },
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=10_000)
    p.add_argument("--rate", type=float, default=200.0, help="events/sec")
    p.add_argument("--brokers", default="localhost:19092")
    p.add_argument("--topic", default="bsky.posts")
    args = p.parse_args()

    confirmed: set[str] = set()
    errors: list[str] = []
    lock = Lock()

    def on_delivery(err, msg):
        u = msg.key().decode() if msg.key() else json.loads(msg.value())["did"].split(":", 1)[1]
        with lock:
            if err is None:
                confirmed.add(u)
            else:
                errors.append(f"{u}: {err}")

    producer = Producer({
        "bootstrap.servers": args.brokers,
        "enable.idempotence": True,
        # Serial in-flight prevents sequence-number desync on broker restart.
        "max.in.flight.requests.per.connection": 1,
        "acks": "all",
        "retries": 10,
        "retry.backoff.ms": 500,
        "compression.type": "zstd",
    })

    sleep_s = 1.0 / args.rate
    started = time.time()
    all_uuids = []

    for i in range(args.n):
        u = str(uuid.uuid4())
        all_uuids.append(u)
        try:
            producer.produce(
                args.topic,
                key=u.encode(),
                value=json.dumps(build_event(u)).encode("utf-8"),
                on_delivery=on_delivery,
            )
        except Exception as exc:
            errors.append(f"{u}: {exc}")
            break
        if i % 100 == 0:
            producer.poll(0)
        time.sleep(sleep_s)

    producer.flush(60)
    elapsed = time.time() - started

    # Write only confirmed-delivered UUIDs as ground truth
    with OUT.open("w") as f:
        for u in all_uuids:
            if u in confirmed:
                f.write(u + "\n")

    print(f"Produced {len(all_uuids)} markers in {elapsed:.1f}s, {len(confirmed)} confirmed, {len(errors)} errors")
    if errors:
        print(f"First errors: {errors[:3]}")
    print(f"Wrote {len(confirmed)} confirmed UUIDs to {OUT}")


if __name__ == "__main__":
    main()
