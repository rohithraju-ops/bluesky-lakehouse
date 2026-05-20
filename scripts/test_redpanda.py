import json
import time
import uuid
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = "localhost:19092"
TOPIC = "test-roundtrip"
GROUP = f"test-{uuid.uuid4().hex[:8]}"  # unique per run = no offset surprises

def ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BROKER})
    fs = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Created topic {topic}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Topic {topic} exists, reusing")
            else:
                raise

def produce() -> list[str]:
    producer = Producer({"bootstrap.servers": BROKER})
    sent = []
    for i in range(10):
        payload = json.dumps({"n": i, "ts": time.time()})
        producer.produce(TOPIC, payload.encode())
        sent.append(payload)
    producer.flush()
    print(f"Produced {len(sent)} messages")
    return sent

def consume(expected: int) -> list[str]:
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    received = []
    deadline = time.time() + 10
    while len(received) < expected and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        received.append(msg.value().decode())
    consumer.close()
    print(f"Consumed {len(received)} messages")
    return received

def main() -> None:
    ensure_topic()
    sent = produce()
    received = consume(len(sent))
    assert sorted(sent) == sorted(received), "Round-trip mismatch"
    print(f"OK: {len(sent)} sent / {len(received)} received / all match")

if __name__ == "__main__":
    main()