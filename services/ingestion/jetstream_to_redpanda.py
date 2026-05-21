import asyncio
import json
import logging
import os
import signal
import time
from typing import Optional

import websockets
from confluent_kafka import Producer

LOGGER = logging.getLogger("ingestion")

# Jetstream is a public Bluesky service
JETSTREAM_HOST = "jetstream2.us-east.bsky.network"
WANTED_COLLECTIONS = ["app.bsky.feed.post"]

# Redpanda config
BROKER = os.environ.get("REDPANDA_BROKER", "localhost:19092")
TOPIC = "bsky.posts"

RECONNECT_BASE_SECONDS = 1
RECONNECT_MAX_SECONDS = 30

# Stats reporting interval
STATS_EVERY = 1000


class Ingestor:
    def __init__(self) -> None:
        self.producer = Producer({
            "bootstrap.servers": BROKER,
            "linger.ms": 100,
            "compression.type": "zstd",
            "acks": "all",
            "enable.idempotence": True,
        })
        self.cursor: Optional[int] = None  # lastseen event time_us
        self.shutdown = asyncio.Event()
        self.stats = {"events": 0, "bytes": 0, "start": time.time()}

    def _build_url(self) -> str:
        params = [f"wantedCollections={c}" for c in WANTED_COLLECTIONS]
        if self.cursor is not None:
            # rewind a few seconds for gapless playback if reconnecting
            params.append(f"cursor={self.cursor - 5_000_000}")
        return f"wss://{JETSTREAM_HOST}/subscribe?" + "&".join(params)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            LOGGER.error("Delivery failed for offset %s: %s", msg.offset(), err)

    def _maybe_log_stats(self) -> None:
        n = self.stats["events"]
        if n and n % STATS_EVERY == 0:
            elapsed = time.time() - self.stats["start"]
            rate = n / elapsed
            mb = self.stats["bytes"] / (1024 * 1024)
            LOGGER.info(
                "Processed %d events | %.1f events/sec | %.2f MB total",
                n, rate, mb,
            )

    async def _consume_one_session(self) -> None:
        """Open one WebSocket session; stream events until disconnect or shutdown."""
        url = self._build_url()
        LOGGER.info("Connecting: %s", url[:100])
        async with websockets.connect(url, max_size=2**20) as ws:
            async for raw in ws:
                if self.shutdown.is_set():
                    break
                # raw is str (text frame) — produce raw JSON bytes to Redpanda
                payload = raw.encode() if isinstance(raw, str) else raw
                self.producer.produce(TOPIC, payload, callback=self._on_delivery)
                # poll(0) services delivery callbacks without blocking
                self.producer.poll(0)
                # update cursor + stats
                event = json.loads(raw)
                self.cursor = event.get("time_us", self.cursor)
                self.stats["events"] += 1
                self.stats["bytes"] += len(payload)
                self._maybe_log_stats()

    async def run(self) -> None:
        backoff = RECONNECT_BASE_SECONDS
        while not self.shutdown.is_set():
            try:
                await self._consume_one_session()
                backoff = RECONNECT_BASE_SECONDS  # reset on clean exit
            except Exception as e:
                LOGGER.warning("Connection error: %s. Reconnecting in %ds.", e, backoff)
                try:
                    await asyncio.wait_for(self.shutdown.wait(), timeout=backoff)
                    break  # shutdown signaled during sleep
                except asyncio.TimeoutError:
                    pass  # backoff elapsed; loop and retry
                backoff = min(backoff * 2, RECONNECT_MAX_SECONDS)
        LOGGER.info("Flushing producer buffer...")
        self.producer.flush(timeout=10)
        LOGGER.info("Final stats: %s", self.stats)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


async def main() -> None:
    setup_logging()
    ingestor = Ingestor()
    loop = asyncio.get_running_loop()

    def handler() -> None:
        LOGGER.info("Shutdown signal received")
        ingestor.shutdown.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handler)

    await ingestor.run()


if __name__ == "__main__":
    asyncio.run(main())