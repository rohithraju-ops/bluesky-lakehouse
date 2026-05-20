import asyncio
import json
from datetime import datetime, timezone
import websockets

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)

async def main() -> None:
    async with websockets.connect(JETSTREAM_URL) as ws:
        for i in range(10):
            raw = await ws.recv()
            event = json.loads(raw)
            ts = datetime.fromtimestamp(event["time_us"] / 1_000_000, tz=timezone.utc)
            record = (event.get("commit") or {}).get("record") or {}
            text = record.get("text", "")[:80].replace("\n", " ")
            did = event["did"][:20]
            print(f"[{i+1}/10] {ts.isoformat()}  did={did}...  {text!r}")

if __name__ == "__main__":
    asyncio.run(main())