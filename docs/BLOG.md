# Exactly-Once Delivery in a Real-Time Lakehouse: What I Built and What I Learned

*A deep-dive into building a streaming lakehouse on the Bluesky firehose — with a working exactly-once experiment.*

---

## Why I built this

I wanted to work through the full stack of a modern streaming lakehouse end-to-end: not just the happy path, but failure recovery, exactly-once semantics, and how all the layers (broker, streaming SQL engine, table format, batch transforms, orchestration) fit together. The Bluesky Jetstream firehose was a perfect data source — high-volume, real-time, free, and genuinely interesting to explore.

The result is `bluesky-lakehouse`: a 9-day build log covering Redpanda → RisingWave → Apache Iceberg → dbt-DuckDB → Dagster → Streamlit.

---

## The architecture in one sentence

Bluesky posts flow from a WebSocket firehose into Redpanda, get processed by RisingWave into materialized views and an Iceberg sink, land as Parquet files in MinIO, get transformed by dbt, orchestrated by Dagster, and visualized in Streamlit.

---

## The interesting parts

### Watermarks and why they matter

RisingWave (and most streaming engines) need to know when a time window is "done" — when it's safe to emit the result and free the state. That's what a watermark is: a signal that says "I don't expect any more events with `event_time < T`."

Without a watermark, a 5-minute tumble window would have to wait forever before it could close. With one, the engine closes the window as soon as the watermark passes the window's end time.

```sql
CREATE SOURCE bsky_raw (
    ...
    event_time TIMESTAMPTZ AS to_timestamp(time_us / 1000000.0),
    WATERMARK FOR event_time AS event_time - INTERVAL '10 seconds'
) WITH (...);
```

The `- INTERVAL '10 seconds'` means the engine will tolerate up to 10 seconds of late events. Anything later than that gets dropped. This is the lateness/completeness tradeoff that every streaming system forces you to make.

### RisingWave quirks I hit

**`now()` in streaming MVs**: RisingWave forbids `now()` in the SELECT clause of a materialized view — you can only use it in WHERE/HAVING. The reason: a streaming MV is incrementally updated on each new event, not on a timer. `now()` would change every second, making the result non-deterministic with respect to the input stream. The fix: use a plain VIEW instead (which re-evaluates on every query).

**TUMBLE over subqueries**: RisingWave's window table functions require the first argument to be a named table or CTE — not an inline subquery. This tripped up the hashtag extraction query, which needed to unnest `regexp_matches` results before passing them into a TUMBLE window. The fix was a CTE:

```sql
WITH hashtag_events AS (
    SELECT event_time, unnest(regexp_matches(text, '#(\w+)', 'g')) AS hashtag
    FROM bsky_raw WHERE ...
)
SELECT window_start, hashtag, COUNT(*) AS mentions
FROM TUMBLE(hashtag_events, event_time, INTERVAL '5 minutes')
GROUP BY window_start, hashtag;
```

**`regexp_matches` return type**: In RisingWave, `unnest(regexp_matches(...))` returns `VARCHAR` directly, not `VARCHAR[]`. So `match[1]` (array indexing) throws a type error. Use `match` directly.

### The exactly-once experiment

This was the most technically interesting part. The claim: "RisingWave + Iceberg guarantees exactly-once delivery, even under failure."

To test it:
1. A marker producer emits 50,000 UUID-tagged events to Redpanda with idempotent producer settings.
2. While production is running, we inject failures: pause Polaris (catalog unavailable), restart MinIO (storage temporarily down).
3. After production finishes, we kill and restart Redpanda (broker failure).
4. Wait 90 seconds for the Iceberg sink to flush.
5. Scan the Iceberg table and join back to the ground truth UUID list.

**Result: PASS — 50,000 / 50,000 markers. Zero duplicates, zero missing.**

The key mechanisms:
- **Idempotent Kafka producer** (`enable.idempotence=True`): The broker deduplicates retried produces using producer ID + sequence number. Even if the network drops after a produce, retrying won't create a duplicate.
- **RisingWave offset binding**: RisingWave tracks which Kafka offsets it has processed. On restart, it resumes from where it left off — no re-processing.
- **Two-phase commit on the Iceberg sink**: When RisingWave commits a new Iceberg snapshot, it atomically advances its stored Kafka offset. Either both happen or neither does. This prevents the "processed but not committed" or "committed but not processed" failure modes.

**What exactly-once does NOT mean**: it doesn't mean the pipeline can survive anything. In particular:
- RisingWave in playground mode is ephemeral — all state (offsets, MVs, sinks) is in-memory. A container restart loses everything. This is a dev/demo limitation, not an architectural one; a production deployment uses persistent storage.
- `kill_redpanda` was intentionally run *after* the producer finished. Killing Redpanda while producing breaks the idempotent producer's sequence number tracking (the broker restarts with no PID memory, causing "out of order sequence number" errors). This is a Redpanda/Kafka-level limitation, not something RisingWave can fix.

### The latency measurement

I built a `event_lateness_hist` view that computes, for each event currently in `bsky_raw`, the difference between processing time and event time. The result:

- p50 latency: ~54 seconds
- p99 latency: ~100 seconds

Most of this is the Jetstream delivery delay — Bluesky's WebSocket firehose already has some buffering. The 10-second watermark adds at most 10 seconds. The rest is network + broker + RisingWave processing.

### dbt-DuckDB reading Iceberg directly

DuckDB has a native Iceberg extension that can read table files directly from S3/MinIO without going through a catalog:

```sql
SELECT *
FROM iceberg_scan('s3://lakehouse/warehouse/bsky/posts', allow_moved_paths = true)
```

The `allow_moved_paths = true` flag handles the case where files have been moved (compacted) between snapshot reads. `unsafe_enable_version_guessing = true` is needed because DuckDB can't always find the metadata version file without catalog help.

This means the dbt batch layer has no hard dependency on Polaris — it reads the files directly. The tradeoff is that it bypasses catalog-level schema evolution tracking.

### Dagster software-defined assets

The Dagster layer connects all pipeline stages as a lineage graph. The key insight: assets aren't steps in a pipeline — they're *datasets* with declared dependencies. Dagster figures out the execution order.

For the streaming layer, the assets are pure observers — they don't control RisingWave, they just check row counts:

```python
@asset(group_name="streaming")
def mv_posts_flat() -> MaterializeResult:
    n = _row_count("posts_flat")
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})
```

The dbt models are auto-generated from the dbt manifest via `@dbt_assets`:

```python
@dbt_assets(manifest=DBT_MANIFEST)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
```

Dagster reads the dbt project graph and creates one asset node per model, with dependencies inferred from `ref()` calls. The hourly schedule re-runs `dbt build` automatically.

---

## What I'd do differently in production

1. **RisingWave with persistent storage**: Mount a volume for RisingWave's meta store and state backend. Without this, any container restart loses all MVs and sink offsets.

2. **Schema registry**: Bluesky's Jetstream format evolves. A schema registry (Confluent or Redpanda SR) would let you track the `app.bsky.feed.post` record schema over time and validate producers.

3. **Iceberg catalog for dbt**: Instead of `iceberg_scan(path)`, use a proper catalog connector so dbt can discover tables by name and handle schema evolution through Polaris.

4. **Dagster sensors instead of schedules**: The hourly dbt schedule is fine for demo, but a sensor that fires when new Iceberg snapshots appear would be more efficient — don't run dbt if nothing changed.

5. **Separate the chaos scenarios**: Killing the broker while the producer is running tests a different thing than killing it after. The experiment should include both, with separate verdicts.

---

## The stack, honestly rated

| Component | Rating | Why |
|---|---|---|
| Redpanda | ⭐⭐⭐⭐⭐ | Drop-in Kafka replacement, fast, great CLI (`rpk`) |
| RisingWave | ⭐⭐⭐⭐ | Excellent streaming SQL, but playground mode's ephemerality is a footgun |
| Apache Iceberg | ⭐⭐⭐⭐⭐ | The table format is excellent; Polaris adds complexity but is the right abstraction |
| MinIO | ⭐⭐⭐⭐⭐ | S3-compatible, zero config for local dev |
| dbt-DuckDB | ⭐⭐⭐⭐ | Surprisingly powerful for local batch; DuckDB's Iceberg support is still maturing |
| Dagster | ⭐⭐⭐⭐ | The asset model is the right abstraction; startup time and module resolution are rough edges |
| Streamlit | ⭐⭐⭐⭐ | Fast to build; the rerun-loop auto-refresh works but feels fragile |

---

## Code

[github.com/your-handle/bluesky-lakehouse](https://github.com/your-handle/bluesky-lakehouse)

All 9 days, all code, all the mistakes I made along the way.
