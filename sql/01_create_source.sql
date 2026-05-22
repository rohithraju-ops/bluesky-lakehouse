-- Declares a streaming source that subscribes to the bsky.posts Redpanda topic.
-- A SOURCE in RisingWave is NOT a table — it's a logical "input port" that
-- tells the engine where events come from and how to parse them. Nothing is
-- materialized by this statement; it just sets up the subscription wiring.
-- Downstream materialized views consume from this source.

CREATE SOURCE IF NOT EXISTS bsky_raw (
    -- Top-level Jetstream event fields
    did VARCHAR,
    time_us BIGINT,
    kind VARCHAR,

    commit STRUCT<
        operation VARCHAR,
        collection VARCHAR,
        record STRUCT<
            text VARCHAR,
            langs VARCHAR[],
            "createdAt" VARCHAR
        >
    >
)
WITH (
    connector = 'kafka',
    topic = 'bsky.posts',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;