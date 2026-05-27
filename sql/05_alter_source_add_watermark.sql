-- sql/05_alter_source_add_watermark.sql
-- RisingWave does not support ALTER SOURCE to add a watermark, so we drop and
-- recreate bsky_raw. CASCADE drops all downstream MVs and sinks automatically.
-- Re-run sql/02, sql/03, sql/04 after this file to restore them.

DROP SOURCE IF EXISTS bsky_raw CASCADE;

CREATE SOURCE bsky_raw (
    did       VARCHAR,
    time_us   BIGINT,
    kind      VARCHAR,

    commit STRUCT<
        rev        VARCHAR,
        operation  VARCHAR,
        collection VARCHAR,
        rkey       VARCHAR,
        record     STRUCT<
            "$type"     VARCHAR,
            "createdAt" VARCHAR,
            text        VARCHAR,
            langs       VARCHAR[]
        >,
        cid        VARCHAR
    >,

    -- Computed column: microseconds epoch → wall-clock timestamp
    event_time TIMESTAMPTZ AS to_timestamp(time_us / 1000000.0),

    -- Watermark: tells RisingWave all events with event_time < watermark have arrived.
    -- 10-second lag matches empirical p99 lateness measured in sql/10.
    WATERMARK FOR event_time AS event_time - INTERVAL '10 seconds'
) WITH (
    connector = 'kafka',
    topic = 'bsky.posts',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;
