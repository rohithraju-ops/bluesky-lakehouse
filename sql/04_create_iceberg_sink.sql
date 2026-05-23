-- Continuous Iceberg sink: every row that lands in posts_flat is written
-- to an Iceberg table named bsky.posts in the lakehouse_catalog. RisingWave
-- handles snapshot commits, file rolling, and exactly-once semantics on commit.

CREATE SINK IF NOT EXISTS posts_iceberg_sink FROM posts_flat WITH (
    connector = 'iceberg',
    type = 'append-only',
    force_append_only = 'true',

    -- Catalog connection: REST protocol, pointing at Polaris
    catalog.type = 'rest',
    catalog.uri = 'http://polaris:8181/api/catalog',
    catalog.credential = 'root:s3cr3t',
    catalog.scope = 'PRINCIPAL_ROLE:ALL',

    -- Which table to write to (catalog name . namespace . table)
    warehouse.path = 'lakehouse_catalog',
    database.name = 'bsky',
    table.name = 'posts',

    -- Storage credentials (Polaris vends these; we also provide them statically)
    s3.endpoint = 'http://minio:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minio',
    s3.secret.key = 'minio123',
    s3.path.style.access = 'true',

    -- Commit cadence: emitting a snapshot every minute (default is ~5 minutes)
    commit_checkpoint_interval = 60
);