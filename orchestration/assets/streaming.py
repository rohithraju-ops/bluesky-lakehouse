# orchestration/assets/streaming.py
"""
RisingWave materialized views as Dagster assets.
We don't (re)build them — they update continuously.
We observe row counts as health checks.
"""
import subprocess

import psycopg
from dagster import MaterializeResult, MetadataValue, asset

DSN = "host=localhost port=4566 dbname=dev user=root"


def _row_count(mv: str) -> int:
    with psycopg.connect(DSN) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {mv}").fetchone()[0]


@asset(group_name="streaming", description="Per-minute per-language post counts")
def mv_posts_per_minute_by_lang() -> MaterializeResult:
    n = _row_count("posts_per_minute_by_lang")
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(group_name="streaming", description="Flattened post stream (Iceberg source)")
def mv_posts_flat() -> MaterializeResult:
    n = _row_count("posts_flat")
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(group_name="streaming", description="5-min trending hashtags")
def mv_trending_hashtags_5m() -> MaterializeResult:
    n = _row_count("trending_hashtags_5m")
    return MaterializeResult(metadata={"row_count": MetadataValue.int(n)})


@asset(
    group_name="streaming",
    deps=[mv_posts_flat],
    description="Iceberg sink (commits every 60s)",
)
def iceberg_posts_table() -> MaterializeResult:
    result = subprocess.run(
        ["docker", "run", "--rm", "--network", "bluesky-lakehouse_default",
         "-e", "MC_HOST_local=http://minio:minio123@minio:9000",
         "minio/mc", "ls", "--recursive", "local/lakehouse/warehouse/bsky/posts/"],
        capture_output=True, text=True,
    )
    parquet_count = sum(1 for line in result.stdout.splitlines() if ".parquet" in line)
    return MaterializeResult(metadata={"parquet_file_count": MetadataValue.int(parquet_count)})


assets = [mv_posts_per_minute_by_lang, mv_posts_flat, mv_trending_hashtags_5m, iceberg_posts_table]
