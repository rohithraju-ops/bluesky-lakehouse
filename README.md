# bluesky-lakehouse

Real-time streaming lakehouse on the Bluesky Jetstream firehose.

## Stack
Redpanda · RisingWave · MinIO · Apache Iceberg · Apache Polaris · Dagster · dbt-duckdb · Streamlit · Terraform → AWS S3/Glue/Athena

## Run locally
docker compose up -d
uv run python scripts/test_jetstream.py
uv run python scripts/test_redpanda.py

## Day-by-day notes
- Project scaffold, Redpanda up, firehose + broker smoke tests pass.
- Continous Ingestion + MinIO foundation
- Risingwave + First Materialized View
- Polaris catalog + Iceberg sink and parquet files landing in MinIO