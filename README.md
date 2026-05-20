# bluesky-lakehouse

Real-time streaming lakehouse on the Bluesky Jetstream firehose.

## Stack
Redpanda · RisingWave · MinIO · Apache Iceberg · Apache Polaris · Dagster · dbt-duckdb · Streamlit · Terraform → AWS S3/Glue/Athena

## Run locally
docker compose up -d
uv run python scripts/test_jetstream.py
uv run python scripts/test_redpanda.py

## Day-by-day notes
- Day 1: project scaffold, Redpanda up, firehose + broker smoke tests pass.