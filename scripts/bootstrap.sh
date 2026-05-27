#!/usr/bin/env bash
# bootstrap.sh — one-command local setup for bluesky-lakehouse
# Usage: bash scripts/bootstrap.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

info()  { echo "[bootstrap] $*"; }
die()   { echo "[bootstrap] ERROR: $*" >&2; exit 1; }

# ── 1. Install Python deps ────────────────────────────────────────────────────
info "Installing Python dependencies..."
uv sync

# ── 2. Start Docker services ──────────────────────────────────────────────────
info "Starting Docker services (redpanda, risingwave, minio, polaris)..."
docker compose up -d
info "Waiting 15s for services to be healthy..."
sleep 15

# ── 3. Polaris catalog setup ──────────────────────────────────────────────────
info "Configuring Polaris catalog..."
bash scripts/setup_polaris.sh
bash scripts/fix_polaris_permissions.sh

# ── 4. Create Iceberg table ───────────────────────────────────────────────────
info "Creating Iceberg table in Polaris..."
uv run python scripts/create_iceberg_table.py

# ── 5. Load RisingWave objects ────────────────────────────────────────────────
PSQL="psql -h localhost -p 4566 -d dev -U root"
info "Loading RisingWave source, MVs, and Iceberg sink..."

# Load in dependency order: watermark source first, then downstream MVs
for f in \
    sql/05_alter_source_add_watermark.sql \
    sql/02_create_mv_posts_per_minute.sql \
    sql/03_create_mv_posts_flat.sql \
    sql/04_create_iceberg_sink.sql \
    sql/06_mv_posts_per_second.sql \
    sql/07_mv_trending_hashtags.sql \
    sql/08_mv_trending_hashtags_1h_hop.sql \
    sql/09_mv_spike_detector.sql \
    sql/10_lateness_measurement.sql; do
    info "  Applying $f..."
    $PSQL -f "$f" > /dev/null
done

# ── 6. dbt setup ──────────────────────────────────────────────────────────────
info "Running dbt deps + parse..."
(cd analytics && DBT_PROFILES_DIR=. uv run dbt deps --quiet && DBT_PROFILES_DIR=. uv run dbt parse --quiet)

# ── 7. Start ingestor ─────────────────────────────────────────────────────────
info "Starting Bluesky ingestor in background..."
uv run python -m services.ingestion.ingestor &
INGESTOR_PID=$!
echo "$INGESTOR_PID" > .ingestor.pid
info "Ingestor PID: $INGESTOR_PID (saved to .ingestor.pid)"

# ── 8. Give ingestor time to warm up, then run dbt ───────────────────────────
info "Waiting 90s for Iceberg data to land before running dbt..."
sleep 90
info "Running dbt build..."
(cd analytics && DBT_PROFILES_DIR=. uv run dbt run --quiet)

# ── 9. Start Streamlit ────────────────────────────────────────────────────────
info "Starting Streamlit dashboard on port 8501..."
uv run streamlit run dashboard/app.py --server.port 8501 &
echo $! > .streamlit.pid

info ""
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
info "  bluesky-lakehouse is running!"
info ""
info "  Dashboard:    http://localhost:8501"
info "  MinIO:        http://localhost:9001  (minio / minio123)"
info "  RisingWave:   psql -h localhost -p 4566 -d dev -U root"
info ""
info "  To start Dagster UI:"
info "    uv run dagster dev -m orchestration.definitions"
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
