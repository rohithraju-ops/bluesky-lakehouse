#!/usr/bin/env bash
# bluesky-lake — smart startup script
# Idempotent: skips anything already running.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m'

ok()   { echo -e "${GREEN}  ✓${NC}  $*"; }
skip() { echo -e "${GRAY}  –${NC}  $*"; }
info() { echo -e "${CYAN}  →${NC}  $*"; }
warn() { echo -e "${YELLOW}  !${NC}  $*"; }

echo ""
echo -e "${CYAN}  Bluesky Lakehouse${NC}"
echo -e "${GRAY}  ──────────────────────────────────${NC}"

# ── 1. Docker containers ──────────────────────────────────────────────────────
info "Checking Docker services..."

CONTAINERS=(redpanda risingwave minio polaris)
NEED_START=false
for c in "${CONTAINERS[@]}"; do
  STATUS=$(docker inspect --format='{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
  if [[ "$STATUS" != "running" ]]; then
    NEED_START=true
    break
  fi
done

if $NEED_START; then
  info "Starting Docker containers..."
  docker compose up -d --quiet-pull 2>/dev/null || docker compose up -d
  info "Waiting for services to be healthy..."
  WAIT=0
  until docker inspect --format='{{.State.Health.Status}}' risingwave 2>/dev/null | grep -q "healthy"; do
    sleep 2; WAIT=$((WAIT+2))
    [[ $WAIT -gt 60 ]] && { warn "RisingWave health check timed out"; break; }
  done
  sleep 3
  ok "Docker services started"
else
  skip "Docker services already running"
fi

# ── 2. RisingWave objects ─────────────────────────────────────────────────────
info "Checking RisingWave objects..."

PSQL="psql -h localhost -p 4566 -d dev -U root -t -c"
SOURCE_OK=$($PSQL "SELECT COUNT(*) FROM rw_catalog.rw_sources WHERE name='bsky_raw';" 2>/dev/null | tr -d ' \n' || echo "0")
MV_COUNT=$($PSQL "SELECT COUNT(*) FROM rw_catalog.rw_materialized_views;" 2>/dev/null | tr -d ' \n' || echo "0")
SINK_OK=$($PSQL "SELECT COUNT(*) FROM rw_catalog.rw_sinks WHERE name='posts_iceberg_sink';" 2>/dev/null | tr -d ' \n' || echo "0")

if [[ "$SOURCE_OK" == "1" && "$MV_COUNT" -ge 7 && "$SINK_OK" == "1" ]]; then
  skip "RisingWave objects already loaded (source + ${MV_COUNT} MVs + sink)"
else
  info "Loading RisingWave SQL objects..."
  SQL_ORDER=(
    sql/05_alter_source_add_watermark.sql
    sql/02_create_mv_posts_per_minute.sql
    sql/03_create_mv_posts_flat.sql
    sql/04_create_iceberg_sink.sql
    sql/06_mv_posts_per_second.sql
    sql/07_mv_trending_hashtags.sql
    sql/08_mv_trending_hashtags_1h_hop.sql
    sql/09_mv_spike_detector.sql
    sql/10_lateness_measurement.sql
  )
  for f in "${SQL_ORDER[@]}"; do
    [[ -f "$f" ]] && psql -h localhost -p 4566 -d dev -U root -f "$f" -q 2>/dev/null || true
  done
  ok "RisingWave objects loaded"
fi

# ── 3. Ingestor ───────────────────────────────────────────────────────────────
info "Checking ingestor..."

if pgrep -f "services.ingestion.jetstream_to_redpanda" > /dev/null 2>&1; then
  skip "Ingestor already running (PID $(pgrep -f 'services.ingestion.ingestor' | head -1))"
else
  info "Starting ingestor..."
  LOG="$ROOT/logs/ingestor.log"
  mkdir -p "$ROOT/logs"
  nohup uv run python -m services.ingestion.jetstream_to_redpanda >> "$LOG" 2>&1 &
  echo $! > "$ROOT/.ingestor.pid"
  ok "Ingestor started  (PID $!  •  logs: logs/ingestor.log)"
fi

# ── 4. Dashboard ──────────────────────────────────────────────────────────────
info "Checking dashboard..."

PORT=8502
# Find a free port starting from 8501
for p in 8501 8502 8503 8504; do
  if ! lsof -i ":$p" -sTCP:LISTEN -t > /dev/null 2>&1; then
    PORT=$p; break
  fi
done

EXISTING_DASH=$(pgrep -f "streamlit run dashboard/app.py" | head -1 || true)
if [[ -n "$EXISTING_DASH" ]]; then
  # scan known ports to find which one the dashboard is actually listening on
  EXISTING_PORT=""
  for p in 8501 8502 8503 8504; do
    if lsof -i ":$p" -sTCP:LISTEN -t > /dev/null 2>&1; then
      EXISTING_PORT=$p; break
    fi
  done
  PORT=${EXISTING_PORT:-8501}
  skip "Dashboard already running on port ${PORT}"
else
  info "Starting dashboard on port $PORT..."
  LOG="$ROOT/logs/dashboard.log"
  nohup uv run streamlit run dashboard/app.py \
    --server.port "$PORT" \
    --server.headless true \
    >> "$LOG" 2>&1 &
  echo $! > "$ROOT/.dashboard.pid"
  sleep 3
  ok "Dashboard started  (PID $!  •  logs: logs/dashboard.log)"
fi

# ── 5. Open browser ───────────────────────────────────────────────────────────
URL="http://localhost:${PORT}"
echo ""
echo -e "${GREEN}  ──────────────────────────────────${NC}"
echo -e "${GREEN}  Bluesky Lakehouse is live!${NC}"
echo ""
echo -e "  Dashboard   ${CYAN}${URL}${NC}"
echo -e "  MinIO       ${CYAN}http://localhost:9001${NC}  ${GRAY}(minio / minio123)${NC}"
echo -e "  Redpanda    ${CYAN}http://localhost:8080${NC}"
echo -e "  RisingWave  ${GRAY}psql -h localhost -p 4566 -d dev -U root${NC}"
echo -e "${GREEN}  ──────────────────────────────────${NC}"
echo ""

open "$URL" 2>/dev/null || true
