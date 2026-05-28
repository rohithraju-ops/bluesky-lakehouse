#!/usr/bin/env bash
# bluesky-lake stop — kill ingestor + dashboard, leave Docker running
set -euo pipefail

GREEN='\033[0;32m'
GRAY='\033[0;90m'
CYAN='\033[0;36m'
NC='\033[0m'

ok()   { echo -e "${GREEN}  ✓${NC}  $*"; }
skip() { echo -e "${GRAY}  –${NC}  $*"; }
info() { echo -e "${CYAN}  →${NC}  $*"; }

echo ""
echo -e "${CYAN}  Bluesky Lakehouse — stopping${NC}"
echo -e "${GRAY}  ──────────────────────────────────${NC}"

# ── Ingestor ──────────────────────────────────────────────────────────────────
INGESTOR_PIDS=$(pgrep -f "services.ingestion.jetstream_to_redpanda" 2>/dev/null || true)
if [[ -n "$INGESTOR_PIDS" ]]; then
  echo "$INGESTOR_PIDS" | xargs kill 2>/dev/null || true
  ok "Ingestor stopped (PID $INGESTOR_PIDS)"
else
  skip "Ingestor not running"
fi

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASH_PIDS=$(pgrep -f "streamlit run dashboard/app.py" 2>/dev/null || true)
if [[ -n "$DASH_PIDS" ]]; then
  echo "$DASH_PIDS" | xargs kill 2>/dev/null || true
  ok "Dashboard stopped (PID $DASH_PIDS)"
else
  skip "Dashboard not running"
fi

echo ""
echo -e "${GREEN}  ──────────────────────────────────${NC}"
echo -e "${GRAY}  Docker (Redpanda, RisingWave, MinIO, Polaris) left running.${NC}"
echo -e "${GRAY}  Run  bluesky-lake       to restart.${NC}"
echo -e "${GRAY}  Run  docker compose down  to also stop Docker.${NC}"
echo -e "${GREEN}  ──────────────────────────────────${NC}"
echo ""
