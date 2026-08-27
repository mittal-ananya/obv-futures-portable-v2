#!/usr/bin/env bash
set -euo pipefail

ROOT="${OBVFUTPORT_V2_ROOT:-/opt/cloud-deploy-candidates/obv-futures-portable-v2}"
PYTHON="${PYTHON:-/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python}"
HOST="${OBVFUTPORT_V2_DASHBOARD_HOST:-127.0.0.1}"
PORT="${OBVFUTPORT_V2_DASHBOARD_PORT:-8096}"
WORKERS="${OBVFUTPORT_V2_DASHBOARD_WORKERS:-1}"

export OBVFUTPORT_V2_ROOT="$ROOT"
export OBVFUTPORT_V2_STATE_DIR="${OBVFUTPORT_V2_STATE_DIR:-$ROOT/state}"
export OBVFUTPORT_V2_DASHBOARD_CACHE_TTL_SECONDS="${OBVFUTPORT_V2_DASHBOARD_CACHE_TTL_SECONDS:-5}"
export PYTHONPATH="$ROOT/src"

exec "$PYTHON" -m uvicorn obvfut_portable_v2.dashboard:app --host "$HOST" --port "$PORT" --workers "$WORKERS"
