#!/usr/bin/env bash
set -euo pipefail

ROOT="${OBVFUTPORT_V2_ROOT:-/opt/cloud-deploy-candidates/obv-futures-portable-v2}"
PYTHON="${PYTHON:-/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python}"
HOST="${V2MATRIX_PORTFOLIOS_HOST:-127.0.0.1}"
PORT="${V2MATRIX_PORTFOLIOS_PORT:-8099}"

export OBVFUTPORT_V2_ROOT="$ROOT"
export V2MATRIX_ROOT="${V2MATRIX_ROOT:-/opt/cloud-deploy-candidates/v2matrix}"
export PYTHONPATH="$ROOT/src"

exec "$PYTHON" -m uvicorn obvfut_portable_v2.v2matrix_portfolios:app --host "$HOST" --port "$PORT"
