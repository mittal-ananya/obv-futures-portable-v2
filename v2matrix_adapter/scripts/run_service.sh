#!/usr/bin/env bash
set -euo pipefail

PACKAGE_ROOT="${PACKAGE_ROOT:-/opt/cloud-deploy-candidates/v2matrix}"
PYTHON="${PYTHON:-/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python}"
HOST="${MATRIX_HOST:-127.0.0.1}"
PORT="${MATRIX_PORT:-8098}"

export PACKAGE_ROOT
export MATRIX_PORTFOLIO_STATE_ROOT="${MATRIX_PORTFOLIO_STATE_ROOT:-/opt/cloud-deploy-candidates/obv-futures-portable-v2/state}"
export PYTHONPATH="$PACKAGE_ROOT/src"

exec "$PYTHON" -m uvicorn v2matrix.app:app --host "$HOST" --port "$PORT"
