#!/usr/bin/env bash
set -euo pipefail

ROOT="${OBVFUTPORT_V2_ROOT:-/opt/cloud-deploy-candidates/obv-futures-portable-v2}"
PYTHON="${PYTHON:-/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python}"
export PYTHONPATH="$ROOT/src"
exec "$PYTHON" -m obvfut_portable_v2.passive_runner run --config "$ROOT/config/runtime.json"
