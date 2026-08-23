#!/usr/bin/env python3
"""Launch a detached symbol-atomic v2 recalibration run.

The launcher only prepares the symbol list, builds a small orchestrator script,
and starts it detached. The orchestrator builds a per-key target-stream index
first, then starts v2_symbol_atomic_recalibration_runner.py with --require-index.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = PACKAGE_ROOT / "scripts"
SRC_ROOT = PACKAGE_ROOT / "src"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import v2_canonical_joint_gate as gate  # noqa: E402


DEFAULT_PYTHON = "/opt/cloud-deploy-candidates/intraday-short-straddle-v1/.venv/bin/python"
DEFAULT_PYTHONPATH = "src:scripts:/opt/cloud-deploy-candidates/obv-futures-portable-v1/src"


def parse_csv(raw: str | None) -> list[str]:
    return [item.strip().upper() for item in str(raw or "").split(",") if item.strip()]


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def build_run_script(
    *,
    output_dir: Path,
    package_root: Path,
    python_bin: str,
    pythonpath: str,
    start_date: str,
    end_date: str,
    skip_weekends: bool,
) -> str:
    weekend_arg = "" if skip_weekends else "  --no-skip-weekends \\\n"
    return f"""#!/usr/bin/env bash
set -euo pipefail
cd {package_root}
OUT="{output_dir}"
INDEX_ROOT="$OUT/target_stream_index"
SYMBOLS=$(cat "$OUT/symbols.csv")
echo "$(date '+%Y-%m-%dT%H:%M:%S%z') stage=index_build_start"
env PYTHONPATH={pythonpath} {python_bin} scripts/v2_canonical_joint_gate.py \\
  --mode build-index \\
  --config config/runtime.json \\
  --start-date {start_date} \\
  --end-date {end_date} \\
  --output-dir "$OUT/index_build" \\
  --index-root "$INDEX_ROOT" \\
  --symbols "$SYMBOLS" \\
  --reuse-index \\
{weekend_arg}  > "$OUT/index_build.log" 2>&1
echo "$(date '+%Y-%m-%dT%H:%M:%S%z') stage=index_build_done"
echo "$(date '+%Y-%m-%dT%H:%M:%S%z') stage=atomic_run_start"
env PYTHONPATH={pythonpath} {python_bin} scripts/v2_symbol_atomic_recalibration_runner.py \\
  --config config/runtime.json \\
  --start-date {start_date} \\
  --end-date {end_date} \\
  --symbols "$SYMBOLS" \\
  --output-dir "$OUT/atomic_run" \\
  --index-root "$INDEX_ROOT" \\
  --require-index \\
{weekend_arg}  > "$OUT/atomic_run.log" 2>&1
echo "$(date '+%Y-%m-%dT%H:%M:%S%z') stage=atomic_run_done"
"""


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = Path(args.output_root)
    output_dir = output_root / f"{args.name_prefix}_{time.strftime('%Y%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=False)

    exclude = set(parse_csv(args.exclude_symbols))
    requested = parse_csv(args.symbols)
    probe_dir = output_dir / "_symbol_probe"
    runner = gate.prepare_runner(Path(args.config), probe_dir, retain_seconds=False)
    metas = gate.selected_metas(runner, requested, args.max_symbols)
    all_symbols = [meta.symbol for meta in metas]
    symbols = [symbol for symbol in all_symbols if symbol not in exclude]
    if args.expect_total is not None and len(all_symbols) != args.expect_total:
        raise SystemExit(f"Expected {args.expect_total} total symbols, got {len(all_symbols)}")
    if args.expect_run_count is not None and len(symbols) != args.expect_run_count:
        raise SystemExit(f"Expected {args.expect_run_count} run symbols, got {len(symbols)}")
    if not symbols:
        raise SystemExit("No symbols selected for run")

    symbols_path = output_dir / "symbols.csv"
    symbols_path.write_text(",".join(symbols) + "\n", encoding="utf-8")
    manifest = {
        "schema": "obvfutport_v2.symbol_atomic_recalibration_launch.v1",
        "created_at_epoch": time.time(),
        "package_root": str(PACKAGE_ROOT),
        "config": str(args.config),
        "output_dir": str(output_dir),
        "index_root": str(output_dir / "target_stream_index"),
        "atomic_output_dir": str(output_dir / "atomic_run"),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "skip_weekends": not args.no_skip_weekends,
        "all_symbol_count": len(all_symbols),
        "run_symbol_count": len(symbols),
        "excluded_symbols": sorted(exclude),
        "symbols_csv": str(symbols_path),
        "symbols": symbols,
        "pause_file": str(output_dir / "atomic_run" / "pause.request"),
        "stop_file": str(output_dir / "atomic_run" / "stop.request"),
    }
    atomic_write_json(output_dir / "orchestrator_manifest.json", manifest)

    run_script = output_dir / "run.sh"
    run_script.write_text(
        build_run_script(
            output_dir=output_dir,
            package_root=PACKAGE_ROOT,
            python_bin=args.python_bin,
            pythonpath=args.pythonpath,
            start_date=args.start_date,
            end_date=args.end_date,
            skip_weekends=not args.no_skip_weekends,
        ),
        encoding="utf-8",
    )
    os.chmod(run_script, 0o755)

    log_path = output_dir / "orchestrator.log"
    if args.no_detach:
        result = subprocess.run(["bash", str(run_script)], cwd=str(PACKAGE_ROOT), check=False)
        manifest["pid"] = None
        manifest["exit_code"] = result.returncode
    else:
        log = log_path.open("ab", buffering=0)
        proc = subprocess.Popen(
            ["setsid", "bash", str(run_script)],
            cwd=str(PACKAGE_ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )
        manifest["pid"] = proc.pid
        manifest["exit_code"] = None
    manifest["orchestrator_log"] = str(log_path)
    manifest["run_script"] = str(run_script)
    atomic_write_json(output_dir / "orchestrator_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/runtime.json")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-root", default="/tmp")
    parser.add_argument("--name-prefix", default="obvfutport_v2_symbol_atomic")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--exclude-symbols", default="")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--expect-total", type=int, default=None)
    parser.add_argument("--expect-run-count", type=int, default=None)
    parser.add_argument("--no-skip-weekends", action="store_true")
    parser.add_argument("--python-bin", default=DEFAULT_PYTHON)
    parser.add_argument("--pythonpath", default=DEFAULT_PYTHONPATH)
    parser.add_argument("--no-detach", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
