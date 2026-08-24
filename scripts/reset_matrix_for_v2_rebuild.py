#!/usr/bin/env python3
"""Back up and reset Matrix state before a full OBVFUTPORT-v2 rebuild."""

from __future__ import annotations

import argparse
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def copy_if_exists(source: Path, target: Path) -> int:
    if not source.exists():
        return 0
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return int(source.stat().st_size)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-state-dir", default="/opt/cloud-deploy-candidates/matrix-v1/state")
    parser.add_argument("--reason", default="obvfutport_v2_selected_candidate_rebuild")
    args = parser.parse_args()

    state_dir = Path(args.matrix_state_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = state_dir / "backups" / f"pre_selected_candidate_rebuild_{stamp}"
    copied = {
        name: copy_if_exists(state_dir / name, backup_dir / name)
        for name in ("matrix_state.json", "matrix_events.jsonl", "matrix_v2_bridge_state.json")
    }
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    matrix_seed = {
        "schema": "matrix_v1.state.v1",
        "event_count": 0,
        "instruments": {},
        "rebuilt_from": args.reason,
        "updated_at_epoch": time.time(),
        "updated_at_utc": now_utc,
    }
    bridge_seed = {
        "schema": "matrix_v1.v2_bridge_state.v1",
        "entries_by_position_id": {},
        "ledger_offsets": {},
        "posted_event_ids": [],
        "reset_reason": args.reason,
        "selected_exited_position_ids": [],
        "updated_at_epoch": time.time(),
        "updated_at_utc": now_utc,
    }
    atomic_write_json(state_dir / "matrix_state.json", matrix_seed)
    (state_dir / "matrix_events.jsonl").write_text("", encoding="utf-8")
    atomic_write_json(state_dir / "matrix_v2_bridge_state.json", bridge_seed)
    report = {
        "schema": "matrix_v1.pre_selected_candidate_rebuild_backup.v1",
        "ok": True,
        "backup_dir": str(backup_dir),
        "copied": copied,
        "reason": args.reason,
        "updated_epoch": time.time(),
    }
    atomic_write_json(state_dir / f"selected_candidate_rebuild_backup_report_{stamp}.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
