from __future__ import annotations

import json
import sys

from scripts.prepare_v2_checkpoint_reseed_root import main


def _write_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_prepare_reseed_root_preserves_baseline_only_symbol(tmp_path, monkeypatch) -> None:
    root = tmp_path / "v2"
    current_universe = root / "config" / "universe.json"
    adaptive = root / "config" / "adaptive.json"
    _write_json(
        root / "config" / "runtime.json",
        {
            "hurst_universe_manifest_path": str(current_universe),
            "state_dir": str(root / "state"),
            "target_stream_root": str(root / "state" / "target_stream"),
        },
    )
    _write_json(
        current_universe,
        {"entries": [{"symbol": "BANKNIFTY", "execution_key": "NFO:BANKNIFTY26AUGFUT"}]},
    )
    _write_json(adaptive, {})

    stream_dir = root / "state" / "target_stream" / "2026-08-28"
    stream_dir.mkdir(parents=True, exist_ok=True)
    (stream_dir / "target_quotes_2026-08-28.jsonl").write_text('{"ok": true}\n' * 128, encoding="utf-8")

    baseline_state = root / "state" / "eod_baselines" / "post_eod_20260828"
    (baseline_state / "instruments" / "BANKNIFTY").mkdir(parents=True)
    (baseline_state / "instruments" / "DALBHARAT").mkdir(parents=True)
    previous_root = tmp_path / "previous_reseed"
    previous_universe = previous_root / "shard_00" / "config" / "universe.json"
    _write_json(
        previous_universe,
        {
            "entries": [
                {"symbol": "BANKNIFTY", "execution_key": "NFO:BANKNIFTY26AUGFUT"},
                {"symbol": "DALBHARAT", "execution_key": "NFO:DALBHARAT26AUGFUT"},
            ]
        },
    )
    _write_json(
        previous_root / "shard_setup.json",
        {"shards": [{"universe": str(previous_universe)}]},
    )
    _write_json(
        root / "state" / "eod_baselines" / "latest_post_eod_baseline_manifest.json",
        {"baseline_state": str(baseline_state), "source_run_root": str(previous_root)},
    )

    output_root = tmp_path / "prepared"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare_v2_checkpoint_reseed_root.py",
            "--root",
            str(root),
            "--output-root",
            str(output_root),
            "--start-date",
            "2026-08-28",
            "--end-date",
            "2026-08-28",
            "--adaptive-override",
            str(adaptive),
            "--shard-count",
            "1",
        ],
    )

    assert main() == 0
    report = json.loads((output_root / "shard_setup.json").read_text(encoding="utf-8"))
    assert report["baseline_symbols_missing_from_current_universe"] == ["DALBHARAT"]
    assert report["baseline_symbols_preserved"] == ["DALBHARAT"]
    shard_universe = json.loads((output_root / "shard_00" / "config" / "universe.json").read_text(encoding="utf-8"))
    preserved = [entry for entry in shard_universe["entries"] if entry["symbol"] == "DALBHARAT"]
    assert preserved
    assert preserved[0]["baseline_universe_reconciliation"]["mode"] == "preserve_baseline_only_symbol"
