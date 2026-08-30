from __future__ import annotations

from scripts.install_v2_reseed_state import write_access_preflight


def test_write_access_preflight_accepts_replaceable_v2_tree(tmp_path) -> None:
    prod_state = tmp_path / "state"
    instruments = prod_state / "instruments" / "BANKNIFTY"
    reports = prod_state / "reports"
    backups = prod_state / "backups"
    instruments.mkdir(parents=True)
    reports.mkdir(parents=True)
    backups.mkdir(parents=True)
    (instruments / "model_state.json").write_text("{}", encoding="utf-8")
    (prod_state / "status.json").write_text("{}", encoding="utf-8")

    report = write_access_preflight(
        [
            prod_state / "instruments",
            prod_state / "status.json",
            reports,
            backups,
        ]
    )

    assert report["ok"] is True
    assert report["issue_count"] == 0
    assert report["checked"] >= 4
