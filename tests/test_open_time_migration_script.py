import json
import sqlite3

from tools.migrate_open_time_v2 import run_migration


def create_database(path):
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE markets (id TEXT PRIMARY KEY, name TEXT, open_time TEXT)"
    )
    rows = [
        (
            "lunar",
            "可迁移大集",
            json.dumps(
                {"type": "lunar", "days": [1, 6], "text": "逢一逢六"},
                ensure_ascii=False,
            ),
        ),
        (
            "review",
            "待核实夜市",
            json.dumps(
                {"type": "custom", "custom": "营业时间待核实"},
                ensure_ascii=False,
            ),
        ),
        (
            "native",
            "新版早市",
            json.dumps(
                {
                    "version": 2,
                    "timezone": "Asia/Shanghai",
                    "type": "daily",
                    "time_slots": [{"start": "05:00", "end": "10:00"}],
                    "rule": {},
                    "exceptions": [],
                    "source_text": "每日营业",
                    "migration_status": "native",
                },
                ensure_ascii=False,
            ),
        ),
    ]
    connection.executemany("INSERT INTO markets VALUES (?, ?, ?)", rows)
    connection.commit()
    connection.close()


def read_open_time(path, market_id):
    connection = sqlite3.connect(path)
    raw = connection.execute(
        "SELECT open_time FROM markets WHERE id = ?", (market_id,)
    ).fetchone()[0]
    connection.close()
    return json.loads(raw)


def test_dry_run_does_not_write_or_create_backup(tmp_path):
    db_path = tmp_path / "markets.db"
    report_dir = tmp_path / "dry-run"
    create_database(db_path)

    report = run_migration(db_path, report_dir=report_dir)
    assert report["mode"] == "dry_run"
    assert report["backup_path"] is None
    assert report["counts"] == {
        "total": 3,
        "native": 1,
        "migrated": 1,
        "needs_review": 1,
        "failed": 0,
    }
    assert read_open_time(db_path, "lunar")["type"] == "lunar"
    assert "version" not in read_open_time(db_path, "lunar")
    assert (report_dir / "success.csv").exists()
    assert (report_dir / "needs_review.csv").exists()
    assert (report_dir / "failed.csv").exists()


def test_apply_backs_up_then_updates_only_unambiguous_rows(tmp_path):
    db_path = tmp_path / "markets.db"
    report_dir = tmp_path / "applied"
    create_database(db_path)
    original_review = read_open_time(db_path, "review")

    report = run_migration(db_path, apply=True, report_dir=report_dir)
    backup_path = report["backup_path"]
    assert backup_path
    assert read_open_time(backup_path, "lunar").get("version") is None

    migrated = read_open_time(db_path, "lunar")
    assert migrated["version"] == 2
    assert migrated["rule"]["days"] == [1, 6, 11, 16, 21, 26]
    assert read_open_time(db_path, "review") == original_review
    assert read_open_time(db_path, "native")["migration_status"] == "native"

    review_csv = (report_dir / "needs_review.csv").read_text(encoding="utf-8-sig")
    assert "needs_review" in review_csv
    assert "review" in review_csv
    assert "营业时间待核实" in review_csv
