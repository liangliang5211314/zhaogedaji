import json
import sqlite3
from pathlib import Path

from tools.sync_markets_db import PROTECTED_COLUMNS, sync


SCHEMA = """
CREATE TABLE markets (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    open_time TEXT,
    lat REAL,
    lng REAL,
    status TEXT DEFAULT 'pending',
    rating REAL DEFAULT NULL,
    review_count INTEGER DEFAULT 0,
    fav_count INTEGER DEFAULT 0,
    created_by TEXT DEFAULT '',
    created_at TEXT DEFAULT 'target-created',
    updated_at TEXT DEFAULT 'target-updated'
);
CREATE TABLE users (id INTEGER PRIMARY KEY, phone TEXT);
CREATE TABLE reviews (id INTEGER PRIMARY KEY, market_id TEXT, content TEXT);
CREATE TABLE favorites (user_id INTEGER, market_id TEXT);
"""


def make_db(path: Path, source: bool = False) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    if source:
        conn.execute(
            "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "m1", "新名称", "大集", '{"version":2}', 115.1, 38.6,
                "published", 1.0, 1, 1, "source-user", "source-created", "source-updated",
            ),
        )
        conn.execute(
            "INSERT INTO markets(id,name,category,status,rating) VALUES(?,?,?,?,?)",
            ("m2", "新增集市", "夜市", "published", 4.9),
        )
    else:
        conn.execute(
            "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "m1", "旧名称", "早市", "{}", 114.0, 37.0,
                "published", 4.8, 26, 9, "target-user", "target-created", "target-updated",
            ),
        )
        conn.execute("INSERT INTO users VALUES(1,'13800000000')")
        conn.execute("INSERT INTO reviews VALUES(1,'m1','真实评价')")
        conn.execute("INSERT INTO favorites VALUES(1,'m1')")
    conn.commit()
    conn.close()


def snapshot_user_data(path: Path):
    conn = sqlite3.connect(path)
    result = {
        "users": conn.execute("SELECT * FROM users").fetchall(),
        "reviews": conn.execute("SELECT * FROM reviews").fetchall(),
        "favorites": conn.execute("SELECT * FROM favorites").fetchall(),
    }
    conn.close()
    return result


def test_sync_is_idempotent_and_preserves_user_data(tmp_path):
    source = tmp_path / "source.db"
    target = tmp_path / "target.db"
    reports = tmp_path / "reports"
    backups = tmp_path / "backups"
    make_db(source, source=True)
    make_db(target)
    before = snapshot_user_data(target)

    preview = sync(source, target, reports)
    assert [(c.market_id, c.action) for c in preview["changes"]] == [
        ("m1", "update"),
        ("m2", "insert"),
    ]
    assert preview["backup"] is None

    applied = sync(source, target, reports, apply=True, backup_dir=backups)
    assert applied["backup"].is_file()
    assert sqlite3.connect(applied["backup"]).execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert snapshot_user_data(target) == before

    conn = sqlite3.connect(target)
    existing = conn.execute(
        "SELECT name,category,rating,review_count,fav_count,created_by,created_at FROM markets WHERE id='m1'"
    ).fetchone()
    inserted = conn.execute(
        "SELECT name,category,rating,review_count,fav_count,created_by FROM markets WHERE id='m2'"
    ).fetchone()
    conn.close()
    assert existing == ("新名称", "大集", 4.8, 26, 9, "target-user", "target-created")
    assert inserted == ("新增集市", "夜市", None, 0, 0, "")

    second = sync(source, target, reports)
    assert second["changes"] == []
    summary = json.loads(second["json"].read_text(encoding="utf-8"))
    assert summary["insert"] == 0
    assert summary["update"] == 0
    assert summary["protected_columns"] == list(PROTECTED_COLUMNS)


def test_sync_rejects_same_database(tmp_path):
    db = tmp_path / "same.db"
    make_db(db)
    try:
        sync(db, db, tmp_path / "reports")
    except ValueError as error:
        assert "不能是同一个文件" in str(error)
    else:
        raise AssertionError("同库同步必须拒绝")
