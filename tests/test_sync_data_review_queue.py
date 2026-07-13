import sqlite3
from pathlib import Path

from tools.sync_data_review_queue import sync


SCHEMA = """
CREATE TABLE markets (id TEXT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE data_review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    source_payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT,
    updated_at TEXT,
    UNIQUE(entity_type, entity_id, issue_type)
);
CREATE TABLE users (id INTEGER PRIMARY KEY, phone TEXT);
"""


def make_db(path: Path, source: bool) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT INTO markets VALUES('m1','示例集市')")
    if source:
        conn.execute(
            """INSERT INTO data_review_queue
               (entity_type,entity_id,issue_type,reason,status)
               VALUES('market','m1','geocode','坐标待复核','pending')"""
        )
        conn.execute(
            """INSERT INTO data_review_queue
               (entity_type,entity_id,issue_type,reason,status)
               VALUES('market','m2','open_time_v2','已处理来源项','resolved')"""
        )
    else:
        conn.execute("INSERT INTO users VALUES(1,'13800000000')")
        conn.execute(
            """INSERT INTO data_review_queue
               (entity_type,entity_id,issue_type,reason,status)
               VALUES('market','m-existing','geocode','已人工处理','resolved')"""
        )
    conn.commit()
    conn.close()


def test_pending_queue_sync_is_insert_only_and_idempotent(tmp_path):
    source = tmp_path / "source.db"
    target = tmp_path / "target.db"
    make_db(source, True)
    make_db(target, False)

    preview = sync(source, target, tmp_path / "reports")
    assert len(preview["rows"]) == 1
    applied = sync(
        source,
        target,
        tmp_path / "reports",
        apply=True,
        backup_dir=tmp_path / "backups",
    )
    assert applied["backup"].is_file()

    conn = sqlite3.connect(target)
    assert conn.execute("SELECT * FROM users").fetchall() == [(1, "13800000000")]
    assert conn.execute(
        "SELECT reason,status FROM data_review_queue WHERE entity_id='m-existing'"
    ).fetchone() == ("已人工处理", "resolved")
    assert conn.execute(
        "SELECT issue_type,status FROM data_review_queue WHERE entity_id='m1'"
    ).fetchone() == ("geocode", "pending")
    conn.close()

    second = sync(source, target, tmp_path / "reports")
    assert second["rows"] == []
