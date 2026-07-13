import json
import os
import sqlite3
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


def schedule(kind, rule, migration_status="native"):
    return {
        "version": 2,
        "timezone": "Asia/Shanghai",
        "type": kind,
        "time_slots": [{"start": "07:00", "end": "13:00"}],
        "rule": rule,
        "exceptions": [],
        "source_text": "测试规则",
        "migration_status": migration_status,
    }


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "reminders.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO users(id,uid,phone,nickname,status)
           VALUES(201,'U201','13800000201','本地居民','normal')"""
    )
    values = [
        (
            "lunar",
            "李官大集",
            schedule("lunar", {"days": [1, 6, 11, 16, 21, 26], "leap_month": "exclude"}),
        ),
        ("daily", "枣沟头早市", schedule("daily", {})),
        ("review", "待复核集市", schedule("daily", {}, "needs_review")),
    ]
    for market_id, name, open_time in values:
        conn.execute(
            "INSERT INTO markets(id,name,status,open_time) VALUES(?,?,?,?)",
            (market_id, name, "published", json.dumps(open_time, ensure_ascii=False)),
        )
    conn.commit()
    conn.close()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def user_headers():
    return {"Authorization": f"Bearer {app_module._make_token(201, 'user')}"}


def admin_headers():
    return {"X-Admin-Key": app_module.ADMIN_KEY}


def test_daily_and_needs_review_markets_cannot_create_reminders(client):
    daily = client.post("/api/reminders/daily", json={}, headers=user_headers())
    assert daily.status_code == 400
    assert "每日营业" in daily.get_json()["msg"]

    review = client.post("/api/reminders/review", json={}, headers=user_headers())
    assert review.status_code == 422


def test_two_fixed_reminder_tasks_are_idempotent(client):
    subscribed = client.post(
        "/api/reminders/lunar",
        json={"remind_type": "once", "remind_evening": True, "remind_morning": True},
        headers=user_headers(),
    )
    assert subscribed.status_code == 200

    evening = client.post(
        "/api/admin/reminders/build-tasks",
        json={"at": "2026-07-13T20:00:00+08:00"},
        headers=admin_headers(),
    ).get_json()["data"]
    assert evening["open_date"] == "2026-07-14"
    assert evening["created"] == 1

    repeated = client.post(
        "/api/admin/reminders/build-tasks",
        json={"at": "2026-07-13T20:05:00+08:00"},
        headers=admin_headers(),
    ).get_json()["data"]
    assert repeated["created"] == 0
    assert repeated["already_exists"] == 1

    morning = client.post(
        "/api/admin/reminders/build-tasks",
        json={"at": "2026-07-14T06:30:00+08:00"},
        headers=admin_headers(),
    ).get_json()["data"]
    assert morning["created"] == 1

    tasks = client.get(
        "/api/admin/reminder-tasks?status=pending", headers=admin_headers()
    ).get_json()["data"]["list"]
    assert [(item["slot"], item["open_date"]) for item in tasks] == [
        ("evening", "2026-07-14"),
        ("morning", "2026-07-14"),
    ]
    morning_task = next(item for item in tasks if item["slot"] == "morning")
    client.post(
        f"/api/admin/reminder-tasks/{morning_task['id']}/mark-sent",
        headers=admin_headers(),
    )
    conn = app_module.get_db()
    status = conn.execute(
        "SELECT status FROM market_reminders WHERE market_id='lunar'"
    ).fetchone()["status"]
    conn.close()
    assert status == "triggered"
