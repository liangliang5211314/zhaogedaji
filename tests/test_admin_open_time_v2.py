import os
import sqlite3
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


def valid_schedule():
    return {
        "version": 2,
        "timezone": "Asia/Shanghai",
        "type": "weekly",
        "time_slots": [{"start": "08:00", "end": "12:00"}],
        "rule": {"weekdays": [6, 7]},
        "exceptions": [],
        "source_text": "每周六、周日",
        "migration_status": "native",
    }


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "admin-open-time.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def admin_headers():
    return {"X-Admin-Key": app_module.ADMIN_KEY}


def test_admin_market_editor_only_writes_valid_v2(client):
    legacy = client.post(
        "/api/admin/markets",
        json={"name": "旧结构集市", "openTime": {"type": "daily"}},
        headers=admin_headers(),
    )
    assert legacy.status_code == 400

    created = client.post(
        "/api/admin/markets",
        json={
            "name": "周末市集",
            "region": "河北省·保定市·唐县",
            "openTime": valid_schedule(),
        },
        headers=admin_headers(),
    )
    assert created.status_code == 200
    market_id = created.get_json()["market_id"]

    invalid_update = client.put(
        f"/api/admin/markets/{market_id}",
        json={"open_time": "每周末"},
        headers=admin_headers(),
    )
    assert invalid_update.status_code == 400

    conn = app_module.get_db()
    conn.execute(
        """INSERT INTO data_review_queue(
               entity_type,entity_id,issue_type,reason,source_payload,status
           ) VALUES('market',?,'open_time_v2','旧规则无法确认','{}','pending')""",
        (market_id,),
    )
    conn.commit()
    conn.close()

    fixed = valid_schedule()
    fixed["rule"] = {"weekdays": [1]}
    fixed["source_text"] = "每周一"
    response = client.put(
        f"/api/admin/markets/{market_id}",
        json={"openTime": fixed},
        headers=admin_headers(),
    )
    assert response.status_code == 200
    conn = app_module.get_db()
    queue_status = conn.execute(
        "SELECT status FROM data_review_queue WHERE entity_id=?", (market_id,)
    ).fetchone()["status"]
    conn.close()
    assert queue_status == "resolved"


def test_validate_endpoint_rejects_needs_review(client):
    value = valid_schedule()
    value["migration_status"] = "needs_review"
    response = client.post(
        "/api/admin/open-time/validate",
        json={"open_time": value},
        headers=admin_headers(),
    )
    assert response.status_code == 400
    assert "needs_review" in response.get_json()["errors"][0]
