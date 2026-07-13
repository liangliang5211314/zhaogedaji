import os
import sqlite3
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


@pytest.fixture()
def review_client(tmp_path, monkeypatch):
    db_path = tmp_path / "review-queue.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    connection = sqlite3.connect(db_path)
    connection.execute(
        "INSERT INTO markets(id,name,region,status) VALUES('m1','待复核大集','河北省·保定市·唐县','published')"
    )
    connection.execute(
        """INSERT INTO data_review_queue(
               entity_type,entity_id,issue_type,reason,source_payload,status
           ) VALUES('market','m1','open_time_v2','旧规则无法确认','{}','pending')"""
    )
    connection.commit()
    connection.close()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def admin_headers():
    return {"X-Admin-Key": app_module.ADMIN_KEY}


def test_review_queue_lists_pending_items_with_market_context(review_client):
    response = review_client.get(
        "/api/admin/data-review-queue?status=pending&issue_type=open_time_v2",
        headers=admin_headers(),
    )
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["data"]["total"] == 1
    assert payload["data"]["list"][0]["market_name"] == "待复核大集"
    assert payload["data"]["pending_counts"] == {"open_time_v2": 1}


def test_review_queue_status_can_be_resolved(review_client):
    listed = review_client.get(
        "/api/admin/data-review-queue", headers=admin_headers()
    ).get_json()["data"]["list"]
    queue_id = listed[0]["id"]
    response = review_client.patch(
        f"/api/admin/data-review-queue/{queue_id}",
        json={"status": "resolved"},
        headers=admin_headers(),
    )
    assert response.status_code == 200
    pending = review_client.get(
        "/api/admin/data-review-queue?status=pending", headers=admin_headers()
    ).get_json()["data"]
    assert pending["total"] == 0

