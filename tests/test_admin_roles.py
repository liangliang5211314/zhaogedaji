import os
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


@pytest.fixture()
def role_client(tmp_path, monkeypatch):
    db_path = tmp_path / "admin-roles.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    conn = app_module.get_db()
    conn.execute(
        """INSERT INTO users(uid,phone,nickname,password,role,status)
           VALUES('reviewer','13800000001','审核员王宁','', 'admin','normal')"""
    )
    reviewer_id = conn.execute(
        "SELECT id FROM users WHERE uid='reviewer'"
    ).fetchone()[0]
    conn.execute(
        """INSERT INTO users(uid,phone,nickname,password,role,status)
           VALUES('owner','13800000002','站长亮','', 'superadmin','normal')"""
    )
    owner_id = conn.execute(
        "SELECT id FROM users WHERE uid='owner'"
    ).fetchone()[0]
    conn.commit()
    conn.close()
    app_module.app.config.update(TESTING=True)
    return (
        app_module.app.test_client(),
        {"Authorization": f"Bearer {app_module._make_token(reviewer_id, 'admin')}"},
        {"Authorization": f"Bearer {app_module._make_token(owner_id, 'superadmin')}"},
        reviewer_id,
    )


def test_reviewer_can_audit_but_cannot_open_system_settings(role_client):
    client, reviewer_headers, owner_headers, _ = role_client

    markets = client.get("/api/admin/markets", headers=reviewer_headers)
    assert markets.status_code == 200

    denied = client.get("/api/admin/settings/oauth", headers=reviewer_headers)
    assert denied.status_code == 403
    assert "超级管理员" in denied.get_json()["msg"]

    allowed = client.get("/api/admin/settings/oauth", headers=owner_headers)
    assert allowed.status_code == 200


def test_legacy_admin_key_is_disabled_outside_testing(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy-key-disabled.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    monkeypatch.setattr(app_module, "ALLOW_LEGACY_ADMIN_KEY", False)
    app_module.init_db()
    previous_testing = app_module.app.testing
    app_module.app.config.update(TESTING=False)
    try:
        response = app_module.app.test_client().get(
            "/api/admin/markets",
            headers={"X-Admin-Key": app_module.ADMIN_KEY},
        )
    finally:
        app_module.app.config.update(TESTING=previous_testing)
    assert response.status_code == 403


def test_reviewer_cannot_promote_accounts(role_client):
    client, reviewer_headers, _, reviewer_id = role_client
    response = client.put(
        f"/api/admin/users/{reviewer_id}",
        json={"nickname": "审核员王宁", "role": "superadmin"},
        headers=reviewer_headers,
    )
    assert response.status_code == 200
    conn = app_module.get_db()
    role = conn.execute(
        "SELECT role FROM users WHERE id=?", (reviewer_id,)
    ).fetchone()[0]
    conn.close()
    assert role == "admin"


def test_todo_summary_uses_real_queue_counts(role_client):
    client, reviewer_headers, _, _ = role_client
    conn = app_module.get_db()
    conn.execute(
        """INSERT INTO data_review_queue(
               entity_type,entity_id,issue_type,reason,status
           ) VALUES('market','m-coordinate','geocode','坐标不足','pending')"""
    )
    conn.execute(
        """INSERT INTO data_review_queue(
               entity_type,entity_id,issue_type,reason,status
           ) VALUES('market','m-time','open_time_v2','集期待复核','pending')"""
    )
    conn.commit()
    conn.close()

    data = client.get(
        "/api/admin/todo-summary", headers=reviewer_headers
    ).get_json()["data"]
    counts = {item["key"]: item["count"] for item in data["items"]}
    assert counts["coordinates"] == 1
    assert counts["open_time"] == 1
    assert data["total"] == sum(counts.values())


def test_data_review_records_operator_and_reason(role_client):
    client, reviewer_headers, _, reviewer_id = role_client
    conn = app_module.get_db()
    cursor = conn.execute(
        """INSERT INTO data_review_queue(
               entity_type,entity_id,issue_type,reason,status
           ) VALUES('market','m-review','geocode','坐标不足','pending')"""
    )
    queue_id = cursor.lastrowid
    conn.commit()
    conn.close()

    resolved = client.patch(
        f"/api/admin/data-review-queue/{queue_id}",
        json={"status": "resolved", "reason": "人工核对村委会坐标"},
        headers=reviewer_headers,
    )
    assert resolved.status_code == 200

    conn = app_module.get_db()
    row = conn.execute(
        "SELECT status,reviewed_by,review_reason FROM data_review_queue WHERE id=?",
        (queue_id,),
    ).fetchone()
    conn.close()
    assert dict(row) == {
        "status": "resolved",
        "reviewed_by": reviewer_id,
        "review_reason": "人工核对村委会坐标",
    }


def test_seller_application_review_records_operator_and_promotes_user(role_client):
    client, reviewer_headers, _, reviewer_id = role_client
    conn = app_module.get_db()
    conn.execute(
        """INSERT INTO users(uid,phone,nickname,password,role,status)
           VALUES('seller-user','13800000003','摊主张师傅','', 'user','normal')"""
    )
    user_id = conn.execute(
        "SELECT id FROM users WHERE uid='seller-user'"
    ).fetchone()[0]
    cursor = conn.execute(
        """INSERT INTO seller_applications(
               user_id,applicant_name,phone,market_id,market_name,business_category
           ) VALUES(?,?,?,?,?,?)""",
        (user_id, "张师傅", "13800000003", "m-1", "李官大集", "土货特产"),
    )
    application_id = cursor.lastrowid
    conn.commit()
    conn.close()

    pending = client.get(
        "/api/admin/seller-applications?status=pending",
        headers=reviewer_headers,
    ).get_json()["data"]
    assert pending["total"] == 1
    assert pending["list"][0]["market_name"] == "李官大集"

    missing_reason = client.patch(
        f"/api/admin/seller-applications/{application_id}",
        json={"status": "rejected"},
        headers=reviewer_headers,
    )
    assert missing_reason.status_code == 400

    approved = client.patch(
        f"/api/admin/seller-applications/{application_id}",
        json={"status": "approved", "reason": "证件与摊位信息核验通过"},
        headers=reviewer_headers,
    )
    assert approved.status_code == 200

    conn = app_module.get_db()
    application = conn.execute(
        "SELECT status,reviewed_by,review_reason FROM seller_applications WHERE id=?",
        (application_id,),
    ).fetchone()
    role = conn.execute("SELECT role FROM users WHERE id=?", (user_id,)).fetchone()[0]
    audit = conn.execute(
        "SELECT user_id,action,target FROM operation_logs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    assert dict(application) == {
        "status": "approved",
        "reviewed_by": reviewer_id,
        "review_reason": "证件与摊位信息核验通过",
    }
    assert role == "seller"
    assert dict(audit) == {
        "user_id": reviewer_id,
        "action": "review_seller_application",
        "target": f"seller_application:{application_id}",
    }
