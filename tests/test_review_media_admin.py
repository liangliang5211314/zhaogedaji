import os
import sqlite3
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "reviews.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO users(id,uid,phone,nickname,status)
           VALUES(101,'U101','13800000101','旅行者','normal')"""
    )
    conn.execute(
        """INSERT INTO markets(id,name,status,rating,review_count)
           VALUES('m1','李官大集','published',NULL,0)"""
    )
    conn.commit()
    conn.close()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def user_headers():
    return {"Authorization": f"Bearer {app_module._make_token(101, 'user')}"}


def admin_headers():
    return {"X-Admin-Key": app_module.ADMIN_KEY}


def test_video_review_stays_pending_until_admin_approval(client):
    response = client.post(
        "/api/reviews",
        json={
            "market_id": "m1",
            "rating": 4.5,
            "content": "现场很有烟火气",
            "images": ["/uploads/review.jpg"],
            "videos": ["/uploads/review.mp4"],
            "tags": ["土货多"],
        },
        headers=user_headers(),
    )
    assert response.status_code == 200

    pending = client.get(
        "/api/admin/reviews?status=pending", headers=admin_headers()
    ).get_json()["data"]["list"]
    assert pending[0]["videos"] == ["/uploads/review.mp4"]
    assert pending[0]["images"] == ["/uploads/review.jpg"]

    review_id = pending[0]["id"]
    approved = client.post(
        f"/api/admin/reviews/{review_id}/approve",
        json={},
        headers=admin_headers(),
    )
    assert approved.status_code == 200

    public = client.get("/api/reviews?market_id=m1").get_json()["data"]["list"]
    assert public[0]["videos"] == ["/uploads/review.mp4"]
    conn = app_module.get_db()
    market = conn.execute(
        "SELECT rating,review_count FROM markets WHERE id='m1'"
    ).fetchone()
    conn.close()
    assert market["rating"] == 4.5
    assert market["review_count"] == 1


def test_reject_requires_reason_and_recalculates_rating(client):
    client.post(
        "/api/reviews",
        json={"market_id": "m1", "rating": 3, "content": "示例评价"},
        headers=user_headers(),
    )
    review_id = client.get(
        "/api/admin/reviews?status=pending", headers=admin_headers()
    ).get_json()["data"]["list"][0]["id"]

    missing_reason = client.post(
        f"/api/admin/reviews/{review_id}/reject",
        json={},
        headers=admin_headers(),
    )
    assert missing_reason.status_code == 400

    rejected = client.post(
        f"/api/admin/reviews/{review_id}/reject",
        json={"reason": "画面包含联系方式"},
        headers=admin_headers(),
    )
    assert rejected.status_code == 200
    conn = app_module.get_db()
    review = conn.execute(
        "SELECT status,audit_reason,audited_at FROM reviews WHERE id=?", (review_id,)
    ).fetchone()
    market = conn.execute(
        "SELECT rating,review_count FROM markets WHERE id='m1'"
    ).fetchone()
    conn.close()
    assert review["status"] == "rejected"
    assert review["audit_reason"] == "画面包含联系方式"
    assert review["audited_at"]
    assert market["rating"] is None
    assert market["review_count"] == 0
