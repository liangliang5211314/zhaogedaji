import json
import os
import sqlite3
import tempfile

import pytest

os.environ["ZHAOGEDAJI_DB_PATH"] = str(tempfile.mkdtemp()) + "/bootstrap.db"
import app as app_module


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "nearby.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    conn = sqlite3.connect(db_path)
    rows = [
        ("near", "近处大集", "农村大集", "河北省·保定市·竞秀区", 38.8730, 115.4646, "published"),
        ("far", "远处大集", "农村大集", "河北省·保定市·满城区", 38.9500, 115.3000, "published"),
        ("hidden", "未发布大集", "农村大集", "河北省·保定市·竞秀区", 38.8731, 115.4647, "pending"),
        ("missing", "无坐标大集", "农村大集", "河北省·保定市·竞秀区", None, None, "published"),
    ]
    conn.executemany(
        "INSERT INTO markets(id,name,category,region,lat,lng,status,open_time) VALUES(?,?,?,?,?,?,?,?)",
        [row + (json.dumps({"type": "lunar", "days": [1, 6]}),) for row in rows],
    )
    conn.commit()
    conn.close()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def test_nearby_orders_by_distance_and_returns_meters(client):
    response = client.get("/api/markets/nearby?lat=38.8730&lng=115.4646&radius=100&limit=10")
    payload = response.get_json()
    assert response.status_code == 200
    assert [item["id"] for item in payload["data"]["list"]] == ["near", "far"]
    assert payload["data"]["list"][0]["distance"] == 0
    assert payload["data"]["list"][1]["distance"] > 1000


def test_nearby_applies_radius_and_limit(client):
    response = client.get("/api/markets/nearby?lat=38.8730&lng=115.4646&radius=1&limit=1")
    items = response.get_json()["data"]["list"]
    assert len(items) == 1
    assert items[0]["id"] == "near"


def test_nearby_rejects_invalid_coordinates_and_excludes_unpublished(client):
    invalid = client.get("/api/markets/nearby?lat=999&lng=115.4")
    assert invalid.status_code == 400
    valid = client.get("/api/markets/nearby?lat=38.8730&lng=115.4646&radius=100")
    ids = {item["id"] for item in valid.get_json()["data"]["list"]}
    assert "hidden" not in ids
    assert "missing" not in ids
