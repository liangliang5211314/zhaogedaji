import json
import os
import sqlite3
import tempfile
from datetime import datetime

import pytest

os.environ["ZHAOGEDAJI_DB_PATH"] = str(tempfile.mkdtemp()) + "/bootstrap.db"
import app as app_module


def specific_schedule(day, start, end, *, status="native"):
    return json.dumps(
        {
            "version": 2,
            "timezone": "Asia/Shanghai",
            "type": "specific_dates",
            "time_slots": [{"start": start, "end": end}],
            "rule": {
                "entries": [
                    {
                        "calendar": "solar",
                        "year": 2026,
                        "month": 7,
                        "day": day,
                        "duration_days": 1,
                    }
                ]
            },
            "exceptions": [],
            "source_text": f"2026年7月{day}日",
            "migration_status": status,
        },
        ensure_ascii=False,
    )


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


@pytest.fixture()
def recommendation_client(tmp_path, monkeypatch):
    db_path = tmp_path / "recommendations.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    connection = sqlite3.connect(db_path)
    rows = [
        (
            "ended",
            "今天大集",
            "农村大集",
            38.8730,
            115.4646,
            specific_schedule(13, "07:00", "13:00"),
        ),
        (
            "tomorrow",
            "明天大集",
            "农村大集",
            38.8740,
            115.4646,
            specific_schedule(14, "07:00", "13:00"),
        ),
        (
            "night",
            "古城夜市",
            "夜市",
            38.8750,
            115.4646,
            specific_schedule(13, "18:00", "23:00"),
        ),
    ]
    connection.executemany(
        """INSERT INTO markets
           (id,name,category,lat,lng,open_time,status,region)
           VALUES(?,?,?,?,?,?,'published','河北省·保定市·竞秀区')""",
        rows,
    )
    connection.commit()
    connection.close()
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


def test_afternoon_recommends_night_then_tomorrow_and_sinks_ended(
    recommendation_client, monkeypatch
):
    monkeypatch.setattr(
        app_module, "_nearby_now", lambda: datetime(2026, 7, 13, 15, 0)
    )
    response = recommendation_client.get(
        "/api/markets/nearby?lat=38.8730&lng=115.4646&radius=10&limit=3&sort=recommended"
    )
    data = response.get_json()["data"]
    assert [item["id"] for item in data["list"]] == [
        "night",
        "tomorrow",
        "ended",
    ]
    assert [item["status_label"] for item in data["list"]] == [
        "今晚开市",
        "明天开集",
        "今日已结束",
    ]
    assert data["list"][0]["next_open_date"] == "2026-07-13"
    assert data["list"][2]["schedule_status"] == "ended_today"


def test_default_list_stays_distance_sorted_while_returning_recommendations(
    recommendation_client, monkeypatch
):
    monkeypatch.setattr(
        app_module, "_nearby_now", lambda: datetime(2026, 7, 13, 15, 0)
    )
    response = recommendation_client.get(
        "/api/markets/nearby?lat=38.8730&lng=115.4646&radius=10&limit=3"
    )
    data = response.get_json()["data"]
    assert [item["id"] for item in data["list"]] == [
        "ended",
        "tomorrow",
        "night",
    ]
    assert [item["id"] for item in data["recommended"]] == [
        "night",
        "tomorrow",
        "ended",
    ]
    assert [item["distance"] for item in data["list"]] == sorted(
        item["distance"] for item in data["list"]
    )


def test_morning_prioritizes_today_market_and_category_filter_overrides_mix(
    recommendation_client, monkeypatch
):
    monkeypatch.setattr(
        app_module, "_nearby_now", lambda: datetime(2026, 7, 13, 10, 0)
    )
    response = recommendation_client.get(
        "/api/markets/nearby?lat=38.8730&lng=115.4646&radius=10&limit=3&sort=recommended"
    )
    items = response.get_json()["data"]["list"]
    assert [item["id"] for item in items] == ["ended", "night", "tomorrow"]
    assert items[0]["is_open_now"] is True
    assert items[0]["status_label"] == "今天有集"

    filtered = recommendation_client.get(
        "/api/markets/nearby?lat=38.8730&lng=115.4646&radius=10&category=夜市"
    ).get_json()["data"]["list"]
    assert [item["id"] for item in filtered] == ["night"]
