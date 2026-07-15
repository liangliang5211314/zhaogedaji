import json
import os
import sqlite3
import tempfile
from datetime import datetime

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


def schedule(kind, rule, *, status="native"):
    return json.dumps(
        {
            "version": 2,
            "timezone": "Asia/Shanghai",
            "type": kind,
            "time_slots": [{"start": "07:00", "end": "13:00"}],
            "rule": rule,
            "exceptions": [],
            "source_text": "推荐页回归数据",
            "migration_status": status,
        },
        ensure_ascii=False,
    )


def specific_date(month, day, *, status="native"):
    return schedule(
        "specific_dates",
        {
            "entries": [
                {
                    "calendar": "solar",
                    "year": 2026,
                    "month": month,
                    "day": day,
                    "duration_days": 1,
                }
            ]
        },
        status=status,
    )


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "recommendation-home.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    conn = sqlite3.connect(db_path)
    rows = [
        ("temple-near", "龙潭庙会", "庙会", specific_date(7, 14), ["当地特色"], 4.8, 18),
        ("temple-far", "秋收庙会", "庙会", specific_date(10, 2), [], 4.6, 12),
        ("temple-review", "待复核庙会", "庙会", specific_date(7, 20, status="needs_review"), [], 5.0, 99),
        ("feature", "大王店古集", "农村大集", schedule("daily", {}), ["土货特产"], 4.7, 36),
        ("ordinary", "普通大集", "农村大集", schedule("daily", {}), [], 4.9, 50),
    ]
    conn.executemany(
        """INSERT INTO markets
           (id,name,category,region,address,open_time,tags,rating,review_count,status)
           VALUES(?,?,?,'河北省·保定市·徐水区','保定市徐水区',?,?,?,?, 'published')""",
        [
            (market_id, name, category, open_time, json.dumps(tags, ensure_ascii=False), rating, reviews)
            for market_id, name, category, open_time, tags, rating, reviews in rows
        ],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(app_module, "_nearby_now", lambda: datetime(2026, 7, 13, 9, 0))
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def test_recommendations_are_scoped_and_hide_invalid_or_distant_temples(client):
    response = client.get("/api/markets/recommendations?region=徐水区")
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["scope_region"] == "徐水区"
    assert data["time_mode"] == "morning"
    assert data["today_total"] == 2
    assert [item["id"] for item in data["temple_fairs"]] == ["temple-near"]
    assert [item["id"] for item in data["temple_timeline"]] == ["temple-near"]
    assert data["temple_fairs"][0]["days_until"] == 1
    assert [item["id"] for item in data["featured_markets"]] == [
        "temple-near",
        "feature",
    ]


def test_recommendations_without_region_do_not_fall_back_to_national_counts(client):
    response = client.get("/api/markets/recommendations")
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert data["scope_region"] is None
    assert data["today_total"] == 0
    assert data["temple_fairs"] == []
    assert data["temple_timeline"] == []
    assert data["featured_markets"] == []
