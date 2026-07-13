import json
import os
import sqlite3
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


@pytest.fixture()
def calendar_client(tmp_path, monkeypatch):
    db_path = tmp_path / "calendar.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client(), db_path


def _lunar_schedule():
    return {
        "version": 2,
        "type": "lunar",
        "timezone": "Asia/Shanghai",
        "migration_status": "native",
        "rule": {
            "days": [1, 6, 11, 16, 21, 26],
            "leap_month": "exclude",
        },
        "time_slots": [{"start": "07:00", "end": "13:00"}],
        "exceptions": [],
    }


def test_market_calendar_converts_lunar_days(calendar_client):
    client, db_path = calendar_client
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO markets(id,name,status,open_time) VALUES(?,?,?,?)",
        ("calendar-lunar", "李官大集", "published", json.dumps(_lunar_schedule())),
    )
    conn.commit()
    conn.close()

    response = client.get('/api/markets/calendar-lunar/calendar?year=2026&month=7')
    assert response.status_code == 200
    days = response.get_json()['data']['open_days']
    assert [item['date'] for item in days] == [
        '2026-07-05', '2026-07-10', '2026-07-14',
        '2026-07-19', '2026-07-24', '2026-07-29',
    ]
    assert [item['lunar_label'] for item in days] == [
        '廿一', '廿六', '初一', '初六', '十一', '十六',
    ]


def test_market_calendar_rejects_needs_review(calendar_client):
    client, db_path = calendar_client
    value = _lunar_schedule()
    value['migration_status'] = 'needs_review'
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO markets(id,name,status,open_time) VALUES(?,?,?,?)",
        ("calendar-review", "待复核集市", "published", json.dumps(value)),
    )
    conn.commit()
    conn.close()

    response = client.get('/api/markets/calendar-review/calendar?year=2026&month=7')
    assert response.status_code == 422
    assert '人工复核' in response.get_json()['msg']
