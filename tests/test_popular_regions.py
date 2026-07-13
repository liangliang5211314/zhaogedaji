import json
import os
import sqlite3
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "popular-regions.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    conn = sqlite3.connect(db_path)
    rows = [
        ("m1", "青州古城早市", "早市", "山东省·潍坊市·青州市", ["土货特产"]),
        ("m2", "青州花卉大集", "农村大集", "山东省·潍坊市·青州市", ["花卉", "土货特产"]),
        ("m3", "青州庙会", "庙会", "山东省·潍坊市·青州市", []),
        ("m4", "彭州周末集", "农村大集", "四川省·成都市·彭州市", ["川西土货"]),
        ("m5", "未发布", "夜市", "四川省·成都市·彭州市", ["夜市"]),
    ]
    for market_id, name, category, region, tags in rows:
        conn.execute(
            """INSERT INTO markets(id,name,category,region,tags,status)
               VALUES(?,?,?,?,?,?)""",
            (
                market_id,
                name,
                category,
                region,
                json.dumps(tags, ensure_ascii=False),
                "hidden" if market_id == "m5" else "published",
            ),
        )
    conn.commit()
    conn.close()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def test_popular_regions_aggregate_published_markets_and_tags(client):
    response = client.get("/api/regions/popular?limit=2")
    assert response.status_code == 200
    items = response.get_json()["data"]
    assert items[0]["region"] == "山东省·潍坊市·青州市"
    assert items[0]["display_name"] == "山东省 · 青州市"
    assert items[0]["market_count"] == 3
    assert items[0]["featured_tags"] == ["土货特产", "庙会", "花卉"]
    assert items[1]["market_count"] == 1


def test_popular_regions_limit_is_validated(client):
    response = client.get("/api/regions/popular?limit=not-a-number")
    assert response.status_code == 400
