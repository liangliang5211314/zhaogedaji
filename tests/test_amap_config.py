import os
import tempfile

import pytest


os.environ.setdefault("ZHAOGEDAJI_DB_PATH", str(tempfile.mkdtemp()) + "/bootstrap.db")
import app as app_module


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "amap-config.db"
    monkeypatch.setattr(app_module, "DB_PATH", str(db_path))
    app_module.init_db()
    app_module.app.config.update(TESTING=True)
    return app_module.app.test_client()


def admin_headers():
    return {"X-Admin-Key": app_module.ADMIN_KEY}


def test_public_map_config_never_exposes_web_service_key(client):
    conn = app_module.get_db()
    conn.execute(
        "INSERT OR REPLACE INTO app_settings(key,value) VALUES('amap_ws_key',?)",
        ("server-only-web-service-key",),
    )
    conn.commit()
    conn.close()

    response = client.get("/api/app-config/map")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["data"]["configured"] is False
    assert "amap_ws_key" not in response.get_data(as_text=True)
    assert "server-only-web-service-key" not in response.get_data(as_text=True)


def test_admin_can_store_js_config_without_returning_full_values(client):
    js_key = "a" * 32
    security_code = "b" * 32
    saved = client.post(
        "/api/admin/amap-js-config",
        json={"js_key": js_key, "security_code": security_code},
        headers=admin_headers(),
    )
    assert saved.status_code == 200

    admin_state = client.get(
        "/api/admin/amap-js-config",
        headers=admin_headers(),
    ).get_json()["data"]
    assert admin_state == {
        "configured": True,
        "key_tail": "aaaaaa",
        "security_tail": "bbbbbb",
    }

    public_state = client.get("/api/app-config/map").get_json()["data"]
    assert public_state == {
        "configured": True,
        "js_key": js_key,
        "security_code": security_code,
    }


def test_admin_district_lookup_requires_web_service_key(client):
    response = client.get(
        "/api/admin/districts?city=保定市",
        headers=admin_headers(),
    )
    assert response.get_json()["code"] == 400
    assert "Web 服务 Key" in response.get_json()["message"]
