import sqlite3

from tools import geocode_all_markets as geo


def market(**overrides):
    value = {
        "id": "wangjing",
        "name": "王京大集",
        "region": "河北省·保定市·唐县",
        "address": "河北省保定市唐县王京镇王京村",
        "category": "农村大集",
        "lat": None,
        "lng": None,
    }
    value.update(overrides)
    return value


def amap_payload(*, adcode="130627", level="村庄"):
    return {
        "status": "1",
        "geocodes": [
            {
                "formatted_address": "河北省保定市唐县王京镇王京村",
                "province": "河北省",
                "city": "保定市",
                "district": "唐县",
                "adcode": adcode,
                "level": level,
                "location": "115.0399,38.6260",
            }
        ],
    }


def test_query_priority_is_region_village_then_name_then_address():
    queries = geo.build_queries(market())
    assert queries == [
        "河北省保定市唐县王京村",
        "河北省保定市唐县王京",
        "河北省保定市唐县王京镇王京村",
    ]


def test_result_requires_precise_level_and_exact_county_adcode():
    result, reason = geo.validate_market_result(
        market(), amap_payload(), "130627"
    )
    assert reason == ""
    assert result["level"] == "村庄"
    assert result["adcode"] == "130627"
    assert result["lng"] == 115.0399
    assert result["lat"] == 38.626

    result, reason = geo.validate_market_result(
        market(), amap_payload(adcode="130606"), "130627"
    )
    assert result is None
    assert reason == "adcode不一致:130606"

    result, reason = geo.validate_market_result(
        market(), amap_payload(level="区县"), "130627"
    )
    assert result is None
    assert reason == "精度不足:区县"


def test_key_falls_back_to_database_without_logging_secret(monkeypatch):
    monkeypatch.delenv("AMAP_WS_KEY", raising=False)
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE app_settings(key TEXT PRIMARY KEY, value TEXT)")
    connection.execute(
        "INSERT INTO app_settings VALUES ('amap_ws_key', 'database-secret')"
    )
    assert geo.load_key(connection) == "database-secret"
    connection.close()


def test_geocode_row_uses_admin_adcode_and_stops_on_first_valid_query(monkeypatch):
    calls = []

    monkeypatch.setattr(
        geo,
        "admin_info",
        lambda row, key, pause, cache: {
            "lat": 38.7,
            "lng": 115.0,
            "adcode": "130627",
        },
    )

    def fake_geocode(key, query, city, pause):
        calls.append(query)
        return amap_payload()

    monkeypatch.setattr(geo, "amap_geocode", fake_geocode)
    result, query, reason, expected_adcode = geo.geocode_row(
        market(), "secret", 0, {}
    )
    assert reason == ""
    assert expected_adcode == "130627"
    assert query == "河北省保定市唐县王京村"
    assert calls == [query]
    assert result["adcode"] == "130627"
