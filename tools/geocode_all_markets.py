#!/usr/bin/env python3
"""Precision-geocode markets with AMap Web Service and produce an audit report."""

import argparse
import csv
import json
import math
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "zhaojishi.db"
DEFAULT_REPORT = ROOT / "outputs" / "geocode_report.csv"
DEFAULT_CENTER_CACHE = ROOT / "outputs" / "geocode_admin_centers.json"

PRECISE_LEVELS = {"村庄", "兴趣点", "门牌号", "小区", "道路", "街道", "乡镇"}
MARKET_SUFFIXES = (
    "农村大集", "集贸市场", "农贸市场", "批发市场", "综合市场",
    "大集", "集市", "庙会", "早市", "夜市", "赶会", "赶集", "市场",
)
PLACE_ENDINGS = ("村", "庄", "镇", "乡", "街道", "社区", "小区", "路", "街")
REPORT_FIELDS = (
    "id", "name", "清洗后名称", "region", "查询词", "数据来源",
    "期望adcode", "命中adcode", "命中level", "旧坐标", "新坐标",
    "偏移距离", "状态", "原因",
)

# 仅收录可核对的一一对应行政区更名。发生拆分/合并且无法唯一落区的旧区县不在此表。
ADMIN_DISTRICT_ALIASES = {
    "清苑县": ("清苑区", "130608"),
    "滦县": ("滦州市", "130284"),
    "肥乡县": ("肥乡区", "130407"),
    "宽城县": ("宽城满族自治县", "130827"),
    "围场县": ("围场满族蒙古族自治县", "130828"),
    "南和县": ("南和区", "130506"),
    "万全县": ("万全区", "130708"),
    "南市区": ("莲池区", "130606"),
}

# 定州、辛集为省直辖县级市；旧数据可能仍带保定/石家庄父级，校验以县级 adcode 为准。
DIRECT_COUNTY_CITIES = {"定州市": "130682", "辛集市": "130181"}

OCR_CONTAMINATION_PATTERNS = (
    r"\d", r"是农历", r"是阳历", r"是大集", r"天庙会", r"天大集",
    r"[：:]", r"[》《<>]", r"0g", r"1[士土]", r"大集.*大集",
)


def parse_args():
    parser = argparse.ArgumentParser(description="全量精准化集市坐标")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    parser.add_argument(
        "--key",
        default="",
        help="默认依次读取 AMAP_WS_KEY、app_settings.amap_ws_key",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--suspicious-km", type=float, default=30.0)
    parser.add_argument("--retry-success", action="store_true")
    parser.add_argument("--resume", action="store_true", help="跳过本报告中已完成补救轮的记录")
    parser.add_argument("--address-rescue", action="store_true", help="仅用地址中提取的新地名补救失败记录")
    return parser.parse_args()


def clean_text(value):
    value = re.sub(r"[（(][^）)]*[）)]", "", str(value or ""))
    return re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", value)


def analyze_market_name(value):
    """Return a conservative cleaned name and an OCR review reason when unsafe."""
    raw = str(value or "").strip()
    # Arabic digits and punctuation at the very beginning are common OCR list markers.
    cleaned = re.sub(r"^[一二三四五六七八九十]+[、。，.]+", "", raw).strip()
    cleaned = re.sub(r"^[\s\d\W_]+", "", cleaned, flags=re.UNICODE).strip()
    if not cleaned:
        return raw, "OCR脏数据:名称清洗后为空"
    if raw.count("（") != raw.count("）") or raw.count("(") != raw.count(")"):
        return cleaned, "OCR脏数据:名称括号残缺"
    if re.search(r"[、。，,.！!？?—\-]", cleaned):
        return cleaned, "OCR脏数据:名称含未清理分隔符"
    if re.search(r"([\u4e00-\u9fff]{2,4})\1", cleaned):
        return cleaned, "OCR脏数据:名称含重复识别片段"
    if any(re.search(pattern, cleaned, re.IGNORECASE) for pattern in OCR_CONTAMINATION_PATTERNS):
        return cleaned, "OCR脏数据:名称含内嵌数字或说明文字"
    if len(clean_text(cleaned)) > 24:
        return cleaned, "OCR脏数据:名称过长疑似混入说明"
    if not any(token in cleaned for token in MARKET_SUFFIXES + ("会",)):
        # 普通地名本身可以没有后缀，因此不据此拒绝；只标记明显残缺的单字。
        if len(clean_text(cleaned)) < 2:
            return cleaned, "OCR脏数据:名称信息不足"
    if len(strip_market_suffix(cleaned)) < 2:
        return cleaned, "OCR脏数据:清洗后缺少可定位地名"
    return cleaned, ""


def strip_market_suffix(value):
    value = clean_text(value)
    changed = True
    while changed:
        changed = False
        for suffix in MARKET_SUFFIXES:
            if value.endswith(suffix):
                value = value[:-len(suffix)]
                changed = True
                break
    return value


def region_parts(region):
    return [part.strip() for part in re.split(r"[·\s]+", region or "") if part.strip()]


def region_text(row):
    return "".join(region_parts(row["region"]))


def expected_admin(row):
    parts = region_parts(row["region"])
    province = parts[0] if parts else ""
    city = parts[1] if len(parts) > 1 else ""
    district = parts[2] if len(parts) > 2 else ""
    address = clean_text(row["address"])
    if not city:
        match = re.search(r"([^省]+市)", address)
        city = match.group(1) if match else ""
    if not district:
        tail = address[address.find(city) + len(city):] if city and city in address else address
        match = re.search(r"(.{2,8}?(?:县|区|旗|市))", tail)
        district = match.group(1) if match else ""
    return province, city, district


def place_name(row):
    name = strip_market_suffix(row["name"])
    if name:
        return name
    address = clean_text(row["address"])
    for admin in expected_admin(row):
        address = address.replace(clean_text(admin), "")
    return strip_market_suffix(address)


def place_tokens(row):
    raw = place_name(row)
    candidates = []

    def add(value):
        value = clean_text(value)
        if len(value) >= 2 and value not in candidates:
            candidates.append(value)

    repeated = re.fullmatch(r"(.{2,4})\1(镇|乡|村|庄|街道)", raw)
    if repeated:
        add(repeated.group(1) + repeated.group(2))
        add(repeated.group(1))

    village = re.match(r"^(.{2,8}?(?:村|庄))", raw)
    if village and village.group(1) != raw:
        add(village.group(1))

    if raw.endswith(("镇", "乡")):
        for admin_length in (3, 4, 5):
            if len(raw) - admin_length >= 2:
                add(raw[:-admin_length])

    add(raw)
    for token in address_place_tokens(row):
        add(token)
    return candidates


def address_place_tokens(row):
    address_tail = clean_text(row.get("address") or "")
    for admin in expected_admin(row):
        address_tail = address_tail.replace(clean_text(admin), "")
    values = []

    def add_address(value):
        value = strip_market_suffix(value)
        if len(value) >= 2 and value not in values:
            values.append(value)

    address_place = re.search(r"(?:镇|乡|街道)([\u4e00-\u9fff]{2,12})$", address_tail)
    if address_place:
        add_address(address_place.group(1))
    address_village = re.search(r"([\u4e00-\u9fff]{2,10}(?:村|庄))$", address_tail)
    if address_village:
        add_address(address_village.group(1))
    return values


def add_village_suffix(name):
    if not name or name.endswith(PLACE_ENDINGS):
        return name
    return name + "村"


def build_queries(row):
    region = region_text(row)
    tokens = place_tokens(row)
    name = tokens[0] if tokens else place_name(row)
    queries = [region + add_village_suffix(name), region + name, row["address"]]
    for token in tokens[1:4]:
        queries.extend((region + add_village_suffix(token), region + token))
    result = []
    seen = set()
    for query in queries:
        query = clean_text(query)
        if query and query not in seen:
            seen.add(query)
            result.append(query)
    return result


def load_key(conn, explicit=""):
    key = (explicit or os.environ.get("AMAP_WS_KEY", "")).strip()
    if key:
        return key
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='amap_ws_key'"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    return str(row[0]).strip() if row and row[0] else ""


def haversine_m(lat1, lng1, lat2, lng2):
    radius = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def amap_geocode(key, query, city, pause):
    params = urllib.parse.urlencode({
        "key": key, "address": query, "city": city, "output": "json",
    })
    request = urllib.request.Request(
        "https://restapi.amap.com/v3/geocode/geo?" + params,
        headers={"User-Agent": "zhaogedaji-geocode/3.0"},
    )
    payload = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            time.sleep(max(0.0, pause))
        if payload.get("infocode") != "10021":
            return payload
        time.sleep(max(0.8, pause * 4) * (attempt + 1))
    return payload


def amap_place_search(key, keywords, city, pause):
    params = urllib.parse.urlencode({
        "key": key,
        "keywords": keywords,
        "city": city,
        "citylimit": "true",
        "offset": 20,
        "page": 1,
        "extensions": "base",
        "output": "json",
    })
    request = urllib.request.Request(
        "https://restapi.amap.com/v3/place/text?" + params,
        headers={"User-Agent": "zhaogedaji-geocode/4.0"},
    )
    payload = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            time.sleep(max(0.0, pause))
        if payload.get("infocode") != "10021":
            return payload
        time.sleep(max(0.8, pause * 4) * (attempt + 1))
    return payload


def amap_reverse_geocode(key, lat, lng, pause):
    params = urllib.parse.urlencode({
        "key": key,
        "location": f"{lng},{lat}",
        "extensions": "base",
        "output": "json",
    })
    request = urllib.request.Request(
        "https://restapi.amap.com/v3/geocode/regeo?" + params,
        headers={"User-Agent": "zhaogedaji-geocode/4.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            return json.loads(response.read().decode("utf-8"))
    finally:
        time.sleep(max(0.0, pause))


def build_place_queries(row):
    queries = []
    district = canonical_district(expected_admin(row)[2])
    tokens = address_place_tokens(row) if row.get("_address_only") else place_tokens(row)
    if row.get("_address_only"):
        existing = {strip_market_suffix(token) for token in place_tokens({**row, "address": ""})}
        tokens = [token for token in tokens if strip_market_suffix(token) not in existing]
    for token in tokens[:4]:
        base = strip_market_suffix(token)
        if not base:
            continue
        village = base if base.endswith("村") else base + "村"
        local = (village, base + "大集", base + "市场")
        if district:
            queries.extend(district + value for value in local)
        else:
            queries.extend(local)
    result = []
    seen = set()
    for query in queries:
        query = clean_text(query)
        if query and query not in seen:
            seen.add(query)
            result.append(query)
    return result


def place_result_level(poi):
    text = clean_text("".join((str(poi.get("name") or ""), str(poi.get("type") or ""))))
    if any(token in text for token in ("村民委员会", "村委会", "村庄")):
        return "村庄"
    if any(token in text for token in ("乡镇", "镇政府", "乡政府")):
        return "乡镇"
    if any(token in text for token in ("市场", "大集", "集贸", "农贸", "庙会")):
        return "兴趣点"
    return "兴趣点"


def validate_place_result(row, payload, expected_adcode):
    if payload.get("status") != "1":
        return None, f"地点搜索错误:{payload.get('info')}:{payload.get('infocode')}"
    _, _, district = expected_admin(row)
    expected_district = canonical_district(district)
    tokens = place_tokens(row)
    rejected = []
    for poi in payload.get("pois") or []:
        actual_adcode = str(poi.get("adcode") or "")
        actual_district = normalize_admin(poi.get("adname"))
        if expected_adcode and actual_adcode and actual_adcode != expected_adcode:
            rejected.append(f"地点搜索adcode不一致:{actual_adcode or '空'}")
            continue
        if expected_district and actual_district and normalize_admin(expected_district) != actual_district:
            rejected.append(f"地点搜索区县不一致:{poi.get('adname')}")
            continue
        text = clean_text("".join((
            str(poi.get("name") or ""), str(poi.get("address") or ""),
            str(poi.get("pname") or ""), str(poi.get("cityname") or ""),
            str(poi.get("adname") or ""),
        )))
        if tokens and not any(strip_market_suffix(token) in text for token in tokens):
            rejected.append("地点搜索结果缺少集市地名")
            continue
        coords = parse_location(poi)
        if not coords:
            rejected.append("地点搜索坐标格式无效")
            continue
        return {
            "lat": coords[0],
            "lng": coords[1],
            "level": place_result_level(poi),
            "adcode": actual_adcode or expected_adcode,
            "formatted_address": clean_text("".join((
                str(poi.get("pname") or ""), str(poi.get("cityname") or ""),
                str(poi.get("adname") or ""), str(poi.get("address") or ""),
                str(poi.get("name") or ""),
            ))),
            "source": "place_text",
        }, ""
    return None, rejected[0] if rejected else "地点搜索未找到结果"


def parse_location(geo):
    try:
        lng, lat = (geo.get("location") or "").split(",", 1)
        lat, lng = float(lat), float(lng)
    except (TypeError, ValueError):
        return None
    if 18 <= lat <= 54 and 73 <= lng <= 135:
        return lat, lng
    return None


def normalize_admin(value):
    return clean_text(value).replace("自治县", "县").replace("自治旗", "旗")


def canonical_district(value):
    name = str(value or "")
    return ADMIN_DISTRICT_ALIASES.get(name, (name, ""))[0]


def district_adcode_override(value):
    name = str(value or "")
    return ADMIN_DISTRICT_ALIASES.get(name, (name, ""))[1] or DIRECT_COUNTY_CITIES.get(name, "")


def validate_market_result(row, payload, expected_adcode):
    if payload.get("status") != "1":
        return None, f"高德错误:{payload.get('info')}:{payload.get('infocode')}"
    province, city, district = expected_admin(row)
    rejected = []
    for geo in payload.get("geocodes") or []:
        level = geo.get("level") or ""
        if level not in PRECISE_LEVELS:
            rejected.append(f"精度不足:{level or '未知'}")
            continue
        actual_province = normalize_admin(geo.get("province"))
        actual_city = normalize_admin(geo.get("city"))
        actual_district = normalize_admin(geo.get("district"))
        actual_adcode = str(geo.get("adcode") or "")
        if province and actual_province and normalize_admin(province) != actual_province:
            rejected.append(f"省不一致:{geo.get('province')}")
            continue
        expected_district = canonical_district(district)
        is_direct_city = expected_district in DIRECT_COUNTY_CITIES
        if city and actual_city and normalize_admin(city) != actual_city and not is_direct_city:
            rejected.append(f"市不一致:{geo.get('city')}")
            continue
        if expected_district and actual_district and normalize_admin(expected_district) != actual_district:
            rejected.append(f"区县不一致:{geo.get('district')}")
            continue
        if not expected_adcode or actual_adcode != expected_adcode:
            rejected.append(f"adcode不一致:{actual_adcode or '空'}")
            continue
        tokens = place_tokens(row)
        formatted = clean_text(geo.get("formatted_address"))
        if tokens and not any(token in formatted for token in tokens):
            rejected.append("命中地址缺少集市地名")
            continue
        coords = parse_location(geo)
        if not coords:
            rejected.append("坐标格式无效")
            continue
        return {
            "lat": coords[0], "lng": coords[1], "level": level,
            "adcode": actual_adcode,
            "formatted_address": geo.get("formatted_address") or "",
            "source": "geocode",
        }, ""
    return None, rejected[0] if rejected else "未找到结果"


def load_json(path, default):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def load_report(path):
    try:
        with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
            return {row["id"]: row for row in csv.DictReader(handle) if row.get("id")}
    except OSError:
        return {}


def write_report(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows.values(), key=lambda row: (row["状态"], row["name"], row["id"])))


def report_is_applied(row, report_row):
    if not report_row or report_row.get("状态") != "成功":
        return False
    try:
        lng, lat = report_row["新坐标"].split(",", 1)
        return abs(float(row["lat"]) - float(lat)) < 1e-7 and abs(float(row["lng"]) - float(lng)) < 1e-7
    except (TypeError, ValueError):
        return False


def backup_database(db_path):
    backup_dir = Path(db_path).parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / (
        f"zhaojishi_before_geocode_{datetime.now():%Y%m%d_%H%M%S_%f}.db"
    )
    source_conn = sqlite3.connect(db_path)
    backup_conn = sqlite3.connect(target)
    try:
        source_conn.backup(backup_conn)
    finally:
        backup_conn.close()
        source_conn.close()
    return target


def admin_info(row, key, pause, cache):
    province, city, district = expected_admin(row)
    lookup_district = canonical_district(district)
    cache_key = "|".join((province, city, lookup_district))
    if cache_key in cache and cache[cache_key] and cache[cache_key].get("adcode"):
        return cache[cache_key]
    override_adcode = district_adcode_override(district)
    query_city = "" if lookup_district in DIRECT_COUNTY_CITIES else city
    query = "".join(part for part in (province, query_city, lookup_district) if part)
    if not lookup_district or not query:
        return None
    try:
        payload = amap_geocode(key, query, city, pause)
    except Exception:
        return None
    info = None
    if payload.get("status") == "1":
        for geo in payload.get("geocodes") or []:
            coords = parse_location(geo)
            if (
                coords
                and geo.get("adcode")
                and normalize_admin(geo.get("district")) == normalize_admin(lookup_district)
                and (not override_adcode or str(geo.get("adcode")) == override_adcode)
            ):
                info = {
                    "lat": coords[0],
                    "lng": coords[1],
                    "adcode": str(geo["adcode"]),
                }
                break
    if info:
        cache[cache_key] = info
    return info


def candidate_rows(conn, args, key, reports):
    rows = [dict(row) for row in conn.execute("SELECT * FROM markets ORDER BY region,name")]
    missing = [
        row for row in rows
        if (not row["lat"] or not row["lng"])
        and not (
            args.resume
            and (reports.get(str(row["id"])) or {}).get("清洗后名称")
        )
    ]
    center_cache = load_json(DEFAULT_CENTER_CACHE, {})
    if args.limit and len(missing) >= args.limit:
        return missing[:args.limit], center_cache

    suspicious = []
    for row in rows:
        if not row["lat"] or not row["lng"]:
            continue
        if not args.retry_success and report_is_applied(row, reports.get(row["id"])):
            continue
        center = admin_info(row, key, args.sleep, center_cache)
        if not center:
            continue
        distance = haversine_m(float(row["lat"]), float(row["lng"]), center["lat"], center["lng"])
        if distance > args.suspicious_km * 1000:
            suspicious.append(row)
            if args.limit and len(missing) + len(suspicious) >= args.limit:
                break
    write_json(DEFAULT_CENTER_CACHE, center_cache)
    candidates = missing + suspicious
    return candidates[:args.limit] if args.limit else candidates, center_cache


def geocode_row(row, key, pause, admin_cache):
    expected = admin_info(row, key, pause, admin_cache)
    if not expected:
        return None, "", "无法确认区县adcode", ""
    last_reason = "未尝试"
    last_level = ""
    last_query = ""
    saw_result = False
    if not row.get("_previous_reason"):
        for query in build_queries(row):
            last_query = query
            try:
                payload = amap_geocode(key, query, expected_admin(row)[1], pause)
            except Exception as exc:
                last_reason = f"请求失败:{exc}"
                continue
            geos = payload.get("geocodes") or []
            if geos:
                saw_result = True
                last_level = geos[0].get("level") or ""
            result, last_reason = validate_market_result(row, payload, expected["adcode"])
            if result:
                return result, query, "", expected["adcode"]
    # 地理编码只能落到区县时，再以区县 adcode 限定地点搜索。
    for query in build_place_queries(row):
        last_query = query
        try:
            payload = amap_place_search(key, query, expected["adcode"], pause)
        except Exception as exc:
            last_reason = f"地点搜索请求失败:{exc}"
            continue
        result, last_reason = validate_place_result(row, payload, expected["adcode"])
        if result:
            try:
                reverse = amap_reverse_geocode(key, result["lat"], result["lng"], pause)
                component = (reverse.get("regeocode") or {}).get("addressComponent") or {}
                reverse_adcode = str(component.get("adcode") or "")
            except Exception as exc:
                last_reason = f"地点搜索逆地理校验失败:{exc}"
                continue
            if reverse_adcode != expected["adcode"]:
                last_reason = f"地点搜索逆地理adcode不一致:{reverse_adcode or '空'}"
                continue
            result["adcode"] = reverse_adcode
            return result, query, "", expected["adcode"]
    return (
        None,
        last_query,
        last_reason if saw_result else last_reason or "未找到结果",
        expected["adcode"],
    )


def format_coords(lat, lng):
    if not lat or not lng:
        return ""
    return f"{float(lng):.6f},{float(lat):.6f}"


def ensure_review_queue(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS data_review_queue (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type    TEXT NOT NULL,
            entity_id      TEXT NOT NULL,
            issue_type     TEXT NOT NULL,
            reason         TEXT NOT NULL DEFAULT '',
            source_payload TEXT NOT NULL DEFAULT '{}',
            status         TEXT NOT NULL DEFAULT 'pending',
            created_at     TEXT DEFAULT (datetime('now','localtime')),
            updated_at     TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(entity_type, entity_id, issue_type)
        );
        CREATE INDEX IF NOT EXISTS idx_data_review_queue_status
        ON data_review_queue(status, issue_type);
    """)


def enqueue_review(conn, entity_id, issue_type, reason, payload):
    conn.execute(
        """INSERT INTO data_review_queue(
               entity_type,entity_id,issue_type,reason,source_payload,status
           ) VALUES('market',?,?,?,?, 'pending')
           ON CONFLICT(entity_type,entity_id,issue_type) DO UPDATE SET
               reason=excluded.reason,
               source_payload=excluded.source_payload,
               status=CASE WHEN data_review_queue.status='resolved' THEN 'resolved' ELSE 'pending' END,
               updated_at=datetime('now','localtime')""",
        (str(entity_id), issue_type, reason, json.dumps(payload, ensure_ascii=False)),
    )


def enqueue_open_time_reviews(conn, report_path=None):
    path = Path(report_path or ROOT / "output" / "open_time_v2_migration" / "needs_review.csv")
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            entity_id = row.get("id")
            if not entity_id:
                continue
            enqueue_review(
                conn,
                entity_id,
                "open_time_v2",
                row.get("reason") or "open_time v2 迁移需人工复核",
                {
                    "name": row.get("name") or "",
                    "source_text": row.get("source_text") or "",
                    "open_time": row.get("open_time") or "",
                },
            )
            count += 1
    return count


def main():
    args = parse_args()
    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row
    key = load_key(conn, args.key)
    if not key:
        conn.close()
        raise SystemExit(
            "缺少高德 Web 服务 Key，请设置 app_settings.amap_ws_key 或 AMAP_WS_KEY"
        )
    reports = load_report(args.report)
    rows, admin_cache = candidate_rows(conn, args, key, reports)
    backup = None if args.dry_run else backup_database(args.db)
    if not args.dry_run:
        ensure_review_queue(conn)
        open_time_reviews = enqueue_open_time_reviews(conn)
    else:
        open_time_reviews = 0
    succeeded = failed = doubtful = dirty = cleaned_names = skipped_address = 0
    levels = Counter()

    for index, row in enumerate(rows, 1):
        original_name = row["name"]
        cleaned_name, ocr_reason = analyze_market_name(original_name)
        working_row = dict(row)
        working_row["name"] = cleaned_name
        working_row["_previous_reason"] = (reports.get(str(row["id"])) or {}).get("原因", "")
        working_row["_address_only"] = args.address_rescue
        if args.address_rescue and (ocr_reason or not build_place_queries(working_row)):
            skipped_address += 1
            continue
        if not ocr_reason and cleaned_name != original_name:
            cleaned_names += 1
            if not args.dry_run:
                conn.execute(
                    "UPDATE markets SET name=?,updated_at=datetime('now','localtime') WHERE id=?",
                    (cleaned_name, row["id"]),
                )
        if ocr_reason:
            result, query, reason, expected_adcode = None, "", ocr_reason, ""
        else:
            result, query, reason, expected_adcode = geocode_row(
                working_row, key, args.sleep, admin_cache
            )
        old_coords = format_coords(row["lat"], row["lng"])
        if result:
            offset = ""
            if row["lat"] and row["lng"]:
                offset = f"{haversine_m(float(row['lat']), float(row['lng']), result['lat'], result['lng']):.0f}m"
            if not args.dry_run:
                conn.execute(
                    "UPDATE markets SET name=?,lat=?,lng=?,updated_at=datetime('now','localtime') WHERE id=?",
                    (cleaned_name, result["lat"], result["lng"], row["id"]),
                )
                conn.execute(
                    """UPDATE data_review_queue SET status='resolved',updated_at=datetime('now','localtime')
                       WHERE entity_type='market' AND entity_id=?
                       AND issue_type IN ('geocode','market_name_ocr')""",
                    (str(row["id"]),),
                )
            reports[row["id"]] = {
                "id": row["id"], "name": original_name, "清洗后名称": cleaned_name,
                "region": row["region"], "查询词": query,
                "数据来源": result.get("source", "geocode"), "期望adcode": expected_adcode,
                "命中adcode": result["adcode"],
                "命中level": result["level"], "旧坐标": old_coords,
                "新坐标": format_coords(result["lat"], result["lng"]),
                "偏移距离": offset, "状态": "成功",
                "原因": ("dry-run；" if args.dry_run else "") + result["formatted_address"],
            }
            succeeded += 1
            levels[result["level"]] += 1
        else:
            is_dirty = reason.startswith("OCR脏数据:")
            status = "需复核" if is_dirty else ("存疑" if old_coords else "失败")
            reports[row["id"]] = {
                "id": row["id"], "name": original_name, "清洗后名称": cleaned_name,
                "region": row["region"], "查询词": query, "数据来源": "",
                "期望adcode": expected_adcode,
                "命中adcode": "", "命中level": "",
                "旧坐标": old_coords, "新坐标": "",
                "偏移距离": "", "状态": status, "原因": reason,
            }
            if not args.dry_run:
                issue_type = "market_name_ocr" if is_dirty else "geocode"
                enqueue_review(conn, row["id"], issue_type, reason, {
                    "name": original_name,
                    "cleaned_name": cleaned_name,
                    "region": row["region"],
                    "address": row["address"],
                    "query": query,
                    "expected_adcode": expected_adcode,
                })
            dirty += is_dirty
            doubtful += status == "存疑"
            failed += status == "失败"
        if index % 20 == 0:
            if not args.dry_run:
                conn.commit()
            write_json(DEFAULT_CENTER_CACHE, admin_cache)
            write_report(args.report, reports)
            print(json.dumps({"完成": index, "成功": succeeded, "失败": failed, "需复核脏数据": dirty, "存疑": doubtful}, ensure_ascii=False), flush=True)

    if not args.dry_run:
        conn.commit()
    write_json(DEFAULT_CENTER_CACHE, admin_cache)
    write_report(args.report, reports)
    review_report = Path(args.report).with_name(Path(args.report).stem + "_needs_review.csv")
    write_report(
        review_report,
        {key: value for key, value in reports.items() if value.get("状态") != "成功"},
    )
    remaining = conn.execute(
        "SELECT COUNT(*) FROM markets WHERE lat IS NULL OR lng IS NULL OR lat=0 OR lng=0"
    ).fetchone()[0]
    total_market_count = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    conn.close()
    total = len(rows)
    all_statuses = Counter(row.get("状态") for row in reports.values())
    all_sources = Counter((row.get("数据来源") or "geocode_v1") for row in reports.values() if row.get("状态") == "成功")
    all_levels = Counter(row.get("命中level") for row in reports.values() if row.get("状态") == "成功")
    true_denominator = sum(all_statuses.values()) - all_statuses.get("需复核", 0)
    true_rate = all_statuses.get("成功", 0) / true_denominator * 100 if true_denominator else 0
    database_denominator = total_market_count - all_statuses.get("需复核", 0)
    database_coverage = (total_market_count - remaining) / database_denominator * 100 if database_denominator else 0
    print(json.dumps({
        "处理数": total,
        "成功": succeeded,
        "本轮成功率": f"{(succeeded / (total - dirty) * 100 if total > dirty else 0):.2f}%",
        "精度分布": dict(levels),
        "失败": failed,
        "OCR脏数据": dirty,
        "清洗名称": cleaned_names,
        "地址补救无新地名跳过": skipped_address,
        "存疑": doubtful,
        "open_time复核入队": open_time_reviews,
        "累计状态": dict(all_statuses),
        "累计数据来源": dict(all_sources),
        "累计精度分布": dict(all_levels),
        "候选集真实成功率": f"{true_rate:.2f}%",
        "全库真实坐标覆盖率": f"{database_coverage:.2f}%",
        "剩余缺坐标": remaining,
        "备份": str(backup) if backup else "dry-run未备份",
        "报告": str(Path(args.report)),
        "复核清单": str(review_report),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
