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
    "id", "name", "region", "查询词", "期望adcode", "命中adcode",
    "命中level", "旧坐标", "新坐标", "偏移距离", "状态", "原因",
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
    return parser.parse_args()


def clean_text(value):
    value = re.sub(r"[（(][^）)]*[）)]", "", str(value or ""))
    return re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", value)


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
    return candidates


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
        if city and actual_city and normalize_admin(city) != actual_city:
            rejected.append(f"市不一致:{geo.get('city')}")
            continue
        if district and actual_district and normalize_admin(district) != actual_district:
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
    cache_key = "|".join((province, city, district))
    if cache_key in cache and cache[cache_key] and cache[cache_key].get("adcode"):
        return cache[cache_key]
    query = "".join(part for part in (province, city, district) if part)
    if not district or not query:
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
                and normalize_admin(geo.get("district")) == normalize_admin(district)
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
    missing = [row for row in rows if not row["lat"] or not row["lng"]]
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
    backup = None if args.dry_run or not rows else backup_database(args.db)
    succeeded = failed = doubtful = 0
    levels = Counter()

    for index, row in enumerate(rows, 1):
        result, query, reason, expected_adcode = geocode_row(
            row, key, args.sleep, admin_cache
        )
        old_coords = format_coords(row["lat"], row["lng"])
        if result:
            offset = ""
            if row["lat"] and row["lng"]:
                offset = f"{haversine_m(float(row['lat']), float(row['lng']), result['lat'], result['lng']):.0f}m"
            if not args.dry_run:
                conn.execute(
                    "UPDATE markets SET lat=?,lng=?,updated_at=datetime('now','localtime') WHERE id=?",
                    (result["lat"], result["lng"], row["id"]),
                )
            reports[row["id"]] = {
                "id": row["id"], "name": row["name"], "region": row["region"],
                "查询词": query, "期望adcode": expected_adcode,
                "命中adcode": result["adcode"],
                "命中level": result["level"], "旧坐标": old_coords,
                "新坐标": format_coords(result["lat"], result["lng"]),
                "偏移距离": offset, "状态": "成功",
                "原因": ("dry-run；" if args.dry_run else "") + result["formatted_address"],
            }
            succeeded += 1
            levels[result["level"]] += 1
        else:
            status = "存疑" if old_coords else "失败"
            reports[row["id"]] = {
                "id": row["id"], "name": row["name"], "region": row["region"],
                "查询词": query, "期望adcode": expected_adcode,
                "命中adcode": "", "命中level": "",
                "旧坐标": old_coords, "新坐标": "",
                "偏移距离": "", "状态": status, "原因": reason,
            }
            doubtful += status == "存疑"
            failed += status == "失败"
        if index % 20 == 0:
            if not args.dry_run:
                conn.commit()
            write_json(DEFAULT_CENTER_CACHE, admin_cache)
            write_report(args.report, reports)
            print(json.dumps({"完成": index, "成功": succeeded, "失败": failed, "存疑": doubtful}, ensure_ascii=False), flush=True)

    if not args.dry_run:
        conn.commit()
    write_json(DEFAULT_CENTER_CACHE, admin_cache)
    write_report(args.report, reports)
    remaining = conn.execute(
        "SELECT COUNT(*) FROM markets WHERE lat IS NULL OR lng IS NULL OR lat=0 OR lng=0"
    ).fetchone()[0]
    conn.close()
    total = len(rows)
    print(json.dumps({
        "处理数": total,
        "成功": succeeded,
        "成功率": f"{(succeeded / total * 100 if total else 0):.2f}%",
        "精度分布": dict(levels),
        "失败": failed,
        "存疑": doubtful,
        "剩余缺坐标": remaining,
        "备份": str(backup) if backup else "dry-run未备份",
        "报告": str(Path(args.report)),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
