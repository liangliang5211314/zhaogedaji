"""open_time v2 migration and calendar calculations.

The database continues to store JSON in ``markets.open_time``.  This module is
the single place that understands the v2 contract; display labels and next-open
dates are derived and never written back into the source JSON.
"""

from __future__ import annotations

import copy
import calendar
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from lunar_python import Solar


DEFAULT_TIMEZONE = "Asia/Shanghai"
OPEN_TYPES = {"daily", "lunar", "solar", "weekly", "specific_dates"}
MIGRATION_STATUSES = {"native", "migrated", "needs_review"}
LEAP_MONTH_MODES = {"exclude", "include", "only"}
EXCEPTION_ACTIONS = {"closed", "open_override", "time_override"}
ROOT_KEYS = {
    "version",
    "timezone",
    "type",
    "time_slots",
    "rule",
    "exceptions",
    "source_text",
    "migration_status",
}
TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
CHINESE_NUMBERS = {
    1: "一",
    2: "二",
    3: "三",
    4: "四",
    5: "五",
    6: "六",
    7: "七",
    8: "八",
    9: "九",
    10: "十",
}
LUNAR_DAY_NAMES = {
    1: "初一",
    2: "初二",
    3: "初三",
    4: "初四",
    5: "初五",
    6: "初六",
    7: "初七",
    8: "初八",
    9: "初九",
    10: "初十",
    11: "十一",
    12: "十二",
    13: "十三",
    14: "十四",
    15: "十五",
    16: "十六",
    17: "十七",
    18: "十八",
    19: "十九",
    20: "二十",
    21: "廿一",
    22: "廿二",
    23: "廿三",
    24: "廿四",
    25: "廿五",
    26: "廿六",
    27: "廿七",
    28: "廿八",
    29: "廿九",
    30: "三十",
}


class OpenTimeValidationError(ValueError):
    def __init__(self, errors: list[str]):
        self.errors = tuple(errors)
        super().__init__("; ".join(errors))


@dataclass(frozen=True)
class MigrationResult:
    value: dict | None
    status: str
    reasons: tuple[str, ...] = ()


def _as_int_list(value, field: str, minimum: int, maximum: int) -> list[int]:
    if not isinstance(value, list) or not value:
        raise OpenTimeValidationError([f"{field} 必须是非空数组"])
    if any(isinstance(item, bool) or not isinstance(item, int) for item in value):
        raise OpenTimeValidationError([f"{field} 只能包含整数"])
    result = sorted(set(value))
    if any(item < minimum or item > maximum for item in result):
        raise OpenTimeValidationError([f"{field} 必须在 {minimum}..{maximum} 之间"])
    return result


def _validate_time_slots(value, field: str = "time_slots") -> list[dict]:
    if not isinstance(value, list):
        raise OpenTimeValidationError([f"{field} 必须是数组"])
    result = []
    for index, slot in enumerate(value):
        if not isinstance(slot, dict) or set(slot) != {"start", "end"}:
            raise OpenTimeValidationError(
                [f"{field}[{index}] 只能包含 start 和 end"]
            )
        if not TIME_RE.fullmatch(str(slot["start"])) or not TIME_RE.fullmatch(
            str(slot["end"])
        ):
            raise OpenTimeValidationError([f"{field}[{index}] 必须使用 HH:mm"])
        result.append({"start": slot["start"], "end": slot["end"]})
    return result


def _validate_exceptions(value) -> list[dict]:
    if not isinstance(value, list):
        raise OpenTimeValidationError(["exceptions 必须是数组"])
    result = []
    allowed = {"date", "action", "time_slots", "reason", "source", "updated_at"}
    required = {"date", "action", "reason", "source", "updated_at"}
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise OpenTimeValidationError([f"exceptions[{index}] 必须是对象"])
        extra = set(item) - allowed
        missing = required - set(item)
        if extra or missing:
            raise OpenTimeValidationError(
                [f"exceptions[{index}] 字段不合法：extra={sorted(extra)}, missing={sorted(missing)}"]
            )
        try:
            date.fromisoformat(item["date"])
        except (TypeError, ValueError):
            raise OpenTimeValidationError([f"exceptions[{index}].date 必须是 YYYY-MM-DD"])
        if item["action"] not in EXCEPTION_ACTIONS:
            raise OpenTimeValidationError([f"exceptions[{index}].action 不支持"])
        if not str(item["reason"]).strip() or not str(item["source"]).strip():
            raise OpenTimeValidationError([f"exceptions[{index}] reason/source 不能为空"])
        try:
            updated_at = datetime.fromisoformat(
                str(item["updated_at"]).replace("Z", "+00:00")
            )
        except ValueError:
            raise OpenTimeValidationError([f"exceptions[{index}].updated_at 必须是 ISO-8601"])
        if updated_at.utcoffset() is None:
            raise OpenTimeValidationError(
                [f"exceptions[{index}].updated_at 必须包含时区"]
            )
        slots = _validate_time_slots(item.get("time_slots", []), f"exceptions[{index}].time_slots")
        if item["action"] == "time_override" and not slots:
            raise OpenTimeValidationError([f"exceptions[{index}] time_override 必须给出 time_slots"])
        normalized = {
            "date": item["date"],
            "action": item["action"],
            "reason": str(item["reason"]).strip(),
            "source": str(item["source"]).strip(),
            "updated_at": item["updated_at"],
        }
        if slots:
            normalized["time_slots"] = slots
        result.append(normalized)
    return result


def validate_open_time(value: dict) -> dict:
    """Validate and return a canonical copy of an open_time v2 object."""
    if not isinstance(value, dict):
        raise OpenTimeValidationError(["open_time 必须是对象"])
    extra = set(value) - ROOT_KEYS
    required = {
        "version",
        "timezone",
        "type",
        "time_slots",
        "rule",
        "exceptions",
        "migration_status",
    }
    missing = required - set(value)
    if extra or missing:
        raise OpenTimeValidationError(
            [f"根字段不合法：extra={sorted(extra)}, missing={sorted(missing)}"]
        )
    if value["version"] != 2:
        raise OpenTimeValidationError(["version 必须为 2"])
    try:
        ZoneInfo(value["timezone"])
    except (TypeError, ZoneInfoNotFoundError):
        raise OpenTimeValidationError(["timezone 不是有效 IANA 时区"])
    if value["type"] not in OPEN_TYPES:
        raise OpenTimeValidationError(["type 不支持"])
    if value["migration_status"] not in MIGRATION_STATUSES:
        raise OpenTimeValidationError(["migration_status 不支持"])
    if not isinstance(value["rule"], dict):
        raise OpenTimeValidationError(["rule 必须是对象"])

    normalized = copy.deepcopy(value)
    normalized["time_slots"] = _validate_time_slots(value["time_slots"])
    normalized["exceptions"] = _validate_exceptions(value["exceptions"])
    normalized["source_text"] = str(value.get("source_text", "")).strip()

    rule = value["rule"]
    if value["type"] == "daily":
        if rule:
            raise OpenTimeValidationError(["daily.rule 必须是空对象"])
        normalized["rule"] = {}
    elif value["type"] == "lunar":
        if set(rule) != {"days", "leap_month"}:
            raise OpenTimeValidationError(["lunar.rule 只能包含 days 和 leap_month"])
        if rule["leap_month"] not in LEAP_MONTH_MODES:
            raise OpenTimeValidationError(["lunar.rule.leap_month 不支持"])
        normalized["rule"] = {
            "days": _as_int_list(rule["days"], "lunar.rule.days", 1, 30),
            "leap_month": rule["leap_month"],
        }
    elif value["type"] == "solar":
        if set(rule) != {"days"}:
            raise OpenTimeValidationError(["solar.rule 只能包含 days"])
        normalized["rule"] = {
            "days": _as_int_list(rule["days"], "solar.rule.days", 1, 31)
        }
    elif value["type"] == "weekly":
        if set(rule) != {"weekdays"}:
            raise OpenTimeValidationError(["weekly.rule 只能包含 weekdays"])
        normalized["rule"] = {
            "weekdays": _as_int_list(rule["weekdays"], "weekly.rule.weekdays", 1, 7)
        }
    else:
        if set(rule) != {"entries"} or not isinstance(rule["entries"], list) or not rule["entries"]:
            raise OpenTimeValidationError(["specific_dates.rule.entries 必须是非空数组"])
        entries = []
        allowed = {"calendar", "year", "month", "day", "duration_days"}
        for index, entry in enumerate(rule["entries"]):
            if not isinstance(entry, dict) or set(entry) - allowed:
                raise OpenTimeValidationError([f"specific_dates.entries[{index}] 字段不合法"])
            if not {"calendar", "month", "day", "duration_days"}.issubset(entry):
                raise OpenTimeValidationError([f"specific_dates.entries[{index}] 缺少字段"])
            if entry["calendar"] not in {"lunar", "solar"}:
                raise OpenTimeValidationError([f"specific_dates.entries[{index}].calendar 不支持"])
            month, day = entry["month"], entry["day"]
            duration = entry["duration_days"]
            if not isinstance(month, int) or not 1 <= month <= 12:
                raise OpenTimeValidationError([f"specific_dates.entries[{index}].month 越界"])
            max_day = 30 if entry["calendar"] == "lunar" else 31
            if not isinstance(day, int) or not 1 <= day <= max_day:
                raise OpenTimeValidationError([f"specific_dates.entries[{index}].day 越界"])
            if not isinstance(duration, int) or not 1 <= duration <= 30:
                raise OpenTimeValidationError([f"specific_dates.entries[{index}].duration_days 越界"])
            if "year" in entry:
                year = entry["year"]
                if not isinstance(year, int) or not 1900 <= year <= 2099:
                    raise OpenTimeValidationError([f"specific_dates.entries[{index}].year 越界"])
                if entry["calendar"] == "solar":
                    try:
                        date(year, month, day)
                    except ValueError:
                        raise OpenTimeValidationError([f"specific_dates.entries[{index}] 不是有效阳历日期"])
            entries.append({key: entry[key] for key in ("calendar", "year", "month", "day", "duration_days") if key in entry})
        normalized["rule"] = {"entries": entries}
    return normalized


def _computable_open_time(value: dict) -> dict:
    canonical = validate_open_time(value)
    if canonical["migration_status"] == "needs_review":
        raise OpenTimeValidationError(["needs_review 记录禁止参与开集日期计算"])
    return canonical


def _legacy_time_slots(value: dict) -> tuple[list[dict] | None, str | None]:
    start = str(value.get("start") or "").strip()
    end = str(value.get("end") or "").strip()
    if not start and not end:
        return [], None
    if not start or not end:
        return None, "旧时间段只有 start 或 end，需人工确认"
    try:
        return _validate_time_slots([{"start": start, "end": end}]), None
    except OpenTimeValidationError as exc:
        return None, str(exc)


def _has_lunar_anchor_evidence(text: str, first: int, second: int) -> bool:
    compact = re.sub(r"[\s,，、/\-]", "", text or "")
    variants = {
        f"逢{first}逢{second}",
        f"逢{CHINESE_NUMBERS.get(first, first)}逢{CHINESE_NUMBERS.get(second, second)}",
    }
    return any(item in compact for item in variants)


def _expand_five_day_pattern(days: list[int], source_text: str) -> list[int] | None:
    anchors = sorted(set(days))
    if len(anchors) != 2 or anchors[1] - anchors[0] != 5:
        return None
    if not _has_lunar_anchor_evidence(source_text, anchors[0], anchors[1]):
        return None
    return list(range(anchors[0], 31, 5))


def _migration_v2(kind: str, rule: dict, source: str, slots: list[dict], exceptions=None) -> dict:
    return validate_open_time(
        {
            "version": 2,
            "timezone": DEFAULT_TIMEZONE,
            "type": kind,
            "time_slots": slots,
            "rule": rule,
            "exceptions": exceptions or [],
            "source_text": source,
            "migration_status": "migrated",
        }
    )


def migrate_open_time(raw) -> MigrationResult:
    """Convert a legacy value to v2 without guessing uncertain schedules."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return MigrationResult(None, "needs_review", ("open_time 不是有效 JSON",))
    if not isinstance(raw, dict):
        return MigrationResult(None, "needs_review", ("open_time 不是对象",))
    if raw.get("version") == 2:
        try:
            return MigrationResult(validate_open_time(raw), "native")
        except OpenTimeValidationError as exc:
            return MigrationResult(None, "needs_review", exc.errors)

    legacy_type = raw.get("type")
    source = str(raw.get("text") or raw.get("custom") or "").strip()
    slots, slot_error = _legacy_time_slots(raw)
    if slot_error:
        return MigrationResult(None, "needs_review", (slot_error,))
    try:
        if legacy_type == "daily":
            value = _migration_v2("daily", {}, source, slots)
        elif legacy_type == "lunar":
            days = _as_int_list(raw.get("days"), "legacy lunar.days", 1, 30)
            if len(days) == 2:
                expanded = _expand_five_day_pattern(days, source)
                if expanded is None:
                    return MigrationResult(
                        None,
                        "needs_review",
                        ("lunar.days 只有两个锚点且缺少逢集口诀证据",),
                    )
                days = expanded
            value = _migration_v2(
                "lunar",
                {"days": days, "leap_month": raw.get("leap_month", "exclude")},
                source,
                slots,
            )
        elif legacy_type in {"solar", "monthly"}:
            days = _as_int_list(raw.get("days"), "legacy solar.days", 1, 31)
            value = _migration_v2("solar", {"days": days}, source, slots)
        elif legacy_type == "weekday":
            weekdays = _as_int_list(raw.get("days"), "legacy weekday.days", 0, 6)
            weekdays = sorted(7 if item == 0 else item for item in weekdays)
            value = _migration_v2("weekly", {"weekdays": weekdays}, source, slots)
        elif legacy_type == "festival":
            calendar = raw.get("calendar", "lunar")
            if calendar not in {"lunar", "solar"}:
                raise OpenTimeValidationError(["festival.calendar 不支持"])
            duration = int(raw.get("duration") or 1)
            entries = []
            for item in raw.get("dates") or []:
                match = re.fullmatch(r"(\d{1,2})-(\d{1,2})", str(item))
                if not match:
                    raise OpenTimeValidationError([f"festival 日期无法解析：{item}"])
                entries.append(
                    {
                        "calendar": calendar,
                        "month": int(match.group(1)),
                        "day": int(match.group(2)),
                        "duration_days": duration,
                    }
                )
            value = _migration_v2("specific_dates", {"entries": entries}, source, slots)
        elif legacy_type in {"lunar_event", "solar_event"}:
            calendar = "lunar" if legacy_type == "lunar_event" else "solar"
            entry = {
                "calendar": calendar,
                "month": int(raw.get("month")),
                "day": int(raw.get("day")),
                "duration_days": int(raw.get("duration") or 1),
            }
            value = _migration_v2("specific_dates", {"entries": [entry]}, source, slots)
        elif legacy_type == "custom":
            return MigrationResult(None, "needs_review", ("custom/纯文本没有可验证的日期规则",))
        else:
            return MigrationResult(None, "needs_review", (f"未知旧类型：{legacy_type!r}",))
    except (OpenTimeValidationError, TypeError, ValueError) as exc:
        reasons = exc.errors if isinstance(exc, OpenTimeValidationError) else (str(exc),)
        return MigrationResult(None, "needs_review", tuple(reasons))
    return MigrationResult(value, "migrated")


@lru_cache(maxsize=4096)
def _lunar_parts(day: date) -> tuple[int, int, int, bool]:
    lunar = Solar.fromYmd(day.year, day.month, day.day).getLunar()
    month = lunar.getMonth()
    return lunar.getYear(), abs(month), lunar.getDay(), month < 0


def _specific_entry_starts_on(entry: dict, day: date) -> bool:
    if entry["calendar"] == "solar":
        return (
            ("year" not in entry or entry["year"] == day.year)
            and entry["month"] == day.month
            and entry["day"] == day.day
        )
    lunar_year, lunar_month, lunar_day, is_leap = _lunar_parts(day)
    return (
        not is_leap
        and ("year" not in entry or entry["year"] == lunar_year)
        and entry["month"] == lunar_month
        and entry["day"] == lunar_day
    )


def _main_rule_matches(value: dict, day: date) -> bool:
    kind, rule = value["type"], value["rule"]
    if kind == "daily":
        return True
    if kind == "solar":
        return day.day in rule["days"]
    if kind == "weekly":
        return day.isoweekday() in rule["weekdays"]
    if kind == "lunar":
        _, _, lunar_day, is_leap = _lunar_parts(day)
        leap_mode = rule["leap_month"]
        if leap_mode == "exclude" and is_leap:
            return False
        if leap_mode == "only" and not is_leap:
            return False
        return lunar_day in rule["days"]
    for entry in rule["entries"]:
        for offset in range(entry["duration_days"]):
            if _specific_entry_starts_on(entry, day - timedelta(days=offset)):
                return True
    return False


def _exceptions_on(value: dict, day: date) -> list[dict]:
    target = day.isoformat()
    return [item for item in value["exceptions"] if item["date"] == target]


def _effective_day(value: dict, day: date) -> tuple[bool, list[dict]]:
    base_open = _main_rule_matches(value, day)
    exceptions = _exceptions_on(value, day)
    if not exceptions:
        return base_open, value["time_slots"]
    if any(item["action"] == "closed" for item in exceptions):
        return False, []
    open_override = any(item["action"] == "open_override" for item in exceptions)
    slot_overrides = [item for item in exceptions if item.get("time_slots")]
    if slot_overrides:
        latest = max(
            slot_overrides,
            key=lambda item: datetime.fromisoformat(
                item["updated_at"].replace("Z", "+00:00")
            ),
        )
        slots = latest["time_slots"]
    else:
        slots = value["time_slots"]
    return base_open or open_override, slots


def is_open_on_date(value: dict, day: date) -> bool:
    canonical = _computable_open_time(value)
    return _effective_day(canonical, day)[0]


def _slot_bounds(day: date, slot: dict, tz: ZoneInfo) -> tuple[datetime, datetime]:
    start_t = time.fromisoformat(slot["start"])
    end_t = time.fromisoformat(slot["end"])
    start = datetime.combine(day, start_t, tzinfo=tz)
    end = datetime.combine(day, end_t, tzinfo=tz)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def _active_in_slots(now: datetime, day: date, slots: list[dict], tz: ZoneInfo) -> bool:
    return any(start <= now < end for start, end in (_slot_bounds(day, slot, tz) for slot in slots))


def _all_slots_ended(now: datetime, day: date, slots: list[dict], tz: ZoneInfo) -> bool:
    return bool(slots) and all(now >= _slot_bounds(day, slot, tz)[1] for slot in slots)


def _display_rule(value: dict) -> str:
    kind, rule = value["type"], value["rule"]
    if kind == "daily":
        return "每日营业"
    if kind == "lunar":
        days = rule["days"]
        if len(days) >= 2 and all(days[index] - days[index - 1] == 5 for index in range(1, len(days))):
            first, second = days[0], days[1]
            return f"阴历逢{CHINESE_NUMBERS.get(first, first)}、逢{CHINESE_NUMBERS.get(second, second)}"
        return "阴历每月" + "、".join(str(item) for item in days) + "日"
    if kind == "solar":
        return "阳历每月" + "、".join(str(item) for item in rule["days"]) + "日"
    if kind == "weekly":
        names = {1: "周一", 2: "周二", 3: "周三", 4: "周四", 5: "周五", 6: "周六", 7: "周日"}
        return "每" + "、".join(names[item] for item in rule["weekdays"])
    parts = []
    for entry in rule["entries"]:
        calendar = "阴历" if entry["calendar"] == "lunar" else "阳历"
        year = f"{entry['year']}年" if "year" in entry else "每年"
        parts.append(f"{year}{calendar}{entry['month']}月{entry['day']}日")
    return "、".join(parts)


def _local_now(now: datetime | None, timezone: str) -> datetime:
    tz = ZoneInfo(timezone)
    if now is None:
        return datetime.now(tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=tz)
    return now.astimezone(tz)


def compute_open_time(value: dict, now: datetime | None = None, horizon_days: int = 400) -> dict:
    """Derive current and next-opening state from a validated v2 object."""
    canonical = _computable_open_time(value)
    local_now = _local_now(now, canonical["timezone"])
    tz = ZoneInfo(canonical["timezone"])
    today = local_now.date()
    today_open, today_slots = _effective_day(canonical, today)
    previous = today - timedelta(days=1)
    previous_open, previous_slots = _effective_day(canonical, previous)
    open_now = (
        (today_open and _active_in_slots(local_now, today, today_slots, tz))
        or (previous_open and _active_in_slots(local_now, previous, previous_slots, tz))
    )
    ended_today = today_open and _all_slots_ended(local_now, today, today_slots, tz) and not open_now

    next_day = None
    for offset in range(horizon_days + 1):
        candidate = today + timedelta(days=offset)
        candidate_open, candidate_slots = _effective_day(canonical, candidate)
        if not candidate_open:
            continue
        if offset == 0 and _all_slots_ended(local_now, candidate, candidate_slots, tz):
            continue
        next_day = candidate
        break

    if open_now or (today_open and not ended_today):
        status = "daily" if canonical["type"] == "daily" else "today"
    elif ended_today:
        status = "ended_today"
    elif next_day == today + timedelta(days=1):
        status = "tomorrow"
    elif next_day:
        status = "future"
    else:
        status = "no_upcoming_date"

    return {
        "is_open_now": open_now,
        "is_open_today": today_open,
        "next_open_date": next_day.isoformat() if next_day else None,
        "status": status,
        "display_rule": _display_rule(canonical),
    }


def compute_month_calendar(value: dict, year: int, month: int) -> dict:
    """Return open dates for a Gregorian month, including lunar labels for UI."""
    if not isinstance(year, int) or not 1900 <= year <= 2099:
        raise ValueError("year 必须在 1900..2099 之间")
    if not isinstance(month, int) or not 1 <= month <= 12:
        raise ValueError("month 必须在 1..12 之间")
    canonical = _computable_open_time(value)
    open_days = []
    for day_number in range(1, calendar.monthrange(year, month)[1] + 1):
        solar_day = date(year, month, day_number)
        is_open, slots = _effective_day(canonical, solar_day)
        if not is_open:
            continue
        lunar_year, lunar_month, lunar_day, is_leap = _lunar_parts(solar_day)
        open_days.append(
            {
                "date": solar_day.isoformat(),
                "lunar_year": lunar_year,
                "lunar_month": lunar_month,
                "lunar_day": lunar_day,
                "lunar_label": LUNAR_DAY_NAMES[lunar_day],
                "is_leap_month": is_leap,
                "time_slots": slots,
            }
        )
    return {
        "year": year,
        "month": month,
        "timezone": canonical["timezone"],
        "open_days": open_days,
    }
