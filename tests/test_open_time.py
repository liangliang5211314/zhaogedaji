import json
from datetime import date, datetime

import pytest

from open_time import (
    OpenTimeValidationError,
    compute_month_calendar,
    compute_open_time,
    is_open_on_date,
    migrate_open_time,
    validate_open_time,
)


def schedule(kind, rule, *, slots=None, exceptions=None):
    return {
        "version": 2,
        "timezone": "Asia/Shanghai",
        "type": kind,
        "time_slots": slots if slots is not None else [{"start": "07:00", "end": "13:00"}],
        "rule": rule,
        "exceptions": exceptions or [],
        "source_text": "测试规则",
        "migration_status": "native",
    }


def exception(day, action, *, slots=None, updated_at="2026-07-01T08:00:00+08:00"):
    value = {
        "date": day,
        "action": action,
        "reason": "回归测试",
        "source": "admin",
        "updated_at": updated_at,
    }
    if slots is not None:
        value["time_slots"] = slots
    return value


def test_lunar_anchor_migration_requires_source_evidence():
    raw = {"type": "lunar", "days": [1, 6], "text": "阴历逢一逢六", "start": "", "end": ""}
    result = migrate_open_time(json.dumps(raw, ensure_ascii=False))
    assert result.status == "migrated"
    assert result.value["rule"]["days"] == [1, 6, 11, 16, 21, 26]

    ambiguous = migrate_open_time({"type": "lunar", "days": [1, 6], "text": "待核实"})
    assert ambiguous.status == "needs_review"
    assert ambiguous.value is None


@pytest.mark.parametrize(
    ("raw", "expected_status", "expected_type"),
    [
        ({"type": "daily", "text": "每天"}, "migrated", "daily"),
        ({"type": "solar", "days": [1, 6]}, "migrated", "solar"),
        ({"type": "monthly", "days": [2, 7]}, "migrated", "solar"),
        ({"type": "weekday", "days": [0, 1]}, "migrated", "weekly"),
        (
            {"type": "festival", "calendar": "lunar", "dates": ["02-19", "09-09"]},
            "migrated",
            "specific_dates",
        ),
        ({"type": "lunar_event", "month": 3, "day": 3}, "migrated", "specific_dates"),
        ({"type": "solar_event", "month": 10, "day": 2}, "migrated", "specific_dates"),
        ({"type": "custom", "custom": "具体时间待核实"}, "needs_review", None),
    ],
)
def test_all_legacy_type_mappings(raw, expected_status, expected_type):
    result = migrate_open_time(raw)
    assert result.status == expected_status
    assert (result.value or {}).get("type") == expected_type
    if raw["type"] == "weekday":
        assert result.value["rule"]["weekdays"] == [1, 7]


def test_fixed_today_lunar_conversion_and_cross_month_next_open():
    value = schedule(
        "lunar",
        {"days": [1, 6, 11, 16, 21, 26], "leap_month": "exclude"},
    )
    before = compute_open_time(value, datetime(2026, 7, 13, 10, 0))
    assert before["is_open_today"] is False
    assert before["next_open_date"] == "2026-07-14"
    assert before["status"] == "tomorrow"

    open_day = compute_open_time(value, datetime(2026, 7, 14, 8, 0))
    assert open_day["is_open_now"] is True
    assert open_day["next_open_date"] == "2026-07-14"

    across_month = compute_open_time(value, datetime(2026, 7, 30, 10, 0))
    assert across_month["next_open_date"] == "2026-08-03"


def test_month_calendar_matches_hong_kong_observatory_2026_table():
    # Source: HKO 2026 Gregorian-Lunar calendar conversion table.
    # https://www.hko.gov.hk/tc/gts/time/calendar/pdf/files/2026.pdf
    value = schedule(
        "lunar",
        {"days": [1, 6, 11, 16, 21, 26], "leap_month": "exclude"},
    )
    result = compute_month_calendar(value, 2026, 7)
    assert [item["date"] for item in result["open_days"]] == [
        "2026-07-05",
        "2026-07-10",
        "2026-07-14",
        "2026-07-19",
        "2026-07-24",
        "2026-07-29",
    ]
    assert [item["lunar_label"] for item in result["open_days"]] == [
        "廿一",
        "廿六",
        "初一",
        "初六",
        "十一",
        "十六",
    ]


def test_leap_month_modes_are_explicit():
    normal_first = date(2025, 6, 25)
    leap_first = date(2025, 7, 25)
    base_rule = {"days": [1], "leap_month": "exclude"}
    assert is_open_on_date(schedule("lunar", base_rule), normal_first) is True
    assert is_open_on_date(schedule("lunar", base_rule), leap_first) is False

    include = schedule("lunar", {"days": [1], "leap_month": "include"})
    assert is_open_on_date(include, normal_first) is True
    assert is_open_on_date(include, leap_first) is True

    only = schedule("lunar", {"days": [1], "leap_month": "only"})
    assert is_open_on_date(only, normal_first) is False
    assert is_open_on_date(only, leap_first) is True


def test_closed_exception_wins_and_time_override_changes_hours():
    rules = {"days": [1, 6, 11, 16, 21, 26], "leap_month": "exclude"}
    closed = schedule(
        "lunar",
        rules,
        exceptions=[
            exception("2026-07-14", "open_override"),
            exception("2026-07-14", "closed"),
        ],
    )
    result = compute_open_time(closed, datetime(2026, 7, 13, 10, 0))
    assert result["next_open_date"] == "2026-07-19"

    changed_hours = schedule(
        "lunar",
        rules,
        exceptions=[
            exception(
                "2026-07-14",
                "time_override",
                slots=[{"start": "15:00", "end": "18:00"}],
            )
        ],
    )
    assert compute_open_time(changed_hours, datetime(2026, 7, 14, 10, 0))["is_open_now"] is False
    assert compute_open_time(changed_hours, datetime(2026, 7, 14, 16, 0))["is_open_now"] is True


def test_daily_schedule_and_one_day_closure():
    daily = schedule("daily", {})
    current = compute_open_time(daily, datetime(2026, 7, 13, 8, 0))
    assert current["is_open_now"] is True
    assert current["status"] == "daily"

    closed_today = schedule(
        "daily",
        {},
        exceptions=[exception("2026-07-13", "closed")],
    )
    result = compute_open_time(closed_today, datetime(2026, 7, 13, 8, 0))
    assert result["is_open_today"] is False
    assert result["next_open_date"] == "2026-07-14"


def test_specific_dates_support_multiple_annual_dates_and_duration():
    value = schedule(
        "specific_dates",
        {
            "entries": [
                {"calendar": "solar", "month": 7, "day": 14, "duration_days": 2},
                {"calendar": "solar", "month": 10, "day": 2, "duration_days": 1},
            ]
        },
    )
    assert is_open_on_date(value, date(2026, 7, 14)) is True
    assert is_open_on_date(value, date(2026, 7, 15)) is True
    assert is_open_on_date(value, date(2026, 7, 16)) is False
    assert is_open_on_date(value, date(2026, 10, 2)) is True


def test_open_override_can_add_a_date_and_overnight_slots_cross_midnight():
    value = schedule(
        "weekly",
        {"weekdays": [1]},
        slots=[{"start": "20:00", "end": "02:00"}],
        exceptions=[exception("2026-07-14", "open_override")],
    )
    assert is_open_on_date(value, date(2026, 7, 14)) is True
    after_midnight = compute_open_time(value, datetime(2026, 7, 15, 1, 0))
    assert after_midnight["is_open_now"] is True


def test_v2_validation_rejects_unknown_fields():
    value = schedule("daily", {})
    value["derived_next_open"] = "2026-07-14"
    with pytest.raises(OpenTimeValidationError):
        validate_open_time(value)


def test_needs_review_records_never_participate_in_runtime_calculation():
    value = schedule("daily", {})
    value["migration_status"] = "needs_review"
    with pytest.raises(OpenTimeValidationError, match="禁止参与"):
        compute_open_time(value, datetime(2026, 7, 13, 8, 0))
