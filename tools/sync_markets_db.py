#!/usr/bin/env python3
"""按集市 ID 将基础资料从来源库幂等同步到目标库。

默认只生成差异报告；传入 --apply 才会写目标库。写入前强制创建 SQLite
在线备份。评分、点评数、收藏数、创建人等用户相关字段永不覆盖。
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


SYNC_COLUMNS = (
    "name",
    "category",
    "address",
    "region",
    "open_time",
    "scale",
    "phone",
    "tags",
    "description",
    "lat",
    "lng",
    "source",
    "status",
    "icon",
    "bg",
)
PROTECTED_COLUMNS = (
    "rating",
    "review_count",
    "fav_count",
    "created_by",
    "created_at",
)


@dataclass(frozen=True)
class Change:
    market_id: str
    name: str
    action: str
    changed_fields: tuple[str, ...] = ()


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def check_database(conn: sqlite3.Connection, label: str) -> None:
    result = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if result != "ok":
        raise RuntimeError(f"{label}数据库完整性检查失败: {result}")
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='markets'"
    ).fetchone():
        raise RuntimeError(f"{label}数据库缺少 markets 表")


def market_columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(markets)")}


def build_changes(
    source: sqlite3.Connection, target: sqlite3.Connection
) -> tuple[list[Change], tuple[str, ...]]:
    source_columns = market_columns(source)
    target_columns = market_columns(target)
    if not {"id", "name"}.issubset(source_columns & target_columns):
        raise RuntimeError("来源库和目标库的 markets 表都必须包含 id、name")

    shared = tuple(column for column in SYNC_COLUMNS if column in source_columns & target_columns)
    select_columns = ", ".join(("id",) + shared)
    target_rows = {
        row["id"]: row
        for row in target.execute(f"SELECT {select_columns} FROM markets")
    }

    changes: list[Change] = []
    for source_row in source.execute(f"SELECT {select_columns} FROM markets ORDER BY id"):
        market_id = str(source_row["id"])
        target_row = target_rows.get(market_id)
        if target_row is None:
            changes.append(Change(market_id, source_row["name"] or "", "insert"))
            continue
        changed_fields = tuple(
            column for column in shared if source_row[column] != target_row[column]
        )
        if changed_fields:
            changes.append(
                Change(market_id, source_row["name"] or "", "update", changed_fields)
            )
    return changes, shared


def backup_database(target_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = backup_dir / f"{target_path.stem}.before_market_sync.{stamp}.db"
    with closing(connect(target_path)) as source, closing(connect(backup_path)) as destination:
        source.backup(destination)
        check_database(destination, "备份")
    return backup_path


def apply_changes(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    changes: list[Change],
    shared_columns: tuple[str, ...],
) -> None:
    change_by_id = {change.market_id: change for change in changes}
    if not change_by_id:
        return

    select_columns = ", ".join(("id",) + shared_columns)
    source_rows = {
        str(row["id"]): row
        for row in source.execute(f"SELECT {select_columns} FROM markets")
        if str(row["id"]) in change_by_id
    }
    target_columns = market_columns(target)

    with target:
        for market_id, change in change_by_id.items():
            row = source_rows[market_id]
            if change.action == "insert":
                columns = ("id",) + shared_columns
                placeholders = ", ".join("?" for _ in columns)
                target.execute(
                    f"INSERT INTO markets ({', '.join(columns)}) VALUES ({placeholders})",
                    tuple(row[column] for column in columns),
                )
                continue

            assignments = [f"{column} = ?" for column in change.changed_fields]
            values: list[Any] = [row[column] for column in change.changed_fields]
            if "updated_at" in target_columns:
                assignments.append("updated_at = datetime('now','localtime')")
            values.append(market_id)
            target.execute(
                f"UPDATE markets SET {', '.join(assignments)} WHERE id = ?", values
            )


def write_report(
    report_dir: Path,
    changes: list[Change],
    source_count: int,
    target_count: int,
    applied: bool,
    backup_path: Path | None,
) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    csv_path = report_dir / f"market_sync_diff_{stamp}.csv"
    json_path = report_dir / f"market_sync_summary_{stamp}.json"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(("market_id", "name", "action", "changed_fields"))
        for change in changes:
            writer.writerow(
                (change.market_id, change.name, change.action, "|".join(change.changed_fields))
            )

    inserts = sum(change.action == "insert" for change in changes)
    updates = sum(change.action == "update" for change in changes)
    summary = {
        "applied": applied,
        "source_markets": source_count,
        "target_markets_before": target_count,
        "insert": inserts,
        "update": updates,
        "unchanged_source_rows": source_count - inserts - updates,
        "protected_columns": list(PROTECTED_COLUMNS),
        "backup": str(backup_path) if backup_path else None,
        "diff_csv": str(csv_path),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return csv_path, json_path


def sync(
    source_path: Path,
    target_path: Path,
    report_dir: Path,
    apply: bool = False,
    backup_dir: Path | None = None,
) -> dict[str, Any]:
    source_path = source_path.resolve()
    target_path = target_path.resolve()
    if source_path == target_path:
        raise ValueError("来源库与目标库不能是同一个文件")
    if not source_path.is_file() or not target_path.is_file():
        raise FileNotFoundError("来源库或目标库不存在")

    backup_path = None
    with closing(connect(source_path)) as source, closing(connect(target_path)) as target:
        check_database(source, "来源")
        check_database(target, "目标")
        changes, shared_columns = build_changes(source, target)
        source_count = source.execute("SELECT count(*) FROM markets").fetchone()[0]
        target_count = target.execute("SELECT count(*) FROM markets").fetchone()[0]

    if apply:
        backup_path = backup_database(
            target_path, backup_dir or target_path.parent / "backups"
        )
        with closing(connect(source_path)) as source, closing(connect(target_path)) as target:
            apply_changes(source, target, changes, shared_columns)
            check_database(target, "同步后目标")

    csv_path, json_path = write_report(
        report_dir, changes, source_count, target_count, apply, backup_path
    )
    return {
        "changes": changes,
        "backup": backup_path,
        "csv": csv_path,
        "json": json_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="集市资料来源 SQLite")
    parser.add_argument("--target", type=Path, required=True, help="生产/目标 SQLite")
    parser.add_argument("--report-dir", type=Path, default=Path("output/market-sync"))
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--apply", action="store_true", help="备份后写入目标库")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = sync(
        args.source, args.target, args.report_dir, args.apply, args.backup_dir
    )
    inserts = sum(change.action == "insert" for change in result["changes"])
    updates = sum(change.action == "update" for change in result["changes"])
    mode = "已写入" if args.apply else "仅预览"
    print(f"{mode}: 新增 {inserts}，更新 {updates}")
    if result["backup"]:
        print(f"备份: {result['backup']}")
    print(f"报告: {result['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
