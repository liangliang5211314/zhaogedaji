#!/usr/bin/env python3
"""将来源库的待处理数据复核项幂等补入目标库，不覆盖既有处理结果。"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from tools.sync_markets_db import backup_database, check_database, connect
except ModuleNotFoundError:  # 直接执行 tools/ 下脚本时
    from sync_markets_db import backup_database, check_database, connect


QUEUE_COLUMNS = (
    "entity_type",
    "entity_id",
    "issue_type",
    "reason",
    "source_payload",
    "status",
    "created_at",
    "updated_at",
)
KEY_COLUMNS = ("entity_type", "entity_id", "issue_type")


def has_queue_table(conn: sqlite3.Connection) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='data_review_queue'"
        ).fetchone()
    )


def pending_rows(source: sqlite3.Connection) -> list[sqlite3.Row]:
    if not has_queue_table(source):
        return []
    columns = {row[1] for row in source.execute("PRAGMA table_info(data_review_queue)")}
    if not set(QUEUE_COLUMNS).issubset(columns):
        raise RuntimeError("来源 data_review_queue 字段不完整")
    return source.execute(
        f"SELECT {', '.join(QUEUE_COLUMNS)} FROM data_review_queue "
        "WHERE status='pending' ORDER BY id"
    ).fetchall()


def build_inserts(
    source: sqlite3.Connection, target: sqlite3.Connection
) -> list[sqlite3.Row]:
    if not has_queue_table(target):
        raise RuntimeError("目标库缺少 data_review_queue，请先运行 init_db")
    existing = {
        tuple(row[column] for column in KEY_COLUMNS)
        for row in target.execute(
            f"SELECT {', '.join(KEY_COLUMNS)} FROM data_review_queue"
        )
    }
    return [
        row
        for row in pending_rows(source)
        if tuple(row[column] for column in KEY_COLUMNS) not in existing
    ]


def apply_inserts(target: sqlite3.Connection, rows: list[sqlite3.Row]) -> None:
    if not rows:
        return
    placeholders = ", ".join("?" for _ in QUEUE_COLUMNS)
    with target:
        target.executemany(
            f"INSERT INTO data_review_queue ({', '.join(QUEUE_COLUMNS)}) "
            f"VALUES ({placeholders})",
            [tuple(row[column] for column in QUEUE_COLUMNS) for row in rows],
        )


def write_report(
    report_dir: Path,
    rows: list[sqlite3.Row],
    applied: bool,
    backup_path: Path | None,
) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    csv_path = report_dir / f"review_queue_diff_{stamp}.csv"
    json_path = report_dir / f"review_queue_summary_{stamp}.json"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow((*KEY_COLUMNS, "reason"))
        for row in rows:
            writer.writerow((*[row[column] for column in KEY_COLUMNS], row["reason"]))
    by_type: dict[str, int] = {}
    for row in rows:
        by_type[row["issue_type"]] = by_type.get(row["issue_type"], 0) + 1
    summary = {
        "applied": applied,
        "insert": len(rows),
        "by_issue_type": by_type,
        "existing_records_never_updated": True,
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
    with closing(connect(source_path)) as source, closing(connect(target_path)) as target:
        check_database(source, "来源")
        check_database(target, "目标")
        rows = build_inserts(source, target)

    backup_path = None
    if apply:
        backup_path = backup_database(
            target_path, backup_dir or target_path.parent / "backups"
        )
        with closing(connect(target_path)) as target:
            apply_inserts(target, rows)
            check_database(target, "同步后目标")

    csv_path, json_path = write_report(report_dir, rows, apply, backup_path)
    return {"rows": rows, "backup": backup_path, "csv": csv_path, "json": json_path}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, default=Path("output/review-queue-sync"))
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = sync(
        args.source, args.target, args.report_dir, args.apply, args.backup_dir
    )
    mode = "已写入" if args.apply else "仅预览"
    print(f"{mode}: 新增待复核 {len(result['rows'])}")
    if result["backup"]:
        print(f"备份: {result['backup']}")
    print(f"报告: {result['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
