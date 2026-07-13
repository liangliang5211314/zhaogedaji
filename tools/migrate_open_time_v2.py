#!/usr/bin/env python3
"""Safely migrate markets.open_time to the v2 contract.

Dry-run is the default.  ``--apply`` creates a SQLite backup first, then updates
only records whose legacy rules can be converted without guessing.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from open_time import migrate_open_time  # noqa: E402


DEFAULT_DB = ROOT / "data" / "zhaojishi.db"


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _backup_database(connection: sqlite3.Connection, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as backup:
        connection.backup(backup)


def _source_text(raw: str) -> str:
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return str(raw)
    if not isinstance(value, dict):
        return str(value)
    return str(value.get("text") or value.get("custom") or "")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_migration(
    db_path: str | Path = DEFAULT_DB,
    *,
    apply: bool = False,
    report_dir: str | Path | None = None,
    backup_dir: str | Path | None = None,
) -> dict:
    db_path = Path(db_path).resolve()
    stamp = _timestamp()
    if report_dir is None:
        report_dir = ROOT / "output" / f"open_time_v2_migration_{stamp}"
    report_dir = Path(report_dir).resolve()
    if backup_dir is None:
        backup_dir = (
            ROOT / "data" / "backups"
            if db_path == DEFAULT_DB.resolve()
            else db_path.parent / "backups"
        )
    backup_dir = Path(backup_dir).resolve()

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": str(db_path),
        "mode": "apply" if apply else "dry_run",
        "backup_path": None,
        "counts": {
            "total": 0,
            "native": 0,
            "migrated": 0,
            "needs_review": 0,
            "failed": 0,
        },
        "success": [],
        "needs_review": [],
        "failed": [],
    }
    updates: list[tuple[str, str]] = []

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT id, name, open_time FROM markets WHERE open_time IS NOT NULL"
        ).fetchall()
        report["counts"]["total"] = len(rows)
        for row in rows:
            try:
                result = migrate_open_time(row["open_time"])
                report["counts"][result.status] += 1
                if result.status in {"native", "migrated"}:
                    encoded = json.dumps(
                        result.value,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    if result.status == "migrated":
                        updates.append((encoded, row["id"]))
                    report["success"].append(
                        {
                            "id": row["id"],
                            "name": row["name"],
                            "migration_status": result.status,
                            "source_text": result.value["source_text"],
                            "old_open_time": row["open_time"],
                            "new_open_time": encoded,
                        }
                    )
                elif result.status == "needs_review":
                    report["needs_review"].append(
                        {
                            "id": row["id"],
                            "name": row["name"],
                            "migration_status": "needs_review",
                            "source_text": _source_text(row["open_time"]),
                            "reason": "; ".join(result.reasons),
                            "open_time": row["open_time"],
                        }
                    )
            except Exception as exc:  # keep one bad row from hiding the report
                report["counts"]["failed"] += 1
                report["failed"].append(
                    {
                        "id": row["id"],
                        "name": row["name"],
                        "open_time": row["open_time"],
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        if apply:
            backup_path = backup_dir / f"{db_path.stem}.open-time-v1.{stamp}.sqlite3"
            _backup_database(connection, backup_path)
            report["backup_path"] = str(backup_path)
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.executemany(
                    "UPDATE markets SET open_time = ? WHERE id = ?",
                    updates,
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
    finally:
        connection.close()

    success_path = report_dir / "success.csv"
    review_path = report_dir / "needs_review.csv"
    failed_path = report_dir / "failed.csv"
    _write_csv(
        success_path,
        [
            "id",
            "name",
            "migration_status",
            "source_text",
            "old_open_time",
            "new_open_time",
        ],
        report["success"],
    )
    _write_csv(
        review_path,
        ["id", "name", "migration_status", "source_text", "reason", "open_time"],
        report["needs_review"],
    )
    _write_csv(
        failed_path,
        ["id", "name", "open_time", "error"],
        report["failed"],
    )
    report["report_paths"] = {
        "success": str(success_path),
        "needs_review": str(review_path),
        "failed": str(failed_path),
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--report-dir", type=Path)
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="create a backup and write unambiguous v2 records",
    )
    args = parser.parse_args()
    report = run_migration(
        args.db,
        apply=args.apply,
        report_dir=args.report_dir,
        backup_dir=args.backup_dir,
    )
    counts = report["counts"]
    print(
        f"{report['mode']}: total={counts['total']} "
        f"native={counts['native']} migrated={counts['migrated']} "
        f"needs_review={counts['needs_review']} failed={counts['failed']}"
    )
    if report["backup_path"]:
        print(f"backup: {report['backup_path']}")
    print(f"reports: {Path(report['report_paths']['success']).parent}")
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
