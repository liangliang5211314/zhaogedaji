#!/usr/bin/env bash
# 找个大集生产部署：预检 → 代码差异 → 数据库备份 → 更新 → 迁移/同步 → 重启 → 验证

set -Eeuo pipefail

PROJECT="${PROJECT:-$(cd "$(dirname "$0")" && pwd)}"
DEPLOY_REMOTE="${DEPLOY_REMOTE:-origin}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-main}"
SERVICE_NAME="${SERVICE_NAME:-zhaojishi}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:5000/api/health}"
PUBLIC_HEALTH_URL="${PUBLIC_HEALTH_URL:-}"
MARKET_SOURCE_DB="${MARKET_SOURCE_DB:-}"
APPLY_MARKET_SYNC="${APPLY_MARKET_SYNC:-0}"
CURRENT_STEP="预检"
OLD_COMMIT=""
BACKUP_DIR=""

on_error() {
    local code=$?
    trap - ERR
    echo
    echo "部署在“$CURRENT_STEP”失败，未自动覆盖数据库备份。"
    if [ -n "$OLD_COMMIT" ]; then
        echo "原代码提交: $OLD_COMMIT"
    fi
    if [ -n "$BACKUP_DIR" ]; then
        echo "部署前备份: $BACKUP_DIR"
        echo "人工回滚时先停止服务，再恢复该目录内的 zhaojishi.db；代码可临时切到原提交验证。"
    fi
    exit "$code"
}
trap on_error ERR

find_venv() {
    if [ -n "${VENV:-}" ]; then
        return
    fi
    local candidate
    for candidate in \
        "$PROJECT/38b982d1de7beb5083833ca4c8158371_venv" \
        "$PROJECT/.venv"; do
        if [ -x "$candidate/bin/python3" ] || [ -x "$candidate/bin/python" ]; then
            VENV="$candidate"
            return
        fi
    done
    echo "未找到 Python 虚拟环境，请通过 VENV=/绝对路径 指定。" >&2
    return 1
}

find_venv
if [ -x "$VENV/bin/python3" ]; then PY="$VENV/bin/python3"; else PY="$VENV/bin/python"; fi
if [ -x "$VENV/bin/pip3" ]; then PIP="$VENV/bin/pip3"; else PIP="$VENV/bin/pip"; fi
GUNICORN="$VENV/bin/gunicorn"

cd "$PROJECT"
if [ ! -d .git ]; then
    echo "PROJECT 不是 Git 工作区: $PROJECT" >&2
    exit 1
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "服务器存在未提交的已跟踪文件改动，拒绝部署。" >&2
    git status --short
    exit 1
fi
git remote get-url "$DEPLOY_REMOTE" >/dev/null
OLD_COMMIT="$(git rev-parse HEAD)"

# 直接读取配置，不导入 app.py，避免备份前触发 init_db。
DB_PATH="$($PY - "$PROJECT" <<'PY'
import os
import sys
from pathlib import Path

project = Path(sys.argv[1])
sys.path.insert(0, str(project))
try:
    import config as cfg
except ImportError:
    cfg = None

db_path = getattr(cfg, "DB_PATH", None) or os.environ.get(
    "ZHAOGEDAJI_DB_PATH", str(project / "data" / "zhaojishi.db")
)
secrets = {
    "API_SECRET": getattr(cfg, "API_SECRET", None) or os.environ.get("API_SECRET"),
    "ADMIN_KEY": getattr(cfg, "ADMIN_KEY", None) or os.environ.get("ADMIN_KEY"),
    "JWT_SECRET": getattr(cfg, "JWT_SECRET", None) or os.environ.get("JWT_SECRET"),
}
defaults = {
    "API_SECRET": "zhaojishi_secret_2024",
    "ADMIN_KEY": "admin_zhaojishi_2024",
    "JWT_SECRET": "zhaojishi_jwt_2025",
}
unsafe = [name for name, value in secrets.items() if not value or value == defaults[name]]
if unsafe:
    raise SystemExit("生产密钥未配置或仍为默认值: " + ", ".join(unsafe))
print(Path(db_path).expanduser().resolve())
PY
)"
if [ ! -f "$DB_PATH" ]; then
    echo "生产数据库不存在: $DB_PATH" >&2
    exit 1
fi

CURRENT_STEP="获取代码差异"
git fetch "$DEPLOY_REMOTE" "$DEPLOY_BRANCH"
TARGET_COMMIT="$(git rev-parse FETCH_HEAD)"
if ! git merge-base --is-ancestor "$OLD_COMMIT" "$TARGET_COMMIT"; then
    echo "远端不是当前提交的快进更新，拒绝自动合并。" >&2
    exit 1
fi
echo "待部署代码: $OLD_COMMIT -> $TARGET_COMMIT"
git diff --stat "$OLD_COMMIT" "$TARGET_COMMIT"

CURRENT_STEP="备份生产数据库"
STAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="$PROJECT/data/backups/deploy_$STAMP"
mkdir -p "$BACKUP_DIR"
printf '%s\n' "$OLD_COMMIT" > "$BACKUP_DIR/code_commit_before.txt"
printf '%s\n' "$TARGET_COMMIT" > "$BACKUP_DIR/code_commit_target.txt"
BACKUP_DB="$BACKUP_DIR/zhaojishi.db"
"$PY" - "$DB_PATH" "$BACKUP_DB" <<'PY'
import sqlite3
import sys

source_path, backup_path = sys.argv[1:]
source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
backup = sqlite3.connect(backup_path)
try:
    if source.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise SystemExit("生产数据库完整性检查失败，停止部署")
    source.backup(backup)
    if backup.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise SystemExit("数据库备份完整性检查失败，停止部署")
finally:
    backup.close()
    source.close()
PY
chmod 600 "$BACKUP_DB"
echo "部署前数据库备份完成: $BACKUP_DB"

CURRENT_STEP="更新代码"
git merge --ff-only "$TARGET_COMMIT"

CURRENT_STEP="安装依赖"
if [ -f requirements.txt ]; then
    "$PIP" install -r requirements.txt --quiet
fi

CURRENT_STEP="初始化数据库结构"
"$PY" - <<'PY'
from app import DB_PATH
print("数据库结构初始化完成:", DB_PATH)
PY

if [ -n "$MARKET_SOURCE_DB" ]; then
    CURRENT_STEP="核对集市数据差异"
    if [ ! -f "$MARKET_SOURCE_DB" ]; then
        echo "集市来源库不存在: $MARKET_SOURCE_DB" >&2
        exit 1
    fi
    "$PY" tools/sync_markets_db.py \
        --source "$MARKET_SOURCE_DB" \
        --target "$DB_PATH" \
        --report-dir "$BACKUP_DIR/market-sync-preview"
    if [ "$APPLY_MARKET_SYNC" = "1" ]; then
        CURRENT_STEP="同步集市基础资料"
        "$PY" tools/sync_markets_db.py \
            --source "$MARKET_SOURCE_DB" \
            --target "$DB_PATH" \
            --report-dir "$BACKUP_DIR/market-sync-applied" \
            --backup-dir "$BACKUP_DIR/market-sync-backups" \
            --apply
    else
        echo "仅生成集市差异，未写入；确认后设置 APPLY_MARKET_SYNC=1 再部署。"
    fi
fi

CURRENT_STEP="准备运行目录"
mkdir -p logs data static/uploads
chmod 755 static/uploads
[ -f .env ] && chmod 600 .env

CURRENT_STEP="重启服务"
if systemctl cat "$SERVICE_NAME.service" >/dev/null 2>&1; then
    systemctl restart "$SERVICE_NAME"
    echo "已通过 systemd 重启 $SERVICE_NAME"
else
    PIDFILE="$PROJECT/logs/gunicorn.pid"
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        kill -TERM "$(cat "$PIDFILE")"
        for _ in $(seq 1 10); do
            kill -0 "$(cat "$PIDFILE")" 2>/dev/null || break
            sleep 1
        done
    fi
    "$GUNICORN" -c "$PROJECT/gunicorn_conf.py" --pid "$PIDFILE" --daemon app:app
    echo "已通过 gunicorn 启动"
fi

CURRENT_STEP="本机健康检查"
for _ in $(seq 1 20); do
    if curl --fail --silent --show-error "$HEALTH_URL" >/dev/null; then
        break
    fi
    sleep 1
done
curl --fail --silent --show-error "$HEALTH_URL" >/dev/null
echo "本机健康检查通过: $HEALTH_URL"

if [ -n "$PUBLIC_HEALTH_URL" ]; then
    CURRENT_STEP="公网健康检查"
    curl --fail --silent --show-error "$PUBLIC_HEALTH_URL" >/dev/null
    echo "公网健康检查通过: $PUBLIC_HEALTH_URL"
fi

CURRENT_STEP="记录部署结果"
printf '%s\n' "$(git rev-parse HEAD)" > "$BACKUP_DIR/code_commit_deployed.txt"
"$PY" - "$DB_PATH" <<'PY'
import sqlite3
import sys

conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
try:
    result = conn.execute("PRAGMA integrity_check").fetchone()[0]
finally:
    conn.close()
if result != "ok":
    raise SystemExit("部署后数据库完整性检查失败: " + result)
print("部署后数据库完整性检查通过")
PY

echo "部署完成，提交: $(git rev-parse --short HEAD)"
echo "备份与差异报告: $BACKUP_DIR"
