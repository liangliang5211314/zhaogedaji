#!/bin/bash
# 找个大集 一键部署脚本
# 用法: bash deploy.sh
# 支持参数: --no-backup  跳过备份（紧急热修复用）

set -e

PROJECT="/www/wwwroot/zhaogedaji"
VENV="$PROJECT/38b982d1de7beb5083833ca4c8158371_venv"
PY="$VENV/bin/python3"
PIP="$VENV/bin/pip3"
GUNICORN="$VENV/bin/gunicorn"
PIDFILE="$PROJECT/logs/gunicorn.pid"

NO_BACKUP=0
for arg in "$@"; do
    [ "$arg" = "--no-backup" ] && NO_BACKUP=1
done

echo "========================================"
echo "  找个大集部署  $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# ── 0. 拉取最新代码 ───────────────────────────────────────────
echo "[0/5] 拉取代码..."
cd "$PROJECT"
git pull origin main
echo "  代码已更新"

# ── 1. 备份 ───────────────────────────────────────────────────
if [ "$NO_BACKUP" -eq 0 ]; then
    echo "[1/6] 备份..."
    STAMP=$(date +%Y%m%d_%H%M%S)
    cp "$PROJECT/app.py" "$PROJECT/app.py.bak.$STAMP" 2>/dev/null || true
    cp "$PROJECT/data/zhaojishi.db" "$PROJECT/data/zhaojishi.db.bak.$STAMP" 2>/dev/null || true
    # 只保留最近5个备份
    ls -t "$PROJECT"/app.py.bak.* 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
    ls -t "$PROJECT"/data/zhaojishi.db.bak.* 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null || true
    echo "  备份完成 ($STAMP)"
else
    echo "[1/6] 备份: 已跳过 (--no-backup)"
fi

# ── 2. 目录与权限 ─────────────────────────────────────────────
echo "[2/6] 检查目录..."
mkdir -p "$PROJECT/logs" "$PROJECT/data" "$PROJECT/static/uploads"
chmod 755 "$PROJECT/static/uploads"
# .env 只有 root 可读
[ -f "$PROJECT/.env" ] && chmod 600 "$PROJECT/.env"
echo "  目录就绪"

# ── 3. 安装依赖 ───────────────────────────────────────────────
echo "[3/6] 安装依赖..."
if [ -f "$PROJECT/requirements.txt" ]; then
    $PIP install -r "$PROJECT/requirements.txt" --quiet
    echo "  依赖已更新"
else
    echo "  requirements.txt 不存在，跳过"
fi

# ── 4. 数据库迁移 ─────────────────────────────────────────────
echo "[4/6] 数据库迁移..."
cd "$PROJECT"
$PY -c "
import sys; sys.path.insert(0, '.')
from app import init_db, app
with app.app_context():
    init_db()
print('  数据库迁移完成')
"

# ── 5. 重启服务 ───────────────────────────────────────────────
echo "[5/6] 重启服务..."

# 优雅停止
if [ -f "$PIDFILE" ] && kill -0 "$(cat $PIDFILE)" 2>/dev/null; then
    kill -TERM "$(cat $PIDFILE)"
    # 等待最多10秒
    for i in $(seq 1 10); do
        kill -0 "$(cat $PIDFILE)" 2>/dev/null || break
        sleep 1
    done
fi

# 安装 systemd 服务（如果存在服务文件但还未注册）
if [ -f "$PROJECT/zhaojishi.service" ] && [ ! -f /etc/systemd/system/zhaojishi.service ]; then
    cp "$PROJECT/zhaojishi.service" /etc/systemd/system/zhaojishi.service
    systemctl daemon-reload
    systemctl enable zhaojishi
    echo "  systemd 服务已注册"
fi

# 启动
if systemctl is-enabled zhaojishi 2>/dev/null | grep -q enabled; then
    systemctl start zhaojishi
    echo "  通过 systemd 启动"
else
    $GUNICORN -c "$PROJECT/gunicorn_conf.py" --daemon app:app
    echo "  通过 gunicorn --daemon 启动"
fi

sleep 2

# ── 验证 ──────────────────────────────────────────────────────
echo ""
echo "验证服务..."
HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:5000/api/health)
if [ "$HTTP" = "200" ]; then
    echo "  HTTP $HTTP  服务正常"
else
    echo "  HTTP $HTTP  服务异常！查看日志："
    echo "    tail -50 $PROJECT/logs/gunicorn_error.log"
    exit 1
fi

echo ""
echo "========================================"
echo "  部署完成"
echo "========================================"
