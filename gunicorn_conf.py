"""Gunicorn 生产配置"""
import multiprocessing

# ── 绑定 ─────────────────────────────────────────────────────
bind    = '127.0.0.1:5000'
backlog = 512

# ── Worker ───────────────────────────────────────────────────
# SQLite 单写，worker 数不宜过多，2核服务器用3个
workers       = multiprocessing.cpu_count() * 2 - 1
worker_class  = 'sync'
threads       = 1
timeout       = 200
graceful_timeout = 60
keepalive     = 5

# ── 进程 ─────────────────────────────────────────────────────
chdir = '/www/wwwroot/zhaogedaji'
# pidfile 只在手动 --daemon 启动时需要；systemd 管理时不设置

# ── 日志 ─────────────────────────────────────────────────────
loglevel    = 'info'
errorlog    = '/www/wwwroot/zhaogedaji/logs/gunicorn_error.log'
accesslog   = '/www/wwwroot/zhaogedaji/logs/access.log'
access_log_format = '%(t)s %(h)s "%(r)s" %(s)s %(L)ss %(b)sB'

# ── 进程名 ───────────────────────────────────────────────────
proc_name = 'zhaogedaji'
