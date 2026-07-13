"""Gunicorn 生产配置"""
import multiprocessing
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
chdir = BASE_DIR
# pidfile 只在手动 --daemon 启动时需要；systemd 管理时不设置

# ── 日志 ─────────────────────────────────────────────────────
loglevel    = 'info'
errorlog    = os.path.join(BASE_DIR, 'logs', 'gunicorn_error.log')
accesslog   = os.path.join(BASE_DIR, 'logs', 'access.log')
access_log_format = '%(t)s %(h)s "%(r)s" %(s)s %(L)ss %(b)sB'

# ── 进程名 ───────────────────────────────────────────────────
proc_name = 'zhaogedaji'
