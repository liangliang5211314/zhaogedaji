"""日志系统配置 - RotatingFileHandler for app.log and error.log"""
import logging, os
from logging.handlers import RotatingFileHandler


def setup_logging(log_dir=None):
    base = log_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(base, exist_ok=True)
    fmt = logging.Formatter('[%(asctime)s] %(levelname)-8s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # app.log - INFO+ rotating 10MB x 5
    fh = RotatingFileHandler(os.path.join(base, 'app.log'), maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    fh.setFormatter(fmt)
    root.addHandler(fh)
    # error.log - ERROR+ rotating 10MB x 10
    eh = RotatingFileHandler(os.path.join(base, 'zhaojishi_error.log'), maxBytes=10*1024*1024, backupCount=10, encoding='utf-8')
    eh.setLevel(logging.ERROR)
    eh.setFormatter(fmt)
    root.addHandler(eh)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    return logging.getLogger('zhaojishi')
