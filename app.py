"""
找个大集 · 服务器端 API v2.0
在原有基础上新增：用户系统、JWT认证、收藏、点评、公告、轮播图
"""
import os, json, uuid, sqlite3, hashlib, hmac, random, time, re, logging
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory

# ── 配置 & 日志（优先于其他模块）────────────────────────────
try:
    import config as _cfg
    DB_PATH    = _cfg.DB_PATH
    API_SECRET = _cfg.API_SECRET
    ADMIN_KEY  = _cfg.ADMIN_KEY
    JWT_SECRET = _cfg.JWT_SECRET
except ImportError:
    DB_PATH    = os.path.join(os.path.dirname(__file__), 'data', 'zhaojishi.db')
    API_SECRET = os.environ.get('API_SECRET', 'zhaojishi_secret_2024')
    ADMIN_KEY  = os.environ.get('ADMIN_KEY',  'admin_zhaojishi_2024')
    JWT_SECRET = os.environ.get('JWT_SECRET', 'zhaojishi_jwt_2025')

try:
    from log_config import setup_logging
    logger = setup_logging()
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('zhaojishi')

app = Flask(__name__, static_folder='static', static_url_path='')

STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')

# 短信验证码持久化到 sms_codes 表（不再用内存缓存，防止进程重启丢失）

# ════════════════════════════════════════════════════════════
# 数据库
# ════════════════════════════════════════════════════════════

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _guess_category(name, fallback='农村大集'):
    """根据名称关键词推断分类"""
    if not name:
        return fallback or '农村大集'
    rules = [
        (['早市', '早集', '早摊', '晨市'],                              '早市'),
        (['夜市', '夜集', '夜间市场', '夜摊', '夜间'],                    '夜市'),
        (['庙会', '庙市', '庙集'],                                       '庙会'),
        (['批发', '交易市场', '交易中心', '配送中心', '仓储',
          '农产品中心', '综合批发'],                                      '批发市场'),
        (['农贸', '菜市场', '便民市场', '生鲜', '蔬菜市场',
          '菜场', '果蔬', '蔬果', '粮油', '农副'],                        '农贸市场'),
        (['花鸟', '鸟市', '花卉', '花市', '鱼市', '水族', '植物'],        '花鸟市场'),
        (['宠物'],                                                        '宠物市场'),
        (['古玩', '古董', '文玩', '收藏', '古货', '古物', '字画', '玉器'], '古玩市场'),
        (['二手', '旧货', '跳蚤', '闲置', '废品'],                        '二手市集'),
        (['小吃街', '小吃城', '美食街', '夜宵', '烧烤街'],                 '小吃街'),
        (['美食', '小吃', '饮食', '餐饮', '食品', '美味'],                 '美食集市'),
        (['大集', '赶集', '逢集', '集会', '集市', '农村市场'],             '农村大集'),
    ]
    for keywords, cat in rules:
        if any(k in name for k in keywords):
            return cat
    return fallback or '农村大集'

def init_db():
    conn = get_db()
    conn.executescript("""
        -- ── 原有表（保持不变）────────────────────────────────
        CREATE TABLE IF NOT EXISTS markets (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            category    TEXT,
            address     TEXT,
            region      TEXT,
            open_time   TEXT,
            phone       TEXT,
            tags        TEXT,
            description TEXT,
            rating      REAL DEFAULT 5.0,
            review_count INTEGER DEFAULT 0,
            fav_count   INTEGER DEFAULT 0,
            lat         REAL,
            lng         REAL,
            source      TEXT DEFAULT 'manual',
            status      TEXT DEFAULT 'pending',
            created_by  TEXT DEFAULT '',
            icon        TEXT DEFAULT '🏮',
            bg          TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            updated_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS spider_queue (
            id          TEXT PRIMARY KEY,
            platform    TEXT,
            raw_title   TEXT,
            raw_text    TEXT,
            market_name TEXT,
            category    TEXT,
            address     TEXT,
            region      TEXT,
            open_time   TEXT,
            phone       TEXT,
            tags        TEXT,
            description TEXT,
            confidence  INTEGER DEFAULT 0,
            likes       INTEGER DEFAULT 0,
            source_url  TEXT,
            status      TEXT DEFAULT 'pending',
            pushed_at   TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS push_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            platform   TEXT,
            count      INTEGER,
            ip         TEXT,
            pushed_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        -- ── 新增表 ────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            uid         TEXT UNIQUE NOT NULL,
            phone       TEXT UNIQUE NOT NULL,
            nickname    TEXT DEFAULT '',
            password    TEXT DEFAULT '',
            role        TEXT DEFAULT 'user',
            avatar      TEXT DEFAULT '👤',
            bio         TEXT DEFAULT '',
            gender      TEXT DEFAULT '',
            region      TEXT DEFAULT '',
            email       TEXT DEFAULT '',
            birth_year  TEXT DEFAULT '',
            interests   TEXT DEFAULT '[]',
            status      TEXT DEFAULT 'normal',
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            last_login  TEXT
        );

        CREATE TABLE IF NOT EXISTS favorites (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            market_id   TEXT NOT NULL,
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(user_id, market_id)
        );

        CREATE TABLE IF NOT EXISTS reviews (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id   TEXT NOT NULL,
            user_id     INTEGER NOT NULL,
            rating      REAL NOT NULL,
            content     TEXT DEFAULT '',
            images      TEXT DEFAULT '[]',
            tags        TEXT DEFAULT '[]',
            likes       INTEGER DEFAULT 0,
            status      TEXT DEFAULT 'pending',
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS banners (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT DEFAULT '',
            image_url   TEXT DEFAULT '',
            link_url    TEXT DEFAULT '',
            sort_order  INTEGER DEFAULT 0,
            active      INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS notices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT NOT NULL,
            content     TEXT DEFAULT '',
            type        TEXT DEFAULT 'info',
            start_date  TEXT DEFAULT '',
            end_date    TEXT DEFAULT '',
            active      INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS operation_logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER,
            action      TEXT,
            target      TEXT,
            detail      TEXT,
            ip          TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS categories (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT UNIQUE NOT NULL,
            icon           TEXT DEFAULT '🏮',
            sort_order     INTEGER DEFAULT 0,
            active         INTEGER DEFAULT 1,
            is_market_type INTEGER DEFAULT 0,
            created_at     TEXT DEFAULT (datetime('now','localtime'))
        );

        CREATE TABLE IF NOT EXISTS sms_codes (
            phone      TEXT PRIMARY KEY,
            code       TEXT NOT NULL,
            expire     REAL NOT NULL,
            sent_at    REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_settings (
            key    TEXT PRIMARY KEY,
            value  TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS market_visits (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id  TEXT NOT NULL,
            user_id    INTEGER,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(market_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS feedbacks (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            type       TEXT DEFAULT 'other',
            content    TEXT NOT NULL,
            contact    TEXT DEFAULT '',
            images     TEXT DEFAULT '[]',
            user_id    INTEGER,
            nickname   TEXT DEFAULT '',
            status     TEXT DEFAULT 'pending',
            reply      TEXT DEFAULT '',
            ip         TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            handled_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS market_reminders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            market_id   TEXT NOT NULL,
            remind_type TEXT NOT NULL DEFAULT 'once',
            status      TEXT NOT NULL DEFAULT 'active',
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            updated_at  TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(user_id, market_id)
        );

        CREATE INDEX IF NOT EXISTS idx_reminder_user   ON market_reminders(user_id);
        CREATE INDEX IF NOT EXISTS idx_reminder_market ON market_reminders(market_id, status);

        CREATE TABLE IF NOT EXISTS ai_collect_logs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            scope           TEXT NOT NULL,
            saved           INTEGER DEFAULT 0,
            total_raw       INTEGER DEFAULT 0,
            providers_json  TEXT DEFAULT '[]',
            comparison_json TEXT DEFAULT '[]',
            created_at      TEXT DEFAULT (datetime('now','localtime'))
        );

        -- spider_queue 扩展字段（若列已存在会被忽略）
        -- 用 executescript 无法 try/catch，改到 init_db() Python 层处理

        -- 默认分类
        INSERT OR IGNORE INTO categories(name,icon,sort_order) VALUES
            ('农村大集','🏮',1),('庙会','🎪',2),
            ('早市','🌅',3),('夜市','🌙',4),
            ('农贸市场','🌾',5),('宠物市场','🐾',6),
            ('古玩市场','🏺',7),('花鸟市场','🌸',8),
            ('二手市集','♻️',9),('美食集市','🍜',10);

        -- 索引
        CREATE INDEX IF NOT EXISTS idx_market_status   ON markets(status);
        CREATE INDEX IF NOT EXISTS idx_market_region   ON markets(region);
        CREATE INDEX IF NOT EXISTS idx_market_category ON markets(category);
        CREATE INDEX IF NOT EXISTS idx_queue_status    ON spider_queue(status);
        CREATE INDEX IF NOT EXISTS idx_fav_user        ON favorites(user_id);
        CREATE INDEX IF NOT EXISTS idx_review_market   ON reviews(market_id);
        CREATE INDEX IF NOT EXISTS idx_review_status   ON reviews(status);
    """)

    # 为已有数据库补充新字段（兼容旧库）
    for col_sql in [
        "ALTER TABLE users ADD COLUMN wx_openid TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN mp_openid TEXT DEFAULT ''",
        "ALTER TABLE spider_queue ADD COLUMN lat REAL",
        "ALTER TABLE spider_queue ADD COLUMN lng REAL",
        "ALTER TABLE spider_queue ADD COLUMN source TEXT DEFAULT ''",
        "ALTER TABLE spider_queue ADD COLUMN rating REAL",
        "ALTER TABLE spider_queue ADD COLUMN fav_count INTEGER DEFAULT 0",
        "ALTER TABLE categories ADD COLUMN default_schedule TEXT DEFAULT 'lunar'",
        "ALTER TABLE categories ADD COLUMN is_market_type INTEGER DEFAULT 0",
        "ALTER TABLE markets ADD COLUMN review_count INTEGER DEFAULT 0",
        "ALTER TABLE markets ADD COLUMN rating REAL",
    ]:
        try: conn.execute(col_sql)
        except: pass

    # 标记默认的"市场类"分类（菜市场/农贸/批发等进入市场区块，不在主列表）
    _market_type_names = ['便民市场', '农贸市场', '批发市场', '菜市场', '农产品市场',
                          '宠物市场', '花鸟市场', '古玩市场', '二手市集', '美食集市',
                          '小吃街', '集市']
    for _n in _market_type_names:
        try:
            conn.execute("UPDATE categories SET is_market_type=1 WHERE name=?", (_n,))
        except: pass

    # 清除虚假5.0评分（0条点评的集市恢复无评分状态）
    try:
        conn.execute("UPDATE markets SET rating=NULL WHERE review_count=0 AND rating=5.0")
    except: pass

    # 预置微信配置（不覆盖已有值；凭证请通过后台管理页面或 .env 配置）
    try:
        import config as _c
        _wx_appid  = _c.WX_APPID
        _wx_secret = _c.WX_SECRET
        _qwen_key  = _c.QWEN_API_KEY
    except Exception:
        _wx_appid  = os.environ.get('WX_APPID',  '')
        _wx_secret = os.environ.get('WX_SECRET', '')
        _qwen_key  = os.environ.get('QWEN_API_KEY', '')
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('wx_appid',?)",  (_wx_appid,))
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('wx_secret',?)", (_wx_secret,))
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('mp_appid','')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('mp_secret','')")
    # 运营地区白名单（默认关闭=全国开放；enabled=true 时只显示白名单内地区的集市）
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('region_whitelist_enabled','false')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('region_whitelist','[\"河北省·保定市\",\"北京市\"]')")
    # 是否在首页展示"附近市场"区块（便民市场/农贸市场等）
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('show_market_section','false')")
    # 通义千问 API Key（用于图片识别导入集市）
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('gemini_api_key',?)", (_qwen_key,))
    # AI 校验接口配置（支持多接口切换）
    # AI 校验接口配置
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('ai_verify_provider','deepseek')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('deepseek_api_key','')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('doubao_api_key','')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('doubao_model','')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('qwen_verify_key','')")
    # 图片识别多接口配置
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('doubao_vision_model','')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('glm_api_key','')")
    conn.execute("INSERT OR IGNORE INTO app_settings(key,value) VALUES('kimi_api_key','')")

    # 初始化管理员账号
    def add_admin(uid, phone, nickname, role, avatar):
        exists = conn.execute("SELECT id FROM users WHERE phone=?", (phone,)).fetchone()
        if not exists:
            conn.execute("""
                INSERT INTO users (uid,phone,nickname,password,role,avatar,bio,status)
                VALUES (?,?,?,?,?,?,?,?)
            """, (uid, phone, nickname, _hash(phone), role, avatar,
                  '平台'+nickname, 'normal'))

    add_admin('ZJS10001','18612116214','总管理员','superadmin','👑')
    add_admin('ZJS10002','17601637515','副管理员','admin','⚙️')
    conn.commit()
    conn.close()
    print("✅ 数据库初始化完成")

# ════════════════════════════════════════════════════════════
# 工具函数
# ════════════════════════════════════════════════════════════

def _parse_tags(raw):
    """兼容 JSON 数组和逗号分隔两种格式的 tags"""
    if not raw:
        return []
    try:
        return json.loads(raw)
    except Exception:
        return [t.strip() for t in raw.split(',') if t.strip()]

def _hash(s):
    return hashlib.sha256(s.encode()).hexdigest()

def _make_token(user_id, role):
    import base64
    payload = json.dumps({
        'user_id': user_id,
        'role': role,
        'exp': (datetime.now() + timedelta(days=7)).isoformat()
    })
    return base64.b64encode(f"{JWT_SECRET}:{payload}".encode()).decode()

def _decode_token(token):
    try:
        import base64
        raw = base64.b64decode(token.encode()).decode()
        prefix = f"{JWT_SECRET}:"
        if not raw.startswith(prefix):
            return None
        payload = json.loads(raw[len(prefix):])
        if datetime.fromisoformat(payload['exp']) < datetime.now():
            return None
        return payload
    except:
        return None

def _get_user_from_token():
    auth = request.headers.get('Authorization', '')
    if not auth.startswith('Bearer '):
        return None
    payload = _decode_token(auth[7:])
    if not payload:
        return None
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE id=?",
                       (payload['user_id'],)).fetchone()
    conn.close()
    return dict(row) if row else None

def _log(action, target='', detail=''):
    try:
        user = _get_user_from_token()
        conn = get_db()
        conn.execute(
            "INSERT INTO operation_logs(user_id,action,target,detail,ip) VALUES(?,?,?,?,?)",
            (user['id'] if user else None, action, target, detail,
             request.remote_addr or '')
        )
        conn.commit()
        conn.close()
    except:
        pass

def ok(data=None, msg='success'):
    return jsonify({'code': 200, 'msg': msg, 'data': data})

def err(msg, code=400):
    return jsonify({'code': code, 'msg': msg}), code

def _generate_uid():
    conn = get_db()
    row = conn.execute("SELECT MAX(id) as max_id FROM users").fetchone()
    conn.close()
    next_id = (row['max_id'] or 0) + 1
    return f'ZJS{10000 + next_id}'

# 鉴权装饰器
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = _get_user_from_token()
        if not user:
            return err('请先登录', 401)
        if user['status'] == 'banned':
            return err('账号已被封禁', 403)
        request.current_user = user
        return f(*args, **kwargs)
    return decorated


def _region_initials(region: str) -> str:
    """将 '河北省·保定市·唐县' 转为拼音首字母，如 'HBBDTX'"""
    try:
        from pypinyin import lazy_pinyin, Style
        suffix = '省市区县镇乡村街道'
        parts = [p for p in region.split('·') if p]
        result = ''
        for part in parts[:3]:
            clean = part.rstrip(suffix)
            if not clean:
                clean = part
            letters = lazy_pinyin(clean, style=Style.FIRST_LETTER)
            result += ''.join(letters).upper()
        return result or 'MK'
    except Exception:
        # pypinyin 未安装时降级用 uuid 前8位
        return ''


def make_market_id(region: str, conn) -> str:
    """根据地区生成形如 HBBDTX0001 的集市 ID"""
    prefix = _region_initials(region)
    if not prefix:
        return str(uuid.uuid4())
    # 查当前该前缀下最大序号
    row = conn.execute(
        "SELECT MAX(CAST(SUBSTR(id, ?) AS INTEGER)) FROM markets WHERE id LIKE ?",
        (len(prefix) + 1, f'{prefix}%')
    ).fetchone()
    seq = (row[0] or 0) + 1
    return f'{prefix}{seq:04d}'


def log_action(user_id, action, target='', detail=''):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO operation_logs(user_id,action,target,detail,ip) VALUES(?,?,?,?,?)",
            (user_id, action, target, detail, request.remote_addr))
        conn.commit()
        conn.close()
    except Exception:
        pass


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 支持旧的 X-Admin-Key 方式（兼容现有后台）
        old_key = request.headers.get('X-Admin-Key', '')
        if hmac.compare_digest(old_key, ADMIN_KEY):
            request.current_user = {'id': 0, 'role': 'superadmin', 'phone': 'system'}
            return f(*args, **kwargs)
        # 新的 JWT 方式
        user = _get_user_from_token()
        if not user or user['role'] not in ('admin', 'superadmin'):
            return err('权限不足', 403)
        request.current_user = user
        return f(*args, **kwargs)
    return decorated

def require_api_secret(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('X-API-Secret', '')
        if not hmac.compare_digest(token, API_SECRET):
            return jsonify({'error': '无效的API密钥'}), 401
        return f(*args, **kwargs)
    return decorated

# ════════════════════════════════════════════════════════════
# 短信接口（保留原有逻辑，略作增强）
# ════════════════════════════════════════════════════════════

def _send_aliyun_sms(phone, code, sign_name, template_code):
    from alibabacloud_dysmsapi20170525.client import Client
    from alibabacloud_tea_openapi import models as open_api_models
    from alibabacloud_dysmsapi20170525 import models as sms_models
    try:
        import config as _c
        _key_id  = _c.ALI_SMS_KEY_ID
        _key_sec = _c.ALI_SMS_KEY_SEC
    except Exception:
        _key_id  = os.environ.get('ALI_SMS_KEY_ID',  '')
        _key_sec = os.environ.get('ALI_SMS_KEY_SEC', '')
    cfg = open_api_models.Config(
        access_key_id=_key_id,
        access_key_secret=_key_sec,
        endpoint='dysmsapi.aliyuncs.com'
    )
    client = Client(cfg)
    req = sms_models.SendSmsRequest(
        phone_numbers=phone, sign_name=sign_name,
        template_code=template_code,
        template_param=f'{{"code":"{code}"}}'
    )
    resp = client.send_sms(req)
    return resp.body.code == 'OK', resp.body.message

@app.route('/api/sms/send', methods=['POST'])
def sms_send():
    data          = request.get_json(force=True) or {}
    phone         = data.get('phone', '').strip()
    sign_name     = data.get('sign_name') or '尊熊'
    template_code = data.get('template_code') or 'SMS_332555688'

    if not phone or not re.match(r'^1[3-9]\d{9}$', phone):
        return jsonify({'success': False, 'message': '手机号格式错误'}), 400

    conn = get_db()
    # 清理过期验证码
    conn.execute("DELETE FROM sms_codes WHERE expire < ?", (time.time(),))
    # 频率限制
    existing = conn.execute("SELECT sent_at FROM sms_codes WHERE phone=?", (phone,)).fetchone()
    if existing and time.time() - existing['sent_at'] < 60:
        conn.close()
        return jsonify({'success': False, 'message': '请60秒后再试'}), 429

    code = str(random.randint(100000, 999999))
    conn.execute(
        "INSERT OR REPLACE INTO sms_codes(phone,code,expire,sent_at) VALUES(?,?,?,?)",
        (phone, code, time.time() + 300, time.time())
    )
    conn.commit()
    conn.close()

    if template_code:
        try:
            ok_sent, msg = _send_aliyun_sms(phone, code, sign_name, template_code)
            if not ok_sent:
                return jsonify({'success': False, 'message': msg})
            return jsonify({'success': True, 'message': '发送成功'})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    # 演示模式：返回验证码（未配置真实短信时使用）
    return jsonify({'success': True, 'message': '发送成功', 'code': code})

@app.route('/api/sms/verify', methods=['POST'])
def sms_verify():
    data  = request.get_json(force=True) or {}
    phone = data.get('phone', '').strip()
    code  = data.get('code', '').strip()
    conn  = get_db()
    record = conn.execute("SELECT * FROM sms_codes WHERE phone=?", (phone,)).fetchone()
    if not record:
        conn.close()
        return jsonify({'success': False, 'message': '验证码不存在或已过期'})
    if time.time() > record['expire']:
        conn.execute("DELETE FROM sms_codes WHERE phone=?", (phone,))
        conn.commit(); conn.close()
        return jsonify({'success': False, 'message': '验证码已过期'})
    if record['code'] != code:
        conn.close()
        return jsonify({'success': False, 'message': '验证码错误'})
    conn.execute("DELETE FROM sms_codes WHERE phone=?", (phone,))
    conn.commit(); conn.close()
    return jsonify({'success': True, 'message': '验证成功'})

# ════════════════════════════════════════════════════════════
# 认证接口（新增）
# ════════════════════════════════════════════════════════════

@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    data       = request.get_json(force=True) or {}
    phone      = data.get('phone', '').strip()
    password   = data.get('password', '').strip()
    sms_code   = data.get('smsCode', '').strip()
    login_type = data.get('type', 'sms')   # sms | password

    if not re.match(r'^1[3-9]\d{9}$', phone):
        return err('手机号格式不正确')

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE phone=?", (phone,)).fetchone()
    user = dict(user) if user else None

    if login_type == 'sms':
        record = conn.execute("SELECT * FROM sms_codes WHERE phone=?", (phone,)).fetchone()
        if not record or record['code'] != sms_code:
            conn.close()
            return err('验证码错误或已过期')
        if time.time() > record['expire']:
            conn.execute("DELETE FROM sms_codes WHERE phone=?", (phone,))
            conn.commit()
            conn.close()
            return err('验证码已过期')
        conn.execute("DELETE FROM sms_codes WHERE phone=?", (phone,))
        conn.commit()
        # 自动注册
        is_new = False
        if not user:
            is_new = True
            uid = _generate_uid()
            conn.execute("""
                INSERT INTO users(uid,phone,nickname,password,role,avatar,status)
                VALUES(?,?,?,?,?,?,?)
            """, (uid, phone, f'用户{phone[-4:]}', '', 'user', '👤', 'normal'))
            conn.commit()
            user = dict(conn.execute(
                "SELECT * FROM users WHERE phone=?", (phone,)).fetchone())
    else:
        if not user:
            conn.close()
            return err('账号不存在')
        if user['password'] != _hash(password):
            conn.close()
            return err('密码错误')

    if user['status'] == 'banned':
        conn.close()
        return err('账号已被封禁', 403)

    conn.execute("UPDATE users SET last_login=? WHERE id=?",
                 (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user['id']))
    conn.commit()

    # 附带收藏列表
    favs = conn.execute(
        "SELECT market_id FROM favorites WHERE user_id=?", (user['id'],)).fetchall()
    fav_list = [r['market_id'] for r in favs]
    conn.close()

    token = _make_token(user['id'], user['role'])
    _log('login', f"user:{user['id']}")

    user_data = {
        'id': user['id'], 'uid': user['uid'], 'phone': user['phone'],
        'nickname': user['nickname'], 'avatar': user['avatar'],
        'bio': user['bio'], 'role': user['role'], 'gender': user['gender'],
        'region': user['region'], 'status': user['status'],
        'birthYear': user['birth_year'] if 'birth_year' in user.keys() else '',
        'interests': json.loads(user['interests'] if 'interests' in user.keys() and user['interests'] else '[]'),
        'favList': fav_list,
        'createdAt': user['created_at'],
    }
    return ok({'token': token, 'user': user_data, 'isNew': is_new if login_type == 'sms' else False})


def _get_setting(conn, key, default=''):
    row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

@app.route('/api/auth/wx-oauth', methods=['POST'])
def wx_oauth():
    """微信网页授权登录（服务号H5）"""
    import urllib.request as _urllib
    data   = request.get_json(force=True) or {}
    code   = data.get('code', '').strip()
    if not code:
        return err('缺少code参数')
    conn   = get_db()
    appid  = _get_setting(conn, 'wx_appid')
    secret = _get_setting(conn, 'wx_secret')
    conn.close()
    if not appid or not secret:
        return err('微信登录未配置，请在后台设置AppID和AppSecret')
    # 1. code换取access_token + openid
    token_url = (f'https://api.weixin.qq.com/sns/oauth2/access_token'
                 f'?appid={appid}&secret={secret}&code={code}&grant_type=authorization_code')
    try:
        with _urllib.urlopen(token_url, timeout=10) as r:
            td = json.loads(r.read().decode())
    except Exception as e:
        return err(f'微信接口请求失败: {e}')
    if 'errcode' in td:
        return err(f'微信授权失败: {td.get("errmsg","")}')
    openid       = td.get('openid', '')
    access_token = td.get('access_token', '')
    if not openid:
        return err('获取openid失败')
    # 2. 获取用户信息
    info_url = (f'https://api.weixin.qq.com/sns/userinfo'
                f'?access_token={access_token}&openid={openid}&lang=zh_CN')
    try:
        with _urllib.urlopen(info_url, timeout=10) as r:
            ui = json.loads(r.read().decode('utf-8'))
    except Exception:
        ui = {}
    # 验证昵称合法性：微信昵称通常含中文或特殊字符
    # 若只含 ASCII 字母数字（像 token/openid/session_key），视为无效昵称
    _nick_raw = (ui.get('nickname') or '').strip()
    import re as _re
    if not _nick_raw or _re.match(r'^[A-Za-z0-9_\-]{10,}$', _nick_raw):
        _nick_raw = ''
    nickname   = _nick_raw or f'微信用户{openid[-4:]}'
    avatar_url = ui.get('headimgurl') or '👤'
    # 3. 查找或创建用户
    conn  = get_db()
    user  = conn.execute("SELECT * FROM users WHERE wx_openid=?", (openid,)).fetchone()
    is_new = False
    if not user:
        is_new  = True
        uid     = _generate_uid()
        phone_p = f'wx_{openid}'   # 微信用户手机号占位，可后续绑定
        # 若同手机已存在（极少情况），直接关联openid
        exist_phone = conn.execute("SELECT id FROM users WHERE phone=?", (phone_p,)).fetchone()
        if not exist_phone:
            conn.execute("""
                INSERT INTO users(uid,phone,nickname,password,role,avatar,status,wx_openid,created_at)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (uid, phone_p, nickname, '', 'user', avatar_url, 'normal', openid,
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
        user = dict(conn.execute("SELECT * FROM users WHERE wx_openid=?", (openid,)).fetchone())
    else:
        user = dict(user)
        conn.execute("UPDATE users SET nickname=?,avatar=?,last_login=? WHERE wx_openid=?",
                     (nickname, avatar_url, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), openid))
        conn.commit()
    favs     = conn.execute("SELECT market_id FROM favorites WHERE user_id=?", (user['id'],)).fetchall()
    fav_list = [r['market_id'] for r in favs]
    conn.close()
    token = _make_token(user['id'], user['role'])
    return ok({
        'token': token, 'isNew': is_new,
        'user': {
            'id': user['id'], 'uid': user.get('uid',''),
            'phone': '' if (user.get('phone','') or '').startswith('wx_') else user.get('phone',''), 'nickname': nickname,
            'avatar': avatar_url, 'role': user.get('role','user'),
            'bio': user.get('bio',''), 'region': user.get('region',''),
            'status': user.get('status','normal'), 'favList': fav_list,
        }
    })


@app.route('/api/admin/settings/oauth', methods=['GET'])
@admin_required
def get_oauth_settings():
    conn = get_db()
    keys = ['wx_appid','wx_secret','mp_appid','mp_secret']
    cfg  = {k: _get_setting(conn, k) for k in keys}
    conn.close()
    # secret脱敏显示
    for k in ['wx_secret','mp_secret']:
        if cfg[k]: cfg[k+'_masked'] = cfg[k][:6] + '****' + cfg[k][-4:]
    return ok(cfg)


@app.route('/api/admin/settings/oauth', methods=['POST'])
@admin_required
def save_oauth_settings():
    data = request.get_json(force=True) or {}
    conn = get_db()
    for key in ['wx_appid','wx_secret','mp_appid','mp_secret']:
        if key in data:
            conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES(?,?)",
                         (key, data[key].strip()))
    conn.commit()
    conn.close()
    return ok({'msg': '保存成功'})


@app.route('/api/admin/settings/regions', methods=['GET'])
@admin_required
def get_region_settings():
    conn = get_db()
    enabled = _get_setting(conn, 'region_whitelist_enabled', 'false') == 'true'
    try:
        regions = json.loads(_get_setting(conn, 'region_whitelist', '[]'))
    except: regions = []
    conn.close()
    return ok({'enabled': enabled, 'regions': regions})

@app.route('/api/admin/settings/regions', methods=['POST'])
@admin_required
def save_region_settings():
    data = request.get_json(force=True) or {}
    conn = get_db()
    if 'enabled' in data:
        conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('region_whitelist_enabled',?)",
                     ('true' if data['enabled'] else 'false',))
    if 'regions' in data:
        conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('region_whitelist',?)",
                     (json.dumps(data['regions'], ensure_ascii=False),))
    conn.commit()
    conn.close()
    return ok({'msg': '保存成功'})


@app.route('/api/regions/open-cities', methods=['GET'])
def get_open_cities():
    """返回所有有已发布集市数据的城市列表（自动开通逻辑，无需手动配置）"""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT region FROM markets WHERE status='published' AND region IS NOT NULL AND region != ''"
    ).fetchall()
    conn.close()
    direct = {'北京市', '上海市', '天津市', '重庆市'}
    cities = set()
    for (region,) in rows:
        parts = [p.strip() for p in region.split('·') if p.strip()]
        if not parts:
            continue
        if parts[0] in direct:
            cities.add(parts[0])
        elif len(parts) >= 2:
            cities.add(parts[1])
        else:
            cities.add(parts[0])
    return jsonify({'code': 200, 'data': sorted(cities)})


@app.route('/api/admin/region-map-stats', methods=['GET'])
@admin_required
def admin_region_map_stats():
    """返回各省已发布集市数量，供后台地图可视化"""
    conn = get_db()
    rows = conn.execute("""
        SELECT
            CASE WHEN region LIKE '%·%'
                 THEN substr(region, 1, instr(region,'·')-1)
                 ELSE region END AS prov,
            COUNT(*) AS cnt
        FROM markets
        WHERE status='published' AND region != '' AND region IS NOT NULL
        GROUP BY prov ORDER BY cnt DESC
    """).fetchall()
    conn.close()
    return jsonify({'code': 200, 'data': [{'prov': r[0], 'count': r[1]} for r in rows]})


@app.route('/api/admin/districts', methods=['GET'])
@admin_required
def admin_get_districts():
    """代理高德区县查询，避免前端跨域问题"""
    import urllib.request, urllib.parse
    city = request.args.get('city', '').strip()
    if not city:
        return jsonify(code=400, message='city required')
    conn = get_db()
    key_row = conn.execute("SELECT value FROM settings WHERE key='amap_ws_key'").fetchone()
    conn.close()
    key = (key_row[0] if key_row else '') or '4f51ff7eb37bec522a9278847a44d2f0'
    url = (f'https://restapi.amap.com/v3/config/district'
           f'?keywords={urllib.parse.quote(city)}&subdistrict=1&key={key}&extensions=base')
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        districts = []
        if data.get('status') == '1' and data.get('districts'):
            for d in data['districts'][0].get('districts', []):
                if d.get('name'):
                    districts.append(d['name'])
        return jsonify(code=200, data=districts)
    except Exception as e:
        return jsonify(code=500, message=str(e), data=[])


@app.route('/api/admin/ai/verify-market', methods=['POST'])
@admin_required
def ai_verify_market():
    """用AI核实集市是否真实存在、开集时间是否正确，并用高德重新geocode坐标"""
    try:
        import requests as req_lib
    except ImportError:
        return jsonify(code=500, msg='缺少requests库')

    data       = request.get_json() or {}
    name       = data.get('name', '').strip()
    category   = data.get('category', '').strip()
    address    = data.get('address', '').strip()
    region     = data.get('region', '').strip()
    open_time  = data.get('open_time', '').strip()

    if not name:
        return jsonify(code=400, msg='缺少集市名称')

    conn = get_db()
    def _k(key): return _get_setting(conn, key, '')
    # 优先使用豆包，其次 Kimi → DeepSeek → 通义千问 → GLM
    ai_cfg = None
    if _k('doubao_api_key') and _k('doubao_model'):
        ai_cfg = ('豆包', 'https://ark.cn-beijing.volces.com/api/v3/chat/completions',
                  _k('doubao_api_key'), _k('doubao_model'))
    elif _k('kimi_api_key'):
        ai_cfg = ('Kimi', 'https://api.moonshot.cn/v1/chat/completions', _k('kimi_api_key'), 'moonshot-v1-8k')
    elif _k('deepseek_api_key'):
        ai_cfg = ('DeepSeek', 'https://api.deepseek.com/chat/completions', _k('deepseek_api_key'), 'deepseek-chat')
    elif _k('gemini_api_key'):
        ai_cfg = ('通义千问', 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions', _k('gemini_api_key'), 'qwen-plus')
    elif _k('glm_api_key'):
        ai_cfg = ('智谱GLM', 'https://open.bigmodel.cn/api/paas/v4/chat/completions', _k('glm_api_key'), 'glm-4-flash')
    amap_key = _k('amap_ws_key') or '4f51ff7eb37bec522a9278847a44d2f0'

    # ── 本地查重 ──────────────────────────────────────────────────
    import re as _re2
    def _norm_name(s):
        return _re2.sub(r'[\s　]*(大集|集市|庙会|集|会|早市|夜市|农贸市场)$', '', s.strip())

    norm_input = _norm_name(name)
    region_key = region.replace('·', ' ').split()[-1] if region else ''
    dups = []
    if region_key:
        rows = conn.execute(
            "SELECT id, name, region, open_time, status FROM markets "
            "WHERE status != 'deleted' AND region LIKE ? LIMIT 200",
            (f'%{region_key}%',)
        ).fetchall()
        for r in rows:
            rid, rname, rregion, rot, rstatus = r[0], r[1], r[2], r[3] or '', r[4]
            if _norm_name(rname) == norm_input or rname == name:
                try:
                    ot_obj = json.loads(rot)
                    ot_text = (ot_obj.get('text') or ot_obj.get('custom') or rot)[:30]
                except Exception:
                    ot_text = rot[:30]
                dups.append({'id': rid, 'name': rname,
                             'region': rregion, 'open_time': ot_text, 'status': rstatus})
    conn.close()

    if not ai_cfg:
        return jsonify(code=400, msg='未配置AI API Key')

    # ── 高德 geocoding ────────────────────────────────────────────
    def _geocode(query):
        import urllib.request, urllib.parse
        url = (f'https://restapi.amap.com/v3/geocode/geo'
               f'?address={urllib.parse.quote(query)}&key={amap_key}&output=json')
        try:
            req2 = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req2, timeout=8) as resp:
                d = json.loads(resp.read())
            if d.get('status') == '1' and d.get('geocodes'):
                geo = d['geocodes'][0]
                loc = geo.get('location', '')
                if loc and ',' in loc:
                    lng_s, lat_s = loc.split(',', 1)
                    return float(lat_s), float(lng_s), geo.get('level', ''), geo.get('formatted_address', '')
        except Exception:
            pass
        return None

    # 精确度优先：全名+地址 → 名称+地区 → 纯地址
    region_clean = region.replace('·', '').replace('省', '省').replace('市', '市')
    geo_result = (_geocode(name + ' ' + (address or region_clean)) or
                  _geocode(name + ' ' + region_clean) or
                  _geocode((address or '') + ' ' + region_clean) or
                  _geocode(region_clean + (address or '')))
    # 只接受精确到乡镇/村庄/兴趣点级别的结果
    _precise_levels = {'村庄','兴趣点','门牌号','小区','楼栋','单元','房间号','道路','街道','乡镇','工厂'}
    new_lat = new_lng = geo_addr = geo_level = None
    if geo_result:
        new_lat, new_lng, geo_level, geo_addr = geo_result
        if geo_level not in _precise_levels:
            new_lat = new_lng = None  # 精度不够，不推荐

    # ── 日期缩写等价检测（逢四逢九 = 初四初九十四十九二十四二十九）─────
    _ABBR_MAP = {
        frozenset([1,6,11,16,21,26]): ['逢一逢六','逢一、六','逢一六'],
        frozenset([2,7,12,17,22,27]): ['逢二逢七','逢二、七','逢二七'],
        frozenset([3,8,13,18,23,28]): ['逢三逢八','逢三、八','逢三八'],
        frozenset([4,9,14,19,24,29]): ['逢四逢九','逢四、九','逢四九'],
        frozenset([5,10,15,20,25,30]): ['逢五逢十','逢五、十','逢五十'],
    }
    def _abbr_days(s):
        for days_set, patterns in _ABBR_MAP.items():
            if any(p in s for p in patterns):
                return days_set
        return None

    # ── AI 核实 ───────────────────────────────────────────────────
    _daily_cats = {'早市', '夜市', '农贸市场', '早集', '夜集', '菜市场'}
    _is_daily = category in _daily_cats or bool(
        open_time and any(k in open_time for k in ['每天','早晨','早上','傍晚','上午','下午','晚上','凌晨',':00']))
    if _is_daily:
        _date_rule = ('  - 每日营业格式（早市/夜市/农贸市场）：每天HH:MM-HH:MM\n'
                      '    时间段格式即为正确，无需改成农历格式，date_ok填true')
    else:
        _date_rule = (
            '  - 农村大集：农历每月初X、初X……或"逢四逢九""逢一逢六"等缩写均为合法格式，date_ok填true\n'
            '  - 庙会：农历X月X日（如"农历三月十五"）\n'
            '  - 待核实：若完全不知道，填"待核实"\n'
            '  - 注意："逢四逢九"与"农历每月初四、初九、十四、十九、二十四、二十九"完全等价，都是正确的，不需要修正'
        )

    prompt = f"""你是一个中国农村集市核实助手。请根据你已有的知识核实以下集市信息。

集市名称：{name}
分类：{category}
地址：{address or region}
开集时间：{open_time or '（未填写）'}

【严格要求，必须遵守】
1. exists字段：
   - 只有当你对该集市有明确了解时才填"yes"
   - 名称/地址含糊、或该地区你没有具体知识时，必须填"uncertain"，绝对不能猜测填"yes"
   - 确定不存在或明显是AI编造的填"no"

2. date_ok和date_fix字段：
   - 只有当你明确知道该集市的正确开集时间，且与填写的时间不符时，才填date_ok=false并给出date_fix
   - 如果你不确定正确的开集时间，即使开集时间看起来奇怪，也必须填date_ok=true，date_fix原样返回
   - 绝对不能根据猜测或"该地区常见规律"来修改开集日期，那样会产生错误数据
   - 格式检查规则：
{_date_rule}

3. reason字段：简明说明判断依据，如果是uncertain请说明原因

只输出JSON，不要任何说明文字：
{{"exists":"yes/no/uncertain","reason":"一句话说明理由","date_ok":true或false,"date_fix":"如date_ok为false且你确实知道正确日期才填，否则原样返回open_time"}}"""

    try:
        r = req_lib.post(ai_cfg[1],
            headers={'Authorization': f'Bearer {ai_cfg[2]}', 'Content-Type': 'application/json'},
            json={'model': ai_cfg[3], 'messages': [{'role': 'user', 'content': prompt}], 'temperature': 0.1},
            timeout=(10, 30))
        r.raise_for_status()
        text = r.json()['choices'][0]['message']['content'].strip()
        import re as _re
        m2 = _re.search(r'\{[\s\S]*?\}', text)
        result = json.loads(m2.group()) if m2 else {
            'exists': 'uncertain', 'reason': text[:100], 'date_ok': True, 'date_fix': open_time
        }

        # 后处理：缩写等价 → 强制 date_ok=True
        if not result.get('date_ok') and open_time:
            orig_days = _abbr_days(open_time)
            fix_days  = _abbr_days(result.get('date_fix', ''))
            if orig_days and (orig_days == fix_days or orig_days == _abbr_days(result.get('date_fix','') + open_time)):
                result['date_ok'] = True
                result['date_fix'] = open_time

        resp = dict(code=200, ai=ai_cfg[0], **result)
        if new_lat and new_lng:
            resp['new_lat']    = round(new_lat, 6)
            resp['new_lng']    = round(new_lng, 6)
            resp['geo_level']  = geo_level
            resp['geo_addr']   = geo_addr
        if dups:
            resp['dups'] = dups
        return jsonify(**resp)
    except Exception as e:
        return jsonify(code=500, msg=str(e)[:120])


@app.route('/api/admin/ai/test-key', methods=['POST'])
@admin_required
def ai_test_key():
    """测试 AI API Key 是否有效"""
    try:
        import requests as req_lib
    except ImportError:
        return jsonify(code=500, msg='缺少 requests 库')

    data     = request.get_json() or {}
    provider = data.get('provider', '').strip()
    key      = data.get('key', '').strip()
    model    = data.get('model', '').strip()

    if not key:
        return jsonify(code=400, ok=False, msg='Key 为空')

    _PROVIDER_URLS = {
        'qwen':     ('https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions', 'qwen-turbo'),
        'doubao':   ('https://ark.cn-beijing.volces.com/api/v3/chat/completions', model),
        'glm':      ('https://open.bigmodel.cn/api/paas/v4/chat/completions', 'glm-4-flash'),
        'kimi':     ('https://api.moonshot.cn/v1/chat/completions', 'moonshot-v1-8k'),
        'deepseek': ('https://api.deepseek.com/chat/completions', 'deepseek-chat'),
    }

    if provider not in _PROVIDER_URLS:
        return jsonify(code=400, ok=False, msg=f'未知接口：{provider}')

    url, mdl = _PROVIDER_URLS[provider]
    if not mdl:
        return jsonify(code=400, ok=False, msg='豆包需要填写 Endpoint Model ID')

    try:
        resp = req_lib.post(url,
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={'model': mdl,
                  'messages': [{'role': 'user', 'content': '你好，回复"OK"两个字即可'}],
                  'max_tokens': 10},
            timeout=15)
        if resp.status_code == 200:
            reply = resp.json().get('choices', [{}])[0].get('message', {}).get('content', '')
            return jsonify(code=200, ok=True, msg=f'连接成功，模型回复：{reply[:30]}')
        elif resp.status_code in (401, 403):
            return jsonify(code=200, ok=False, msg='Key 无效或已过期（鉴权失败）')
        elif resp.status_code == 429:
            return jsonify(code=200, ok=False, msg='请求频率超限，Key 有效但额度不足')
        else:
            try:
                err = resp.json().get('error', {}).get('message', resp.text[:80])
            except Exception:
                err = resp.text[:80]
            return jsonify(code=200, ok=False, msg=f'HTTP {resp.status_code}：{err}')
    except Exception as e:
        return jsonify(code=200, ok=False, msg=f'连接超时或网络错误：{str(e)[:80]}')


@app.route('/api/admin/ai/get-townships', methods=['POST'])
@admin_required
def ai_get_townships():
    """多路并发查询乡镇列表，取并集，减少遗漏"""
    try:
        import requests as req_lib
        import re as _re
        from concurrent.futures import ThreadPoolExecutor, as_completed
    except ImportError as e:
        return jsonify(code=500, msg=f'缺少依赖库: {e}')

    data     = request.get_json() or {}
    province = data.get('province', '').strip()
    city     = data.get('city', '').strip()
    county   = data.get('county', '').strip()
    if not county:
        return jsonify(code=400, msg='请填写县/区名称')

    conn    = get_db()
    api_key = _get_setting(conn, 'gemini_api_key', '')
    conn.close()
    if not api_key:
        return jsonify(code=400, msg='请先在系统设置中配置通义千问 API Key')

    location = f'{province}{city}{county}'

    # 三条不同角度的提示词，并发请求后取并集
    prompts = [
        (
            f'请列出【{location}】截至2024年底所有在册的乡镇、街道、乡的完整名单。'
            f'严格按照民政部行政区划，不要遗漏任何一个。'
            f'只输出JSON数组，每个元素是乡镇名字符串，不含其他文字。'
            f'示例：["XX镇","XX乡","XX街道"]'
        ),
        (
            f'【{location}】的行政区划中，所有镇、乡、街道的名称是什么？'
            f'请逐一列出，包括偏远山区乡镇，不要只列主要的。'
            f'返回格式：只输出JSON数组，如["北店头镇","都亭乡","庆都山街道"]，不要其他内容。'
        ),
        (
            f'中国{location}下辖哪些乡镇级行政区划（包括镇、乡、民族乡、街道办事处）？'
            f'请给出完整列表，尤其不要遗漏名字相近或偏僻的乡镇。'
            f'仅输出JSON数组格式，每项为乡镇名称字符串。'
        ),
    ]

    def _call(prompt):
        try:
            resp = req_lib.post(
                'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json={'model': 'qwen-plus', 'messages': [{'role': 'user', 'content': prompt}], 'temperature': 0.2},
                timeout=30
            )
            resp.raise_for_status()
            text = resp.json()['choices'][0]['message']['content'].strip()
            m = _re.search(r'\[.*?\]', text, _re.DOTALL)
            if m:
                lst = json.loads(m.group())
                return [t.strip() for t in lst if isinstance(t, str) and t.strip()]
        except Exception:
            pass
        return []

    # 并发执行三路请求
    all_names = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(_call, p) for p in prompts]
        for f in as_completed(futures):
            all_names.extend(f.result())

    if not all_names:
        return jsonify(code=500, msg='AI查询失败，请稍后重试')

    # 去重：忽略后缀差异（"王京镇"和"王京"算同一个）
    seen, result = set(), []
    for name in all_names:
        key = name.rstrip('镇乡街道办')
        if key not in seen:
            seen.add(key)
            result.append(name)

    # 按首字拼音排序（简单按unicode，够用）
    result.sort()
    return jsonify(code=200, data=result, county=county, sources=3)


@app.route('/api/auth/signup', methods=['POST'])
def user_signup():
    """用户自助注册"""
    d        = request.get_json(force=True) or {}
    phone    = (d.get('phone') or '').strip()
    sms_code = (d.get('smsCode') or '').strip()
    nickname = (d.get('nickname') or '').strip()
    password = (d.get('password') or '').strip()
    if not re.match(r'^1[3-9]\d{9}$', phone):
        return err('手机号格式不正确')
    conn = get_db()
    record = conn.execute("SELECT * FROM sms_codes WHERE phone=?", (phone,)).fetchone()
    if not record or record['code'] != sms_code:
        conn.close()
        return err('验证码错误或已过期')
    if time.time() > record['expire']:
        conn.execute("DELETE FROM sms_codes WHERE phone=?", (phone,))
        conn.commit(); conn.close()
        return err('验证码已过期')
    conn.execute("DELETE FROM sms_codes WHERE phone=?", (phone,))
    conn.commit()
    user = conn.execute("SELECT * FROM users WHERE phone=?", (phone,)).fetchone()
    if user:
        conn.close()
        return err('该手机号已注册，请直接登录')
    if not nickname or len(nickname) < 1:
        nickname = f'用户{phone[-4:]}'
    uid    = _generate_uid()
    hashed = _hash(password) if password else ''
    conn.execute(
        'INSERT INTO users(uid,phone,nickname,password,role,avatar,status,created_at) VALUES(?,?,?,?,?,?,?,?)',
        (uid, phone, nickname, hashed, 'user', '👤', 'normal',
         datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    user = dict(conn.execute("SELECT * FROM users WHERE phone=?", (phone,)).fetchone())
    conn.close()
    token = _make_token(user['id'], user['role'])
    return ok({'token': token, 'user': {
        'id': user['id'], 'uid': user['uid'], 'phone': phone,
        'nickname': nickname, 'avatar': '👤', 'role': 'user',
        'bio': '', 'gender': '', 'region': '', 'status': 'normal',
        'favList': [], 'createdAt': user['created_at'],
    }})

@app.route('/api/auth/register', methods=['POST'])
@admin_required
def register_user():
    """管理员直接注册用户"""
    d        = request.get_json() or {}
    phone    = (d.get('phone') or '').strip()
    nickname = (d.get('nickname') or '').strip()
    password = d.get('password') or phone
    role     = d.get('role') or 'user'
    if role not in ('user', 'admin'): role = 'user'
    if not phone or not re.match(r'^1[3-9]\d{9}$', phone):
        return err('手机号格式不正确')
    conn = get_db()
    if conn.execute('SELECT id FROM users WHERE phone=?', (phone,)).fetchone():
        return err('该手机号已注册')
    if not nickname:
        nickname = f'用户{phone[-4:]}'
    uid    = f'ZJS{int(time.time()) % 100000:05d}'
    hashed = hashlib.sha256(password.encode()).hexdigest()
    conn.execute(
        'INSERT INTO users(uid,phone,nickname,password,role,avatar,status,created_at) VALUES(?,?,?,?,?,?,?,?)',
        (uid, phone, nickname, hashed, role, '⚙️' if role == 'admin' else '👤', 'normal',
         datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    row = conn.execute('SELECT * FROM users WHERE phone=?', (phone,)).fetchone()
    return ok({'id': row['id'], 'uid': row['uid'], 'phone': phone,
               'nickname': nickname, 'role': role})

@app.route('/api/auth/me', methods=['GET'])
@login_required
def auth_me():
    u = request.current_user
    conn = get_db()
    favs = conn.execute(
        "SELECT market_id FROM favorites WHERE user_id=?", (u['id'],)).fetchall()
    fav_list = [r['market_id'] for r in favs]
    review_count  = conn.execute(
        "SELECT COUNT(*) FROM reviews WHERE user_id=?", (u['id'],)).fetchone()[0]
    publish_count = conn.execute(
        "SELECT COUNT(*) FROM markets WHERE source='manual' AND status!='hidden'").fetchone()[0]
    conn.close()
    return ok({
        **{k: u[k] for k in ('id','uid','phone','nickname','avatar',
                              'bio','role','gender','region','status')},
        'favList': fav_list,
        'favCount': len(fav_list),
        'reviewCount': review_count,
        'publishCount': publish_count,
    })


@app.route('/api/auth/update-profile', methods=['POST'])
@login_required
def update_profile():
    u    = request.current_user
    data = request.get_json(force=True) or {}
    allowed = ('nickname', 'bio', 'gender', 'avatar', 'email', 'region', 'birth_year')
    fields  = {k: v for k, v in data.items() if k in allowed}
    if 'interests' in data:
        fields['interests'] = json.dumps(data['interests'], ensure_ascii=False) if isinstance(data['interests'], list) else data['interests']
    if 'nickname' in fields:
        nick = fields['nickname'].strip()
        if not nick or len(nick) > 20:
            return err('昵称长度1-20字')
        fields['nickname'] = nick
    if not fields:
        return err('无有效字段')
    sets   = ', '.join(f"{k}=?" for k in fields)
    params = list(fields.values()) + [u['id']]
    conn   = get_db()
    conn.execute(f"UPDATE users SET {sets} WHERE id=?", params)
    conn.commit()
    row = conn.execute("SELECT * FROM users WHERE id=?", (u['id'],)).fetchone()
    conn.close()
    return ok({k: row[k] for k in ('id','uid','phone','nickname','avatar',
                                    'bio','role','gender','region','status')})

# ════════════════════════════════════════════════════════════
# 集市公开接口（保留原有，扩展字段）
# ════════════════════════════════════════════════════════════

@app.route('/api/markets', methods=['GET'])
def list_markets():
    region   = request.args.get('region')
    category = request.args.get('category') or request.args.get('cat')
    keyword  = request.args.get('keyword')  or request.args.get('kw')
    page     = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 20)), 1000)

    conn   = get_db()
    # 只返回分类处于启用状态的集市（active=0 的分类不展示）
    sql    = """SELECT * FROM markets WHERE status='published'
                AND (category IS NULL OR category NOT IN
                     (SELECT name FROM categories WHERE active=0))"""
    params = []

    # 运营地区白名单过滤
    wl_enabled = _get_setting(conn, 'region_whitelist_enabled', 'false') == 'true'
    if wl_enabled:
        try:
            wl = json.loads(_get_setting(conn, 'region_whitelist', '[]'))
        except: wl = []
        if wl:
            conds = ' OR '.join(['region LIKE ?' for _ in wl])
            sql += f' AND ({conds})'
            params += [f'%{r}%' for r in wl]

    if region:   sql += " AND region LIKE ?";                params.append(f'%{region}%')
    if category: sql += " AND category=?";                   params.append(category)
    if keyword:  sql += " AND (name LIKE ? OR address LIKE ?)"; params += [f'%{keyword}%']*2

    total = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY rating DESC, created_at DESC LIMIT {per_page} OFFSET {(page-1)*per_page}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()

    markets = []
    for r in rows:
        d = dict(r)
        d['tags'] = _parse_tags(d.get('tags'))
        try:
            ot = json.loads(d.get('open_time') or '{}')
            d['openTime'] = ot if isinstance(ot, dict) else {'type':'custom','custom':str(ot)}
        except:
            d['openTime'] = {'type': 'custom', 'custom': d.get('open_time', '')}
        markets.append(d)

    return jsonify({'code': 200, 'data': {'list': markets, 'total': total, 'page': page},
                    # 兼容旧格式
                    'markets': markets, 'total': total})


@app.route('/api/markets', methods=['POST'])
@login_required
def create_market():
    """前台用户发布市集（status=pending，等待管理员审核）"""
    data     = request.get_json(force=True) or {}
    user     = request.current_user
    name     = (data.get('name') or '').strip()
    if not name:
        return jsonify({'code': 400, 'msg': '集市名称不能为空'}), 400

    icon_map  = {'早市':'🌅','集市':'🏮','夜市':'🌙','农贸市场':'🌾',
                 '宠物市场':'🐾','古玩市场':'🏺','花鸟市场':'🌸',
                 '二手集市':'♻️','美食集市':'🍜','跳蚤市场':'🎪'}
    cat  = data.get('category') or data.get('cat') or '集市'
    icon = icon_map.get(cat, '🏮')

    conn = get_db()
    market_id = make_market_id(data.get('region', ''), conn)
    conn.execute(
        """INSERT INTO markets
               (id,name,category,address,region,open_time,phone,tags,description,
                lat,lng,icon,bg,source,status,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'linear-gradient(135deg,#1A3A7A,#2B5BA8)',
                   'user','pending',?)
        """,
        (market_id, name, cat,
         data.get('address',''), data.get('region',''),
         data.get('open_time',''), data.get('phone',''),
         json.dumps(data.get('tags') or []),
         data.get('description',''),
         data.get('lat'), data.get('lng'),
         icon,
         user['id'])
    )
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '提交成功，等待审核', 'data': {'id': market_id}})


@app.route('/api/my/markets', methods=['GET'])
@login_required
def my_markets():
    user = request.current_user
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM markets WHERE created_by=? ORDER BY created_at DESC",
        (user['id'],)).fetchall()
    conn.close()
    items = []
    for r in rows:
        d = dict(r)
        d['tags'] = _parse_tags(d.get('tags'))
        try:
            d['open_time'] = json.loads(d.get('open_time') or '{}')
        except Exception:
            pass
        items.append(d)
    return ok(items)

@app.route('/api/markets/<market_id>', methods=['PUT'])
@login_required
def update_my_market(market_id):
    user = request.current_user
    conn = get_db()
    row = conn.execute("SELECT * FROM markets WHERE id=? AND created_by=?",
                       (market_id, user['id'])).fetchone()
    if not row:
        conn.close()
        return err('集市不存在或无权修改', 403)
    if row['status'] not in ('pending', 'rejected'):
        conn.close()
        return err('只能修改审核中或已拒绝的集市', 400)
    data = request.get_json(force=True) or {}
    allowed = ('name','category','address','region','phone','description','tags','open_time','lat','lng')
    fields = {k: v for k, v in data.items() if k in allowed}
    if 'tags' in fields:
        fields['tags'] = json.dumps(fields['tags'], ensure_ascii=False)
    if 'open_time' in fields and not isinstance(fields['open_time'], str):
        fields['open_time'] = json.dumps(fields['open_time'], ensure_ascii=False)
    fields['status'] = 'pending'  # 重新提交变回待审核
    fields['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sets = ', '.join(f"{k}=?" for k in fields)
    params = list(fields.values()) + [market_id]
    conn.execute(f"UPDATE markets SET {sets} WHERE id=?", params)
    conn.commit()
    conn.close()
    return ok({'msg': '已更新，等待重新审核'})

@app.route('/api/markets/<market_id>/coords', methods=['PATCH'])
def patch_market_coords(market_id):
    """前端 geocoding 完成后回存坐标（只在坐标为空时写入，防止覆盖精确数据）"""
    data = request.get_json(force=True) or {}
    try:
        lat = float(data.get('lat') or 0)
        lng = float(data.get('lng') or 0)
    except (TypeError, ValueError):
        return jsonify({'code': 400, 'msg': 'invalid coords'}), 400
    if not lat or not lng:
        return jsonify({'code': 400, 'msg': 'missing coords'}), 400
    conn = get_db()
    # 只在原始坐标为空时写入，避免覆盖人工填入的精确坐标
    conn.execute(
        "UPDATE markets SET lat=?, lng=? WHERE id=? AND (lat IS NULL OR lat=0 OR lng IS NULL OR lng=0)",
        (lat, lng, market_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'code': 200})


@app.route('/api/markets/<market_id>', methods=['DELETE'])
@login_required
def delete_my_market(market_id):
    user = request.current_user
    conn = get_db()
    row = conn.execute("SELECT * FROM markets WHERE id=? AND created_by=?",
                       (market_id, user['id'])).fetchone()
    if not row:
        conn.close()
        return err('集市不存在或无权删除', 403)
    if row['status'] == 'published':
        conn.close()
        return err('已发布的集市不能删除，请联系管理员', 400)
    conn.execute("DELETE FROM markets WHERE id=?", (market_id,))
    conn.commit()
    conn.close()
    return ok({'msg': '已撤回'})

@app.route('/api/markets/<market_id>', methods=['GET'])
def get_market(market_id):
    conn = get_db()
    row  = conn.execute(
        "SELECT * FROM markets WHERE id=? AND status='published'", (market_id,)).fetchone()
    conn.close()
    if not row:
        return err('未找到', 404)
    d = dict(row)
    d['tags'] = json.loads(d.get('tags') or '[]')
    return ok(d)


@app.route('/api/stats', methods=['GET'])
def stats():
    conn = get_db()
    published = conn.execute(
        "SELECT COUNT(*) FROM markets WHERE status='published'").fetchone()[0]
    pending   = conn.execute(
        "SELECT COUNT(*) FROM spider_queue WHERE status='pending'").fetchone()[0]
    categories = conn.execute(
        "SELECT category, COUNT(*) as cnt FROM markets "
        "WHERE status='published' GROUP BY category").fetchall()
    conn.close()
    return jsonify({
        'published_count': published,
        'pending_review':  pending,
        'categories': {r['category']: r['cnt'] for r in categories},
    })

# ════════════════════════════════════════════════════════════
# 收藏接口（新增）
# ════════════════════════════════════════════════════════════

@app.route('/api/favorites', methods=['GET'])
@login_required
def get_favorites():
    u      = request.current_user
    cat    = request.args.get('cat', '')
    region = request.args.get('region', '')

    conn = get_db()
    sql  = """SELECT m.* FROM markets m
              JOIN favorites f ON f.market_id = m.id
              WHERE f.user_id=? AND m.status='published'"""
    params = [u['id']]
    if cat:    sql += " AND m.category=?";          params.append(cat)
    if region: sql += " AND m.region LIKE ?";       params.append(f'%{region}%')
    sql += " ORDER BY f.created_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        d['tags'] = _parse_tags(d.get('tags'))
        result.append(d)
    return ok({'list': result, 'total': len(result)})


@app.route('/api/favorites/<market_id>', methods=['POST'])
@login_required
def add_favorite(market_id):
    u    = request.current_user
    conn = get_db()
    m    = conn.execute("SELECT id FROM markets WHERE id=?", (market_id,)).fetchone()
    if not m:
        conn.close()
        return err('集市不存在', 404)
    try:
        conn.execute("INSERT INTO favorites(user_id, market_id) VALUES(?,?)",
                     (u['id'], market_id))
        conn.execute("UPDATE markets SET fav_count=fav_count+1 WHERE id=?", (market_id,))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # 已收藏，忽略
    conn.close()
    return ok({'faved': True})


@app.route('/api/favorites/<market_id>', methods=['DELETE'])
@login_required
def remove_favorite(market_id):
    u    = request.current_user
    conn = get_db()
    rows = conn.execute(
        "DELETE FROM favorites WHERE user_id=? AND market_id=?",
        (u['id'], market_id)).rowcount
    if rows > 0:
        conn.execute(
            "UPDATE markets SET fav_count=MAX(0,fav_count-1) WHERE id=?", (market_id,))
    conn.commit()
    conn.close()
    return ok({'faved': False})


@app.route('/api/favorites/ids', methods=['GET'])
@login_required
def get_fav_ids():
    """只返回收藏的ID列表，前端快速判断是否已收藏"""
    u    = request.current_user
    conn = get_db()
    rows = conn.execute(
        "SELECT market_id FROM favorites WHERE user_id=?", (u['id'],)).fetchall()
    conn.close()
    return ok([r['market_id'] for r in rows])


# ════════════════════════════════════════════════════════════
# 提醒接口
# ════════════════════════════════════════════════════════════

@app.route('/api/reminders', methods=['GET'])
@login_required
def get_reminders():
    """获取当前用户的提醒列表（含集市基本信息）"""
    u    = request.current_user
    conn = get_db()
    rows = conn.execute("""
        SELECT r.id, r.market_id, r.remind_type, r.status, r.created_at,
               m.name AS market_name, m.category, m.icon, m.region,
               m.address, m.open_time
        FROM market_reminders r
        JOIN markets m ON m.id = r.market_id
        WHERE r.user_id=? AND r.status='active'
        ORDER BY r.created_at DESC
    """, (u['id'],)).fetchall()
    conn.close()
    return ok([dict(r) for r in rows])


@app.route('/api/reminders/<market_id>', methods=['POST'])
@login_required
def set_reminder(market_id):
    """设置或更新提醒（upsert）"""
    u    = request.current_user
    data = request.get_json() or {}
    remind_type = data.get('remind_type', 'once')
    if remind_type not in ('once', 'recurring'):
        return err('remind_type 必须为 once 或 recurring', 400)
    conn = get_db()
    m = conn.execute("SELECT id FROM markets WHERE id=?", (market_id,)).fetchone()
    if not m:
        conn.close()
        return err('集市不存在', 404)
    conn.execute("""
        INSERT INTO market_reminders(user_id, market_id, remind_type, status, updated_at)
        VALUES(?, ?, ?, 'active', datetime('now','localtime'))
        ON CONFLICT(user_id, market_id) DO UPDATE SET
            remind_type=excluded.remind_type,
            status='active',
            updated_at=datetime('now','localtime')
    """, (u['id'], market_id, remind_type))
    conn.commit()
    conn.close()
    label = '提醒一次' if remind_type == 'once' else '长期提醒'
    return ok({'remind_type': remind_type, 'label': label})


@app.route('/api/reminders/<market_id>', methods=['DELETE'])
@login_required
def cancel_reminder(market_id):
    """取消提醒"""
    u    = request.current_user
    conn = get_db()
    conn.execute(
        "UPDATE market_reminders SET status='cancelled', updated_at=datetime('now','localtime') WHERE user_id=? AND market_id=?",
        (u['id'], market_id)
    )
    conn.commit()
    conn.close()
    return ok({'cancelled': True})


@app.route('/api/reminders/status/<market_id>', methods=['GET'])
@login_required
def get_reminder_status(market_id):
    """查询当前用户对某集市的提醒状态"""
    u    = request.current_user
    conn = get_db()
    row  = conn.execute(
        "SELECT remind_type FROM market_reminders WHERE user_id=? AND market_id=? AND status='active'",
        (u['id'], market_id)
    ).fetchone()
    conn.close()
    if row:
        return ok({'active': True, 'remind_type': row['remind_type']})
    return ok({'active': False})


# ════════════════════════════════════════════════════════════
# 点评接口（新增）
# ════════════════════════════════════════════════════════════

@app.route('/api/reviews/stats', methods=['GET'])
def review_stats():
    market_id = request.args.get('market_id')
    if not market_id:
        return err('缺少market_id')
    conn = get_db()
    rows = conn.execute(
        "SELECT rating, COUNT(*) as cnt FROM reviews WHERE market_id=? AND status='approved' GROUP BY rating",
        (market_id,)
    ).fetchall()
    dist = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for r in rows:
        dist[int(r['rating'])] = r['cnt']
    total = sum(dist.values())
    raw_avg = sum(k * v for k, v in dist.items()) / total if total else 0

    # 贝叶斯平均：全平台均分作为先验，权重5
    platform_row = conn.execute(
        "SELECT AVG(rating) FROM reviews WHERE status='approved'"
    ).fetchone()[0]
    platform_avg = float(platform_row) if platform_row else 3.5
    C = 5  # 先验权重
    bayesian_avg = round((C * platform_avg + sum(k*v for k,v in dist.items())) / (C + total), 1) if total > 0 else 0

    # 访问人数
    visit_count = conn.execute(
        "SELECT COUNT(*) FROM market_visits WHERE market_id=?", (market_id,)
    ).fetchone()[0]

    conn.close()
    return ok({
        'avg': bayesian_avg,
        'raw_avg': round(raw_avg, 1),
        'total': total,
        'distribution': dist,
        'visit_count': visit_count,
        # 显示级别: 0=无评分, 1=仅星星, 2=分数+供参考, 3=完整
        'display_level': 0 if total == 0 else (1 if total < 5 else (2 if total < 10 else 3))
    })


@app.route('/api/reviews', methods=['GET'])
def get_reviews():
    market_id = request.args.get('market_id')
    page      = int(request.args.get('page', 1))
    per_page  = min(int(request.args.get('per_page', 20)), 50)

    conn   = get_db()
    sql    = """SELECT r.*, u.nickname, u.avatar
                FROM reviews r LEFT JOIN users u ON u.id=r.user_id
                WHERE r.status='approved'"""
    params = []
    if market_id:
        sql += " AND r.market_id=?"; params.append(market_id)
    total = conn.execute(
        f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY r.created_at DESC LIMIT {per_page} OFFSET {(page-1)*per_page}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()

    result = []
    for r in rows:
        d = dict(r)
        d['images'] = json.loads(d.get('images') or '[]')
        d['tags']   = json.loads(d.get('tags')   or '[]')
        d['userNick']   = d.pop('nickname', '匿名')
        d['userAvatar'] = d.pop('avatar',   '👤')
        result.append(d)
    return ok({'list': result, 'total': total})


@app.route('/api/reviews', methods=['POST'])
@login_required
def submit_review():
    u    = request.current_user
    data = request.get_json(force=True) or {}
    mid  = data.get('marketId') or data.get('market_id')
    if not mid:
        return err('缺少集市ID')
    rating  = float(data['rating']) if data.get('rating') else None
    content = data.get('content', '').strip()
    conn    = get_db()
    m       = conn.execute("SELECT id FROM markets WHERE id=?", (mid,)).fetchone()
    if not m:
        conn.close()
        return err('集市不存在', 404)
    conn.execute("""
        INSERT INTO reviews(market_id,user_id,rating,content,images,tags,status)
        VALUES(?,?,?,?,?,?,?)
    """, (mid, u['id'], rating, content,
          json.dumps(data.get('images', []), ensure_ascii=False),
          json.dumps(data.get('tags',   []), ensure_ascii=False),
          'pending'))
    conn.commit()
    conn.close()
    _log('submit_review', f'market:{mid}')
    return ok(None, '点评提交成功，等待审核')

# ════════════════════════════════════════════════════════════
# 公告 & 轮播图（新增）
# ════════════════════════════════════════════════════════════


# ────────────────────────────────────────────
# 分类管理
# ────────────────────────────────────────────
@app.route('/api/app/config', methods=['GET'])
def app_config():
    conn = get_db()
    show_market = _get_setting(conn, 'show_market_section', 'false') == 'true'
    # 返回所有激活分类及其 is_market_type 标记（给前台分类归属判断）
    # 兼容旧数据库（is_market_type 列可能不存在）
    try:
        cats = conn.execute("SELECT name, is_market_type FROM categories WHERE active=1").fetchall()
        active_cats = [{'name': r[0], 'is_market_type': bool(r[1])} for r in cats]
    except Exception:
        cats = conn.execute("SELECT name FROM categories WHERE active=1").fetchall()
        active_cats = [{'name': r[0], 'is_market_type': False} for r in cats]
    conn.close()
    return jsonify({'code': 200, 'data': {
        'show_market_section': show_market,
        'active_categories': active_cats,
    }})

@app.route('/api/admin/settings/market-section', methods=['GET', 'POST'])
@admin_required
def settings_market_section():
    conn = get_db()
    if request.method == 'GET':
        val = _get_setting(conn, 'show_market_section', 'false')
        conn.close()
        return jsonify({'code': 200, 'data': {'show_market_section': val == 'true'}})
    data = request.get_json() or {}
    enabled = 'true' if data.get('enabled') else 'false'
    conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('show_market_section',?)", (enabled,))
    conn.commit(); conn.close()
    return jsonify({'code': 200, 'msg': '保存成功'})

@app.route('/api/admin/gemini-key', methods=['GET', 'POST'])
@admin_required
def gemini_key():
    conn = get_db()
    if request.method == 'GET':
        key = _get_setting(conn, 'gemini_api_key', '')
        conn.close()
        return jsonify({'code': 200, 'data': {'key': key}})
    data = request.get_json() or {}
    conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('gemini_api_key',?)", (data.get('key',''),))
    conn.commit(); conn.close()
    return jsonify({'code': 200, 'msg': '保存成功'})


@app.route('/api/admin/vision-config', methods=['GET', 'POST'])
@admin_required
def vision_config():
    """图片识别多接口配置"""
    conn = get_db()
    if request.method == 'GET':
        cfg = {
            'qwen_key':          _get_setting(conn, 'gemini_api_key', ''),
            'doubao_key':        _get_setting(conn, 'doubao_api_key', ''),
            'doubao_vision_model': _get_setting(conn, 'doubao_vision_model', ''),
            'glm_key':           _get_setting(conn, 'glm_api_key', ''),
            'kimi_key':          _get_setting(conn, 'kimi_api_key', ''),
        }
        conn.close()
        return jsonify({'code': 200, 'data': cfg})
    data = request.get_json() or {}
    mapping = {
        'qwen_key':           'gemini_api_key',
        'doubao_key':         'doubao_api_key',
        'doubao_vision_model':'doubao_vision_model',
        'glm_key':            'glm_api_key',
        'kimi_key':           'kimi_api_key',
    }
    for field, db_key in mapping.items():
        v = data.get(field, '')
        if v != '':
            conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES(?,?)", (db_key, v.strip()))
    conn.commit(); conn.close()
    return jsonify({'code': 200, 'msg': '保存成功'})


# ── 视觉模型提供商表（新增只需加一行）──────────────────────────
_VISION_PROVIDERS = {
    'qwen': {
        'name':      '通义千问 Qwen-VL',
        'url':       'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
        'key':       'gemini_api_key',
        'model':     'qwen-vl-max',
    },
    'doubao': {
        'name':      '豆包 Doubao Vision',
        'url':       'https://ark.cn-beijing.volces.com/api/v3/chat/completions',
        'key':       'doubao_api_key',
        'model':     '',
        'model_key': 'doubao_vision_model',   # endpoint ID
    },
    'glm': {
        'name':      '智谱 GLM-4V',
        'url':       'https://open.bigmodel.cn/api/paas/v4/chat/completions',
        'key':       'glm_api_key',
        'model':     'glm-4v-flash',
    },
    'kimi': {
        'name':      'Kimi Vision',
        'url':       'https://api.moonshot.cn/v1/chat/completions',
        'key':       'kimi_api_key',
        'model':     'moonshot-v1-8k-vision-preview',
    },
}

VISION_PROMPT = """你是农村集市数据整理专家，熟悉中国各地赶集和庙会习俗。
仔细识别图片中所有集市/庙会信息，输出JSON数组。

【日期解析规则】
- "逢四、九"/"四、九" → {"type":"lunar","days":[4,9,14,19,24,29],"text":"逢四逢九"}
- "逢五、十" → {"type":"lunar","days":[5,10,15,20,25,30],"text":"逢五逢十"}
- "逢三、八" → {"type":"lunar","days":[3,8,13,18,23,28],"text":"逢三逢八"}
- "逢二、七" → {"type":"lunar","days":[2,7,12,17,22,27],"text":"逢二逢七"}
- "逢一、六" → {"type":"lunar","days":[1,6,11,16,21,26],"text":"逢一逢六"}
- "逢双日" → {"type":"lunar","days":[2,4,6,8,10,12,14,16,18,20,22,24,26,28,30],"text":"逢双日"}
- "逢单日" → {"type":"lunar","days":[1,3,5,7,9,11,13,15,17,19,21,23,25,27,29],"text":"逢单日"}
- "天天有"/"百日集" → {"type":"daily","text":"天天有"}
- 农历固定日期"三月十五" → {"type":"lunar_event","month":3,"day":15,"text":"三月十五"}
- 阳历"4月18" → {"type":"solar_event","month":4,"day":18,"text":"4月18"}

【热度与规模估算】根据你对该地区集市的了解，每条记录额外给出：
- "scale": "大型/中型/小型"（大型=地区知名/千余摊位，中型=镇级/数百摊位，小型=村级小集）
- "heat": 1到100整数（综合地名知名度、人口规模、交通位置估算）
- "heat_reason": "不超过15字的理由"

【输出格式】每条记录：
{"name":"村名+大集或庙会","region":"省·市·县","category":"农村大集或农村庙会",
"address":"村/镇名","open_time":{...},"scale":"中型","heat":60,"heat_reason":"镇级集市人流较多"}

【规则】
- 图片标题"XX县赶集时间表"→以此推断region
- 同行多个地名每个单独一条
- 含"庙会"字样category为"农村庙会"，其余为"农村大集"
- name格式：地名+大集（或庙会），如"西三庄大集"
- 只输出JSON数组，不要其他任何文字"""


@app.route('/api/admin/gemini/recognize', methods=['POST'])
@admin_required
def gemini_recognize():
    import base64
    try:
        import requests as req
        from PIL import Image
        import io as _io
    except ImportError as e:
        return jsonify({'code': 500, 'msg': f'缺少依赖：{e}'})

    # 获取请求参数
    providers_req = request.form.get('providers', 'qwen')   # 逗号分隔的接口列表
    provider_names = [p.strip() for p in providers_req.split(',') if p.strip() in _VISION_PROVIDERS]
    if not provider_names:
        provider_names = ['qwen']

    files = request.files.getlist('images')
    if not files:
        return jsonify({'code': 400, 'msg': '请上传图片'})

    conn = get_db()
    # 构建每个接口的 (name, url, key, model)
    active_providers = []
    for pname in provider_names:
        p   = _VISION_PROVIDERS[pname]
        key = _get_setting(conn, p['key'], '')
        if not key:
            continue
        model = p.get('model', '')
        if p.get('model_key'):
            model = _get_setting(conn, p['model_key'], '') or model
        if not model:
            continue
        active_providers.append({'id': pname, 'name': p['name'], 'url': p['url'], 'key': key, 'model': model})
    conn.close()

    if not active_providers:
        return jsonify({'code': 400, 'msg': '请先在采集设置中配置至少一个视觉模型的 API Key'})

    def _compress_image(file_stream):
        img = Image.open(file_stream).convert('RGB')
        w, h = img.size
        if max(w, h) > 1200:
            s = 1200 / max(w, h)
            img = img.resize((int(w*s), int(h*s)), Image.LANCZOS)
        buf = _io.BytesIO()
        img.save(buf, format='JPEG', quality=80)
        return base64.b64encode(buf.getvalue()).decode()

    def _parse_json_from_text(text):
        m = re.search(r'\[[\s\S]*\]', text)
        if not m:
            return []
        try:
            return json.loads(m.group())
        except Exception:
            partial = m.group()
            last = partial.rfind('},')
            if last > 0:
                try:
                    return json.loads(partial[:last+1] + ']')
                except Exception:
                    pass
        return []

    all_results = []
    errors      = []
    provider_stats = {}   # {provider_id: count}

    for f in files:
        try:
            img_b64 = _compress_image(f.stream)
        except Exception as e:
            errors.append(f'{f.filename} 图片处理失败：{e}')
            continue

        msg_content = [
            {'type': 'image_url', 'image_url': {'url': f'data:image/jpeg;base64,{img_b64}'}},
            {'type': 'text', 'text': VISION_PROMPT}
        ]

        for p in active_providers:
            try:
                resp = req.post(
                    p['url'],
                    headers={'Authorization': f'Bearer {p["key"]}', 'Content-Type': 'application/json'},
                    json={
                        'model':       p['model'],
                        'messages':    [{'role': 'user', 'content': msg_content}],
                        'temperature': 0.1,
                        'max_tokens':  8000,
                    },
                    timeout=120
                )
                result = resp.json()
                if 'error' in result:
                    raise Exception(result['error'].get('message', str(result)))
                text  = result['choices'][0]['message']['content']
                items = _parse_json_from_text(text)
                for item in items:
                    item['_provider']      = p['id']
                    item['_provider_name'] = p['name']
                all_results.extend(items)
                provider_stats[p['id']] = provider_stats.get(p['id'], 0) + len(items)
                logging.info(f"vision [{p['id']}] {f.filename}: {len(items)} items")
            except Exception as e:
                err_msg = f"[{p['name']}] {f.filename}: {str(e)[:80]}"
                errors.append(err_msg)
                logging.warning(f'vision recognize error: {err_msg}')

    return jsonify({
        'code': 200,
        'data':  all_results,
        'errors': errors,
        'total': len(all_results),
        'provider_stats': provider_stats,
    })


@app.route('/api/admin/ai-verify-config', methods=['GET', 'POST'])
@admin_required
def ai_verify_config():
    """AI校验接口的配置读写（provider + 各平台 key/model）"""
    conn = get_db()
    if request.method == 'GET':
        cfg = {
            'provider':    _get_setting(conn, 'ai_verify_provider', 'deepseek'),
            'deepseek_key': _get_setting(conn, 'deepseek_api_key', ''),
            'doubao_key':   _get_setting(conn, 'doubao_api_key', ''),
            'doubao_model': _get_setting(conn, 'doubao_model', ''),
            'qwen_key':     _get_setting(conn, 'qwen_verify_key', ''),
        }
        conn.close()
        return jsonify({'code': 200, 'data': cfg})
    data = request.get_json() or {}
    pairs = [
        ('ai_verify_provider', data.get('provider', '')),
        ('deepseek_api_key',   data.get('deepseek_key', '')),
        ('doubao_api_key',     data.get('doubao_key', '')),
        ('doubao_model',       data.get('doubao_model', '')),
        ('qwen_verify_key',    data.get('qwen_key', '')),
    ]
    for k, v in pairs:
        if v != '':  # 空字符串不覆盖（允许只更新部分字段）
            conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES(?,?)", (k, v.strip()))
    conn.commit(); conn.close()
    return jsonify({'code': 200, 'msg': '保存成功'})


# ── AI 接口调用表（新增接口只需在此处加一行）────────────────────
_AI_PROVIDERS = {
    'deepseek': {
        'url':   'https://api.deepseek.com/chat/completions',
        'key':   'deepseek_api_key',
        'model': 'deepseek-chat',
        'json_mode': True,
    },
    'doubao': {
        'url':   'https://ark.cn-beijing.volces.com/api/v3/chat/completions',
        'key':   'doubao_api_key',
        'model': '',          # 从 doubao_model 设置读取（endpoint ID）
        'model_setting': 'doubao_model',
        'json_mode': False,   # 豆包不支持 json_object response_format
    },
    'qwen': {
        'url':   'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
        'key':   'qwen_verify_key',
        'model': 'qwen-turbo',
        'json_mode': False,
    },
}


@app.route('/api/admin/ai/verify-markets', methods=['POST'])
@admin_required
def ai_verify_markets():
    """批量校验导入集市的日期信息（多接口可切换）"""
    try:
        import requests as req_lib
    except ImportError:
        return jsonify({'code': 500, 'msg': '缺少 requests 库'})

    items = request.get_json() or []
    conn  = get_db()
    provider_name = _get_setting(conn, 'ai_verify_provider', 'deepseek')
    provider = _AI_PROVIDERS.get(provider_name, _AI_PROVIDERS['deepseek'])
    api_key  = _get_setting(conn, provider['key'], '')
    model    = _get_setting(conn, provider.get('model_setting', ''), '') or provider['model']
    conn.close()

    if not api_key:
        pname = {'deepseek':'DeepSeek','doubao':'豆包','qwen':'通义千问'}.get(provider_name, provider_name)
        return jsonify({'code': 400, 'msg': f'请先配置 {pname} API Key'})
    if not model:
        return jsonify({'code': 400, 'msg': '豆包需要填写 Endpoint Model ID'})

    def _ot_to_text(ot):
        if not isinstance(ot, dict): return str(ot)
        t, days, txt = ot.get('type',''), ot.get('days',[]), ot.get('text','')
        if txt: return txt
        if t == 'lunar':       return f"农历逢{'/'.join(str(d) for d in days)}日"
        if t == 'solar':       return f"阳历逢{'/'.join(str(d) for d in days)}日"
        if t == 'lunar_event': return f"农历{ot.get('month')}月{ot.get('day')}日（庙会）"
        if t == 'daily':       return '天天有'
        if t == 'weekday':
            wn = {1:'周一',2:'周二',3:'周三',4:'周四',5:'周五',6:'周六',7:'周日'}
            return '、'.join(wn.get(d,'') for d in days)
        return ot.get('custom', '未知')

    PROMPT_TPL = (
        "你是中国农村集市数据核验专家，熟悉各地赶集和庙会习俗。"
        "判断以下集市开集日期信息是否合理准确。\n\n"
        "名称：{name}\n地区：{region}\n类型：{category}\n"
        "开集时间：{ot_text}\n地址：{address}\n\n"
        "核验要点：\n"
        "- 农村大集通常农历逢几，间隔5天（逢一逢六/逢三逢八等）\n"
        "- 庙会是农历固定日期（三月十五/正月初八等），不是逢几\n"
        "- 阳历逢几的集市较少见\n"
        "- 根据地区习俗判断是否合理\n\n"
        "只返回JSON，不要其他文字：\n"
        '{{"valid":true或false,"confidence":0到100整数,"reason":"不超过20字",'
        '"suggestion":null或修正的open_time对象}}\n'
        'open_time格式：{{"type":"lunar","days":[1,6,11,16,21,26],"text":"逢一逢六"}} '
        '或 {{"type":"lunar_event","month":3,"day":15,"text":"三月十五"}}'
    )

    results = []
    for i, item in enumerate(items):
        name     = (item.get('name') or item.get('market_name', '')).strip()
        region   = item.get('region', '')
        category = item.get('category', '农村大集')
        address  = item.get('address', '')
        ot       = item.get('open_time', {})
        if isinstance(ot, str):
            try: ot = json.loads(ot)
            except: ot = {}

        prompt = PROMPT_TPL.format(
            name=name, region=region, category=category,
            ot_text=_ot_to_text(ot), address=address
        )
        body = {
            'model': model,
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': 0,
            'max_tokens': 300,
        }
        if provider.get('json_mode'):
            body['response_format'] = {'type': 'json_object'}

        try:
            resp    = req_lib.post(
                provider['url'],
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json=body, timeout=25
            )
            content = resp.json()['choices'][0]['message']['content'].strip()
            # 豆包等不保证纯JSON，提取第一个{}块
            m = re.search(r'\{.*\}', content, re.S)
            result  = json.loads(m.group() if m else content)
            results.append({
                'index':      i,
                'valid':      bool(result.get('valid', True)),
                'confidence': int(result.get('confidence', 80)),
                'reason':     str(result.get('reason', ''))[:60],
                'suggestion': result.get('suggestion'),
            })
        except Exception as e:
            logging.warning(f'ai_verify [{provider_name}] item {i} failed: {e}')
            results.append({'index': i, 'valid': True, 'confidence': 50,
                            'reason': '验证跳过', 'suggestion': None})

    return jsonify({'code': 200, 'results': results, 'provider': provider_name})


@app.route('/api/admin/gemini/import', methods=['POST'])
@admin_required
def gemini_import():
    """将本地整理好的 JSON 数据批量写入采集队列"""
    items = request.get_json() or []
    conn = get_db()
    saved = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for item in items:
        name = (item.get('name') or item.get('market_name') or '').strip()
        if not name:
            continue
        open_time = item.get('open_time', {})
        lat = item.get('lat') or None
        lng = item.get('lng') or None
        try:
            if lat is not None: lat = float(lat)
            if lng is not None: lng = float(lng)
        except (ValueError, TypeError):
            lat = lng = None
        try:    rating_val = float(item['rating']) if item.get('rating') else None
        except: rating_val = None
        try:    fav_val = int(item['fav_count']) if item.get('fav_count') else 0
        except: fav_val = 0
        conn.execute("""
            INSERT INTO spider_queue
            (id,platform,raw_title,market_name,category,region,address,open_time,tags,
             description,confidence,lat,lng,rating,fav_count,status,pushed_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)
        """, (
            str(uuid.uuid4()), 'json_import', name, name,
            item.get('category', '农村大集'),
            item.get('region', ''),
            item.get('address', '') or '',
            json.dumps(open_time, ensure_ascii=False),
            json.dumps([], ensure_ascii=False),
            item.get('description', '') or '',
            90, lat, lng, rating_val, fav_val, now
        ))
        saved += 1
    conn.commit(); conn.close()
    log_action(request.current_user.get('id', 0), 'json_import', 'queue', f'批量导入 {saved} 条进入待审核队列')
    return jsonify({'code': 200, 'saved': saved})


@app.route('/api/admin/ai/collect-markets', methods=['POST'])
@admin_required
def ai_collect_markets():
    """多AI并发采集集市数据，合并去重后导入队列"""
    try:
        import requests as req_lib
        import re as _re
        from concurrent.futures import ThreadPoolExecutor, as_completed
    except ImportError as e:
        return jsonify(code=500, msg=f'缺少依赖: {e}')

    data     = request.get_json() or {}
    province = data.get('province', '').strip()
    city     = data.get('city', '').strip()
    county   = data.get('county', '').strip()
    township = data.get('township', '').strip()
    types    = data.get('types', ['农村大集（赶集）', '庙会（含山会、节会、香火会等）'])
    want_coord = data.get('wantCoord', True)
    extra    = data.get('extra', '').strip()

    if not city:
        return jsonify(code=400, msg='请至少选择市')

    conn = get_db()
    def _k(key): return _get_setting(conn, key, '')
    qwen_key    = _k('gemini_api_key')
    deepseek_key= _k('deepseek_api_key')
    doubao_key  = _k('doubao_api_key')
    doubao_model= _k('doubao_model')
    glm_key     = _k('glm_api_key')
    kimi_key    = _k('kimi_api_key')
    conn.close()

    # 采集范围：有县取县，无县取市
    scope     = f'{province}{city}{county}' if county else f'{province}{city}'
    ref_area  = county if county else city  # 用于行政归属校验描述
    addr_example = f'{county}XXX镇XXX村' if county else f'{city}XXX县XXX镇XXX村'
    name_example = 'XXX镇XXX村大集' if county else 'XXX县XXX镇大集'
    region_val   = f'{province}·{city}·{county}' if county else f'{province}·{city}'

    # 各 AI 接口定义
    providers = []
    if qwen_key:
        providers.append(('通义千问', 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
                          qwen_key, 'qwen-plus'))
    if deepseek_key:
        providers.append(('DeepSeek', 'https://api.deepseek.com/chat/completions',
                          deepseek_key, 'deepseek-chat'))
    if doubao_key and doubao_model:
        providers.append(('豆包', 'https://ark.cn-beijing.volces.com/api/v3/chat/completions',
                          doubao_key, doubao_model))
    if glm_key:
        providers.append(('智谱GLM', 'https://open.bigmodel.cn/api/paas/v4/chat/completions',
                          glm_key, 'glm-4-flash'))
    if kimi_key:
        providers.append(('Kimi', 'https://api.moonshot.cn/v1/chat/completions',
                          kimi_key, 'moonshot-v1-8k'))

    if not providers:
        return jsonify(code=400, msg='未配置任何 AI API Key，请先在系统设置中配置通义千问或其他 AI')

    def _extract_items(text):
        m = _re.search(r'\[[\s\S]*\]', text)
        if m:
            try:
                lst = json.loads(m.group())
                return [i for i in lst if isinstance(i, dict) and i.get('name')]
            except Exception:
                pass
        items = []
        for obj_m in _re.finditer(r'\{[^{}]*"name"\s*:\s*"[^"]+[^{}]*\}', text):
            try:
                obj = json.loads(obj_m.group())
                if obj.get('name'):
                    items.append(obj)
            except Exception:
                pass
        return items

    def _call_one(ai_name, url, key, model, prompt):
        """单次AI调用，返回items列表"""
        try:
            r = req_lib.post(url,
                headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
                json={'model': model, 'messages': [{'role': 'user', 'content': prompt}], 'temperature': 0.2},
                timeout=(15, 60))
            r.raise_for_status()
            text = r.json()['choices'][0]['message']['content'].strip()
            return _extract_items(text), None
        except Exception as e:
            return [], str(e)[:120]

    def _make_prompt(tgt_scope, tgt_ref, tgt_region):
        """为指定范围生成采集prompt"""
        cf = '\n    "lat": 38.123456,\n    "lng": 114.654321,' if want_coord else ''
        cr = '\n5. 坐标GCJ-02坐标系精确到小数点后6位，不确定填镇政府坐标' if want_coord else ''
        el = f'\n额外要求：{extra}' if extra else ''
        return f"""任务：尽可能多地列出【{tgt_scope}】的{', '.join(types)}，不要只给2-3条，请把你知道的全部列出来。{el}

要求：
1. 只写你有把握的集市，宁可少写也不要编造——没有记录的地方就跳过
2. 不要为每个乡镇"配"一个集市，没有集市的乡镇直接略过
3. 庙会尤其要谨慎，只写确实存在且有一定知名度的，不要推测
4. 名字格式：XX镇大集 / XX村大集 / XX庙会，不加多余修饰词
5. 地址归属严格按{tgt_ref}官方行政区划{cr}

只输出JSON数组（尽量多条），格式如下：
[
  {{"name":"XX镇大集","category":"农村大集","address":"{tgt_ref}XX镇","region":"{tgt_region}","open_time":"农历每月初一、初六、十一、十六、二十一、二十六","description":"XX镇大集是当地最大的农村集市，历史悠久，每逢开集吸引周边村镇居民前来赶集。","tags":"农产品,土特产,便民","rating":4.0,"fav_count":300{cf}}},
  {{"name":"XX庙会","category":"庙会","address":"{tgt_ref}XX镇XX村","region":"{tgt_region}","open_time":"农历三月十五","description":"XX庙会每年农历三月十五举行，香火旺盛，周边百里皆知。","tags":"庙会,传统文化","rating":4.5,"fav_count":600{cf}}}
]
open_time规范：逢一逢六→"农历每月初一、初六、十一、十六、二十一、二十六"；逢四逢九→"农历每月初四、初九、十四、十九、二十四、二十九"；庙会→"农历X月X日"；不确定→"待核实"
rating：5.0最知名/4.5人气旺/4.0普通镇级/3.5村级小集
请输出尽量完整的列表，不要截断，直接输出JSON数组："""

    townships = []
    provider_results = []

    if not county:
        # ── 市级采集：先拿县区列表，再逐县并发查询 ──────────────────
        # 1. 获取县区列表（用第一个可用AI，快速调用）
        counties = []
        p0 = providers[0]
        try:
            _r = req_lib.post(p0[1],
                headers={'Authorization': f'Bearer {p0[2]}', 'Content-Type': 'application/json'},
                json={'model': p0[3],
                      'messages': [{'role': 'user', 'content':
                          f'请列出{province}{city}所有县、区、县级市的名称，只输出JSON字符串数组，如["XX县","XX区"]，不含其他文字。'}],
                      'temperature': 0.1},
                timeout=(10, 25))
            _txt = _r.json()['choices'][0]['message']['content']
            _m = _re.search(r'\[[\s\S]*?\]', _txt)
            if _m:
                counties = [c.strip() for c in json.loads(_m.group()) if isinstance(c, str) and c.strip()]
        except Exception:
            pass

        if counties:
            # 2. 为每个县选一个AI（轮询分配，均衡负载）
            def _collect_county(idx_cnty):
                idx, cnty = idx_cnty
                ai = providers[idx % len(providers)]
                rgn = f'{province}·{city}·{cnty}'
                prompt = _make_prompt(f'{province}{city}{cnty}', cnty, rgn)
                items, err = _call_one(*ai, prompt)
                for it in items:
                    it.setdefault('region', rgn)
                    it['_source'] = ai[0]
                return ai[0], items, err

            # 3. 分批并发（每批8个县）
            ai_totals = {p[0]: {'items': [], 'error': None} for p in providers}
            BATCH = 8
            all_items = []
            for i in range(0, len(counties), BATCH):
                batch = list(enumerate(counties[i:i+BATCH], start=i))
                with ThreadPoolExecutor(max_workers=len(batch)) as ex:
                    for ai_name, items, err in ex.map(_collect_county, batch):
                        all_items.extend(items)
                        ai_totals[ai_name]['items'].extend(items)
                        if err and not ai_totals[ai_name]['error']:
                            ai_totals[ai_name]['error'] = err

            provider_results = [{'name': k, 'items': v['items'], 'error': v['error']}
                                 for k, v in ai_totals.items() if v['items'] or v['error']]
            townships = counties  # 复用字段传给前端展示县区数
        else:
            # 获取县区失败，降级为全市单次多AI查询
            rgn = f'{province}·{city}'
            prompt = _make_prompt(f'{province}{city}', city, rgn)
            with ThreadPoolExecutor(max_workers=len(providers)) as ex:
                futures = {ex.submit(_call_one, *p, prompt): p[0] for p in providers}
                for f in as_completed(futures):
                    items, err = f.result()
                    provider_results.append({'name': futures[f], 'items': items, 'error': err})
    else:
        # ── 县级采集：多AI并发，取并集 ────────────────────────────────
        rgn = f'{province}·{city}·{county}'
        prompt = _make_prompt(f'{province}{city}{county}', county, rgn)
        with ThreadPoolExecutor(max_workers=len(providers)) as ex:
            futures = {ex.submit(_call_one, *p, prompt): p[0] for p in providers}
            for f in as_completed(futures):
                items, err = f.result()
                provider_results.append({'name': futures[f], 'items': items, 'error': err})

    # 合并 + 去重（按名称+地址，批次内）
    seen, merged = set(), []
    for pr in provider_results:
        for item in pr['items']:
            key_str = f"{item.get('name','').strip()}|{item.get('address','').strip()}"
            if key_str not in seen:
                seen.add(key_str)
                item['_source'] = pr['name']
                merged.append(item)

    # 写入 spider_queue（与已有数据去重）
    conn = get_db()

    def _norm(s):
        """简单标准化：去空格、去常见后缀"""
        s = (s or '').strip()
        for suf in ['大集', '集市', '集会', '农贸市场', '早市', '夜市', '庙会']:
            if s.endswith(suf):
                s = s[:-len(suf)]; break
        return s.lower()

    # 拉取已有名称（markets + spider_queue），构建去重集合
    existing_names = set()
    for row in conn.execute("SELECT name FROM markets WHERE region LIKE ?", (f'%{city}%',)):
        existing_names.add(_norm(row[0]))
    for row in conn.execute("SELECT market_name FROM spider_queue WHERE region LIKE ?", (f'%{city}%',)):
        existing_names.add(_norm(row[0]))

    saved = 0
    skipped = 0
    now   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for item in merged:
        name = item.get('name', '').strip()
        if not name: continue
        if _norm(name) in existing_names:
            skipped += 1
            continue
        existing_names.add(_norm(name))  # 防止同批次重复写入
        lat = lng = None
        try:
            if item.get('lat'): lat = float(item['lat'])
            if item.get('lng'): lng = float(item['lng'])
        except: pass
        try: rating_val = float(item['rating']) if item.get('rating') else None
        except: rating_val = None
        try: fav_val = int(item['fav_count']) if item.get('fav_count') else 0
        except: fav_val = 0
        conn.execute("""
            INSERT INTO spider_queue
            (id,platform,raw_title,market_name,category,region,address,open_time,tags,
             description,confidence,lat,lng,rating,fav_count,status,pushed_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)
        """, (
            str(uuid.uuid4()), f'ai_collect:{item.get("_source","")}',
            name, name,
            item.get('category', '农村大集'),
            item.get('region', region_val),
            item.get('address', '') or '',
            json.dumps(item.get('open_time', ''), ensure_ascii=False),
            item.get('tags', '') or '',
            item.get('description', '') or '',
            85, lat, lng, rating_val, fav_val, now
        ))
        saved += 1
    # 统计每个AI的独立贡献（_source标记了每条唯一记录来自哪个AI）
    contribution = {}
    for item in merged:
        src = item.get('_source', '未知')
        contribution[src] = contribution.get(src, 0) + 1

    stats = [{'name': r['name'], 'count': len(r['items']), 'error': r['error'],
              'unique': contribution.get(r['name'], 0)}
             for r in provider_results]

    # 写入 ai_collect_logs
    log_action(request.current_user.get('id', 0), 'json_import', 'queue',
               f'AI多平台采集 {scope}，{len(providers)} 个接口，合并去重后导入 {saved} 条（跳过重复 {skipped} 条）')
    conn.execute(
        "INSERT INTO ai_collect_logs(scope,saved,total_raw,providers_json,comparison_json) VALUES(?,?,?,?,?)",
        (scope, saved, len(merged),
         json.dumps(stats, ensure_ascii=False),
         json.dumps([{'name': k, 'unique': v} for k, v in contribution.items()], ensure_ascii=False))
    )
    conn.commit()
    conn.close()

    return jsonify(code=200, saved=saved, skipped=skipped, total_raw=len(merged),
                   stats=stats, scope=scope, townships=townships)


@app.route('/api/admin/ai/collect-logs', methods=['GET'])
@admin_required
def ai_collect_logs_list():
    """获取 AI 采集日志列表"""
    page  = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    offset = (page - 1) * limit
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM ai_collect_logs").fetchone()[0]
    rows  = conn.execute(
        "SELECT * FROM ai_collect_logs ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (limit, offset)
    ).fetchall()
    conn.close()
    logs = []
    for r in rows:
        logs.append({
            'id':         r['id'],
            'scope':      r['scope'],
            'saved':      r['saved'],
            'total_raw':  r['total_raw'],
            'providers':  json.loads(r['providers_json'] or '[]'),
            'comparison': json.loads(r['comparison_json'] or '[]'),
            'created_at': r['created_at'],
        })
    return jsonify(code=200, logs=logs, total=total, page=page, limit=limit)


@app.route('/api/admin/dedup_check', methods=['POST'])
@admin_required
def dedup_check():
    """
    批量去重检测：检查待导入数据是否与 markets / spider_queue 已有数据重复。
    规则（同分类内）：
      1. 名称完全相同            → dup
      2. 去掉县级前缀后名称相同   → dup
      3. 名称包含关系             → dup
      4. 同县 + 相同开集日期      → maybe
    分类分组：庙会 / 非庙会 分开，互不去重。
    同一批次内部也进行去重。
    """
    import re, json as _json

    items = request.get_json() or []
    conn = get_db()

    def _cat_group(cat):
        """把分类归为大组：庙会 or 集市"""
        return '庙会' if '庙会' in (cat or '') else '集市'

    def _strip_county(name, region):
        """去掉县/市/区前缀，返回核心名称"""
        parts = [p.strip() for p in (region or '').split('·') if p.strip()]
        for p in reversed(parts):   # 从最细粒度（县）开始
            if name.startswith(p):
                name = name[len(p):]
        return name.strip()

    def _extract_lunar_days(open_time_raw):
        """从 open_time 字段提取农历日期数字集合，用于比较"""
        if not open_time_raw:
            return set()
        try:
            ot = _json.loads(open_time_raw) if isinstance(open_time_raw, str) else open_time_raw
            if isinstance(ot, dict) and ot.get('type') == 'lunar':
                return set(ot.get('days', []))
        except Exception:
            pass
        # 纯文本解析
        DMAP = [('三十',30),('二十九',29),('二十八',28),('二十七',27),('二十六',26),
                ('二十五',25),('二十四',24),('二十三',23),('二十二',22),('二十一',21),
                ('二十',20),('十九',19),('十八',18),('十七',17),('十六',16),
                ('十五',15),('十四',14),('十三',13),('十二',12),('十一',11),
                ('初十',10),('初九',9),('初八',8),('初七',7),('初六',6),
                ('初五',5),('初四',4),('初三',3),('初二',2),('初一',1)]
        s = str(open_time_raw)
        days = set()
        for w, n in DMAP:
            if w in s:
                days.add(n)
                s = s.replace(w, '')
        return days

    def _county(region):
        parts = [p.strip() for p in (region or '').split('·') if p.strip()]
        return parts[-1] if parts else ''

    # 从数据库加载已有数据（markets + pending queue，同分类）
    db_markets = conn.execute(
        "SELECT name, category, region, open_time FROM markets WHERE status != 'deleted'"
    ).fetchall()
    db_queue = conn.execute(
        "SELECT market_name AS name, category, region, open_time FROM spider_queue WHERE status = 'pending'"
    ).fetchall()
    conn.close()

    existing = [dict(r) for r in db_markets] + [dict(r) for r in db_queue]

    results = []
    # 用于批次内部去重
    batch_seen = {}   # key: (cat_group, normalized_name) → index

    for idx, item in enumerate(items):
        name = (item.get('name') or item.get('market_name') or '').strip()
        cat  = item.get('category', '') or ''
        region = item.get('region', '') or ''
        open_time_raw = item.get('open_time', '')
        county = _county(region)
        cg = _cat_group(cat)
        norm = _strip_county(name, region)

        status = 'new'
        reason = None
        existing_name = None

        # ── 规则4预处理：提取日期集合 ──────────────────────────────
        item_days = _extract_lunar_days(open_time_raw if isinstance(open_time_raw, str)
                                        else _json.dumps(open_time_raw, ensure_ascii=False))

        # ── 与数据库已有数据比较 ────────────────────────────────────
        for e in existing:
            e_cat = e.get('category', '') or ''
            if _cat_group(e_cat) != cg:
                continue   # 不同大类，跳过
            e_name   = (e.get('name') or '').strip()
            e_region = e.get('region', '') or ''
            e_norm   = _strip_county(e_name, e_region)
            e_county = _county(e_region)

            # 规则1：完全相同
            if name == e_name:
                status = 'dup'; reason = f'名称完全相同（已有：{e_name}）'; existing_name = e_name; break
            # 规则2：去县前缀后相同
            if norm and e_norm and norm == e_norm:
                status = 'dup'; reason = f'去掉县级前缀后名称相同（已有：{e_name}）'; existing_name = e_name; break
            # 规则3：包含关系（两者都不为空且差别不超过6个字）
            if norm and e_norm and abs(len(norm) - len(e_norm)) <= 6:
                if norm in e_norm or e_norm in norm:
                    status = 'dup'; reason = f'名称高度相似（已有：{e_name}）'; existing_name = e_name; break

        # ── 批次内部去重 ────────────────────────────────────────────
        batch_key = (cg, norm or name)
        if status == 'new':
            if batch_key in batch_seen:
                prev_idx = batch_seen[batch_key]
                status = 'batch_dup'
                reason = f'与批次内第{prev_idx+1}条重复（{items[prev_idx].get("name","")}）'
            else:
                batch_seen[batch_key] = idx

        results.append({
            'index':         idx,
            'status':        status,    # new / dup / maybe / batch_dup
            'reason':        reason,
            'existing_name': existing_name,
        })

    dup_count   = sum(1 for r in results if r['status'] in ('dup', 'batch_dup'))
    maybe_count = sum(1 for r in results if r['status'] == 'maybe')
    new_count   = sum(1 for r in results if r['status'] == 'new')

    return jsonify({
        'code': 200,
        'data': results,
        'summary': {'new': new_count, 'dup': dup_count, 'maybe': maybe_count}
    })


@app.route('/api/admin/db_dedup', methods=['GET'])
@admin_required
def db_dedup():
    """扫描 markets 表内部已有的重复数据，按同分类组返回重复集合。"""
    import re as _re, json as _json

    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, category, region, open_time, status FROM markets WHERE status != 'deleted' ORDER BY name"
    ).fetchall()
    conn.close()

    def _cat_group(cat):
        return '庙会' if '庙会' in (cat or '') else '集市'

    def _strip_county(name, region):
        parts = [p.strip() for p in (region or '').split('·') if p.strip()]
        for p in reversed(parts):
            if name.startswith(p):
                name = name[len(p):]
        return name.strip()

    def _extract_lunar_days(raw):
        if not raw:
            return set()
        try:
            ot = _json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(ot, dict) and ot.get('type') == 'lunar':
                return set(ot.get('days', []))
        except Exception:
            pass
        DMAP = [('三十',30),('二十九',29),('二十八',28),('二十七',27),('二十六',26),
                ('二十五',25),('二十四',24),('二十三',23),('二十二',22),('二十一',21),
                ('二十',20),('十九',19),('十八',18),('十七',17),('十六',16),
                ('十五',15),('十四',14),('十三',13),('十二',12),('十一',11),
                ('初十',10),('初九',9),('初八',8),('初七',7),('初六',6),
                ('初五',5),('初四',4),('初三',3),('初二',2),('初一',1)]
        s = str(raw)
        days = set()
        for w, n in DMAP:
            if w in s:
                days.add(n)
                s = s.replace(w, '')
        return days

    def _county(region):
        parts = [p.strip() for p in (region or '').split('·') if p.strip()]
        return parts[-1] if parts else ''

    markets = [dict(r) for r in rows]
    # 按分类分组处理
    dup_groups = []     # [{reason, markets:[{id,name,status}]}]
    visited_ids = set()

    for i, m in enumerate(markets):
        if m['id'] in visited_ids:
            continue
        cg = _cat_group(m['category'])
        norm_i = _strip_county(m['name'], m['region'])
        county_i = _county(m['region'])
        days_i = _extract_lunar_days(m['open_time'])

        group = [m]
        reasons = []

        for j in range(i + 1, len(markets)):
            n = markets[j]
            if n['id'] in visited_ids:
                continue
            if _cat_group(n['category']) != cg:
                continue

            norm_j = _strip_county(n['name'], n['region'])
            county_j = _county(n['region'])
            days_j = _extract_lunar_days(n['open_time'])

            matched = False
            reason = None
            # 规则1：完全相同
            if m['name'] == n['name']:
                matched = True; reason = '名称完全相同'
            # 规则2：去县前缀后相同
            elif norm_i and norm_j and norm_i == norm_j:
                matched = True; reason = '去掉县级前缀后名称相同'
            # 规则3：包含关系（差≤6字）
            elif norm_i and norm_j and abs(len(norm_i) - len(norm_j)) <= 6 and (norm_i in norm_j or norm_j in norm_i):
                matched = True; reason = '名称高度相似（包含关系）'

            if matched:
                group.append(n)
                if reason not in reasons:
                    reasons.append(reason)
                visited_ids.add(n['id'])

        if len(group) > 1:
            visited_ids.add(m['id'])
            dup_groups.append({
                'reason': '、'.join(reasons),
                'cat_group': cg,
                'markets': [{'id': x['id'], 'name': x['name'],
                             'category': x['category'], 'region': x['region'],
                             'open_time': x['open_time'], 'status': x['status']} for x in group]
            })

    return jsonify({'code': 200, 'groups': dup_groups, 'total_dup': sum(len(g['markets']) for g in dup_groups)})


@app.route('/api/categories', methods=['GET'])
def list_categories():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM categories WHERE active=1 ORDER BY sort_order,id"
    ).fetchall()
    conn.close()
    return jsonify({'code': 200, 'data': [dict(r) for r in rows]})


@app.route('/api/admin/categories', methods=['GET'])
@admin_required
def admin_list_categories():
    conn = get_db()
    rows = conn.execute("SELECT * FROM categories ORDER BY sort_order,id").fetchall()
    conn.close()
    return jsonify({'code': 200, 'data': [dict(r) for r in rows]})


@app.route('/api/admin/categories', methods=['POST'])
@admin_required
def admin_add_category():
    data = request.get_json(force=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'code': 400, 'msg': '分类名称不能为空'}), 400
    icon  = data.get('icon', '🏮')
    order = int(data.get('sort_order', 99))
    conn  = get_db()
    try:
        sched = data.get('default_schedule', 'lunar')
        conn.execute(
            "INSERT INTO categories(name,icon,sort_order,default_schedule) VALUES(?,?,?,?)",
            (name, icon, order, sched))
        conn.commit()
        cat_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return jsonify({'code': 200, 'msg': '添加成功', 'data': {'id': cat_id}})
    except Exception as e:
        conn.close()
        return jsonify({'code': 400, 'msg': f'分类已存在或出错: {e}'}), 400


@app.route('/api/admin/categories/<int:cid>', methods=['PUT'])
@admin_required
def admin_update_category(cid):
    data = request.get_json(force=True) or {}
    conn = get_db()
    fields, vals = [], []
    if 'name'             in data: fields.append('name=?');             vals.append(data['name'])
    if 'icon'             in data: fields.append('icon=?');             vals.append(data['icon'])
    if 'sort_order'       in data: fields.append('sort_order=?');       vals.append(int(data['sort_order']))
    if 'active'           in data: fields.append('active=?');           vals.append(int(data['active']))
    if 'default_schedule' in data: fields.append('default_schedule=?'); vals.append(data['default_schedule'])
    if 'is_market_type'   in data: fields.append('is_market_type=?');   vals.append(int(bool(data['is_market_type'])))
    if fields:
        vals.append(cid)
        conn.execute(f"UPDATE categories SET {','.join(fields)} WHERE id=?", vals)
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '更新成功'})


@app.route('/api/admin/categories/sync', methods=['POST'])
@admin_required
def admin_sync_categories():
    """扫描 markets 表中实际用到的分类，把缺失的自动补入 categories 表"""
    conn = get_db()
    # 获取 markets 表中所有不重复的分类
    used = {r[0] for r in conn.execute(
        "SELECT DISTINCT category FROM markets WHERE category IS NOT NULL AND category != ''"
    ).fetchall()}
    # 获取已有的分类名
    existing = {r[0] for r in conn.execute("SELECT name FROM categories").fetchall()}
    added = []
    for name in used:
        if name not in existing:
            conn.execute(
                "INSERT OR IGNORE INTO categories(name, icon, sort_order, active) VALUES (?,?,99,1)",
                (name, '🏮')
            )
            added.append(name)
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'added': added, 'msg': f'同步完成，新增 {len(added)} 个分类'})


@app.route('/api/admin/categories/<int:cid>', methods=['DELETE'])
@admin_required
def admin_delete_category(cid):
    conn = get_db()
    in_use = conn.execute(
        "SELECT COUNT(*) FROM markets WHERE category=(SELECT name FROM categories WHERE id=?)", (cid,)
    ).fetchone()[0]
    if in_use > 0:
        conn.close()
        return jsonify({'code': 400, 'msg': f'该分类下有 {in_use} 个集市，无法删除，请先修改这些集市的分类'}), 400
    conn.execute("DELETE FROM categories WHERE id=?", (cid,))
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '删除成功'})


@app.route('/api/admin/categories/reorder', methods=['POST'])
@admin_required
def admin_reorder_categories():
    """接受 [{id, sort_order}] 列表，批量更新排序"""
    items = request.get_json(force=True) or []
    conn = get_db()
    for item in items:
        conn.execute("UPDATE categories SET sort_order=? WHERE id=?",
                     (item['sort_order'], item['id']))
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '排序已保存'})


@app.route('/api/markets/navigate', methods=['POST'])
def record_navigate():
    """记录用户点击导航，用于统计去过人数"""
    data = request.get_json(force=True) or {}
    market_id = data.get('market_id')
    if not market_id:
        return err('缺少market_id')
    user_id = None
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        try:
            payload = _decode_token(auth[7:])
            user_id = payload.get('user_id')
        except Exception:
            pass
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO market_visits(market_id, user_id) VALUES(?,?)",
            (market_id, user_id)
        )
        conn.commit()
    except Exception:
        pass
    visit_count = conn.execute(
        "SELECT COUNT(*) FROM market_visits WHERE market_id=?", (market_id,)
    ).fetchone()[0]
    conn.close()
    return ok({'visit_count': visit_count})


@app.route('/api/banners', methods=['GET'])
def get_banners():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM banners WHERE active=1 ORDER BY sort_order").fetchall()
    conn.close()
    return ok([dict(r) for r in rows])


@app.route('/api/notices', methods=['GET'])
def get_notices():
    today = datetime.now().strftime('%Y-%m-%d')
    conn  = get_db()
    rows  = conn.execute("""
        SELECT * FROM notices WHERE active=1
        AND (start_date='' OR start_date<=?)
        AND (end_date=''   OR end_date>=?)
        ORDER BY created_at DESC LIMIT 10
    """, (today, today)).fetchall()
    conn.close()
    return ok([dict(r) for r in rows])

# ════════════════════════════════════════════════════════════
# 爬虫接口（保留原有）
# ════════════════════════════════════════════════════════════

@app.route('/api/spider/push', methods=['POST'])
@require_api_secret
def spider_push():
    data  = request.get_json(force=True)
    items = data.get('items', [])
    if not items:
        return jsonify({'error': '无数据'}), 400
    conn = get_db()
    saved = skipped = 0
    for item in items:
        item_id = item.get('id') or str(uuid.uuid4())
        if item.get('source_url'):
            dup = conn.execute(
                "SELECT id FROM spider_queue WHERE source_url=?",
                (item['source_url'],)).fetchone()
            if dup:
                skipped += 1
                continue
        conn.execute("""
            INSERT OR IGNORE INTO spider_queue
            (id,platform,raw_title,raw_text,market_name,category,
             address,region,open_time,phone,tags,description,
             confidence,likes,source_url,status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'','pending')
        """, (
            item_id,
            item.get('platform', data.get('platform', '')),
            item.get('title', ''),
            item.get('raw_text', '')[:1000],
            item.get('market_name', ''),
            _guess_category(item.get('market_name', '') or item.get('title', ''), item.get('category', '')),
            item.get('address', ''),
            item.get('region', ''),
            item.get('open_time', ''),
            item.get('phone', ''),
            json.dumps(item.get('tags', []), ensure_ascii=False),
            item.get('description', '')[:500],
            item.get('confidence', 0),
            item.get('likes', 0),
        ))
        saved += 1
    conn.execute(
        "INSERT INTO push_logs(platform,count,ip) VALUES(?,?,?)",
        (data.get('platform', ''), saved, request.remote_addr))
    conn.commit()
    conn.close()
    return jsonify({'saved': saved, 'skipped': skipped})


@app.route('/api/spider/status', methods=['GET'])
@require_api_secret
def spider_status():
    conn    = get_db()
    pending  = conn.execute("SELECT COUNT(*) FROM spider_queue WHERE status='pending'").fetchone()[0]
    approved = conn.execute("SELECT COUNT(*) FROM spider_queue WHERE status='approved'").fetchone()[0]
    rejected = conn.execute("SELECT COUNT(*) FROM spider_queue WHERE status='rejected'").fetchone()[0]
    logs     = conn.execute(
        "SELECT platform,count,ip,pushed_at FROM push_logs ORDER BY id DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return jsonify({
        'queue': {'pending': pending, 'approved': approved, 'rejected': rejected},
        'recent_pushes': [dict(r) for r in logs],
    })

# ════════════════════════════════════════════════════════════
# 管理员接口（保留原有 + 扩展）
# ════════════════════════════════════════════════════════════

@app.route('/api/admin/queue', methods=['GET'])
@admin_required
def admin_queue():
    status = request.args.get('status', 'pending')
    conn   = get_db()
    rows   = conn.execute(
        "SELECT * FROM spider_queue WHERE status=? ORDER BY confidence DESC, likes DESC",
        (status,)).fetchall()
    conn.close()
    items = []
    for r in rows:
        d = dict(r)
        d['tags'] = _parse_tags(d.get('tags'))
        items.append(d)
    return jsonify({'items': items, 'total': len(items)})


@app.route('/api/admin/queue/<item_id>/approve', methods=['POST'])
@admin_required
def admin_approve(item_id):
    conn = get_db()
    row  = conn.execute(
        "SELECT * FROM spider_queue WHERE id=?", (item_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': '未找到'}), 404
    item      = dict(row)
    overrides = request.get_json(force=True) or {}
    cat       = overrides.get('category', item['category']) or '农村大集'
    icon_map  = {'早市':'🌅','集市':'🏮','夜市':'🌙','农贸市场':'🌾',
                 '宠物市场':'🐾','古玩市场':'🏺','花鸟市场':'🌸',
                 '二手集市':'♻️','美食集市':'🍜','跳蚤市场':'🎪'}
    market_id = make_market_id(overrides.get('region', item['region']), conn)
    conn.execute("""
        INSERT INTO markets(id,name,category,address,region,open_time,
        phone,tags,description,rating,fav_count,source,status,icon,lat,lng)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'published',?,?,?)
    """, (
        market_id,
        overrides.get('name',    item['market_name']) or item['raw_title'][:30],
        cat,
        overrides.get('address', item['address']),
        overrides.get('region',  item['region']),
        overrides.get('open_time', item['open_time']),
        overrides.get('phone',   item['phone']),
        item['tags'],
        overrides.get('description', item['description']),
        item.get('rating') or None,
        item.get('fav_count') or 0,
        f"spider_{item['platform']}",
        icon_map.get(cat, '🏮'),
        overrides.get('lat', item.get('lat')),
        overrides.get('lng', item.get('lng')),
    ))
    conn.execute(
        "UPDATE spider_queue SET status='approved' WHERE id=?", (item_id,))
    conn.commit()
    conn.close()
    name_logged = overrides.get('name', item['market_name']) or item['raw_title'][:30]
    log_action(request.current_user.get('id', 0), 'queue_approve', f'market:{market_id}', name_logged)
    return jsonify({'market_id': market_id, 'message': '已发布'})


@app.route('/api/admin/queue/<item_id>/reject', methods=['POST'])
@admin_required
def admin_reject(item_id):
    conn = get_db()
    row  = conn.execute("SELECT market_name FROM spider_queue WHERE id=?", (item_id,)).fetchone()
    conn.execute(
        "UPDATE spider_queue SET status='rejected' WHERE id=?", (item_id,))
    conn.commit()
    conn.close()
    log_action(request.current_user.get('id', 0), 'queue_reject', f'queue:{item_id}', (row[0] if row else ''))
    return jsonify({'message': '已拒绝'})


@app.route('/api/admin/queue/clear', methods=['POST'])
@admin_required
def admin_queue_clear():
    data   = request.get_json(force=True) or {}
    status = data.get('status', 'pending')   # 默认只清待审核
    conn   = get_db()
    if status == 'all':
        cnt = conn.execute("DELETE FROM spider_queue").rowcount
    else:
        cnt = conn.execute("DELETE FROM spider_queue WHERE status=?", (status,)).rowcount
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': f'已清空 {cnt} 条', 'count': cnt})


@app.route('/api/admin/clear_all_markets', methods=['POST'])
@admin_required
def admin_markets_clear_all():
    """清空所有集市相关数据：markets / spider_queue / favorites / reviews"""
    conn = get_db()
    m_cnt  = conn.execute("DELETE FROM markets").rowcount
    q_cnt  = conn.execute("DELETE FROM spider_queue").rowcount
    conn.execute("DELETE FROM favorites")
    conn.execute("DELETE FROM reviews")
    conn.commit()
    conn.close()
    uid = request.current_user.get('id', 0)
    log_action(uid, 'clear_all_markets', 'markets',
               f'清空全部集市数据：markets={m_cnt} 条，queue={q_cnt} 条')
    return jsonify({'code': 200, 'markets': m_cnt, 'queue': q_cnt,
                    'msg': f'已清空 {m_cnt} 条集市和 {q_cnt} 条待审队列'})


@app.route('/api/admin/queue/<item_id>/update', methods=['POST'])
@admin_required
def admin_queue_update(item_id):
    data = request.get_json() or {}
    conn = get_db()
    conn.execute("""
        UPDATE spider_queue SET
            market_name = ?,
            category    = ?,
            region      = ?,
            open_time   = ?,
            address     = ?,
            phone       = ?,
            lat         = ?,
            lng         = ?
        WHERE id = ?
    """, (
        data.get('market_name', ''),
        data.get('category', '农村大集'),
        data.get('region', ''),
        json.dumps(data.get('open_time', {}), ensure_ascii=False),
        data.get('address', ''),
        data.get('phone', '') or None,
        data.get('lat') or None,
        data.get('lng') or None,
        item_id
    ))
    conn.commit(); conn.close()
    return jsonify({'code': 200, 'msg': '保存成功'})


@app.route('/api/admin/queue/<item_id>/fix-time', methods=['POST'])
@admin_required
def admin_queue_fix_time(item_id):
    """快速修正队列条目的 open_time 字段（AI 核实后一键修正）"""
    data = request.get_json() or {}
    open_time = data.get('open_time', '').strip()
    if not open_time:
        return jsonify(code=400, msg='open_time 不能为空')
    conn = get_db()
    conn.execute("UPDATE spider_queue SET open_time=? WHERE id=?", (open_time, item_id))
    conn.commit(); conn.close()
    return jsonify(code=200, msg='已修正')


@app.route('/api/admin/queue/<item_id>/delete', methods=['POST'])
@admin_required
def admin_queue_delete(item_id):
    conn = get_db()
    conn.execute("DELETE FROM spider_queue WHERE id=?", (item_id,))
    conn.commit(); conn.close()
    return jsonify({'code': 200, 'msg': '已删除'})


@app.route('/api/admin/region_hierarchy', methods=['GET'])
@admin_required
def admin_region_hierarchy():
    """返回省>市>县三级地区层级，数据来源：markets表 + region_whitelist 配置合并"""
    conn = get_db()
    # 从 markets 表取所有 region
    rows = conn.execute("SELECT DISTINCT region FROM markets WHERE region IS NOT NULL AND region != ''").fetchall()
    # 从 region_whitelist 设置取
    try:
        wl = json.loads(_get_setting(conn, 'region_whitelist', '[]'))
    except Exception:
        wl = []
    conn.close()

    all_regions = set(r[0] for r in rows) | set(wl)
    hierarchy = {}   # {省: {市: [县, ...]}}
    for region in sorted(all_regions):
        parts = [p.strip() for p in region.split('·') if p.strip()]
        if not parts:
            continue
        prov = parts[0] if len(parts) > 0 else ''
        city = parts[1] if len(parts) > 1 else ''
        county = parts[2] if len(parts) > 2 else ''
        if prov not in hierarchy:
            hierarchy[prov] = {}
        if city:
            if city not in hierarchy[prov]:
                hierarchy[prov][city] = []
            if county and county not in hierarchy[prov][city]:
                hierarchy[prov][city].append(county)
    # 排序
    result = {}
    for prov in sorted(hierarchy):
        result[prov] = {}
        for city in sorted(hierarchy[prov]):
            result[prov][city] = sorted(hierarchy[prov][city])
    return jsonify({'code': 200, 'data': result})


@app.route('/api/admin/markets', methods=['GET'])
@admin_required
def admin_markets():
    status = request.args.get('status', '')   # 空=全部
    kw     = request.args.get('kw', '')
    cat    = request.args.get('cat', '')      # 分类筛选
    region = request.args.get('region', '')   # 地区筛选（省·市·县，任意层级前缀匹配）
    page   = int(request.args.get('page', 1))
    per    = int(request.args.get('per_page', 50))
    conn   = get_db()
    sql    = "SELECT * FROM markets WHERE 1=1"
    params = []
    if status:
        sql += " AND status=?"; params.append(status)
    if kw:
        sql += " AND (name LIKE ? OR address LIKE ? OR region LIKE ?)"; params += [f'%{kw}%']*3
    if cat == '其他':
        sql += " AND category NOT LIKE '%大集%' AND category NOT LIKE '%庙会%' AND category NOT LIKE '%早市%' AND category NOT LIKE '%夜市%'"
    elif cat:
        sql += " AND category LIKE ?"; params.append(f'%{cat}%')
    if region:
        sql += " AND region LIKE ?"; params.append(f'{region}%')
    total = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY created_at DESC LIMIT {per} OFFSET {(page-1)*per}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()
    items = []
    for r in rows:
        d = dict(r)
        d['tags'] = _parse_tags(d.get('tags'))
        items.append(d)
    return jsonify({'code': 200, 'data': items, 'total': total,
                    'markets': items})  # 兼容旧格式




@app.route('/api/admin/markets/<market_id>/approve', methods=['POST'])
@admin_required
def admin_approve_market(market_id):
    conn = get_db()
    conn.execute("UPDATE markets SET status='published' WHERE id=?", (market_id,))
    conn.commit()
    conn.close()
    log_action(request.current_user.get('id',0), 'approve_market', market_id, '审核通过')
    return jsonify({'code': 200, 'msg': '已通过'})


@app.route('/api/admin/markets/<market_id>/reject', methods=['POST'])
@admin_required
def admin_reject_market(market_id):
    data   = request.get_json(force=True) or {}
    reason = data.get('reason', '内容不符合发布规范')
    conn   = get_db()
    conn.execute("UPDATE markets SET status='rejected' WHERE id=?", (market_id,))
    conn.commit()
    conn.close()
    log_action(request.current_user.get('id',0), 'reject_market', market_id, f'拒绝:{reason}')
    return jsonify({'code': 200, 'msg': '已拒绝'})

@app.route('/api/admin/markets', methods=['POST'])
@admin_required
def admin_add_market():
    data      = request.get_json(force=True)
    cat       = data.get('category', '集市')
    icon_map  = {'早市':'🌅','集市':'🏮','夜市':'🌙','农贸市场':'🌾',
                 '宠物市场':'🐾','古玩市场':'🏺','花鸟市场':'🌸',
                 '二手集市':'♻️','美食集市':'🍜','跳蚤市场':'🎪'}
    conn = get_db()
    market_id = make_market_id(data.get('region', ''), conn)
    conn.execute("""
        INSERT INTO markets(id,name,category,address,region,open_time,
        phone,tags,description,rating,lat,lng,source,status,icon,bg)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'manual','published',?,?)
    """, (
        market_id, data['name'], cat,
        data.get('address',''), data.get('region',''),
        json.dumps(data.get('openTime', data.get('open_time', {})),
                   ensure_ascii=False),
        data.get('phone',''),
        json.dumps(data.get('tags',[]), ensure_ascii=False),
        data.get('description', data.get('desc','')),
        data.get('rating') or None,
        data.get('lat'), data.get('lng'),
        icon_map.get(cat, '🏮'),
        data.get('bg',''),
    ))
    conn.commit()
    conn.close()
    _log('add_market', f'market:{market_id}', data['name'])
    return jsonify({'market_id': market_id, 'message': '已添加'})


@app.route('/api/admin/markets/<market_id>', methods=['GET'])
@admin_required
def admin_get_market(market_id):
    conn = get_db()
    row  = conn.execute("SELECT * FROM markets WHERE id=?", (market_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'code': 404, 'msg': '集市不存在'}), 404
    d = dict(row)
    for k in ('tags', 'open_time'):
        if d.get(k):
            try: d[k] = json.loads(d[k])
            except: pass
    return jsonify({'code': 200, 'data': d})


@app.route('/api/admin/markets/<market_id>', methods=['PUT'])
@admin_required
def admin_update_market(market_id):
    data   = request.get_json(force=True)
    fields = {k: v for k, v in data.items()
              if k in ('name','category','address','region','open_time',
                       'phone','description','rating','status','lat','lng',
                       'icon','bg','tags')}
    if 'tags' in fields and isinstance(fields['tags'], (list, dict)):
        fields['tags'] = json.dumps(fields['tags'], ensure_ascii=False)
    if 'openTime' in data:
        fields['open_time'] = json.dumps(data['openTime'], ensure_ascii=False)
    if 'open_time' in fields and isinstance(fields['open_time'], (dict, list)):
        fields['open_time'] = json.dumps(fields['open_time'], ensure_ascii=False)
    fields['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sets   = ', '.join(f"{k}=?" for k in fields)
    params = list(fields.values()) + [market_id]
    conn   = get_db()
    conn.execute(f"UPDATE markets SET {sets} WHERE id=?", params)
    conn.commit()
    conn.close()
    _log('update_market', f'market:{market_id}')
    return jsonify({'message': '已更新'})


@app.route('/api/admin/markets/<market_id>', methods=['DELETE'])
@admin_required
def admin_delete_market(market_id):
    hard = request.args.get('hard', '0') == '1'
    conn = get_db()
    if hard:
        conn.execute("DELETE FROM markets WHERE id=?", (market_id,))
        msg = '已彻底删除'
    else:
        conn.execute("UPDATE markets SET status='hidden' WHERE id=?", (market_id,))
        msg = '已下架'
    conn.commit()
    conn.close()
    _log('delete_market', f'market:{market_id} hard={hard}')
    return jsonify({'code': 200, 'message': msg})


@app.route('/api/admin/markets/<market_id>/overwrite-from-queue/<queue_id>', methods=['POST'])
@admin_required
def admin_market_overwrite_from_queue(market_id, queue_id):
    """用采集队列某条数据覆盖已有集市（名称/开集时间/地址/描述/坐标等），并将队列项标记为已处理。"""
    conn = get_db()
    q = conn.execute("SELECT * FROM spider_queue WHERE id=?", (queue_id,)).fetchone()
    if not q:
        conn.close()
        return jsonify(code=404, msg='队列项不存在')
    q = dict(q)
    fields = {}
    if q.get('market_name') or q.get('raw_title'):
        fields['name'] = q.get('market_name') or q.get('raw_title')
    if q.get('category'):  fields['category'] = q['category']
    if q.get('address'):   fields['address']   = q['address']
    if q.get('region'):    fields['region']    = q['region']
    if q.get('open_time'): fields['open_time'] = q['open_time']
    if q.get('phone'):     fields['phone']     = q['phone']
    if q.get('description'): fields['description'] = q['description']
    if q.get('lat'):       fields['lat']       = q['lat']
    if q.get('lng'):       fields['lng']       = q['lng']
    fields['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sets   = ', '.join(f"{k}=?" for k in fields)
    params = list(fields.values()) + [market_id]
    conn.execute(f"UPDATE markets SET {sets} WHERE id=?", params)
    conn.execute("UPDATE spider_queue SET status='approved' WHERE id=?", (queue_id,))
    conn.commit()
    conn.close()
    _log('overwrite_market', f'market:{market_id} from queue:{queue_id}')
    return jsonify(code=200, msg='已覆盖')


@app.route('/api/admin/stats', methods=['GET'])
@admin_required
def admin_stats():
    conn = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    # 本周每天发布量（最近7天）
    weekly = []
    for i in range(6, -1, -1):
        day = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        cnt = conn.execute("SELECT COUNT(*) FROM markets WHERE created_at LIKE ?",
                           (f'{day}%',)).fetchone()[0]
        weekly.append(cnt)
    # 最新动态：最近用户注册 + 集市提交/审核
    recent = []
    for row in conn.execute("SELECT nickname, created_at FROM users ORDER BY created_at DESC LIMIT 4").fetchall():
        recent.append({'type': 'user', 'text': f'用户 {row[0]} 注册了账号', 'time': row[1][:16]})
    for row in conn.execute("SELECT name, status, updated_at, created_at FROM markets ORDER BY created_at DESC LIMIT 4").fetchall():
        label = '提交了集市信息' if row[1] == 'pending' else ('审核通过' if row[1] == 'published' else '集市被拒绝')
        recent.append({'type': row[1], 'text': f'{row[0]} {label}', 'time': (row[2] or row[3] or '')[:16]})
    recent.sort(key=lambda x: x['time'], reverse=True)
    data = {
        'totalMarkets':   conn.execute("SELECT COUNT(*) FROM markets WHERE status='published'").fetchone()[0],
        'pendingMarkets': conn.execute("SELECT COUNT(*) FROM markets WHERE status='pending'").fetchone()[0],
        'totalUsers':     conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        'todayNewUsers':  conn.execute("SELECT COUNT(*) FROM users WHERE created_at LIKE ?",
                                       (f'{today}%',)).fetchone()[0],
        'totalReviews':   conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0],
        'pendingReviews': conn.execute("SELECT COUNT(*) FROM reviews WHERE status='pending'").fetchone()[0],
        'pendingQueue':    conn.execute("SELECT COUNT(*) FROM spider_queue WHERE status='pending'").fetchone()[0],
        'totalRegions':    conn.execute("SELECT COUNT(DISTINCT region) FROM markets WHERE region!='' AND status='published'").fetchone()[0],
        'pendingFeedbacks': conn.execute("SELECT COUNT(*) FROM feedbacks WHERE status='pending'").fetchone()[0],
        'weeklyMarkets':  weekly,
        'recentActivity': recent[:6],
    }
    # 地区分布（按省份/城市统计，取集市 region 字段第一段）
    region_rows = conn.execute("""
        SELECT
            CASE
                WHEN region LIKE '%·%' THEN substr(region,1,instr(region,'·')-1)
                ELSE region
            END as prov,
            COUNT(*) as cnt
        FROM markets
        WHERE status='published' AND region!='' AND region IS NOT NULL
        GROUP BY prov ORDER BY cnt DESC LIMIT 12
    """).fetchall()
    data['regionDistribution'] = [{'name': r[0], 'count': r[1]} for r in region_rows]

    # 分类统计
    cat_rows = conn.execute(
        "SELECT category, COUNT(*) as cnt FROM markets WHERE status='published' AND category!='' GROUP BY category"
    ).fetchall()
    data['categoryStats'] = [{'name': r[0], 'count': r[1]} for r in cat_rows]

    conn.close()
    return ok(data)


@app.route('/api/admin/users', methods=['GET'])
@admin_required
def admin_users():
    kw      = request.args.get('kw', '')
    page    = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 20)), 100)
    conn    = get_db()
    sql     = "SELECT * FROM users"
    params  = []
    if kw:
        sql += " WHERE phone LIKE ? OR nickname LIKE ?"
        params = [f'%{kw}%', f'%{kw}%']
    total = conn.execute(
        f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY created_at DESC LIMIT {per_page} OFFSET {(page-1)*per_page}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()
    users = [{k: row[k] for k in ('id','uid','phone','nickname','avatar',
                                   'role','status','gender','region','created_at','last_login')}
             for row in rows]
    return ok({'list': users, 'total': total})


@app.route('/api/admin/users/<int:uid>/ban', methods=['POST'])
@admin_required
def ban_user(uid):
    conn = get_db()
    row  = conn.execute("SELECT role FROM users WHERE id=?", (uid,)).fetchone()
    if not row:
        conn.close()
        return err('用户不存在', 404)
    if row['role'] == 'superadmin':
        conn.close()
        return err('不能封禁超级管理员')
    conn.execute("UPDATE users SET status='banned' WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    _log('ban_user', f'user:{uid}')
    return ok()


@app.route('/api/admin/users/<int:uid>/unban', methods=['POST'])
@admin_required
def unban_user(uid):
    conn = get_db()
    conn.execute("UPDATE users SET status='normal' WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    _log('unban_user', f'user:{uid}')
    return ok()


@app.route('/api/admin/users/<int:uid>', methods=['PUT'])
@admin_required
def admin_update_user(uid):
    data = request.get_json(force=True) or {}
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        conn.close()
        return err('用户不存在', 404)
    user = dict(user)

    nickname = (data.get('nickname') or '').strip()
    if not nickname:
        conn.close()
        return err('昵称不能为空')

    new_role = data.get('role', user['role'])
    # 非超级管理员不能修改超管角色
    if user['role'] == 'superadmin' and request.current_user.get('role') != 'superadmin':
        new_role = 'superadmin'

    interests = data.get('interests', json.loads(user.get('interests') or '[]'))
    if isinstance(interests, list):
        interests = json.dumps(interests, ensure_ascii=False)

    fields = {
        'nickname':   nickname,
        'role':       new_role,
        'gender':     data.get('gender',     user['gender'] or ''),
        'region':     data.get('region',     user['region'] or '').strip(),
        'bio':        data.get('bio',        user['bio'] or '').strip(),
        'birth_year': data.get('birth_year', user['birth_year'] or ''),
        'interests':  interests,
        'email':      data.get('email',      user['email'] or '').strip(),
        'status':     data.get('status',     user['status']),
    }

    new_pwd = (data.get('password') or '').strip()
    if new_pwd:
        fields['password'] = _hash(new_pwd)

    set_clause = ', '.join(f'{k}=?' for k in fields)
    conn.execute(f"UPDATE users SET {set_clause} WHERE id=?",
                 list(fields.values()) + [uid])
    conn.commit()
    conn.close()
    log_action(request.current_user.get('id', 0), 'update_user', f'user:{uid}',
               f'修改用户 {nickname}')
    return ok({'id': uid, 'nickname': nickname})


@app.route('/api/admin/reviews', methods=['GET'])
@admin_required
def admin_reviews():
    status   = request.args.get('status', 'pending')
    page     = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 20)), 100)
    conn     = get_db()
    sql      = """SELECT r.*, u.nickname, u.phone, m.name as market_name
                  FROM reviews r
                  LEFT JOIN users u ON u.id=r.user_id
                  LEFT JOIN markets m ON m.id=r.market_id
                  WHERE r.status=?"""
    total    = conn.execute(
        f"SELECT COUNT(*) FROM ({sql})", (status,)).fetchone()[0]
    sql     += f" ORDER BY r.created_at DESC LIMIT {per_page} OFFSET {(page-1)*per_page}"
    rows     = conn.execute(sql, (status,)).fetchall()
    conn.close()
    result   = []
    for r in rows:
        d = dict(r)
        d['images'] = json.loads(d.get('images') or '[]')
        d['tags']   = json.loads(d.get('tags')   or '[]')
        result.append(d)
    return ok({'list': result, 'total': total})


@app.route('/api/admin/reviews/<int:rid>/approve', methods=['POST'])
@admin_required
def approve_review(rid):
    conn = get_db()
    r    = conn.execute("SELECT * FROM reviews WHERE id=?", (rid,)).fetchone()
    if not r:
        conn.close()
        return err('未找到', 404)
    conn.execute("UPDATE reviews SET status='approved' WHERE id=?", (rid,))
    # 重新计算评分
    market_id = r['market_id']
    approved  = conn.execute(
        "SELECT rating FROM reviews WHERE market_id=? AND status='approved'",
        (market_id,)).fetchall()
    if approved:
        avg = sum(x['rating'] for x in approved) / len(approved)
        conn.execute("UPDATE markets SET rating=?, review_count=? WHERE id=?",
                     (round(avg, 1), len(approved), market_id))
    conn.commit()
    conn.close()
    return ok()


@app.route('/api/admin/reviews/<int:rid>/reject', methods=['POST'])
@admin_required
def reject_review(rid):
    conn = get_db()
    conn.execute("UPDATE reviews SET status='rejected' WHERE id=?", (rid,))
    conn.commit()
    conn.close()
    return ok()


@app.route('/api/admin/banners', methods=['GET'])
@admin_required
def admin_get_banners():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM banners ORDER BY sort_order").fetchall()
    conn.close()
    return ok([dict(r) for r in rows])


@app.route('/api/admin/banners', methods=['POST'])
@admin_required
def admin_add_banner():
    data = request.get_json(force=True) or {}
    conn = get_db()
    conn.execute(
        "INSERT INTO banners(title,image_url,link_url,sort_order) VALUES(?,?,?,?)",
        (data.get('title',''), data.get('imageUrl',''),
         data.get('linkUrl',''), data.get('sortOrder',0)))
    conn.commit()
    conn.close()
    return ok()


@app.route('/api/admin/banners/<int:bid>', methods=['DELETE'])
@admin_required
def admin_delete_banner(bid):
    conn = get_db()
    conn.execute("DELETE FROM banners WHERE id=?", (bid,))
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '删除成功'})


@app.route('/api/admin/notices', methods=['GET'])
@admin_required
def admin_list_notices():
    conn  = get_db()
    rows  = conn.execute("SELECT * FROM notices ORDER BY id DESC").fetchall()
    conn.close()
    items = [dict(r) for r in rows]
    return jsonify({'code': 200, 'data': items, 'notices': items})


@app.route('/api/admin/notices', methods=['POST'])
@admin_required
def admin_add_notice():
    data = request.get_json(force=True) or {}
    conn = get_db()
    conn.execute(
        "INSERT INTO notices(title,content,type,start_date,end_date) VALUES(?,?,?,?,?)",
        (data.get('title',''), data.get('content',''),
         data.get('type','info'), data.get('startDate',''), data.get('endDate','')))
    nid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '添加成功', 'data': {'id': nid}})


@app.route('/api/admin/notices/<int:nid>', methods=['DELETE'])
@admin_required
def admin_delete_notice(nid):
    conn = get_db()
    conn.execute("DELETE FROM notices WHERE id=?", (nid,))
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '删除成功'})


@app.route('/api/admin/logs', methods=['GET'])
@admin_required
def admin_logs():
    page    = int(request.args.get('page', 1))
    per     = min(int(request.args.get('per_page', 50)), 200)
    action  = request.args.get('action', '')   # 操作类型筛选
    kw      = request.args.get('kw', '')        # 关键词（目标/详情）
    conn    = get_db()
    sql     = "SELECT * FROM operation_logs WHERE 1=1"
    params  = []
    if action:
        sql += " AND action=?"; params.append(action)
    if kw:
        sql += " AND (target LIKE ? OR detail LIKE ? OR action LIKE ?)"; params += [f'%{kw}%']*3
    total = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY created_at DESC LIMIT {per} OFFSET {(page-1)*per}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()
    return ok({'logs': [dict(r) for r in rows], 'total': total, 'page': page, 'per': per})


@app.route('/api/admin/logs/clear', methods=['POST'])
@admin_required
def admin_logs_clear():
    conn = get_db()
    cnt  = conn.execute("SELECT COUNT(*) FROM operation_logs").fetchone()[0]
    conn.execute("DELETE FROM operation_logs")
    conn.commit()
    conn.close()
    # 记录一条"清空日志"操作（操作人自己）
    log_action(request.current_user.get('id', 0), 'clear_logs', '', f'共清除 {cnt} 条日志')
    return ok({'msg': f'已清空 {cnt} 条日志', 'deleted': cnt})


@app.route('/api/admin/logs/action_types', methods=['GET'])
@admin_required
def admin_log_action_types():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT action FROM operation_logs ORDER BY action").fetchall()
    conn.close()
    return ok([r[0] for r in rows])


# ════════════════════════════════════════════════════════════
# 文件上传接口
# ════════════════════════════════════════════════════════════
import base64, mimetypes
from werkzeug.utils import secure_filename

UPLOAD_DIR   = os.path.join(os.path.dirname(__file__), 'static', 'uploads')
ALLOWED_IMG  = {'jpg','jpeg','png','gif','webp'}
ALLOWED_VID  = {'mp4','mov','m4v'}
MAX_IMG_SIZE = 10 * 1024 * 1024   # 10MB
MAX_VID_SIZE = 50 * 1024 * 1024   # 50MB

@app.route('/api/upload', methods=['POST'])
@login_required
def upload_file():
    if 'file' not in request.files:
        return err('未找到文件')
    f    = request.files['file']
    ftype = request.form.get('type', 'image')
    if not f.filename:
        return err('文件名为空')

    ext = f.filename.rsplit('.', 1)[-1].lower() if '.' in f.filename else ''
    if ftype == 'image' and ext not in ALLOWED_IMG:
        return err(f'不支持的图片格式，支持：{", ".join(ALLOWED_IMG)}')
    if ftype == 'video' and ext not in ALLOWED_VID:
        return err(f'不支持的视频格式，支持：{", ".join(ALLOWED_VID)}')

    # 读取并检查大小
    data = f.read()
    max_size = MAX_IMG_SIZE if ftype == 'image' else MAX_VID_SIZE
    if len(data) > max_size:
        return err(f'文件过大，最大 {max_size // (1024*1024)}MB')

    # 保存文件
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    filename = f'{uuid.uuid4().hex}.{ext}'
    filepath = os.path.join(UPLOAD_DIR, filename)
    with open(filepath, 'wb') as out:
        out.write(data)

    url = f'/static/uploads/{filename}'
    return ok({'url': url, 'filename': filename, 'size': len(data)})

# ════════════════════════════════════════════════════════════
# 健康检查 & 静态文件
# ════════════════════════════════════════════════════════════

@app.route('/api/health', methods=['GET'])
def health():
    return ok({'status': 'ok', 'time': datetime.now().isoformat()})


@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'index.html')

@app.route('/app')
def app_redirect():
    from flask import redirect
    return redirect('/app/index.html')

@app.route('/<path:path>')
def static_files(path):
    try:
        return send_from_directory(STATIC_DIR, path)
    except Exception:
        # app/ 路径下的 404 回退到新版首页，其余回退到旧版
        if path.startswith('app/'):
            return send_from_directory(os.path.join(STATIC_DIR, 'app'), 'index.html')
        return send_from_directory(STATIC_DIR, 'index.html')


# ════════════════════════════════════════════════════════════
# 帮助与反馈
# ════════════════════════════════════════════════════════════

@app.route('/api/feedback', methods=['POST'])
def submit_feedback():
    data    = request.get_json(force=True)
    content = (data.get('content') or '').strip()
    if not content:
        return jsonify({'code': 400, 'msg': '反馈内容不能为空'}), 400
    fb_type = data.get('type', 'other')
    contact = (data.get('contact') or '')[:100]
    images  = json.dumps(data.get('images') or [], ensure_ascii=False)
    user_id = None
    nickname = ''
    token_user = _get_user_from_token()
    if token_user:
        user_id  = token_user['id']
        nickname = token_user.get('nickname', '')
    conn = get_db()
    conn.execute(
        "INSERT INTO feedbacks(type,content,contact,images,user_id,nickname,ip) VALUES(?,?,?,?,?,?,?)",
        (fb_type, content, contact, images, user_id, nickname, request.remote_addr)
    )
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '感谢您的反馈！'})


@app.route('/api/admin/feedbacks', methods=['GET'])
@admin_required
def admin_list_feedbacks():
    status = request.args.get('status', '')
    page   = int(request.args.get('page', 1))
    per    = int(request.args.get('per_page', 50))
    conn   = get_db()
    sql    = "SELECT * FROM feedbacks WHERE 1=1"
    params = []
    if status:
        sql += " AND status=?"; params.append(status)
    total = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY created_at DESC LIMIT {per} OFFSET {(page-1)*per}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()
    items = [dict(r) for r in rows]
    pending = conn.execute("SELECT COUNT(*) FROM feedbacks WHERE status='pending'").fetchone()[0] if False else 0
    conn2 = get_db()
    pending = conn2.execute("SELECT COUNT(*) FROM feedbacks WHERE status='pending'").fetchone()[0]
    conn2.close()
    return jsonify({'code': 200, 'data': items, 'total': total, 'pending': pending})


@app.route('/api/admin/feedbacks/<int:fb_id>', methods=['PUT'])
@admin_required
def admin_update_feedback(fb_id):
    data   = request.get_json(force=True)
    status = data.get('status', 'handled')
    reply  = data.get('reply', '')
    conn   = get_db()
    conn.execute(
        "UPDATE feedbacks SET status=?, reply=?, handled_at=? WHERE id=?",
        (status, reply, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fb_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'code': 200, 'msg': '已更新'})


# ════════════════════════════════════════════════════════════
# 管理端：提醒列表（供推送使用）
# ════════════════════════════════════════════════════════════

@app.route('/api/admin/reminders', methods=['GET'])
@admin_required
def admin_get_reminders():
    """管理端：查看所有活跃提醒，供发送通知使用"""
    conn  = get_db()
    remind_type = request.args.get('remind_type', '')  # 'once'/'recurring'/''
    sql = """
        SELECT r.id, r.remind_type, r.status, r.created_at, r.updated_at,
               u.id AS user_id, u.nickname, u.phone, u.wx_openid, u.mp_openid,
               m.id AS market_id, m.name AS market_name, m.category,
               m.region, m.open_time
        FROM market_reminders r
        JOIN users u ON u.id = r.user_id
        JOIN markets m ON m.id = r.market_id
        WHERE r.status='active'
    """
    params = []
    if remind_type:
        sql += " AND r.remind_type=?"; params.append(remind_type)
    sql += " ORDER BY r.created_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return ok({'list': [dict(r) for r in rows], 'total': len(rows)})


@app.route('/api/admin/reminders/<int:rid>/mark-sent', methods=['POST'])
@admin_required
def admin_mark_reminder_sent(rid):
    """将一次性提醒标记为已触发"""
    conn = get_db()
    conn.execute(
        "UPDATE market_reminders SET status='triggered', updated_at=datetime('now','localtime') WHERE id=? AND remind_type='once'",
        (rid,)
    )
    conn.commit()
    conn.close()
    return ok({'done': True})


# ════════════════════════════════════════════════════════════
# 高德 POI 导入
# ════════════════════════════════════════════════════════════

@app.route('/api/admin/amap-poi/search', methods=['POST'])
@admin_required
def amap_poi_search():
    import urllib.request, urllib.parse
    data    = request.get_json(force=True)
    keyword = (data.get('keyword') or '集市').strip()
    city    = (data.get('city') or '').strip()
    page    = max(1, int(data.get('page', 1)))
    amap_key = (data.get('key') or '').strip()
    if not amap_key:
        return jsonify({'code': 400, 'msg': '请先在设置中填写高德Web服务API Key'}), 400
    params = urllib.parse.urlencode({
        'key': amap_key, 'keywords': keyword, 'city': city,
        'offset': 25, 'page': page, 'extensions': 'all', 'output': 'json',
    })
    url = f'https://restapi.amap.com/v3/place/text?{params}'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Python/zhaojishi'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return jsonify({'code': 500, 'msg': f'高德API请求失败: {e}'}), 500
    if result.get('status') != '1':
        return jsonify({'code': 400, 'msg': f"高德API错误: {result.get('info','未知错误')} ({result.get('infocode','')})"}), 400
    pois = result.get('pois', [])
    # 查询已在队列或集市库中的 amap id 和名称
    conn = get_db()
    _amap_count_today(conn); conn.commit()   # 记录本次调用
    poi_ids = [p.get('id','') for p in pois if p.get('id')]
    existing_ids  = set()
    existing_names = set()
    if poi_ids:
        placeholders = ','.join('?' * len(poi_ids))
        amap_ids = [f'amap_{i}' for i in poi_ids]
        rows = conn.execute(
            f"SELECT id FROM spider_queue WHERE id IN ({placeholders})",
            amap_ids).fetchall()
        existing_ids = {r['id'].replace('amap_','') for r in rows}
        rows2 = conn.execute("SELECT name FROM markets WHERE status!='hidden'").fetchall()
        existing_names = {r['name'] for r in rows2}
    conn.close()
    items = []
    for p in pois:
        loc = p.get('location', '').split(',')
        try:   lng, lat = float(loc[0]), float(loc[1])
        except: lng, lat = None, None
        province = p.get('pname', '')
        city_n   = p.get('cityname', '')
        district = p.get('adname', '')
        region_parts = []
        for x in [province, city_n, district]:
            if x and x not in region_parts: region_parts.append(x)
        pid  = p.get('id', '')
        name = p.get('name', '')
        tel  = p.get('tel', '') or ''
        if isinstance(tel, list):  tel  = ';'.join(tel)
        ptype = p.get('type', '') or ''
        if isinstance(ptype, list): ptype = ptype[0] if ptype else ''
        # 高德评分（extensions=all 时在 biz_ext.rating，5分制）
        biz = p.get('biz_ext') or {}
        if isinstance(biz, list): biz = biz[0] if biz else {}
        try:    amap_rating = float(biz.get('rating') or 0) or None
        except: amap_rating = None
        items.append({
            'id': pid,
            'name': name,
            'address': p.get('address', '') or '',
            'tel': tel,
            'type': ptype,
            'lng': lng, 'lat': lat,
            'region': '·'.join(region_parts),
            'rating': amap_rating,
            'existing': pid in existing_ids or name in existing_names,
        })
    return jsonify({'code': 200, 'data': items,
                    'count': int(result.get('count', 0)), 'page': page})


_sync_jobs = {}   # 内存存储同步任务状态

@app.route('/api/admin/markets/sync-ratings', methods=['POST'])
@admin_required
def sync_market_ratings():
    """启动后台线程批量同步高德评分，立即返回 job_id"""
    import threading
    data     = request.get_json(force=True) or {}
    amap_key = (data.get('key') or '').strip()
    if not amap_key:
        return jsonify({'code': 400, 'msg': '请提供高德Web服务Key'}), 400
    job_id = uuid.uuid4().hex[:12]
    _sync_jobs[job_id] = {'status': 'running', 'done': 0, 'total': 0,
                          'updated': 0, 'not_found': 0, 'skipped': 0, 'msg': ''}

    def _run():
        import urllib.request, urllib.parse, time as _time
        conn    = get_db()
        markets = conn.execute(
            "SELECT id, name, region FROM markets WHERE status='published'"
        ).fetchall()
        _sync_jobs[job_id]['total'] = len(markets)
        updated = skipped = not_found = 0
        for m in markets:
            name  = m['name']
            parts = (m['region'] or '').split('·')
            city  = parts[1] if len(parts) > 1 else (parts[0] if parts else '')
            try:
                params = urllib.parse.urlencode({
                    'key': amap_key, 'keywords': name, 'city': city,
                    'offset': 1, 'page': 1, 'extensions': 'all', 'output': 'json',
                })
                req = urllib.request.Request(
                    f'https://restapi.amap.com/v3/place/text?{params}',
                    headers={'User-Agent': 'Python/zhaojishi'})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                pois = result.get('pois', [])
                amap_rating = None
                for p in pois:
                    if p.get('name', '') == name:
                        biz = p.get('biz_ext') or {}
                        if isinstance(biz, list): biz = biz[0] if biz else {}
                        try:    amap_rating = float(biz.get('rating') or 0) or None
                        except: amap_rating = None
                        break
                if amap_rating is None and pois:
                    biz = pois[0].get('biz_ext') or {}
                    if isinstance(biz, list): biz = biz[0] if biz else {}
                    try:    amap_rating = float(biz.get('rating') or 0) or None
                    except: amap_rating = None
                conn.execute("UPDATE markets SET rating=? WHERE id=?", (amap_rating, m['id']))
                conn.commit()
                if amap_rating: updated += 1
                else:           not_found += 1
            except Exception:
                skipped += 1
            _sync_jobs[job_id]['done']      = updated + not_found + skipped
            _sync_jobs[job_id]['updated']   = updated
            _sync_jobs[job_id]['not_found'] = not_found
            _sync_jobs[job_id]['skipped']   = skipped
            _time.sleep(0.12)
        conn.close()
        _sync_jobs[job_id]['status'] = 'done'
        _sync_jobs[job_id]['msg'] = f'{updated} 条获得评分，{not_found} 条无评分已置空，{skipped} 条请求失败'

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'code': 200, 'job_id': job_id})


@app.route('/api/admin/markets/sync-ratings/status', methods=['GET'])
@admin_required
def sync_ratings_status():
    job_id = request.args.get('job_id', '')
    job    = _sync_jobs.get(job_id)
    if not job:
        return jsonify({'code': 404, 'msg': '任务不存在'}), 404
    return jsonify({'code': 200, 'data': job})


def _amap_count_today(conn):
    """返回今日高德 API 调用次数，并自增1"""
    today = datetime.now().strftime('%Y-%m-%d')
    row   = conn.execute("SELECT value FROM app_settings WHERE key='amap_call_date'").fetchone()
    if not row or row[0] != today:
        conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('amap_call_date',?)", (today,))
        conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('amap_call_count','0')")
    cnt = int(conn.execute("SELECT value FROM app_settings WHERE key='amap_call_count'").fetchone()[0] or 0)
    cnt += 1
    conn.execute("INSERT OR REPLACE INTO app_settings(key,value) VALUES('amap_call_count',?)", (str(cnt),))
    return cnt


@app.route('/api/admin/amap-key/check', methods=['POST'])
@admin_required
def amap_key_check():
    import urllib.request, urllib.parse
    data     = request.get_json(force=True) or {}
    amap_key = (data.get('key') or '').strip()
    if not amap_key:
        return jsonify({'code': 400, 'msg': '请填写Key'}), 400
    # 用一次极简请求验证 Key
    params = urllib.parse.urlencode({'key': amap_key, 'keywords': '市场', 'city': '北京',
                                     'offset': 1, 'page': 1, 'extensions': 'base', 'output': 'json'})
    try:
        req = urllib.request.Request(f'https://restapi.amap.com/v3/place/text?{params}',
                                     headers={'User-Agent': 'Python/zhaojishi'})
        with urllib.request.urlopen(req, timeout=6) as resp:
            result = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return jsonify({'code': 500, 'msg': f'网络请求失败：{e}'}), 500

    conn  = get_db()
    cnt   = _amap_count_today(conn)
    conn.commit()
    conn.close()

    infocode = result.get('infocode', '')
    status   = result.get('status', '0')
    info     = result.get('info', '')
    code_map = {
        '10000': ('valid',   '✅ Key有效'),
        '10001': ('invalid', '❌ Key无效或已过期'),
        '10002': ('invalid', '❌ Key无权限（未开通Web服务）'),
        '10003': ('quota',   '⚠️ 访问过于频繁，请稍后再试'),
        '10004': ('quota',   '⚠️ 今日配额已用尽（免费5000次/天）'),
        '10009': ('invalid', '❌ 请求来源IP不在白名单，请在高德控制台配置'),
    }
    key_status, msg = code_map.get(infocode, ('unknown', f'未知状态：{info}（{infocode}）'))
    return jsonify({'code': 200, 'data': {
        'valid': key_status == 'valid',
        'key_status': key_status,
        'msg': msg,
        'infocode': infocode,
        'today_calls': cnt,
        'daily_limit': 5000,
        'remaining': max(0, 5000 - cnt),
    }})


@app.route('/api/admin/amap-poi/import', methods=['POST'])
@admin_required
def amap_poi_import():
    data = request.get_json(force=True)
    pois = data.get('pois', [])
    if not pois:
        return jsonify({'code': 400, 'msg': '没有选中数据'}), 400
    conn    = get_db()
    success = skip = 0
    now     = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for p in pois:
        name = (p.get('name') or '').strip()
        if not name: continue
        qid = f"amap_{p.get('id') or uuid.uuid4().hex[:12]}"
        if conn.execute("SELECT 1 FROM spider_queue WHERE id=?", (qid,)).fetchone():
            skip += 1; continue
        tel = p.get('tel', '') or ''
        if isinstance(tel, list): tel = ';'.join(tel)
        addr = p.get('address', '') or ''
        if isinstance(addr, list): addr = ' '.join(addr)
        region = p.get('region', '') or ''
        src = p.get('type', '') or ''
        if isinstance(src, list): src = src[0] if src else ''
        cat    = _guess_category(name)
        rating = p.get('rating') or None
        try:    rating = float(rating) if rating else None
        except: rating = None
        conn.execute("""
            INSERT INTO spider_queue
              (id,platform,raw_title,market_name,category,address,region,phone,tags,
               confidence,lat,lng,source,status,pushed_at,rating)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (qid, '高德POI', name, name, cat,
              addr, region, tel,
              '[]', 90, p.get('lat'), p.get('lng'), src,
              'pending', now, rating))
        success += 1
    conn.commit(); conn.close()
    msg = f'导入 {success} 条' + (f'，跳过 {skip} 条重复' if skip else '')
    return jsonify({'code': 200, 'msg': msg, 'success': success, 'skip': skip})


@app.route('/api/admin/amap-poi/queue', methods=['GET'])
@admin_required
def amap_poi_queue():
    status = request.args.get('status', 'pending')
    kw     = request.args.get('kw', '')
    region = request.args.get('region', '')
    page   = int(request.args.get('page', 1))
    per    = min(int(request.args.get('per', 30)), 500)
    conn   = get_db()
    sql    = "SELECT * FROM spider_queue WHERE 1=1"
    params = []
    if status != 'all':
        sql += " AND status=?"; params.append(status)
    if kw:
        sql += " AND (market_name LIKE ? OR address LIKE ? OR region LIKE ?)"; params += [f'%{kw}%']*3
    if region:
        sql += " AND region LIKE ?"; params.append(f'%{region}%')
    total = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params).fetchone()[0]
    sql  += f" ORDER BY pushed_at DESC LIMIT {per} OFFSET {(page-1)*per}"
    rows  = conn.execute(sql, params).fetchall()
    conn.close()
    items = []
    for r in rows:
        d = dict(r)
        d['tags'] = _parse_tags(d.get('tags'))
        items.append(d)
    return jsonify({'code': 200, 'data': items, 'total': total, 'page': page, 'per': per})


# ── 全局错误处理 ──────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    if request.path.startswith('/api/'):
        return jsonify({'code': 404, 'message': '接口不存在'}), 404
    return send_from_directory('.', 'index.html'), 404

@app.errorhandler(500)
def server_error(e):
    logger.error('500 Internal Server Error: %s', e, exc_info=True)
    return jsonify({'code': 500, 'message': '服务器内部错误'}), 500

@app.errorhandler(Exception)
def unhandled(e):
    logger.error('Unhandled exception: %s', e, exc_info=True)
    return jsonify({'code': 500, 'message': '服务器内部错误'}), 500

# ── 启动初始化 ────────────────────────────────────────────────
# 无论用 python app.py 还是 gunicorn 启动，都确保数据库初始化
with app.app_context():
    init_db()

# ════════════════════════════════════════════════════════════
if __name__ == '__main__':
    logger.info('找个大集服务器 v2.0 已启动  http://0.0.0.0:5000')
    app.run(host='0.0.0.0', port=5000, debug=False)
