import os
import re
import json
import time
import uuid
import hmac
import logging
import datetime
import threading
import warnings
from collections import deque, defaultdict

os.environ.setdefault("EVENTLET_NO_GREENDNS", "yes")
warnings.filterwarnings("ignore", message=".*[Ee]ventlet.*")

ASYNC_MODE = os.getenv("SOCKETIO_ASYNC_MODE", "threading").strip().lower()
if ASYNC_MODE == "eventlet":
    import eventlet
    eventlet.monkey_patch()
elif ASYNC_MODE != "threading":
    raise ValueError(f"SOCKETIO_ASYNC_MODE không hợp lệ: {ASYNC_MODE!r}")

from flask import Flask, jsonify, request
from flask_socketio import SocketIO
from waitress import serve
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chat")

REDIS_URL = os.getenv("REDIS_URL")
CHAT_TOKEN = os.getenv("CHAT_TOKEN", "")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")
LIST_KEY = "chat:messages"
USERS_KEY = "chat:users"
MAX_MSGS = int(os.getenv("MAX_MSGS", "500"))
MAX_USER_LEN = 30
MAX_MSG_LEN = 2000
MAX_PASSWORD_LEN = 128
MIN_PASSWORD_LEN = 6
RATE_LIMIT = (8, 10.0)
TOKEN_TTL = int(os.getenv("TOKEN_TTL", str(7 * 24 * 3600)))

USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,30}$")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "chat-secret")

_serializer = URLSafeTimedSerializer(app.config["SECRET_KEY"], salt="chat-auth-v1")


def make_token(username: str) -> str:
    return _serializer.dumps({"u": username})


def verify_token(token: str | None) -> str | None:
    if not token:
        return None
    try:
        data = _serializer.loads(token, max_age=TOKEN_TTL)
        if isinstance(data, dict) and data.get("u"):
            u = str(data["u"])
            if USERNAME_RE.match(u):
                return u
    except (BadSignature, SignatureExpired):
        pass
    return None


# ─── Message store ───
class MemoryStore:
    kind = "memory"

    def __init__(self):
        self._items = deque(maxlen=MAX_MSGS)

    def push(self, packet):
        self._items.append(packet)

    def history(self, limit):
        return list(self._items)[-limit:]

    def ping(self):
        return True


class RedisStore:
    kind = "redis"

    def __init__(self, url):
        import redis
        self.r = redis.from_url(
            url, decode_responses=True,
            socket_timeout=5, socket_connect_timeout=5, health_check_interval=30,
        )

    def push(self, packet):
        pipe = self.r.pipeline()
        pipe.rpush(LIST_KEY, json.dumps(packet, ensure_ascii=False))
        pipe.ltrim(LIST_KEY, -MAX_MSGS, -1)
        pipe.execute()

    def history(self, limit):
        out = []
        for raw in self.r.lrange(LIST_KEY, -limit, -1):
            try:
                out.append(json.loads(raw))
            except (ValueError, TypeError):
                continue
        return out

    def ping(self):
        return self.r.ping()


# ─── User store ───
class MemoryUserStore:
    kind = "memory"

    def __init__(self):
        self._users = {}
        self._lock = threading.Lock()

    def get(self, username):
        with self._lock:
            return self._users.get(username)

    def create(self, username, pwhash):
        with self._lock:
            if username in self._users:
                return False
            self._users[username] = {"pw": pwhash, "created": time.time()}
            return True

    def ping(self):
        return True


class RedisUserStore:
    kind = "redis"

    def __init__(self, url):
        import redis
        self.r = redis.from_url(
            url, decode_responses=True,
            socket_timeout=5, socket_connect_timeout=5, health_check_interval=30,
        )

    def get(self, username):
        raw = self.r.hget(USERS_KEY, username)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return None

    def create(self, username, pwhash):
        payload = json.dumps({"pw": pwhash, "created": time.time()}, ensure_ascii=False)
        return bool(self.r.hsetnx(USERS_KEY, username, payload))

    def ping(self):
        return self.r.ping()


# ─── Khởi tạo ───
def _mask_url(url: str) -> str:
    try:
        return url.split("@")[-1] if "@" in url else url
    except Exception:
        return "?"


store = None
users = None
mq = None
if REDIS_URL:
    try:
        s = RedisStore(REDIS_URL)
        s.ping()
        u = RedisUserStore(REDIS_URL)
        u.ping()
        store, users = s, u
        mq = REDIS_URL if ASYNC_MODE == "eventlet" else None
        log.info("✅ Redis OK: %s", _mask_url(REDIS_URL))
    except Exception as e:
        log.error("❌ Redis (%s): %s → fallback RAM", _mask_url(REDIS_URL), e)
        store = users = None

if store is None:
    if not REDIS_URL:
        log.warning("REDIS_URL chưa đặt → dùng RAM (chỉ phù hợp 1 server).")
    store, users, mq = MemoryStore(), MemoryUserStore(), None


socketio = SocketIO(
    app,
    cors_allowed_origins="*" if CORS_ORIGINS == "*" else CORS_ORIGINS.split(","),
    async_mode=ASYNC_MODE,
    message_queue=mq,
    ping_interval=20,
    ping_timeout=30,
    allow_upgrades=(ASYNC_MODE == "eventlet"),
)


# Khi threading + waitress: waitress không có socket WS -> engineio crash
# trong _upgrade_websocket. Chặn sớm ở lớp WSGI, trả 400 để client fallback.
if ASYNC_MODE != "eventlet":
    class _RejectWS:
        def __init__(self, inner):
            self.inner = inner

        def __call__(self, environ, start_response):
            if environ.get("HTTP_UPGRADE", "").lower() == "websocket":
                start_response("400 Bad Request", [("Content-Type", "text/plain")])
                return [b"websocket disabled; server in threading mode, use polling"]
            return self.inner(environ, start_response)

    app.wsgi_app = _RejectWS(app.wsgi_app)


_CTRL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


def clean(value, max_len):
    return _CTRL.sub("", str(value)).strip()[:max_len].strip()


def now_packet_time():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp()


# ─── Auth helpers ───
def _bearer_token():
    h = request.headers.get("Authorization", "")
    if h.startswith("Bearer "):
        return h[7:].strip()
    return None


def _authorized(token):
    """Trả username nếu hợp lệ, ngược lại None. Hỗ trợ cả CHAT_TOKEN legacy."""
    if not token:
        return None
    if CHAT_TOKEN and hmac.compare_digest(str(token), CHAT_TOKEN):
        return "__shared__"
    return verify_token(token)


def _json_body():
    return request.get_json(silent=True) or {}


_auth_attempts = defaultdict(deque)
_auth_lock = threading.Lock()


def auth_rate_limited(ip):
    now = time.monotonic()
    with _auth_lock:
        q = _auth_attempts[ip]
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= 10:
            return True
        q.append(now)
        return False


_rate = {}
_rate_lock = threading.Lock()
_sessions = {}
_sessions_lock = threading.Lock()


def rate_limited(sid):
    limit, window = RATE_LIMIT
    now = time.monotonic()
    with _rate_lock:
        q = _rate.setdefault(sid, deque())
        while q and now - q[0] > window:
            q.popleft()
        if len(q) >= limit:
            return True
        q.append(now)
        return False


# ─── Routes ───
@app.route("/")
def home():
    return "🚀 Chat Server OK — /auth/register, /auth/login, /health, /history"


@app.route("/health")
def health():
    try:
        store.ping()
        users.ping()
        return {"status": "ok", "store": store.kind, "users": users.kind, "async_mode": ASYNC_MODE}
    except Exception as e:
        log.exception("health check lỗi")
        return {"status": "store_error", "detail": str(e)}, 500


@app.route("/auth/register", methods=["POST"])
def auth_register():
    if auth_rate_limited(request.remote_addr or "?"):
        return {"error": "rate_limited"}, 429
    data = _json_body()
    username = clean(data.get("username", ""), MAX_USER_LEN)
    password = str(data.get("password", ""))[:MAX_PASSWORD_LEN]
    if not USERNAME_RE.match(username):
        return {"error": "invalid_username"}, 400
    if len(password) < MIN_PASSWORD_LEN:
        return {"error": "weak_password"}, 400
    try:
        pwhash = generate_password_hash(password)
        ok = users.create(username, pwhash)
    except Exception:
        log.exception("register lỗi store")
        return {"error": "store_unavailable"}, 503
    if not ok:
        return {"error": "username_taken"}, 409
    log.info("Đăng ký user mới: %s", username)
    return {"ok": True, "token": make_token(username), "username": username}


@app.route("/auth/login", methods=["POST"])
def auth_login():
    if auth_rate_limited(request.remote_addr or "?"):
        return {"error": "rate_limited"}, 429
    data = _json_body()
    username = clean(data.get("username", ""), MAX_USER_LEN)
    password = str(data.get("password", ""))[:MAX_PASSWORD_LEN]
    if not USERNAME_RE.match(username) or not password:
        return {"error": "invalid_credentials"}, 401
    try:
        rec = users.get(username)
    except Exception:
        log.exception("login lỗi store")
        return {"error": "store_unavailable"}, 503
    if not rec or not check_password_hash(rec.get("pw", ""), password):
        return {"error": "invalid_credentials"}, 401
    return {"ok": True, "token": make_token(username), "username": username}


@app.route("/auth/me")
def auth_me():
    token = _bearer_token() or request.args.get("token")
    u = verify_token(token)
    if not u:
        return {"error": "unauthorized"}, 401
    return {"ok": True, "username": u}


@app.route("/history")
def history():
    supplied = _bearer_token() or request.headers.get("X-Chat-Token") or request.args.get("token")
    if not _authorized(supplied):
        return {"error": "unauthorized"}, 401
    try:
        limit = int(request.args.get("limit", "50"))
    except ValueError:
        limit = 50
    limit = max(1, min(limit, MAX_MSGS))
    try:
        return jsonify(store.history(limit))
    except Exception:
        log.exception("đọc history lỗi")
        return {"error": "store_unavailable"}, 503


# ─── Socket.IO ───
@socketio.on("connect")
def on_connect(auth=None):
    token = auth.get("token") if isinstance(auth, dict) else None
    user = _authorized(token)
    if not user:
        return False
    with _sessions_lock:
        _sessions[request.sid] = user


@socketio.on("disconnect")
def on_disconnect(*_):
    with _sessions_lock:
        _sessions.pop(request.sid, None)
    with _rate_lock:
        _rate.pop(request.sid, None)


@socketio.on("message")
def handle_message(data):
    username = _sessions.get(request.sid)
    if not username:
        return {"ok": False, "error": "unauthorized"}

    if isinstance(data, str) and ":" in data:
        _, m = data.split(":", 1)
        data = {"msg": m}

    if not isinstance(data, dict) or "msg" not in data:
        return {"ok": False, "error": "invalid_packet"}

    if rate_limited(request.sid):
        return {"ok": False, "error": "rate_limited"}

    msg = clean(data.get("msg", ""), MAX_MSG_LEN)
    if not msg:
        return {"ok": False, "error": "empty"}

    time_str, ts = now_packet_time()
    packet = {"id": uuid.uuid4().hex, "user": username, "msg": msg,
              "time": time_str, "ts": ts}
    try:
        store.push(packet)
    except Exception:
        log.exception("lưu tin nhắn lỗi")

    socketio.emit("message", packet)
    return {"ok": True, "id": packet["id"]}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    log.info("Chat server cổng %s (store=%s, users=%s, async=%s)",
             port, store.kind, users.kind, ASYNC_MODE)
    if ASYNC_MODE == "eventlet":
        # eventlet: WebSocket native -> tiết kiệm bandwidth
        socketio.run(app, host="0.0.0.0", port=port)
    else:
        # threading + waitress: chỉ polling (fallback)
        serve(app, host="0.0.0.0", port=port, threads=8)
