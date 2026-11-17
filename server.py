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
from collections import deque

# ─── Fix cho Termux/Android: không có /etc/resolv.conf ───
# Phải set TRƯỚC khi import eventlet. Tắt greendns → dùng DNS của OS
# (sau khi bạn đã tạo $PREFIX/etc/resolv.conf như hướng dẫn trước).
os.environ.setdefault("EVENTLET_NO_GREENDNS", "yes")
warnings.filterwarnings("ignore", message=".*[Ee]ventlet.*")

# ─── Async mode: mặc định "threading" (nhẹ, ổn định trên Termux) ───
# Muốn dùng eventlet: export SOCKETIO_ASYNC_MODE=eventlet
ASYNC_MODE = os.getenv("SOCKETIO_ASYNC_MODE", "threading").strip().lower()
if ASYNC_MODE == "eventlet":
    import eventlet
    eventlet.monkey_patch()
elif ASYNC_MODE != "threading":
    raise ValueError(f"SOCKETIO_ASYNC_MODE không hợp lệ: {ASYNC_MODE!r}")

from flask import Flask, jsonify, request
from flask_socketio import SocketIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chat")

REDIS_URL = os.getenv("REDIS_URL")
CHAT_TOKEN = os.getenv("CHAT_TOKEN", "")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")
LIST_KEY = "chat:messages"
MAX_MSGS = int(os.getenv("MAX_MSGS", "500"))
MAX_USER_LEN = 30
MAX_MSG_LEN = 2000
RATE_LIMIT = (8, 10.0)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "chat-secret")


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
            url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            health_check_interval=30,
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


# ─── Khởi tạo store: KHÔNG crash server nếu Redis lỗi, chỉ fallback ───
def _mask_url(url: str) -> str:
    # ẩn mật khẩu khi log
    try:
        return url.split("@")[-1] if "@" in url else url
    except Exception:
        return "?"


store = None
mq = None
if REDIS_URL:
    try:
        candidate = RedisStore(REDIS_URL)
        candidate.ping()  # kiểm tra thật sự
        store = candidate
        mq = REDIS_URL if ASYNC_MODE == "eventlet" else None
        log.info("✅ Redis OK: %s", _mask_url(REDIS_URL))
    except Exception as e:
        log.error("❌ Không kết nối được Redis (%s) → fallback RAM. Chi tiết: %s",
                  _mask_url(REDIS_URL), e)

if store is None:
    if not REDIS_URL:
        log.warning("REDIS_URL chưa đặt → dùng bộ nhớ RAM (chỉ phù hợp 1 server).")
    store = MemoryStore()
    mq = None


socketio = SocketIO(
    app,
    cors_allowed_origins="*" if CORS_ORIGINS == "*" else CORS_ORIGINS.split(","),
    async_mode=ASYNC_MODE,
    message_queue=mq,
    ping_interval=20,
    ping_timeout=30,
)


_CTRL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


def clean(value, max_len):
    return _CTRL.sub("", str(value)).strip()[:max_len].strip()


def now_packet_time():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp()


def token_ok(supplied):
    if not CHAT_TOKEN:
        return True
    return hmac.compare_digest(str(supplied or ""), CHAT_TOKEN)


_rate = {}
_rate_lock = threading.Lock()


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


@app.route("/")
def home():
    return "🚀 Chat Server OK (Socket.IO) — /health, /history"


@app.route("/health")
def health():
    try:
        store.ping()
        return {"status": "ok", "store": store.kind, "async_mode": ASYNC_MODE}
    except Exception as e:
        log.exception("health check lỗi")
        return {"status": "store_error", "detail": str(e)}, 500


@app.route("/history")
def history():
    supplied = request.headers.get("X-Chat-Token") or request.args.get("token")
    if not token_ok(supplied):
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


@socketio.on("connect")
def on_connect(auth=None):
    token = auth.get("token") if isinstance(auth, dict) else None
    if not token_ok(token):
        return False


@socketio.on("disconnect")
def on_disconnect(*_):
    with _rate_lock:
        _rate.pop(request.sid, None)


@socketio.on("message")
def handle_message(data):
    if isinstance(data, str) and ":" in data:
        u, m = data.split(":", 1)
        data = {"user": u, "msg": m}

    if not isinstance(data, dict) or "msg" not in data:
        return {"ok": False, "error": "invalid_packet"}

    if rate_limited(request.sid):
        return {"ok": False, "error": "rate_limited"}

    user = clean(data.get("user", ""), MAX_USER_LEN) or "guest"
    msg = clean(data.get("msg", ""), MAX_MSG_LEN)
    if not msg:
        return {"ok": False, "error": "empty"}

    time_str, ts = now_packet_time()
    packet = {"id": uuid.uuid4().hex, "user": user, "msg": msg,
              "time": time_str, "ts": ts}

    try:
        store.push(packet)
    except Exception:
        log.exception("lưu tin nhắn lỗi")

    socketio.emit("message", packet)
    return {"ok": True, "id": packet["id"]}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    log.info("Chat server cổng %s (store=%s, async=%s)",
             port, store.kind, ASYNC_MODE)
    socketio.run(app, host="0.0.0.0", port=port)
