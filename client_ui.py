import os
import sys
import time
import threading
from collections import OrderedDict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout

import requests
import socketio
from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.text import Text
from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout

console = Console(force_terminal=True if sys.stdout.isatty() else None)

DEFAULT_SERVERS = [
    "http://localhost:5000",
    "https://chat-vps2.onrender.com",
    "https://chat-vps3.onrender.com",
    "https://chat-vps4.onrender.com",
    "https://chat-vps5.onrender.com",
]
SERVERS = [s.strip().rstrip("/") for s in os.getenv("CHAT_SERVERS", "").split(",") if s.strip()] or DEFAULT_SERVERS
CHAT_TOKEN = os.getenv("CHAT_TOKEN", "")
PROBE_TIMEOUT = 60
MAX_MSG_LEN = 2000
HISTORY_LIMIT = 15

COLORS = ["cyan", "magenta", "green", "yellow", "blue", "red"]
HELP = (
    "[bold]/help[/]  trợ giúp   [bold]/history [n][/]  xem n tin gần nhất   "
    "[bold]/server[/]  server đang dùng   [bold]/clear[/]  xoá màn hình   [bold]/quit[/]  thoát"
)

class ChatClient:
    def __init__(self, username):
        self.username = username
        self.sio = None
        self.url = None
        self.stop = threading.Event()
        self.seen = OrderedDict()
        self.seen_lock = threading.Lock()
        self.colors = {}
        self.first_history = True

    def color(self, user):
        if user not in self.colors:
            self.colors[user] = COLORS[len(self.colors) % len(COLORS)]
        return self.colors[user]

    def show(self, packet):
        user = str(packet.get("user", "?"))
        msg = str(packet.get("msg", ""))
        if packet.get("ts"):
            ts = datetime.fromtimestamp(packet["ts"]).strftime("%H:%M:%S")
        else:
            ts = str(packet.get("time") or datetime.now().strftime("%H:%M:%S"))
        me = user == self.username
        color = self.color(user)

        # Fix: Panel(expand=False) co theo nội dung -> msg ngắn hơn header
        # (ts + user) sẽ làm title/subtitle bị cắt cụt. Pad content đủ rộng.
        header_plain = f"{ts} {user}"
        min_content = len(header_plain) + 4
        display_msg = msg if len(msg) >= min_content else msg + " " * (min_content - len(msg))

        header = Text.assemble((ts, "dim"), " ", (user, f"bold {color}"))
        bubble = Panel(
            Text(display_msg),
            title=None if me else header,
            subtitle=header if me else None,
            subtitle_align="right",
            title_align="left",
            border_style=color,
            expand=False,
            padding=(0, 2),
        )
        console.print(Align.right(bubble) if me else Align.left(bubble))

    def info(self, text, style=None):
        console.print(Text(text, style=style or ""))

    def remember(self, packet):
        key = packet.get("id") or f"{packet.get('time')}|{packet.get('user')}|{packet.get('msg')}"
        with self.seen_lock:
            if key in self.seen:
                return False
            self.seen[key] = True
            while len(self.seen) > 2000:
                self.seen.popitem(last=False)
            return True

    def on_packet(self, packet):
        if isinstance(packet, dict) and self.remember(packet):
            self.show(packet)

    @property
    def connected(self):
        return bool(self.sio and self.sio.connected)

    def headers(self):
        return {"X-Chat-Token": CHAT_TOKEN} if CHAT_TOKEN else {}

    def probe(self, url):
        try:
            r = requests.get(f"{url}/health", timeout=PROBE_TIMEOUT)
            if r.ok:
                return url, True
        except requests.RequestException:
            pass
        return url, False

    def try_connect(self, url):
        sio = socketio.Client(reconnection=False, request_timeout=10)

        @sio.on("message")
        def _on_message(data):
            self.on_packet(data)

        @sio.event
        def disconnect():
            if not self.stop.is_set() and sio is self.sio:
                self.info(f"❌ Mất kết nối {url}, đang tìm server khác...", "red")

        try:
            sio.connect(url, transports=["polling"], auth={"token": CHAT_TOKEN}, wait_timeout=10)
        except Exception as e:
            self.info(f"⚠️ Không connect được {url}: {e}", "red")
            return False

        old, self.sio, self.url = self.sio, sio, url
        if old is not None:
            try:
                old.disconnect()
            except Exception:
                pass
        return True

    def connect_any(self):
        ex = ThreadPoolExecutor(max_workers=len(SERVERS))
        futures = [ex.submit(self.probe, u) for u in SERVERS]
        try:
            for fut in as_completed(futures, timeout=PROBE_TIMEOUT + 5):
                url, ok = fut.result()
                if ok and self.try_connect(url):
                    console.print(Panel(f"[green]✅ Đã kết nối {url}[/]", border_style="green"))
                    return True
        except FuturesTimeout:
            pass
        finally:
            ex.shutdown(wait=False, cancel_futures=True)
        return False

    def load_history(self, limit=HISTORY_LIMIT, force=False):
        if not self.url:
            return
        try:
            r = requests.get(f"{self.url}/history", params={"limit": limit}, headers=self.headers(), timeout=10)
            if r.status_code == 401:
                self.info("⚠️ Sai CHAT_TOKEN (server từ chối /history)", "red")
                return
            r.raise_for_status()
            msgs = r.json()
        except Exception as e:
            self.info(f"⚠️ Lỗi load history: {e}", "red")
            return

        if self.first_history or force:
            console.print(Panel(f"[bold cyan]📜 {len(msgs)} tin gần nhất[/]", border_style="bright_black"))
            self.first_history = False
            for p in msgs:
                self.remember(p)
                self.show(p)
        else:
            for p in msgs:
                self.on_packet(p)

    def supervisor(self):
        backoff = 1
        while not self.stop.is_set():
            if not self.connected:
                with console.status("[cyan]Đang kết nối server (Render có thể mất ~30s để thức dậy)...[/]"):
                    ok = self.connect_any()
                if ok:
                    backoff = 1
                    self.load_history()
                else:
                    self.info(f"⚠️ Chưa có server nào sẵn sàng, thử lại sau {backoff}s", "yellow")
                    self.stop.wait(backoff)
                    backoff = min(backoff * 2, 30)
            else:
                self.stop.wait(1)

    def send(self, text):
        if len(text) > MAX_MSG_LEN:
            self.info(f"⚠️ Tin quá dài ({len(text)}/{MAX_MSG_LEN} ký tự), không gửi.", "yellow")
            return
        if not self.connected:
            self.info("⚠️ Chưa kết nối, tin nhắn chưa được gửi.", "yellow")
            return

        def ack(resp=None):
            if isinstance(resp, dict) and not resp.get("ok"):
                reason = {"rate_limited": "gửi quá nhanh, chờ chút"}.get(resp.get("error"), resp.get("error"))
                self.info(f"⚠️ Server từ chối tin nhắn: {reason}", "yellow")

        try:

            self.sio.emit("message", {"user": self.username, "msg": text}, callback=ack)
        except Exception as e:
            self.info(f"⚠️ Gửi lỗi: {e}", "red")

    def close(self):
        self.stop.set()
        try:
            if self.sio:
                self.sio.disconnect()
        except Exception:
            pass

def main():
    username = Prompt.ask("👤 Nhập tên của bạn").strip()[:30] or "guest"
    client = ChatClient(username)

    threading.Thread(target=client.supervisor, daemon=True).start()
    console.print(Panel("[bold cyan]✨ Bắt đầu chat nào! Gõ /help để xem lệnh, Ctrl+C để thoát ✨[/]", border_style="cyan"))

    session = PromptSession()
    try:
        with patch_stdout(raw=True):
            while True:
                try:
                    text = session.prompt(f"{username}> ").strip()
                except (KeyboardInterrupt, EOFError):
                    break
                if not text:
                    continue

                if text.startswith("/"):
                    cmd, _, arg = text.partition(" ")
                    cmd = cmd.lower()
                    if cmd in ("/quit", "/exit", "/q"):
                        break
                    elif cmd == "/help":
                        console.print(HELP)
                    elif cmd == "/clear":
                        console.clear()
                    elif cmd == "/server":
                        console.print(f"🌐 {client.url or 'chưa kết nối'}")
                    elif cmd == "/history":
                        n = int(arg) if arg.strip().isdigit() else HISTORY_LIMIT
                        client.load_history(limit=min(max(n, 1), 200), force=True)
                    else:
                        console.print(f"[yellow]Lệnh không tồn tại: {cmd} (gõ /help)[/]")
                    continue

                client.send(text)
    finally:
        console.print("\n[red]👋 Thoát chat[/]")
        client.close()

if __name__ == "__main__":
    main()
