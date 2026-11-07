"""
Ladder Manager - שירות שרץ מחוץ ל-TradeLocker (Mac / Hetzner) ומנהל עסקאות ידניות.

מקור המידע: Streams API (Socket.IO, דורש developer key ב-TL_DEV_KEY) - כל שינוי מגיע תוך פחות משנייה.
בלי TL_DEV_KEY (או use_stream=False) - חוזר ל-polling של REST כל poll_seconds (איטי, 429).
ב-stream אין strategyId, לכן: פקודות שהשירות פתח נרשמות אצלו, ועסקה חדשה שלא מוכרת נבדקת פעם אחת ב-REST.

מה הוא עושה:
1. עסקה ידנית חדשה (Market) על הסימבול -> פותח עוד copies-1 עסקאות באותו כיוון ובאותו לוט.
   mode="split": הלוט שלך מתחלק - העסקה שלך נסגרת חלקית ונפתחות copies-1 עסקאות בגודל החלק (0.1 -> 5 × 0.02).
   TP מדורג: עסקה 1 = כניסה + tp_step, עסקה 2 = + 2*tp_step ... (ב-Sell יורד).
   runner_last=True -> העסקה האחרונה בלי TP.
2. פקודת Limit/Stop ידנית חדשה -> מוסיף copies-1 פקודות באותו מחיר עם TP מדורג.
   הזזת המחיר של הפקודה שלך מזיזה את כולן (כולל ה-TP), ביטול מבטל את כולן.
3. סנכרון סטופ: הזזת SL באחת מעסקאות הקבוצה -> אותו SL לכל השאר.
   auto_sl: עסקה/פקודה בלי SL מקבלת SL במרחק הזה מהכניסה (ברירת מחדל 10$, 0 = בלי). מחיקה שלו = כמו כל SL (sync_sl_delete).
4. be_after_tp1=True -> הסטופ של השאר זז אחרי TP1. be_to:
   runner_keep_be=True -> כשהשאר עוברות ל-TP1, ה-Runner נשאר בכניסה + be_offset.
   "two_step" (ברירת מחדל): TP1 נתפס -> SL לכניסה + be_offset; המחיר הגיע ל-lock_tp1_at מהדרך TP1→TP2
              (0.5 = חצי הדרך, 1 = TP2) או ש-TP2 נתפס -> SL ל-TP1.
   "tp1": SL ל-TP1 מיד כש-TP1 נתפס.  "entry": SL לכניסה + be_offset.
   "steps": מדרגות - כל step_every רמות (TP) שהמחיר עבר, ה-SL של כולן (כולל ה-Runner) עולה מדרגה:
            step_every=1: TP1 -> כניסה + be_offset, TP2 -> TP1, TP3 -> TP2 ...
            step_every=2: TP2 -> כניסה + be_offset, TP4 -> TP2, TP6 -> TP4 ...
            step_every=1, step_start=2: TP1 -> כלום, TP2 -> כניסה + be_offset, TP3 -> TP2, TP4 -> TP3 ...
            אחרי ה-TP האחרון ה-Runner ממשיך לעלות במדרגות לפי המחיר (כל tp_step נוסף).
   קבוצה שומרת את הגדרות ה-SL שהיא נפתחה איתן (GROUP_KEYS) - שינוי / טעינת תבנית חלים על העסקאות הבאות.
   המחיר הנוכחי מגיע מה-PnL של העסקאות ב-stream (כל ~2 שניות); ב-polling שלב 2 רק כש-TP2 נתפס.

עסקאות של בוטים אחרים (strategyId לא ריק, למשל HGUARD) לא נוגעים בהן.
עסקאות שהיו פתוחות כשהשירות עלה - לא משוכפלות.
ה-TP נקבע פעם אחת בפתיחה; שינוי/מחיקה ידנית של TP לא מוחזרת. רק ה-SL מסונכרן.

active=False: רק מדפיס מה היה עושה, בלי לשלוח פקודות.

Secrets/connection come from env vars: TL_ENV, TL_SERVER, TL_EMAIL, TL_PASSWORD,
TL_ACCOUNT_ID, TL_DEV_KEY (Streams), DISCORD_WEBHOOK (log channel), DISCORD_WEBHOOK_ALERT (control channel, CLI only - the panel shows alerts itself), STATE_FILE.
"""
import argparse
import fcntl
import json
import logging
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
import socketio
from tradelocker import TLAPI
from tradelocker.exceptions import TLAPIOrderException

PARAMS = dict(
    active=False,
    enabled=True,               # False = לא משכפל עסקאות חדשות, אבל ממשיך לנהל SL של קבוצות פתוחות
    language="he",              # "he" / "en"
    symbol="XAUUSD",
    mode="duplicate",           # "duplicate" = כל עסקה בלוט שלך | "split" = הלוט שלך מתחלק ל-copies עסקאות
    copies=5,                   # כמה עסקאות בסך הכול (כולל שלך)
    max_total_lots=0.0,         # duplicate: כמה לוט מקסימום בכל הסולם (0 = בלי). אם עובר - העותקים קטנים כדי להישאר בפנים
    lot_step=0.01,              # split: הלוט הכי קטן; אם אין מספיק ל-copies - פחות עסקאות, השארית אצלך
    tp_step=2.0,                # מרווח בין ה-TP-ים (דולר במחיר)
    runner_last=True,           # העסקה האחרונה בלי TP
    auto_sl=10.0,               # עסקה בלי SL מקבלת SL במרחק הזה מהכניסה (דולר במחיר, 0 = בלי)
    sync_sl=True,               # הזזת SL באחת -> לכולן
    sync_sl_delete=True,        # מחיקת SL מאחת -> נמחק מכולן (False = מחזירים לה את ה-SL של הקבוצה)
    be_after_tp1=False,         # אחרי TP1 - SL של השאר זז (לאן: be_to)
    be_to="two_step",           # "two_step" = כניסה ואז TP1 | "tp1" = SL למחיר TP1 | "entry" = SL לכניסה + be_offset | "steps" = מדרגות
    step_every=1,               # steps: כל כמה TP-ים ה-SL עולה מדרגה (1 = כל TP, 2 = כל שני TP-ים)
    step_start=1,               # steps כל TP: באיזה TP הסטופ זז לכניסה (2 = כלום ב-TP1, TP2 -> כניסה, TP3 -> TP2 ...)
    be_offset=0.0,              # SL לכניסה: כמה מעבר לכניסה (0.3 = נועל 0.3$ רווח לאונקיה)
    lock_tp1_at=0.5,            # two_step: SL ל-TP1 כשהמחיר עבר את החלק הזה מהדרך TP1→TP2 (0.5 = חצי, 1 = TP2)
    runner_keep_be=True,        # כשה-SL של השאר עובר ל-TP1, ה-Runner נשאר בכניסה + be_offset (נותנים לו לרוץ בסיכון 0)
    handle_pending=True,        # לשכפל גם פקודות Limit/Stop
    use_stream=True,            # Streams API (צריך TL_DEV_KEY); False = polling
    stream_url="https://api.tradelocker.com",
    tick_seconds=1.0,           # ב-stream: בדיקה גם בלי אירוע (ניסיונות חוזרים של SL)
    poll_seconds=2.0,           # בלי stream: מגבלת השרת 1 קריאה לשנייה ל-positions/orders (משותפת עם הטרמינל)
    price_decimals=2,
    label="",                   # שם שנוסף לכל הודעה בלוג (כמה משכפלים על אותו ערוץ)
    strategy_tag="LAD",         # strategyId של העסקאות שהשירות פותח
    manual_tags="",             # strategyId נוספים שנחשבים ידני, מופרדים בפסיק (בדיקות: key-undefined = API בלי strategy)
    tp_hit_slack=0.3,           # עסקה שנעלמה נחשבת TP אם המחיר הכי טוב שנראה הגיע עד כדי כך ל-TP שלה (המחיר נדגם כל ~2 שניות)
    sl_retry_seconds=4.0,       # אם ה-SL לא התעדכן אחרי X שניות - מנסים שוב / דרך אחרת
)

TRANSLATIONS = {
    "he": {
        "start": "🚀 Ladder Manager עלה",
        "ladder": "🪜 <b>שוכפלה עסקה</b>",
        "ladder_pending": "🪜 <b>שוכפלה פקודה ממתינה</b>",
        "pending_moved": "↔️ <b>פקודות הוזזו</b>",
        "pending_cancel": "🗑 <b>פקודות בוטלו</b>",
        "pending_filled": "✅ <b>פקודה התמלאה</b>",
        "sl_sync": "🛡 <b>סטופ סונכרן</b>",
        "sl_cleared": "🚫 <b>סטופ נמחק מכל העסקאות</b>",
        "be": "🔒 <b>TP1 נתפס - סטופ הוזז</b>",
        "lock_tp1": "🔐 <b>המחיר המשיך - סטופ ל-TP1</b>",
        "step_up": "🪜 <b>מדרגה - סטופ עלה</b>",
        "group_done": "🏁 <b>הקבוצה נסגרה</b>",
        "error": "⚠️ <b>שגיאה</b>",
        "dry": "(מצב בדיקה - לא נשלחה פקודה)",
        "auto": "אוטומטי", "cap": "מוגבל ל-", "side": "כיוון", "qty": "לוט", "price": "מחיר", "tps": "TP", "sl": "SL", "count": "עסקאות",
    },
    "en": {
        "start": "🚀 Ladder Manager started",
        "ladder": "🪜 <b>Trade laddered</b>",
        "ladder_pending": "🪜 <b>Pending order laddered</b>",
        "pending_moved": "↔️ <b>Orders moved</b>",
        "pending_cancel": "🗑 <b>Orders cancelled</b>",
        "pending_filled": "✅ <b>Order filled</b>",
        "sl_sync": "🛡 <b>Stop synced</b>",
        "sl_cleared": "🚫 <b>Stop removed from all trades</b>",
        "be": "🔒 <b>TP1 hit - stop moved</b>",
        "lock_tp1": "🔐 <b>Price ran on - stop to TP1</b>",
        "step_up": "🪜 <b>Step - stop moved up</b>",
        "group_done": "🏁 <b>Group closed</b>",
        "error": "⚠️ <b>Error</b>",
        "dry": "(dry run - no order sent)",
        "auto": "auto", "cap": "cap", "side": "Side", "qty": "Lot", "price": "Price", "tps": "TP", "sl": "SL", "count": "Trades",
    },
}

log = logging.getLogger("ladder")

# SL management settings a group keeps from the moment it opens: changing them (or loading a template) applies to the
# next trades, an open ladder goes on with the rules it started with
GROUP_KEYS = ("be_after_tp1", "be_to", "step_every", "step_start", "be_offset", "lock_tp1_at", "runner_keep_be")


def env_login(prefix="TL_"):
    """TradeLocker login from env vars: TL_ENV/TL_EMAIL/TL_PASSWORD/TL_SERVER (demo) or TL_LIVE_* (live)."""
    get = lambda key: os.environ.get(prefix + key) or os.environ["TL_" + key]     # live falls back to the demo login
    return {"env": os.environ[prefix + "ENV"], "email": get("EMAIL"), "password": get("PASSWORD"), "server": get("SERVER")}

DONE_STATUSES = ("STATUS_FILLED", "STATUS_CANCELED", "STATUS_REJECTED", "STATUS_EXPIRED")


class StreamFeed:
    """Mirror of the account's positions and orders, fed by the TradeLocker Streams API.
    Socket.IO callbacks only update the mirror (under lock) and set `changed`;
    the manager reads it from its own thread."""

    def __init__(self, url, account_id, symbol, login):
        self.url = url
        self.login = login
        self.account_id = str(account_id)
        self.symbol = symbol
        self.lock = threading.Lock()
        self.changed = threading.Event()
        self.sio = None
        self.expires_at = 0.0
        self._reset()

    def _reset(self):
        self.synced = False
        self.positions = {}     # pid -> latest Position event (full snapshot, replaced)
        self.orders = {}        # oid -> merged OpenOrder events (modify events omit status)
        self.origin = {}        # pid -> order id that opened it
        self.filled = {}        # entry order id -> position id
        self.tp_closed = {}     # position id -> time its TP (closing limit order) filled
        self.pnl = {}           # pid -> open PnL in USD (AccountStatus.positionPnLs)

    @property
    def alive(self):
        return self.sio is not None and self.sio.connected and time.time() < self.expires_at - 120

    def _token(self):
        r = requests.post(f"{self.login['env']}/backend-api/auth/jwt/accounts/tokens",
                          json={"email": self.login["email"], "password": self.login["password"],
                                "server": self.login["server"]}, timeout=10)
        r.raise_for_status()
        data = r.json()
        data = data["data"] if isinstance(data, dict) and "data" in data else data
        for entry in data if isinstance(data, list) else [data]:
            if str(entry["accountId"]).endswith("#" + self.account_id):
                exp = datetime.fromisoformat(entry["expireDate"].replace("Z", "+00:00"))
                return entry["accessToken"], exp.timestamp()
        raise RuntimeError(f"no stream token for account {self.account_id}")

    def connect(self):
        self.close()
        with self.lock:
            self._reset()
        token, self.expires_at = self._token()
        sio = socketio.Client(reconnection=False)
        sio.on("stream", self._on_stream, namespace="/streams-api")
        sio.on("connection", lambda m: log.info(f"stream status: {m}"), namespace="/streams-api")
        sio.on("disconnect", lambda *a: self.changed.set(), namespace="/streams-api")
        sio.connect(self.url, namespaces=["/streams-api"], socketio_path="/streams-api/socket.io",
                    transports=["websocket"], headers={"developer-api-key": os.environ["TL_DEV_KEY"]},
                    wait_timeout=10)
        ack = sio.call("subscriptions", {"action": "SUBSCRIBE", "token": token},
                       namespace="/streams-api", timeout=10)
        if not isinstance(ack, dict) or ack.get("status") != "ok":
            sio.disconnect()
            raise RuntimeError(f"stream subscribe failed: {ack}")
        self.sio = sio
        log.info(f"stream subscribed: {ack.get('message')}")

    def close(self):
        if self.sio is not None:
            try:
                self.sio.disconnect()
            except Exception:
                pass
            self.sio = None

    def _on_stream(self, m):
        if not isinstance(m, dict) or "type" not in m:
            return
        kind = m["type"]
        with self.lock:
            if kind == "Property" and m.get("name") == "SyncEnd":
                self.synced = True
            elif kind == "AccountStatus":
                self.pnl = {x["positionId"]: x["pnl"] for x in m.get("positionPnLs") or []}
            elif kind == "Position" and m.get("instrument") == self.symbol:
                pid = m["positionId"]
                self.positions[pid] = m
                if m.get("openOrderId", "0") != "0":
                    self.origin.setdefault(pid, m["openOrderId"])
            elif kind == "ClosePosition":
                pid = m["positionId"]
                self.positions.pop(pid, None)
                self.origin.pop(pid, None)
                self.filled = {oid: p for oid, p in self.filled.items() if p != pid}
            elif kind == "OpenOrder" and m.get("instrument") == self.symbol and m.get("orderType") == "MARKET":
                # market orders only matter for which position they opened; never kept (a rejected one would linger)
                if m.get("status") == "STATUS_FILLED" and m.get("isOpen") and "positionId" in m:
                    self.filled[m["orderId"]] = m["positionId"]
                    self.origin.setdefault(m["positionId"], m["orderId"])
            elif kind == "OpenOrder" and m.get("instrument") == self.symbol:
                oid = m["orderId"]
                order = self.orders.setdefault(oid, {})
                order.update(m)
                if order.get("status") == "STATUS_FILLED" and order.get("isOpen") and "positionId" in order:
                    self.filled[oid] = order["positionId"]
                    self.origin.setdefault(order["positionId"], oid)
                if (order.get("status") == "STATUS_FILLED" and order.get("isOpen") is False
                        and order.get("orderType") == "LIMIT" and "positionId" in order):
                    now = time.time()
                    self.tp_closed = {p: t for p, t in self.tp_closed.items() if now - t < 86400}
                    self.tp_closed[str(order["positionId"])] = now
                if order.get("status") in DONE_STATUSES:
                    del self.orders[oid]
            else:
                return
        self.changed.set()

    def snapshot(self):
        """Same shape as the REST snapshot, without strategy (filled in by the manager)."""
        def num(v):
            return float(v) if v not in (None, "", "-1") else 0.0

        with self.lock:
            orders = {}
            for oid, o in self.orders.items():
                otype = o["orderType"].lower()
                price = num(o.get("price"))
                stop_price = num(o.get("stopPrice")) or (price if otype == "stop" else 0.0)
                orders[oid] = {
                    "id": oid, "side": o["side"].lower(), "qty": num(o.get("amount")), "type": otype,
                    "price": price, "stop_price": stop_price,
                    "position_id": o.get("positionId", "0") if not o.get("isOpen") else "0",
                    "open": bool(o.get("isOpen")),
                    "sl": num(o.get("slPrice")), "tp": num(o.get("tpPrice")),
                }
            positions = {}
            for pid, p in self.positions.items():
                units = num(p.get("units"))
                sign = 1 if p["side"].lower() == "buy" else -1
                positions[pid] = {
                    "id": pid, "side": p["side"].lower(), "qty": num(p.get("lots")),
                    "price": num(p.get("openPrice")),
                    "sl_id": p.get("stopLossOrderId", "0"), "tp_id": p.get("takeProfitOrderId", "0"),
                    "origin": self.origin.get(pid),
                    # price this position would close at now (bid for a buy, ask for a sell)
                    "mark": num(p.get("openPrice")) + sign * float(self.pnl[pid]) / units
                            if pid in self.pnl and units else None,
                }
            return positions, orders


class LadderManager:
    def __init__(self, params, state_file, account_id=None, stop_event=None, login=None):
        self.p = params                 # shared dict: the Discord panel changes it while running
        self.state_file = state_file
        self.account_id = int(account_id or os.environ.get("TL_ACCOUNT_ID", "0"))
        self.login = login or env_login()
        self.stop = stop_event or threading.Event()
        self.api = None
        self.instr_id = 0
        self.state = {"seen_positions": [], "seen_orders": [], "groups": {}}
        # runtime only: member -> (target_sl, sent_at, attempts)
        self.sl_pending = {}
        self.feed = None
        # each order/modify call takes ~1.3s on the server; send a group's calls together (limit 5/s)
        self.pool = ThreadPoolExecutor(max_workers=5)
        self.first_seen = {}    # pid -> time first seen (stream: SL/TP orders arrive right after the Position)
        self.tags = {}          # order id -> strategy tag, for orders this service sent
        self.strategy = {}      # "p:<id>" / "o:<id>" -> strategyId ("" = manual), resolved via REST

    # ==================================================================
    # General helpers
    # ==================================================================
    def _t(self, key):
        lang = self.p["language"] if self.p["language"] in TRANSLATIONS else "he"
        return TRANSLATIONS[lang][key]

    def _c(self, g, key):
        """A group's own SL setting (see GROUP_KEYS); groups from before it was kept use the current one."""
        return g["cfg"][key] if "cfg" in g and key in g["cfg"] else self.p[key]

    def _round(self, price):
        return round(float(price), self.p["price_decimals"])

    def _same(self, a, b):
        if a is None or b is None:
            return a is None and b is None
        return abs(float(a) - float(b)) < 0.5 * 10 ** -self.p["price_decimals"]

    def _broadcast(self, header, lines):
        # several ladders share the log channel: say whose message it is
        text = header + (f" · {self.p['label']}" if self.p["label"] else "") + "\n" + "\n".join(f"• {line}" for line in lines)
        if not self.p["active"]:
            text += "\n" + self._t("dry")
        log.info(text.replace("<b>", "").replace("</b>", ""))
        # everything goes to the (muted) log channel; errors and startup also to the control channel -
        # under the Discord panel (on_alert) as its "last update" line instead of a new message
        urls = [os.environ.get("DISCORD_WEBHOOK", "")]
        if header in (self._t("error"), self._t("start")):
            if self.on_alert:
                self.on_alert(header.replace("<b>", "").replace("</b>", ""), lines)
            else:
                urls.append(os.environ.get("DISCORD_WEBHOOK_ALERT", ""))
        for url in urls:
            if not url:
                continue
            try:
                requests.post(url, json={"content": text.replace("<b>", "**").replace("</b>", "**")},
                              timeout=5)
            except Exception as exc:
                log.warning(f"discord failed: {exc}")

    def _split_sizes(self, total):
        """split mode: (number of trades, size of each copy); the user's own trade keeps the remainder."""
        step = self.p["lot_step"]
        units = int(round(total / step))
        count = min(self.p["copies"], units)
        if count < 2:
            return count, 0.0
        return count, round((units // count) * step, 2)

    def _plan(self, qty):
        """(number of trades, size of each copy, how much to take off the user's trade).
        duplicate + max_total_lots: when copies of the same size would pass the cap, the room left under it is
        divided between the copies; with no room at all the user's own lot is split instead."""
        step = self.p["lot_step"]
        cap = self.p["max_total_lots"]
        if self.p["mode"] != "split":
            count = self.p["copies"]
            if not cap or qty * count <= cap + 1e-9:
                return count, qty, 0.0
            room = int((cap - qty) / step + 1e-9)       # whole lot steps left under the cap
            if room >= 1:
                extra = min(count - 1, room)
                per = min(room // extra, int(round(qty / step)))
                return extra + 1, round(per * step, 2), 0.0
        count, per = self._split_sizes(qty)
        return count, per, round(per * (count - 1), 2)

    def _tp_levels(self, entry, side, count=None):
        count = count or self.p["copies"]
        sign = 1 if side == "buy" else -1
        levels = []
        for i in range(count):
            if self.p["runner_last"] and i == count - 1:
                levels.append(None)
            else:
                levels.append(self._round(entry + sign * self.p["tp_step"] * (i + 1)))
        return levels

    def _is_manual(self, strategy_id):
        extra = [t.strip() for t in self.p["manual_tags"].split(",") if t.strip()]
        return strategy_id is None or str(strategy_id) in ["", "None", "nan"] + extra

    def _load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file) as f:
                self.state = json.load(f)

    def _save_state(self):
        tmp = self.state_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=1)
        os.replace(tmp, self.state_file)

    # ==================================================================
    # Connection and snapshot
    # ==================================================================
    def connect(self):
        self.api = TLAPI(
            environment=self.login["env"],
            username=self.login["email"],
            password=self.login["password"],
            server=self.login["server"],
            account_id=self.account_id,
            log_level="info",
        )
        self.instr_id = self.api.get_instrument_id_from_symbol_name(self.p["symbol"])
        log.info(f"connected account={self.api.account_id} {self.p['symbol']} instrument={self.instr_id}")
        try:
            log.info(f"rate limits: {self.api.get_config()['rateLimits']}")
        except Exception as exc:
            log.info(f"rate limits unavailable: {exc}")

    def _ensure_stream(self):
        self.feed.connect()
        deadline = time.time() + 15
        while not self.feed.synced:
            if time.time() > deadline:
                raise RuntimeError("stream did not send SyncEnd")
            time.sleep(0.1)
        log.info("stream synced")

    def _resolve_strategies(self, positions, orders):
        """The stream has no strategyId: use our own order tags, else ask REST once per unknown id.
        Returns False while some id is still unknown (the tick is skipped, retried next round)."""
        need_pos = []
        for p in positions.values():
            key = "p:" + p["id"]
            if key not in self.strategy and p["origin"] in self.tags:
                self.strategy[key] = self.tags[p["origin"]]
            if key not in self.strategy:
                need_pos.append(p["id"])
        need_ord = []
        for o in orders.values():
            key = "o:" + o["id"]
            if o["position_id"] != "0" or o["type"] not in ("limit", "stop") or key in self.strategy:
                continue
            if o["id"] in self.tags:
                self.strategy[key] = self.tags[o["id"]]
            else:
                need_ord.append(o["id"])
        if need_pos:
            for r in self.api.get_all_positions().to_dict("records"):
                self.strategy["p:" + str(r["id"])] = "" if self._is_manual(r["strategyId"]) else str(r["strategyId"])
        if need_ord:
            for r in self.api.get_all_orders().to_dict("records"):
                self.strategy["o:" + str(r["id"])] = "" if self._is_manual(r["strategyId"]) else str(r["strategyId"])
        missing = [i for i in need_pos if "p:" + i not in self.strategy] + \
                  [i for i in need_ord if "o:" + i not in self.strategy]
        if missing:
            log.info(f"strategy not known yet for {missing} - waiting")
            return False
        for p in positions.values():
            p["strategy"] = self.strategy["p:" + p["id"]]
        for o in orders.values():
            o["strategy"] = self.strategy["o:" + o["id"]] if "o:" + o["id"] in self.strategy else ""
        live = {"p:" + i for i in positions} | {"o:" + i for i in orders}
        self.strategy = {k: v for k, v in self.strategy.items() if k in live}
        return True

    def snapshot(self):
        """positions + pending entry orders on the symbol, or None while not ready."""
        if self.feed:
            positions, orders = self.feed.snapshot()
            if not self._resolve_strategies(positions, orders):
                return None
        else:
            positions, orders = self._rest_snapshot()
        for p in positions.values():
            sl_order = orders[p["sl_id"]] if p["sl_id"] in orders else None
            p["sl"] = self._round(sl_order["stop_price"]) if sl_order and sl_order["stop_price"] else None
            tp_order = orders[p["tp_id"]] if p["tp_id"] in orders else None
            p["tp"] = self._round(tp_order["price"]) if tp_order and tp_order["price"] else None
            p["has_tp"] = p["tp_id"] != "0"
        pending = {oid: o for oid, o in orders.items()
                   if o["position_id"] == "0" and o["type"] in ("limit", "stop")}
        return positions, pending

    def _rest_snapshot(self):
        pos_df = self.api.get_all_positions()
        ord_df = self.api.get_all_orders()
        positions = {}
        for r in pos_df.to_dict("records"):
            if int(r["tradableInstrumentId"]) != self.instr_id:
                continue
            positions[str(r["id"])] = {
                "id": str(r["id"]), "side": r["side"], "qty": float(r["qty"]),
                "price": float(r["avgPrice"]), "sl_id": str(int(r["stopLossId"])),
                "tp_id": str(int(r["takeProfitId"])), "strategy": r["strategyId"], "mark": None,
            }
        orders = {}
        for r in ord_df.to_dict("records"):
            if int(r["tradableInstrumentId"]) != self.instr_id:
                continue
            orders[str(r["id"])] = {
                "id": str(r["id"]), "side": r["side"], "qty": float(r["qty"]), "type": r["type"],
                "price": float(r["price"]), "stop_price": float(r["stopPrice"]),
                "position_id": str(int(r["positionId"])), "strategy": r["strategyId"],
                "sl": float(r["stopLoss"]), "tp": float(r["takeProfit"]),
            }
        return positions, orders

    # ==================================================================
    # Actions (respect active)
    # ==================================================================
    def _market(self, side, qty, tp, sl, tag):
        if not self.p["active"]:
            return None
        oid = self.api.create_order(
            self.instr_id, qty, side, type_="market",
            take_profit=tp, take_profit_type="absolute" if tp else None,
            stop_loss=sl, stop_loss_type="absolute" if sl else None,
            strategy_id=tag,
        )
        if oid:
            self.tags[str(oid)] = tag
        return oid

    def _auto_sl(self, sl, entry, side):
        """(sl, added): the trade's own SL, or one auto_sl away from entry when it has none."""
        if sl or not self.p["auto_sl"]:
            return sl or None, False
        sign = 1 if side == "buy" else -1
        return self._round(entry - sign * self.p["auto_sl"]), True

    def _pending(self, src, qty, tp, sl, tag):
        if not self.p["active"]:
            return None
        price = src["price"] if src["type"] == "limit" else None
        stop_price = src["stop_price"] if src["type"] == "stop" else None
        oid = self.api.create_order(
            self.instr_id, qty, src["side"], price=price, type_=src["type"],
            validity="GTC", stop_price=stop_price,
            take_profit=tp, take_profit_type="absolute" if tp else None,
            stop_loss=sl, stop_loss_type="absolute" if sl else None,
            strategy_id=tag,
        )
        if oid:
            self.tags[str(oid)] = tag
        return oid

    def _protect(self, pos, sl, tp):
        """PATCH /positions replaces SL and TP together - a missing field clears it,
        so always send both (current value for the one we don't change)."""
        params = {}
        if sl is not None:
            params["stopLoss"] = sl
            params["stopLossType"] = "absolute"
        if tp is not None:
            params["takeProfit"] = tp
            params["takeProfitType"] = "absolute"
        if not self.p["active"]:
            return True
        return self.api.modify_position(int(pos["id"]), params)

    def _order_prot(self, sl, tp):
        params = {}
        if sl:
            params["stopLoss"] = sl
            params["stopLossType"] = "absolute"
        if tp:
            params["takeProfit"] = tp
            params["takeProfitType"] = "absolute"
        return params

    def _modify_order(self, oid, params):
        if not self.p["active"]:
            return True
        return self.api.modify_order(int(oid), params)

    def _delete_order(self, oid):
        if not self.p["active"]:
            return True
        return self.api.delete_order(int(oid))

    def _reduce(self, pos, qty):
        """split mode: take qty off the user's position before the copies open (frees the margin)."""
        if not self.p["active"]:
            return True
        return self.api.close_position(position_id=int(pos["id"]), close_quantity=qty)

    def _lot_line(self, qty, count, per, reduce, opened=None):
        done = f"{opened}/{count}" if opened is not None else f"{count}"
        cap = f"  ({self._t('cap')} {self.p['max_total_lots'] * 100:g} oz)" if self.p["max_total_lots"] else ""
        if reduce:
            return f"{self._t('qty')}: {qty} → {done} × {per}{cap}"
        if per == qty:
            return f"{self._t('qty')}: {qty} × {done}{cap}"
        added = f"{opened - 1}/{count - 1}" if opened is not None else f"{count - 1}"
        return f"{self._t('qty')}: {qty} + {added} × {per}{cap}"

    # ==================================================================
    # Ladder creation
    # ==================================================================
    def _ladder_position(self, pos):
        tag = f"{self.p['strategy_tag']}-{pos['id']}"
        count, per, reduce = self._plan(pos["qty"])
        if count < 2:
            self._broadcast(self._t("error"), [f"{self._t('qty')} {pos['qty']}: < 2 × {self.p['lot_step']}"])
            return
        if reduce and not self._reduce(pos, reduce):
            self._broadcast(self._t("error"), [f"partial close {reduce} failed - no copies opened"])
            return
        levels = self._tp_levels(pos["price"], pos["side"], count)
        sl, auto = self._auto_sl(pos["sl"], pos["price"], pos["side"])
        # one PATCH sets SL and TP together (a missing field clears it); a TP whose order hasn't arrived yet
        # is left alone - the group's SL sync gives that trade the SL
        if not pos["has_tp"] and levels[0] is not None:
            self.pool.submit(self._protect, pos, sl, levels[0])
        elif auto and pos["tp"] is not None:
            self.pool.submit(self._protect, pos, sl, pos["tp"])
        futures = [self.pool.submit(self._market, pos["side"], per, tp, sl, tag) for tp in levels[1:]]
        opened = 1
        errors = []
        for f in futures:
            try:
                f.result()
                opened += 1
            except TLAPIOrderException as exc:
                errors.append(str(exc)[:300])
        if errors:
            self._broadcast(self._t("error"), errors[:1])
        self.state["groups"][tag] = {
            "kind": "open", "side": pos["side"], "entry": pos["price"], "parent": pos["id"],
            "target_sl": sl if self.p["sync_sl"] else None, "seen_sl": {}, "max_members": 0, "be_stage": 0,
            "tp1": levels[0], "step": self.p["tp_step"], "cfg": {k: self.p[k] for k in GROUP_KEYS},
        }
        self._broadcast(self._t("ladder"), [
            f"{self._t('side')}: {pos['side']}  {self._lot_line(pos['qty'], count, per, reduce, opened)}",
            f"{self._t('price')}: {pos['price']}",
            f"{self._t('tps')}: {', '.join(str(x) if x else '—' for x in levels)}",
            f"{self._t('sl')}: {sl or '—'}" + (f" ({self._t('auto')})" if auto else ""),
        ])

    def _adopt_group(self, tag, pos):
        log.info(f"adopting existing ladder {tag}")
        self.state["groups"][tag] = {
            "kind": "open", "side": pos["side"], "entry": pos["price"], "parent": pos["id"],
            "target_sl": pos["sl"] if self.p["sync_sl"] else None, "seen_sl": {}, "max_members": 0,
            "be_stage": 0, "adopted": True,
        }

    def _ladder_pending(self, order):
        tag = f"{self.p['strategy_tag']}-O{order['id']}"
        entry = order["price"] if order["type"] == "limit" else order["stop_price"]
        count, per, reduce = self._plan(order["qty"])
        if count < 2:
            self._broadcast(self._t("error"), [f"{self._t('qty')} {order['qty']}: < 2 × {self.p['lot_step']}"])
            return
        levels = self._tp_levels(entry, order["side"], count)
        tp1 = order["tp"] or levels[0]
        sl, auto = self._auto_sl(order["sl"], entry, order["side"])
        if reduce:
            # modify replaces the order's SL/TP too, so send them with the new size
            params = {"qty": str(round(order["qty"] - reduce, 2))}
            params.update({"price": entry} if order["type"] == "limit" else {"stopPrice": entry})
            params.update(self._order_prot(sl, tp1))
            if not self._modify_order(order["id"], params):
                self._broadcast(self._t("error"), [f"resize order to {params['qty']} failed - no copies placed"])
                return
        elif (not order["tp"] and levels[0] is not None) or auto:
            params = {"price": entry} if order["type"] == "limit" else {"stopPrice": entry}
            params.update(self._order_prot(sl, tp1))
            self.pool.submit(self._modify_order, order["id"], params)
        futures = [self.pool.submit(self._pending, order, per, tp, sl, tag) for tp in levels[1:]]
        errors = []
        for f in futures:
            try:
                f.result()
            except TLAPIOrderException as exc:
                errors.append(str(exc)[:300])
        if errors:
            self._broadcast(self._t("error"), errors[:1])
        self.state["groups"][tag] = {
            "kind": "pending", "side": order["side"], "entry": entry, "parent_order": order["id"],
            "type": order["type"],
            "target_sl": sl if self.p["sync_sl"] else None, "seen_sl": {},
            "max_members": 0, "be_stage": 0, "step": self.p["tp_step"], "tp1": tp1,
            "auto_sl": self.p["auto_sl"] if auto else 0, "cfg": {k: self.p[k] for k in GROUP_KEYS},
        }
        self._broadcast(self._t("ladder_pending"), [
            f"{order['type']} {order['side']}  {self._lot_line(order['qty'], count, per, reduce)}",
            f"{self._t('price')}: {entry}",
            f"{self._t('tps')}: {', '.join(str(x) if x else '—' for x in levels)}",
            f"{self._t('sl')}: {sl or '—'}" + (f" ({self._t('auto')})" if auto else ""),
        ])

    # ==================================================================
    # Pending groups: move / cancel / fill
    # ==================================================================
    def _handle_pending_groups(self, positions, pending):
        for tag, g in list(self.state["groups"].items()):
            if g["kind"] != "pending":
                continue
            children = [o for o in pending.values() if o["strategy"] == tag]
            parent = pending[g["parent_order"]] if g["parent_order"] in pending else None
            if parent:
                entry = parent["price"] if g["type"] == "limit" else parent["stop_price"]
                if not self._same(entry, g["entry"]):
                    delta = entry - g["entry"]
                    # an automatic SL keeps its distance from the order (set from the new price, so it is right
                    # whether or not the terminal moved it too); the user's own SL stays where it is
                    shift = lambda v: self._round(v + delta) if v else None
                    dist = g["auto_sl"] if "auto_sl" in g else 0
                    sign = 1 if g["side"] == "buy" else -1
                    move_sl = (lambda v: self._round(entry - sign * dist)) if dist else (lambda v: v)
                    calls = []
                    for c in children:
                        params = {"price": entry} if g["type"] == "limit" else {"stopPrice": entry}
                        params.update(self._order_prot(move_sl(c["sl"]), shift(c["tp"])))
                        calls.append(self.pool.submit(self._modify_order, c["id"], params))
                    if parent["tp"] or dist:
                        params = {"price": entry} if g["type"] == "limit" else {"stopPrice": entry}
                        params.update(self._order_prot(move_sl(parent["sl"]), shift(parent["tp"])))
                        calls.append(self.pool.submit(self._modify_order, parent["id"], params))
                    if g["target_sl"]:
                        g["target_sl"] = move_sl(g["target_sl"])
                    if "tp1" in g and g["tp1"]:
                        g["tp1"] = shift(g["tp1"])      # the TPs moved with the order
                    for c in calls:
                        c.result()
                    self._broadcast(self._t("pending_moved"), [f"{g['entry']} → {entry}"])
                    g["entry"] = self._round(entry)
                continue
            if self.feed:
                pos_id = self.feed.filled.get(g["parent_order"])
                if pos_id and pos_id not in positions:
                    continue    # fill seen, Position event not here yet
            else:
                pos_id = self.api.get_position_id_from_order_id(int(g["parent_order"]))
            if pos_id and str(pos_id) in positions:
                g["kind"] = "open"
                g["parent"] = str(pos_id)
                g["entry"] = positions[str(pos_id)]["price"]
                self.state["seen_positions"].append(str(pos_id))
                self._broadcast(self._t("pending_filled"), [f"{self._t('price')}: {g['entry']}"])
            else:
                for c in [self.pool.submit(self._delete_order, c["id"]) for c in children]:
                    c.result()
                del self.state["groups"][tag]
                self._broadcast(self._t("pending_cancel"), [f"{len(children)}"])

    # ==================================================================
    # Open groups: SL sync, BE after TP1, cleanup
    # ==================================================================
    def _set_sl(self, member, target, now):
        pid = member["id"]
        if pid in self.sl_pending:
            sent_target, sent_at, attempts = self.sl_pending[pid]
            if self._same(sent_target, target) and now - sent_at < self.p["sl_retry_seconds"]:
                return
            attempts = attempts + 1 if self._same(sent_target, target) else 0
        else:
            attempts = 0
        try:
            if target is None and member["sl_id"] != "0":
                ok = self._delete_order(member["sl_id"])     # a PATCH without stopLoss leaves the SL in place
                via = "delete"
            elif target is None:
                ok = True
                via = "none"
            elif attempts % 2 == 1 and member["sl_id"] != "0":
                ok = self._modify_order(member["sl_id"], {"stopPrice": target})
                via = "order"
            else:
                ok = self._protect(member, target, member["tp"])
                via = "position"
        except Exception as exc:    # e.g. the server refuses an SL the price already passed - retried next round
            ok, via = f"refused: {str(exc)[:120]}", "error"
        log.info(f"set SL {pid} -> {target} via {via} ok={ok} attempt={attempts}")
        self.sl_pending[pid] = (target, now, attempts)

    def _move_steps(self, g, members, clear):
        """be_to="steps": every step_every levels reached, the SL of all of them (runner included) goes up one step -
        level N reached -> SL to level N - step_every (level 0 = entry + be_offset, level k = TP k).
        step_every=1 with step_start=2: nothing at TP1, TP2 -> entry, then TP3 -> TP2, TP4 -> TP3 ..."""
        sign = 1 if g["side"] == "buy" else -1
        step = g["step"] if "step" in g else self.p["tp_step"]
        tp1 = g["tp1"] if "tp1" in g and g["tp1"] else self._round(g["entry"] + sign * step)
        every = max(1, int(self._c(g, "step_every")))
        reached = self._tp_hits(g, members)
        if "best" in g:
            # past the last TP only the runner is left: the price says how many levels it went
            # (floor, not int: a price between entry and TP1 is level 0, not 1)
            reached = max(reached, math.floor((g["best"] - tp1) * sign / step + 1e-9) + 1)
        level = reached // every * every
        start = max(every, int(self._c(g, "step_start")) if every == 1 else every)
        done = g["step_level"] if "step_level" in g else 0
        if level < start or level <= done:
            return clear
        g["step_level"] = level
        j = 0 if level == start else level - every
        sl = self._round(g["entry"] + sign * self._c(g, "be_offset")) if j == 0 else self._round(tp1 + sign * step * (j - 1))
        if g["target_sl"] is not None and (sl - g["target_sl"]) * sign <= 0:
            return clear
        if "runner_sl" in g:
            del g["runner_sl"]          # in steps the runner climbs with the rest
        g["target_sl"] = sl
        g["clear_sl"] = False
        self._broadcast(self._t("step_up"), [f"TP{level} → {self._t('sl')}: {sl}" + (" (BE)" if j == 0 else f" (TP{j})")])
        return False

    def _track(self, g, members):
        """Remember each member's TP and the best price the group has seen, so a trade that left can be told apart:
        closed at its TP, or by hand / by its SL."""
        if "tp_of" not in g:
            g["tp_of"] = {}
        for m in members:
            if m["tp"] is not None:
                g["tp_of"][m["id"]] = m["tp"]
        sign = 1 if g["side"] == "buy" else -1
        for m in members:
            if m["mark"] is not None and ("best" not in g or (m["mark"] - g["best"]) * sign > 0):
                g["best"] = m["mark"]

    def _tp_hits(self, g, members):
        """How many TPs filled. A member that is gone counts only if it had a TP and the price got there - closing a
        trade by hand is not a TP. Without prices (polling) every member that left counts, as before."""
        if "best" not in g:
            return g["max_members"] - len(members)
        sign = 1 if g["side"] == "buy" else -1
        ids = [m["id"] for m in members]
        closed = self.feed.tp_closed if self.feed else {}
        return sum(1 for pid, tp in g["tp_of"].items()
                   if pid not in ids and (pid in closed or (g["best"] - tp) * sign >= -self.p["tp_hit_slack"]))

    def _move_after_tp1(self, g, members, clear):
        """SL of the rest after TP1 (see be_to). Returns the group's clear flag."""
        be_to = self._c(g, "be_to")
        if be_to == "steps":
            return self._move_steps(g, members, clear)
        stage = g["be_stage"] if "be_stage" in g else (2 if "be_done" in g and g["be_done"] else 0)
        hits = self._tp_hits(g, members)
        if hits < 1 or stage == 2:
            return clear
        sign = 1 if g["side"] == "buy" else -1
        step = g["step"] if "step" in g else self.p["tp_step"]
        # the TP1 the ladder opened with; a filled pending group's entry is the fill price
        tp1 = g["tp1"] if "tp1" in g and g["tp1"] else self._round(g["entry"] + sign * step)
        entry_sl = self._round(g["entry"] + sign * self._c(g, "be_offset"))
        header, be = None, None
        if be_to == "two_step":
            trigger = tp1 + sign * step * self._c(g, "lock_tp1_at")
            # the best price seen, not just the latest: a spike to the trigger between two samples still counts
            ran_on = "best" in g and (g["best"] - trigger) * sign >= 0
            if hits >= 2 or ran_on:
                header, be, stage = "lock_tp1", tp1, 2
            elif stage == 0:
                header, be, stage = "be", entry_sl, 1
        else:
            header, be, stage = "be", tp1 if be_to == "tp1" else entry_sl, 2
        g["be_stage"] = stage
        if be is None or not (g["target_sl"] is None or (be - g["target_sl"]) * sign > 0):
            return clear
        lines = [f"{self._t('sl')}: {be}"]
        if self._c(g, "runner_keep_be") and not self._same(be, entry_sl) and any(m["tp"] is None for m in members):
            # the runner keeps the entry SL (or a better one it already had) so one trade rides the whole move
            keep = g["target_sl"] if g["target_sl"] is not None and (g["target_sl"] - entry_sl) * sign > 0 else entry_sl
            g["runner_sl"] = keep
            lines.append(f"Runner: {keep}")
        g["target_sl"] = be
        g["clear_sl"] = False
        self._broadcast(self._t(header), lines)
        return False

    def _want_sl(self, g, m):
        """The SL a member should have: the group's, except the runner after its SL was kept at entry."""
        if "runner_sl" in g and m["tp"] is None:
            return g["runner_sl"]
        return g["target_sl"]

    def _handle_open_groups(self, positions):
        now = time.time()
        for tag, g in list(self.state["groups"].items()):
            if g["kind"] != "open":
                continue
            members = [p for p in positions.values()
                       if p["strategy"] == tag or p["id"] == g["parent"]]
            if not members:
                del self.state["groups"][tag]
                for pid in list(self.sl_pending):
                    if pid not in positions:
                        del self.sl_pending[pid]
                self._broadcast(self._t("group_done"), [tag])
                continue
            g["max_members"] = max(g["max_members"], len(members))
            self._track(g, members)
            if "adopted" in g:
                # taken over from an earlier run (restart, other machine): an SL set while nobody was managing the
                # group counts as the user's - the most common one among the trades goes to all of them
                del g["adopted"]
                sls = [m["sl"] for m in members if m["sl"] is not None]
                if self.p["sync_sl"] and g["target_sl"] is None and sls:
                    g["target_sl"] = max(set(sls), key=sls.count)
                    self._broadcast(self._t("sl_sync"), [f"{self._t('sl')}: {g['target_sl']}",
                                                         f"{self._t('count')}: {len(members)}"])

            clear = "clear_sl" in g and g["clear_sl"]
            if self.p["sync_sl"]:
                changed_to = None
                removed = False
                for m in members:
                    prev = g["seen_sl"][m["id"]] if m["id"] in g["seen_sl"] else "unseen"
                    if prev != "unseen" and not self._same(prev, m["sl"]):
                        if m["sl"] is not None and (clear or not self._same(m["sl"], self._want_sl(g, m))):
                            changed_to = m["sl"]
                        elif m["sl"] is None and g["target_sl"] is not None and self.p["sync_sl_delete"]:
                            removed = True
                    g["seen_sl"][m["id"]] = m["sl"]
                if "runner_sl" in g and (changed_to is not None or removed):
                    del g["runner_sl"]          # the user's own SL change goes to all of them, runner included
                if changed_to is not None:
                    g["target_sl"] = changed_to
                    g["clear_sl"] = clear = False
                    self._broadcast(self._t("sl_sync"), [f"{self._t('sl')}: {changed_to}",
                                                         f"{self._t('count')}: {len(members)}"])
                elif removed:
                    g["target_sl"] = None
                    g["clear_sl"] = clear = True
                    self._broadcast(self._t("sl_cleared"), [f"{self._t('count')}: {len(members)}"])

            if self._c(g, "be_after_tp1") and g["max_members"] >= 2:
                clear = self._move_after_tp1(g, members, clear)

            if g["target_sl"] is not None or clear:
                calls = []
                for m in members:
                    want = self._want_sl(g, m)
                    if not self._same(m["sl"], want):
                        calls.append(self.pool.submit(self._set_sl, m, want, now))
                    elif m["id"] in self.sl_pending:
                        del self.sl_pending[m["id"]]
                for c in calls:
                    c.result()

    # ==================================================================
    # Main loop
    # ==================================================================
    def tick(self):
        snap = self.snapshot()
        if snap is None:
            return
        positions, pending = snap
        seen_p = set(self.state["seen_positions"])
        seen_o = set(self.state["seen_orders"])

        if self.p["handle_pending"]:
            self._handle_pending_groups(positions, pending)
            seen_p = set(self.state["seen_positions"])
            for o in pending.values():
                if self._is_manual(o["strategy"]) and o["id"] not in seen_o:
                    self.state["seen_orders"].append(o["id"])
                    self._save_state()
                    if self.p["enabled"]:
                        self._ladder_pending(o)

        for p in positions.values():
            if not self._is_manual(p["strategy"]):
                continue
            tag = f"{self.p['strategy_tag']}-{p['id']}"
            has_children = any(q["strategy"] == tag for q in positions.values())
            if has_children and tag not in self.state["groups"]:
                self._adopt_group(tag, p)
            if p["id"] not in seen_p and self.feed and not has_children:
                first = self.first_seen.setdefault(p["id"], time.time())
                if time.time() - first < 1.0:
                    continue
            if p["id"] not in seen_p:
                self.state["seen_positions"].append(p["id"])
                self._save_state()
                if not has_children and self.p["enabled"]:
                    self._ladder_position(p)

        self._handle_open_groups(positions)

        # forget ids that no longer exist
        self.state["seen_positions"] = [x for x in self.state["seen_positions"] if x in positions]
        self.state["seen_orders"] = [x for x in self.state["seen_orders"] if x in pending]
        self.first_seen = {k: v for k, v in self.first_seen.items() if k in positions}
        self._save_state()
        self.summary = {
            "positions": len(positions), "pending": len(pending), "at": time.time(),
            "groups": [{"tag": tag, "side": g["side"], "entry": g["entry"], "sl": g["target_sl"],
                        "members": sum(1 for p in positions.values()
                                       if p["strategy"] == tag or ("parent" in g and p["id"] == g["parent"])),
                        "kind": g["kind"]}
                       for tag, g in self.state["groups"].items()],
        }

    summary = None      # last tick's view, read by the Discord panel
    on_alert = None     # set by the Discord panel: (header, lines) of errors / startup

    def _single_instance(self):
        """Two managers on one account would ladder every trade twice - refuse to start a second one."""
        self._lock = open(self.state_file + ".lock", "w")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"another ladder manager is already running for this account ({self.state_file})")

    def _shutdown(self):
        if self.feed:
            self.feed.close()
        self.pool.shutdown(wait=False)
        if hasattr(self, "_lock"):
            self._lock.close()
        log.info(f"ladder manager for {self.account_id} stopped")

    def run(self, once=False):
        self._single_instance()
        try:
            self._run(once)
        finally:
            self._shutdown()

    def _run(self, once):
        self.connect()
        if self.p["use_stream"] and os.environ.get("TL_DEV_KEY"):
            self.feed = StreamFeed(self.p["stream_url"], self.api.account_id, self.p["symbol"], self.login)
            self._ensure_stream()
        else:
            log.info("no TL_DEV_KEY / use_stream=False - REST polling mode")
        self._load_state()
        if not os.path.exists(self.state_file):
            snap = None
            while snap is None and not self.stop.is_set():
                snap = self.snapshot()
                if snap is None:
                    self.stop.wait(1.5)
            if snap is None:
                return
            positions, pending = snap
            self.state["seen_positions"] = list(positions)
            self.state["seen_orders"] = list(pending)
            self._save_state()
            log.info(f"first run: ignoring {len(positions)} open positions, {len(pending)} pending orders")
        self._broadcast(self._t("start"), [
            f"account={self.account_id} enabled={self.p['enabled']}",
            f"{self.p['symbol']} mode={self.p['mode']} copies={self.p['copies']} tp_step={self.p['tp_step']} "
            f"runner_last={self.p['runner_last']} sync_sl={self.p['sync_sl']} "
            f"be_after_tp1={self.p['be_after_tp1']} active={self.p['active']} "
            f"source={'stream' if self.feed else 'polling'}",
        ])
        base = self.p["tick_seconds"] if self.feed else self.p["poll_seconds"]
        backoff = base
        while not self.stop.is_set():
            try:
                if self.feed:
                    if not self.feed.alive:
                        log.info("stream down or token expiring - reconnecting")
                        self._ensure_stream()
                    self.feed.changed.clear()
                self.tick()
                backoff = base
            except Exception as exc:
                if "429" in str(exc):
                    log.warning("rate limited (429) - skipping this round")
                    backoff = base + 1
                else:
                    backoff = min(backoff * 2, 30)
                    log.exception(f"tick failed, retry in {backoff:.1f}s: {exc}")
            if once:
                return
            if self.feed and backoff == base:
                if self.feed.changed.wait(timeout=base):
                    time.sleep(0.15)    # one fill = a burst of events; take them together
            else:
                self.stop.wait(backoff)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--active", action="store_true", help="send real orders (default: dry run)")
    ap.add_argument("--once", action="store_true", help="single tick then exit")
    ap.add_argument("--mode", choices=["split", "duplicate"], help="split = divide your lot, duplicate = same lot each")
    ap.add_argument("--copies", type=int, help="trades in total, yours included")
    ap.add_argument("--tp-step", type=float, help="dollars between the TPs")
    ap.add_argument("--be", action="store_true", help="after TP1 move the others' SL to entry")
    ap.add_argument("--be-offset", type=float, help="BE this many dollars past entry")
    ap.add_argument("--max-lots", type=float, help="duplicate: cap on the whole ladder's lots (copies shrink to fit)")
    ap.add_argument("--no-runner", action="store_true", help="last trade gets a TP too")
    ap.add_argument("--keep-sl-on-delete", action="store_true",
                    help="deleting one trade's SL puts the group SL back instead of removing it from all")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)s | %(message)s", force=True)
    logging.getLogger("tradelocker").setLevel(logging.CRITICAL)
    logging.getLogger("tradelocker").propagate = False
    params = dict(PARAMS)
    for key in params:
        env_key = "LADDER_" + key.upper()
        if env_key in os.environ:
            raw = os.environ[env_key]
            cur = params[key]
            params[key] = raw.lower() in ("1", "true", "yes") if isinstance(cur, bool) \
                else type(cur)(raw)
    if args.active:
        params["active"] = True
    for key, value in (("mode", args.mode), ("copies", args.copies), ("tp_step", args.tp_step),
                       ("be_offset", args.be_offset), ("max_total_lots", args.max_lots)):
        if value is not None:
            params[key] = value
    if args.be:
        params["be_after_tp1"] = True
    if args.no_runner:
        params["runner_last"] = False
    if args.keep_sl_on_delete:
        params["sync_sl_delete"] = False
    state_file = os.environ.get("STATE_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                           f"state-{os.environ.get('TL_ACCOUNT_ID', '0')}.json"))
    try:
        LadderManager(params, state_file).run(once=args.once)
    except RuntimeError as exc:
        log.error(f"{exc} - exiting")
        sys.exit(1)


if __name__ == "__main__":
    main()
