"""
Ladder on the server: runs one or more ladders ("משכפלים"), each a ladder manager on its own TradeLocker account
with its own on/off and settings, and a Discord control panel per ladder.

#ladder (CONTROL_CHANNEL_ID): a guide first (what each button does, with examples), then a pinned header (list of ladders, add / delete) and under it one panel
  message per ladder, edited in place - on/off, account picker, mode / BE / runner / SL-delete toggles, settings modal.
  Two ladders never share an account (each trade would be laddered twice).
#ladder-log: everything the manager reports (via DISCORD_WEBHOOK; errors also go to DISCORD_WEBHOOK_ALERT).

"Off" only stops laddering NEW trades - open groups keep their SL synced, so nothing is left unmanaged.
Only ALLOWED_USER_ID can press anything.

Env: LADDER_DISCORD_TOKEN, CONTROL_CHANNEL_ID, ALLOWED_USER_ID, DATA_DIR, ACCOUNT_NOTES ("590661=copied to Colmex live;...")
plus everything ladder_manager needs (TL_ENV, TL_SERVER, TL_EMAIL, TL_PASSWORD, TL_DEV_KEY, DISCORD_WEBHOOK[_ALERT]).
"""
import asyncio
import json
import logging
import os
import sys
import threading
import time

import discord
import requests

import ladder_manager as lm

log = logging.getLogger("ladder.bot")

DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
ALLOWED_USER_ID = int(os.environ["ALLOWED_USER_ID"])
CONTROL_CHANNEL_ID = int(os.environ["CONTROL_CHANNEL_ID"])

# what the panel may change; everything else stays as in lm.PARAMS
EDITABLE = ("enabled", "mode", "copies", "tp_step", "runner_last", "be_after_tp1", "be_to", "be_offset", "lock_tp1_at",
            "sync_sl_delete", "auto_sl", "runner_keep_be", "step_every", "step_start")

# the 🔒 menu: (be_after_tp1, be_to, step_every, step_start), label, description (example: buy 4000, TP every 3$)
SL_MODES = [
    ((False, "two_step", 1, 1), "כבוי", "הסטופ לא זז. כל עסקה נסגרת ב-TP שלה או בסטופ המקורי"),
    ((True, "two_step", 1, 1), "שני שלבים", "TP1 → סטופ לכניסה. חצי הדרך ל-TP2 → סטופ ל-TP1"),
    ((True, "steps", 1, 1), "🪜 מדרגה בכל TP", "כל TP הסטופ עולה מדרגה: TP1→כניסה, TP2→TP1, TP3→TP2…"),
    ((True, "steps", 1, 2), "🪜 כניסה ב-TP2, ואז מדרגה בכל TP", "TP1→כלום, TP2→כניסה, TP3→TP2, TP4→TP3…"),
    ((True, "steps", 2, 1), "🪜 מדרגה כל 2 TP", "TP2→כניסה, TP4→TP2, TP6→TP4… (יותר מקום לנשום)"),
    ((True, "tp1", 1, 1), "מיד ל-TP1", "TP1 נתפס → הסטופ של השאר ל-TP1"),
    ((True, "entry", 1, 1), "לכניסה", "TP1 נתפס → הסטופ של השאר לכניסה ונשאר שם"),
]


# a template = these settings, written as text in a window ("label: value" per line); loading one sets all of them
TEMPLATE_FIELDS = [   # (label, key, kind)
    ("עסקאות", "copies", "int"),
    ("מרחק TP", "tp_step", "float"),
    ("Runner", "runner_last", "bool"),
    ("Runner בכניסה", "runner_keep_be", "bool"),
    ("סטופ אוטומטי", "auto_sl", "float"),
    ("BE offset", "be_offset", "float"),
    ("נעילת TP1", "lock_tp1_at", "float"),
    ("סטופ אחרי TP", "sl_mode", "mode"),
]
TEMPLATE_KEYS = ("copies", "tp_step", "runner_last", "runner_keep_be", "auto_sl", "be_offset", "lock_tp1_at",
                 "be_after_tp1", "be_to", "step_every", "step_start")


def template_text(p):
    """The settings as the template window shows them, with the SL modes listed under it."""
    lines = []
    for label, key, kind in TEMPLATE_FIELDS:
        if kind == "bool":
            value = "כן" if p[key] else "לא"
        elif kind == "mode":
            value = next((str(i + 1) for i, (mode, _, _) in enumerate(SL_MODES) if mode == sl_mode(p)), "1")
        else:
            value = f"{p[key]:g}"
        lines.append(f"{label}: {value}")
    lines.append("# סטופ אחרי TP: " + " · ".join(f"{i + 1}={label.replace('🪜 ', '')}"
                                               for i, (_, label, _) in enumerate(SL_MODES)))
    return "\n".join(lines)


def parse_template(text, base):
    """Text from the template window -> settings (a line left out keeps base's value). Raises ValueError with why."""
    labels = {label.replace(" ", "").lower(): (key, kind) for label, key, kind in TEMPLATE_FIELDS}
    out = {k: base[k] for k in TEMPLATE_KEYS}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        sep = min([i for i in (line.find(":"), line.find("=")) if i >= 0], default=-1)
        if sep < 0:
            raise ValueError(f"שורה בלי ':' - {line}")
        name, value = line[:sep].replace(" ", "").lower(), line[sep + 1:].strip()
        if name not in labels:
            raise ValueError(f"לא מכיר '{line[:sep].strip()}'")
        key, kind = labels[name]
        try:
            if kind == "bool":
                if value.lower() not in ("כן", "לא", "yes", "no", "1", "0"):
                    raise ValueError
                out[key] = value.lower() in ("כן", "yes", "1")
            elif kind == "mode":
                if int(value) < 1:
                    raise ValueError
                (out["be_after_tp1"], out["be_to"], out["step_every"], out["step_start"]), _, _ = SL_MODES[int(value) - 1]
            elif kind == "int":
                out[key] = int(value)
            else:
                out[key] = float(value)
        except (ValueError, IndexError):
            raise ValueError(f"ערך לא תקין ב-'{line[:sep].strip()}': {value}")
    if not 2 <= out["copies"] <= 10:
        raise ValueError("עסקאות: 2-10")
    if not 0.1 <= out["tp_step"] <= 100:
        raise ValueError("מרחק TP: 0.1-100")
    if not 0 <= out["auto_sl"] <= 200:
        raise ValueError("סטופ אוטומטי: 0-200")
    if not 0 <= out["be_offset"] < out["tp_step"]:
        raise ValueError("BE offset: בין 0 למרחק ה-TP")
    if not 0 < out["lock_tp1_at"] <= 1:
        raise ValueError("נעילת TP1: בין 0 ל-1")
    return out


def template_summary(t):
    mode = next((label for mode, label, _ in SL_MODES if mode == sl_mode(t)), "")
    return (f"{t['copies']} עסקאות · TP כל {t['tp_step']:g}$ · SL {t['auto_sl']:g}$ · {mode}"
            + (" · Runner" if t["runner_last"] else ""))[:100]


def sl_mode(p):
    """The current (be_after_tp1, be_to, step_every, step_start) as it appears in SL_MODES."""
    if not p["be_after_tp1"]:
        return (False, "two_step", 1, 1)
    if p["be_to"] != "steps":
        return (True, p["be_to"], 1, 1)
    return (True, "steps", p["step_every"], p["step_start"] if p["step_every"] == 1 else 1)


def logins():
    """demo always; live when TL_LIVE_ENV is set (same email/password unless TL_LIVE_EMAIL/TL_LIVE_PASSWORD)."""
    out = {"demo": lm.env_login("TL_")}
    if os.environ.get("TL_LIVE_ENV"):
        out["live"] = lm.env_login("TL_LIVE_")
    return out


def split_key(key):
    """Account keys: "590661" = demo (kept bare from before live existed), "live:589911" = live."""
    return ("live", key[5:]) if key.startswith("live:") else ("demo", key)


def account_label(key):
    kind, acc = split_key(key)
    return f"{'🔴 LIVE' if kind == 'live' else 'Demo'} {acc}"


def account_notes():
    notes = {}
    for item in os.environ.get("ACCOUNT_NOTES", "").split(";"):
        if "=" in item:
            acc, text = item.split("=", 1)
            notes[acc.strip()] = text.strip()
    return notes


# ======================================================================
# Settings (survive deploys: DATA_DIR is a volume)
# ======================================================================
class Settings:
    def __init__(self):
        # ladders: [{id, account, panel_message, params}]; limits: account -> max lots of a whole ladder (per account)
        # templates: name -> settings (TEMPLATE_KEYS), shared by all the ladders
        self.data = {"guide_message": 0, "header_message": 0, "ladders": [], "limits": {}, "templates": {}}
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE) as f:
                loaded = json.load(f)
            if "ladders" not in loaded:
                # before there were several ladders: the one account/params becomes ladder 1
                loaded["ladders"] = [{"id": 1, "account": loaded.pop("account", ""),
                                      "panel_message": loaded.pop("panel_message", 0),
                                      "params": loaded.pop("params", {"enabled": False})}]
            self.data.update(loaded)
        if not self.data["ladders"]:
            self.new_ladder(os.environ.get("TL_ACCOUNT_ID", ""))

    def save(self):
        tmp = SETTINGS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=1)
        os.replace(tmp, SETTINGS_FILE)

    def new_ladder(self, account=""):
        lid = max([lad["id"] for lad in self.data["ladders"]] + [0]) + 1
        ladder = {"id": lid, "account": account, "panel_message": 0, "params": {"enabled": False}}
        self.data["ladders"].append(ladder)
        self.save()
        return ladder

    def params(self, ladder):
        params = dict(lm.PARAMS)
        params["active"] = True
        for key, value in ladder["params"].items():
            if key in EDITABLE:
                params[key] = value
        return params


# ======================================================================
# Engine: one ladder = one LadderManager thread for its account, restarted if it crashes
# ======================================================================
class Engine:
    def __init__(self, settings, ladder):
        self.settings = settings
        self.ladder = ladder                # its entry in settings.data["ladders"]
        self.lid = ladder["id"]
        self.params = settings.params(ladder)   # shared with the running manager - edits apply on its next tick
        self.manager = None
        self.stop = None
        self.thread = None
        self.error = ""
        self.last = ""      # the panel's "last update" line: startup, errors, what a button did

    @property
    def name(self):
        return f"משכפל {self.lid}"

    @property
    def account(self):
        return self.ladder["account"]

    def note(self, text):
        self.last = f"{text} · {time.strftime('%H:%M:%S')}"

    def _alert(self, header, lines):
        if header.startswith("🚀"):
            self.note("🚀 השרת מחובר לחשבון ומנהל אותו")
        else:
            self.note(f"{header}: {' · '.join(lines)[:300]}")

    def start(self, account):
        self.stop_engine()
        if not account:
            return
        self.params["max_total_lots"] = self.limit(account)
        self.params["label"] = f"{self.name} · {account_label(account)}"
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._supervise, args=(str(account), self.stop), daemon=True)
        self.thread.start()

    def stop_engine(self):
        if self.thread:
            self.stop.set()
            self.thread.join(timeout=20)
        self.manager = None
        self.thread = None

    def _supervise(self, account, stop):
        kind, acc_id = split_key(account)
        state_file = os.path.join(DATA_DIR, f"state-{account.replace(':', '-')}.json")
        login = logins()[kind]
        while not stop.is_set():
            try:
                self.manager = lm.LadderManager(self.params, state_file, account_id=acc_id, stop_event=stop, login=login)
                self.manager.on_alert = self._alert
                self.error = ""
                self.manager.run()
            except RuntimeError as exc:
                if "already running" not in str(exc):
                    self.error = str(exc)[:200]
                    log.exception(f"ladder manager for {account} crashed - restarting in 15s")
                else:
                    # a deploy starts the new container before the old one lets go of the account: wait for it
                    log.info(f"previous ladder manager for {account} still holds the account - retrying in 15s")
            except Exception as exc:
                self.error = str(exc)[:200]
                log.exception(f"ladder manager for {account} crashed - restarting in 15s")
            stop.wait(15)

    def limit(self, account):
        limits = self.settings.data["limits"]
        return float(limits[account]) if account in limits else 0.0

    def set_limit(self, account, lots):
        self.settings.data["limits"][account] = lots
        self.settings.save()
        if account == self.account:
            self.params["max_total_lots"] = lots

    def set(self, key, value):
        self.params[key] = value
        self.ladder["params"][key] = value
        self.settings.save()

    def apply_template(self, name):
        for key, value in self.settings.data["templates"][name].items():
            self.set(key, value)

    def template_name(self):
        """The template the current settings match, if any."""
        return next((name for name, t in self.settings.data["templates"].items()
                     if all(key in self.params and self.params[key] == value for key, value in t.items())), None)

    def open_groups(self):
        if self.manager is None or self.manager.summary is None:
            return []
        return [g for g in self.manager.summary["groups"] if g["members"] > 0 or g["kind"] == "pending"]

    def status_line(self):
        p = self.params
        acc = account_label(self.account) if self.account else "בלי חשבון"
        groups = self.open_groups()
        return (f"**{self.name}** · {acc} · {'🟢 פועל' if p['enabled'] and self.account else '🔴 כבוי'} · "
                f"{'שכפול' if p['mode'] == 'duplicate' else 'חלוקה'} ×{p['copies']}"
                + (f" · {len(groups)} קבוצות פתוחות" if groups else ""))


def list_accounts():
    """All accounts of the demo and live logins: [(key, label)]. (The API "name" is an internal brand#user id.)"""
    notes = account_notes()
    out = []
    for kind, login in logins().items():
        base = login["env"] + "/backend-api"
        try:
            r = requests.post(base + "/auth/jwt/token", json={"email": login["email"], "password": login["password"],
                                                              "server": login["server"]}, timeout=10)
            r.raise_for_status()
            r = requests.get(base + "/auth/jwt/all-accounts", headers={"Authorization": "Bearer " + r.json()["accessToken"]},
                             timeout=10)
            r.raise_for_status()
        except Exception as exc:
            log.warning(f"{kind} accounts: {exc}")
            continue
        for a in r.json()["accounts"]:
            key = str(a["id"]) if kind == "demo" else f"live:{a['id']}"
            label = f"{account_label(key)} · {float(a['accountBalance']):,.2f} {a['currency']}"
            if str(a["id"]) in notes:
                label += f" ⚠️ {notes[str(a['id'])]}"
            out.append((key, label[:100]))
    return out


# ======================================================================
# Discord panel
# ======================================================================
def after_tp1_text(p):
    if not p["be_after_tp1"]:
        return "ה-SL לא זז"
    if p["be_to"] == "steps" and p["step_every"] == 1 and p["step_start"] == 2:
        return (f"מדרגות מ-TP2 - ב-TP1 ה-SL לא זז, TP2 → כניסה +{p['be_offset']:g}$, ואז כל TP ה-SL של כולן "
                f"(כולל ה-Runner) עולה מדרגה: TP3 → TP2, TP4 → TP3…")
    if p["be_to"] == "steps" and p["step_every"] == 1:
        return f"מדרגות - כל TP ה-SL של כולן (כולל ה-Runner) עולה מדרגה: TP1 → כניסה +{p['be_offset']:g}$, TP2 → TP1, TP3 → TP2…"
    if p["be_to"] == "steps":
        return (f"מדרגות כל 2 TP - ה-SL של כולן (כולל ה-Runner): TP2 → כניסה +{p['be_offset']:g}$, "
                f"TP4 → TP2, TP6 → TP4…")
    if p["be_to"] == "two_step":
        where = "TP2" if p["lock_tp1_at"] >= 1 else f"{p['lock_tp1_at'] * 100:g}% מהדרך ל-TP2"
        return f"ה-SL של השאר לכניסה +{p['be_offset']:g}$, וכשהמחיר מגיע ל-{where} - ל-TP1" + runner_text(p)
    if p["be_to"] == "tp1":
        return "ה-SL של השאר עובר ל-TP1 (נועל את רווח TP1)" + runner_text(p)
    return f"ה-SL של השאר עובר לכניסה +{p['be_offset']:g}$"


def runner_text(p):
    if p["runner_keep_be"] and p["runner_last"]:
        return f" · ה-Runner נשאר בכניסה +{p['be_offset']:g}$"
    return ""


GUIDE = [
    ("📖 מדריך - מה המשכפל עושה", 0x95a5a6, """\
אתה פותח עסקה אחת ב-TradeLocker (ידנית, או פקודת Limit/Stop) - המשכפל הופך אותה ל**סולם** של כמה עסקאות באותו כיוון, \
כל אחת עם TP אחר, ומנהל להן את הסטופ ביחד.

**דוגמה:** קנייה ב-**4000**, SL **3989**, הגדרות: 5 עסקאות · מרחק בין TP-ים 3$ · Runner דלוק
• עסקה 1 → TP **4003**
• עסקה 2 → TP **4006**
• עסקה 3 → TP **4009**
• עסקה 4 → TP **4012**
• עסקה 5 → **Runner** - בלי TP, רצה עד הסטופ
לכולן אותו SL (3989). הזזת ה-SL באחת → כולן זזות איתה.

**פקודה ממתינה:** Limit/Stop מקבלת סולם של פקודות ממתינות באותו מחיר. הזזת המחיר של הפקודה שלך מזיזה את כולן, ביטול מבטל את כולן.

**איך מתחילים:** בלוח של המשכפל → בוחרים חשבון בתפריט → 🟢 הדלק → פותחים עסקה כרגיל."""),
    ("🎛 הכפתורים", 0x95a5a6, """\
🟢 **הדלק / 🔴 כבה** - כבוי = לא משכפל עסקאות **חדשות**. קבוצות שכבר פתוחות ממשיכות לקבל סנכרון סטופ.
⚙️ **הגדרות** - כמה עסקאות בסולם (2-10), מרחק בין TP-ים בדולרים, ומקסימום אונקיות לכל הסולם בחשבון.
🛡 **סטופ** - סטופ אוטומטי (עסקה שנפתחה בלי SL מקבלת SL במרחק הזה, למשל 10$), BE offset (כמה מעבר לכניסה \
הסטופ נכנס, למשל 0.5$ = נועל 0.5$ לאונקיה), ונעילת TP1 במצב "שני שלבים".
🔄 **רענן** - טוען מחדש את רשימת החשבונות.
✂️ **חלוקה / שכפול** - עסקה של 0.10 לוט, 5 עסקאות:
  • **שכפול** → 5 × 0.10 = 0.50 לוט בסך הכול (50 אונקיות)
  • **חלוקה** → 5 × 0.02 = נשאר 0.10 לוט, מחולק לסולם
🔒 **סטופ אחרי TP** - מה קורה לסטופ כשה-TP-ים נתפסים (פירוט בחלק הבא של המדריך). \
נפתח תפריט עם כל המצבים, המצב הנוכחי מסומן - בוחרים, והלוח מראה מה נבחר.
🏃 **Runner** - העסקה האחרונה בלי TP (דלוק) או עם TP כמו כולן (כבוי).
🚫 **מחיקת SL** - מחקת SL מעסקה אחת: נמחק מכולן, או שהמשכפל מחזיר לה את ה-SL.
📋 **תבניות** - שומרים את כל ההגדרות (עסקאות, מרחק TP, Runner, סטופים, מצב סטופ אחרי TP) בשם, \
ואחר כך בוחרים תבנית מהרשימה וכל ההגדרות נטענות בבת אחת - למשל סקאלפ מהיר מול עסקה ארוכה. ➕ פותח חלון עם ההגדרות \
הנוכחיות כטקסט (שורה לכל הגדרה) - משנים, נותנים שם ושומרים. עסקאות שכבר פתוחות ממשיכות עם ההגדרות שהן נפתחו איתן.
🎯 **Runner בכניסה** - במצבים "שני שלבים" ו"מיד ל-TP1": כשהשאר עוברות ל-TP1, ה-Runner נשאר בכניסה (סיכון 0, יותר מקום לרוץ). \
במצבי המדרגות ה-Runner תמיד עולה יחד עם כולן."""),
    ("🔒 מצבי הסטופ אחרי TP - בדוגמה של קנייה ב-4000", 0x95a5a6, """\
(5 עסקאות · TP כל 3$: 4003 / 4006 / 4009 / 4012 · SL 3989 · BE offset 0.5$)

**כבוי** - הסטופ לא זז. כל עסקה נסגרת ב-TP שלה או ב-3989.

**שני שלבים** - TP1 (4003) נתפס → SL של השאר **4000.5**. המחיר מגיע לחצי הדרך ל-TP2 (4004.5) → SL **4003**.

**🪜 מדרגה בכל TP** - כל TP שנתפס, הסטופ של כולן עולה מדרגה אחת:
4003 → SL **4000.5** · 4006 → SL **4003** · 4009 → SL **4006** · 4012 → SL **4009** · \
ה-Runner ממשיך: 4015 → SL **4012**, 4018 → SL **4015**…

**🪜 כניסה ב-TP2, ואז מדרגה בכל TP** - ב-TP1 לא זזים (נותנים לנשום), אחר כך צמוד:
4003 → כלום · 4006 → SL **4000.5** · 4009 → SL **4006** · 4012 → SL **4009** · ה-Runner: 4015 → SL **4012**…

**🪜 מדרגה כל 2 TP** - מחכים לשני TP-ים, ואז קופצים:
4003 → כלום · 4006 → SL **4000.5** · 4009 → כלום · 4012 → SL **4006** · ה-Runner: 4018 → SL **4012**…

**מיד ל-TP1** - TP1 נתפס → SL של השאר **4003** מיד.
**לכניסה** - TP1 נתפס → SL של השאר **4000.5** ונשאר שם.

**מה לבחור?** "מדרגה בכל TP" הכי שקט: אחרי TP1 כבר אין סיכון, אבל עסקאות שנוגעות ב-TP1, חוזרות לכניסה ורק אז רצות - נסגרות. \
"כניסה ב-TP2, ואז מדרגה בכל TP" נותן לנשום עד TP2 ואז צמוד: פחות עסקאות מרוויחות (בין TP1 ל-TP2 הסטופ עוד מקורי), \
אבל הרווח הממוצע הכי גבוה. "מדרגה כל 2 TP" באמצע."""),
]


def guide_embeds():
    return [discord.Embed(title=title, description=text, color=color) for title, color, text in GUIDE]


def header_embed(engines):
    lines = [e.status_line() for e in engines] or ["אין משכפלים - לחץ ➕ כדי להוסיף"]
    lines += ["", "כל משכפל רץ על חשבון משלו עם הדלקה והגדרות משלו. הלוחות שלהם מתחת להודעה הזאת."]
    return discord.Embed(title="🪜 משכפלים", description="\n".join(lines), color=0x3498db)


def panel_embed(engine):
    """The panel as plain lines, one setting per line (inline fields read badly in Hebrew)."""
    p = engine.params
    key = engine.account
    on = p["enabled"] and bool(key)
    acc = account_label(key) if key else "—"
    note = account_notes().get(split_key(key)[1]) if key else None
    cap = p["max_total_lots"]
    lines = [
        "🟢 **פועל** - משכפל עסקאות חדשות" if on
        else "⚪ **בחר חשבון** בתפריט למטה" if not key
        else "🔴 **כבוי** - לא משכפל עסקאות חדשות (קבוצות פתוחות ממשיכות לקבל סנכרון SL)",
        "",
        f"**חשבון:** {acc}" + (f"  ⚠️ {note}" if note else ""),
        f"**תבנית:** {engine.template_name() or '— (הגדרות ידניות)'}",
        f"**מצב:** {'חלוקה (split) - הלוט שלך מתחלק' if p['mode'] == 'split' else 'שכפול (duplicate) - כל עסקה בלוט שלך'}",
        f"**עסקאות:** {p['copies']}",
        f"**מרחק בין TP-ים:** {p['tp_step']:g}$",
        f"**Runner (אחרונה בלי TP):** {'כן' if p['runner_last'] else 'לא'}",
        "**סטופ אוטומטי:** " + (f"{p['auto_sl']:g}$ מהכניסה לעסקה שנפתחה בלי SL" if p["auto_sl"] else "בלי"),
        "**סטופ אחרי TP:** " + after_tp1_text(p),
        f"**מחיקת SL מעסקה:** {'מוחקת מכולן' if p['sync_sl_delete'] else 'מחזירה לה את ה-SL'}",
        f"**הגבלה לחשבון:** " + (f"{cap * 100:g} אונקיות לכל הסולם" if cap else "בלי הגבלה"),
        "",
    ]
    groups = engine.open_groups()
    if groups:
        lines.append(f"**קבוצות פתוחות ({len(groups)}):**")
        lines += [f"• {'קנייה' if g['side'] == 'buy' else 'מכירה'} @{g['entry']} · {g['members']} עסקאות · SL {g['sl'] or '—'}"
                  + (" (פקודה ממתינה)" if g["kind"] == "pending" else "") for g in groups[:5]]
    else:
        lines.append("**קבוצות פתוחות:** אין")
    if engine.last:
        lines += ["", f"**עדכון אחרון:** {engine.last}"]
    embed = discord.Embed(title=f"🪜 {engine.name}" + (f" · {account_label(key)}" if key else ""),
                          description="\n".join(lines), color=0x2ecc71 if on else 0xe74c3c)
    status = "מחובר" if engine.manager and engine.manager.summary else "מתחבר…" if key else "לא מחובר"
    if engine.error:
        status = f"⚠️ {engine.error}"
    embed.set_footer(text=f"{status} · עודכן {time.strftime('%H:%M:%S')}")
    return embed


class SettingsModal(discord.ui.Modal):
    def __init__(self, panel):
        super().__init__(title=f"הגדרות {panel.engine.name}")
        self.panel = panel
        p = panel.engine.params
        self.copies = discord.ui.TextInput(label="כמה עסקאות בסך הכול (2-10)", default=str(p["copies"]), max_length=2)
        self.tp_step = discord.ui.TextInput(label="מרחק בין TP-ים בדולרים", default=str(p["tp_step"]), max_length=6)
        # in ounces: "20" is 20 oz (0.2 lot) - lots invited typing 20 for 20 oz
        self.max_oz = discord.ui.TextInput(label="מקסימום אונקיות לכל הסולם (0 = בלי הגבלה)",
                                           default=f"{p['max_total_lots'] * 100:g}", max_length=6)
        for item in (self.copies, self.tp_step, self.max_oz):
            self.add_item(item)

    async def on_submit(self, interaction):
        p = self.panel.engine.params
        try:
            copies = int(self.copies.value)
            tp_step = float(self.tp_step.value)
            max_oz = float(self.max_oz.value)
            max_lots = round(max_oz / 100, 2)
            if not 2 <= copies <= 10 or not 0.1 <= tp_step <= 100 or not p["be_offset"] < tp_step \
                    or not 0 <= max_oz <= 500:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                f"ערכים לא תקינים: עסקאות 2-10, מרחק TP 0.1-100 (וגדול מ-BE offset {p['be_offset']:g}$), "
                "מקסימום אונקיות 0-500.", ephemeral=True)
            return
        account = self.panel.engine.account
        if account:
            self.panel.engine.set_limit(account, max_lots)
        self.panel.engine.set("copies", copies)
        self.panel.engine.set("tp_step", tp_step)
        cap = f"{max_oz:g} אונקיות" if max_lots else "בלי"
        await self.panel.confirm(interaction, f"✅ ההגדרות נשמרו: {copies} עסקאות · TP כל {tp_step:g}$ · הגבלה {cap}")


class StopModal(discord.ui.Modal):
    def __init__(self, panel):
        super().__init__(title=f"סטופ לוס · {panel.engine.name}")
        self.panel = panel
        p = panel.engine.params
        self.auto_sl = discord.ui.TextInput(label="סטופ אוטומטי: דולרים מהכניסה (0 = בלי)",
                                            default=f"{p['auto_sl']:g}", max_length=6)
        self.be_offset = discord.ui.TextInput(label="SL לכניסה: כמה דולר מעבר לכניסה", default=f"{p['be_offset']:g}",
                                              max_length=6)
        self.lock_at = discord.ui.TextInput(label="SL ל-TP1 אחרי חלק מהדרך ל-TP2 (0.5 / 1)",
                                            default=f"{p['lock_tp1_at']:g}", max_length=4)
        for item in (self.auto_sl, self.be_offset, self.lock_at):
            self.add_item(item)

    async def on_submit(self, interaction):
        p = self.panel.engine.params
        try:
            auto_sl = float(self.auto_sl.value)
            be_offset = float(self.be_offset.value)
            lock_at = float(self.lock_at.value)
            if not 0 <= auto_sl <= 200 or not 0 <= be_offset < p["tp_step"] or not 0 < lock_at <= 1:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                f"ערכים לא תקינים: סטופ אוטומטי 0-200, BE offset בין 0 למרחק ה-TP ({p['tp_step']:g}$), "
                "נעילת TP1 בין 0 ל-1.", ephemeral=True)
            return
        self.panel.engine.set("auto_sl", auto_sl)
        self.panel.engine.set("be_offset", be_offset)
        self.panel.engine.set("lock_tp1_at", lock_at)
        await self.panel.confirm(interaction, f"✅ סטופ: אוטומטי {f'{auto_sl:g}$' if auto_sl else 'בלי'} · "
                                         f"BE +{be_offset:g}$ · TP1 אחרי {lock_at:g}")


class ConfirmSwitch(discord.ui.View):
    def __init__(self, panel, account):
        super().__init__(timeout=60)
        self.panel = panel
        self.account = account

    @discord.ui.button(label="כן, להחליף", style=discord.ButtonStyle.danger)
    async def yes(self, interaction, button):
        await interaction.response.edit_message(content=f"מחליף לחשבון {account_label(self.account)}…", view=None)
        await self.panel.switch(self.account)
        await interaction.edit_original_response(
            content=f"✅ {self.panel.engine.name} עבר לחשבון {account_label(self.account)}")

    @discord.ui.button(label="ביטול", style=discord.ButtonStyle.secondary)
    async def no(self, interaction, button):
        await interaction.response.edit_message(content="בוטל.", view=None)


class AccountSelect(discord.ui.Select):
    def __init__(self, panel, accounts, current, taken):
        # an account another ladder runs on is left out: two managers would ladder every trade twice
        options = [discord.SelectOption(label=label, value=acc, default=acc == current)
                   for acc, label in accounts if acc not in taken][:25]
        if not options:
            options = [discord.SelectOption(label="לא נמצאו חשבונות פנויים - לחץ רענן", value="none")]
        super().__init__(placeholder="בחר חשבון", options=options, custom_id=f"ladder:account:{panel.engine.lid}", row=2)
        self.panel = panel

    async def callback(self, interaction):
        account = self.values[0]
        if account == "none" or account == self.panel.engine.account:
            await interaction.response.defer()
            return
        if account in self.panel.bot.taken_accounts(self.panel.engine):
            await interaction.response.send_message("החשבון הזה כבר רץ במשכפל אחר.", ephemeral=True, delete_after=15)
            return
        warn = []
        groups = self.panel.engine.open_groups()
        if groups:
            warn.append(f"יש {len(groups)} קבוצות פתוחות בחשבון הנוכחי - אחרי ההחלפה ה-SL שלהן לא יסונכרן יותר.")
        if split_key(account)[0] == "live":
            p = self.panel.engine.params
            cap = self.panel.engine.limit(account)
            warn.append(f"⚠️ **{account_label(account)} הוא חשבון אמיתי - כסף אמיתי.** "
                        f"המצב עכשיו: {'🟢 דלוק' if p['enabled'] else '🔴 כבוי'}, "
                        f"{'שכפול' if p['mode'] == 'duplicate' else 'חלוקה'} × {p['copies']}, "
                        f"הגבלה: {f'{cap * 100:g} אונקיות' if cap else 'בלי'}.")
        if warn:
            await interaction.response.send_message("\n".join(warn) + "\nלהחליף?",
                                                    view=ConfirmSwitch(self.panel, account), ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.panel.switch(account)
        await interaction.followup.send(f"✅ {self.panel.engine.name} עבר לחשבון {account_label(account)}", ephemeral=True)


class SLModeSelect(discord.ui.Select):
    """Private menu under the 🔒 button: every SL mode with what it does; the current one is preselected."""

    def __init__(self, panel):
        current = sl_mode(panel.engine.params)
        options = [discord.SelectOption(label=label, description=desc, value=str(i), default=mode == current)
                   for i, (mode, label, desc) in enumerate(SL_MODES)]
        super().__init__(placeholder="בחר מה קורה לסטופ", options=options)
        self.panel = panel

    async def callback(self, interaction):
        (on, to, every, start), label, _ = SL_MODES[int(self.values[0])]
        engine = self.panel.engine
        engine.set("be_to", to)
        engine.set("step_every", every)
        engine.set("step_start", start)
        engine.set("be_after_tp1", on)
        text = f"✅ סטופ אחרי TP: **{label}** - {after_tp1_text(engine.params)}"
        engine.note(f"🔒 סטופ אחרי TP: {label} (השרת אישר)")
        await interaction.response.edit_message(content=text, view=None)
        await self.panel.refresh()


class TemplateModal(discord.ui.Modal):
    """Write a template: a name and the settings as text, prefilled from the current settings (or the template edited)."""

    def __init__(self, panel, old_name=None):
        super().__init__(title="תבנית" + (f" · {old_name}" if old_name else " חדשה"))
        self.panel = panel
        self.old_name = old_name
        templates = panel.settings.data["templates"]
        source = dict(panel.engine.params, **templates[old_name]) if old_name else panel.engine.params
        self.name = discord.ui.TextInput(label="שם התבנית", default=old_name or "", max_length=40,
                                         placeholder="למשל: סקאלפ מהיר")
        self.body = discord.ui.TextInput(label="הגדרות (שורה לכל הגדרה)", style=discord.TextStyle.paragraph,
                                         default=template_text(source), max_length=1500)
        self.add_item(self.name)
        self.add_item(self.body)

    async def on_submit(self, interaction):
        name = self.name.value.strip()
        templates = self.panel.settings.data["templates"]
        try:
            if not name:
                raise ValueError("חסר שם")
            t = parse_template(self.body.value, self.panel.engine.params)
        except ValueError as exc:
            await interaction.response.send_message(f"⚠️ התבנית לא נשמרה: {exc}", ephemeral=True)
            return
        if self.old_name and self.old_name != name and self.old_name in templates:
            del templates[self.old_name]
        templates[name] = t
        self.panel.settings.save()
        await self.panel.bot.render_all_panels()       # the "template" line of every panel may change
        await interaction.response.send_message(
            f"💾 תבנית **{name}** נשמרה: {template_summary(t)}\nכדי לטעון אותה: 📋 תבניות → בוחרים אותה.",
            ephemeral=True, delete_after=30)


class TemplateSelect(discord.ui.Select):
    """Pick a template; action: load / edit / delete."""

    def __init__(self, panel, action):
        templates = panel.settings.data["templates"]
        current = panel.engine.template_name()
        options = [discord.SelectOption(label=name[:100], description=template_summary(t), value=name,
                                        default=action == "load" and name == current)
                   for name, t in list(templates.items())[:25]]
        placeholder = {"load": "בחר תבנית לטעינה", "edit": "איזו תבנית לערוך?", "delete": "איזו תבנית למחוק?"}[action]
        super().__init__(placeholder=placeholder, options=options)
        self.panel = panel
        self.action = action

    async def callback(self, interaction):
        name = self.values[0]
        if name not in self.panel.settings.data["templates"]:
            await interaction.response.edit_message(content="התבנית כבר לא קיימת.", view=None)
            return
        if self.action == "edit":
            await interaction.response.send_modal(TemplateModal(self.panel, name))
            return
        if self.action == "delete":
            del self.panel.settings.data["templates"][name]
            self.panel.settings.save()
            await interaction.response.edit_message(content=f"🗑 תבנית **{name}** נמחקה.", view=None)
            await self.panel.bot.render_all_panels()
            return
        engine = self.panel.engine
        engine.apply_template(name)
        engine.note(f"📋 נטענה תבנית {name} (השרת אישר)")
        await interaction.response.edit_message(
            content=f"📋 נטענה תבנית **{name}**: {template_summary(engine.params)}\n"
                    f"סטופ אחרי TP: {after_tp1_text(engine.params)}\n"
                    "חל על העסקאות הבאות. עסקאות פתוחות ממשיכות עם ההגדרות שהן נפתחו איתן.", view=None)
        await self.panel.refresh()


class TemplateMenu(discord.ui.View):
    """Private menu under 📋: the templates (choosing one loads it) and new / edit / delete."""

    def __init__(self, panel):
        super().__init__(timeout=180)
        self.panel = panel
        if panel.settings.data["templates"]:
            self.add_item(TemplateSelect(panel, "load"))
        else:
            self.remove_item(self.edit)
            self.remove_item(self.delete)

    async def _pick(self, interaction, action):
        view = discord.ui.View(timeout=120)
        view.add_item(TemplateSelect(self.panel, action))
        await interaction.response.edit_message(content=None, view=view)

    @discord.ui.button(label="תבנית חדשה", emoji="➕", style=discord.ButtonStyle.success, row=1)
    async def new(self, interaction, button):
        await interaction.response.send_modal(TemplateModal(self.panel))

    @discord.ui.button(label="ערוך", emoji="✏️", style=discord.ButtonStyle.secondary, row=1)
    async def edit(self, interaction, button):
        await self._pick(interaction, "edit")

    @discord.ui.button(label="מחק", emoji="🗑", style=discord.ButtonStyle.danger, row=1)
    async def delete(self, interaction, button):
        await self._pick(interaction, "delete")


class Panel(discord.ui.View):
    def __init__(self, bot, engine):
        super().__init__(timeout=None)
        self.bot = bot
        self.engine = engine
        self.settings = engine.settings
        for item in self.children:      # one panel per ladder: its buttons need their own ids
            item.custom_id = f"{item.custom_id}:{engine.lid}"
        self.add_item(AccountSelect(self, bot.accounts, engine.account, bot.taken_accounts(engine)))

    async def interaction_check(self, interaction):
        if interaction.user.id != ALLOWED_USER_ID:
            await interaction.response.send_message("אין לך הרשאה ללוח הזה.", ephemeral=True)
            return False
        return True

    async def _toggle(self, interaction, key, value, text):
        self.engine.set(key, value)
        await self.confirm(interaction, text)

    async def confirm(self, interaction, text):
        """The change is already applied on the server: say so to the user only, and in the panel's last line."""
        self.engine.note(text + " (השרת אישר)")
        await interaction.response.send_message(text, ephemeral=True, delete_after=15)
        await self.refresh()

    @discord.ui.button(label="הדלק", emoji="🟢", style=discord.ButtonStyle.success, custom_id="ladder:on", row=0)
    async def on(self, interaction, button):
        if not self.engine.account:
            await interaction.response.send_message("קודם בחר חשבון בתפריט.", ephemeral=True, delete_after=15)
            return
        await self._toggle(interaction, "enabled", True, "🟢 הופעל - משכפל עסקאות חדשות")

    @discord.ui.button(label="כבה", emoji="🔴", style=discord.ButtonStyle.danger, custom_id="ladder:off", row=0)
    async def off(self, interaction, button):
        await self._toggle(interaction, "enabled", False, "🔴 כובה - לא משכפל עסקאות חדשות")

    @discord.ui.button(label="הגדרות", emoji="⚙️", style=discord.ButtonStyle.secondary, custom_id="ladder:settings", row=0)
    async def settings_button(self, interaction, button):
        await interaction.response.send_modal(SettingsModal(self))

    @discord.ui.button(label="סטופ", emoji="🛡", style=discord.ButtonStyle.secondary, custom_id="ladder:stop", row=0)
    async def stop_button(self, interaction, button):
        await interaction.response.send_modal(StopModal(self))

    @discord.ui.button(label="רענן", emoji="🔄", style=discord.ButtonStyle.secondary, custom_id="ladder:refresh", row=0)
    async def refresh_button(self, interaction, button):
        await interaction.response.defer()
        await self.bot.reload_accounts()
        await self.refresh()

    @discord.ui.button(label="חלוקה / שכפול", emoji="✂️", style=discord.ButtonStyle.primary, custom_id="ladder:mode", row=1)
    async def mode(self, interaction, button):
        new = "duplicate" if self.engine.params["mode"] == "split" else "split"
        await self._toggle(interaction, "mode", new, "✂️ מצב: " + ("שכפול" if new == "duplicate" else "חלוקה"))

    @discord.ui.button(label="סטופ אחרי TP", emoji="🔒", style=discord.ButtonStyle.primary, custom_id="ladder:be", row=1)
    async def be(self, interaction, button):
        view = discord.ui.View(timeout=120)
        view.add_item(SLModeSelect(self))
        await interaction.response.send_message(
            f"🔒 **מה יקרה לסטופ כשה-TP-ים נתפסים?**\nעכשיו: {after_tp1_text(self.engine.params)}",
            view=view, ephemeral=True)

    @discord.ui.button(label="Runner", emoji="🏃", style=discord.ButtonStyle.primary, custom_id="ladder:runner", row=1)
    async def runner(self, interaction, button):
        new = not self.engine.params["runner_last"]
        await self._toggle(interaction, "runner_last", new, "🏃 Runner: " + ("כן" if new else "לא"))

    @discord.ui.button(label="מחיקת SL", emoji="🚫", style=discord.ButtonStyle.primary, custom_id="ladder:sldel", row=1)
    async def sldel(self, interaction, button):
        new = not self.engine.params["sync_sl_delete"]
        await self._toggle(interaction, "sync_sl_delete", new,
                           "🚫 מחיקת SL: " + ("מוחקת מכולן" if new else "מחזירה את ה-SL"))

    @discord.ui.button(label="Runner בכניסה", emoji="🎯", style=discord.ButtonStyle.primary, custom_id="ladder:runbe", row=1)
    async def runner_be(self, interaction, button):
        new = not self.engine.params["runner_keep_be"]
        await self._toggle(interaction, "runner_keep_be", new,
                           "🎯 Runner: " + ("ה-SL שלו נשאר בכניסה כשהשאר עוברות ל-TP1" if new
                                           else "עובר ל-TP1 יחד עם השאר"))

    @discord.ui.button(label="תבניות", emoji="📋", style=discord.ButtonStyle.success, custom_id="ladder:tpl", row=3)
    async def templates(self, interaction, button):
        templates = self.settings.data["templates"]
        current = self.engine.template_name()
        text = ("📋 **תבניות** - בוחרים תבנית וכל ההגדרות נטענות." if templates
                else "📋 **אין עדיין תבניות.** ➕ פותח חלון עם ההגדרות הנוכחיות - משנים, נותנים שם ושומרים.")
        if templates:
            text += f"\nעכשיו: {current or 'הגדרות ידניות (לא תואם לאף תבנית)'}"
        await interaction.response.send_message(text, view=TemplateMenu(self), ephemeral=True)

    async def switch(self, account):
        self.engine.ladder["account"] = account
        self.settings.save()
        await asyncio.to_thread(self.engine.start, account)
        self.engine.note(f"🔁 עבר לחשבון {account_label(account)}")
        await self.bot.render_all()     # the other panels' account lists change too

    async def refresh(self):
        await self.bot.render_panel(self.engine)
        await self.bot.render_header()


# ======================================================================
# Header: list of ladders, add / delete
# ======================================================================
class ConfirmDelete(discord.ui.View):
    def __init__(self, bot, engine):
        super().__init__(timeout=60)
        self.bot = bot
        self.engine = engine

    @discord.ui.button(label="כן, למחוק", style=discord.ButtonStyle.danger)
    async def yes(self, interaction, button):
        await interaction.response.edit_message(content=f"מוחק את {self.engine.name}…", view=None)
        await self.bot.delete_ladder(self.engine)
        await interaction.edit_original_response(content=f"🗑 {self.engine.name} נמחק")

    @discord.ui.button(label="ביטול", style=discord.ButtonStyle.secondary)
    async def no(self, interaction, button):
        await interaction.response.edit_message(content="בוטל.", view=None)


class DeleteSelect(discord.ui.Select):
    def __init__(self, bot):
        options = [discord.SelectOption(label=e.status_line().replace("**", "")[:100], value=str(e.lid))
                   for e in bot.engines]
        super().__init__(placeholder="איזה משכפל למחוק?", options=options)
        self.bot = bot

    async def callback(self, interaction):
        engine = next(e for e in self.bot.engines if str(e.lid) == self.values[0])
        warn = [f"למחוק את **{engine.name}**" + (f" ({account_label(engine.account)})" if engine.account else "") + "?"]
        groups = engine.open_groups()
        if groups:
            warn.append(f"⚠️ יש לו {len(groups)} קבוצות פתוחות - אחרי המחיקה ה-SL שלהן לא יסונכרן יותר.")
        await interaction.response.edit_message(content="\n".join(warn), view=ConfirmDelete(self.bot, engine))


class Header(discord.ui.View):
    def __init__(self, bot):
        super().__init__(timeout=None)
        self.bot = bot

    async def interaction_check(self, interaction):
        if interaction.user.id != ALLOWED_USER_ID:
            await interaction.response.send_message("אין לך הרשאה ללוח הזה.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="משכפל חדש", emoji="➕", style=discord.ButtonStyle.success, custom_id="ladder:add")
    async def add(self, interaction, button):
        engine = await self.bot.add_ladder()
        await interaction.response.send_message(f"➕ נוסף {engine.name} (כבוי) - בחר לו חשבון בלוח שלו למטה.",
                                                ephemeral=True, delete_after=15)

    @discord.ui.button(label="מחק משכפל", emoji="🗑", style=discord.ButtonStyle.danger, custom_id="ladder:delete")
    async def delete(self, interaction, button):
        if not self.bot.engines:
            await interaction.response.send_message("אין משכפלים.", ephemeral=True, delete_after=15)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(DeleteSelect(self.bot))
        await interaction.response.send_message("בחר משכפל למחיקה:", view=view, ephemeral=True)


class LadderBot(discord.Client):
    def __init__(self, engines, settings):
        super().__init__(intents=discord.Intents.default())
        self.engines = engines
        self.settings = settings
        self.accounts = []
        self.panel_lock = asyncio.Lock()

    def taken_accounts(self, engine):
        return {e.account for e in self.engines if e is not engine and e.account}

    async def reload_accounts(self):
        try:
            self.accounts = await asyncio.to_thread(list_accounts)
        except Exception as exc:
            log.warning(f"account list failed: {exc}")

    async def setup_hook(self):
        await self.reload_accounts()
        self.add_view(Header(self))     # buttons work after restarts
        for engine in self.engines:
            self.add_view(Panel(self, engine))
        self.loop.create_task(self._refresher())

    async def on_ready(self):
        log.info(f"discord ready as {self.user}")
        await self.render_all()

    async def _channel(self):
        return self.get_channel(CONTROL_CHANNEL_ID) or await self.fetch_channel(CONTROL_CHANNEL_ID)

    async def render_all(self):
        await self.render_guide()
        await self.render_header()
        for engine in self.engines:
            await self.render_panel(engine)

    async def render_guide(self):
        async with self.panel_lock:
            channel = await self._channel()
            msg_id = self.settings.data["guide_message"]
            if msg_id:
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(content=None, embeds=guide_embeds())
                    return
                except discord.NotFound:
                    pass
            # the guide is the first message: the header and the panels are reposted under it
            ladders = self.settings.data["ladders"]
            for mid in [self.settings.data["header_message"]] + [lad["panel_message"] for lad in ladders]:
                if mid:
                    try:
                        await (await channel.fetch_message(mid)).delete()
                    except discord.HTTPException:
                        pass
            self.settings.data["header_message"] = 0
            for ladder in ladders:
                ladder["panel_message"] = 0
            msg = await channel.send(embeds=guide_embeds())
            self.settings.data["guide_message"] = msg.id
            self.settings.save()

    async def render_header(self):
        async with self.panel_lock:
            channel = await self._channel()
            msg_id = self.settings.data["header_message"]
            if msg_id:
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(embed=header_embed(self.engines), view=Header(self))
                    return
                except discord.NotFound:
                    pass
            # the header goes on top: repost the panels under it
            for ladder in self.settings.data["ladders"]:
                if ladder["panel_message"]:
                    try:
                        await (await channel.fetch_message(ladder["panel_message"])).delete()
                    except discord.HTTPException:
                        pass
                    ladder["panel_message"] = 0
            msg = await channel.send(embed=header_embed(self.engines), view=Header(self))
            self.settings.data["header_message"] = msg.id
            self.settings.save()
            try:
                await msg.pin()
            except discord.HTTPException as exc:
                log.warning(f"pin failed: {exc}")

    async def render_panel(self, engine):
        async with self.panel_lock:
            if engine not in self.engines:
                return
            channel = await self._channel()
            view = Panel(self, engine)
            embed = panel_embed(engine)
            msg_id = engine.ladder["panel_message"]
            if msg_id:
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(content=None, embed=embed, view=view)
                    return
                except discord.NotFound:
                    pass
            msg = await channel.send(embed=embed, view=view)
            engine.ladder["panel_message"] = msg.id
            self.settings.save()

    async def render_all_panels(self):
        for engine in self.engines:
            await self.render_panel(engine)

    async def add_ladder(self):
        engine = Engine(self.settings, self.settings.new_ladder())
        self.engines.append(engine)
        self.add_view(Panel(self, engine))
        await self.render_header()
        await self.render_panel(engine)
        return engine

    async def delete_ladder(self, engine):
        await asyncio.to_thread(engine.stop_engine)
        self.engines.remove(engine)
        self.settings.data["ladders"].remove(engine.ladder)
        self.settings.save()
        if engine.ladder["panel_message"]:
            try:
                await (await (await self._channel()).fetch_message(engine.ladder["panel_message"])).delete()
            except discord.HTTPException:
                pass
        await self.render_all()     # its account is free again in the other panels

    async def _refresher(self):
        await self.wait_until_ready()
        last = {}
        while not self.is_closed():
            await asyncio.sleep(10)
            # edit only when something visible changed (Discord rate-limits edits)
            for engine in list(self.engines):
                key = json.dumps([engine.open_groups(), engine.error, engine.last,
                                  engine.manager is not None and engine.manager.summary is not None], default=str)
                if key != last.get(engine.lid):
                    last[engine.lid] = key
                    try:
                        await self.render_panel(engine)
                    except Exception as exc:
                        log.warning(f"panel refresh failed: {exc}")
            key = json.dumps([e.status_line() for e in self.engines])
            if key != last.get("header"):
                last["header"] = key
                try:
                    await self.render_header()
                except Exception as exc:
                    log.warning(f"header refresh failed: {exc}")


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", force=True)
    logging.getLogger("tradelocker").setLevel(logging.CRITICAL)
    logging.getLogger("tradelocker").propagate = False
    logging.getLogger("discord").setLevel(logging.WARNING)
    os.makedirs(DATA_DIR, exist_ok=True)
    settings = Settings()
    engines = [Engine(settings, ladder) for ladder in settings.data["ladders"]]
    for engine in engines:
        engine.start(engine.account)
    LadderBot(engines, settings).run(os.environ["LADDER_DISCORD_TOKEN"], log_handler=None)


if __name__ == "__main__":
    main()
