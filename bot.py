"""
Ladder on the server: runs one or more ladders ("משכפלים"), each a ladder manager on its own TradeLocker account
with its own on/off and settings, and a Discord control panel per ladder.

#ladder (CONTROL_CHANNEL_ID): a pinned header on top (list of ladders, add / delete) and under it one panel
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
            "sync_sl_delete", "auto_sl", "runner_keep_be")


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
        self.data = {"header_message": 0, "ladders": [], "limits": {}}
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
        f"**מצב:** {'חלוקה (split) - הלוט שלך מתחלק' if p['mode'] == 'split' else 'שכפול (duplicate) - כל עסקה בלוט שלך'}",
        f"**עסקאות:** {p['copies']}",
        f"**מרחק בין TP-ים:** {p['tp_step']:g}$",
        f"**Runner (אחרונה בלי TP):** {'כן' if p['runner_last'] else 'לא'}",
        "**סטופ אוטומטי:** " + (f"{p['auto_sl']:g}$ מהכניסה לעסקה שנפתחה בלי SL" if p["auto_sl"] else "בלי"),
        "**אחרי TP1:** " + after_tp1_text(p),
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

    @discord.ui.button(label="SL אחרי TP1", emoji="🔒", style=discord.ButtonStyle.primary, custom_id="ladder:be", row=1)
    async def be(self, interaction, button):
        # cycles: off -> two steps (entry, then TP1) -> TP1 at once -> entry only -> off
        p = self.engine.params
        if not p["be_after_tp1"]:
            self.engine.set("be_to", "two_step")
            self.engine.set("be_after_tp1", True)
        elif p["be_to"] == "two_step":
            self.engine.set("be_to", "tp1")
        elif p["be_to"] == "tp1":
            self.engine.set("be_to", "entry")
        else:
            self.engine.set("be_after_tp1", False)
        await self.confirm(interaction, "🔒 אחרי TP1: " + after_tp1_text(p))

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
        await self.render_header()
        for engine in self.engines:
            await self.render_panel(engine)

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
