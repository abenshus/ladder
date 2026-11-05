"""
Ladder on the server: runs the ladder manager for one TradeLocker account and a Discord control panel.

#ladder (CONTROL_CHANNEL_ID): one pinned panel message, edited in place -
  on/off, account picker, mode / BE / runner / SL-delete toggles, settings modal.
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
EDITABLE = ("enabled", "mode", "copies", "tp_step", "runner_last", "be_after_tp1", "be_offset", "sync_sl_delete")


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
        self.data = {"account": os.environ.get("TL_ACCOUNT_ID", ""), "panel_message": 0,
                     "params": {"enabled": False}, "limits": {}}     # limits: account -> max lots of a whole ladder
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE) as f:
                self.data.update(json.load(f))

    def save(self):
        tmp = SETTINGS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=1)
        os.replace(tmp, SETTINGS_FILE)

    def params(self):
        params = dict(lm.PARAMS)
        params["active"] = True
        for key, value in self.data["params"].items():
            if key in EDITABLE:
                params[key] = value
        return params


# ======================================================================
# Engine: one LadderManager thread for the chosen account, restarted if it crashes
# ======================================================================
class Engine:
    def __init__(self, settings):
        self.settings = settings
        self.params = settings.params()     # shared with the running manager - edits apply on its next tick
        self.manager = None
        self.stop = None
        self.thread = None
        self.error = ""
        self.last = ""      # the panel's "last update" line: startup, errors, what a button did

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
        state_file = os.path.join(DATA_DIR, f"state-{account}.json")
        while not stop.is_set():
            try:
                self.manager = lm.LadderManager(self.params, state_file, account_id=account, stop_event=stop)
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
        if account == self.settings.data["account"]:
            self.params["max_total_lots"] = lots

    def set(self, key, value):
        self.params[key] = value
        self.settings.data["params"][key] = value
        self.settings.save()

    def open_groups(self):
        if self.manager is None or self.manager.summary is None:
            return []
        return [g for g in self.manager.summary["groups"] if g["members"] > 0 or g["kind"] == "pending"]


def list_accounts():
    """All accounts of the TradeLocker login: [(id, label)]."""
    base = os.environ["TL_ENV"] + "/backend-api"
    r = requests.post(base + "/auth/jwt/token", json={"email": os.environ["TL_EMAIL"], "password": os.environ["TL_PASSWORD"],
                                                      "server": os.environ["TL_SERVER"]}, timeout=10)
    r.raise_for_status()
    token = r.json()["accessToken"]
    r = requests.get(base + "/auth/jwt/all-accounts", headers={"Authorization": "Bearer " + token}, timeout=10)
    r.raise_for_status()
    notes = account_notes()
    kind = "Demo" if "demo" in os.environ["TL_ENV"] else "Live"    # the API "name" is an internal brand#user id
    out = []
    for a in r.json()["accounts"]:
        acc = str(a["id"])
        label = f"{kind} {acc} · {float(a['accountBalance']):,.0f} {a['currency']}"
        if acc in notes:
            label += f" ⚠️ {notes[acc]}"
        out.append((acc, label[:100]))
    return out


# ======================================================================
# Discord panel
# ======================================================================
def panel_embed(engine, settings):
    """The panel as plain lines, one setting per line (inline fields read badly in Hebrew)."""
    p = engine.params
    on = p["enabled"]
    acc = settings.data["account"] or "—"
    note = account_notes().get(acc)
    cap = p["max_total_lots"]
    lines = [
        "🟢 **פועל** - משכפל עסקאות חדשות" if on else "🔴 **כבוי** - לא משכפל עסקאות חדשות (קבוצות פתוחות ממשיכות לקבל סנכרון SL)",
        "",
        f"**חשבון:** {acc}" + (f"  ⚠️ {note}" if note else ""),
        f"**מצב:** {'חלוקה (split) - הלוט שלך מתחלק' if p['mode'] == 'split' else 'שכפול (duplicate) - כל עסקה בלוט שלך'}",
        f"**עסקאות:** {p['copies']}",
        f"**מרחק בין TP-ים:** {p['tp_step']:g}$",
        f"**Runner (אחרונה בלי TP):** {'כן' if p['runner_last'] else 'לא'}",
        f"**BE אחרי TP1:** " + (f"כן, כניסה +{p['be_offset']:g}$" if p["be_after_tp1"] else "לא"),
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
    embed = discord.Embed(title="🪜 Ladder", description="\n".join(lines), color=0x2ecc71 if on else 0xe74c3c)
    status = "מחובר" if engine.manager and engine.manager.summary else "מתחבר…"
    if engine.error:
        status = f"⚠️ {engine.error}"
    embed.set_footer(text=f"{status} · עודכן {time.strftime('%H:%M:%S')}")
    return embed


class SettingsModal(discord.ui.Modal, title="הגדרות Ladder"):
    def __init__(self, panel):
        super().__init__()
        self.panel = panel
        p = panel.engine.params
        self.copies = discord.ui.TextInput(label="כמה עסקאות בסך הכול (2-10)", default=str(p["copies"]), max_length=2)
        self.tp_step = discord.ui.TextInput(label="מרחק בין TP-ים בדולרים", default=str(p["tp_step"]), max_length=6)
        self.be_offset = discord.ui.TextInput(label="BE: כמה דולר מעבר לכניסה", default=str(p["be_offset"]), max_length=6)
        # in ounces: "20" is 20 oz (0.2 lot) - lots invited typing 20 for 20 oz
        self.max_oz = discord.ui.TextInput(label="מקסימום אונקיות לכל הסולם (0 = בלי הגבלה)",
                                           default=f"{p['max_total_lots'] * 100:g}", max_length=6)
        for item in (self.copies, self.tp_step, self.be_offset, self.max_oz):
            self.add_item(item)

    async def on_submit(self, interaction):
        try:
            copies = int(self.copies.value)
            tp_step = float(self.tp_step.value)
            be_offset = float(self.be_offset.value)
            max_oz = float(self.max_oz.value)
            max_lots = round(max_oz / 100, 2)
            if not 2 <= copies <= 10 or not 0.1 <= tp_step <= 100 or not 0 <= be_offset < tp_step or not 0 <= max_oz <= 500:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                "ערכים לא תקינים: עסקאות 2-10, מרחק TP 0.1-100, BE offset בין 0 למרחק ה-TP, מקסימום אונקיות 0-500.",
                ephemeral=True)
            return
        account = self.panel.settings.data["account"]
        if account:
            self.panel.engine.set_limit(account, max_lots)
        self.panel.engine.set("copies", copies)
        self.panel.engine.set("tp_step", tp_step)
        self.panel.engine.set("be_offset", be_offset)
        cap = f"{max_oz:g} אונקיות" if max_lots else "בלי"
        await self.panel.confirm(interaction, f"✅ ההגדרות נשמרו: {copies} עסקאות · TP כל {tp_step:g}$ · BE +{be_offset:g}$ · הגבלה {cap}")


class ConfirmSwitch(discord.ui.View):
    def __init__(self, panel, account):
        super().__init__(timeout=60)
        self.panel = panel
        self.account = account

    @discord.ui.button(label="כן, להחליף", style=discord.ButtonStyle.danger)
    async def yes(self, interaction, button):
        await interaction.response.edit_message(content=f"מחליף לחשבון {self.account}…", view=None)
        await self.panel.switch(self.account)
        await interaction.edit_original_response(content=f"✅ השרת עבר לחשבון {self.account}")

    @discord.ui.button(label="ביטול", style=discord.ButtonStyle.secondary)
    async def no(self, interaction, button):
        await interaction.response.edit_message(content="בוטל.", view=None)


class AccountSelect(discord.ui.Select):
    def __init__(self, panel, accounts, current):
        options = [discord.SelectOption(label=label, value=acc, default=acc == current) for acc, label in accounts[:25]]
        if not options:
            options = [discord.SelectOption(label="לא נמצאו חשבונות - לחץ רענן", value="none")]
        super().__init__(placeholder="בחר חשבון", options=options, custom_id="ladder:account", row=2)
        self.panel = panel

    async def callback(self, interaction):
        account = self.values[0]
        if account == "none" or account == self.panel.settings.data["account"]:
            await interaction.response.defer()
            return
        groups = self.panel.engine.open_groups()
        if groups:
            await interaction.response.send_message(
                f"יש {len(groups)} קבוצות פתוחות בחשבון הנוכחי. אחרי ההחלפה ה-SL שלהן לא יסונכרן יותר. להחליף בכל זאת?",
                view=ConfirmSwitch(self.panel, account), ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.panel.switch(account)
        await interaction.followup.send(f"✅ השרת עבר לחשבון {account}", ephemeral=True)


class Panel(discord.ui.View):
    def __init__(self, bot, engine, settings, accounts):
        super().__init__(timeout=None)
        self.bot = bot
        self.engine = engine
        self.settings = settings
        self.accounts = accounts
        self.add_item(AccountSelect(self, accounts, settings.data["account"]))

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
        await self._toggle(interaction, "enabled", True, "🟢 הופעל - משכפל עסקאות חדשות")

    @discord.ui.button(label="כבה", emoji="🔴", style=discord.ButtonStyle.danger, custom_id="ladder:off", row=0)
    async def off(self, interaction, button):
        await self._toggle(interaction, "enabled", False, "🔴 כובה - לא משכפל עסקאות חדשות")

    @discord.ui.button(label="הגדרות", emoji="⚙️", style=discord.ButtonStyle.secondary, custom_id="ladder:settings", row=0)
    async def settings_button(self, interaction, button):
        await interaction.response.send_modal(SettingsModal(self))

    @discord.ui.button(label="רענן", emoji="🔄", style=discord.ButtonStyle.secondary, custom_id="ladder:refresh", row=0)
    async def refresh_button(self, interaction, button):
        await interaction.response.defer()
        await self.bot.reload_accounts()
        await self.refresh()

    @discord.ui.button(label="חלוקה / שכפול", emoji="✂️", style=discord.ButtonStyle.primary, custom_id="ladder:mode", row=1)
    async def mode(self, interaction, button):
        new = "duplicate" if self.engine.params["mode"] == "split" else "split"
        await self._toggle(interaction, "mode", new, "✂️ מצב: " + ("שכפול" if new == "duplicate" else "חלוקה"))

    @discord.ui.button(label="BE אחרי TP1", emoji="🔒", style=discord.ButtonStyle.primary, custom_id="ladder:be", row=1)
    async def be(self, interaction, button):
        new = not self.engine.params["be_after_tp1"]
        await self._toggle(interaction, "be_after_tp1", new, "🔒 BE אחרי TP1: " + ("פועל" if new else "כבוי"))

    @discord.ui.button(label="Runner", emoji="🏃", style=discord.ButtonStyle.primary, custom_id="ladder:runner", row=1)
    async def runner(self, interaction, button):
        new = not self.engine.params["runner_last"]
        await self._toggle(interaction, "runner_last", new, "🏃 Runner: " + ("כן" if new else "לא"))

    @discord.ui.button(label="מחיקת SL", emoji="🚫", style=discord.ButtonStyle.primary, custom_id="ladder:sldel", row=1)
    async def sldel(self, interaction, button):
        new = not self.engine.params["sync_sl_delete"]
        await self._toggle(interaction, "sync_sl_delete", new,
                           "🚫 מחיקת SL: " + ("מוחקת מכולן" if new else "מחזירה את ה-SL"))

    async def switch(self, account):
        self.settings.data["account"] = account
        self.settings.save()
        await asyncio.to_thread(self.engine.start, account)
        self.engine.note(f"🔁 עבר לחשבון {account}")
        await self.refresh()

    async def refresh(self):
        await self.bot.render_panel()


class LadderBot(discord.Client):
    def __init__(self, engine, settings):
        super().__init__(intents=discord.Intents.default())
        self.engine = engine
        self.settings = settings
        self.accounts = []
        self.panel_lock = asyncio.Lock()

    async def reload_accounts(self):
        try:
            self.accounts = await asyncio.to_thread(list_accounts)
        except Exception as exc:
            log.warning(f"account list failed: {exc}")

    async def setup_hook(self):
        await self.reload_accounts()
        self.add_view(Panel(self, self.engine, self.settings, self.accounts))     # buttons work after restarts
        self.loop.create_task(self._refresher())

    async def on_ready(self):
        log.info(f"discord ready as {self.user}")
        await self.render_panel()

    async def render_panel(self):
        async with self.panel_lock:
            channel = self.get_channel(CONTROL_CHANNEL_ID) or await self.fetch_channel(CONTROL_CHANNEL_ID)
            view = Panel(self, self.engine, self.settings, self.accounts)
            embed = panel_embed(self.engine, self.settings)
            msg_id = self.settings.data["panel_message"]
            if msg_id:
                try:
                    msg = await channel.fetch_message(msg_id)
                    await msg.edit(content=None, embed=embed, view=view)
                    return
                except discord.NotFound:
                    pass
            msg = await channel.send(embed=embed, view=view)
            self.settings.data["panel_message"] = msg.id
            self.settings.save()
            try:
                await msg.pin()
            except discord.HTTPException as exc:
                log.warning(f"pin failed: {exc}")

    async def _refresher(self):
        await self.wait_until_ready()
        last = None
        while not self.is_closed():
            await asyncio.sleep(10)
            # edit only when something visible changed (Discord rate-limits edits)
            key = json.dumps([self.engine.open_groups(), self.engine.error, self.engine.last, self.engine.manager is not None
                              and self.engine.manager.summary is not None], default=str)
            if key != last:
                last = key
                try:
                    await self.render_panel()
                except Exception as exc:
                    log.warning(f"panel refresh failed: {exc}")


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", force=True)
    logging.getLogger("tradelocker").setLevel(logging.CRITICAL)
    logging.getLogger("tradelocker").propagate = False
    logging.getLogger("discord").setLevel(logging.WARNING)
    os.makedirs(DATA_DIR, exist_ok=True)
    settings = Settings()
    engine = Engine(settings)
    engine.start(settings.data["account"])
    LadderBot(engine, settings).run(os.environ["LADDER_DISCORD_TOKEN"], log_handler=None)


if __name__ == "__main__":
    main()
