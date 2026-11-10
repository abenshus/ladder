# Ladder

Turns one manual TradeLocker trade into a ladder of trades with stepped take-profits, and keeps their stop-loss in sync.

- **split**: your lot is divided — open 0.1, get 5 × 0.02 (your trade is partially closed, 4 copies open).
- **duplicate** (default): every trade gets your lot — open 0.02, get 5 × 0.02. `max_total_lots` (per account) shrinks the copies
  so the whole ladder stays under the cap.
- TPs every `tp_step` dollars, the last trade without a TP (runner) unless turned off.
- Move the SL of any trade → all get it within ~2s. Delete it → removed from all (or restored, `sync_sl_delete=False`).
- `auto_sl` (default 10): a trade or pending order opened without an SL gets one this many dollars from entry (deleting it follows `sync_sl_delete`; a moved pending order keeps the distance).
- Optional SL move after TP1 (`be_after_tp1`). `be_to="two_step"` (default): TP1 hit → SL to entry + `be_offset`; price gets `lock_tp1_at` of the way to TP2 (0.5 = halfway, 1 = TP2) or TP2 hits → SL to TP1. The price comes from the positions' PnL in the stream (~2s). `be_to="tp1"`: SL to TP1 at once. `be_to="entry"`: entry + `be_offset` only. `runner_keep_be=True` (default): when the rest move to TP1, the runner (last trade, no TP) keeps its SL at entry + `be_offset`, so one trade rides the whole move at zero risk; a manual SL change still goes to all trades. `be_to="steps"`: a step ladder for all trades, runner included - every `step_every` levels reached the SL goes up one step (`step_every=1`: TP1 → entry + `be_offset`, TP2 → TP1, TP3 → TP2…; `step_every=1, step_start=2`: nothing at TP1, TP2 → entry + `be_offset`, TP3 → TP2, TP4 → TP3…; `2`: TP2 → entry + `be_offset`, TP4 → TP2…); past the last TP the runner keeps climbing by the price. The 🔒 button opens a private menu with all the modes (off / two steps / steps every TP / entry at TP2 then every TP / steps every 2 TPs / TP1 / entry), the current one preselected.
- A ladder keeps the SL settings it opened with (`GROUP_KEYS`: SL mode, `be_offset`, `lock_tp1_at`, `runner_keep_be`; `tp_step` as before): changing them or loading a template applies to the next trades. A pending ladder remembers its TP1 (moves with the order), so a fill with slippage still steps from the real TP1.
- 📰 News block (last message in #ladder, `settings.json` → `news`): pick an account that a ladder runs on, ⚙️ news time (Israel) / minutes to cancel, levels `distance:TP step` (default `10:1.5, 15:3`), trades per level / SL / slippage, max risk % of the balance, side (both / buy / sell), SL mode 1-7, runner. 🔍 preview and 🚀 launch read the balance, free margin and leverage from TradeLocker: trades per level and lot are sized so that the worst case (on every side one whole level fills and stops out, SL + slippage) stays within the risk, and one level fits 80% of the free margin. Launch places stop orders at price ± distance, each level a pending ladder group (own TPs, SL, the news SL mode in the group's `cfg`) run by that account's ladder manager; whatever has not filled by news time + minutes is cancelled (`expire_at`). 🗑 cancels all unfilled news orders. Orders go in parallel with retries on 429 (about 10 s per 12 orders) - launch a few minutes before the release.
- Templates (📋 in the panel, shared by all ladders, `settings.json` → `templates`): ➕ opens a window with the current settings as text, one `label: value` per line (trades, TP step, runner, runner at entry, auto SL, BE offset, TP1 lock, SL mode number from the list in the window); give it a name and save. Picking a template from the list loads all of it; ✏️ edit / 🗑 delete. The panel shows which template the settings match.
- Pending limit/stop orders are laddered too; moving or cancelling yours moves or cancels the copies.
- Trades of other bots (any strategyId) are left alone. Copies are tagged `LAD-<parent position>`.

Live data comes from the TradeLocker **Streams API** (developer key), orders go through REST.

## Files

| File | What |
|---|---|
| `ladder_manager.py` | the engine (stream mirror, ladder, SL sync, BE). Also a CLI: `./run_local.sh --active --mode duplicate --tp-step 5 ...` |
| `bot.py` | what the server runs: one engine per ladder (each on its own account) + Discord control panels |
| `run_bot_local.sh` | the server setup on this Mac, secrets from the Keychain |

## Discord

- `#ladder`: a pinned header on top listing the ladders ("משכפלים") with ➕ add / 🗑 delete, and under it one panel
  per ladder (on/off, account picker, split/duplicate, BE, runner, SL-delete, settings incl. the per-account cap in
  ounces). Each ladder has its own account, on/off and settings; an account used by one ladder is not offered to another. Only `ALLOWED_USER_ID` can press. No other messages: startup, errors and what a button did
  show as the panel's "last update" line, and each press gets a private confirmation that disappears.
- `#ladder-log` (muted): every ladder / SL sync / BE / close message, through a webhook, tagged with the ladder and account.
- "Off" stops laddering new trades only; open groups keep their SL synced.

## Run it once

Two managers on one account ladder every trade twice. A lock file stops a second copy on the same machine, but not
the server and this Mac together: stop the local copy before the server one starts (and the other way round).

## Server (Dokploy, project `trading`, app `ladder`)

Dockerfile build, auto-deploy on push to `main`, volume `ladder-data` → `/data` (settings + per-account state).
Env: `TL_ENV TL_LIVE_ENV TL_SERVER TL_EMAIL TL_PASSWORD TL_DEV_KEY LADDER_DISCORD_TOKEN CONTROL_CHANNEL_ID ALLOWED_USER_ID
DISCORD_WEBHOOK DISCORD_WEBHOOK_ALERT ACCOUNT_NOTES`. Secrets are in the Keychain / Bitwarden (`tradelocker-*`,
`ladder-discord-token`, `discord-webhook-ladder*`).

Accounts: demo ones by id ("590661"), live ones as "live:<id>" (same login on rba-live; switching to live always asks first).
Demo 590661 is copied 1:1 to Colmex live by MetaCopier — everything laddered there is real money on Colmex.
