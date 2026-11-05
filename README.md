# Ladder

Turns one manual TradeLocker trade into a ladder of trades with stepped take-profits, and keeps their stop-loss in sync.

- **split**: your lot is divided — open 0.1, get 5 × 0.02 (your trade is partially closed, 4 copies open).
- **duplicate** (default): every trade gets your lot — open 0.02, get 5 × 0.02. `max_total_lots` (per account) shrinks the copies
  so the whole ladder stays under the cap.
- TPs every `tp_step` dollars, the last trade without a TP (runner) unless turned off.
- Move the SL of any trade → all get it within ~2s. Delete it → removed from all (or restored, `sync_sl_delete=False`).
- Optional SL move after TP1 (`be_after_tp1`). `be_to="two_step"` (default): TP1 hit → SL to entry + `be_offset`; price gets `lock_tp1_at` of the way to TP2 (0.5 = halfway, 1 = TP2) or TP2 hits → SL to TP1. The price comes from the positions' PnL in the stream (~2s). `be_to="tp1"`: SL to TP1 at once. `be_to="entry"`: entry + `be_offset` only.
- Pending limit/stop orders are laddered too; moving or cancelling yours moves or cancels the copies.
- Trades of other bots (any strategyId) are left alone. Copies are tagged `LAD-<parent position>`.

Live data comes from the TradeLocker **Streams API** (developer key), orders go through REST.

## Files

| File | What |
|---|---|
| `ladder_manager.py` | the engine (stream mirror, ladder, SL sync, BE). Also a CLI: `./run_local.sh --active --mode duplicate --tp-step 5 ...` |
| `bot.py` | what the server runs: the engine for one chosen account + a Discord control panel |
| `run_bot_local.sh` | the server setup on this Mac, secrets from the Keychain |

## Discord

- `#ladder`: one panel message (on/off, account picker, split/duplicate, BE, runner, SL-delete, settings incl. the
  per-account cap in ounces). Only `ALLOWED_USER_ID` can press. No other messages: startup, errors and what a button did
  show as the panel's "last update" line, and each press gets a private confirmation that disappears.
- `#ladder-log` (muted): every ladder / SL sync / BE / close message, through a webhook.
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
