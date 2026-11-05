#!/bin/zsh
# Runs the ladder manager locally against the demo; secrets come from the Keychain.
cd "$(dirname "$0")"
[ -d .venv ] || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
export TL_ENV="https://rba-demo.tradelocker.com"
export TL_SERVER="8CAP"
# 590661 is copied 1:1 to Colmex live by MetaCopier - test on 595458 (not copied)
export TL_ACCOUNT_ID="${TL_ACCOUNT_ID:-595458}"
export TL_EMAIL="$(security find-generic-password -a "$USER" -s tradelocker-demo-email -w)"
export TL_PASSWORD="$(security find-generic-password -a "$USER" -s tradelocker-demo-password -w)"
export TL_DEV_KEY="$(security find-generic-password -s tradelocker-dev-api-key -w 2>/dev/null)"
export DISCORD_WEBHOOK="$(security find-generic-password -s discord-webhook-ladder-log -w 2>/dev/null)"
export DISCORD_WEBHOOK_ALERT="$(security find-generic-password -s discord-webhook-ladder -w 2>/dev/null)"
exec .venv/bin/python -u ladder_manager.py "$@"   # CLI only; the server runs bot.py
