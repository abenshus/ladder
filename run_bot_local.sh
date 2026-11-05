#!/bin/zsh
# Runs the Discord panel + ladder manager on this Mac (same as the server). Never run it while the server copy is up:
# two managers on one account ladder every trade twice. Secrets come from the Keychain.
cd "$(dirname "$0")"
[ -d .venv ] || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
export TL_ENV="https://rba-demo.tradelocker.com" TL_SERVER="8CAP"
export TL_EMAIL="$(security find-generic-password -a "$USER" -s tradelocker-demo-email -w)"
export TL_PASSWORD="$(security find-generic-password -a "$USER" -s tradelocker-demo-password -w)"
export TL_DEV_KEY="$(security find-generic-password -s tradelocker-dev-api-key -w)"
export DISCORD_WEBHOOK="$(security find-generic-password -s discord-webhook-ladder-log -w)"
export DISCORD_WEBHOOK_ALERT="$(security find-generic-password -s discord-webhook-ladder -w)"
export LADDER_DISCORD_TOKEN="$(security find-generic-password -s ladder-discord-token -w)"
export CONTROL_CHANNEL_ID="1567907244413165678" ALLOWED_USER_ID="828546772547797012"
export ACCOUNT_NOTES="590661=מועתק 1:1 ל-Colmex live"
export DATA_DIR="${DATA_DIR:-$PWD/data}"
exec .venv/bin/python -u bot.py "$@"
