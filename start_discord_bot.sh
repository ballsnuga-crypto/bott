#!/bin/sh
# Mamba / Pterodactyl: set "Startup command" to this file, e.g.:
#   bash ./start_discord_bot.sh
# or (after chmod +x):
#   ./start_discord_bot.sh
#
# Runs economy → Supabase seed first (so logs show in the panel), then starts the bot.

set -eu
cd "$(dirname "$0")" || exit 1
export PYTHONUNBUFFERED=1

if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "No python3 or python in PATH"
  exit 1
fi

"$PY" -u ./seed_economy_to_supabase.py
# Avoid double-seed if RUN_ECONOMY_SEED_ON_START is set in panel env
unset RUN_ECONOMY_SEED_ON_START 2>/dev/null || true
exec "$PY" -u ./index.py
