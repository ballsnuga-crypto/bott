#!/usr/bin/env python3
"""
One-shot: upsert economy_data.json into Supabase public.economy_wallets.

Run on any machine where your real economy JSON lives (e.g. your PC or the Discord bot host):

  python seed_economy_to_supabase.py

Requires SUPABASE_URL + SUPABASE_KEY (service role) in .env — same vars as economy_cog.

Optional env:
  ECONOMY_SEED_PATH=C:\\path\\to\\economy_data.json   (defaults to ./economy_data.json next to this script)

Panel hosting (no working console):
  Option A — set startup command to:  bash ./start_discord_bot.sh
  Option B — in Mamba env set RUN_ECONOMY_SEED_ON_START=1  (runs seed inside index.py before bot.run; idempotent upsert)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

START_WALLET = 500


def _load_dotenv_optional() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parent
    for name in (".env",):
        p = root / name
        if p.exists():
            load_dotenv(p)
            break


def _normalize_cs2_inv(raw) -> list:
    if not isinstance(raw, list):
        return []
    out = []
    for x in raw:
        if isinstance(x, dict) and x.get("i") and x.get("d"):
            out.append({"i": str(x["i"]), "d": str(x["d"])})
    return out


def main() -> int:
    _load_dotenv_optional()

    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()
    if not url or not key:
        print("Set SUPABASE_URL and SUPABASE_KEY (service role) in .env beside this script or in the environment.")
        return 1

    seed = os.getenv("ECONOMY_SEED_PATH", "").strip()
    seed_path = Path(seed) if seed else Path(__file__).resolve().parent / "economy_data.json"
    seed_path = seed_path.expanduser()

    if not seed_path.is_file():
        print(f"economy JSON not found: {seed_path}")
        print(
            "Copy your Discord bot's economy_data.json here, or set ECONOMY_SEED_PATH "
            "to the full path of that file."
        )
        return 1

    try:
        raw = json.loads(seed_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"Failed to read economy JSON: {e}")
        return 1

    if not isinstance(raw, dict):
        print("economy_data.json must be a JSON object keyed by guild_id:user_id.")
        return 1

    try:
        from supabase import create_client
    except ImportError:
        print("Install supabase: pip install supabase")
        return 1

    payload: list[dict] = []
    skipped = 0
    for key, row in raw.items():
        if not isinstance(row, dict):
            skipped += 1
            continue
        try:
            gid_s, uid_s = str(key).split(":", 1)
            gid = int(gid_s.strip())
            uid = int(uid_s.strip())
        except (ValueError, TypeError):
            skipped += 1
            continue
        payload.append(
            {
                "guild_id": gid,
                "user_id": uid,
                "wallet": int(row.get("wallet", START_WALLET)),
                "bank": int(row.get("bank", 0)),
                "last_daily": float(row.get("last_daily", 0.0)),
                "last_work": float(row.get("last_work", 0.0)),
                "last_beg": float(row.get("last_beg", 0.0)),
                "last_crime": float(row.get("last_crime", 0.0)),
                "last_rob": float(row.get("last_rob", 0.0)),
                "last_open": float(row.get("last_open", 0.0)),
                "cs2_inv": _normalize_cs2_inv(row.get("cs2_inv")),
                "cs2_pity": max(0, int(row.get("cs2_pity", 0) or 0)),
            }
        )

    if not payload:
        print(f"No valid rows parsed (skipped {skipped}). Check key format guild_id:user_id.")
        return 1

    client = create_client(url, key)
    batch_size = 500
    written = 0
    for i in range(0, len(payload), batch_size):
        chunk = payload[i : i + batch_size]
        try:
            client.table("economy_wallets").upsert(chunk, on_conflict="guild_id,user_id").execute()
            written += len(chunk)
            print(f"Upserted {written}/{len(payload)}...")
        except Exception as e:
            print(f"Supabase upsert failed: {e!r}")
            return 1

    tail = f" Skipped {skipped} malformed key(s)." if skipped else ""
    print(f"Done. Upserted {len(payload)} wallet row(s).{tail}")
    print('Refresh https://YOUR_SITE/api/casino/debug-balance — supabaseTableRowCount should be > 0.')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
