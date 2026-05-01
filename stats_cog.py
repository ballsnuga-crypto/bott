"""
6stats — per-guild message counts, session activity, streaks, cached ranks.
Uses SQLite + 5-minute rank cache; Discord native timestamps in embeds.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, available_timezones

import discord
from discord.ext import commands
import sys as _sys


def _is_trusted(user: Any) -> bool:
    """Mirror index.is_trusted without importing the module (avoids __main__ cycle)."""
    try:
        main = _sys.modules.get("__main__") or _sys.modules.get("index")
        trusted_ids = set(getattr(main, "TRUSTED_USERS", ()) or ())
    except Exception:
        trusted_ids = set()
    if getattr(user, "id", None) in trusted_ids:
        return True
    perms = getattr(user, "guild_permissions", None)
    return bool(perms is not None and getattr(perms, "administrator", False))

_SCRIPT_ROOT = Path(__file__).resolve().parent
STATS_DB_PATH = _SCRIPT_ROOT / "message_stats.db"

SESSION_GAP_SEC = 3600  # 1 hour — reset session
RANK_CACHE_TTL_SEC = 300  # 5 minutes
ACTIVE_SESSION_MSG_THRESHOLD = 3  # active if session_msg_count >= this (within the 1h session)


# --- Country name / alias → primary IANA timezone ---
# Keys are lowercased with whitespace/punct stripped (handled by _normalize_country_key).
# Values are the canonical IANA zone used for the country's most populous region.
COUNTRY_TIMEZONE_MAP: dict[str, str] = {
    "unitedstates": "America/New_York",
    "usa": "America/New_York",
    "us": "America/New_York",
    "america": "America/New_York",
    "easterntime": "America/New_York",
    "centraltime": "America/Chicago",
    "mountaintime": "America/Denver",
    "pacifictime": "America/Los_Angeles",
    "california": "America/Los_Angeles",
    "newyork": "America/New_York",
    "hawaii": "Pacific/Honolulu",
    "alaska": "America/Anchorage",
    "canada": "America/Toronto",
    "toronto": "America/Toronto",
    "vancouver": "America/Vancouver",
    "mexico": "America/Mexico_City",
    "brazil": "America/Sao_Paulo",
    "argentina": "America/Argentina/Buenos_Aires",
    "chile": "America/Santiago",
    "colombia": "America/Bogota",
    "peru": "America/Lima",
    "unitedkingdom": "Europe/London",
    "uk": "Europe/London",
    "england": "Europe/London",
    "britain": "Europe/London",
    "greatbritain": "Europe/London",
    "scotland": "Europe/London",
    "wales": "Europe/London",
    "ireland": "Europe/Dublin",
    "france": "Europe/Paris",
    "germany": "Europe/Berlin",
    "spain": "Europe/Madrid",
    "portugal": "Europe/Lisbon",
    "italy": "Europe/Rome",
    "netherlands": "Europe/Amsterdam",
    "holland": "Europe/Amsterdam",
    "belgium": "Europe/Brussels",
    "switzerland": "Europe/Zurich",
    "austria": "Europe/Vienna",
    "sweden": "Europe/Stockholm",
    "norway": "Europe/Oslo",
    "denmark": "Europe/Copenhagen",
    "finland": "Europe/Helsinki",
    "poland": "Europe/Warsaw",
    "czechia": "Europe/Prague",
    "czechrepublic": "Europe/Prague",
    "greece": "Europe/Athens",
    "turkey": "Europe/Istanbul",
    "russia": "Europe/Moscow",
    "moscow": "Europe/Moscow",
    "ukraine": "Europe/Kyiv",
    "romania": "Europe/Bucharest",
    "hungary": "Europe/Budapest",
    "bulgaria": "Europe/Sofia",
    "israel": "Asia/Jerusalem",
    "saudiarabia": "Asia/Riyadh",
    "uae": "Asia/Dubai",
    "dubai": "Asia/Dubai",
    "india": "Asia/Kolkata",
    "pakistan": "Asia/Karachi",
    "bangladesh": "Asia/Dhaka",
    "srilanka": "Asia/Colombo",
    "nepal": "Asia/Kathmandu",
    "china": "Asia/Shanghai",
    "shanghai": "Asia/Shanghai",
    "hongkong": "Asia/Hong_Kong",
    "taiwan": "Asia/Taipei",
    "singapore": "Asia/Singapore",
    "malaysia": "Asia/Kuala_Lumpur",
    "thailand": "Asia/Bangkok",
    "vietnam": "Asia/Ho_Chi_Minh",
    "indonesia": "Asia/Jakarta",
    "philippines": "Asia/Manila",
    "japan": "Asia/Tokyo",
    "tokyo": "Asia/Tokyo",
    "korea": "Asia/Seoul",
    "southkorea": "Asia/Seoul",
    "northkorea": "Asia/Pyongyang",
    "australia": "Australia/Sydney",
    "sydney": "Australia/Sydney",
    "melbourne": "Australia/Melbourne",
    "perth": "Australia/Perth",
    "newzealand": "Pacific/Auckland",
    "nz": "Pacific/Auckland",
    "southafrica": "Africa/Johannesburg",
    "egypt": "Africa/Cairo",
    "nigeria": "Africa/Lagos",
    "kenya": "Africa/Nairobi",
    "morocco": "Africa/Casablanca",
    "mongolia": "Asia/Ulaanbaatar",
    "ulaanbaatar": "Asia/Ulaanbaatar",
}


def _normalize_country_key(s: str) -> str:
    return "".join(c for c in s.lower() if c.isalnum())


def resolve_timezone_input(raw: str) -> Optional[str]:
    """Accept IANA zones (America/New_York) or country/region names (UnitedStates, Japan)."""
    if not raw:
        return None
    s = raw.strip()
    if s in available_timezones():
        return s
    # Try case-insensitive IANA match (Discord pastes sometimes have odd casing)
    lower = s.lower()
    for z in available_timezones():
        if z.lower() == lower:
            return z
    key = _normalize_country_key(s)
    z = COUNTRY_TIMEZONE_MAP.get(key)
    if z and z in available_timezones():
        return z
    return None


def _utc_today_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _utc_yesterday_str() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _public_site_base() -> str:
    return (os.getenv("PUBLIC_SITE_URL", "https://6xs.lol") or "https://6xs.lol").strip().rstrip("/")


def _archive_stats_fallback_blocking(
    client: Any, gid: str, uid: str, today_start: str, tomorrow_start: str
) -> Optional[dict[str, Any]]:
    """Table queries only — no ranks. Used if RPC archive_user_stats_and_ranks is not installed."""
    try:
        cresp = (
            client.table("archive_messages")
            .select("message_id", count="exact")
            .eq("guild_id", gid)
            .eq("author_id", uid)
            .execute()
        )
        total = int(getattr(cresp, "count", None) or 0)
        first_iso: Optional[str] = None
        last_iso: Optional[str] = None
        if total > 0:
            asc = (
                client.table("archive_messages")
                .select("created_at_discord")
                .eq("guild_id", gid)
                .eq("author_id", uid)
                .order("created_at_discord", desc=False)
                .limit(1)
                .execute()
            )
            desc = (
                client.table("archive_messages")
                .select("created_at_discord")
                .eq("guild_id", gid)
                .eq("author_id", uid)
                .order("created_at_discord", desc=True)
                .limit(1)
                .execute()
            )
            ad = getattr(asc, "data", None) or []
            dd = getattr(desc, "data", None) or []
            if ad:
                first_iso = str(ad[0].get("created_at_discord") or "").strip() or None
            if dd:
                last_iso = str(dd[0].get("created_at_discord") or "").strip() or None
        tresp = (
            client.table("archive_messages")
            .select("message_id", count="exact")
            .eq("guild_id", gid)
            .eq("author_id", uid)
            .gte("created_at_discord", today_start)
            .lt("created_at_discord", tomorrow_start)
            .execute()
        )
        today_archived = int(getattr(tresp, "count", None) or 0)
        return {
            "total": total,
            "today": today_archived,
            "first_at": first_iso,
            "last_at": last_iso,
            "lifetime_rank": None,
            "today_rank": None,
        }
    except Exception as e:
        print(f"[6stats] archive fallback (Supabase) failed: {e!r}")
        return None


def _fetch_archive_stats_blocking(guild_id: int, user_id: int) -> Optional[dict[str, Any]]:
    """Archive message counts + lifetime/today RANK() via RPC (same source as 6xs.lol)."""
    main = _sys.modules.get("__main__") or _sys.modules.get("index")
    get_client = getattr(main, "_get_supabase_client", None)
    if not callable(get_client):
        return None
    client = get_client()
    if client is None:
        return None
    gid = str(guild_id)
    uid = str(user_id)
    tday = datetime.now(timezone.utc).date()
    today_start = f"{tday.isoformat()}T00:00:00+00:00"
    tomorrow_start = f"{(tday + timedelta(days=1)).isoformat()}T00:00:00+00:00"
    try:
        rpc = client.rpc(
            "archive_user_stats_and_ranks",
            {
                "p_guild_id": gid,
                "p_author_id": uid,
                "p_today_start": today_start,
                "p_today_end": tomorrow_start,
            },
        ).execute()
        rows = getattr(rpc, "data", None) or []
        if rows and isinstance(rows[0], dict):
            o = rows[0]
            fc = o.get("first_at")
            la = o.get("last_at")
            lr = o.get("lifetime_rank")
            tr = o.get("today_rank")
            return {
                "total": int(o.get("lifetime_count") or 0),
                "today": int(o.get("today_count") or 0),
                "first_at": str(fc).strip() if fc is not None else None,
                "last_at": str(la).strip() if la is not None else None,
                "lifetime_rank": int(lr) if lr is not None else None,
                "today_rank": int(tr) if tr is not None else None,
            }
    except Exception as e:
        print(f"[6stats] archive_user_stats_and_ranks RPC failed — run supabase_archive_ranks_rpc.sql? {e!r}")
    return _archive_stats_fallback_blocking(client, gid, uid, today_start, tomorrow_start)


def _fetch_archive_hour_streak_blocking(guild_id: int, user_id: int) -> Optional[int]:
    """Consecutive UTC hours with ≥1 archived message (RPC archive_consecutive_active_hours)."""
    main = _sys.modules.get("__main__") or _sys.modules.get("index")
    get_client = getattr(main, "_get_supabase_client", None)
    if not callable(get_client):
        return None
    client = get_client()
    if client is None:
        return None
    try:
        r = client.rpc(
            "archive_consecutive_active_hours",
            {"p_guild_id": str(guild_id), "p_author_id": str(user_id)},
        ).execute()
        d = getattr(r, "data", None)
        if isinstance(d, (int, float)):
            return int(d)
        if isinstance(d, str) and d.strip().lstrip("-").isdigit():
            return int(d.strip())
        if isinstance(d, list) and len(d) > 0:
            v = d[0]
            if isinstance(v, (int, float)):
                return int(v)
            if isinstance(v, dict):
                for _k, val in v.items():
                    if isinstance(val, (int, float)):
                        return int(val)
                    break
    except Exception as e:
        print(f"[6stats] archive_consecutive_active_hours RPC failed — run supabase_archive_ranks_rpc.sql? {e!r}")
    return None


def _fetch_bio_slug_blocking(user_id: int) -> Optional[str]:
    main = _sys.modules.get("__main__") or _sys.modules.get("index")
    get_client = getattr(main, "_get_supabase_client", None)
    if not callable(get_client):
        return None
    client = get_client()
    if client is None:
        return None
    try:
        r = (
            client.table("user_bio_profiles")
            .select("slug")
            .eq("user_id", str(user_id))
            .limit(1)
            .execute()
        )
        rows = getattr(r, "data", None) or []
        if not rows:
            return None
        s = str(rows[0].get("slug") or "").strip()
        return s or None
    except Exception:
        return None


@dataclass
class UserStatRow:
    lifetime_messages: int
    last_message_ts: Optional[float]
    session_start_ts: Optional[float]
    session_msg_count: int
    streak_days: int
    last_streak_day: Optional[str]
    daily_messages_today: int
    daily_day_key: Optional[str]
    timezone: Optional[str]


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(STATS_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS guild_user_stats (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            lifetime_messages INTEGER NOT NULL DEFAULT 0,
            last_message_ts REAL,
            session_start_ts REAL,
            session_msg_count INTEGER NOT NULL DEFAULT 0,
            streak_days INTEGER NOT NULL DEFAULT 0,
            last_streak_day TEXT,
            daily_messages_today INTEGER NOT NULL DEFAULT 0,
            daily_day_key TEXT,
            timezone TEXT,
            PRIMARY KEY (guild_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_gus_guild_lifetime
            ON guild_user_stats(guild_id, lifetime_messages DESC);
        CREATE INDEX IF NOT EXISTS idx_gus_guild_daily
            ON guild_user_stats(guild_id, daily_day_key, daily_messages_today DESC);

        CREATE TABLE IF NOT EXISTS daily_channel_messages (
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            day_key TEXT NOT NULL,
            msg_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, channel_id, user_id, day_key)
        );
        CREATE INDEX IF NOT EXISTS idx_dcm_lookup
            ON daily_channel_messages(guild_id, channel_id, day_key);
        """
    )
    conn.commit()


def _record_message_blocking(
    guild_id: int,
    channel_id: int,
    user_id: int,
    now: float,
    today: str,
) -> None:
    yesterday = _utc_yesterday_str()
    conn = _connect()
    try:
        _init_db(conn)
        cur = conn.execute(
            "SELECT lifetime_messages, last_message_ts, session_start_ts, session_msg_count, "
            "streak_days, last_streak_day, daily_messages_today, daily_day_key "
            "FROM guild_user_stats WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        row = cur.fetchone()

        if row is None:
            lifetime = 1
            last_ts = now
            sess_start = now
            sess_count = 1
            streak = 1
            last_streak = today
            daily_today = 1
            daily_key = today
        else:
            (
                lifetime,
                last_ts,
                sess_start,
                sess_count,
                streak,
                last_streak,
                daily_today,
                daily_key,
            ) = row
            lifetime = int(lifetime) + 1

            if last_ts is None or (now - float(last_ts)) > SESSION_GAP_SEC:
                sess_count = 1
                sess_start = now
            else:
                sess_count = int(sess_count) + 1
                sess_start = float(sess_start) if sess_start is not None else now

            last_ts = now

            if daily_key != today:
                daily_today = 1
                daily_key = today
            else:
                daily_today = int(daily_today) + 1

            ls = last_streak
            if ls == today:
                pass
            elif ls == yesterday:
                streak = int(streak) + 1
            else:
                streak = 1
            last_streak = today

        conn.execute(
            """
            INSERT INTO guild_user_stats (
                guild_id, user_id, lifetime_messages, last_message_ts, session_start_ts,
                session_msg_count, streak_days, last_streak_day, daily_messages_today, daily_day_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET
                lifetime_messages = excluded.lifetime_messages,
                last_message_ts = excluded.last_message_ts,
                session_start_ts = excluded.session_start_ts,
                session_msg_count = excluded.session_msg_count,
                streak_days = excluded.streak_days,
                last_streak_day = excluded.last_streak_day,
                daily_messages_today = excluded.daily_messages_today,
                daily_day_key = excluded.daily_day_key
            """,
            (
                guild_id,
                user_id,
                lifetime,
                last_ts,
                sess_start,
                sess_count,
                streak,
                last_streak,
                daily_today,
                daily_key,
            ),
        )

        conn.execute(
            """
            INSERT INTO daily_channel_messages (guild_id, channel_id, user_id, day_key, msg_count)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(guild_id, channel_id, user_id, day_key) DO UPDATE SET
                msg_count = msg_count + 1
            """,
            (guild_id, channel_id, user_id, today),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_user_stats_blocking(guild_id: int, user_id: int) -> Optional[UserStatRow]:
    conn = _connect()
    try:
        _init_db(conn)
        cur = conn.execute(
            "SELECT lifetime_messages, last_message_ts, session_start_ts, session_msg_count, "
            "streak_days, last_streak_day, daily_messages_today, daily_day_key, timezone "
            "FROM guild_user_stats WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        r = cur.fetchone()
        if not r:
            return None
        return UserStatRow(
            lifetime_messages=int(r[0]),
            last_message_ts=float(r[1]) if r[1] is not None else None,
            session_start_ts=float(r[2]) if r[2] is not None else None,
            session_msg_count=int(r[3]),
            streak_days=int(r[4]),
            last_streak_day=r[5],
            daily_messages_today=int(r[6]),
            daily_day_key=r[7],
            timezone=r[8],
        )
    finally:
        conn.close()


def _channel_daily_blocking(guild_id: int, channel_id: int, user_id: int, day: str) -> int:
    conn = _connect()
    try:
        _init_db(conn)
        cur = conn.execute(
            "SELECT msg_count FROM daily_channel_messages "
            "WHERE guild_id = ? AND channel_id = ? AND user_id = ? AND day_key = ?",
            (guild_id, channel_id, user_id, day),
        )
        r = cur.fetchone()
        return int(r[0]) if r else 0
    finally:
        conn.close()


def _clear_timezone_blocking(user_id: int, guild_id: int) -> None:
    conn = _connect()
    try:
        _init_db(conn)
        conn.execute(
            "UPDATE guild_user_stats SET timezone = NULL WHERE user_id = ? AND guild_id = ?",
            (user_id, guild_id),
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_row_and_set_tz(user_id: int, guild_id: int, tz: str) -> None:
    conn = _connect()
    try:
        _init_db(conn)
        conn.execute(
            """
            INSERT INTO guild_user_stats (guild_id, user_id, lifetime_messages, session_msg_count, streak_days, daily_messages_today)
            VALUES (?, ?, 0, 0, 0, 0)
            ON CONFLICT(guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id),
        )
        conn.execute(
            "UPDATE guild_user_stats SET timezone = ? WHERE guild_id = ? AND user_id = ?",
            (tz, guild_id, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_timezone_blocking(guild_id: int, user_id: int) -> Optional[str]:
    conn = _connect()
    try:
        _init_db(conn)
        cur = conn.execute(
            "SELECT timezone FROM guild_user_stats WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        r = cur.fetchone()
        return r[0] if r and r[0] else None
    finally:
        conn.close()


def _rebuild_rank_maps_blocking(guild_id: int, today: str) -> tuple[dict[int, int], dict[int, int]]:
    """Return (lifetime_rank_1based, daily_rank_1based) for this guild."""
    conn = _connect()
    try:
        _init_db(conn)
        life: dict[int, int] = {}
        cur = conn.execute(
            "SELECT user_id, lifetime_messages FROM guild_user_stats WHERE guild_id = ? "
            "AND lifetime_messages > 0 "
            "ORDER BY lifetime_messages DESC",
            (guild_id,),
        )
        rows_l = cur.fetchall()
        rank = 0
        last_v: Optional[int] = None
        for i, row in enumerate(rows_l):
            uid, cnt = int(row[0]), int(row[1])
            if last_v is None or cnt != last_v:
                rank = i + 1
                last_v = cnt
            life[uid] = rank

        daily: dict[int, int] = {}
        cur = conn.execute(
            "SELECT user_id, daily_messages_today FROM guild_user_stats WHERE guild_id = ? "
            "AND daily_day_key = ? AND daily_messages_today > 0 "
            "ORDER BY daily_messages_today DESC",
            (guild_id, today),
        )
        rows_d = cur.fetchall()
        rank = 0
        last_v = None
        for i, row in enumerate(rows_d):
            uid, cnt = int(row[0]), int(row[1])
            if last_v is None or cnt != last_v:
                rank = i + 1
                last_v = cnt
            daily[uid] = rank
        return life, daily
    finally:
        conn.close()


def _fetch_leaderboard_rows_blocking(guild_id: int) -> list[sqlite3.Row]:
    conn = _connect()
    try:
        _init_db(conn)
        cur = conn.execute(
            "SELECT user_id, lifetime_messages, daily_messages_today, daily_day_key, "
            "session_start_ts, session_msg_count, streak_days, last_message_ts "
            "FROM guild_user_stats WHERE guild_id = ?",
            (guild_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


class MessageStatsCog(commands.Cog):
    """Track messages for 6stats; ranks refreshed at most every 5 minutes per guild."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._db_lock = asyncio.Lock()
        self._rank_cache: dict[int, dict[str, Any]] = {}

    @staticmethod
    def _format_rank_value(n: int) -> str:
        return f"{n:,}"

    def _leaderboard_embed(
        self,
        guild: discord.Guild,
        mode: str,
        rows: list[sqlite3.Row],
        viewer_id: int,
    ) -> discord.Embed:
        today = _utc_today_str()
        now = time.time()

        # (uid, score_int, display_suffix)
        entries: list[tuple[int, int, str]] = []

        if mode == "daily":
            for r in rows:
                if r["daily_day_key"] != today:
                    continue
                score = int(r["daily_messages_today"] or 0)
                if score <= 0:
                    continue
                entries.append((int(r["user_id"]), score, "msg today"))
            title = "6stats Leaderboard - Daily"
            color = discord.Color.blurple()
        elif mode == "lifetime":
            for r in rows:
                score = int(r["lifetime_messages"] or 0)
                if score <= 0:
                    continue
                entries.append((int(r["user_id"]), score, "msg total"))
            title = "6stats Leaderboard - Lifetime"
            color = discord.Color.dark_teal()
        elif mode == "activity":
            # Active = >= threshold messages in current session and <= 1h since last message.
            for r in rows:
                sess_count = int(r["session_msg_count"] or 0)
                last_ts = r["last_message_ts"]
                sess_start = r["session_start_ts"]
                if sess_count < ACTIVE_SESSION_MSG_THRESHOLD or last_ts is None or sess_start is None:
                    continue
                if now - float(last_ts) > SESSION_GAP_SEC:
                    continue
                sec = max(0, int(now - float(sess_start)))
                if sec <= 0:
                    continue
                entries.append((int(r["user_id"]), sec, "active sec"))
            title = "6stats Leaderboard - Activity"
            color = discord.Color.green()
        else:  # streak
            for r in rows:
                score = int(r["streak_days"] or 0)
                if score <= 0:
                    continue
                entries.append((int(r["user_id"]), score, "day streak"))
            title = "6stats Leaderboard - Streaks"
            color = discord.Color.orange()

        entries.sort(key=lambda x: (-x[1], x[0]))
        top = entries[:15]
        medals = ("🥇", "🥈", "🥉")
        lines: list[str] = []
        viewer_rank: Optional[int] = None
        viewer_score: Optional[int] = None

        for i, (uid, score, _suffix) in enumerate(entries, start=1):
            if uid == viewer_id and viewer_rank is None:
                viewer_rank = i
                viewer_score = score

        for i, (uid, score, suffix) in enumerate(top, start=1):
            member = guild.get_member(uid)
            label = member.display_name if member else f"`{uid}`"
            prefix = medals[i - 1] if i <= 3 else f"`{i}.`"
            if mode == "activity":
                h, rem = divmod(score, 3600)
                m, _ = divmod(rem, 60)
                score_text = f"{h}h {m}m"
            elif mode == "streak":
                score_text = f"{score:,} days"
            else:
                score_text = f"{score:,}"
            lines.append(f"{prefix} **{label}** — **{score_text}**")

        desc = "\n".join(lines) if lines else "No data yet."
        em = discord.Embed(title=title, description=desc, color=color)
        if viewer_rank is None:
            em.set_footer(text=f"Server: {guild.name} · You: unranked")
        else:
            if mode == "activity" and viewer_score is not None:
                h, rem = divmod(viewer_score, 3600)
                m, _ = divmod(rem, 60)
                mine = f"{h}h {m}m"
            elif mode == "streak" and viewer_score is not None:
                mine = f"{viewer_score} days"
            else:
                mine = f"{viewer_score:,}" if viewer_score is not None else "0"
            em.set_footer(text=f"Server: {guild.name} · Your rank: #{viewer_rank} ({mine})")
        return em

    class StatsLeaderboardView(discord.ui.View):
        def __init__(self, cog: "MessageStatsCog", guild_id: int, author_id: int, rows: list[sqlite3.Row]) -> None:
            super().__init__(timeout=180)
            self.cog = cog
            self.guild_id = guild_id
            self.author_id = author_id
            self.rows = rows
            self.mode = "daily"

        async def _swap(self, interaction: discord.Interaction, mode: str) -> None:
            if interaction.guild is None or interaction.guild.id != self.guild_id:
                return await interaction.response.send_message("This leaderboard is no longer valid here.", ephemeral=True)
            self.mode = mode
            em = self.cog._leaderboard_embed(interaction.guild, mode, self.rows, interaction.user.id)
            await interaction.response.edit_message(embed=em, view=self)

        @discord.ui.button(label="Daily", style=discord.ButtonStyle.blurple)
        async def daily_btn(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
            await self._swap(interaction, "daily")

        @discord.ui.button(label="Lifetime", style=discord.ButtonStyle.gray)
        async def lifetime_btn(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
            await self._swap(interaction, "lifetime")

        @discord.ui.button(label="Activity", style=discord.ButtonStyle.green)
        async def activity_btn(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
            await self._swap(interaction, "activity")

        @discord.ui.button(label="Streak", style=discord.ButtonStyle.red)
        async def streak_btn(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
            await self._swap(interaction, "streak")

        async def on_timeout(self) -> None:
            for item in self.children:
                if isinstance(item, discord.ui.Button):
                    item.disabled = True


    async def record_message(self, message: discord.Message) -> None:
        if not message.guild or message.author.bot or message.webhook_id:
            return
        guild_id = message.guild.id
        channel_id = message.channel.id
        user_id = message.author.id
        now = time.time()
        today = _utc_today_str()

        try:
            async with self._db_lock:
                await asyncio.to_thread(
                    _record_message_blocking,
                    guild_id,
                    channel_id,
                    user_id,
                    now,
                    today,
                )
        except Exception as e:
            print(f"[6stats] record_message failed gid={guild_id} uid={user_id}: {e!r}")

    async def get_user_timezone(self, guild_id: int, user_id: int) -> Optional[str]:
        """Used by other modules (e.g. 6xs announcements)."""
        try:
            async with self._db_lock:
                return await asyncio.to_thread(_fetch_timezone_blocking, guild_id, user_id)
        except Exception as e:
            print(f"[6stats] get_user_timezone failed: {e!r}")
            return None

    async def _get_rank_maps(self, guild_id: int) -> tuple[dict[int, int], dict[int, int]]:
        today = _utc_today_str()
        now = time.time()
        c = self._rank_cache.get(guild_id)
        if c and now - float(c["ts"]) < RANK_CACHE_TTL_SEC and c.get("day") == today:
            return c["life"], c["daily"]

        async with self._db_lock:
            life, daily = await asyncio.to_thread(_rebuild_rank_maps_blocking, guild_id, today)
        self._rank_cache[guild_id] = {"ts": now, "day": today, "life": life, "daily": daily}
        return life, daily

    @commands.command(name="stats", aliases=["sixstats", "mystats"])
    async def cmd_stats(self, ctx: commands.Context, member: Optional[discord.Member] = None) -> None:
        """Show message stats embed. Optional: `6stats @member`"""
        if not ctx.guild:
            await ctx.send("Use `6stats` in a server.", delete_after=8)
            return
        member = member or ctx.author
        if member.bot:
            await ctx.send("Bots have no stats.", delete_after=6)
            return

        gid = ctx.guild.id
        uid = member.id

        # Self-heal: the command message itself should count — make sure it's recorded
        # before we read from DB (the on_message hook runs this too, but if the cog was
        # added after startup or an earlier record failed, this backfills reliably).
        if member.id == ctx.author.id:
            await self.record_message(ctx.message)

        async with self._db_lock:
            row = await asyncio.to_thread(_fetch_user_stats_blocking, gid, uid)

        archive_stats = await asyncio.to_thread(_fetch_archive_stats_blocking, gid, uid)
        bio_slug = await asyncio.to_thread(_fetch_bio_slug_blocking, uid)
        hour_streak = await asyncio.to_thread(_fetch_archive_hour_streak_blocking, gid, uid)

        sess_count = int(row.session_msg_count or 0) if row else 0
        is_active = sess_count >= ACTIVE_SESSION_MSG_THRESHOLD
        now = time.time()
        if is_active and row is not None and row.session_start_ts is not None:
            dur_sec = max(0, int(now - row.session_start_ts))
            h, rem = divmod(dur_sec, 3600)
            mnt, _ = divmod(rem, 60)
            activity_val = f"🟢 Active (session: **{h}**h **{mnt}**m)"
        elif not is_active:
            activity_val = (
                "🔴 Inactive — need **≥3** messages within **1 hour** in the server "
                f"(any channel the bot sees) for an active session."
            )
        else:
            activity_val = "🔴 Inactive"

        streak_days = row.streak_days if row else 0
        hour_streak_n = int(hour_streak) if hour_streak is not None else None
        if hour_streak_n is not None:
            streak_val = (
                f"🔥 **{streak_days:,}** day(s) on the **UTC calendar** (all channels, bot tally)\n"
                f"⏱ **{hour_streak_n:,}** consecutive **UTC hour(s)** with a message in **mirrored 6xs archive** channels"
            )
        else:
            streak_val = (
                f"🔥 **{streak_days:,}** day(s) on the **UTC calendar** (all channels, bot tally)\n"
                "⏱ Hour streak unavailable — run latest `supabase_archive_ranks_rpc.sql` in Supabase."
            )

        ts = int(time.time())
        footer_parts = [f"Local time: <t:{ts}:t>"]
        if row and row.timezone:
            try:
                zi = ZoneInfo(row.timezone)
                local = datetime.now(zi).strftime("%I:%M %p").lstrip("0")
                footer_parts.append(f"{row.timezone}: {local}")
            except Exception:
                footer_parts.append(f"Saved TZ: {row.timezone} (invalid — use `6settimezone clear`)")

        site = _public_site_base()
        if archive_stats is not None:
            arch_n = int(archive_stats.get("total") or 0)
            arch_today = int(archive_stats.get("today") or 0)
            fa = archive_stats.get("first_at")
            la = archive_stats.get("last_at")
            if fa and la:
                span_txt = f"First → last archived: **{fa[:10]}** → **{la[:10]}**"
            elif arch_n == 0:
                span_txt = "No messages in the archive yet (mirrored channels only)."
            else:
                span_txt = "Archive dates unavailable."
            if bio_slug:
                bio_line = f"Bio: {site}/{bio_slug}"
            else:
                bio_line = f"Bio: set your slug at {site}/profile/edit (log in on the site)."
            arch_lr = archive_stats.get("lifetime_rank")
            arch_tr = archive_stats.get("today_rank")
            if arch_n > 0 and arch_lr is not None:
                life_line = f"💬 **{arch_n:,}** lifetime · 🏆 Rank **#{arch_lr}**"
            else:
                life_line = f"💬 **{arch_n:,}** lifetime · 🏆 Rank **—**"
            if arch_today > 0 and arch_tr is not None:
                today_line = f"📅 **{arch_today:,}** today (UTC) · 🏆 Rank **#{arch_tr}**"
            else:
                today_line = f"📅 **{arch_today:,}** today (UTC) · 🏆 Rank **—**"
            rank_hint = ""
            if (arch_n > 0 and arch_lr is None) or (arch_today > 0 and arch_tr is None):
                rank_hint = "\n_Run `supabase_archive_ranks_rpc.sql` in Supabase to enable ranks._"
            archive_block = (
                f"{life_line}\n{today_line}\n{span_txt}\n{bio_line}{rank_hint}\n"
                f"_Mirrored 6xs.lol channels only._"
            )
        else:
            archive_block = (
                "Archive stats unavailable (check Supabase). "
                f"Open the site: {site}/archive"
            )

        em = discord.Embed(
            title="Message stats",
            description=(
                "**6xs.lol** block = mirrored archive only. **Activity** = server-wide session the bot tracks. "
                "**Streaks** = UTC calendar days (server) + consecutive UTC active hours (archive)."
            ),
            color=discord.Color.blurple(),
        )
        em.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        em.set_thumbnail(url=member.display_avatar.url)
        em.add_field(
            name="6xs.lol",
            value=archive_block,
            inline=False,
        )
        em.add_field(name="Activity", value=activity_val, inline=False)
        em.add_field(name="Streaks", value=streak_val, inline=False)
        em.set_footer(text=" · ".join(footer_parts))

        await ctx.send(embed=em)

    @commands.command(name="statslb", aliases=["statsleaderboard", "slb", "6statslb"])
    async def cmd_stats_leaderboard(self, ctx: commands.Context) -> None:
        """Interactive 6stats leaderboard: daily/lifetime/activity/streak buttons."""
        if not ctx.guild:
            return await ctx.send("Use `6statslb` in a server.", delete_after=8)

        async with self._db_lock:
            rows = await asyncio.to_thread(_fetch_leaderboard_rows_blocking, ctx.guild.id)

        view = self.StatsLeaderboardView(self, ctx.guild.id, ctx.author.id, rows)
        em = self._leaderboard_embed(ctx.guild, "daily", rows, ctx.author.id)
        await ctx.send(embed=em, view=view)

    @commands.command(
        name="settimezone",
        aliases=["settz", "timezone", "settimezome", "timezome", "settzone"],
    )
    async def cmd_settimezone(
        self,
        ctx: commands.Context,
        members: commands.Greedy[discord.Member] = None,
        *,
        tz_input: str = "",
    ) -> None:
        """
        Set **your** timezone by country or IANA zone:
        `6settimezone UnitedStates` / `6settimezone America/New_York` / `6settimezone Japan`.
        Trusted users can set someone else's: `6settimezone @user UnitedStates`.
        Clear: `6settimezone clear` (or `6settimezone @user clear`).
        """
        if not ctx.guild:
            await ctx.send("Use in a server.", delete_after=6)
            return

        mentioned_members = members or []
        target: discord.Member = ctx.author  # type: ignore[assignment]
        if mentioned_members:
            picked = mentioned_members[0]
            if picked.id != ctx.author.id and not _is_trusted(ctx.author):
                await ctx.send(
                    "Only trusted users can set someone else's timezone.",
                    delete_after=8,
                )
                return
            target = picked

        tz_part = (tz_input or "").strip()

        if not tz_part or tz_part.lower() in ("clear", "none", "remove", "delete", "off"):
            async with self._db_lock:
                await asyncio.to_thread(_clear_timezone_blocking, target.id, ctx.guild.id)
            who = "your" if target.id == ctx.author.id else f"{target.display_name}'s"
            await ctx.send(f"Cleared {who} timezone override.", delete_after=10)
            return

        zone = resolve_timezone_input(tz_part)
        if zone is None:
            await ctx.send(
                "Could not resolve that to a timezone. Try a country name "
                "(e.g. `UnitedStates`, `Japan`, `UK`, `Germany`) or an IANA zone "
                "(e.g. `America/New_York`, `Europe/London`, `Asia/Tokyo`).",
                delete_after=16,
            )
            return

        async with self._db_lock:
            await asyncio.to_thread(_ensure_row_and_set_tz, target.id, ctx.guild.id, zone)
        who = "your" if target.id == ctx.author.id else f"**{target.display_name}**'s"
        try:
            local = datetime.now(ZoneInfo(zone)).strftime("%I:%M %p").lstrip("0")
            clock_note = f" Local time there is **{local}**."
        except Exception:
            clock_note = ""
        await ctx.send(
            f"Saved {who} timezone as **`{zone}`**.{clock_note}",
            delete_after=14,
        )
