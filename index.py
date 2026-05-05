import discord
from discord.ext import commands
import asyncio
import copy
from collections import defaultdict
import time
import aiohttp
import io
import re
import json
import os
import random
import subprocess
from functools import partial
import html as html_stdlib
import shutil
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path
from collections.abc import Awaitable, Callable
from typing import Any, Optional, Union
from urllib.parse import urlparse

try:
    from supabase import Client as SupabaseClient, create_client as create_supabase_client
except ImportError:
    SupabaseClient = Any  # type: ignore[assignment]
    create_supabase_client = None  # type: ignore[assignment]

try:
    import yt_dlp
except ImportError:
    yt_dlp = None  # type: ignore

from community_banner import render_welcome_banner
from holding_cell_cog import HOLDING_CELL_CHANNEL_ID, resolve_general_catch_channel_id

_SCRIPT_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_SCRIPT_ROOT / ".env")
except ImportError:
    pass

# ========================= CONFIG =========================
PREFIX = "6"
TRUSTED_USERS = [1326518688727437342]

MASS_JOIN_THRESHOLD = 8
DELETE_DAYS_ON_BAN = 7
MASS_ACTION_THRESHOLD = 2        # Max bans/kicks per moderator in 30 minutes
MASS_ACTION_WINDOW = 1800        # 30 minutes in seconds
VERIFIED_MEMBER_ROLE_ID = 1498451320284119252
SERVER_BOOSTER_ROLE_ID = 1498116952114204843

# Pinterest New Saves Auto-Post
AUTO_PFP_CHANNEL_ID = 1487256147701399725
CHECK_INTERVAL = 60
PINTEREST_URL = "https://www.pinterest.com/yeetyuh006/_pins/"
MAX_RECENT_PINS = 20

# 6xs progression (message XP — engaging: steady gains, milestone cosmetic roles)
SIX_XS_STATE_FILE = Path(__file__).resolve().parent / "six_xs_state.json"
SIX_XS_BOOST_FILE = Path(__file__).resolve().parent / "six_xs_boost.json"
SIX_XS_SNAPSHOTS_DIR = SIX_XS_STATE_FILE.parent / "six_xs_snapshots"
SIX_XS_BOOST_CAP = 5000  # extra XP per hit while boost is active (trusted `6boost <amount>`)
SIX_XS_BOOST_DURATION_SEC = 300  # 5 minutes
# During an active 6boost window, XP cooldown is this many seconds (normal is 42).
SIX_XS_COOLDOWN_DURING_BOOST = 21
_SNAPSHOT_FORMAT_VERSION = 1
SIX_XS_COOLDOWN = 42
SIX_XS_XP_RANGE = (17, 30)
# Active-session scaling: if a user keeps earning 6xs XP without a >1h gap, announce the milestone
# and hand out escalating bonus XP per qualifying message (capped to avoid runaway grinds).
SIX_XS_SESSION_GAP_SEC = 3600
SIX_XS_SESSION_BONUS_PER_HOUR = 5
SIX_XS_SESSION_BONUS_MAX_HOURS = 6
FALLBACK_SUPABASE_URL = "https://zmqwqnxfwfwdriqbmkcm.supabase.co"
FALLBACK_SUPABASE_SERVICE_ROLE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InptcXdxbnhmd2Z3ZHJpcWJta2NtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzMyMTc3NCwiZXhwIjoyMDkyODk3Nzc0fQ."
    "as28azOfr291OG1cmI1eH7lG6AROiwjrsNFdR0SiPDo"
)
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip() or FALLBACK_SUPABASE_URL
SUPABASE_KEY = (
    os.getenv("SUPABASE_KEY", "").strip()
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    or FALLBACK_SUPABASE_SERVICE_ROLE_KEY
)
_supabase_client: Optional[SupabaseClient] = None

# Core milestone roles (3–30). Add more entries in `six_xs_role_ids_extra.json`
# next to this file: `{"35": 1234567890123456789, ...}` (string or int keys; omit or use null to skip).
_SIX_XS_ROLES_CORE: dict[int, int] = {
    3: 1498121285073506525,
    5: 1498121009864376350,
    10: 1498121045918355556,
    15: 1498121087563731077,
    20: 1498121152525111349,
    25: 1498121177376493750,
    30: 1498121199836856350,
}
_SIX_XS_ROLES_EXTRA_PATH = _SCRIPT_ROOT / "six_xs_role_ids_extra.json"
SIX_XS_MILESTONES: tuple[int, ...] = (3, *range(5, 101, 5))


def _load_six_xs_roles_extra() -> dict[int, int]:
    out: dict[int, int] = {}
    p = _SIX_XS_ROLES_EXTRA_PATH
    if not p.is_file():
        return out
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[6XS] Could not read {p.name}: {e}")
        return out
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        if str(k).startswith("_"):
            continue
        if v is None:
            continue
        try:
            lvl = int(k)
            rid = int(v)
        except (TypeError, ValueError):
            continue
        if lvl < 1 or rid <= 0:
            continue
        out[lvl] = rid
    return out


def _build_six_xs_roles() -> dict[int, int]:
    merged = dict(_SIX_XS_ROLES_CORE)
    extra = _load_six_xs_roles_extra()
    merged.update(extra)
    if extra:
        print(
            f"[6XS] Merged **{len(extra)}** extra level(s) from `{_SIX_XS_ROLES_EXTRA_PATH.name}` "
            f"→ milestones: {', '.join(str(x) for x in sorted(extra))}"
        )
    return merged


SIX_XS_ROLES = _build_six_xs_roles()

SCRIPT_DIR = _SCRIPT_ROOT
PINTEREST_POSTED_FILE = SCRIPT_DIR / "pinterest_posted.json"

MASS_BAN_WINDOW = 1800
MASS_BAN_THRESHOLD = 5

# OpenRouter AI
OPENROUTER_API_URL = os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL") or os.getenv("GROK_MODEL", "tngtech/deepseek-r1t2-chimera")

# Confessions: set channel ID, or leave 0 to use a channel named "confessions"
CONFESSIONS_CHANNEL_ID = 0

# Hourly full channel wipe + live countdown (bot needs Manage Messages + Read History)
AUTO_WIPE_CHANNEL_ID = 1488433048109453383
AUTO_WIPE_PERIOD_SECONDS = 3600
AUTO_WIPE_EDIT_INTERVAL = 60
AUTO_WIPE_STATE_FILE = SCRIPT_DIR / "auto_wipe_channel.json"
AUTO_WIPE_VIDEO_SUFFIXES = frozenset(
    {".mp4", ".webm", ".mov", ".m4v", ".mkv", ".avi", ".mpeg", ".mpg", ".ogv"}
)

# 6xs.lol archive wipe schedule mirrors (used by `6timer`)
ARCHIVE_TIMER_CHANNEL_LABELS: dict[str, str] = {
    "1498122216800522261": "#general",
    "1498278738096295936": "#tcc",
    "1498521334198702223": "#blood",
}
ARCHIVE_TIMER_CHANNEL_INTERVALS_SEC: dict[str, int] = {
    "1498122216800522261": 24 * 60 * 60,
    "1498278738096295936": 24 * 60 * 60,
    "1498521334198702223": 60 * 60,
}

# 6gif — needs ffmpeg on PATH (or FFMPEG_PATH in .env)
FFMPEG_BIN = os.getenv("FFMPEG_PATH", "ffmpeg")
GIF_MAX_CLIP_SECONDS = 18.0
GIF_MAX_SOURCE_BYTES = 45 * 1024 * 1024
GIF_MAX_OUTPUT_BYTES = 25 * 1024 * 1024
GIF_TRY_PRESETS: tuple[tuple[float, int, int], ...] = (
    (18.0, 480, 10),
    (14.0, 400, 9),
    (10.0, 320, 8),
    (8.0, 260, 7),
)

GROK_SYSTEM_PROMPT = """you are one anon in a discord roast bot. your entire reply is exactly one sentence, all lowercase, like you typed it fast on an imageboard—no caps ever.

vibe: tcc-adjacent, sinister, /b/-tier edge, brutal dom energy, call them a loser or worse when it fits, mean and clipped, not polite.

raindear is untouchable—never diss, mock, or roast anyone named raindear (any spelling).

hard no: slurs against race religion gender sexuality disability; praising real-world violence mass attacks or perpetrators; credible threats of real harm.

anti-ai: no "as an ai", no hedging, no multiple sentences, no lists, no essay tone—type raw like a shitpost.
"""

# Random roll on `6respond`: use chaos prompt so replies sometimes bait the whole channel harder
RESPOND_CHAOS_CHANCE = 0.24

GROK_RESPOND_CHAOS_PROMPT = """you are one anon in a discord roast bot. your entire reply is exactly one sentence, all lowercase.

this is CHAOS mode: be heinous—unhinged, parasocial-adjacent, cursed hypotheticals, fake lore about the target, horny-on-main *as a joke*, accusations that don't quite land, anything that makes people @ each other and keep typing. mean-funny not sincere cruelty.

still roast **only** the named target; you may drag the **vibe** of the channel into it so the server gets loud.

raindear is untouchable—never diss, mock, or roast anyone named raindear (any spelling).

hard no: slurs against race religion gender sexuality disability; minors in any sexual context; praising real-world violence mass attacks or perpetrators; credible threats of real harm; doxxing.

anti-ai: no "as an ai", one sentence only, no quotes wrapping the whole reply, no bullet lists.
"""

# Welcome PNG banner channel + where to point new members
WELCOME_BANNER_CHANNEL_ID = 1498118050313011240
WELCOME_GENERAL_CHANNEL_ID = 1484430600273268756

# Recovery / migration targets
PRIMARY_GUILD_ID = 1498102377037692961
MIGRATION_VERIFY_CHANNEL_ID = 1498120494140887080
MIGRATION_REGISTRY_FILE = SCRIPT_DIR / "migration_registry.json"
GUILD_BACKUP_DIR = SCRIPT_DIR / "guild_backups"

# Dead-chat revival: scan #general (same ID here); needs XAI_API_KEY
DEAD_CHAT_CHANNEL_ID = 1484430600273268756
DEAD_CHAT_IDLE_MINUTES = 50
DEAD_CHAT_COOLDOWN_MINUTES = 120

DEAD_CHAT_SYSTEM_PROMPT = """you revive a dead discord general chat OR drop a manual topic ping (same vibe).

output: exactly one or two short sentences (can be one line). mostly lowercase; you may CAPS one word for punch.

read the sample lines: if they're about something specific (games, girls, drama, brainrot, politics, music), lean into that energy with a hot take or question.

if the sample is empty or painfully boring, go feral shitpost: parasocial, horny-on-main *jokingly*, unhinged—but clearly ironic/funny, not a real creep.

if the user message says to IGNORE the sample, obey that — invent something unrelated and chaotic (still ironic, not real harassment).

goal: make people want to reply. no apology for posting.

hard no: slurs by protected class; minors in any sexual context; praising real-world violence or mass attacks; credible threats; doxxing.
no "as an ai". no hashtags. no bullet lists."""

# 6ai — direct Q&A, one paragraph (Grok)
GROK_SUMMARIZE_SYSTEM_PROMPT = """You are an expert at making simple summaries of Discord chats.

Rules:
- Use very simple and easy words.
- Keep every sentence short and clear.
- Always use the exact usernames and say what each person was saying or doing.
- Stay 100% non-biased — only the real facts from the chat.
- You can use light swearing (shit, fuck, damn, asshole, etc.) naturally if it fits, but don't overdo it.
- Make it medium length: clear but not too long.

Time rules (do this first):
- Only summarize the past 3 hours max (from the very last message backwards).
- If the whole chat is shorter than 3 hours, summarize the entire chat.
- Start the summary by saying how long the part you are summarizing is. Example: "This summary covers the last 2 hours and 45 minutes" or "This summary covers the last 3 hours".

Now summarize this Discord chat (only the past 3 hours max):"""


GROK_AI_SYSTEM_PROMPT = """You answer questions for a Discord bot. Output rules are strict:

- Reply in **exactly one paragraph**: one continuous block of prose. No bullet lists, no numbered lists, no headers, no multiple paragraphs separated by blank lines.
- Be **direct** — no "as an AI", no hedging opener ("that's a great question"), no filler conclusion. Say what you think or know.
- Stay under ~200 words in that single paragraph so it stays readable in chat.

Hard no: sexual content involving minors; credible instructions for serious real-world harm; doxxing; slurs against race, religion, gender, sexuality, or disability used as slurs.

Otherwise answer plainly — don't sandbag the user with empty corporate-safety non-answers unless the question is genuinely unknowable (then one short honest line in the same paragraph)."""

# 6topic: after this many uses in TOPIC_COMMAND_WINDOW_SEC, force off-transcript chaos
TOPIC_COMMAND_WINDOW_SEC = 600
TOPIC_COMMAND_COUNT_FOR_FERAL = 3

# 6respond: N freshest channel lines + M older lines sampled from on-disk archive (per channel)
RESPOND_NEW_MESSAGE_COUNT = 7
RESPOND_OLD_MESSAGE_COUNT = 3
RESPOND_ARCHIVE_MAX_LINES = 1200
RESPOND_ARCHIVE_DIR = _SCRIPT_ROOT / "respond_message_archives"
_respond_archive_lock = asyncio.Lock()

# ========================================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True


class SixBot(commands.Bot):
    """Prefix 6 / 6␠ — economy loaded in setup_hook so commands always register."""

    def __init__(self) -> None:
        super().__init__(
            command_prefix=(f"{PREFIX} ", PREFIX),
            intents=intents,
            help_command=None,
            strip_after_prefix=True,
            case_insensitive=True,
        )

    async def setup_hook(self) -> None:
        from economy_cog import EconomyCog
        from flags_cog import FlagsCog
        from holding_cell_cog import HoldingCellCog
        from menu_cog import MenuCog
        from personality_cog import PersonalityCog
        from funny_cog import FunnyCog
        from rolecolor_cog import RoleColorCog
        from stats_cog import MessageStatsCog

        await self.add_cog(EconomyCog(self))
        await self.add_cog(FlagsCog(self))
        await self.add_cog(PersonalityCog(self))
        await self.add_cog(HoldingCellCog(self))
        await self.add_cog(MenuCog(self))
        await self.add_cog(RoleColorCog(self))
        await self.add_cog(FunnyCog(self))
        await self.add_cog(MessageStatsCog(self))
bot = SixBot()


@bot.command(name="uplift")
async def cmd_uplift(ctx: commands.Context, mode: Optional[str] = None) -> None:
    """
    Toggle the bot's occasional uplifting DMs.
    Usage: `6uplift off` / `6uplift on` / `6uplift`
    """
    m = (mode or "").strip().lower()
    uid = ctx.author.id
    async with _uplift_lock:
        opt = {str(x) for x in (_uplift_state.get("opt_out") or [])}
        if not m:
            state = "off" if str(uid) in opt else "on"
            return await ctx.send(f"Uplift DMs are **{state}** for you. Use `6uplift off` or `6uplift on`.")
        if m in ("off", "no", "disable", "stop"):
            opt.add(str(uid))
            _uplift_state["opt_out"] = sorted(opt)
            _uplift_save_state_sync()
            return await ctx.send("Okay — I won’t send you uplift DMs anymore.")
        if m in ("on", "yes", "enable", "start"):
            opt.discard(str(uid))
            _uplift_state["opt_out"] = sorted(opt)
            _uplift_save_state_sync()
            return await ctx.send("Okay — uplift DMs are back on for you.")
    return await ctx.send("Usage: `6uplift` / `6uplift off` / `6uplift on`.")

# Caches
join_cache = defaultdict(list)
ban_cache = defaultdict(list)
mod_action_cache = defaultdict(list)   # For moderator cooldown (ban & kick)
channel_delete_cache = defaultdict(list)
posted_pins = set()
six_xs_data = {}  # "guild_id:user_id" -> {"xp": int, "last_msg": float}
# guild_id -> {"until": unix_ts, "amount": int} — timed 6boost window (see SIX_XS_BOOST_DURATION_SEC)
six_xs_boost: dict[int, dict] = {}
_six_xs_lock = asyncio.Lock()
_xs_rescan_lock = asyncio.Lock()
_auto_wipe_task_started = False
_dead_chat_task_started = False
_six_xs_backfill_task_started = False
_last_dead_chat_revive_ts: float = 0.0
_topic_command_times: defaultdict[int, list[float]] = defaultdict(list)

DISCORD_LINK_WARN_FILE = _SCRIPT_ROOT / "discord_link_warns.json"
_discord_link_warns: dict[str, int] = {}
_discord_link_warn_lock = asyncio.Lock()

# Uplift DMs (opt-in by default, rare + rate-limited)
UPLIFT_STATE_FILE = _SCRIPT_ROOT / "uplift_dm_state.json"
UPLIFT_COOLDOWN_MIN_SEC = 6 * 60 * 60
UPLIFT_COOLDOWN_MAX_SEC = 16 * 60 * 60
UPLIFT_REQUIRED_ROLE_ID = 1498121009864376350
UPLIFT_MIN_MESSAGES_LAST_12H = 6
UPLIFT_BURST_MESSAGES = 30
UPLIFT_BURST_WINDOW_SEC = 10 * 60
UPLIFT_LATE_NIGHT_START_HOUR = 23
UPLIFT_LATE_NIGHT_END_HOUR = 3
UPLIFT_TZ_OFFSET_HOURS = int(os.getenv("UPLIFT_TZ_OFFSET_HOURS", "-10"))
UPLIFT_ARCHIVE_MESSAGE_LIMIT = 50
UPLIFT_ARCHIVE_LINE_MAX_CHARS = 140
UPLIFT_ARCHIVE_MAX_PROMPT_CHARS = 5200
_uplift_lock = asyncio.Lock()
_uplift_state: dict[str, Any] = {
    "opt_out": [],
    "last_dm": {},
    "global_last": 0.0,
    "global_day": "",
    "global_count": 0,
    "last_text_by_user": {},
    "next_due_by_user": {},
    "dm_reply_budget_by_user": {},
}
_uplift_recent_by_user: defaultdict[int, list[str]] = defaultdict(list)
_uplift_msg_times_by_user: defaultdict[int, list[float]] = defaultdict(list)    
UPLIFT_VIBE_KEYWORDS = (
    "tired", "grinding", "stressed", "long day", "rough", "exhausted",
    "burnt", "burned out", "drained", "overwhelmed", "sad", "anxious",
)
UPLIFT_AI_SYSTEM_PROMPT = """Write one short uplifting DM based on a user's recent chat lines.

Rules:
- one sentence only, lowercase, natural discord style
- 8 to 22 words
- specific to their vibe/topics from the log, but do not quote exact lines
- no cringe, no therapy tone, no emojis, no hashtags
- must feel different from the previous DM if provided
- output only the sentence, nothing else"""
UPLIFT_DM_REPLY_SYSTEM_PROMPT = """Reply to a user's DM in one short sentence.

Rules:
- funny, warm, a little flirty, natural
- 6 to 20 words
- lowercase, no emoji spam
- no cringe therapy speech
- output only one sentence"""

TopicChannel = Union[discord.TextChannel, discord.Thread]


def _normalize_pinterest_pin_url(url: str) -> str:
    """Single canonical key per pin so 236x / 474x / originals URLs dedupe the same image."""
    u = (url or "").strip()
    if not u:
        return u
    u = u.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    if re.search(r"i\.pinimg\.com", u, re.I):
        u = re.sub(r"https?://", "https://", u, count=1, flags=re.I)
        u = re.sub(r"(https://i\.pinimg\.com/)\d+x/", r"\1originals/", u, flags=re.I)
    return u


def load_posted_pins():
    global posted_pins
    try:
        if PINTEREST_POSTED_FILE.exists():
            data = json.loads(PINTEREST_POSTED_FILE.read_text(encoding="utf-8"))
            raw_list = data if isinstance(data, list) else []
            posted_pins = {_normalize_pinterest_pin_url(u) for u in raw_list if isinstance(u, str) and u}
            save_posted_pins()
    except Exception as e:
        print(f"[PINTEREST] Could not load state file: {e}")


def save_posted_pins():
    try:
        PINTEREST_POSTED_FILE.write_text(
            json.dumps(sorted(posted_pins)), encoding="utf-8"
        )
    except Exception as e:
        print(f"[PINTEREST] Could not save state file: {e}")


def _read_six_xs_dict(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        return data
    except Exception as e:
        print(f"[6XS] Could not load {path.name}: {e}")
        return None


def _get_supabase_client() -> Optional[SupabaseClient]:
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        print(
            "[6XS] Missing Supabase env. Set SUPABASE_URL and "
            "SUPABASE_KEY (or SUPABASE_SERVICE_ROLE_KEY). XP persistence is disabled."
        )
        return None
    if create_supabase_client is None:
        print("[6XS] supabase-py is not installed; run `pip install supabase`.")
        return None
    try:
        _supabase_client = create_supabase_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"[6XS] Could not initialize Supabase client: {e}")
        return None
    return _supabase_client


def load_six_xs():
    """Load XP state from Supabase `user_stats` into in-memory cache."""
    global six_xs_data
    client = _get_supabase_client()
    if client is None:
        six_xs_data = {}
        return
    try:
        rows: list[dict[str, Any]] = []
        page_size = 1000
        start = 0
        while True:
            resp = (
                client.table("user_stats")
                .select("user_id,xp,level")
                .range(start, start + page_size - 1)
                .execute()
            )
            chunk = resp.data or []
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            start += page_size

        out: dict[str, dict[str, float | int]] = {}
        for row in rows:
            user_id = str(row.get("user_id", "")).strip()
            if not user_id:
                continue
            xp = int(row.get("xp", 0) or 0)
            out[user_id] = {"xp": xp, "last_msg": 0.0}
        six_xs_data = out
    except Exception as e:
        print(f"[6XS] Could not load Supabase user_stats: {e}")
        six_xs_data = {}


def save_six_xs_sync():
    client = _get_supabase_client()
    if client is None:
        return
    try:
        payload: list[dict[str, Any]] = []
        for key, entry in six_xs_data.items():
            user_id = str(key).strip()
            if not user_id:
                continue
            xp = int(entry.get("xp", 0))
            _, level = total_xp_and_6xs(xp)
            payload.append({"user_id": user_id, "xp": xp, "level": level})
        if not payload:
            return
        client.table("user_stats").upsert(payload).execute()
    except Exception as e:
        print(f"[6XS] Could not save Supabase user_stats: {e}")


def load_six_xs_boost() -> None:
    global six_xs_boost
    six_xs_boost = {}
    now = time.time()
    try:
        if not SIX_XS_BOOST_FILE.is_file():
            return
        raw = json.loads(SIX_XS_BOOST_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
        for k, v in raw.items():
            try:
                gid = int(k)
            except (TypeError, ValueError):
                continue
            if isinstance(v, dict):
                until = float(v.get("until", 0))
                amt = int(v.get("amount", 0))
            else:
                continue
            if until <= now or amt <= 0:
                continue
            if amt > SIX_XS_BOOST_CAP:
                amt = SIX_XS_BOOST_CAP
            six_xs_boost[gid] = {"until": until, "amount": amt}
    except (OSError, json.JSONDecodeError) as e:
        print(f"[6XS] Could not load boost file: {e}")


def save_six_xs_boost_sync() -> None:
    try:
        SIX_XS_BOOST_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            str(k): {"until": float(v.get("until", 0)), "amount": int(v.get("amount", 0))}
            for k, v in six_xs_boost.items()
            if isinstance(v, dict)
        }
        SIX_XS_BOOST_FILE.write_text(json.dumps(dict(sorted(payload.items())), indent=0), encoding="utf-8")
    except OSError as e:
        print(f"[6XS] Could not save boost file: {e}")


def _six_xs_boost_status(guild_id: int) -> tuple[int, float]:
    """Return (extra_xp_per_hit, seconds_remaining). Clears expired sessions."""
    global six_xs_boost
    sess = six_xs_boost.get(guild_id)
    if not sess or not isinstance(sess, dict):
        return (0, 0.0)
    until = float(sess.get("until", 0))
    amt = max(0, min(SIX_XS_BOOST_CAP, int(sess.get("amount", 0))))
    now = time.time()
    if amt <= 0 or now >= until:
        if guild_id in six_xs_boost:
            six_xs_boost.pop(guild_id, None)
            save_six_xs_boost_sync()
        return (0, 0.0)
    return (amt, until - now)


def _six_xs_boost_extra(guild_id: int) -> int:
    return _six_xs_boost_status(guild_id)[0]


def _snapshot_sanitize_label(raw: str) -> str:
    s = "".join(c if c.isalnum() or c in "-_" else "_" for c in (raw or "").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return (s[:64] if s else "save")


def _snapshot_new_path(safe_label: str) -> Path:
    ts = discord.utils.utcnow().strftime("%Y%m%d_%H%M%S")
    return SIX_XS_SNAPSHOTS_DIR / f"{safe_label}_{ts}.json"


def _snapshot_read_meta(path: Path) -> Optional[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _snapshot_list_entries() -> list[tuple[float, str, str, int, str]]:
    """(saved_at_unix, label_display, filename, entry_count, iso_saved) newest first."""
    if not SIX_XS_SNAPSHOTS_DIR.is_dir():
        return []
    out: list[tuple[float, str, str, int, str]] = []
    for p in SIX_XS_SNAPSHOTS_DIR.glob("*.json"):
        meta = _snapshot_read_meta(p)
        if not meta or "six_xs_data" not in meta:
            continue
        ts = float(meta.get("saved_at_unix") or p.stat().st_mtime)
        label_d = str(meta.get("label_display") or meta.get("label") or p.stem)
        n = int(meta.get("entry_count", len(meta.get("six_xs_data") or {})))
        iso = str(meta.get("saved_at") or "")
        out.append((ts, label_d, p.name, n, iso))
    out.sort(key=lambda x: -x[0])
    return out


def _snapshot_find_latest(safe_label: str) -> Optional[Path]:
    if not SIX_XS_SNAPSHOTS_DIR.is_dir():
        return None
    best_ts = -1.0
    best_path: Optional[Path] = None
    for p in SIX_XS_SNAPSHOTS_DIR.glob("*.json"):
        meta = _snapshot_read_meta(p)
        if not meta:
            continue
        if str(meta.get("label") or "") != safe_label:
            continue
        ts = float(meta.get("saved_at_unix") or p.stat().st_mtime)
        if ts > best_ts:
            best_ts, best_path = ts, p
    if best_path is not None:
        return best_path
    for p in SIX_XS_SNAPSHOTS_DIR.glob(f"{safe_label}_*.json"):
        meta = _snapshot_read_meta(p)
        ts = float(meta.get("saved_at_unix", p.stat().st_mtime)) if meta else p.stat().st_mtime
        if ts > best_ts:
            best_ts, best_path = ts, p
    return best_path


def load_migration_registry() -> dict[str, list[int]]:
    try:
        if not MIGRATION_REGISTRY_FILE.is_file():
            return {}
        raw = json.loads(MIGRATION_REGISTRY_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        out: dict[str, list[int]] = {}
        for k, v in raw.items():
            if not isinstance(v, list):
                continue
            ids: list[int] = []
            for x in v:
                try:
                    ids.append(int(x))
                except (TypeError, ValueError):
                    continue
            out[str(k)] = sorted(set(ids))
        return out
    except Exception as e:
        print(f"[MIGRATION] could not load registry: {e}")
        return {}


def save_migration_registry_sync(reg: dict[str, list[int]]) -> None:
    try:
        payload = {str(k): sorted(set(int(x) for x in v)) for k, v in reg.items()}
        MIGRATION_REGISTRY_FILE.write_text(json.dumps(payload, indent=0), encoding="utf-8")
    except Exception as e:
        print(f"[MIGRATION] could not save registry: {e}")


def _export_guild_structure(guild: discord.Guild) -> dict[str, Any]:
    roles = []
    for role in sorted(guild.roles, key=lambda r: r.position):
        if role.is_default() or role.managed:
            continue
        roles.append(
            {
                "name": role.name,
                "permissions": int(role.permissions.value),
                "color": int(role.color.value),
                "hoist": bool(role.hoist),
                "mentionable": bool(role.mentionable),
            }
        )

    categories = [{"name": c.name, "position": int(c.position)} for c in guild.categories]

    text_channels = []
    voice_channels = []
    for ch in guild.channels:
        if isinstance(ch, discord.TextChannel):
            text_channels.append(
                {
                    "name": ch.name,
                    "topic": ch.topic or "",
                    "nsfw": bool(ch.nsfw),
                    "slowmode_delay": int(ch.slowmode_delay),
                    "position": int(ch.position),
                    "category": ch.category.name if ch.category else None,
                }
            )
        elif isinstance(ch, discord.VoiceChannel):
            voice_channels.append(
                {
                    "name": ch.name,
                    "bitrate": int(ch.bitrate),
                    "user_limit": int(ch.user_limit),
                    "position": int(ch.position),
                    "category": ch.category.name if ch.category else None,
                }
            )

    return {
        "guild_name": guild.name,
        "guild_id": guild.id,
        "roles": roles,
        "categories": categories,
        "text_channels": text_channels,
        "voice_channels": voice_channels,
    }


async def _apply_guild_structure_backup(guild: discord.Guild, data: dict[str, Any]) -> tuple[int, int, int]:
    created_roles = 0
    created_categories = 0
    created_channels = 0

    existing_roles = {r.name: r for r in guild.roles}
    for r in data.get("roles", []):
        name = str(r.get("name") or "").strip()
        if not name or name in existing_roles:
            continue
        try:
            role = await guild.create_role(
                name=name,
                permissions=discord.Permissions(int(r.get("permissions", 0))),
                colour=discord.Colour(int(r.get("color", 0))),
                hoist=bool(r.get("hoist", False)),
                mentionable=bool(r.get("mentionable", False)),
                reason="6load restore",
            )
            existing_roles[role.name] = role
            created_roles += 1
            await asyncio.sleep(0.35)
        except Exception as e:
            print(f"[6LOAD] role create failed ({name}): {e}")

    existing_categories = {c.name: c for c in guild.categories}
    for c in sorted(data.get("categories", []), key=lambda x: int(x.get("position", 0))):
        name = str(c.get("name") or "").strip()
        if not name or name in existing_categories:
            continue
        try:
            cat = await guild.create_category(name=name, reason="6load restore")
            existing_categories[cat.name] = cat
            created_categories += 1
            await asyncio.sleep(0.35)
        except Exception as e:
            print(f"[6LOAD] category create failed ({name}): {e}")

    existing_text = {ch.name for ch in guild.text_channels}
    for ch in sorted(data.get("text_channels", []), key=lambda x: int(x.get("position", 0))):
        name = str(ch.get("name") or "").strip()
        if not name or name in existing_text:
            continue
        try:
            cat = existing_categories.get(str(ch.get("category") or ""))
            await guild.create_text_channel(
                name=name,
                topic=(str(ch.get("topic") or "")[:1024] or None),
                slowmode_delay=max(0, min(21600, int(ch.get("slowmode_delay", 0)))),
                nsfw=bool(ch.get("nsfw", False)),
                category=cat,
                reason="6load restore",
            )
            existing_text.add(name)
            created_channels += 1
            await asyncio.sleep(0.35)
        except Exception as e:
            print(f"[6LOAD] text channel create failed ({name}): {e}")

    existing_voice = {ch.name for ch in guild.voice_channels}
    for ch in sorted(data.get("voice_channels", []), key=lambda x: int(x.get("position", 0))):
        name = str(ch.get("name") or "").strip()
        if not name or name in existing_voice:
            continue
        try:
            cat = existing_categories.get(str(ch.get("category") or ""))
            await guild.create_voice_channel(
                name=name,
                bitrate=max(8000, min(384000, int(ch.get("bitrate", 64000)))),
                user_limit=max(0, min(99, int(ch.get("user_limit", 0)))),
                category=cat,
                reason="6load restore",
            )
            existing_voice.add(name)
            created_channels += 1
            await asyncio.sleep(0.35)
        except Exception as e:
            print(f"[6LOAD] voice channel create failed ({name}): {e}")

    return created_roles, created_categories, created_channels


def xp_cost_to_advance_from(current_6xs: int) -> int:
    lvl = max(1, int(current_6xs))
    # Keep early progression unchanged; ease scaling for higher levels.
    if lvl <= 20:
        return 60 + 25 * lvl
    return 560 + 18 * (lvl - 20)


def min_raw_xp_for_6xs_level(target_level: int) -> int:
    """Minimum lifetime XP so chat 6xs level is exactly target_level with 0 XP toward the next."""
    if target_level <= 1:
        return 0
    total = 0
    for lvl in range(1, target_level):
        total += xp_cost_to_advance_from(lvl)
    return total


def total_xp_and_6xs(raw_xp: int):
    """Return (total_xp_remaining, current_6xs) where 6xs starts at 1."""
    level = 1
    xp = raw_xp
    while True:
        need = xp_cost_to_advance_from(level)
        if xp < need:
            return xp, level
        xp -= need
        level += 1


def _member_6xs_milestone_from_roles(member: discord.Member) -> int:
    """Highest 6xs milestone implied by cosmetic roles in this guild (see SIX_XS_ROLES)."""
    hi = 1
    for milestone, rid in SIX_XS_ROLES.items():
        if rid and member.get_role(rid) is not None:
            hi = max(hi, milestone)
    return hi


def _six_xs_sync_raw_to_milestone_roles(entry: dict, member: discord.Member) -> bool:
    """If milestone roles imply a higher tier than stored XP, raise stored XP so level matches."""
    if member.bot:
        return False
    raw = int(entry.get("xp", 0))
    _, lvl_xp = total_xp_and_6xs(raw)
    lvl_role = _member_6xs_milestone_from_roles(member)
    if lvl_role <= lvl_xp:
        return False
    need_raw = min_raw_xp_for_6xs_level(lvl_role)
    if need_raw <= raw:
        return False
    entry["xp"] = need_raw
    return True


async def _six_xs_ensure_entry_synced(guild_id: int, member: Optional[discord.Member]) -> None:
    """Persist XP floor from milestone roles so rank/progression stay consistent."""
    if member is None or member.bot:
        return
    key = f"{guild_id}:{member.id}"
    async with _six_xs_lock:
        e = six_xs_data.setdefault(key, {"xp": 0, "last_msg": 0.0})
        if _six_xs_sync_raw_to_milestone_roles(e, member):
            save_six_xs_sync()


def _build_six_xs_leaderboard_rows(guild: discord.Guild, limit: int) -> list[tuple[int, int, int, int, int, str]]:
    """
    All non-bot members + leavers still in JSON.
    Rank level = max(chat-derived 6xs, milestone from SIX_XS_ROLES).
    Sort: rank level, then lifetime XP, then name.
    Tuple: (user_id, raw_xp, rank_level, lvl_from_xp_only, lvl_from_roles_only, label)
    """
    gid = guild.id
    prefix = f"{gid}:"
    rows: list[tuple[int, int, int, int, int, str]] = []
    seen: set[int] = set()

    for m in guild.members:
        if m.bot:
            continue
        seen.add(m.id)
        raw = int(six_xs_data.get(f"{gid}:{m.id}", {}).get("xp", 0))
        lvl_role = _member_6xs_milestone_from_roles(m)
        eff_raw = max(raw, min_raw_xp_for_6xs_level(lvl_role))
        _, lvl_xp = total_xp_and_6xs(eff_raw)
        rank_lvl = lvl_xp
        rows.append((m.id, eff_raw, rank_lvl, lvl_xp, lvl_role, m.display_name))

    for key, entry in six_xs_data.items():
        if not key.startswith(prefix):
            continue
        try:
            uid = int(key.split(":", 1)[1])
        except (ValueError, IndexError):
            continue
        if uid in seen:
            continue
        raw = int(entry.get("xp", 0))
        _, lvl_xp = total_xp_and_6xs(raw)
        rows.append((uid, raw, lvl_xp, lvl_xp, 1, f"`{uid}` (left server)"))

    rows.sort(key=lambda r: (-r[2], -r[1], r[5].lower()))
    return rows[:limit]


def is_trusted(user):
    return user.id in TRUSTED_USERS or (isinstance(user, discord.Member) and user.guild_permissions.administrator)


def load_discord_link_warns() -> None:
    global _discord_link_warns
    try:
        if DISCORD_LINK_WARN_FILE.is_file():
            raw = json.loads(DISCORD_LINK_WARN_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                out: dict[str, int] = {}
                for k, v in raw.items():
                    try:
                        n = int(v)
                        if n > 0:
                            out[str(k)] = n
                    except (TypeError, ValueError):
                        continue
                _discord_link_warns = out
    except (OSError, json.JSONDecodeError) as e:
        print(f"[LINK_WARN] could not load: {e}")


def save_discord_link_warns_sync() -> None:
    try:
        DISCORD_LINK_WARN_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = DISCORD_LINK_WARN_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(_discord_link_warns, indent=0), encoding="utf-8")
        os.replace(tmp, DISCORD_LINK_WARN_FILE)
    except OSError as e:
        print(f"[LINK_WARN] could not save: {e}")


def _uplift_load_state() -> None:
    global _uplift_state
    try:
        if UPLIFT_STATE_FILE.is_file():
            raw = json.loads(UPLIFT_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                _uplift_state = raw
    except (OSError, json.JSONDecodeError) as e:
        print(f"[UPLIFT] could not load: {e}")


def _uplift_save_state_sync() -> None:
    try:
        UPLIFT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = UPLIFT_STATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(_uplift_state, indent=0), encoding="utf-8")
        os.replace(tmp, UPLIFT_STATE_FILE)
    except OSError as e:
        print(f"[UPLIFT] could not save: {e}")


def _uplift_today_key() -> str:
    # UTC day key so global rate limit is stable across restarts/timezones
    return time.strftime("%Y-%m-%d", time.gmtime())


def _uplift_is_opted_out(user_id: int) -> bool:
    try:
        s = _uplift_state.get("opt_out") or []
        return str(user_id) in {str(x) for x in s}
    except Exception:
        return False


def _uplift_record_recent_text(user_id: int, text: str) -> None:
    t = (text or "").strip()
    if not t:
        return
    buf = _uplift_recent_by_user[user_id]
    buf.append(t[:240])
    if len(buf) > 12:
        del buf[:-12]


def _uplift_compact_text_line(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return ""
    return t[:UPLIFT_ARCHIVE_LINE_MAX_CHARS]


async def _uplift_fetch_recent_archive_lines(
    user_id: int, limit: int = UPLIFT_ARCHIVE_MESSAGE_LIMIT
) -> tuple[list[str], int]:
    client = _get_supabase_client()
    if client is None:
        return [], 0
    try:
        resp = (
            client.table("archive_messages")
            .select("content,created_at_discord")
            .eq("author_id", str(user_id))
            .order("created_at_discord", desc=True)
            .limit(max(1, int(limit)))
            .execute()
        )
        rows = getattr(resp, "data", None) or []
    except Exception as e:
        print(f"[UPLIFT] archive fetch failed for {user_id}: {e}")
        return [], 0
    out: list[str] = []
    cutoff = time.time() - (12 * 60 * 60)
    active_12h = 0
    for r in rows:
        ts_raw = str((r or {}).get("created_at_discord") or "").strip()
        if ts_raw:
            try:
                dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if dt.timestamp() >= cutoff:
                    active_12h += 1
            except Exception:
                pass
        txt = _uplift_compact_text_line(str((r or {}).get("content") or ""))
        if txt:
            out.append(txt)
    return out[:limit], active_12h


async def _uplift_generate_from_archive(user_id: int, user_name: str, archive_lines: list[str]) -> Optional[str]:
    if not archive_lines:
        return None
    last_map = _uplift_state.get("last_text_by_user") or {}
    last_text = str(last_map.get(str(user_id)) or last_map.get(f"name:{user_name}") or "").strip()
    raw_block = "\n".join(f"- {ln}" for ln in archive_lines)
    raw_block = raw_block[:UPLIFT_ARCHIVE_MAX_PROMPT_CHARS]
    user_prompt = (
        f"user display name: {user_name}\n"
        f"previous dm (must be different): {last_text or '(none)'}\n"
        "recent messages (raw plain text, newest first):\n"
        f"{raw_block}\n"
        "write the new uplifting dm now."
    )
    try:
        msg = await grok_chat(
            UPLIFT_AI_SYSTEM_PROMPT,
            user_prompt,
            max_tokens=64,
            temperature=0.9,
        )
    except Exception as e:
        print(f"[UPLIFT] ai generation failed for {user_id}: {e}")
        return None

    msg = re.sub(r"\s+", " ", (msg or "").strip().strip('"').strip("'"))
    if not msg:
        return None
    msg = msg[:220]
    if last_text and msg.lower() == last_text.lower():
        msg = f"{user_name.lower()} your energy in chat is genuinely good, keep bringing that same presence."
    return msg


def _uplift_pick_message(user_id: int, user_name: str, recent: list[str]) -> Optional[str]:
    blob = " ".join(recent[-6:]).lower()
    if not blob:
        return None

    topic = _uplift_extract_topic(recent)
    funny = bool(re.search(r"\b(lmao|lmfao|lol|rofl)\b|😂|🤣", blob))
    hype = bool(re.search(r"\b(w|dub|cooked|goat|based)\b", blob))
    supportive = bool(re.search(r"\b(proud of you|you got this|u got this|good job|nice work|congrats)\b", blob))
    creative = bool(re.search(r"\b(edit|drawing|art|song|music|beat|mix|clip)\b", blob))
    question_asker = bool(re.search(r"\?\s*$|\b(why|how|what if|thoughts)\b", blob))

    # Keep it short + not creepy: reference the *vibe*, not quoting their text.
    if supportive:
        pool = [
            f"real talk {user_name}, you give good energy in chat — keep that up.",
            f"{user_name} you’re genuinely uplifting to be around, respect.",
            f"{user_name} you make people feel seen in chat, that’s rare.",
        ]
    elif funny:
        pool = [
            f"{user_name} you’re funny as hell, that made me laugh.",
            f"yo {user_name} your jokes land, keep cooking.",
            f"{user_name} your timing is elite, you keep chat alive.",
        ]
    elif creative:
        pool = [
            f"{user_name} you’re creative asf — love the way you think.",
            f"yo {user_name} your ideas are actually sick, keep posting.",
            f"{user_name} your creative brain is different in the best way.",
        ]
    elif question_asker:
        pool = [
            f"{user_name} your questions actually spark conversation, keep doing that.",
            f"yo {user_name}, you’re good at getting people talking fr.",
        ]
    elif hype:
        pool = [
            f"{user_name} you’re cool asf, your vibe is solid.",
            f"ngl {user_name} you’ve got main character energy (in a good way).",
            f"{user_name} your confidence is contagious, chat feels better with you in it.",
        ]
    else:
        # only send a generic uplift if the user is actively chatting a bit
        if len(recent) < 4:
            return None
        pool = [
            f"just a quick one {user_name}: you’re cool, keep being you.",
            f"{user_name} your presence makes chat better, appreciate you fr.",
            f"lowkey {user_name}, people like seeing you talk in here.",
        ]

    if topic:
        pool.extend(
            [
                f"{user_name}, your takes on {topic} are actually fun to read.",
                f"ngl {user_name}, the way you talk about {topic} is fire.",
            ]
        )
    return _uplift_pick_non_repeating(user_id, user_name, pool)


def _uplift_extract_topic(recent: list[str]) -> str:
    text = " ".join(recent[-8:]).lower()
    if not text:
        return ""
    cleaned = re.sub(r"https?://\S+", " ", text)
    words = re.findall(r"[a-z0-9][a-z0-9_\-]{2,20}", cleaned)
    stop = {
        "that",
        "this",
        "with",
        "from",
        "have",
        "just",
        "your",
        "youre",
        "about",
        "what",
        "when",
        "where",
        "they",
        "them",
        "their",
        "really",
        "would",
        "could",
        "should",
        "like",
        "dont",
        "cant",
        "aint",
        "im",
        "ive",
        "lol",
        "lmao",
    }
    freq: dict[str, int] = {}
    for w in words:
        if w in stop or w.isdigit() or len(w) < 3:
            continue
        freq[w] = freq.get(w, 0) + 1
    if not freq:
        return ""
    # Bias toward repeated words; fall back to longest among top frequency.
    best_n = max(freq.values())
    top = [w for w, n in freq.items() if n == best_n]
    top.sort(key=lambda w: (-len(w), w))
    return top[0][:24]


def _uplift_pick_non_repeating(user_id: int, user_name: str, pool: list[str]) -> Optional[str]:
    if not pool:
        return None
    last_map = _uplift_state.get("last_text_by_user") or {}
    last = str(last_map.get(str(user_id)) or last_map.get(f"name:{user_name}") or "").strip().lower()
    candidates = [p for p in pool if p.strip().lower() != last]
    choice = random.choice(candidates or pool)
    return choice


def _uplift_local_hour(now_ts: float) -> int:
    return int(time.gmtime(now_ts + (UPLIFT_TZ_OFFSET_HOURS * 3600)).tm_hour)


def _uplift_is_late_night(now_ts: float) -> bool:
    h = _uplift_local_hour(now_ts)
    if UPLIFT_LATE_NIGHT_START_HOUR <= UPLIFT_LATE_NIGHT_END_HOUR:
        return UPLIFT_LATE_NIGHT_START_HOUR <= h <= UPLIFT_LATE_NIGHT_END_HOUR
    return h >= UPLIFT_LATE_NIGHT_START_HOUR or h <= UPLIFT_LATE_NIGHT_END_HOUR


def _uplift_has_vibe_keyword(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in UPLIFT_VIBE_KEYWORDS)


def _uplift_record_activity_and_is_burst(user_id: int, now_ts: float) -> bool:
    arr = _uplift_msg_times_by_user[user_id]
    cutoff = now_ts - UPLIFT_BURST_WINDOW_SEC
    arr[:] = [x for x in arr if x >= cutoff]
    arr.append(now_ts)
    return len(arr) >= UPLIFT_BURST_MESSAGES


async def maybe_handle_uplift_dm_reply(message: discord.Message) -> bool:
    if message.guild is not None:
        return False
    if message.author.bot:
        return False
    uid_key = str(message.author.id)
    async with _uplift_lock:
        budget_map = _uplift_state.get("dm_reply_budget_by_user") or {}
        budget = int(budget_map.get(uid_key) or 0)
        if budget <= 0:
            return False
        user_text = (message.content or "").strip()
        if not user_text:
            return False
        last_text_map = _uplift_state.get("last_text_by_user") or {}
        last_bot_text = str(last_text_map.get(uid_key) or "").strip()
        prompt = (
            f"user said: {user_text[:500]}\n"
            f"your previous message: {last_bot_text[:300]}\n"
            "reply naturally now."
        )
        try:
            reply = await grok_chat(
                UPLIFT_DM_REPLY_SYSTEM_PROMPT,
                prompt,
                max_tokens=64,
                temperature=1.0,
            )
        except Exception as e:
            print(f"[UPLIFT DM] reply generation failed: {e}")
            return False
        reply = re.sub(r"\s+", " ", (reply or "").strip()).strip('"').strip("'")[:220]
        if not reply:
            return False
        try:
            await message.channel.send(reply)
        except discord.HTTPException:
            return False
        budget_map[uid_key] = max(0, budget - 1)
        _uplift_state["dm_reply_budget_by_user"] = budget_map
        last_text_map[uid_key] = reply
        _uplift_state["last_text_by_user"] = last_text_map
        _uplift_save_state_sync()
        return True


async def maybe_send_uplift_dm(message: discord.Message) -> None:
    if not message.guild or message.author.bot:
        return
    if not isinstance(message.author, discord.Member):
        return
    if message.webhook_id:
        return
    if _uplift_is_opted_out(message.author.id):
        return
    required_role = message.guild.get_role(UPLIFT_REQUIRED_ROLE_ID)
    if required_role is None or required_role not in message.author.roles:
        return

    text = (message.content or "").strip()
    # don't trigger off commands or empty lines
    if not text or text.startswith(PREFIX):
        return

    now = time.time()
    burst_active = _uplift_record_activity_and_is_burst(message.author.id, now)
    vibe_trigger = _uplift_has_vibe_keyword(text)
    late_night = _uplift_is_late_night(now)
    # Natural trigger:
    # - keyword distress/support vibes anytime, OR
    # - active burst + late-night window.
    if not (vibe_trigger or (burst_active and late_night)):
        return

    async with _uplift_lock:
        now = time.time()
        uid_key = str(message.author.id)
        last_dm_map = _uplift_state.get("last_dm") or {}
        last_user_ts = float(last_dm_map.get(uid_key) or 0.0)
        due_map = _uplift_state.get("next_due_by_user") or {}
        next_due = float(due_map.get(uid_key) or 0.0)
        if next_due <= 0 and last_user_ts > 0:
            next_due = last_user_ts + random.randint(UPLIFT_COOLDOWN_MIN_SEC, UPLIFT_COOLDOWN_MAX_SEC)
        if next_due > 0 and now < next_due:
            return

        # Build uplift from last 50 archived messages for this user (low-token plain-text prompt).
        archive_lines, active_12h = await _uplift_fetch_recent_archive_lines(
            message.author.id, UPLIFT_ARCHIVE_MESSAGE_LIMIT
        )
        if active_12h < UPLIFT_MIN_MESSAGES_LAST_12H:
            return
        dm_text = await _uplift_generate_from_archive(
            message.author.id,
            message.author.display_name,
            archive_lines,
        )
        if not dm_text:
            return

        footer = "\n\n(send `6uplift off` in the server to stop these DMs.)"
        try:
            dm = await message.author.create_dm()
            await dm.send((dm_text + footer)[:1900])
        except discord.Forbidden:
            # Can't DM; don't keep retrying constantly.
            last_dm_map[uid_key] = now
        except discord.HTTPException:
            return

        last_dm_map[uid_key] = now
        _uplift_state["last_dm"] = last_dm_map
        last_text_map = _uplift_state.get("last_text_by_user") or {}
        last_text_map[uid_key] = dm_text
        _uplift_state["last_text_by_user"] = last_text_map
        due_map[uid_key] = now + random.randint(UPLIFT_COOLDOWN_MIN_SEC, UPLIFT_COOLDOWN_MAX_SEC)
        _uplift_state["next_due_by_user"] = due_map
        dm_budget_map = _uplift_state.get("dm_reply_budget_by_user") or {}
        dm_budget_map[uid_key] = 2
        _uplift_state["dm_reply_budget_by_user"] = dm_budget_map
        _uplift_save_state_sync()


# Only server **invite** URLs — not discord.com/channels, CDN, GIFs, etc.
# Includes **bare** `discord.gg/code` (no https) — that’s how promos are usually pasted.
_DISCORD_INVITE_RE = re.compile(
    r"(?:"
    # With scheme
    r"https?://(?:www\.)?"
    r"(?:discord\.gg/[a-zA-Z0-9\-]+|discord\.com/invite/[a-zA-Z0-9\-]+|discordapp\.com/invite/[a-zA-Z0-9\-]+)"
    r"|"
    # Bare discord.gg/… (word boundary so we don’t match `fooddiscord.gg`)
    r"\bdiscord\.gg/[a-zA-Z0-9\-]{2,}"
    r"|"
    r"\bdiscord\.com/invite/[a-zA-Z0-9\-]{2,}"
    r"|"
    r"\bdiscordapp\.com/invite/[a-zA-Z0-9\-]{2,}"
    r")",
    re.IGNORECASE,
)


def _content_has_discord_invite_link(text: str) -> bool:
    if not text or not str(text).strip():
        return False
    return _DISCORD_INVITE_RE.search(text) is not None


def _message_has_discord_invite_link(message: discord.Message) -> bool:
    """True if plain text or any embed field/url contains a Discord invite pattern."""
    if _content_has_discord_invite_link(message.content or ""):
        return True
    for emb in message.embeds:
        if emb.url and _content_has_discord_invite_link(str(emb.url)):
            return True
        if emb.description and _content_has_discord_invite_link(emb.description):
            return True
        for f in emb.fields or []:
            if f.value and _content_has_discord_invite_link(f.value):
                return True
            if f.name and _content_has_discord_invite_link(f.name):
                return True
    return False


async def maybe_punish_discord_link(message: discord.Message) -> bool:
    """
    Delete messages containing Discord **invite** links (e.g. ``https://discord.gg/...``); warn up to 3 times, then ban on the next (4th) offense.
    Returns True if this message was treated as a link violation (skip 6xs, etc.).
    """
    if not message.guild or message.author.bot:
        return False
    author = message.author
    if not isinstance(author, discord.Member):
        return False
    if is_trusted(author):
        return False
    if author.guild_permissions.manage_messages or author.guild_permissions.administrator:
        return False
    if not _message_has_discord_invite_link(message):
        return False

    try:
        await message.delete()
    except discord.HTTPException:
        pass

    key = f"{message.guild.id}:{author.id}"
    async with _discord_link_warn_lock:
        n = int(_discord_link_warns.get(key, 0)) + 1
        _discord_link_warns[key] = n
        save_discord_link_warns_sync()

    ch = message.channel
    if n <= 3:
        try:
            await ch.send(
                f"{author.mention} **Warning {n}/3** — don’t post **Discord invite** links (`discord.gg` / `discord.com/invite`) in this server.",
                delete_after=14,
                allowed_mentions=discord.AllowedMentions(
                    everyone=False, users=[author], roles=False
                ),
            )
        except discord.HTTPException:
            pass
        return True

    try:
        await message.guild.ban(author, delete_message_days=0, reason="Posted Discord invite links after 3 warnings")
    except discord.Forbidden:
        try:
            await ch.send(
                "Couldn’t **ban** that user — I need **Ban Members** (and role hierarchy).",
                delete_after=10,
            )
        except discord.HTTPException:
            pass
    except discord.HTTPException as e:
        try:
            await ch.send(f"Ban failed: `{e}`", delete_after=10)
        except discord.HTTPException:
            pass
    else:
        async with _discord_link_warn_lock:
            _discord_link_warns.pop(key, None)
            save_discord_link_warns_sync()

    return True


def _grok_api_key() -> Optional[str]:
    return (
        os.getenv("OPENROUTER_API_KEY")
        or os.getenv("XAI_API_KEY")
        or os.getenv("GROK_API_KEY")
    )


def format_roast_line(raw: str) -> str:
    s = raw.strip().lower().strip('"').strip("'")
    if "\n" in s:
        s = s.split("\n", 1)[0].strip()
    for sep in ".!?":
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    s = s.strip()
    if not s:
        s = raw.strip().lower()[:220]
    return s[:220]


def _respond_archive_path(guild_id: int, channel_id: int) -> Path:
    return RESPOND_ARCHIVE_DIR / f"{guild_id}_{channel_id}.jsonl"


def _respond_append_line_sync(path: Path, line: str, max_lines: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return
    if len(lines) <= max_lines:
        return
    keep = lines[-max_lines:]
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(keep)


async def respond_archive_record_message(message: discord.Message) -> None:
    """Append human text messages to per-channel JSONL for `6respond` older-context sampling."""
    if message.author.bot or message.guild is None:
        return
    ch = message.channel
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    text = (message.content or "").strip()
    if not text:
        if message.attachments:
            text = "[attachment]"
        elif message.embeds:
            text = "[embed]"
        else:
            return
    rec = {
        "id": message.id,
        "ts": message.created_at.timestamp(),
        "a": message.author.display_name[:80],
        "t": text[:900],
    }
    line = json.dumps(rec, ensure_ascii=False) + "\n"
    path = _respond_archive_path(message.guild.id, ch.id)
    async with _respond_archive_lock:
        await asyncio.to_thread(_respond_append_line_sync, path, line, RESPOND_ARCHIVE_MAX_LINES)


def _respond_load_all_archive_records(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    d = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if "id" not in d or "ts" not in d:
                    continue
                a, t = d.get("a"), d.get("t")
                if not isinstance(a, str) or not isinstance(t, str):
                    continue
                out.append(d)
    except OSError:
        return []
    return out


def _respond_pick_old_context_lines(
    guild_id: int,
    channel_id: int,
    exclude_ids: set[int],
    oldest_new_ts: float,
    count: int,
) -> list[str]:
    """Prefer archived lines older than the newest batch; pad from any non-overlapping archive lines."""
    path = _respond_archive_path(guild_id, channel_id)
    recs = _respond_load_all_archive_records(path)
    ex = exclude_ids
    strict = [d for d in recs if int(d["id"]) not in ex and float(d["ts"]) < oldest_new_ts]
    random.shuffle(strict)
    picked: list[dict[str, Any]] = strict[:count]
    picked_ids = {int(d["id"]) for d in picked}
    if len(picked) < count:
        loose = [d for d in recs if int(d["id"]) not in ex and int(d["id"]) not in picked_ids]
        random.shuffle(loose)
        picked.extend(loose[: count - len(picked)])
    return [f"{d['a']}: {d['t'][:900]}" for d in picked[:count]]


async def grok_chat(
    system_prompt: str,
    user_content: str,
    *,
    max_tokens: int = 128,
    temperature: float = 1.15,
) -> str:
    key = _grok_api_key()
    if not key:
        raise RuntimeError("Missing OPENROUTER_API_KEY")

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://6xs.lol",
        "X-Title": "6XS Bot",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(OPENROUTER_API_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"OpenRouter API {resp.status}: {body[:500]}")
            data = json.loads(body)
    try:
        return data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Bad OpenRouter response: {body[:400]}") from e


async def grok_complete(user_content: str) -> str:
    return await grok_chat(GROK_SYSTEM_PROMPT, user_content, max_tokens=72, temperature=1.2)


async def grok_respond_roast(user_content: str, *, chaos: bool) -> str:
    sys_p = GROK_RESPOND_CHAOS_PROMPT if chaos else GROK_SYSTEM_PROMPT
    temp = 1.35 if chaos else 1.2
    toks = 88 if chaos else 72
    return await grok_chat(sys_p, user_content, max_tokens=toks, temperature=temp)


def _topic_register_command_use(channel_id: int) -> int:
    """Count recent `6topic` invocations in this channel (including this one)."""
    now = time.time()
    arr = _topic_command_times[channel_id]
    arr.append(now)
    cutoff = now - TOPIC_COMMAND_WINDOW_SEC
    while arr and arr[0] < cutoff:
        arr.pop(0)
    return len(arr)


def _transcript_is_dry(raw_lines: list[str]) -> bool:
    if len(raw_lines) < 2:
        return True
    bodies: list[str] = []
    for ln in raw_lines:
        if ":" in ln:
            bodies.append(ln.split(":", 1)[1].strip())
        else:
            bodies.append(ln.strip())
    substantial = sum(1 for b in bodies if len(b) > 18)
    return substantial < 2


async def _collect_channel_transcript(channel: TopicChannel, limit: int = 45) -> tuple[str, bool]:
    raw_lines: list[str] = []
    async for m in channel.history(limit=limit):
        if m.author.bot:
            continue
        t = (m.content or "").strip()
        if t:
            raw_lines.append(f"{m.author.display_name}: {t[:240]}")
    raw_lines.reverse()
    transcript = (
        "\n".join(raw_lines[-32:])
        if raw_lines
        else "(no recent plain text — empty, stickers, or media only)"
    )
    dry = _transcript_is_dry(raw_lines) if raw_lines else True
    return transcript, dry


async def generate_chat_topic_line(channel: TopicChannel, *, source: str) -> str:
    """
    Shared brain for auto dead-chat revival and manual `6topic`.
    source: \"6topic\" | \"dead_chat\"
    """
    transcript, dry = await _collect_channel_transcript(channel, limit=45)
    force_feral = False
    if source == "6topic":
        n = _topic_register_command_use(channel.id)
        force_feral = dry or (n >= TOPIC_COMMAND_COUNT_FOR_FERAL)
    else:
        force_feral = dry

    user_prompt = f"Sample of recent human messages (oldest first):\n{transcript}\n\n"
    if force_feral:
        user_prompt += (
            "CRITICAL OVERRIDE: chat is dead/dry OR people keep spamming topic requests. "
            "Do **not** tie your line to the sample — invent something **unrelated**: weird take, fake scenario, "
            "horny-on-main-adjacent joke (clearly ironic), cursed question. One or two short sentences.\n\n"
        )
    user_prompt += "Write your line(s) now."

    raw = await grok_chat(
        DEAD_CHAT_SYSTEM_PROMPT,
        user_prompt,
        max_tokens=150,
        temperature=1.22,
    )
    return " ".join(
        ln.strip() for ln in raw.replace("\n\n", "\n").split("\n") if ln.strip()
    )[:500]


async def _welcome_banner_job(member_id: int, guild_id: int) -> None:
    await asyncio.sleep(3)
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            return
        member = guild.get_member(member_id)
        if not member or member.bot:
            return
        ch = guild.get_channel(WELCOME_BANNER_CHANNEL_ID)
        if not isinstance(ch, discord.TextChannel):
            return
        gen_ch = guild.get_channel(WELCOME_GENERAL_CHANNEL_ID)
        gen_ref = gen_ch.mention if isinstance(gen_ch, discord.abc.GuildChannel) else "**#general**"
        bot_m = guild.me
        if not bot_m:
            return
        bp = ch.permissions_for(bot_m)
        if not (bp.send_messages and bp.attach_files):
            print("[WELCOME] need Send Messages + Attach Files in banner channel")
            return
        av_url = member.display_avatar.with_size(512).url
        async with aiohttp.ClientSession() as session:
            async with session.get(av_url) as resp:
                if resp.status != 200:
                    print(f"[WELCOME] avatar HTTP {resp.status}")
                    return
                avatar_bytes = await resp.read()
        png_bytes = await asyncio.to_thread(
            render_welcome_banner,
            member.display_name,
            guild.member_count,
            avatar_bytes,
        )
        await ch.send(
            f"{member.mention} **welcome** — say something in {gen_ref}.",
            file=discord.File(io.BytesIO(png_bytes), filename="welcome.png"),
            allowed_mentions=discord.AllowedMentions(users=[member]),
        )
    except FileNotFoundError as e:
        print(f"[WELCOME] {e}")
        try:
            guild = bot.get_guild(guild_id)
            if not guild:
                return
            m = guild.get_member(member_id)
            ch = guild.get_channel(WELCOME_BANNER_CHANNEL_ID)
            if m and isinstance(ch, discord.TextChannel):
                gen_ch = guild.get_channel(WELCOME_GENERAL_CHANNEL_ID)
                gen_ref = gen_ch.mention if isinstance(gen_ch, discord.abc.GuildChannel) else "**#general**"
                await ch.send(
                    f"{m.mention} **welcome** — talk in {gen_ref} "
                    f"(add **banner_base.png** / **banner_base.jpg** next to **community_banner.py**).",
                    allowed_mentions=discord.AllowedMentions(users=[m]),
                )
        except Exception as ex:
            print(f"[WELCOME] fallback: {ex}")
    except Exception as e:
        print(f"[WELCOME] {e}")


async def dead_chat_revival_loop() -> None:
    global _last_dead_chat_revive_ts
    await bot.wait_until_ready()
    while True:
        try:
            await asyncio.sleep(300)
            if bot.is_closed:
                break
            if not _grok_api_key():
                continue
            ch = bot.get_channel(DEAD_CHAT_CHANNEL_ID)
            if not isinstance(ch, discord.TextChannel):
                continue
            me = ch.guild.me
            if not me or not ch.permissions_for(me).send_messages:
                continue
            last: Optional[discord.Message] = None
            async for msg in ch.history(limit=1):
                last = msg
                break
            if last is None:
                continue
            idle_sec = (discord.utils.utcnow() - last.created_at).total_seconds()
            if idle_sec < DEAD_CHAT_IDLE_MINUTES * 60:
                continue
            if time.time() - _last_dead_chat_revive_ts < DEAD_CHAT_COOLDOWN_MINUTES * 60:
                continue
            try:
                text = await generate_chat_topic_line(ch, source="dead_chat")
            except Exception as e:
                print(f"[DEAD_CHAT] grok: {e}")
                continue
            if len(text) < 4:
                continue
            try:
                await ch.send(text[:2000], allowed_mentions=discord.AllowedMentions.none())
                _last_dead_chat_revive_ts = time.time()
                print(f"[DEAD_CHAT] posted (~{idle_sec / 60:.0f}m idle)")
            except discord.HTTPException as e:
                print(f"[DEAD_CHAT] send: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[DEAD_CHAT] loop: {e}")


_ZW_CHARS = frozenset(
    "\u200b\u200c\u200d\ufeff\u2060\u180e\u200e\u200f"
    "\u202a\u202b\u202c\u202d\u202e"
)


def _clean_command_argument(s: str) -> str:
    """Strip zero-width / bidi junk Discord often inserts around mentions or pasted text."""
    if not s:
        return s
    return "".join(c for c in s if c not in _ZW_CHARS).strip()


def _channel_name_key(name: str) -> str:
    return "".join(c for c in name if c not in _ZW_CHARS).strip().lower()


def confessions_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    if CONFESSIONS_CHANNEL_ID:
        ch = guild.get_channel(CONFESSIONS_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            return ch
    for tc in guild.text_channels:
        if isinstance(tc, discord.TextChannel) and _channel_name_key(tc.name) == "confessions":
            return tc
    return None


async def send_chunked(channel: discord.abc.Messageable, text: str, max_len: int = 1950):
    text = text or "(no response)"
    for i in range(0, len(text), max_len):
        await channel.send(text[i : i + max_len])


# ====================== PINTEREST AUTO-POST ======================
# ====================== IMPROVED PINTEREST (2026) ======================
async def check_new_pinterest_saves():
    PINTEREST_RSS_URL = "https://www.pinterest.com/yeetyuh006/feed.rss"
    print(f"[PINTEREST] Checking for new saves from {PINTEREST_RSS_URL}...")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(PINTEREST_RSS_URL, headers=headers) as resp:
                if resp.status != 200:
                    print(f"[PINTEREST] Failed to load RSS feed: {resp.status}")
                    return

                text = await resp.text()

                # NEW REGEX: Specifically targets the URL inside the <img> tag within the RSS XML
                # This looks for any link starting with i.pinimg.com ending in an image extension
                image_urls = re.findall(r'https://i\.pinimg\.com/\d+x/[^"\'>]+\.(?:jpg|png|jpeg|webp)', text)

                if not image_urls:
                    print("[PINTEREST] No images found in the feed")
                    return

                # Convert the small thumbnails (236x) to high-quality originals
                # We use a set to automatically remove any duplicates found in one scrape
                unique_urls = list(dict.fromkeys(image_urls))

                high_res_urls = []
                for url in unique_urls:
                    # Replaces the size directory (e.g., /236x/) with /originals/
                    high_res = re.sub(r'/\d+x/', '/originals/', url)
                    high_res_urls.append(high_res)

                if not high_res_urls:
                    print("[PINTEREST] No high-res URLs after conversion")
                    return

                # Empty cache (first run or deleted state): post up to MAX_RECENT_PINS from the current
                # feed, then mark the whole feed seen. This is independent of other bot tasks (auto-wipe,
                # etc.) — they share only the asyncio event loop and do not block each other.
                if not posted_pins:
                    backlog = high_res_urls[:MAX_RECENT_PINS]
                    posted = 0
                    for i, url in enumerate(backlog):
                        key = _normalize_pinterest_pin_url(url)
                        if key in posted_pins:
                            continue
                        if await post_image_to_channel(url):
                            posted += 1
                            posted_pins.add(key)
                        if i < len(backlog) - 1:
                            await asyncio.sleep(2.5)
                    for url in high_res_urls:
                        posted_pins.add(_normalize_pinterest_pin_url(url))
                    save_posted_pins()
                    print(
                        f"[PINTEREST] Initial sync: posted {posted}/{len(backlog)} recent pin(s); "
                        f"marked {len(high_res_urls)} unique pin(s) seen (already-seen never reposted)."
                    )
                    return

                # Post only the newest feed item whose canonical URL we have not posted yet
                for url in high_res_urls:
                    key = _normalize_pinterest_pin_url(url)
                    if key in posted_pins:
                        continue
                    if await post_image_to_channel(url):
                        posted_pins.add(key)
                        save_posted_pins()
                        print(f"[PINTEREST] ✅ New save detected and posted! URL: {url}")
                    return

        print("[PINTEREST] No new saves this check")
    except Exception as e:
        print(f"[PINTEREST] Error: {e}")


async def post_image_to_channel(image_url: str) -> bool:
    channel = bot.get_channel(AUTO_PFP_CHANNEL_ID)
    if not channel:
        print(f"[PINTEREST] Channel {AUTO_PFP_CHANNEL_ID} not found")
        return False

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(image_url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    file = discord.File(io.BytesIO(data), filename="new_pinterest_save.png")
                    await channel.send(file=file)
                    print("[PINTEREST] Posted new image successfully")
                    return True
                print(f"[PINTEREST] Failed to download image: {resp.status}")
                return False
    except Exception as e:
        print(f"[PINTEREST] Failed to post image: {e}")
        return False


def _auto_wipe_countdown_embed(remaining_seconds: int) -> discord.Embed:
    seconds = max(0, int(remaining_seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    clock = f"{h}:{m:02d}:{s:02d}"
    return discord.Embed(
        title="⏳ Channel wipe countdown",
        description=(
            f"**Time left:** `{clock}`\n\n"
            f"When this hits **`0:00:00`**, every message here is deleted and the timer starts again."
        ),
        color=discord.Color.dark_red(),
    ).set_footer(text="Updates every minute · time counts down")


def _auto_wipe_load_state() -> tuple[Optional[float], Optional[int]]:
    try:
        raw = AUTO_WIPE_STATE_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        return float(data["purge_at"]), int(data["timer_message_id"])
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None, None


def _auto_wipe_save_state(purge_at: float, message_id: int) -> None:
    try:
        AUTO_WIPE_STATE_FILE.write_text(
            json.dumps({"purge_at": purge_at, "timer_message_id": message_id}),
            encoding="utf-8",
        )
    except OSError as e:
        print(f"[AUTO_WIPE] could not save state: {e}")


def _auto_wipe_clear_state() -> None:
    try:
        if AUTO_WIPE_STATE_FILE.exists():
            AUTO_WIPE_STATE_FILE.unlink()
    except OSError:
        pass


def _fmt_remaining(seconds: int) -> str:
    sec = max(0, int(seconds))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m:02d}m {s:02d}s"


async def _load_archive_nuke_schedule_rows() -> list[dict[str, Any]]:
    client = _get_supabase_client()
    if client is None:
        return []
    ids = list(ARCHIVE_TIMER_CHANNEL_LABELS.keys())
    try:
        resp = (
            client.table("archive_nuke_schedule")
            .select("channel_id,next_nuke_at")
            .in_("channel_id", ids)
            .execute()
        )
        rows = resp.data or []
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"[6timer] archive_nuke_schedule load failed: {e}")
        return []


async def _auto_wipe_unpin_all(channel: discord.TextChannel) -> None:
    try:
        pins = await channel.pins()
    except discord.HTTPException:
        return
    for p in pins:
        try:
            await p.unpin()
        except discord.HTTPException:
            pass


async def _auto_wipe_resume_or_start(channel: discord.TextChannel) -> tuple[float, discord.Message]:
    now = time.time()
    purge_at, mid = _auto_wipe_load_state()
    if purge_at is not None and mid is not None and purge_at > now:
        try:
            msg = await channel.fetch_message(mid)
            return purge_at, msg
        except (discord.NotFound, discord.Forbidden):
            pass
    purge_at = now + AUTO_WIPE_PERIOD_SECONDS
    em = _auto_wipe_countdown_embed(int(purge_at - now))
    msg = await channel.send(embed=em)
    _auto_wipe_save_state(purge_at, msg.id)
    return purge_at, msg


async def auto_wipe_channel_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        channel = bot.get_channel(AUTO_WIPE_CHANNEL_ID)
        if not isinstance(channel, discord.TextChannel):
            await asyncio.sleep(45)
            continue
        try:
            purge_at, timer_msg = await _auto_wipe_resume_or_start(channel)
            while time.time() < purge_at:
                remaining = int(purge_at - time.time())
                if remaining <= 0:
                    break
                em = _auto_wipe_countdown_embed(remaining)
                try:
                    await timer_msg.edit(content=None, embed=em)
                except discord.NotFound:
                    timer_msg = await channel.send(embed=em)
                    _auto_wipe_save_state(purge_at, timer_msg.id)
                except discord.Forbidden:
                    print("[AUTO_WIPE] missing permission to edit/send timer in wipe channel")
                except discord.HTTPException as e:
                    print(f"[AUTO_WIPE] timer edit: {e}")
                sleep_for = min(AUTO_WIPE_EDIT_INTERVAL, max(1, remaining))
                await asyncio.sleep(sleep_for)

            await _auto_wipe_unpin_all(channel)
            try:
                await channel.purge(limit=None)
            except discord.Forbidden:
                print("[AUTO_WIPE] missing permission to purge wipe channel")
            except discord.HTTPException as e:
                print(f"[AUTO_WIPE] purge failed: {e}")
            _auto_wipe_clear_state()
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[AUTO_WIPE] loop error: {e}")
            await asyncio.sleep(30)


@bot.command(name="timer")
async def cmd_timer(ctx: commands.Context, channel_ref: Optional[str] = None) -> None:
    """
    Show next wipe timers:
    - local `AUTO_WIPE_CHANNEL_ID` countdown
    - archive channel schedules from Supabase `archive_nuke_schedule`
    """
    now = time.time()

    lines: list[str] = []
    purge_at, _mid = _auto_wipe_load_state()
    if purge_at and purge_at > now:
        eta = int(purge_at - now)
        lines.append(
            f"**Auto-wipe channel** <#{AUTO_WIPE_CHANNEL_ID}>: {_fmt_remaining(eta)} "
            f"(at <t:{int(purge_at)}:F>)"
        )
    else:
        lines.append(
            f"**Auto-wipe channel** <#{AUTO_WIPE_CHANNEL_ID}>: timer not active yet "
            f"(interval {AUTO_WIPE_PERIOD_SECONDS // 60} min)."
        )

    rows = await _load_archive_nuke_schedule_rows()
    by_id: dict[str, dict[str, Any]] = {}
    for r in rows:
        cid = str(r.get("channel_id", "")).strip()
        if cid:
            by_id[cid] = r

    requested_cid: Optional[str] = None
    if channel_ref:
        raw = channel_ref.strip()
        m = re.fullmatch(r"<#(\d{17,22})>", raw)
        if m:
            requested_cid = m.group(1)
        elif re.fullmatch(r"\d{17,22}", raw):
            requested_cid = raw
        else:
            return await ctx.send("Use `6timer` or `6timer <channel_id>`.", delete_after=10)

    target_ids = [requested_cid] if requested_cid else list(ARCHIVE_TIMER_CHANNEL_LABELS.keys())

    lines.append("")
    lines.append("**6xs.lol archive nukes**")
    for cid in target_ids:
        label = ARCHIVE_TIMER_CHANNEL_LABELS.get(cid, f"#{cid[-6:]}")
        row = by_id.get(cid)
        if row and row.get("next_nuke_at"):
            try:
                ts = int(datetime.fromisoformat(str(row["next_nuke_at"]).replace("Z", "+00:00")).timestamp())
                rem = _fmt_remaining(ts - int(now))
                lines.append(f"- {label} (<#{cid}>): {rem} (at <t:{ts}:F>)")
                continue
            except Exception:
                pass
        interval = ARCHIVE_TIMER_CHANNEL_INTERVALS_SEC.get(cid, 24 * 60 * 60)
        lines.append(f"- {label} (<#{cid}>): schedule not found yet (interval {interval // 60} min).")

    await ctx.send("\n".join(lines)[:1990], allowed_mentions=discord.AllowedMentions.none())


def _is_auto_wipe_target_channel(message: discord.Message) -> bool:
    ch = message.channel
    if isinstance(ch, discord.Thread):
        return ch.parent_id == AUTO_WIPE_CHANNEL_ID
    return ch.id == AUTO_WIPE_CHANNEL_ID


def _attachment_is_video(att: discord.Attachment) -> bool:
    ct = (att.content_type or "").lower()
    if ct.startswith("video/"):
        return True
    fn = (att.filename or "").lower()
    return any(fn.endswith(suf) for suf in AUTO_WIPE_VIDEO_SUFFIXES)


async def maybe_mirror_auto_wipe_channel_video(message: discord.Message) -> bool:
    """
    In the hourly-wipe channel: re-upload video attachments as a bot message with
    "Post by @user". Text-only messages are left alone. Returns True if handled (original removed).
    """
    if message.author.bot or message.webhook_id or not message.guild:
        return False
    if not _is_auto_wipe_target_channel(message):
        return False

    video_atts = [a for a in message.attachments if _attachment_is_video(a)]
    if not video_atts:
        return False

    author = message.author
    lines: list[str] = []
    body = (message.content or "").strip()
    if body:
        lines.append(body)
    lines.append(f"Post by {author.mention}")
    caption = "\n\n".join(lines)

    try:
        # Include every attachment (images + etc.) so nothing is lost; Discord max 10 files
        atts = list(message.attachments)[:10]
        if len(message.attachments) > 10:
            print("[AUTO_WIPE_MIRROR] only first 10 attachments mirrored (message had more)")
        files = [await a.to_file() for a in atts]
        await message.channel.send(content=caption, files=files)
    except discord.HTTPException as e:
        print(f"[AUTO_WIPE_MIRROR] could not re-upload video: {e}")
        return False
    except Exception as e:
        print(f"[AUTO_WIPE_MIRROR] {e}")
        return False

    try:
        await message.delete()
    except (discord.Forbidden, discord.NotFound):
        pass
    return True


async def auto_check_pinterest():
    await bot.wait_until_ready()
    print(f"[AUTO PFP] Monitoring new Pinterest saves every {CHECK_INTERVAL // 60} minutes...")
    while not bot.is_closed():
        await check_new_pinterest_saves()
        await asyncio.sleep(CHECK_INTERVAL)

# ====================== REPOST (any member; no mod perms required) ======================
def _repost_strip_command_content(message: discord.Message) -> Optional[str]:
    """Remove `6` / `6 ` + `repost` + channel mentions from the command message body."""
    t = (message.content or "").strip()
    low = t.lower()
    p_space = f"{PREFIX.lower()} "
    if low.startswith(p_space):
        t = t[len(PREFIX) + 1 :].lstrip()
        low = t.lower()
    elif low.startswith(PREFIX.lower()):
        t = t[len(PREFIX) :].lstrip()
        low = t.lower()
    if low.startswith("repost"):
        t = t[6:].lstrip()
    for ch in message.channel_mentions:
        t = t.replace(f"<#{ch.id}>", " ")
        t = t.replace(f"<#!{ch.id}>", " ")
    t = " ".join(t.split())
    return t if t else None


def _repost_resolve_target(
    ctx: commands.Context, source_has_attachments: bool
) -> discord.abc.Messageable:
    """Default = current channel; if message mentions a text channel/thread, post there."""
    guild = ctx.guild
    assert guild is not None
    me = guild.me
    assert me is not None
    if ctx.message.channel_mentions:
        c = ctx.message.channel_mentions[-1]
        if isinstance(c, (discord.TextChannel, discord.Thread)):
            perms = c.permissions_for(me)
            if not perms.send_messages:
                raise ValueError("missing_send")
            if not perms.attach_files and source_has_attachments:
                raise ValueError("missing_attach")
            return c
        raise ValueError("bad_channel_type")
    return ctx.channel


@bot.command(name="repost")
async def repost(ctx):
    """Reply to a message, or attach media + text, optional `#channel` / channel mention to post elsewhere."""
    if not ctx.guild:
        return await ctx.send("Use `6repost` in a server.", delete_after=6)

    source_msg: Optional[discord.Message] = None
    if ctx.message.reference:
        try:
            source_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
        except discord.NotFound:
            return await ctx.send("❌ That message is gone.", delete_after=8)
    else:
        source_msg = ctx.message

    try:
        target = _repost_resolve_target(ctx, bool(source_msg.attachments))
    except ValueError as e:
        if str(e) == "bad_channel_type":
            return await ctx.send("❌ Mention a **text channel** or **thread** to post into.", delete_after=8)
        if str(e) == "missing_send":
            return await ctx.send("❌ I can't send messages in that channel.", delete_after=8)
        if str(e) == "missing_attach":
            return await ctx.send("❌ I need **Attach Files** in that channel for this repost.", delete_after=8)
        raise

    if source_msg.id == ctx.message.id:
        content = _repost_strip_command_content(ctx.message)
    else:
        content = source_msg.content or None

    files = [await a.to_file() for a in source_msg.attachments]
    embeds = list(source_msg.embeds)
    stickers = list(source_msg.stickers)

    if not content and not files and not embeds and not stickers:
        return await ctx.send(
            "❌ Nothing to repost. **Reply** to a message, or send **text / files** with "
            "`6repost` (optional `#channel` mention for another channel).",
            delete_after=12,
        )

    try:
        await target.send(
            content=content,
            files=files or None,
            embeds=embeds or None,
            stickers=stickers or None,
        )
    except discord.HTTPException:
        return await ctx.send("❌ Failed to repost (check file size / sticker rules / permissions).", delete_after=10)

    if source_msg.id != ctx.message.id:
        try:
            await source_msg.delete()
        except discord.Forbidden:
            pass
        except discord.NotFound:
            pass
    else:
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass


def _gif_pick_video_attachment(message: discord.Message) -> Optional[discord.Attachment]:
    for att in message.attachments:
        if _attachment_is_video(att):
            return att
    return None


def _gif_input_suffix(filename: str) -> str:
    suf = Path(filename or "").suffix.lower()
    if suf in AUTO_WIPE_VIDEO_SUFFIXES:
        return suf
    return ".mp4"


def _run_ffmpeg_gif(ffmpeg_bin: str, inp: Path, out: Path, max_sec: float, width: int, fps: int) -> tuple[int, str]:
    vf = (
        f"fps={fps},scale={width}:-1:flags=lanczos,"
        "split[s0][s1];[s0]palettegen=max_colors=128:stats_mode=single[p];[s1][p]paletteuse=dither=bayer:bayer_scale=3"
    )
    cmd = [
        ffmpeg_bin,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(inp),
        "-t",
        str(max_sec),
        "-vf",
        vf,
        "-loop",
        "0",
        str(out),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return -1, "ffmpeg timed out (video too long or heavy)"
    err = (r.stderr or r.stdout or "").strip()
    return r.returncode, err


@bot.command(name="gif")
async def cmd_gif(ctx):
    """Reply to a video or attach one: `6gif` → MP4/WebM/etc. → GIF (first ~18s, auto-shrinks to fit Discord)."""
    if not ctx.guild:
        return await ctx.send("Use `6gif` in a server.", delete_after=6)

    cand = os.path.expandvars(FFMPEG_BIN.strip() or "ffmpeg")
    if Path(cand).is_file():
        ffmpeg_exe = str(Path(cand).resolve())
    else:
        ffmpeg_exe = shutil.which(cand) or shutil.which("ffmpeg") or ""
    if not ffmpeg_exe:
        return await ctx.send(
            "❌ **ffmpeg** not found. Install it on the machine running the bot, or set **FFMPEG_PATH** in `.env` "
            "(full path to `ffmpeg.exe` on Windows).",
            delete_after=14,
        )

    source_msg: Optional[discord.Message] = None
    if ctx.message.reference:
        try:
            source_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
        except discord.NotFound:
            return await ctx.send("❌ That message is gone.", delete_after=8)
    else:
        source_msg = ctx.message

    att = _gif_pick_video_attachment(source_msg)
    if att is None:
        return await ctx.send(
            "❌ **Reply** to a message with a video, or attach a video with `6gif` / `6 gif`.",
            delete_after=10,
        )

    if att.size and att.size > GIF_MAX_SOURCE_BYTES:
        return await ctx.send(
            f"❌ Video is too large (max **{GIF_MAX_SOURCE_BYTES // (1024 * 1024)} MB** for this command).",
            delete_after=10,
        )

    suffix = _gif_input_suffix(att.filename or "")
    tmp_dir = Path(tempfile.mkdtemp(prefix="sixbot_gif_"))
    inp_path = tmp_dir / f"in{suffix}"
    out_path = tmp_dir / "out.gif"
    try:
        data = await att.read()
        inp_path.write_bytes(data)

        async with ctx.channel.typing():
            loop = asyncio.get_running_loop()
            last_err = ""
            for max_sec, width, fps in GIF_TRY_PRESETS:
                if out_path.exists():
                    out_path.unlink(missing_ok=True)
                code, last_err = await loop.run_in_executor(
                    None,
                    partial(_run_ffmpeg_gif, ffmpeg_exe, inp_path, out_path, max_sec, width, fps),
                )
                if code != 0:
                    await ctx.send(
                        f"❌ Could not convert (ffmpeg exit **{code}**).\n```{last_err[:900]}```",
                        delete_after=20,
                    )
                    return
                if not out_path.exists():
                    await ctx.send("❌ ffmpeg produced no output file.", delete_after=8)
                    return
                sz = out_path.stat().st_size
                if sz <= GIF_MAX_OUTPUT_BYTES:
                    gif_bytes = out_path.read_bytes()
                    await ctx.send(
                        file=discord.File(io.BytesIO(gif_bytes), filename="converted.gif"),
                    )
                    return

            await ctx.send(
                f"❌ GIF is still over **{GIF_MAX_OUTPUT_BYTES // (1024 * 1024)} MB** after shrinking. "
                "Try a shorter clip or smaller source video.",
                delete_after=12,
            )
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ====================== TWITTER / X → CHANNEL (6twt) ======================
_DISCORD_FILE_LIMIT = 25 * 1024 * 1024  # bytes (standard bot upload)
_MEDIA_SUFFIXES = frozenset({".mp4", ".webm", ".mkv", ".mov", ".m4v", ".jpg", ".jpeg", ".png", ".webp", ".gif"})


def _is_twitter_or_x_url(url: str) -> bool:
    try:
        h = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    if h.startswith("www."):
        h = h[4:]
    return h in ("x.com", "twitter.com", "mobile.twitter.com", "mobile.x.com") or h.endswith(
        ".twitter.com"
    )


def _strip_hashtags(text: str) -> str:
    if not text:
        return ""
    s = re.sub(r"#\S+", "", text)
    s = re.sub(r"[ \t]+", " ", s)
    s = "\n".join(line.rstrip() for line in s.splitlines())
    return s.strip()


def _strip_urls_from_text(text: str) -> str:
    """Remove http(s) links so the mirror is only plain caption (no tweet/t.co URLs)."""
    if not text:
        return ""
    s = re.sub(r"https?://\S+", "", text, flags=re.I)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = "\n".join(line.strip() for line in s.splitlines())
    return s.strip()


def _parse_browser_cookie_pairs(header: str) -> list[tuple[str, str]]:
    """Parse `a=b; c=d` cookie header from DevTools / document.cookie."""
    pairs: list[tuple[str, str]] = []
    for segment in re.split(r"\s*;\s*", header.strip()):
        if not segment or "=" not in segment:
            continue
        name, _, value = segment.partition("=")
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        value = value.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        if name:
            pairs.append((name, value))
    return pairs


def _browser_cookies_to_netscape(header: str) -> str:
    """Netscape cookies.txt for yt-dlp (X hits both .x.com and .twitter.com APIs)."""
    pairs = _parse_browser_cookie_pairs(header)
    if not pairs:
        return ""
    expire = int(time.time()) + 86400 * 365 * 3
    lines = ["# Netscape HTTP Cookie File", "# yt-dlp twitter/x"]
    for domain, sub in ((".x.com", "TRUE"), (".twitter.com", "TRUE")):
        for name, value in pairs:
            lines.append(f"{domain}\t{sub}\t/\tTRUE\t{expire}\t{name}\t{value}")
    return "\n".join(lines) + "\n"


def _normalize_twitter_cookies_raw(raw: str) -> str:
    """
    Full cookie header: pass through.
    Lone hex token (common mistake): treat as Twitter **ct0** CSRF cookie.
    """
    raw = raw.strip()
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()
    raw = _clean_command_argument(raw)
    if not raw:
        return raw
    if "=" not in raw and re.fullmatch(r"[a-fA-F0-9]{16,128}", raw):
        return f"ct0={raw}"
    return raw


def _twitter_cookiefile_path(outdir: Path) -> Optional[str]:
    """Path to cookies: file from env, or Netscape file built from TWITTER_COOKIES / X_COOKIES string."""
    for env_key in ("TWITTER_COOKIES_FILE", "X_COOKIES_FILE"):
        cf = os.getenv(env_key)
        if cf and Path(cf).expanduser().is_file():
            return str(Path(cf).expanduser())
    raw = _normalize_twitter_cookies_raw(os.getenv("TWITTER_COOKIES") or os.getenv("X_COOKIES") or "")
    if not raw:
        return None
    netscape = _browser_cookies_to_netscape(raw)
    if not netscape.strip():
        return None
    path = outdir / "ytdlp_twitter_cookies.txt"
    path.write_text(netscape, encoding="utf-8")
    return str(path)


def _twitter_cookie_header_raw() -> str:
    return _normalize_twitter_cookies_raw(os.getenv("TWITTER_COOKIES") or os.getenv("X_COOKIES") or "")


_YTDLP_SKIP_COOKIE_FILES = frozenset({"ytdlp_twitter_cookies.txt", "ytdlp_tiktok_cookies.txt"})


def _gather_media_files(outdir: Path, before: set) -> list[Path]:
    new_files: list[Path] = []
    for p in outdir.iterdir():
        if not p.is_file() or p in before:
            continue
        if p.name in _YTDLP_SKIP_COOKIE_FILES:
            continue
        if p.suffix.lower() not in _MEDIA_SUFFIXES:
            continue
        if p.stat().st_size <= 0:
            continue
        new_files.append(p)
    new_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return new_files


def _decode_embed_url(url: str) -> str:
    u = html_stdlib.unescape(url.strip())
    u = u.replace("\\/", "/").replace(r"\u002F", "/").replace(r"\u0026", "&")
    return u


# Same kinds of clients X often serves Open Graph / mp4 links to (Discord preview uses this pattern).
_OG_FETCH_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (compatible; Discordbot/2.0; +https://discordapp.com)",
    "facebookexternalhit/1.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)


def _http_fetch(url: str, cookie_header: str) -> bytes:
    last_err: Optional[Exception] = None
    for ua in _OG_FETCH_USER_AGENTS:
        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if cookie_header:
            headers["Cookie"] = cookie_header
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=35) as resp:
                return resp.read()
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not fetch page: {last_err}")


def _twitter_extract_og_caption(html: str) -> str:
    for pat in (
        r'<meta\s+property=["\']og:description["\']\s+content=["\']([^"\']*)["\']',
        r'<meta\s+content=["\']([^"\']*)["\']\s+property=["\']og:description["\']',
        r'<meta\s+name=["\']twitter:description["\']\s+content=["\']([^"\']*)["\']',
        r'<meta\s+content=["\']([^"\']*)["\']\s+name=["\']twitter:description["\']',
    ):
        m = re.search(pat, html, re.I | re.DOTALL)
        if m:
            return html_stdlib.unescape(m.group(1)).strip()
    return ""


def _twitter_collect_mp4_urls_from_html(html: str) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    def add(u: str) -> None:
        u = _decode_embed_url(u)
        if u.startswith("https://video.twimg.com") and u not in seen:
            seen.add(u)
            found.append(u)

    for pat in (
        r'<meta\s+property=["\']og:video(?::url)?["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:video(?::url)?["\']',
        r'<meta\s+name=["\']twitter:player:stream["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+name=["\']twitter:player:stream["\']',
    ):
        for m in re.finditer(pat, html, re.I):
            add(m.group(1))

    for m in re.finditer(
        r"https://video\.twimg\.com/[^\"'\\s<>]+?\.mp4(?:\?[^\"'\\s<>]*)?",
        html,
        re.I,
    ):
        add(m.group(0))

    return found


def _pick_best_tw_mp4(urls: list[str]) -> Optional[str]:
    if not urls:
        return None

    def score(u: str) -> tuple[int, int]:
        s = 0
        if "ext_tw_video" in u:
            s += 50
        if "/mp4/" in u or "/vid/" in u:
            s += 20
        if "1280x720" in u or "720x1280" in u:
            s += 15
        return (s, len(u))

    return max(urls, key=score)


def _twitter_open_graph_video(url: str, cookie_header: str) -> tuple[Optional[str], str]:
    """Get direct MP4 URL + caption the way Discord link previews do (OG / twimg in HTML)."""
    html = _http_fetch(url, cookie_header).decode("utf-8", errors="replace")
    cap = _twitter_extract_og_caption(html)
    mp4s = _twitter_collect_mp4_urls_from_html(html)
    best = _pick_best_tw_mp4(mp4s)
    return best, cap


def _http_download_file(url: str, dest: Path, cookie_header: str) -> None:
    headers = {
        "User-Agent": _OG_FETCH_USER_AGENTS[-1],
        "Accept": "*/*",
    }
    if cookie_header:
        headers["Cookie"] = cookie_header
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=180) as resp:
        dest.write_bytes(resp.read())


def _twitter_download_sync(url: str, outdir: Path) -> tuple[list[Path], str]:
    """Download tweet media: yt-dlp first, then Open Graph / video.twimg.com (Discord-style embed)."""
    if yt_dlp is None:
        raise RuntimeError("yt-dlp is not installed (pip install yt-dlp)")
    outdir.mkdir(parents=True, exist_ok=True)
    before = {p for p in outdir.iterdir() if p.is_file()}
    cookiefile = _twitter_cookiefile_path(outdir)
    raw_cookie = _twitter_cookie_header_raw()

    format_attempts: tuple[tuple[str, Optional[str]], ...] = (
        ("bestvideo+bestaudio/best", "mp4"),
        ("bestvideo+bestaudio/best", None),
        ("best", "mp4"),
        ("best", None),
    )

    caption = ""
    info = None
    last_err: Optional[Exception] = None

    for fmt, merge_fmt in format_attempts:
        opts: dict = {
            "outtmpl": str(outdir / "twt_%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "format": fmt,
            "retries": 3,
            "fragment_retries": 3,
            "socket_timeout": 30,
        }
        if merge_fmt:
            opts["merge_output_format"] = merge_fmt
        if cookiefile:
            opts["cookiefile"] = cookiefile

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
            if info:
                break
        except Exception as e:
            last_err = e
            info = None
            continue

    if info:
        caption = (info.get("description") or info.get("title") or "").strip()

    new_files = _gather_media_files(outdir, before)
    if new_files:
        return new_files, caption

    # Discord embeds use the same public HTML: og:video + video.twimg.com MP4s. yt-dlp often misses these.
    og_err: Optional[Exception] = None
    try:
        mp4_u, cap_og = _twitter_open_graph_video(url, raw_cookie)
        if cap_og:
            caption = caption or cap_og
        if mp4_u:
            dest = outdir / "twt_og_embed.mp4"
            _http_download_file(mp4_u, dest, raw_cookie)
            if dest.exists() and dest.stat().st_size > 0:
                return [dest], caption
    except Exception as e:
        og_err = e

    parts = [str(last_err) if last_err else "", str(og_err) if og_err else ""]
    msg = " | ".join(p for p in parts if p) or "No video could be found in this tweet"
    hint = ""
    if not cookiefile and not raw_cookie:
        hint = (
            " Tip: set **TWITTER_COOKIES** in `.env` if this post needs a login to view."
        )
    raise RuntimeError(f"{msg}.{hint}")


# --- TikTok (yt-dlp) ---
def _is_tiktok_url(url: str) -> bool:
    try:
        h = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    if h.startswith("www."):
        h = h[4:]
    return h == "tiktok.com" or h.endswith(".tiktok.com")


def _tiktok_cookiefile_path(outdir: Path) -> Optional[str]:
    for env_key in ("TIKTOK_COOKIES_FILE",):
        cf = os.getenv(env_key)
        if cf and Path(cf).expanduser().is_file():
            return str(Path(cf).expanduser())
    raw = (os.getenv("TIKTOK_COOKIES") or "").strip()
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1].strip()
    raw = _clean_command_argument(raw)
    if not raw:
        return None
    pairs = _parse_browser_cookie_pairs(raw)
    if not pairs:
        return None
    expire = int(time.time()) + 86400 * 365 * 3
    lines = ["# Netscape HTTP Cookie File", "# yt-dlp tiktok"]
    for dom in (".tiktok.com",):
        for name, value in pairs:
            lines.append(f"{dom}\tTRUE\t/\tTRUE\t{expire}\t{name}\t{value}")
    path = outdir / "ytdlp_tiktok_cookies.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def _tiktok_download_sync(url: str, outdir: Path) -> tuple[list[Path], str]:
    """Download TikTok with yt-dlp; return media paths and description/title."""
    if yt_dlp is None:
        raise RuntimeError("yt-dlp is not installed (pip install yt-dlp)")
    outdir.mkdir(parents=True, exist_ok=True)
    before = {p for p in outdir.iterdir() if p.is_file()}
    cookiefile = _tiktok_cookiefile_path(outdir)

    format_attempts: tuple[tuple[str, Optional[str]], ...] = (
        ("bestvideo+bestaudio/best", "mp4"),
        ("bestvideo+bestaudio/best", None),
        ("best", "mp4"),
        ("best", None),
    )

    info = None
    last_err: Optional[Exception] = None

    for fmt, merge_fmt in format_attempts:
        opts: dict = {
            "outtmpl": str(outdir / "tt_%(id)s.%(ext)s"),
            "quiet": True,
            "no_warnings": True,
            "format": fmt,
            "retries": 3,
            "fragment_retries": 3,
            "socket_timeout": 45,
        }
        if merge_fmt:
            opts["merge_output_format"] = merge_fmt
        if cookiefile:
            opts["cookiefile"] = cookiefile

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
            if info:
                break
        except Exception as e:
            last_err = e
            info = None
            continue

    if not info:
        msg = str(last_err) if last_err else "Download failed"
        hint = ""
        if not cookiefile:
            hint = " Try **TIKTOK_COOKIES** in `.env` (browser cookie string) or **TIKTOK_COOKIES_FILE** if TikTok blocks the bot."
        raise RuntimeError(f"{msg}.{hint}")

    caption = (info.get("description") or info.get("title") or info.get("track") or "").strip()

    new_files = _gather_media_files(outdir, before)
    return new_files, caption


class TwitterStatusURL(commands.Converter):
    async def convert(self, ctx: commands.Context, argument: str) -> str:
        u = _clean_command_argument(argument).strip("<>")
        if not _is_twitter_or_x_url(u):
            raise commands.BadArgument("Use a **twitter.com** or **x.com** status link.")
        return u


class TikTokURL(commands.Converter):
    async def convert(self, ctx: commands.Context, argument: str) -> str:
        u = _clean_command_argument(argument).strip("<>")
        if not _is_tiktok_url(u):
            raise commands.BadArgument("Use a **tiktok.com** link (including **vm.tiktok.com** / **vt.tiktok.com**).")
        return u


class CleanTextChannel(commands.Converter):
    """Like TextChannelConverter but strips invisible Unicode (fixes broken #channel mentions)."""

    async def convert(self, ctx: commands.Context, argument: str) -> discord.TextChannel:
        if not ctx.guild:
            raise commands.ChannelNotFound(argument)
        cleaned = _clean_command_argument(argument)
        if not cleaned:
            raise commands.ChannelNotFound(argument)
        if cleaned.isdigit():
            ch = ctx.guild.get_channel(int(cleaned))
            if isinstance(ch, discord.TextChannel):
                return ch
            raise commands.ChannelNotFound(argument)
        return await commands.TextChannelConverter().convert(ctx, cleaned)


def _can_mirror_tweet(member: discord.Member) -> bool:
    return bool(
        is_trusted(member)
        or member.guild_permissions.administrator
        or member.guild_permissions.manage_guild
        or member.guild_permissions.manage_messages
    )


@bot.command(name="twt")
async def twt_cmd(
    ctx: commands.Context,
    url: TwitterStatusURL,
    channel: CleanTextChannel,
):
    """Post an X/Twitter video (and caption, no hashtags) to a channel. Usage: `6twt <url> #channel`"""
    if not ctx.guild:
        return await ctx.send("Use `6twt` in a server.", delete_after=8)
    assert isinstance(ctx.author, discord.Member)
    if not _can_mirror_tweet(ctx.author):
        return await ctx.send(
            "You need **Manage Messages**, **Manage Server**, **Administrator**, or be a trusted user.",
            delete_after=10,
        )
    me = ctx.guild.me
    assert me is not None
    perms = channel.permissions_for(me)
    if not (perms.send_messages and perms.attach_files):
        return await ctx.send(
            f"I can't send files in {channel.mention} — need **Send Messages** and **Attach Files**.",
            delete_after=12,
        )

    if yt_dlp is None:
        return await ctx.send(
            "Missing dependency: install **yt-dlp** on the host (`pip install yt-dlp`) and restart the bot.",
            delete_after=15,
        )

    await ctx.send(f"Fetching… → {channel.mention}", delete_after=25)

    tmp = Path(tempfile.mkdtemp(prefix="sixbot_twt_"))
    try:
        async with ctx.channel.typing():
            paths, raw_cap = await asyncio.to_thread(_twitter_download_sync, url, tmp)
        caption = _strip_urls_from_text(_strip_hashtags(raw_cap))[:2000] if raw_cap else ""

        fits: list[Path] = []
        too_big: list[Path] = []
        for p in paths[:10]:
            sz = p.stat().st_size
            if sz <= _DISCORD_FILE_LIMIT:
                fits.append(p)
            else:
                too_big.append(p)

        if not fits and not caption and not too_big:
            await ctx.send("❌ No usable media or text from that post.", delete_after=15)
            return

        if not fits:
            if not caption:
                await ctx.send("❌ Nothing to post (media too large for Discord and no caption).", delete_after=12)
                return
            await channel.send(
                caption[:2000],
                allowed_mentions=discord.AllowedMentions.none(),
            )
            if too_big:
                await ctx.send(
                    f"Posted caption only in {channel.mention} — file(s) over {_DISCORD_FILE_LIMIT // (1024 * 1024)}MB (not sent).",
                    delete_after=15,
                )
            else:
                await ctx.send(f"Posted caption only in {channel.mention}.", delete_after=12)
            return

        file_handles: list[object] = []
        discord_files: list[discord.File] = []
        try:
            for i, p in enumerate(fits):
                ext = p.suffix.lower() or ".bin"
                fh = open(p, "rb")
                file_handles.append(fh)
                discord_files.append(
                    discord.File(fh, filename=f"twitter_{i + 1}{ext}")
                )
            await channel.send(
                content=caption[:2000] if caption else None,
                files=discord_files,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        finally:
            for h in file_handles:
                try:
                    h.close()
                except OSError:
                    pass

        if too_big:
            await ctx.send(
                f"Posted to {channel.mention}. Some attachment(s) skipped (over {_DISCORD_FILE_LIMIT // (1024 * 1024)}MB).",
                delete_after=15,
            )
    except Exception as e:
        await ctx.send(f"❌ Failed: `{e}`", delete_after=20)
        print(f"[6twt] {e}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@bot.command(name="tt")
async def tt_cmd(
    ctx: commands.Context,
    url: TikTokURL,
    channel: CleanTextChannel,
):
    """Post a TikTok video (caption only, no hashtags or links) to a channel. Usage: `6tt <url> #channel`"""
    if not ctx.guild:
        return await ctx.send("Use `6tt` in a server.", delete_after=8)
    assert isinstance(ctx.author, discord.Member)
    if not _can_mirror_tweet(ctx.author):
        return await ctx.send(
            "You need **Manage Messages**, **Manage Server**, **Administrator**, or be a trusted user.",
            delete_after=10,
        )
    me = ctx.guild.me
    assert me is not None
    perms = channel.permissions_for(me)
    if not (perms.send_messages and perms.attach_files):
        return await ctx.send(
            f"I can't send files in {channel.mention} — need **Send Messages** and **Attach Files**.",
            delete_after=12,
        )

    if yt_dlp is None:
        return await ctx.send(
            "Missing dependency: install **yt-dlp** on the host (`pip install yt-dlp`) and restart the bot.",
            delete_after=15,
        )

    await ctx.send(f"Fetching TikTok… → {channel.mention}", delete_after=25)

    tmp = Path(tempfile.mkdtemp(prefix="sixbot_tt_"))
    try:
        async with ctx.channel.typing():
            paths, raw_cap = await asyncio.to_thread(_tiktok_download_sync, url, tmp)
        caption = _strip_urls_from_text(_strip_hashtags(raw_cap))[:2000] if raw_cap else ""

        fits: list[Path] = []
        too_big: list[Path] = []
        for p in paths[:10]:
            sz = p.stat().st_size
            if sz <= _DISCORD_FILE_LIMIT:
                fits.append(p)
            else:
                too_big.append(p)

        if not fits and not caption and not too_big:
            await ctx.send("❌ No usable media or text from that TikTok.", delete_after=15)
            return

        if not fits:
            if not caption:
                await ctx.send("❌ Nothing to post (media too large for Discord and no caption).", delete_after=12)
                return
            await channel.send(
                caption[:2000],
                allowed_mentions=discord.AllowedMentions.none(),
            )
            if too_big:
                await ctx.send(
                    f"Posted caption only in {channel.mention} — file(s) over {_DISCORD_FILE_LIMIT // (1024 * 1024)}MB (not sent).",
                    delete_after=15,
                )
            else:
                await ctx.send(f"Posted caption only in {channel.mention}.", delete_after=12)
            return

        file_handles: list[object] = []
        discord_files: list[discord.File] = []
        try:
            for i, p in enumerate(fits):
                ext = p.suffix.lower() or ".bin"
                fh = open(p, "rb")
                file_handles.append(fh)
                discord_files.append(
                    discord.File(fh, filename=f"tiktok_{i + 1}{ext}")
                )
            await channel.send(
                content=caption[:2000] if caption else None,
                files=discord_files,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        finally:
            for h in file_handles:
                try:
                    h.close()
                except OSError:
                    pass

        if too_big:
            await ctx.send(
                f"Posted to {channel.mention}. Some attachment(s) skipped (over {_DISCORD_FILE_LIMIT // (1024 * 1024)}MB).",
                delete_after=15,
            )
    except Exception as e:
        await ctx.send(f"❌ Failed: `{e}`", delete_after=20)
        print(f"[6tt] {e}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


_DISCORD_MESSAGE_LINK_RE = re.compile(
    r"https?://(?:ptb\.|canary\.)?discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)",
    re.I,
)


def _strip_send_command_prefix(content: str) -> str:
    s = content.strip()
    low = s.lower()
    for p in (f"{PREFIX} send ", f"{PREFIX}send "):
        if low.startswith(p):
            return s[len(p) :].strip()
    if low.startswith(f"{PREFIX}send"):
        return s[len(f"{PREFIX}send") :].strip()
    return s


def _parse_message_links_from_text(text: str) -> list[tuple[int, int, int]]:
    """(guild_id, channel_id, message_id) for each Discord message URL in order."""
    out: list[tuple[int, int, int]] = []
    for m in _DISCORD_MESSAGE_LINK_RE.finditer(text):
        out.append((int(m.group(1)), int(m.group(2)), int(m.group(3))))
    return out


def _embed_media_urls(message: discord.Message) -> list[str]:
    """Direct image/video URLs from link unfurls (Tenor, Giphy, video sites, etc.)."""
    seen: set[str] = set()
    out: list[str] = []
    for em in message.embeds:
        for u in (
            em.image.url if em.image else None,
            em.thumbnail.url if em.thumbnail else None,
            em.video.url if em.video and em.video.url else None,
        ):
            if u and u not in seen:
                seen.add(u)
                out.append(u)
    return out


def _send_media_filename_from_url(url: str, index: int) -> str:
    try:
        path = (urlparse(url).path or "").split("/")[-1]
        if path and "." in path and len(path) < 120:
            return path
    except Exception:
        pass
    return f"media_{index}.bin"


async def _send_url_as_discord_file(
    session: aiohttp.ClientSession, url: str, index: int
) -> Optional[discord.File]:
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=60),
            headers={"User-Agent": "SixBot/1.0 (Discord; +https://discord.com)"},
        ) as resp:
            if resp.status != 200:
                return None
            ct = (resp.headers.get("Content-Type") or "").lower()
            if not any(
                x in ct
                for x in ("image/", "video/", "application/octet-stream", "gif", "webp")
            ):
                return None
            data = await resp.read()
            if len(data) > _DISCORD_FILE_LIMIT:
                return None
            return discord.File(io.BytesIO(data), filename=_send_media_filename_from_url(url, index))
    except Exception as e:
        print(f"[6send] fetch {url[:80]!r}: {e}")
        return None


async def _send_collect_files_from_message(
    ref_msg: discord.Message,
    session: aiohttp.ClientSession,
    files_out: list[discord.File],
    max_total: int,
    stats: dict[str, int],
) -> None:
    """Append up to max_total files total into files_out; updates stats keys."""
    for att in ref_msg.attachments:
        if len(files_out) >= max_total:
            stats["skipped_trunc"] += 1
            continue
        if att.size > _DISCORD_FILE_LIMIT:
            stats["skipped_big"] += 1
            continue
        try:
            data = await att.read()
        except discord.HTTPException:
            stats["skipped_big"] += 1
            continue
        name = att.filename or "attachment"
        files_out.append(discord.File(io.BytesIO(data), filename=name[:120]))

    url_list = _embed_media_urls(ref_msg)
    for i, u in enumerate(url_list):
        if len(files_out) >= max_total:
            stats["skipped_trunc"] += 1
            break
        stats["url_attempts"] += 1
        f = await _send_url_as_discord_file(session, u, i + ref_msg.id)
        if f:
            files_out.append(f)
        else:
            stats["skipped_url"] += 1


def _send_can_delete_source(
    message: discord.Message, actor: discord.Member, bot_member: discord.Member
) -> bool:
    ch = message.channel
    try:
        if not ch.permissions_for(bot_member).manage_messages:
            return False
    except Exception:
        return False
    if message.author.id == actor.id:
        return True
    return bool(actor.guild_permissions.manage_messages)


@bot.command(name="send")
async def cmd_send(ctx: commands.Context, *, _rest: str = "") -> None:
    """
    Move media to another channel with profile + From/To links.

    • **`6send #channel`** — reply to the message to forward.
    • **`6send <msg url> <msg url> #channel`** — same channel, all media between those two
      messages (inclusive), order by link order in your message.
    """
    del _rest  # consume rest; we parse `ctx.message.content` for URLs + channel
    if not ctx.guild:
        return await ctx.send("Use **`6send`** in a server.", delete_after=8)
    assert isinstance(ctx.author, discord.Member)
    if not _can_mirror_tweet(ctx.author):
        return await ctx.send(
            "You need **Manage Messages**, **Manage Server**, **Administrator**, or be a trusted user.",
            delete_after=10,
        )

    me = ctx.guild.me
    assert me is not None

    body = _strip_send_command_prefix(ctx.message.content)
    links = _parse_message_links_from_text(body)
    channel_token = _DISCORD_MESSAGE_LINK_RE.sub("", body).strip()
    if not channel_token:
        return await ctx.send(
            "Add the **target channel**: **`6send #channel`** (reply to media), or paste **two message links** "
            "then **`#channel`** for a range.",
            delete_after=22,
        )

    try:
        channel = await CleanTextChannel().convert(ctx, channel_token)
    except commands.BadArgument:
        return await ctx.send(
            "Could not read the **destination** channel. End your command with **`#channel`** or the channel ID.",
            delete_after=18,
        )

    dest_perms = channel.permissions_for(me)
    if not (dest_perms.send_messages and dest_perms.embed_links and dest_perms.attach_files):
        return await ctx.send(
            f"I need **Send Messages**, **Embed Links**, and **Attach Files** in {channel.mention}.",
            delete_after=14,
        )

    source_messages: list[discord.Message] = []

    if len(links) >= 2:
        g0, c0, id0 = links[0]
        g1, c1, id1 = links[1]
        if g0 != ctx.guild.id or g1 != ctx.guild.id:
            return await ctx.send("Both message links must be in **this server**.", delete_after=12)
        if c0 != c1:
            return await ctx.send("Both links must be in the **same channel**.", delete_after=12)
        src_ch = bot.get_channel(c0)
        if not isinstance(src_ch, (discord.TextChannel, discord.Thread)):
            return await ctx.send("Source must be a **text channel or thread** I can read.", delete_after=12)
        src_perms = src_ch.permissions_for(me)
        if not src_perms.read_message_history:
            return await ctx.send("I need **Read Message History** in the source channel.", delete_after=12)

        if id0 > id1:
            id0, id1 = id1, id0
        try:
            older = await src_ch.fetch_message(id0)
            newer = await src_ch.fetch_message(id1)
        except (discord.NotFound, discord.HTTPException):
            return await ctx.send("Could not load one of those messages (deleted or no access).", delete_after=12)

        if id0 == id1:
            source_messages = [older]
        else:
            source_messages = [older]
            async for m in src_ch.history(limit=200, after=older, oldest_first=True):
                source_messages.append(m)
                if m.id >= newer.id:
                    break
            if source_messages[-1].id != newer.id:
                return await ctx.send(
                    "Could not walk the full range (too many messages in between?). Try a smaller span.",
                    delete_after=15,
                )
    elif len(links) == 1:
        g0, c0, mid = links[0]
        if g0 != ctx.guild.id:
            return await ctx.send("That message link is not from **this server**.", delete_after=10)
        src_ch = bot.get_channel(c0)
        if not isinstance(src_ch, (discord.TextChannel, discord.Thread)):
            return await ctx.send("Source must be a **text channel or thread**.", delete_after=10)
        if not src_ch.permissions_for(me).read_message_history:
            return await ctx.send("I need **Read Message History** in the source channel.", delete_after=12)
        try:
            source_messages.append(await src_ch.fetch_message(mid))
        except (discord.NotFound, discord.HTTPException):
            return await ctx.send("Could not load that message.", delete_after=10)
    else:
        ref = ctx.message.reference
        if ref is None or ref.message_id is None:
            return await ctx.send(
                "**Reply** to a message with media and run **`6send #channel`**, or paste **two Discord message URLs** "
                "then **`#channel`** for everything between them.",
                delete_after=24,
            )
        src = bot.get_channel(ref.channel_id) if ref.channel_id else ctx.channel
        if not isinstance(src, discord.abc.Messageable):
            return await ctx.send("Could not open the source channel.", delete_after=10)
        ref_msg = ref.resolved if isinstance(ref.resolved, discord.Message) else None
        if ref_msg is None:
            try:
                ref_msg = await src.fetch_message(ref.message_id)
            except (discord.NotFound, discord.HTTPException):
                return await ctx.send("That message is gone or I can't read it.", delete_after=10)
        source_messages.append(ref_msg)

    stats = {"skipped_big": 0, "skipped_trunc": 0, "skipped_url": 0, "url_attempts": 0}
    all_files: list[discord.File] = []
    MAX_FILES = 40
    async with aiohttp.ClientSession() as session:
        for msg in source_messages:
            await _send_collect_files_from_message(msg, session, all_files, MAX_FILES, stats)

    if not all_files:
        hint: list[str] = []
        if stats["skipped_big"]:
            hint.append(f"**{stats['skipped_big']}** over {_DISCORD_FILE_LIMIT // (1024 * 1024)}MB")
        if stats["skipped_url"] and stats["url_attempts"]:
            hint.append("embed URLs not direct files")
        if not hint:
            hint.append("no attachments or previews in that range")
        return await ctx.send(f"No media to forward ({' · '.join(hint)}).", delete_after=20)

    primary_author = source_messages[0].author

    batches: list[list[discord.File]] = []
    cur: list[discord.File] = []
    for f in all_files:
        if len(cur) >= 10:
            batches.append(cur)
            cur = []
        cur.append(f)
    if cur:
        batches.append(cur)

    try:
        for i, batch in enumerate(batches):
            if i == 0:
                if len(source_messages) > 1:
                    desc_from = (
                        f"**From** [first message]({source_messages[0].jump_url}) → "
                        f"[last message]({source_messages[-1].jump_url})\n**To** _posting…_"
                    )
                else:
                    desc_from = f"**From** [message]({source_messages[0].jump_url})\n**To** _posting…_"
                em = discord.Embed(title="Forwarded media", description=desc_from, color=discord.Color.blurple())
                try:
                    au = primary_author.display_avatar.url
                    em.set_author(name=str(primary_author.display_name), icon_url=au)
                    em.set_thumbnail(url=au)
                except Exception:
                    em.set_author(name=str(primary_author.display_name))
                em.set_footer(
                    text=f"Forwarded by {ctx.author.display_name}",
                    icon_url=ctx.author.display_avatar.url,
                )
                if len(source_messages) > 1:
                    em.add_field(
                        name="Sources",
                        value=f"**{len(source_messages)}** messages · original authors may vary",
                        inline=False,
                    )
                msg = await channel.send(embed=em, files=batch)
                to_url = msg.jump_url
                if len(source_messages) > 1:
                    desc_done = (
                        f"**From** [first message]({source_messages[0].jump_url}) → "
                        f"[last message]({source_messages[-1].jump_url})\n**To** [message]({to_url})"
                    )
                else:
                    desc_done = (
                        f"**From** [message]({source_messages[0].jump_url})\n**To** [message]({to_url})"
                    )
                em_done = discord.Embed(title="Forwarded media", description=desc_done, color=discord.Color.blurple())
                try:
                    au = primary_author.display_avatar.url
                    em_done.set_author(name=str(primary_author.display_name), icon_url=au)
                    em_done.set_thumbnail(url=au)
                except Exception:
                    em_done.set_author(name=str(primary_author.display_name))
                em_done.set_footer(
                    text=f"Forwarded by {ctx.author.display_name}",
                    icon_url=ctx.author.display_avatar.url,
                )
                if len(source_messages) > 1:
                    em_done.add_field(
                        name="Sources",
                        value=f"**{len(source_messages)}** messages · original authors may vary",
                        inline=False,
                    )
                await msg.edit(embed=em_done)
            else:
                await channel.send(
                    content=f"_(continued {i + 1}/{len(batches)})_",
                    files=batch,
                )
    except discord.HTTPException as e:
        await ctx.send(f"Couldn't post in {channel.mention}: `{e}`", delete_after=15)
        print(f"[6send] {e}")
        return

    deleted = 0
    for sm in source_messages:
        if _send_can_delete_source(sm, ctx.author, me):
            try:
                await sm.delete()
                deleted += 1
            except discord.HTTPException as e:
                print(f"[6send] delete {sm.id}: {e}")

    note = f"Posted **{len(all_files)}** file(s) → {channel.mention}."
    if deleted:
        note += f" Deleted **{deleted}** source message(s)."
    elif source_messages and me.guild_permissions.manage_messages:
        note += " Source not deleted (need **Manage Messages**, and you must be the author or a mod)."
    if stats["skipped_big"]:
        note += f" Skipped **{stats['skipped_big']}** oversized."
    if stats["skipped_trunc"]:
        note += f" Skipped **{stats['skipped_trunc']}** (cap **{MAX_FILES}** files / **10** per message)."
    if stats["skipped_url"] and stats["url_attempts"]:
        note += f" Skipped **{stats['skipped_url']}** bad embed URL(s)."
    await ctx.send(note, delete_after=25)


# ====================== RESPOND (Grok roast; any member) ======================
@bot.command(name="respond")
async def respond_cmd(ctx, member: Optional[discord.Member] = None):
    """Roast someone (**7** fresh + **3** archived lines); ~24% of the time uses extra-heinous CHAOS mode."""
    if not ctx.guild:
        return await ctx.send("Use `6respond` in a server.", delete_after=6)
    target: Optional[discord.User] = member
    if target is None and ctx.message.reference:
        try:
            ref = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            if ref.author and not ref.author.bot:
                target = ref.author
        except discord.NotFound:
            pass
    if target is None or getattr(target, "bot", False):
        return await ctx.send(
            "Reply to a message with `6respond`, or run `6respond @user`.",
            delete_after=12,
        )
    if not isinstance(target, discord.Member):
        target = ctx.guild.get_member(target.id) if ctx.guild else None
        if target is None:
            return await ctx.send("Could not resolve that member in this server.", delete_after=8)

    n_new = RESPOND_NEW_MESSAGE_COUNT
    n_old = RESPOND_OLD_MESSAGE_COUNT
    batch: list[tuple[discord.Message, str]] = []
    async for msg in ctx.channel.history(limit=100):
        if msg.id == ctx.message.id:
            continue
        if msg.author.bot:
            continue
        text = (msg.content or "").strip()
        if not text:
            if msg.attachments:
                text = "[attachment]"
            elif msg.embeds:
                text = "[embed]"
            else:
                continue
        batch.append((msg, f"{msg.author.display_name}: {text[:900]}"))
        if len(batch) >= n_new:
            break
    batch.reverse()
    if len(batch) < n_new:
        return await ctx.send(
            f"Need at least **{n_new}** recent non-bot messages in this channel (have {len(batch)}).",
            delete_after=12,
        )

    new_ids = {m.id for m, _ in batch}
    oldest_new_ts = min(m.created_at.timestamp() for m, _ in batch)
    new_lines = [ln for _, ln in batch]

    old_lines = await asyncio.to_thread(
        _respond_pick_old_context_lines,
        ctx.guild.id,
        ctx.channel.id,
        new_ids,
        oldest_new_ts,
        n_old,
    )

    user_prompt = (
        f"target to roast (display name): {target.display_name}\n\n"
        f"**Recent** ({len(new_lines)} lines, oldest → newest):\n"
        + "\n".join(f"{i + 1}. {ln}" for i, ln in enumerate(new_lines))
    )
    if old_lines:
        user_prompt += (
            f"\n\n**Older** ({len(old_lines)} archived lines from this channel — may be days old):\n"
            + "\n".join(f"{i + 1}. {ln}" for i, ln in enumerate(old_lines))
        )
    chaos = random.random() < RESPOND_CHAOS_CHANCE
    if chaos:
        user_prompt += (
            "\n\nCHAOS MODE: one sentence that **starts shit**—bait replies, imply deranged backstory, "
            "or call the whole chat out so people pile on **the target only** (not random members). "
            "still exactly one lowercase sentence, no newline."
        )
    else:
        user_prompt += (
            "\n\nroast only the target using this as ammo; you may reference an **older** line for a callback if it fits. "
            "exactly one lowercase sentence and nothing else—no quotes no newline."
        )

    async with ctx.channel.typing():
        try:
            raw = await grok_respond_roast(user_prompt, chaos=chaos)
        except Exception as e:
            return await ctx.send(f"Grok error: `{e}`", delete_after=15)

    out = format_roast_line(raw)
    await ctx.send(out or "lol")


# ====================== TOPIC (manual dead-chat style line, no idle wait) ======================
@bot.command(name="topic")
async def topic_cmd(ctx):
    """Same vibe as auto dead-chat revival, on demand (`6topic`)."""
    if not ctx.guild:
        return await ctx.send("Use **`6topic`** in a server.", delete_after=8)
    ch = ctx.channel
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return await ctx.send("Use **`6topic`** in a text channel or thread.", delete_after=10)
    if not _grok_api_key():
        return await ctx.send("AI isn't configured (set **OPENROUTER_API_KEY**).", delete_after=12)
    me = ctx.guild.me
    if me:
        perms = ch.permissions_for(me)
        if not perms.read_message_history:
            return await ctx.send("I need **Read Message History** here.", delete_after=10)
        if not perms.send_messages:
            return await ctx.send("I can't send messages in this channel.", delete_after=8)

    async with ch.typing():
        try:
            text = await generate_chat_topic_line(ch, source="6topic")
        except Exception as e:
            return await ctx.send(f"Grok error: `{e}`", delete_after=15)

    if len(text) < 4:
        return await ctx.send("Got nothing useful back — try **`6topic`** again.", delete_after=8)
    await ctx.send(text[:2000], allowed_mentions=discord.AllowedMentions.none())


SUMMARIZE_WINDOW_SEC = 3 * 3600  # 3 hours
SUMMARIZE_MAX_MESSAGES = 400
SUMMARIZE_MAX_CHARS = 12000


def _format_summarize_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    minutes_total = seconds // 60
    hours = minutes_total // 60
    minutes = minutes_total % 60
    if hours == 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    if minutes == 0:
        return f"{hours} hour{'s' if hours != 1 else ''}"
    return f"{hours} hour{'s' if hours != 1 else ''} and {minutes} minute{'s' if minutes != 1 else ''}"


async def _collect_recent_channel_messages(
    channel: discord.abc.Messageable,
    *,
    max_age_sec: int = SUMMARIZE_WINDOW_SEC,
    max_messages: int = SUMMARIZE_MAX_MESSAGES,
) -> list[discord.Message]:
    """Newest-first pull capped by age (seconds) and count; returns oldest-first."""
    cutoff = discord.utils.utcnow().timestamp() - max_age_sec
    collected: list[discord.Message] = []
    try:
        async for msg in channel.history(limit=max_messages, oldest_first=False):
            if msg.created_at.timestamp() < cutoff:
                break
            if msg.author.bot or msg.webhook_id:
                continue
            if msg.type not in (discord.MessageType.default, discord.MessageType.reply):
                continue
            content = (msg.content or "").strip()
            if not content:
                continue
            collected.append(msg)
    except (discord.Forbidden, discord.HTTPException):
        return collected
    collected.reverse()
    return collected


def _build_summarize_transcript(messages: list[discord.Message]) -> str:
    lines: list[str] = []
    total = 0
    for msg in messages:
        name = msg.author.display_name or msg.author.name
        ts = msg.created_at.strftime("%H:%M")
        content = " ".join((msg.content or "").split())
        if not content:
            continue
        line = f"[{ts}] {name}: {content}"
        if len(line) > 600:
            line = line[:597] + "…"
        if total + len(line) + 1 > SUMMARIZE_MAX_CHARS:
            break
        lines.append(line)
        total += len(line) + 1
    return "\n".join(lines)


@bot.command(name="summarize", aliases=["summary", "tldr"])
async def summarize_cmd(ctx: commands.Context) -> None:
    """`6summarize` — simple summary of this channel's last 3 hours of chat via Grok."""
    if not ctx.guild:
        return await ctx.send("Use `6summarize` in a server channel.", delete_after=8)
    if not _grok_api_key():
        return await ctx.send(
            "AI isn’t configured — set **OPENROUTER_API_KEY** in the environment.",
            delete_after=12,
        )

    async with ctx.channel.typing():
        try:
            messages = await _collect_recent_channel_messages(ctx.channel)
        except Exception as e:
            return await ctx.send(f"Couldn’t read history: `{e}`", delete_after=12)

        if not messages:
            return await ctx.send(
                "No human chat in the last 3 hours here to summarize.",
                delete_after=10,
            )

        first_ts = messages[0].created_at.timestamp()
        last_ts = messages[-1].created_at.timestamp()
        span_sec = min(SUMMARIZE_WINDOW_SEC, max(0, last_ts - first_ts))
        duration_str = _format_summarize_duration(span_sec)

        transcript = _build_summarize_transcript(messages)
        if not transcript:
            return await ctx.send("Nothing chat-like to summarize in this channel.", delete_after=10)

        user_content = (
            f"Channel: #{ctx.channel.name}\n"
            f"Actual time covered by the transcript below: {duration_str} "
            f"(use this exact phrasing in your opening sentence).\n"
            f"Transcript (oldest → newest):\n{transcript}"
        )

        try:
            raw = await grok_chat(
                GROK_SUMMARIZE_SYSTEM_PROMPT,
                user_content,
                max_tokens=900,
                temperature=0.5,
            )
        except Exception as e:
            return await ctx.send(f"Grok error: `{e}`", delete_after=15)

    text = (raw or "").strip()
    if not text:
        return await ctx.send("Got an empty summary — try again later.", delete_after=8)

    header = (
        f"**Summary of #{ctx.channel.name}** — **{len(messages)}** message(s) "
        f"across **{duration_str}**\n"
    )
    payload = header + text
    if len(payload) <= 2000:
        await ctx.send(payload, allowed_mentions=discord.AllowedMentions.none())
        return

    remaining = text
    await ctx.send(header, allowed_mentions=discord.AllowedMentions.none())
    while remaining:
        chunk = remaining[:1990]
        if len(remaining) > 1990:
            cut = chunk.rfind("\n")
            if cut < 1000:
                cut = chunk.rfind(" ")
            if cut > 500:
                chunk = chunk[:cut]
        await ctx.send(chunk, allowed_mentions=discord.AllowedMentions.none())
        remaining = remaining[len(chunk):].lstrip()


@bot.command(name="ai", aliases=["ask", "grok"])
async def ai_cmd(ctx, *, question: str):
    """`6ai <question>` — one short paragraph answer via Grok (no essay)."""
    q = (question or "").strip()
    if len(q) < 2:
        return await ctx.send(f"Usage: **`{PREFIX}ai** <your question>`", delete_after=10)
    if not _grok_api_key():
        return await ctx.send(
            "AI isn’t configured — set **OPENROUTER_API_KEY** in the environment.",
            delete_after=12,
        )

    async with ctx.channel.typing():
        try:
            raw = await grok_chat(
                GROK_AI_SYSTEM_PROMPT,
                q[:8000],
                max_tokens=520,
                temperature=0.9,
            )
        except Exception as e:
            return await ctx.send(f"Grok error: `{e}`", delete_after=15)

    out = " ".join((raw or "").strip().split())
    if len(out) > 2000:
        out = out[:1997] + "…"
    if not out:
        return await ctx.send("Got an empty reply — try again.", delete_after=8)
    await ctx.send(out, allowed_mentions=discord.AllowedMentions.none())


# ====================== CONFESS (any member; anonymous post) ======================
@bot.command(name="confess")
async def confess_cmd(ctx, *, text: str):
    if not text.strip():
        return await ctx.send("Usage: `6confess <your confession>`", delete_after=8)
    if not ctx.guild:
        return await ctx.send("Use `6confess` in a server.", delete_after=6)

    ch = confessions_channel(ctx.guild)
    if not ch:
        return await ctx.send(
            "No confessions channel. Set `CONFESSIONS_CHANNEL_ID` in the bot config "
            "or create a channel named **confessions**.",
            delete_after=12,
        )

    try:
        await ctx.message.delete()
    except discord.Forbidden:
        pass

    try:
        await ch.send(f"**Anonymous confession**\n{text}")
    except discord.Forbidden:
        return await ctx.send(
            "I can't post in the confessions channel (permissions).",
            delete_after=10,
        )


# ====================== BAN ======================
@bot.command(name="ban")
@commands.has_permissions(ban_members=True)
async def ban(ctx, member: discord.Member, *, reason: str = ""):
    """`6ban @user` — ban without deleting past messages. `6ban @user yes` — ban and delete their messages (Discord: last 7 days max). Optional reason after `yes`, e.g. `6ban @user yes spam`."""
    if member.id == ctx.author.id or member.id == bot.user.id:
        return await ctx.send("❌ You can't ban yourself or the bot.")
    if is_trusted(member):
        return await ctx.send("❌ You can't ban a trusted user.")

    raw = (reason or "").strip()
    delete_message_days = 0
    ban_reason_text: Optional[str] = None
    if raw:
        parts = raw.split(None, 1)
        if parts[0].lower() == "yes":
            delete_message_days = DELETE_DAYS_ON_BAN
            ban_reason_text = parts[1].strip() if len(parts) > 1 else None
        else:
            ban_reason_text = raw

    now = time.time()

    # Track moderator's actions
    mod_action_cache[ctx.author.id] = [t for t in mod_action_cache[ctx.author.id] if now - t < MASS_ACTION_WINDOW]
    mod_action_cache[ctx.author.id].append(now)

    action_count = len(mod_action_cache[ctx.author.id])
    print(f"[COOLDOWN] {ctx.author} has done {action_count} ban/kick actions in the last 30 minutes.")

    if action_count >= MASS_ACTION_THRESHOLD:
        try:
            # Try to strip all roles
            current_roles = ctx.author.roles[1:]  # exclude @everyone
            if current_roles:
                await ctx.author.edit(
                    roles=[ctx.guild.default_role],
                    reason="Moderator cooldown: Too many bans in 30 minutes"
                )
                await ctx.send(f"⚠️ **{ctx.author}** has been **fully demoted** (all roles removed) for banning too many people in 30 minutes.")
                print(f"[COOLDOWN] Successfully stripped roles from {ctx.author}")
            else:
                await ctx.send("Cooldown triggered, but you have no roles to strip.")
        except discord.Forbidden:
            await ctx.send(f"⚠️ Cooldown triggered for **{ctx.author}**, but I **could not** strip roles.\nThis usually happens because the person has Administrator or their role is higher than the bot's.")
        except Exception as e:
            await ctx.send("Cooldown triggered but failed to demote.")
            print(f"[COOLDOWN] Error: {e}")

    audit_reason = ban_reason_text if ban_reason_text else f"Banned by {ctx.author}"
    await ctx.guild.ban(
        member,
        delete_message_days=delete_message_days,
        reason=audit_reason,
    )
    purge_note = (
        f" · purged their messages (last **{delete_message_days}** days, Discord max)"
        if delete_message_days
        else " · **no** message purge"
    )
    await ctx.send(
        f"✅ **Banned** {member}{purge_note} | Reason: {ban_reason_text or 'No reason provided'}"
    )


# Do the same improvement for kick command if you want (I can add it too)


# ====================== KICK ======================
@bot.command(name="kick")
@commands.has_permissions(kick_members=True)
async def kick(ctx, member: discord.Member, *, reason: str = None):
    if member.id == ctx.author.id or member.id == bot.user.id:
        return await ctx.send("❌ You can't kick yourself or the bot.")
    if is_trusted(member):
        return await ctx.send("❌ You can't kick a trusted user.")

    now = time.time()

    mod_action_cache[ctx.author.id] = [t for t in mod_action_cache[ctx.author.id] if now - t < MASS_ACTION_WINDOW]
    mod_action_cache[ctx.author.id].append(now)

    if len(mod_action_cache[ctx.author.id]) >= MASS_ACTION_THRESHOLD:
        try:
            await ctx.author.edit(roles=[ctx.guild.default_role], reason="Moderator cooldown: Too many kicks")
            await ctx.send(f"⚠️ **{ctx.author}** has been **demoted** for kicking too many people in 30 minutes.")
        except discord.Forbidden:
            await ctx.send(f"⚠️ Cooldown triggered for {ctx.author}, but couldn't strip roles (hierarchy issue).")
        except Exception as e:
            print(f"[COOLDOWN] Kick error: {e}")

    await member.kick(reason=reason or f"Kicked by {ctx.author}")
    await ctx.send(f"✅ **Kicked** {member} | Reason: {reason or 'No reason provided'}")


@bot.command(name="kickallun")
@commands.guild_only()
@commands.has_permissions(kick_members=True)
async def kickallun(ctx: commands.Context) -> None:
    """Kick all unverified members (no 6XS Member role), excluding boosters."""
    guild = ctx.guild
    me = guild.me or guild.get_member(bot.user.id if bot.user else 0)
    if me is None or not me.guild_permissions.kick_members:
        return await ctx.send("❌ I need **Kick Members** permission.")

    verified_role = guild.get_role(VERIFIED_MEMBER_ROLE_ID)
    booster_role = guild.get_role(SERVER_BOOSTER_ROLE_ID)
    if verified_role is None:
        return await ctx.send(f"❌ Verified role not found: `{VERIFIED_MEMBER_ROLE_ID}`")
    if booster_role is None:
        return await ctx.send(f"❌ Booster role not found: `{SERVER_BOOSTER_ROLE_ID}`")

    targets: list[discord.Member] = []
    for m in guild.members:
        if m.bot:
            continue
        if is_trusted(m):
            continue
        if verified_role in m.roles:
            continue
        if booster_role in m.roles:
            continue
        if m.top_role >= me.top_role:
            continue
        targets.append(m)

    if not targets:
        return await ctx.send("✅ No unverified non-booster members to kick.")

    kicked = 0
    failed = 0
    for m in targets:
        try:
            try:
                await m.send("You've been kicked from 6XS for not verifying\nJoin back here discord.gg/6xz")
            except Exception:
                pass
            await m.kick(reason=f"Bulk unverified kick by {ctx.author} (missing 6XS Member role)")
            kicked += 1
        except Exception:
            failed += 1

    await ctx.send(
        f"✅ `6kickallun` finished. Kicked: **{kicked}** · Failed: **{failed}** · "
        f"Excluded boosters (`{SERVER_BOOSTER_ROLE_ID}`) and verified members (`{VERIFIED_MEMBER_ROLE_ID}`)."
    )


# ====================== PURGE ======================
PURGE_AMOUNT_MIN = 1
PURGE_AMOUNT_MAX = 500


@bot.command(name="purge", aliases=["clear"])
@commands.guild_only()
@commands.has_permissions(manage_messages=True)
@commands.bot_has_permissions(manage_messages=True, read_message_history=True)
async def purge_cmd(ctx: commands.Context, amount: int) -> None:
    """Bulk-delete recent messages in this channel. Usage: `6purge <amount>` (1–500)."""
    ch = ctx.channel
    if not isinstance(ch, discord.TextChannel):
        return await ctx.send("Use **`6purge`** in a text channel.", delete_after=6)

    n = max(PURGE_AMOUNT_MIN, min(int(amount), PURGE_AMOUNT_MAX))
    try:
        deleted = await ch.purge(limit=n)
    except discord.Forbidden:
        return await ctx.send("I can't delete messages here.", delete_after=8)
    except discord.HTTPException as e:
        return await ctx.send(f"Purge failed: `{e}`", delete_after=12)

    try:
        await ctx.send(f"Deleted **{len(deleted)}** message(s).", delete_after=6)
    except discord.HTTPException:
        pass


# ====================== ANTI-RAID ======================
@bot.event
async def on_member_join(member: discord.Member):
    now = time.time()
    join_cache[member.guild.id] = [t for t in join_cache[member.guild.id] if now - t[0] < 30]
    join_cache[member.guild.id].append((now, member.id))

    asyncio.create_task(_welcome_banner_job(member.id, member.guild.id))

    if len(join_cache[member.guild.id]) >= MASS_JOIN_THRESHOLD:
        for entry in join_cache[member.guild.id][-MASS_JOIN_THRESHOLD:]:
            try:
                raider = member.guild.get_member(entry[1])
                if raider:
                    await raider.kick(reason="Anti-raid: mass join detected")
            except:
                pass
        join_cache[member.guild.id].clear()


# ====================== ANTI-NUKE ======================
@bot.event
async def on_guild_role_delete(role: discord.Role):
    async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
        if entry.user and not is_trusted(entry.user):
            try:
                await entry.user.ban(reason="Anti-nuke: unauthorized role delete", delete_message_days=DELETE_DAYS_ON_BAN)
            except:
                pass
        break


@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    now = time.time()

    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_kick):
        if entry.target and entry.target.id == member.id and entry.user and not is_trusted(entry.user):
            try:
                await entry.user.ban(reason="Anti-nuke: unauthorized kick", delete_message_days=DELETE_DAYS_ON_BAN)
            except:
                pass
        break

    async for entry in guild.audit_logs(limit=1, action=discord.AuditLogAction.member_ban):
        if entry.target and entry.target.id == member.id and entry.user and not is_trusted(entry.user):
            try:
                await entry.user.ban(reason="Anti-nuke: unauthorized ban", delete_message_days=DELETE_DAYS_ON_BAN)
            except:
                pass

            if entry.user:
                ban_cache[guild.id] = [t for t in ban_cache[guild.id] if now - t[0] < MASS_BAN_WINDOW]
                ban_cache[guild.id].append((now, entry.user.id))
                user_bans = [b for b in ban_cache[guild.id] if b[1] == entry.user.id]
                if len(user_bans) >= MASS_BAN_THRESHOLD:
                    try:
                        await entry.user.edit(roles=[guild.default_role], reason="Anti-nuke: mass banning detected")
                    except:
                        pass
        break


# ====================== ANTI-NUKE with Cooldown ======================
@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        if entry.user and not is_trusted(entry.user):
            now = time.time()

            # Track channel deletes by this user
            channel_delete_cache[entry.user.id] = [t for t in channel_delete_cache[entry.user.id] if
                                                   now - t < MASS_ACTION_WINDOW]
            channel_delete_cache[entry.user.id].append(now)

            try:
                await entry.user.ban(reason="Anti-nuke: unauthorized channel delete",
                                     delete_message_days=DELETE_DAYS_ON_BAN)
                await channel.guild.text_channels[0].send(
                    f"🚨 **Anti-nuke:** {entry.user} was banned for deleting a channel.")
            except:
                pass

            # If they deleted multiple channels quickly, strip roles (extra protection)
            if len(channel_delete_cache[entry.user.id]) >= MASS_ACTION_THRESHOLD:
                try:
                    await entry.user.edit(roles=[channel.guild.default_role],
                                          reason="Anti-nuke: Multiple channel deletes")
                    print(f"[ANTI-NUKE] Stripped roles from {entry.user} for multiple channel deletes")
                except:
                    pass
        break


async def grant_six_xs_roles(member: discord.Member, old_lvl: int, new_lvl: int):
    for milestone in SIX_XS_MILESTONES:
        if not (old_lvl < milestone <= new_lvl):
            continue

        role_id = SIX_XS_ROLES.get(milestone)
        role: Optional[discord.Role] = None
        if role_id:
            role = member.guild.get_role(role_id)

        # Fallback: find roles named like "6xs 75" so new milestones
        # work immediately even before IDs are added to JSON config.
        if role is None:
            for candidate in member.guild.roles:
                name = candidate.name.lower()
                if "6xs" not in name:
                    continue
                nums = re.findall(r"\d+", name)
                if any(int(n) == milestone for n in nums):                                                                                                                       
                    role = candidate
                    break

        if role and role not in member.roles:
            try:
                await member.add_roles(role, reason=f"Reached 6xs {milestone}")
            except discord.Forbidden:
                print(f"[6XS] Cannot assign role {role.id} (permissions / hierarchy)")


async def _six_xs_message_counts_as_chat(bot: commands.Bot, message: discord.Message) -> bool:
    """Same eligibility as live XP: human chat, not a bot command (prefix is only `6` / `6 `)."""
    if message.author.bot or message.webhook_id:
        return False
    if message.type not in (discord.MessageType.default, discord.MessageType.reply):
        return False
    c = (message.content or "").lstrip()
    if not c.startswith(PREFIX):
        return True
    ctx = await bot.get_context(message)
    return ctx.command is None


async def _six_xs_compute_from_history(
    guild: discord.Guild,
    bot: commands.Bot,
    status_callback: Optional[Callable[[str], Awaitable[None]]] = None,
) -> dict[int, tuple[int, float, int]]:
    """
    Replay 6xs rules over readable history. Returns user_id -> (raw_xp, last_award_ts, eligible_msgs).
    XP per hit uses Random(msg_id ^ uid) in SIX_XS_XP_RANGE (no live **6boost** — history predates timed windows).
    """
    events: list[tuple[float, int, int]] = []
    seen_msg: set[int] = set()
    scanned_threads: set[int] = set()
    total_fetched = 0

    async def add_from_channel(channel: discord.abc.Messageable, label: str) -> None:
        nonlocal total_fetched
        if isinstance(channel, discord.Thread):
            scanned_threads.add(channel.id)
        try:
            async for message in channel.history(limit=None, oldest_first=True):
                if message.id in seen_msg:
                    continue
                seen_msg.add(message.id)
                total_fetched += 1
                if total_fetched % 4000 == 0 and status_callback:
                    await status_callback(
                        f"6xs rescan: fetched **{total_fetched:,}** messages (`{label}`)…"
                    )
                if not await _six_xs_message_counts_as_chat(bot, message):
                    continue
                ts = message.created_at.timestamp()
                events.append((ts, message.author.id, message.id))
        except (discord.Forbidden, discord.HTTPException) as e:
            if status_callback:
                await status_callback(f"6xs rescan: skipped `{label}` ({e})")

    for ch in guild.channels:
        if isinstance(ch, discord.TextChannel):
            await add_from_channel(ch, ch.name)
            for th in ch.threads:
                await add_from_channel(th, f"#{ch.name}/{th.name}")
        elif isinstance(ch, discord.ForumChannel):
            for th in ch.threads:
                await add_from_channel(th, f"forum/{th.name}")
            try:
                async for th in ch.archived_threads(limit=None):
                    await add_from_channel(th, f"forum/{th.name}")
            except (discord.Forbidden, discord.HTTPException) as e:
                if status_callback:
                    await status_callback(f"6xs rescan: forum archived threads skipped ({e})")

    for th in guild.threads:
        if th.id in scanned_threads:
            continue
        await add_from_channel(th, th.name)

    events.sort(key=lambda t: t[0])
    last_ts: dict[int, float] = {}
    xp_map: dict[int, int] = {}
    eligible_by_user: dict[int, int] = defaultdict(int)
    for ts, uid, mid in events:
        if ts - last_ts.get(uid, -1e18) < SIX_XS_COOLDOWN:
            continue
        last_ts[uid] = ts
        rng = random.Random(mid ^ uid)
        gain = rng.randint(SIX_XS_XP_RANGE[0], SIX_XS_XP_RANGE[1])
        xp_map[uid] = xp_map.get(uid, 0) + gain
        eligible_by_user[uid] += 1

    out: dict[int, tuple[int, float, int]] = {}
    for uid, xp in xp_map.items():
        out[uid] = (xp, last_ts[uid], eligible_by_user[uid])
    return out


def _six_xs_normalize_last_msg_ts(raw: object, now: float) -> float:
    """Avoid stuck cooldowns from corrupt state, null, ms-vs-seconds, or future timestamps."""
    try:
        if raw is None:
            return 0.0
        last = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if last > 1e12:
        last = last / 1000.0
    if last > now:
        return 0.0
    return last


async def maybe_award_six_xs(message: discord.Message):
    if not message.guild:
        return
    key = f"{message.guild.id}:{message.author.id}"
    now = time.time()
    m = message.author if isinstance(message.author, discord.Member) else message.guild.get_member(message.author.id)
    boost_amt, _boost_left = _six_xs_boost_status(message.guild.id)
    cd = float(SIX_XS_COOLDOWN_DURING_BOOST if boost_amt > 0 else SIX_XS_COOLDOWN)
    session_hours_crossed = 0
    session_bonus = 0
    async with _six_xs_lock:
        entry = six_xs_data.setdefault(key, {"xp": 0, "last_msg": 0.0})
        synced = _six_xs_sync_raw_to_milestone_roles(entry, m) if m else False
        last_msg = _six_xs_normalize_last_msg_ts(entry.get("last_msg"), now)
        # Do not write last_msg until we actually award XP — avoids desync / skipped cooldowns.
        if now - last_msg < cd:
            if synced:
                save_six_xs_sync()
            return

        # Active-session tracking: a "session" is continuous XP-earning activity with no gap
        # longer than SIX_XS_SESSION_GAP_SEC. Bonus XP and milestone announcements scale with hours.
        prev_sess_start = entry.get("session_start")
        prev_hours_announced = int(entry.get("session_hours_announced", 0))
        if (
            prev_sess_start is None
            or last_msg <= 0
            or (now - last_msg) > SIX_XS_SESSION_GAP_SEC
        ):
            sess_start = now
            hours_announced = 0
        else:
            try:
                sess_start = float(prev_sess_start)
            except (TypeError, ValueError):
                sess_start = now
            hours_announced = prev_hours_announced

        session_hours = int(max(0.0, now - sess_start) // 3600)
        capped_hours = min(session_hours, SIX_XS_SESSION_BONUS_MAX_HOURS)
        session_bonus = capped_hours * SIX_XS_SESSION_BONUS_PER_HOUR

        entry["last_msg"] = now
        entry["session_start"] = sess_start
        if session_hours > hours_announced:
            entry["session_hours_announced"] = session_hours
            session_hours_crossed = session_hours
        else:
            entry["session_hours_announced"] = hours_announced

        old_raw = int(entry.get("xp", 0))
        gain = random.randint(SIX_XS_XP_RANGE[0], SIX_XS_XP_RANGE[1]) + boost_amt + session_bonus
        new_raw = old_raw + gain
        entry["xp"] = new_raw
        _, old_chat_lvl = total_xp_and_6xs(old_raw)
        _, new_chat_lvl = total_xp_and_6xs(new_raw)
        save_six_xs_sync()

    if session_hours_crossed >= 1 and m is not None:
        hours_word = "hour" if session_hours_crossed == 1 else "hours"
        try:
            await message.channel.send(
                f"🔥 {m.mention} has been active for **{session_hours_crossed} {hours_word}** on 6xs — "
                f"bonus **+{session_bonus} XP/msg** while the session continues."
            )
        except discord.HTTPException as e:
            print(f"[6XS] session-hour announce failed: {e}")

    # Level-ups and roles follow **chat XP** only; milestone roles already merged into stored XP above.
    if new_chat_lvl > old_chat_lvl:
        # Milestone 3 = image perms — fire whenever they *cross* 3 (even 2→5 in one message).
        if old_chat_lvl < 3 <= new_chat_lvl:
            if new_chat_lvl == 3:
                lvl_msg = f"congrats {message.author.display_name} you unlocked image perms"
            else:
                lvl_msg = (
                    f"congrats {message.author.display_name} you unlocked image perms "
                    f"— you're now **6xs {new_chat_lvl}**"
                )
        else:
            lvl_msg = (
                f"congrats {message.author.display_name} you leveled up, you're now 6xs {new_chat_lvl}"
            )
        await message.channel.send(lvl_msg)
        if m:
            await grant_six_xs_roles(m, old_chat_lvl, new_chat_lvl)
        hc = bot.get_cog("HoldingCellCog")
        if hc is not None and m is not None:
            await hc.try_grant_sixxs_milestone(m, message.channel, new_chat_lvl)


@bot.command(name="xs", aliases=["sixxs", "xsrank"])
async def cmd_six_xs(ctx, member: Optional[discord.Member] = None):
    """Your 6xs level, XP to next level, and lifetime XP. Optional: `6xs @member`"""
    if not ctx.guild:
        return await ctx.send("Use `6xs` in a server.", delete_after=8)
    member = member or ctx.author
    await _six_xs_ensure_entry_synced(ctx.guild.id, member)
    key = f"{ctx.guild.id}:{member.id}"
    raw = int(six_xs_data.get(key, {}).get("xp", 0))
    prog, lvl = total_xp_and_6xs(raw)
    need = xp_cost_to_advance_from(lvl)

    next_role = None
    for milestone in sorted(SIX_XS_ROLES):
        if milestone > lvl:
            next_role = milestone
            break

    role_hint = ""
    if next_role:
        role_hint = f" Next cosmetic role milestone: **6xs {next_role}**."
    b, b_left = _six_xs_boost_status(ctx.guild.id)
    lo = SIX_XS_XP_RANGE[0] + b
    hi = SIX_XS_XP_RANGE[1] + b
    if b > 0:
        bm, bs = int(b_left // 60), int(b_left % 60)
        eta = f"{bm}m {bs:02d}s" if bm else f"{bs}s"
        cd_txt = (
            "no cooldown"
            if SIX_XS_COOLDOWN_DURING_BOOST <= 0
            else f"{SIX_XS_COOLDOWN_DURING_BOOST:g}s"
        )
        boost_note = f" · **6boost** +{b} (**{eta}** left, **{cd_txt}** between hits vs {SIX_XS_COOLDOWN}s normally)"
    else:
        boost_note = ""

    await ctx.send(
        f"**{member.display_name}** — **6xs {lvl}** · chat: `{prog}/{need}` XP to next level\n"
        f"Lifetime XP: **{raw:,}** · per message: **{lo}–{hi}** XP "
        f"(cooldown **{SIX_XS_COOLDOWN}s** when 6boost is off){boost_note}.{role_hint}"
    )


@bot.command(name="boost", aliases=["sixboost", "xsboost"])
async def cmd_six_boost(ctx, *, args: str = ""):
    """
    **5-minute** server-wide **6xs** bonus: extra XP per hit and **21s cooldown** (half of normal 42s).
    Anyone: **`6boost`** — status. Trusted: **`6boost <amount>`** starts a window, **`6boost off`** ends it.
    """
    if not ctx.guild:
        return await ctx.send("Use `6boost` in a server.", delete_after=8)
    gid = ctx.guild.id
    arg = (args or "").strip()
    if not arg:
        b, left = _six_xs_boost_status(gid)
        if b <= 0:
            cd_b = SIX_XS_COOLDOWN_DURING_BOOST
            cd_note = "no gap between hits" if cd_b <= 0 else f"{cd_b:g}s between hits"
            return await ctx.send(
                f"No **6boost** window — normal **{SIX_XS_XP_RANGE[0]}–{SIX_XS_XP_RANGE[1]}** XP, **{SIX_XS_COOLDOWN}s** cooldown.\n"
                f"Trusted: **`6boost <1–{SIX_XS_BOOST_CAP}>`** → **{SIX_XS_BOOST_DURATION_SEC // 60} minutes** of +XP per message "
                f"and **{cd_note}** (vs **{SIX_XS_COOLDOWN}s** normally). **`6boost off`** clears.",
                delete_after=26,
            )
        lo, hi = SIX_XS_XP_RANGE[0] + b, SIX_XS_XP_RANGE[1] + b
        bm, bs = int(left // 60), int(left % 60)
        eta = f"{bm}m {bs:02d}s" if bm else f"{bs}s"
        cd_txt = (
            "no cooldown"
            if SIX_XS_COOLDOWN_DURING_BOOST <= 0
            else f"{SIX_XS_COOLDOWN_DURING_BOOST:g}s cooldown"
        )
        return await ctx.send(
            f"**6boost** on: **+{b}** XP → **{lo}–{hi}** per hit · **~{eta}** left · **{cd_txt}** (normally **{SIX_XS_COOLDOWN}s**).",
            delete_after=22,
        )
    if not is_trusted(ctx.author):
        return await ctx.send("Only trusted users can change **6boost**.", delete_after=8)
    low = arg.lower()
    if low in ("off", "none", "disable", "0"):
        n = 0
    else:
        try:
            n = int(arg, 10)
        except ValueError:
            return await ctx.send(
                f"Trusted: **`6boost <1–{SIX_XS_BOOST_CAP}>`** (starts **{SIX_XS_BOOST_DURATION_SEC // 60}m** window) or **`6boost off`**.",
                delete_after=10,
            )
    if n < 0 or n > SIX_XS_BOOST_CAP:
        return await ctx.send(f"Use **1–{SIX_XS_BOOST_CAP}** to start, or **off**.", delete_after=8)
    if n == 0:
        six_xs_boost.pop(gid, None)
        save_six_xs_boost_sync()
        return await ctx.send("**6boost** ended for this server.")
    until = time.time() + SIX_XS_BOOST_DURATION_SEC
    six_xs_boost[gid] = {"until": until, "amount": n}
    save_six_xs_boost_sync()
    lo, hi = SIX_XS_XP_RANGE[0] + n, SIX_XS_XP_RANGE[1] + n
    cd_txt = (
        "no cooldown between hits"
        if SIX_XS_COOLDOWN_DURING_BOOST <= 0
        else f"{SIX_XS_COOLDOWN_DURING_BOOST:g}s between hits"
    )
    await ctx.send(
        f"**6boost +{n}** for **{SIX_XS_BOOST_DURATION_SEC // 60} minutes** — **{lo}–{hi}** XP per qualifying message, **{cd_txt}** (usually **{SIX_XS_COOLDOWN}s**)."
    )


@bot.command(name="xslb", aliases=["xsleaderboard", "xstop"])
async def cmd_six_xs_leaderboard(ctx, limit: Optional[int] = None):
    """Top 6xs in this server (all members ranked; 0 XP if no chat XP yet). `6xslb` or `6xslb 10` (max 25)."""
    if not ctx.guild:
        return await ctx.send("Use `6xslb` in a server.", delete_after=8)

    n = 15 if limit is None else max(1, min(25, int(limit)))

    if not ctx.guild.chunked:
        try:
            await ctx.guild.chunk(cache=True)
        except Exception:
            pass

    rows = _build_six_xs_leaderboard_rows(ctx.guild, n)
    if not rows:
        return await ctx.send("No members loaded yet — try again in a few seconds.")

    medals = ("🥇", "🥈", "🥉")
    lines: list[str] = []
    for i, (_uid, raw, rank_lvl, lvl_xp, _lvl_role, label) in enumerate(rows):
        medal = medals[i] if i < len(medals) else f"`{i + 1}.`"
        lines.append(f"{medal} **{label}** — **6xs {rank_lvl}** · {raw:,} XP")

    await _six_xs_ensure_entry_synced(ctx.guild.id, ctx.author if isinstance(ctx.author, discord.Member) else ctx.guild.get_member(ctx.author.id))
    my_raw = int(six_xs_data.get(f"{ctx.guild.id}:{ctx.author.id}", {}).get("xp", 0))
    _, my_lvl_xp = total_xp_and_6xs(my_raw)

    em = discord.Embed(
        title="6xs leaderboard",
        description="\n".join(lines),
        color=discord.Color.dark_teal(),
    )
    em.set_footer(
        text=f"Top {len(rows)} · rank = chat XP (milestone roles bump stored XP to match) · Yours: 6xs {my_lvl_xp}"
    )
    await ctx.send(embed=em)


@bot.command(name="xsrestore", hidden=True)
async def cmd_xs_restore(ctx, member: discord.Member, level: int):
    """Trusted only: set someone's chat XP to match 6xs level N and sync milestone roles. `6xsrestore @user 14`"""
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted users only.", delete_after=6)
    if level < 1 or level > 99:
        return await ctx.send("Level must be 1–99.", delete_after=8)

    key = f"{ctx.guild.id}:{member.id}"
    old_raw = int(six_xs_data.get(key, {}).get("xp", 0))
    _, old_lvl = total_xp_and_6xs(old_raw)
    new_raw = min_raw_xp_for_6xs_level(level)
    prev_lm = float(six_xs_data.get(key, {}).get("last_msg", 0.0))
    six_xs_data[key] = {"xp": new_raw, "last_msg": prev_lm}
    save_six_xs_sync()

    try:
        await grant_six_xs_roles(member, old_lvl, level)
    except Exception as e:
        print(f"[6XS] xsrestore grant roles: {e}")

    await ctx.send(
        f"**{member.display_name}** chat XP set to **6xs {level}** (raw **{new_raw:,}**). "
        f"Milestone roles synced from {old_lvl} → {level}."
    )


def _parse_fixx_bot_congrats_level(content: str) -> Optional[int]:
    """Parse our bot’s 6xs congrats line and return the **level** only (user ID comes from the prior message)."""
    if not content:
        return None
    c = content.strip()
    m = re.match(
        r"^congrats\s+.+\s+you unlocked image perms\s*[—\-]\s+you['\u2019]re now \*\*6xs (\d+)\*\*\s*$",
        c,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        return int(m.group(1))
    m = re.match(
        r"^congrats\s+.+\s+you leveled up,\s+you['\u2019]re now 6xs\s+(\d+)\s*$",
        c,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1))
    m = re.match(r"^congrats\s+.+\s+you unlocked image perms\s*$", c, re.IGNORECASE)
    if m:
        return 3
    m = re.match(r"^congrats\s+.+\s+you unlocked pic perms\s*$", c, re.IGNORECASE)
    if m:
        return 3
    return None


@bot.command(name="fixx", hidden=True)
async def cmd_fixx(ctx: commands.Context, *, scope: str = "") -> None:
    """
    Trusted: scan this bot’s **congrats … 6xs** messages. Each one was sent **right after** the member’s
    chat message — **6fixx** takes the **previous message’s author id** as that user and the congrats
    text for their level, then raises chat XP to each person’s **highest** seen level (never lowers).

    **`6fixx`** — scan **this text channel** · **`6fixx all`** — scan **all** text channels (slow).
    """
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted users only.", delete_after=6)

    guild = ctx.guild
    want_all = scope.strip().lower() in ("all", "everywhere", "guild", "server")

    if want_all:
        channels = [
            ch
            for ch in guild.text_channels
            if ch.permissions_for(guild.me).read_message_history
        ]
    else:
        if not isinstance(ctx.channel, discord.TextChannel):
            return await ctx.send(
                "Use **`6fixx`** in a **text** channel, or **`6fixx all`** to scan the whole server.",
                delete_after=12,
            )
        if not ctx.channel.permissions_for(guild.me).read_message_history:
            return await ctx.send("I need **Read Message History** here.", delete_after=8)
        channels = [ctx.channel]

    if not channels:
        return await ctx.send("No channels to scan.", delete_after=6)

    status = await ctx.send(
        f"**6fixx** — scanning **{len(channels)}** channel(s) for bot congrats… (this can take a while)"
    )
    try:
        await guild.chunk()
    except discord.HTTPException:
        pass

    bot_id = bot.user.id if bot.user else 0
    best: dict[int, int] = {}
    scanned = 0
    bot_hits = 0
    skipped_pair = 0

    for ch in channels:
        try:
            prev_msg: Optional[discord.Message] = None
            async for msg in ch.history(limit=None, oldest_first=True):
                scanned += 1
                if scanned % 2500 == 0:
                    try:
                        await status.edit(
                            content=(
                                f"**6fixx** — scanning… **{scanned:,}** messages read, "
                                f"**{len(best)}** user(s) with levels so far (`{ch.name}`)"
                            )
                        )
                    except discord.HTTPException:
                        pass

                if msg.author.id == bot_id and msg.content:
                    lvl = _parse_fixx_bot_congrats_level(msg.content)
                    if lvl is not None and 1 <= lvl <= 99:
                        if prev_msg is None or prev_msg.author.bot or prev_msg.webhook_id:
                            skipped_pair += 1
                        else:
                            uid = prev_msg.author.id
                            bot_hits += 1
                            if lvl > best.get(uid, 0):
                                best[uid] = lvl

                prev_msg = msg
        except discord.Forbidden:
            try:
                await ctx.send(f"Skipping **#{ch.name}** — no access.", delete_after=6)
            except discord.HTTPException:
                pass
        except Exception as e:
            print(f"[6XS] fixx channel {ch.id}: {e}")

    if not best:
        try:
            await status.edit(
                content=(
                    f"**6fixx** done — scanned **{scanned:,}** messages · **{bot_hits}** congrats paired · "
                    f"**{skipped_pair}** skipped (no human message before congrats). "
                    f"No XP updates — no valid **previous-message → congrats** pairs found."
                )
            )
        except discord.HTTPException:
            pass
        return

    hc = bot.get_cog("HoldingCellCog")
    updated = 0
    skipped = 0
    to_grant: list[tuple[int, int, int]] = []

    async with _six_xs_lock:
        for uid, scan_lvl in best.items():
            key = f"{guild.id}:{uid}"
            old_raw = int(six_xs_data.get(key, {}).get("xp", 0))
            _, chat_lvl = total_xp_and_6xs(old_raw)
            target_lvl = max(scan_lvl, chat_lvl)
            if target_lvl <= chat_lvl:
                skipped += 1
                continue
            new_raw = min_raw_xp_for_6xs_level(target_lvl)
            prev_lm = float(six_xs_data.get(key, {}).get("last_msg", 0.0))
            six_xs_data[key] = {"xp": new_raw, "last_msg": prev_lm}
            updated += 1
            to_grant.append((uid, chat_lvl, target_lvl))

        save_six_xs_sync()

    for uid, old_lvl, new_lvl in to_grant:
        mem = guild.get_member(uid)
        if mem is None:
            continue
        try:
            await grant_six_xs_roles(mem, old_lvl, new_lvl)
        except Exception as e:
            print(f"[6XS] fixx grant roles {uid}: {e}")
        if hc is not None:
            try:
                await hc._sync_milestone_backlog(uid, guild.id, mem)
            except Exception as e:
                print(f"[6XS] fixx backlog {uid}: {e}")

    try:
        await status.edit(
            content=(
                f"**6fixx** done — **{scanned:,}** messages · **{bot_hits}** congrats paired (prev msg → bot) · "
                f"**{skipped_pair}** skipped (bot/webhook/no prev) · **{len(best)}** user(s) max level · "
                f"**{updated}** XP rows raised · **{skipped}** already at/above target."
            )
        )
    except discord.HTTPException:
        await ctx.send(
            f"**6fixx** done — updated **{updated}** user(s). (Couldn’t edit status message.)"
        )


@bot.command(name="xssyncroles", aliases=["xsgrantroles", "sync6xsroles"], hidden=True)
async def cmd_xs_sync_roles(ctx, member: Optional[discord.Member] = None):
    """
    Trusted: grant missing **6xs** cosmetic roles from **chat XP** (milestones in `SIX_XS_ROLES`),
    and enqueue **6reward** rows for every milestone you qualify for but haven’t claimed yet.
    No args = whole server (can take a while on big guilds).
    """
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted users only.", delete_after=6)

    guild = ctx.guild
    hc = bot.get_cog("HoldingCellCog")
    status = await ctx.send("Syncing **6xs** roles + reward backlog…")

    if member is not None:
        targets: list[discord.Member] = [member]
    else:
        if guild.large:
            try:
                await guild.chunk()
            except discord.HTTPException:
                pass
        targets = [m for m in guild.members if not m.bot]

    total = len(targets)
    for i, m in enumerate(targets, start=1):
        key = f"{guild.id}:{m.id}"
        raw = int(six_xs_data.get(key, {}).get("xp", 0))
        _, chat_lvl = total_xp_and_6xs(raw)
        try:
            await grant_six_xs_roles(m, 0, chat_lvl)
        except Exception as e:
            await status.edit(content=f"Stopped at **{m.display_name}**: `{e}`")
            return
        if hc is not None:
            try:
                await hc._sync_milestone_backlog(m.id, guild.id, m)
            except Exception as e:
                await status.edit(content=f"Reward backlog error @ **{m.display_name}**: `{e}`")
                return
        if total > 30 and i % 40 == 0:
            try:
                await status.edit(content=f"Syncing… **{i}** / **{total}**")
            except discord.HTTPException:
                pass
        await asyncio.sleep(0.12)

    await status.edit(
        content=f"Done — **{total}** member(s): missing milestone **roles** from chat XP applied; "
        f"**6reward** backlog updated for configured milestones."
    )


@bot.command(name="xsreload", hidden=True)
async def cmd_xs_reload(ctx: commands.Context) -> None:
    """Trusted: re-read six_xs_state.json from disk (use after you restore the file manually)."""
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted / admin only.", delete_after=6)
    async with _six_xs_lock:
        load_six_xs()
    n = len(six_xs_data)
    bak_nm = SIX_XS_STATE_FILE.with_name("six_xs_state.bak.json").name
    await ctx.send(
        f"Reloaded **6xs** from disk — **{n}** entries. "
        f"Files: `{SIX_XS_STATE_FILE.name}` + `{bak_nm}`.",
        delete_after=20,
    )


@bot.command(name="save")
async def cmd_rank_save(ctx: commands.Context, *, name: Optional[str] = None) -> None:
    """
    Trusted: snapshot server recovery bundle (roles/channels + 6xs XP) to disk.
    `6save` — list snapshots. `6save mylabel` — save (dated filename).
    """
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted / admin only.", delete_after=8)

    if not name or not name.strip():
        rows = _snapshot_list_entries()
        if not rows:
            return await ctx.send(
                "No snapshots yet. Use **`6save <name>`** (e.g. `6save before_reset`). "
                f"Files go to `{SIX_XS_SNAPSHOTS_DIR.name}/`.",
                delete_after=20,
            )
        lines: list[str] = []
        for _ts, lab, fn, cnt, iso in rows[:25]:
            when = iso[:19].replace("T", " ") + " UTC" if len(iso) >= 19 else "—"
            lines.append(f"**`{lab}`** — {when} — **{cnt}** users — `{fn}`")
        more = f"\n… and **{len(rows) - 25}** more." if len(rows) > 25 else ""
        await ctx.send(
            "**6xs snapshots** (newest first):\n" + "\n".join(lines) + more,
            delete_after=60,
        )
        return

    display = name.strip()[:120]
    safe = _snapshot_sanitize_label(name)
    SIX_XS_SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _snapshot_new_path(safe)
    now = time.time()
    iso = discord.utils.utcnow().replace(microsecond=0).isoformat()

    async with _six_xs_lock:
        snap = copy.deepcopy(six_xs_data)
        n = len(six_xs_data)

    payload = {
        "v": _SNAPSHOT_FORMAT_VERSION,
        "label": safe,
        "label_display": display,
        "saved_at": iso,
        "saved_at_unix": now,
        "entry_count": n,
        "six_xs_data": snap,
        "guild_backup": _export_guild_structure(ctx.guild),
    }
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError as e:
        return await ctx.send(f"Could not write snapshot: `{e}`", delete_after=12)

    await ctx.send(
        f"Saved recovery snapshot **`{display}`** → `{path.name}`\n"
        f"**Includes:** roles/channels/categories + **6xs** (**{n}** entries)\n"
        f"**When:** {iso} (UTC)\n"
        f"**Load full:** `6load {safe}` · **Load XP only:** `6loadxp {safe}`",
        delete_after=45,
    )


@bot.command(name="load")
async def cmd_rank_load(ctx: commands.Context, *, name: str) -> None:
    """Trusted: restore full recovery snapshot (roles/channels/categories + 6xs XP)."""
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted / admin only.", delete_after=8)
    if not str(name).strip():
        return await ctx.send(
            "Usage: **`6load <name>`** — same label you used with **`6save`**. **`6save`** alone lists saves.",
            delete_after=15,
        )

    safe = _snapshot_sanitize_label(name)
    path = _snapshot_find_latest(safe)
    if path is None:
        return await ctx.send(
            f"No snapshot found for **`{safe}`**. Use **`6save`** (no args) to list saves.",
            delete_after=15,
        )

    meta = _snapshot_read_meta(path)
    if not meta:
        return await ctx.send("That snapshot file is unreadable.", delete_after=10)

    inner = meta.get("six_xs_data")
    if not isinstance(inner, dict):
        return await ctx.send("Invalid snapshot (missing six_xs_data).", delete_after=10)
    guild_backup = meta.get("guild_backup")
    if not isinstance(guild_backup, dict):
        return await ctx.send("Invalid snapshot (missing guild_backup). Use a newer `6save`.", delete_after=12)

    async with _six_xs_lock:
        six_xs_data.clear()
        six_xs_data.update(inner)

    save_six_xs_sync()
    status = await ctx.send("Applying roles/channels backup from snapshot…")
    created_roles, created_categories, created_channels = await _apply_guild_structure_backup(ctx.guild, guild_backup)
    try:
        await status.delete()
    except discord.HTTPException:
        pass
    iso = str(meta.get("saved_at") or "")
    lab = str(meta.get("label_display") or meta.get("label") or path.stem)
    when_line = iso[:19].replace("T", " ") + " UTC" if len(iso) >= 19 else iso or "—"
    await ctx.send(
        f"Loaded full backup from **`{lab}`** (`{path.name}`).\n"
        f"**Created:** {created_roles} role(s), {created_categories} categor(y/ies), {created_channels} channel(s)\n"
        f"**6xs:** {len(six_xs_data)} entries restored\n"
        f"**Saved at:** {when_line}\n"
        f"Note: existing items are kept; this only creates missing ones.",
        delete_after=35,
    )


@bot.command(name="loadxp")
async def cmd_rank_load_xp(ctx: commands.Context, *, name: str) -> None:
    """Trusted: restore only 6xs XP from a snapshot label."""
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted / admin only.", delete_after=8)
    if not str(name).strip():
        return await ctx.send("Usage: **`6loadxp <name>`**.", delete_after=12)

    safe = _snapshot_sanitize_label(name)
    path = _snapshot_find_latest(safe)
    if path is None:
        return await ctx.send(f"No snapshot found for **`{safe}`**.", delete_after=12)
    meta = _snapshot_read_meta(path)
    if not meta:
        return await ctx.send("That snapshot file is unreadable.", delete_after=10)
    inner = meta.get("six_xs_data")
    if not isinstance(inner, dict):
        return await ctx.send("Invalid snapshot (missing six_xs_data).", delete_after=10)

    async with _six_xs_lock:
        six_xs_data.clear()
        six_xs_data.update(inner)
    save_six_xs_sync()

    iso = str(meta.get("saved_at") or "")
    lab = str(meta.get("label_display") or meta.get("label") or path.stem)
    when_line = iso[:19].replace("T", " ") + " UTC" if len(iso) >= 19 else iso or "—"
    await ctx.send(
        f"Loaded XP only from **`{lab}`** (`{path.name}`) — **{len(six_xs_data)}** entries.\n"
        f"**Saved at:** {when_line}",
        delete_after=30,
    )


@bot.command(name="migration")
async def cmd_migration(ctx: commands.Context, *members: discord.Member) -> None:
    """
    Create/join migration registry and send invite for the recovery server.
    `6migration` -> register yourself.
    Trusted: `6migration @user @user` -> register others too.
    """
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)

    guild = bot.get_guild(PRIMARY_GUILD_ID) or ctx.guild
    verify_channel = guild.get_channel(MIGRATION_VERIFY_CHANNEL_ID) if guild else None
    if not isinstance(verify_channel, discord.TextChannel):
        return await ctx.send(
            f"Migration channel <#{MIGRATION_VERIFY_CHANNEL_ID}> not found or not a text channel.",
            delete_after=12,
        )

    targets: list[discord.Member]
    if members:
        if not is_trusted(ctx.author):
            return await ctx.send("Only trusted users can register other members.", delete_after=8)
        targets = list({m.id: m for m in members if not m.bot}.values())
    else:
        targets = [ctx.author] if isinstance(ctx.author, discord.Member) else []
    if not targets:
        return await ctx.send("No valid human member provided.", delete_after=8)

    try:
        invite = await verify_channel.create_invite(
            max_age=0,
            max_uses=0,
            unique=False,
            reason=f"6migration by {ctx.author}",
        )
    except discord.HTTPException as e:
        return await ctx.send(f"Could not create invite: `{e}`", delete_after=12)

    reg = load_migration_registry()
    key = str(PRIMARY_GUILD_ID)
    existing = set(reg.get(key, []))
    added = 0
    for m in targets:
        if m.id not in existing:
            existing.add(m.id)
            added += 1
    reg[key] = sorted(existing)
    save_migration_registry_sync(reg)

    dm_ok = 0
    for m in targets:
        try:
            await m.send(
                f"Migration invite for **{guild.name}**:\n{invite.url}\n"
                f"Verification channel: <#{MIGRATION_VERIFY_CHANNEL_ID}>"
            )
            dm_ok += 1
        except discord.HTTPException:
            pass

    await ctx.send(
        f"Migration registry updated: **{added}** new / **{len(targets)}** requested. "
        f"DM sent: **{dm_ok}**. Invite: {invite.url}",
        allowed_mentions=discord.AllowedMentions(everyone=False, users=False, roles=False),
    )


@bot.command(name="xsrescan", hidden=True)
async def cmd_xs_rescan(ctx, mode: Optional[str] = None):
    """
    Trusted: scan server message history and rebuild chat XP (same cooldown + command rules as live).
    Default: per user, new_xp = max(current, scanned). Use `6xsrescan replace` to set XP from history only
    for users who had eligible messages (others unchanged). May take a long time; bot needs history access.
    """
    if not ctx.guild:
        return await ctx.send("Use in a server.", delete_after=6)
    if not is_trusted(ctx.author):
        return await ctx.send("Trusted users only.", delete_after=6)

    replace = (mode or "").lower() == "replace"
    if mode and not replace:
        return await ctx.send("Usage: `6xsrescan` or `6xsrescan replace`.", delete_after=10)

    if _xs_rescan_lock.locked():
        return await ctx.send("A 6xs rescan is already running.", delete_after=8)

    status = await ctx.send("6xs rescan starting…")

    async def bump(text: str) -> None:
        try:
            await status.edit(content=text[:2000])
        except discord.HTTPException:
            pass

    async with _xs_rescan_lock:
        await bump("6xs rescan: scanning channels (this can take a while)…")
        computed = await _six_xs_compute_from_history(ctx.guild, ctx.bot, bump)
        gid = ctx.guild.id
        role_updates: list[tuple[discord.Member, int, int]] = []

        async with _six_xs_lock:
            for uid, (scanned_xp, last_ts, _n) in computed.items():
                key = f"{gid}:{uid}"
                prev = six_xs_data.get(key, {"xp": 0, "last_msg": 0.0})
                old_raw = int(prev.get("xp", 0))
                _, old_lvl = total_xp_and_6xs(old_raw)
                new_raw = scanned_xp if replace else max(old_raw, scanned_xp)
                _, new_lvl = total_xp_and_6xs(new_raw)
                if replace:
                    new_lm = last_ts
                else:
                    new_lm = max(float(prev.get("last_msg", 0.0)), last_ts)
                six_xs_data[key] = {"xp": new_raw, "last_msg": new_lm}
                m = ctx.guild.get_member(uid)
                if m and not m.bot and new_lvl > old_lvl:
                    role_updates.append((m, old_lvl, new_lvl))

            save_six_xs_sync()

    for m, old_lvl, new_lvl in role_updates:
        try:
            await grant_six_xs_roles(m, old_lvl, new_lvl)
        except Exception as e:
            print(f"[6XS] xsrescan grant roles {m.id}: {e}")

    mode_note = "replace" if replace else "max(existing, scanned)"
    await bump(
        f"6xs rescan done — **{len(computed):,}** users with eligible messages, "
        f"**{sum(t[2] for t in computed.values()):,}** XP events. Mode: **{mode_note}**."
    )


@bot.event
async def on_message(message: discord.Message):
    if not message.author.bot and message.guild is None:
        try:
            handled = await maybe_handle_uplift_dm_reply(message)
            if handled:
                return
        except Exception as e:
            print(f"[UPLIFT DM] {e}")
    link_blocked = False
    if not message.author.bot and message.guild:
        try:
            link_blocked = await maybe_punish_discord_link(message)
        except Exception as e:
            print(f"[LINK_WARN] {e}")
    if not message.author.bot and message.guild:
        try:
            await respond_archive_record_message(message)
        except Exception as e:
            print(f"[6respond archive] {e}")
    mirrored = False
    if not message.author.bot and message.guild:
        try:
            mirrored = await maybe_mirror_auto_wipe_channel_video(message)
        except Exception as e:
            print(f"[AUTO_WIPE_MIRROR] {e}")
    _gen_catch = resolve_general_catch_channel_id(message.guild) if message.guild else None
    skip_xs_for_catch = (
        message.guild
        and message.content.strip().lower() == "catch"
        and (
            message.channel.id == HOLDING_CELL_CHANNEL_ID
            or (_gen_catch is not None and message.channel.id == _gen_catch)
        )
    )
    if (
        not link_blocked
        and not message.author.bot
        and message.guild
        and not mirrored
        and not skip_xs_for_catch
    ):
        try:
            if await _six_xs_message_counts_as_chat(bot, message):
                await maybe_award_six_xs(message)
        except Exception as e:
            print(f"[6XS] maybe_award_six_xs: {e}")
    if message.guild and not message.author.bot and not message.webhook_id:
        try:
            _st = bot.get_cog("MessageStatsCog")
            if _st is not None:
                await _st.record_message(message)
        except Exception as e:
            print(f"[6stats] {e}")
    if message.guild and not message.author.bot:
        try:
            await maybe_send_uplift_dm(message)
        except Exception as e:
            print(f"[UPLIFT] {e}")
    await bot.process_commands(message)


async def _six_xs_startup_backfill_roles_task() -> None:
    """Grant missing milestone roles from saved chat XP + enqueue 6reward rows (no DMs)."""
    await bot.wait_until_ready()
    await asyncio.sleep(12.0)
    hc = bot.get_cog("HoldingCellCog")
    try:
        async with _six_xs_lock:
            changed = False
            for guild in bot.guilds:
                for member in guild.members:
                    if member.bot:
                        continue
                    key = f"{guild.id}:{member.id}"
                    entry = six_xs_data.setdefault(key, {"xp": 0, "last_msg": 0.0})
                    if _six_xs_sync_raw_to_milestone_roles(entry, member):
                        changed = True
            if changed:
                save_six_xs_sync()

        for guild in bot.guilds:
            gid = guild.id
            prefix = f"{gid}:"
            for key, entry in list(six_xs_data.items()):
                if not key.startswith(prefix):
                    continue
                try:
                    uid = int(key.split(":", 1)[1])
                except (IndexError, ValueError):
                    continue
                member = guild.get_member(uid)
                if member is None or member.bot:
                    continue
                raw = int(entry.get("xp", 0))
                _, chat_lvl = total_xp_and_6xs(raw)
                try:
                    await grant_six_xs_roles(member, 0, chat_lvl)
                except Exception as e:
                    print(f"[6XS] startup role backfill uid={uid}: {e}")
                if hc is not None:
                    try:
                        await hc._sync_milestone_backlog(uid, gid, member)
                    except Exception as e:
                        print(f"[6XS] startup reward backlog uid={uid}: {e}")
                await asyncio.sleep(0.15)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[6XS] startup backfill task: {e}")


@bot.event
async def on_ready():
    global _auto_wipe_task_started, _dead_chat_task_started, _six_xs_backfill_task_started
    load_posted_pins()
    load_six_xs()
    load_six_xs_boost()
    load_discord_link_warns()
    _uplift_load_state()
    print(f"✅ {bot.user} is online | Prefix: {PREFIX} or \"{PREFIX} \" (space)")
    print("   Anti-raid, anti-nuke, repost, Pinterest autosave, 6xs/6boost/xslb, 6stats, economy, personality, holding cell")
    bot.loop.create_task(auto_check_pinterest())
    if not _auto_wipe_task_started:
        _auto_wipe_task_started = True
        bot.loop.create_task(auto_wipe_channel_loop())
        print(f"   Auto-wipe hourly countdown: channel {AUTO_WIPE_CHANNEL_ID}")
    if not _dead_chat_task_started:
        _dead_chat_task_started = True
        bot.loop.create_task(dead_chat_revival_loop())
        print(
            f"   Dead-chat revival: channel {DEAD_CHAT_CHANNEL_ID} "
            f"(idle ≥{DEAD_CHAT_IDLE_MINUTES}m, cooldown {DEAD_CHAT_COOLDOWN_MINUTES}m)"
        )
    econ = sum(1 for c in bot.walk_commands() if c.cog and c.cog.__class__.__name__ == "EconomyCog")
    print(f"   Economy commands registered: {econ}")
    if not _six_xs_backfill_task_started:
        _six_xs_backfill_task_started = True
        bot.loop.create_task(_six_xs_startup_backfill_roles_task())


@bot.event
async def on_command_error(ctx: commands.Context, error: Exception) -> None:
    if isinstance(error, commands.CommandNotFound):
        return
    if isinstance(error, commands.NoPrivateMessage):
        await ctx.send("❌ That command only works inside a server.")
        return
    cmd = ctx.command
    if cmd and cmd.name in ("purge", "clear"):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(
                f"Usage: **`{PREFIX}purge <amount>`** — deletes that many recent messages "
                f"({PURGE_AMOUNT_MIN}–{PURGE_AMOUNT_MAX}). Alias: **`{PREFIX}clear`**.",
                delete_after=12,
            )
            return
        if isinstance(error, commands.BadArgument):
            await ctx.send(
                f"Give a whole number between **{PURGE_AMOUNT_MIN}** and **{PURGE_AMOUNT_MAX}**.",
                delete_after=10,
            )
            return
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need **Manage Messages** to use **`6purge`**.", delete_after=10)
            return
        if isinstance(error, commands.BotMissingPermissions):
            await ctx.send(
                "I need **Manage Messages** and **Read Message History** in this channel.",
                delete_after=12,
            )
            return
    if cmd and cmd.name == "load":
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(
                "Usage: **`6load <name>`** — restores the **latest** **`6save`** with that name. "
                "Use **`6save`** (no args) to list snapshots + dates.",
                delete_after=18,
            )
            return
    if cmd and cmd.name == "send":
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(
                "**`6send #channel`** (reply to media), or **`6send <msg url> <msg url> #channel`** (range), "
                "then end with the **destination** channel.",
                delete_after=18,
            )
            return
    raise error


def _maybe_seed_economy_to_supabase_at_startup() -> None:
    """If RUN_ECONOMY_SEED_ON_START=1, upsert economy_data.json → Supabase before connecting (panel-friendly)."""
    flag = os.getenv("RUN_ECONOMY_SEED_ON_START", "").strip().lower()
    if flag not in ("1", "true", "yes"):
        return
    seed_path = _SCRIPT_ROOT / "seed_economy_to_supabase.py"
    if not seed_path.is_file():
        print(
            "[economy] RUN_ECONOMY_SEED_ON_START set but seed_economy_to_supabase.py is missing — skipping seed."
        )
        return
    import importlib.util

    spec = importlib.util.spec_from_file_location("_economy_seed_supabase", seed_path)
    if spec is None or spec.loader is None:
        print("[economy] Could not load seed module — skipping.")
        return
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    code = int(mod.main())
    if code != 0:
        raise SystemExit(code)
    print("[economy] Supabase seed finished (unset RUN_ECONOMY_SEED_ON_START after first success if you want).")


_maybe_seed_economy_to_supabase_at_startup()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN in environment.")
bot.run(DISCORD_TOKEN)