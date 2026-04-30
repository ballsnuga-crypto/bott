"""
The Holding Cell — spawn/catch card game, cell currency, role shop, sharpshooters.
"""
from __future__ import annotations

import asyncio
import os
import random
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import discord
from discord.ext import commands

# --- Channel & paths ---
def _env_channel_id(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


HOLDING_CELL_CHANNEL_ID = _env_channel_id("HOLDING_CELL_CHANNEL_ID", 1487660007011909784)
# Optional: set to #general’s ID so `catch` works there too. If 0, uses the text channel named "general".
GENERAL_CATCH_CHANNEL_ID = _env_channel_id("GENERAL_CATCH_CHANNEL_ID", 0)
IMAGES_ROOT = Path(__file__).resolve().parent / "images"

# 6shoot: max once per member per guild in this window (seconds)
SHOOT_COOLDOWN_SEC = 3600

# Cell upgrades (vs ~1M top shop tier — expensive but reachable)
DEFENSE_LEVEL_MAX = 5
HASTE_LEVEL_MAX = 5
DEFENSE_UPGRADE_COSTS: tuple[int, ...] = (28_000, 75_000, 195_000, 420_000, 850_000)
HASTE_UPGRADE_COSTS: tuple[int, ...] = (40_000, 110_000, 275_000, 625_000, 1_100_000)


def _defense_block_chance(level: int) -> float:
    """Chance to fully block a **6shoot** hit (no stun, no siphon)."""
    lv = max(0, min(DEFENSE_LEVEL_MAX, level))
    if lv <= 0:
        return 0.0
    return min(0.62, 0.08 + lv * 0.108)


def _haste_income_multiplier(level: int) -> float:
    lv = max(0, min(HASTE_LEVEL_MAX, level))
    return 1.0 + lv * 0.12


def resolve_general_catch_channel_id(guild: Optional[discord.Guild]) -> Optional[int]:
    """Text channel where `catch` is allowed besides the holding cell (usually #general)."""
    if guild is None:
        return None
    if GENERAL_CATCH_CHANNEL_ID:
        return GENERAL_CATCH_CHANNEL_ID
    gen = discord.utils.get(guild.text_channels, name="general")
    return gen.id if gen else None


def resolve_catch_text_channel_ids(guild: Optional[discord.Guild]) -> frozenset[int]:
    ids: set[int] = {HOLDING_CELL_CHANNEL_ID}
    g = resolve_general_catch_channel_id(guild)
    if g:
        ids.add(g)
    return frozenset(ids)


# Timer spawns: random delay between these (seconds)
SPAWN_INTERVAL_MIN_SEC = 30 * 60
SPAWN_INTERVAL_MAX_SEC = 80 * 60

# Activity spawns: human messages in the guild since last spawn; not too often
ACTIVITY_MESSAGES_THRESHOLD = 55
ACTIVITY_MIN_GAP_SEC = 11 * 60
ACTIVITY_TRIGGER_CHANCE = 0.13

# Spawn pool is built at startup: only names that have at least one card image on disk.
CHARACTER_CANDIDATES: tuple[str, ...] = (
    "ishowspeed",
    "mrbeast",
    "quackity",
    "cookieking",
    "arthur",
    "elliot",
    "ryaxrise",
    "brianmoser",
    "walterwhite",
    "adam",
    "epstein",
)

# variation -> (rarity_key, spawn_weight)
VARIATION_META: dict[str, tuple[str, int]] = {
    "god": ("mythic", 3),
    "serialkiller": ("legendary", 8),
    "evil": ("epic", 18),
    "alien": ("rare", 28),
    "troll": ("rare", 28),
    "egirl": ("uncommon", 42),
    "rapper": ("uncommon", 42),
    "artist": ("uncommon", 42),
    "professor": ("common", 65),
    "engineer": ("common", 65),
    "teacher": ("common", 65),
    "worker": ("common", 65),
}

# income per card per real hour (absurdity scale)
VARIATION_INCOME_PER_HOUR: dict[str, int] = {
    "god": 1400,
    "serialkiller": 900,
    "evil": 550,
    "alien": 300,
    "troll": 300,
    "egirl": 160,
    "rapper": 160,
    "artist": 160,
    "professor": 70,
    "engineer": 70,
    "teacher": 70,
    "worker": 55,
}

RARITY_EMBED_COLORS: dict[str, discord.Color] = {
    "mythic": discord.Color.gold(),
    "legendary": discord.Color.purple(),
    "epic": discord.Color.dark_magenta(),
    "rare": discord.Color.blue(),
    "uncommon": discord.Color.green(),
    "common": discord.Color.dark_gray(),
}

CHAR_DISPLAY: dict[str, str] = {
    "ishowspeed": "Ishowspeed",
    "mrbeast": "MrBeast",
    "quackity": "Quackity",
    "cookieking": "CookieKing",
    "arthur": "Arthur",
    "elliot": "Elliot",
    "ryaxrise": "Ryaxrise",
    "brianmoser": "Brian Moser",
    "walterwhite": "Walter White",
    "adam": "Adam",
    "epstein": "Epstein",
}

SHARPSHOOTER_CHARS = frozenset({"arthur", "elliot", "adam"})

DB_PATH = Path(__file__).resolve().parent / "holding_cell.db"


def _variation_display(variation: str) -> str:
    m = {
        "serialkiller": "Serial Killer",
        "egirl": "E-Girl",
    }
    return m.get(variation, variation.replace("_", " ").title())


def _pick_weighted_variation() -> str:
    keys = list(VARIATION_META.keys())
    weights = [VARIATION_META[k][1] for k in keys]
    return random.choices(keys, weights=weights, k=1)[0]


# Tried in order; `.png` covers assets like `images/brianmoser/brianmoser_professor.png`.
_CARD_ART_EXTENSIONS: tuple[str, ...] = (".jpg", ".jpeg", ".png", ".webp")
_CARD_ART_EXT_SET = frozenset(e.lower() for e in _CARD_ART_EXTENSIONS)


def _resolve_card_image_path(character: str, variation: str) -> Path:
    """
    Find card art under `images/` (flat or per-character subfolder).
    Supports .jpg / .jpeg / .png / .webp; folder and file names are matched case-insensitively.
    """
    stem = f"{character}_{variation}"
    char_lower = character.lower()
    stem_lower = stem.lower()
    fallback = IMAGES_ROOT / f"{stem}.jpg"

    for ext in _CARD_ART_EXTENSIONS:
        p = IMAGES_ROOT / f"{stem}{ext}"
        if p.is_file():
            return p
        p = IMAGES_ROOT / character / f"{stem}{ext}"
        if p.is_file():
            return p

    if not IMAGES_ROOT.is_dir():
        return fallback
    try:
        for child in IMAGES_ROOT.iterdir():
            if not child.is_dir() or child.name.lower() != char_lower:
                continue
            for ext in _CARD_ART_EXTENSIONS:
                p = child / f"{stem}{ext}"
                if p.is_file():
                    return p
            for f in child.iterdir():
                if f.is_file() and f.stem.lower() == stem_lower and f.suffix.lower() in _CARD_ART_EXT_SET:
                    return f
    except OSError:
        pass
    return fallback


def _character_has_any_card_image(character: str) -> bool:
    return any(_resolve_card_image_path(character, v).is_file() for v in VARIATION_META)


def _characters_with_any_card_image() -> tuple[str, ...]:
    return tuple(c for c in CHARACTER_CANDIDATES if _character_has_any_card_image(c))


def _pick_spawn_character_variation(roster: tuple[str, ...]) -> tuple[str, str]:
    """Prefer a (character, variation) pair that exists on disk; keeps variation rarity weights."""
    pool: tuple[str, ...] = roster if roster else CHARACTER_CANDIDATES
    for _ in range(64):
        c = random.choice(pool)
        v = _pick_weighted_variation()
        if _resolve_card_image_path(c, v).is_file():
            return c, v
    c = random.choice(pool)
    return c, _pick_weighted_variation()


def _catch_xp_for_rarity(rarity: str) -> int:
    return {
        "mythic": 120,
        "legendary": 95,
        "epic": 75,
        "rare": 55,
        "uncommon": 35,
        "common": 18,
    }.get(rarity, 20)


def _six_xs_runtime_module() -> Any:
    """
    Return the module that holds six_xs_data (same process as the running bot).

    If the app was started with `python index.py`, it is loaded as __main__, not `index`.
    Doing `import index` then re-executes index.py and calls bot.run() again →
    RuntimeError: asyncio.run() cannot be called from a running event loop.
    """
    idx = sys.modules.get("index")
    if idx is not None and hasattr(idx, "six_xs_data"):
        return idx
    main = sys.modules.get("__main__")
    if main is not None and hasattr(main, "six_xs_data"):
        return main
    import importlib

    return importlib.import_module("index")


def _sixxs_reward_milestone_keys() -> list[int]:
    """Same milestones as cosmetic `SIX_XS_ROLES` in `index` (includes 31+ from `six_xs_role_ids_extra.json`)."""
    try:
        six = _six_xs_runtime_module()
        roles = getattr(six, "SIX_XS_ROLES", None)
        if not isinstance(roles, dict):
            return [3, 5, 10, 15, 20, 25, 30]
        return sorted(int(k) for k in roles.keys())
    except Exception:
        return [3, 5, 10, 15, 20, 25, 30]


def _sixxs_reward_tiers_hint() -> str:
    ks = _sixxs_reward_milestone_keys()
    if not ks:
        return "**none** configured"
    if len(ks) <= 12:
        return "**" + "**, **".join(str(x) for x in ks) + "**"
    return (
        f"**{ks[0]}** … **{ks[-1]}** (**{len(ks)}** tiers — same as **6xs** milestone roles; "
        f"levels **31+** come from **`six_xs_role_ids_extra.json`**)"
    )


class HoldingCellCog(commands.Cog):
    """Spawn loop, catch race, currency, shop, sharpshooters."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._db_lock = asyncio.Lock()
        self._spawn_lock = asyncio.Lock()
        self._activity_lock = asyncio.Lock()
        self._active_spawn: Optional[dict[str, Any]] = None
        self._spawn_task: Optional[asyncio.Task[None]] = None
        self._last_any_spawn_ts: float = time.time()
        self._activity_counts: defaultdict[int, int] = defaultdict(int)
        self._reward_cmd_lock = asyncio.Lock()
        self._spawn_characters: tuple[str, ...] = ()

    async def cog_load(self) -> None:
        IMAGES_ROOT.mkdir(parents=True, exist_ok=True)
        await self._init_db()
        discovered = _characters_with_any_card_image()
        self._spawn_characters = discovered if discovered else CHARACTER_CANDIDATES
        if discovered:
            print(f"[HOLDING_CELL] Spawn roster ({len(discovered)} with images): {', '.join(discovered)}")
        else:
            print("[HOLDING_CELL] No images under images/ — using full candidate roster (may show missing assets).")
        self._last_any_spawn_ts = time.time()
        self._spawn_task = asyncio.create_task(self._spawn_loop())

    async def cog_unload(self) -> None:
        if self._spawn_task:
            self._spawn_task.cancel()
            try:
                await self._spawn_task
            except asyncio.CancelledError:
                pass

    async def cog_command_error(self, ctx: commands.Context, error: Exception) -> None:
        err = error.original if isinstance(error, commands.CommandInvokeError) and error.original else error
        if ctx.command and ctx.command.name == "shoot" and isinstance(err, commands.CommandOnCooldown):
            r = err.retry_after
            m, s = int(r // 60), int(r % 60)
            await ctx.send(f"**6shoot** is on cooldown — **{m}m {s}s** left.", delete_after=12)
            return
        raise error

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    async def _init_db(self) -> None:
        def _run() -> None:
            c = self._connect()
            try:
                c.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS user_stats (
                        user_id INTEGER NOT NULL,
                        guild_id INTEGER NOT NULL,
                        total_currency INTEGER NOT NULL DEFAULT 0,
                        last_claim_ts REAL NOT NULL,
                        fastest_catch_ms REAL,
                        slowest_catch_ms REAL,
                        cattlepass_xp INTEGER NOT NULL DEFAULT 0,
                        income_stunned_until REAL NOT NULL DEFAULT 0,
                        defense_level INTEGER NOT NULL DEFAULT 0,
                        haste_level INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (user_id, guild_id)
                    );
                    CREATE TABLE IF NOT EXISTS user_inventory (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        guild_id INTEGER NOT NULL,
                        character TEXT NOT NULL,
                        variation TEXT NOT NULL,
                        catch_time_ms REAL NOT NULL,
                        caught_at REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_inv_user ON user_inventory(user_id, guild_id);
                    CREATE TABLE IF NOT EXISTS milestone_rewards (
                        user_id INTEGER NOT NULL,
                        guild_id INTEGER NOT NULL,
                        milestone INTEGER NOT NULL,
                        created_at REAL NOT NULL,
                        PRIMARY KEY (user_id, guild_id, milestone)
                    );
                    CREATE TABLE IF NOT EXISTS milestone_claimed (
                        user_id INTEGER NOT NULL,
                        guild_id INTEGER NOT NULL,
                        milestone INTEGER NOT NULL,
                        claimed_at REAL NOT NULL,
                        PRIMARY KEY (user_id, guild_id, milestone)
                    );
                    """
                )
                # --- migrations (new columns on existing DBs) ---
                cur = c.execute("PRAGMA table_info(user_stats)")
                col_names = {row[1] for row in cur.fetchall()}
                for col_name, ddl in (
                    ("defense_level", "INTEGER NOT NULL DEFAULT 0"),
                    ("haste_level", "INTEGER NOT NULL DEFAULT 0"),
                ):
                    if col_name not in col_names:
                        c.execute(f"ALTER TABLE user_stats ADD COLUMN {col_name} {ddl}")
                c.commit()
            finally:
                c.close()

        await asyncio.to_thread(_run)

    async def _db_exec(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        async with self._db_lock:

            def _run() -> None:
                c = self._connect()
                try:
                    c.execute(sql, params)
                    c.commit()
                finally:
                    c.close()

            await asyncio.to_thread(_run)

    async def _db_fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
        async with self._db_lock:

            def _run() -> Optional[sqlite3.Row]:
                c = self._connect()
                try:
                    cur = c.execute(sql, params)
                    return cur.fetchone()
                finally:
                    c.close()

            return await asyncio.to_thread(_run)

    async def _db_fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        async with self._db_lock:

            def _run() -> list[sqlite3.Row]:
                c = self._connect()
                try:
                    cur = c.execute(sql, params)
                    return list(cur.fetchall())
                finally:
                    c.close()

            return await asyncio.to_thread(_run)

    async def _ensure_stats(self, user_id: int, guild_id: int) -> None:
        row = await self._db_fetchone(
            "SELECT 1 FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (user_id, guild_id),
        )
        if row is None:
            await self._db_exec(
                "INSERT INTO user_stats (user_id, guild_id, last_claim_ts) VALUES (?, ?, ?)",
                (user_id, guild_id, time.time()),
            )

    async def _apply_catch(
        self,
        member: discord.Member,
        guild_id: int,
        character: str,
        variation: str,
        rarity: str,
        catch_time_ms: float,
    ) -> int:
        """Insert inventory row, update catch stats / Cattlepass XP. Returns XP gained."""
        uid = member.id
        await self._ensure_stats(uid, guild_id)
        xp_gain = _catch_xp_for_rarity(rarity)

        row = await self._db_fetchone(
            "SELECT fastest_catch_ms, slowest_catch_ms, cattlepass_xp FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, guild_id),
        )
        assert row is not None
        fastest = row["fastest_catch_ms"]
        slowest = row["slowest_catch_ms"]
        new_fast = catch_time_ms if fastest is None else min(fastest, catch_time_ms)
        new_slow = catch_time_ms if slowest is None else max(slowest, catch_time_ms)

        await self._db_exec(
            """INSERT INTO user_inventory (user_id, guild_id, character, variation, catch_time_ms, caught_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (uid, guild_id, character, variation, catch_time_ms, time.time()),
        )
        await self._db_exec(
            """UPDATE user_stats SET fastest_catch_ms = ?, slowest_catch_ms = ?, cattlepass_xp = cattlepass_xp + ?
               WHERE user_id = ? AND guild_id = ?""",
            (new_fast, new_slow, xp_gain, uid, guild_id),
        )
        return xp_gain

    def _image_path(self, character: str, variation: str) -> Path:
        return _resolve_card_image_path(character, variation)

    async def _get_holding_cell_text_channel(self) -> Optional[discord.TextChannel]:
        cid = HOLDING_CELL_CHANNEL_ID
        ch = self.bot.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            return ch
        try:
            fetched = await self.bot.fetch_channel(cid)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
            print(f"[HOLDING_CELL] fetch_channel {cid} failed: {e}")
            return None
        if isinstance(fetched, discord.TextChannel):
            return fetched
        print(f"[HOLDING_CELL] Channel {cid} is not a text channel.")
        return None

    async def _spawn_once(self) -> bool:
        ch = await self._get_holding_cell_text_channel()
        if ch is None:
            print(f"[HOLDING_CELL] Channel {HOLDING_CELL_CHANNEL_ID} not available (cache/API).")
            return False

        async with self._spawn_lock:
            self._active_spawn = None

        character, variation = _pick_spawn_character_variation(self._spawn_characters)
        rarity_key = VARIATION_META[variation][0]
        img = self._image_path(character, variation)

        title = f"[{rarity_key.upper()}] {CHAR_DISPLAY.get(character, character.title())}"
        gen_note = ""
        gen_id = resolve_general_catch_channel_id(ch.guild)
        if gen_id and gen_id != ch.id:
            gen_note = f"\n\n_Type `catch` here or in <#{gen_id}> — same cell._"
        desc = f"**{_variation_display(variation)}** · type `catch` to claim!{gen_note}"
        color = RARITY_EMBED_COLORS.get(rarity_key, discord.Color.blurple())
        em = discord.Embed(title=title, description=desc, color=color)

        spawned_at = time.time()
        try:
            if img.is_file():
                f = discord.File(img, filename=img.name)
                msg = await ch.send(embed=em, file=f)
            else:
                em.set_footer(text=f"Missing asset: `{img.name}` — spawn still valid.")
                msg = await ch.send(embed=em)
        except discord.HTTPException as e:
            print(f"[HOLDING_CELL] spawn send failed: {e}")
            return False

        async with self._spawn_lock:
            self._active_spawn = {
                "message_id": msg.id,
                "channel_id": ch.id,
                "spawned_at": spawned_at,
                "character": character,
                "variation": variation,
                "rarity": rarity_key,
                "settled": False,
            }
        gid = ch.guild.id
        async with self._activity_lock:
            self._last_any_spawn_ts = time.time()
            self._activity_counts[gid] = 0
        print(f"[HOLDING_CELL] Spawned {character}_{variation} ({rarity_key}) msg={msg.id}")
        return True

    async def _spawn_loop(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                delay = random.uniform(SPAWN_INTERVAL_MIN_SEC, SPAWN_INTERVAL_MAX_SEC)
                await asyncio.sleep(delay)
                if self.bot.is_closed():
                    break
                await self._spawn_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[HOLDING_CELL] spawn_loop error: {e}")
                await asyncio.sleep(60)

    async def _maybe_activity_spawn(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return
        gid = message.guild.id
        should_spawn = False
        async with self._activity_lock:
            now = time.time()
            self._activity_counts[gid] += 1
            if now - self._last_any_spawn_ts < ACTIVITY_MIN_GAP_SEC:
                return
            if self._activity_counts[gid] < ACTIVITY_MESSAGES_THRESHOLD:
                return
            if random.random() > ACTIVITY_TRIGGER_CHANCE:
                return
            self._activity_counts[gid] = 0
            should_spawn = True
        if should_spawn:
            await self._spawn_once()

    def _read_chat_6xs_level(self, guild_id: int, user_id: int) -> int:
        six = _six_xs_runtime_module()
        raw = int(six.six_xs_data.get(f"{guild_id}:{user_id}", {}).get("xp", 0))
        _, lvl = six.total_xp_and_6xs(raw)
        return int(lvl)

    def _read_effective_6xs_level(
        self, guild_id: int, user_id: int, member: Optional[discord.Member]
    ) -> int:
        """Same basis as `6xs` rank: max(chat XP level, cosmetic role milestones)."""
        chat = self._read_chat_6xs_level(guild_id, user_id)
        if member is None:
            return chat
        six = _six_xs_runtime_module()
        role_hi = six._member_6xs_milestone_from_roles(member)
        return max(chat, int(role_hi))

    async def _sync_milestone_backlog(
        self, user_id: int, guild_id: int, member: Optional[discord.Member] = None
    ) -> int:
        """Grant pending rows for every reward tier at or below effective 6xs rank (chat + roles)."""
        lvl = self._read_effective_6xs_level(guild_id, user_id, member)
        added = 0
        for m in _sixxs_reward_milestone_keys():
            if m > lvl:
                break
            cl = await self._db_fetchone(
                "SELECT 1 FROM milestone_claimed WHERE user_id = ? AND guild_id = ? AND milestone = ?",
                (user_id, guild_id, m),
            )
            if cl is not None:
                continue
            pe = await self._db_fetchone(
                "SELECT 1 FROM milestone_rewards WHERE user_id = ? AND guild_id = ? AND milestone = ?",
                (user_id, guild_id, m),
            )
            if pe is not None:
                continue
            if await self._try_insert_milestone_reward(user_id, guild_id, m):
                added += 1
        return added

    async def try_grant_sixxs_milestone(
        self,
        member: discord.Member,
        channel: discord.abc.Messageable,
        new_level: int,
    ) -> None:
        uid, gid = member.id, member.guild.id
        added = await self._sync_milestone_backlog(uid, gid, member)
        if added <= 0:
            return
        tip = (
            f"You have **{added}** holding cell reward(s) ready (6xs milestones you’ve earned). "
            f"Run **`6reward`** in the server — the card goes **straight to your inventory**."
        )
        # No proactive reward DMs: keep rewards in-server only.
        try:
            await channel.send(
                f"{member.mention} — **{added}** **`6reward`** waiting. Use **`6reward`** to claim.",
                delete_after=90,
            )
        except discord.HTTPException:
            pass

    async def _try_insert_milestone_reward(self, user_id: int, guild_id: int, milestone: int) -> bool:
        async with self._db_lock:

            def _run() -> bool:
                c = self._connect()
                try:
                    if (
                        c.execute(
                            "SELECT 1 FROM milestone_claimed WHERE user_id = ? AND guild_id = ? AND milestone = ?",
                            (user_id, guild_id, milestone),
                        ).fetchone()
                        is not None
                    ):
                        return False
                    c.execute(
                        """INSERT OR IGNORE INTO milestone_rewards (user_id, guild_id, milestone, created_at)
                           VALUES (?, ?, ?, ?)""",
                        (user_id, guild_id, milestone, time.time()),
                    )
                    ok = c.total_changes > 0
                    c.commit()
                    return ok
                finally:
                    c.close()

            return await asyncio.to_thread(_run)

    async def _member_for_6xs_rewards(self, ctx: commands.Context) -> Optional[discord.Member]:
        """Fresh Member with roles (matches `6xs` / leaderboard); avoids stale cache skipping milestones."""
        guild = ctx.guild
        if guild is None:
            return None
        uid = ctx.author.id
        try:
            return await guild.fetch_member(uid)
        except discord.NotFound:
            return guild.get_member(uid)
        except discord.HTTPException:
            m = guild.get_member(uid)
            if m is not None:
                return m
            return ctx.author if isinstance(ctx.author, discord.Member) else None

    @commands.command(name="reward", aliases=["cellreward", "hreward"])
    async def cmd_reward(self, ctx: commands.Context) -> None:
        """Spend one pending 6xs milestone reward — card goes straight to your inventory."""
        if not ctx.guild:
            return await ctx.send("Use in a server.", delete_after=6)
        async with self._reward_cmd_lock:
            uid, gid = ctx.author.id, ctx.guild.id
            mem = await self._member_for_6xs_rewards(ctx)
            await self._sync_milestone_backlog(uid, gid, mem)
            row = await self._db_fetchone(
                """SELECT milestone FROM milestone_rewards
                   WHERE user_id = ? AND guild_id = ? ORDER BY created_at ASC LIMIT 1""",
                (uid, gid),
            )
            if row is None:
                chat_lv = self._read_chat_6xs_level(gid, uid)
                eff_lv = self._read_effective_6xs_level(gid, uid, mem)
                role_lv = 1
                if mem is not None:
                    six = _six_xs_runtime_module()
                    role_lv = int(six._member_6xs_milestone_from_roles(mem))
                return await ctx.send(
                    "No reward pending. Your **6xs** for rewards is **"
                    f"{eff_lv}** (chat **{chat_lv}**, roles **{role_lv}** — same as `6xs` rank). "
                    f"Reward tiers: {_sixxs_reward_tiers_hint()}. If you already used **`6reward`** for each, you’re caught up.",
                    delete_after=18,
                )
            ms = int(row["milestone"])
            if not isinstance(ctx.author, discord.Member):
                return await ctx.send("Could not resolve your member profile.", delete_after=8)
            mem = ctx.author

            character, variation = _pick_spawn_character_variation(self._spawn_characters)
            rarity_key = VARIATION_META[variation][0]
            img = self._image_path(character, variation)

            await self._db_exec(
                "DELETE FROM milestone_rewards WHERE user_id = ? AND guild_id = ? AND milestone = ?",
                (uid, gid, ms),
            )
            await self._db_exec(
                "INSERT OR REPLACE INTO milestone_claimed (user_id, guild_id, milestone, claimed_at) VALUES (?, ?, ?, ?)",
                (uid, gid, ms, time.time()),
            )

            try:
                xp_gain = await self._apply_catch(mem, gid, character, variation, rarity_key, 0.0)
            except Exception as e:
                print(f"[HOLDING_CELL] 6reward apply_catch: {e}")
                return await ctx.send(
                    "Milestone was marked used but the card **failed** to save — ping staff. "
                    f"**6xs {ms}** · `{character}_{variation}`.",
                    delete_after=25,
                )

            title = f"[{rarity_key.upper()}] {CHAR_DISPLAY.get(character, character.title())}"
            desc = (
                f"**{_variation_display(variation)}** · **6reward** milestone **6xs {ms}**\n"
                f"{mem.mention} — added to inventory · +**{xp_gain}** Cattlepass™ XP"
            )
            color = RARITY_EMBED_COLORS.get(rarity_key, discord.Color.blurple())
            em = discord.Embed(title=title, description=desc, color=color)
            try:
                if img.is_file():
                    await ctx.send(embed=em, file=discord.File(img, filename=img.name))
                else:
                    em.set_footer(text=f"Missing asset: `{img.name}`")
                    await ctx.send(embed=em)
            except discord.HTTPException as e:
                print(f"[HOLDING_CELL] 6reward send: {e}")
                await ctx.send(
                    f"{mem.mention} — **{CHAR_DISPLAY.get(character, character)}** "
                    f"({_variation_display(variation)}) saved · +**{xp_gain}** XP (couldn’t attach image).",
                    delete_after=25,
                )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or not message.guild:
            return
        await self._maybe_activity_spawn(message)

        if message.content.strip().lower() != "catch":
            return
        if message.channel.id not in resolve_catch_text_channel_ids(message.guild):
            return

        async with self._spawn_lock:
            sp = self._active_spawn
            if not sp or sp.get("settled"):
                return
            if sp["channel_id"] != HOLDING_CELL_CHANNEL_ID:
                return
            sp["settled"] = True
            spawn_message_id = sp["message_id"]
            character = sp["character"]
            variation = sp["variation"]
            rarity = sp["rarity"]
            spawned_at = sp["spawned_at"]

        catch_time_ms = max(0.0, (time.time() - spawned_at) * 1000.0)
        author = message.author
        if not isinstance(author, discord.Member):
            async with self._spawn_lock:
                cur = self._active_spawn
                if cur and cur.get("message_id") == spawn_message_id:
                    cur["settled"] = False
            return

        gid = message.guild.id
        try:
            xp_gain = await self._apply_catch(author, gid, character, variation, rarity, catch_time_ms)
        except Exception as e:
            print(f"[HOLDING_CELL] catch apply: {e}")
            async with self._spawn_lock:
                cur = self._active_spawn
                if cur and cur.get("message_id") == spawn_message_id:
                    cur["settled"] = False
            return

        try:
            await message.add_reaction("✅")
        except discord.HTTPException:
            pass

        try:
            await message.channel.send(
                f"{author.mention} caught **{CHAR_DISPLAY.get(character, character)}** "
                f"({_variation_display(variation)}) in **{catch_time_ms / 1000:.2f}s** · "
                f"+**{xp_gain}** Cattlepass™ XP",
                delete_after=25,
            )
        except discord.HTTPException:
            pass

        async with self._spawn_lock:
            cur = self._active_spawn
            if cur and cur.get("message_id") == spawn_message_id:
                self._active_spawn = None

    def _hourly_rate(self, user_id: int, guild_id: int, rows: list[sqlite3.Row]) -> int:
        total = 0
        for r in rows:
            total += VARIATION_INCOME_PER_HOUR.get(r["variation"], 50)
        return total

    def _headshot_multiplier(self, rows: list[sqlite3.Row]) -> float:
        """Sharpshooters: 10% base triple, 25% if evil/serialkiller."""
        mult = 1.0
        for r in rows:
            ch = r["character"]
            var = r["variation"]
            if ch not in SHARPSHOOTER_CHARS:
                continue
            p = 0.25 if var in ("evil", "serialkiller") else 0.10
            if random.random() < p:
                mult = 3.0
                break
        return mult

    @commands.command(name="cellclaim", aliases=["cclaim", "clclaim"])
    async def cmd_cell_claim(self, ctx: commands.Context) -> None:
        """Accrue cell currency from your inventory since last claim (retroactive)."""
        if not ctx.guild:
            return await ctx.send("Use in a server.", delete_after=6)
        uid, gid = ctx.author.id, ctx.guild.id
        await self._ensure_stats(uid, gid)

        row = await self._db_fetchone(
            "SELECT last_claim_ts, income_stunned_until, total_currency, haste_level FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        assert row is not None
        now = time.time()
        if row["income_stunned_until"] and now < row["income_stunned_until"]:
            rem = int(row["income_stunned_until"] - now)
            return await ctx.send(
                f"Your income is **stunned** for **{rem // 3600}h {(rem % 3600) // 60}m** (sharpshooter hit).",
                delete_after=12,
            )

        inv = await self._db_fetchall(
            "SELECT character, variation FROM user_inventory WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        if not inv:
            return await ctx.send("No catches yet — wait for a spawn in the holding cell channel.", delete_after=8)

        rate = self._hourly_rate(uid, gid, inv)
        if rate <= 0:
            return await ctx.send("Nothing to claim.", delete_after=6)

        t0 = row["last_claim_ts"]
        hours = max(0.0, (now - t0) / 3600.0)
        earned = int(rate * hours)
        mult = self._headshot_multiplier(inv)
        h_lv = int(row["haste_level"] or 0)
        h_mult = _haste_income_multiplier(h_lv)
        final = int(earned * mult * h_mult)

        new_bal = row["total_currency"] + final
        await self._db_exec(
            "UPDATE user_stats SET total_currency = ?, last_claim_ts = ? WHERE user_id = ? AND guild_id = ?",
            (new_bal, now, uid, gid),
        )

        bonus = f" · **Headshot!** ×3 on this claim" if mult > 1.0 else ""
        h_note = f" · **Haste** ×{h_mult:.2f}" if h_mult > 1.0 else ""
        await ctx.send(
            f"**+{final:,}** cell ({hours:.2f}h @ **{rate:,}**/hr){bonus}{h_note}\n"
            f"Balance: **{new_bal:,}**",
        )

    @commands.command(name="cellbal", aliases=["cball", "cbal"])
    async def cmd_cell_bal(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            return await ctx.send("Use in a server.", delete_after=6)
        uid, gid = ctx.author.id, ctx.guild.id
        await self._ensure_stats(uid, gid)
        em = await self._build_cell_profile_embed(uid, gid)
        view = CellProfileView(self, gid, uid)
        await ctx.send(embed=em, view=view)

    async def _build_cell_profile_embed(self, uid: int, gid: int) -> discord.Embed:
        row = await self._db_fetchone(
            "SELECT total_currency, cattlepass_xp, fastest_catch_ms, slowest_catch_ms, last_claim_ts, "
            "income_stunned_until, defense_level, haste_level FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        inv = await self._db_fetchall(
            "SELECT COUNT(*) AS c FROM user_inventory WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        n = inv[0]["c"] if inv else 0
        rate_rows = await self._db_fetchall(
            "SELECT character, variation FROM user_inventory WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        rate = self._hourly_rate(uid, gid, rate_rows)
        assert row is not None
        now = time.time()
        stun = ""
        if row["income_stunned_until"] and now < row["income_stunned_until"]:
            stun = f"**Stunned** until <t:{int(row['income_stunned_until'])}:R>"

        fast = row["fastest_catch_ms"]
        slow = row["slowest_catch_ms"]
        fast_s = f"{fast / 1000:.2f}s" if fast is not None else "—"
        slow_s = f"{slow / 1000:.2f}s" if slow is not None else "—"
        d_lv = int(row["defense_level"] or 0)
        h_lv = int(row["haste_level"] or 0)
        eff_rate = int(rate * _haste_income_multiplier(h_lv))
        block_pct = int(round(100 * _defense_block_chance(d_lv)))

        em = discord.Embed(title="Holding Cell — profile", color=discord.Color.dark_teal())
        if stun:
            em.description = stun
        em.add_field(name="Balance", value=f"**{row['total_currency']:,}**", inline=True)
        em.add_field(name="Base income / hr", value=f"**{rate:,}**", inline=True)
        em.add_field(name="Cards", value=str(n), inline=True)
        em.add_field(
            name="🛡️ Defense",
            value=f"**Lv {d_lv}/{DEFENSE_LEVEL_MAX}** · ~**{block_pct}%** block vs **6shoot**",
            inline=True,
        )
        em.add_field(
            name="⚡ Haste",
            value=f"**Lv {h_lv}/{HASTE_LEVEL_MAX}** · **×{_haste_income_multiplier(h_lv):.2f}** claim income",
            inline=True,
        )
        em.add_field(name="Eff. income / hr", value=f"**{eff_rate:,}**", inline=True)
        em.add_field(name="Cattlepass™ XP", value=str(row["cattlepass_xp"]), inline=True)
        em.add_field(name="Fastest catch", value=fast_s, inline=True)
        em.add_field(name="Slowest catch", value=slow_s, inline=True)
        return em

    async def _try_buy_defense(self, uid: int, gid: int) -> tuple[bool, str]:
        await self._ensure_stats(uid, gid)
        row = await self._db_fetchone(
            "SELECT total_currency, defense_level FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        assert row is not None
        lv = int(row["defense_level"] or 0)
        if lv >= DEFENSE_LEVEL_MAX:
            return False, "Defense is already **maxed**."
        cost = DEFENSE_UPGRADE_COSTS[lv]
        bal = int(row["total_currency"])
        if bal < cost:
            return False, f"Need **{cost:,}** cell (you have **{bal:,}**)."
        await self._db_exec(
            "UPDATE user_stats SET total_currency = total_currency - ?, defense_level = defense_level + 1 "
            "WHERE user_id = ? AND guild_id = ?",
            (cost, uid, gid),
        )
        return True, f"Defense **{lv} → {lv + 1}** for **{cost:,}** cell."

    async def _try_buy_haste(self, uid: int, gid: int) -> tuple[bool, str]:
        await self._ensure_stats(uid, gid)
        row = await self._db_fetchone(
            "SELECT total_currency, haste_level FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        assert row is not None
        lv = int(row["haste_level"] or 0)
        if lv >= HASTE_LEVEL_MAX:
            return False, "Haste is already **maxed**."
        cost = HASTE_UPGRADE_COSTS[lv]
        bal = int(row["total_currency"])
        if bal < cost:
            return False, f"Need **{cost:,}** cell (you have **{bal:,}**)."
        await self._db_exec(
            "UPDATE user_stats SET total_currency = total_currency - ?, haste_level = haste_level + 1 "
            "WHERE user_id = ? AND guild_id = ?",
            (cost, uid, gid),
        )
        return True, f"Haste **{lv} → {lv + 1}** for **{cost:,}** cell."

    async def _withdraw_all_cells_to_wallet(self, uid: int, gid: int) -> tuple[bool, str]:
        """Move all **cell** balance into **EconomyCog** wallet (1 cell = 1 coin)."""
        await self._ensure_stats(uid, gid)
        row = await self._db_fetchone(
            "SELECT total_currency FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        assert row is not None
        bal = int(row["total_currency"])
        if bal <= 0:
            return False, "No **cell** to withdraw."
        econ = self.bot.get_cog("EconomyCog")
        if econ is None:
            return False, "Economy isn’t loaded."
        await self._db_exec(
            "UPDATE user_stats SET total_currency = 0 WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        try:
            d = econ._get(gid, uid)
            d["wallet"] = int(d["wallet"]) + bal
            await econ._save()
        except Exception as e:
            await self._db_exec(
                "UPDATE user_stats SET total_currency = ? WHERE user_id = ? AND guild_id = ?",
                (bal, uid, gid),
            )
            return False, f"Could not credit wallet — cell **restored**. Error: `{e}`"
        return True, f"**{bal:,}** cell → **{bal:,}** coins (**wallet**). Use **`6shop`**, **`6gamble`**, etc."

    async def play_panel_cellbal(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Use this in a **server**.", ephemeral=True)
            return
        uid, gid = interaction.user.id, guild.id
        await self._ensure_stats(uid, gid)
        em = await self._build_cell_profile_embed(uid, gid)
        view = CellProfileView(self, gid, uid)
        await interaction.followup.send(embed=em, view=view, ephemeral=True)

    async def play_panel_cellclaim(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            await interaction.followup.send("Use this in a **server**.", ephemeral=True)
            return
        uid, gid = interaction.user.id, guild.id
        await self._ensure_stats(uid, gid)
        row = await self._db_fetchone(
            "SELECT last_claim_ts, income_stunned_until, total_currency, haste_level FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        assert row is not None
        now = time.time()
        if row["income_stunned_until"] and now < row["income_stunned_until"]:
            rem = int(row["income_stunned_until"] - now)
            await interaction.followup.send(
                f"Your income is **stunned** for **{rem // 3600}h {(rem % 3600) // 60}m** (sharpshooter hit).",
                ephemeral=True,
            )
            return
        inv = await self._db_fetchall(
            "SELECT character, variation FROM user_inventory WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        if not inv:
            await interaction.followup.send(
                "No catches yet — wait for a spawn in the holding cell channel.", ephemeral=True
            )
            return
        rate = self._hourly_rate(uid, gid, inv)
        if rate <= 0:
            await interaction.followup.send("Nothing to claim.", ephemeral=True)
            return
        t0 = row["last_claim_ts"]
        hours = max(0.0, (now - t0) / 3600.0)
        earned = int(rate * hours)
        mult = self._headshot_multiplier(inv)
        h_lv = int(row["haste_level"] or 0)
        h_mult = _haste_income_multiplier(h_lv)
        final = int(earned * mult * h_mult)
        new_bal = row["total_currency"] + final
        await self._db_exec(
            "UPDATE user_stats SET total_currency = ?, last_claim_ts = ? WHERE user_id = ? AND guild_id = ?",
            (new_bal, now, uid, gid),
        )
        bonus = f" · **Headshot!** ×3" if mult > 1.0 else ""
        h_note = f" · **Haste** ×{h_mult:.2f}" if h_mult > 1.0 else ""
        await interaction.followup.send(
            f"**+{final:,}** cell ({hours:.2f}h @ **{rate:,}**/hr){bonus}{h_note}\nBalance: **{new_bal:,}**",
            ephemeral=True,
        )

    async def play_panel_cellinv(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None:
            await interaction.followup.send("Use in a server.", ephemeral=True)
            return
        rows = await self._db_fetchall(
            """SELECT character, variation, catch_time_ms FROM user_inventory
               WHERE user_id = ? AND guild_id = ? ORDER BY caught_at DESC LIMIT 20""",
            (interaction.user.id, guild.id),
        )
        if not rows:
            await interaction.followup.send("Empty inventory.", ephemeral=True)
            return
        lines = [
            f"· **{CHAR_DISPLAY.get(r['character'], r['character'])}** "
            f"({_variation_display(r['variation'])}) — {r['catch_time_ms'] / 1000:.2f}s"
            for r in rows
        ]
        em = discord.Embed(
            title="Recent catches (last 20)",
            description="\n".join(lines),
            color=discord.Color.dark_blue(),
        )
        await interaction.followup.send(embed=em, ephemeral=True)

    async def play_panel_cellshop(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        from economy_cog import SHOP_ROLES, resolve_shop_role

        guild = interaction.guild
        if guild is None:
            await interaction.followup.send("Use in a server.", ephemeral=True)
            return
        lines = []
        for i, sr in enumerate(SHOP_ROLES):
            ok = "✅" if resolve_shop_role(guild, sr) else "⚠️"
            lines.append(f"**{i + 1}.** {ok} {sr.name} — **{sr.price:,}** cell")
        em = discord.Embed(
            title="Holding Cell — role shop",
            description="\n".join(lines),
            color=discord.Color.dark_green(),
        )
        em.set_footer(text="6cellbuy <number>  ·  coin role shop: 6shop")
        await interaction.followup.send(embed=em, ephemeral=True)

    async def play_panel_rares(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        lines = [
            "**Mythic** — `god`",
            "**Legendary** — `serialkiller`",
            "**Epic** — `evil`",
            "**Rare** — `alien`, `troll`",
            "**Uncommon** — `egirl`, `rapper`, `artist`",
            "**Common** — `professor`, `engineer`, `teacher`, `worker`",
            "",
            "**Characters** — " + ", ".join(CHAR_DISPLAY.get(c, c) for c in self._spawn_characters),
            "",
            "**Upgrades** — `6cellbal` → **Defense** / **Haste** (cell).",
            "",
            "**Sharpshooters** — Arthur, Elliot, Adam · `6shoot`",
            "",
            f"**Spawn** — <#{HOLDING_CELL_CHANNEL_ID}> · **`catch`**",
        ]
        em = discord.Embed(
            title="Holding Cell — cheat sheet",
            description="\n".join(lines)[:4000],
            color=discord.Color.gold(),
        )
        await interaction.followup.send(embed=em, ephemeral=True)

    @commands.command(name="cellinv", aliases=["cinv"])
    async def cmd_cell_inv(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            return await ctx.send("Use in a server.", delete_after=6)
        rows = await self._db_fetchall(
            """SELECT character, variation, catch_time_ms FROM user_inventory
               WHERE user_id = ? AND guild_id = ? ORDER BY caught_at DESC LIMIT 20""",
            (ctx.author.id, ctx.guild.id),
        )
        if not rows:
            return await ctx.send("Empty inventory.", delete_after=6)
        lines = [
            f"· **{CHAR_DISPLAY.get(r['character'], r['character'])}** "
            f"({_variation_display(r['variation'])}) — {r['catch_time_ms'] / 1000:.2f}s"
            for r in rows
        ]
        em = discord.Embed(
            title="Recent catches (last 20)",
            description="\n".join(lines),
            color=discord.Color.dark_blue(),
        )
        await ctx.send(embed=em)

    @commands.command(name="rares", aliases=["cellrares", "crares"])
    async def cmd_rares(self, ctx: commands.Context) -> None:
        """All rarities, variations, characters, and income tiers."""
        lines = [
            "**Mythic** — `god`",
            "**Legendary** — `serialkiller`",
            "**Epic** — `evil`",
            "**Rare** — `alien`, `troll`",
            "**Uncommon** — `egirl`, `rapper`, `artist`",
            "**Common** — `professor`, `engineer`, `teacher`, `worker`",
            "",
            "**Characters (spawn pool)** — "
            + ", ".join(CHAR_DISPLAY.get(c, c) for c in self._spawn_characters),
            "",
            "**Upgrades** — **`6cellbal`** shows **buttons**: **Defense** (chance to fully block a **`6shoot`** hit) "
            "and **Haste** (+**12%** **`6cellclaim`** income per level, max **5**). Paid in **cell**.",
            "",
            "**Sharpshooters** — Arthur, Elliot, Adam → passive headshot on `6cellclaim` "
            "(10% ×3 income, 25% if **evil** or **serialkiller**). `6shoot @user` to sabotage.",
            "",
            f"**Spawn channel** — <#{HOLDING_CELL_CHANNEL_ID}> · type **`catch`** here **or in #general** (same cell).",
            "",
            "**Spawns** — random **30–80 min** timer, plus light **activity** in the server (not too often), "
            f"plus **`6reward`** for **6xs** milestones {_sixxs_reward_tiers_hint()} (card goes **straight to inventory**).",
            "",
            "**Commands** — `6reward` · `6cellclaim` · `6cellbal` · `6cellinv` · `6cellshop` · `6cellbuy <#>` · "
            f"`6shoot @user` (**{SHOOT_COOLDOWN_SEC // 60}m** cooldown)\n"
            "**Aliases** — e.g. `6cball`/`6cbal`, `6cclaim`, `6cinv`, `6cshop`, `6cbuy`, `6crares`, `6sniper`.",
        ]
        em = discord.Embed(
            title="The Holding Cell — rarities & roster",
            description="\n".join(lines),
            color=discord.Color.gold(),
        )
        em.add_field(
            name="Income / card / hour (examples)",
            value="god **1,400** · serialkiller **900** · evil **550** · rare **300** · "
            "uncommon **160** · common **55–70** (stacks per duplicate).",
            inline=False,
        )
        await ctx.send(embed=em)

    @commands.command(name="shoot")
    @commands.cooldown(1, SHOOT_COOLDOWN_SEC, commands.BucketType.member)
    async def cmd_shoot(self, ctx: commands.Context, target: discord.Member) -> None:
        """Sharpshooter sabotage: stun rival income 2h or steal part of uncollected accrual."""
        if not ctx.guild:
            return await ctx.send("Use in a server.", delete_after=6)
        if target.bot:
            return await ctx.send("Pick a member.", delete_after=6)
        uid, gid = ctx.author.id, ctx.guild.id
        await self._ensure_stats(uid, gid)

        inv = await self._db_fetchall(
            "SELECT character, variation FROM user_inventory WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        if not any(r["character"] in SHARPSHOOTER_CHARS for r in inv):
            return await ctx.send(
                "You need **Arthur**, **Elliot**, or **Adam** in your inventory to use **6shoot**.",
                delete_after=10,
            )

        await self._ensure_stats(target.id, gid)
        hit = random.random() < 0.52
        if not hit:
            return await ctx.send(f"{ctx.author.mention} missed **{target.display_name}**.")

        tdef = await self._db_fetchone(
            "SELECT defense_level FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (target.id, gid),
        )
        d_lv = int(tdef["defense_level"] or 0) if tdef is not None else 0
        if random.random() < _defense_block_chance(d_lv):
            pct = int(round(100 * _defense_block_chance(d_lv)))
            return await ctx.send(
                f"{ctx.author.mention}’s shot **blocked** by {target.mention} "
                f"— **defense lv {d_lv}** (~**{pct}%** resist)."
            )

        stun_until = time.time() + 2 * 3600
        await self._db_exec(
            "UPDATE user_stats SET income_stunned_until = ? WHERE user_id = ? AND guild_id = ?",
            (stun_until, target.id, gid),
        )

        trow = await self._db_fetchone(
            "SELECT last_claim_ts, total_currency FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (target.id, gid),
        )
        inv_t = await self._db_fetchall(
            "SELECT character, variation FROM user_inventory WHERE user_id = ? AND guild_id = ?",
            (target.id, gid),
        )
        assert trow is not None
        now = time.time()
        rate_t = self._hourly_rate(target.id, gid, inv_t)
        hours = max(0.0, (now - trow["last_claim_ts"]) / 3600.0)
        uncollected = int(rate_t * hours)
        steal = min(max(uncollected // 4, 50), 2500)
        steal = min(steal, uncollected) if uncollected > 0 else 0

        if steal > 0:
            await self._db_exec(
                "UPDATE user_stats SET total_currency = total_currency + ? WHERE user_id = ? AND guild_id = ?",
                (steal, uid, gid),
            )
            await self._db_exec(
                "UPDATE user_stats SET last_claim_ts = ? WHERE user_id = ? AND guild_id = ?",
                (now, target.id, gid),
            )

        await ctx.send(
            f"**Hit!** {target.mention} is **stunned** (no income) for **2h**."
            + (f" You siphoned **{steal:,}** uncollected cell." if steal else "")
        )

    @commands.command(name="cellshop", aliases=["cshop"])
    async def cmd_cell_shop(self, ctx: commands.Context) -> None:
        from economy_cog import SHOP_ROLES, resolve_shop_role

        lines = []
        for i, sr in enumerate(SHOP_ROLES):
            ok = "✅" if ctx.guild and resolve_shop_role(ctx.guild, sr) else "⚠️"
            lines.append(f"**{i + 1}.** {ok} {sr.name} — **{sr.price:,}** cell")
        em = discord.Embed(
            title="Holding Cell — role shop",
            description="\n".join(lines),
            color=discord.Color.dark_green(),
        )
        em.set_footer(text="6cellbuy <number>  e.g. 6cellbuy 1")
        await ctx.send(embed=em)

    @commands.command(name="cellbuy", aliases=["cbuy"])
    async def cmd_cell_buy(self, ctx: commands.Context, slot: str) -> None:
        from economy_cog import SHOP_ROLES, resolve_shop_role

        if not ctx.guild or not isinstance(ctx.author, discord.Member):
            return await ctx.send("Use in a server.", delete_after=6)
        try:
            n = int(str(slot).strip())
        except ValueError:
            return await ctx.send("Usage: `6cellbuy <number>`", delete_after=6)
        idx = n - 1
        if idx < 0 or idx >= len(SHOP_ROLES):
            return await ctx.send(f"Pick **1–{len(SHOP_ROLES)}** (see `6cellshop`).", delete_after=8)

        sr = SHOP_ROLES[idx]
        uid, gid = ctx.author.id, ctx.guild.id
        await self._ensure_stats(uid, gid)
        row = await self._db_fetchone(
            "SELECT total_currency FROM user_stats WHERE user_id = ? AND guild_id = ?",
            (uid, gid),
        )
        assert row is not None
        if row["total_currency"] < sr.price:
            return await ctx.send(f"Need **{sr.price:,}** cell (you have **{row['total_currency']:,}**).", delete_after=10)

        role = resolve_shop_role(ctx.guild, sr)
        if not role:
            return await ctx.send(
                "That role isn’t in this server (name must match the shop entry exactly), or fix **role_id** in config.",
                delete_after=12,
            )
        if role in ctx.author.roles:
            return await ctx.send("You already have that role.", delete_after=6)

        try:
            await ctx.author.add_roles(role, reason="Holding cell shop")
        except discord.Forbidden:
            return await ctx.send("I can't assign that role (perms / hierarchy).", delete_after=8)

        await self._db_exec(
            "UPDATE user_stats SET total_currency = total_currency - ? WHERE user_id = ? AND guild_id = ?",
            (sr.price, uid, gid),
        )
        await ctx.send(f"**{sr.name}** unlocked for **{sr.price:,}** cell.")


class CellProfileView(discord.ui.View):
    """Defense / haste upgrades from **`6cellbal`** (and **`6play`**)."""

    def __init__(self, cog: HoldingCellCog, guild_id: int, user_id: int) -> None:
        super().__init__(timeout=300)
        self.cog = cog
        self.gid = guild_id
        self.uid = user_id
        self._add_upgrade_buttons()

    def _add_upgrade_buttons(self) -> None:
        async def on_defense(interaction: discord.Interaction) -> None:
            if interaction.user.id != self.uid:
                await interaction.response.send_message("not your panel", ephemeral=True)
                return
            ok, msg = await self.cog._try_buy_defense(self.uid, self.gid)
            if not ok:
                await interaction.response.send_message(msg, ephemeral=True)
                return
            em = await self.cog._build_cell_profile_embed(self.uid, self.gid)
            new_view = CellProfileView(self.cog, self.gid, self.uid)
            await interaction.response.edit_message(embed=em, view=new_view)
            await interaction.followup.send(msg, ephemeral=True)

        async def on_haste(interaction: discord.Interaction) -> None:
            if interaction.user.id != self.uid:
                await interaction.response.send_message("not your panel", ephemeral=True)
                return
            ok, msg = await self.cog._try_buy_haste(self.uid, self.gid)
            if not ok:
                await interaction.response.send_message(msg, ephemeral=True)
                return
            em = await self.cog._build_cell_profile_embed(self.uid, self.gid)
            new_view = CellProfileView(self.cog, self.gid, self.uid)
            await interaction.response.edit_message(embed=em, view=new_view)
            await interaction.followup.send(msg, ephemeral=True)

        d_btn = discord.ui.Button(label="🛡️ Defense +1", style=discord.ButtonStyle.primary, row=0)
        h_btn = discord.ui.Button(label="⚡ Haste +1", style=discord.ButtonStyle.success, row=0)
        async def on_withdraw(interaction: discord.Interaction) -> None:
            if interaction.user.id != self.uid:
                await interaction.response.send_message("not your panel", ephemeral=True)
                return
            ok, msg = await self.cog._withdraw_all_cells_to_wallet(self.uid, self.gid)
            if not ok:
                await interaction.response.send_message(msg, ephemeral=True)
                return
            em = await self.cog._build_cell_profile_embed(self.uid, self.gid)
            new_view = CellProfileView(self.cog, self.gid, self.uid)
            await interaction.response.edit_message(embed=em, view=new_view)
            await interaction.followup.send(msg, ephemeral=True)

        d_btn.callback = on_defense
        h_btn.callback = on_haste
        self.add_item(d_btn)
        self.add_item(h_btn)
        w_btn = discord.ui.Button(
            label="💸 Withdraw all → coins",
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        w_btn.callback = on_withdraw
        self.add_item(w_btn)
