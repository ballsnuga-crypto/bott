"""
6flags — guess the country from the flag; streak game + per-server leaderboard.
"""
from __future__ import annotations

import asyncio
import difflib
import json
import os
import random
import unicodedata
from pathlib import Path
import discord
import pycountry
from discord.ext import commands

FLAGS_LB_FILE = (Path(__file__).resolve().parent / "flags_leaderboard.json").resolve()
FLAG_ROUND_SECONDS = 10
FLAGCDN_URL = "https://flagcdn.com/w320/{code}.png"

# Extra accepted answers (normalized) → alpha-2
_EXTRA_ALIASES: dict[str, frozenset[str]] = {
    "US": frozenset(
        {"usa", "u.s.", "u.s.a.", "america", "united states of america", "the states"}
    ),
    "GB": frozenset(
        {"uk", "u.k.", "britain", "great britain", "united kingdom", "england"}
    ),
    "AE": frozenset({"uae", "emirates", "dubai"}),
    "BO": frozenset({"bolivia", "bolivia plurinational state of"}),
    "CD": frozenset({"drc", "congo kinshasa", "dr congo"}),
    "CG": frozenset({"congo brazzaville", "republic of the congo"}),
    "CI": frozenset({"ivory coast", "cote divoire", "côte divoire"}),
    "CZ": frozenset({"czechia"}),
    "FM": frozenset({"micronesia"}),
    "KR": frozenset({"south korea", "korea south", "republic of korea"}),
    "KP": frozenset({"north korea", "korea north", "dprk"}),
    "LA": frozenset({"laos"}),
    "MD": frozenset({"moldova"}),
    "MK": frozenset({"north macedonia", "macedonia"}),
    "RU": frozenset({"russia", "russian federation"}),
    "SZ": frozenset({"eswatini", "swaziland"}),
    "SY": frozenset({"syria"}),
    "TW": frozenset({"taiwan"}),
    "TZ": frozenset({"tanzania"}),
    "VN": frozenset({"vietnam", "viet nam"}),
    "VE": frozenset({"venezuela", "bolivarian republic of venezuela"}),
}


def _norm_answer(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )
    s = s.replace("'", "").replace(".", " ")
    return " ".join(s.split())


def _all_country_codes() -> list[str]:
    codes: list[str] = []
    for c in pycountry.countries:
        a2 = getattr(c, "alpha_2", None)
        if not a2 or len(a2) != 2:
            continue
        codes.append(a2.upper())
    return codes


_STOP = frozenset(
    {
        "the",
        "of",
        "and",
        "republic",
        "democratic",
        "federal",
        "people",
        "state",
        "states",
        "kingdom",
        "union",
        "islamic",
        "arab",
        "saint",
        "st",
        "northern",
        "southern",
        "plurinational",
    }
)


def _strip_stops(s: str) -> str:
    return " ".join(w for w in s.split() if w not in _STOP)


def _ratio(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def _ratio_threshold_for_pair(g: str, t: str) -> float:
    """Stricter for short strings so 'niger' ≠ 'nigeria' / 'iran' ≠ 'iraq'; looser for long official names."""
    m = max(len(g), len(t))
    if m <= 12:
        return 0.78
    if m <= 22:
        return 0.68
    return 0.62


def _lenient_match(guess: str, target: str) -> bool:
    """
    Typo- and partial-name tolerant: similarity vs full name, stripped name,
    word-sorted name, strong match on a single long word, or first-word match.
    Only compared against the correct country for this flag.
    """
    g = guess
    t = target
    if not g or not t:
        return False
    if g == t:
        return True
    if len(g) < 3:
        return False
    # "nigeria" is not a typo for "niger" — block longer guess that extends a single short word.
    if (
        len(t) >= 4
        and len(t) <= 10
        and " " not in t
        and len(g) > len(t) + 1
        and g.startswith(t)
    ):
        return False

    th = _ratio_threshold_for_pair(g, t)
    if _ratio(g, t) >= th:
        return True

    g2, t2 = _strip_stops(g), _strip_stops(t)
    if len(g2) >= 3 and len(t2) >= 3:
        # Stripping "Republic of the Niger" → "niger" must not let "nigeria" match via ratio.
        collapsed = len(t.split()) >= 3 and len(t2.split()) == 1
        blocked = (
            collapsed
            and len(t2) >= 4
            and len(t2) <= 10
            and " " not in t2
            and len(g2) > len(t2) + 1
            and g2.startswith(t2)
        )
        if not blocked:
            th2 = _ratio_threshold_for_pair(g2, t2)
            if _ratio(g2, t2) >= th2:
                return True

    gs = " ".join(sorted(g.split()))
    ts = " ".join(sorted(t.split()))
    if len(gs) >= 4 and (" " in gs or " " in ts):
        if _ratio(gs, ts) >= _ratio_threshold_for_pair(gs, ts):
            return True

    tw = [w for w in t.split() if len(w) >= 4]
    multi = len(t.split()) >= 3
    for w in tw:
        if (
            multi
            and len(w) <= 10
            and len(g) > len(w) + 1
            and g.startswith(w)
        ):
            continue
        if w == g:
            return True
        wth = max(0.72, _ratio_threshold_for_pair(g, w))
        if _ratio(g, w) >= wth:
            return True
        # Shorter guess as prefix of answer (e.g. "niger" → Nigeria); not longer guess vs short name.
        if len(g) >= 4 and w.startswith(g) and len(w) > len(g) and _ratio(g, w) >= 0.62:
            return True
        if len(g) >= 4 and g.startswith(w) and len(g) > len(w) and _ratio(g, w) >= 0.86:
            return True

    if len(t) > len(g) + 6 and t.split() and len(g) >= 4:
        first = t.split()[0]
        fth = _ratio_threshold_for_pair(g, first)
        if _ratio(g, first) >= fth:
            return True

    return False


def _match_country(guess: str, code: str) -> bool:
    g = _norm_answer(guess)
    if not g:
        return False
    c = pycountry.countries.get(alpha_2=code)
    if not c:
        return False
    if len(g) == 2 and g.isalpha():
        oc = pycountry.countries.get(alpha_2=g.upper())
        if oc and oc.alpha_2 == code:
            return True
    if len(g) == 3 and g.isalpha():
        oc = pycountry.countries.get(alpha_3=g.upper())
        if oc and oc.alpha_2 == code:
            return True
    name_n = _norm_answer(c.name)
    if g == name_n:
        return True
    off = getattr(c, "official_name", None)
    off_n = _norm_answer(off) if off else ""
    if off_n and g == off_n:
        return True
    extras = _EXTRA_ALIASES.get(code)
    if extras and g in extras:
        return True

    if _lenient_match(g, name_n):
        return True
    if off_n and _lenient_match(g, off_n):
        return True
    if extras:
        for ex in extras:
            if _lenient_match(g, ex):
                return True

    try:
        matches = pycountry.countries.search_fuzzy(guess.strip())
    except LookupError:
        matches = []
    if matches and matches[0].alpha_2 == code:
        return True
    return False


class FlagsCog(commands.Cog):
    """🚩 Flag guessing streak + leaderboard."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._lock = asyncio.Lock()
        self._lb: dict[str, dict[str, int]] = {}
        self._flags_active: set[int] = set()
        self._codes = _all_country_codes()
        self._load_lb()

    def _load_lb(self) -> None:
        try:
            raw = json.loads(FLAGS_LB_FILE.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self._lb = {}
                return
            out: dict[str, dict[str, int]] = {}
            for gk, gv in raw.items():
                if not isinstance(gv, dict):
                    continue
                inner: dict[str, int] = {}
                for uk, uv in gv.items():
                    try:
                        inner[str(uk)] = int(uv)
                    except (TypeError, ValueError):
                        continue
                out[str(gk)] = inner
            self._lb = out
        except (OSError, json.JSONDecodeError):
            self._lb = {}

    async def _save_lb(self) -> None:
        async with self._lock:
            snap = {gk: dict(gv) for gk, gv in self._lb.items()}
        FLAGS_LB_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = FLAGS_LB_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snap, indent=0), encoding="utf-8")
        os.replace(tmp, FLAGS_LB_FILE)

    def _record_best(self, guild_id: int, user_id: int, streak: int) -> int:
        """Returns new personal best for this server (may equal old best)."""
        if streak <= 0:
            return int(self._lb.get(str(guild_id), {}).get(str(user_id), 0))
        gk, uk = str(guild_id), str(user_id)
        cur = int(self._lb.get(gk, {}).get(uk, 0))
        if streak > cur:
            if gk not in self._lb:
                self._lb[gk] = {}
            self._lb[gk][uk] = streak
            return streak
        return cur

    @commands.command(name="flags", aliases=["flag", "flaggame"])
    async def flags_cmd(self, ctx: commands.Context, *, sub: str = "") -> None:
        """🚩 Guess flags — **10s** per round; longest streak wins. `6flags lb` for leaderboard."""
        if ctx.guild is None:
            return await ctx.send("❌ use this in a server.", delete_after=8)
        parts = (sub or "").strip().split(maxsplit=1)
        arg0 = parts[0].lower() if parts else ""
        if arg0 in ("lb", "leaderboard", "top", "highscores", "highscore"):
            return await self._flags_leaderboard(ctx)

        uid = ctx.author.id
        if uid in self._flags_active:
            return await ctx.send(
                "❌ you already have a **flags** run going — finish it or wait for the timeout.",
                delete_after=8,
            )

        self._flags_active.add(uid)
        streak = 0
        pool = list(self._codes)
        random.shuffle(pool)
        idx = 0

        try:
            while True:
                if idx >= len(pool):
                    random.shuffle(pool)
                    idx = 0
                code = pool[idx]
                idx += 1

                c = pycountry.countries.get(alpha_2=code)
                name_hint = c.name if c else code

                em = discord.Embed(
                    title="🚩 Guess the flag",
                    description=(
                        f"{ctx.author.mention} — reply with the **country** (or common) name.\n"
                        f"**{FLAG_ROUND_SECONDS} seconds** • streak: **{streak}** correct"
                    ),
                    color=discord.Color.blurple(),
                )
                em.set_image(url=FLAGCDN_URL.format(code=code.lower()))
                em.set_footer(text="ISO countries via flagcdn · pycountry name matching")

                await ctx.send(embed=em)

                def check(m: discord.Message) -> bool:
                    return (
                        m.channel.id == ctx.channel.id
                        and m.author.id == uid
                        and not m.author.bot
                    )

                try:
                    msg = await self.bot.wait_for(
                        "message", timeout=FLAG_ROUND_SECONDS, check=check
                    )
                except asyncio.TimeoutError:
                    async with self._lock:
                        best = self._record_best(ctx.guild.id, uid, streak)
                    await self._save_lb()
                    await ctx.send(
                        f"⏱ **Time's up!** Game over — **{streak}** in a row.\n"
                        f"Your server best: **{best}** — `6flags lb` for the board."
                    )
                    break

                if _match_country(msg.content, code):
                    streak += 1
                    try:
                        await msg.add_reaction("✅")
                    except discord.HTTPException:
                        pass
                else:
                    async with self._lock:
                        best = self._record_best(ctx.guild.id, uid, streak)
                    await self._save_lb()
                    await ctx.send(
                        f"❌ Nope — **`{name_hint}`**.\n"
                        f"You reached **{streak}** in a row. Server best: **{best}**.\n"
                        f"`6flags` to play again · `6flags lb` for ranks."
                    )
                    break
        finally:
            self._flags_active.discard(uid)

    async def _flags_leaderboard(self, ctx: commands.Context) -> None:
        assert ctx.guild is not None
        gk = str(ctx.guild.id)
        rows = self._lb.get(gk, {})
        if not rows:
            return await ctx.send(
                "🏆 no **flags** scores here yet — run `6flags` and build a streak.",
                delete_after=12,
            )
        sorted_u = sorted(rows.items(), key=lambda x: x[1], reverse=True)[:15]
        lines: list[str] = []
        for i, (uk, score) in enumerate(sorted_u, 1):
            mem = ctx.guild.get_member(int(uk))
            label = mem.display_name if mem else f"<@{uk}>"
            lines.append(f"**{i}.** {label} — **{score}** flags")
        em = discord.Embed(
            title=f"🏆 Flag streak — {ctx.guild.name}",
            description="\n".join(lines),
            color=discord.Color.gold(),
        )
        em.set_footer(text="Best consecutive correct guesses (one run).")
        await ctx.send(embed=em)

