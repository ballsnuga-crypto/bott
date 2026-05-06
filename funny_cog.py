"""
6funny — bulk 24h chat scan + Grok top-N leaderboard (activity + humor); midnight UTC payout.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Any, Optional

import aiohttp
import discord
from discord.errors import DiscordServerError, HTTPException
from discord.ext import commands, tasks

GROK_API_URL = os.getenv(
    "OPENROUTER_API_URL",
    "https://api.venice.ai/api/v1/chat/completions",
)
FUNNY_GROK_MODEL = (
    os.getenv("FUNNY_GROK_MODEL")
    or os.getenv("OPENROUTER_MODEL")
    or "venice-uncensored-1-2"
)

FUNNY_PRIZE_COINS = int(os.getenv("FUNNY_PRIZE_COINS", "5000"))
FUNNY_ANNOUNCE_CHANNEL_ID = int(os.getenv("FUNNY_ANNOUNCE_CHANNEL_ID", "0"))
FUNNY_CRON_CHANNEL_ID = int(os.getenv("FUNNY_CRON_CHANNEL_ID", "0"))

FUNNY_CMD_COOLDOWN_SEC = int(os.getenv("FUNNY_CMD_COOLDOWN_SEC", str(3600)))
FUNNY_HISTORY_HOURS = int(os.getenv("FUNNY_HISTORY_HOURS", "24"))
# How many messages to pull from the window. Default 15k so busy #general doesn't drop active users.
# Set FUNNY_HISTORY_LIMIT=0 or "none" / "unlimited" / "all" to fetch every message in the time window (slower).
_hlim_raw = os.getenv("FUNNY_HISTORY_LIMIT", "15000").strip().lower()
if _hlim_raw in ("0", "none", "unlimited", "all"):
    FUNNY_HISTORY_FETCH_LIMIT: Optional[int] = None
else:
    try:
        _hlim_n = int(_hlim_raw)
    except ValueError:
        _hlim_n = 15000
    FUNNY_HISTORY_FETCH_LIMIT = None if _hlim_n <= 0 else min(max(_hlim_n, 100), 100_000)

FUNNY_HISTORY_RETRIES = max(1, int(os.getenv("FUNNY_HISTORY_RETRIES", "5")))
FUNNY_HISTORY_RETRY_BASE_SEC = float(os.getenv("FUNNY_HISTORY_RETRY_BASE_SEC", "2.0"))

FUNNY_LOG_MAX_CHARS = int(os.getenv("FUNNY_LOG_MAX_CHARS", "200000"))
FUNNY_TOP_N = int(os.getenv("FUNNY_TOP_N", "10"))

# Only this Discord user may run `6funny` / rescan. Optional: FUNNY_CMD_ALLOWED_IDS=comma list **replaces** this set.
FUNNY_SCAN_OWNER_ID = 1326518688727437342
_raw_allowed = os.getenv("FUNNY_CMD_ALLOWED_IDS", "").strip()
if _raw_allowed:
    FUNNY_CMD_ALLOWED_IDS = frozenset(
        int(x.strip()) for x in _raw_allowed.split(",") if x.strip().isdigit()
    )
    if not FUNNY_CMD_ALLOWED_IDS:
        FUNNY_CMD_ALLOWED_IDS = frozenset({FUNNY_SCAN_OWNER_ID})
else:
    FUNNY_CMD_ALLOWED_IDS = frozenset({FUNNY_SCAN_OWNER_ID})

FUNNY_BAR_FULL = "\u2588"
FUNNY_BAR_EMPTY = "\u2591"


def _bulk_rank_system_prompt(n: int) -> str:
    return f"""You are a ruthless Discord judge. Analyze this chat log from the past {FUNNY_HISTORY_HOURS} hours.

Your ranking must mix TWO signals:
1) **Most active** posters (high message count) deserve slots — chat carries the room.
2) **Low-activity** users who still landed sharp / funny lines must also appear when they outshine lurkers.
3) Messages may be **Arabic, English, or any language** — judge humor from non-English text the same way; never skip a user just because their name or messages use RTL / Arabic script.

Return ONLY a valid JSON array of objects, nothing else: [{{"userId": "123", "score": 95, "roast": "why", "best_quote": "line"}}]

Rules:
- No markdown, no code fences, no text outside the JSON array.
- Let DISTINCT = number of distinct human userIds in the log (including lines marked no plain text).
- If DISTINCT >= {n}, you MUST return exactly {n} objects, all different userIds from the log.
- If DISTINCT < {n}, return exactly DISTINCT objects (every distinct author).
- Order: funniest / strongest overall humor impact first, but obey the mix above (active + sporadic funny people).
- Each log line is TAB-separated: **1st column** = user snowflake (digits only), 2nd = display name (any language/emoji/RTL), 3rd = message. userId in JSON must match the **1st column** exactly.
- score: integer 1–100. roast: one sharp sentence. best_quote: short excerpt from their messages in the log."""


GROK_FILL_PROMPT = """You are finishing a comedy leaderboard. Some users are already ranked.

Return ONLY a JSON array (no markdown) of EXACTLY {need} objects, same shape:
[{{"userId": "123", "score": 70, "roast": "...", "best_quote": "..."}}]

Rules:
- Log lines are TAB-separated: column1=snowflake id, column2=display name, column3=text (any language).
- userId MUST be chosen ONLY from the provided candidate_ids list.
- Do NOT include any userId from the excluded_ids list.
- Pick the next-funniest among candidates by the chat log. Scores should be plausible vs a typical leaderboard (often a bit lower than the very top)."""


def _grok_key() -> Optional[str]:
    return (
        os.getenv("VENICE_API_KEY")
        or os.getenv("VENICE_INFERENCE_KEY")
        or os.getenv("OPENROUTER_API_KEY")
        or os.getenv("XAI_API_KEY")
        or os.getenv("GROK_API_KEY")
    )


def _is_transient_discord_api_error(exc: BaseException) -> bool:
    if isinstance(exc, DiscordServerError):
        return True
    if isinstance(exc, HTTPException):
        st = getattr(exc, "status", None)
        return st in (408, 429, 500, 502, 503, 504)
    return False


async def _funny_safe_send(
    ctx: commands.Context,
    content: Optional[str] = None,
    *,
    embed: Optional[discord.Embed] = None,
    delete_after: Optional[float] = None,
) -> bool:
    """Avoid crashing `on_command_error` if Discord returns 503 while sending the error reply."""
    try:
        if embed is not None:
            kwargs: dict[str, Any] = {"embed": embed}
            if content:
                kwargs["content"] = content
            if delete_after is not None:
                kwargs["delete_after"] = delete_after
            await ctx.send(**kwargs)
        else:
            await ctx.send(content or "", delete_after=delete_after)
        return True
    except (HTTPException, DiscordServerError, OSError, ConnectionError) as exc:
        print(f"[funny] ctx.send failed (transient?): {exc}")
        return False


def funny_make_bar(score: int) -> str:
    try:
        s = int(score)
    except (TypeError, ValueError):
        s = 0
    filled = max(0, min(10, round(s / 10)))
    return FUNNY_BAR_FULL * filled + FUNNY_BAR_EMPTY * (10 - filled)


def _strip_markdown_json(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```\s*$", "", s)
    return s.strip()


def _parse_grok_rank_array(raw: str) -> list[dict[str, Any]]:
    s = _strip_markdown_json(raw)
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        start = s.find("[")
        end = s.rfind("]")
        if start < 0 or end <= start:
            return []
        try:
            data = json.loads(s[start : end + 1])
        except json.JSONDecodeError:
            return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            out.append(item)
    return out


def _log_cell(s: str, max_len: int) -> str:
    """TSV-safe cell: no tabs/newlines (works with Arabic, emoji, RTL names)."""
    return (s or "").replace("\n", " ").replace("\r", " ").replace("\t", " ")[:max_len]


def _compile_log_lines(
    messages: list[discord.Message],
) -> tuple[str, set[int], dict[int, int]]:
    """
    valid_ids = anyone who posted in the window (including attachment-only).
    Each line: SNOWFLAKE\\tDISPLAY_NAME\\tMESSAGE — id always first so RTL/Arabic names never break parsing.
    """
    lines: list[str] = []
    valid_ids: set[int] = set()
    ids_with_text: set[int] = set()
    counts: dict[int, int] = defaultdict(int)
    uid_display: dict[int, str] = {}

    for m in messages:
        if m.author.bot:
            continue
        uid = m.author.id
        valid_ids.add(uid)
        counts[uid] += 1
        if uid not in uid_display:
            uid_display[uid] = _log_cell(m.author.display_name, 120)

        text = (m.content or "").strip()
        if text:
            ids_with_text.add(uid)
            safe = _log_cell(text, 500)
            lines.append(f"{uid}\t{uid_display[uid]}\t{safe}")

    for uid in sorted(valid_ids - ids_with_text):
        nm = uid_display.get(uid, str(uid))
        lines.append(
            f"{uid}\t{nm}\t[active in this window — no plain text; stickers/attachments/embeds only]"
        )

    return "\n".join(lines), valid_ids, dict(counts)


def _rank_preamble(distinct: int, activity: dict[int, int]) -> str:
    top_act = sorted(activity.items(), key=lambda x: (-x[1], x[0]))[:25]
    act_line = ", ".join(f"{uid} ({n} msgs)" for uid, n in top_act) or "(none)"
    return (
        f"DISTINCT_AUTHORS_IN_WINDOW: {distinct}\n"
        f"TARGET_LEADERBOARD_SIZE: {min(FUNNY_TOP_N, distinct)}\n"
        f"MOST_ACTIVE_USER_IDS (message count):\n{act_line}\n"
    )


def _snippet_for_user(log: str, uid: int, max_len: int = 220) -> str:
    prefix = f"{uid}\t"
    for line in log.split("\n"):
        if not line.startswith(prefix):
            continue
        rest = line[len(prefix) :]
        tab = rest.find("\t")
        if tab < 0:
            continue
        body = rest[tab + 1 :].strip()
        if body:
            return body[:max_len] + ("…" if len(body) > max_len else "")
    return "—"


async def _grok_post(
    system: str, user_content: str, *, max_tokens: int, temperature: float = 0.82
) -> str:
    key = _grok_key()
    if not key:
        raise RuntimeError("Missing VENICE_API_KEY (or OPENROUTER_API_KEY)")
    payload = {
        "model": FUNNY_GROK_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_content[:FUNNY_LOG_MAX_CHARS]},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if "openrouter.ai" in GROK_API_URL:
        headers["HTTP-Referer"] = "https://6xs.lol"
        headers["X-Title"] = "6XS Bot"
    async with aiohttp.ClientSession() as session:
        async with session.post(
            GROK_API_URL, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=120)
        ) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Chat API {resp.status}: {body[:500]}")
            data = json.loads(body)
    try:
        return data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Bad Grok response: {body[:400]}") from e


async def _grok_rank_chat_log(preamble: str, log: str) -> list[dict[str, Any]]:
    system = _bulk_rank_system_prompt(FUNNY_TOP_N)
    user_body = preamble + "\nCHAT LOG (one line per message):\n\n" + log
    raw = await _grok_post(system, user_body, max_tokens=2600)
    return _parse_grok_rank_array(raw)


async def _grok_fill_rank(
    log: str,
    excluded: set[int],
    candidates: list[int],
    need: int,
) -> list[dict[str, Any]]:
    if need <= 0 or not candidates:
        return []
    system = GROK_FILL_PROMPT.format(need=need)
    user_body = (
        f"excluded_ids: {sorted(excluded)}\n"
        f"candidate_ids (pick only from here): {candidates[:50]}\n\n"
        f"CHAT LOG:\n\n{log[: min(len(log), FUNNY_LOG_MAX_CHARS - 800)]}"
    )
    raw = await _grok_post(system, user_body, max_tokens=900, temperature=0.75)
    return _parse_grok_rank_array(raw)


def _normalize_rank_rows(
    parsed: list[dict[str, Any]], valid_ids: set[int]
) -> list[tuple[int, int, str, str]]:
    rows: list[tuple[int, int, str, str]] = []
    for item in parsed:
        uid_raw = item.get("userId", item.get("user_id", ""))
        if uid_raw is None:
            continue
        s = str(uid_raw).strip()
        m = re.search(r"\d{17,20}", s)
        if m:
            s = m.group(0)
        try:
            uid = int(s)
        except (TypeError, ValueError):
            continue
        if valid_ids and uid not in valid_ids:
            continue
        try:
            score = int(item.get("score", 0))
        except (TypeError, ValueError):
            score = 0
        score = max(1, min(100, score))
        roast = str(item.get("roast", "") or "").strip() or "—"
        quote = str(item.get("best_quote", "") or "").strip() or "—"
        if len(roast) > 350:
            roast = roast[:347] + "…"
        if len(quote) > 280:
            quote = quote[:277] + "…"
        rows.append((score, uid, roast, quote))
    rows.sort(key=lambda x: (-x[0], x[1]))
    seen: set[int] = set()
    unique: list[tuple[int, int, str, str]] = []
    for row in rows:
        if row[1] in seen:
            continue
        seen.add(row[1])
        unique.append(row)
    return unique


def _merge_cap_rows(
    rows: list[tuple[int, int, str, str]],
    valid_ids: set[int],
    activity: dict[int, int],
    log: str,
) -> list[tuple[int, int, str, str]]:
    """Dedupe by uid, sort by score, cap at FUNNY_TOP_N."""
    target = min(FUNNY_TOP_N, len(valid_ids))
    by_uid: dict[int, tuple[int, int, str, str]] = {}
    for row in rows:
        uid = row[1]
        if uid not in by_uid or row[0] > by_uid[uid][0]:
            by_uid[uid] = row
    merged = sorted(by_uid.values(), key=lambda x: (-x[0], x[1]))
    return merged[:target]


def _activity_pad_rows(
    rows: list[tuple[int, int, str, str]],
    valid_ids: set[int],
    activity: dict[int, int],
    log: str,
) -> list[tuple[int, int, str, str]]:
    """Pad with highest-activity users not yet listed until target or pool exhausted."""
    target = min(FUNNY_TOP_N, len(valid_ids))
    have = {r[1] for r in rows}
    min_score = min((r[0] for r in rows), default=55)
    floor = max(1, min(min_score - 1, 45))
    pool = [uid for uid in valid_ids if uid not in have]
    pool.sort(key=lambda u: (-activity.get(u, 0), u))
    out = list(rows)
    slot = len(out)
    for uid in pool:
        if slot >= target:
            break
        n = max(1, activity.get(uid, 1))
        sc = max(1, min(floor, 10 + min(30, n * 3)))
        quote = _snippet_for_user(log, uid)
        if quote == "—":
            quote = "[No text line matched — volume-based slot]"
        roast = (
            "Slotted by **message volume** in the window — fill out the board; "
            "judge humor as thinner vs the top lines."
        )
        out.append((sc, uid, roast, quote))
        slot += 1
    out.sort(key=lambda x: (-x[0], x[1]))
    return out[:target]


async def full_leaderboard_pipeline(
    log: str, valid_ids: set[int], activity: dict[int, int]
) -> list[tuple[int, int, str, str]]:
    distinct = len(valid_ids)
    if distinct == 0:
        return []
    target = min(FUNNY_TOP_N, distinct)
    preamble = _rank_preamble(distinct, activity)
    parsed = await _grok_rank_chat_log(preamble, log)
    rows = _normalize_rank_rows(parsed, valid_ids)
    rows = _merge_cap_rows(rows, valid_ids, activity, log)

    # Grok often returns < target; retry fill + pad until full or no progress.
    for attempt in range(4):
        if len(rows) >= target:
            break
        before = len(rows)
        ranked_ids = {r[1] for r in rows}
        need = target - len(rows)
        candidates = [
            uid
            for uid in sorted(valid_ids - ranked_ids, key=lambda u: (-activity.get(u, 0), u))
        ]
        if candidates and need > 0:
            try:
                fill_raw = await _grok_fill_rank(log, ranked_ids, candidates, need)
                fill_rows = _normalize_rank_rows(fill_raw, valid_ids)
                rows = _merge_cap_rows(rows + fill_rows, valid_ids, activity, log)
            except Exception as e:
                print(f"[funny] fill grok ({attempt}): {e}")

        if len(rows) < target:
            rows = _activity_pad_rows(rows, valid_ids, activity, log)
            rows = _merge_cap_rows(rows, valid_ids, activity, log)

        if len(rows) == before:
            break

    return _merge_cap_rows(rows, valid_ids, activity, log)


async def fetch_channel_log_24h(
    channel: discord.TextChannel | discord.Thread,
    *,
    limit: Optional[int] = None,
    hours: int = FUNNY_HISTORY_HOURS,
) -> tuple[str, set[int], dict[int, int]]:
    after = datetime.now(timezone.utc) - timedelta(hours=hours)
    lim = FUNNY_HISTORY_FETCH_LIMIT if limit is None else limit
    last_err: Optional[BaseException] = None

    for attempt in range(FUNNY_HISTORY_RETRIES):
        collected: list[discord.Message] = []
        try:
            async for m in channel.history(limit=lim, after=after, oldest_first=True):
                collected.append(m)
            log, ids, activity = _compile_log_lines(collected)
            if len(log) > FUNNY_LOG_MAX_CHARS:
                log = log[: FUNNY_LOG_MAX_CHARS - 80] + "\n...[log truncated for API size]"
            return log, ids, activity
        except discord.Forbidden:
            raise
        except (DiscordServerError, HTTPException, asyncio.TimeoutError, OSError, ConnectionError) as e:
            last_err = e
            if isinstance(e, HTTPException):
                st = getattr(e, "status", None)
                if st == 403:
                    raise
                if st is not None and st < 500 and st not in (408, 429):
                    raise
            transient = (
                isinstance(e, (asyncio.TimeoutError, OSError, ConnectionError))
                or _is_transient_discord_api_error(e)
            )
            if not transient:
                raise
            wait = min(FUNNY_HISTORY_RETRY_BASE_SEC * (2**attempt), 45.0)
            print(f"[funny] history fetch retry {attempt + 1}/{FUNNY_HISTORY_RETRIES} after {e!r} (sleep {wait:.1f}s)")
            await asyncio.sleep(wait)

    assert last_err is not None
    raise last_err


class FunnyCog(commands.Cog):
    """`6funny` bulk-scans 24h history; midnight task scans configured general channel."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._guild_cmd_ts: dict[int, float] = {}
        self._guild_op_lock: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _guild_cooldown_remaining(self, guild_id: int) -> float:
        last = self._guild_cmd_ts.get(guild_id, 0.0)
        elapsed = time.time() - last
        return max(0.0, FUNNY_CMD_COOLDOWN_SEC - elapsed)

    def _touch_guild_cooldown(self, guild_id: int) -> None:
        self._guild_cmd_ts[guild_id] = time.time()

    @commands.command(name="funny", aliases=["funnylb", "funleaderboard"])
    async def funny_cmd(self, ctx: commands.Context) -> None:
        """Scan this channel’s last 24h (up to FUNNY_HISTORY_LIMIT msgs) and show the comedy leaderboard."""
        if ctx.guild is None:
            await _funny_safe_send(ctx, "❌ server only.", delete_after=8)
            return

        if ctx.author.id not in FUNNY_CMD_ALLOWED_IDS:
            await _funny_safe_send(ctx, "❌ you don’t have access to **`6funny`**.", delete_after=8)
            return

        ch = ctx.channel
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            await _funny_safe_send(ctx, "❌ use a text channel or thread.", delete_after=8)
            return

        if not _grok_key():
            await _funny_safe_send(
                ctx,
                "❌ API not configured (**VENICE_API_KEY** or **OPENROUTER_API_KEY**).",
                delete_after=12,
            )
            return

        async with self._guild_op_lock[ctx.guild.id]:
            rem = self._guild_cooldown_remaining(ctx.guild.id)
            if rem > 0:
                mins = max(1, math.ceil(rem / 60))
                await _funny_safe_send(
                    ctx,
                    f"⏳ The chat is still being analyzed. Try again in **{mins}** minutes.",
                    delete_after=12,
                )
                return

            async with ctx.typing():
                try:
                    log, valid_ids, activity = await fetch_channel_log_24h(ch)
                except discord.Forbidden:
                    await _funny_safe_send(ctx, "❌ I can’t read history here.", delete_after=10)
                    return
                except (DiscordServerError, HTTPException) as e:
                    print(f"[funny] fetch (Discord API, after retries): {e}")
                    await _funny_safe_send(
                        ctx,
                        "❌ **Discord** is having issues (**503** / gateway). That’s on their side — "
                        "wait **1–2 minutes** and run **`6funny`** again.",
                        delete_after=22,
                    )
                    return
                except Exception as e:
                    print(f"[funny] fetch: {e}")
                    await _funny_safe_send(ctx, "❌ couldn’t load messages.", delete_after=10)
                    return

            if not log.strip():
                await _funny_safe_send(
                    ctx,
                    "No usable messages in the last **24 hours** here (bots and empty messages skipped).",
                    delete_after=12,
                )
                return

            async with ctx.typing():
                try:
                    rows = await full_leaderboard_pipeline(log, valid_ids, activity)
                except Exception as e:
                    print(f"[funny] grok: {e}")
                    await _funny_safe_send(ctx, f"❌ analysis failed: `{e}`", delete_after=15)
                    return

            if not rows:
                await _funny_safe_send(
                    ctx,
                    "Couldn’t build a leaderboard — try again later.",
                    delete_after=12,
                )
                return

            blocks: list[str] = []
            for i, (sc, uid, roast, quote) in enumerate(rows, 1):
                bar = funny_make_bar(sc)
                blocks.append(
                    f"**{i}.** <@{uid}> — **{sc}**/100\n{bar}\n"
                    f"*Verdict:* {discord.utils.escape_markdown(roast)}\n"
                    f"*Best line:* {discord.utils.escape_markdown(quote)}"
                )

            desc = "\n\n".join(blocks)
            if len(desc) > 4090:
                desc = desc[:4087] + "…"

            em = discord.Embed(
                title="The Comedy Leaderboard",
                description=desc,
                color=discord.Color.dark_magenta(),
            )
            ch_name = getattr(ch, "name", None) or "channel"
            em.set_footer(
                text=f"Past {FUNNY_HISTORY_HOURS}h in #{ch_name} · activity + humor mix · midnight UTC payout"
            )
            if await _funny_safe_send(ctx, embed=em):
                self._touch_guild_cooldown(ctx.guild.id)

    async def _run_silent_payout_for_channel(
        self, channel: discord.TextChannel | discord.Thread
    ) -> None:
        guild = channel.guild
        econ = self.bot.get_cog("EconomyCog")
        try:
            log, valid_ids, activity = await fetch_channel_log_24h(channel)
        except Exception as e:
            print(f"[funny] cron fetch: {e}")
            return

        if not log.strip() or not valid_ids:
            print("[funny] cron: no log / no users")
            return

        try:
            rows = await full_leaderboard_pipeline(log, valid_ids, activity)
        except Exception as e:
            print(f"[funny] cron grok: {e}")
            return

        if not rows:
            print("[funny] cron: empty ranking")
            return

        best_score, winner_id, _, _ = rows[0]
        if econ is not None:
            try:
                await econ._add_wallet(guild.id, winner_id, FUNNY_PRIZE_COINS)  # type: ignore[attr-defined]
            except Exception as e:
                print(f"[funny] cron wallet: {e}")

        announce_ch: Optional[discord.abc.Messageable] = None
        if FUNNY_ANNOUNCE_CHANNEL_ID:
            ch = self.bot.get_channel(FUNNY_ANNOUNCE_CHANNEL_ID)
            if isinstance(ch, discord.abc.Messageable):
                announce_ch = ch
            else:
                try:
                    fetched = await self.bot.fetch_channel(FUNNY_ANNOUNCE_CHANNEL_ID)
                    if isinstance(fetched, discord.abc.Messageable):
                        announce_ch = fetched
                except (discord.HTTPException, discord.NotFound):
                    pass

        msg = (
            f"👑 <@{winner_id}> secured **{FUNNY_PRIZE_COINS:,}** coins for being the funniest today. "
            f"Peak score: **{best_score}**/100."
        )
        if announce_ch:
            try:
                await announce_ch.send(msg)
            except discord.HTTPException as e:
                print(f"[funny] cron announce: {e}")

    @tasks.loop(time=dt_time(hour=0, minute=0, second=0, tzinfo=timezone.utc))
    async def funny_midnight_payout(self) -> None:
        cid = FUNNY_CRON_CHANNEL_ID or FUNNY_ANNOUNCE_CHANNEL_ID
        if not cid:
            print("[funny] cron: set FUNNY_CRON_CHANNEL_ID (or FUNNY_ANNOUNCE_CHANNEL_ID) for midnight scan")
            return

        ch = self.bot.get_channel(cid)
        if ch is None:
            try:
                ch = await self.bot.fetch_channel(cid)
            except (discord.HTTPException, discord.NotFound):
                print(f"[funny] cron: channel {cid} not found")
                return

        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            print(f"[funny] cron: channel {cid} is not text/thread")
            return

        await self._run_silent_payout_for_channel(ch)

    @funny_midnight_payout.before_loop
    async def _before_funny_payout(self) -> None:
        await self.bot.wait_until_ready()

    async def cog_load(self) -> None:
        if not self.funny_midnight_payout.is_running():
            self.funny_midnight_payout.start()

    async def cog_unload(self) -> None:
        self.funny_midnight_payout.cancel()
