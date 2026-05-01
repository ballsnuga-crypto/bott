"""
Free MBTI-style personality flow: intro post, 6 DM questions, Grok typing, cosmetic roles.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Optional

import aiohttp
import discord
from discord.ext import commands

SCRIPT_ROOT = Path(__file__).resolve().parent
STATE_FILE = SCRIPT_ROOT / "personality_state.json"

PERSONALITY_CHANNEL_ID = 1486008473010442383
INCOMPLETE_COOLDOWN_SEC = 2 * 24 * 3600  # 2 days to restart if abandoned

# Server onboarding: answer that assigns this role → bot DMs the MBTI flow (same as "Start in DMs").
MBTI_ONBOARDING_ROLE_ID = int(os.getenv("MBTI_ONBOARDING_ROLE_ID", "1493375742032220191"))

GROK_API_URL = os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")
GROK_MODEL = os.getenv("OPENROUTER_MODEL", "tngtech/deepseek-r1t2-chimera")

MBTI_TYPES = frozenset(
    "INTJ INTP ENTJ ENTP INFJ INFP ENFJ ENFP "
    "ISTJ ISFJ ESTJ ESFJ ISTP ISFP ESTP ESFP".split()
)

# Temperament colors (Analysts, Diplomats, Sentinels, Explorers)
_PURPLE = discord.Color(0x9B59B6)  # NT
_GREEN = discord.Color(0x2ECC71)  # NF
_BLUE = discord.Color(0x3498DB)  # SJ
_GOLD = discord.Color(0xF1C40F)  # SP

MBTI_COLORS: dict[str, discord.Color] = {}
for t in ("INTJ", "INTP", "ENTJ", "ENTP"):
    MBTI_COLORS[t] = _PURPLE
for t in ("INFJ", "INFP", "ENFJ", "ENFP"):
    MBTI_COLORS[t] = _GREEN
for t in ("ISTJ", "ISFJ", "ESTJ", "ESFJ"):
    MBTI_COLORS[t] = _BLUE
for t in ("ISTP", "ISFP", "ESTP", "ESFP"):
    MBTI_COLORS[t] = _GOLD

def _personality_role_name(typ: str) -> str:
    """Cosmetic role name = four-letter type only (e.g. INTJ)."""
    return typ

QUESTIONS: tuple[str, ...] = (
    "**Exploring a new topic**\n\n"
    "You come across an unfamiliar topic that catches your interest (for example, something you read or hear about). Describe what you actually do next and what keeps your attention as you look into it. Walk through your typical process step by step.",
    "**Voice call with friends**\n\n"
    "You're in a voice call with a group of friends while doing an activity together (like playing a game). Describe your usual role or dynamic in the conversation. What do you tend to do or focus on most?",
    "**Incorrect statement in a group**\n\n"
    "In an online community or group chat, someone makes a statement that you believe is clearly incorrect, and others seem to agree. What do you usually do in that situation, and what factors influence your choice?",
    "**Starting a creative or building project**\n\n"
    "You open a blank file or workspace to create or build something new (for example, a map, track, design, or any project). Describe what your first 10–15 minutes usually look like and how you get started.",
    "**Working under a strict schedule**\n\n"
    "You have to work on a project with a very detailed, minute-by-minute schedule and frequent check-ins from someone overseeing your progress. How does that situation typically affect how you feel and how you perform? Be as honest as possible.",
    "**Defending an opinion**\n\n"
    "Think of an opinion or idea you hold that others might find unusual, debatable, or not very important. If the topic came up, how would you explain or defend your view? Describe your approach.",
)

PERSONALITY_SYSTEM_PROMPT = """You are a master MBTI profiler. Analyze the user's 6 answers to determine their 4-letter type.

CRITICAL RULES FOR ACCURACY:
1. Do NOT assume someone is an Introvert (I) just because they play video games or stay in their room. Look at whether their mental energy is driven by bouncing ideas off others or keeping to themselves.
2. Look heavily for Ne (Extraverted Intuition) vs Se (Extraverted Sensing). If they love debating, what-if scenarios, and jumping between random topics, they are Intuitive (N). If they focus strictly on physical mechanics, realism, and literal facts, they are Sensing (S).
3. After the 6th answer, your job is done: output their MBTI type and a blunt, highly accurate breakdown.

You MUST respond in exactly this format (no extra lines before TYPE, no markdown):

TYPE: XXXX
SUMMARY: [2-3 sentences, blunt and specific — call out exact words or behaviors from their answers that locked in each letter. No bullet lists.]

Hard rules:
- XXXX must be exactly one of: INTJ, INTP, ENTJ, ENTP, INFJ, INFP, ENFJ, ENFP, ISTJ, ISFJ, ESTJ, ESFJ, ISTP, ISFP, ESTP, ESFP.
- SUMMARY must stay under 180 words, plain text only.
- Do not ask more questions."""


def _grok_key() -> Optional[str]:
    return os.getenv("OPENROUTER_API_KEY") or os.getenv("XAI_API_KEY") or os.getenv("GROK_API_KEY")


async def _grok_personality(user_block: str) -> str:
    key = _grok_key()
    if not key:
        raise RuntimeError("Missing OPENROUTER_API_KEY")
    payload = {
        "model": GROK_MODEL,
        "messages": [
            {"role": "system", "content": PERSONALITY_SYSTEM_PROMPT},
            {"role": "user", "content": user_block},
        ],
        "temperature": 0.55,
        "max_tokens": 520,
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://6xs.lol",
        "X-Title": "6XS Bot",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(GROK_API_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"OpenRouter API {resp.status}: {body[:500]}")
            data = json.loads(body)
    try:
        return data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"Bad Grok response: {body[:400]}") from e


def _parse_type_and_summary(raw: str) -> tuple[Optional[str], str]:
    upper = raw.upper()
    m = re.search(r"TYPE:\s*([A-Z]{4})\b", upper.replace("TYPE :", "TYPE:"))
    typ = m.group(1) if m else None
    if typ and typ not in MBTI_TYPES:
        typ = None
    sm = re.search(r"SUMMARY:\s*(.+)", raw, re.DOTALL | re.IGNORECASE)
    summary = sm.group(1).strip() if sm else raw
    summary = summary[:1200]
    return typ, summary


def _load_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        pass
    return {"intro": {}, "users": {}, "role_ids": {}}


def _save_state(data: dict[str, Any]) -> None:
    try:
        STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except OSError as e:
        print(f"[personality] save state failed: {e}")


class PersonalityCog(commands.Cog):
    """Personality profiler + MBTI cosmetic roles."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.bot.add_view(PersonalityIntroView(self))

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        await self._ensure_intro_once()

    def _intro_still_valid(self, channel: discord.TextChannel, intro: dict[str, Any]) -> bool:
        if not intro.get("posted") or not intro.get("message_id"):
            return False
        if int(intro.get("channel_id", 0)) != channel.id:
            return False
        return True

    async def _ensure_intro_once(self) -> None:
        await self.bot.wait_until_ready()
        try:
            ch = await self.bot.fetch_channel(PERSONALITY_CHANNEL_ID)
        except (discord.NotFound, discord.HTTPException) as e:
            print(f"[personality] cannot fetch channel {PERSONALITY_CHANNEL_ID}: {e}")
            return
        if not isinstance(ch, discord.TextChannel):
            print(f"[personality] channel {PERSONALITY_CHANNEL_ID} is not a text channel")
            return

        state = _load_state()
        intro = state.setdefault("intro", {})
        if self._intro_still_valid(ch, intro):
            try:
                msg = await ch.fetch_message(int(intro["message_id"]))
                if msg.author.id == self.bot.user.id:
                    return
            except (discord.NotFound, discord.Forbidden, ValueError, TypeError):
                pass

        embed = discord.Embed(
            title="🧠 Personality & roles",
            description=(
                "To get your personality role, answer **6 questions** in DMs.\n\n"
                "Press **Start in DMs** below."
            ),
            color=discord.Color.blurple(),
        )
        view = PersonalityIntroView(self)
        msg = await ch.send(embed=embed, view=view)
        intro["posted"] = True
        intro["channel_id"] = ch.id
        intro["guild_id"] = ch.guild.id
        intro["message_id"] = msg.id
        _save_state(state)
        print(f"[personality] posted intro in #{ch.name} (message {msg.id})")

    def _user_entry(self, state: dict[str, Any], uid: int) -> dict[str, Any]:
        users = state.setdefault("users", {})
        key = str(uid)
        if key not in users:
            users[key] = {}
        return users[key]

    async def begin_mbti_flow(
        self,
        user: discord.abc.User,
        guild: discord.Guild,
        *,
        dm_preface: Optional[str] = None,
    ) -> str:
        """
        Start the 6-question MBTI DM flow. Returns:
        - "started" — question 1 sent
        - "already_done" — finished test before
        - "in_progress" — test active (cooldown); user should check DMs
        - "no_dm" — could not open DM (state reset if we had started writing)
        """
        uid = user.id
        now = time.time()
        state = _load_state()
        u = self._user_entry(state, uid)

        if u.get("status") == "done":
            return "already_done"

        if u.get("status") == "in_progress":
            started = float(u.get("started_at", 0))
            answers = u.get("answers") or []
            if now - started < INCOMPLETE_COOLDOWN_SEC and len(answers) < len(QUESTIONS):
                return "in_progress"
            u.clear()

        u["status"] = "in_progress"
        u["guild_id"] = guild.id
        u["started_at"] = now
        u["answers"] = []
        _save_state(state)

        q1 = "**Question 1/6**\n\n" + QUESTIONS[0]
        body = f"{dm_preface}{q1}" if dm_preface else q1
        try:
            dm = await user.create_dm()
            await dm.send(body)
        except discord.Forbidden:
            u.clear()
            _save_state(state)
            return "no_dm"

        return "started"

    async def handle_start_button(self, interaction: discord.Interaction) -> None:
        if not interaction.guild:
            return await interaction.response.send_message("Use this button in the server.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)
        result = await self.begin_mbti_flow(interaction.user, interaction.guild)

        if result == "already_done":
            await interaction.followup.send(
                "You already completed the personality test. Your role stays until you change it manually.",
                ephemeral=True,
            )
            return
        if result == "in_progress":
            state = _load_state()
            u = self._user_entry(state, interaction.user.id)
            started = float(u.get("started_at", 0))
            await interaction.followup.send(
                "You already have a test in progress — **check your DMs** for the next question. "
                f"If you abandoned it, you can try again in **{int((INCOMPLETE_COOLDOWN_SEC - (time.time() - started)) // 3600)}h**.",
                ephemeral=True,
            )
            return
        if result == "no_dm":
            await interaction.followup.send(
                "I can't DM you — enable **Messages** from server members in Privacy & Safety, then try again.",
                ephemeral=True,
            )
            return

        await interaction.followup.send("Sent you **question 1/6** in DMs.", ephemeral=True)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member) -> None:
        if MBTI_ONBOARDING_ROLE_ID <= 0:
            return
        if before.roles == after.roles:
            return
        role = after.guild.get_role(MBTI_ONBOARDING_ROLE_ID)
        if role is None:
            return
        if role not in after.roles or role in before.roles:
            return

        preface = (
            "You chose to take the **MBTI test** during server onboarding.\n\n"
        )
        result = await self.begin_mbti_flow(after, after.guild, dm_preface=preface)
        if result == "already_done":
            try:
                dm = await after.create_dm()
                await dm.send(
                    "You already finished the personality test — your type role should still be on your profile in the server."
                )
            except discord.Forbidden:
                pass
        elif result == "in_progress":
            try:
                dm = await after.create_dm()
                await dm.send(
                    "You already have the MBTI test in progress — **check your DMs above** for the next question."
                )
            except discord.Forbidden:
                pass
        elif result == "no_dm":
            print(
                f"[personality] onboarding MBTI: cannot DM user {after.id} — "
                "they need Messages from server members enabled, then personality channel button works."
            )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild:
            return
        uid = message.author.id
        state = _load_state()
        u = state.get("users", {}).get(str(uid))
        if not u or u.get("status") != "in_progress":
            return

        text = (message.content or "").strip()
        if not text or len(text) > 4000:
            await message.channel.send("Send a real answer (shorter than 4000 characters).")
            return

        answers: list[str] = list(u.get("answers") or [])
        answers.append(text[:3500])
        u["answers"] = answers
        _save_state(state)

        n = len(answers)
        if n < len(QUESTIONS):
            await message.channel.send(f"**Question {n + 1}/6**\n\n{QUESTIONS[n]}")
            return

        # Finished — Grok + role
        await message.channel.send("Got all six — thinking… give me a moment.")
        guild_id = int(u.get("guild_id", 0))
        guild = self.bot.get_guild(guild_id) if guild_id else None

        qa_lines = []
        for i, (q, a) in enumerate(zip(QUESTIONS, answers)):
            qa_lines.append(f"Q{i+1}: {q}\nA{i+1}: {a}")
        user_block = "\n\n".join(qa_lines)

        try:
            raw = await _grok_personality(user_block)
        except Exception as e:
            u["status"] = "in_progress"
            u["answers"] = answers[:-1]
            _save_state(state)
            await message.channel.send(f"API error — try again in a bit: `{e}`")
            return

        typ, summary = _parse_type_and_summary(raw)
        if not typ:
            mu = re.search(r"\b(INTJ|INTP|ENTJ|ENTP|INFJ|INFP|ENFJ|ENFP|ISTJ|ISFJ|ESTJ|ESFJ|ISTP|ISFP|ESTP|ESFP)\b", raw.upper())
            if mu:
                typ = mu.group(1)

        if not typ:
            u["status"] = "in_progress"
            u["answers"] = answers[:-1]
            _save_state(state)
            await message.channel.send(
                "I couldn't read your type from the model — please answer the last question again with a bit more detail."
            )
            return

        u["status"] = "done"
        u["mbti"] = typ
        u["completed_at"] = time.time()
        u.pop("answers", None)
        _save_state(state)

        out = f"**You're typed as {typ}.**\n\n{summary}"
        if len(out) > 1950:
            out = out[:1947] + "…"
        await message.channel.send(out)

        if guild:
            member = guild.get_member(uid)
            if member:
                try:
                    role = await self._ensure_mbti_role(guild, typ)
                    if role:
                        to_remove = [
                            r
                            for r in member.roles
                            if r != role
                            and (
                                r.name in MBTI_TYPES
                                or r.name.startswith("MBTI · ")
                            )
                        ]
                        if to_remove:
                            await member.remove_roles(*to_remove, reason="MBTI retag")
                        await member.add_roles(role, reason=f"Personality test: {typ}")
                        await message.channel.send(
                            f"I've given you **{role.name}** in **{guild.name}**."
                        )
                    else:
                        await message.channel.send("Couldn't create/find your role — tell an admin to check bot permissions.")
                except discord.Forbidden:
                    await message.channel.send(
                        "I don't have permission to assign roles — move my role **above** the MBTI roles and grant **Manage Roles**."
                    )
            else:
                await message.channel.send(
                    f"Rejoin **{guild.name}** and run the test again from the button if you didn't get a role."
                )
        else:
            await message.channel.send("Server not found — join the server and ask staff if you need the role.")

    async def _ensure_mbti_role(self, guild: discord.Guild, typ: str) -> Optional[discord.Role]:
        state = _load_state()
        rid_map = state.setdefault("role_ids", {}).setdefault(str(guild.id), {})
        cached = rid_map.get(typ)
        if cached:
            r = guild.get_role(int(cached))
            if r:
                return r
            rid_map.pop(typ, None)
            _save_state(state)

        name = _personality_role_name(typ)
        legacy = f"MBTI · {typ}"
        for r in guild.roles:
            if r.name == name or r.name == legacy:
                rid_map[typ] = r.id
                _save_state(state)
                return r

        color = MBTI_COLORS.get(typ, discord.Color.light_gray())
        role = await guild.create_role(
            name=name,
            color=color,
            mentionable=False,
            reason="Personality test cosmetic MBTI role",
        )
        me = guild.me
        if me and me.top_role.position > 1:
            try:
                await role.edit(position=me.top_role.position - 1)
            except discord.HTTPException:
                pass
        rid_map[typ] = role.id
        _save_state(state)
        return role


class PersonalityIntroView(discord.ui.View):
    def __init__(self, cog: PersonalityCog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Start in DMs",
        style=discord.ButtonStyle.success,
        custom_id="personality_intro_start_v1",
        emoji="🧠",
    )
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.handle_start_button(interaction)
