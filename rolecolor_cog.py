"""
6rolecolor — personal color-only role matching another role's colour or gradient (not its permissions).
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands

ROLECOLOR_STATE = (Path(__file__).resolve().parent / "rolecolor_state.json").resolve()
ROLE_NAME_PREFIX = "6xs·c·"


def _managed_role_name(user_id: int) -> str:
    return f"{ROLE_NAME_PREFIX}{user_id}"


def _truncate_label(name: str, max_len: int = 80) -> str:
    s = name.replace("\n", " ").strip()
    if len(s) <= max_len:
        return s or "role"
    return s[: max_len - 1] + "…"


def _format_source_colours(source: discord.Role) -> str:
    """Describe primary + gradient stops for Discord’s multi-colour roles."""
    p = source.color
    bits: list[str] = []
    if p.value:
        bits.append(f"#{p.value:06x}")
    else:
        bits.append("default")
    if source.secondary_color is not None:
        bits.append(f"#{source.secondary_color.value:06x}")
    if source.tertiary_color is not None:
        bits.append(f"#{source.tertiary_color.value:06x}")
    if len(bits) == 1:
        return bits[0]
    return "gradient " + " → ".join(bits)


class RoleColorPickView(discord.ui.View):
    """Pick one of your roles with buttons (paged)."""

    def __init__(
        self,
        cog: RoleColorCog,
        guild: discord.Guild,
        owner_id: int,
        roles: list[discord.Role],
        page: int = 0,
    ) -> None:
        super().__init__(timeout=180)
        self.cog = cog
        self.guild = guild
        self.owner_id = owner_id
        self.roles = roles
        self.page = page
        self.per_page = 20
        self.message: Optional[discord.Message] = None

        start = page * self.per_page
        chunk = roles[start : start + self.per_page]
        for i, role in enumerate(chunk):
            row = i // 5
            self.add_item(_RoleColorPickBtn(role.id, _truncate_label(role.name), row))

        total_pages = max(1, (len(roles) + self.per_page - 1) // self.per_page)
        if total_pages > 1:
            self.add_item(
                _RoleColorNavBtn(
                    -1,
                    disabled=page <= 0,
                    row=4,
                )
            )
            self.add_item(
                _RoleColorNavBtn(
                    1,
                    disabled=page >= total_pages - 1,
                    row=4,
                )
            )

    @staticmethod
    def make_embed(page: int, total_roles: int, per_page: int) -> discord.Embed:
        total_pages = max(1, (total_roles + per_page - 1) // per_page)
        if total_pages > 1:
            foot = f"Page **{page + 1}** / **{total_pages}** · **{total_roles}** role(s) you can use"
        else:
            foot = f"**{total_roles}** role(s) — tap a button to copy that colour"
        return discord.Embed(
            title="6rolecolor — pick a role",
            description=(
                "• **`6rolecolor @Role`** to type a role.\n"
                "• **`6rolecolor off`** removes the cosmetic role."
            ),
            color=discord.Color.blurple(),
        ).set_footer(text=foot)

    async def handle_pick(self, interaction: discord.Interaction, role_id: int) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This menu isn’t for you.", ephemeral=True
            )
            return
        if interaction.guild is None:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        role = interaction.guild.get_role(role_id)
        if role is None:
            await interaction.response.send_message(
                "That role isn’t here anymore — run **`6rolecolor`** again.",
                ephemeral=True,
            )
            return
        member = interaction.guild.get_member(self.owner_id)
        if member is None:
            try:
                member = await interaction.guild.fetch_member(self.owner_id)
            except discord.HTTPException:
                await interaction.response.send_message(
                    "Couldn’t load your member profile.", ephemeral=True
                )
                return
        ok, msg = await self.cog._apply_source_color(self.guild, member, role)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True)
            return
        await interaction.response.edit_message(content=msg, embed=None, view=None)

    async def handle_page(self, interaction: discord.Interaction, delta: int) -> None:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "This menu isn’t for you.", ephemeral=True
            )
            return
        new_page = self.page + delta
        total_pages = (len(self.roles) + self.per_page - 1) // self.per_page
        if new_page < 0 or new_page >= total_pages:
            await interaction.response.defer()
            return
        nv = RoleColorPickView(
            self.cog, self.guild, self.owner_id, self.roles, page=new_page
        )
        nv.message = self.message
        emb = self.make_embed(new_page, len(self.roles), self.per_page)
        await interaction.response.edit_message(embed=emb, view=nv)

    async def on_timeout(self) -> None:
        for c in self.children:
            c.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class _RoleColorPickBtn(discord.ui.Button):
    def __init__(self, role_id: int, label: str, row: int) -> None:
        super().__init__(
            label=label,
            style=discord.ButtonStyle.secondary,
            row=row,
            custom_id=f"rcpick:{role_id}",
        )
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view: RoleColorPickView = self.view  # type: ignore
        await view.handle_pick(interaction, self.role_id)


class _RoleColorNavBtn(discord.ui.Button):
    def __init__(self, delta: int, *, disabled: bool, row: int) -> None:
        super().__init__(
            emoji="◀" if delta < 0 else "▶",
            style=discord.ButtonStyle.gray,
            row=row,
            disabled=disabled,
            custom_id=f"rcnav:{delta}",
        )
        self.delta = delta

    async def callback(self, interaction: discord.Interaction) -> None:
        view: RoleColorPickView = self.view  # type: ignore
        await view.handle_page(interaction, self.delta)


class RoleColorCog(commands.Cog):
    """Cosmetic role colour copied from a role you already have."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._lock = asyncio.Lock()
        self._data: dict[str, dict[str, int]] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(ROLECOLOR_STATE.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self._data = {}
                return
            out: dict[str, dict[str, int]] = {}
            for gk, gv in raw.items():
                if not isinstance(gv, dict):
                    continue
                inner: dict[str, int] = {}
                for uk, rv in gv.items():
                    try:
                        inner[str(uk)] = int(rv)
                    except (TypeError, ValueError):
                        continue
                out[str(gk)] = inner
            self._data = out
        except (OSError, json.JSONDecodeError):
            self._data = {}

    async def _save(self) -> None:
        async with self._lock:
            snap: dict[str, dict[str, int]] = {
                gk: dict(gv) for gk, gv in self._data.items()
            }
        ROLECOLOR_STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = ROLECOLOR_STATE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snap, indent=0), encoding="utf-8")
        os.replace(tmp, ROLECOLOR_STATE)

    def _remember(self, guild_id: int, user_id: int, role_id: int) -> None:
        gk, uk = str(guild_id), str(user_id)
        self._data.setdefault(gk, {})[uk] = role_id

    def _forget(self, guild_id: int, user_id: int) -> None:
        gk, uk = str(guild_id), str(user_id)
        if gk in self._data and uk in self._data[gk]:
            del self._data[gk][uk]
            if not self._data[gk]:
                del self._data[gk]

    def _cached_role_id(self, guild_id: int, user_id: int) -> Optional[int]:
        rid = self._data.get(str(guild_id), {}).get(str(user_id))
        return int(rid) if rid is not None else None

    def _find_managed_role(
        self, guild: discord.Guild, user_id: int
    ) -> Optional[discord.Role]:
        want = _managed_role_name(user_id)
        rid = self._cached_role_id(guild.id, user_id)
        if rid:
            r = guild.get_role(rid)
            if r and r.name == want:
                return r
            self._forget(guild.id, user_id)
        for r in guild.roles:
            if r.name == want:
                self._remember(guild.id, user_id, r.id)
                return r
        return None

    @staticmethod
    def _can_manage_role(guild: discord.Guild, role: discord.Role) -> bool:
        me = guild.me
        if not me or not me.guild_permissions.manage_roles:
            return False
        if role >= me.top_role:
            return False
        return True

    def _eligible_source_roles(self, member: discord.Member) -> list[discord.Role]:
        """Roles the member has that can be used as colour sources (same rules as before)."""
        guild = member.guild
        me = guild.me
        if not me:
            return []
        skip = _managed_role_name(member.id)
        out: list[discord.Role] = []
        for r in member.roles:
            if r.is_default():
                continue
            if r.managed:
                continue
            if r.name == skip:
                continue
            if r >= me.top_role:
                continue
            out.append(r)
        return sorted(out, key=lambda x: x.position, reverse=True)

    async def _apply_source_color(
        self,
        guild: discord.Guild,
        member: discord.Member,
        source: discord.Role,
    ) -> tuple[bool, str]:
        """Apply colour from `source`; return (success, user-facing message)."""
        if source.is_default():
            return False, "❌ pick a real role, not **@everyone**."
        if source.managed:
            return (
                False,
                "❌ that role is **integration-managed** (bot/boost) — pick a normal role you have.",
            )
        if source not in member.roles:
            return (
                False,
                f"❌ you need to **have** {source.mention} before you can match its colour.\n"
                "*If staff gave you access under a different role, use that role instead.*",
            )
        if not self._can_manage_role(guild, source):
            return (
                False,
                "❌ I can’t work with that role — move **my role** **above** it and give me **Manage Roles**.",
            )

        me = guild.me
        assert me is not None
        cap = me.top_role.position - 1
        if cap < 1:
            return (
                False,
                "❌ my role is too low in the list — raise it under **Server settings → Roles**.",
            )

        want_pos = member.top_role.position + 1
        if want_pos > cap:
            return (
                False,
                "❌ your **top role** is too high for me to place a colour-only role above it. "
                "Ask an admin to lower your highest role **below** mine, or use an alt.",
            )

        name = _managed_role_name(member.id)
        perms = discord.Permissions.none()
        # Mirror Discord’s role colours object (primary + optional gradient stops).
        sec = source.secondary_color
        tert = source.tertiary_color

        try:
            managed = self._find_managed_role(guild, member.id)
            if managed:
                await managed.edit(
                    name=name,
                    color=source.color,
                    secondary_color=sec,
                    tertiary_color=tert,
                    permissions=perms,
                    hoist=False,
                    mentionable=False,
                    reason=f"6rolecolor — match {source.name}",
                )
                role = managed
            else:
                role = await guild.create_role(
                    name=name,
                    color=source.color,
                    secondary_color=sec,
                    tertiary_color=tert,
                    permissions=perms,
                    hoist=False,
                    mentionable=False,
                    reason=f"6rolecolor — match {source.name}",
                )
                self._remember(guild.id, member.id, role.id)

            await role.edit(position=want_pos)
            if role not in member.roles:
                await member.add_roles(role, reason="6rolecolor cosmetic")
        except discord.Forbidden:
            return (
                False,
                "❌ **Manage Roles** denied or hierarchy blocked — put **my role** above the colour role "
                "and above **your** highest role slot I need to use.",
            )
        except discord.HTTPException as e:
            return False, f"❌ Discord rejected that: `{e}`"

        await self._save()
        col_desc = _format_source_colours(source)
        return (
            True,
            f"✅ Your **display colour** now follows **{source.name}** (`{col_desc}`).\n"
            f"Role **{role.name}** has **no permissions** — only the colour/gradient matches, not {source.mention}’s powers.\n"
            f"`6rolecolor off` to remove.",
        )

    @commands.command(name="rolecolor")
    async def rolecolor_cmd(self, ctx: commands.Context, *, rest: str = "") -> None:
        """
        Copy **name colour / gradient** from a role you **already have** onto a personal role (no extra permissions).
        `6rolecolor` — **buttons** to pick from your roles · `6rolecolor @Role` · `6rolecolor off` to remove.
        """
        if ctx.guild is None:
            return await ctx.send("❌ use this in a server.", delete_after=8)

        rest = (rest or "").strip()
        low = rest.lower()

        if not rest:
            guild = ctx.guild
            member = ctx.author
            if not isinstance(member, discord.Member):
                member = guild.get_member(member.id) or await guild.fetch_member(member.id)

            me = guild.me
            if not me or not me.guild_permissions.manage_roles:
                return await ctx.send(
                    "❌ I need **Manage Roles** to set colours.", delete_after=10
                )

            roles = self._eligible_source_roles(member)
            if not roles:
                return await ctx.send(
                    "❌ No roles here to pick from — you need at least one **normal** role "
                    "that sits **below** my highest role (not @everyone, not integration-managed, not your "
                    f"**{_managed_role_name(member.id)}** slot). Ask an admin to adjust role order or use "
                    "**`6rolecolor @Role`** if the role exists but isn’t listed.",
                    delete_after=20,
                )

            view = RoleColorPickView(self, guild, member.id, roles, page=0)
            emb = view.make_embed(0, len(roles), view.per_page)
            msg = await ctx.send(embed=emb, view=view)
            view.message = msg
            return

        if low in ("off", "clear", "none", "remove", "reset"):
            return await self._rolecolor_clear(ctx)

        try:
            source = await commands.RoleConverter().convert(ctx, rest)
        except commands.BadArgument:
            return await ctx.send(
                "❌ couldn’t find that role — mention it (`@Role`) or use its exact name, "
                "or run **`6rolecolor`** with no args to use buttons.",
                delete_after=10,
            )

        assert ctx.guild is not None
        guild = ctx.guild
        member = ctx.author
        if not isinstance(member, discord.Member):
            member = guild.get_member(member.id) or await guild.fetch_member(member.id)

        ok, msg = await self._apply_source_color(guild, member, source)
        await ctx.send(msg, delete_after=None if ok else 12)

    async def _rolecolor_clear(self, ctx: commands.Context) -> None:
        assert ctx.guild is not None
        guild = ctx.guild
        member = ctx.author
        if not isinstance(member, discord.Member):
            member = guild.get_member(member.id) or await guild.fetch_member(member.id)

        role = self._find_managed_role(guild, member.id)
        if not role:
            self._forget(guild.id, member.id)
            await self._save()
            return await ctx.send("Nothing to clear — you don’t have a **6rolecolor** role.", delete_after=8)

        try:
            if role in member.roles:
                await member.remove_roles(role, reason="6rolecolor off")
            if len(role.members) == 0 and self._can_manage_role(guild, role):
                await role.delete(reason="6rolecolor off — empty")
            self._forget(guild.id, member.id)
            await self._save()
        except discord.Forbidden:
            return await ctx.send(
                "❌ couldn’t remove the role — check **Manage Roles** and hierarchy.",
                delete_after=10,
            )
        except discord.HTTPException as e:
            return await ctx.send(f"❌ `{e}`", delete_after=8)

        await ctx.send("✅ Colour role removed.", delete_after=8)
