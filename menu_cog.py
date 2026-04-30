"""Public **`6help`** (all commands) and **`6play`** (dropdown + quick actions)."""
from __future__ import annotations

from collections import defaultdict

import discord
from discord.ext import commands


def _pfx(ctx: commands.Context) -> str:
    return (ctx.prefix or "6").strip().rstrip()


def _collect_help_lines(bot: commands.Bot, prefix: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for cmd in sorted(bot.walk_commands(), key=lambda c: c.qualified_name):
        if cmd.hidden:
            continue
        root = cmd.root_parent or cmd
        if getattr(root, "hidden", False):
            continue
        cog_name = cmd.cog_name or "Other"
        brief = (cmd.help or cmd.brief or "—").strip().split("\n")[0][:120]
        line = f"`{prefix}{cmd.qualified_name}` — {brief}"
        rows.append((cog_name, line))
    return rows


def _help_embeds(bot: commands.Bot, prefix: str) -> list[discord.Embed]:
    by: dict[str, list[str]] = defaultdict(list)
    for cog_name, line in _collect_help_lines(bot, prefix):
        by[cog_name].append(line)
    for k in by:
        by[k].sort()

    embeds: list[discord.Embed] = []
    title = "Six bot — all commands"
    desc = f"Prefix **`{prefix}`** or **`{prefix} `** · Hub: **`{prefix}play`**"
    em = discord.Embed(title=title, description=desc, color=discord.Color.blurple())
    total_chars = len(desc)
    first = True

    for cog_name in sorted(by.keys()):
        body = "\n".join(by[cog_name])
        if len(body) > 1024:
            body = body[:1021] + "…"
        field_len = len(cog_name) + len(body) + 32
        if em.fields and (total_chars + field_len > 5200 or len(em.fields) >= 6):
            embeds.append(em)
            em = discord.Embed(
                title="Six bot — commands (continued)",
                color=discord.Color.blurple(),
            )
            total_chars = 0
            first = False
        em.add_field(name=cog_name[:256], value=body, inline=False)
        total_chars += field_len

    if em.fields or first:
        embeds.append(em)
    if not embeds:
        embeds.append(
            discord.Embed(title="Help", description="No commands registered.", color=discord.Color.greyple())
        )
    return embeds


class PlayHubView(discord.ui.View):
    def __init__(self, bot: commands.Bot, owner_id: int) -> None:
        super().__init__(timeout=360)
        self.bot = bot
        self.owner_id = owner_id
        opts = [
            discord.SelectOption(label="Cell — profile & upgrades", value="cellbal"),
            discord.SelectOption(label="Cell — claim income", value="cellclaim"),
            discord.SelectOption(label="Cell — inventory", value="cellinv"),
            discord.SelectOption(label="Cell — role shop (cells)", value="cellshop"),
            discord.SelectOption(label="Cell — rarities / tips", value="rares"),
            discord.SelectOption(label="Coins — balance", value="balance"),
            discord.SelectOption(label="Coins — daily", value="daily"),
            discord.SelectOption(label="Coins — 6shop (banner + roles)", value="shop_hint"),
            discord.SelectOption(label="Gamble — 6gamble", value="hint_gamble"),
            discord.SelectOption(label="Gamble — 6crash", value="hint_crash"),
            discord.SelectOption(label="Gamble — 6ladder", value="hint_ladder"),
            discord.SelectOption(label="Gamble — 6mines", value="hint_mines"),
            discord.SelectOption(label="Gamble — 6blackjack", value="hint_bj"),
            discord.SelectOption(label="Gamble — 6coinflip", value="hint_cf"),
            discord.SelectOption(label="CS2 — 6crate / 6unbox", value="hint_cs2"),
            discord.SelectOption(label="Poly — 6polytrending / 6polybet", value="hint_poly"),
            discord.SelectOption(label="6xs — rank", value="hint_xs"),
            discord.SelectOption(label="6xs — leaderboard", value="hint_xslb"),
            discord.SelectOption(label="Fun — 6topic", value="hint_topic"),
            discord.SelectOption(label="Fun — 6gif", value="hint_gif"),
            discord.SelectOption(label="Media — 6repost", value="hint_repost"),
            discord.SelectOption(label="Show full command list (6help)", value="help"),
        ]
        sel = discord.ui.Select(placeholder="Choose a command…", row=0, min_values=1, max_values=1, options=opts)
        sel.callback = self._on_select
        self.add_item(sel)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message("This **`6play`** panel isn’t yours.", ephemeral=True)
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction) -> None:
        assert interaction.data and "values" in interaction.data
        v = interaction.data["values"][0]
        hc = self.bot.get_cog("HoldingCellCog")
        ec = self.bot.get_cog("EconomyCog")

        if v == "cellbal" and hc:
            await hc.play_panel_cellbal(interaction)
            return
        if v == "cellclaim" and hc:
            await hc.play_panel_cellclaim(interaction)
            return
        if v == "cellinv" and hc:
            await hc.play_panel_cellinv(interaction)
            return
        if v == "cellshop" and hc:
            await hc.play_panel_cellshop(interaction)
            return
        if v == "rares" and hc:
            await hc.play_panel_rares(interaction)
            return
        if v == "balance" and ec:
            await ec.play_panel_balance(interaction)
            return
        if v == "daily" and ec:
            await ec.play_panel_daily(interaction)
            return

        p = "6"
        hints: dict[str, str] = {
            "shop_hint": f"Run **`{p}shop`** in this channel — **6XS Shop** banner + role purchase buttons.",
            "hint_gamble": f"`{p}gamble <amount>` — high/low style game (wallet).",
            "hint_crash": f"`{p}crash` — multiplier rocket (follow prompts).",
            "hint_ladder": f"`{p}ladder <stake>` — climb or bank.",
            "hint_mines": f"`{p}mines` — minefield (follow prompts).",
            "hint_bj": f"`{p}blackjack <bet>` — 21.",
            "hint_cf": f"`{p}coinflip <amount> heads|tails`",
            "hint_cs2": f"`{p}crate` / `{p}unbox <crate_id>` — CS2-style cases · `{p}inv`",
            "hint_poly": f"`{p}polytrending` · `{p}polybet` / `{p}polycheck`",
            "hint_xs": f"`{p}xs` — chat rank · `{p}xslb` — leaderboard",
            "hint_xslb": f"`{p}xslb`",
            "hint_topic": f"`{p}topic` — random revive line (Grok).",
            "hint_gif": f"`{p}gif` — clip to GIF (reply to video).",
            "hint_repost": f"`{p}repost` — mirror tweets / TikTok (see command help).",
            "help": f"Run **`{p}help`** in a channel to post the **full** command list for everyone.",
        }

        if v == "help":
            await interaction.response.defer(ephemeral=True)
            embeds = _help_embeds(self.bot, p)
            await interaction.followup.send(embed=embeds[0], ephemeral=True)
            for extra in embeds[1:10]:
                await interaction.followup.send(embed=extra, ephemeral=True)
            return

        text = hints.get(v)
        if text:
            await interaction.response.send_message(text, ephemeral=True)
            return

        await interaction.response.send_message("Unknown option — try again.", ephemeral=True)

    async def on_timeout(self) -> None:
        for c in self.children:
            c.disabled = True
        try:
            if hasattr(self, "message") and self.message:
                await self.message.edit(view=self)
        except discord.HTTPException:
            pass


class MenuCog(commands.Cog):
    """`6help` + `6play`."""

    @commands.command(name="help")
    async def help_cmd(self, ctx: commands.Context) -> None:
        """List every command the bot exposes (non-hidden)."""
        p = _pfx(ctx)
        for em in _help_embeds(ctx.bot, p):
            await ctx.send(embed=em)

    @commands.command(name="play", aliases=["panel", "hub"])
    async def play_cmd(self, ctx: commands.Context) -> None:
        """Buttons + dropdown for cell, coins, and common games."""
        em = discord.Embed(
            title="Six — play hub",
            description=(
                "**Dropdown** — pick an action (most cell/coin tools run **ephemerally**).\n"
                "**6shop** (coin role shop with banner) — choose *Coins — 6shop* and run the command shown."
            ),
            color=discord.Color.dark_magenta(),
        )
        view = PlayHubView(ctx.bot, ctx.author.id)
        view.message = await ctx.send(embed=em, view=view)
