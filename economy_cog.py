"""
Economy commands: balance, bank, games with buttons. Prefix is bot PREFIX (6).
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import traceback
import re
import secrets
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote_plus, urlparse

import aiohttp
import discord
from discord.ext import commands
try:
    from supabase import Client as SupabaseClient, create_client as create_supabase_client
except ImportError:
    SupabaseClient = Any  # type: ignore[assignment]
    create_supabase_client = None  # type: ignore[assignment]

# Always same file next to this module (not cwd) — survives restarts
ECONOMY_FILE = (Path(__file__).resolve().parent / "economy_data.json").resolve()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = (
    os.getenv("SUPABASE_KEY", "").strip()
    or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
)
SHOP_BANNER_PATH = Path(__file__).resolve().parent / "assets" / "6xs_shop_banner.png"
SHOP_BANNER_FILENAME = "6xs_shop_banner.png"
POLY_BETS_FILE = (Path(__file__).resolve().parent / "polymarket_bets.json").resolve()
GAMMA_API = "https://gamma-api.polymarket.com"
POLY_HTTP_HEADERS = {"User-Agent": "SixBot/1.0 (Discord economy; +https://discord.com)"}
POLY_MIN_BET = 25
POLY_POLL_IDLE_SECONDS = 180  # no pending bets — light API use
POLY_POLL_ACTIVE_SECONDS = 20  # while someone has an open Polymarket bet
POLY_PRICE_SETTLED_HI = 0.995  # Gamma often delays closed=true on 5m crypto markets
POLY_PRICE_SETTLED_LO = 0.03
POLY_MAX_PENDING_PER_USER = 8
POLY_HOT_TTL_SECONDS = 600
POLY_PICK_VIEW_TIMEOUT = 180
# Payout on win ≈ stake / entry_price (Polymarket-style: shares worth $1 if correct); clamp tiny prices
POLY_ENTRY_PRICE_FLOOR = 1e-5

# --- flair ---
E = {
    "coin": "🪙",
    "cash": "💵",
    "bank": "🏦",
    "chart": "📈",
    "dice": "🎲",
    "slot": "🎰",
    "box": "📦",
    "gift": "🎁",
    "rob": "🦹",
    "work": "💼",
    "beg": "🥺",
    "crime": "👮",
    "bj": "🃏",
    "ladder": "🪜",
    "crash": "🚀",
    "trophy": "🏆",
    "arrow": "➡️",
    "x": "❌",
    "check": "✅",
    "shop": "🛒",
    "mine": "💣",
    "gem": "💎",
    "poly": "📈",
    "cs2": "🔫",
    "case": "🧰",
}


@dataclass(frozen=True)
class ShopRole:
    """If role_id is 0, the bot finds the role by exact Discord name (match_name)."""

    name: str
    role_id: int
    price: int
    match_name: Optional[str] = None


def resolve_shop_role(guild: discord.Guild, sr: ShopRole) -> Optional[discord.Role]:
    if sr.role_id:
        r = guild.get_role(sr.role_id)
        if r is not None:
            return r
    if sr.match_name:
        return discord.utils.get(guild.roles, name=sr.match_name)
    return None


# Shop order + prices (embed + buttons top → bottom; highest tier first)
SHOP_ROLES: tuple[ShopRole, ...] = (
    ShopRole("GOD", 0, 1_000_000, "GOD"),
    ShopRole("Unbelievably the Richest", 0, 925_000, "Unbelievably the Richest"),
    ShopRole("Owner of All Comgirls", 0, 810_000, "Owner of All Comgirls"),
    ShopRole("The #1 Male Manipulator", 0, 777_250, "The #1 Male Manipulator"),
    ShopRole("The Savior of All Comgirls", 0, 543_495, "The Savior of All Comgirls"),
    ShopRole("Dark Prince of Death", 0, 265_000, "Dark Prince of Death"),
    ShopRole("Mass Murderer", 0, 95_000, "Mass Murderer"),
    ShopRole("Rich", 1487665016499867748, 28_900),
    ShopRole("Sinister", 1487665180908191775, 19_700),
    ShopRole("Dark Triad", 1487665364723568660, 13_200),
    ShopRole("Nihilist", 1487665889309491321, 8_900),
    ShopRole("Niche", 1487665252962140281, 6_300),
)

CS2_PLACEHOLDER_BASE = "https://placehold.co/512x320"


def _cs2_img(bg: str, fg: str, text: str) -> str:
    return f"{CS2_PLACEHOLDER_BASE}/{bg}/{fg}/png?text={quote_plus(text[:42])}"


_CS2_STEAM_URLS_FILE = Path(__file__).resolve().parent / "cs2_steam_urls.json"


def _load_cs2_steam_urls() -> dict[str, str]:
    try:
        raw = json.loads(_CS2_STEAM_URLS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, str) and v.strip():
            out[k] = v.strip()
    return out


_CS2_STEAM_IMAGES = _load_cs2_steam_urls()


def _cs2_item_image(iid: str, bg: str, fg: str, text: str) -> str:
    u = _CS2_STEAM_IMAGES.get(iid)
    if u:
        return u
    return _cs2_img(bg, fg, text)


@dataclass(frozen=True)
class CS2ItemDef:
    id: str
    name: str
    rarity: str
    tier: int  # 0=Consumer … 6=Extraordinary — used for pity (boost tier >= 3)
    sell: int
    image: str


@dataclass(frozen=True)
class CS2CrateDef:
    id: str
    name: str
    price: int
    pool: tuple[tuple[str, float], ...]


# --- CS2-style cosmetics (Steam economy art from cs2_steam_urls.json; fallback placehold.co) ---
CS2_ITEMS: dict[str, CS2ItemDef] = {
    "c_sand": CS2ItemDef("c_sand", "P250 | Sand Dune", "Consumer Grade", 0, 8, _cs2_item_image("c_sand", "2b3545", "94a3b8", "P250 Sand Dune")),
    "c_urban": CS2ItemDef("c_urban", "UMP-45 | Urban DDPAT", "Consumer Grade", 0, 9, _cs2_item_image("c_urban", "2b3545", "94a3b8", "UMP Urban")),
    "c_safari": CS2ItemDef("c_safari", "Nova | Safari Mesh", "Consumer Grade", 0, 7, _cs2_item_image("c_safari", "2b3545", "94a3b8", "Nova Safari")),
    "c_ground": CS2ItemDef("c_ground", "PP-Bizon | Sand Dashed", "Consumer Grade", 0, 8, _cs2_item_image("c_ground", "2b3545", "94a3b8", "Bizon Sand")),
    "i_cortex": CS2ItemDef("i_cortex", "USP-S | Cortex", "Industrial Grade", 1, 35, _cs2_item_image("i_cortex", "334155", "7dd3fc", "USP Cortex")),
    "i_capil": CS2ItemDef("i_capil", "Five-SeveN | Capillary", "Industrial Grade", 1, 32, _cs2_item_image("i_capil", "334155", "7dd3fc", "FiveSeven Capillary")),
    "i_offworld": CS2ItemDef("i_offworld", "Glock-18 | Off World", "Industrial Grade", 1, 38, _cs2_item_image("i_offworld", "334155", "7dd3fc", "Glock Off World")),
    "i_oxide": CS2ItemDef("i_oxide", "MAC-10 | Oceanic", "Industrial Grade", 1, 34, _cs2_item_image("i_oxide", "334155", "7dd3fc", "MAC10 Oceanic")),
    "m_phantom": CS2ItemDef("m_phantom", "M4A4 | Magnesium", "Mil-Spec", 2, 120, _cs2_item_image("m_phantom", "1e3a5f", "60a5fa", "M4A4 Magnesium")),
    "m_guerrilla": CS2ItemDef("m_guerrilla", "AK-47 | Uncharted", "Mil-Spec", 2, 145, _cs2_item_image("m_guerrilla", "1e3a5f", "60a5fa", "AK Uncharted")),
    "m_trigger": CS2ItemDef("m_trigger", "Desert Eagle | Directive", "Mil-Spec", 2, 110, _cs2_item_image("m_trigger", "1e3a5f", "60a5fa", "Deagle Directive")),
    "m_ghost": CS2ItemDef("m_ghost", "AWP | Acheron", "Mil-Spec", 2, 155, _cs2_item_image("m_ghost", "1e3a5f", "60a5fa", "AWP Acheron")),
    "r_mecha": CS2ItemDef("r_mecha", "FAMAS | Mecha Industries", "Restricted", 3, 420, _cs2_item_image("r_mecha", "3b0764", "c084fc", "FAMAS Mecha")),
    "r_monkey": CS2ItemDef("r_monkey", "M4A1-S | Monkey Business", "Restricted", 3, 480, _cs2_item_image("r_monkey", "3b0764", "c084fc", "M4A1 Monkey")),
    "r_hyper": CS2ItemDef("r_hyper", "Galil AR | Chromatic Aberration", "Restricted", 3, 450, _cs2_item_image("r_hyper", "3b0764", "c084fc", "Galil Chromatic")),
    "r_print": CS2ItemDef("r_print", "P90 | Printstream", "Restricted", 3, 510, _cs2_item_image("r_print", "3b0764", "c084fc", "P90 Printstream")),
    "cl_neon": CS2ItemDef("cl_neon", "MP7 | Neon Ply", "Classified", 4, 980, _cs2_item_image("cl_neon", "4c0519", "fb7185", "MP7 Neon")),
    "cl_empress": CS2ItemDef("cl_empress", "AK-47 | The Empress", "Classified", 4, 1450, _cs2_item_image("cl_empress", "4c0519", "fb7185", "AK Empress")),
    "cl_kumicho": CS2ItemDef("cl_kumicho", "Desert Eagle | Kumicho Dragon", "Classified", 4, 1320, _cs2_item_image("cl_kumicho", "4c0519", "fb7185", "Deagle Kumicho")),
    "cv_lightning": CS2ItemDef("cv_lightning", "AWP | Lightning Strike", "Covert", 5, 3200, _cs2_item_image("cv_lightning", "450a0a", "fca5a5", "AWP Lightning")),
    "cv_fade": CS2ItemDef("cv_fade", "Glock-18 | Fade", "Covert", 5, 2800, _cs2_item_image("cv_fade", "450a0a", "fca5a5", "Glock Fade")),
    "cv_fire": CS2ItemDef("cv_fire", "M4A4 | Howl", "Covert", 5, 6500, _cs2_item_image("cv_fire", "450a0a", "fca5a5", "M4 Howl")),
    "ex_dragon": CS2ItemDef("ex_dragon", "AWP | Dragon Lore", "Contraband", 6, 18000, _cs2_item_image("ex_dragon", "713f12", "fde047", "AWP Dragon Lore")),
    "ex_sapphire": CS2ItemDef("ex_sapphire", "M9 Bayonet | Sapphire", "★ Covert Knife", 6, 22000, _cs2_item_image("ex_sapphire", "713f12", "fde047", "M9 Sapphire")),
    "ex_ruby": CS2ItemDef("ex_ruby", "Karambit | Ruby", "★ Covert Knife", 6, 25000, _cs2_item_image("ex_ruby", "713f12", "fde047", "Karambit Ruby")),
}

CS2_CRATES: dict[str, CS2CrateDef] = {
    "recruit": CS2CrateDef(
        "recruit",
        "Recruit Case",
        150,
        (
            ("c_sand", 95.0),
            ("c_urban", 95.0),
            ("c_safari", 95.0),
            ("c_ground", 95.0),
            ("i_cortex", 55.0),
            ("i_capil", 55.0),
            ("i_offworld", 50.0),
            ("i_oxide", 50.0),
            ("m_phantom", 18.0),
            ("m_guerrilla", 16.0),
            ("m_trigger", 18.0),
            ("m_ghost", 12.0),
            ("r_mecha", 2.2),
            ("r_monkey", 1.8),
            ("cl_neon", 0.35),
            ("cv_lightning", 0.04),
            ("ex_dragon", 0.003),
        ),
    ),
    "veteran": CS2CrateDef(
        "veteran",
        "Veteran Case",
        500,
        (
            ("i_cortex", 70.0),
            ("i_capil", 70.0),
            ("i_offworld", 65.0),
            ("i_oxide", 65.0),
            ("m_phantom", 45.0),
            ("m_guerrilla", 42.0),
            ("m_trigger", 45.0),
            ("m_ghost", 38.0),
            ("r_mecha", 22.0),
            ("r_hyper", 20.0),
            ("r_print", 18.0),
            ("cl_neon", 9.0),
            ("cl_empress", 5.5),
            ("cl_kumicho", 5.0),
            ("cv_lightning", 1.2),
            ("cv_fade", 1.0),
            ("cv_fire", 0.25),
            ("ex_dragon", 0.02),
            ("ex_sapphire", 0.012),
        ),
    ),
    "elite": CS2CrateDef(
        "elite",
        "Elite Case",
        1400,
        (
            ("m_phantom", 35.0),
            ("m_guerrilla", 32.0),
            ("m_trigger", 35.0),
            ("m_ghost", 28.0),
            ("r_mecha", 40.0),
            ("r_monkey", 36.0),
            ("r_hyper", 38.0),
            ("r_print", 34.0),
            ("cl_neon", 28.0),
            ("cl_empress", 22.0),
            ("cl_kumicho", 20.0),
            ("cv_lightning", 8.0),
            ("cv_fade", 7.0),
            ("cv_fire", 2.5),
            ("ex_dragon", 0.18),
            ("ex_sapphire", 0.10),
            ("ex_ruby", 0.06),
        ),
    ),
}

CS2_PITY_TIER_MIN = 3  # Restricted or better resets pity; below stacks luck
CS2_PITY_STEP = 0.055
CS2_PITY_CAP = 28
CS2_KEEP_SELL_TIMEOUT = 120
CS2_TRADE_OFFER_TTL = 300

START_WALLET = 500
COOLDOWN_DAILY = 86400
COOLDOWN_WORK = 2700
COOLDOWN_BEG = 300
COOLDOWN_CRIME = 2400
COOLDOWN_ROB = 7200
COOLDOWN_OPEN = 10800


def _key(gid: int, uid: int) -> str:
    return f"{gid}:{uid}"


def parse_amount(arg: Optional[str], maximum: int) -> Optional[int]:
    if arg is None:
        return None
    s = str(arg).strip().lower().replace(",", "")
    if s in ("all", "max"):
        return maximum
    try:
        v = int(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _fmt(n: int) -> str:
    return f"{n:,}"


def _cs2_normalize_inv(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for x in raw:
        if isinstance(x, dict) and x.get("i") and x.get("d"):
            did = str(x["d"])
            if did in CS2_ITEMS:
                out.append({"i": str(x["i"]), "d": did})
    return out


def _ensure_cs2_fields(d: dict[str, Any]) -> None:
    inv = d.get("cs2_inv")
    if not isinstance(inv, list):
        d["cs2_inv"] = []
    d["cs2_inv"] = _cs2_normalize_inv(d["cs2_inv"])
    d["cs2_pity"] = max(0, int(d.get("cs2_pity", 0) or 0))


def _cs2_sorted_rows(d: dict[str, Any]) -> list[tuple[dict[str, str], CS2ItemDef]]:
    _ensure_cs2_fields(d)
    rows: list[tuple[dict[str, str], CS2ItemDef]] = []
    for entry in d["cs2_inv"]:
        it = CS2_ITEMS.get(entry["d"])
        if it:
            rows.append((entry, it))
    rows.sort(key=lambda r: (-r[1].tier, -r[1].sell, r[1].name.lower()))
    return rows


def _resolve_crate_id(arg: str) -> Optional[str]:
    s = arg.strip().lower()
    if s in ("recruit", "rec", "r"):
        return "recruit"
    if s in ("veteran", "vet", "v"):
        return "veteran"
    if s in ("elite", "e", "pro"):
        return "elite"
    return s if s in CS2_CRATES else None


def _roll_cs2(crate_id: str, pity: int) -> tuple[str, int]:
    crate = CS2_CRATES[crate_id]
    mult = 1.0 + min(pity, CS2_PITY_CAP) * CS2_PITY_STEP
    choices: list[str] = []
    weights: list[float] = []
    for iid, w in crate.pool:
        it = CS2_ITEMS[iid]
        ww = w * (mult if it.tier >= CS2_PITY_TIER_MIN else 1.0)
        choices.append(iid)
        weights.append(ww)
    got = random.choices(choices, weights=weights, k=1)[0]
    new_pity = 0 if CS2_ITEMS[got].tier >= CS2_PITY_TIER_MIN else pity + 1
    return got, new_pity


def _deck() -> list[str]:
    ranks = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
    suits = ["♠", "♥", "♦", "♣"]
    return [f"{r}{s}" for r in ranks for s in suits]


def _card_value(rank: str) -> list[int]:
    if rank == "A":
        return [1, 11]
    if rank in ("J", "Q", "K") or rank == "10":
        return [10]
    return [int(rank)]


def hand_total(cards: list[str]) -> int:
    """Best blackjack total <= 21 if possible."""
    vals = [c[:-1] for c in cards]
    totals = {0}
    for r in vals:
        nv = set()
        for t in totals:
            for v in _card_value(r):
                nv.add(t + v)
        totals = nv
    valid = [t for t in totals if t <= 21]
    if valid:
        return max(valid)
    return min(totals)


def hand_total_display(cards: list[str]) -> str:
    """Human-readable totals; soft hands like A+7 → `8/18`."""
    vals = [c[:-1] for c in cards]
    totals = {0}
    for r in vals:
        nv = set()
        for t in totals:
            for v in _card_value(r):
                nv.add(t + v)
        totals = nv
    valid = sorted({t for t in totals if t <= 21})
    if len(valid) >= 2:
        return f"{valid[0]}/{valid[-1]}"
    if len(valid) == 1:
        return str(valid[0])
    return str(min(totals))


def format_hand(cards: list[str], hide_second: bool = False) -> str:
    if hide_second and len(cards) > 1:
        return f"`{cards[0]}` + `??`"
    return " ".join(f"`{c}`" for c in cards)


def format_hand_with_total(cards: list[str], *, hide_dealer_hole: bool = False) -> str:
    line = format_hand(cards, hide_second=hide_dealer_hole)
    if hide_dealer_hole and len(cards) > 1:
        up = hand_total_display([cards[0]])
        tot = f"{up} / ?"
    else:
        tot = hand_total_display(cards)
    return f"{line}\n**total:** `{tot}`"


def _poly_parse_json_field(val: Any) -> list[Any]:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return []
    return []


def extract_polymarket_slugs(url: str) -> tuple[Optional[str], Optional[str]]:
    """From a polymarket.com link: (event_slug, market_slug_or_None)."""
    try:
        p = urlparse(url.strip())
    except Exception:
        return None, None
    host = (p.netloc or "").lower()
    if "polymarket.com" not in host:
        return None, None
    parts = [x for x in p.path.split("/") if x]
    event_slug, market_slug = None, None
    if "event" in parts:
        i = parts.index("event")
        if i + 1 < len(parts):
            event_slug = parts[i + 1]
        if i + 2 < len(parts):
            market_slug = parts[i + 2]
    if "market" in parts:
        i = parts.index("market")
        if i + 1 < len(parts):
            market_slug = parts[i + 1]
    return event_slug, market_slug


def market_outcomes_list(m: dict[str, Any]) -> list[str]:
    raw = _poly_parse_json_field(m.get("outcomes"))
    return [str(x) for x in raw]


def poly_outcome_prices_floats(m: dict[str, Any]) -> Optional[list[float]]:
    outs = market_outcomes_list(m)
    prices = _poly_parse_json_field(m.get("outcomePrices"))
    if len(outs) != len(prices) or not outs:
        return None
    try:
        return [float(prices[i]) for i in range(len(outs))]
    except (TypeError, ValueError):
        return None


def poly_price_for_named_outcome(m: dict[str, Any], name: str) -> Optional[float]:
    """Implied probability / price for one outcome (same ordering as Gamma `outcomePrices`)."""
    outs = market_outcomes_list(m)
    pf = poly_outcome_prices_floats(m)
    if pf is None:
        return None
    want = (name or "").strip().lower()
    for i, o in enumerate(outs):
        if (o or "").strip().lower() == want:
            p = float(pf[i])
            if p <= 0 or p > 1.0:
                return None
            return min(max(p, POLY_ENTRY_PRICE_FLOOR), 1.0)
    return None


def poly_prices_for_outcomes_list(m: dict[str, Any]) -> list[Optional[float]]:
    outs = market_outcomes_list(m)
    pf = poly_outcome_prices_floats(m)
    if pf is None:
        return [None] * len(outs)
    out: list[Optional[float]] = []
    for i in range(len(outs)):
        try:
            p = float(pf[i])
            if p <= 0 or p > 1.0:
                out.append(None)
            else:
                out.append(min(max(p, POLY_ENTRY_PRICE_FLOOR), 1.0))
        except (TypeError, ValueError):
            out.append(None)
    return out


def poly_win_payout_coins(stake: int, entry_price: float) -> int:
    """Total coins returned if correct: stake / price (like $1 payout per share on Polymarket)."""
    p = float(entry_price)
    if p <= 0 or p > 1.0:
        return max(1, int(stake * 2))
    p = max(p, POLY_ENTRY_PRICE_FLOOR)
    return max(1, int(round(stake / p)))


def poly_settlement_winner(m: dict[str, Any]) -> Optional[str]:
    """Winner outcome when Polymarket has effectively settled, or None if still open.

    Gamma often keeps `closed=false` for a while after short windows (e.g. BTC 5m) even
    when outcome prices are already ~1/0 — we treat decisive binary prices as settled.
    """
    outs = market_outcomes_list(m)
    prices = _poly_parse_json_field(m.get("outcomePrices"))
    if len(outs) != len(prices) or not outs:
        return None
    try:
        pf = [float(prices[i]) for i in range(len(outs))]
    except (TypeError, ValueError):
        return None
    n = len(pf)
    hi, lo = max(pf), min(pf)

    if m.get("closed"):
        if hi < 0.0001 and lo < 0.0001:
            return None
        wi = max(range(n), key=lambda j: pf[j])
        return outs[wi]

    if n == 2 and hi >= POLY_PRICE_SETTLED_HI and lo <= POLY_PRICE_SETTLED_LO:
        return outs[0] if pf[0] >= pf[1] else outs[1]

    return None


def open_markets_from_event(ev: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in ev.get("markets") or []:
        if not m or m.get("closed"):
            continue
        if poly_settlement_winner(m) is not None:
            continue
        out.append(m)
    return out


def expand_polybet_glued_urls(args: tuple[str, ...]) -> tuple[list[str], bool]:
    """Split `Uphttps://...` into `https://...`; returns (tokens, glued_detected)."""
    glued = False
    out: list[str] = []
    for a in args:
        al = a.lower()
        pos = -1
        for needle in ("https://", "http://"):
            j = al.find(needle)
            if j >= 0 and (pos < 0 or j < pos):
                pos = j
        if pos > 0:
            glued = True
            out.append(a[pos:])
        else:
            out.append(a)
    return out, glued


_POLY_FRAGMENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def normalize_polybet_url_fragments(tokens: list[str]) -> list[str]:
    """
    If a Polymarket URL was split at `#` into `.../path` + `eventId`, glue back into one token.
    Discord / parsers sometimes break `...#gEDW3Ty4` into two argv pieces.
    """
    if len(tokens) < 2:
        return tokens
    out: list[str] = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        low = t.lower()
        if (
            "polymarket.com" in low
            and "#" not in t
            and i + 1 < len(tokens)
            and _POLY_FRAGMENT_RE.fullmatch(tokens[i + 1])
        ):
            out.append(t + "#" + tokens[i + 1])
            i += 2
            continue
        out.append(t)
        i += 1
    return out


def _polybet_token_is_http_url(s: str) -> bool:
    sl = (s or "").strip().lower()
    return sl.startswith("http://") or sl.startswith("https://")


def _poly_ui_label(s: str, max_len: int) -> str:
    s = (s or "?").strip().replace("\n", " ")
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


class EconomyCog(commands.Cog):
    """💰 Server economy + mini-games."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._lock = asyncio.Lock()
        self._data: dict[str, dict[str, Any]] = {}
        self._last_mtime_ns: int = 0
        self._supabase: Optional[SupabaseClient] = None
        self._dirty: bool = False
        self._crash_users: set[int] = set()
        self._bj_users: set[int] = set()
        self._ladder_users: set[int] = set()
        self._mines_users: set[int] = set()
        self._poly_pending: list[dict[str, Any]] = []
        self._poly_lock = asyncio.Lock()
        self._poly_task: Optional[asyncio.Task] = None
        self._poly_pick_users: set[int] = set()
        self._poly_hot_cache: dict[str, tuple[float, list[dict[str, str]]]] = {}
        self._cs2_open_users: set[int] = set()
        self._cs2_roll_pending: dict[int, dict[str, Any]] = {}
        self._trade_offers: dict[tuple[int, int, int], dict[str, Any]] = {}
        self._load()
        self._load_poly_bets()

    def _resolve_supabase_client(self) -> Optional[SupabaseClient]:
        """Create client lazily from current os.environ (fixes late env on some hosts)."""
        if self._supabase is not None:
            return self._supabase
        if create_supabase_client is None:
            return None
        url = os.getenv("SUPABASE_URL", "").strip() or SUPABASE_URL
        key = (
            os.getenv("SUPABASE_KEY", "").strip()
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
            or SUPABASE_KEY
        )
        if not url or not key:
            return None
        try:
            self._supabase = create_supabase_client(url, key)
            print("[economy] Supabase client connected (wallets + website sync).")
            return self._supabase
        except Exception as e:
            print(f"[economy] Supabase connection failed: {e}")
            return None

    def _wallet_row_payload_from_key(self, key: str) -> Optional[dict[str, Any]]:
        row = self._data.get(key)
        if not row or not isinstance(row, dict):
            return None
        try:
            gid_s, uid_s = key.split(":", 1)
            gid = int(gid_s)
            uid = int(uid_s)
        except (ValueError, TypeError):
            return None
        return {
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
            "cs2_inv": _cs2_normalize_inv(row.get("cs2_inv")),
            "cs2_pity": max(0, int(row.get("cs2_pity", 0) or 0)),
        }

    def _upsert_wallet_keys_to_supabase(self, keys: list[str]) -> bool:
        sb = self._resolve_supabase_client()
        if sb is None:
            if keys:
                print(
                    "[economy] supabase upsert skipped: no client "
                    "(set SUPABASE_URL + SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY on this host)"
                )
            return False
        if not keys:
            return False
        payload: list[dict[str, Any]] = []
        for key in keys:
            p = self._wallet_row_payload_from_key(key)
            if p:
                payload.append(p)
        if not payload:
            return False
        try:
            sb.table("economy_wallets").upsert(payload, on_conflict="guild_id,user_id").execute()
            print(f"[economy] supabase upsert {len(payload)} wallet row(s)")
            return True
        except Exception:
            print(f"[economy] supabase upsert failed ({len(payload)} row(s)):")
            traceback.print_exc()
            return False

    def _load_from_supabase(self) -> bool:
        sb = self._resolve_supabase_client()
        if sb is None:
            return False
        try:
            rows: list[dict[str, Any]] = []
            page = 0
            page_size = 1000
            while True:
                resp = (
                    sb.table("economy_wallets")
                    .select("guild_id,user_id,wallet,bank,last_daily,last_work,last_beg,last_crime,last_rob,last_open,cs2_inv,cs2_pity")
                    .range(page * page_size, page * page_size + page_size - 1)
                    .execute()
                )
                chunk = resp.data or []
                if not chunk:
                    break
                rows.extend(chunk)
                if len(chunk) < page_size:
                    break
                page += 1
            if not rows:
                return False
            out: dict[str, dict[str, Any]] = {}
            for row in rows:
                gid = int(row.get("guild_id") or 0)
                uid = int(row.get("user_id") or 0)
                if gid <= 0 or uid <= 0:
                    continue
                key = _key(gid, uid)
                out[key] = {
                    "wallet": int(row.get("wallet", START_WALLET)),
                    "bank": int(row.get("bank", 0)),
                    "last_daily": float(row.get("last_daily", 0.0)),
                    "last_work": float(row.get("last_work", 0.0)),
                    "last_beg": float(row.get("last_beg", 0.0)),
                    "last_crime": float(row.get("last_crime", 0.0)),
                    "last_rob": float(row.get("last_rob", 0.0)),
                    "last_open": float(row.get("last_open", 0.0)),
                    "cs2_inv": _cs2_normalize_inv(row.get("cs2_inv")),
                    "cs2_pity": max(0, int(row.get("cs2_pity", 0) or 0)),
                }
            if not out:
                return False
            self._data = out
            print(f"[economy] loaded {len(self._data)} users from supabase economy_wallets")
            return True
        except Exception as e:
            print(f"[economy] supabase load failed: {e}")
            return False

    @staticmethod
    def _poly_hot_key(guild_id: int, user_id: int) -> str:
        return f"{guild_id}:{user_id}"

    def _load(self) -> None:
        if self._load_from_supabase():
            return
        path = ECONOMY_FILE
        try:
            if not path.exists():
                print(f"[economy] no save yet — will create {path}")
                self._data = {}
                self._last_mtime_ns = 0
                return
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                print(f"[economy] {path} is not a JSON object — backing up and resetting")
                path.rename(path.with_suffix(".json.bak"))
                self._data = {}
                return
            self._data = {}
            for key, row in raw.items():
                if not isinstance(row, dict):
                    continue
                self._data[str(key)] = {
                    "wallet": int(row.get("wallet", START_WALLET)),
                    "bank": int(row.get("bank", 0)),
                    "last_daily": float(row.get("last_daily", 0.0)),
                    "last_work": float(row.get("last_work", 0.0)),
                    "last_beg": float(row.get("last_beg", 0.0)),
                    "last_crime": float(row.get("last_crime", 0.0)),
                    "last_rob": float(row.get("last_rob", 0.0)),
                    "last_open": float(row.get("last_open", 0.0)),
                    "cs2_inv": _cs2_normalize_inv(row.get("cs2_inv")),
                    "cs2_pity": max(0, int(row.get("cs2_pity", 0) or 0)),
                }
            print(f"[economy] loaded {len(self._data)} users from {path}")
            try:
                self._last_mtime_ns = path.stat().st_mtime_ns
            except OSError:
                pass
        except Exception as e:
            print(f"[economy] load failed: {e} — starting empty (data file: {path})")
            self._data = {}

    def _reload_if_changed(self) -> None:
        path = ECONOMY_FILE
        try:
            if not path.exists():
                return
            mtime = path.stat().st_mtime_ns
            if mtime > self._last_mtime_ns:
                self._load()
        except OSError:
            return

    def _load_poly_bets(self) -> None:
        try:
            if not POLY_BETS_FILE.exists():
                self._poly_pending = []
                return
            raw = json.loads(POLY_BETS_FILE.read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                self._poly_pending = []
                return
            self._poly_pending = [b for b in raw if isinstance(b, dict)]
            fixed = any(not b.get("id") for b in self._poly_pending)
            for b in self._poly_pending:
                if not b.get("id"):
                    b["id"] = secrets.token_urlsafe(10)
            if fixed:
                try:
                    POLY_BETS_FILE.write_text(
                        json.dumps(self._poly_pending, indent=0), encoding="utf-8"
                    )
                except OSError:
                    pass
        except Exception as e:
            print(f"[economy] polymarket bets load failed: {e}")
            self._poly_pending = []

    async def _save_poly_bets(self) -> None:
        async with self._poly_lock:
            snap = list(self._poly_pending)
        path = POLY_BETS_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snap, indent=0), encoding="utf-8")
        os.replace(tmp, path)

    async def cog_load(self) -> None:
        # Seed Supabase immediately on startup so web casino can read balances
        # even before anyone runs a new economy command.
        if self._resolve_supabase_client() is not None:
            try:
                if not self._data:
                    self._load()
                if self._data:
                    await self._save()
                    print(
                        f"[economy] supabase startup sync: {len(self._data)} wallet row(s)"
                    )
                else:
                    print(
                        "[economy] supabase startup: no economy data in memory or on disk "
                        f"({ECONOMY_FILE}) — run 6bal in the server or restore economy_data.json, "
                        "then restart the bot so rows can upsert."
                    )
            except Exception as e:
                print(f"[economy] initial supabase sync failed: {e}")
        self._poly_task = asyncio.create_task(self._poly_poll_loop())

    async def cog_unload(self) -> None:
        t = self._poly_task
        self._poly_task = None
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    async def _gamma_get_json(
        self, session: aiohttp.ClientSession, path: str, params: Optional[dict[str, Any]] = None
    ) -> Any:
        url = f"{GAMMA_API}{path}"
        async with session.get(
            url, params=params, headers=POLY_HTTP_HEADERS, timeout=aiohttp.ClientTimeout(total=28)
        ) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            return await resp.json()

    async def _fetch_market_by_slug(self, session: aiohttp.ClientSession, slug: str) -> Optional[dict[str, Any]]:
        from urllib.parse import quote

        data = await self._gamma_get_json(session, f"/markets/slug/{quote(slug, safe='')}")
        return data if isinstance(data, dict) else None

    async def _fetch_event_by_slug(self, session: aiohttp.ClientSession, slug: str) -> Optional[dict[str, Any]]:
        from urllib.parse import quote

        data = await self._gamma_get_json(session, f"/events/slug/{quote(slug, safe='')}")
        return data if isinstance(data, dict) else None

    async def _poly_resolve_market(
        self, session: aiohttp.ClientSession, url: str, market_index: int
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        ev_slug, m_slug = extract_polymarket_slugs(url)
        if not ev_slug and not m_slug:
            return None, "that doesn’t look like a **polymarket.com** link."
        if m_slug:
            m = await self._fetch_market_by_slug(session, m_slug)
            if m:
                return m, None
            return None, "couldn’t load that market — open the **exact** outcome page and copy the URL again."
        assert ev_slug is not None
        ev = await self._fetch_event_by_slug(session, ev_slug)
        if not ev:
            return None, "couldn’t find that event — paste the full URL from Polymarket."
        oms = open_markets_from_event(ev)
        if not oms:
            return None, "no **open** markets on that event anymore."
        if market_index < 1 or market_index > len(oms):
            return None, (
                f"this event has **{len(oms)}** open markets — run `6polyinfo <url>` "
                f"then `6polybet <amount> <market#> <same url>` (buttons pick your side)."
            )
        return oms[market_index - 1], None

    async def _poly_poll_loop(self) -> None:
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                await self._poly_poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[economy] polymarket poll: {e}")
            async with self._poly_lock:
                n_pending = len(self._poly_pending)
            await asyncio.sleep(
                POLY_POLL_ACTIVE_SECONDS if n_pending else POLY_POLL_IDLE_SECONDS
            )

    async def _poly_poll_once(self) -> None:
        async with self._poly_lock:
            if not self._poly_pending:
                return
            batch = list(self._poly_pending)
        async with aiohttp.ClientSession(headers=POLY_HTTP_HEADERS) as session:
            for bet in batch:
                slug = bet.get("market_slug")
                if not slug:
                    continue
                m = await self._fetch_market_by_slug(session, slug)
                if not m:
                    continue
                if poly_settlement_winner(m) is None:
                    pick = str(bet.get("outcome_pick") or "")
                    ep_live = poly_price_for_named_outcome(m, pick)
                    need_save = False
                    async with self._poly_lock:
                        if bet.get("entry_price") is None and ep_live is not None:
                            bet["entry_price"] = ep_live
                            need_save = True
                    if need_save:
                        await self._save_poly_bets()
                    continue
                await self._poly_settle_one(bet, m)

    async def _poly_settle_one(self, bet: dict[str, Any], m: dict[str, Any]) -> None:
        bid = bet.get("id")
        winner = poly_settlement_winner(m)
        gid = int(bet["guild_id"])
        cid = int(bet["channel_id"])
        uid = int(bet["user_id"])
        amt = int(bet["amount"])
        pick = str(bet.get("outcome_pick") or "")
        mention = f"<@{uid}>"

        async with self._poly_lock:
            n_before = len(self._poly_pending)
            self._poly_pending = [b for b in self._poly_pending if b.get("id") != bid]
            if len(self._poly_pending) == n_before:
                return
        await self._save_poly_bets()

        channel = self.bot.get_channel(cid)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(cid)
            except (discord.HTTPException, discord.NotFound):
                channel = None
        q = (m.get("question") or bet.get("question") or "")[:200]
        if winner is None:
            d = self._get(gid, uid)
            d["wallet"] = int(d["wallet"]) + amt
            await self._save()
            if channel:
                try:
                    await channel.send(
                        f"{E['poly']} {mention} polymarket **settlement unclear** for your bet — "
                        f"**{_fmt(amt)}** {E['coin']} refunded.\n*{q}*"
                    )
                except discord.HTTPException:
                    pass
            return

        won = pick.lower() == winner.lower()
        if won:
            raw_ep = bet.get("entry_price")
            if raw_ep is not None:
                try:
                    ep = float(raw_ep)
                    pay = poly_win_payout_coins(amt, ep)
                    mult = pay / amt if amt else 0.0
                    pay_note = f"**~{mult:.2f}×** return (locked **{ep * 100:.1f}¢** implied — matches site-style payout)"
                except (TypeError, ValueError):
                    pay = int(amt * 2)
                    pay_note = "**2×** (legacy bet — no saved price)"
            else:
                pay = int(amt * 2)
                pay_note = "**2×** (legacy bet — no saved price)"
            d = self._get(gid, uid)
            d["wallet"] = int(d["wallet"]) + pay
            await self._save()
            if channel:
                try:
                    await channel.send(
                        f"{E['check']} **congratulations** {mention} — your Polymarket pick **{pick}** won "
                        f"on a **{_fmt(amt)}** {E['coin']} stake! paid **{_fmt(pay)}** {E['coin']} ({pay_note}).\n"
                        f"**resolved:** `{winner}`\n*{q}*"
                    )
                except discord.HTTPException:
                    pass
        else:
            if channel:
                try:
                    await channel.send(
                        f"{E['x']} {mention} Polymarket bet settled — **{pick}** lost "
                        f"(market resolved **`{winner}`**). stake was **{_fmt(amt)}** {E['coin']}.\n*{q}*"
                    )
                except discord.HTTPException:
                    pass

    async def _save(self) -> None:
        async with self._lock:
            path = ECONOMY_FILE
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._data, indent=0), encoding="utf-8")
            os.replace(tmp, path)
            try:
                self._last_mtime_ns = path.stat().st_mtime_ns
            except OSError:
                pass
            sb = self._resolve_supabase_client()
            if sb is not None:
                payload: list[dict[str, Any]] = []
                for key, row in self._data.items():
                    try:
                        gid_s, uid_s = key.split(":", 1)
                        gid = int(gid_s)
                        uid = int(uid_s)
                    except (ValueError, TypeError):
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
                            "cs2_inv": _cs2_normalize_inv(row.get("cs2_inv")),
                            "cs2_pity": max(0, int(row.get("cs2_pity", 0) or 0)),
                        }
                    )
                if payload:
                    try:
                        sb.table("economy_wallets").upsert(payload, on_conflict="guild_id,user_id").execute()
                        print(f"[economy] supabase full save ok ({len(payload)} row(s))")
                    except Exception:
                        print(f"[economy] supabase save failed ({len(payload)} rows):")
                        traceback.print_exc()
        self._dirty = False

    async def _flush_dirty(self) -> None:
        if self._dirty:
            await self._save()

    def _get(self, guild_id: int, user_id: int) -> dict[str, Any]:
        self._reload_if_changed()
        k = _key(guild_id, user_id)
        if k not in self._data:
            self._data[k] = {
                "wallet": START_WALLET,
                "bank": 0,
                "last_daily": 0.0,
                "last_work": 0.0,
                "last_beg": 0.0,
                "last_crime": 0.0,
                "last_rob": 0.0,
                "last_open": 0.0,
                "cs2_inv": [],
                "cs2_pity": 0,
            }
            self._dirty = True
        _ensure_cs2_fields(self._data[k])
        return self._data[k]

    def _total(self, d: dict[str, Any]) -> int:
        return int(d["wallet"]) + int(d["bank"])

    async def _add_wallet(self, guild_id: int, user_id: int, delta: int) -> None:
        u = self._get(guild_id, user_id)
        u["wallet"] = max(0, int(u["wallet"]) + delta)
        await self._save()

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.guild is None:
            try:
                await ctx.send(f"{E['x']} economy commands only work in a **server** channel.")
            except discord.HTTPException:
                pass
            return False
        self._get(ctx.guild.id, ctx.author.id)
        if self._dirty:
            await self._flush_dirty()
        else:
            # Existing users: nothing was dirty so full _save never ran; still push this row
            # so Supabase stays in sync with the local JSON (fixes website balance at 0).
            k = _key(ctx.guild.id, ctx.author.id)
            if not self._upsert_wallet_keys_to_supabase([k]):
                print(f"[economy] warning: single-row supabase sync failed for {k}")
        return True

    async def cog_unload(self) -> None:
        try:
            await self._save()
            print(f"[economy] flushed → {ECONOMY_FILE}")
        except Exception as e:
            print(f"[economy] shutdown save failed: {e}")

    # ---------- commands ----------

    @commands.command(name="balance", aliases=["bal", "money"])
    async def balance(self, ctx: commands.Context, member: Optional[discord.Member] = None):
        """💵 Wallet + bank."""
        m = member or ctx.author
        d = self._get(ctx.guild.id, m.id)
        w, b = int(d["wallet"]), int(d["bank"])
        em = discord.Embed(
            title=f"{E['coin']} balance — {m.display_name}",
            color=discord.Color.gold(),
        )
        em.add_field(name=f"{E['cash']} wallet", value=f"**{_fmt(w)}** coins", inline=True)
        em.add_field(name=f"{E['bank']} bank", value=f"**{_fmt(b)}** coins", inline=True)
        em.add_field(name=f"{E['chart']} net", value=f"**{_fmt(w + b)}**", inline=True)
        await ctx.send(embed=em)
        await self._flush_dirty()

    @commands.command(name="pushwallet")
    async def push_wallet(self, ctx: commands.Context):
        """Force-sync **your** wallet row to Supabase (fixes website / casino balance)."""
        self._get(ctx.guild.id, ctx.author.id)
        k = _key(ctx.guild.id, ctx.author.id)
        if self._upsert_wallet_keys_to_supabase([k]):
            await ctx.send(
                f"{E['check']} pushed wallet to Supabase (`{k}`). Refresh the site in a few seconds.",
                delete_after=22,
            )
        else:
            await ctx.send(
                f"{E['x']} Supabase write failed. On Railway set **SUPABASE_URL** + "
                f"**SUPABASE_SERVICE_ROLE_KEY**, redeploy, then check logs for `[economy]`.",
                delete_after=28,
            )

    @commands.command(name="syncallwallets")
    @commands.is_owner()
    async def sync_all_wallets(self, ctx: commands.Context):
        """Bot owner: full JSON save + upsert **all** in-memory wallets to Supabase."""
        await self._save()
        await ctx.send(
            f"{E['check']} full save done (**{len(self._data)}** keys). Check Supabase row count.",
            delete_after=20,
        )

    @commands.command(name="moneyset")
    @commands.has_permissions(administrator=True)
    async def moneyset(self, ctx: commands.Context, user_id: int, amount: int):
        """Admin: `6moneyset <user_id> <amount>` — sets **wallet** (bank unchanged)."""
        if amount < 0:
            return await ctx.send(f"{E['x']} amount must be **≥ 0**.", delete_after=8)
        d = self._get(ctx.guild.id, user_id)
        d["wallet"] = amount
        await self._save()
        mem = ctx.guild.get_member(user_id)
        label = mem.mention if mem else f"`{user_id}`"
        await ctx.send(
            f"{E['check']} {label} **wallet** → **{_fmt(amount)}** {E['coin']} "
            f"(bank still **{_fmt(int(d['bank']))}**)"
        )

    @commands.command(name="deposit", aliases=["dep"])
    async def deposit(self, ctx: commands.Context, amount: Optional[str] = None):
        """🏦 wallet → bank."""
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt > w:
            return await ctx.send(f"{E['x']} usage: `6deposit <amount|all>` — not enough in wallet.", delete_after=8)
        d["wallet"] = w - amt
        d["bank"] = int(d["bank"]) + amt
        await self._save()
        em = discord.Embed(
            description=f"{E['check']} deposited **{_fmt(amt)}** {E['coin']} → bank",
            color=discord.Color.green(),
        )
        await ctx.send(embed=em)

    @commands.command(name="withdraw", aliases=["with"])
    async def withdraw(self, ctx: commands.Context, amount: Optional[str] = None):
        """🏦 bank → wallet."""
        d = self._get(ctx.guild.id, ctx.author.id)
        b = int(d["bank"])
        amt = parse_amount(amount, b)
        if amt is None or amt > b:
            return await ctx.send(f"{E['x']} usage: `6withdraw <amount|all>` — not enough in bank.", delete_after=8)
        d["bank"] = b - amt
        d["wallet"] = int(d["wallet"]) + amt
        await self._save()
        em = discord.Embed(
            description=f"{E['check']} withdrew **{_fmt(amt)}** {E['coin']} → wallet",
            color=discord.Color.green(),
        )
        await ctx.send(embed=em)

    @commands.command(name="daily")
    async def daily(self, ctx: commands.Context):
        """🎁 once / 24h."""
        d = self._get(ctx.guild.id, ctx.author.id)
        now = time.time()
        left = COOLDOWN_DAILY - (now - float(d["last_daily"]))
        if left > 0:
            h, m = int(left // 3600), int((left % 3600) // 60)
            return await ctx.send(f"{E['x']} daily in **{h}h {m}m**.", delete_after=8)
        gain = random.randint(400, 1200)
        d["last_daily"] = now
        d["wallet"] = int(d["wallet"]) + gain
        await self._save()
        em = discord.Embed(
            title=f"{E['gift']} daily reward",
            description=f"+**{_fmt(gain)}** {E['coin']}",
            color=discord.Color.gold(),
        )
        await ctx.send(embed=em)

    @commands.command(name="work")
    async def work(self, ctx: commands.Context):
        """💼 honest wages."""
        d = self._get(ctx.guild.id, ctx.author.id)
        now = time.time()
        left = COOLDOWN_WORK - (now - float(d["last_work"]))
        if left > 0:
            m, s = int(left // 60), int(left % 60)
            return await ctx.send(f"{E['x']} work again in **{m}m {s}s**.", delete_after=8)
        gain = random.randint(80, 260)
        d["last_work"] = now
        d["wallet"] = int(d["wallet"]) + gain
        await self._save()
        jobs = ["shift", "gig", "contract", "overtime"]
        em = discord.Embed(
            description=f"{E['work']} finished a **{random.choice(jobs)}** — **+{_fmt(gain)}** {E['coin']}",
            color=discord.Color.blue(),
        )
        await ctx.send(embed=em)

    @commands.command(name="beg")
    async def beg(self, ctx: commands.Context):
        """🥺 spare change."""
        d = self._get(ctx.guild.id, ctx.author.id)
        now = time.time()
        left = COOLDOWN_BEG - (now - float(d["last_beg"]))
        if left > 0:
            return await ctx.send(f"{E['x']} beg again in **{int(left)}s**.", delete_after=6)
        gain = random.randint(5, 55)
        d["last_beg"] = now
        d["wallet"] = int(d["wallet"]) + gain
        await self._save()
        await ctx.send(f"{E['beg']} someone tossed you **{_fmt(gain)}** {E['coin']}")

    @commands.command(name="crime")
    async def crime(self, ctx: commands.Context):
        """👮 risky score."""
        d = self._get(ctx.guild.id, ctx.author.id)
        now = time.time()
        left = COOLDOWN_CRIME - (now - float(d["last_crime"]))
        if left > 0:
            m = int(left // 60)
            return await ctx.send(f"{E['x']} crime cooldown **{m}m** left.", delete_after=8)
        d["last_crime"] = now
        if random.random() < 0.48:
            gain = random.randint(180, 650)
            d["wallet"] = int(d["wallet"]) + gain
            await self._save()
            em = discord.Embed(
                description=f"{E['crime']} got away — **+{_fmt(gain)}** {E['coin']}",
                color=discord.Color.dark_green(),
            )
        else:
            fine = random.randint(80, 320)
            d["wallet"] = max(0, int(d["wallet"]) - fine)
            await self._save()
            em = discord.Embed(
                description=f"{E['crime']} caught — **-{_fmt(fine)}** {E['coin']}",
                color=discord.Color.red(),
            )
        await ctx.send(embed=em)

    @commands.command(name="open")
    async def open_box(self, ctx: commands.Context):
        """📦 loot box (cooldown)."""
        d = self._get(ctx.guild.id, ctx.author.id)
        now = time.time()
        left = COOLDOWN_OPEN - (now - float(d["last_open"]))
        if left > 0:
            h = int(left // 3600)
            m = int((left % 3600) // 60)
            return await ctx.send(f"{E['x']} next box in **{h}h {m}m**.", delete_after=8)
        d["last_open"] = now
        roll = random.random()
        if roll < 0.05:
            gain, tier = random.randint(800, 2500), "MYTHIC"
            color = discord.Color.purple()
        elif roll < 0.2:
            gain, tier = random.randint(300, 800), "rare"
            color = discord.Color.blue()
        else:
            gain, tier = random.randint(40, 280), "common"
            color = discord.Color.light_grey()
        d["wallet"] = int(d["wallet"]) + gain
        await self._save()
        em = discord.Embed(
            title=f"{E['box']} crate opened — **{tier}**",
            description=f"+**{_fmt(gain)}** {E['coin']}",
            color=color,
        )
        await ctx.send(embed=em)

    @commands.command(name="transfer", aliases=["pay", "give"])
    async def transfer(self, ctx: commands.Context, member: discord.Member, amount: Optional[str] = None):
        """➡️ send coins."""
        if member.bot:
            return
        if member.id == ctx.author.id:
            return await ctx.send(f"{E['x']} pick someone else.", delete_after=6)
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt > w:
            return await ctx.send(f"{E['x']} `6transfer @user <amount|all>`", delete_after=8)
        d["wallet"] = w - amt
        t = self._get(ctx.guild.id, member.id)
        t["wallet"] = int(t["wallet"]) + amt
        await self._save()
        em = discord.Embed(
            description=f"{E['arrow']} sent **{_fmt(amt)}** {E['coin']} → **{member.display_name}**",
            color=discord.Color.teal(),
        )
        await ctx.send(embed=em)

    @commands.command(name="rob")
    async def rob(self, ctx: commands.Context, member: discord.Member):
        """🦹 steal (high risk)."""
        if member.bot:
            return
        if member.id == ctx.author.id:
            return await ctx.send(f"{E['x']} can't rob yourself.", delete_after=6)
        now = time.time()
        ra = self._get(ctx.guild.id, ctx.author.id)
        left = COOLDOWN_ROB - (now - float(ra["last_rob"]))
        if left > 0:
            m = int(left // 60)
            return await ctx.send(f"{E['x']} rob cooldown **{m}m**.", delete_after=8)
        vic = self._get(ctx.guild.id, member.id)
        vw = int(vic["wallet"])
        if vw < 120:
            return await ctx.send(f"{E['x']} they're broke (wallet < 120).", delete_after=8)
        ra["last_rob"] = now
        if random.random() < 0.22:
            cut = int(vw * random.uniform(0.08, 0.22))
            vic["wallet"] = vw - cut
            ra["wallet"] = int(ra["wallet"]) + cut
            await self._save()
            em = discord.Embed(
                description=f"{E['rob']} yoinked **{_fmt(cut)}** {E['coin']} from **{member.display_name}**",
                color=discord.Color.dark_red(),
            )
        else:
            fine = random.randint(60, 200)
            ra["wallet"] = max(0, int(ra["wallet"]) - fine)
            await self._save()
            em = discord.Embed(
                description=f"{E['rob']} caught — fined **{_fmt(fine)}** {E['coin']}",
                color=discord.Color.red(),
            )
        await ctx.send(embed=em)

    @commands.command(name="wealthy", aliases=["rich", "leaderboard", "lb"])
    async def wealthy(self, ctx: commands.Context):
        """🏆 top balances."""
        gid = ctx.guild.id
        rows: list[tuple[int, int]] = []
        for k, v in self._data.items():
            if not k.startswith(f"{gid}:"):
                continue
            uid = int(k.split(":")[1])
            tot = int(v["wallet"]) + int(v["bank"])
            rows.append((uid, tot))
        rows.sort(key=lambda x: x[1], reverse=True)
        rows = rows[:12]
        lines = []
        for i, (uid, tot) in enumerate(rows, 1):
            mem = ctx.guild.get_member(uid)
            name = mem.display_name if mem else f"<@{uid}>"
            lines.append(f"**{i}.** {name} — **{_fmt(tot)}** {E['coin']}")
        em = discord.Embed(
            title=f"{E['trophy']} richest in {ctx.guild.name}",
            description="\n".join(lines) or "no data yet",
            color=discord.Color.gold(),
        )
        await ctx.send(embed=em)

    @commands.command(name="gamble")
    async def gamble(self, ctx: commands.Context, amount: Optional[str] = None):
        """🎰 slots."""
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt < 10 or amt > w:
            return await ctx.send(f"{E['x']} `6gamble <amount|all>` (min 10)", delete_after=8)
        reels = ["🍒", "🍋", "⭐", "💎", "7️⃣"]
        spin = [random.choice(reels) for _ in range(3)]
        d["wallet"] = w - amt
        mult = 0
        if spin[0] == spin[1] == spin[2]:
            if spin[0] == "7️⃣":
                mult = 8
            elif spin[0] == "💎":
                mult = 6
            else:
                mult = 4
        elif spin[0] == spin[1] or spin[1] == spin[2] or spin[0] == spin[2]:
            mult = 2
        win = int(amt * mult)
        d["wallet"] += win
        await self._save()
        line = " | ".join(spin)
        net = win - amt
        sign = "+" if net >= 0 else ""
        em = discord.Embed(
            title=f"{E['slot']} slots",
            description=f"**{line}**\n\n{sign}**{_fmt(net)}** {E['coin']}",
            color=discord.Color.green() if net >= 0 else discord.Color.red(),
        )
        await ctx.send(embed=em)

    @commands.command(name="coinflip", aliases=["cf"])
    async def coinflip(self, ctx: commands.Context, amount: Optional[str] = None):
        """🪙 heads / tails buttons."""
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt < 10 or amt > w:
            return await ctx.send(f"{E['x']} `6coinflip <amount|all>` (min 10)", delete_after=8)

        d["wallet"] = w - amt
        await self._save()

        view = CoinflipView(self, ctx.guild.id, ctx.author.id, amt, ctx.channel.id)
        em = discord.Embed(
            title=f"{E['coin']} coinflip — **{_fmt(amt)}** {E['coin']}",
            description="pick a side — 2x on win",
            color=discord.Color.orange(),
        )
        view.message = await ctx.send(embed=em, view=view)

    @commands.command(name="crash")
    async def crash_cmd(self, ctx: commands.Context, amount: Optional[str] = None):
        """🚀 multiplier — cash out before boom."""
        if ctx.author.id in self._crash_users:
            return await ctx.send(f"{E['x']} finish your current crash first.", delete_after=6)
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt < 20 or amt > w:
            return await ctx.send(f"{E['x']} `6crash <amount|all>` (min 20)", delete_after=8)
        d["wallet"] = w - amt
        await self._save()
        crash_at = random.uniform(1.15, min(8.5, 1.15 + random.random() * 7))
        self._crash_users.add(ctx.author.id)
        view = CrashView(self, ctx.guild.id, ctx.author.id, amt, crash_at)
        em = discord.Embed(
            title=f"{E['crash']} crash",
            description=f"bet **{_fmt(amt)}** {E['coin']}\n**mult:** `1.00x`\n\n{cash_out_hint()}",
            color=discord.Color.dark_blue(),
        )
        view.message = await ctx.send(embed=em, view=view)
        view.start_tick()

    @commands.command(name="ladder", aliases=["climb"])
    async def ladder_cmd(self, ctx: commands.Context, amount: Optional[str] = None):
        """🪜 climb for bigger pot or fall."""
        if ctx.author.id in self._ladder_users:
            return await ctx.send(f"{E['x']} finish your ladder first.", delete_after=6)
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt < 25 or amt > w:
            return await ctx.send(f"{E['x']} `6ladder <amount|all>` (min 25)", delete_after=8)
        d["wallet"] = w - amt
        await self._save()
        self._ladder_users.add(ctx.author.id)
        view = LadderView(self, ctx.guild.id, ctx.author.id, amt)
        em = discord.Embed(
            title=f"{E['ladder']} ladder",
            description=f"stake **{_fmt(amt)}** {E['coin']}\n**pot:** `{_fmt(amt)}`\n\n"
            f"**climb** ~68% → pot ×1.45\n**bank** keep pot",
            color=discord.Color.dark_teal(),
        )
        view.message = await ctx.send(embed=em, view=view)

    @commands.command(name="mines")
    async def mines_cmd(self, ctx: commands.Context, amount: Optional[str] = None):
        """💣 4×4 grid — tap safe tiles to raise mult; hit a mine and lose."""
        if ctx.author.id in self._mines_users:
            return await ctx.send(f"{E['x']} finish your current mines game first.", delete_after=6)
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt < 25 or amt > w:
            return await ctx.send(f"{E['x']} `6mines <amount|all>` (min 25)", delete_after=8)
        d["wallet"] = w - amt
        await self._save()
        self._mines_users.add(ctx.author.id)
        view = MinesView(self, ctx.guild.id, ctx.author.id, amt)
        em = view.build_embed()
        view.message = await ctx.send(embed=em, view=view)

    @commands.command(name="blackjack", aliases=["bj"])
    async def blackjack_cmd(self, ctx: commands.Context, amount: Optional[str] = None):
        """🃏 hit / stand."""
        if ctx.author.id in self._bj_users:
            return await ctx.send(f"{E['x']} finish your hand first.", delete_after=6)
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amount, w)
        if amt is None or amt < 20 or amt > w:
            return await ctx.send(f"{E['x']} `6blackjack <amount|all>` (min 20)", delete_after=8)

        d["wallet"] = w - amt
        await self._save()

        deck = _deck()
        random.shuffle(deck)
        ph = [deck.pop(), deck.pop()]
        dh = [deck.pop(), deck.pop()]
        ptot, dtot = hand_total(ph), hand_total(dh)

        if dtot == 21:
            if ptot == 21:
                d["wallet"] = int(d["wallet"]) + amt
                await self._save()
                em = discord.Embed(
                    title=f"{E['bj']} push",
                    description=f"both 21 — bet returned **{_fmt(amt)}** {E['coin']}\n\n"
                    f"**you**\n{format_hand_with_total(ph)}\n\n**dealer**\n{format_hand_with_total(dh)}",
                    color=discord.Color.light_grey(),
                )
                return await ctx.send(embed=em)
            em = discord.Embed(
                title=f"{E['bj']} dealer blackjack",
                description=f"{E['x']} lost **{_fmt(amt)}** {E['coin']}\n\n"
                f"**you**\n{format_hand_with_total(ph)}\n\n**dealer**\n{format_hand_with_total(dh)}",
                color=discord.Color.red(),
            )
            return await ctx.send(embed=em)

        if ptot == 21:
            pay = int(amt * 2.5)
            d["wallet"] = int(d["wallet"]) + pay
            await self._save()
            em = discord.Embed(
                title=f"{E['bj']} blackjack",
                description=f"{E['check']} natural 21 — **{_fmt(pay)}** {E['coin']} (2.5×)\n\n"
                f"**dealer shows**\n{format_hand_with_total(dh, hide_dealer_hole=True)}",
                color=discord.Color.gold(),
            )
            return await ctx.send(embed=em)

        self._bj_users.add(ctx.author.id)
        view = BlackjackView(self, ctx.guild.id, ctx.author.id, amt, deck, ph, dh)
        em = view._embed()
        view.message = await ctx.send(embed=em, view=view)

    @commands.command(name="polytrending", aliases=["polyhot"])
    async def polytrending_cmd(self, ctx: commands.Context):
        """📈 top Polymarket events by 24h volume — bet with a number (no URL copy)."""
        async with aiohttp.ClientSession(headers=POLY_HTTP_HEADERS) as session:
            try:
                evs = await self._gamma_get_json(
                    session,
                    "/events",
                    {
                        "active": "true",
                        "closed": "false",
                        "order": "volume24hr",
                        "ascending": "false",
                        "limit": "8",
                    },
                )
            except Exception:
                return await ctx.send(f"{E['x']} couldn’t reach Polymarket right now — try again later.", delete_after=8)
        if not isinstance(evs, list) or not evs:
            return await ctx.send(f"{E['x']} no trending data returned.", delete_after=8)
        items: list[dict[str, str]] = []
        lines = []
        for i, e in enumerate(evs[:8], start=1):
            slug = e.get("slug") or ""
            title = (e.get("title") or slug or "?")[:120]
            vol = e.get("volume24hr") or e.get("volume") or ""
            url = f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com"
            items.append({"title": title, "url": url, "slug": slug})
            lines.append(f"`{i}.` **{title}**\n└ 24h vol: `{vol}`")
        key = self._poly_hot_key(ctx.guild.id, ctx.author.id)
        self._poly_hot_cache[key] = (time.time() + POLY_HOT_TTL_SECONDS, items)
        em = discord.Embed(
            title=f"{E['poly']} Polymarket trending",
            description="\n\n".join(lines),
            color=discord.Color.blue(),
        )
        em.set_footer(
            text=f"your picks expire in {POLY_HOT_TTL_SECONDS // 60} min — "
            f"`6polybet <amount> <#>` · win pays ~stake÷implied price (shown on buttons)"
        )
        await ctx.send(embed=em)

    @commands.command(name="polyinfo", aliases=["polymarket"])
    async def polyinfo_cmd(self, ctx: commands.Context, *, url: str):
        """📈 list open markets on an event (for multi-outcome pages)."""
        url = url.strip()
        ev_slug, m_slug = extract_polymarket_slugs(url)
        async with aiohttp.ClientSession(headers=POLY_HTTP_HEADERS) as session:
            if m_slug:
                m = await self._fetch_market_by_slug(session, m_slug)
                if not m:
                    return await ctx.send(f"{E['x']} couldn’t load that market link.", delete_after=8)
            elif ev_slug:
                ev = await self._fetch_event_by_slug(session, ev_slug)
                if not ev:
                    return await ctx.send(f"{E['x']} couldn’t find that event.", delete_after=8)
                oms = open_markets_from_event(ev)
                if not oms:
                    return await ctx.send(f"{E['x']} no **open** markets on that event.", delete_after=8)
                if len(oms) > 1:
                    parts = []
                    for i, mk in enumerate(oms, start=1):
                        q = (mk.get("question") or "?")[:100]
                        parts.append(f"`{i}.` {q}")
                    em = discord.Embed(
                        title=f"{E['poly']} open markets ({len(oms)})",
                        description="\n".join(parts)
                        + "\n\n`6polybet <amount> <#> <same url>` — buttons pick the side",
                        color=discord.Color.blurple(),
                    )
                    return await ctx.send(embed=em)
                m = oms[0]
            else:
                return await ctx.send(
                    f"{E['x']} paste a **polymarket.com** link (`/event/...` or `/event/.../market-slug`).",
                    delete_after=10,
                )
        outs = market_outcomes_list(m)
        em = discord.Embed(
            title=f"{E['poly']} market",
            description=(
                f"{m.get('question', '?')}\n\n"
                f"**outcomes:** {', '.join(f'`{o}`' for o in outs)}\n"
                f"**status:** "
                f"{'settled' if poly_settlement_winner(m) is not None else ('closed' if m.get('closed') else 'open')}\n\n"
                f"`6polybet <amount> <this url>` — then use the **buttons** to pick a side."
            ),
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=em)

    @commands.command(name="polybet")
    async def polybet_cmd(self, ctx: commands.Context, *args: str):
        """📈 stake on Polymarket — choose outcome with buttons (or from 6polyhot #)."""
        if not args:
            return await ctx.send(
                f"{E['x']} `6polybet <amount> <# from 6polyhot>` or `6polybet <amount> [market#] <url>`\n"
                f"— then tap **Up / Down** (or **Yes / No**, etc.) on the buttons.",
                delete_after=14,
            )

        args, glued = expand_polybet_glued_urls(args)
        args = normalize_polybet_url_fragments(list(args))
        if len(args) < 2:
            return await ctx.send(
                f"{E['x']} `6polybet <amount> <#>` from **`6polyhot`**, or `6polybet <amount> <url>`",
                delete_after=12,
            )

        amt_s = args[0]
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        amt = parse_amount(amt_s, w)
        if amt is None or amt < POLY_MIN_BET or amt > w:
            return await ctx.send(
                f"{E['x']} need **{_fmt(POLY_MIN_BET)}+** coins and ≤ wallet (`6polybet ...`).",
                delete_after=8,
            )

        if ctx.author.id in self._poly_pick_users:
            return await ctx.send(
                f"{E['x']} finish or cancel your open **pick** message first (buttons).",
                delete_after=6,
            )

        async with self._poly_lock:
            pending_u = sum(1 for b in self._poly_pending if int(b.get("user_id", 0)) == ctx.author.id)
        if pending_u >= POLY_MAX_PENDING_PER_USER:
            return await ctx.send(
                f"{E['x']} you already have **{POLY_MAX_PENDING_PER_USER}** open Polymarket bets.",
                delete_after=8,
            )

        url: Optional[str] = None
        m_idx = 1
        rest = list(args[1:])

        # `6polybet <amt> <market#> <url>` — URL may contain `#eventId` (must not be mistaken for 6polyhot #)
        if len(rest) >= 2 and rest[0].isdigit() and _polybet_token_is_http_url(rest[1]):
            m_idx = int(rest[0])
            url = rest[1].strip()
            rest = rest[2:]
            if rest:
                return await ctx.send(
                    f"{E['x']} extra text after the URL — use spaces only: "
                    f"`6polybet <amount> <market#> <full url>`.",
                    delete_after=12,
                )
        elif rest and rest[0].isdigit():
            n = int(rest[0])
            key = self._poly_hot_key(ctx.guild.id, ctx.author.id)
            ent = self._poly_hot_cache.get(key)
            if not ent or ent[0] < time.time():
                return await ctx.send(
                    f"{E['x']} run **`6polyhot`** first — your numbered list expires in "
                    f"**{POLY_HOT_TTL_SECONDS // 60} min**.",
                    delete_after=10,
                )
            items = ent[1]
            if n < 1 or n > len(items):
                return await ctx.send(
                    f"{E['x']} pick **1–{len(items)}** from your last **`6polyhot`**.",
                    delete_after=8,
                )
            url = items[n - 1]["url"]
            rest = rest[1:]
            if rest and rest[0].isdigit():
                m_idx = int(rest[0])
                rest = rest[1:]
            if rest:
                return await ctx.send(
                    f"{E['x']} after the hot **`#`**, only an optional **market#** is allowed "
                    f"(see `6polyinfo` if the event has many markets).",
                    delete_after=12,
                )
        elif rest and _polybet_token_is_http_url(rest[0]):
            url = rest[0].strip()
            rest = rest[1:]
            if rest:
                return await ctx.send(f"{E['x']} extra text after the URL — check spacing.", delete_after=8)
        else:
            return await ctx.send(
                f"{E['x']} use **`6polybet <amount> <#>`** from `6polyhot`, "
                f"**`6polybet <amount> <url>`**, or **`6polybet <amount> <market#> <url>`** "
                f"(full Polymarket link, including `#…` if present).",
                delete_after=14,
            )

        assert url is not None
        async with aiohttp.ClientSession(headers=POLY_HTTP_HEADERS) as session:
            m, err = await self._poly_resolve_market(session, url, m_idx)
            if err:
                return await ctx.send(f"{E['x']} {err}", delete_after=14)
        assert m is not None
        if poly_settlement_winner(m) is not None:
            return await ctx.send(
                f"{E['x']} that market is **already resolved** (or prices already final) — pick an open one.",
                delete_after=8,
            )

        outs = market_outcomes_list(m)
        if not outs:
            return await ctx.send(f"{E['x']} that market has no outcomes in the API.", delete_after=8)
        if len(outs) > 25:
            return await ctx.send(
                f"{E['x']} too many outcomes (**{len(outs)}**) — open a **single** market page and paste that URL.",
                delete_after=10,
            )

        slug = m.get("slug")
        if not slug:
            return await ctx.send(f"{E['x']} that market has no slug — try another link.", delete_after=8)

        if glued:
            try:
                await ctx.message.delete()
            except discord.HTTPException:
                pass

        self._poly_pick_users.add(ctx.author.id)
        view = PolyBetPickView(self, ctx.guild.id, ctx.author.id, ctx.channel.id, amt, m)
        em = discord.Embed(
            title=f"{E['poly']} pick your side",
            description=(
                f"{ctx.author.mention} **{_fmt(amt)}** {E['coin']} — tap a button to **lock** the bet.\n\n"
                f"*{_poly_ui_label(m.get('question') or '?', 400)}*"
            ),
            color=discord.Color.dark_teal(),
        )
        em.set_footer(
            text="¢ = implied odds from Polymarket; win pays ~stake÷price. Up/Down: yes≈Up, no≈Down on Yes/No markets."
        )
        try:
            view.message = await ctx.send(embed=em, view=view)
        except discord.HTTPException:
            self._poly_pick_users.discard(ctx.author.id)
            raise

    @commands.command(name="polycheck", aliases=["polyrefresh"])
    async def polycheck_cmd(self, ctx: commands.Context):
        """Poll Polymarket + show your open bets (stake, pick, price, payout if you win)."""
        await self._poly_poll_once()

        assert ctx.guild is not None
        uid, gid = ctx.author.id, ctx.guild.id
        async with self._poly_lock:
            n_all = len(self._poly_pending)
            my_bets = [
                b
                for b in self._poly_pending
                if int(b.get("user_id", 0)) == uid and int(b.get("guild_id", 0)) == gid
            ]

        if not my_bets:
            if n_all == 0:
                return await ctx.send(f"{E['x']} no pending Polymarket bets.", delete_after=8)
            return await ctx.send(
                f"{E['poly']} polled **{n_all}** open bet(s) — you have **none** in this server.",
                delete_after=14,
            )

        lines: list[str] = []
        lines.append(f"You have **{len(my_bets)}** open here (polled just now).\n")
        show_cap = 10
        slice_bets = my_bets[:show_cap]

        async with aiohttp.ClientSession(headers=POLY_HTTP_HEADERS) as session:
            for i, bet in enumerate(slice_bets, start=1):
                amt = int(bet.get("amount", 0))
                pick = str(bet.get("outcome_pick") or "?")
                q = _poly_ui_label(str(bet.get("question") or "?"), 220)

                ep_lock: Optional[float] = None
                raw_ep = bet.get("entry_price")
                if raw_ep is not None:
                    try:
                        ep_lock = float(raw_ep)
                        if ep_lock <= 0 or ep_lock > 1.0:
                            ep_lock = None
                    except (TypeError, ValueError):
                        ep_lock = None

                ep_live: Optional[float] = None
                slug = bet.get("market_slug")
                if slug:
                    m_fresh = await self._fetch_market_by_slug(session, str(slug))
                    if m_fresh:
                        ep_live = poly_price_for_named_outcome(m_fresh, pick)

                if ep_lock is None and ep_live is not None:
                    need_save = False
                    async with self._poly_lock:
                        if bet.get("entry_price") is None:
                            bet["entry_price"] = ep_live
                            need_save = True
                    if need_save:
                        await self._save_poly_bets()
                    raw_ep = bet.get("entry_price")
                    ep_lock = None
                    if raw_ep is not None:
                        try:
                            ep_lock = float(raw_ep)
                            if ep_lock <= 0 or ep_lock > 1.0:
                                ep_lock = None
                        except (TypeError, ValueError):
                            ep_lock = None

                if ep_lock is not None:
                    pay = poly_win_payout_coins(amt, ep_lock)
                    prof = pay - amt
                    mult = pay / amt if amt else 0.0
                    price_line = f"**{ep_lock * 100:.1f}¢** locked · **~{mult:.2f}×** if correct"
                    win_line = f"**{_fmt(pay)}** {E['coin']} total back · **+{_fmt(prof)}** {E['coin']} profit"
                    if ep_live is not None and abs(ep_live - ep_lock) > 0.02:
                        price_line += f"\n└ *market now **{ep_live * 100:.1f}¢** (moved since lock)*"
                else:
                    pay_settle = int(amt * 2)
                    prof_settle = pay_settle - amt
                    price_line = "**—** *(no saved lock + couldn’t read price — settle **2×**)*"
                    win_line = (
                        f"**{_fmt(pay_settle)}** {E['coin']} back · **+{_fmt(prof_settle)}** {E['coin']} *(2×)*"
                    )

                lines.append(
                    f"**{i}.** {q}\n"
                    f"└ **Cost / stake:** {_fmt(amt)} {E['coin']}\n"
                    f"└ **Your pick:** `{pick}`\n"
                    f"└ **Price / odds:** {price_line}\n"
                    f"└ **If it cashes (you win):** {win_line}\n"
                )

        if len(my_bets) > show_cap:
            lines.append(f"\n_…and **{len(my_bets) - show_cap}** more open here — trim list after some settle._")

        at_risk = sum(int(b.get("amount", 0)) for b in my_bets)
        lines.append(f"\n**Total staked (all your open here):** {_fmt(at_risk)} {E['coin']}")

        desc = "\n".join(lines)
        if len(desc) > 4090:
            desc = desc[:4087] + "…"

        em = discord.Embed(
            title=f"{E['poly']} Polymarket — your open bets",
            description=desc,
            color=discord.Color.blue(),
        )
        em.set_footer(text="Resolved markets credit automatically in the bet channel.")
        await ctx.send(embed=em)

    def _trade_purge_expired(self) -> None:
        t = time.time()
        for k in list(self._trade_offers.keys()):
            if t - float(self._trade_offers[k].get("ts", 0)) > CS2_TRADE_OFFER_TTL:
                del self._trade_offers[k]

    @commands.command(name="crate", aliases=["crates", "cases"])
    async def crate_cmd(self, ctx: commands.Context):
        """🧰 CS2-style cases — prices and rarities."""
        lines = []
        for cid in ("recruit", "veteran", "elite"):
            c = CS2_CRATES[cid]
            lines.append(f"**{c.name}** (`{cid}`) — **{_fmt(c.price)}** {E['coin']}")
        em = discord.Embed(
            title=f"{E['case']} CS2 cases",
            description="\n".join(lines)
            + "\n\n**`6unbox recruit`** · **`6unbox veteran`** · **`6unbox elite`**\n"
            "dry streaks **raise odds** for Restricted+ until you hit one.",
            color=discord.Color.dark_blue(),
        )
        await ctx.send(embed=em)

    @commands.command(name="unbox", aliases=["opencase", "caseopen", "cs2open"])
    async def unbox_cmd(self, ctx: commands.Context, crate_key: Optional[str] = None):
        """🧰 open a paid CS2 case — then **Keep** or **Sell** (`6open` is the free cooldown box)."""
        if not crate_key:
            return await ctx.send(
                f"{E['x']} **`6unbox recruit`** · **`6unbox veteran`** · **`6unbox elite`** — see **`6crate`**",
                delete_after=10,
            )
        cid = _resolve_crate_id(crate_key)
        if cid is None:
            return await ctx.send(f"{E['x']} unknown case — **`6crate`** for names.", delete_after=8)
        if ctx.author.id in self._cs2_open_users:
            return await ctx.send(f"{E['x']} finish the **Keep / Sell** on your last unbox first.", delete_after=6)
        crate = CS2_CRATES[cid]
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        if w < crate.price:
            return await ctx.send(
                f"{E['x']} need **{_fmt(crate.price)}** {E['coin']} for **{crate.name}**.", delete_after=8
            )
        d["wallet"] = w - crate.price
        await self._save()
        pity = int(d["cs2_pity"])
        got_id, next_pity = _roll_cs2(cid, pity)
        it = CS2_ITEMS[got_id]
        self._cs2_open_users.add(ctx.author.id)
        self._cs2_roll_pending[ctx.author.id] = {"gid": ctx.guild.id, "def_id": got_id, "next_pity": next_pity}
        view = CS2KeepSellView(self, ctx.author.id, it)
        col = discord.Color.gold() if it.tier >= 5 else discord.Color.dark_teal()
        if it.tier >= 6:
            col = discord.Color.from_rgb(255, 215, 0)
        em = discord.Embed(
            title=f"{E['case']} you unboxed…",
            description=(
                f"**{it.name}**\n**{it.rarity}**\n"
                f"instant sell: **{_fmt(it.sell)}** {E['coin']}\n\n"
                f"**Keep** → `6inv` · **Sell** → coins now"
            ),
            color=col,
        )
        em.set_image(url=it.image)
        try:
            view.message = await ctx.send(embed=em, view=view)
        except discord.HTTPException:
            self._cs2_open_users.discard(ctx.author.id)
            self._cs2_roll_pending.pop(ctx.author.id, None)
            d["wallet"] = int(d["wallet"]) + crate.price
            await self._save()
            raise

    @commands.command(name="inv", aliases=["inventory", "cs2inv"])
    async def inv_cmd(self, ctx: commands.Context, page: int = 1):
        """🔫 CS2 inventory — image + value per slot."""
        d = self._get(ctx.guild.id, ctx.author.id)
        rows = _cs2_sorted_rows(d)
        if not rows:
            return await ctx.send(
                f"{E['x']} empty — open a case: **`6unbox recruit`** · **`6crate`**", delete_after=10
            )
        n = len(rows)
        if page < 1 or page > n:
            return await ctx.send(f"{E['x']} use page **1–{n}** (`6inv`)", delete_after=6)
        idx = page - 1
        entry, it = rows[idx]
        total_val = sum(r[1].sell for r in rows)
        em = discord.Embed(
            title=f"{E['cs2']} {ctx.author.display_name} — slot **{page}** / {n}",
            description=(
                f"**{it.name}**\n**{it.rarity}**\n"
                f"sell value: **{_fmt(it.sell)}** {E['coin']}\n"
                f"instance: `{entry['i'][:12]}…`\n\n"
                f"**inventory sell total:** **{_fmt(total_val)}** {E['coin']}"
            ),
            color=discord.Color.dark_grey(),
        )
        em.set_image(url=it.image)
        view = CS2InvNavView(ctx.author.id, rows, idx)
        await ctx.send(embed=em, view=view)

    @commands.command(name="trade")
    async def trade_cmd(self, ctx: commands.Context, member: discord.Member, *slots: str):
        """🔫 offer items by **`6inv`** slot # — other user runs `6tradeaccept`."""
        self._trade_purge_expired()
        if member.bot or member.id == ctx.author.id:
            return await ctx.send(f"{E['x']} pick another member.", delete_after=6)
        if not slots:
            return await ctx.send(
                f"{E['x']} **`6trade @user <inv #>`** (check **`6inv`** for slot numbers)", delete_after=10
            )
        try:
            idxs = [int(s) for s in slots]
        except ValueError:
            return await ctx.send(f"{E['x']} slots must be numbers.", delete_after=6)
        if len(set(idxs)) != len(idxs):
            return await ctx.send(f"{E['x']} don’t repeat the same slot.", delete_after=6)
        d_from = self._get(ctx.guild.id, ctx.author.id)
        rows = _cs2_sorted_rows(d_from)
        if not rows:
            return await ctx.send(f"{E['x']} nothing to trade — **`6inv`**", delete_after=8)
        names: list[str] = []
        give_iids: list[str] = []
        for i in idxs:
            if i < 1 or i > len(rows):
                return await ctx.send(f"{E['x']} you don’t have slot **{i}** — **`6inv`**", delete_after=8)
            give_iids.append(rows[i - 1][0]["i"])
            names.append(rows[i - 1][1].name)
        self._trade_offers[(ctx.guild.id, ctx.author.id, member.id)] = {"give": give_iids, "ts": time.time()}
        pretty = "\n".join(f"• {n}" for n in names)
        await ctx.send(
            f"{member.mention}\n**{ctx.author.display_name}** offers:\n{pretty}\n\n"
            f"**`6tradeaccept {ctx.author.mention} <your inv #>`** · "
            f"**`6tradedecline {ctx.author.mention}`**\n"
            f"_(offer expires in {CS2_TRADE_OFFER_TTL // 60} min)_",
            allowed_mentions=discord.AllowedMentions(users=True),
        )

    @commands.command(name="tradeaccept")
    async def tradeaccept_cmd(self, ctx: commands.Context, member: discord.Member, *slots: str):
        """🔫 accept a trade — give your slot # back."""
        self._trade_purge_expired()
        if not slots:
            return await ctx.send(
                f"{E['x']} **`6tradeaccept @user <your inv #>`** (one or more slots)", delete_after=10
            )
        key = (ctx.guild.id, member.id, ctx.author.id)
        off = self._trade_offers.get(key)
        if not off:
            return await ctx.send(
                f"{E['x']} no pending offer **from** {member.display_name} **to** you.", delete_after=10
            )
        try:
            bob_idxs = [int(s) for s in slots]
        except ValueError:
            return await ctx.send(f"{E['x']} slots must be numbers.", delete_after=6)
        d_alice = self._get(ctx.guild.id, member.id)
        d_bob = self._get(ctx.guild.id, ctx.author.id)
        rows_alice = _cs2_sorted_rows(d_alice)
        rows_bob = _cs2_sorted_rows(d_bob)
        give_a = list(off["give"])
        sa = set(give_a)
        pulled_a = [e for e in d_alice["cs2_inv"] if e["i"] in sa]
        if len(pulled_a) != len(sa):
            del self._trade_offers[key]
            return await ctx.send(
                f"{E['x']} offer is stale — sender no longer has those items.", delete_after=10
            )
        bob_iids: list[str] = []
        for i in bob_idxs:
            if i < 1 or i > len(rows_bob):
                return await ctx.send(f"{E['x']} you don’t have slot **{i}** — **`6inv`**", delete_after=8)
            bob_iids.append(rows_bob[i - 1][0]["i"])
        sb = set(bob_iids)
        if len(sb) != len(bob_iids):
            return await ctx.send(f"{E['x']} don’t duplicate slots.", delete_after=8)
        pulled_b = [e for e in d_bob["cs2_inv"] if e["i"] in sb]
        if len(pulled_b) != len(sb):
            return await ctx.send(f"{E['x']} couldn’t read your items — try again.", delete_after=8)
        d_alice["cs2_inv"] = [e for e in d_alice["cs2_inv"] if e["i"] not in sa]
        d_bob["cs2_inv"] = [e for e in d_bob["cs2_inv"] if e["i"] not in sb]
        d_alice["cs2_inv"].extend(pulled_b)
        d_bob["cs2_inv"].extend(pulled_a)
        del self._trade_offers[key]
        await self._save()
        await ctx.send(
            f"{E['check']} **{ctx.author.display_name}** ⇄ **{member.display_name}** — "
            f"swapped **{len(pulled_a)}** for **{len(pulled_b)}** item(s)."
        )

    @commands.command(name="tradedecline")
    async def tradedecline_cmd(self, ctx: commands.Context, member: discord.Member):
        """🔫 decline a pending trade."""
        self._trade_purge_expired()
        key = (ctx.guild.id, member.id, ctx.author.id)
        if self._trade_offers.pop(key, None):
            return await ctx.send(f"{E['check']} declined **{member.display_name}**’s offer.")
        await ctx.send(f"{E['x']} no active offer from them to you.", delete_after=8)

    @commands.command(name="shop", aliases=["store"])
    async def shop_cmd(self, ctx: commands.Context):
        """🛒 buy cosmetic roles with wallet coins."""
        assert ctx.guild is not None
        d = self._get(ctx.guild.id, ctx.author.id)
        w = int(d["wallet"])
        em = _shop_list_embed(ctx.guild, ctx.author.display_name, w)
        view = ShopView(self, ctx.guild.id, ctx.author.id)
        if SHOP_BANNER_PATH.is_file():
            em.set_image(url=f"attachment://{SHOP_BANNER_FILENAME}")
            file = discord.File(SHOP_BANNER_PATH, filename=SHOP_BANNER_FILENAME)
            view.message = await ctx.send(embed=em, file=file, view=view)
        else:
            view.message = await ctx.send(embed=em, view=view)

    async def play_panel_balance(self, interaction: discord.Interaction) -> None:
        """Ephemeral balance for `6play` menu."""
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None:
            await interaction.followup.send(f"{E['x']} use this in a **server**.", ephemeral=True)
            return
        uid = interaction.user.id
        d = self._get(guild.id, uid)
        w, b = int(d["wallet"]), int(d["bank"])
        em = discord.Embed(
            title=f"{E['coin']} balance — {interaction.user.display_name}",
            color=discord.Color.gold(),
        )
        em.add_field(name=f"{E['cash']} wallet", value=f"**{_fmt(w)}** coins", inline=True)
        em.add_field(name=f"{E['bank']} bank", value=f"**{_fmt(b)}** coins", inline=True)
        em.add_field(name=f"{E['chart']} net", value=f"**{_fmt(w + b)}**", inline=True)
        await interaction.followup.send(embed=em, ephemeral=True)
        await self._flush_dirty()

    async def play_panel_daily(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None:
            await interaction.followup.send(f"{E['x']} use this in a **server**.", ephemeral=True)
            return
        d = self._get(guild.id, interaction.user.id)
        now = time.time()
        left = COOLDOWN_DAILY - (now - float(d["last_daily"]))
        if left > 0:
            h, m = int(left // 3600), int((left % 3600) // 60)
            await interaction.followup.send(f"{E['x']} daily in **{h}h {m}m**.", ephemeral=True)
            return
        gain = random.randint(400, 1200)
        d["last_daily"] = now
        d["wallet"] = int(d["wallet"]) + gain
        await self._save()
        em = discord.Embed(
            title=f"{E['gift']} daily reward",
            description=f"+**{_fmt(gain)}** {E['coin']}",
            color=discord.Color.gold(),
        )
        await interaction.followup.send(embed=em, ephemeral=True)


class CS2KeepSellView(discord.ui.View):
    """After unbox — pity updates when finalized (keep or sell). Timeout = keep."""

    def __init__(self, cog: EconomyCog, uid: int, item: CS2ItemDef):
        super().__init__(timeout=CS2_KEEP_SELL_TIMEOUT)
        self.cog = cog
        self.uid = uid
        self.item = item
        self.message: Optional[discord.Message] = None
        self._done = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your unbox", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        if self._done:
            return
        await self._finalize(keep=True, interaction=None)

    async def _finalize(self, *, keep: bool, interaction: Optional[discord.Interaction]) -> None:
        if self._done:
            if interaction and not interaction.response.is_done():
                await interaction.response.send_message("already decided", ephemeral=True)
            return
        self._done = True
        pend = self.cog._cs2_roll_pending.pop(self.uid, None)
        self.cog._cs2_open_users.discard(self.uid)
        it = self.item
        if not pend:
            for c in self.children:
                c.disabled = True
            if interaction and not interaction.response.is_done():
                await interaction.response.send_message(
                    "session expired — ping staff if your balance looks wrong.", ephemeral=True
                )
            elif self.message:
                try:
                    await self.message.edit(view=self)
                except discord.HTTPException:
                    pass
            self.stop()
            return
        d = self.cog._get(int(pend["gid"]), self.uid)
        d["cs2_pity"] = int(pend["next_pity"])
        if keep:
            d["cs2_inv"].append({"i": secrets.token_urlsafe(10), "d": it.id})
            txt = f"**Kept** `{it.name}` — see **`6inv`**"
            col = discord.Color.green()
        else:
            d["wallet"] = int(d["wallet"]) + it.sell
            txt = f"**Sold** for **{_fmt(it.sell)}** {E['coin']}"
            col = discord.Color.gold()
        await self.cog._save()
        for c in self.children:
            c.disabled = True
        em = discord.Embed(title=f"{E['cs2']} unbox result", description=txt, color=col)
        em.set_thumbnail(url=it.image)
        try:
            if interaction is not None:
                await interaction.response.edit_message(embed=em, view=self)
            elif self.message:
                await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass
        self.stop()

    @discord.ui.button(label="keep", style=discord.ButtonStyle.success, row=0, emoji="📥")
    async def keep_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._finalize(keep=True, interaction=interaction)

    @discord.ui.button(label="sell", style=discord.ButtonStyle.danger, row=0, emoji="💵")
    async def sell_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._finalize(keep=False, interaction=interaction)


class CS2InvNavView(discord.ui.View):
    def __init__(self, uid: int, rows: list[tuple[dict[str, str], CS2ItemDef]], idx: int):
        super().__init__(timeout=180)
        self.uid = uid
        self.rows = rows
        self.idx = idx
        self._apply_nav_state()

    def _apply_nav_state(self) -> None:
        for c in self.children:
            if isinstance(c, discord.ui.Button) and c.label:
                lb = c.label.lower()
                if "prev" in lb:
                    c.disabled = self.idx <= 0
                elif "next" in lb:
                    c.disabled = self.idx >= len(self.rows) - 1

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your inventory", ephemeral=True)
            return False
        return True

    def _embed(self) -> discord.Embed:
        entry, it = self.rows[self.idx]
        n = len(self.rows)
        total_val = sum(r[1].sell for r in self.rows)
        em = discord.Embed(
            title=f"{E['cs2']} inventory — slot **{self.idx + 1}** / {n}",
            description=(
                f"**{it.name}**\n**{it.rarity}**\n"
                f"sell value: **{_fmt(it.sell)}** {E['coin']}\n"
                f"instance: `{entry['i'][:12]}…`\n\n"
                f"**inventory sell total:** **{_fmt(total_val)}** {E['coin']}"
            ),
            color=discord.Color.dark_grey(),
        )
        em.set_image(url=it.image)
        return em

    @discord.ui.button(label="◀ prev", style=discord.ButtonStyle.secondary, row=0)
    async def prev_b(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.idx > 0:
            self.idx -= 1
        self._apply_nav_state()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="next ▶", style=discord.ButtonStyle.secondary, row=0)
    async def next_b(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.idx < len(self.rows) - 1:
            self.idx += 1
        self._apply_nav_state()
        await interaction.response.edit_message(embed=self._embed(), view=self)


class PolyOutcomeSelect(discord.ui.Select):
    def __init__(self, outcomes: list[str], prices: Optional[list[Optional[float]]] = None):
        pl = prices if prices is not None else [None] * len(outcomes)
        opts: list[discord.SelectOption] = []
        for j, o in enumerate(outcomes):
            lab = _poly_ui_label(o, 72)
            p = pl[j] if j < len(pl) else None
            if p is not None and 0 < p < 1:
                lab = f"{lab} {p * 100:.0f}¢"[:100]
            opts.append(discord.SelectOption(label=lab, value=str(j)))
        super().__init__(
            placeholder="Choose an outcome…",
            min_values=1,
            max_values=1,
            options=opts,
            row=0,
        )
        self._outcomes = outcomes

    async def callback(self, interaction: discord.Interaction) -> None:
        v = self.view
        if isinstance(v, PolyBetPickView):
            idx = int(self.values[0])
            await v._commit(self._outcomes[idx], interaction)


class PolyOutcomeBtn(discord.ui.Button):
    def __init__(self, outcome: str, row: int, price: Optional[float] = None):
        lab = _poly_ui_label(outcome, 58)
        if price is not None and 0 < price < 1:
            lab = f"{lab} {price * 100:.0f}¢"[:80]
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=lab,
            row=row,
        )
        self.outcome = outcome

    async def callback(self, interaction: discord.Interaction) -> None:
        v = self.view
        if isinstance(v, PolyBetPickView):
            await v._commit(self.outcome, interaction)


class PolyBetPickView(discord.ui.View):
    """Outcome picker — wallet charge happens on button/select."""

    def __init__(self, cog: EconomyCog, gid: int, uid: int, cid: int, amount: int, m: dict[str, Any]):
        super().__init__(timeout=POLY_PICK_VIEW_TIMEOUT)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.cid = cid
        self.amount = amount
        self.m = m
        self.message: Optional[discord.Message] = None
        self._done = False
        outs = market_outcomes_list(m)
        prices = poly_prices_for_outcomes_list(m)
        if len(outs) <= 5:
            for i, o in enumerate(outs):
                p = prices[i] if i < len(prices) else None
                self.add_item(PolyOutcomeBtn(o, row=0, price=p))
        else:
            self.add_item(PolyOutcomeSelect(outs, prices))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your bet picker", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        self.cog._poly_pick_users.discard(self.uid)
        if self._done or not self.message:
            return
        for c in self.children:
            c.disabled = True
        try:
            em = discord.Embed(
                title=f"{E['poly']} pick expired",
                description="no coins were taken — run **`6polybet`** again.",
                color=discord.Color.light_grey(),
            )
            await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    async def _commit(self, canon: str, interaction: discord.Interaction) -> None:
        if self._done:
            await interaction.response.send_message("already locked in", ephemeral=True)
            return
        slug = self.m.get("slug")
        if not slug:
            self.cog._poly_pick_users.discard(self.uid)
            await interaction.response.send_message("market slug missing — try again.", ephemeral=True)
            return

        async with aiohttp.ClientSession(headers=POLY_HTTP_HEADERS) as session:
            fresh = await self.cog._fetch_market_by_slug(session, slug)
        if not fresh:
            self.cog._poly_pick_users.discard(self.uid)
            await interaction.response.send_message("couldn’t re-fetch market — try again.", ephemeral=True)
            return
        if poly_settlement_winner(fresh) is not None:
            self.cog._poly_pick_users.discard(self.uid)
            await interaction.response.send_message(
                "that market already resolved — nothing charged.", ephemeral=True
            )
            return

        ep = poly_price_for_named_outcome(fresh, canon)
        if ep is None:
            self.cog._poly_pick_users.discard(self.uid)
            await interaction.response.send_message(
                "Couldn’t read **implied price** for that outcome (API) — nothing charged. "
                "Try again or another market.",
                ephemeral=True,
            )
            return

        d = self.cog._get(self.gid, self.uid)
        w = int(d["wallet"])
        if w < self.amount:
            self.cog._poly_pick_users.discard(self.uid)
            await interaction.response.send_message(
                f"need **{_fmt(self.amount)}** {E['coin']} — balance changed.", ephemeral=True
            )
            return

        async with self.cog._poly_lock:
            pending_u = sum(
                1 for b in self.cog._poly_pending if int(b.get("user_id", 0)) == self.uid
            )
        if pending_u >= POLY_MAX_PENDING_PER_USER:
            self.cog._poly_pick_users.discard(self.uid)
            await interaction.response.send_message("too many open Polymarket bets.", ephemeral=True)
            return

        d["wallet"] = w - self.amount
        await self.cog._save()

        bet = {
            "id": secrets.token_urlsafe(10),
            "guild_id": self.gid,
            "channel_id": self.cid,
            "user_id": self.uid,
            "amount": self.amount,
            "outcome_pick": canon,
            "market_slug": slug,
            "question": (fresh.get("question") or "")[:500],
            "created_ts": time.time(),
            "entry_price": ep,
        }
        async with self.cog._poly_lock:
            self.cog._poly_pending.append(bet)
        await self.cog._save_poly_bets()

        self._done = True
        self.cog._poly_pick_users.discard(self.uid)
        for c in self.children:
            c.disabled = True

        est = poly_win_payout_coins(self.amount, ep)
        mult = est / self.amount if self.amount else 0.0
        em = discord.Embed(
            title=f"{E['poly']} Polymarket bet locked",
            description=(
                f"<@{self.uid}> staked **{_fmt(self.amount)}** {E['coin']} on **`{canon}`** "
                f"at **{ep * 100:.1f}¢** implied.\n"
                f"if correct: **~{_fmt(est)}** {E['coin']} back (**~{mult:.2f}×** stake) — same **stake ÷ price** idea as the site.\n"
                f"ping here when it settles (~every **{POLY_POLL_ACTIVE_SECONDS}s**).\n\n"
                f"*{_poly_ui_label(bet['question'], 220)}*"
            ),
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()


def _shop_list_embed(guild: discord.Guild, buyer_name: str, wallet: int) -> discord.Embed:
    lines = []
    missing: list[str] = []
    for r in SHOP_ROLES:
        role = resolve_shop_role(guild, r)
        if role:
            rn = role.name
        elif r.match_name:
            rn = f"(no role named `{r.match_name}`)"
        else:
            rn = f"(missing id `{r.role_id}`)"
        if not role:
            missing.append(r.name)
        lines.append(f"**{r.name}** — {_fmt(r.price)} {E['coin']}\n└ {rn}")
    desc = "\n\n".join(lines)
    desc += f"\n\n{E['cash']} **your wallet:** {_fmt(wallet)} {E['coin']}"
    desc += (
        "\n\n**Personality role** — to get it, answer **6 questions** in DMs: "
        f"<#1486008473010442383> → **Start in DMs**."
    )
    if missing:
        desc += f"\n\n⚠️ Some roles aren’t in this server — tell an admin."
    return discord.Embed(
        title=f"{E['shop']} role shop",
        description=f"Pick a role below, then confirm.\n\n{desc}",
        color=discord.Color.blurple(),
    ).set_footer(text=buyer_name)


def cash_out_hint() -> str:
    return f"{E['check']} **cash out** before it nukes"


class ShopRoleButton(discord.ui.Button):
    def __init__(self, sr: ShopRole, shop: "ShopView", *, row: int):
        label = f"{sr.name} · {_fmt(sr.price)}"[:80]
        super().__init__(style=discord.ButtonStyle.secondary, label=label, row=row)
        self.sr = sr
        self.shop = shop

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.shop.uid:
            await interaction.response.send_message("not your shop session", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("guild only", ephemeral=True)
            return
        member = guild.get_member(interaction.user.id)
        if not member:
            await interaction.response.send_message("could not load your member profile", ephemeral=True)
            return
        role = resolve_shop_role(guild, self.sr)
        if role is None:
            hint = (
                f"exact name **`{self.sr.match_name}`**"
                if self.sr.match_name
                else f"id `{self.sr.role_id}`"
            )
            await interaction.response.send_message(
                f"role **{self.sr.name}** is not in this server ({hint}).",
                ephemeral=True,
            )
            return
        if role in member.roles:
            await interaction.response.send_message(f"You already have **{role.name}**.", ephemeral=True)
            return
        d = self.shop.cog._get(guild.id, interaction.user.id)
        w = int(d["wallet"])
        if w < self.sr.price:
            await interaction.response.send_message(
                f"Need **{_fmt(self.sr.price)}** {E['coin']} in **wallet** (not bank). "
                f"Short by **{_fmt(self.sr.price - w)}** — use `6withdraw` first.",
                ephemeral=True,
            )
            return
        em = discord.Embed(
            title=f"{E['shop']} confirm purchase",
            description=(
                f"Buy **{role.name}** for **{_fmt(self.sr.price)}** {E['coin']}?\n\n"
                f"{E['cash']} wallet: **{_fmt(w)}** → **{_fmt(w - self.sr.price)}**"
            ),
            color=discord.Color.orange(),
        )
        view = ConfirmPurchaseView(self.shop.cog, guild.id, interaction.user.id, self.sr)
        await interaction.response.edit_message(embed=em, view=view)
        view.message = interaction.message


class ShopView(discord.ui.View):
    def __init__(self, cog: EconomyCog, gid: int, uid: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        for i, sr in enumerate(SHOP_ROLES):
            self.add_item(ShopRoleButton(sr, self, row=i // 5))

    async def on_timeout(self) -> None:
        for c in self.children:
            c.disabled = True
        try:
            if self.message:
                await self.message.edit(view=self)
        except discord.HTTPException:
            pass


class ConfirmPurchaseView(discord.ui.View):
    def __init__(self, cog: EconomyCog, gid: int, uid: int, item: ShopRole):
        super().__init__(timeout=120)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.item = item

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your purchase", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for c in self.children:
            c.disabled = True
        try:
            if self.message:
                ex = discord.Embed(
                    description="⏱️ confirmation expired — run `6shop` again.",
                    color=discord.Color.light_grey(),
                )
                await self.message.edit(embed=ex, view=self)
        except discord.HTTPException:
            pass

    @discord.ui.button(label="confirm", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm_buy(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("guild only", ephemeral=True)
        member = guild.get_member(interaction.user.id)
        if not member:
            return await interaction.response.send_message("member not found", ephemeral=True)
        role = resolve_shop_role(guild, self.item)
        if role is None:
            return await interaction.response.send_message("role no longer exists.", ephemeral=True)
        if role in member.roles:
            return await interaction.response.send_message("you already have this role.", ephemeral=True)

        d = self.cog._get(guild.id, self.uid)
        w = int(d["wallet"])
        if w < self.item.price:
            return await interaction.response.send_message(
                f"no longer enough coins (need **{_fmt(self.item.price)}**).",
                ephemeral=True,
            )

        d["wallet"] = w - self.item.price
        await self.cog._save()

        try:
            await member.add_roles(role, reason="Economy shop purchase")
        except discord.Forbidden:
            d["wallet"] = int(d["wallet"]) + self.item.price
            await self.cog._save()
            await interaction.response.send_message(
                "I can’t assign that role — check bot role **position** above the shop roles. **Refunded.**",
                ephemeral=True,
            )
            return
        except discord.HTTPException:
            d["wallet"] = int(d["wallet"]) + self.item.price
            await self.cog._save()
            await interaction.response.send_message("Discord error assigning role. **Refunded.**", ephemeral=True)
            return

        d2 = self.cog._get(guild.id, self.uid)
        nw = int(d2["wallet"])
        em = discord.Embed(
            title=f"{E['check']} purchased",
            description=f"You now have **{role.name}**\n{E['cash']} wallet: **{_fmt(nw)}** {E['coin']}",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=em, view=None)

    @discord.ui.button(label="cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_buy(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        guild = interaction.guild
        if not guild:
            return await interaction.response.send_message("guild only", ephemeral=True)
        d = self.cog._get(guild.id, self.uid)
        w = int(d["wallet"])
        em = _shop_list_embed(guild, interaction.user.display_name, w)
        view = ShopView(self.cog, guild.id, self.uid)
        await interaction.response.edit_message(embed=em, view=view)
        view.message = interaction.message


class LadderView(discord.ui.View):
    def __init__(self, cog: EconomyCog, gid: int, uid: int, stake: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.pot = stake
        self.message: Optional[discord.Message] = None
        self._done = False

    def _embed(self) -> discord.Embed:
        return discord.Embed(
            title=f"{E['ladder']} ladder",
            description=f"**pot:** `{_fmt(self.pot)}` {E['coin']}\n\n"
            f"**climb** ~68% → ×1.45\n**bank** secure pot",
            color=discord.Color.dark_teal(),
        )

    async def on_timeout(self) -> None:
        self.cog._ladder_users.discard(self.uid)
        if self._done or not self.message:
            return
        self._done = True
        for c in self.children:
            c.disabled = True
        if self.pot > 0:
            d = self.cog._get(self.gid, self.uid)
            d["wallet"] = int(d["wallet"]) + self.pot
            await self.cog._save()
            desc = f"{E['check']} timeout — banked **{_fmt(self.pot)}** {E['coin']}"
        else:
            desc = f"{E['x']} timeout — pot empty"
        try:
            em = discord.Embed(title=f"{E['ladder']} ladder", description=desc, color=discord.Color.light_grey())
            await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your ladder", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="climb", style=discord.ButtonStyle.primary, emoji="🪜")
    async def climb_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._done:
            return await interaction.response.send_message("round over", ephemeral=True)
        if random.random() < 0.32:
            self._done = True
            self.cog._ladder_users.discard(self.uid)
            self.pot = 0
            for c in self.children:
                c.disabled = True
            em = discord.Embed(
                title=f"{E['ladder']} fell off",
                description=f"{E['x']} slipped — **0** {E['coin']}",
                color=discord.Color.red(),
            )
            await interaction.response.edit_message(embed=em, view=self)
            self.stop()
            return
        self.pot = max(1, int(self.pot * 1.45))
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="bank", style=discord.ButtonStyle.success, emoji="🏦")
    async def bank_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._done:
            return await interaction.response.send_message("round over", ephemeral=True)
        self._done = True
        self.cog._ladder_users.discard(self.uid)
        gain = self.pot
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + gain
        await self.cog._save()
        for c in self.children:
            c.disabled = True
        em = discord.Embed(
            title=f"{E['ladder']} banked",
            description=f"{E['check']} secured **{_fmt(gain)}** {E['coin']}",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()


class CoinflipView(discord.ui.View):
    def __init__(self, cog: EconomyCog, gid: int, uid: int, bet: int, channel_id: int):
        super().__init__(timeout=45)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.bet = bet
        self.channel_id = channel_id
        self.message: Optional[discord.Message] = None
        self._done = False

    async def on_timeout(self) -> None:
        if self._done or not self.message:
            return
        self._done = True
        for c in self.children:
            c.disabled = True
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + self.bet
        await self.cog._save()
        em = discord.Embed(
            title=f"{E['coin']} coinflip",
            description=f"{E['x']} timed out — **{_fmt(self.bet)}** {E['coin']} refunded",
            color=discord.Color.light_grey(),
        )
        try:
            await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your flip", ephemeral=True)
            return False
        return True

    async def _resolve(self, interaction: discord.Interaction, choice: str) -> None:
        if self._done:
            await interaction.response.send_message("already done", ephemeral=True)
            return
        self._done = True
        for c in self.children:
            c.disabled = True
        win_side = random.choice(["heads", "tails"])
        d = self.cog._get(self.gid, self.uid)
        if choice == win_side:
            d["wallet"] = int(d["wallet"]) + self.bet * 2
            result = f"{E['check']} **{win_side}** — net **+{_fmt(self.bet)}** {E['coin']}"
            color = discord.Color.green()
        else:
            result = f"{E['x']} **{win_side}** — lost **{_fmt(self.bet)}** {E['coin']}"
            color = discord.Color.red()
        await self.cog._save()
        em = discord.Embed(title=f"{E['coin']} coinflip", description=result, color=color)
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()

    @discord.ui.button(label="heads", style=discord.ButtonStyle.primary, emoji="🪙")
    async def heads(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "heads")

    @discord.ui.button(label="tails", style=discord.ButtonStyle.secondary, emoji="🪙")
    async def tails(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "tails")


class CrashView(discord.ui.View):
    def __init__(self, cog: EconomyCog, gid: int, uid: int, bet: float, crash_at: float):
        super().__init__(timeout=45)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.bet = int(bet)
        self.crash_at = crash_at
        self.mult = 1.0
        self.message: Optional[discord.Message] = None
        self._done = False
        self._task: Optional[asyncio.Task] = None

    def start_tick(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def on_timeout(self) -> None:
        self.cog._crash_users.discard(self.uid)
        if self._done or not self.message:
            return
        self._done = True
        d = self.cog._get(self.gid, self.uid)
        em = discord.Embed(
            title=f"{E['crash']} crashed",
            description=f"{E['x']} timeout — lost **{_fmt(self.bet)}** {E['coin']}",
            color=discord.Color.red(),
        )
        for c in self.children:
            c.disabled = True
        try:
            await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    async def _loop(self) -> None:
        try:
            while not self._done and self.mult < self.crash_at:
                await asyncio.sleep(1.1)
                if self._done:
                    return
                self.mult += random.uniform(0.07, 0.24)
                if self.mult >= self.crash_at:
                    await self._boom(from_loop=True)
                    return
                em = discord.Embed(
                    title=f"{E['crash']} crash",
                    description=f"bet **{_fmt(self.bet)}** {E['coin']}\n**mult:** `{self.mult:.2f}x`\n\n{cash_out_hint()}",
                    color=discord.Color.dark_blue(),
                )
                try:
                    if self.message:
                        await self.message.edit(embed=em, view=self)
                except discord.HTTPException:
                    break
        except asyncio.CancelledError:
            pass
        finally:
            self.cog._crash_users.discard(self.uid)

    async def _boom(self, *, from_loop: bool = False) -> None:
        if self._done:
            return
        self._done = True
        self.cog._crash_users.discard(self.uid)
        for c in self.children:
            c.disabled = True
        if not from_loop and self._task:
            self._task.cancel()
        em = discord.Embed(
            title=f"{E['crash']} nuked at {self.crash_at:.2f}x",
            description=f"{E['x']} lost **{_fmt(self.bet)}** {E['coin']}",
            color=discord.Color.red(),
        )
        try:
            if self.message:
                await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your crash", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="cash out", style=discord.ButtonStyle.success, emoji="💰")
    async def cashout(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._done:
            await interaction.response.send_message("already over", ephemeral=True)
            return
        self._done = True
        self.cog._crash_users.discard(self.uid)
        if self._task:
            self._task.cancel()
        payout = int(self.bet * self.mult)
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + payout
        await self.cog._save()
        for c in self.children:
            c.disabled = True
        profit = payout - self.bet
        em = discord.Embed(
            title=f"{E['crash']} cashed",
            description=f"{E['check']} **{self.mult:.2f}x** → **{_fmt(payout)}** {E['coin']} (**+{_fmt(profit)}**)",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=em, view=self)


class MineCell(discord.ui.Button):
    def __init__(self, idx: int):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="\u200b",
            emoji="⬜",
            row=idx // 4,
            custom_id=f"mines_{idx}",
        )
        self.idx = idx

    async def callback(self, interaction: discord.Interaction):
        v = self.view
        if isinstance(v, MinesView):
            await v._on_tile(self.idx, interaction)


class MinesCashOut(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="cash out",
            style=discord.ButtonStyle.success,
            emoji="💰",
            row=4,
            custom_id="mines_cash",
        )

    async def callback(self, interaction: discord.Interaction):
        v = self.view
        if isinstance(v, MinesView):
            await v._cashout(interaction)


class MinesView(discord.ui.View):
    """4×4 grid, 3 random mines; fair mult step = unrevealed / unrevealed-safe before each safe pick."""

    def __init__(self, cog: EconomyCog, gid: int, uid: int, bet: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.bet = int(bet)
        self.message: Optional[discord.Message] = None
        self._done = False
        self.mult = 1.0
        self.mines = set(random.sample(range(16), 3))
        self.revealed: set[int] = set()
        for i in range(16):
            self.add_item(MineCell(i))
        self.add_item(MinesCashOut())

    def _revealed_safe(self) -> int:
        return len([i for i in self.revealed if i not in self.mines])

    def _sync_buttons(self) -> None:
        for c in self.children:
            if isinstance(c, MineCell):
                idx = c.idx
                if idx in self.revealed:
                    if idx in self.mines:
                        c.emoji = None
                        c.label = E["mine"]
                        c.style = discord.ButtonStyle.danger
                    else:
                        c.label = "\u200b"
                        c.emoji = E["gem"]
                        c.style = discord.ButtonStyle.success
                    c.disabled = True
                else:
                    if self._done and idx in self.mines:
                        c.emoji = None
                        c.label = E["mine"]
                        c.style = discord.ButtonStyle.danger
                    else:
                        c.label = "\u200b"
                        c.emoji = "⬜"
                        c.style = discord.ButtonStyle.secondary
                    c.disabled = self._done
            else:
                c.disabled = self._done

    def build_embed(self) -> discord.Embed:
        rs = self._revealed_safe()
        pay = int(self.bet * self.mult)
        desc = (
            f"bet **{_fmt(self.bet)}** {E['coin']}\n"
            f"**mult:** `{self.mult:.2f}x`  →  **now:** `{_fmt(pay)}` {E['coin']}\n"
            f"**gems found:** `{rs}` / 13\n\n"
            f"tap a tile — {E['gem']} safe raises mult · {E['mine']} loses the bet"
        )
        return discord.Embed(title=f"{E['mine']} mines", description=desc, color=discord.Color.dark_grey())

    async def on_timeout(self) -> None:
        self.cog._mines_users.discard(self.uid)
        if self._done or not self.message:
            return
        self._done = True
        self._sync_buttons()
        payout = int(self.bet * self.mult)
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + payout
        await self.cog._save()
        profit = payout - self.bet
        sign = "+" if profit >= 0 else ""
        em = discord.Embed(
            title=f"{E['mine']} timed out",
            description=f"{E['check']} auto cash-out **{_fmt(payout)}** {E['coin']} ({sign}{_fmt(profit)})",
            color=discord.Color.light_grey(),
        )
        try:
            await self.message.edit(embed=em, view=self)
        except discord.HTTPException:
            pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your mines", ephemeral=True)
            return False
        return True

    async def _on_tile(self, idx: int, interaction: discord.Interaction) -> None:
        if self._done:
            return await interaction.response.send_message("round over", ephemeral=True)
        if idx in self.revealed:
            return await interaction.response.send_message("already revealed", ephemeral=True)

        if idx in self.mines:
            self._done = True
            self.cog._mines_users.discard(self.uid)
            self.revealed.add(idx)
            self._sync_buttons()
            for c in self.children:
                if not isinstance(c, MineCell):
                    c.disabled = True
            em = discord.Embed(
                title=f"{E['mine']} hit a mine",
                description=f"{E['x']} lost **{_fmt(self.bet)}** {E['coin']}",
                color=discord.Color.red(),
            )
            await interaction.response.edit_message(embed=em, view=self)
            self.stop()
            return

        revealed_safe = self._revealed_safe()
        s_unrevealed = 13 - revealed_safe
        u_unrevealed = 16 - len(self.revealed)
        if s_unrevealed > 0 and u_unrevealed > 0:
            self.mult *= u_unrevealed / s_unrevealed
        self.revealed.add(idx)

        if self._revealed_safe() >= 13:
            await self._finalize_clear(interaction)
            return

        self._sync_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _finalize_clear(self, interaction: discord.Interaction) -> None:
        self._done = True
        self.cog._mines_users.discard(self.uid)
        payout = int(self.bet * self.mult)
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + payout
        await self.cog._save()
        self._sync_buttons()
        for c in self.children:
            if not isinstance(c, MineCell):
                c.disabled = True
        profit = payout - self.bet
        em = discord.Embed(
            title=f"{E['mine']} cleared",
            description=f"{E['check']} all safe tiles — **{_fmt(payout)}** {E['coin']} (**+{_fmt(profit)}**)\n**{self.mult:.2f}x**",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()

    async def _cashout(self, interaction: discord.Interaction) -> None:
        if self._done:
            return await interaction.response.send_message("round over", ephemeral=True)
        self._done = True
        self.cog._mines_users.discard(self.uid)
        payout = int(self.bet * self.mult)
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + payout
        await self.cog._save()
        self._sync_buttons()
        for c in self.children:
            if not isinstance(c, MineCell):
                c.disabled = True
        profit = payout - self.bet
        sign = "+" if profit >= 0 else ""
        em = discord.Embed(
            title=f"{E['mine']} cashed out",
            description=f"{E['check']} **{self.mult:.2f}x** → **{_fmt(payout)}** {E['coin']} ({sign}{_fmt(profit)})",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()


class BlackjackView(discord.ui.View):
    def __init__(self, cog: EconomyCog, gid: int, uid: int, bet: int, deck: list[str], ph: list[str], dh: list[str]):
        super().__init__(timeout=120)
        self.cog = cog
        self.gid = gid
        self.uid = uid
        self.bet = bet
        self.deck = deck
        self.ph = ph
        self.dh = dh
        self.message: Optional[discord.Message] = None
        self._over = False

    async def on_timeout(self) -> None:
        self.cog._bj_users.discard(self.uid)
        if self._over or not self.message:
            return
        self._over = True
        for c in self.children:
            c.disabled = True
        d = self.cog._get(self.gid, self.uid)
        d["wallet"] = int(d["wallet"]) + self.bet
        await self.cog._save()
        em = discord.Embed(
            title=f"{E['bj']} timeout",
            description=f"{E['x']} refunded **{_fmt(self.bet)}** {E['coin']}",
            color=discord.Color.light_grey(),
        )
        try:
            await self.message.edit(embed=em, view=None)
        except discord.HTTPException:
            pass

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.uid:
            await interaction.response.send_message("not your hand", ephemeral=True)
            return False
        return True

    def _embed(self, hide_dealer: bool = True) -> discord.Embed:
        em = discord.Embed(
            title=f"{E['bj']} blackjack — bet {_fmt(self.bet)} {E['coin']}",
            color=discord.Color.dark_green(),
        )
        em.add_field(
            name="you",
            value=format_hand_with_total(self.ph),
            inline=False,
        )
        em.add_field(
            name="dealer",
            value=format_hand_with_total(self.dh, hide_dealer_hole=hide_dealer),
            inline=False,
        )
        em.set_footer(text="hit / stand")
        return em

    async def _finish_win(self, interaction: discord.Interaction, msg: str, payout_mult: float) -> None:
        self._over = True
        self.cog._bj_users.discard(self.uid)
        for c in self.children:
            c.disabled = True
        d = self.cog._get(self.gid, self.uid)
        pay = int(self.bet * payout_mult)
        d["wallet"] = int(d["wallet"]) + pay
        await self.cog._save()
        em = self._embed(hide_dealer=False)
        em.description = msg
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()

    async def _finish_lose(self, interaction: discord.Interaction, msg: str) -> None:
        self._over = True
        self.cog._bj_users.discard(self.uid)
        for c in self.children:
            c.disabled = True
        em = self._embed(hide_dealer=False)
        em.description = msg
        await interaction.response.edit_message(embed=em, view=self)
        self.stop()

    @discord.ui.button(label="hit", style=discord.ButtonStyle.primary, emoji="➕")
    async def hit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._over:
            return await interaction.response.send_message("hand over", ephemeral=True)
        self.ph.append(self.deck.pop())
        ht = hand_total(self.ph)
        if ht > 21:
            await self._finish_lose(
                interaction,
                f"{E['x']} **bust** ({hand_total_display(self.ph)}) — lost **{_fmt(self.bet)}**",
            )
            return
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="stand", style=discord.ButtonStyle.danger, emoji="✋")
    async def stand(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self._over:
            return await interaction.response.send_message("hand over", ephemeral=True)
        while hand_total(self.dh) < 17 and self.deck:
            self.dh.append(self.deck.pop())
        pt, dt = hand_total(self.ph), hand_total(self.dh)
        if dt > 21:
            await self._finish_win(
                interaction,
                f"{E['check']} dealer bust ({hand_total_display(self.dh)}) — **blackjack pay**",
                2.0,
            )
        elif pt > dt:
            await self._finish_win(
                interaction,
                f"{E['check']} you **{hand_total_display(self.ph)}** beat **{hand_total_display(self.dh)}**",
                2.0,
            )
        elif pt == dt:
            self.cog._bj_users.discard(self.uid)
            d = self.cog._get(self.gid, self.uid)
            d["wallet"] = int(d["wallet"]) + self.bet
            await self.cog._save()
            self._over = True
            for c in self.children:
                c.disabled = True
            em = self._embed(hide_dealer=False)
            em.description = f"push **{hand_total_display(self.ph)}** — bet returned"
            await interaction.response.edit_message(embed=em, view=self)
            self.stop()
        else:
            await self._finish_lose(
                interaction,
                f"{E['x']} **{hand_total_display(self.ph)}** vs **{hand_total_display(self.dh)}** — lost **{_fmt(self.bet)}**",
            )


