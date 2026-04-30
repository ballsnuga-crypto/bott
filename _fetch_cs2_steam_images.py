"""Fetch Steam CDN economy image URLs -> cs2_steam_urls.json (UTF-8)."""
from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
OUT = Path(__file__).resolve().parent / "cs2_steam_urls.json"

WEARS = [
    "Field-Tested",
    "Minimal Wear",
    "Factory New",
    "Well-Worn",
    "Battle-Scarred",
]

# (id, base name, extra full hash tries after wears)
GUNS: list[tuple[str, str, list[str]]] = [
    ("c_sand", "P250 | Sand Dune", []),
    ("c_urban", "UMP-45 | Urban DDPAT", []),
    ("c_safari", "Nova | Safari Mesh", ["Nova | Predator (Field-Tested)"]),
    ("c_ground", "PP-Bizon | Sand Dashed", []),
    ("i_cortex", "USP-S | Cortex", []),
    ("i_capil", "Five-SeveN | Capillary", []),
    ("i_offworld", "Glock-18 | Off World", []),
    ("i_oxide", "MAC-10 | Oceanic", []),
    ("m_phantom", "M4A4 | Magnesium", []),
    ("m_guerrilla", "AK-47 | Uncharted", []),
    ("m_trigger", "Desert Eagle | Directive", []),
    ("m_ghost", "AWP | Acheron", []),
    ("r_mecha", "FAMAS | Mecha Industries", []),
    ("r_monkey", "M4A1-S | Monkey Business", ["M4A1-S | Decimator (Field-Tested)"]),
    ("r_hyper", "Galil AR | Chromatic Aberration", []),
    ("r_print", "P90 | Printstream", ["P90 | Asiimov (Field-Tested)"]),
    ("cl_neon", "MP7 | Neon Ply", []),
    ("cl_empress", "AK-47 | The Empress", []),
    ("cl_kumicho", "Desert Eagle | Kumicho Dragon", []),
    ("cv_lightning", "AWP | Lightning Strike", []),
    ("cv_fade", "Glock-18 | Fade", ["Glock-18 | Twilight Galaxy (Minimal Wear)"]),
    ("cv_fire", "M4A4 | Howl", ["M4A4 | Neo-Noir (Field-Tested)"]),
    ("ex_dragon", "AWP | Dragon Lore", ["AWP | Asiimov (Field-Tested)"]),
]

KNIVES: list[tuple[str, list[str]]] = [
    (
        "ex_sapphire",
        [
            "M9 Bayonet | Gamma Doppler (Factory New)",
            "M9 Bayonet | Doppler (Factory New)",
            "M9 Bayonet | Tiger Tooth (Factory New)",
        ],
    ),
    (
        "ex_ruby",
        [
            "Karambit | Gamma Doppler (Factory New)",
            "Karambit | Doppler (Factory New)",
            "Karambit | Tiger Tooth (Factory New)",
        ],
    ),
]


def extract_img(html: str) -> str | None:
    m = re.search(r"https://[^\"']+economy/image/[^\"']+", html)
    return m.group(0) if m else None


def try_fetch(hashname: str) -> str | None:
    path = urllib.parse.quote(hashname, safe="")
    url = f"https://steamcommunity.com/market/listings/730/{path}"
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            html = r.read().decode("utf-8", "replace")
    except Exception:
        return None
    if "There are no listings for this item" in html:
        return None
    return extract_img(html)


def try_skin(iid: str, base: str, extras: list[str]) -> str:
    for w in WEARS:
        u = try_fetch(f"{base} ({w})")
        time.sleep(0.28)
        if u:
            return u
    for h in extras:
        u = try_fetch(h)
        time.sleep(0.28)
        if u:
            return u
    return ""


def main() -> None:
    out: dict[str, str] = {}
    for iid, base, extras in GUNS:
        out[iid] = try_skin(iid, base, extras)
        time.sleep(0.35)

    for iid, cands in KNIVES:
        u = ""
        for h in cands:
            u = try_fetch("\u2605 " + h) or try_fetch(h)
            time.sleep(0.3)
            if u:
                break
        out[iid] = u or ""
        time.sleep(0.35)

    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("wrote", OUT, "keys", sum(1 for v in out.values() if v), "/", len(out))


if __name__ == "__main__":
    main()
