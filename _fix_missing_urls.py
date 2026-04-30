import json
import re
import urllib.parse
import urllib.request
from pathlib import Path

UA = {"User-Agent": "Mozilla/5.0"}
OUT = Path(__file__).resolve().parent / "cs2_steam_urls.json"


def extract(html: str):
    m = re.search(r"https://[^\"']+economy/image/[^\"']+", html)
    return m.group(0) if m else None


def fetch(h: str):
    q = urllib.parse.quote(h, safe="")
    url = f"https://steamcommunity.com/market/listings/730/{q}"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        html = r.read().decode("utf-8", "replace")
    if "There are no listings for this item" in html:
        return None
    return extract(html)


data = json.loads(OUT.read_text(encoding="utf-8"))
fixes = {
    "r_print": fetch("P90 | Printstream (Minimal Wear)") or fetch("P90 | Printstream (Field-Tested)"),
    "r_monkey": fetch("M4A1-S | Monkey Business (Field-Tested)")
    or fetch("M4A1-S | Monkey Business (Minimal Wear)"),
    "cv_fade": fetch("Glock-18 | Fade (Factory New)") or fetch("Glock-18 | Fade (Minimal Wear)"),
    "cv_fire": fetch("M4A4 | Howl (Field-Tested)") or fetch("M4A4 | Howl (Minimal Wear)"),
    "ex_dragon": fetch("AWP | Dragon Lore (Field-Tested)")
    or fetch("AWP | Dragon Lore (Minimal Wear)")
    or fetch("AWP | Dragon Lore (Factory New)"),
    "ex_sapphire": fetch("\u2605 M9 Bayonet | Doppler (Factory New)"),
    "ex_ruby": fetch("\u2605 Karambit | Doppler (Factory New)"),
}
for k, v in fixes.items():
    if v:
        data[k] = v
        print("fixed", k)
    else:
        print("still missing", k)

OUT.write_text(json.dumps(data, indent=2), encoding="utf-8")
