"""Pull skin preview URLs from ByMykel CSGO-API skins.json."""
import json
import urllib.request

URL = "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/skins.json"
NEED = [
    "P250 | Sand Dune",
    "UMP-45 | Urban DDPAT",
    "Nova | Safari Mesh",
    "PP-Bizon | Sand Dashed",
    "USP-S | Cortex",
    "Five-SeveN | Capillary",
    "Glock-18 | Off World",
    "MAC-10 | Oceanic",
    "M4A4 | Magnesium",
    "AK-47 | Uncharted",
    "Desert Eagle | Directive",
    "AWP | Acheron",
    "FAMAS | Mecha Industries",
    "M4A1-S | Monkey Business",
    "Galil AR | Chromatic Aberration",
    "P90 | Printstream",
    "MP7 | Neon Ply",
    "AK-47 | The Empress",
    "Desert Eagle | Kumicho Dragon",
    "AWP | Lightning Strike",
    "Glock-18 | Fade",
    "M4A4 | Howl",
    "AWP | Dragon Lore",
    "M9 Bayonet | Doppler",
    "Karambit | Doppler",
]

raw = urllib.request.urlopen(URL, timeout=60).read()
skins = json.loads(raw.decode("utf-8"))
by_name = {s["name"]: s.get("image") or "" for s in skins}
for n in NEED:
    u = by_name.get(n, "")
    print(n, "=>", (u[:100] + "…") if len(u) > 100 else u)
