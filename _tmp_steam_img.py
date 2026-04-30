import re
import urllib.request

url = "https://steamcommunity.com/market/listings/730/Five-SeveN%20%7C%20Capillary%20%28Field-Tested%29"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=20) as r:
    html = r.read().decode("utf-8", "replace")
m = re.search(r"https://[^\"']+economy/image/[^\"']+", html)
print(m.group(0) if m else "not found")
m2 = re.search(r"economy/image/([^\"'\\s]+)", html)
print(m2.group(0)[:120] if m2 else "no2")
