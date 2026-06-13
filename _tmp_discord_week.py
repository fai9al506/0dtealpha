"""Extract full week Jun 1-5 from daytrading-central export, one file per day."""
import json
from datetime import datetime
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
PATH = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output\Volland Discord - Volland Daytrading - ⛅│volland-daytrading-central [1362818729347645754] (after 2026-05-31).json"

with open(PATH, encoding="utf-8-sig") as f:
    data = json.load(f)

by_day = {}
for m in data.get("messages", []):
    ts = m.get("timestamp")
    if not ts:
        continue
    try:
        et = datetime.fromisoformat(ts).astimezone(NY)
    except ValueError:
        continue
    d = et.date().isoformat()
    if d < "2026-06-01" or d > "2026-06-05":
        continue
    author = (m.get("author") or {}).get("name", "?")
    content = (m.get("content") or "").replace("\n", " | ")
    embeds = m.get("embeds") or []
    etxt = ""
    if embeds:
        parts = []
        for e in embeds:
            t = e.get("title") or ""
            dd = (e.get("description") or "").replace("\n", " | ")
            parts.append(f"{t} {dd}".strip())
        joined = " || ".join(p for p in parts if p)
        if joined:
            etxt = " [EMBED: " + joined[:350] + "]"
    if content or etxt:
        by_day.setdefault(d, []).append(f"[{et.strftime('%H:%M')}] {author}: {content[:450]}{etxt}")

for d, lines in sorted(by_day.items()):
    fn = f"_tmp_discord_{d}.txt"
    with open(fn, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"{d}: {len(lines)} messages -> {fn}")
