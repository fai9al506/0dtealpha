"""Extract Jun 4 (evening) + Jun 5 messages from the daytrading-central export, in ET."""
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
PATH = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output\Volland Discord - Volland Daytrading - ⛅│volland-daytrading-central [1362818729347645754] (after 2026-05-31).json"

with open(PATH, encoding="utf-8-sig") as f:
    data = json.load(f)
msgs = data.get("messages", [])
print(f"total messages: {len(msgs)}")

out = []
for m in msgs:
    ts = m.get("timestamp")
    if not ts:
        continue
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        continue
    et = dt.astimezone(NY)
    if not (et.date().isoformat() in ("2026-06-04", "2026-06-05")):
        continue
    author = (m.get("author") or {}).get("name", "?")
    content = (m.get("content") or "").replace("\n", " | ")
    embeds = m.get("embeds") or []
    etxt = ""
    if embeds:
        parts = []
        for e in embeds:
            t = e.get("title") or ""
            d = (e.get("description") or "").replace("\n", " | ")
            parts.append(f"{t} {d}".strip())
        etxt = " [EMBED: " + " || ".join(parts)[:400] + "]"
    if content or etxt:
        out.append(f"[{et.strftime('%m-%d %H:%M')}] {author}: {content[:600]}{etxt}")

print(f"Jun 4-5 messages: {len(out)}")
with open("_tmp_discord_friday.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
print("written to _tmp_discord_friday.txt")
