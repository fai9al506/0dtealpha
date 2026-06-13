"""Extract Jun 3 messages from the two Discord exports."""
import json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

FILES = [
    r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output\Volland Discord - Volland Daytrading - ⛅│volland-daytrading-central [1362818729347645754] (after 2026-05-31).json",
    r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output\Volland Discord - 0DTE Alerts - 📨┃0dte-alerts [1362822187647500489] (after 2026-06-01).json",
]
for f in FILES:
    try:
        data = json.load(open(f, encoding="utf-8"))
    except Exception as e:
        print(f"!! {f}: {e}")
        continue
    msgs = data.get("messages", [])
    day = [m for m in msgs if (m.get("timestamp") or "").startswith("2026-06-03")]
    print(f"\n######## {data.get('channel',{}).get('name','?')} — Jun 3 messages: {len(day)} (total {len(msgs)}) ########")
    for m in day:
        t = m["timestamp"][11:16]  # UTC clock
        author = m.get("author", {}).get("nickname") or m.get("author", {}).get("name", "?")
        content = (m.get("content") or "").replace("\n", " | ")
        if not content and m.get("embeds"):
            content = " | ".join((e.get("description") or e.get("title") or "")[:300] for e in m["embeds"])
        if content.strip():
            print(f"[{t}Z] {author}: {content[:600]}")
