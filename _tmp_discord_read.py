import json, sys, glob

path = sys.argv[1] if len(sys.argv) > 1 else None
with open(path, encoding="utf-8-sig") as f:
    data = json.load(f)
msgs = data.get("messages", [])
print(f"{len(msgs)} messages, channel={data.get('channel', {}).get('name')}")
for m in msgs:
    ts = (m.get("timestamp") or "")[:16].replace("T", " ")
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
        etxt = " [EMBED: " + " || ".join(parts)[:600] + "]"
    if content or etxt:
        print(f"[{ts}] {author}: {content[:500]}{etxt}")
