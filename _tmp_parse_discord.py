import json, glob, os, re

DIR = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"
# the two "after 2026-06-07" files
files = [f for f in glob.glob(os.path.join(DIR, "*.json")) if "after 2026-06-07" in f]
OUT = open("_tmp_discord_parsed.txt", "w", encoding="utf-8")
def w(s): OUT.write(s + "\n")

for f in files:
    with open(f, encoding="utf-8") as fh:
        data = json.load(fh)
    chan = data.get("channel", {}).get("name", "?")
    msgs = data.get("messages", [])
    w(f"\n########## {os.path.basename(f)} — {len(msgs)} msgs ##########")
    for m in msgs:
        ts = m.get("timestamp", "")[:16].replace("T", " ")
        author = m.get("author", {}).get("name", "?")
        content = (m.get("content") or "").strip()
        # include embeds text
        emb = m.get("embeds", [])
        emb_txt = []
        for e in emb:
            if e.get("title"): emb_txt.append("TITLE:"+e["title"])
            if e.get("description"): emb_txt.append(e["description"])
            for fld in e.get("fields", []):
                emb_txt.append(f"{fld.get('name','')}={fld.get('value','')}")
        full = content
        if emb_txt:
            full += " || " + " | ".join(emb_txt)
        full = re.sub(r"\s+", " ", full).strip()
        if not full:
            continue
        w(f"[{ts}] {author}: {full[:800]}")
OUT.close()
print("done -> _tmp_discord_parsed.txt")
