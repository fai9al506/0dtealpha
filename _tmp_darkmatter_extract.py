import json, glob, os, re
DIR = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"
files = sorted(glob.glob(os.path.join(DIR, "*.json")))
OUT = open("_tmp_darkmatter.txt", "w", encoding="utf-8")
def w(s): OUT.write(s+"\n")

# 1) all messages authored by dark matter, across every export
authors_seen = set()
dm_msgs = []
thread_refs = set()
for f in files:
    try:
        with open(f, encoding="utf-8") as fh: data = json.load(fh)
    except Exception as e:
        continue
    for m in data.get("messages", []):
        a = (m.get("author", {}) or {}).get("name", "")
        authors_seen.add(a)
        content = (m.get("content") or "")
        # capture dark matter's own posts
        if "darkmatter" in a.lower() or "dark matter" in a.lower():
            ts = m.get("timestamp","")[:16].replace("T"," ")
            emb=[]
            for e in m.get("embeds",[]):
                if e.get("title"): emb.append("T:"+e["title"])
                if e.get("description"): emb.append(e["description"])
                for fl in e.get("fields",[]): emb.append(f"{fl.get('name','')}={fl.get('value','')}")
            full = re.sub(r"\s+"," ",(content+" || "+" | ".join(emb)) if emb else content).strip()
            dm_msgs.append((ts, full, os.path.basename(f)))
        # detect thread references / mentions of dark matter plan
        if re.search(r"thread|trade plan|@Dark ?Matter", content, re.I):
            thread_refs.add((m.get("timestamp","")[:10], re.sub(r"\s+"," ",content)[:160]))

w(f"Authors with 'darkmatter' in name: {[a for a in authors_seen if 'dark' in a.lower()]}")
w(f"\nTotal Dark Matter authored messages found across ALL exports: {len(dm_msgs)}\n")
dm_msgs.sort()
for ts, full, src in dm_msgs:
    if full.strip():
        w(f"[{ts}] {full[:1000]}")

w("\n\n===== messages REFERENCING thread/trade-plan/@Dark Matter (to locate his thread) =====")
for d, c in sorted(thread_refs):
    w(f"[{d}] {c}")
OUT.close()
print("done -> _tmp_darkmatter.txt;  DM msgs:", len(dm_msgs))
