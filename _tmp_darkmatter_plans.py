import json, glob, os, re
DIR = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"
files = sorted(glob.glob(os.path.join(DIR, "*.json")))
OUT = open("_tmp_darkmatter_plans.txt", "w", encoding="utf-8")
def w(s): OUT.write(s+"\n")

dm=[]
for f in files:
    try:
        with open(f, encoding="utf-8") as fh: data=json.load(fh)
    except: continue
    for m in data.get("messages", []):
        a=(m.get("author",{}) or {}).get("name","")
        if "darkmatter" not in a.lower(): continue
        content=(m.get("content") or "")
        emb=[]
        for e in m.get("embeds",[]):
            if e.get("title"): emb.append("T:"+e["title"])
            if e.get("description"): emb.append(e["description"])
            for fl in e.get("fields",[]): emb.append(f"{fl.get('name','')}={fl.get('value','')}")
        full=re.sub(r"\s+"," ",(content+" "+" ".join(emb))).strip()
        ts=m.get("timestamp","")[:16].replace("T"," ")
        dm.append((ts, full))

dm.sort()
# substantive: long messages OR messages with spx/es levels + bias words
biaswords=re.compile(r"\b(bias|plan|level|target|support|resist|bull|bear|short|long|gamma|charm|vanna|vix|overvix|undervix|dealer|delta|hedg|magnet|edge|flip|reversion|trend|7[0-9]{3}|spx|range)\b", re.I)
kept=[(ts,full) for ts,full in dm if len(full)>=160 and biaswords.search(full)]
w(f"Dark Matter substantive planning/bias posts (>=160 chars, bias-related): {len(kept)} of {len(dm)} total\n")
for ts, full in kept:
    w(f"\n[{ts}]\n{full[:1400]}")
OUT.close()
print(f"done -> _tmp_darkmatter_plans.txt ; kept {len(kept)} of {len(dm)}")
