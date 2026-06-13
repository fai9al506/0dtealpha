import json, glob, os, re
DIR = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"
OUT=open("_tmp_dm_execution.txt","w",encoding="utf-8")
def w(s): OUT.write(s+"\n")

# A) ALL Dark Matter live messages from the day-trading CENTRAL channel (execution calls live here)
central=[f for f in glob.glob(os.path.join(DIR,"*.json")) if "Daytrading - central" in f or "daytrading-central" in f]
msgs=[]
for f in central:
    try:
        with open(f,encoding="utf-8") as fh: data=json.load(fh)
    except: continue
    for m in data.get("messages",[]):
        a=(m.get("author",{}) or {}).get("name","")
        if "darkmatter" not in a.lower(): continue
        ts=m.get("timestamp","")[:16].replace("T"," ")
        c=re.sub(r"\s+"," ",(m.get("content") or "")).strip()
        if c: msgs.append((ts,c))
# dedup + sort
seen=set(); uniq=[]
for ts,c in sorted(msgs):
    k=(ts,c)
    if k in seen: continue
    seen.add(k); uniq.append((ts,c))

EXEC=re.compile(r"\b(short|long|cover|covering|covered|stop|stopped|add|adding|added|trim|trimm|fill|filled|entry|enter|exit|exited|flat|flatten|target|tgt|scalp|bid|offer|sold|sell|buy|bought|bot|in here|out here|position|size|risk|reject|reclaim|break|fade|holding|trail)\b",re.I)
LEVEL=re.compile(r"\b(6[5-9]\d{2}|7[0-6]\d{2})\b")

w(f"===== DARK MATTER LIVE CALLS (day-trading-central) — {len(uniq)} msgs =====")
w("(★ = execution language, # = price level mentioned)\n")
for ts,c in uniq:
    tag=""
    if EXEC.search(c): tag+="★"
    if LEVEL.search(c): tag+="#"
    w(f"[{ts}] {tag:<3}{c[:400]}")
OUT.close()
print(f"done -> _tmp_dm_execution.txt ; {len(uniq)} central msgs, "
      f"{sum(1 for ts,c in uniq if EXEC.search(c))} with execution language")
