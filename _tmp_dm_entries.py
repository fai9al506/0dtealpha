"""Extract Dark Matter's ACTUAL entry setups from his weekly thread files +
day-trading channel, with date/direction/level — to reverse-engineer his entries.
"""
import glob, os, re, json
# thread text files (his dark-matter-trade weekly threads, full daily updates)
files=sorted(glob.glob("_tmp_dm_*.txt"))
files=[f for f in files if re.search(r"_tmp_dm_\d+-\d+\.txt$", f)]
OUT=open("_tmp_dm_entries.txt","w",encoding="utf-8")
def w(s): OUT.write(s+"\n")

# patterns for explicit entries/setups with a price level
SETUP=re.compile(r"(Setup [A-D][^\n]{0,120})", re.I)
BUYFADE=re.compile(r"((?:Buy|Fade|Entry|Long|Short|Stand aside|Pin|Magnet|Active floor|Active ceiling|Hard cap|Hard ceiling)[:\s][^\n]{0,110})", re.I)
LVL=re.compile(r"\b(6[5-9]\d{2}|7[0-6]\d{2})\b")
DIR=re.compile(r"\b(long|short|buy|fade|sell|cushion|floor|support|resist|ceiling|cap|reject|breakdown|breakout|continuation|reclaim)\b", re.I)

for f in files:
    txt=open(f,encoding="utf-8").read()
    wk=re.search(r"_tmp_dm_(\d+-\d+)",f).group(1)
    w(f"\n========== WEEK {wk} ==========")
    seen=set()
    for m in SETUP.finditer(txt):
        s=re.sub(r"\s+"," ",m.group(1)).strip()
        if s[:30] in seen or not LVL.search(s): continue
        seen.add(s[:30]); w("  [SETUP] "+s[:130])
    for m in BUYFADE.finditer(txt):
        s=re.sub(r"\s+"," ",m.group(1)).strip()
        if not LVL.search(s) or len(s)<12: continue
        if s[:40] in seen: continue
        seen.add(s[:40]); w("  [LVL]   "+s[:120])
OUT.close()
print("done -> _tmp_dm_entries.txt")
print("lines:", sum(1 for _ in open("_tmp_dm_entries.txt",encoding="utf-8")))
