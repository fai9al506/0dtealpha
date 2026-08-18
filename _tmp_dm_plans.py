# -*- coding: utf-8 -*-
import glob, re
from bs4 import BeautifulSoup
base = r"C:/Users/Faisa/OneDrive/Desktop/DiscordChatExporter.win-x64/Output"
files = sorted(glob.glob(base + "/*dark-matter-trade*7_*.html") +
               glob.glob(base + "/*dark-matter-trade*6_29*.html") +
               glob.glob(base + "/*dark-matter-trade*6_22*.html"))
out = []
for f in files:
    soup = BeautifulSoup(open(f, encoding="utf-8").read(), "html.parser")
    name = re.search(r'- ([0-9]+_[0-9]+ .*?) \[', f)
    out.append("\n\n############################################################")
    out.append("PLAN: " + (name.group(1) if name else f.split('/')[-1][:60]))
    out.append("############################################################")
    txt = []
    for m in soup.select(".chatlog__markdown"):
        t = m.get_text("\n", strip=True)
        if t: txt.append(t)
    full = re.sub(r'\n{3,}', '\n\n', "\n".join(txt))
    out.append(full[:7000])
open("_tmp_dm_plans.txt", "w", encoding="utf-8").write("\n".join(out))
print("wrote _tmp_dm_plans.txt", len("\n".join(out)), "chars")
