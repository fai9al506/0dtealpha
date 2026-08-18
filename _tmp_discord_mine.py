# -*- coding: utf-8 -*-
import re
from bs4 import BeautifulSoup

PATH = r"C:/Users/Faisa/OneDrive/Desktop/DiscordChatExporter.win-x64/Output/Volland Discord - Volland Daytrading - ⛅│volland-daytrading-central [1362818729347645754] (2026-06-21 to 2026-07-28).html"
soup = BeautifulSoup(open(PATH, encoding="utf-8").read(), "html.parser")
msgs = []
cur = None
for grp in soup.select(".chatlog__message-group"):
    a = grp.select_one(".chatlog__author")
    if a: cur = a.get("title") or a.get_text(strip=True)
    for m in grp.select(".chatlog__message"):
        ts = m.select_one(".chatlog__timestamp")
        ts = ts.get_text(strip=True) if ts else ""
        md = m.select_one(".chatlog__markdown")
        t = md.get_text(" ", strip=True) if md else ""
        if t: msgs.append((ts, cur, t))

ANALYSTS = {'wizardofops', 'apollobix', 'bigbill8887', 'otc4313', 'l0rd.helmet', 'gammahivey'}
KW = re.compile(r'\b(regime|chop|choppy|range[- ]?bound|low ?vol|no ?vol|grind|pin|magnet|gamma|vanna|charm|dealer|fade|trend|breakout|break down|VIX|mean revert|reversion|summer|melt|drift|squeeze|flip|resist|support|floor|ceiling|short|sell|put|call wall|0dte|scalp|bias|setup|edge)\b', re.I)

out = []
out.append("========== ALL POSTS FROM KEY ANALYSTS ==========\n")
for ts, a, t in msgs:
    if a in ANALYSTS and len(t) >= 40:
        out.append(f"[{ts}] <{a}> {t}")

out.append("\n\n========== KEYWORD-MATCHED POSTS >=80 chars (any author) ==========\n")
seen = set()
for ts, a, t in msgs:
    if len(t) >= 80 and KW.search(t) and t not in seen:
        seen.add(t)
        out.append(f"[{ts}] <{a}> {t}")

open("_tmp_discord_mined.txt", "w", encoding="utf-8").write("\n".join(out))
print(f"analyst posts + kw posts -> _tmp_discord_mined.txt ({len(out)} lines)")
