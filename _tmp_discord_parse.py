# -*- coding: utf-8 -*-
import re, sys
from collections import Counter, defaultdict
from bs4 import BeautifulSoup

PATH = r"C:/Users/Faisa/OneDrive/Desktop/DiscordChatExporter.win-x64/Output/Volland Discord - Volland Daytrading - ⛅│volland-daytrading-central [1362818729347645754] (2026-06-21 to 2026-07-28).html"
html = open(PATH, encoding="utf-8").read()
soup = BeautifulSoup(html, "html.parser")

# Messages: each message-group has an author; each message has a timestamp + markdown
msgs = []
cur_author = None
for grp in soup.select(".chatlog__message-group"):
    a = grp.select_one(".chatlog__author")
    author = a.get("title") or a.get_text(strip=True) if a else cur_author
    if author: cur_author = author
    for m in grp.select(".chatlog__message"):
        ts_el = m.select_one(".chatlog__timestamp")
        ts = ts_el.get_text(strip=True) if ts_el else ""
        md = m.select_one(".chatlog__markdown")
        txt = md.get_text(" ", strip=True) if md else ""
        if txt:
            msgs.append((ts, cur_author, txt))

print(f"TOTAL messages: {len(msgs)}")
auth = Counter(a for _, a, _ in msgs)
print("\nTOP AUTHORS by message count:")
for a, n in auth.most_common(15):
    chars = sum(len(t) for _, aa, t in msgs if aa == a)
    print(f"  {n:5d} msgs  {chars:7d} chars  {a}")

with open("_tmp_discord_extract.txt", "w", encoding="utf-8") as f:
    for ts, a, t in msgs:
        f.write(f"[{ts}] <{a}> {t}\n")
print("\nFull transcript -> _tmp_discord_extract.txt")

# Substantive posts: long messages (plans/analysis) from anyone
print("\n=== LONG posts (>=400 chars) count by author ===")
longby = Counter(a for _, a, t in msgs if len(t) >= 400)
for a, n in longby.most_common(10):
    print(f"  {n:4d}  {a}")
