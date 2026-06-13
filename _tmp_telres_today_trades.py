import os, requests
TG = os.environ.get("TELEGRAM_BOT_TOKEN") or "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
CHAT = "-1003792574755"
msg = """<b>📋 Today's GEX Long trades — gate removed, TS GEX (2026-06-02)</b>
Exit: SL14 / target=max(magnet,+20) / trail act15 gap5

<pre>
entry  para        g al  entry  tgt  result   pnl   MFE   MAE  exit
09:36  BOFA-PURE   A +1  7589  7620  WIN    +30.8 +30.9  -3.1 11:22 tgt
10:08  BOFA-PURE   B +1  7595  7620  WIN    +24.9 +24.9  -1.7 11:22 tgt
10:24  BOFA-PURE   B +1  7597  7620  WIN    +22.6 +22.6  -4.0 11:22 tgt
10:40  BOFA-PURE   B +1  7602  7622  WIN    +13.2 +18.2  +0.0 11:32 trl
10:56  BOFA-PURE   B +1  7611  7631  EXP     -0.7  +9.0  -7.6 16:00 eod
11:22  BOFA-PURE   B -1  7620  7640  LOSS   -14.0  +0.0 -14.3 13:28 stp
13:26  BOFA-PURE   B +1  7606  7626  EXP     +4.0  +6.0  -2.9 16:00 eod
</pre>

<b>With align≥0 filter (recommended):</b> 6t · <b>4W / 0L / 2 flat</b> · <b>+94.9p (~$474 @1MES)</b>
(the −14 loser was the align −1 entry chasing INTO the 7620 magnet)

First 4 all tagged the +GEX magnet 7620; MAE never beyond −4 — clean. Portal-only sim; absolute $ directional, not a forecast."""
r = requests.post(f"https://api.telegram.org/bot{TG}/sendMessage",
                  json={"chat_id": CHAT, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True})
print(r.status_code, r.text[:200])
