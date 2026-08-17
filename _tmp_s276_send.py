# -*- coding: utf-8 -*-
import os, requests
token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat = "-1003792574755"  # 0DTE Alpha Researchs
assert token, "NO TOKEN"

msg = """<b>S276 — DO WAR HEADLINES HURT US?  Answer: NO.</b>

Stopping the book when the news feels hot would have <b>cost</b> us money.

<i>Window 2026-03-02 → 08-14, 116 sessions. V16 book, chain sim, real costs charged
(−0.6 pt/trade + $1.92 per contract round-turn). 20 "war days" named from the 2026
US–Iran war timeline (Feb 28 strikes · Hormuz · Mar 27 VIX peak 31 · Apr 13 blockade ·
Jun 17 peace MOU · Jul 7 truce over · Jul 23 oil $100).</i>

<b>1. War days look like every other day</b>
<pre>
                    days    total    per day  green
war-headline days     20   +$1,976     +$99    45%
all other sessions    96  +$10,628    +$111    61%
difference                             -$12
</pre>
Permutation test <b>p = 0.89</b>. There is no effect.
Blocking all 20 costs <b>$1,976</b> and beats only <b>54%</b> of random blocks.
For scale: the Friday gate beat <b>400 of 400</b> random blocks. That is what real looks like.

<b>2. High volatility is when this book EARNS</b>
<pre>
VIX       trades  avg pt   WR
 0-16       112    +2.29   63%
16-18       389    +2.29   58%
18-20       432    +1.60   59%   &lt;- our weak zone
20-22        89    +3.79   71%
22-26       195    +4.40   75%
26+          98    +4.97   64%
</pre>
<b>March</b> = hottest war month (VIX 27, oil +60%) = our <b>best</b>: +$244/day, 77% green.
<b>June</b> = the month peace was signed (VIX 18.9) = our <b>worst</b>: −$97/day, 19% green.
The bad month was the calm one.

<b>3. Hot tape helps shorts, costs longs a little</b>
<pre>
              war days        normal days
SHORT   +6.94 pt / 69% WR   +2.65 pt / 65%
LONG    +1.60 pt / 61% WR   +2.61 pt / 61%
AG Short +9.76 pt / 80% WR  +1.19 pt / 62%
</pre>

<b>4. The stop held every single time</b>
Worst single trade on any war day = <b>−20.0 pt = exactly the stop</b>. Not one gap-through.
Same worst case as a normal day.
Two trades were open when the sharpest 5-minute SPX moves landed. Both lost the stop and
nothing more. We are flat overnight, so a weekend headline cannot reach a position.

<b>5. What about big gaps?</b>
The only bucket that looked bad was a large overnight gap (|gap| ≥ 0.9%, 10 days, −$36/day).
It fails the honesty test: blocking those days <b>helps</b> in June–July but <b>costs $1,545</b>
in March–April. That is the June regime again, not gaps.

<b>MY SUGGESTION — do not add a news stand-down.</b>
1. Keep trading normally when a headline hits. The data says hot tape pays us.
2. The 20-point stop is the protection, and it worked on all 20 war days.
3. If you ever want a hot-day rule, make it <b>trade the shorts, size the longs down</b> —
   not "stop". That is where the difference actually sits.
4. Nothing to change in the code today.

<b>CAVEATS</b> — 20 war days is a directional signal, not proof. Broker-truth covers only
2 of the 20 (Jun 17/18). Our 6 months contain no crash and no limit-down open.

<b>TODAY:</b> the US–Iran ceasefire <b>expires today</b> with no extension and talks called
"static". If it turns hot, this study says trade it as a normal day.
"""

r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  data={"chat_id": chat, "text": msg, "parse_mode": "HTML",
                        "disable_web_page_preview": True}, timeout=60)
j = r.json()
print("HTTP", r.status_code, "ok=", j.get("ok"), "desc=", j.get("description"), "len=", len(msg))
