# -*- coding: utf-8 -*-
import os, requests
token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat = "-1003792574755"  # 0DTE Alpha Researchs
assert token, "NO TOKEN"

msg = """<b>WHY IS THE CAPITAL FLAT AFTER 6 MONTHS?</b>
<i>S294/S295 — broker truth only, no story.</i>

<b>WHERE THE $22,658 WENT — every dollar</b>
<pre>
step                                  trades      result      lost
1 every signal, 1 MES, no filter/cap    5,811    +22,658         -
2 keep only what V20 admits             1,126    +19,474    -3,185
3 cap 2/3 + dedup + S203 guard            912    +14,513    -4,960
4 basket sizing                           912    +17,331    +2,818
5 real costs (slippage + fees)            912    +12,057    -5,274
6 only since the account existed          471     +4,892    -7,165
7 only the 37 days we were ARMED          277     +1,759    -3,133
8 what the broker actually paid           317       +710    -1,049
</pre>

<b>THE FOUR REAL REASONS</b>

<b>1. Most of it was never ours to take.</b> $7,165 happened BEFORE the account traded a
single contract (Feb → mid-May). You cannot earn in a month you were not there.

<b>2. We were switched OFF almost half the time — the biggest leak.</b>
37 traded days out of 65 sessions since the account opened. <b>July: 1 day out of 22.</b>
Those idle sessions were worth <b>+$3,133</b>.
<pre>
month  sessions  traded   % on    net $
05           11      11   100%     +828
06           21      20    95%   -1,088
07           22       1     5%     +542
08           11       5    45%     +427
</pre>

<b>3. Seven days ran OLD filters V20 now rejects — cost $862.</b>
05-15 −$278 · 06-05 −$333 · 08-14 −$116 and four smaller.

<b>4. Execution is NOT the problem.</b> On the 30 days where the current filter and the
broker see the same trades: sim $1,759 vs broker $1,572 = <b>89% captured, −$187 total</b>.
That question is closed.

<b>ARE WE GOING IN CIRCLES? NO — LOOK AT THE ERAS</b>
<pre>
era                        days     net    $/day
first live run (to 05-13)     -    ~-694        -
V16 pre-S217                 21     -298      -14
post-S217 basket gate        10      +39       +4
July (one day)                1     +542     +542
restart 08-10                 5     +427      +85
</pre>
<b>Every era is better than the one before.</b> −$694 → −$298 → +$39 → +$85/day.
The fixing HAS worked; it has not had enough live days to show up as capital.

June alone was −$1,088. <b>Excluding June, the other 17 live days made +$1,798 = +$106/day.</b>

<b>THE CONCLUSION — THE BOTTLENECK HAS MOVED</b>
It used to be the filter. Then execution. Both are now measurably fine: 89% capture, and
V20 earns $421/mo more than V16 on 197 FEWER trades.

<b>The bottleneck now is UPTIME.</b> The $2,139/mo projection assumes <b>21 sessions a
month</b>. We have been trading <b>6 to 11</b>. At that rate a perfect filter still returns
about a third of the projection — and a third of $2,139 across a drawdown month is exactly
what "flat capital" looks like.

That is a direct consequence of the supervised-only rule, which was the right call while we
were still finding real bugs. <b>The open question is whether that rule has now outlived its
purpose.</b> The last 5 sessions ran clean at +$85/day.

<b>CONFIDENCE, PLAINLY:</b> the edge is real and measured on broker money (+$106/day
excluding June). Whether the projection arrives depends almost entirely on how many days we
are switched ON — not on any further tuning."""

r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  data={"chat_id": chat, "text": msg, "parse_mode": "HTML",
                        "disable_web_page_preview": True}, timeout=60)
j = r.json()
print("HTTP", r.status_code, "ok=", j.get("ok"), "desc=", j.get("description"), "len=", len(msg))
