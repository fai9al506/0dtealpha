# -*- coding: utf-8 -*-
import os, requests
token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat = "-1003792574755"  # 0DTE Alpha Researchs
assert token, "NO TOKEN"

msg = """<b>SKEW CHARM SCALING PLAN — final (S286)</b>

Scale the proven setup, not the account. Basis: 117 sessions, filter <b>V20</b>, chain sim,
−0.6 pt/contract + $1.92/RT charged inside, basket sizing on, cap 2 long / 3 short.
Margin $265/MES. <b>Accounts margin separately — no cross-margin.</b>

<b>THE RUNGS</b>
<pre>
rung  what                       $/mo   worst  best    MaxDD  pk  need eq
R0    today, all 1x             2,171   -484  5,025  -1,703   6   2,271
R1b   SC-short 2x STACKED only  2,712   -184  6,339  -1,428  10   3,786
R1a   SC-short 2x ALL           3,171   +642  7,511  -1,648  12   4,543
R1c   2x slot1, 3x slot2        2,932    -37  6,733  -1,213  12   4,543
R2a   SC-short 3x ALL           4,172 +1,659  9,997  -2,545  18   6,814
</pre>
<b>R1b is the only step that improves money AND risk together</b> — +$541/mo while the
drawdown FALLS from −$1,703 to −$1,428. The extra contracts go only where the edge is:
the 2nd concurrent SC short earns <b>+$24.9</b> and the 3rd <b>+$45.3</b>, against +$11.5
for the first. That premium survives a within-day control (+$19.1 vs +$10.1 on the SAME
days), so it is not a good-day illusion.

<b>R1b HOLDS UP MONTH BY MONTH</b>
<pre>
         R0        R1b      delta
2026-03  +4,915   +6,056   +1,141  HELPS
2026-04  +1,826   +2,272     +446  HELPS
2026-05  +2,048   +2,572     +524  HELPS
2026-06    -484     -184     +300  HELPS
2026-07  +1,158   +1,075      -83  hurts
2026-08  +2,594   +3,282     +688  HELPS
</pre>
5 of 6 months better. Worst DAY also improves: −$697 → −$672.

<b>HOW IT COMBINES WITH BASKET SIZING — multiplicative</b>
<pre>
                     1x    2x    4x
R0  today           251    71     -
R1b                 166   142    14
</pre>
A stacked SC short whose basket CONFIRMS = 2 x 2 = <b>4 MES</b>, on 14 of 322 trades.
That is already inside the peak of 10 MES / $2,650 margin.

<b>GAP RISK</b> — 40-pt gap through the stops, vs $6,016 main capital.
The $300 breaker blocks new entries, it does NOT flatten, so a gap is unbounded by it.
<pre>
R0    6 MES   $1,200   20%
R1b  10 MES   $2,000   33%
R1a  12 MES   $2,400   40%   &lt;- ceiling for this capital
R2a  18 MES   $3,600   60%   BLOCKED
</pre>

<b>CALENDAR</b>
<pre>
now -> Sep     stay R0, collect V20 sessions
~2026-09-01    R1b   needs short acct $3,786 (holds $3,271)
~2026-10-15    R1a   needs short acct $4,543
~2026-11-15    review only - do NOT arm R2a on this capital
</pre>

<b>FOUR GATES — all must pass on the due date or the rung waits</b>
1. Funding: SHORT account only, no cross-margin. Read live from the broker.
2. Evidence: enough live sessions at the current rung.
3. Gap: 40-pt gap under 40% of main capital. Gate on this, NOT MaxDD (non-monotonic, ±$400 noise).
4. Clean: no mismatch, no orphan, no stuck fill (S279), no feed alert.

<b>Dates are targets. Funding is the rule.</b>

<b>WHAT THIS DOES NOT DO</b>
· Does not touch longs — SC long does not improve stacked (+$13.6 then +$11.4).
· Does not raise the cap — 8/8 buys +29% money for +90% drawdown.
· Does not double the whole book — same money as SC-short 3x for 39% more drawdown.

Full detail in <code>SCALING_PLAN.md</code>."""

r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  data={"chat_id": chat, "text": msg, "parse_mode": "HTML",
                        "disable_web_page_preview": True}, timeout=60)
j = r.json()
print("HTTP", r.status_code, "ok=", j.get("ok"), "desc=", j.get("description"), "len=", len(msg))
