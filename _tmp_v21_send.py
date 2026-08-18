# -*- coding: utf-8 -*-
import os, requests
token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat = "-1003792574755"  # 0DTE Alpha Researchs
assert token, "NO TOKEN"

m1 = """<b>V21 IS LIVE — UPDATED PROJECTION</b>

<b>V21 = V20 + no SHORTS when yesterday fell more than 0.8% AND VIX &lt; 24.</b>

<b>PROJECTION</b>
<pre>
                 $        SAR
AVERAGE month  +2,372     8,895
WORST month      +530     1,988
BEST month     +4,906    18,398
</pre>
<b>Every month is positive.</b>

<b>MONTH BY MONTH</b>
<pre>
month      V20       V21     change
2026-03  +4,906    +4,906      same
2026-04  +1,730    +1,857      +127
2026-05  +2,122    +2,122      same
2026-06    -225      +530      +754
2026-07  +1,237    +1,385      +148
2026-08  +2,417    +2,417      same
</pre>
LOMO <b>6/6</b> — helps 3 months, unchanged 3, hurts none.

<b>RISK — the reason V21 exists</b>
<pre>
                        V20       V21
max drawdown         -1,585      -906   (-43%)
worst 5-day window   -1,196      -752
the June 5-12 week   -1,219      -465
</pre>

<b>DISTRIBUTION</b> — best day 8% of total · best 3 days 23% · without the best 3 days
~$1,850/mo · <b>without the best month ~$1,760/mo</b> (the honest floor) · 6 of 6 months green.

<b>TRADE COUNT = RISK</b> — V21 takes <b>861 trades / 7.4 per day</b> against V16's 1,111 / 9.5:
<b>250 fewer exposures for $654/month more.</b>

<b>WHY IT WORKS</b>
After a session that fell &gt;0.5%, the next day averaged <b>+0.22% and rose 68% of the time</b>.
Our fade shorts sold into that bounce: <b>+0.81 pt after a down day vs +4.57 pt otherwise</b>.
The VIX ceiling is what makes it safe — above VIX 26 the effect INVERTS (+15.74 pt, 100% WR)
because the selling continues. Without the ceiling the rule deletes March's +150 pts and is
worth exactly zero.

Blocks <b>27 shorts on 4 days in 6 months</b> at 37% WR (t=-2.01), and beats <b>500 random
27-short removals on every metric</b> (96-100%).

<b>⚠️ THE TRAP THAT ALMOST KILLED IT</b>
Measured on <code>chain_snapshots</code> (2-min samples, first row 09:32) instead of
<code>spx_ohlc_1m</code> (1-min bars, 09:31), the same rule reads 37 trades at 51% WR and
t=-0.81 — pure noise. One missing minute shifts the daily figure ~0.08%, enough to flip whole
days across the -0.8% line. Same rule, different sampling, opposite conclusion.

<b>HONEST LIMITS</b> — 4 days in 6 months, and 2026-06-11 alone is 59% of the value. It does
nothing in 3 of the 6 months. <b>It is insurance, not income.</b>

<b>⚠️ ASSUMED UPTIME: 21 sessions/month.</b> We trade 6-11. At that rate expect a third of this.

Sweep: 0 mismatches, 16 setups, 3,058 signals. <code>LIVE_VER = v21-sb</code>."""

m2 = """<b>SIX MONTHS, FLAT — the story</b>

The month-by-month version of how we got here, with the real numbers:
February watching only · March first order · April 75 trades · May +$828 · <b>June −$1,088</b>
· July one single trading day · August restart at +$85/day.

Plus the full waterfall showing where every dollar of the portal's $22,658 went — and why
the flat line is not a treadmill.

https://claude.ai/code/artifact/3bf35438-8dac-4bdc-85d7-b3e4bc885421"""

for msg in (m1, m2):
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      data={"chat_id": chat, "text": msg, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=60)
    j = r.json()
    print("HTTP", r.status_code, "ok=", j.get("ok"), "desc=", j.get("description"))
