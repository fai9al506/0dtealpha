# -*- coding: utf-8 -*-
import os, requests
token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat = "-1003792574755"  # 0DTE Alpha Researchs
assert token, "NO TOKEN"

msg = """<b>SESSION REPORT — 2026-08-17</b>

<b>TRADING:</b> 5 trades, both accounts flat, <b>+$68.75 gross / +$55.31 net</b>.
2 Skew Charm shorts won (+$60, +$137). Both ES Absorption longs lost (−$65, −$36) —
the setup we parked. 1 SC long −$27 at the flatten.

<b>1. WAR / BREAKING NEWS — refuted, no change</b>
20 war-headline days (2026 US–Iran timeline) vs 96 other sessions: <b>+$99 vs +$111/day,
p=0.89</b>. Blocking them would have <b>cost $1,976</b> and beats only 54% of random blocks.
High VIX is where we EARN (22–26 = +$4.40/trade, 75% WR); our weak zone is the calm 18–20.
March, the war month, was our best. June, the peace month, our worst. The 20-pt stop held
on all 20 days.

<b>2. V20 IS THE LIVE FILTER</b> (from 2026-08-18)
V20 = V16 rules + ES Absorption only at VIX ≥ 20 + no Friday.
<pre>
             $/mo   worst   best   MaxDD  trades
V16 (old)   1,718    -484  +4,666  -2,288  1,111
V20         2,139    -484  +5,025  -1,783    914
</pre>
<b>197 FEWER trades for $421/mo more.</b> Rules change → version number changes; the ledger
is <code>FILTER_VERSIONS.md</code>, the dropdown shows only "V20 (live)".

<b>3. ES ABSORPTION — the edge is VOLATILITY, and volatility left</b>
Per trade: VIX&lt;18 −$2.6 · 18–20 −$6.4 · <b>20–22 +$21.6 · 22–26 +$15.2</b> · 26+ −$1.0.
Mar–Apr (VIX 24.8) +$1,239; May–Aug (VIX 18.1) −$412. Not the Sierra feed switch, not
market-wide (Skew Charm still earns +$18/trade below VIX 18). Gated, not deleted —
<b>switching it OFF measured −$1,036</b> because its slots refill with weaker trades.

<b>4. MONITORING GAP CLOSED</b>
Our weekly check only ever watched the rules that BLOCK trades. <b>Nothing ever asked whether
the setups we ALLOW still earn</b> — that is why ES Absorption died in May and was found in
August. Now ONE monthly report (1st trading day, 17:00 ET): what we TRADE (cap-replayed,
split long/short) + every PORTAL-ONLY detector.

<b>5. SKEW CHARM SCALING — R1d, due 2026-09-01</b>
2 contracts on the 2nd concurrent SC short, 3 on the 3rd (2nd earns +$24.9, 3rd +$45.3 vs
+$11.5 for the first).
<pre>
                    $/mo  worst mo  worst day   MaxDD
today              2,139      -484       -697  -1,783
R1d alone          2,604      -476       -672  -1,880
R1d + breaker      2,642       -52       -374  -1,700
</pre>
<b>R1d alone makes the tail WORSE. Only safe with the day breaker</b> (shipped today).
<b>Fundable now</b> — peak 8 MES = $2,120 vs $3,271 held. The +$1,000 is not needed.

<b>6. JULY EXPLAINED</b>
07-29 FOMC (−1.32%), 07-30 relief rally (+1.7%, MSFT +16%). Trend days; Skew Charm shorted
into them — six full stops in a row on 07-30. Data was clean. Discord for that window:
<i>"Every fade lost."</i>
<b>TESTED AND REJECTED:</b> skipping strong-contradiction trades (fixes July, breaks June)
and skipping FOMC days — <b>we earn +$147/day on FOMC and +$271 the day after vs +$118
normally</b>, so the room's "0DTE fails on FOMC" is refuted on our data.

<b>7. SHIPPED TODAY</b>
· <b>S279</b> a fill the bot never saw (58 min, cost ~$25) — isolate + observe + heal. 14/14 audit.
· <b>S293</b> per-setup day breaker: 2 full stops in a row → that setup+direction pauses for
the day. 16/16 audit, and it caught a real double-count bug before shipping.
· <b>STUDY_CHECKLIST.md</b> — run before ANY backtest. Headed by "model every risk control,
not just the filter", the mistake that started all this.
· Portal trade log: <b>SB column</b> + <b>SB P&L</b> in the summary.

<b>MY MISTAKES TODAY, all caught and corrected:</b>
1. Sizing is <code>max(2)</code>, not multiply — invented 4-contract trades.
2. My replay was missing the S203 underwater guard.
3. The new monitor scored SIGNALS not TRADES → two false alarms on our best setup.
4. Said the Discord export was missing when it was there.

<b>🔴 STILL BLOCKED:</b> v7 account <code>210XFR64</code> does not exist at the broker
("Invalid Account ID") and no $3,000 arrived. Awaiting TS support."""

r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  data={"chat_id": chat, "text": msg, "parse_mode": "HTML",
                        "disable_web_page_preview": True}, timeout=60)
j = r.json()
print("HTTP", r.status_code, "ok=", j.get("ok"), "desc=", j.get("description"), "len=", len(msg))
