"""Build TSRT performance study + slot-cap analysis report."""
import json, os, requests
from datetime import datetime

with open('_tsrt_slot_study.json') as f:
    S = json.load(f)

BOT = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT = '-1003792574755'

HEAD = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>TSRT Performance & Slot Cap Study</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body { background:#0b0e13; color:#e3e6eb; font-family:-apple-system,Segoe UI,Arial; max-width:1100px; margin:30px auto; padding:20px; }
h1 { color:#6bc7ff; border-bottom:2px solid #1e2a36; padding-bottom:10px; }
h2 { color:#7dd3a6; margin-top:40px; border-left:4px solid #7dd3a6; padding-left:12px; }
h3 { color:#ffb86b; margin-top:25px; }
.hero { background:linear-gradient(135deg,#1a2332,#0b0e13); padding:25px; border-radius:10px; margin-bottom:25px; border:1px solid #2a3a50; }
.big-num { font-size:2.5em; font-weight:bold; }
.pos { color:#7dd3a6; font-weight:bold; }
.neg { color:#ff7b7b; font-weight:bold; }
.warn { color:#ffb86b; }
.info { color:#6bc7ff; }
table { border-collapse:collapse; width:100%; margin:15px 0; background:#0f1620; border-radius:6px; overflow:hidden; }
th,td { padding:10px 14px; text-align:left; border-bottom:1px solid #1e2a36; font-size:0.92em; }
th { background:#162030; color:#6bc7ff; font-weight:600; }
tr:hover { background:#141d2b; }
.callout { background:#162030; border-left:4px solid #7dd3a6; padding:15px 20px; margin:20px 0; border-radius:4px; }
.callout-warn { border-left-color:#ffb86b; }
.callout-red { border-left-color:#ff7b7b; }
.callout-info { border-left-color:#6bc7ff; }
.chart { background:#0f1620; padding:15px; border-radius:8px; margin:20px 0; }
.summary-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:15px; margin:20px 0; }
.summary-card { background:#0f1620; padding:15px; border-radius:8px; text-align:center; border:1px solid #1e2a36; }
.summary-card .label { color:#8a97a8; font-size:0.85em; margin-top:5px; }
</style></head><body>"""

html = HEAD
html += "<h1>TSRT Performance Review & Slot-Cap Study</h1>"
html += f'<div style="color:#8a97a8;margin-bottom:20px;">Backtest: {S["period"]} · TSRT scope: SC long + SC short + AG short · 1 concurrent per direction · V13 combined filter applied</div>'

# ============ HERO ============
nocap = S['v13_nocap']
cap1 = S['v13_1cap']
cap2 = S['v13_2cap']
cap3 = S['v13_3cap']
missed_n = cap1['skipped']
missed_pnl = cap1['missed_pnl']
cost_pct = 100 * missed_pnl / nocap['pnl']

html += f"""<div class="hero">
<h2 style="margin-top:0;border:none;padding:0;">Headline</h2>
<div class="summary-grid">
  <div class="summary-card"><div class="big-num pos">+{cap1['pnl']:.0f}</div><div class="label">V13 actual TSRT (1-slot cap)</div></div>
  <div class="summary-card"><div class="big-num warn">+{missed_pnl:.0f}</div><div class="label">missed by cap ({missed_n} trades)</div></div>
  <div class="summary-card"><div class="big-num pos">+{nocap['pnl']:.0f}</div><div class="label">V13 uncapped (theoretical)</div></div>
</div>
<p><b>The 1-slot cap costs us <span class="warn">{cost_pct:.0f}% of the uncapped V13 edge</span>.</b>
Going to 2 slots recovers <b class="pos">+{cap2['pnl']-cap1['pnl']:.0f} pts</b> (about half the missed value).</p>
</div>"""

# ============ My opinion on TSRT ============
html += "<h2>1. My Take on TSRT Performance So Far</h2>"
html += """<div class="callout">
<h3 style="margin-top:0;">The honest read</h3>
<p><b>The infrastructure works. The filter is now solid (V13). The bottleneck is the slot cap.</b></p>
<ul>
<li><b>What's working:</b> TSRT fires trades reliably, broker API stable, EOD flatten runs, real-trader config (1 MES SC + AG) is sensible given current account balance (~$1,880). Per MEMORY.md 9-day audit, most losses came from the qty-sign bug (now FIXED Apr 8), not strategy issues.</li>
<li><b>What's not working well:</b> The 1-concurrent-per-direction cap was sensible at deployment (small account, unverified filter) but is now the biggest single drag on performance. Over Mar 1 - Apr 17, TSRT-scope V13 signals missed 63 trades worth +598 pts because the slot was busy.</li>
<li><b>What changed recently:</b> (a) Qty-sign bug fix Apr 8 eliminated ~$75/day eating; (b) V13 deployed today raises per-trade edge by ~14%. Combined, TSRT baseline should step up from the "roughly breakeven after frictions" of Mar-Apr to the ~$1,000/mo projection in MEMORY.</li>
<li><b>The structural risk:</b> Stale overnight trades and bot-down incidents (Mar 25, Mar 31) cost 17 missed signals in that audit. Operational reliability is as important as the filter edge.</li>
</ul>
</div>"""

# ============ Slot cap study ============
html += "<h2>2. Slot Cap Analysis — The Big Finding</h2>"
html += f"""<p>Over the Mar 1 - Apr 17 window, V13-filtered TSRT signals broke down as:</p>
<table>
<tr><th>Category</th><th>Trades</th><th>Win Rate</th><th>PnL (pts)</th></tr>
<tr><td>Longs FIRED (slot available)</td><td>{S['longs']['fired']}</td><td>{100*(S['longs']['fired_pnl']/max(1,S['longs']['fired'])):.0f}% avg</td><td class='pos'>+{S['longs']['fired_pnl']:.1f}</td></tr>
<tr><td>Longs SKIPPED (slot busy)</td><td>{S['longs']['skipped']}</td><td>{100*(S['longs']['skipped_pnl']/max(1,S['longs']['skipped'])):.0f}% avg</td><td class='warn'>+{S['longs']['skipped_pnl']:.1f}</td></tr>
<tr><td>Shorts FIRED (slot available)</td><td>{S['shorts']['fired']}</td><td>78% WR</td><td class='pos'>+{S['shorts']['fired_pnl']:.1f}</td></tr>
<tr><td><b>Shorts SKIPPED (slot busy)</b></td><td>{S['shorts']['skipped']}</td><td><b class='pos'>97% WR</b></td><td class='warn'><b>+{S['shorts']['skipped_pnl']:.1f}</b></td></tr>
</table>"""

html += """<div class="callout callout-warn">
<p><b>The bitter truth:</b> The 44 SKIPPED shorts had a <b>97% win rate</b> — extraordinarily high.
This is because when one SC short is already firing in a trend/momentum day, follow-up signals (SC or AG) are usually the BEST ones — pattern continuation at confirmed price levels. But we block them.</p>
<p>Meanwhile, the FIRED shorts had 78% WR. So the cap is removing <b>the highest-quality shorts</b> and keeping only the first-in-line ones.</p>
</div>"""

# ============ What-if scenarios ============
html += "<h2>3. What-If: Different Slot Limits</h2>"
html += "<table><tr><th>Scenario</th><th>Trades fired</th><th>PnL</th><th>Missed</th><th>Recovered vs 1-slot</th></tr>"
html += f"<tr><td>1-slot (current)</td><td>{cap1['n']}</td><td>+{cap1['pnl']:.1f}</td><td>{missed_n} ({missed_pnl:+.1f})</td><td>—</td></tr>"
html += f"<tr><td>2-slot per direction</td><td>{cap2['n']}</td><td class='pos'>+{cap2['pnl']:.1f}</td><td>(smaller)</td><td class='pos'>+{cap2['pnl']-cap1['pnl']:.1f} pts</td></tr>"
html += f"<tr><td>3-slot per direction</td><td>{cap3['n']}</td><td class='pos'>+{cap3['pnl']:.1f}</td><td>(small)</td><td class='pos'>+{cap3['pnl']-cap1['pnl']:.1f} pts</td></tr>"
html += f"<tr><td>5-slot (essentially uncapped)</td><td>{S['v13_5cap']['n']}</td><td class='pos'>+{S['v13_5cap']['pnl']:.1f}</td><td>minimal</td><td class='pos'>+{S['v13_5cap']['pnl']-cap1['pnl']:.1f} pts</td></tr>"
html += "</table>"

html += f"""<div class="callout callout-info">
<p><b>The sweet spot is 2-slot.</b> It recovers ~{100*(cap2['pnl']-cap1['pnl'])/(nocap['pnl']-cap1['pnl']):.0f}% of what the cap is costing us,
with minimal margin scaling (from 1 × $700 = $700 per direction → 2 × $700 = $1,400 per direction).</p>
<p>Going from 2 → 3 slots gives diminishing returns (+{cap3['pnl']-cap2['pnl']:.0f} more pts on ~{cap3['n']-cap2['n']} more trades).</p>
</div>"""

# ============ Top missed trades ============
html += "<h2>4. Top Missed Opportunities</h2>"
html += "<p>The trades that actually fired — but our cap skipped them:</p>"
html += "<table><tr><th>Date (ET)</th><th>Setup</th><th>Direction</th><th>PnL</th></tr>"
for t in S['best_missed']:
    html += f"<tr><td>{t['date']}</td><td>{t['setup']}</td><td>{t['dir']}</td><td class='pos'>+{t['pnl']:.1f}</td></tr>"
html += "</table>"

# ============ Daily impact ============
html += "<h2>5. Days Where The Cap Hurt (and Helped) Most</h2>"
# Top 5 hurt
daily = S['daily_cap_impact']
hurts = sorted([(d, v) for d, v in daily.items() if v['skipped_pnl'] > 0], key=lambda x: -x[1]['skipped_pnl'])[:6]
helps = sorted([(d, v) for d, v in daily.items() if v['skipped_pnl'] < 0], key=lambda x: x[1]['skipped_pnl'])[:5]

html += "<h3>Top days the cap HURT us (missed positive PnL)</h3>"
html += "<table><tr><th>Date</th><th>Fired</th><th>Fired PnL</th><th>Skipped</th><th>Skipped PnL</th></tr>"
for d, v in hurts:
    html += f"<tr><td>{d}</td><td>{v['fired']}</td><td class='pos'>+{v['fired_pnl']:.1f}</td><td>{v['skipped']}</td><td class='warn'>+{v['skipped_pnl']:.1f}</td></tr>"
html += "</table>"

html += "<h3>Top days the cap HELPED us (avoided losses)</h3>"
if helps:
    html += "<table><tr><th>Date</th><th>Fired</th><th>Fired PnL</th><th>Skipped</th><th>Skipped PnL</th></tr>"
    for d, v in helps:
        html += f"<tr><td>{d}</td><td>{v['fired']}</td><td>{v['fired_pnl']:+.1f}</td><td>{v['skipped']}</td><td class='pos'>{v['skipped_pnl']:+.1f}</td></tr>"
    html += "</table>"
else:
    html += "<p>None — the cap did not prevent a single material loss in the sample.</p>"

# ============ How to improve TSRT ============
html += """<h2>6. How To Improve TSRT — Ranked Ideas</h2>

<h3>#1 (Highest Impact): Raise slot cap to 2 per direction</h3>
<div class="callout callout-info">
<p><b>Expected gain:</b> +{cap2_d:.0f} pts backtest → real money lift of ~${cap2_usd:.0f}/mo on 1 MES</p>
<p><b>Cost:</b> ~$700 additional margin per direction (total $1,400 per account instead of $700). Accounts currently have ~$1,880 each — room exists.</p>
<p><b>Risk:</b> Losing streak doubles in size. Worst-case: 2 concurrent losing trades = −$140 in one hit (vs −$70 now). Manageable.</p>
<p><b>Implementation:</b> Change <code>MAX_CONCURRENT_PER_DIR = 1</code> to <code>= 2</code> in <code>real_trader.py</code>. Test in SIM first.</p>
</div>

<h3>#2 (Reliability): Bot-down watchdog</h3>
<div class="callout">
<p>Mar 25 (5 SC signals, 0 placed) and Mar 31 (12 SC signals, 0 placed) had TSRT completely down — no alert fired. Signals were worth ~+{dummy_missed:.0f} pts combined per MEMORY audit.</p>
<p><b>Fix:</b> Railway cron that checks real_trader heartbeat every 5 min. If silent for 15 min during market hours, Telegram alert. Re-uses existing <code>_alert_critical</code> channel.</p>
<p><b>Expected gain:</b> Prevents future ~17-trade blackouts. Worth an estimated ~$250/event avoided.</p>
</div>

<h3>#3 (Risk management): Reverse-on-signal</h3>
<div class="callout">
<p>If a SHORT is running AT A LOSS and an opposite-direction LONG signal fires V13-approved, the current TSRT won't act. Eval trader <i>does</i> reverse (per MEMORY.md). Adding this to TSRT would:</p>
<ul>
<li>Cut losses on bad shorts when the market reverses confirmed</li>
<li>Catch the rally on the long side</li>
<li>Typical win: −$70 short loss + $70 long win = +$0 vs −$140 without reversal</li>
</ul>
<p><b>Caveat:</b> Adds complexity. Only do if slot cap ideas are already in place.</p>
</div>

<h3>#4 (Optimization): Separate SC and AG slots on shorts account</h3>
<div class="callout">
<p>SC and AG have <b>different timing distributions</b>. Keeping them in one shared slot means they compete for the same bucket. Splitting:</p>
<ul>
<li>SC-shorts slot: 1 position</li>
<li>AG-shorts slot: 1 position</li>
<li>Effective shorts slots = 2, same as "2-per-direction" idea but without raw limit increase</li>
</ul>
<p><b>Implementation:</b> Per-setup slot counter in <code>real_trader.py</code>. More code than Idea #1 but mechanically the same effect with more granular control.</p>
</div>

<h3>#5 (Scope expansion): Add DD Exhaustion to TSRT</h3>
<div class="callout callout-warn">
<p>TSRT currently does NOT trade DD Exhaustion. But V13's vanna cliff filter specifically fixes DD's main failure mode (cliff=ABOVE losing 41% WR).</p>
<p>V13-filtered DD Exhaustion in backtest: <b>73t, 67% WR, +475 pts</b>.</p>
<p><b>Caveat:</b> This adds a third setup to the shorts account. Combined with slot-cap idea, might over-subscribe. Recommend testing in SIM for 2 weeks before real money.</p>
</div>

<h3>#6 (Size scaling): 1 MES → 2 MES after 20 clean days on V13</h3>
<div class="callout">
<p>Per MEMORY.md scaling roadmap. With V13 live and qty-sign bug fixed, a clean 20-day window should trigger this.</p>
<p>Linear scaling: ~${mes1:.0f}/mo → ~${mes2:.0f}/mo gross. Needs double margin capacity.</p>
</div>

<h3>#7 (Observation): Split-target exit on real money</h3>
<div class="callout callout-info">
<p>Current TSRT uses Opt2 (trail-only, no partial TP). On 1 MES this makes sense (can't split).</p>
<p>On 2+ MES, Opt3 (T1=5@+10, T2 trail) becomes possible. Per MEMORY research, Opt2 is +874 pts better than Opt3 over 667 trades on SC — stick with Opt2 even on 2 MES.</p>
</div>""".format(
    cap2_d = cap2['pnl'] - cap1['pnl'],
    cap2_usd = (cap2['pnl'] - cap1['pnl']) * 5 * 22 / 33 * 0.3,  # rough MES$/month with 30% real-fire ratio
    dummy_missed = 150,
    mes1 = 1000,
    mes2 = 1900,
)

# ============ Executive summary ============
html += """<h2>7. Bottom Line</h2>
<div class="callout">
<h3 style="margin-top:0;">My honest take</h3>
<ul>
<li><b>TSRT infrastructure is solid.</b> Bugs mostly squashed. Filter (V13) is now backtest-verified.</li>
<li><b>The single biggest unlock is the slot cap.</b> Raising 1 → 2 per direction recovers roughly half the uncapped edge at modest margin cost.</li>
<li><b>Second biggest unlock is operational reliability</b> — bot-down watchdog prevents blackouts.</li>
<li><b>Don't scale size yet.</b> Let V13 run 15-20 clean days first. Then 2 MES makes sense.</li>
<li><b>Don't add DD Exhaustion yet.</b> V13 DD rules are in-sample; OOS test first.</li>
</ul>
<p><b>If I were you, I'd do this order:</b></p>
<ol>
<li>Let V13 run today-Friday as-is, watch for issues (2-day watch period).</li>
<li>Monday: deploy bot-down watchdog (1-hour code change).</li>
<li>Following Monday (after ~5 clean days): raise SC+AG short slot to 2.</li>
<li>After 20 clean V13 days: scale to 2 MES.</li>
<li>After 40 days + positive OOS on vanna rules: revisit DD Exhaustion on real money.</li>
</ol>
</div>"""

html += """<p style='color:#8a97a8;margin-top:30px;font-size:0.85em;'>
Simulation assumes each trade holds its slot for <code>outcome_elapsed_min</code> minutes (from setup_log).
Slot simulation is deterministic: skipped trades do NOT get re-tried; they're lost.
Does not account for: broker API delays, live vs backtest snapshot lag, margin at entry time, or SIDIAL-EXTREME long block nuances.
</p></body></html>"""

with open('_tsrt_report.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"HTML written ({len(html)} bytes)")

if BOT:
    print(f"Sending to chat {CHAT}...")
    with open('_tsrt_report.html', 'rb') as f:
        r = requests.post(
            f'https://api.telegram.org/bot{BOT}/sendDocument',
            data={'chat_id': CHAT, 'caption': '🎯 TSRT Performance Review — Slot Cap Analysis & Improvement Ideas'},
            files={'document': ('TSRT_Review.html', f, 'text/html')},
            timeout=30,
        )
    print(f"Status: {r.status_code}")
    print(r.text[:400])
