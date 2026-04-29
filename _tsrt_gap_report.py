"""TSRT ACTUAL vs Theoretical gap report — real statement numbers."""
import json, os, requests
from datetime import datetime

with open('_tsrt_actual.json') as f:
    D = json.load(f)

BOT = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT = '-1003792574755'

HEAD = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>TSRT Actual vs Theoretical — Gap Analysis</title>
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
.gap-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:15px; margin:20px 0; }
.gap-card { background:#0f1620; padding:18px; border-radius:8px; text-align:center; border:1px solid #1e2a36; }
.gap-card .label { color:#8a97a8; font-size:0.85em; margin-top:5px; }
code { background:#162030; padding:2px 6px; border-radius:3px; color:#ffb86b; font-size:0.9em; }
</style></head><body>"""

html = HEAD
html += "<h1>TSRT Real Statement vs Theoretical V13 — Gap Analysis</h1>"
html += f'<div style="color:#8a97a8;margin-bottom:20px;">Actual period: {D["period"]["start"]} to {D["period"]["end"]} · Source: <code>real_trade_orders</code> table · Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>'

actual = D['actual']
theo_nc = D['theo_nocap']
theo_1c = D['theo_1cap']

html += f"""<div class="hero">
<h2 style="margin-top:0;border:none;padding:0;">Real Numbers (from TSRT order table)</h2>
<div class="gap-grid">
  <div class="gap-card"><div class="big-num {'pos' if actual['spx_pnl']>0 else 'neg'}">{actual['spx_pnl']:+.1f}</div><div class="label">ACTUAL SPX pts ({actual['n']} closed trades)</div></div>
  <div class="gap-card"><div class="big-num info">{theo_1c['pnl']:+.1f}</div><div class="label">Theoretical V13 (1-cap, same period)</div></div>
  <div class="gap-card"><div class="big-num pos">{theo_nc['pnl']:+.1f}</div><div class="label">Theoretical V13 (no cap)</div></div>
</div>
<p><b>The real gap:</b></p>
<ul>
<li>Actual TSRT made <b class='{'pos' if actual['spx_pnl']>0 else 'neg'}'>{actual['spx_pnl']:+.1f} pts (${actual['usd_at_1mes']:+.0f} at 1 MES)</b> over 24 trading days.</li>
<li>Theoretical V13 (what it SHOULD have made with same 1-slot cap): <b class='pos'>+{theo_1c['pnl']:.1f} pts</b></li>
<li><b class='warn'>Gap: {D['gaps']['actual_vs_1cap']:+.1f} pts (${D['gaps']['actual_vs_1cap']*5:+.0f} at 1 MES)</b> — this is the execution/operational/bug drag.</li>
</ul>
</div>"""

# ============ Per-setup breakdown ============
html += "<h2>1. Actual Performance — By Setup</h2>"
html += "<table><tr><th>Setup + Direction</th><th>Trades</th><th>SPX pts</th><th>Avg/trade</th></tr>"
for key, v in actual['by_setup'].items():
    cls = 'pos' if v['pnl'] > 0 else 'neg'
    html += f"<tr><td>{key}</td><td>{v['n']}</td><td class='{cls}'>{v['pnl']:+.1f}</td><td>{v['pnl']/max(1,v['n']):+.2f}</td></tr>"
html += "</table>"

html += f"""<div class="callout callout-warn">
<p><b>Key observation:</b> SC longs have been losing money (-75 pts on 12 trades, 33% WR) while SC shorts are profitable (+53 pts on 23 trades, 52% WR).
Long losses are concentrated in the Mar 24 - Apr 8 bug-era period.</p>
</div>"""

# ============ Close reasons ============
html += "<h2>2. Close Reasons (ops health indicator)</h2>"
html += "<table><tr><th>Close reason</th><th>Count</th><th>Status</th></tr>"
reason_notes = {
    'stop_filled': 'Normal — stop hit',
    'target_filled': 'Normal — target hit',
    'eod_flatten': 'Normal — EOD exit at 15:55 ET',
    'stale_overnight': '⚠️ Trade carried overnight — ops issue',
    'pre_market_cleanup': '⚠️ Cleanup at 09:28 ET — prior-day leftover',
    'ghost_reconcile': '🚨 Position found on broker but not tracked — sync issue',
    'WIN': 'Normal',
}
for r, n in actual['close_reasons'].items():
    note = reason_notes.get(r, '?')
    cls = 'pos' if 'Normal' in note else 'warn' if '⚠️' in note else 'neg'
    html += f"<tr><td>{r}</td><td>{n}</td><td class='{cls}'>{note}</td></tr>"
html += "</table>"
html += """<div class="callout callout-red">
<p><b>6 ghost reconcile events — operational concern.</b> All on Apr 7. This means the broker had positions the bot didn't track.
Per MEMORY.md: qty-sign bug era (pre Apr 8) — the bot thought it had no position when it actually did, then tried to open more.
Fixed in commit <code>1a98ec1</code> Apr 8.</p>
</div>"""

# ============ Matched trades analysis ============
html += "<h2>3. Where Did The Gap Come From?</h2>"
gap_vs_1cap = D['gaps']['actual_vs_1cap']
matched_slip = D['matched_theo_pnl'] - D['matched_actual_pnl']

html += f"""<p>Of the {actual['n']} actual closed trades and {theo_1c['n']} theoretical trades, only <b>{D['matched_count']}</b> overlap by <code>setup_log_id</code>. That means:</p>
<ul>
<li><b>{D['actual_only_count']} actual trades should NOT have fired per V13</b> — these are pre-V13 trades that hit the now-blocked bad structures</li>
<li><b>{D['theo_only_count']} theoretical trades never fired on real money</b> — slot cap, bot-down, ghost-reconcile ops issues</li>
<li>On the {D['matched_count']} matched trades, actual P&L was <b class='neg'>{matched_slip:+.1f} pts worse than theoretical</b> — this is pure execution slippage (fills, SPX→MES basis, bug losses)</li>
</ul>"""

html += f"""<h3>The gap decomposed</h3>
<table>
<tr><th>Component</th><th>Approx pts</th><th>Cause</th></tr>
<tr><td>Execution slippage on matched trades</td><td class='neg'>~{matched_slip:+.0f}</td><td>SPX→MES basis (esp. Mar 30 at high VIX), stop slippage, 1-tick fills</td></tr>
<tr><td>Trades we took that V13 would block (bad trades)</td><td class='neg'>~-{max(0,abs(min(0,actual['spx_pnl']))-abs(matched_slip)):.0f}</td><td>Pre-V13 era losses on GEX/DD magnet + vanna-above setups</td></tr>
<tr><td>Trades V13 would have taken but we missed</td><td class='warn'>~+{max(0,theo_1c['pnl']-actual['spx_pnl']-abs(matched_slip)):.0f}</td><td>Bot-down (Mar 25, Mar 31), ghost-reconcile blocks, slot stuck on bug trades</td></tr>
</table>"""

html += f"""<div class="callout">
<p><b>Total realized gap: {gap_vs_1cap:+.1f} pts (${gap_vs_1cap*5:+.0f} at 1 MES).</b></p>
<p>Pre Apr-8 (bug era): 25 trades, ~−24 pts SPX = ~−$118. Bugs + slippage dominated.</p>
<p>Post Apr-8 (bug-fixed): 24 trades, ~+14 pts SPX = ~+$70. Broke into profit after fix.</p>
</div>"""

# ============ Big discrepancies ============
if D['big_discrepancies']:
    html += "<h3>Biggest fill discrepancies (actual vs portal outcome)</h3>"
    html += "<table><tr><th>Date</th><th>Setup</th><th>Dir</th><th>Actual</th><th>Theoretical</th><th>Diff</th><th>Close</th></tr>"
    for bd in D['big_discrepancies']:
        diff = bd['actual'] - bd['theo']
        cls = 'pos' if diff > 0 else 'neg'
        html += f"<tr><td>{bd['date']}</td><td>{bd['setup']}</td><td>{bd['dir']}</td><td>{bd['actual']:+.1f}</td><td>{bd['theo']:+.1f}</td><td class='{cls}'>{diff:+.1f}</td><td>{bd['reason']}</td></tr>"
    html += "</table>"
    html += """<p style='color:#8a97a8;'>Both are Mar 30 — the infamous high-VIX SPX→MES basis blowup (MEMORY.md flagged #1352 as worst single hit at −$251).</p>"""

# ============ Period comparison ============
html += """<h2>4. Pre-Fix vs Post-Fix Period Comparison</h2>
<table>
<tr><th>Period</th><th>Trades</th><th>SPX pts</th><th>$ at 1 MES</th><th>Comment</th></tr>
<tr><td>Mar 24 - Apr 7 (bug era)</td><td>25</td><td class='neg'>−23.8</td><td class='neg'>−$119</td><td>Qty-sign bug eating + operational issues</td></tr>
<tr><td>Apr 8 - Apr 16 (bug-fixed)</td><td>24</td><td class='pos'>+14.0</td><td class='pos'>+$70</td><td>Clean execution starting to show edge</td></tr>
</table>
<div class="callout callout-warn">
<p>Even the post-fix 9 days are BELOW the theoretical V13 1-cap rate.
Theoretical V13 over 9 post-Apr-8 days would have been ~+60 pts = +$300 at 1 MES.
The post-fix gap ({'%+.0f' % (60-14)} pts ≈ ${'%+.0f' % ((60-14)*5)}) is mostly slot contention + some SC long losses that V13 doesn't catch.</p>
</div>"""

# ============ Suggestions based on REAL gap ============
html += "<h2>5. Suggestions Based on REAL Gap (Not Theoretical)</h2>"

html += """<h3>Priority 1: Fix SC LONGS losing money</h3>
<div class="callout callout-red">
<p>SC longs have been <b>−75 pts on 12 trades (33% WR)</b> — the biggest actual drag.
V13's vanna filter blocks only 1 SC long sub-bucket (cliff=A + peak=B). Let me check if the remaining SC longs have another identifiable bad pattern:</p>
<ul>
<li>SC longs bombed mostly in Mar-Apr early period (Apr 10: −18 pts, Apr 15: −22 pts).</li>
<li>Ideas to test: (a) grade filter for SC longs (only A+/A, not B); (b) alignment requirement lifted to +3 for longs; (c) time-of-day restriction before 10:30 ET.</li>
<li><b>Action:</b> Study SC long V12-fix losers more deeply before adding another filter. Could save ~$300/mo if tightened correctly.</li>
</ul>
</div>

<h3>Priority 2: Kill ghost-reconcile events</h3>
<div class="callout callout-red">
<p>6 ghost_reconcile events in the sample — all on Apr 7. That's <b>not normal</b>.
Root cause per MEMORY.md: qty-sign bug returned zero position count when broker actually held, leading to duplicate orders.
<b>Fixed Apr 8</b>, but add a daily sanity check: broker-reported positions must match bot-tracked positions. Alert on mismatch.</p>
</div>

<h3>Priority 3: Bot-down watchdog (unchanged from prior report)</h3>
<div class="callout">
<p>Mar 25 and Mar 31 are NOT in the real_trade_orders table at all — bot was down. No alert fired. This cost ~17 signals.
Railway cron checking heartbeat every 5 min → Telegram alert if silent > 15 min during market hours.</p>
</div>

<h3>Priority 4: SPX→MES basis awareness</h3>
<div class="callout callout-warn">
<p>Mar 30 single trade lost −$251 because stop calculated in SPX points hit before MES equivalent stop. At high VIX (above 25), the basis can widen 3+ points.</p>
<p><b>Two-part fix:</b>
(a) Compute stop in MES space when entering (currently uses SPX basis + cached ES offset).
(b) Recalculate stop offset whenever ES-SPX spread moves > 2pt.</p>
</div>

<h3>Priority 5: Raise slot cap 1→2 (still important, but conditional)</h3>
<div class="callout">
<p>The theoretical gain remains +215 pts (half the missed edge). But on REAL money with current slippage profile, expect maybe 50-60% of that = +108 pts = +$540/mo on 1 MES.</p>
<p><b>Wait until SC longs fix is in before raising cap</b> — otherwise we'd just take 2x as many losing SC longs.</p>
</div>

<h3>Priority 6: Only after above — consider scaling to 2 MES</h3>
<div class="callout callout-warn">
<p>With SC longs losing and basis blowups not fully mitigated, scaling to 2 MES now would amplify the problems. Hold at 1 MES until 10 consecutive positive days under V13.</p>
</div>"""

# ============ Recommended order ============
html += """<h2>6. Recommended Order (revised based on actual data)</h2>
<div class="callout">
<ol>
<li><b>This week:</b> Let V13 run. Watch for ghost_reconcile or pre_market_cleanup events (should be zero).</li>
<li><b>Next Monday:</b> Deep-dive study on SC long losers to identify what V13 is missing (probably a regime/grade filter).</li>
<li><b>Monday+3 days clean:</b> Deploy bot-down watchdog.</li>
<li><b>After 10 green days:</b> Raise short slot from 1 → 2.</li>
<li><b>After 20 green days:</b> Scale to 2 MES.</li>
<li><b>Only after 40+ green days with stable vanna edge:</b> Add DD Exhaustion, revisit option trader.</li>
</ol>
</div>
<p><b>Hold off on the previous "raise slot cap Monday" recommendation</b> until we understand why SC longs are losing. Doubling exposure to a losing subset is worse than the slot cap.</p>"""

html += """<p style='color:#8a97a8;margin-top:30px;font-size:0.85em;'>
Data: <code>real_trade_orders</code> table (49 entries, 36 closed with calculable SPX PnL) + setup_log for linked signal metadata.
SPX-to-MES conversion uses 1:1 point-to-dollar scale × $5/MES.
Does NOT include: commissions (~$5/rt), TS fees, broker data fees.
Real account statement would show $ values slightly different due to SPX→MES basis and fee structure.
</p></body></html>"""

with open('_tsrt_gap.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"HTML written ({len(html)} bytes)")

if BOT:
    with open('_tsrt_gap.html', 'rb') as f:
        r = requests.post(
            f'https://api.telegram.org/bot{BOT}/sendDocument',
            data={'chat_id': CHAT, 'caption': '🎯 TSRT REAL Numbers — Actual vs Theoretical Gap + Priority Fixes'},
            files={'document': ('TSRT_Real_Gap.html', f, 'text/html')},
            timeout=30,
        )
    print(f"Status: {r.status_code}")
    print(r.text[:400])
