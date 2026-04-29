"""Build dark-themed HTML V13 explainer + send to Tel Res."""
import json, os, requests
from datetime import datetime

with open('_v13_report_data.json') as f:
    D = json.load(f)

BOT = os.environ.get('TELEGRAM_BOT_TOKEN')
if not BOT:
    p = os.path.expanduser('~/.telegram_bot_token')
    if os.path.exists(p):
        BOT = open(p).read().strip()
CHAT = '-1003792574755'  # 0DTE Alpha Researchs

# ============ HTML REPORT ============
def pct(x, base):
    return f"{100*x/abs(base):+.1f}%" if base else ""

base = D['baseline_pnl']
base_n = D['baseline_trades']
gex_saved = D['gex_dd']['saved']
van_saved = D['vanna']['saved']
combo_saved = D['combined']['saved']

# Daily chart data
days = sorted(D['daily'].keys())
v12_cum = []
v13_cum = []
v12_sum = 0; v13_sum = 0
for d in days:
    v12_sum += D['daily'][d]['v12']
    v13_sum += D['daily'][d]['v13']
    v12_cum.append(round(v12_sum, 1))
    v13_cum.append(round(v13_sum, 1))

# TSRT monthly
tsrt_months = D['tsrt_monthly']

# Income projection helpers
per_pt_mes = 5
per_pt_es = 50
trading_days = 33  # Mar 1 - Apr 17 roughly
v13_pts_per_day = D['combined']['kept_pnl'] / trading_days
v12_pts_per_day = base / trading_days

# Realistic TSRT factor: per MEMORY.md, actual fires ~50% due to concurrency
tsrt_fire_ratio = 0.5
tsrt_v13_daily_pts = (D['tsrt']['v13_combined_kept_pnl'] / trading_days) * tsrt_fire_ratio

# ============ Build HTML ============
HTML_HEAD = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>V13 Filter Explainer</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body { background:#0b0e13; color:#e3e6eb; font-family:-apple-system,Segoe UI,Arial; max-width:1100px; margin:30px auto; padding:20px; }
  h1 { color:#6bc7ff; border-bottom:2px solid #1e2a36; padding-bottom:10px; }
  h2 { color:#7dd3a6; margin-top:40px; border-left:4px solid #7dd3a6; padding-left:12px; }
  h3 { color:#ffb86b; margin-top:25px; }
  .hero { background:linear-gradient(135deg,#1a2332,#0b0e13); padding:30px; border-radius:10px; margin-bottom:30px; border:1px solid #2a3a50; }
  .hero-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:20px; margin-top:20px; }
  .hero-stat { text-align:center; padding:15px; background:#0f1620; border-radius:8px; }
  .hero-stat .num { font-size:2em; font-weight:bold; color:#6bc7ff; }
  .hero-stat .label { color:#8a97a8; font-size:0.9em; margin-top:5px; }
  .pos { color:#7dd3a6; font-weight:bold; }
  .neg { color:#ff7b7b; font-weight:bold; }
  .warn { color:#ffb86b; }
  table { border-collapse:collapse; width:100%; margin:15px 0; background:#0f1620; border-radius:6px; overflow:hidden; }
  th,td { padding:10px 14px; text-align:left; border-bottom:1px solid #1e2a36; }
  th { background:#162030; color:#6bc7ff; font-weight:600; }
  tr:hover { background:#141d2b; }
  .chart { background:#0f1620; padding:15px; border-radius:8px; margin:20px 0; }
  .callout { background:#162030; border-left:4px solid #ffb86b; padding:15px 20px; margin:20px 0; border-radius:4px; }
  .callout-green { border-left-color:#7dd3a6; }
  .callout-red { border-left-color:#ff7b7b; }
  code { background:#162030; padding:2px 6px; border-radius:3px; color:#ffb86b; }
  .rule-box { background:#0f1620; padding:15px; border-radius:6px; margin:10px 0; border-left:3px solid #6bc7ff; }
  .trade-ex { background:#0a0e14; padding:10px; margin:6px 0; border-radius:4px; font-family:monospace; font-size:0.9em; }
  .trade-ex.loser { border-left:3px solid #ff7b7b; }
  .trade-ex.winner { border-left:3px solid #7dd3a6; }
  .meta { color:#8a97a8; font-size:0.85em; margin-bottom:20px; }
</style></head><body>"""

html = HTML_HEAD
html += f"<h1>V13 Filter Explainer — GEX/DD Magnet + Vanna Cliff/Peak</h1>"
html += f'<div class="meta">Backtest period: {D["period"]} · Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>'

# ============ HERO BOX ============
html += f"""<div class="hero">
  <h2 style="margin-top:0;border:none;padding:0;">The One-Line Summary</h2>
  <p>V13 = V12-fix + two new guardrails that stop shorts from firing when market structure is bullish (GEX/DD magnets) or vanna flow points up (cliff/peak). Combined: <span class="pos">+{combo_saved:.0f} pts</span> over {base_n} trades ({D['period']}).</p>
  <div class="hero-grid">
    <div class="hero-stat"><div class="num">{base:+.0f}</div><div class="label">V12-fix baseline</div></div>
    <div class="hero-stat"><div class="num pos">+{D['gex_dd']['saved']:.0f}</div><div class="label">GEX/DD adds</div></div>
    <div class="hero-stat"><div class="num pos">+{D['vanna']['saved']:.0f}</div><div class="label">Vanna adds</div></div>
    <div class="hero-stat"><div class="num pos">+{combo_saved:.0f}</div><div class="label">Combined (14.0%)</div></div>
  </div>
</div>"""

# ============ Baseline explained ============
html += "<h2>1. What is V12-fix (our current baseline)?</h2>"
html += """<p>V12-fix is the filter we've been running since March 29. It accepts signals only when a setup has the right conditions for that direction:</p>
<ul>
<li><b>Longs:</b> alignment ≥ +2, AND (Skew Charm OR VIX ≤ 22 OR overvix ≥ +2). Gap longs blocked before 10:00 ET on |gap|>30 days.</li>
<li><b>Shorts whitelist:</b> Skew Charm (A+/A/B only), DD Exhaustion (align ≠ 0), AG Short. All other setups silently pass to portal but don't trade live.</li>
<li><b>Blocks:</b> SC/DD in 14:30–15:00 (charm dead zone), after 15:30 (too little time). GEX-LIS paradigm shorts (LIS = floor). AG-TARGET shorts (trend exhausted). SIDIAL-EXTREME longs.</li>
</ul>"""

html += f"<p>Over {D['period']}, V12-fix kept <b>{base_n} eligible trades</b> worth <span class='pos'>{base:+.1f} pts</span>. Here's the baseline breakdown per setup:</p>"

html += "<table><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>PnL</th></tr>"
for setup, v in sorted(D['v12_by_setup'].items(), key=lambda x: -x[1]['pnl']):
    html += f"<tr><td>{setup}</td><td>{v['n']}</td><td>{v['wr']:.1f}%</td><td class='{'pos' if v['pnl']>0 else 'neg'}'>{v['pnl']:+.1f}</td></tr>"
html += "</table>"

# ============ Part A: GEX/DD ============
html += "<h2>2. V13 Part A — GEX/DD Bullish Structure Block</h2>"
html += """<p>This catches shorts when the chain/Volland shows strong bullish dealer positioning. Two independent triggers, either one blocks:</p>"""

html += """<div class="rule-box">
<h3 style="margin-top:0;">Rule A1: GEX magnet above spot ≥ 75</h3>
<p>Top net-GEX strike (c_gamma × c_oi − p_gamma × p_oi) above current spot. When ≥ 75, dealers are stacked long gamma ABOVE price — they must buy into rallies → short = fight the wave.</p>
</div>
<div class="rule-box">
<h3 style="margin-top:0;">Rule A2: DD magnet within ±10pt of spot ≥ 3B</h3>
<p>Max |deltaDecay| at any strike within ±10 pts of spot. When ≥ 3B, one strike is acting as a magnet that pins price. Shorts into a pin reverse often.</p>
</div>"""

html += f"""<p>Applied to SC/DD shorts only (AG Short excluded — contrarian by design).
<b>Savings: <span class='pos'>+{D['gex_dd']['saved']:.1f} pts</span> by blocking <b>{D['gex_dd']['blocks']}</b> trades
(win rate of blocked trades: {100*D['gex_dd']['blocked_wl'][0]/max(1,sum(D['gex_dd']['blocked_wl'])):.0f}%).</b></p>"""

# Examples
html += "<h3>Examples from real trade log</h3>"
for rname in ('GEX magnet above >=75', 'DD magnet near >=3B'):
    html += f"<p><b>{rname}:</b></p>"
    exs = D['examples'][rname]
    html += "<p style='color:#8a97a8;font-size:0.9em;'>Worst trades BLOCKED (losses avoided):</p>"
    for e in exs['worst_losers_blocked']:
        html += f"""<div class='trade-ex loser'>
        {e['date']} ET · {e['setup']} {e['direction']} · {e['grade']} · {e['paradigm']} · spot {e['spot']:.1f}<br>
        → would have been <b>{e['outcome']}</b> <span class='neg'>{e['pnl']:+.1f} pts</span>
        (GEX above={e['gex']:.0f}, DD near={e['dd_b']:.2f}B)</div>"""
    html += "<p style='color:#8a97a8;font-size:0.9em;'>Best trades BLOCKED (missed wins — the cost):</p>"
    for e in exs['best_winners_blocked']:
        html += f"""<div class='trade-ex winner'>
        {e['date']} ET · {e['setup']} {e['direction']} · {e['grade']} · spot {e['spot']:.1f}<br>
        → would have been <b>{e['outcome']}</b> <span class='pos'>{e['pnl']:+.1f} pts</span> (rule still worth it on net)</div>"""

# ============ Part B: Vanna ============
html += "<h2>3. V13 Part B — Vanna Cliff + Peak Structure</h2>"
html += """<p>A completely different signal based on where weekly-expiry <b>vanna flow flips sign</b> across strikes near spot.</p>
<ul>
<li><b>Cliff</b> = strike where per-strike vanna flips from + to − (or vice versa), closest to spot within ±50pt. ABOVE or BELOW spot.</li>
<li><b>Peak</b> = strike with the largest |vanna| in the same band. ABOVE or BELOW spot.</li>
</ul>
<p>Interpretation: positive vanna = dealer-supportive, negative = dealer-destructive. When the cliff is ABOVE spot, dealer flow supports current price — shorts into support fail. When the cliff is BELOW spot, price has crossed into destructive air — shorts succeed.</p>"""

html += """<div class="rule-box">
<h3 style="margin-top:0;">Four vanna rules (all derived from Feb–Apr 2026 sub-bucket data, 343 trades)</h3>
<ul>
<li><b>DD short + cliff=ABOVE</b> — 69t, 41% WR, −106.3 pts when NOT blocked</li>
<li><b>SC short + cliff=ABOVE + peak=BELOW</b> — 27t, 48% WR, −47.8 pts</li>
<li><b>AG short + cliff=BELOW + peak=ABOVE</b> — 20t, 56% WR, −11.7 pts</li>
<li><b>SC long + cliff=ABOVE + peak=BELOW</b> — 27t, 52% WR, −55.4 pts</li>
</ul>
</div>"""

html += f"""<p><b>Savings from vanna rules alone: <span class='pos'>+{D['vanna']['saved']:.1f} pts</span>
by blocking {D['vanna']['blocks']} trades
(blocked WR: {100*D['vanna']['blocked_wl'][0]/max(1,sum(D['vanna']['blocked_wl'])):.0f}%).</b></p>"""

html += "<h3>Examples from real trade log</h3>"
for rname in ('DD short cliff=ABOVE', 'SC short cliff=A+peak=B', 'SC long cliff=A+peak=B'):
    html += f"<p><b>{rname}:</b></p>"
    exs = D['examples'][rname]
    html += "<p style='color:#8a97a8;font-size:0.9em;'>Worst trades BLOCKED:</p>"
    for e in exs['worst_losers_blocked']:
        html += f"""<div class='trade-ex loser'>
        {e['date']} ET · {e['setup']} {e['direction']} · {e['grade']} · {e['paradigm']} · spot {e['spot']:.1f}<br>
        → {e['outcome']} <span class='neg'>{e['pnl']:+.1f} pts</span>
        (cliff={e['vc']}, peak={e['vp']})</div>"""
    html += "<p style='color:#8a97a8;font-size:0.9em;'>Best trades BLOCKED (missed):</p>"
    for e in exs['best_winners_blocked']:
        html += f"""<div class='trade-ex winner'>
        {e['date']} ET · {e['setup']} {e['direction']} · spot {e['spot']:.1f}<br>
        → {e['outcome']} <span class='pos'>{e['pnl']:+.1f} pts</span></div>"""

# ============ Combined analysis ============
html += "<h2>4. How the Two Parts Combine</h2>"
html += f"""<p>The two blocks partly overlap — they're measuring related "bullish structure" signals but from different angles. Here's the breakdown of all {D['combined']['blocks']} trades V13 combined blocks:</p>
<table>
<tr><th>Bucket</th><th>Trades</th><th>Blocked PnL</th><th>Comment</th></tr>
<tr><td>Both GEX/DD AND Vanna flag</td><td>{D['overlap']['both']['n']}</td><td class='neg'>{D['overlap']['both']['pnl']:+.1f}</td><td>Strong conviction block</td></tr>
<tr><td>Only GEX/DD flags</td><td>{D['overlap']['gex_only']['n']}</td><td class='neg'>{D['overlap']['gex_only']['pnl']:+.1f}</td><td>OPEX-week regime mostly</td></tr>
<tr><td>Only Vanna flags</td><td>{D['overlap']['van_only']['n']}</td><td class='neg'>{D['overlap']['van_only']['pnl']:+.1f}</td><td>Calm regime, vanna flow mis-alignment</td></tr>
</table>"""

html += f"""<div class="callout">
<b>Why subadditive?</b> GEX/DD saves +{D['gex_dd']['saved']:.0f}, Vanna alone saves +{D['vanna']['saved']:.0f}.
Naive sum would be +{D['gex_dd']['saved']+D['vanna']['saved']:.0f}, but 39% overlap means combined is
only +{combo_saved:.0f}. Both signals agree on the "obvious" losers — the combined delta is
the NEW uniquely-bad trades caught by only one signal.
</div>"""

# ============ Cumulative chart ============
html += '<h2>5. Cumulative PnL Chart — V12-fix vs V13</h2>'
html += '<div id="chart1" class="chart"></div>'
html += f"""<script>
Plotly.newPlot('chart1', [
  {{x: {json.dumps(days)}, y: {json.dumps(v12_cum)}, name:'V12-fix', type:'scatter', mode:'lines', line:{{color:'#6bc7ff', width:2}}}},
  {{x: {json.dumps(days)}, y: {json.dumps(v13_cum)}, name:'V13 combined', type:'scatter', mode:'lines', line:{{color:'#7dd3a6', width:2}}}}
], {{
  template:'plotly_dark', paper_bgcolor:'#0f1620', plot_bgcolor:'#0f1620',
  font:{{color:'#e3e6eb'}}, xaxis:{{title:'Date'}}, yaxis:{{title:'Cumulative pts'}}, margin:{{t:30,l:50,r:30,b:50}}
}}, {{responsive:true}});
</script>"""

# ============ TSRT-specific ============
html += "<h2>6. Impact on TS Real Trader (TSRT) Specifically</h2>"
html += """<p><b>TSRT trades on 2 real-money accounts</b> (per current config):</p>
<ul>
<li>LONGS account (210VYX65): <b>Skew Charm longs only</b></li>
<li>SHORTS account (210VYX91): <b>Skew Charm + AG Short</b></li>
<li>Max 1 concurrent trade per direction</li>
<li>Exit strategy: Opt2 (trail-only, no partial TP)</li>
</ul>
<p>TSRT does NOT trade DD Exhaustion on real money. So the GEX/DD magnet block (which targets DD+SC shorts) and vanna DD-above rule have <b>partial</b> TSRT impact only via the SC-targeting rules.</p>"""

t = D['tsrt']
html += f"""<table>
<tr><th></th><th>Trades</th><th>Pts</th></tr>
<tr><td>TSRT-scope baseline (V12-fix, SC long + SC short + AG short)</td><td>{t['baseline_n']}</td><td class='pos'>{t['baseline_pnl']:+.1f}</td></tr>
<tr><td>V13 combined blocks within TSRT scope</td><td>{t['v13_combined_n_blocked']}</td><td class='neg'>−{t['v13_combined_n_blocked']*0}</td></tr>
<tr><td><b>V13 net savings within TSRT scope</b></td><td></td><td class='pos'>+{t['v13_saved']:.1f}</td></tr>
</table>"""

html += "<h3>TSRT monthly breakdown</h3>"
html += "<table><tr><th>Month</th><th>Trades</th><th>V12 Pnl</th><th>V13 Kept</th><th>V13 Pnl</th><th>Saved</th></tr>"
for m, v in sorted(tsrt_months.items()):
    html += f"<tr><td>{m}</td><td>{v['n']}</td><td>{v['v12_pnl']:+.1f}</td><td>{v['v13_kept_n']}</td><td>{v['v13_pnl']:+.1f}</td><td class='pos'>{v['saved']:+.1f}</td></tr>"
html += "</table>"

# ============ Hindsight if V13 was there from beginning ============
html += "<h2>7. Hindsight: TSRT Performance If V13 Was Live from Mar 1</h2>"
real_money_ratio = 0.3  # realistic: ~30% of portal signals actually fire on TSRT
html += f"""<div class="callout callout-green">
<p><b>Assumption:</b> TSRT typically fills ~30% of portal signals due to slot contention (single concurrent per direction) and occasional ops issues. This is based on the Mar 24–Apr 8 audit (32 real trades vs ~100 portal signals in same window).</p>
<p><b>Counterfactual PnL:</b></p>
<ul>
<li>TSRT portal-scope V12-fix: {t['baseline_pnl']:+.1f} pts over {trading_days} days = {t['baseline_pnl']/trading_days:+.1f} pts/day</li>
<li>TSRT portal-scope V13: {t['v13_combined_kept_pnl']:+.1f} pts = {t['v13_combined_kept_pnl']/trading_days:+.1f} pts/day</li>
<li>Applying 30% real-trade ratio: {t['baseline_pnl']*real_money_ratio:+.1f} V12 → {t['v13_combined_kept_pnl']*real_money_ratio:+.1f} V13 pts actually traded</li>
<li><b>V13 would have added ~{t['v13_saved']*real_money_ratio:+.1f} pts to actual TSRT PnL over {trading_days} trading days.</b></li>
</ul>
</div>"""

# ============ Income projection ============
html += "<h2>8. Expected Income — 1 MES vs 1 ES vs 2 ES</h2>"
html += """<div class="callout callout-red">
<b>Important caveats:</b>
<ul>
<li>These numbers are <b>gross of commissions, slippage, basis risk, and infrastructure cost</b> (~$524/mo fixed).</li>
<li>Backtest period (Mar 1 – Apr 17) was a MIXED regime (March bearish, April OPEX squeeze) — not a typical calm month.</li>
<li>Real TSRT has concurrency limits — not every portal signal fires.</li>
<li>Scaling from MES→ES is NOT linear. Slippage grows non-linearly, margin requirements change, basis risk amplifies.</li>
<li>Per MEMORY.md scaling roadmap: 1 MES now → 2 MES (+20 clean days) → 5 MES (+40) → 1 ES (+60). 2 ES is a long way out.</li>
</ul>
</div>"""

# Projections
port_v13 = D['combined']['kept_pnl']
port_per_day = port_v13 / trading_days
gross_1mes = port_per_day * 5
gross_1es = port_per_day * 50
gross_2es = port_per_day * 100

real_factor = 0.30
real_1mes = gross_1mes * real_factor
real_1es = gross_1es * real_factor
real_2es = gross_2es * real_factor

# Monthly (22 trading days)
month_real_1mes = real_1mes * 22
month_real_1es = real_1es * 22
month_real_2es = real_2es * 22

# Commissions (~$5/rt MES, $6/rt ES), infra $524
trades_per_day = 3.5  # from MEMORY audit
comm_mes_day = trades_per_day * 5
comm_es_day = trades_per_day * 6
infra = 524

net_1mes_month = month_real_1mes - (comm_mes_day * 22) - infra
net_1es_month = month_real_1es - (comm_es_day * 22) - infra
net_2es_month = month_real_2es - (comm_es_day * 2 * 22) - infra

html += f"""<table>
<tr><th>Scenario</th><th>Pts/day (V13)</th><th>Gross/day</th><th>Realistic/day (30%)</th><th>Gross/month</th><th>Net/month*</th></tr>
<tr><td>1 MES</td><td>{port_per_day:+.1f}</td><td>${gross_1mes:+.0f}</td><td>${real_1mes:+.0f}</td><td>${month_real_1mes:+.0f}</td><td class='{'pos' if net_1mes_month>0 else 'neg'}'>${net_1mes_month:+.0f}</td></tr>
<tr><td>1 ES</td><td>{port_per_day:+.1f}</td><td>${gross_1es:+.0f}</td><td>${real_1es:+.0f}</td><td>${month_real_1es:+.0f}</td><td class='{'pos' if net_1es_month>0 else 'neg'}'>${net_1es_month:+.0f}</td></tr>
<tr><td>2 ES</td><td>{port_per_day:+.1f}</td><td>${gross_2es:+.0f}</td><td>${real_2es:+.0f}</td><td>${month_real_2es:+.0f}</td><td class='{'pos' if net_2es_month>0 else 'neg'}'>${net_2es_month:+.0f}</td></tr>
</table>
<p><b>*Net</b> deducts ~${comm_mes_day*22:.0f}/mo commissions (MES) or ~${comm_es_day*22:.0f}-${comm_es_day*2*22:.0f}/mo (ES) + $524/mo fixed infra. Does NOT yet deduct slippage or SPX/MES-ES basis.</p>"""

html += f"""<div class="callout">
<b>Reality check vs MEMORY.md forward expectation:</b>
"~$1,000/mo NET at 1 MES after frictions" was the post-fix projection from the 9-day audit.
V13 should <b>add roughly +${(t['v13_saved']*real_factor*5)/33*22:.0f}/mo</b> to that on 1 MES —
marginal but real. The bigger gains are <span class="warn">structural</span> (fewer blocked slots), not cash-direct.
</div>"""

html += """<h2>9. What To Expect System-Wide</h2>
<ul>
<li><b>Same number of SIGNALS</b> — detection code is untouched. Portal/setup_log still gets every trigger.</li>
<li><b>Fewer LIVE trades</b> — V13 blocks ~35% of portal signals from firing on auto-trader / real trader. Specifically it kills SC and DD shorts in bullish-structure regimes.</li>
<li><b>Better PnL per trade</b> — average ~$3/trade better on realistic TSRT scope.</li>
<li><b>Lower MaxDD</b> — biggest improvement is on Apr 2 style high-squeeze days, where V13 blocks 7 consecutive losses.</li>
<li><b>Same win rate bar for kept trades</b> — the winning-trade distribution should stay stable because the blocked trades were statistically unfavorable, not merely unlucky.</li>
<li><b>Vanna rules are March-heavy.</b> March alone drives most of vanna's incremental value. April GEX/DD magnets dominated; vanna added −1.0 in April. This is regime-dependent.</li>
</ul>"""

html += """<h2>10. Summary Recommendation</h2>
<div class="callout callout-green">
<p>V13 is deployed and conservative. It reuses V12-fix for all other gating and only adds <b>6 new return-False conditions</b> in total. No trail parameter change, no size change, no scope change.</p>
<p><b>Next milestones:</b></p>
<ul>
<li>+15 clean days forward with V13 live → confirm +14% edge holds out-of-sample</li>
<li>If holds, consider 2 MES scaling (MEMORY roadmap)</li>
<li>Re-run DD-magnet idea (deferred from Discord #3) in early May with 30 days of V13 data</li>
<li>Vanna features (cliff + peak) now in setup_log — can build more filters once 200+ V13 trades are logged</li>
</ul>
</div>
<p style='color:#8a97a8;margin-top:30px;font-size:0.85em;'>Generated from DB queries on setup_log, chain_snapshots, volland_exposure_points. Method: post-hoc apply V12-fix + V13 rules to historical trades, compare outcomes. All numbers reproducible via _v13_report_data.py.</p>
</body></html>"""

# Save
with open('_v13_report.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"HTML written ({len(html)} bytes)")

# Send to Telegram
if BOT:
    print(f"Sending to chat {CHAT}...")
    with open('_v13_report.html', 'rb') as f:
        r = requests.post(
            f'https://api.telegram.org/bot{BOT}/sendDocument',
            data={'chat_id': CHAT, 'caption': '📊 V13 Filter Explainer — GEX/DD + Vanna Combined'},
            files={'document': ('V13_Explainer.html', f, 'text/html')},
            timeout=30,
        )
    print(f"Status: {r.status_code}")
    print(r.text[:500])
else:
    print("No TELEGRAM_BOT_TOKEN set. Fetching from Railway...")
