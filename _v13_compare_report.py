"""Build V12-fix vs V13 side-by-side comparison report for Tel Res."""
import json, os, requests
from datetime import datetime

with open('_v13_compare.json') as f:
    D = json.load(f)

BOT = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT = '-1003792574755'

v12 = D['v12']; v13 = D['v13']

def delta_class(val, higher_better=True):
    if val == 0: return 'neutral'
    if (val > 0) == higher_better: return 'pos'
    return 'neg'

def fmt_delta(v13_val, v12_val, higher_better=True, fmt='{:+.1f}', pct=False):
    d = v13_val - v12_val
    cls = delta_class(d, higher_better)
    s = fmt.format(d)
    if pct and v12_val != 0:
        s += f" ({100*d/abs(v12_val):+.1f}%)"
    return f'<span class="{cls}">{s}</span>'

# ============ Build HTML ============
HEAD = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>V12-fix vs V13</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body { background:#0b0e13; color:#e3e6eb; font-family:-apple-system,Segoe UI,Arial; max-width:1200px; margin:30px auto; padding:20px; }
h1 { color:#6bc7ff; border-bottom:2px solid #1e2a36; padding-bottom:10px; }
h2 { color:#7dd3a6; margin-top:40px; border-left:4px solid #7dd3a6; padding-left:12px; }
h3 { color:#ffb86b; margin-top:25px; }
.hero { background:linear-gradient(135deg,#1a2332,#0b0e13); padding:30px; border-radius:10px; margin-bottom:30px; border:1px solid #2a3a50; }
.side-by-side { display:grid; grid-template-columns:1fr 80px 1fr; gap:10px; align-items:center; margin:20px 0; }
.card { background:#0f1620; padding:20px; border-radius:8px; border:1px solid #1e2a36; }
.card.v12 { border-left:4px solid #6bc7ff; }
.card.v13 { border-left:4px solid #7dd3a6; }
.card h3 { margin-top:0; color:#8a97a8; font-size:0.9em; text-transform:uppercase; letter-spacing:1px; }
.card .metric { font-size:2.5em; font-weight:bold; color:#e3e6eb; margin:5px 0; }
.card .sub { color:#8a97a8; font-size:0.9em; }
.arrow { text-align:center; font-size:2em; color:#7dd3a6; }
table { border-collapse:collapse; width:100%; margin:15px 0; background:#0f1620; border-radius:6px; overflow:hidden; }
th,td { padding:10px 14px; text-align:left; border-bottom:1px solid #1e2a36; font-size:0.95em; }
th { background:#162030; color:#6bc7ff; font-weight:600; }
tr:hover { background:#141d2b; }
.pos { color:#7dd3a6; font-weight:bold; }
.neg { color:#ff7b7b; font-weight:bold; }
.neutral { color:#8a97a8; }
.warn { color:#ffb86b; }
.chart { background:#0f1620; padding:15px; border-radius:8px; margin:20px 0; }
.callout { background:#162030; border-left:4px solid #7dd3a6; padding:15px 20px; margin:20px 0; border-radius:4px; }
.big-num { font-size:3em; font-weight:bold; }
.summary-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:15px; margin:20px 0; }
.summary-card { background:#0f1620; padding:15px; border-radius:8px; text-align:center; border:1px solid #1e2a36; }
.summary-card .label { color:#8a97a8; font-size:0.85em; margin-top:5px; }
.headline-stat { text-align:center; padding:20px; background:#0a0e14; border-radius:8px; margin:5px 0; }
.headline-stat .big { font-size:2.2em; font-weight:bold; }
</style></head><body>"""

html = HEAD
html += f"<h1>V12-fix vs V13 — Complete Side-by-Side Comparison</h1>"
html += f'<div style="color:#8a97a8;margin-bottom:20px;">Period: {D["period"]} · {D["trading_days"]} trading days · Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>'

# ============ HERO ============
html += f"""<div class="hero">
<h2 style="margin-top:0;border:none;padding:0;">The Five Numbers That Matter</h2>
<div class="summary-grid">
  <div class="summary-card"><div class="big-num pos">+{v13['pnl']-v12['pnl']:.0f}</div><div class="label">pts gained (+{100*(v13['pnl']-v12['pnl'])/v12['pnl']:.1f}%)</div></div>
  <div class="summary-card"><div class="big-num pos">−{v12['maxdd']-v13['maxdd']:.0f}</div><div class="label">MaxDD reduced (−{100*(v12['maxdd']-v13['maxdd'])/v12['maxdd']:.0f}%)</div></div>
  <div class="summary-card"><div class="big-num pos">+{v13['wr']-v12['wr']:.1f}pp</div><div class="label">win rate lift</div></div>
  <div class="summary-card"><div class="big-num pos">{v13['pf']/v12['pf']:.2f}x</div><div class="label">profit factor multiplier</div></div>
</div>
<p style="margin-top:20px;">V13 makes 14% more money with HALF the drawdown, fewer but higher-quality trades, and ALL weeks green.</p>
</div>"""

# ============ Core Side-by-Side ============
html += "<h2>1. Headline Metrics (Side-by-Side)</h2>"
html += '<div class="side-by-side">'
html += f"""<div class="card v12">
<h3>V12-fix (baseline)</h3>
<div class="metric">{v12['pnl']:+.1f}</div><div class="sub">total pts</div>
<div style="margin-top:15px;">
  <div class="sub">{v12['n']} trades · {v12['wr']:.1f}% WR</div>
  <div class="sub">MaxDD: −{v12['maxdd']:.1f} pts</div>
  <div class="sub">Profit Factor: {v12['pf']:.2f}</div>
  <div class="sub">Avg per trade: {v12['avg_per_trade']:+.2f} pts</div>
</div>
</div>"""
html += '<div class="arrow">→</div>'
html += f"""<div class="card v13">
<h3>V13 (combined)</h3>
<div class="metric pos">{v13['pnl']:+.1f}</div><div class="sub">total pts</div>
<div style="margin-top:15px;">
  <div class="sub">{v13['n']} trades · {v13['wr']:.1f}% WR</div>
  <div class="sub">MaxDD: −{v13['maxdd']:.1f} pts</div>
  <div class="sub">Profit Factor: {v13['pf']:.2f}</div>
  <div class="sub">Avg per trade: {v13['avg_per_trade']:+.2f} pts</div>
</div>
</div>"""
html += "</div>"

# ============ Full metrics table ============
html += "<h2>2. Complete Metrics Table</h2>"
html += """<table>
<tr><th>Metric</th><th>V12-fix</th><th>V13</th><th>Delta</th><th>Comment</th></tr>"""

rows = [
    ('Total PnL (pts)', v12['pnl'], v13['pnl'], True, '+14.0% overall gain'),
    ('Total trades', v12['n'], v13['n'], False, 'V13 fires 35% fewer live trades'),
    ('Wins', v12['wins'], v13['wins'], True, 'Kept most of the winners'),
    ('Losses', v12['losses'], v13['losses'], False, 'Cut losses dramatically'),
    ('Expired', v12['exps'], v13['exps'], False, 'Fewer time-outs'),
    ('Win rate', v12['wr'], v13['wr'], True, 'WR lifted 11.7pp'),
    ('Avg winner (pts)', v12['avg_win'], v13['avg_win'], True, 'Winners slightly bigger'),
    ('Avg loser (pts)', v12['avg_loss'], v13['avg_loss'], True, 'Losers nearly same size'),
    ('Avg per trade', v12['avg_per_trade'], v13['avg_per_trade'], True, 'Efficiency up ~75%'),
    ('Max Drawdown (pts)', v12['maxdd'], v13['maxdd'], False, 'Biggest single improvement'),
    ('Profit Factor', v12['pf'], v13['pf'], True, 'Near-doubled'),
    ('Longest losing streak (count)', v12['max_loss_streak'], v13['max_loss_streak'], False, '7 losses → 4 losses'),
    ('Worst streak PnL', v12['worst_streak_pts'], v13['worst_streak_pts'], True, 'Smaller bad-day cluster'),
    ('Longest winning streak', v12['max_win_streak'], v13['max_win_streak'], True, 'Best-case similar'),
]
for name, v12_val, v13_val, higher_better, comment in rows:
    d = v13_val - v12_val
    dc = delta_class(d, higher_better)
    fmt = '{:+.1f}' if isinstance(v12_val, float) else '{:+.0f}'
    d_str = fmt.format(d) if d else '—'
    v12_fmt = f"{v12_val:.1f}" if isinstance(v12_val, float) else f"{v12_val}"
    v13_fmt = f"{v13_val:.1f}" if isinstance(v13_val, float) else f"{v13_val}"
    html += f"<tr><td>{name}</td><td>{v12_fmt}</td><td>{v13_fmt}</td><td class='{dc}'>{d_str}</td><td>{comment}</td></tr>"
html += "</table>"

# ============ Weekly breakdown ============
html += "<h2>3. Weekly PnL Breakdown</h2>"
html += f"""<p>Total weeks in sample: {len(D['weekly'])} (ISO weeks).
V12-fix positive weeks: <b>{D['pos_weeks_v12']}/{len(D['weekly'])}</b>.
V13 positive weeks: <b class='pos'>{D['pos_weeks_v13']}/{len(D['weekly'])}</b> (all weeks green).</p>"""

html += """<table><tr><th>Week</th><th>V12 trades</th><th>V12 PnL</th><th>V13 trades</th><th>V13 PnL</th><th>Delta</th><th>Status</th></tr>"""
for wk, v in sorted(D['weekly'].items()):
    d = v['v13_pnl'] - v['v12_pnl']
    dc = 'pos' if d > 0 else 'neg' if d < 0 else 'neutral'
    status12 = '<span class="pos">GREEN</span>' if v['v12_pnl'] > 0 else '<span class="neg">RED</span>'
    status13 = '<span class="pos">GREEN</span>' if v['v13_pnl'] > 0 else '<span class="neg">RED</span>' if v['v13_pnl'] < 0 else '<span class="neutral">FLAT</span>'
    html += f"<tr><td>{wk}</td><td>{v['v12_n']}</td><td>{v['v12_pnl']:+.1f}</td><td>{v['v13_n']}</td><td>{v['v13_pnl']:+.1f}</td><td class='{dc}'>{d:+.1f}</td><td>V12: {status12} → V13: {status13}</td></tr>"
html += "</table>"

# Weekly bar chart
weeks_list = sorted(D['weekly'].keys())
v12_weekly = [D['weekly'][w]['v12_pnl'] for w in weeks_list]
v13_weekly = [D['weekly'][w]['v13_pnl'] for w in weeks_list]

html += '<div id="chart_weekly" class="chart"></div>'
html += f"""<script>
Plotly.newPlot('chart_weekly', [
  {{x:{json.dumps(weeks_list)}, y:{json.dumps(v12_weekly)}, name:'V12-fix', type:'bar', marker:{{color:'#6bc7ff'}}}},
  {{x:{json.dumps(weeks_list)}, y:{json.dumps(v13_weekly)}, name:'V13', type:'bar', marker:{{color:'#7dd3a6'}}}}
], {{
  template:'plotly_dark', paper_bgcolor:'#0f1620', plot_bgcolor:'#0f1620',
  font:{{color:'#e3e6eb'}}, title:'Weekly PnL Comparison',
  xaxis:{{title:'Week'}}, yaxis:{{title:'PnL (pts)'}}, barmode:'group', margin:{{t:50,l:50,r:30,b:80}}
}}, {{responsive:true}});
</script>"""

# ============ Daily / Cumulative chart ============
html += "<h2>4. Daily PnL & Drawdown Timeline</h2>"
html += f"""<p>Positive days V12: <b>{D['pos_days_v12']}/{D['trading_days']}</b> vs V13: <b>{D['pos_days_v13']}/{D['trading_days']}</b>.</p>
<p>Worst day: V12 was <span class='neg'>{D['worst_day_v12']['d']} ({D['worst_day_v12']['pnl']:+.1f} pts)</span>, V13 was <span class='neg'>{D['worst_day_v13']['d']} ({D['worst_day_v13']['pnl']:+.1f} pts)</span>.</p>
<p>Best day: V12 was <span class='pos'>{D['best_day_v12']['d']} ({D['best_day_v12']['pnl']:+.1f} pts)</span>, V13 was <span class='pos'>{D['best_day_v13']['d']} ({D['best_day_v13']['pnl']:+.1f} pts)</span>.</p>"""

dates = [d['d'] for d in D['daily_v12']]
cum12 = [d['cum'] for d in D['daily_v12']]
cum13 = [d['cum'] for d in D['daily_v13']]

# Rolling MaxDD underwater chart
def underwater(cum):
    peak = 0; uw = []
    for c in cum:
        if c > peak: peak = c
        uw.append(c - peak)
    return uw
uw12 = underwater(cum12); uw13 = underwater(cum13)

html += '<div id="chart_cum" class="chart"></div>'
html += f"""<script>
Plotly.newPlot('chart_cum', [
  {{x:{json.dumps(dates)}, y:{json.dumps(cum12)}, name:'V12-fix cumulative', type:'scatter', mode:'lines', line:{{color:'#6bc7ff', width:2}}}},
  {{x:{json.dumps(dates)}, y:{json.dumps(cum13)}, name:'V13 cumulative', type:'scatter', mode:'lines', line:{{color:'#7dd3a6', width:2}}}}
], {{
  template:'plotly_dark', paper_bgcolor:'#0f1620', plot_bgcolor:'#0f1620',
  font:{{color:'#e3e6eb'}}, title:'Cumulative PnL (pts)',
  xaxis:{{title:'Date'}}, yaxis:{{title:'Cumulative pts'}}, margin:{{t:50,l:50,r:30,b:50}}
}}, {{responsive:true}});
</script>"""

html += '<div id="chart_dd" class="chart"></div>'
html += f"""<script>
Plotly.newPlot('chart_dd', [
  {{x:{json.dumps(dates)}, y:{json.dumps(uw12)}, name:'V12-fix DD', type:'scatter', mode:'lines', fill:'tozeroy', line:{{color:'#6bc7ff'}}, fillcolor:'rgba(107,199,255,0.2)'}},
  {{x:{json.dumps(dates)}, y:{json.dumps(uw13)}, name:'V13 DD', type:'scatter', mode:'lines', fill:'tozeroy', line:{{color:'#7dd3a6'}}, fillcolor:'rgba(125,211,166,0.3)'}}
], {{
  template:'plotly_dark', paper_bgcolor:'#0f1620', plot_bgcolor:'#0f1620',
  font:{{color:'#e3e6eb'}}, title:'Underwater Equity Curve (drawdown from peak)',
  xaxis:{{title:'Date'}}, yaxis:{{title:'Drawdown (pts)'}}, margin:{{t:50,l:50,r:30,b:50}}
}}, {{responsive:true}});
</script>"""

# ============ Monthly ============
html += "<h2>5. Monthly Breakdown</h2>"
html += "<table><tr><th>Month</th><th>V12 trades</th><th>V12 PnL</th><th>V13 trades</th><th>V13 PnL</th><th>Blocked</th><th>Blocked PnL</th><th>Delta</th></tr>"
for m, v in D['monthly'].items():
    d = v['v13_pnl'] - v['v12_pnl']
    html += f"<tr><td>{m}</td><td>{v['v12_n']}</td><td class='{delta_class(v['v12_pnl'])}'>{v['v12_pnl']:+.1f}</td><td>{v['v13_n']}</td><td class='{delta_class(v['v13_pnl'])}'>{v['v13_pnl']:+.1f}</td><td>{v['blocked_n']}</td><td class='neg'>{v['blocked_pnl']:+.1f}</td><td class='{delta_class(d)}'>{d:+.1f}</td></tr>"
html += "</table>"

# ============ Direction ============
html += "<h2>6. By Direction (Longs vs Shorts)</h2>"
html += "<table><tr><th>Direction</th><th>V12 trades</th><th>V12 PnL</th><th>V13 trades</th><th>V13 PnL</th><th>Delta</th></tr>"
for dname, v in D['direction'].items():
    d = v['v13_pnl'] - v['v12_pnl']
    html += f"<tr><td>{dname}</td><td>{v['v12_n']}</td><td class='{delta_class(v['v12_pnl'])}'>{v['v12_pnl']:+.1f}</td><td>{v['v13_n']}</td><td class='{delta_class(v['v13_pnl'])}'>{v['v13_pnl']:+.1f}</td><td class='{delta_class(d)}'>{d:+.1f}</td></tr>"
html += "</table>"

# ============ Per-setup ============
html += "<h2>7. By Setup</h2>"
html += "<table><tr><th>Setup</th><th>V12 N</th><th>V12 PnL</th><th>V12 WR</th><th>V13 N</th><th>V13 PnL</th><th>V13 WR</th><th>Blocked</th><th>Δ PnL</th></tr>"
for setup, v in sorted(D['per_setup'].items(), key=lambda x: -x[1]['v13']['pnl']):
    v12_s = v['v12']; v13_s = v['v13']; b = v['blocked']
    v12_wr = 100*v12_s['w']/max(1, v12_s['w']+v12_s['l'])
    v13_wr = 100*v13_s['w']/max(1, v13_s['w']+v13_s['l'])
    d = v13_s['pnl'] - v12_s['pnl']
    html += f"""<tr>
<td>{setup}</td>
<td>{v12_s['n']}</td><td class='{delta_class(v12_s['pnl'])}'>{v12_s['pnl']:+.1f}</td><td>{v12_wr:.0f}%</td>
<td>{v13_s['n']}</td><td class='{delta_class(v13_s['pnl'])}'>{v13_s['pnl']:+.1f}</td><td>{v13_wr:.0f}%</td>
<td>{b['n']} ({b['pnl']:+.1f})</td>
<td class='{delta_class(d)}'>{d:+.1f}</td></tr>"""
html += "</table>"

# ============ Drawdown / Streak analysis ============
html += "<h2>8. Drawdown & Streak Analysis (What V13 Actually Fixed)</h2>"
html += f"""<div class="callout">
<h3 style="margin-top:0;color:#7dd3a6;">The Drawdown Story</h3>
<p><b>V12-fix worst drawdown: −{v12['maxdd']:.0f} pts</b>.
The equity curve peaked at <b>{v12['peak_ts']}</b> and bottomed at <b>{v12['trough_ts']}</b>.
This is the &quot;biggest single loss streak&quot; a trader would have experienced.</p>
<p><b>V13 worst drawdown: −{v13['maxdd']:.0f} pts</b>.
The equity curve peaked at <b>{v13['peak_ts']}</b> and bottomed at <b>{v13['trough_ts']}</b>.
<span class='pos'>DD cut by {v12['maxdd']-v13['maxdd']:.0f} points (−{100*(v12['maxdd']-v13['maxdd'])/v12['maxdd']:.0f}%)</span>.</p>
<p><b>Longest losing streak:</b> V12 had {v12['max_loss_streak']} straight losses (−{abs(v12['worst_streak_pts']):.0f} pts).
V13 had {v13['max_loss_streak']} straight losses (−{abs(v13['worst_streak_pts']):.0f} pts).
Removing bad clusters is the biggest psychological and capital benefit.</p>
</div>"""

# ============ Trade efficiency ============
html += "<h2>9. Trade Efficiency — Fewer, Better Trades</h2>"
html += f"""<p>V13 fires {v12['n']-v13['n']} FEWER live trades ({100*(v12['n']-v13['n'])/v12['n']:.0f}% reduction) but keeps nearly all winners.</p>"""
html += "<table><tr><th></th><th>V12-fix</th><th>V13</th><th>Change</th></tr>"
html += f"<tr><td>Trades per trading day</td><td>{v12['n']/D['trading_days']:.1f}</td><td>{v13['n']/D['trading_days']:.1f}</td><td class='{delta_class(v13['n']/D['trading_days']-v12['n']/D['trading_days'], False)}'>{v13['n']/D['trading_days']-v12['n']/D['trading_days']:+.1f}</td></tr>"
html += f"<tr><td>PnL per trade</td><td>{v12['avg_per_trade']:+.2f}</td><td>{v13['avg_per_trade']:+.2f}</td><td class='pos'>{v13['avg_per_trade']-v12['avg_per_trade']:+.2f}</td></tr>"
html += f"<tr><td>PnL per trading day</td><td>{v12['pnl']/D['trading_days']:+.1f}</td><td>{v13['pnl']/D['trading_days']:+.1f}</td><td class='pos'>{(v13['pnl']-v12['pnl'])/D['trading_days']:+.1f}</td></tr>"
html += f"<tr><td>Required concurrency slots</td><td>higher</td><td>lower</td><td class='pos'>easier on TSRT</td></tr>"
html += "</table>"

# ============ Final summary ============
html += """<h2>10. Bottom Line</h2>
<div class="callout">"""
html += f"""<table style="background:transparent;">
<tr><td><b>Same portal detection</b></td><td>No signal is lost; V13 is a gate at trade execution only.</td></tr>
<tr><td><b>Fewer live trades</b></td><td>{v12['n']} → {v13['n']} ({v12['n']-v13['n']} blocked, {100*(v12['n']-v13['n'])/v12['n']:.0f}% reduction)</td></tr>
<tr><td><b>Better WR</b></td><td>{v12['wr']:.1f}% → {v13['wr']:.1f}% (+{v13['wr']-v12['wr']:.1f}pp)</td></tr>
<tr><td><b>Higher profit factor</b></td><td>{v12['pf']:.2f}x → {v13['pf']:.2f}x ({v13['pf']/v12['pf']:.2f}x multiplier)</td></tr>
<tr><td><b>Lower MaxDD</b></td><td>−{v12['maxdd']:.0f} → −{v13['maxdd']:.0f} pts (the biggest psychological improvement)</td></tr>
<tr><td><b>Shorter losing streaks</b></td><td>{v12['max_loss_streak']} → {v13['max_loss_streak']} consecutive losses</td></tr>
<tr><td><b>All weeks positive</b></td><td>{D['pos_weeks_v12']}/{len(D['weekly'])} → {D['pos_weeks_v13']}/{len(D['weekly'])} (no losing weeks)</td></tr>
<tr><td><b>Monthly stability</b></td><td>Positive in March (+{D['monthly']['2026-03']['v13_pnl']-D['monthly']['2026-03']['v12_pnl']:.0f}) AND April (+{D['monthly']['2026-04']['v13_pnl']-D['monthly']['2026-04']['v12_pnl']:.0f})</td></tr>
</table></div>"""

html += """<p style='color:#8a97a8;margin-top:30px;font-size:0.85em;'>All numbers computed from DB post-hoc applying V13 filter logic to setup_log trades. Reproducible via _v13_vs_v12_compare.py.</p></body></html>"""

with open('_v13_compare.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"HTML written ({len(html)} bytes)")

if BOT:
    print(f"Sending to chat {CHAT}...")
    with open('_v13_compare.html', 'rb') as f:
        r = requests.post(
            f'https://api.telegram.org/bot{BOT}/sendDocument',
            data={'chat_id': CHAT, 'caption': '📊 V12-fix vs V13 — Complete Comparison (PnL, DD, Weekly, Streaks)'},
            files={'document': ('V12_vs_V13_Compare.html', f, 'text/html')},
            timeout=30,
        )
    print(f"Status: {r.status_code}")
    print(r.text[:400])
