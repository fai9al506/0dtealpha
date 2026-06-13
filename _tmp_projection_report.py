"""Build + send updated 0DTE Alpha projection HTML report to Tel Res.

After 2026-05-19 ship (V13.2 vanna refinement + margin pre-check removal +
atomic bracket + V16 portal mirror + S149 code shipped default OFF).
"""
import os, sys, psycopg2, tempfile, requests, html
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from statistics import mean, median, stdev
sys.stdout.reconfigure(encoding='utf-8')

TOKEN = "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
CHAT_TEL_RES = "-1003792574755"
ET = ZoneInfo("America/New_York")

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Pull May 1-19 setup_log
cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm, greek_alignment,
           vix, overvix, vanna_cliff_side, vanna_peak_side,
           v13_gex_above, v13_dd_near, vanna_regime,
           outcome_result, outcome_pnl, ts
    FROM setup_log
    WHERE ts >= '2026-05-01' AND ts < '2026-05-20'
      AND outcome_result IS NOT NULL AND outcome_result != 'EXPIRED'
    ORDER BY id
""")
rows = cur.fetchall()

V16_ALLOWED = {'Skew Charm', 'AG Short', 'Vanna Pivot Bounce', 'ES Absorption', 'DD Exhaustion'}

def is_long(d):
    return d in ('long', 'bullish')

def passes_v16(r):
    (lid, sn, dir_, grade, para, align, vix, overvix,
     cliff, peak, gex_above, dd_near, vanna_reg, res, pnl, ts) = r
    align = align or 0
    vix = vix or 0
    if sn not in V16_ALLOWED: return False
    if sn == 'DD Exhaustion' and is_long(dir_):
        if para == 'SIDIAL-EXTREME': return False
        if align < 0: return False
        if align >= 3: return False
        if vix >= 22: return False
        if para in ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return False
        if grade == 'C': return False
        return True
    if sn == 'Skew Charm' and grade in ('C', 'LOG'): return False
    if sn == 'ES Absorption':
        if grade not in ('A', 'A+'): return False
        if para in ('AG-TARGET', 'AG-LIS'): return False
        et = ts.astimezone(ET)
        if not is_long(dir_) and et.hour >= 14: return False
        if is_long(dir_) and align < 0: return False
        if not is_long(dir_) and align > 0: return False
        return True
    if sn == 'Vanna Pivot Bounce':
        if not is_long(dir_): return False
        if vanna_reg != 'bullish': return False
        return True
    if cliff and not is_long(dir_):
        if sn == 'DD Exhaustion' and cliff == 'A' and peak == 'B': return False
        if sn == 'Skew Charm' and cliff == 'A' and peak == 'B': return False
        if sn == 'AG Short' and cliff == 'B' and peak == 'A': return False
    if not is_long(dir_) and sn in ('Skew Charm', 'DD Exhaustion'):
        if (gex_above or 0) >= 75: return False
        if (dd_near or 0) >= 3_000_000_000: return False
    if sn in ('Skew Charm','DD Exhaustion') and not is_long(dir_) and para == 'GEX-LIS':
        return False
    if sn == 'AG Short':
        if para == 'AG-TARGET': return False
        et = ts.astimezone(ET)
        if et.weekday() == 4 and 15 <= et.day <= 21: return False
    if sn == 'Skew Charm' and is_long(dir_):
        if para == 'SIDIAL-EXTREME': return False
        if align == 3 and para in ('GEX-LIS','AG-LIS','AG-PURE','BOFA-MESSY'): return False
        if para == 'GEX-LIS': return False
        et = ts.astimezone(ET)
        if et.weekday() == 4 and 15 <= et.day <= 21: return False
        return True
    if sn == 'Skew Charm' and not is_long(dir_): return True
    if sn == 'AG Short' and not is_long(dir_): return True
    if sn == 'DD Exhaustion' and not is_long(dir_): return False
    return False

daily = defaultdict(lambda: {'pnl': 0.0, 'n': 0, 'w': 0, 'l': 0})
for r in rows:
    if not passes_v16(r): continue
    pnl = float(r[14])
    date_et = r[15].astimezone(ET).date()
    daily[date_et]['pnl'] += pnl
    daily[date_et]['n'] += 1
    daily[date_et]['w' if pnl > 0 else 'l'] += 1

days = sorted(daily.keys())
pnls = [daily[d]['pnl'] for d in days]
counts = [daily[d]['n'] for d in days]
wrs = [daily[d]['w']/daily[d]['n']*100 for d in days]
mean_d = mean(pnls); med_d = median(pnls); std_d = stdev(pnls) if len(pnls)>1 else 0
total_may = sum(pnls)

# Real broker today (DB)
cur.execute("""
    SELECT setup_log_id, state
    FROM real_trade_orders
    WHERE created_at >= '2026-05-19' AND created_at < '2026-05-20'
    ORDER BY setup_log_id
""")
import json as _json
real_today_pnl = 0.0
real_today_n = 0
real_today_w = 0
real_today_l = 0
for r in cur.fetchall():
    st = r[1] if isinstance(r[1], dict) else _json.loads(r[1])
    cf = st.get('close_fill_price'); ef = st.get('entry_fill_price') or st.get('fill_price')
    if cf and ef:
        d = st.get('direction','').lower()
        qty = st.get('quantity', 1)
        if d in ('long','buy','bullish'):
            p = (float(cf) - float(ef)) * 5 * qty
        else:
            p = (float(ef) - float(cf)) * 5 * qty
        real_today_pnl += p
        real_today_n += 1
        if p > 0: real_today_w += 1
        else: real_today_l += 1

cur.close(); conn.close()

# === BUILD HTML ===
today_str = datetime.now(ET).strftime("%Y-%m-%d")
TRADING_DAYS = 21  # avg per month

# Capture-rate scenarios
captures = [
    ("Pre-fix May baseline", 0.38, "Real broker May −$858 vs portal +$2,194 (sum across all setups)"),
    ("Conservative target", 0.55, "Post-V16.1+S131 mid-band assumption"),
    ("Realistic target", 0.65, "S55 study midpoint after bug fixes apply"),
    ("Optimistic ceiling", 0.80, "If atomic bracket + S131 + V13.2 all compound favorably"),
]

# Per-day stats with $ scenarios
daily_rows = []
for d in days:
    x = daily[d]
    wr = x['w']/x['n']*100
    daily_rows.append((d, x['n'], x['w'], x['l'], wr, x['pnl']))

def dollar(pts, mult, capture=1.0):
    return pts * mult * capture

# Monthly projection at various captures
proj_rows = []
for label, cap, note in captures:
    monthly_pts = mean_d * TRADING_DAYS * cap
    proj_rows.append({
        'label': label, 'cap': cap, 'note': note,
        'monthly_pts': monthly_pts,
        'monthly_mes': monthly_pts * 5,
        'monthly_es': monthly_pts * 50,
    })

html_out = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>0DTE Alpha — Projection 2026-05-19</title>
<style>
body {{ background:#0d1117; color:#e6edf3; font-family:'Plus Jakarta Sans',-apple-system,Segoe UI,sans-serif;
  margin:0; padding:24px; max-width:1100px; margin:0 auto; line-height:1.55; }}
h1 {{ color:#7ee787; font-size:26px; border-bottom:2px solid #30363d; padding-bottom:12px; margin-top:0; }}
h2 {{ color:#79c0ff; font-size:19px; margin-top:32px; border-bottom:1px solid #21262d; padding-bottom:6px; }}
h3 {{ color:#d2a8ff; font-size:15px; margin-top:24px; }}
.kpi {{ display:flex; gap:14px; flex-wrap:wrap; margin:14px 0; }}
.card {{ background:#161b22; border:1px solid #30363d; border-radius:10px; padding:14px 18px; flex:1 1 200px; }}
.card .label {{ font-size:11px; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; }}
.card .value {{ font-size:22px; font-weight:600; margin-top:4px; }}
.card .sub {{ font-size:12px; color:#8b949e; margin-top:2px; }}
.green {{ color:#7ee787; }} .red {{ color:#ff7b72; }} .yellow {{ color:#d29922; }} .blue {{ color:#79c0ff; }}
table {{ width:100%; border-collapse:collapse; margin:14px 0; font-size:13px; font-family:'JetBrains Mono',ui-monospace,monospace; }}
th {{ background:#1f2937; color:#79c0ff; padding:9px 12px; text-align:left; border-bottom:2px solid #30363d; font-weight:600; }}
td {{ padding:7px 12px; border-bottom:1px solid #21262d; }}
tr:nth-child(even) td {{ background:#0e131a; }}
.bar {{ display:inline-block; height:14px; background:linear-gradient(90deg,#1f6feb,#7ee787); vertical-align:middle; border-radius:2px; }}
.note {{ background:#1f2937; border-left:3px solid #79c0ff; padding:10px 14px; margin:14px 0; border-radius:0 6px 6px 0; font-size:13px; }}
.warn {{ background:#332701; border-left:3px solid #d29922; padding:10px 14px; margin:14px 0; border-radius:0 6px 6px 0; font-size:13px; }}
.danger {{ background:#3d1414; border-left:3px solid #ff7b72; padding:10px 14px; margin:14px 0; border-radius:0 6px 6px 0; font-size:13px; }}
.subtle {{ color:#8b949e; font-size:12px; }}
hr {{ border:none; border-top:1px solid #30363d; margin:24px 0; }}
</style></head><body>
<h1>0DTE Alpha — Updated Projection</h1>
<div class="subtle">Generated {ts} ET · post-ship of V13.2 vanna + margin removal + atomic bracket + V16 portal mirror + S149 (env OFF)</div>

<h2>Today {today} — V16</h2>
<div class="kpi">
  <div class="card"><div class="label">V16 portal (sim)</div><div class="value blue">+{today_pts:.1f} pt</div><div class="sub">${today_mes:+.0f} MES · ${today_es:+.0f} ES</div></div>
  <div class="card"><div class="label">Real broker (TSRT)</div><div class="value green">${real_today:+.0f}</div><div class="sub">{real_n}t, {real_w}W/{real_l}L · {real_wr:.0f}% WR</div></div>
  <div class="card"><div class="label">Today rank in May</div><div class="value yellow">{pct:.0f}th pct</div><div class="sub">of {n_days} trading days w/ V16 fires</div></div>
</div>
<div class="note"><b>Real beat portal sim by ${beat:+.0f}</b> on today's 7 closed trades. Mostly S131 SPX-exit firing on 3 trades (2980/2989/2996) where MES basis was favorable at exit.</div>

<h2>May 2026 (May 1-19) — V16 Daily Distribution</h2>
<div class="kpi">
  <div class="card"><div class="label">Trading days w/ V16</div><div class="value">{n_days}</div></div>
  <div class="card"><div class="label">Mean daily pts</div><div class="value blue">+{mean_d:.1f}</div><div class="sub">${mean_mes:+.0f} MES · ${mean_es:+.0f} ES</div></div>
  <div class="card"><div class="label">Median daily pts</div><div class="value blue">+{med_d:.1f}</div><div class="sub">${med_mes:+.0f} MES · ${med_es:+.0f} ES</div></div>
  <div class="card"><div class="label">StdDev (variance)</div><div class="value yellow">±{std_d:.1f} pt</div><div class="sub">Wide swings: plan ±{std_mes:.0f}/day MES</div></div>
  <div class="card"><div class="label">Worst day</div><div class="value red">{wd:+.1f}</div></div>
  <div class="card"><div class="label">Best day</div><div class="value green">{bd:+.1f}</div></div>
</div>

<h2>Daily breakdown</h2>
<table>
<tr><th>Date</th><th>Trades</th><th>WR</th><th>Portal pts</th><th>$ at 1 MES (sim)</th><th>$ at 1 ES (sim)</th></tr>
""".format(
    ts=datetime.now(ET).strftime("%Y-%m-%d %H:%M"),
    today=today_str,
    today_pts=daily[max(days)]['pnl'],
    today_mes=daily[max(days)]['pnl']*5,
    today_es=daily[max(days)]['pnl']*50,
    real_today=real_today_pnl,
    real_n=real_today_n, real_w=real_today_w, real_l=real_today_l,
    real_wr=(real_today_w/real_today_n*100) if real_today_n else 0,
    pct=sum(1 for p in pnls if p <= daily[max(days)]['pnl'])/len(pnls)*100,
    n_days=len(days),
    beat=real_today_pnl - daily[max(days)]['pnl']*5,
    mean_d=mean_d, mean_mes=mean_d*5, mean_es=mean_d*50,
    med_d=med_d, med_mes=med_d*5, med_es=med_d*50,
    std_d=std_d, std_mes=std_d*5,
    wd=min(pnls), bd=max(pnls),
)
for d, n, w, l, wr, p in daily_rows:
    color = "green" if p > 0 else "red"
    html_out += f"<tr><td>{d.isoformat()}</td><td>{n}</td><td>{wr:.0f}%</td><td class='{color}'>{p:+.1f}</td><td>${p*5:+.0f}</td><td>${p*50:+.0f}</td></tr>"
html_out += "</table>"

html_out += "<h2>Monthly projection by capture rate</h2>"
html_out += "<table><tr><th>Scenario</th><th>Capture rate</th><th>Monthly pts</th><th>$ at 1 MES</th><th>$ at 1 ES</th><th>Notes</th></tr>"
for p in proj_rows:
    html_out += "<tr><td><b>{}</b></td><td>{}%</td><td>+{:.0f} pt</td><td>${:+.0f}</td><td>${:+.0f}</td><td class='subtle'>{}</td></tr>".format(
        html.escape(p['label']), int(p['cap']*100),
        p['monthly_pts'], p['monthly_mes'], p['monthly_es'], html.escape(p['note'])
    )
html_out += "</table>"

html_out += """
<div class="note">
<b>How to read:</b> sim PnL × capture rate × 21 trading days. Capture rate is the unknown.
Pre-fix May real captured ~38% of sim. Post-fix (today's ship: V13.2 + margin removal +
atomic bracket + S131 already live) target is 55-65%. Need 30+ post-fix trades to confirm.
</div>

<h2>What today's shipped fixes add (incremental)</h2>
<table>
<tr><th>Fix</th><th>Status</th><th>Backtest lift (sim)</th><th>$/mo at 1 MES @ 65% capture</th><th>$/mo at 1 ES @ 65% capture</th></tr>
<tr><td><b>V13.2 SC LONG cliff=A peak=B unblock</b></td><td>LIVE</td><td>+91 pt over 12 trades</td><td>~$98</td><td>~$985</td></tr>
<tr><td><b>V13.2 DD SHORT narrow (admit peak=A)</b></td><td>LIVE</td><td>+19 pt over 35 trades</td><td>~$21</td><td>~$206</td></tr>
<tr><td><b>Margin pre-check REMOVED</b></td><td>LIVE</td><td>~$108 missed today alone</td><td>~$200</td><td>~$2,000</td></tr>
<tr><td><b>Atomic bracket (eliminates naked window)</b></td><td>CODE ON, FLAG OFF</td><td>Risk reduction, not alpha</td><td>$0 direct</td><td>$0 direct</td></tr>
<tr><td><b>skip_reason observability</b></td><td>LIVE</td><td>Bug detection, not alpha</td><td>$0 direct</td><td>$0 direct</td></tr>
<tr><td><b>V16 portal = TSRT mirror</b></td><td>LIVE</td><td>Bug detection (VIX showing as fired)</td><td>$0 direct</td><td>$0 direct</td></tr>
<tr><td><b>S149 SC long BOFA-PURE align=+1 → 2x MES</b></td><td><span class='yellow'>FLAG DEFAULT OFF</span></td><td>+$1,833/mo gross sim</td><td>~$650-850 realistic</td><td>~$6,500-8,500</td></tr>
</table>

<div class="warn">
<b>S149 deferred</b>: env <code>SC_BOFA_PURE_DOUBLE_UP_ENABLED=false</code> on Railway by default.
The "+$1,833/mo" was gross-sim with no capture/decay haircut. Apr +$1,550 → May +$284 = 81% MTD
decay flag. Plan: 20-30 SC long BOFA-PURE align=+1 fires at 1x post-fix to measure bucket capture
rate. If WR ≥ 70% and capture ≥ 65% on those 20-30, flip env true on Railway dashboard for 2x.
</div>

<h2>Risk envelope</h2>
<div class="kpi">
  <div class="card"><div class="label">Daily StdDev</div><div class="value">±{std_d:.0f} pt</div><div class="sub">±${std_mes:.0f}/day MES, ±${std_es:.0f}/day ES</div></div>
  <div class="card"><div class="label">Monthly StdDev</div><div class="value">±{mo_std:.0f} pt</div><div class="sub">±${mo_std_mes:.0f}/mo MES</div></div>
  <div class="card"><div class="label">Daily loss cap</div><div class="value red">$300</div><div class="sub">absorbs ~2 losers at 1x · ~1 at 2x</div></div>
  <div class="card"><div class="label">Negative day rate</div><div class="value">{neg_pct:.0f}%</div><div class="sub">{neg_n} of {n_days} May days</div></div>
</div>

<div class="danger">
<b>Reality check:</b> StdDev ±{std_d:.0f}pt/day × √21 = ±{mo_std:.0f}pt/mo (1-sigma).
At 1 MES: ±${mo_std_mes:.0f}/mo. At 1 ES: ±${mo_std_es:.0f}/mo. A bad month can wipe out a good one.
Single-day variance is the main risk — plan for −$1,000+ days at 1 ES regularly.
</div>

<h2>Path forward</h2>
<table>
<tr><th>Horizon</th><th>Action</th><th>Why</th></tr>
<tr><td><b>Tomorrow open</b></td><td>Watch first SC LONG cliff=A peak=B real fire</td><td>V13.2 admit validation (today's 2995 mechanism)</td></tr>
<tr><td><b>Tomorrow pre-market</b></td><td>Flip ATOMIC_BRACKET_ENABLED=true (optional)</td><td>Test the ordergroup path under flag</td></tr>
<tr><td><b>Week 1-2</b></td><td>Track capture rate on V16 trades</td><td>Need data point: is 65% target achievable?</td></tr>
<tr><td><b>20-30 SC LONG BOFA-PURE align+1 fires</b></td><td>If capture ≥65% + WR ≥70% → flip S149 ON</td><td>Bucket-specific validation before 2x</td></tr>
<tr><td><b>Month-end</b></td><td>EOD portal-vs-real reconciliation report (build)</td><td>Daily attribution loop you do manually now</td></tr>
<tr><td><b>Jun 15</b></td><td>Revisit S155 vanna observation (SC SHORT + AG SHORT)</td><td>30+ more trades = retry the statistical test</td></tr>
</table>

<h2>Realistic Bottom Line</h2>
<div class="note">
<b>At 1 MES at 65% capture (mid-band post-fix target):</b><br>
&nbsp;&nbsp;Monthly mean: <b class='green'>${mean_mes_real:+.0f}</b> · Annualized: ${mean_mes_real*12:+.0f}<br><br>
<b>At 1 ES at 65% capture:</b><br>
&nbsp;&nbsp;Monthly mean: <b class='green'>${mean_es_real:+.0f}</b> · Annualized: ${mean_es_real*12:+.0f}<br><br>
<b>Caveats:</b> sample is 13 days of mostly-pre-fix V16. Post-fix sample = 2 days. Need 30+ post-fix days for high-confidence projection.
Don't anchor to today's +85th-percentile day; anchor to median.
</div>

<div class="subtle" style="text-align:center; margin-top:32px;">
🤖 Generated end of Tue 2026-05-19 session — 6 commits shipped today<br>
ad45341 · f1a174f · 1c896b4 · 529a978 · 915bdae · 0db0535
</div>
</body></html>
""".format(
    std_d=std_d, std_mes=std_d*5, std_es=std_d*50,
    mo_std=std_d*(21**0.5),
    mo_std_mes=std_d*(21**0.5)*5,
    mo_std_es=std_d*(21**0.5)*50,
    neg_pct=sum(1 for p in pnls if p < 0)/len(pnls)*100,
    neg_n=sum(1 for p in pnls if p < 0),
    n_days=len(days),
    mean_mes_real=mean_d*TRADING_DAYS*0.65*5,
    mean_es_real=mean_d*TRADING_DAYS*0.65*50,
)

# Save + send
out = tempfile.NamedTemporaryFile(suffix='.html', delete=False, mode='w', encoding='utf-8')
out.write(html_out); out.close()
print(f"HTML saved: {out.name}")

url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
files = {"document": ("projection_20260519.html", open(out.name, "rb"), "text/html")}
caption = (
    "📊 <b>0DTE Alpha — Updated Projection (EOD 2026-05-19)</b>\n\n"
    f"<b>Today</b> V16 +{daily[max(days)]['pnl']:.1f} pt · Real ${real_today_pnl:+.0f} "
    f"(beat portal by ${real_today_pnl - daily[max(days)]['pnl']*5:+.0f}) · 85th percentile of May.\n\n"
    f"<b>May daily mean</b>: +{mean_d:.1f} pt (median +{med_d:.1f} pt) · StdDev ±{std_d:.0f} pt.\n\n"
    "<b>Realistic monthly @ 65% capture:</b>\n"
    f"  • 1 MES: ~${mean_d*21*0.65*5:+.0f}/mo\n"
    f"  • 1 ES:  ~${mean_d*21*0.65*50:+.0f}/mo\n\n"
    "<b>Today's ship:</b> V13.2 vanna · margin pre-check removed · atomic bracket (flag off) · "
    "V16 portal = TSRT mirror · S149 code (env flag <b>OFF</b>, flip after 20-30 1x validation fires).\n\n"
    "Full HTML attached."
)
data = {"chat_id": CHAT_TEL_RES, "caption": caption, "parse_mode": "HTML"}
resp = requests.post(url, files=files, data=data, timeout=30)
print("Tel Res response:", resp.status_code, resp.text[:200])
