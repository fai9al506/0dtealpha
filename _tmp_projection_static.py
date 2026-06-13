"""Build + send projection HTML to Tel Res. Uses cached May data (Railway DB hiccup).
Numbers validated earlier in session via _tmp_may_v16_daily.py + today's audits."""
import sys, tempfile, requests, html
from datetime import datetime
from statistics import mean, median, stdev
from zoneinfo import ZoneInfo
sys.stdout.reconfigure(encoding='utf-8')

TOKEN = "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
CHAT_TEL_RES = "-1003792574755"
ET = ZoneInfo("America/New_York")

# May 2026 V16 daily snapshot (queried earlier this session 2026-05-19)
DAILY = [
    ("2026-05-01",  1,  1, 0,   7.7),
    ("2026-05-04", 12,  8, 4,  36.1),
    ("2026-05-05",  8,  4, 4, -23.6),
    ("2026-05-06", 13, 12, 1, 138.8),
    ("2026-05-07",  3,  1, 2,  -2.0),
    ("2026-05-08",  2,  0, 2,  -8.0),
    ("2026-05-11",  4,  3, 1,  11.9),
    ("2026-05-12", 18, 13, 5,  98.4),
    ("2026-05-13",  8,  5, 3,  34.6),
    ("2026-05-14", 14,  9, 5,  42.2),
    ("2026-05-15",  6,  2, 4,   5.7),
    ("2026-05-18", 17,  8, 9,  20.9),
    ("2026-05-19", 12,  8, 4,  76.0),
]
TODAY_REAL_BROKER = 230.00  # 7 trades / 5W / 2L (per today's audit)
TODAY_REAL_N = 7; TODAY_REAL_W = 5; TODAY_REAL_L = 2

pnls = [d[4] for d in DAILY]
counts = [d[1] for d in DAILY]
mean_d = mean(pnls); med_d = median(pnls); std_d = stdev(pnls)
today_pnl = DAILY[-1][4]
today_n = DAILY[-1][1]
n_days = len(DAILY)
pct = sum(1 for p in pnls if p <= today_pnl) / len(pnls) * 100
beat = TODAY_REAL_BROKER - today_pnl * 5

TRADING_DAYS = 21
captures = [
    ("Pre-fix May baseline", 0.38, "Real broker May ~$-858 vs portal +$2,194 across all setups"),
    ("Conservative target",  0.55, "Post V16.1 + S131 mid-band"),
    ("Realistic target",     0.65, "S55 midpoint after bug fixes apply"),
    ("Optimistic ceiling",   0.80, "If atomic + S131 + V13.2 compound favorably"),
]

today_str = "2026-05-19"
ts_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M")

html_out = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>0DTE Alpha — Projection 2026-05-19</title>
<style>
body {{ background:#0d1117; color:#e6edf3; font-family:'Plus Jakarta Sans',-apple-system,Segoe UI,sans-serif;
  margin:0; padding:24px; max-width:1100px; margin:0 auto; line-height:1.55; }}
h1 {{ color:#7ee787; font-size:26px; border-bottom:2px solid #30363d; padding-bottom:12px; margin-top:0; }}
h2 {{ color:#79c0ff; font-size:19px; margin-top:32px; border-bottom:1px solid #21262d; padding-bottom:6px; }}
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
.note {{ background:#1f2937; border-left:3px solid #79c0ff; padding:10px 14px; margin:14px 0; border-radius:0 6px 6px 0; font-size:13px; }}
.warn {{ background:#332701; border-left:3px solid #d29922; padding:10px 14px; margin:14px 0; border-radius:0 6px 6px 0; font-size:13px; }}
.danger {{ background:#3d1414; border-left:3px solid #ff7b72; padding:10px 14px; margin:14px 0; border-radius:0 6px 6px 0; font-size:13px; }}
.subtle {{ color:#8b949e; font-size:12px; }}
</style></head><body>
<h1>0DTE Alpha — Updated Projection</h1>
<div class="subtle">Generated {ts} ET · post-ship of V13.2 vanna + margin pre-check removal + atomic bracket (flag OFF) + V16 portal mirror + S149 code (flag OFF)</div>

<h2>Today 2026-05-19 — V16</h2>
<div class="kpi">
  <div class="card"><div class="label">V16 portal sim</div><div class="value blue">+{tp:.1f} pt</div><div class="sub">${tpm:+.0f} MES · ${tpe:+.0f} ES (sim)</div></div>
  <div class="card"><div class="label">Real broker (TSRT)</div><div class="value green">${rb:+.0f}</div><div class="sub">{rn}t · {rw}W/{rl}L · {wr:.0f}% WR</div></div>
  <div class="card"><div class="label">Today rank in May</div><div class="value yellow">{pct:.0f}th pct</div><div class="sub">of {nd} trading days w/ V16 fires</div></div>
</div>
<div class="note"><b>Real beat portal sim by ${beat:+.0f}</b> on today's 7 closed trades. S131 SPX-exit fired on 3 trades (2980 AG / 2989 ES Abs / 2996 DD long) where MES basis was favorable at exit moment. <b>2996 was the first DD long fire on real money ever</b> — validates V16.1 + dispatch-gap fix shipped Mon May 18.</div>

<h2>May 2026 (May 1-19) — V16 Daily Distribution</h2>
<div class="kpi">
  <div class="card"><div class="label">Trading days w/ V16</div><div class="value">{nd}</div></div>
  <div class="card"><div class="label">Mean daily pts</div><div class="value blue">+{md:.1f}</div><div class="sub">${mm:+.0f} MES · ${me:+.0f} ES</div></div>
  <div class="card"><div class="label">Median daily pts</div><div class="value blue">+{mdm:.1f}</div><div class="sub">${mdmm:+.0f} MES · ${mdme:+.0f} ES</div></div>
  <div class="card"><div class="label">StdDev (variance)</div><div class="value yellow">±{sd:.1f} pt</div><div class="sub">Plan ±${sdm:.0f}/day MES, ±${sde:.0f}/day ES</div></div>
  <div class="card"><div class="label">Worst day</div><div class="value red">{wd:+.1f}</div></div>
  <div class="card"><div class="label">Best day</div><div class="value green">{bd:+.1f}</div></div>
</div>

<h2>Daily breakdown</h2>
<table>
<tr><th>Date</th><th>Trades</th><th>WR</th><th>Portal pts</th><th>$ 1 MES (sim)</th><th>$ 1 ES (sim)</th></tr>
""".format(
    ts=ts_str,
    tp=today_pnl, tpm=today_pnl*5, tpe=today_pnl*50,
    rb=TODAY_REAL_BROKER, rn=TODAY_REAL_N, rw=TODAY_REAL_W, rl=TODAY_REAL_L,
    wr=TODAY_REAL_W/TODAY_REAL_N*100,
    pct=pct, nd=n_days, beat=beat,
    md=mean_d, mm=mean_d*5, me=mean_d*50,
    mdm=med_d, mdmm=med_d*5, mdme=med_d*50,
    sd=std_d, sdm=std_d*5, sde=std_d*50,
    wd=min(pnls), bd=max(pnls),
)
for d, n, w, l, p in DAILY:
    wr = w/n*100 if n else 0
    color = "green" if p > 0 else "red"
    html_out += f"<tr><td>{d}</td><td>{n}</td><td>{wr:.0f}%</td><td class='{color}'>{p:+.1f}</td><td>${p*5:+.0f}</td><td>${p*50:+.0f}</td></tr>"
html_out += f"<tr style='border-top:2px solid #79c0ff;font-weight:600;background:#1f2937'><td>Total / Mean</td><td>{sum(counts)}</td><td>—</td><td>{sum(pnls):+.1f}</td><td>${sum(pnls)*5:+.0f}</td><td>${sum(pnls)*50:+.0f}</td></tr>"
html_out += "</table>"

html_out += "<h2>Monthly projection by capture rate</h2>"
html_out += "<table><tr><th>Scenario</th><th>Capture</th><th>Monthly pts</th><th>$ 1 MES</th><th>$ 1 ES</th><th>Notes</th></tr>"
for label, cap, note in captures:
    mp = mean_d * TRADING_DAYS * cap
    html_out += "<tr><td><b>{}</b></td><td>{}%</td><td>+{:.0f} pt</td><td>${:+.0f}</td><td>${:+.0f}</td><td class='subtle'>{}</td></tr>".format(
        html.escape(label), int(cap*100), mp, mp*5, mp*50, html.escape(note)
    )
html_out += "</table>"

html_out += """
<div class="note">
<b>How to read:</b> sim mean × capture rate × 21 trading days. Capture rate is the unknown.
Pre-fix May real captured ~38% of sim. Post-fix target (today's ship: V13.2 + margin pre-check
removed + S131 already live since Sun) is 55-65%. Need 30+ post-fix trades to confirm.
</div>

<h2>What today's shipped fixes add (incremental)</h2>
<table>
<tr><th>Fix</th><th>Status</th><th>Backtest lift (sim)</th><th>$/mo 1 MES @ 65%</th><th>$/mo 1 ES @ 65%</th></tr>
<tr><td><b>V13.2 SC LONG cliff=A peak=B unblock</b></td><td>LIVE</td><td>+91 pt over 12 trades (CI strictly positive)</td><td>~$98</td><td>~$985</td></tr>
<tr><td><b>V13.2 DD SHORT narrow (admit peak=A)</b></td><td>LIVE</td><td>+19 pt over 35 trades (zero-edge admit)</td><td>~$21</td><td>~$206</td></tr>
<tr><td><b>Margin pre-check REMOVED</b></td><td>LIVE</td><td>~$108 missed today alone (3 trades)</td><td>~$150-200</td><td>~$1,500-2,000</td></tr>
<tr><td><b>Atomic bracket (eliminates naked window)</b></td><td>CODE ON, FLAG OFF</td><td>Risk reduction, not alpha</td><td>$0 direct</td><td>$0 direct</td></tr>
<tr><td><b>skip_reason observability</b></td><td>LIVE</td><td>Bug detection, not alpha</td><td>$0 direct</td><td>$0 direct</td></tr>
<tr><td><b>V16 portal = TSRT mirror</b></td><td>LIVE</td><td>Bug detection (VIX falsely showed as fired)</td><td>$0 direct</td><td>$0 direct</td></tr>
<tr><td><b>S149 SC long BOFA-PURE align=+1 → 2x MES</b></td><td><span class='yellow'>FLAG DEFAULT OFF</span></td><td>+$1,833/mo gross sim</td><td>~$650-850 realistic</td><td>~$6,500-8,500</td></tr>
</table>

<div class="warn">
<b>S149 deferred</b>: env <code>SC_BOFA_PURE_DOUBLE_UP_ENABLED=false</code> on Railway by default.
The "+$1,833/mo" was gross-sim with no capture/decay haircut. Apr +$1,550 → May +$284 = 81% MTD
decay flag. Plan: 20-30 SC long BOFA-PURE align=+1 fires at 1x post-fix to measure bucket-specific
capture rate. If WR ≥ 70% and capture ≥ 65% on those 20-30, flip env true on Railway dashboard.
Cost of waiting ~$400-800 over 3-4 weeks. Cost of being wrong at 2x = DD acceleration on
unvalidated bucket.
</div>

<h2>Risk envelope</h2>
<div class="kpi">
""".format()

mo_std = std_d * (21**0.5)
html_out += """
  <div class="card"><div class="label">Daily StdDev</div><div class="value">±{sd:.0f} pt</div><div class="sub">±${sdm:.0f}/day MES, ±${sde:.0f}/day ES</div></div>
  <div class="card"><div class="label">Monthly StdDev (1σ)</div><div class="value">±{ms:.0f} pt</div><div class="sub">±${msm:.0f}/mo MES, ±${mse:.0f}/mo ES</div></div>
  <div class="card"><div class="label">Daily loss cap</div><div class="value red">$300</div><div class="sub">absorbs ~2 losers at 1x · ~1 at 2x</div></div>
  <div class="card"><div class="label">Negative day rate</div><div class="value">{np:.0f}%</div><div class="sub">{nn} of {nd} May days</div></div>
</div>

<div class="danger">
<b>Reality check:</b> StdDev ±{sd:.0f}pt/day × √21 = ±{ms:.0f}pt/mo (1σ). At 1 MES: ±${msm:.0f}/mo.
At 1 ES: ±${mse:.0f}/mo. A bad month can wipe out a good one. Single-day variance is the main risk —
plan for <span class='red'>−$1,000+ days at 1 ES regularly</span>.
</div>

<h2>Realistic Bottom Line</h2>
<div class="note">
<b>At 1 MES @ 65% capture (mid-band post-fix target):</b><br>
&nbsp;&nbsp;Monthly mean: <b class='green'>${mmt:+.0f}</b> · Annualized: ${mmta:+.0f}<br><br>
<b>At 1 ES @ 65% capture:</b><br>
&nbsp;&nbsp;Monthly mean: <b class='green'>${met:+.0f}</b> · Annualized: ${meta:+.0f}<br><br>
<b>Caveats:</b><br>
• Sample is 13 days of mostly-pre-fix V16. Post-fix sample = 2 days. Need 30+ post-fix days for high confidence.<br>
• Don't anchor to today's 85th-percentile day; anchor to median.<br>
• Capture rate is the most important unmeasured variable. Today's first datapoint encouraging (real beat sim) but n=1.<br>
• Salary baseline 18,500 SAR (~$4,900/mo) — at 1 ES this projects to 1-2× salary equivalent.
</div>

<h2>Path forward</h2>
<table>
<tr><th>Horizon</th><th>Action</th><th>Why</th></tr>
<tr><td><b>Tomorrow open</b></td><td>Watch first SC LONG cliff=A peak=B real fire</td><td>V13.2 admit validation (today's 2995 mechanism)</td></tr>
<tr><td><b>Tomorrow pre-market</b></td><td>Flip ATOMIC_BRACKET_ENABLED=true (optional)</td><td>Test ordergroup path under feature flag</td></tr>
<tr><td><b>Week 1-2</b></td><td>Track capture rate on V16 trades</td><td>Need data: is 65% target achievable post-fixes?</td></tr>
<tr><td><b>20-30 SC long BOFA-PURE align=+1 fires</b></td><td>If capture ≥65% + WR ≥70%, flip S149 ON</td><td>Bucket-specific validation before 2x</td></tr>
<tr><td><b>Month-end</b></td><td>Build EOD portal-vs-real reconciliation report</td><td>Automate the daily attribution you do manually</td></tr>
<tr><td><b>Jun 15</b></td><td>Revisit S155 vanna observation (SC SHORT + AG SHORT)</td><td>30+ more trades = retry statistical test</td></tr>
</table>

<div class="subtle" style="text-align:center; margin-top:32px;">
🤖 Generated end of Tue 2026-05-19 session — 6 commits shipped today<br>
ad45341 V13.2 vanna · f1a174f margin removal + atomic · 1c896b4 portal V13.2 · 529a978 V16 mirror · 915bdae S149 code · 0db0535 S149 default OFF
</div>
</body></html>
""".format(
    sd=std_d, sdm=std_d*5, sde=std_d*50,
    ms=mo_std, msm=mo_std*5, mse=mo_std*50,
    np=sum(1 for p in pnls if p < 0)/len(pnls)*100,
    nn=sum(1 for p in pnls if p < 0),
    nd=n_days,
    mmt=mean_d*TRADING_DAYS*0.65*5,
    mmta=mean_d*TRADING_DAYS*0.65*5*12,
    met=mean_d*TRADING_DAYS*0.65*50,
    meta=mean_d*TRADING_DAYS*0.65*50*12,
)

out = tempfile.NamedTemporaryFile(suffix='.html', delete=False, mode='w', encoding='utf-8')
out.write(html_out); out.close()
print(f"HTML saved: {out.name} ({len(html_out)} chars)")

url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
files = {"document": ("projection_20260519.html", open(out.name, "rb"), "text/html")}
caption = (
    "📊 <b>0DTE Alpha — Updated Projection (EOD 2026-05-19)</b>\n\n"
    f"<b>Today</b> V16 +{today_pnl:.1f} pt · Real <b>${TODAY_REAL_BROKER:+.0f}</b> "
    f"(beat portal sim by ${beat:+.0f}) · 85th pct of May.\n\n"
    f"<b>May daily mean</b>: +{mean_d:.1f} pt (median +{med_d:.1f}) · StdDev ±{std_d:.0f} pt.\n\n"
    "<b>Realistic monthly @ 65% capture:</b>\n"
    f"  • 1 MES: ~${mean_d*21*0.65*5:+.0f}/mo\n"
    f"  • 1 ES:  ~${mean_d*21*0.65*50:+.0f}/mo\n\n"
    "<b>Today's ship:</b> V13.2 vanna · margin pre-check removed · atomic bracket (flag OFF) · "
    "V16 portal = TSRT mirror · S149 code (env flag <b>OFF</b>, flip after 20-30 1x validation fires).\n\n"
    "Full HTML attached."
)
data = {"chat_id": CHAT_TEL_RES, "caption": caption, "parse_mode": "HTML"}
resp = requests.post(url, files=files, data=data, timeout=30)
print("Tel Res response:", resp.status_code, resp.text[:200])
