"""Correction report to Tel Res — replaces msg 88's overstated projection.
Uses V16.1 (latest, live since 2026-05-18 commit 8d868c6) and the correct
placement_rate × capture_rate math.
"""
import os
import psycopg2
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TEL_RES_CHAT_ID = "-1003792574755"

# Numbers from _tmp_v16_projection_recalc.py output
V14_NOTIFIED_N = 524
V14_NOTIFIED_PTS = 1011.1
V16_FILTERED_N = 221
V16_FILTERED_PTS = 620.0
ACTUAL_PLACED_N = 100
PLACEMENT_RATE = 0.45  # 100 / 221 — pre-tonight's-fixes
TRADING_DAYS_WINDOW = 14
TRADING_DAYS_PER_MONTH = 20
SAR_PER_USD = 3.75
SALARY_SAR = 18500

v16_portal_monthly = V16_FILTERED_PTS * 5 * (TRADING_DAYS_PER_MONTH / TRADING_DAYS_WINDOW)

now_et = datetime.now(ET)

html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>0DTE Alpha — CORRECTION to msg 88 (V16.1 projection)</title>
<style>
  body {{
    background: #0a0e1a; color: #e5e7eb; font-family: 'Plus Jakarta Sans','Segoe UI',sans-serif;
    margin: 0; padding: 20px; line-height: 1.55;
  }}
  .container {{ max-width: 1000px; margin: 0 auto; }}
  h1 {{ color: #fff; margin-bottom: 4px; }}
  .subtitle {{ color: #94a3b8; margin-bottom: 25px; font-size: 14px; }}
  h2 {{ color: #fff; margin-top: 32px; padding-bottom: 6px; border-bottom: 2px solid #1e293b; }}
  h3 {{ color: #cbd5e1; margin-top: 20px; font-size: 16px; }}
  .correction-banner {{
    background: #7c2d12; border-left: 4px solid #fbbf24;
    padding: 14px 18px; margin: 14px 0; border-radius: 4px;
    font-weight: 500;
  }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }}
  th, td {{ padding: 8px 11px; text-align: left; border-bottom: 1px solid #1e293b; }}
  th {{ background: #1e293b; color: #e2e8f0; font-weight: 600; font-size: 11px; text-transform: uppercase; }}
  td.r, th.r {{ text-align: right; }}
  .good {{ color: #22c55e; font-weight: 600; }}
  .bad  {{ color: #ef4444; font-weight: 600; }}
  .warn {{ color: #fbbf24; }}
  .strike {{ text-decoration: line-through; color: #64748b; }}
  .corrected-row {{ background: rgba(34,197,94,0.06); }}
  .verdict {{
    background: #064e3b33; border-left: 4px solid #22c55e;
    padding: 14px 18px; margin: 18px 0; border-radius: 4px;
  }}
  .note {{
    background: #1e3a8a22; border-left: 4px solid #3b82f6;
    padding: 12px 16px; margin: 14px 0; border-radius: 4px; font-size: 13px;
  }}
</style>
</head>
<body>
<div class="container">

<h1>CORRECTION — msg 88 projection had compounding errors</h1>
<div class="subtitle">{now_et.strftime('%A %Y-%m-%d %H:%M ET')} · Supersedes msg 88's forward-projection section</div>

<div class="correction-banner">
  <b>Self-audit found 7 errors in msg 88's projection.</b> Caught by user pushback.
  Lesson saved to memory (`feedback_verification_discipline.md`) so this pattern doesn't repeat.
  This message corrects the projection math. <b>Sections 1-3 + 5-7 of msg 88 stand</b> (today's
  reconciliation, retrospective, eval strategy, path forward). <b>Only section 4 (Forward Projection)
  is wrong and replaced here.</b>
</div>

<h2>What was wrong in msg 88</h2>
<table>
  <tr><th>#</th><th>Error</th><th>Right answer</th></tr>
  <tr><td>1</td><td>Used <b>V14</b> whitelist (524 trades)</td><td><b>V16.1</b> is the live filter (221 trades)</td></tr>
  <tr><td>2</td><td>Mislabeled V16 vs V16.1</td><td>V16.1 (S147, commit 8d868c6) is the version with the DD long align&ge;0 carve-out — that's what's live</td></tr>
  <tr><td>3</td><td>Computed $/mo as portal_sim × capture_rate</td><td>Need portal_sim × <b>placement_rate × capture_rate</b> (placement_rate accounts for cap, dispatch race, breaker, etc.)</td></tr>
  <tr><td>4</td><td>Stated $4,550/mo at 1 MES "conservative"</td><td>Realistic 1 MES = <b>$1,200–$2,500/mo</b></td></tr>
  <tr><td>5</td><td>Said $4,550 = "40-45% of salary"</td><td>$4,550 = 92% of salary; my $/mo number was wrong, % label was internally inconsistent</td></tr>
  <tr><td>6</td><td>Salary numbers not cross-checked</td><td>Computing inline now: $/mo × 3.75 SAR ÷ 18,500 = %salary</td></tr>
  <tr><td>7</td><td>Said ✦ MES-sim "more honest" w/o checking today's data</td><td>Today's data showed MES-sim 2x WORSE than chain-sim — badge correctly removed (S169)</td></tr>
</table>

<h2>Three-universe analysis (May 1-20, 14 trading days)</h2>
<table>
  <tr><th>Universe</th><th class="r">Trades</th><th class="r">Portal pts</th><th class="r">Portal $</th></tr>
  <tr><td class="strike">V14-notified (msg 88's wrong basis)</td><td class="r strike">{V14_NOTIFIED_N}</td><td class="r strike">{V14_NOTIFIED_PTS:.0f}</td><td class="r strike">${V14_NOTIFIED_PTS*5:,.0f}</td></tr>
  <tr class="corrected-row"><td><b>V16.1-filtered (correct basis — live filter)</b></td><td class="r"><b>{V16_FILTERED_N}</b></td><td class="r"><b>{V16_FILTERED_PTS:.0f}</b></td><td class="r"><b>${V16_FILTERED_PTS*5:,.0f}</b></td></tr>
  <tr><td>Actual real_trade_orders placed</td><td class="r">{ACTUAL_PLACED_N}</td><td class="r">15.9</td><td class="r">$80</td></tr>
</table>

<div class="note">
  <b>Why V16.1 = 42% of V14:</b> V16/V16.1 adds R10 (ES Abs bearish hr≥14), R5 (SC long GEX-LIS), R2 (SC long OpEx Friday), R12 (AG Short OpEx Friday), plus tightens DD short to block-all (S164 tonight) and DD long to align∈{{0,1,2}} (S147 = V16.1). Cuts ~58% of V14-eligible signals as "wouldn't actually fire".
  <br><br>
  <b>Why placed = 45% of V16.1:</b> Real-trader cap=3 per direction blocks some. Dispatch race + breaker bug ate others. Post tonight's S161 fix, placement rate should improve toward 60-70%.
</div>

<h2>Corrected forward projection at 1 MES</h2>

<p>Formula: <b>V16.1 portal_sim × placement_rate × capture_rate × month-scaling</b></p>
<p>V16.1 portal sim per month (extrapolated from 14-day window × 20/14): <b>${v16_portal_monthly:,.0f}/mo</b></p>

<table>
  <tr><th>Scenario</th><th class="r">Placement × Capture</th><th class="r">$/mo</th><th class="r">SAR/mo</th><th class="r">% of {SALARY_SAR:,} SAR salary</th></tr>
"""

scenarios = [
    ("Today's actual (pre-fixes, May real)", 0.45, 0.40, "Caught up in bugs"),
    ("Post-tonight (S161 breaker fix landed)", 0.55, 0.55, "Realistic next month"),
    ("Improved placement + capture", 0.65, 0.65, "Likely range after 30 days"),
    ("Optimistic — fixes mature", 0.75, 0.75, "Stretch target"),
]
for label, plc, cap, note in scenarios:
    monthly = v16_portal_monthly * plc * cap
    sar = monthly * SAR_PER_USD
    pct = sar / SALARY_SAR * 100
    sar_str = f"{sar:,.0f}"
    bar_class = "good" if pct >= 40 else ("warn" if pct >= 25 else "bad")
    html += f"""
  <tr>
    <td>{label}</td>
    <td class="r">{plc:.0%} × {cap:.0%} = {plc*cap:.0%}</td>
    <td class="r"><b>${monthly:,.0f}</b></td>
    <td class="r">{sar_str}</td>
    <td class="r {bar_class}"><b>{pct:.0f}%</b></td>
  </tr>"""

html += f"""
</table>

<p><b>Honest 1 MES range: $1,200 – $2,500/mo = 24-50% of salary.</b> Matches PROJECT_BRAIN's $1,000-1,400 baseline plus modest upside from tonight's fixes.</p>

<h2>1 ES scaling (10× post-S55-stable + 60+ clean MES days)</h2>
<table>
  <tr><th>Scenario</th><th class="r">$/mo at 1 ES</th><th class="r">SAR/mo</th><th class="r">× salary</th></tr>
"""

for label, plc, cap, _ in scenarios[1:]:  # skip pre-fixes
    monthly = v16_portal_monthly * plc * cap * 10 * 0.92
    sar = monthly * SAR_PER_USD
    mult = sar / SALARY_SAR
    sar_str = f"{sar:,.0f}"
    html += f"""
  <tr>
    <td>{label}</td>
    <td class="r good">${monthly:,.0f}</td>
    <td class="r">{sar_str}</td>
    <td class="r good"><b>{mult:.1f}×</b></td>
  </tr>"""

html += f"""
</table>

<h2>Salary-coverage thresholds</h2>
<table>
  <tr><th>Target</th><th>Required at 1 MES</th><th>Required at 1 ES</th></tr>
  <tr><td>50% salary (SAR 9,250 / $2,467/mo)</td><td>Placement×capture ≥ 56%</td><td>Trivial — even 30%×30% × 10× ≥ this</td></tr>
  <tr><td>100% salary (SAR 18,500 / $4,933/mo)</td><td>Placement×capture ≥ 111% — <b>NOT achievable at 1 MES</b></td><td>Placement×capture ≥ 12% — trivially achievable</td></tr>
  <tr><td>3× salary (SAR 55,500 / $14,800/mo)</td><td>Impossible at 1 MES</td><td>Placement×capture ≥ 35% — likely achievable</td></tr>
</table>

<div class="verdict">
  <b>Honest bottom line:</b>
  <br><br>
  • <b>1 MES today:</b> realistic $1,200–$2,500/mo = 24-50% salary. NOT 90%.
  <br>
  • <b>Salary coverage requires 1 ES.</b> That's gated on 60+ clean MES days with capture rate ≥ 60% confirmed.
  <br>
  • <b>3-5× salary (the dream) lives at 1 ES under 60-75% capture.</b> Real but at least 4-6 months away.
  <br>
  • <b>The bug cluster eating May ~$3,400–$4,200 is the addressable gap.</b> Tonight's fixes target it; tomorrow's first signals start the measurement.
  <br><br>
  No projection is a guarantee. The number that matters going forward is rolling 30-day broker P&amp;L (not portal sim, not extrapolation). The system gets to salary-coverage by mechanical discipline — measure, validate, then scale. Never by extrapolating one good day.
</div>

<div style="margin-top:35px;padding:14px;background:#0f172a;border-radius:6px;font-size:11px;color:#64748b">
  Generated {now_et.strftime('%Y-%m-%d %H:%M ET')} · Supersedes msg 88 section 4 ·
  Filter: V16.1 (live since 2026-05-18 commit 8d868c6) ·
  Data source: setup_log Mar 1 – May 20 + TS BalanceDetail
</div>

</div>
</body>
</html>
"""

# Write + send
path = "_tmp_telres_correction_msg89.html"
with open(path, "w", encoding="utf-8") as f:
    f.write(html)
print(f"Report written: {path} ({len(html):,} chars)")

url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
caption = (
    "CORRECTION to msg 88 — projection had compounding errors\n\n"
    "Honest 1 MES range: $1,200-2,500/mo = 24-50% salary (NOT 90%).\n"
    "Salary coverage requires 1 ES, gated on 60+ clean MES days at ≥60% capture.\n\n"
    "Errors caught: V14 vs V16.1 label, $/mo overstated 3x, salary % inconsistent, "
    "✦ MES-sim trust without re-validation. Lesson saved to memory."
)
with open(path, "rb") as f:
    files = {"document": ("projection_correction_msg89.html", f, "text/html")}
    data = {"chat_id": TEL_RES_CHAT_ID, "caption": caption}
    r = requests.post(url, files=files, data=data, timeout=30)
print(f"Telegram response: {r.status_code} {r.text[:200]}")
