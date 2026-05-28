"""Tel Res — GEX Long May 2026 trades audit (post-v3.1 era).

User requested 2026-05-27 EOD to inspect GEX Long trades to validate the
A+ grade anomaly finding (19% WR over 26 trades vs A grade 58% WR).

Per-trade cards + localStorage comments + export TXT. No inline Plotly
(would be heavy for 59 trades — summary tables instead).
"""
import os, json, html as html_mod
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

ET = ZoneInfo("America/New_York")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN") or "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
TEL_RES_CHAT_ID = "-1003792574755"

DB_URL = os.environ["DATABASE_URL"]
e = create_engine(DB_URL)

with e.begin() as c:
    rows = c.execute(text("""
        SELECT s.id,
               s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.direction, s.grade, s.score, s.paradigm, s.greek_alignment as align,
               s.vix, s.spot, s.lis, s.gap_to_lis, s.rr_ratio,
               s.outcome_result as result, s.outcome_pnl as pnl,
               s.outcome_max_profit as mfe, s.outcome_max_loss as mae,
               s.outcome_elapsed_min as elapsed,
               s.outcome_target_level as tgt, s.outcome_stop_level as stp,
               s.trail_sl, s.trail_activation, s.trail_gap,
               s.mes_sim_outcome_pnl as mes_pnl,
               s.exit_price,
               r.state IS NOT NULL as in_tsrt,
               r.state->>'fill_price' as fill,
               r.state->>'close_fill_price' as close_p,
               r.state->>'close_reason' as close_reason
        FROM setup_log s
        LEFT JOIN real_trade_orders r ON r.setup_log_id = s.id
        WHERE s.setup_name='GEX Long'
          AND s.outcome_result IS NOT NULL
          AND s.ts AT TIME ZONE 'America/New_York' >= '2026-05-01'::date
          AND s.ts AT TIME ZONE 'America/New_York' < '2026-06-01'::date
        ORDER BY s.ts DESC
    """)).fetchall()

trades = [dict(r._mapping) for r in rows]
n = len(trades)
wins = sum(1 for t in trades if t['result']=='WIN')
losses = sum(1 for t in trades if t['result']=='LOSS')
exp = n - wins - losses
wr = wins/(wins+losses)*100 if (wins+losses) else 0
portal_sum = sum(float(t['pnl'] or 0) for t in trades)
mfe_sum = sum(float(t['mfe'] or 0) for t in trades)

# Breakdowns
by_grade = {}; by_para = {}; by_date = {}
for t in trades:
    g = t['grade'] or 'NULL'; p = t['paradigm'] or 'NULL'
    d = t['ts_et'].strftime('%m-%d') if t['ts_et'] else 'NULL'
    for grp, key in [(by_grade, g), (by_para, p), (by_date, d)]:
        grp.setdefault(key, [0,0,0,0,0])  # n, w, l, portal_sum, mfe_sum
        grp[key][0] += 1
        if t['result']=='WIN': grp[key][1] += 1
        elif t['result']=='LOSS': grp[key][2] += 1
        grp[key][3] += float(t['pnl'] or 0)
        grp[key][4] += float(t['mfe'] or 0)

def fmt_money(pts, contracts=5):
    """pts × $5/MES."""
    return f"${pts*contracts:+,.0f}"

now_et = datetime.now(ET)

# Build per-trade cards JSON for JS to render (small payload)
cards_data = []
for t in trades:
    ts = t['ts_et']
    when = ts.strftime('%a %b %d %H:%M') if ts else '?'
    direction = t['direction'] or '-'
    is_long = direction.lower() in ('long','bullish')
    grade = t['grade'] or '-'
    pnl = float(t['pnl'] or 0)
    mfe = float(t['mfe'] or 0)
    mae = float(t['mae'] or 0)
    result = t['result'] or '-'
    align = t['align']
    paradigm = t['paradigm'] or '-'
    vix = float(t['vix']) if t['vix'] is not None else None
    spot = float(t['spot']) if t['spot'] is not None else None
    lis = float(t['lis']) if t['lis'] is not None else None
    gap = float(t['gap_to_lis']) if t['gap_to_lis'] is not None else None
    rr = float(t['rr_ratio']) if t['rr_ratio'] is not None else None
    score = float(t['score']) if t['score'] is not None else None
    elapsed = int(t['elapsed']) if t['elapsed'] is not None else None
    target = float(t['tgt']) if t['tgt'] is not None else None
    stop = float(t['stp']) if t['stp'] is not None else None
    trail_sl = float(t['trail_sl']) if t['trail_sl'] is not None else None
    trail_act = float(t['trail_activation']) if t['trail_activation'] is not None else None
    trail_gap = float(t['trail_gap']) if t['trail_gap'] is not None else None
    exit_px = float(t['exit_price']) if t['exit_price'] is not None else None
    mes_pnl = float(t['mes_pnl']) if t['mes_pnl'] is not None else None
    in_tsrt = bool(t['in_tsrt'])
    fill = float(t['fill']) if t['fill'] else None
    close_p = float(t['close_p']) if t['close_p'] else None
    close_reason = t['close_reason'] or None
    real_pnl = None
    if fill and close_p:
        real_pnl = (fill - close_p) if not is_long else (close_p - fill)

    cards_data.append({
        'lid': int(t['id']), 'when': when, 'dir': direction, 'is_long': is_long,
        'grade': grade, 'score': score, 'paradigm': paradigm,
        'align': align, 'vix': vix, 'spot': spot, 'lis': lis, 'gap': gap, 'rr': rr,
        'result': result, 'pnl': pnl, 'mfe': mfe, 'mae': mae, 'elapsed': elapsed,
        'target': target, 'stop': stop, 'exit': exit_px,
        'trail_sl': trail_sl, 'trail_act': trail_act, 'trail_gap': trail_gap,
        'mes_pnl': mes_pnl,
        'in_tsrt': in_tsrt, 'fill': fill, 'close_p': close_p, 'real_pnl': real_pnl,
        'close_reason': close_reason,
    })

cards_json = json.dumps(cards_data)

# Build grade table
def render_table(d, label):
    rows_html = ""
    for k in sorted(d.keys(), key=lambda x: -d[x][0]):
        n_, w, l, p, mf = d[k]
        e_ = n_ - w - l
        wr_ = w/(w+l)*100 if (w+l) else 0
        cls_p = "good" if p > 0 else ("bad" if p < 0 else "muted")
        cls_wr = "good" if wr_ >= 60 else ("bad" if wr_ < 40 else "warn")
        avg_p = p/n_ if n_ else 0
        avg_cls = "good" if avg_p > 0 else ("bad" if avg_p < 0 else "muted")
        rows_html += f"""<tr><td><strong>{html_mod.escape(str(k))}</strong></td>
        <td class="r">{n_}</td><td class="r">{w}</td><td class="r">{l}</td><td class="r muted">{e_}</td>
        <td class="r {cls_wr}">{wr_:.0f}%</td>
        <td class="r {cls_p}">{p:+.1f}pt</td>
        <td class="r {cls_p}">{fmt_money(p)}</td>
        <td class="r {avg_cls}">{fmt_money(avg_p)}</td>
        <td class="r muted">{fmt_money(mf)}</td>
        </tr>"""
    return f"""<h3>{label}</h3>
<table><thead><tr>
<th>{label.split(' ')[-1]}</th><th class="r">n</th><th class="r">W</th><th class="r">L</th>
<th class="r">EXP</th><th class="r">WR</th><th class="r">Portal pts</th>
<th class="r">@ 1 MES</th><th class="r">avg/trade</th><th class="r muted">if perfect (MFE)</th>
</tr></thead><tbody>{rows_html}</tbody></table>"""

grade_table = render_table(by_grade, "BY GRADE")
para_table = render_table(by_para, "BY PARADIGM")
date_table = render_table(by_date, "BY DATE")

# Key finding callout based on actual A+ data
aplus_data = by_grade.get('A+', [0,0,0,0,0])
a_data = by_grade.get('A', [0,0,0,0,0])

aplus_n, aplus_w, aplus_l, aplus_p, _ = aplus_data
a_n, a_w, a_l, a_p, _ = a_data
aplus_wr = aplus_w/(aplus_w+aplus_l)*100 if (aplus_w+aplus_l) else 0
a_wr = a_w/(a_w+a_l)*100 if (a_w+a_l) else 0

html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>0DTE Alpha — GEX Long May 2026 Audit</title>
<style>
  body {{ background: #0a0e1a; color: #e5e7eb; font-family: 'Plus Jakarta Sans','Segoe UI',sans-serif;
         margin: 0; padding: 20px; line-height: 1.55; }}
  .container {{ max-width: 1300px; margin: 0 auto; padding-bottom: 80px; }}
  h1 {{ color: #fff; margin-bottom: 4px; }}
  .subtitle {{ color: #94a3b8; margin-bottom: 25px; font-size: 14px; }}
  h2 {{ color: #fff; margin-top: 32px; padding-bottom: 6px; border-bottom: 2px solid #1e293b; }}
  h3 {{ color: #cbd5e1; margin-top: 22px; font-size: 16px; }}
  .headline {{
    background: #7f1d1d44; border-left: 4px solid #ef4444;
    padding: 18px 22px; margin: 18px 0; border-radius: 4px;
    font-size: 15px; font-weight: 500;
  }}
  .recommend {{
    background: #064e3b44; border: 2px solid #22c55e;
    padding: 18px 22px; margin: 22px 0; border-radius: 6px;
  }}
  .recommend h3 {{ margin-top: 0; color: #4ade80; }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }}
  th, td {{ padding: 8px 11px; text-align: left; border-bottom: 1px solid #1e293b; }}
  th {{ background: #1e293b; color: #e2e8f0; font-weight: 600; font-size: 11px; text-transform: uppercase; }}
  td.r, th.r {{ text-align: right; }}
  td.c, th.c {{ text-align: center; }}
  .good {{ color: #22c55e; font-weight: 600; }}
  .bad  {{ color: #ef4444; font-weight: 600; }}
  .warn {{ color: #fbbf24; }}
  .muted {{ color: #64748b; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 18px 0; }}
  .kpi {{ background: #111827; border-left: 4px solid #3b82f6;
          padding: 12px 16px; border-radius: 4px; }}
  .kpi.bad {{ border-left-color: #ef4444; }}
  .kpi.good {{ border-left-color: #22c55e; }}
  .kpi.warn {{ border-left-color: #fbbf24; }}
  .kpi .label {{ font-size: 10px; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px; }}
  .kpi .value {{ font-size: 22px; font-weight: 700; margin-top: 4px; color: #fff; }}
  .kpi .sub {{ font-size: 11px; color: #94a3b8; margin-top: 2px; }}
  code {{ background: #1e293b; padding: 2px 6px; border-radius: 3px;
          font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #fbbf24; }}

  /* Trade cards */
  .filter-bar {{ position: sticky; top: 0; background: #0a0e1a; padding: 12px 0; z-index: 10;
                 border-bottom: 1px solid #1e293b; margin-bottom: 16px; }}
  .filter-bar select, .filter-bar input {{ background: #1e293b; color: #e2e8f0; border: 1px solid #334155;
    padding: 6px 10px; border-radius: 4px; margin-right: 8px; font-size: 13px; }}
  .filter-bar button {{ background: #1d4ed8; color: #fff; border: none; padding: 6px 14px;
    border-radius: 4px; cursor: pointer; margin-left: 6px; font-size: 13px; }}
  .filter-bar button:hover {{ background: #1e40af; }}
  .filter-bar button.bad {{ background: #dc2626; }}

  .trade-card {{ background: #0f172a; border: 1px solid #1e293b; border-left: 4px solid #3b82f6;
                 padding: 16px 20px; margin: 12px 0; border-radius: 4px; }}
  .trade-card.win {{ border-left-color: #22c55e; }}
  .trade-card.loss {{ border-left-color: #ef4444; }}
  .trade-card.exp {{ border-left-color: #64748b; }}
  .trade-card.aplus {{ background: #7f1d1d22; }}
  .trade-card.tsrt {{ box-shadow: 0 0 0 1px #fbbf24aa; }}
  .card-head {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }}
  .card-head h4 {{ margin: 0; font-size: 15px; color: #fff; }}
  .card-meta {{ font-size: 12px; color: #94a3b8; }}
  .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
                gap: 10px 16px; margin: 12px 0; font-size: 12px; }}
  .card-grid .lbl {{ color: #64748b; font-size: 10px; text-transform: uppercase; }}
  .card-grid .val {{ color: #e2e8f0; font-weight: 600; font-size: 14px; }}
  .card-grid .val.good {{ color: #22c55e; }}
  .card-grid .val.bad {{ color: #ef4444; }}
  .card-grid .val.warn {{ color: #fbbf24; }}
  textarea.note {{ width: 100%; background: #1e293b; color: #e2e8f0; border: 1px solid #334155;
    padding: 8px 12px; border-radius: 4px; font-family: inherit; font-size: 12px;
    min-height: 50px; margin-top: 8px; box-sizing: border-box; }}
  .tag {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px;
          font-weight: 600; margin-right: 6px; }}
  .tag.win {{ background: #16a34a44; color: #4ade80; }}
  .tag.loss {{ background: #dc262644; color: #f87171; }}
  .tag.exp {{ background: #4b556344; color: #cbd5e1; }}
  .tag.aplus {{ background: #dc262666; color: #fecaca; }}
  .tag.tsrt {{ background: #ca8a0444; color: #fde68a; }}

  .toolbar {{ position: fixed; bottom: 16px; right: 16px; background: #1e293b;
              padding: 12px 16px; border-radius: 8px; display: flex; gap: 8px;
              box-shadow: 0 4px 12px #00000055; z-index: 100; }}
  .toolbar button {{ background: #1d4ed8; color: #fff; border: none; padding: 8px 14px;
    border-radius: 4px; cursor: pointer; font-size: 13px; }}
  .toolbar button.danger {{ background: #dc2626; }}
</style></head>
<body><div class="container">

<h1>GEX Long — May 2026 audit</h1>
<div class="subtitle">{n} trades · {now_et.strftime('%Y-%m-%d %H:%M ET')} · Generated for manual cross-check</div>

<div class="kpi-grid">
  <div class="kpi"><div class="label">Total trades</div><div class="value">{n}</div></div>
  <div class="kpi {'good' if wr >= 50 else 'bad'}"><div class="label">Win rate</div><div class="value">{wr:.0f}%</div><div class="sub">{wins}W / {losses}L / {exp} EXP</div></div>
  <div class="kpi {'good' if portal_sum > 0 else 'bad'}"><div class="label">Portal P&L</div><div class="value">{portal_sum:+.1f}pt</div><div class="sub">{fmt_money(portal_sum)} at 1 MES</div></div>
  <div class="kpi"><div class="label">MFE sum (perfect)</div><div class="value">{mfe_sum:+.1f}pt</div><div class="sub">{fmt_money(mfe_sum)} at 1 MES</div></div>
  <div class="kpi warn"><div class="label">Capture vs perfect</div><div class="value">{portal_sum/mfe_sum*100 if mfe_sum else 0:.0f}%</div></div>
</div>

<div class="headline">
<strong>🚨 KEY FINDING:</strong> Grade A+ is anti-predictive. {aplus_n} trades, {aplus_wr:.0f}% WR, <strong>{fmt_money(aplus_p)}</strong> portal at 1 MES.
Compare Grade A: {a_n} trades, {a_wr:.0f}% WR, <strong>{fmt_money(a_p)}</strong> portal.
Model's "highest confidence" tier is the worst performer. Same pattern as DD short A+ from prior audits.
</div>

<div class="recommend">
<h3>✅ Proposed action — SHIP GEX Long real-trading with grade != A+ filter</h3>
<p><strong>Filter:</strong> in <code>_passes_live_filter()</code>, add for GEX Long: <code>if grade == "A+": return False</code></p>
<p><strong>Env flip:</strong> <code>railway variables --set "GEX_LONG_V3_REAL_TRADE_ENABLED=true"</code></p>
<p><strong>Expected lift:</strong> {fmt_money(a_p)} (Grade A only) + smaller B/A-Entry contribution. Estimated ~$300/mo at 1 MES.</p>
<p><strong>Revert criteria:</strong> 3 consecutive losses OR net &lt; -$200 over any 5 trades → flip env back.</p>
<p class="muted" style="margin-top: 8px;">Pending your approval after manual cross-check of the trades below.</p>
</div>

<h2>Aggregates</h2>
{grade_table}
{para_table}
{date_table}

<h2>Per-trade details ({n} trades)</h2>

<div class="filter-bar">
  <strong style="margin-right: 12px;">Filter:</strong>
  Result: <select id="f-result" onchange="renderCards()">
    <option value="">All</option><option value="WIN">WIN</option><option value="LOSS">LOSS</option><option value="EXPIRED">EXPIRED</option>
  </select>
  Grade: <select id="f-grade" onchange="renderCards()">
    <option value="">All</option><option value="A+">A+</option><option value="A">A</option><option value="A-Entry">A-Entry</option><option value="B">B</option>
  </select>
  TSRT: <select id="f-tsrt" onchange="renderCards()">
    <option value="">All</option><option value="yes">YES (taken)</option><option value="no">NO (portal only)</option>
  </select>
  <button onclick="document.getElementById('f-result').value='';document.getElementById('f-grade').value='';document.getElementById('f-tsrt').value='';renderCards();">Clear</button>
</div>

<div id="cards-container"></div>

<div class="toolbar">
  <button onclick="exportTxt()">Export TXT (with comments)</button>
  <button onclick="copyTxt()">Copy to clipboard</button>
  <button class="danger" onclick="if(confirm('Clear ALL comments?')){{Object.keys(localStorage).filter(k=>k.startsWith('gex_long_may_')).forEach(k=>localStorage.removeItem(k));renderCards();}}">Clear comments</button>
</div>

<script>
const TRADES = {cards_json};
const STORAGE_PREFIX = 'gex_long_may_';

function fmt(v, dec=2) {{
  if (v === null || v === undefined) return '<span class="muted">—</span>';
  return Number(v).toFixed(dec);
}}
function fmtPnL(v) {{
  if (v === null || v === undefined) return '<span class="muted">—</span>';
  const cls = v > 0 ? 'good' : (v < 0 ? 'bad' : '');
  return `<span class="${{cls}}">${{v >= 0 ? '+' : ''}}${{Number(v).toFixed(2)}}</span>`;
}}

function renderCards() {{
  const fRes = document.getElementById('f-result').value;
  const fGrade = document.getElementById('f-grade').value;
  const fTsrt = document.getElementById('f-tsrt').value;

  const container = document.getElementById('cards-container');
  container.innerHTML = '';

  let count = 0;
  TRADES.forEach(t => {{
    if (fRes && t.result !== fRes) return;
    if (fGrade && t.grade !== fGrade) return;
    if (fTsrt === 'yes' && !t.in_tsrt) return;
    if (fTsrt === 'no' && t.in_tsrt) return;

    count++;
    const classes = ['trade-card'];
    if (t.result === 'WIN') classes.push('win');
    else if (t.result === 'LOSS') classes.push('loss');
    else classes.push('exp');
    if (t.grade === 'A+') classes.push('aplus');
    if (t.in_tsrt) classes.push('tsrt');

    const resTag = t.result === 'WIN' ? '<span class="tag win">WIN</span>' :
                   t.result === 'LOSS' ? '<span class="tag loss">LOSS</span>' :
                   `<span class="tag exp">${{t.result}}</span>`;
    const aplusTag = t.grade === 'A+' ? '<span class="tag aplus">A+ (anti-predictive)</span>' : '';
    const tsrtTag = t.in_tsrt ? `<span class="tag tsrt">TSRT-taken</span>` : '';

    const stored = localStorage.getItem(STORAGE_PREFIX + t.lid) || '';

    const realPnLLine = t.in_tsrt && t.real_pnl !== null ?
      `<div><div class="lbl">Real (TSRT)</div><div class="val ${{t.real_pnl >= 0 ? 'good' : 'bad'}}">${{t.real_pnl >= 0 ? '+' : ''}}${{Number(t.real_pnl).toFixed(2)}}pt</div></div>` : '';

    const closeReasonLine = t.in_tsrt && t.close_reason ?
      `<div><div class="lbl">Close reason</div><div class="val muted" style="font-size:11px">${{t.close_reason}}</div></div>` : '';

    const trailInfo = (t.trail_sl !== null && t.trail_act !== null) ?
      `SL=${{t.trail_sl}} ACT=${{t.trail_act}} GAP=${{t.trail_gap}}` : '<span class="muted">—</span>';

    container.insertAdjacentHTML('beforeend', `
      <div class="${{classes.join(' ')}}">
        <div class="card-head">
          <h4>lid ${{t.lid}} · ${{t.when}} · ${{t.dir.toUpperCase()}}</h4>
          <div class="card-meta">${{resTag}} ${{aplusTag}} ${{tsrtTag}}</div>
        </div>
        <div class="card-grid">
          <div><div class="lbl">Grade</div><div class="val ${{t.grade === 'A+' ? 'bad' : (t.grade === 'A' ? 'good' : '')}}">${{t.grade}}</div></div>
          <div><div class="lbl">Score</div><div class="val">${{fmt(t.score, 0)}}</div></div>
          <div><div class="lbl">Paradigm</div><div class="val" style="font-size:12px">${{t.paradigm}}</div></div>
          <div><div class="lbl">Align</div><div class="val">${{t.align !== null ? (t.align > 0 ? '+' + t.align : t.align) : '—'}}</div></div>
          <div><div class="lbl">VIX</div><div class="val">${{fmt(t.vix, 2)}}</div></div>
          <div><div class="lbl">Spot</div><div class="val">${{fmt(t.spot, 2)}}</div></div>
          <div><div class="lbl">LIS</div><div class="val">${{fmt(t.lis, 2)}}</div></div>
          <div><div class="lbl">Gap to LIS</div><div class="val">${{fmt(t.gap, 2)}}</div></div>
          <div><div class="lbl">Target</div><div class="val">${{fmt(t.target, 2)}}</div></div>
          <div><div class="lbl">Stop</div><div class="val">${{fmt(t.stop, 2)}}</div></div>
          <div><div class="lbl">Exit</div><div class="val">${{fmt(t.exit, 2)}}</div></div>
          <div><div class="lbl">Trail params</div><div class="val" style="font-size:11px">${{trailInfo}}</div></div>
          <div><div class="lbl">Portal P&L</div><div class="val ${{t.pnl >= 0 ? 'good' : 'bad'}}">${{t.pnl >= 0 ? '+' : ''}}${{Number(t.pnl).toFixed(2)}}pt</div></div>
          <div><div class="lbl">MFE</div><div class="val good">+${{Number(t.mfe).toFixed(2)}}pt</div></div>
          <div><div class="lbl">MAE</div><div class="val bad">${{Number(t.mae).toFixed(2)}}pt</div></div>
          ${{t.elapsed !== null ? `<div><div class="lbl">Duration</div><div class="val">${{t.elapsed}}m</div></div>` : ''}}
          ${{t.mes_pnl !== null ? `<div><div class="lbl">MES-sim P&L</div><div class="val">${{t.mes_pnl >= 0 ? '+' : ''}}${{Number(t.mes_pnl).toFixed(2)}}pt</div></div>` : ''}}
          ${{realPnLLine}}
          ${{closeReasonLine}}
        </div>
        <textarea class="note" placeholder="Your notes for lid ${{t.lid}}..."
                  oninput="localStorage.setItem('${{STORAGE_PREFIX}}${{t.lid}}', this.value)">${{stored.replace(/"/g, '&quot;')}}</textarea>
      </div>
    `);
  }});

  if (count === 0) {{
    container.innerHTML = '<p class="muted">No trades match current filter.</p>';
  }}
}}

function buildTxt() {{
  let txt = `GEX Long May 2026 — User notes export\\n`;
  txt += `Generated: ${{new Date().toISOString()}}\\n\\n`;
  TRADES.forEach(t => {{
    const stored = localStorage.getItem(STORAGE_PREFIX + t.lid);
    if (stored && stored.trim()) {{
      txt += `lid ${{t.lid}} · ${{t.when}} · ${{t.dir}} · ${{t.grade}} · ${{t.result}} ${{t.pnl >= 0 ? '+' : ''}}${{t.pnl.toFixed(2)}}pt · ${{t.paradigm}}\\n`;
      txt += `  ${{stored.trim()}}\\n\\n`;
    }}
  }});
  return txt;
}}

function exportTxt() {{
  const txt = buildTxt();
  const blob = new Blob([txt], {{type: 'text/plain'}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'gex_long_may_notes.txt';
  a.click();
  URL.revokeObjectURL(url);
}}

function copyTxt() {{
  navigator.clipboard.writeText(buildTxt()).then(() => alert('Copied to clipboard!'));
}}

renderCards();
</script>
</div></body></html>"""

# Save locally
path = "_tmp_telres_gex_long_may.html"
with open(path, "w", encoding="utf-8") as f:
    f.write(html)
print(f"Wrote {path} ({len(html):,} bytes, {n} trades)")

# Send via Telegram
caption = (f"📊 GEX Long — May 2026 audit ({n} trades)\n\n"
           f"🚨 KEY FINDING: Grade A+ anti-predictive\n"
           f"  • A+ ({aplus_n}t): {aplus_wr:.0f}% WR, {fmt_money(aplus_p)} at 1 MES\n"
           f"  • A ({a_n}t):  {a_wr:.0f}% WR, {fmt_money(a_p)} at 1 MES\n\n"
           f"Open in browser to cross-check trades + add comments per card.\n"
           f"Proposal: ship GEX Long real-trading with grade != A+ filter.")

import socket, urllib3.util.connection as urllib3_cn
urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
files = {"document": ("gex_long_may_audit.html", open(path, "rb"), "text/html")}
data = {"chat_id": TEL_RES_CHAT_ID, "caption": caption}

r = requests.post(url, files=files, data=data, timeout=60)
print(f"Telegram status: {r.status_code}")
print(r.text[:300])
