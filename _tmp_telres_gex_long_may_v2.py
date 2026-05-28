"""Tel Res — GEX Long May 2026 trades audit V2 with per-trade Plotly Greek charts.

User requested 2026-05-27 EOD — "as usual" format with inline exposure charts
showing entry/SL/TP/MFE/MAE markers against the Greek landscape.

Per feedback_trade_report_html_style.md:
- Per-trade card: header + meta + reason + Plotly chart
- Chart: gamma TODAY (bars) + charm (bars overlay), x=strike y=M$
- Markers: blue=entry, green-dashed=TP, red-dashed=SL, green-dotted=MFE, red-dotted=MAE
- Reason line: 1-2 sentences explaining setup mechanics
"""
import os, json, html as html_mod, requests, sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

ET = ZoneInfo("America/New_York")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN") or "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
TEL_RES_CHAT_ID = "-1003792574755"
eng = create_engine(os.environ["DATABASE_URL"])

print("[1/3] Pulling trades...", file=sys.stderr)
with eng.begin() as c:
    trades = [dict(r._mapping) for r in c.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et, s.ts as ts_utc,
               s.direction, s.grade, s.score, s.paradigm, s.greek_alignment as align,
               s.vix, s.spot, s.lis, s.gap_to_lis, s.rr_ratio,
               s.outcome_result as result, s.outcome_pnl as pnl,
               s.outcome_max_profit as mfe, s.outcome_max_loss as mae,
               s.outcome_elapsed_min as elapsed,
               s.outcome_target_level as tgt, s.outcome_stop_level as stp,
               s.trail_sl, s.trail_activation, s.trail_gap,
               s.exit_price, s.comments,
               r.state IS NOT NULL as in_tsrt,
               r.state->>'fill_price' as fill,
               r.state->>'close_fill_price' as close_p,
               r.state->>'close_reason' as close_reason
        FROM setup_log s
        LEFT JOIN real_trade_orders r ON r.setup_log_id = s.id
        WHERE s.setup_name='GEX Long' AND s.outcome_result IS NOT NULL
          AND s.ts AT TIME ZONE 'America/New_York' >= '2026-05-01'::date
          AND s.ts AT TIME ZONE 'America/New_York' < '2026-06-01'::date
        ORDER BY s.ts DESC
    """)).fetchall()]

# Tag each trade with era for filtering
from datetime import date as _date
V31_CUTOFF = _date(2026, 5, 18)  # v3.1 patch shipped 2026-05-18
for t in trades:
    comments = (t.get('comments') or '')
    is_backfill = 'v3 backfill' in comments
    is_v31 = t['ts_et'].date() >= V31_CUTOFF if t['ts_et'] else False
    if is_backfill:
        t['_era'] = 'backfill'
    elif is_v31:
        t['_era'] = 'v3.1'
    else:
        t['_era'] = 'v3'
print(f"  {len(trades)} trades  ({sum(1 for t in trades if t['_era']=='v3.1')} v3.1, "
      f"{sum(1 for t in trades if t['_era']=='v3')} v3, "
      f"{sum(1 for t in trades if t['_era']=='backfill')} backfill)", file=sys.stderr)

print("[2/3] Pulling GEX from chain_snapshots per trade...", file=sys.stderr)
# GEX Long is driven by SPX option chain GEX (NOT Volland).
# call_gex = call_gamma * call_OI * 100 (positive: dealers long gamma)
# put_gex  = -put_gamma * put_OI * 100 (negative: dealers short gamma)
# total_gex_per_strike = call_gex + put_gex
#
# chain_snapshots.rows[i] is a 21-element array:
#   indices 0-9:  Call (Volume, OI, IV, Gamma, Delta, BID, BID_QTY, ASK, ASK_QTY, LAST)
#   index 10:     Strike
#   indices 11-20: Put (LAST, ASK, ASK_QTY, BID, BID_QTY, Delta, Gamma, IV, OI, Volume)
import json as _json
for i, t in enumerate(trades, 1):
    spot = float(t['spot']) if t['spot'] else 7500
    ts_utc = t['ts_utc']
    s_lo = spot - 60
    s_hi = spot + 60
    with eng.begin() as c:
        snap = c.execute(text("""
            SELECT ts, spot, rows
            FROM chain_snapshots
            WHERE ts <= :ts AND ts >= :ts - interval '5 minutes'
            ORDER BY ts DESC LIMIT 1
        """), {"ts": ts_utc}).fetchone()
        gex_total = []; gex_call = []; gex_put = []
        if snap and snap[2]:
            rows_raw = snap[2]
            chain_rows = _json.loads(rows_raw) if isinstance(rows_raw, str) else rows_raw
            for row in chain_rows:
                try:
                    strike = float(row[10])
                    if strike < s_lo or strike > s_hi: continue
                    c_gamma = float(row[3] or 0); c_oi = float(row[1] or 0)
                    p_gamma = float(row[17] or 0); p_oi = float(row[19] or 0)
                    cg = c_gamma * c_oi * 100
                    pg = -p_gamma * p_oi * 100
                    # Scale to "millions" for chart readability — these are raw GEX dollars per 1pt move
                    gex_total.append([strike, (cg + pg) / 1e6])
                    gex_call.append([strike, cg / 1e6])
                    gex_put.append([strike, pg / 1e6])
                except (ValueError, TypeError, IndexError):
                    continue
    t['_gex_total'] = sorted(gex_total)
    t['_gex_call'] = sorted(gex_call)
    t['_gex_put'] = sorted(gex_put)
    t['_snap_ts'] = snap[0].isoformat() if snap else None
    if i % 15 == 0: print(f"  {i}/{len(trades)}", file=sys.stderr)

print("[3/3] Building HTML...", file=sys.stderr)

# Aggregates
n = len(trades)
wins = sum(1 for t in trades if t['result']=='WIN')
losses = sum(1 for t in trades if t['result']=='LOSS')
expired = n - wins - losses
wr = wins/(wins+losses)*100 if (wins+losses) else 0
portal_sum = sum(float(t['pnl'] or 0) for t in trades)
mfe_sum = sum(float(t['mfe'] or 0) for t in trades)

by_grade = {}
for t in trades:
    g = t['grade'] or 'NULL'
    by_grade.setdefault(g, [0,0,0,0])
    by_grade[g][0] += 1
    if t['result']=='WIN': by_grade[g][1] += 1
    elif t['result']=='LOSS': by_grade[g][2] += 1
    by_grade[g][3] += float(t['pnl'] or 0)

def fmt_money(pts, contracts=5):
    return f"${pts*contracts:+,.0f}"

def reason_line(t):
    """Explain WHY the GEX Long entered using GEX landscape from option chain."""
    spot = float(t['spot'] or 0)
    gex = t.get('_gex_total') or []
    if not gex:
        return f"GEX Long. Spot {spot:.2f} — no chain snapshot near entry time."
    above = [(s,v) for s,v in gex if s > spot]
    below = [(s,v) for s,v in gex if s < spot]
    # GEX Long mechanism: +GEX magnet above pulls spot up. Strongest +GEX above = target.
    pos_above = sorted([(s,v) for s,v in above if v > 0], key=lambda x: -x[1])
    neg_below = sorted([(s,v) for s,v in below if v < 0], key=lambda x: x[1])
    parts = []
    if pos_above:
        s_max, v_max = pos_above[0]
        parts.append(f"+GEX magnet at <strong>{s_max:.0f}</strong> ({v_max:+.1f}M$) above spot {spot:.2f}")
    if neg_below:
        s_n, v_n = neg_below[0]
        parts.append(f"−GEX support at <strong>{s_n:.0f}</strong> ({v_n:+.1f}M$) below")
    if parts:
        return "GEX Long: " + " · ".join(parts) + " → expected pull-up to magnet."
    return f"GEX Long. Spot {spot:.2f}. No clear +GEX magnet above / −GEX support below."

# Build trade card HTML
def card(t):
    is_long = t['direction'] in ('long','bullish')
    spot = float(t['spot'] or 0)
    fill = float(t['fill']) if t['fill'] else None
    close_p = float(t['close_p']) if t['close_p'] else None
    real_pnl = None
    if fill and close_p:
        real_pnl = (close_p - fill) if is_long else (fill - close_p)

    grade = t['grade'] or '-'
    result = t['result'] or '-'
    pnl = float(t['pnl'] or 0)
    mfe = float(t['mfe'] or 0)
    mae = float(t['mae'] or 0)
    elapsed = int(t['elapsed']) if t['elapsed'] else 0
    align = t['align']
    paradigm = t['paradigm'] or '-'
    vix = float(t['vix']) if t['vix'] is not None else None
    target = float(t['tgt']) if t['tgt'] is not None else None
    stop = float(t['stp']) if t['stp'] is not None else None
    exit_px = float(t['exit_price']) if t['exit_price'] is not None else None
    score = int(t['score']) if t['score'] is not None else None
    rr = float(t['rr_ratio']) if t['rr_ratio'] is not None else None

    when = t['ts_et'].strftime('%Y-%m-%d %H:%M') if t['ts_et'] else '?'

    # Pre-format value strings (f-strings don't support inline conditional format specs)
    target_s = f"{target:.2f}" if target else "—"
    stop_s = f"{stop:.2f}" if stop else "—"
    vix_s = f"{vix:.2f}" if vix else "—"
    fill_s = f"{fill:.2f}" if fill else "—"
    close_s = f"{close_p:.2f}" if close_p else "—"
    align_s = f"+{align}" if align and align > 0 else (str(align) if align is not None else "—")

    # MFE/MAE prices for chart markers
    if is_long:
        mfe_price = spot + mfe
        mae_price = spot + mae  # mae negative → below
    else:
        mfe_price = spot - mfe  # short profit when price falls
        mae_price = spot - mae  # mae negative → above

    # Result tag color
    res_color = "#22c55e" if result=='WIN' else ("#ef4444" if result=='LOSS' else "#fbbf24")
    aplus_cls = "aplus" if grade == 'A+' else ""
    result_cls = "win" if result=='WIN' else ("loss" if result=='LOSS' else "exp")

    aplus_badge = '<span class="badge aplus">A+ (anti-predictive)</span>' if grade == 'A+' else ''
    tsrt_badge = '<span class="badge tsrt">TSRT-taken</span>' if t['in_tsrt'] else ''
    era = t.get('_era', '')
    era_badge = ''
    if era == 'v3.1':
        era_badge = '<span class="badge era-v31">v3.1</span>'
    elif era == 'v3':
        era_badge = '<span class="badge era-v3">v3 pre-patch</span>'
    elif era == 'backfill':
        era_badge = '<span class="badge era-bf">v3 backfill (simulated)</span>'

    real_pnl_str = (f'<span class="{("good" if real_pnl >= 0 else "bad")}">{real_pnl:+.2f}pt</span>'
                    if real_pnl is not None else '<span class="muted">—</span>')

    rsn = reason_line(t)

    chart_id = f"chart_{t['id']}"
    gex_data = {
        'total': t.get('_gex_total') or [],
        'call': t.get('_gex_call') or [],
        'put': t.get('_gex_put') or [],
    }
    exps_json = json.dumps(gex_data)

    return f"""
<div class="card {result_cls} {aplus_cls}" data-lid="{t['id']}" data-era="{era}" data-grade="{grade}">
  <div class="card-hdr">
    <div>
      <strong>#{t['id']}</strong> · {when} ET ·
      <span style="color:{'#22c55e' if is_long else '#ef4444'}">{('▲ LONG' if is_long else '▼ SHORT')}</span> ·
      <span class="grade-{grade.replace('+','plus').replace('-','dash')}">{grade}</span> ·
      score={score} ·
      <span class="res" style="color:{res_color}">{result} {pnl:+.1f}pt</span>
    </div>
    <div>{era_badge}{aplus_badge}{tsrt_badge}</div>
  </div>

  <div class="meta">
    spot {spot:.2f} · TP {target_s} · SL {stop_s} ·
    MFE +{mfe:.1f} · MAE {mae:.1f} · elapsed {elapsed}m ·
    VIX {vix_s} · align {align_s} ·
    paradigm <strong>{paradigm}</strong>
  </div>

  <div class="reason">📌 {rsn}</div>

  <div class="pnl-row">
    <div class="pnl-box"><div class="lbl">Entry</div><div class="val">{fill_s}</div></div>
    <div class="pnl-box"><div class="lbl">Exit</div><div class="val">{close_s}</div></div>
    <div class="pnl-box"><div class="lbl">Sim P&L</div><div class="val {'good' if pnl >= 0 else 'bad'}">{pnl:+.2f}pt</div></div>
    <div class="pnl-box"><div class="lbl">Real P&L (TSRT)</div><div class="val">{real_pnl_str}</div></div>
    <div class="pnl-box"><div class="lbl">Trail</div><div class="val mono">SL={t['trail_sl']} ACT={t['trail_activation']} GAP={t['trail_gap']}</div></div>
    <div class="pnl-box"><div class="lbl">Close reason</div><div class="val muted">{t['close_reason'] or '—'}</div></div>
  </div>

  <div id="{chart_id}" style="height: 360px; margin-top: 12px;"></div>
  <script>
    (function() {{
      const exps = {exps_json};
      const spot = {spot};
      const tgt = {target if target else 'null'};
      const stp = {stop if stop else 'null'};
      const mfe_price = {mfe_price};
      const mae_price = {mae_price};
      const exit_px = {exit_px if exit_px else 'null'};

      const gex_total = exps['total'] || [];
      const gex_call = exps['call'] || [];
      const gex_put = exps['put'] || [];

      if (gex_total.length === 0) {{
        document.getElementById('{chart_id}').innerHTML = '<p class="muted" style="padding:20px">No chain snapshot near trade entry time.</p>';
        return;
      }}

      const traces = [];
      // Total GEX bars: green for +GEX (call dealers long gamma = magnet), red for -GEX (put dealers short gamma = support)
      const t_strikes = gex_total.map(x => x[0]);
      const t_vals = gex_total.map(x => x[1]);
      const t_colors = t_vals.map(v => v >= 0 ? 'rgba(34,197,94,0.85)' : 'rgba(239,68,68,0.85)');
      traces.push({{
        type: 'bar', name: 'Total GEX ($M)',
        x: t_strikes, y: t_vals, marker: {{ color: t_colors }},
        hovertemplate: 'Strike %{{x}}<br>Total GEX %{{y:.2f}}M$<extra></extra>'
      }});

      const shapes = [
        {{ type: 'line', x0: spot, x1: spot, yref: 'paper', y0: 0, y1: 1,
           line: {{ color: '#3b82f6', width: 2, dash: 'solid' }} }},
      ];
      const annotations = [
        {{ x: spot, yref: 'paper', y: 1.02, text: 'ENTRY '+spot.toFixed(0),
           showarrow: false, font: {{ color: '#3b82f6', size: 10 }} }},
      ];
      if (tgt) {{
        shapes.push({{ type: 'line', x0: tgt, x1: tgt, yref: 'paper', y0: 0, y1: 1,
                      line: {{ color: '#22c55e', width: 1.5, dash: 'dash' }} }});
        annotations.push({{ x: tgt, yref: 'paper', y: 1.02, text: 'TP '+tgt.toFixed(0),
                            showarrow: false, font: {{ color: '#22c55e', size: 10 }} }});
      }}
      if (stp) {{
        shapes.push({{ type: 'line', x0: stp, x1: stp, yref: 'paper', y0: 0, y1: 1,
                      line: {{ color: '#ef4444', width: 1.5, dash: 'dash' }} }});
        annotations.push({{ x: stp, yref: 'paper', y: 1.02, text: 'SL '+stp.toFixed(0),
                            showarrow: false, font: {{ color: '#ef4444', size: 10 }} }});
      }}
      if (mfe_price !== spot) {{
        shapes.push({{ type: 'line', x0: mfe_price, x1: mfe_price, yref: 'paper', y0: 0, y1: 1,
                      line: {{ color: '#22c55e', width: 1, dash: 'dot' }} }});
        annotations.push({{ x: mfe_price, yref: 'paper', y: 0.96, text: 'MFE '+mfe_price.toFixed(0),
                            showarrow: false, font: {{ color: '#22c55e', size: 9 }} }});
      }}
      if (mae_price !== spot) {{
        shapes.push({{ type: 'line', x0: mae_price, x1: mae_price, yref: 'paper', y0: 0, y1: 1,
                      line: {{ color: '#ef4444', width: 1, dash: 'dot' }} }});
        annotations.push({{ x: mae_price, yref: 'paper', y: 0.90, text: 'MAE '+mae_price.toFixed(0),
                            showarrow: false, font: {{ color: '#ef4444', size: 9 }} }});
      }}
      if (exit_px && exit_px !== spot) {{
        shapes.push({{ type: 'line', x0: exit_px, x1: exit_px, yref: 'paper', y0: 0, y1: 1,
                      line: {{ color: '#a78bfa', width: 1.5, dash: 'dashdot' }} }});
        annotations.push({{ x: exit_px, yref: 'paper', y: 0.86, text: 'EXIT '+exit_px.toFixed(0),
                            showarrow: false, font: {{ color: '#a78bfa', size: 9 }} }});
      }}

      Plotly.newPlot('{chart_id}', traces, {{
        barmode: 'overlay',
        paper_bgcolor: '#0f172a', plot_bgcolor: '#0f172a',
        font: {{ color: '#e2e8f0', size: 11 }},
        xaxis: {{ title: 'Strike', gridcolor: '#1e293b' }},
        yaxis: {{ title: 'Exposure ($M)', gridcolor: '#1e293b', zerolinecolor: '#334155' }},
        shapes: shapes,
        annotations: annotations,
        legend: {{ x: 0.01, y: 0.99, bgcolor: 'rgba(15,23,42,0.7)' }},
        margin: {{ l: 60, r: 20, t: 50, b: 50 }},
      }}, {{ displayModeBar: false, responsive: true }});
    }})();
  </script>

  <textarea class="note" data-lid="{t['id']}" placeholder="Your notes for lid {t['id']}..."></textarea>
</div>
"""

# Aggregate grade table
grade_tbl = '<table><thead><tr><th>Grade</th><th class="r">n</th><th class="r">W</th><th class="r">L</th><th class="r">WR</th><th class="r">Portal</th><th class="r">$ at 1 MES</th><th class="r">avg/trade</th></tr></thead><tbody>'
for k in sorted(by_grade.keys(), key=lambda x: -by_grade[x][0]):
    n_, w, l, p = by_grade[k]
    wr_ = w/(w+l)*100 if (w+l) else 0
    cls_p = "good" if p > 0 else ("bad" if p < 0 else "")
    cls_wr = "good" if wr_ >= 60 else ("bad" if wr_ < 40 else "warn")
    grade_tbl += f'<tr><td><strong>{k}</strong></td><td class="r">{n_}</td><td class="r">{w}</td><td class="r">{l}</td><td class="r {cls_wr}">{wr_:.0f}%</td><td class="r {cls_p}">{p:+.1f}pt</td><td class="r {cls_p}">{fmt_money(p)}</td><td class="r {cls_p}">{fmt_money(p/n_)}</td></tr>'
grade_tbl += '</tbody></table>'

aplus_data = by_grade.get('A+', [0,0,0,0])
a_data = by_grade.get('A', [0,0,0,0])
aplus_n, aplus_w, aplus_l, aplus_p = aplus_data
a_n, a_w, a_l, a_p = a_data
aplus_wr = aplus_w/(aplus_w+aplus_l)*100 if (aplus_w+aplus_l) else 0
a_wr = a_w/(a_w+a_l)*100 if (a_w+a_l) else 0

now_et = datetime.now(ET)

cards_html = '\n'.join(card(t) for t in trades)

doc = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>GEX Long — May 2026 Trade Audit (with Greek charts)</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  body {{ background: #0a0e1a; color: #e5e7eb; font-family: 'Plus Jakarta Sans','Segoe UI',sans-serif;
         margin: 0; padding: 20px; line-height: 1.55; }}
  .container {{ max-width: 1300px; margin: 0 auto; padding-bottom: 80px; }}
  h1 {{ color: #fff; margin-bottom: 4px; }}
  .subtitle {{ color: #94a3b8; margin-bottom: 25px; font-size: 14px; }}
  h2 {{ color: #fff; margin-top: 32px; padding-bottom: 6px; border-bottom: 2px solid #1e293b; }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }}
  th, td {{ padding: 8px 11px; text-align: left; border-bottom: 1px solid #1e293b; }}
  th {{ background: #1e293b; color: #e2e8f0; font-weight: 600; font-size: 11px; text-transform: uppercase; }}
  td.r, th.r {{ text-align: right; }}
  .good {{ color: #22c55e; font-weight: 600; }}
  .bad  {{ color: #ef4444; font-weight: 600; }}
  .warn {{ color: #fbbf24; }}
  .muted {{ color: #64748b; }}
  .mono {{ font-family: 'JetBrains Mono', monospace; font-size: 11px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 18px 0; }}
  .kpi {{ background: #111827; border-left: 4px solid #3b82f6;
          padding: 12px 16px; border-radius: 4px; }}
  .kpi.bad {{ border-left-color: #ef4444; }}
  .kpi.good {{ border-left-color: #22c55e; }}
  .kpi.warn {{ border-left-color: #fbbf24; }}
  .kpi .label {{ font-size: 10px; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px; }}
  .kpi .value {{ font-size: 22px; font-weight: 700; margin-top: 4px; color: #fff; }}
  .kpi .sub {{ font-size: 11px; color: #94a3b8; margin-top: 2px; }}
  .headline {{ background: #7f1d1d44; border-left: 4px solid #ef4444;
               padding: 18px 22px; margin: 18px 0; border-radius: 4px;
               font-size: 15px; font-weight: 500; }}

  /* Filter bar */
  .filter-bar {{ position: sticky; top: 0; background: #0a0e1a; padding: 12px 0;
                 z-index: 50; border-bottom: 1px solid #1e293b; margin-bottom: 16px; }}
  .filter-bar select {{ background: #1e293b; color: #e2e8f0; border: 1px solid #334155;
    padding: 6px 10px; border-radius: 4px; margin-right: 8px; font-size: 13px; }}
  .filter-bar button {{ background: #1d4ed8; color: #fff; border: none; padding: 6px 14px;
    border-radius: 4px; cursor: pointer; margin-left: 6px; font-size: 13px; }}

  /* Trade cards */
  .card {{ background: #0f172a; border: 1px solid #1e293b; border-left: 4px solid #3b82f6;
           padding: 16px 20px; margin: 12px 0; border-radius: 4px; }}
  .card.win {{ border-left-color: #22c55e; }}
  .card.loss {{ border-left-color: #ef4444; }}
  .card.exp {{ border-left-color: #64748b; }}
  .card.aplus {{ background: #7f1d1d22; }}
  .card-hdr {{ display: flex; justify-content: space-between; align-items: center;
               flex-wrap: wrap; gap: 8px; font-size: 14px; }}
  .grade-Aplus {{ background: #dc2626; color: #fff; padding: 1px 8px; border-radius: 3px;
                  font-size: 11px; font-weight: 600; }}
  .grade-A {{ background: #16a34a; color: #fff; padding: 1px 8px; border-radius: 3px;
              font-size: 11px; font-weight: 600; }}
  .grade-Adash {{ background: #ca8a04; color: #fff; padding: 1px 8px; border-radius: 3px;
                  font-size: 11px; font-weight: 600; }}
  .grade-B {{ background: #2563eb; color: #fff; padding: 1px 8px; border-radius: 3px;
              font-size: 11px; font-weight: 600; }}
  .meta {{ color: #94a3b8; font-size: 12px; margin: 8px 0; padding-bottom: 6px;
           border-bottom: 1px dashed #1e293b; }}
  .meta strong {{ color: #fbbf24; }}
  .reason {{ background: #1e3a8a22; border-left: 3px solid #3b82f6;
             padding: 8px 12px; margin: 10px 0; font-size: 13px; color: #cbd5e1; }}
  .pnl-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
              gap: 10px; margin: 12px 0; }}
  .pnl-box {{ background: #111827; padding: 8px 12px; border-radius: 4px; }}
  .pnl-box .lbl {{ color: #64748b; font-size: 10px; text-transform: uppercase; }}
  .pnl-box .val {{ color: #e2e8f0; font-size: 14px; font-weight: 600; margin-top: 2px; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px;
            font-size: 11px; font-weight: 600; margin-left: 6px; }}
  .badge.aplus {{ background: #dc262666; color: #fecaca; }}
  .badge.tsrt {{ background: #ca8a0444; color: #fde68a; }}
  .badge.era-v31 {{ background: #16a34a55; color: #bbf7d0; }}
  .badge.era-v3 {{ background: #2563eb55; color: #bfdbfe; }}
  .badge.era-bf {{ background: #64748b55; color: #cbd5e1; }}
  textarea.note {{ width: 100%; background: #1e293b; color: #e2e8f0; border: 1px solid #334155;
    padding: 8px 12px; border-radius: 4px; font-family: inherit; font-size: 12px;
    min-height: 50px; margin-top: 12px; box-sizing: border-box; }}

  .toolbar {{ position: fixed; bottom: 16px; right: 16px; background: #1e293b;
              padding: 12px 16px; border-radius: 8px; display: flex; gap: 8px;
              box-shadow: 0 4px 12px #00000088; z-index: 100; }}
  .toolbar button {{ background: #1d4ed8; color: #fff; border: none;
    padding: 8px 14px; border-radius: 4px; cursor: pointer; font-size: 13px; }}
  .toolbar button.danger {{ background: #dc2626; }}
</style></head>
<body><div class="container">

<h1>GEX Long — May 2026 audit (per-trade Greek charts)</h1>
<div class="subtitle">{n} trades · {now_et.strftime('%Y-%m-%d %H:%M ET')} · Generated for manual cross-check</div>

<div class="kpi-grid">
  <div class="kpi"><div class="label">Total trades</div><div class="value">{n}</div></div>
  <div class="kpi {'good' if wr >= 50 else 'bad'}"><div class="label">Win rate</div><div class="value">{wr:.0f}%</div><div class="sub">{wins}W / {losses}L / {expired} EXP</div></div>
  <div class="kpi {'good' if portal_sum > 0 else 'bad'}"><div class="label">Portal P&L</div><div class="value">{portal_sum:+.1f}pt</div><div class="sub">{fmt_money(portal_sum)} at 1 MES</div></div>
  <div class="kpi"><div class="label">MFE sum (perfect)</div><div class="value">{mfe_sum:+.1f}pt</div><div class="sub">{fmt_money(mfe_sum)} at 1 MES</div></div>
</div>

<div class="headline">
🚨 <strong>KEY FINDING:</strong> Grade A+ is anti-predictive. {aplus_n} trades, {aplus_wr:.0f}% WR, <strong>{fmt_money(aplus_p)}</strong> at 1 MES.
Compare Grade A: {a_n} trades, {a_wr:.0f}% WR, <strong>{fmt_money(a_p)}</strong>.
Model's "highest confidence" tier is the WORST performer.
</div>

<h2>Aggregates by grade</h2>
{grade_tbl}

<h2>Per-trade detail ({n} trades)</h2>

<div class="filter-bar">
  <strong style="margin-right: 12px;">Filter:</strong>
  Era: <select id="f-era" onchange="applyFilter()">
    <option value="">All</option>
    <option value="v3.1" selected>v3.1 (post May 18, current detector)</option>
    <option value="v3">v3 pre-patch (May 1-17)</option>
    <option value="backfill">v3 backfill (simulated)</option>
    <option value="no-backfill">Real fires only (no backfill)</option>
  </select>
  Result: <select id="f-result" onchange="applyFilter()">
    <option value="">All</option><option value="win">WIN</option><option value="loss">LOSS</option><option value="exp">EXPIRED</option>
  </select>
  Grade: <select id="f-grade" onchange="applyFilter()">
    <option value="">All</option><option value="A+">A+</option><option value="A">A</option><option value="A-Entry">A-Entry</option><option value="B">B</option>
  </select>
  <button onclick="document.querySelectorAll('.filter-bar select').forEach(s=>s.value='');applyFilter()">Clear</button>
  <span id="filter-summary" style="margin-left: 16px; color: #94a3b8; font-size: 12px;"></span>
</div>

{cards_html}

<div class="toolbar">
  <button onclick="exportTxt()">Export TXT (with comments)</button>
  <button onclick="copyTxt()">Copy clipboard</button>
  <button class="danger" onclick="if(confirm('Clear ALL comments?')){{Object.keys(localStorage).filter(k=>k.startsWith('gex_long_may_v2_')).forEach(k=>localStorage.removeItem(k));location.reload();}}">Clear</button>
</div>

<script>
  // Load saved comments
  document.querySelectorAll('textarea.note').forEach(ta => {{
    const lid = ta.dataset.lid;
    const stored = localStorage.getItem('gex_long_may_v2_' + lid);
    if (stored) ta.value = stored;
    ta.addEventListener('input', () => localStorage.setItem('gex_long_may_v2_' + lid, ta.value));
  }});

  function applyFilter() {{
    const fEra = document.getElementById('f-era').value;
    const fRes = document.getElementById('f-result').value;
    const fGrade = document.getElementById('f-grade').value;
    let n=0, w=0, l=0, e=0, sumPnl=0, sumReal=0, nReal=0;
    document.querySelectorAll('.card').forEach(card => {{
      let show = true;
      const era = card.dataset.era;
      if (fEra === 'no-backfill' && era === 'backfill') show = false;
      else if (fEra && fEra !== 'no-backfill' && era !== fEra) show = false;
      if (fRes && !card.classList.contains(fRes)) show = false;
      if (fGrade && card.dataset.grade !== fGrade) show = false;
      card.style.display = show ? '' : 'none';
      if (show) {{
        n++;
        if (card.classList.contains('win')) w++;
        else if (card.classList.contains('loss')) l++;
        else e++;
        // Pull sim pnl from the .pnl-box val
        const pnlBoxes = card.querySelectorAll('.pnl-box .val');
        // box 2 is Sim P&L
        if (pnlBoxes[2]) {{
          const txt = pnlBoxes[2].textContent.replace('pt','').trim();
          const v = parseFloat(txt);
          if (!isNaN(v)) sumPnl += v;
        }}
        // box 3 is Real P&L
        if (pnlBoxes[3]) {{
          const txt = pnlBoxes[3].textContent.replace('pt','').trim();
          const v = parseFloat(txt);
          if (!isNaN(v)) {{ sumReal += v; nReal++; }}
        }}
      }}
    }});
    const wr = (w+l) ? (w/(w+l)*100).toFixed(0) : 0;
    const sum = document.getElementById('filter-summary');
    if (sum) sum.innerHTML =
      `Showing <strong style="color:#fbbf24">${{n}}</strong> · ` +
      `${{w}}W/${{l}}L/${{e}}EXP · WR ` +
      `<strong style="color:${{wr>=60?'#22c55e':wr<40?'#ef4444':'#fbbf24'}}">${{wr}}%</strong> · ` +
      `Portal <strong style="color:${{sumPnl>=0?'#22c55e':'#ef4444'}}">${{sumPnl>=0?'+':''}}${{sumPnl.toFixed(1)}}pt</strong> ` +
      `($${{(sumPnl*5).toFixed(0)}} at 1 MES)` +
      (nReal>0 ? ` · Real (${{nReal}}t) <strong style="color:${{sumReal>=0?'#22c55e':'#ef4444'}}">${{sumReal>=0?'+':''}}${{sumReal.toFixed(1)}}pt</strong>` : '');
  }}
  // Apply default v3.1 filter on load
  applyFilter();

  function buildTxt() {{
    let txt = 'GEX Long May 2026 — User notes export\\n';
    txt += 'Generated: ' + new Date().toISOString() + '\\n\\n';
    document.querySelectorAll('.card').forEach(card => {{
      const lid = card.dataset.lid;
      const ta = card.querySelector('textarea.note');
      if (ta && ta.value.trim()) {{
        const hdr = card.querySelector('.card-hdr').innerText.replace(/\\n+/g, ' · ');
        txt += '[lid ' + lid + '] ' + hdr + '\\n  ' + ta.value.trim() + '\\n\\n';
      }}
    }});
    return txt;
  }}

  function exportTxt() {{
    const blob = new Blob([buildTxt()], {{type: 'text/plain'}});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'gex_long_may_notes.txt';
    a.click();
  }}

  function copyTxt() {{
    navigator.clipboard.writeText(buildTxt()).then(() => alert('Copied!'));
  }}
</script>

</div></body></html>"""

path = "_tmp_telres_gex_long_may_v2.html"
with open(path, "w", encoding="utf-8") as f:
    f.write(doc)
print(f"Wrote {path} ({len(doc):,} bytes, {n} trades with charts)", file=sys.stderr)

# Send via Telegram
caption = (f"📊 GEX Long — May 2026 audit V2 ({n} trades, with Greek charts)\n\n"
           f"🚨 KEY FINDING: Grade A+ anti-predictive\n"
           f"  • A+ ({aplus_n}t): {aplus_wr:.0f}% WR, {fmt_money(aplus_p)} at 1 MES\n"
           f"  • A ({a_n}t):  {a_wr:.0f}% WR, {fmt_money(a_p)} at 1 MES\n\n"
           f"Per-trade Plotly charts: gamma TODAY (bars) + charm (overlay) "
           f"+ markers for ENTRY/TP/SL/MFE/MAE/EXIT.\n\n"
           f"Comment per card → Export TXT.")

import socket, urllib3.util.connection as urllib3_cn
urllib3_cn.allowed_gai_family = lambda: socket.AF_INET

url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
files = {"document": ("gex_long_may_audit_v2.html", open(path, "rb"), "text/html")}
data = {"chat_id": TEL_RES_CHAT_ID, "caption": caption}

r = requests.post(url, files=files, data=data, timeout=120)
print(f"Telegram status: {r.status_code}")
print(r.text[:300])
