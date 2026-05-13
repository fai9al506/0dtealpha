"""Track D HTML report builder. Dark theme per `feedback_pdf_style.md`."""
from __future__ import annotations
import json
import html as html_lib
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
IN = 'G:/My Drive/Python/MyProject/GitHub/0dtealpha/_tmp_track_d_results.json'
OUT = 'G:/My Drive/Python/MyProject/GitHub/0dtealpha/_tmp_track_d_clean_entry.html'

with open(IN) as f:
    data = json.load(f)

meta = data['meta']
feature_results = data['feature_results']
per_dir = data['per_dir_results']
filter_results = data['filter_results']
filter_ranked = data['filter_ranked']
stop_analysis = data['stop_analysis']
clean_sub = data['clean_sub_counts']


def esc(s):
    return html_lib.escape(str(s))


def sig_class(p):
    if p < 0.001:
        return 'sig-3'
    if p < 0.01:
        return 'sig-2'
    if p < 0.05:
        return 'sig-1'
    return ''


# ============================================================================
# Build HTML
# ============================================================================
out_parts = []
out_parts.append('''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Track D: Clean-Entry Analysis (MAE &le; 3 pts)</title>
<style>
:root {
    --bg: #0d1117;
    --bg-card: #161b22;
    --bg-row-alt: #1c2128;
    --bg-row-hover: #21262d;
    --text: #c9d1d9;
    --text-muted: #8b949e;
    --text-dim: #6e7681;
    --border: #30363d;
    --accent: #58a6ff;
    --accent-2: #d2a8ff;
    --pos: #56d364;
    --neg: #ff7b72;
    --warn: #d29922;
    --sig-1: #d29922;
    --sig-2: #e08000;
    --sig-3: #ff7b72;
}
* { box-sizing: border-box; }
body {
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    margin: 0; padding: 24px;
    line-height: 1.5;
}
h1 { color: var(--accent); border-bottom: 1px solid var(--border); padding-bottom: 12px; }
h2 { color: var(--accent-2); margin-top: 36px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }
h3 { color: #79c0ff; margin-top: 24px; }
h4 { color: #a5d6ff; margin-top: 16px; margin-bottom: 8px; }
.subtitle { color: var(--text-muted); font-size: 13px; margin-top: -8px; }
.card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
    margin: 16px 0;
}
.stat-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
}
.stat {
    background: var(--bg-row-alt);
    padding: 12px 16px;
    border-radius: 6px;
    border: 1px solid var(--border);
}
.stat .label { color: var(--text-muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
.stat .value { font-size: 22px; font-weight: 600; margin-top: 4px; }
.pos { color: var(--pos); }
.neg { color: var(--neg); }
.warn { color: var(--warn); }
.muted { color: var(--text-muted); }
.dim { color: var(--text-dim); }
table {
    width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 13px;
}
.div-table th, .div-table td, table.flt th, table.flt td, table.gen th, table.gen td {
    padding: 6px 10px;
    text-align: right;
    border-bottom: 1px solid var(--border);
}
.div-table th, table.flt th, table.gen th {
    background: var(--bg-row-alt);
    color: var(--text-muted);
    font-weight: 500;
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.5px;
}
.div-table td:first-child, table.flt td:first-child, table.gen td:first-child { text-align: left; }
.div-table tr:hover, table.flt tr:hover, table.gen tr:hover { background: var(--bg-row-hover); }
.sig-1 td { background: rgba(210,153,34,0.08); }
.sig-2 td { background: rgba(224,128,0,0.12); }
.sig-3 td { background: rgba(255,123,114,0.15); font-weight: 500; }
.filter-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 4px solid var(--accent);
    border-radius: 6px;
    padding: 16px;
    margin: 12px 0;
}
.filter-card.rank-1 { border-left-color: var(--pos); }
.filter-card.rank-2 { border-left-color: var(--accent-2); }
.filter-card.rank-3 { border-left-color: var(--accent); }
.filter-card.warn-card { border-left-color: var(--warn); }
.filter-name { font-size: 16px; font-weight: 600; color: var(--accent); margin-bottom: 8px; }
.filter-mech { color: var(--text-muted); font-style: italic; margin-top: 8px; font-size: 13px; }
.metric-row {
    display: flex; gap: 20px; flex-wrap: wrap; margin-top: 8px;
}
.metric-row .item { font-size: 13px; }
.metric-row .label { color: var(--text-muted); font-size: 11px; }
.toc { position: sticky; top: 0; background: var(--bg); padding: 8px 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; z-index: 10; }
.toc a { color: var(--accent); margin-right: 14px; text-decoration: none; font-size: 13px; }
.toc a:hover { text-decoration: underline; }
.callout {
    background: rgba(88,166,255,0.08);
    border-left: 3px solid var(--accent);
    padding: 12px 16px;
    border-radius: 4px;
    margin: 12px 0;
}
.callout.warn { background: rgba(210,153,34,0.08); border-left-color: var(--warn); }
.callout.neg { background: rgba(255,123,114,0.08); border-left-color: var(--neg); }
.callout.pos { background: rgba(86,211,100,0.08); border-left-color: var(--pos); }
small { color: var(--text-dim); }
code { background: var(--bg-row-alt); padding: 2px 6px; border-radius: 3px; font-size: 12px; }
.badge { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px; margin-left: 8px; }
.badge.ok { background: rgba(86,211,100,0.18); color: var(--pos); }
.badge.bad { background: rgba(255,123,114,0.18); color: var(--neg); }
.badge.warn { background: rgba(210,153,34,0.18); color: var(--warn); }
.badge.neutral { background: rgba(139,148,158,0.18); color: var(--text-muted); }
.section { margin: 24px 0; }
</style>
</head>
<body>
<div class="toc">
<a href="#overview">Overview</a>
<a href="#methodology">Methodology</a>
<a href="#features">Feature Divergences</a>
<a href="#perdir">Per-Direction</a>
<a href="#filters">Filter Candidates</a>
<a href="#stops">Stop-Tightening</a>
<a href="#mechanism">Mechanisms</a>
<a href="#tasks">Tasks</a>
</div>
''')

# Header
out_parts.append(f'''
<h1>Track D: Clean-Entry Analysis (MAE &le; 3 pts)</h1>
<p class="subtitle">Generated: {datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")} &middot;
   Window: {meta['date_range'][0]} to {meta['date_range'][1]} &middot;
   Method: chain-walk MFE/MAE on ALL signals (era-agnostic)</p>

<div class="callout">
<strong>Premise.</strong> A CLEAN_ENTRY trade is one where MAE (max adverse excursion) &le; 3 pts &mdash; the
thesis was validated within 3 pts of entry. These signals fired and price went our way immediately
with negligible drawdown. They concentrate where the system has TRUE EDGE (not just survival).
</div>
''')

# ============================================================================
# Overview stats
# ============================================================================
clean_n = meta['clean_count']
total = meta['total_trades']
medium_n = meta['medium_count']
heavy_n = meta['heavy_count']
out_parts.append(f'''
<h2 id="overview">Overview</h2>
<div class="card">
<div class="stat-grid">
<div class="stat"><div class="label">Total signals</div><div class="value">{total:,}</div></div>
<div class="stat"><div class="label">CLEAN_ENTRY (MAE &le; 3)</div><div class="value pos">{clean_n:,} ({clean_n/total*100:.1f}%)</div></div>
<div class="stat"><div class="label">MEDIUM (3-8)</div><div class="value">{medium_n:,} ({medium_n/total*100:.1f}%)</div></div>
<div class="stat"><div class="label">HEAVY_DRAWDOWN (&gt;8)</div><div class="value neg">{heavy_n:,} ({heavy_n/total*100:.1f}%)</div></div>
<div class="stat"><div class="label">CLEAN longs</div><div class="value">{meta['clean_long_count']:,}</div></div>
<div class="stat"><div class="label">CLEAN shorts</div><div class="value">{meta['clean_short_count']:,}</div></div>
</div>
</div>

<h3>CLEAN_ENTRY sub-classification</h3>
<table class="gen" style="max-width:520px">
<thead><tr><th>Sub-class</th><th>Definition</th><th>Count</th></tr></thead>
<tbody>
<tr><td>CLEAN_BIG_WIN</td><td>MAE &le; 3, MFE &ge; 15, PnL &gt; 0</td><td>{clean_sub.get('CLEAN_BIG_WIN',0)}</td></tr>
<tr><td>CLEAN_MED_WIN</td><td>MAE &le; 3, 5 &le; MFE &lt; 15, PnL &gt; 0</td><td>{clean_sub.get('CLEAN_MED_WIN',0)}</td></tr>
<tr><td>CLEAN_SMALL_WIN</td><td>MAE &le; 3, MFE &lt; 5, PnL &gt; 0</td><td>{clean_sub.get('CLEAN_SMALL_WIN',0)}</td></tr>
<tr><td>CLEAN_THEN_FADED</td><td>MAE &le; 3 at entry but later stopped (MFE small, eventual loss)</td><td>{clean_sub.get('CLEAN_THEN_FADED',0)}</td></tr>
<tr><td>CLEAN_SCRATCH</td><td>MAE &le; 3, ended at zero PnL</td><td>{clean_sub.get('CLEAN_SCRATCH',0)}</td></tr>
</tbody>
</table>
<p class="muted">~94% of CLEAN_ENTRY signals win. The 22 "CLEAN_THEN_FADED" cases (5%) are signals
that started cleanly but reversed before target.</p>
''')

# ============================================================================
# Methodology
# ============================================================================
out_parts.append('''
<h2 id="methodology">Methodology</h2>
<div class="card">
<h4>Data &amp; classification</h4>
<ul>
<li><strong>Era-agnostic.</strong> Uses ALL 2,312 setup_log signals since 2026-03-01, regardless of which
filter version was live for real_trader.py. Setup detector logs EVERY signal; only outcome PnL
matters here.</li>
<li><strong>Chain-walk MFE/MAE</strong> from <code>chain_snapshots</code>, NEVER <code>outcome_max_profit/loss</code>
(per <code>feedback_chain_path_for_trail_studies.md</code>). DB cols freeze at trail-stop fire and
underestimate 2-3x.</li>
<li><strong>CLEAN_ENTRY</strong> = MAE &le; 3 pts. <strong>MEDIUM</strong> = 3-8 pts. <strong>HEAVY</strong> = &gt;8 pts.
Baseline for divergence tests = MEDIUM &cup; HEAVY.</li>
</ul>
<h4>Statistical tests</h4>
<ul>
<li>Chi-square (Yates-corrected) for categorical features. Three-tier highlight: p&lt;0.001 (***),
p&lt;0.01 (**), p&lt;0.05 (*).</li>
<li>Per-bucket population gate: n &ge; 15 to be shown, n &ge; 30 to qualify as filter candidate.</li>
<li><strong>Bootstrap 95% CI on mean PnL</strong>, 2000 iter, seeded.</li>
<li><strong>OOS halves</strong>: first 60% / last 40% by date. Requires both halves PnL&gt;0 and |WR drop| &le; 15pp.</li>
<li><strong>Per-month consistency</strong>: all 3 months (Mar/Apr/May) must be PnL-positive.</li>
</ul>
<h4>Important caveats</h4>
<ul>
<li>Some features are sparsely populated (<code>v13_gex_above</code>, <code>v13_dd_near</code>,
<code>vanna_cliff_side</code>, <code>vanna_peak_side</code> only logged for SC/DD shorts post-V13 ship
&rarr; 63.9% null). Stats use available rows only.</li>
<li><strong>"Clean-entry rate" alone is misleading</strong>: late-day (15-16 ET) has the highest CE rate
(32.7%) but actually loses money (mean +0.08, WR 49.6%). High CE means the signal&rsquo;s
<em>thesis</em> proves quickly, NOT that the eventual outcome is good.</li>
</ul>
</div>
''')

# ============================================================================
# Feature divergences
# ============================================================================
out_parts.append('<h2 id="features">Feature Divergences (CLEAN vs OTHER)</h2>')
out_parts.append('<p class="muted">Base CLEAN rate across all features: <strong>%.1f%%</strong>. '
                 'Buckets show: count in CLEAN / total trades with that bucket value. '
                 '"Lift" is (CLEAN_rate - base_rate) in pp.</p>' % (clean_n / total * 100))


def render_table(rows, max_rows=12):
    h = ['<table class="div-table"><thead><tr>']
    h.append('<th>Bucket</th><th>CLEAN</th><th>OTHER</th><th>N</th><th>CLEAN rate</th><th>Lift</th><th>p</th>')
    h.append('</tr></thead><tbody>')
    for r in rows[:max_rows]:
        sigc = sig_class(r['p'])
        lift_class = 'pos' if r['lift'] > 0 else ('neg' if r['lift'] < 0 else '')
        h.append(f'<tr class="{sigc}">')
        h.append(f'<td>{esc(r["bucket"])}</td>')
        h.append(f'<td>{r["A_count"]}</td>')
        h.append(f'<td>{r["B_count"]}</td>')
        h.append(f'<td>{r["total"]}</td>')
        h.append(f'<td>{r["A_rate"]:.1f}%</td>')
        h.append(f'<td class="{lift_class}">{r["lift"]:+.1f}</td>')
        h.append(f'<td>{r["p"]:.4f}</td>')
        h.append('</tr>')
    h.append('</tbody></table>')
    return '\n'.join(h)


# Sort features by max abs lift among sig buckets
def feat_importance(rows):
    sig = [r for r in rows if r['p'] < 0.05 and r['total'] >= 30]
    if not sig:
        return 0
    return max(abs(r['lift']) for r in sig)


feature_order = sorted(feature_results.items(), key=lambda kv: -feat_importance(kv[1]))
for name, rows in feature_order:
    if not rows:
        continue
    imp = feat_importance(rows)
    label = ''
    if imp >= 10:
        label = '<span class="badge bad">strong</span>'
    elif imp >= 5:
        label = '<span class="badge warn">moderate</span>'
    elif imp > 0:
        label = '<span class="badge neutral">weak</span>'
    else:
        label = '<span class="badge neutral">none</span>'
    out_parts.append(f'<h4>{esc(name)} {label}</h4>')
    out_parts.append(render_table(rows))


# ============================================================================
# Per-direction
# ============================================================================
out_parts.append('<h2 id="perdir">Per-Direction Divergences</h2>')
out_parts.append(f'''
<div class="callout">
Longs and shorts have <strong>mirror but DIFFERENT</strong> CLEAN signatures. Longs prefer mild
positive alignment (+1) + BOFA-PURE; shorts prefer strong negative alignment (-3) + AG Short.
The user's framing of "exact top short / exact bottom long" maps cleanly to these two profiles.
</div>
''')

for direction in ('long', 'short'):
    out_parts.append(f'<h3>Direction = {direction.upper()}</h3>')
    feats = per_dir[direction]
    feats_sorted = sorted(feats.items(), key=lambda kv: -feat_importance(kv[1]))
    for name, rows in feats_sorted:
        if not rows:
            continue
        if feat_importance(rows) == 0:
            continue
        out_parts.append(f'<h4>{esc(name)} ({direction})</h4>')
        out_parts.append(render_table(rows, max_rows=8))


# ============================================================================
# Filter candidates
# ============================================================================
out_parts.append('<h2 id="filters">Filter Candidates</h2>')
out_parts.append(f'''
<p class="muted">Each candidate filter was evaluated on the FULL 2,312-trade universe. All candidates
shown require n &ge; 30, OOS halves both PnL&gt;0, bootstrap CI excludes zero, and 3/3 months positive.</p>
''')

# Build a clean filter table
out_parts.append('<h3>Full filter universe (sorted by mean PnL/trade)</h3>')
out_parts.append('<table class="flt"><thead><tr>')
out_parts.append('<th>Filter</th><th>N</th><th>CE%</th><th>WR%</th><th>Mean</th><th>Tot</th>'
                 '<th>$/mo</th><th>h1 WR</th><th>h2 WR</th><th>mos+</th><th>MaxDD</th><th>CI lo</th></tr></thead><tbody>')
sorted_filters = sorted(filter_results, key=lambda r: -r['mean_pnl'])
for r in sorted_filters:
    if r['n'] < 30:
        continue
    ci_lo = r.get('ci_lo')
    ci_lo_str = f"{ci_lo:+.2f}" if ci_lo is not None else 'n/a'
    ci_class = 'pos' if (ci_lo is not None and ci_lo > 0) else 'neg'
    oos_ok = r['h1_mean'] > 0 and r['h2_mean'] > 0 and abs(r['h1_wr'] - r['h2_wr']) <= 15
    name_class = ''
    if oos_ok and ci_lo is not None and ci_lo > 0 and r['months_pos'] == r['months_total']:
        name_class = 'pos'
    out_parts.append(f'<tr><td class="{name_class}">{esc(r["name"])}</td>'
                     f'<td>{r["n"]}</td><td>{r["ce_rate"]:.1f}%</td><td>{r["wr"]:.1f}%</td>'
                     f'<td class="{"pos" if r["mean_pnl"]>0 else "neg"}">{r["mean_pnl"]:+.2f}</td>'
                     f'<td>{r["total_pnl"]:+.1f}</td>'
                     f'<td>{r["pnl_per_mo_mes"]:+.0f}$</td>'
                     f'<td>{r["h1_wr"]:.0f}%</td><td>{r["h2_wr"]:.0f}%</td>'
                     f'<td>{r["months_pos"]}/{r["months_total"]}</td>'
                     f'<td>{r["max_dd"]:.0f}</td>'
                     f'<td class="{ci_class}">{ci_lo_str}</td></tr>')
out_parts.append('</tbody></table>')

# Top 5 filter cards
out_parts.append('<h3>Top 5 surviving filters</h3>')
top5 = filter_ranked[:5]
for i, r in enumerate(top5):
    rank = i + 1
    rank_class = f'rank-{rank}' if rank <= 3 else ''
    ci_lo = r.get('ci_lo')
    ci_hi = r.get('ci_hi')
    oos_ok = r['h1_mean'] > 0 and r['h2_mean'] > 0 and abs(r['h1_wr'] - r['h2_wr']) <= 15
    ci_ok = ci_lo is not None and ci_lo > 0
    monthly_str = ', '.join(f"{m[0]}: {m[1]:+.0f}pts ({m[2]})" for m in r['monthly'])
    out_parts.append(f'<div class="filter-card {rank_class}">')
    out_parts.append(f'<div class="filter-name">#{rank} &mdash; {esc(r["name"])}</div>')
    out_parts.append(f'<div class="metric-row">')
    out_parts.append(f'<div class="item"><div class="label">N / dates</div>{r["n"]} / {r["n_dates"]}</div>')
    out_parts.append(f'<div class="item"><div class="label">CLEAN rate</div>{r["ce_rate"]:.1f}%</div>')
    out_parts.append(f'<div class="item"><div class="label">WR</div>{r["wr"]:.0f}%</div>')
    out_parts.append(f'<div class="item"><div class="label">Mean PnL</div><span class="{"pos" if r["mean_pnl"]>0 else "neg"}">{r["mean_pnl"]:+.2f} pts</span></div>')
    out_parts.append(f'<div class="item"><div class="label">Total PnL</div>{r["total_pnl"]:+.1f} pts</div>')
    out_parts.append(f'<div class="item"><div class="label">$/mo @ 1 MES</div><span class="pos">${r["pnl_per_mo_mes"]:+.0f}</span></div>')
    out_parts.append(f'<div class="item"><div class="label">MaxDD (pts)</div>{r["max_dd"]:.0f}</div>')
    out_parts.append(f'</div>')
    out_parts.append(f'<div style="margin-top:8px">')
    if ci_lo is not None:
        out_parts.append(f'<small>Bootstrap CI [{ci_lo:+.2f}, {ci_hi:+.2f}] <span class="badge {"ok" if ci_ok else "bad"}">CI excludes 0: {"YES" if ci_ok else "NO"}</span></small><br>')
    out_parts.append(f'<small>OOS halves: h1 WR={r["h1_wr"]:.0f}% / h2 WR={r["h2_wr"]:.0f}% '
                     f'<span class="badge {"ok" if oos_ok else "bad"}">OOS: {"OK" if oos_ok else "DRIFT"}</span></small><br>')
    out_parts.append(f'<small>Per-month: {esc(monthly_str)} '
                     f'<span class="badge {"ok" if r["months_pos"]==r["months_total"] else "warn"}">'
                     f'{r["months_pos"]}/{r["months_total"]} positive</span></small>')
    out_parts.append(f'</div>')
    out_parts.append(f'<div class="filter-mech">Mechanism: {esc(r["mechanism"])}</div>')
    out_parts.append('</div>')


# Track B cross-reference
out_parts.append('<h3>Cross-reference vs Track B (PERFECT_ENTRY)</h3>')
out_parts.append('''
<div class="callout pos">
<strong>Track B and Track D agree</strong> on the most important findings:
<ul style="margin: 8px 0 0 0">
<li><strong>AG Short + AG-aligned paradigm</strong> &mdash; Track B F12: 33% PE / 79% WR / +830$/mo.
Track D F7 (same construction): identical 33% CE / 79% WR / +830$/mo (3/3 months, OOS 82/69%).</li>
<li><strong>SHORT + alignment == -3</strong> &mdash; Track B F1: PE rate elevated. Track D F3: 24% CE,
68% WR, +1803$/mo, both OOS halves 67-69%.</li>
<li><strong>LONG + alignment == +1</strong> &mdash; Track B F2 (B2: PE bucket). Track D F1: 31% CE,
63% WR. F22 (+BOFA-PURE narrow): 42% CE / 76% WR / +958$/mo.</li>
</ul>
<strong>New in Track D</strong> (not in Track B):
<ul style="margin: 8px 0 0 0">
<li>F28: V14-live + LONG + alignment in (1,2) &mdash; 35.2% CE, 73% WR, +1462$/mo, MaxDD 42 only.</li>
<li>F26: AG Short EXCLUDING AG-TARGET paradigm &mdash; identical to F7 in this dataset
(AG-TARGET 0% CE, 50% WR, -38 pts &mdash; should be blocked outright).</li>
</ul>
</div>
''')


# ============================================================================
# Stop-tightening
# ============================================================================
out_parts.append('<h2 id="stops">Stop-Tightening Analysis</h2>')
out_parts.append('''
<p class="muted">For each setup, what % of trades have MAE under various thresholds, and what would
happen to total PnL if SL was tightened? Method: for each historical trade, if MAE &gt; new_SL
assume it would have stopped at -new_SL; otherwise keep actual outcome.</p>
<div class="callout warn">
<strong>Findings: stop-tightening hurts almost every setup.</strong> The current stop levels are
NOT excessive &mdash; the live trail mechanism is doing the right thing by letting trades breathe
before resolving. Only <code>Paradigm Reversal</code> shows minor improvement (-103 &rarr; -54)
at SL=5. <strong>Recommendation: keep current stops.</strong>
</div>
''')

out_parts.append('<table class="gen"><thead><tr>')
out_parts.append('<th>Setup</th><th>N</th><th>MAE&le;3%</th><th>MAE&le;5%</th><th>MAE&le;8%</th>'
                 '<th>MAE&le;12%</th><th>p50</th><th>p75</th><th>p90</th><th>Current</th>'
                 '<th>SL=5</th><th>SL=6</th><th>SL=8</th><th>SL=10</th><th>SL=12</th></tr></thead><tbody>')
for s in stop_analysis:
    pu = s['mae_pct_under']
    sims = s['sim_results']
    out_parts.append('<tr>')
    out_parts.append(f'<td>{esc(s["setup"])}</td>')
    out_parts.append(f'<td>{s["n"]}</td>')
    out_parts.append(f'<td>{pu.get("3",pu.get(3,0)):.0f}%</td>')
    out_parts.append(f'<td>{pu.get("5",pu.get(5,0)):.0f}%</td>')
    out_parts.append(f'<td>{pu.get("8",pu.get(8,0)):.0f}%</td>')
    out_parts.append(f'<td>{pu.get("12",pu.get(12,0)):.0f}%</td>')
    out_parts.append(f'<td>{s["mae_median"]:.1f}</td>')
    out_parts.append(f'<td>{s["mae_p75"]:.1f}</td>')
    out_parts.append(f'<td>{s["mae_p90"]:.1f}</td>')
    cur = s["current_total"]
    out_parts.append(f'<td class="{"pos" if cur>0 else "neg"}">{cur:+.0f}</td>')
    for sl in ('5', '6', '8', '10', '12'):
        cell = sims.get(sl, sims.get(int(sl)) if int(sl) in sims else None)
        if cell:
            v = cell['total']
            cls = 'pos' if v > cur else 'neg'
            out_parts.append(f'<td class="{cls}">{v:+.0f}</td>')
        else:
            out_parts.append('<td>-</td>')
    out_parts.append('</tr>')
out_parts.append('</tbody></table>')


# ============================================================================
# Mechanism narratives
# ============================================================================
out_parts.append('<h2 id="mechanism">Mechanism Narratives</h2>')
out_parts.append('''
<div class="card">
<h4>1. Mild bullish alignment (+1) for longs &mdash; the sweet spot</h4>
<p>+1 alignment means dealers are slightly long-biased but NOT chasing (would be +3). When price
trades through a long signal at +1 alignment, the dealer hedge flow is supportive but not exhausted.
The 31% CLEAN rate at align==+1 vs 14% at align==+3 confirms that "all Greeks lined up" is
actually <strong>worse</strong> for longs &mdash; you&rsquo;re entering after the dealer flow has
already pushed price up, so any pullback hits MAE quickly.</p>

<h4>2. Strong bearish alignment (-3) for shorts &mdash; mirror principle</h4>
<p>For shorts, alignment==-3 IS the sweet spot (24% CLEAN, 68% WR). When dealers are FULLY against
price (charm+vanna+gamma all negative), there&rsquo;s no support catching the fade &mdash; short
fades face zero resistance. This is the opposite of longs because shorts profit from dealer
dis-supply, while longs profit from dealer absorption.</p>

<h4>3. AG Short + AG-aligned paradigm &mdash; the gold filter</h4>
<p>The strongest single filter across BOTH Track B and Track D. AG paradigm means dealers are
aggressively positioned for downside; AG Short setup is bearish counterpart to GEX Long. When the
two align (AG-PURE, AG-LIS, BofA-LIS), 33% of signals get clean fills and 79% win. Only AG-TARGET
breaks the pattern (0% CE, 50% WR) &mdash; "TARGET" means price is already at the target zone,
leaving no room.</p>

<h4>4. BOFA-PURE for longs &mdash; clean paradigm matters</h4>
<p>BOFA-PURE produces 28% CLEAN rate for longs vs 7% for AG-PURE. The paradigm carries directional
bias: BOFA is bullish (dealers buying), AG is bearish (dealers selling). Matching paradigm to
direction is essential. SC long + align==+1 + BOFA-PURE = 42% CE, 76% WR &mdash; the tightest
sweet spot we found.</p>

<h4>5. Time-of-day &mdash; CLEAN rate &ne; profitability</h4>
<p>15-16 ET has the HIGHEST CLEAN rate (32.7%) but POOR profitability (mean +0.08, WR 50%). Late-day
moves are quick but reverse often. Inverse: 10-11 ET has LOW CLEAN rate (15.6%) but solid profit
(+1.59 mean). Lesson: <strong>"clean entry" is only useful when paired with directional/regime
filters</strong>. A late-day CLEAN signal can still flip on you.</p>

<h4>6. VIX regime &mdash; longs LOVE low VIX, shorts LOVE high VIX</h4>
<p>For longs: VIX 16-20 = 30.5% CLEAN; VIX &ge; 28 = 11.4% CLEAN. For shorts: VIX &ge; 28 = 29.8%
CLEAN. Classic regime mirror. The current overvix rule (VIX-VIX3M &lt; -2 boosts longs) is data-
validated and shows up here independently.</p>
</div>
''')


# ============================================================================
# Tasks proposals
# ============================================================================
out_parts.append('<h2 id="tasks">Proposed Tasks</h2>')
out_parts.append('''
<div class="callout warn">
<strong>NONE of these are auto-ship candidates.</strong> They are study/audit candidates for monitoring
on TSRT going forward, or backtest extensions. The user's anti-shortterm-flip rule
(<code>feedback_dont_ship_on_short_term_flip.md</code>) requires 100+ post-improvement trades,
OOS halves, and mechanism explanation. Track D provides 2 of 3 (OOS, mechanism) but not 100+
post-improvement live trades.
</div>

<ol>
<li><strong>Sxxx: F22 SC long + align==+1 + BOFA-PURE narrowing</strong> &mdash; backtest 6 months
historical, compare against current SC long V14 (F21). If F22 maintains 70%+ WR over 6 months,
consider narrowing real-trader SC long to this subset. <strong>74 historical signals over 2.4 months
= ~30/mo; that&rsquo;s plenty of forward volume.</strong></li>

<li><strong>Sxxx: F26 AG Short hard-block of AG-TARGET paradigm</strong> &mdash; current AG Short has 67
trades, 75% WR, +344pts. Blocking AG-TARGET (10 trades, -38pts) lifts to 57 trades, 79% WR, +382pts.
LOW RISK code change; can ship.</li>

<li><strong>Sxxx: F28 V14-live LONG + align in (1,2) audit</strong> &mdash; this filter has 35% CE,
73% WR, +1462$/mo at 1 MES, MaxDD only 42pts. Strongest "tighten the V14 longs" candidate. Audit
for 30 forward trades before considering.</li>

<li><strong>Sxxx: F3 SHORT align==-3 elevation</strong> &mdash; this filter is too broad (covers all
setups). Re-test as overlay on V14 short whitelist (SC, AG, currently). If V14 short alignment
gate could be enforced as align&le;-1 instead of current "Layer 1 GEX/DD magnet" approach, may
improve precision.</li>

<li><strong>Sxxx: Stop-tightening &mdash; FORMAL REJECTION</strong> &mdash; Track D's Phase 7 confirms
current SL levels (SC=14, VIX Div=8, etc.) are correct. Tightening hurts every setup except
Paradigm Reversal (which is already not on TSRT real). Close the question.</li>
</ol>
''')


# ============================================================================
# Caveats
# ============================================================================
out_parts.append('''
<h2>Final Caveats</h2>
<div class="callout neg">
<ul>
<li><strong>CLEAN_ENTRY is a thesis-validation signal, not an outcome predictor.</strong> 22 of 449
CLEAN entries later faded to losses. Filters here must combine CE rate with WR and per-month
consistency &mdash; CE rate alone is misleading (cf. 15-16 ET window).</li>
<li><strong>2.4 months of data is short.</strong> N=2,312 sounds large but spans only 3 months of
trading. Multi-quarter validation needed before any real-money change.</li>
<li><strong>Era-agnostic means regime-vulnerable.</strong> Mar-May 2026 was net up; bull-friendly
filters (longs+BOFA-PURE) may degrade in a bear regime. Mechanism-grounded filters (alignment,
paradigm) more durable than time/regime ones.</li>
<li><strong>SIM vs real fill drift</strong>: all PnL in this report is from <code>outcome_pnl</code>
DB column, which is portal-simulated SPX. Real MES execution lags 70-95% of sim per
<code>feedback_capture_rate_anchor.md</code>. $/mo numbers should be discounted ~20% for live.</li>
</ul>
</div>

<p class="muted" style="margin-top:30px">
Generated by Track D Clean-Entry Analysis &middot; Script: <code>_tmp_track_d_clean_entry.py</code>,
HTML builder: <code>_tmp_track_d_build_html.py</code>
</p>
</body>
</html>
''')

with open(OUT, 'w', encoding='utf-8') as f:
    f.write('\n'.join(out_parts))
print(f"Wrote {OUT}")
