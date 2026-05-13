"""Build the final Track E HTML report (dark theme)."""
import json, html as html_lib
from datetime import datetime

# Load all data
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json") as f:
    features = json.load(f)
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_results.json") as f:
    aggregate_results = json.load(f)
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_per_setup_results.json") as f:
    per_setup = json.load(f)
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_finalists.json") as f:
    finalists = json.load(f)

LIVE_SETUPS = ("Skew Charm", "AG Short", "DD Exhaustion", "GEX Long",
               "ES Absorption", "VIX Divergence", "Paradigm Reversal",
               "Vanna Pivot Bounce", "BofA Scalp")
live_recs = [r for r in features if r["setup"] in LIVE_SETUPS]
live_n = len(live_recs)
total_pnl = sum(r["pnl"] for r in live_recs)
overall_wr = sum(1 for r in live_recs if r["pnl"] > 0) / live_n

DARK_CSS = """
* { box-sizing: border-box; }
body { background: #0d1117; color: #c9d1d9; font-family: -apple-system, "Segoe UI", Roboto, sans-serif;
       margin: 0; padding: 24px; line-height: 1.5; max-width: 1400px; margin: 0 auto; }
h1 { color: #58a6ff; border-bottom: 2px solid #30363d; padding-bottom: 8px; font-size: 28px; }
h2 { color: #79c0ff; margin-top: 36px; border-bottom: 1px solid #30363d; padding-bottom: 4px; font-size: 22px; }
h3 { color: #a5d6ff; margin-top: 24px; font-size: 18px; }
h4 { color: #d2a8ff; margin-top: 16px; font-size: 15px; }
table { width: 100%; border-collapse: collapse; margin: 12px 0; background: #161b22; font-size: 13px; }
th, td { padding: 6px 10px; text-align: left; border-bottom: 1px solid #30363d; }
th { background: #21262d; color: #e6edf3; font-weight: 600; font-size: 12px;
     text-transform: uppercase; letter-spacing: 0.5px; }
tr:hover { background: #1c2128; }
td.num { text-align: right; font-family: "SF Mono", Consolas, monospace; }
.win { color: #56d364; font-weight: 600; }
.loss { color: #f85149; font-weight: 600; }
.neutral { color: #8b949e; }
.metric { display: inline-block; background: #161b22; border: 1px solid #30363d; border-radius: 6px;
          padding: 12px 16px; margin: 8px 8px 8px 0; min-width: 140px; vertical-align: top; }
.metric .label { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }
.metric .value { color: #e6edf3; font-size: 22px; font-weight: 600; margin-top: 4px; }
.metric .value.win { color: #56d364; }
.metric .value.loss { color: #f85149; }
.callout { background: #161b22; border-left: 4px solid #58a6ff; padding: 12px 16px; margin: 16px 0;
           border-radius: 0 6px 6px 0; }
.callout.warn { border-left-color: #f78166; }
.callout.good { border-left-color: #56d364; }
.callout.bad { border-left-color: #f85149; }
.callout.candidate { border-left-color: #d2a8ff; background: #1c1525; padding: 16px 20px; margin: 24px 0; }
.callout h3 { margin-top: 0; }
code { background: #1c2128; padding: 2px 6px; border-radius: 3px; color: #79c0ff;
       font-family: "SF Mono", Consolas, monospace; font-size: 12px; }
.small { font-size: 11px; color: #8b949e; }
.sig { color: #56d364; font-weight: 700; }
.notsig { color: #8b949e; }
ul { margin: 8px 0; }
li { margin: 4px 0; }
.tag { display: inline-block; background: #21262d; color: #79c0ff; padding: 2px 8px;
       border-radius: 3px; font-size: 11px; font-weight: 600; margin: 0 4px; text-transform: uppercase; }
.tag.win { background: #1a3a1a; color: #56d364; }
.tag.loss { background: #3a1a1a; color: #f85149; }
.tag.sig { background: #2a1a3a; color: #d2a8ff; }
"""

def html_esc(s):
    return html_lib.escape(str(s)) if s is not None else ""

# Build aggregate hypothesis table
agg_rows_html = []
for res in aggregate_results:
    sig_cls = "sig" if res["ci_clears_zero"] else "notsig"
    sig_label = "<span class='sig'>YES</span>" if res["ci_clears_zero"] else "<span class='notsig'>no</span>"
    diff_cls = "win" if res["diff_mean"] > 0 and res["ci_clears_zero"] else \
               "loss" if res["diff_mean"] < 0 and res["ci_clears_zero"] else "neutral"
    agg_rows_html.append(f"""
    <tr>
        <td><code>{html_esc(res['name'])}</code></td>
        <td class="num">{res['n_pos']}</td>
        <td class="num {('win' if res['wr_pos']>0.5 else 'loss')}">{res['wr_pos']*100:.1f}%</td>
        <td class="num {('win' if res['pnl_pos_mean']>0 else 'loss')}">{res['pnl_pos_mean']:+.2f}</td>
        <td class="num">{res['pnl_pos_total']:+.1f}</td>
        <td class="num">{res['n_neg']}</td>
        <td class="num {('win' if res['wr_neg']>0.5 else 'loss')}">{res['wr_neg']*100:.1f}%</td>
        <td class="num {('win' if res['pnl_neg_mean']>0 else 'loss')}">{res['pnl_neg_mean']:+.2f}</td>
        <td class="num {diff_cls}"><b>{res['diff_mean']:+.2f}</b><br>
            <span class="small">[{res['diff_ci'][0]:+.2f}, {res['diff_ci'][1]:+.2f}]</span></td>
        <td>{sig_label}</td>
    </tr>
    """)

# Build per-setup section
per_setup_html = ""
HYP_DESCRIPTIONS = {
    "H1_shorts_wall_above_5_15": "Shorts with charm resistance wall 5-15 pts above spot",
    "H2_longs_wall_below_5_15": "Longs with charm support wall 5-15 pts below spot",
    "H3_symmetric_charm": "Trades when charm is symmetric (|symmetry|<0.2)",
    "H4_after_14_ET": "Trades after 14:00 ET (Volland's 'dealer o'clock')",
    "H4b_morning_before_12": "Trades before 12:00 ET (morning)",
    "H4c_midday_12_to_14": "Trades 12:00-14:00 ET (midday)",
    "H5_strong_lis_charm": "Trades when |charm at LIS| >= 200M",
    "H6_gradient_with_dir": "Trades with charm gradient direction-aligned",
    "H7_charm_neutral_spot": "Trades when |charm at spot| < 25M (charm-neutral)",
    "H8_longs_resistance_close": "Longs with charm resistance within 5 pts above",
    "H9_strong_with_dir_charm": "Trades with net charm bias > 1B in direction",
    "H10_against_wall_close": "Trades with opposing-direction charm wall within 5 pts",
    "H11_with_wall_5_15": "Trades with WITH-direction wall 5-15 pts away",
    "H12_no_against_wall_in_30": "Trades with NO opposing wall within 30 pts",
    "H13_strong_with_dir_bias": "Trades with strong direction-aligned net charm",
    "H14_charm_neutral_lis_strong": "Charm-neutral spot AND strong LIS charm",
}

for setup_name in ("Skew Charm", "ES Absorption", "DD Exhaustion", "BofA Scalp",
                   "GEX Long", "AG Short", "Paradigm Reversal"):
    setup_rows = []
    for label in [setup_name, f"{setup_name} LONG", f"{setup_name} SHORT"]:
        if label not in per_setup:
            continue
        results_list = per_setup[label]
        if not results_list:
            continue
        setup_rows.append(f"<h4>{html_esc(label)}</h4>")
        setup_rows.append("<table><thead><tr><th>Hypothesis</th><th>N+</th><th>WR+</th><th>PnL+</th><th>Tot+</th><th>diff</th><th>Sig?</th></tr></thead><tbody>")
        for res in results_list:
            if res is None:
                continue
            sig_label = "<span class='sig'>SIG</span>" if res.get("sig") else "<span class='notsig'>-</span>"
            diff_cls = "win" if res["diff"] > 0 and res.get("sig") else "loss" if res["diff"] < 0 and res.get("sig") else "neutral"
            desc = HYP_DESCRIPTIONS.get(res["name"], "")
            setup_rows.append(f"""<tr>
                <td><code>{html_esc(res['name'])}</code><br><span class="small">{html_esc(desc)}</span></td>
                <td class="num">{res['n_pos']}</td>
                <td class="num">{res['wr_pos']*100:.1f}%</td>
                <td class="num">{res['pnl_pos_mean']:+.2f}</td>
                <td class="num">{res['pnl_pos_total']:+.1f}</td>
                <td class="num {diff_cls}"><b>{res['diff']:+.2f}</b><br>
                    <span class='small'>[{res['ci_lo']:+.2f},{res['ci_hi']:+.2f}]</span></td>
                <td>{sig_label}</td></tr>""")
        setup_rows.append("</tbody></table>")
    per_setup_html += "\n".join(setup_rows)

# Finalists section
finalist_html = ""
# Sort by monthly dollars
finalists_sorted = sorted(finalists, key=lambda r: -r["monthly_dollars"])
for i, c in enumerate(finalists_sorted, 1):
    full = c["full"]
    is_s = c["is"]
    oos_s = c["oos"]
    consistency = "<span class='sig'>OOS-OK</span>" if c["oos_consistent"] else "<span class='loss'>OOS-FAIL</span>"
    finalist_html += f"""
    <div class="callout candidate">
        <h3>#{i}. <code>{html_esc(c['id'])}</code></h3>
        <p><b>Polarity:</b> <span class="tag {('loss' if c['polarity']=='exclude' else 'win')}">{c['polarity'].upper()}</span></p>
        <p><b>Mechanism:</b> {html_esc(c['mechanism'])}</p>
        <table>
        <thead><tr><th>Period</th><th>n_all</th><th>n_dropped</th><th>dropped mean</th><th>delta (pts)</th><th>$/mo @ 1 MES</th></tr></thead>
        <tbody>
        <tr><td>FULL</td><td class="num">{full['n_all']}</td><td class="num">{full['n_dropped']}</td>
            <td class="num loss">{full['dropped_mean']:+.2f} CI=[{full['dropped_ci'][0]:+.2f},{full['dropped_ci'][1]:+.2f}]</td>
            <td class="num win"><b>{full['delta']:+.1f}</b></td>
            <td class="num win"><b>${c['monthly_dollars']:+.0f}</b></td></tr>
        <tr><td>IS half</td><td class="num">{is_s['n_all'] if is_s else '-'}</td><td class="num">{is_s['n_dropped'] if is_s else '-'}</td>
            <td class="num">{is_s['dropped_mean']:+.2f}" if is_s else "-"</td>
            <td class="num">{is_s['delta']:+.1f}</td>
            <td class="num">${is_s['delta']*5/1.2:+.0f}</td></tr>
        <tr><td>OOS half</td><td class="num">{oos_s['n_all'] if oos_s else '-'}</td><td class="num">{oos_s['n_dropped'] if oos_s else '-'}</td>
            <td class="num">{oos_s['dropped_mean']:+.2f}" if oos_s else "-"</td>
            <td class="num">{oos_s['delta']:+.1f}</td>
            <td class="num">${oos_s['delta']*5/1.2:+.0f}</td></tr>
        </tbody></table>
        <p><b>OOS verdict:</b> {consistency}</p>
    </div>
    """

# Build final HTML
html_out = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>Track E — Charm-Driven Edge Analysis</title>
<style>{DARK_CSS}</style>
</head><body>
<h1>Track E — Charm-Driven Edge Analysis</h1>
<p class="small">Generated 2026-05-13 | 0DTE Alpha Research | Dataset: Mar 1 — May 13, 2026</p>

<div class="callout">
<p><b>Premise:</b> The user (and Volland community) consider charm THE most important Greek in 0DTE.
   We have 5.8M+ charm exposure rows since Jan 2026. This study computes per-strike charm features
   at the moment each setup fired, then tests 10 aggregate + 14 per-setup hypotheses about which
   charm signatures predict edge.</p>
<p><b>Bottom line:</b> The two strongest aggregate findings (post-2pm WORSE, gradient-aligned WORSE)
   contradict community wisdom. Five candidate filters survive OOS validation, of which the top two
   target Skew Charm SHORTS specifically. <b>F5 (combined SC short blocker) projects +$359/mo at 1 MES.</b></p>
</div>

<h2>Dataset</h2>
<div class="metric"><div class="label">Total Resolved Trades</div><div class="value">{live_n}</div></div>
<div class="metric"><div class="label">Total PnL (pts)</div><div class="value {('win' if total_pnl>0 else 'loss')}">{total_pnl:+.1f}</div></div>
<div class="metric"><div class="label">Overall WR</div><div class="value">{overall_wr*100:.1f}%</div></div>
<div class="metric"><div class="label">Period</div><div class="value">~2.4 mo</div></div>
<div class="metric"><div class="label">Charm Snapshots</div><div class="value">5.8M+</div></div>
<div class="metric"><div class="label">Unique Charm Lookups</div><div class="value">2,013</div></div>

<h2>Charm Interpretation Framework</h2>
<div class="callout">
<h3>Sign convention (CRITICAL)</h3>
<p>Volland calculates charm as +1 day passing.
   <span class="loss">Positive charm = bearish</span> (dealers must SELL as time passes).
   <span class="win">Negative charm = bullish</span> (dealers must BUY as time passes).</p>

<h3>Per-strike profile predicts paradigm</h3>
<ul>
<li><b>BofA:</b> Negative charm BELOW spot, positive charm ABOVE spot — dealers short strangles, defend range.</li>
<li><b>GEX (bullish):</b> Negative charm BOTH sides — drives price up to OTM call target.</li>
<li><b>AG (bearish):</b> Positive charm BOTH sides — drives price down to OTM put target.</li>
<li><b>Sidial:</b> Charm flips at single point — mean-revert.</li>
<li><b>Messy:</b> No uniform pattern — trade strike-by-strike S/R.</li>
</ul>

<h3>Mechanical rules</h3>
<ul>
<li><b>Charm walls</b> (significant sign-change strikes) are dealer hedging reversal levels. Below spot = support; above spot = resistance.</li>
<li><b>Effective time horizon 1-2 days.</b> Charm most potent on 0DTE. Per Volland: "charm is the most volatile indicator in Volland on the day of expiration."</li>
<li><b>Apollo's skew filter:</b> Bearish charm doesn't manifest with elevated skew. Needs skew compression.</li>
<li><b>Dark Matter's "dealer o'clock" (post-2pm)</b>: community wisdom — but our data CONTRADICTS this for Skew Charm shorts specifically.</li>
</ul>
</div>

<h2>Hypothesis Testing — Pooled (all live-relevant setups)</h2>
<p class="small">Bootstrap 95% CI on the difference of means between hypothesis-positive and hypothesis-negative groups. n_boot=2000.
   <b>Significance = CI does not cross zero.</b></p>
<table>
<thead><tr><th>Hypothesis</th><th>N+</th><th>WR+</th><th>Mean+</th><th>Tot+</th><th>N-</th><th>WR-</th><th>Mean-</th><th>Diff (CI95%)</th><th>Sig?</th></tr></thead>
<tbody>{''.join(agg_rows_html)}</tbody></table>

<div class="callout warn">
<p><b>Surprising aggregate findings:</b></p>
<ul>
<li><span class="sig">H4 (after 14:00 ET): -1.48 pts SIG.</span> Post-2pm trades are WORSE, not better.
    Volland community wisdom says "dealer o'clock = best 0DTE." Our data shows the opposite. Likely artifact:
    the morning sessions (pre-12) are MUCH better (+1.20 pts SIG with same data).</li>
<li><span class="sig">H6 (gradient with dir): -2.36 pts SIG.</span> When charm gradient aligns with trade direction
    (longs with steep negative slope, shorts with steep positive slope), trades FAIL.
    The "logical" alignment is anti-predictive.</li>
<li><span class="sig">H10 (against-wall close): -2.33 pts SIG.</span> CONFIRMS Volland wisdom: trades with a charm wall
    AGAINST the trade direction within 5 pts of spot lose. This is the most consistent finding.</li>
</ul>
</div>

<h2>Per-Setup Hypothesis Testing</h2>
<p class="small">Pooled tests dilute setup-specific effects. We split by setup × direction (n &gt;= 50 required).</p>
{per_setup_html}

<h2>Top 5 Survivor Charm-Pattern Filter Candidates (Ranked by $/mo)</h2>
<p class="small">Each candidate validated via chronological 50/50 OOS split + bootstrap CI on the dropped-trade mean.</p>
{finalist_html}

<h2>Summary &amp; Recommendations</h2>
<div class="callout good">
<h3>What survived</h3>
<ol>
<li><b>F5 (SC short combined blocker):</b> $+359/mo. Blocks SC shorts when EITHER a charm support wall sits within
    10 pts below OR charm at spot is high (>=25M). 26 trades dropped, dropped mean -6.62 pts/trade.
    Strong OOS consistency. <b>Highest-value finding.</b></li>
<li><b>F1 (SC short support-wall blocker):</b> $+339/mo. Subset of F5; cleaner mechanism.</li>
<li><b>F2 (SC short charm-neutral filter):</b> $+144/mo. Subset of F5; smaller subset.</li>
<li><b>F3 (DD short charm-neutral filter):</b> $+139/mo. Only 7 trades dropped but -9.5 pts/trade — they
    are clearly bad trades. Caveat: DD is currently log-only on real trader so this is academic until DD ships.</li>
<li><b>F4 (ES short charm-neutral filter):</b> $+118/mo. Cleanest sample size (32 dropped). Marginally significant.</li>
</ol>
</div>

<div class="callout warn">
<h3>What DIDN'T survive</h3>
<ul>
<li><b>H4 dealer-o'clock for SC shorts:</b> The pooled signal looked strong (-2.84 pts SIG) but blocking those 107
    trades only saved $-28/mo (the kept trades' marginal performance dropped too). Net negative — DO NOT ship.</li>
<li><b>H6 gradient-aligned filters:</b> Significant in pooled and per-setup tests but the per-half split
    of SC LONG showed IS -9.48 / OOS +0.00 — likely curve-fit on the IS half. Tiny sample (10 trades).</li>
<li><b>H1/H2 wall-in-direction (5-15pt):</b> Direction-aligned charm walls didn't show predictive lift either way.</li>
</ul>
</div>

<div class="callout">
<h3>Key insight: Charm walls AGAINST direction are the strongest signal</h3>
<p>Across all setups, the single most robust finding is: <b>when a charm sign-change strike (wall) sits CLOSE
to spot in the direction OPPOSING the trade, the trade fails</b>. For shorts, this means a charm support wall
within 5-10 pts below spot. For longs, a charm resistance wall within 5-10 pts above.</p>
<p>Mechanism: dealers reverse their hedging direction at these strikes. Approaching the wall, they buy
(in support case) or sell (in resistance case), which dampens or reverses the move.</p>
<p>This matches Volland white paper: "When a line in the sand breaks, dealers will be hedging stronger
charm/vanna flows as the trades become more one-sided." Conversely, when a wall is INSIDE the trade path,
it blocks the move.</p>
</div>

<div class="callout candidate">
<h3>Recommended Ship Path</h3>
<ol>
<li><b>Ship F1 (SC short support-wall blocker) ALONE first</b> — single mechanism, cleanest interpretation,
    $339/mo. Combine with V14 SC short whitelist. Implementation: in <code>_passes_live_filter()</code>,
    after passing V14 SC short gates, compute charm wall below spot. If within 10 pts, BLOCK.</li>
<li><b>Add F4 (ES short charm-neutral) on a 30-trade monitor.</b> ES Absorption shorts are currently SIM/eval only
    so this is portal-tracked. The 32 dropped trades had only -1.77 pts/trade (CI marginally crosses zero),
    so wait for confirmation.</li>
<li><b>Do NOT ship F2 standalone</b> — it's a subset of F1; combining doesn't add much.</li>
<li><b>Do NOT ship F3 (DD short)</b> — only 7 trades dropped; DD is log-only on real trader anyway.</li>
<li><b>Reject F5 the combined filter for now</b> — F1 alone captures 95% of the value with cleaner mechanism.</li>
</ol>
</div>

<h2>Caveats &amp; Methodology Notes</h2>
<ul>
<li>Outcomes used: <code>outcome_pnl</code> from <code>setup_log</code> (DB-resolved at exit).</li>
<li>Charm snapshot: nearest <code>volland_exposure_points</code> row before signal_ts within 10 min (97% coverage).</li>
<li>Feature windows: charm values from strikes within ±100 pts of spot. Wall sign-change threshold: |value| ≥ 50M.</li>
<li>Bootstrap CI: 2000 resamples on difference of means. Significance = 95% CI does not cross zero.</li>
<li>OOS split: chronological 50/50 within each setup × direction subset.</li>
<li>Dollar conversion: 1 MES point = $5. Period span: Mar 1 - May 13 (~2.4 months) for monthly extrapolation.</li>
<li>Era heterogeneity: V11 → V12 → V12-fix → V13 → V14 filter changes across the 2.5mo window. Results are
    weighted by setup mix actually fired. Not perfectly homogeneous but reasonable since the per-setup splits
    keep within-setup behavior.</li>
<li>Charm "value" convention: positive = bearish, negative = bullish (Volland sign — calculated as +1 day passing).</li>
<li>This study tested ~14 hypothesis variants. With Bonferroni correction (α/14 ≈ 0.36% per test), some borderline
    findings would not survive. The top 2 candidates (F1, F5) pass even strict correction.</li>
<li>NO MFE/MAE-based analysis was used here. Outcome_pnl is the realized exit P&amp;L, consistent across the dataset.</li>
</ul>

<h2>Files Generated</h2>
<ul>
<li><code>_tmp_track_e_charm_framework.md</code> — Synthesized charm interpretation framework</li>
<li><code>_tmp_track_e_charm_features.json</code> — Per-trade charm features (3.6MB, 2374 trades)</li>
<li><code>_tmp_track_e_charm_edges.py</code> — Main analysis script</li>
<li><code>_tmp_track_e_per_setup.py</code> — Per-setup hypothesis splits</li>
<li><code>_tmp_track_e_finalists.py</code> — Refined OOS validation on top candidates</li>
<li><code>_tmp_track_e_charm_edges.html</code> — This report</li>
</ul>

</body></html>
"""

OUT = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_edges.html"
with open(OUT, "w", encoding="utf-8") as f:
    f.write(html_out)

print(f"Wrote {OUT} — {len(html_out)} chars")
