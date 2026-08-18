# -*- coding: utf-8 -*-
OUT=r"C:/Users/Faisa/Downloads/basket_SB_CORRECTED.html"
css="""body{background:#0d1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:900px;margin:0 auto;padding:24px;line-height:1.55}
h1{font-size:22px;margin-bottom:2px} h2{color:#58a6ff;border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:22px;font-size:16px}
.sub{color:#8b949e;font-size:12.5px;margin-top:0}
table{width:100%;border-collapse:collapse;font-size:12.5px;margin:8px 0} td,th{border:1px solid #30363d;padding:5px 8px;text-align:right} th{background:#161b22;color:#8b949e} td:first-child,th:first-child{text-align:left}
.good{color:#3fb950;font-weight:600}.bad{color:#f85149;font-weight:600}.warn{color:#d29922}.mut{color:#8b949e}
.hl{background:#21262d} .rec{background:#0f2417;border:1px solid #238636;border-radius:8px;padding:12px 16px;margin:10px 0}
.warnbox{background:#2d2212;border:1px solid #9e6a03;border-radius:8px;padding:10px 14px;margin:10px 0;font-size:12.5px}
code{background:#161b22;padding:1px 5px;border-radius:4px;color:#79c0ff} ul{margin:6px 0}"""
def row(c,cls=""): return "<tr"+(f' class="{cls}"' if cls else "")+">"+"".join(f"<td>{x}</td>" for x in c)+"</tr>"
H=f"""<!doctype html><html><head><meta charset="utf-8"><title>Basket SB — CORRECTED</title><style>{css}</style></head><body>
<h1>✅ Basket SB — CORRECTED &amp; portal-verified study</h1>
<p class="sub">Supersedes the earlier report (it used a wrong baseline). Verified: my V16-base = portal CSV trade_log_2026-06-24 EXACTLY (120 trades / −231.1 pts / 0 P&amp;L mismatch). Chain pts @1MES · Mar 27–Jun 24 · 794 V16 trades · baseline +1,639.</p>

<div class="warnbox"><b>What was wrong before:</b> the prior report used <code>live_pass=true</code> as "baseline" — but <code>live_pass</code> IS V16-SB (gate already applied), so it compared SB to itself. Rebuilt on the true V16-base (<code>passes_v16</code>, after a Vanna-Pivot-Bounce drift fix). Conclusion <b>reversed</b>.</div>

<h2>1 · FILTER effect — which trades to take (1× sizing)</h2>
<table><tr><th>scheme</th><th>pts</th><th>vs base</th><th>capAdj</th><th>WR</th><th>maxDD$</th></tr>
{row(['baseline (take all)','1,639','1.00×','1.00×','53%','2,044'],'hl')}
{row(['<b>open 0/0/1 (the shipped gate)</b>','1,606','<b>0.98×</b>','2.91×','<span class=good>66%</span>','<span class=good>198</span>'])}
{row(['mom 0/0/1','887','<span class=bad>0.54×</span>','2.44×','61%','349'])}
</table>
<p class="mut">Open is the better <b>filter</b>: ~same return as no-gate but <b>~1/10 the drawdown</b> ($2,044→$198) and 66% WR. Skipping the trades the basket fights is what crushes drawdown. Momentum is a <b>worse</b> filter (0.54×). The gate is DEFENSIVE — same money, far safer.</p>

<h2>2 · SIZING effect — how much to bet (on the open filter)</h2>
<table><tr><th>scheme</th><th>pts</th><th>vs base</th><th>capAdj</th><th>WR</th><th>maxDD$</th></tr>
{row(['open 0/0/1 (no sizing)','1,606','0.98×','2.91×','66%','198'])}
{row(['<b>open 0/1/2 (filter + 2× confirmed)</b>','<b>3,700</b>','<b>2.26×</b>','2.28×','61%','<span class=good>239</span>'],'hl')}
{row(['mom 0/1/2 (for reference)','2,556','1.56×','1.58×','56%','<span class=bad>1,068</span>'])}
</table>
<p class="mut"><b>open 0/1/2 dominates:</b> 2.26× the money at tiny drawdown ($239). Filter gives safety, 2×-confirmed sizing gives return, and they don't fight. Beats mom 0/1/2 on <b>both</b> return and drawdown.</p>

<h2>3 · Does the gate actually work live?</h2>
<ul>
<li><b>Yes — it saved June with REAL live data:</b> June base −243 pts → <b>+95 pts</b> with the actual stamped basket (+362 ideal). Gate-active era (Jun-16+) it blocked 78 trades and cut the book from −250 to −96 pts.</li>
<li><b>Live capture is healthy</b> (~390 rows/day, all 6 names since Jun-11). <b>0 fail-opens / 0 bypasses since the gate went live → $0 cost.</b></li>
</ul>

<h2>4 · Recommendation</h2>
<div class="rec">
<b>① Keep the open 0/0/1 filter</b> (already shipped) — it's the validated best filter: defensive, 66% WR, 1/10 drawdown.<br><br>
<b>② Add 2×-confirmed sizing → open 0/1/2</b> (skip fight · 1× neutral · 2× confirmed). This is the real upgrade: 2.26× return at ~same tiny drawdown. <b>Whole-contract feasible at 1 MES</b> (2 on confirmed / 1 on neutral / skip fight).<br><br>
<b>③ Drop momentum</b> — worse filter AND worse filter+sizing than open.
</div>

<h2>5 · Caveats</h2>
<p class="mut">Backtest uses a complete (yfinance, 1h) basket → idealized; open 0/1/2's $239 drawdown is optimistic vs the sparser live feed (live June save was +95, not +362). 2× sizing is real leverage → larger live swings. Sim chain pts; real capture ~85–89%. Deploy sizing only after forward-validating on the live (now-healthy) capture, and scale gradually.</p>
<p class="mut" style="border-top:1px solid #30363d;padding-top:8px;margin-top:16px">0DTE Alpha · Basket SB corrected · portal-verified 2026-06-25</p>
</body></html>"""
open(OUT,"w",encoding="utf-8").write(H); print("wrote",len(H),"bytes")
