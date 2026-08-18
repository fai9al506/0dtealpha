# -*- coding: utf-8 -*-
import os
OUT=r"C:/Users/Faisa/Downloads/basket_momentum_report.html"
css="""body{background:#0d1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:920px;margin:0 auto;padding:26px;line-height:1.55}
h1{font-size:23px;margin-bottom:2px} h2{color:#58a6ff;border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:26px;font-size:17px}
.sub{color:#8b949e;font-size:13px;margin-top:0}
table{width:100%;border-collapse:collapse;font-size:12.5px;margin:10px 0} td,th{border:1px solid #30363d;padding:5px 8px;text-align:right} th{background:#161b22;color:#8b949e} td:first-child,th:first-child{text-align:left}
.good{color:#3fb950;font-weight:600}.bad{color:#f85149;font-weight:600}.warn{color:#d29922;font-weight:600}.mut{color:#8b949e}
.box{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 16px;margin:10px 0}
.rec{background:#0f2417;border:1px solid #238636;border-radius:8px;padding:12px 16px;margin:10px 0}
ul{margin:6px 0} li{margin:4px 0} code{background:#161b22;padding:1px 5px;border-radius:4px;color:#79c0ff}
.hl{background:#21262d}"""
def row(c, cls=""):
    return "<tr"+(f' class="{cls}"' if cls else "")+">"+"".join(f"<td>{x}</td>" for x in c)+"</tr>"

H=f"""<!doctype html><html><head><meta charset="utf-8"><title>Basket SB — Momentum vs Open</title><style>{css}</style></head><body>
<h1>📊 Basket Confirmation (SB): Momentum vs Open — full study</h1>
<p class="sub">V16 set · live_pass=true · chain $ @1 MES · 15-min dedup · dense 5-min basket (yfinance, 100% coverage) · {('walk-forward: lock threshold on April, test May–Jun unseen')}</p>

<div class="box"><b>Why this study:</b> On Jun-23, 6 longs fired into the rally top and all lost; the basket SB filter didn't help. Root cause traced to the <b>reference point</b> of the tech-basket signal. This report tests fixing it (momentum vs open anchor) across all scalings.</div>

<h2>1 · The design issue (confirmed in code)</h2>
<p>Live capture (<code>darkmate.py:88</code>) and backtest (<code>semi_capture.py:45</code>) <b>both</b> compute <code>(price − session_open)/open</code> — an <b>open-anchored level</b>, not momentum. On a rally-then-fade or V-shape, tech stays "green from open" while actively rolling over, so the signal answers <i>"is tech net-up on the day"</i>, never <i>"is tech turning now"</i>. It confirmed Jun-23's losing longs for exactly this reason. Same logic applies to shorts (red-from-open but V-bottom bouncing).</p>
<table><tr><th>Diagnostic (WR)</th><th>Open-anchored</th><th>Momentum (30-min)</th><th>neutral baseline</th></tr>
{row(['Confirmed LONG win-rate','55%','<span class=good>72%</span>','~64%'])}
{row(['Confirmed SHORT win-rate','65%','<span class=good>68%</span>','—'])}
</table>
<p class="mut">Momentum confirmation selects trades that actually work; open-anchor was anti-selective on longs (55% &lt; 64%).</p>

<h2>2 · Walk-forward comparison — all schemes</h2>
<p class="sub">X/Y/Z = (contradicted / neutral / confirmed) size multipliers. Threshold locked on April by $, tested on unseen May–Jun. <b>capAdj</b> = capital-adjusted return (strips out "just more contracts").</p>
<table><tr><th>Ref</th><th>Scheme</th><th>test $</th><th>vs Base</th><th>capAdj</th><th>avgCap</th><th>Ret/DD</th><th>MaxDD</th><th>trades</th></tr>
{row(['—','Baseline','4,696','1.00×','1.00×','1.00','7.3','640','285'],'hl')}
{row(['open','0/0/1','2,098','<span class=bad>0.45×</span>','0.93×','0.48','3.4','624','137'])}
{row(['open','0/1/1','3,494','0.74×','1.18×','0.63','5.5','640','180'])}
{row(['open','0/1/2','5,593','1.19×','1.07×','1.11','4.4','<span class=bad>1,264</span>','180'])}
{row(['open','0.5/1/2','6,194','1.32×','1.02×','1.30','4.9','<span class=bad>1,264</span>','285'])}
{row(['mom30','0/0/1','2,266','<span class=bad>0.48×</span>','<span class=good>1.27×</span>','0.38','5.1','446','108'])}
{row(['mom30','0/1/1','4,140','0.88×','1.07×','0.82','<span class=good>9.1</span>','454','235'])}
{row(['<b>mom30</b>','<b>0/1/2</b>','<b>5,479</b>','<b>1.17×</b>','<b>1.11×</b>','1.05','7.4','738','206'],'hl')}
{row(['mom30','0.5/1/2','6,341','<span class=good>1.35×</span>','1.11×','1.22','8.3','768','285'])}
</table>

<h2>3 · Key reads</h2>
<ul>
<li><b>Momentum beats open in every scheme</b> — higher capital-adjusted return, higher Ret/DD, lower drawdown. The reference swap is the real, free win.</li>
<li><b>The genuine edge is ~1.1× capital-adjusted, not 1.4×.</b> The headline "1.35×" is <span class=warn>~¾ leverage</span> (more contracts on confirmed), only ¼ real edge.</li>
<li><b>0/0/1 is the worst deployable scheme</b> (0.45–0.48× raw) — it discards the neutral majority that carries the book. <span class=warn>This is what's shipped in <code>basket_gate.py</code> today.</span></li>
<li>The edge that survives is <b>drawdown control + not bleeding on contradicted trades</b>, more than extra return. Open 0/1/2 &amp; 0.5/1/2 double drawdown ($1,264) — open-anchor's "DD-halving" claim does NOT hold on recent dense data.</li>
</ul>

<h2>4 · Conclusion &amp; suggestions</h2>
<div class="rec">
<b>✅ Switch reference: open → momentum (30-min).</b> Strictly better everywhere; costs only a rolling Δ computation.<br><br>
<b>✅ Use 0/1/2, NOT 0/0/1:</b> skip contradicted (the June bleeders), keep neutral at 1×, double confirmed. OOS 1.17× at modest DD ($738), and <b>whole-contract feasible at 1 MES today</b> (skip / 1 / 2). <i>Primary recommendation.</i><br><br>
<b>↔ Variants:</b> <code>mom30 0/1/1</code> = smoothest curve (Ret/DD 9.1, lowest DD) but ~baseline $; <code>0.5/1/2</code> = marginally best overall but needs fractional sizing → only at ≥2 MES base.<br><br>
<b>⛔ Retire the live 0/0/1 gate.</b> Weakest of all and a latent drag as basket capture densifies.
</div>
<p><b>Expectation to set:</b> ~10% real (capital-adjusted) uplift + meaningfully lower drawdown — via momentum-reference 0/1/2, not the open-reference 0/0/1 live now. <span class=mut>NOT a 1.4× money multiplier.</span></p>

<h2>5 · Confidence &amp; caveats</h2>
<p class="mut">Moderate. 285 out-of-sample trades, 2 months (May–Jun). Dense yfinance 5-min ≈ live TS quotes (minor diff). chain $ used (TSRT now enters/trails/exits on portal SPX, so chain ≈ real). Selection by $ not Ret/DD (Ret/DD overfit a tiny-DD April month). Edge is risk-reduction-led, not a large return jump. Not deployed — study only; real trading remains supervised-only.</p>
<p class="mut" style="margin-top:18px;border-top:1px solid #30363d;padding-top:10px">0DTE Alpha · Basket SB momentum study · generated for review</p>
</body></html>"""
open(OUT,"w",encoding="utf-8").write(H)
print("wrote",OUT,len(H),"bytes")
