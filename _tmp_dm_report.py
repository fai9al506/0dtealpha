# -*- coding: utf-8 -*-
"""Generate the Dark Matter framework study + backtest HTML report."""
import json, io, base64
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

weekly = json.load(open("_tmp_weekly_align.json"))

BG="#0e1117"; CARD="#161b22"; FG="#e6edf3"; MUT="#8b949e"; GRN="#3fb950"; RED="#f85149"; BLU="#58a6ff"; YEL="#d29922"; PUR="#bc8cff"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":CARD,"savefig.facecolor":BG,
    "text.color":FG,"axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,
    "axes.edgecolor":"#30363d","font.size":10,"axes.titlecolor":FG})

def b64(fig):
    bio=io.BytesIO(); fig.savefig(bio,format="png",dpi=110,bbox_inches="tight"); plt.close(fig)
    return base64.b64encode(bio.getvalue()).decode()

# Chart 1: weekly long/short/total PnL colored by his vol regime
fig,ax=plt.subplots(figsize=(10,4))
wk=[w["week"][5:] for w in weekly]
tot=[w["total_pnl"] for w in weekly]
cols=[RED if w["his_vol"]=="EXTREME" else (BLU if w["his_bias"] in("SHORT","SHORT/RANGE") else GRN) for w in weekly]
ax.bar(wk,tot,color=cols)
ax.axhline(0,color=MUT,lw=.8)
for i,w in enumerate(weekly):
    ax.annotate(w["his_bias"],(i,tot[i]),ha="center",va="bottom" if tot[i]>=0 else "top",fontsize=7,color=MUT,rotation=0)
ax.set_title("Our system's weekly P&L ($ @1MES, quality-traded set)  vs  Dark Matter's weekly bias")
ax.set_ylabel("Our P&L $")
c1=b64(fig)

# Chart 2: regime x side
fig,ax=plt.subplots(figsize=(7,4.2))
groups=["NORMAL\n(VIX<20)","EXTREME\n(VIX>=20)"]
longs=[6535,3657]; shorts=[1152,4794]
x=range(len(groups)); wbar=0.36
ax.bar([i-wbar/2 for i in x],longs,wbar,label="LONGS",color=GRN)
ax.bar([i+wbar/2 for i in x],shorts,wbar,label="SHORTS",color=RED)
ax.set_xticks(list(x)); ax.set_xticklabels(groups)
for i,v in enumerate(longs): ax.annotate(f"${v:,}",(i-wbar/2,v),ha="center",va="bottom",fontsize=8)
for i,v in enumerate(shorts): ax.annotate(f"${v:,}",(i+wbar/2,v),ha="center",va="bottom",fontsize=8)
ax.set_title("Full-history P&L by VIX regime x side  (the validated edge)")
ax.set_ylabel("$ @1MES"); ax.legend()
c2=b64(fig)

# Chart 3: refuted ideas (down-block by month) to show honesty
fig,ax=plt.subplots(figsize=(9,3.6))
mos=["Feb","Mar","Apr","May","Jun"]
blockval=[198,945,79,545,-811]
ax.bar(mos,blockval,color=[GRN if v>0 else RED for v in blockval])
ax.axhline(0,color=MUT,lw=.8)
ax.set_title("Why a 'block dip-longs on down days' gate is REJECTED: it only helps June")
ax.set_ylabel("$ blocked-bucket would lose")
c3=b64(fig)

rows="".join(
 f"<tr><td>{w['week']}</td><td><b style='color:{'#f85149' if w['his_vol']=='EXTREME' else '#58a6ff'}'>{w['his_bias']}</b></td>"
 f"<td>{w['his_vol']}</td><td>{w['vix_avg']}</td>"
 f"<td style='color:{'#3fb950' if w['long_pnl']>=0 else '#f85149'}'>${w['long_pnl']:,}</td>"
 f"<td style='color:{'#3fb950' if w['short_pnl']>=0 else '#f85149'}'>${w['short_pnl']:,}</td>"
 f"<td style='color:{'#3fb950' if w['total_pnl']>=0 else '#f85149'}'><b>${w['total_pnl']:,}</b></td>"
 f"<td style='color:#8b949e'>{w['note']}</td></tr>"
 for w in weekly)

html=f"""<!doctype html><html><head><meta charset="utf-8"><title>Dark Matter Framework Study</title>
<style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1000px;margin:0 auto;padding:28px;line-height:1.55}}
h1{{font-size:26px;margin:0 0 4px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:34px}}
h3{{color:{PUR};margin-top:22px}}
.sub{{color:{MUT};margin-bottom:18px}}
.card{{background:{CARD};border:1px solid #30363d;border-radius:10px;padding:16px 20px;margin:14px 0}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin:10px 0}}
td,th{{border:1px solid #30363d;padding:6px 8px;text-align:left}} th{{background:#1c2230;color:{MUT}}}
img{{width:100%;border-radius:8px;margin:10px 0;border:1px solid #30363d}}
.k{{color:{GRN};font-weight:700}} .b{{color:{RED};font-weight:700}}
.big{{font-size:19px;color:{YEL};font-weight:700}}
ul li{{margin:5px 0}} .pill{{display:inline-block;background:#1c2230;border:1px solid #30363d;border-radius:20px;padding:2px 10px;font-size:12px;color:{MUT};margin:2px}}
blockquote{{border-left:3px solid {BLU};margin:10px 0;padding:4px 14px;color:{MUT};background:#11161f}}
</style></head><body>

<h1>📘 Dark Matter Framework — Study, Backtest &amp; Action Plan</h1>
<div class="sub">8 weekly plans studied (Apr 13 → Jun 8 2026) · validated against our full-history DB (Feb–Jun, 3,568 outcomes) · generated for Faisal</div>

<div class="card">
<div class="big">Bottom line in one paragraph</div>
Dark Matter doesn't predict direction — he <b>classifies the dealer/vol regime first, then picks the matching mode</b>.
7 of his 8 weeks were low-vol "positive-gamma" regimes → <span class="k">RANGE / buy-dips / fade-rips</span> — which is <i>exactly</i> what
our system already does, and those are the weeks <b>we make money</b> (+$330 to +$2,472/wk). Only the 6/8 week flipped to
<span class="b">EXTREME-vol "amplification"</span> → <b>short / fade-rallies</b> — and that's the only week <b>we bled</b>.
The data confirms the mechanism: in high-vol regimes the <b>short side roughly quadruples</b> (see chart 2). Our gap is not that our
longs are wrong — it's that <b>we have no regime switch and almost no short supply</b> when vol regime flips. That switch is the edge.
</div>

<h2>1 · How he sees the market (plain English)</h2>
<p>Every plan is built in the same order. He never starts from "I think it goes up/down." He starts from <b>who is in control — dealers or the tape</b>:</p>

<h3>Layer 1 — Regime classification (the master switch)</h3>
<ul>
<li><b>Spot-vol beta</b> — his first read. If spot and vol move the <i>same</i> direction, dealers are hedging reactively → <span class="b">they're "passengers," moves amplify</span>. If normal/negative, dealers are in control → mean-reversion works.</li>
<li><b>Gamma sign</b> — <span class="k">positive gamma = pin/chop</span> (dealers sell rips, buy dips → fade both ways). <span class="b">Negative gamma = trend</span> (ranges extend, levels break).</li>
<li><b>Vol regime vs baseline</b> — he ranks today's IV against its own 6-month percentile. Below ~90th pct = <span class="k">NORMAL/LOW</span> (classical). Above = <span class="b">EXTREME</span>, and the <b>polarity inverts</b>: positive vanna <i>above</i> spot stops being a magnet and becomes <b>resistance</b>; negative vanna below becomes support. <i>(6/8: IV 21.22 vs baseline 17.58 → EXTREME.)</i></li>
<li><b>Catalyst window</b> — earnings/CPI/FOMC/geopolitics override everything: "assume dealer flow is secondary until the dust settles." Scheduled = plan around; unscheduled shock = react, don't predict.</li>
</ul>

<h3>Layer 2 — Levels (where, not which way)</h3>
<p>He overlays Volland Greek levels (gamma/vanna/charm walls) on <b>volume-profile value areas + prior-week highs/lows</b>. Confluence = strength, tagged by term-stack (<span class="pill">[0]</span> 0DTE <span class="pill">[W]</span> weekly <span class="pill">[M]</span> monthly <span class="pill">[L]</span> LEAPS — a <code>[0+W+M+L]</code> quad-stack is the strongest). He <b>weights</b> each level by vol + catalyst: <i>"no catalyst + low vol → the level holds; high vol + catalyst → it most likely won't."</i></p>

<h3>Layer 3 — Bias = a decision tree, never a fixed direction</h3>
<blockquote>"Keep bias unchecked." "Your bias has to be flexible." "Trade level to level."</blockquote>
<p>His plan is always "<i>above X → range A↔B; below X → waterfall to C</i>." He waits for a <b>15-minute rejection/hold confirmation at the level</b> before entering — he never front-runs. This confirmation step is <b>why his discretion works where a blind mechanical gate fails.</b></p>

<h3>His trade-plan template (the 6/8 example)</h3>
<p>Market Context (macro/catalyst chain) → Tiered Calendar (🔴🟠🟡) → Multi-Week Thesis Watch (levels graded A–D, confirmed vs observation) → Paradigm (dealer-hedging $ sign) → Vol-Regime (EXTREME/NORMAL + polarity rule) → Structural Ladder (levels w/ term-stack tags) → Setups A/B/C/D (each: thesis, entry, stop, targets, R:R, option expression, invalidation, grade, <b>size</b>) → Directional Bias callout → Quick-Reference cheat sheet.</p>
<p><b>Sizing discipline:</b> he half-sizes everything in "mixed-signal" regimes and quarter-sizes pure momentum-chase setups. On 6/8 <i>every</i> setup was ≤0.5×, and the only long (Setup B cushion-buy) was explicitly "scout-size, untested, intraday-only." He treats counter-trend longs in a bear regime as the <i>lowest</i>-conviction trade — the opposite of what our system did that week.</p>

<h2>2 · His 8-week bias vs reality vs OUR P&L</h2>
<p>His regime read drove a correct, flexible bias every week. The right-hand columns are <b>our</b> system's P&L (quality-traded, portal-sim, $@1MES). Note the alignment: we win in his RANGE/LONG weeks, we lose in his one SHORT/EXTREME week.</p>
<table><tr><th>Week</th><th>His bias</th><th>His vol</th><th>VIX avg</th><th>Our LONG $</th><th>Our SHORT $</th><th>Our TOTAL $</th><th>His read</th></tr>{rows}</table>
<img src="data:image/png;base64,{c1}">
<p style="color:{MUT};font-size:12px">Green = his range/long weeks (we're aligned, profitable). Red = his EXTREME-vol short week (6/8 — the only week our long-biased book lost). 5/25 SPX-move figure is a Memorial-Day short-week data artifact; ignore.</p>

<h2>3 · The validated edge — regime decides which SIDE pays</h2>
<p>Bucketing <b>every</b> quality-traded signal (full history) by VIX regime and side:</p>
<img src="data:image/png;base64,{c2}">
<table><tr><th>Regime</th><th>LONGS</th><th>SHORTS</th><th>Read</th></tr>
<tr><td><b style="color:{GRN}">NORMAL</b> (VIX&lt;20)</td><td class="k">+$6,535 · 59% WR · n=494</td><td>+$1,152 · 50% · n=427</td><td>Longs dominate → <b>our current edge fits</b></td></tr>
<tr><td><b style="color:{RED}">EXTREME</b> (VIX≥20)</td><td>+$3,657 · 62% · n=270</td><td class="b">+$4,794 · 57% · n=349</td><td>Short side ~4× bigger → <b>where we under-trade</b></td></tr></table>
<p>The edge is <b>not</b> "stop buying dips" — our longs held up in both regimes. It's that <b>the short side roughly quadruples in high-vol regimes</b> (driven by March's 251 shorts at 64% WR / +$5,324). We currently fire very few shorts (post-V16: 37 shorts vs 112 longs), so we leave most of the high-vol money on the table — and on a true amplification day (Jun 9) our long-only book actively bleeds.</p>

<h2>4 · What I tested and REJECTED (so we don't repeat the overfit)</h2>
<p>Honesty first — I tried the "obvious" fixes against full history and they fail:</p>
<img src="data:image/png;base64,{c3}">
<ul>
<li><span class="b">Block dip-longs on down days</span> (`spot ≥15pt below open`): looked great on the recent window (+$1,019) but it's <b>June-only overfit</b> — it would have <i>lost</i> money Feb–May (March −$945 of blocked winners). REJECTED.</li>
<li><span class="b">VIX-rising / spot-vol-beta gates</span>: same June-only pattern. REJECTED.</li>
<li><span class="b">Daily-loss cap (S208)</span> across full history: <b>loses at every level</b> ($300 −$446 … $150 −$1,387) because it blocks afternoon mean-reversion recoveries. Keep the $300 breaker as tail-risk insurance only. REJECTED as a profit lever.</li>
</ul>
<p><b>Lesson that matches Dark Matter exactly:</b> a blind mechanical regime <i>gate</i> doesn't work — because the same "down day" is a buyable dip in a bull-grind and a falling knife in an amplification regime. The difference is the <b>regime read + level confirmation</b>, which is discretionary. So we build <b>decision-support</b>, not an auto-block.</p>

<h2>5 · The plan — how we trade like him &amp; sharpen our edge</h2>
<div class="card">
<h3>Phase 1 (now, ~1 wk) — Regime Dashboard / Telegram read</h3>
A daily + intraday read, from data we already scrape, that prints the mode:
<ul>
<li><b>Vol regime:</b> VIX vs its 6-month percentile → <span class="k">NORMAL</span> / <span class="b">EXTREME</span> (his ≥90th-pct rule; practically VIX≥~20 + rising).</li>
<li><b>Gamma/paradigm sign</b> (Volland paradigm + delta-decay-hedging sign) → pin vs amplification.</li>
<li><b>Catalyst flag:</b> auto-tag scheduled CPI/PPI/NFP/FOMC days (known calendar) as "reduce size / expect break."</li>
<li>Output: <i>"MODE: NORMAL — mean-reversion on, longs primary"</i> vs <i>"MODE: EXTREME/AMPLIFICATION — fade rallies, longs scout-size, shorts primary."</i></li>
</ul>
This alone lets you (and later the system) <b>stop the Jun-9-style bleed</b> by not running a long-only book into an amplification day. No filter overfit — it's a state read you act on.
</div>
<div class="card">
<h3>Phase 2 (~2–3 wks) — Fade-the-Wall short setup</h3>
Build the short setup we're missing: <b>fire a short when price rallies into a strong overhead Volland resistance wall (vanna/gamma/charm) AND shows a 15-min rejection</b> — Dark Matter's Setup A. Today our only fade-short (AG Short) needs spot pinned right under LIS, so it almost never fires on the days it's needed. A multi-wall fade-short, <b>weighted to fire more in EXTREME regimes</b>, harvests the +$4,794 short bucket we currently miss. Forward-log it first (portal-only), validate ≥30 signals, then enable on TSRT.
</div>
<div class="card">
<h3>Phase 3 (ongoing) — Adopt his plan discipline</h3>
<ul>
<li><b>Half-size in mixed/EXTREME regimes</b> (he never full-sizes counter-trend in high vol).</li>
<li><b>Level-confirmation entries</b> (15-min rejection/hold) instead of firing on first touch.</li>
<li><b>Pre-map a decision tree each morning</b> ("above X range, below X waterfall") from our Volland levels — automatable as a daily brief.</li>
</ul>
</div>

<h2>6 · Projection (honest, ranged)</h2>
<table><tr><th>Scenario</th><th>Assumption</th><th>Incremental $/mo @1MES</th><th>Confidence</th></tr>
<tr><td>Keep current system</td><td>Low-vol edge intact (most months)</td><td>~$2,000 (baseline, unchanged)</td><td>High</td></tr>
<tr><td>+ Regime dashboard (defense)</td><td>Avoid 1–2 amplification bleed days/mo (~$300–700 each)</td><td>+$150–400 saved</td><td>Med (mechanism clear; discretionary)</td></tr>
<tr><td>+ Fade-the-wall shorts (offense)</td><td>Capture part of the high-vol short bucket; only pays in EXTREME months (~2 of 5 in our sample)</td><td>+$200–500 in EXTREME months, ~$0 in calm</td><td>Med-Low (needs setup built + 30-signal forward test)</td></tr>
</table>
<p style="color:{MUT}">I deliberately do <b>not</b> quote a single big number. His own $ P&L is not public; what's measurable is that the short side carried +$4,794 in high-vol regimes we barely traded, and our long-only book lost on the one amplification week. The realistic, defensible target is: <b>same ~$2k/mo in normal regimes, but turn the occasional −$300/day bleed days into flat-or-green, plus harvest shorts when vol spikes.</b> Convert to firm $ only after Phase-2 is built and forward-tested.</p>

<h2>7 · Caveats (validation protocol)</h2>
<ul>
<li>P&L above is portal-sim points × 5 ($@1MES); broker-realistic (mes_sim) runs a touch lower on big runners. Relative regime comparisons hold.</li>
<li>EXTREME-regime sample is March-heavy (April EXTREME shorts actually lost −$371) — the short edge is real but <b>noisy month-to-month</b>; that's why we forward-test before sizing up.</li>
<li>We do <b>not</b> capture his exact aggregate vanna/gamma hedging $; our classifier leans on VIX + Volland paradigm + DD-hedging sign, which is a coarser proxy.</li>
<li>His method is fundamentally <b>discretionary with level confirmation</b> — the win is decision-support + a richer toolkit, not a single auto-gate (every auto-gate I tested overfit to June).</li>
</ul>

<p style="color:{MUT};font-size:12px;margin-top:30px">Sources: 8 weekly HTML plans (Volland Discord ⚛️dark-matter-trade) · setup_log/real_trade_orders full history · scripts _tmp_dm_*.py, _tmp_fullhist_*.py, _tmp_s208_cap.py, _tmp_dm_regime_test.py. No numbers from memory — all from DB queries.</p>
</body></html>"""

open("daily_trade_logs/dark_matter_framework_study.html","w",encoding="utf-8").write(html)
print("wrote daily_trade_logs/dark_matter_framework_study.html  (", len(html), "bytes )")
