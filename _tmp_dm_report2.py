# -*- coding: utf-8 -*-
"""Enhanced Dark Matter framework report v2 — adds worked trade examples +
illustration charts (regime contrast, his Jun-9 cascade call vs reality, level ladder)."""
import json, io, base64
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

weekly=json.load(open("_tmp_weekly_align.json"))
intr=json.load(open("_tmp_intraday.json"))

BG="#0e1117";CARD="#161b22";FG="#e6edf3";MUT="#8b949e";GRN="#3fb950";RED="#f85149";BLU="#58a6ff";YEL="#d29922";PUR="#bc8cff";ORG="#db6d28"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":CARD,"savefig.facecolor":BG,"text.color":FG,
 "axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,"axes.edgecolor":"#30363d","font.size":10,"axes.titlecolor":FG})
def b64(fig):
    bio=io.BytesIO();fig.savefig(bio,format="png",dpi=115,bbox_inches="tight");plt.close(fig);return base64.b64encode(bio.getvalue()).decode()

def series(day):
    s=intr[day]; x=list(range(len(s))); y=[p[1] for p in s]; lab=[p[0] for p in s]; return x,y,lab

# ---- Chart 1: regime contrast (Jun9 EXTREME vs May27 NORMAL) ----
fig,(a1,a2)=plt.subplots(1,2,figsize=(11,3.8))
x,y,lab=series("2026-06-09")
a1.plot(x,y,color=RED,lw=1.6)
a1.set_title("EXTREME / Amplification — Jun 9 (VIX>20)\n238-pt crash, levels BREAK")
a1.set_ylabel("SPX"); a1.set_xticks([0,len(x)//2,len(x)-1]); a1.set_xticklabels([lab[0],lab[len(x)//2],lab[-1]])
a1.annotate("open 7448",(0,y[0]),color=MUT,fontsize=8)
a1.annotate("low 7243",(y.index(min(y)),min(y)),color=RED,fontsize=8)
x2,y2,lab2=series("2026-05-27")
a2.plot(x2,y2,color=GRN,lw=1.6)
a2.set_title("NORMAL / Positive-gamma — May 27 (VIX~16)\n30-pt pin, levels HOLD (mean-revert)")
a2.set_xticks([0,len(x2)//2,len(x2)-1]); a2.set_xticklabels([lab2[0],lab2[len(x2)//2],lab2[-1]])
a2.set_ylim(min(y2)-15,max(y2)+15)
c_regime=b64(fig)

# ---- Chart 2: Jun 9 — his night-before cascade call vs reality ----
fig,ax=plt.subplots(figsize=(11,4.6))
x,y,lab=series("2026-06-09")
ax.plot(x,y,color=FG,lw=1.7,label="Actual SPX (Jun 9)")
# his pre-mapped levels (6/9 update + 6/8 plan)
his_levels=[(7500,"7500 broken-floor fade (Setup D)",ORG,"--"),
            (7390,"7390 — 'below this, selloff' (his 6/9 call)",YEL,":"),
            (7375,"7375 target 1 (delta pin)",YEL,":"),
            (7350,"7350 — 'critical, lose it = unwind fast'",ORG,"--"),
            (7290,"7290 target",RED,":"),
            (7200,"7200 next target",RED,":")]
for lvl,txt,col,ls in his_levels:
    if min(y)-20<=lvl<=max(y)+20:
        ax.axhline(lvl,color=col,ls=ls,lw=1.1,alpha=.9)
        ax.annotate(txt,(len(x)*0.62,lvl),color=col,fontsize=8,va="bottom")
ax.annotate("ACTUAL LOW 7243\n(between his 7290 & 7200 targets)",(y.index(min(y)),min(y)),
            color=RED,fontsize=9,fontweight="bold",xytext=(y.index(min(y))-55,min(y)+30),
            arrowprops=dict(arrowstyle="->",color=RED))
ax.set_title("WORKED EXAMPLE — Dark Matter's Jun-9 cascade call (posted the night BEFORE) vs what happened")
ax.set_ylabel("SPX"); ax.set_xticks([0,len(x)//2,len(x)-1]); ax.set_xticklabels([lab[0],lab[len(x)//2],lab[-1]])
ax.legend(loc="upper right")
c_jun9=b64(fig)

# ---- Chart 3: his level ladder (severity from Key Levels Table image) ----
ladder=[(7600,"[W+M+L]","Vanna cap",ORG),(7575,"[0+W+M+L]","Vanna WALL +$588M (strongest)",RED),
 (7539,"[chart]","5/31 VAH",BLU),(7500,"[W+M]","Gamma transit (conflict)",ORG),
 (7480,"[W]","Vanna repellent -$36M",YEL),(7450,"[W]","Vanna repellent",YEL),
 (7408,"[chart]","May POC (MR anchor)",BLU),(7375,"[0+W+M]","Delta PIN -$5.57B",RED),
 (7350,"[chart+W+M]","Gamma floor (cascade if broken)",ORG),(7277,"[chart]","5/4 VAL",BLU),
 (7100,"[0+W+M+L]","Vanna FLOOR (buy dips)",RED),(7000,"[0+W+M+L]","Delta FLOOR (catastrophic)",RED)]
fig,ax=plt.subplots(figsize=(10,5))
for lvl,tag,role,col in ladder:
    ax.axhline(lvl,color=col,lw=6,alpha=.85)
    ax.annotate(f"{lvl}  {tag}  ·  {role}",(0.01,lvl),fontsize=9,color=FG,va="center")
ax.axhline(7405,color=FG,lw=1,ls="--"); ax.annotate("  SPOT ~7405 (Sun eve)",(0.55,7405),color=FG,fontsize=9,fontweight="bold")
ax.set_ylim(6980,7615); ax.set_xlim(0,1); ax.set_xticks([])
ax.set_title("His 'Key Levels Table' rebuilt - severity = conviction (red = highest)\nRED 7575/7375/7100/7000  |  ORANGE 7600/7500/7350  |  YELLOW repellents  |  BLUE chart anchors")
c_ladder=b64(fig)

# ---- Chart 4: regime x side ----
fig,ax=plt.subplots(figsize=(7,4))
g=["NORMAL\n(VIX<20)","EXTREME\n(VIX>=20)"];L=[6535,3657];S=[1152,4794];x=range(2);w=.36
ax.bar([i-w/2 for i in x],L,w,label="LONGS",color=GRN);ax.bar([i+w/2 for i in x],S,w,label="SHORTS",color=RED)
ax.set_xticks(list(x));ax.set_xticklabels(g)
for i,v in enumerate(L):ax.annotate(f"${v:,}",(i-w/2,v),ha="center",va="bottom",fontsize=8)
for i,v in enumerate(S):ax.annotate(f"${v:,}",(i+w/2,v),ha="center",va="bottom",fontsize=8)
ax.set_title("Full-history P&L by VIX regime x side (the validated edge)");ax.set_ylabel("$ @1MES");ax.legend()
c_rs=b64(fig)

# ---- Chart 5: weekly bias vs our pnl ----
fig,ax=plt.subplots(figsize=(10,3.8))
wk=[w["week"][5:] for w in weekly];tot=[w["total_pnl"] for w in weekly]
cols=[RED if w["his_vol"]=="EXTREME" else GRN for w in weekly]
ax.bar(wk,tot,color=cols);ax.axhline(0,color=MUT,lw=.8)
for i,w in enumerate(weekly):ax.annotate(w["his_bias"],(i,tot[i]),ha="center",va="bottom" if tot[i]>=0 else "top",fontsize=7,color=MUT)
ax.set_title("Our weekly P&L ($@1MES) colored by his vol-regime — green=range/long weeks (aligned), red=EXTREME week (we bled)")
ax.set_ylabel("Our P&L $")
c_wk=b64(fig)

rows="".join(
 f"<tr><td>{w['week']}</td><td><b style='color:{RED if w['his_vol']=='EXTREME' else BLU}'>{w['his_bias']}</b></td>"
 f"<td>{w['his_vol']}</td><td>{w['vix_avg']}</td>"
 f"<td style='color:{GRN if w['long_pnl']>=0 else RED}'>${w['long_pnl']:,}</td>"
 f"<td style='color:{GRN if w['short_pnl']>=0 else RED}'>${w['short_pnl']:,}</td>"
 f"<td style='color:{GRN if w['total_pnl']>=0 else RED}'><b>${w['total_pnl']:,}</b></td></tr>" for w in weekly)

H=f"""<!doctype html><html><head><meta charset="utf-8"><title>Dark Matter Framework Study</title><style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial,sans-serif;max-width:1000px;margin:0 auto;padding:26px;line-height:1.55}}
h1{{font-size:25px;margin:0 0 4px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:34px}} h3{{color:{PUR}}}
.sub{{color:{MUT};margin-bottom:16px}} .card{{background:{CARD};border:1px solid #30363d;border-radius:10px;padding:14px 18px;margin:14px 0}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin:10px 0}} td,th{{border:1px solid #30363d;padding:6px 8px;text-align:left}} th{{background:#1c2230;color:{MUT}}}
img{{width:100%;border-radius:8px;margin:8px 0;border:1px solid #30363d}} .k{{color:{GRN};font-weight:700}} .b{{color:{RED};font-weight:700}}
.big{{font-size:18px;color:{YEL};font-weight:700}} .pill{{display:inline-block;background:#1c2230;border:1px solid #30363d;border-radius:20px;padding:2px 10px;font-size:12px;color:{MUT};margin:2px}}
blockquote{{border-left:3px solid {BLU};margin:10px 0;padding:4px 14px;color:{MUT};background:#11161f}}
.ex{{background:#11161f;border:1px solid #30363d;border-left:4px solid {PUR};border-radius:8px;padding:12px 16px;margin:12px 0}}
.tag{{font-size:11px;padding:1px 7px;border-radius:10px;font-weight:700}}
</style></head><body>

<h1>📘 Dark Matter Framework — Study, Worked Examples &amp; Action Plan</h1>
<div class="sub">8 weekly plans (Apr 13 → Jun 8 2026) · validated vs our full-history DB (Feb–Jun, 3,568 outcomes) · for Faisal</div>

<div class="card"><div class="big">The whole thing in one paragraph</div>
He never bets a direction — he <b>reads the dealer/vol regime first, then trades the matching mode</b>. In low-vol "positive-gamma"
weeks (7 of his 8) he does <span class="k">range / buy-dips / fade-rips</span> — literally what our system already does, and those are
the weeks <b>we make money</b>. In the one EXTREME-vol "amplification" week (6/8) he flips to <span class="b">short / fade-rallies</span>
— the only week <b>we bled</b>. The data proves the mechanism: in high-vol regimes the <b>short side roughly quadruples</b>. Our gap is
that we have <b>no regime switch and almost no short supply</b> when vol flips. Building that switch is the edge.</div>

<h2>1 · Two pictures that explain everything</h2>
<p>Same market, two regimes. On the left, an EXTREME/amplification day: dealers amplify, levels break, price travels 238 pts. On the
right, a normal positive-gamma day: dealers pin, levels hold, price oscillates 30 pts and mean-reverts. <b>Our buy-the-dip system is
built for the right-hand world and gets run over in the left.</b> Dark Matter's whole skill is knowing which world he's in before the open.</p>
<img src="data:image/png;base64,{c_regime}">

<h2>2 · How he sees the market (plain English)</h2>
<p>Every plan is built in the same order — control first, direction last:</p>
<div class="card"><b>Layer 1 — Regime (the master switch)</b><ul>
<li><b>Spot-vol beta:</b> spot &amp; vol moving together → dealers reactive → <span class="b">moves amplify</span>. Normal → dealers in control → mean-revert.</li>
<li><b>Gamma sign:</b> <span class="k">positive = pin/chop</span> (fade both ways); <span class="b">negative = trend</span> (ranges extend, levels break).</li>
<li><b>Vol vs its 6-mo baseline:</b> above ~90th pct = <span class="b">EXTREME</span> → <b>polarity inverts</b> (vanna above spot becomes resistance, not magnet). <i>6/8: IV 21.22 vs 17.58 → EXTREME.</i></li>
<li><b>Catalyst window:</b> CPI/FOMC/NFP/geopolitics override — "dealer flow is secondary till the dust settles."</li></ul></div>
<div class="card"><b>Layer 2 — Levels (where, not which way):</b> Volland gamma/vanna/charm walls overlaid on volume-profile value areas + prior-week highs/lows, tagged by term-stack <span class="pill">[0]</span><span class="pill">[W]</span><span class="pill">[M]</span><span class="pill">[L]</span> (quad-stack = strongest), weighted by vol+catalyst.</div>
<div class="card"><b>Layer 3 — Bias = a decision tree, never fixed:</b> "above X range A↔B; below X waterfall to C," entered only on a <b>15-min rejection/hold confirmation</b> at the level. He never front-runs. This confirmation step is why his discretion beats a blind mechanical gate. Half-size in mixed/EXTREME regimes.</div>

<h2>3 · His level map, rebuilt from his own data (the 6/8 week)</h2>
<p>This is his "Key Levels Table" redrawn — the <b>severity color is his conviction</b> (🔴 highest). Note he doesn't treat all levels equally: the red 7575 vanna wall (+$588M) and the red floors (7100/7000) are load-bearing; the yellow 7450/7480 are weak "repellents." <b>We store the same per-strike charm/vanna/gamma in <code>volland_exposure_points</code>, so we can auto-generate this table ourselves.</b></p>
<img src="data:image/png;base64,{c_ladder}">

<h2>4 · Worked examples — actual trades/calls he posted</h2>

<div class="ex"><span class="tag" style="background:{RED};color:#000">SHORT · EXTREME regime</span>
<h3 style="margin:6px 0">Jun 9 — the cascade he called the night before</h3>
In his 6/9 update (posted ~Sun night) he wrote: <i>"below 7390, first target 7375 then 7350. That 7350 is so critical — lose it and a lot unwinds fast, breaking toward 7290, then 7200, 7150."</i> The next day SPX opened 7448, sliced every level, and bottomed at <b>7243 — sitting right between his 7290 and 7200 targets.</b> Meanwhile our system fired 10 buy-the-dip longs into that exact cascade.
<img src="data:image/png;base64,{c_jun9}"></div>

<div class="ex"><span class="tag" style="background:{RED};color:#000">SHORT setups · 6/8 plan</span>
<h3 style="margin:6px 0">His fade-the-wall short structure (Setup A &amp; D)</h3>
<b>Setup A — Fade-short</b> @ SPX 7,565–7,587 (the +$588M vanna wall) · stop 7,600 · targets 7,500 → 7,408 → 7,375 · <b>R:R 2.6→7.6:1</b> · Grade B · 0.5×. Trigger = a 15-min bar closes back below 7,585 after tagging it.<br>
<b>Setup D — Broken-floor fade-short</b> @ 7,500 (the 6-week floor that broke Friday, now resistance) · stop 7,540 · targets 7,408 → 7,375 · Grade B− · 0.5×.<br>
<span style="color:{MUT}">Neither triggered Jun 9 (price never rallied back to 7,500+), but the <b>thesis — "sell rallies, dealers are forced sellers" — was exactly right.</b> His only long that day (Setup B cushion-buy 7,350) was explicitly "scout-size, untested, intraday-only" — he treated the counter-trend long as the lowest-conviction trade, the opposite of what we did.</span></div>

<div class="ex"><span class="tag" style="background:{GRN};color:#000">LONG · NORMAL regime</span>
<h3 style="margin:6px 0">His dip-buy / breakout longs in positive-gamma weeks</h3>
<b>4/13 Setup B (PRIMARY, Grade A):</b> LONG the bounce at the 6,803 gamma wall — buy the proven floor in a positive-gamma regime.<br>
<b>5/11 Setup A (Grade B):</b> Breakout-LONG continuation above 7,450 in a strong uptrend — "buy dips, fade rips only with discipline."<br>
<span style="color:{MUT}">In these regimes <b>his playbook = our playbook.</b> Same mean-reversion/dip-buy logic we run every day. That's why we're aligned and profitable in 7 of his 8 weeks (next section).</span></div>

<h2>5 · His 8-week bias vs reality vs OUR P&L</h2>
<p>His regime read produced a correct flexible bias every week. Right columns = <b>our</b> P&L (quality-traded, portal-sim, $@1MES). We win in his range/long weeks; we lose only in his EXTREME short week.</p>
<table><tr><th>Week</th><th>His bias</th><th>His vol</th><th>VIX</th><th>Our LONG$</th><th>Our SHORT$</th><th>Our TOTAL$</th></tr>{rows}</table>
<img src="data:image/png;base64,{c_wk}">
<p style="color:{MUT};font-size:12px">Green bars = his range/long weeks (we're aligned &amp; green). Red = his EXTREME-vol short week (6/8 — the only week our long-biased book lost). 5/25 SPX-move figure was a holiday-week data artifact.</p>

<h2>6 · The validated edge — regime decides which SIDE pays</h2>
<img src="data:image/png;base64,{c_rs}">
<table><tr><th>Regime</th><th>LONGS</th><th>SHORTS</th><th>Read</th></tr>
<tr><td><b style="color:{GRN}">NORMAL</b> (VIX&lt;20)</td><td class="k">+$6,535 · 59% · n=494</td><td>+$1,152 · 50% · n=427</td><td>Longs dominate → our current edge fits</td></tr>
<tr><td><b style="color:{RED}">EXTREME</b> (VIX≥20)</td><td>+$3,657 · 62% · n=270</td><td class="b">+$4,794 · 57% · n=349</td><td>Short side ~4× bigger → where we under-trade</td></tr></table>
<p>The edge is <b>not</b> "stop buying dips" (our longs held up in both regimes). It's that <b>the short side roughly quadruples in high-vol</b> (March alone: 251 shorts, 64% WR, +$5,324). Post-V16 we fire only 37 shorts vs 112 longs — we leave the high-vol money on the table and our long-only book actively bleeds on amplification days.</p>

<h2>7 · What I tested and REJECTED (no overfit)</h2>
<ul>
<li><span class="b">Block dip-longs on down days</span>: looked great recently (+$1,019) but <b>June-only overfit</b> — would've lost Feb–May (Mar −$945 of blocked winners). ❌</li>
<li><span class="b">VIX-rising / spot-vol-beta gates</span>: same June-only pattern. ❌</li>
<li><span class="b">Daily-loss cap (S208)</span> full history: loses at every level ($300 −$446 … $150 −$1,387) — blocks afternoon recoveries. Keep $300 breaker as tail-insurance only. ❌</li></ul>
<p><b>Lesson = his lesson:</b> a blind mechanical gate fails because the same "down day" is a buyable dip in a bull-grind and a knife in an amplification regime. The difference is the <b>regime read + level confirmation</b> — discretionary. So we build <b>decision-support, not an auto-block.</b></p>

<h2>8 · The plan — trade like him &amp; sharpen our edge</h2>
<div class="card"><h3>Phase 1 (~1 wk) — Regime Dashboard / Telegram read</h3>
Daily + intraday mode read from data we already scrape: VIX-vs-6mo-percentile + Volland paradigm/DD-hedging sign + a scheduled-catalyst flag → prints <i>"MODE: NORMAL — mean-reversion on, longs primary"</i> vs <i>"MODE: EXTREME/AMPLIFICATION — fade rallies, longs scout-size, shorts primary."</i> This is the <b>defense that stops Jun-9 bleeds</b> — no filter overfit, just a state read you act on.</div>
<div class="card"><h3>Phase 2 (~2–3 wks) — Fade-the-Wall short setup</h3>
Build the short we lack: fire when price rallies into a strong overhead Volland wall (auto-ranked from <code>volland_exposure_points</code>, like his Key Levels Table) AND prints a 15-min rejection — his Setup A. Weighted to fire more in EXTREME regimes to harvest the <b>+$4,794 short bucket</b> we currently miss. Forward-log ≥30 signals, then enable on TSRT.</div>
<div class="card"><h3>Phase 3 — His discipline</h3>Half-size in EXTREME · 15-min confirmation entries · a daily auto-brief that prints our own "above X range / below X waterfall" decision tree from our levels.</div>

<h2>9 · Projection (honest, ranged)</h2>
<table><tr><th>Scenario</th><th>Assumption</th><th>Incremental $/mo @1MES</th><th>Confidence</th></tr>
<tr><td>Keep current system</td><td>Low-vol edge intact (most months)</td><td>~$2,000 baseline (unchanged)</td><td>High</td></tr>
<tr><td>+ Regime dashboard (defense)</td><td>Avoid 1–2 amplification bleed days/mo</td><td>+$150–400 saved</td><td>Med</td></tr>
<tr><td>+ Fade-the-wall shorts (offense)</td><td>Capture part of high-vol short bucket; pays only in EXTREME months (~2 of 5 here)</td><td>+$200–500 in EXTREME months</td><td>Med-Low (needs build + forward test)</td></tr></table>
<p style="color:{MUT}">No single hero number — his own $ P&L isn't public. What's measurable: the short side carried +$4,794 in high-vol regimes we barely traded, and our long-only book lost on the one amplification week. Realistic target: <b>same ~$2k/mo in normal regimes, turn the −$300/day bleed days flat-or-green, and harvest shorts when vol spikes.</b> Firm $ after Phase 2 is built &amp; forward-tested.</p>

<h2>10 · Caveats (validation protocol)</h2>
<ul><li>P&L = portal-sim points ×5 ($@1MES); broker-realistic runs a touch lower on big runners — relative regime comparisons hold.</li>
<li>EXTREME sample is March-heavy (April EXTREME shorts actually lost −$371) → real but noisy month-to-month; forward-test before sizing.</li>
<li>We don't capture his exact aggregate vanna/gamma hedging $ — classifier uses VIX + paradigm + DD-sign proxy.</li>
<li>His method is discretionary with level confirmation — the win is decision-support + a richer toolkit, not a single auto-gate.</li></ul>

<p style="color:{MUT};font-size:12px;margin-top:28px">Sources: 8 weekly HTML plans (Volland Discord ⚛️dark-matter-trade) incl. Key/IV Levels tables · setup_log/real_trade_orders/chain_snapshots full history · scripts _tmp_dm_*.py, _tmp_fullhist_*.py, _tmp_s208_cap.py, _tmp_dm_regime_test.py. All numbers from DB queries, none from memory.</p>
</body></html>"""
open("daily_trade_logs/dark_matter_framework_study.html","w",encoding="utf-8").write(H)
print("wrote report,",len(H),"bytes")
