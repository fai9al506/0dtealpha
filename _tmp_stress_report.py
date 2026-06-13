# -*- coding: utf-8 -*-
"""Stress-test report: defensive-only, all-months concentration, Jun3/4 dive (bar gamma),
cap-aware sim. Recomputes cap-aware + Jun3/4 from DB; embeds all-months summary."""
import os, json, io, base64
from datetime import timedelta, time as dtime
from collections import defaultdict
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
LAG=20; CAP=300.0
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=LAG); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
def gmap(cut):
    q=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option='TODAY' AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10'
        AND strike BETWEEN 6800 AND 7800) q WHERE tt<=TIME :c ORDER BY d,strike,ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"c":cut}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
g940=gmap("09:40"); g1230=gmap("12:30")
def gnet(day,spot,t):
    et=(t-timedelta(minutes=LAG)).time(); m=None
    if et>=dtime(12,30): m=g1230.get(day) or g940.get(day)
    elif et>=dtime(9,40): m=g940.get(day)
    else: return None
    return sum(v for k,v in m.items() if abs(k-spot)<=60) if m else None
def fullsize(L,sb,g):
    sm=1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm=(1.25 if g<0 else 0.75) if (L and g is not None) else 1.0
    return sm*gm

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot, sl.outcome_elapsed_min
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts ASC""")).fetchall()
byday=defaultdict(list)
for et,setup,direction,st,spot,elap in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    sz=fullsize(L,semi_at(d,etn),gnet(d,float(spot),etn))
    byday[d].append({"en_t":etn,"cl_t":etn+timedelta(minutes=float(elap) if elap is not None else 30),"base":usd,"sz_pnl":usd*sz})
def simcap(tt,us):
    taken=[];tr=False
    for t in sorted(tt,key=lambda x:x["en_t"]):
        if tr: continue
        r=sum((x["sz_pnl"] if us else x["base"]) for x in taken if x["cl_t"]<=t["en_t"])
        if r<=-CAP: tr=True; continue
        taken.append(t)
    return sum((x["sz_pnl"] if us else x["base"]) for x in taken)
caprows=[]; tb=tnc=tc=0
for d in sorted(byday):
    bc=simcap(byday[d],False); snc=sum(t["sz_pnl"] for t in byday[d]); sc=simcap(byday[d],True)
    tb+=bc; tnc+=snc; tc+=sc; caprows.append((d,bc,snc,sc))

# charts
BG="#0e1117";FG="#e6edf3";GRN="#3fb950";RED="#f85149";BLU="#58a6ff";MUT="#8b949e";YEL="#d29922"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":"#161b22","savefig.facecolor":BG,"text.color":FG,
 "axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,"axes.edgecolor":"#30363d"})
# chart1: cap-aware daily delta
fig,ax=plt.subplots(figsize=(11,4)); x=range(len(caprows))
ax.bar(x,[r[3]-r[1] for r in caprows],color=[GRN if (r[3]-r[1])>=0 else RED for r in caprows])
ax.axhline(0,color="#30363d",lw=.8); ax.set_xticks(list(x)); ax.set_xticklabels([r[0][5:] for r in caprows],rotation=45,fontsize=8)
ax.set_title("Sized(cap) − baseline, per day — concentration & cap effect (real TSRT)")
ax.set_ylabel("Δ $")
b=io.BytesIO();fig.savefig(b,format="png",dpi=110,bbox_inches="tight");plt.close(fig);c1=base64.b64encode(b.getvalue()).decode()
# chart2: Jun3 vs Jun4 intraday gamma+price
fig,axs=plt.subplots(1,2,figsize=(11,3.8))
for ax,DAY,ttl in [(axs[0],"2026-06-03","Jun 3 — longs LOST"),(axs[1],"2026-06-04","Jun 4 — longs WON")]:
    sp=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM chain_snapshots
       WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
       AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '16:00' ORDER BY ts"""),{"d":DAY}).fetchall()
    xs=range(len(sp)); ax.plot(xs,[float(r[1]) for r in sp],color=BLU,lw=1.6,label="SPX")
    ax.set_title(ttl,fontsize=10); ax.set_xticks([])
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, strike, value, current_price FROM volland_exposure_points
       WHERE greek='gamma' AND expiration_option='TODAY' AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d
       AND strike BETWEEN 6800 AND 7800 ORDER BY ts_utc"""),{"d":DAY}).fetchall()
    sn=defaultdict(list); spo={}
    for et,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); sn[key].append((float(k),float(v)))
        if cp: spo[key]=float(cp)
    tt=sorted(sn); gx=[]; gy=[]
    for i,t in enumerate(tt):
        s0=spo.get(t) or float(sp[0][1]); gx.append(i*len(sp)/max(len(tt),1)); gy.append(sum(v for k,v in sn[t] if abs(k-s0)<=60)/1e6)
    ax2=ax.twinx(); ax2.plot(gx,gy,color=YEL,lw=1.4,alpha=.8,label="0DTE gamma ±60 ($M)"); ax2.axhline(0,color=MUT,lw=.6,ls=":")
    ax2.tick_params(colors=YEL,labelsize=7)
axs[0].set_ylabel("SPX"); fig.suptitle("Jun 3 vs Jun 4 — bar-by-bar 0DTE gamma (yellow) vs price (blue)",color=FG,fontsize=11)
b=io.BytesIO();fig.savefig(b,format="png",dpi=110,bbox_inches="tight");plt.close(fig);c2=base64.b64encode(b.getvalue()).decode()

def cc(v): return GRN if v>=0 else RED
crows="".join(f"<tr><td>{d}</td><td style='color:{cc(bc)}'>{bc:+.0f}</td><td style='color:{cc(snc)}'>{snc:+.0f}</td>"
  f"<td style='color:{cc(sc)}'>{sc:+.0f}</td><td style='color:{cc(sc-bc)}'><b>{sc-bc:+.0f}</b></td>"
  f"<td style='text-align:left;color:{MUT}'>{'cap helped' if sc>snc+1 else ('cap hurt (cut recovery)' if sc<snc-1 else '')}</td></tr>" for d,bc,snc,sc in caprows)

html=f"""<!doctype html><html><head><meta charset="utf-8"><title>2-Factor Sizing — Stress Test</title><style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial;max-width:1000px;margin:0 auto;padding:24px;line-height:1.5}}
h1{{font-size:22px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:26px}}
table{{width:100%;border-collapse:collapse;font-size:12.5px;margin:8px 0}} td,th{{border:1px solid #30363d;padding:5px 8px;text-align:right}}
th{{background:#1c2230;color:{MUT}}} td:first-child{{text-align:left}} img{{width:100%;border-radius:8px;border:1px solid #30363d}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 18px;margin:12px 0}}
.warn{{color:{YEL};font-weight:700}} .bad{{color:{RED};font-weight:700}}</style></head><body>
<h1>🧪 2-Factor Sizing — Honest Stress Test</h1>
<div class="card">Deep validation per request: defensive-only half, all-months concentration, Jun 3 vs Jun 4 with bar-by-bar gamma, and a cap-aware sim. <span class="warn">Verdict: the edge is FRAGILE — concentration-dependent, regime-specific, and weaker than the headline numbers.</span></div>

<h2>1 · Defensive-only half is a NET LOSER (all months)</h2>
<p>Downsize-only (0.5× when fighting the signal, never >1×) — the "reliable" piece tested alone, Mar–Jun, no look-ahead:</p>
<table><tr><th>Month</th><th>Baseline</th><th>Defensive</th><th>Δ</th></tr>
<tr><td>Mar (volatile)</td><td>+7,394</td><td>+6,104</td><td class="bad">−1,290</td></tr>
<tr><td>Apr (grind)</td><td>+3,791</td><td>+3,170</td><td class="bad">−621</td></tr>
<tr><td>May (grind)</td><td>+5,048</td><td>+4,071</td><td class="bad">−977</td></tr>
<tr><td>Jun (bleed)</td><td>+59</td><td>+409</td><td style="color:{GRN}">+349</td></tr>
<tr style="background:#1c2230"><td><b>Total</b></td><td><b>+16,291</b></td><td><b>+13,753</b></td><td class="bad"><b>−2,538</b></td></tr></table>
<p style="color:{MUT};font-size:12.5px">Downsizing "fighting" trades only pays in June (real trend/bleed). In the Feb–May grind, fought trades often <i>won</i> (mean-reversion), so cutting them gave up profit. <b>The bleed-insurance does NOT stand alone — same June-only pattern as every other defensive idea.</b></p>

<h2>2 · The full edge is concentration-dependent</h2>
<p>Full 2-factor, all months, no look-ahead: uplift <b>+$3,523</b> over 71 days — but:</p>
<ul><li><b>Top single day = 56% of the entire uplift.</b> Remove the top 3 days → only <b>+$381</b> left.</li>
<li><b>35 negative days vs 33 positive</b> — it loses on the majority of days, wins big on a few (positive-skew lottery).</li>
<li>Real TSRT post-V16: Jun 4 alone = <b>59%</b> of the uplift; minus top 3 days = <b>−$235 (negative)</b>.</li></ul>
<img src="data:image/png;base64,{c1}">

<h2>3 · Why Jun 4 won and Jun 3 lost — it was the TAPE, and gamma is unreliable</h2>
<p>Both days had similar (flat-to-green) semis, so the sizing 2×'d the longs both days. The difference was the price action — and bar-by-bar gamma exposes a flaw:</p>
<table><tr><th>Day</th><th>Tape</th><th>0DTE gamma at entries</th><th>Longs</th><th>Sized result</th></tr>
<tr><td>Jun 3</td><td>down-drift (−39p, "COMM seller")</td><td><b>NEGATIVE all day</b> (−72M→−170M)</td><td>4/5 stopped −70</td><td class="bad">2× doubled the loss</td></tr>
<tr><td>Jun 4</td><td>up-trend (+53p)</td><td>~0 / positive AM, neg PM</td><td>7/8 won +44..+122</td><td style="color:{GRN}">2× doubled the win</td></tr></table>
<img src="data:image/png;base64,{c2}">
<p style="color:{MUT};font-size:12.5px"><b>Key flaw exposed:</b> my gamma rule boosts longs 1.25× when gamma is <i>negative</i> ("squeeze fuel"). But Jun 3 was negative-gamma <i>all day</i> and longs <b>lost</b> — negative gamma <b>amplified the DOWN move</b>, not up. <b>Negative gamma isn't bullish — it amplifies whichever way the tape is already going</b> (which we can't know in advance). So the gamma factor is unreliable bar-by-bar; the daily "neg-gamma → longs win" was an aggregate artifact.</p>

<h2>4 · Cap-aware sim — the $300 breaker is a wash (slightly hurts)</h2>
<p>Applying the live $300 daily-loss breaker to the SIZED P&L (it trips earlier under 2×):</p>
<table><tr><th>Day</th><th>Base (cap)</th><th>Sized no-cap</th><th>Sized (cap)</th><th>Δ vs base</th><th>cap effect</th></tr>{crows}
<tr style="background:#1c2230"><td><b>TOTAL</b></td><td><b>{tb:+.0f}</b></td><td><b>{tnc:+.0f}</b></td><td><b>{tc:+.0f}</b></td><td><b>{tc-tb:+.0f}</b></td><td style="text-align:left">cap net −$80</td></tr></table>
<p style="color:{MUT};font-size:12.5px">The cap <b>helps</b> on a one-way bleed (May 18: stops earlier) but <b>hurts</b> on whipsaw days (Jun 3, Jun 8): under 2× the breaker trips before a recovering trade and locks in the loss. Net effect ≈ −$80 — <b>more sensitive, not a clean win.</b></p>

<h2>Verdict</h2>
<div class="card">After full stress-testing: <span class="bad">the 2-factor sizing is NOT a robust ~1.5× edge.</span>
<ul><li>Defensive half alone = net loser (June-only).</li>
<li>Full edge = concentrated in a handful of days (top day 49–59%), more losing days than winning.</li>
<li>Gamma factor = unreliable bar-by-bar (negative gamma amplifies both directions).</li>
<li>The $300 cap doesn't rescue it (slightly hurts).</li></ul>
The most defensible read: it's a <b>positive-skew bet that pays on rare big trend-green days</b> (Jun 4) and bleeds the rest of the time — net-positive in these samples only because of a few outliers. <b>Not safe to scale real money on; the semi signal's real (small) value is regime-dependent, concentrated, and needs long forward-validation.</b></div>
<p style="color:{MUT};font-size:11px">Source: real_trade_orders broker fills + setup_log + semi_basket (lag20) + volland gamma. All no-look-ahead. All-months figures from 1h-semi lagged-60 portal run.</p>
</body></html>"""
open("daily_trade_logs/tsrt_2factor_stresstest.html","w",encoding="utf-8").write(html)
print(f"cap: base {tb:+.0f} | sized no-cap {tnc:+.0f} | sized cap {tc:+.0f} | wrote stress report")
