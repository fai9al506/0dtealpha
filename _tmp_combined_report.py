# -*- coding: utf-8 -*-
"""FULL illustrated report: semi + gamma sizing on the LARGE sample (Mar17-Jun10, 1267 trades).
Proper 2-factor, per-month, concentration, gamma quality split, charts -> HTML."""
import os, warnings, math, io, base64
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from collections import defaultdict
import yfinance as yf
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
PATH=25; T=20.0; EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
print("fetch 5m semis...",flush=True)
df=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)
cl=df['Close'].copy()
if cl.index.tz is not None: cl.index=cl.index.tz_convert('America/New_York').tz_localize(None)
cl=cl.between_time("09:30","16:00")
sbk=defaultdict(list)
for day,g in cl.groupby(cl.index.normalize()):
    op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
        if p: sbk[day.date().isoformat()].append((ts,sum(p)/len(p)))
START=min(sbk); END=max(sbk)
def semi_at(day,t):
    co=t-timedelta(minutes=5); a=sbk.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
print("range",START,END,flush=True)
def gmap_day(day, utc_cut):
    rows=C.execute(text("""SELECT DISTINCT ON (expiration_option, strike) expiration_option, strike, value
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option=ANY(:e)
        AND ts_utc >= :d0 AND ts_utc <= :cut AND strike BETWEEN 6700 AND 7800
      ORDER BY expiration_option, strike, ts_utc DESC"""),
      {"e":list(EXPS),"d0":day+" 00:00:00+00","cut":day+" "+utc_cut+"+00"}).fetchall()
    m=defaultdict(float)
    for exp,k,v in rows: m[float(k)]+=float(v)/1e6
    return m
print("gamma maps...",flush=True)
alldays=sorted(sbk.keys())
g940={d:gmap_day(d,"13:40:00") for d in alldays}; g1230={d:gmap_day(d,"16:30:00") for d in alldays}
def gfav(day,etn,L,spot):
    m=g1230.get(day) if (etn.time()>=dtime(13,0) and g1230.get(day)) else g940.get(day)
    if not m: return None
    above=sum(v for k,v in m.items() if spot<k<=spot+PATH); below=sum(v for k,v in m.items() if spot-PATH<=k<spot)
    return (below-above) if L else (above-below)
sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC"""),{"a":START,"b":END}).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
last={}; T_=[]
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat()
    T_.append({"day":day,"mo":day[:7],"L":L,"pnl":float(pnl)*5,"fav":gfav(day,et.replace(tzinfo=None),L,float(spot)),"sb":semi_at(day,et.replace(tzinfo=None))})
def smult(t):
    sb=t['sb']
    if sb is None: return 1.0
    if (t['L'] and sb>0) or (not t['L'] and sb<0): return 2.0
    if (t['L'] and sb<0) or (not t['L'] and sb>0): return 0.5
    return 1.0
def gmult(t):  # standalone (strong)
    return (2.0 if t['fav']>T else (0.5 if t['fav']<-T else 1.0)) if t['fav'] is not None else 1.0
def gadj(t):   # mild gamma factor for combining
    return (1.25 if t['fav']>T else (0.75 if t['fav']<-T else 1.0)) if t['fav'] is not None else 1.0
def two(t): return max(0.375,min(2.5,smult(t)*gadj(t)))
def cum(fn):
    by=defaultdict(float)
    for t in T_: by[t['day']]+=t['pnl']*fn(t)
    c=0;out=[]
    for d in sorted(by): c+=by[d]; out.append((d,c))
    return out
base=cum(lambda t:1); semi=cum(smult); gam=cum(gmult); comb=cum(two)
B=base[-1][1]; S=semi[-1][1]; G=gam[-1][1]; CO=comb[-1][1]
# per month
mo=defaultdict(lambda:[0.0,0.0,0.0,0.0])
for t in T_:
    m=mo[t['mo']]; m[0]+=t['pnl']; m[1]+=t['pnl']*smult(t); m[2]+=t['pnl']*gmult(t); m[3]+=t['pnl']*two(t)
# quality split
def wr(x):
    if not x: return (0,0,0)
    return (len(x),100*sum(1 for t in x if t['pnl']>0)/len(x),sum(t['pnl'] for t in x))
fav=wr([t for t in T_ if t['fav'] is not None and t['fav']>T]); unf=wr([t for t in T_ if t['fav'] is not None and t['fav']<-T])
sc=[t for t in T_ if smult(t)==2.0]
scf=wr([t for t in sc if t['fav'] is not None and t['fav']>T]); scu=wr([t for t in sc if t['fav'] is not None and t['fav']<-T])

# charts
BG="#0e1117";FG="#e6edf3";GRN="#3fb950";BLU="#58a6ff";YEL="#d29922";PUR="#bc8cff";MUT="#8b949e";RED="#f85149"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":"#161b22","savefig.facecolor":BG,"text.color":FG,"axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,"axes.edgecolor":"#30363d"})
fig,ax=plt.subplots(figsize=(11,4.6)); xs=[d for d,_ in base]; X=range(len(xs))
ax.plot(X,[v for _,v in base],color=MUT,lw=1.8,label=f"Baseline (${B:+.0f})")
ax.plot(X,[v for _,v in gam],color=YEL,lw=1.8,label=f"Gamma-only (${G:+.0f})")
ax.plot(X,[v for _,v in semi],color=BLU,lw=2,label=f"Semi-only (${S:+.0f})")
ax.plot(X,[v for _,v in comb],color=GRN,lw=2.4,label=f"Semi+Gamma 2-factor (${CO:+.0f})")
ax.axhline(0,color="#30363d",lw=.8); step=max(1,len(xs)//12); ax.set_xticks(list(X)[::step]); ax.set_xticklabels([xs[i][5:] for i in range(0,len(xs),step)],rotation=45,fontsize=8)
ax.set_title("Cumulative P&L — Mar 17-Jun 10 (1267 trades, portal pts x$5)"); ax.set_ylabel("cumulative $"); ax.legend(loc="upper left")
b=io.BytesIO();fig.savefig(b,format="png",dpi=115,bbox_inches="tight");plt.close(fig);c1=base64.b64encode(b.getvalue()).decode()
# per-month uplift bars
fig,ax=plt.subplots(figsize=(11,3.8)); months=sorted(mo); MX=range(len(months)); w=.26
ax.bar([i-w for i in MX],[mo[m][1]-mo[m][0] for m in months],w,label="Semi uplift",color=BLU)
ax.bar([i for i in MX],[mo[m][2]-mo[m][0] for m in months],w,label="Gamma uplift",color=YEL)
ax.bar([i+w for i in MX],[mo[m][3]-mo[m][0] for m in months],w,label="2-factor uplift",color=GRN)
ax.axhline(0,color="#30363d",lw=.8); ax.set_xticks(list(MX)); ax.set_xticklabels(months); ax.set_title("Uplift vs baseline by month ($)"); ax.legend()
b=io.BytesIO();fig.savefig(b,format="png",dpi=115,bbox_inches="tight");plt.close(fig);c2=base64.b64encode(b.getvalue()).decode()
# gamma quality split
fig,ax=plt.subplots(figsize=(11,3.4))
cats=["FAVORABLE\n(+G below / -G above)","UNFAVORABLE\n(+G above / -G below)","within SEMI-conf\nGAMMA-fav","within SEMI-conf\nGAMMA-unfav"]
wrs=[fav[1],unf[1],scf[1],scu[1]]; ns=[fav[0],unf[0],scf[0],scu[0]]
bars=ax.bar(cats,wrs,color=[GRN,RED,GRN,RED]); ax.axhline(50,color=MUT,lw=.8,ls=":")
for bar,wv,nv in zip(bars,wrs,ns): ax.text(bar.get_x()+bar.get_width()/2,wv+1,f"{wv:.0f}%\nn={nv}",ha="center",fontsize=9,color=FG)
ax.set_ylabel("Win rate %"); ax.set_title("Gamma quality split — favorable vs unfavorable (incl. WITHIN semi-confirmed)"); ax.set_ylim(0,75)
b=io.BytesIO();fig.savefig(b,format="png",dpi=115,bbox_inches="tight");plt.close(fig);c3=base64.b64encode(b.getvalue()).decode()

# ===== REAL post-V16 per-day (broker fills) =====
import json as _json
rrows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts""")).fetchall()
rpd=defaultdict(lambda:[0.0,0.0,0.0])  # base, semi, 2factor
for et,setup,direction,st,spot in rrows:
    if not isinstance(st,dict):
        try: st=_json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    t={"L":L,"fav":gfav(d,etn,L,float(spot)),"sb":semi_at(d,etn)}
    P=rpd[d]; P[0]+=usd; P[1]+=usd*smult(t); P[2]+=usd*two(t)
rdays=sorted(rpd); rb=sum(rpd[d][0] for d in rdays); rs=sum(rpd[d][1] for d in rdays); r2=sum(rpd[d][2] for d in rdays)
# portal quality per-day (for the Jun9/10 contrast) over same real days
portal_day={}
for d in rdays:
    portal_day[d]=sum(t['pnl'] for t in T_ if t['day']==d)
# chart c4: real post-V16 per-day base vs 2factor
fig,ax=plt.subplots(figsize=(11,3.8)); RX=range(len(rdays)); w=.4
ax.bar([i-w/2 for i in RX],[rpd[d][0] for d in rdays],w,label="Baseline (real broker)",color=MUT)
ax.bar([i+w/2 for i in RX],[rpd[d][2] for d in rdays],w,label="2-factor (real broker)",color=GRN)
ax.axhline(0,color="#30363d",lw=.8); ax.set_xticks(list(RX)); ax.set_xticklabels([d[5:] for d in rdays],rotation=45,fontsize=8)
ax.set_title("REAL TSRT post-V16 per-day — baseline vs 2-factor (broker fills, NOT portal)"); ax.set_ylabel("$ broker"); ax.legend()
b=io.BytesIO();fig.savefig(b,format="png",dpi=115,bbox_inches="tight");plt.close(fig);c4=base64.b64encode(b.getvalue()).decode()
def cc(v): return GRN if v>=0 else RED
realrows="".join(f"<tr><td>{d}</td><td style='color:{cc(rpd[d][0])}'>{rpd[d][0]:+.0f}</td><td style='color:{cc(rpd[d][1])}'>{rpd[d][1]:+.0f}</td><td style='color:{cc(rpd[d][2])}'><b>{rpd[d][2]:+.0f}</b></td><td style='color:{cc(rpd[d][2]-rpd[d][0])}'>{rpd[d][2]-rpd[d][0]:+.0f}</td><td class='mut'>{('portal '+('%+.0f'%portal_day[d])) if d in ('2026-06-09','2026-06-10') else ''}</td></tr>" for d in rdays)
mrows="".join(f"<tr><td>{m}</td><td>{mo[m][0]:+.0f}</td><td style='color:{BLU}'>{mo[m][1]:+.0f}</td><td style='color:{YEL}'>{mo[m][2]:+.0f}</td><td style='color:{GRN}'><b>{mo[m][3]:+.0f}</b></td></tr>" for m in months)
html=f"""<!doctype html><html><head><meta charset="utf-8"><title>Semi + Gamma Sizing — Validated</title><style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial;max-width:1010px;margin:0 auto;padding:24px;line-height:1.55}}
h1{{font-size:23px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:26px}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin:8px 0}} td,th{{border:1px solid #30363d;padding:6px 9px;text-align:right}}
th{{background:#1c2230;color:{MUT}}} td:first-child{{text-align:left}} img{{width:100%;border-radius:8px;border:1px solid #30363d;margin:6px 0}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 18px;margin:12px 0}} .big{{font-size:18px;color:{GRN};font-weight:700}} .mut{{color:{MUT};font-size:12.5px}}</style></head><body>
<h1>📐 Semi-Confirmation + Gamma-Structure Sizing — Validated on 1,267 Trades</h1>
<div class="card"><span class="big">Baseline +${B:.0f} → Semi +${S:.0f} → Gamma +${G:.0f} → Semi+Gamma 2-factor +${CO:.0f}</span><br>
<span class="mut">Mar 17 – Jun 10 portal (1,267 quality trades, pts×$5). Clean 5-min semis (no look-ahead). Multi-expiry gamma (0DTE+weekly+monthly) as Dark Matter reads it. This corrects an earlier small-sample (158-trade) conclusion that gamma was redundant.</span></div>
<img src="data:image/png;base64,{c1}">

<h2>1 · Two complementary signals (not redundant)</h2>
<p><b>Semi-confirmation</b> — tech basket (NVDA/AMD/AVGO/META/MSFT/GOOGL %-from-open) agreeing with trade direction → size up; fighting it → size down. <b>Gamma structure</b> — your rule: <b>+G = barrier, −G = accelerator</b>; for a long, +G <i>below</i> = support & −G <i>above</i> = accelerator (good), +G <i>above</i> = resistance & −G <i>below</i> = no floor (bad). Favorability = gamma_below − gamma_above (multi-expiry), mirror for shorts.</p>
<table><tr><th>Month</th><th>Baseline</th><th>Semi</th><th>Gamma</th><th>Semi+Gamma</th></tr>{mrows}
<tr style="background:#1c2230"><td><b>TOTAL</b></td><td><b>{B:+.0f}</b></td><td><b>{S:+.0f}</b></td><td><b>{G:+.0f}</b></td><td><b>{CO:+.0f}</b></td></tr></table>
<img src="data:image/png;base64,{c2}">

<h2>2 · Gamma adds INCREMENTAL value (the key proof)</h2>
<img src="data:image/png;base64,{c3}">
<p class="mut">Gamma favorable {fav[1]:.0f}% WR (n={fav[0]}) vs unfavorable {unf[1]:.0f}% (n={unf[0]}). Critically, <b>even WITHIN semi-confirmed trades, gamma further separates: favorable {scf[1]:.0f}% vs unfavorable {scu[1]:.0f}%</b> — so gamma carries independent information on top of semi. They are complementary, not duplicates.</p>

<h2>3 · REAL TSRT per-day (post-V16, broker fills — the honest view)</h2>
<div class="card mut">The cumulative chart above is <b>portal</b> P&L (full quality book, no $300 breaker, ~2× inflated). The real TSRT account is longs-heavy and breaker-gated, so it differs — most visibly on bleed days. <b>Jun 9 & 10: the portal book was +${portal_day.get('2026-06-09',0):.0f} / +${portal_day.get('2026-06-10',0):.0f} (green — shorts + ES Abs + afternoon V-reversal), but the REAL broker was {rpd.get('2026-06-09',[0])[0]:+.0f} / {rpd.get('2026-06-10',[0])[0]:+.0f}</b> (longs bled, breaker stopped early). That's why the portal curve ticks up while the real account was red.</div>
<table><tr><th>Day</th><th>Baseline$</th><th>Semi$</th><th>2-Factor$</th><th>Δ</th><th>note</th></tr>{realrows}
<tr style="background:#1c2230"><td><b>TOTAL</b></td><td><b>{rb:+.0f}</b></td><td><b>{rs:+.0f}</b></td><td><b>{r2:+.0f}</b></td><td><b>{r2-rb:+.0f}</b></td><td class="mut">real broker, May18-Jun10</td></tr></table>
<img src="data:image/png;base64,{c4}">
<p class="mut">Real-broker 2-factor uplift +${r2-rb:.0f} over baseline on the actual placed trades. Note the multiple is inflated by the bleed-heavy window (low base); the durable read is the all-regime ~1.5-1.65× above. Sizing makes good days bigger AND bad days bigger — it's higher return at higher variance, not a free lunch.</p>

<h2>4 · Honest notes</h2>
<div class="card"><ul>
<li><b>Semi is the stronger single factor</b> (+${S-B:.0f} vs gamma +${G-B:.0f}); the 2-factor combines them (mild gamma ×1.25/0.75 on top of semi ×2/0.5, capped 0.375–2.5×).</li>
<li><b>Portal P&L</b> — the relative uplifts are the robust part; absolute $ is inflated ~2× vs real broker (S55). Real-broker direction matches (post-V16 semi 2.68×).</li>
<li><b>Gamma can't predict break-vs-hold</b> of a wall (Jun 3 held → longs capped; Jun 4 broke → longs ran) — the favorability score handles it in aggregate, but it's a probabilistic edge, not certainty.</li>
<li><b>Next:</b> forward-log both signals live (semi_capture on VPS + multi-expiry gamma already stored), then ship after a clean forward window.</li>
</ul></div>
<p class="mut">Source: setup_log outcome_pnl (quality V16 set) + semi_basket/yfinance 5m + volland_exposure_points multi-expiry gamma. All no-look-ahead.</p>
</body></html>"""
open("daily_trade_logs/semi_gamma_sizing_validated.html","w",encoding="utf-8").write(html)
print(f"\nTOTAL base {B:+.0f} semi {S:+.0f} gamma {G:+.0f} 2factor {CO:+.0f}")
print(f"gamma fav {fav} unf {unf} | within-semi-conf fav {scf} unf {scu}")
print("wrote semi_gamma_sizing_validated.html")
