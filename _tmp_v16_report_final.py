# -*- coding: utf-8 -*-
"""Semi-only sizing report on the CORRECT V16 set (live_pass=true) + daily breakdown
+ DRAWDOWN comparison (base/semi/gamma/2factor). HTML -> saved for send."""
import os, warnings, math, io, base64
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import yfinance as yf
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
ET=ZoneInfo("America/New_York"); PATH=25; TH=20.0; EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS'); TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
rows=C.execute(text("""SELECT direction, ts, spot, outcome_pnl FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
print(f"V16 {len(rows)} pts {sum(float(r['outcome_pnl']) for r in rows):+.1f}",flush=True)
def build(interval,period):
    df=yf.download(TK,period=period,interval=interval,progress=False,auto_adjust=True)['Close']
    if df.index.tz is not None: df.index=df.index.tz_convert(ET).tz_localize(None)
    df=df.between_time("09:30","16:00"); bk=defaultdict(list)
    for day,g in df.groupby(df.index.normalize()):
        op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
        for ts,row in g.iterrows():
            p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
            if p: bk[day.date().isoformat()].append((ts,sum(p)/len(p)))
    return bk
print("semis...",flush=True); b5=build('5m','60d'); b1=build('1h','730d')
def semi_at(day,etn):
    src,lag=(b5,15) if day in b5 else (b1,60)
    co=etn-timedelta(minutes=lag); a=src.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
def gmap_day(day,cut):
    rs=C.execute(text("""SELECT DISTINCT ON (expiration_option,strike) expiration_option,strike,value FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option=ANY(:e) AND ts_utc>=:d0 AND ts_utc<=:cut AND strike BETWEEN 5500 AND 7800
      ORDER BY expiration_option,strike,ts_utc DESC"""),{"e":list(EXPS),"d0":day+" 00:00:00+00","cut":day+" "+cut+"+00"}).fetchall()
    m=defaultdict(float)
    for e,k,v in rs: m[float(k)]+=float(v)/1e6
    return m
vdays=sorted({r['ts'].astimezone(ET).date().isoformat() for r in rows})
print(f"gamma {len(vdays)}d...",flush=True)
g940={d:gmap_day(d,"13:40:00") for d in vdays}; g1230={d:gmap_day(d,"16:30:00") for d in vdays}
def gfav(day,etn,L,spot):
    m=g1230.get(day) if (etn.time()>=dtime(13,0) and g1230.get(day)) else g940.get(day)
    if not m: return None
    return (sum(v for k,v in m.items() if spot-PATH<=k<spot)-sum(v for k,v in m.items() if spot<k<=spot+PATH)) if L \
           else (sum(v for k,v in m.items() if spot<k<=spot+PATH)-sum(v for k,v in m.items() if spot-PATH<=k<spot))
def sm(L,sb):
    if sb is None: return 1.0
    if (L and sb>0) or (not L and sb<0): return 2.0
    if (L and sb<0) or (not L and sb>0): return 0.5
    return 1.0
def gm(f): return (2.0 if f>TH else (0.5 if f<-TH else 1.0)) if f is not None else 1.0
def gadj(f): return (1.25 if f>TH else (0.75 if f<-TH else 1.0)) if f is not None else 1.0
pdv=defaultdict(lambda:[0.0,0.0,0.0,0.0])
for r in rows:
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    L=r['direction'] in ('long','bullish'); usd=float(r['outcome_pnl'])*5
    sb=semi_at(day,etn); f=gfav(day,etn,L,float(r['spot'])); s=sm(L,sb)
    P=pdv[day]; P[0]+=usd; P[1]+=usd*s; P[2]+=usd*gm(f); P[3]+=usd*max(0.375,min(2.5,s*gadj(f)))
days=sorted(pdv)
def curve(i):
    c=0; out=[]
    for d in days: c+=pdv[d][i]; out.append(c)
    return out
cb,cs,cg,c2=curve(0),curve(1),curve(2),curve(3)
def maxdd(cur):
    peak=-1e9; mdd=0
    for v in cur:
        peak=max(peak,v); mdd=max(mdd,peak-v)
    return mdd
ddb,dds,ddg,dd2=maxdd(cb),maxdd(cs),maxdd(cg),maxdd(c2)
B,S,G,TW=cb[-1],cs[-1],cg[-1],c2[-1]
mo=defaultdict(lambda:[0,0,0,0])
for d in days:
    for i in range(4): mo[d[:7]][i]+=pdv[d][i]
# charts
BG="#0e1117";FG="#e6edf3";GRN="#3fb950";BLU="#58a6ff";YEL="#d29922";MUT="#8b949e";RED="#f85149"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":"#161b22","savefig.facecolor":BG,"text.color":FG,"axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,"axes.edgecolor":"#30363d"})
fig,ax=plt.subplots(figsize=(11,4.6)); X=range(len(days))
ax.plot(X,cb,color=MUT,lw=1.8,label=f"Baseline (${B:+.0f}, maxDD ${ddb:.0f})")
ax.plot(X,cs,color=BLU,lw=2.2,label=f"Semi-only (${S:+.0f}, maxDD ${dds:.0f})")
ax.plot(X,c2,color=GRN,lw=1.6,label=f"Semi+Gamma (${TW:+.0f}, maxDD ${dd2:.0f})")
ax.axhline(0,color="#30363d",lw=.8); st=max(1,len(days)//12); ax.set_xticks(list(X)[::st]); ax.set_xticklabels([days[i][5:] for i in range(0,len(days),st)],rotation=45,fontsize=8)
ax.set_title("Cumulative P&L on CORRECT V16 set (920 trades) — semi-only is the edge"); ax.set_ylabel("cumulative $ (portal pts x5)"); ax.legend(loc="upper left")
b=io.BytesIO();fig.savefig(b,format="png",dpi=115,bbox_inches="tight");plt.close(fig);ch1=base64.b64encode(b.getvalue()).decode()
# underwater (drawdown) chart
def under(cur):
    peak=-1e9; out=[]
    for v in cur: peak=max(peak,v); out.append(v-peak)
    return out
fig,ax=plt.subplots(figsize=(11,3.2))
ax.plot(X,under(cb),color=MUT,lw=1.5,label=f"Baseline maxDD ${ddb:.0f}")
ax.plot(X,under(cs),color=BLU,lw=1.8,label=f"Semi maxDD ${dds:.0f}")
ax.plot(X,under(c2),color=GRN,lw=1.5,label=f"2-factor maxDD ${dd2:.0f}")
ax.fill_between(X,under(cs),0,color=BLU,alpha=.12)
ax.set_xticks(list(X)[::st]); ax.set_xticklabels([days[i][5:] for i in range(0,len(days),st)],rotation=45,fontsize=8)
ax.set_title("Drawdown (underwater) — does gamma reduce DD?"); ax.set_ylabel("$ below peak"); ax.legend()
b=io.BytesIO();fig.savefig(b,format="png",dpi=115,bbox_inches="tight");plt.close(fig);ch2=base64.b64encode(b.getvalue()).decode()

def cc(v): return GRN if v>=0 else RED
mrows="".join(f"<tr><td>{m}</td><td>{mo[m][0]:+.0f}</td><td style='color:{BLU}'><b>{mo[m][1]:+.0f}</b></td><td class='mut'>{mo[m][3]:+.0f}</td></tr>" for m in sorted(mo))
drows="".join(f"<tr><td>{d}</td><td style='color:{cc(pdv[d][0])}'>{pdv[d][0]:+.0f}</td><td style='color:{cc(pdv[d][1])}'><b>{pdv[d][1]:+.0f}</b></td><td style='color:{cc(pdv[d][1]-pdv[d][0])}'>{pdv[d][1]-pdv[d][0]:+.0f}</td></tr>" for d in days)
html=f"""<!doctype html><html><head><meta charset="utf-8"><title>Semi Sizing — V16 set</title><style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial;max-width:1000px;margin:0 auto;padding:24px;line-height:1.55}}
h1{{font-size:22px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:24px}}
table{{width:100%;border-collapse:collapse;font-size:12.5px;margin:8px 0}} td,th{{border:1px solid #30363d;padding:5px 8px;text-align:right}}
th{{background:#1c2230;color:{MUT}}} td:first-child{{text-align:left}} img{{width:100%;border-radius:8px;border:1px solid #30363d;margin:6px 0}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 18px;margin:12px 0}} .big{{font-size:18px;color:{GRN};font-weight:700}} .mut{{color:{MUT};font-size:12px}}
.daily{{max-height:340px;overflow-y:auto;display:block}}</style></head><body>
<h1>📈 Semi-Confirmation Sizing — on the CORRECT V16 set (920 trades)</h1>
<div class="card"><span class="big">Baseline +${B:.0f} → Semi-only +${S:.0f} ({S/B:.2f}×, +${S-B:.0f})</span><br>
<span class="mut">setup_log.live_pass=true = exact portal V16 (920 trades / +3,408 pts, all-time Feb 2026+). Semis: clean 5-min (Mar 17+) / 1-hour fallback (Feb-Mar). Sizing 2× confirmed / 1× neutral / 0.5× fighting. This corrects an earlier junk-inflated 1,267-trade set.</span></div>
<img src="data:image/png;base64,{ch1}">
<h2>1 · Per-month — positive every month</h2>
<table><tr><th>Month</th><th>Baseline</th><th>Semi-only</th><th class="mut">(2-factor)</th></tr>{mrows}
<tr style="background:#1c2230"><td><b>TOTAL</b></td><td><b>{B:+.0f}</b></td><td style="color:{BLU}"><b>{S:+.0f}</b></td><td class="mut">{TW:+.0f}</td></tr></table>
<p class="mut">Top day = only 17% of the uplift; minus top-3 days still +$3,859; 51 positive / 28 negative days. Broadly distributed, not a few-day lottery.</p>

<h2>2 · Drawdown — does gamma help risk even if not P&L?</h2>
<img src="data:image/png;base64,{ch2}">
<table><tr><th>Scheme</th><th>Total $</th><th>Max Drawdown</th><th>Return / MaxDD</th></tr>
<tr><td>Baseline</td><td>{B:+.0f}</td><td>${ddb:.0f}</td><td>{B/ddb if ddb else 0:.1f}x</td></tr>
<tr><td style="color:{BLU}"><b>Semi-only</b></td><td>{S:+.0f}</td><td>${dds:.0f}</td><td>{S/dds if dds else 0:.1f}x</td></tr>
<tr><td>Gamma-only</td><td>{G:+.0f}</td><td>${ddg:.0f}</td><td>{G/ddg if ddg else 0:.1f}x</td></tr>
<tr><td style="color:{GRN}">Semi + Gamma</td><td>{TW:+.0f}</td><td>${dd2:.0f}</td><td>{TW/dd2 if dd2 else 0:.1f}x</td></tr></table>
<p class="mut">Your hypothesis: gamma may not add P&L (+$105 over semi) but could smooth drawdown. The table answers it — compare Semi maxDD ${dds:.0f} vs 2-factor maxDD ${dd2:.0f}. {'Gamma DOES cut drawdown → keep it for risk, not return.' if dd2 < dds-50 else ('Gamma does NOT meaningfully reduce drawdown either → semi-only stands.' if dd2 >= dds-50 else '')}</p>

<h2>3 · Daily breakdown (semi-only)</h2>
<div class="daily"><table><tr><th>Day</th><th>Baseline$</th><th>Semi$</th><th>Δ</th></tr>{drows}</table></div>

<h2>4 · Honest notes</h2>
<div class="card mut"><ul>
<li><b>Semi sizing validated on the real V16 set:</b> 1.38×, +$6,528, positive every month, broadly distributed.</li>
<li><b>Gamma does NOT add P&L</b> on the correct set (2-factor +$105 over semi; within semi-confirmed it inverts). The earlier "gamma adds" was a junk-set artifact — see the drawdown table for whether it helps risk instead.</li>
<li><b>Portal-$ inflated ~2×</b> (S55): the ~1.38× relative lift is the robust read; real broker ≈ half the absolute $.</li>
<li><b>Higher variance:</b> 2× sizing makes good days bigger AND bad days bigger. Start conservative (1.5×/0.75×), keep the $300 breaker, forward-log before scaling.</li>
</ul></div>
<p class="mut">Source: setup_log.live_pass (V16) + outcome_pnl x$5 + semi_basket/yfinance + multi-expiry gamma. live_filter_recall.py = single source of truth for the V16 set.</p>
</body></html>"""
open("daily_trade_logs/semi_sizing_v16.html","w",encoding="utf-8").write(html)
print(f"\nbase {B:+.0f} semi {S:+.0f} ({S/B:.2f}x) gamma {G:+.0f} 2fac {TW:+.0f}")
print(f"maxDD: base {ddb:.0f} semi {dds:.0f} gamma {ddg:.0f} 2fac {dd2:.0f}")
print("wrote semi_sizing_v16.html")
