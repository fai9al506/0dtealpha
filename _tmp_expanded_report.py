# -*- coding: utf-8 -*-
"""Expanded Mar-Jun 2-factor sizing backtest across regimes (portal P&L).
Semis via yfinance 1h (reaches March). PnL chart + VIX regime context + monthly table.
"""
import os, io, base64, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta
from collections import defaultdict
import yfinance as yf
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
START,END="2026-03-01","2026-06-10"

# ---- semis 1h ----
print("fetching 1h semis...",flush=True)
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
df=yf.download(TK,start="2026-02-20",end="2026-06-11",interval='1h',progress=False,auto_adjust=True)
cl=df['Close'].copy()
if cl.index.tz is not None: cl.index=cl.index.tz_convert('America/New_York').tz_localize(None)
cl=cl.between_time("09:30","16:00")
sb=defaultdict(list)
for day,g in cl.groupby(cl.index.normalize()):
    op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
        if p: sb[day.date().isoformat()].append((ts,sum(p)/len(p)))
LAG_MIN=int(os.getenv("SEMI_LAG_MIN","0"))
def bstr(day,t):
    cutoff=t-timedelta(minutes=LAG_MIN)
    a=sb.get(day,[]); pr=[v for (x,v) in a if x<=cutoff]; return pr[-1] if pr else None
print("semi days:",len(sb)," LAG_MIN=",LAG_MIN,flush=True)

# ---- gamma TODAY near-spot ----
gq=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
   SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
   WHERE greek='gamma' AND expiration_option='TODAY'
     AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
     AND strike BETWEEN 6300 AND 7800) q WHERE tt<=TIME '09:40' ORDER BY d,strike,ts_utc DESC""")
gm=defaultdict(dict)
for d,s,v in C.execute(gq,{"a":START,"b":END}).fetchall(): gm[d.isoformat()][float(s)]=float(v)
def gnet(day,spot): dd=gm.get(day,{}); return sum(v for k,v in dd.items() if abs(k-spot)<=60)

# ---- signals (portal) ----
sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl, vix
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC"""),
  {"a":START,"b":END}).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
last={}; T=[]; dayvix=defaultdict(list)
for et,s,d,g,a,spot,pnl,vix in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if vix is not None: dayvix[et.date().isoformat()].append(float(vix))
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); spot=float(spot); b=bstr(day,et.replace(tzinfo=None)); gg=gnet(day,spot)
    sm=1.0
    if b is not None:
        if (L and b>0) or (not L and b<0): sm=2.0
        elif (L and b<0) or (not L and b>0): sm=0.5
    gmu=(1.25 if gg<0 else 0.75) if L else 1.0
    T.append({"day":day,"pnl":float(pnl)*5,"sm":sm,"sz":sm*gmu})

days=sorted(set(t["day"] for t in T))
cb=cs=ct=0; CB=[];CS=[];CT=[];VX=[]
mo=defaultdict(lambda:[0.0,0.0,0.0,0])
for d in days:
    dd=[t for t in T if t["day"]==d]
    b=sum(t["pnl"] for t in dd); s2=sum(t["pnl"]*t["sm"] for t in dd); tw=sum(t["pnl"]*t["sz"] for t in dd)
    cb+=b;cs+=s2;ct+=tw; CB.append(cb);CS.append(cs);CT.append(ct)
    vv=dayvix.get(d,[]); VX.append(sorted(vv)[len(vv)//2] if vv else None)
    M=mo[d[:7]]; M[0]+=b;M[1]+=s2;M[2]+=tw;M[3]+=len(dd)

# chart
BG="#0e1117";FG="#e6edf3";GRN="#3fb950";BLU="#58a6ff";YEL="#d29922";MUT="#8b949e";RED="#f85149"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":"#161b22","savefig.facecolor":BG,"text.color":FG,
 "axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,"axes.edgecolor":"#30363d"})
fig,ax=plt.subplots(figsize=(12,5))
x=range(len(days))
ax.plot(x,CB,color=MUT,lw=1.8,label=f"Baseline 1x  (${cb:+.0f})")
ax.plot(x,CS,color=BLU,lw=1.8,label=f"Semi-sizing  (${cs:+.0f})")
ax.plot(x,CT,color=GRN,lw=2.4,label=f"2-Factor  (${ct:+.0f})")
ax.axhline(0,color="#30363d",lw=.8)
ax2=ax.twinx(); vlin=[v if v else float('nan') for v in VX]
ax2.plot(x,vlin,color=RED,lw=1,alpha=.45,label="VIX (right)")
ax2.set_ylabel("VIX",color=RED); ax2.tick_params(colors=RED)
# month gridlines
mk=[i for i,d in enumerate(days) if d[8:10]=="02" or i==0]
ax.set_xticks([i for i,d in enumerate(days) if d.endswith('-01') or i==0 or d[8:]=='02'])
ax.set_xticklabels([days[i][5:] for i in ax.get_xticks().astype(int) if i<len(days)],rotation=45,fontsize=8)
ax.set_title("2-Factor Sizing across regimes (Mar-Jun, portal P&L @1MES) — cumulative; VIX shows regime")
ax.set_ylabel("cumulative $"); ax.legend(loc="upper left")
bio=io.BytesIO();fig.savefig(bio,format="png",dpi=115,bbox_inches="tight");plt.close(fig)
chart=base64.b64encode(bio.getvalue()).decode()

def c(v): return GRN if v>=0 else RED
REG={"2026-03":"high-VIX / bearish (volatile)","2026-04":"recovery / bull-grind",
     "2026-05":"bull-grind / low-vol","2026-06":"macro selloff (volatile)"}
mr="".join(f"<tr><td>{m}</td><td style='text-align:left;color:{MUT}'>{REG.get(m,'')}</td><td>{int(v[3])}</td>"
  f"<td style='color:{c(v[0])}'>{v[0]:+.0f}</td><td style='color:{c(v[1])}'>{v[1]:+.0f}</td>"
  f"<td style='color:{c(v[2])}'><b>{v[2]:+.0f}</b></td><td style='color:{c(v[2]-v[0])}'>{v[2]-v[0]:+.0f}</td></tr>"
  for m,v in sorted(mo.items()))
html=f"""<!doctype html><html><head><meta charset="utf-8"><title>2-Factor Across Regimes</title><style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial;max-width:1000px;margin:0 auto;padding:24px;line-height:1.5}}
h1{{font-size:23px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:26px}}
table{{width:100%;border-collapse:collapse;font-size:13px;margin:8px 0}} td,th{{border:1px solid #30363d;padding:6px 8px;text-align:right}}
th{{background:#1c2230;color:{MUT}}} td:first-child,td:nth-child(2){{text-align:left}}
img{{width:100%;border-radius:8px;border:1px solid #30363d}} .card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 18px;margin:12px 0}}
.big{{font-size:18px;color:{YEL};font-weight:700}}</style></head><body>
<h1>📈 2-Factor Sizing across regimes — March → June 2026</h1>
<div class="card"><span class="big">Baseline +${cb:.0f} → Semi +${cs:.0f} → 2-Factor +${ct:.0f}</span><br>
Portal P&L @1 MES over {len(days)} trading days, {len(T)} quality trades. Same trades — only sizing changes (0.375×–2.5×). VIX line shows the regime: March volatile, Apr–May grind, June volatile.</div>
<img src="data:image/png;base64,{chart}">
<h2>By month / regime</h2>
<table><tr><th>Month</th><th>Regime</th><th>trades</th><th>Baseline$</th><th>Semi$</th><th>2-Factor$</th><th>Δ vs base</th></tr>{mr}</table>
<p style="color:{MUT};font-size:12.5px">It helps most in the <b>volatile months</b> (March, June) where confirmation separates winners from bleed; in calm grind months (Apr–May) the uplift is smaller because most trades win anyway. Portal P&L (real broker ~½ on big runners). Semis = avg %-from-open of NVDA/AMD/AVGO/META/MSFT/GOOGL (1h bars here); gamma = 0DTE net near spot (longs only).</p>
<p style="color:{MUT};font-size:11px">Source: setup_log outcomes + yfinance 1h semis + volland gamma. Forward-validate before scaling 2×.</p>
</body></html>"""
open("daily_trade_logs/tsrt_2factor_regimes.html","w",encoding="utf-8").write(html)
print(f"baseline {cb:+.0f} | semi {cs:+.0f} | 2factor {ct:+.0f} | {len(T)} trades, {len(days)} days")
for m,v in sorted(mo.items()): print(f"  {m}: base {v[0]:+.0f} semi {v[1]:+.0f} 2fac {v[2]:+.0f} (n={int(v[3])})")
print("wrote daily_trade_logs/tsrt_2factor_regimes.html")
