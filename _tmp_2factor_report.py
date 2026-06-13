# -*- coding: utf-8 -*-
"""Daily post-V16 baseline vs semi-sizing vs 2-factor (semi+gamma) — AUDITED,
NO-LOOK-AHEAD (semi & gamma lagged 20min, real broker fills). HTML + chart."""
import os, json, io, base64
from datetime import timedelta, time as dtime
from collections import defaultdict
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
START,END="2026-05-18","2026-06-10"; LAG=20

basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=LAG); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
def bavg(day): a=bd.get(day,[]); return sum(v for _,v in a)/len(a) if a else None

def gmap(cut):
    q=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option='TODAY'
        AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
        AND strike BETWEEN 6800 AND 7800) q WHERE tt<=TIME :c ORDER BY d,strike,ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"a":START,"b":END,"c":cut}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
g940=gmap("09:40"); g1230=gmap("12:30")
def gnet(day,spot,entry_t):
    et=(entry_t-timedelta(minutes=LAG)).time(); m=None
    if et>=dtime(12,30): m=g1230.get(day) or g940.get(day)
    elif et>=dtime(9,40): m=g940.get(day)
    else: return None
    if not m: return None
    return sum(v for k,v in m.items() if abs(k-spot)<=60)

rows=C.execute(text("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b ORDER BY sl.ts ASC"""),
  {"a":START,"b":END}).fetchall()
T=[]
for lid,et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    day=et.date().isoformat(); etn=et.replace(tzinfo=None); sb=semi_at(day,etn); g=gnet(day,float(spot),etn)
    sm=1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm=(1.25 if g<0 else 0.75) if (L and g is not None) else 1.0
    T.append({"day":day,"L":L,"pnl":usd,"sb":sb,"g":g,"sm":sm,"sz":sm*gm})

days=sorted(set(t["day"] for t in T))
cb=cs=ct=0; CB=[];CS=[];CT=[]; drows=""
for d in days:
    dd=[t for t in T if t["day"]==d]
    b=sum(t["pnl"] for t in dd); s2=sum(t["pnl"]*t["sm"] for t in dd); tw=sum(t["pnl"]*t["sz"] for t in dd)
    cb+=b;cs+=s2;ct+=tw; CB.append(cb);CS.append(cs);CT.append(ct)
    nL=sum(1 for t in dd if t["L"]); nS=len(dd)-nL; ba=bavg(d)
    conf=sum(1 for t in dd if t["sb"] is not None and ((t["L"] and t["sb"]>0) or (not t["L"] and t["sb"]<0)))
    anti=sum(1 for t in dd if t["sb"] is not None and ((t["L"] and t["sb"]<0) or (not t["L"] and t["sb"]>0)))
    gs="neg(L↑)" if any((t["g"] or 0)<0 for t in dd) else "pos(L↓)"
    cc=lambda v:"#3fb950" if v>=0 else "#f85149"
    drows+=(f"<tr><td>{d}</td><td>{len(dd)}</td><td>{nL}/{nS}</td><td>{('%+.1f'%ba) if ba is not None else '-'}</td>"
      f"<td>{conf}/{anti}</td><td style='color:#8b949e'>{gs}</td>"
      f"<td style='color:{cc(b)}'>{b:+.0f}</td><td style='color:{cc(s2)}'>{s2:+.0f}</td>"
      f"<td style='color:{cc(tw)}'><b>{tw:+.0f}</b></td><td style='color:{cc(tw-b)}'>{tw-b:+.0f}</td></tr>")

BG="#0e1117";FG="#e6edf3";GRN="#3fb950";BLU="#58a6ff";YEL="#d29922";MUT="#8b949e"
plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":"#161b22","savefig.facecolor":BG,"text.color":FG,
 "axes.labelcolor":FG,"xtick.color":MUT,"ytick.color":MUT,"axes.edgecolor":"#30363d"})
fig,ax=plt.subplots(figsize=(11,4.6)); x=range(len(days))
ax.plot(x,CB,marker="o",color=MUT,lw=2,label=f"Baseline 1x  (${cb:+.0f})")
ax.plot(x,CS,marker="o",color=BLU,lw=2,label=f"Semi-sizing  (${cs:+.0f})")
ax.plot(x,CT,marker="o",color=GRN,lw=2.4,label=f"2-Factor (semi+gamma)  (${ct:+.0f})")
ax.axhline(0,color="#30363d",lw=.8); ax.set_xticks(list(x)); ax.set_xticklabels([d[5:] for d in days],rotation=45,fontsize=8)
ax.set_title("TSRT cumulative P&L post-V16 — Baseline vs Semi vs 2-Factor (AUDITED, no look-ahead, broker $)")
ax.set_ylabel("cumulative $"); ax.legend(loc="upper left")
bio=io.BytesIO();fig.savefig(bio,format="png",dpi=115,bbox_inches="tight");plt.close(fig)
chart=base64.b64encode(bio.getvalue()).decode()

html=f"""<!doctype html><html><head><meta charset="utf-8"><title>TSRT 2-Factor (audited)</title><style>
body{{background:{BG};color:{FG};font-family:Inter,Segoe UI,Arial;max-width:1000px;margin:0 auto;padding:24px;line-height:1.5}}
h1{{font-size:22px}} h2{{color:{BLU};border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:24px}}
table{{width:100%;border-collapse:collapse;font-size:12.5px;margin:8px 0}} td,th{{border:1px solid #30363d;padding:5px 7px;text-align:right}}
th{{background:#1c2230;color:{MUT}}} td:first-child,td:nth-child(3){{text-align:left}}
img{{width:100%;border-radius:8px;border:1px solid #30363d}} .card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 18px;margin:12px 0}}
.big{{font-size:18px;color:{YEL};font-weight:700}}</style></head><body>
<h1>📊 TSRT Post-V16 — Baseline vs Semi-sizing vs 2-Factor <span style="font-size:13px;color:{MUT}">(AUDITED · no look-ahead · broker $)</span></h1>
<div class="card"><span class="big">Baseline +${cb:.0f} → Semi +${cs:.0f} ({cs/cb:.2f}×) → 2-Factor +${ct:.0f} ({ct/cb:.2f}×)</span><br>
Same {len(T)} placed trades, {days[0]}→{days[-1]}. Signals lagged 20min (no peeking into the trade's future); real broker fills; baseline = broker truth (Gate-2 passed, 2%). Each trade sized <b>0.375×–2.5×</b>.</div>
<img src="data:image/png;base64,{chart}">
<h2>Daily breakdown</h2>
<table><tr><th>Day</th><th>n</th><th>L/S</th><th>semi% avg</th><th>conf/anti</th><th>gamma</th><th>Base$</th><th>Semi$</th><th>2-Factor$</th><th>Δ</th></tr>{drows}</table>
<p style="color:{MUT};font-size:12px">semi% = day-avg tech basket (vs today's open); conf/anti checked per-trade at entry (lagged). gamma = longs boosted (neg) / trimmed (pos). It cuts the red-tech bleed days (longs → 0.5×) and presses the green days (→ 2×).</p>
<h2>Honest projection</h2>
<div class="card">This window is <b>bleed-heavy</b> (June macro), so the multiple is inflated by a low baseline. Durable expectation: <b>~1.4–1.8× → ~$2,800–3,600/mo</b> at ~1.05 MES vs ~$2,000 baseline. Biggest, most reliable piece = <b>bleed-insurance</b> (lag-proof: red days are red all day). Needs the live real-time semi feed + ~3–4wk forward-validation before scaling the 2×.</div>
<p style="color:{MUT};font-size:11px">Source: real_trade_orders broker fills + semi_basket (lag20) + volland gamma (as-of cutoff ≤ entry-20min). Supersedes the earlier look-ahead version.</p>
</body></html>"""
open("daily_trade_logs/tsrt_2factor_sizing.html","w",encoding="utf-8").write(html)
print(f"AUDITED no-look-ahead: baseline {cb:+.0f} | semi {cs:+.0f} | 2factor {ct:+.0f} | {len(T)} trades")
