"""Why do a few days carry the P&L? Decompose day P&L into its drivers."""
import os, sys, importlib.util, io, contextlib, collections, statistics
from datetime import timedelta
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
mm=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(mm)
from sqlalchemy import text

POL=os.environ.get("POL","base012"); CAP=int(os.environ.get("CAP","3"))

# rerun the sim but keep per-day trade detail
byday=collections.defaultdict(list)
for r in mm.cands: byday[r["ts"].astimezone(mm.ET).date()].append(r)
detail={}
for d in sorted(byday):
    openp=[]; realized=0.0; placed=[]; rec=[]
    for r in byday[d]:
        et=r["ts"].astimezone(mm.ET); il=r["direction"] in ("long","bullish")
        still=[]
        for p in openp:
            if p["exit"]<=et: realized+=p["pnl"]
            else: still.append(p)
        openp=still
        take,qty=mm.decide(POL,r,il)
        if not take: continue
        if any(s==r["setup_name"] and dl==il and (et-t).total_seconds()<90 for s,dl,t in placed): continue
        if sum(1 for p in openp if p["is_long"]==il)>=CAP: continue
        if realized<=-300: continue
        stack=[p for p in openp if p["setup"]==r["setup_name"] and p["is_long"]==il]
        if len(stack)>=2:
            sgn=1.0 if il else -1.0
            if sum((float(r["spot"])-p["entry"])*sgn for p in stack)<0: continue
        pts=float(r["outcome_pnl"]); pnl=pts*5*qty-qty
        openp.append({"setup":r["setup_name"],"is_long":il,"entry":float(r["spot"]),
                      "exit":et+timedelta(minutes=int(r["outcome_elapsed_min"] or 60)),"pnl":pnl})
        placed.append((r["setup_name"],il,et))
        rec.append(dict(pts=pts,qty=qty,il=il,pnl=pnl,setup=r["setup_name"]))
    for p in openp: realized+=p["pnl"]
    detail[d]=rec

# market context per day
with mm.E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    ctx={}
    for d,o,cl,lo,hi in c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date d,
               (array_agg(bar_open ORDER BY ts ASC))[1], (array_agg(bar_close ORDER BY ts DESC))[1],
               min(bar_low), max(bar_high)
        FROM spx_ohlc_1m WHERE ts>=:a AND ts<:b GROUP BY 1"""), {"a":mm.START,"b":mm.END}):
        ctx[d]=dict(o=float(o),c=float(cl),rng=float(hi)-float(lo),net=float(cl)-float(o))
    vix={r[0]:float(r[1]) for r in c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date, avg(vix) FROM setup_log
        WHERE ts>=:a AND ts<:b AND vix IS NOT NULL GROUP BY 1"""),{"a":mm.START,"b":mm.END})}

days=sorted(detail); tots={d:sum(x["pnl"] for x in detail[d]) for d in days}
srt=sorted(days,key=lambda d:-tots[d]); TOT=sum(tots.values())
print(f"=== {POL} cap {CAP}/{CAP}  {mm.START} -> {mm.END}   {len(days)} sessions   total ${TOT:,.0f} ===")
print(f"\n{'day':<12}{'$':>8}{'n':>4}{'WR':>5}{'2x':>4}{'sameDir%':>9}{'pts/t':>7}{'SPXnet':>8}{'SPXrng':>8}{'VIX':>6}")
def line(d):
    r=detail[d]; n=len(r)
    if not n: return f"{str(d):<12}{'0':>8}{0:>4}"
    w=sum(1 for x in r if x["pts"]>0); q2=sum(1 for x in r if x["qty"]==2)
    nl=sum(1 for x in r if x["il"]); sd=max(nl,n-nl)/n*100
    cx=ctx.get(d,{}); 
    return (f"{str(d):<12}{tots[d]:>8,.0f}{n:>4}{w/n*100:>4.0f}%{q2:>4}{sd:>8.0f}%"
            f"{sum(x['pts'] for x in r)/n:>7.1f}{cx.get('net',0):>8.1f}{cx.get('rng',0):>8.1f}{vix.get(d,0):>6.1f}")
print(" TOP 5 DAYS")
for d in srt[:5]: print(" "+line(d))
print(" WORST 3 DAYS")
for d in srt[-3:]: print(" "+line(d))
print(" MEDIAN 3 DAYS")
for d in srt[len(srt)//2-1:len(srt)//2+2]: print(" "+line(d))

# aggregate: top3 vs rest
def agg(ds):
    r=[x for d in ds for x in detail[d]]
    if not r: return None
    n=len(r); w=sum(1 for x in r if x["pts"]>0)
    return dict(days=len(ds), n=n, tpd=n/len(ds), wr=w/n*100,
                pts=sum(x["pts"] for x in r)/n, q2=sum(1 for x in r if x["qty"]==2)/n*100,
                total=sum(x["pnl"] for x in r),
                avgwin=statistics.mean([x["pts"] for x in r if x["pts"]>0] or [0]),
                avgloss=statistics.mean([x["pts"] for x in r if x["pts"]<=0] or [0]),
                rng=statistics.mean([ctx[d]["rng"] for d in ds if d in ctx]),
                net=statistics.mean([abs(ctx[d]["net"]) for d in ds if d in ctx]))
print(f"\n{'bucket':<14}{'days':>5}{'trades':>7}{'t/day':>7}{'WR':>6}{'pts/t':>7}{'%2x':>6}{'avgWin':>8}{'avgLoss':>8}{'SPXrng':>8}{'|SPXnet|':>9}{'total$':>9}")
for lbl,ds in [("TOP 3",srt[:3]),("next 5",srt[3:8]),("rest",srt[3:]),("bottom 5",srt[-5:])]:
    a=agg(ds)
    print(f"{lbl:<14}{a['days']:>5}{a['n']:>7}{a['tpd']:>7.1f}{a['wr']:>5.0f}%{a['pts']:>7.2f}{a['q2']:>5.0f}%"
          f"{a['avgwin']:>8.1f}{a['avgloss']:>8.1f}{a['rng']:>8.1f}{a['net']:>9.1f}{a['total']:>9,.0f}")
print(f"\n  P&L excluding top 3 days: ${TOT-sum(tots[d] for d in srt[:3]):,.0f} over {len(days)-3} sessions"
      f"  (${(TOT-sum(tots[d] for d in srt[:3]))/(len(days)-3)*21:,.0f}/month)")
# correlation day$ vs drivers
import math
def corr(a,b):
    ma,mb=statistics.mean(a),statistics.mean(b)
    num=sum((x-ma)*(y-mb) for x,y in zip(a,b))
    return num/math.sqrt(sum((x-ma)**2 for x in a)*sum((y-mb)**2 for y in b))
ds=[d for d in days if d in ctx and detail[d]]
print(f"\n  correlation of day-$ with: n_trades {corr([tots[d] for d in ds],[len(detail[d]) for d in ds]):+.2f}"
      f" | SPX range {corr([tots[d] for d in ds],[ctx[d]['rng'] for d in ds]):+.2f}"
      f" | |SPX net move| {corr([tots[d] for d in ds],[abs(ctx[d]['net']) for d in ds]):+.2f}"
      f" | same-dir% {corr([tots[d] for d in ds],[max(sum(1 for x in detail[d] if x['il']),len(detail[d])-sum(1 for x in detail[d] if x['il']))/len(detail[d]) for d in ds]):+.2f}")
