"""Test the user's hypothesis (2026-06-08 trade-review):
WIN when STRONG +GEX magnet ALIGNS with STRONG charm magnet; LOSE when magnet is
weak/negligible (#263,#798) or messy (#1504).
Features per GEX Long signal (TS GEX + Volland charm, spot+/-50):
  magnet_dom  = +GEX magnet value / max|GEX|        (is the magnet dominant?)
  charm_dom   = |bullish charm magnet| / max|charm| (is charm strong?)
  R5_align    = charm magnet within 10pt of GEX magnet (do they align?)
Re-sim exit SL14/target=magnet/trail15/5. Test filters vs baseline.
"""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
SL,TACT,TGAP=14.0,15.0,5.0
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(s,a=()):
    global conn,cur
    try: cur.execute(s,a);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(s,a);return cur.fetchall()
DP=defaultdict(list)
for d,ts,sp in q("""SELECT (ts AT TIME ZONE 'America/New_York')::date,ts,spot FROM chain_snapshots
  WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-01' AND '2026-06-08'
  AND (ts AT TIME ZONE 'America/New_York')::time<'16:00' AND spot IS NOT NULL ORDER BY ts"""):
    DP[d].append((ts,float(sp)))
def sim(day,ts,entry,target):
    path=[sp for (t2,sp) in DP.get(day,[]) if t2>=ts]
    if not path: return None
    s=entry-SL;mfe=0;ta=False;tstop=s
    for sp in path:
        mfe=max(mfe,sp-entry);stop=tstop if ta else s
        if sp<=stop: return ('WIN' if stop-entry>0 else 'LOSS',round(stop-entry,1),round(mfe,1))
        if sp>=target: return ('WIN',round(target-entry,1),round(mfe,1))
        if not ta and mfe>=TACT: ta=True;tstop=entry+(mfe-TGAP)
        elif ta:
            nt=entry+(mfe-TGAP)
            if nt>tstop:tstop=nt
    return ('EXPIRED',round(path[-1]-entry,1),round(mfe,1))

sigs=q("""SELECT id, ts, (ts AT TIME ZONE 'America/New_York') t, spot FROM setup_log
  WHERE setup_name='GEX Long' AND grade!='LOG'
  AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-15' AND '2026-06-08'
  ORDER BY ts""")
rows=[]
for lid,ts,t,spot in sigs:
    if not spot: continue
    spot=float(spot); d=t.date()
    cr=q("""SELECT rows FROM chain_snapshots WHERE ts BETWEEN %s - interval '90 sec' AND %s + interval '90 sec'
        ORDER BY abs(extract(epoch FROM (ts-%s))) LIMIT 1""",(ts,ts,ts))
    if not cr or not cr[0][0]: continue
    chain=cr[0][0] if isinstance(cr[0][0],list) else json.loads(cr[0][0])
    gex=[]
    for rr in chain:
        try: s=float(rr[iS])
        except: continue
        if abs(s-spot)>50: continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    ga=[(s,v) for s,v in gex if s>spot and v>0]
    if not ga: continue
    magnet,mval=max(ga,key=lambda x:x[1])
    maxabs=max(abs(v) for _,v in gex) or 1
    magnet_dom=mval/maxabs
    ch=q("""SELECT strike,value FROM volland_exposure_points WHERE greek='charm'
        AND ts_utc=(SELECT ts_utc FROM volland_exposure_points WHERE greek='charm'
        AND ts_utc BETWEEN %s-interval '6 min' AND %s ORDER BY abs(extract(epoch FROM(ts_utc-%s))) LIMIT 1)
        AND strike BETWEEN %s AND %s""",(ts,ts,ts,spot-50,spot+50))
    charm=[(float(s),float(v)) for s,v in ch]
    charm_dom=0; r5=False; cmag=None
    if charm:
        ca=[(s,v) for s,v in charm if s>spot and v<0]
        maxc=max(abs(v) for _,v in charm) or 1
        if ca:
            cmag,cval=min(ca,key=lambda x:x[1])
            charm_dom=abs(cval)/maxc
            r5=abs(cmag-magnet)<=10
    res=sim(d,ts,spot,max(magnet,spot+5))
    if not res: continue
    rows.append(dict(lid=lid,res=res[0],pnl=res[1],mfe=res[2],
        magnet_dom=magnet_dom,charm_dom=charm_dom,r5=r5,has_charm=bool(charm)))
print(f"GEX Long signals with features + sim: {len(rows)}\n")

def stat(g,label):
    if not g: print(f"  {label:42s} n=0"); return
    n=len(g);w=sum(1 for r in g if r['res']=='WIN');p=sum(r['pnl'] for r in g)
    print(f"  {label:42s} n={n:3d}  WR={w/n*100:3.0f}%  PnL={p:+7.1f}p  avg={p/n:+5.2f}")

print("=== BASELINE ===")
stat(rows,"all signals")
print("\n=== BY MAGNET DOMINANCE (magnet / max|GEX|) ===")
for lo,hi in [(0,.3),(.3,.5),(.5,.7),(.7,2)]:
    stat([r for r in rows if lo<=r['magnet_dom']<hi], f"magnet_dom {lo:.1f}-{hi:.1f}")
print("\n=== BY CHARM-GEX ALIGNMENT (R5: charm magnet within 10pt of GEX magnet) ===")
stat([r for r in rows if r['r5']], "R5_align = TRUE (aligned)")
stat([r for r in rows if not r['r5']], "R5_align = FALSE")
print("\n=== BY CHARM DOMINANCE (|charm magnet| / max|charm|) ===")
for lo,hi in [(0,.4),(.4,.7),(.7,2)]:
    stat([r for r in rows if lo<=r['charm_dom']<hi], f"charm_dom {lo:.1f}-{hi:.1f}")
print("\n=== COMBINED FILTER (user's thesis: strong magnet AND aligned strong charm) ===")
stat([r for r in rows if r['magnet_dom']>=.5 and r['r5']], "magnet_dom>=.5 AND R5_align")
stat([r for r in rows if r['magnet_dom']>=.5 and r['r5'] and r['charm_dom']>=.4], "+ charm_dom>=.4 (full thesis)")
stat([r for r in rows if not (r['magnet_dom']>=.5 and r['r5'])], "REJECTED by (dom>=.5 & R5)")
conn.close()
