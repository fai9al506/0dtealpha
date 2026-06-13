"""DIRECTIONAL predictive-power test (removes long-only beta confound).
Does charm sign / gamma regime predict the SIGN of the forward move better than base rate?
This is the proper test of 'GEX has edge, especially when charm supports direction'.

For each 30-min-spaced snapshot: total_gex sign, aggregatedCharm sign, +GEX wall vs spot,
VIX, prior-30min move. Forward returns at +30m / +60m / EOD. Measure P(up) conditioned.
"""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START,END="2026-02-23","2026-06-02"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
def _c(): return psycopg2.connect(DB,keepalives=1,keepalives_idle=30,keepalives_interval=10,keepalives_count=5)
conn=_c();cur=conn.cursor()
def q(sql,args):
    global conn,cur
    try: cur.execute(sql,args);return cur.fetchall()
    except psycopg2.OperationalError:
        conn=_c();cur=conn.cursor();cur.execute(sql,args);return cur.fetchall()

# cache full-day spot paths 09:30-16:00
DP=defaultdict(list)
for d,ts,spot in q(f"""SELECT (ts AT TIME ZONE 'America/New_York')::date, ts, spot FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND spot IS NOT NULL ORDER BY ts""",()):
    DP[d].append((ts,float(spot)))
def spot_at(day,ts,fwd_sec):
    arr=DP.get(day,[]); target=None
    for t2,sp in arr:
        if (t2-ts).total_seconds()>=fwd_sec: return sp
    return arr[-1][1] if arr else None   # EOD fallback
def spot_back(day,ts,back_sec):
    arr=DP.get(day,[]); prev=None
    for t2,sp in arr:
        if t2<=ts: prev=sp
        else: break
    # find spot back_sec before
    for t2,sp in arr:
        if (ts-t2).total_seconds()<=back_sec: return sp
    return arr[0][1] if arr else None

def aggcharm(ts):
    r=q("""SELECT payload->'statistics'->>'aggregatedCharm' FROM volland_snapshots
        WHERE ts BETWEEN %s-interval '4 min' AND %s+interval '2 min'
        AND payload->'statistics'->>'aggregatedCharm' IS NOT NULL
        ORDER BY abs(extract(epoch FROM(ts-%s))) LIMIT 1""",(ts,ts,ts))
    if not r or r[0][0] in (None,''): return None
    try: return float(str(r[0][0]).replace('$','').replace(',',''))
    except: return None

snaps=q(f"""SELECT ts,(ts AT TIME ZONE 'America/New_York') t, spot, rows, vix FROM chain_snapshots
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '{START}' AND '{END}'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:45' AND '15:00'
    AND spot IS NOT NULL ORDER BY ts""",())
print(f"snapshots: {len(snaps)}")
recs=[]; last={}
for ts,t,spot,rows,vix in snaps:
    d=t.date(); lf=last.get(d)
    if lf is not None and (t-lf).total_seconds()<30*60: continue
    last[d]=t
    rows=rows if isinstance(rows,list) else json.loads(rows)
    gex=[]
    for rr in rows:
        try: s=float(rr[iS])
        except: continue
        if not (spot-60<=s<=spot+60): continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    tg=sum(v for _,v in gex)
    ga=[(s,v) for s,v in gex if s>spot and v>0]
    wall=max(ga,key=lambda x:x[1])[0] if ga else None
    ch=aggcharm(ts)
    f30=spot_at(d,ts,30*60); f60=spot_at(d,ts,60*60); feod=DP[d][-1][1] if DP.get(d) else None
    back30=spot_back(d,ts,30*60)
    recs.append({'spot':spot,'tg':tg,'ch':ch,'wall':wall,'vix':vix,
                 'r30':(f30-spot) if f30 else None,'r60':(f60-spot) if f60 else None,
                 'reod':(feod-spot) if feod else None,'prior':(spot-back30) if back30 else None})
print(f"usable records: {len(recs)}\n")

def pup(rs,key):
    v=[r[key] for r in rs if r[key] is not None]
    if not v: return None,0
    up=sum(1 for x in v if x>0); return up/len(v)*100, len(v)
def avg(rs,key):
    v=[r[key] for r in rs if r[key] is not None]
    return (sum(v)/len(v)) if v else None

print("=== BASE RATE (all records) ===")
for h in ['r30','r60','reod']:
    p,n=pup(recs,h); print(f"  P(up) {h}: {p:.0f}%  (n={n})  avg move {avg(recs,h):+.1f}p")

print("\n=== CONDITIONED ON CHARM SIGN (the user's thesis) ===")
cpos=[r for r in recs if r['ch'] is not None and r['ch']>0]
cneg=[r for r in recs if r['ch'] is not None and r['ch']<0]
for lab,rs in [('charm>0',cpos),('charm<0',cneg)]:
    print(f"  {lab} (n={len(rs)}):")
    for h in ['r30','r60','reod']:
        p,n=pup(rs,h); a=avg(rs,h)
        print(f"     P(up) {h}: {p:.0f}%   avg {a:+.1f}p")

print("\n=== GAMMA REGIME: trend vs mean-revert (does prior move continue?) ===")
for lab,rs in [('+gamma (tg>=0)',[r for r in recs if r['tg']>=0]),
               ('-gamma (tg<0)', [r for r in recs if r['tg']<0])]:
    # among up-prior and down-prior, what's fwd direction?
    upprior=[r for r in rs if r['prior'] is not None and r['prior']>3]
    dnprior=[r for r in rs if r['prior'] is not None and r['prior']<-3]
    pu,nu=pup(upprior,'r30'); pd,nd=pup(dnprior,'r30')
    print(f"  {lab} (n={len(rs)}):  after UP move -> P(up next30)={pu:.0f}%(n={nu})   "
          f"after DOWN move -> P(up next30)={pd:.0f}%(n={nd})")
    print(f"       (trend regime: up-after-up high & up-after-down low; revert: opposite)")

print("\n=== CONFLUENCE: charm>0 AND spot below +GEX wall (room up) ===")
conf=[r for r in recs if r['ch'] is not None and r['ch']>0 and r['wall'] and r['spot']<r['wall']]
anti=[r for r in recs if r['ch'] is not None and r['ch']<0 and r['wall'] and r['spot']<r['wall']]
for lab,rs in [('charm>0 & below wall',conf),('charm<0 & below wall',anti)]:
    p,n=pup(rs,'r60'); print(f"  {lab} (n={n}):  P(up r60)={p:.0f}%  avg {avg(rs,'r60'):+.1f}p")

print("\n=== CHARM x GAMMA REGIME crosstab — P(up r60) ===")
for cl,cf in [('charm>0',lambda r:r['ch'] and r['ch']>0),('charm<0',lambda r:r['ch'] and r['ch']<0)]:
    for gl,gf in [('+gam',lambda r:r['tg']>=0),('-gam',lambda r:r['tg']<0)]:
        sub=[r for r in recs if cf(r) and gf(r)]
        p,n=pup(sub,'r60'); print(f"  {cl} & {gl}:  P(up r60)={p if p else 0:.0f}%  (n={n})  avg {avg(sub,'r60') or 0:+.1f}p")
conn.close()
