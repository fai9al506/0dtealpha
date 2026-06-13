"""Verify the user's BURIED-MAGNET veto across FULL history (TS GEX).
VETO a GEX Long if:
  total GEX < 0  AND  magnet NOT in top-3 strikes by |GEX|
  AND NOT (charm rescue: strongest charm above spot within 10pt of the GEX magnet
           AND |that charm| >= 50 M$)   <- the #1642 exception
Re-sim SL14/target=magnet/trail15/5. Window spot+/-40. Report removed = W vs L.
"""
import psycopg2, json
from collections import defaultdict
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
SL,TACT,TGAP=14.0,15.0,5.0
CHARM_FLOOR_M=50.0
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
        if sp<=stop: return ('WIN' if stop-entry>0 else 'LOSS',round(stop-entry,1))
        if sp>=target: return ('WIN',round(target-entry,1))
        if not ta and mfe>=TACT: ta=True;tstop=entry+(mfe-TGAP)
        elif ta:
            nt=entry+(mfe-TGAP)
            if nt>tstop:tstop=nt
    return ('EXPIRED',round(path[-1]-entry,1))
sigs=q("""SELECT id,ts,(ts AT TIME ZONE 'America/New_York') t, spot FROM setup_log
  WHERE setup_name='GEX Long' AND grade!='LOG'
  AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-15' AND '2026-06-08' ORDER BY ts""")
import datetime as dt
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
    ga=[(s,v) for s,v in gex if s>spot and v>0]
    if not gex or not ga: continue
    total=sum(v for _,v in gex)
    magnet,_=max(ga,key=lambda x:x[1])
    ranked=sorted(gex,key=lambda x:-abs(x[1]))
    rank=[s for s,_ in ranked].index(magnet)+1
    ch=q("""SELECT strike,value FROM volland_exposure_points WHERE greek='charm'
        AND ts_utc=(SELECT ts_utc FROM volland_exposure_points WHERE greek='charm'
        AND ts_utc BETWEEN %s-interval '6 min' AND %s ORDER BY abs(extract(epoch FROM(ts_utc-%s))) LIMIT 1)
        AND strike BETWEEN %s AND %s""",(ts,ts,ts,spot-50,spot+50))
    charm=[(float(s),float(v)/1e6) for s,v in ch]
    charm_rescue=False
    above=[(s,v) for s,v in charm if s>spot]
    if above:
        cs,cv=max(above,key=lambda x:abs(x[1]))  # strongest charm above
        if abs(cs-magnet)<=10 and abs(cv)>=CHARM_FLOOR_M: charm_rescue=True
    veto=(total<0) and (rank>3) and (not charm_rescue)
    res=sim(d,ts,spot,max(magnet,spot+5))
    if not res: continue
    rows.append(dict(lid=lid,date=str(d)[:10],res=res[0],pnl=res[1],total=total,rank=rank,veto=veto,rescue=charm_rescue))
def summ(g,label):
    if not g: print(f"{label}: n=0"); return
    n=len(g);w=sum(1 for r in g if r['res']=='WIN');p=sum(r['pnl'] for r in g)
    print(f"{label}: {n} trades | WR {w/n*100:.0f}% | {p:+.1f}p (${p*5:+,.0f})")
print(f"FULL-HISTORY GEX Long signals: {len(rows)}\n")
summ(rows,"BEFORE veto (all)        ")
summ([r for r in rows if not r['veto']],"AFTER veto (kept)        ")
rem=[r for r in rows if r['veto']]
print(f"\nVETOED {len(rem)} trades  ({sum(1 for r in rem if r['res']=='WIN')}W / {sum(1 for r in rem if r['res']=='LOSS')}L / {sum(1 for r in rem if r['res']=='EXPIRED')}EXP):")
for r in sorted(rem,key=lambda x:x['date']):
    print(f"   {r['lid']} {r['date']} {r['res']:5} {r['pnl']:+6.1f}p  (total {r['total']:+.0f}, rank {r['rank']})")
print(f"\nCharm-rescued (would-be vetoed but kept by charm): "
      f"{sum(1 for r in rows if r['rescue'] and r['total']<0 and r['rank']>3)}")
for r in rows:
    if r['rescue'] and r['total']<0 and r['rank']>3:
        print(f"   RESCUED {r['lid']} {r['date']} {r['res']} {r['pnl']:+.1f}p")
conn.close()
