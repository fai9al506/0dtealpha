import os, psycopg2, json, bisect
from collections import defaultdict
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()

def ceil_strike(rows, spot):
    best=None
    for r in rows:
        try: cg=float(r[3]); coi=float(r[1]); pg=float(r[17]); poi=float(r[19]); k=float(r[10])
        except: continue
        net=cg*coi - pg*poi
        if k>spot and 0<k-spot<=60 and net>0 and (best is None or net>best[1]): best=(k,net)
    return best[0] if best else None

def run(label, dstart, dend):
    # preload chain snaps for window, precompute ceiling dist per snap
    cur.execute("""SELECT ts, spot, rows FROM chain_snapshots
       WHERE spot IS NOT NULL AND ts >= %s::timestamptz - interval '6 hour' AND ts < %s::timestamptz + interval '6 hour'
       ORDER BY ts""",(dstart,dend))
    snaps=[]  # (epoch, spot, ceil_dist)
    for ts,spot,rows in cur.fetchall():
        rows=rows if isinstance(rows,list) else json.loads(rows)
        cs=ceil_strike(rows,float(spot))
        snaps.append((ts.timestamp(), float(spot), (cs-float(spot)) if cs else None))
    st=[s[0] for s in snaps]
    # preload volland charm TODAY, group by ts -> strongest +charm strike above current_price
    cur.execute("""SELECT ts_utc, current_price, strike, value FROM volland_exposure_points
       WHERE greek='charm' AND expiration_option='TODAY'
         AND ts_utc >= %s::timestamptz - interval '6 hour' AND ts_utc < %s::timestamptz + interval '6 hour'
       ORDER BY ts_utc""",(dstart,dend))
    vg=defaultdict(lambda:[None,None])  # ts -> [cp, best(strike,val)]
    for ts,cp,k,v in cur.fetchall():
        if cp is None: continue
        cp=float(cp); k=float(k); v=float(v); e=ts.timestamp()
        rec=vg[e]; rec[0]=cp
        if k>cp and 0<k-cp<=60 and v>0 and (rec[1] is None or v>rec[1][1]): rec[1]=(k,v)
    vts=sorted(vg); 
    def vdist(epoch):
        if not vts: return None
        i=bisect.bisect_left(vts,epoch); cand=[]
        if i<len(vts): cand.append(vts[i])
        if i>0: cand.append(vts[i-1])
        best=min(cand,key=lambda t:abs(t-epoch))
        if abs(best-epoch)>600: return None
        rec=vg[best]; 
        return (rec[1][0]-rec[0]) if rec[1] else None

    cur.execute("""SELECT id, ts, setup_name, spot, outcome_pnl FROM setup_log
       WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
         AND ts AT TIME ZONE 'America/New_York' >= %s AND ts AT TIME ZONE 'America/New_York' < %s ORDER BY ts""",(dstart,dend))
    out=[]
    for idv,ts,nm,spot,pnl in cur.fetchall():
        if spot is None or not snaps: continue
        e=ts.timestamp(); i=bisect.bisect_left(st,e); cand=[]
        if i<len(st): cand.append(i)
        if i>0: cand.append(i-1)
        j=min(cand,key=lambda k:abs(st[k]-e))
        dc=snaps[j][2] if abs(st[j]-e)<=600 else None
        out.append(dict(pnl=float(pnl),nm=nm,dc=dc,vc=vdist(e)))
    print(f"\n===== {label} (n={len(out)}) =====")
    def s(pred,lab):
        g=[x for x in out if pred(x)]
        if not g: print(f"  {lab:<40} n=0"); return
        w=sum(1 for x in g if x['pnl']>0); pts=sum(x['pnl'] for x in g)
        print(f"  {lab:<40} n={len(g):>3} WR={w/len(g)*100:>3.0f}% pts={pts:>+7.1f} $@1={pts*5-len(g):>+6.0f}")
    print(" [TS +GEX ceiling dist above spot]")
    s(lambda x:x['dc'] is not None and x['dc']<=5,"within 5pt of ceiling (INTO wall)")
    s(lambda x:x['dc'] is not None and 5<x['dc']<=15,"5-15pt below ceiling")
    s(lambda x:x['dc'] is not None and x['dc']>15,">15pt room below")
    s(lambda x:x['dc'] is None,"no +GEX wall within 60 (open sky)")
    print(" [Volland charm resistance dist above spot]")
    s(lambda x:x['vc'] is not None and x['vc']<=8,"within 8pt of charm resistance")
    s(lambda x:x['vc'] is not None and x['vc']>8,">8pt below charm res")
    s(lambda x:x['vc'] is None,"no charm res above")

run("SICK ERA Jun11-Jul9 (low-vol)","2026-06-11","2026-07-10")
run("CONTROL Mar10-27 (high-vol)","2026-03-10","2026-03-28")
c.close()
