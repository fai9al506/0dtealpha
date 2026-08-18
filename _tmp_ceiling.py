import os, psycopg2, json
from collections import defaultdict
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()

def netgex_ceiling(rows, spot):
    # returns (dist_to_ceiling, above_wall_flag) using nearest strong +GEX strike above spot within 60pt
    best=None
    for r in rows:
        try:
            cg=float(r[3]); coi=float(r[1]); pg=float(r[17]); poi=float(r[19]); k=float(r[10])
        except: continue
        net=(cg*coi - pg*poi)
        if k>spot and 0<k-spot<=60 and net>0:
            if best is None or net>best[1]: best=(k,net)
    if best is None: return None
    return best[0]-spot

def volland_res(cur, ts, spot, greek):
    # strongest positive-value strike above spot within 60pt for this greek near ts
    cur.execute("""SELECT strike, value FROM volland_exposure_points
       WHERE greek=%s AND ts_utc BETWEEN %s::timestamptz - interval '10 min' AND %s::timestamptz + interval '10 min'
         AND expiration_option='TODAY'""",(greek,ts,ts))
    best=None
    for k,v in cur.fetchall():
        k=float(k); v=float(v)
        if k>spot and 0<k-spot<=60 and v>0:
            if best is None or v>best[1]: best=(k,v)
    return (best[0]-spot) if best else None

def study(label, dstart, dend):
    cur.execute("""SELECT id, ts, ts AT TIME ZONE 'America/New_York', setup_name, spot, outcome_pnl, paradigm
       FROM setup_log WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
         AND ts AT TIME ZONE 'America/New_York' >= %s AND ts AT TIME ZONE 'America/New_York' < %s ORDER BY ts""",(dstart,dend))
    L=cur.fetchall()
    out=[]
    for idv,tsutc,et,nm,spot,pnl,para in L:
        if spot is None: continue
        spot=float(spot); pnl=float(pnl)
        cur.execute("""SELECT rows FROM chain_snapshots WHERE spot IS NOT NULL
            ORDER BY abs(EXTRACT(EPOCH FROM (ts - %s))) LIMIT 1""",(tsutc,))
        rr=cur.fetchone()
        if not rr: continue
        rows=rr[0] if isinstance(rr[0],list) else json.loads(rr[0])
        dc=netgex_ceiling(rows,spot)
        vc=volland_res(cur,tsutc,spot,'charm')
        out.append(dict(id=idv,nm=nm,pnl=pnl,para=para,dc=dc,vc=vc))
    print(f"\n===== {label}  (n={len(out)} longs) =====")
    def s(pred,lab):
        g=[x for x in out if pred(x)]
        if not g: print(f"  {lab:<38} n=0"); return
        w=sum(1 for x in g if x['pnl']>0); pts=sum(x['pnl'] for x in g)
        print(f"  {lab:<38} n={len(g):>3} WR={w/len(g)*100:>3.0f}% pts={pts:>+7.1f} $@1={pts*5-len(g):>+6.0f}")
    print(" [TS +GEX ceiling distance above spot]")
    s(lambda x:x['dc'] is not None and x['dc']<=5,"spot within 5pt of TS +GEX ceiling (AT/INTO wall)")
    s(lambda x:x['dc'] is not None and 5<x['dc']<=15,"5-15pt below ceiling")
    s(lambda x:x['dc'] is not None and x['dc']>15,">15pt room below ceiling")
    s(lambda x:x['dc'] is None,"no +GEX wall within 60pt above (open sky)")
    print(" [Volland charm resistance distance above spot]")
    s(lambda x:x['vc'] is not None and x['vc']<=5,"within 5pt of charm resistance")
    s(lambda x:x['vc'] is not None and 5<x['vc']<=20,"5-20pt below charm res")
    s(lambda x:x['vc'] is not None and x['vc']>20,">20pt below charm res")
    return out

rec=study("SICK ERA Jun11-Jul9 (low-vol chop)","2026-06-11","2026-07-10")
ctl=study("CONTROL Mar (high-vol)","2026-03-01","2026-04-01")
c.close()
