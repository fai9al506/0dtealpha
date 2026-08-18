import os, psycopg2, json, bisect
from collections import defaultdict
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
# volland expiration values
cur.execute("SELECT DISTINCT expiration_option FROM volland_exposure_points WHERE ts_utc>='2026-07-06' LIMIT 20")
print("volland expiration_option values:",[r[0] for r in cur.fetchall()])

def ceil_strike(rows, spot):
    best=None
    for r in rows:
        try: cg=float(r[3]); coi=float(r[1]); pg=float(r[17]); poi=float(r[19]); k=float(r[10])
        except: continue
        net=cg*coi - pg*poi
        if k>spot and 0<k-spot<=60 and net>0 and (best is None or net>best[1]): best=(k,net)
    return best[0] if best else None

# sick era: recompute dc + join setup + paradigm
cur.execute("""SELECT ts, spot, rows FROM chain_snapshots WHERE spot IS NOT NULL
   AND ts>='2026-06-11'::timestamptz - interval '6h' AND ts<'2026-07-10'::timestamptz ORDER BY ts""")
snaps=[]
for ts,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows)
    cs=ceil_strike(rows,float(spot)); snaps.append((ts.timestamp(),float(spot),(cs-float(spot)) if cs else None))
st=[s[0] for s in snaps]
cur.execute("""SELECT id, ts, setup_name, spot, outcome_pnl, paradigm, vix FROM setup_log
   WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
     AND ts AT TIME ZONE 'America/New_York'>='2026-06-11' AND ts AT TIME ZONE 'America/New_York'<'2026-07-10' ORDER BY ts""")
out=[]
for idv,ts,nm,spot,pnl,para,vix in cur.fetchall():
    if spot is None: continue
    e=ts.timestamp(); i=bisect.bisect_left(st,e); cand=[k for k in (i,i-1) if 0<=k<len(st)]
    j=min(cand,key=lambda k:abs(st[k]-e)); dc=snaps[j][2] if abs(st[j]-e)<=600 else None
    void = (dc is None) or (dc>15)
    out.append(dict(nm=nm,pnl=float(pnl),para=para,dc=dc,void=void))

print("\n=== VOID longs (no wall <=15pt above) by SETUP, sick era ===")
bys=defaultdict(list)
for x in out:
    if x['void']: bys[x['nm']].append(x)
for nm,g in sorted(bys.items(),key=lambda x:-len(x[1])):
    w=sum(1 for x in g if x['pnl']>0)
    print(f"  {nm:<16} n={len(g):>2} WR={w/len(g)*100:>3.0f}% pts={sum(x['pnl'] for x in g):>+7.1f}")
print("\n=== VOID longs by PARADIGM (is it just GEX-TARGET?) ===")
byp=defaultdict(list)
for x in out:
    if x['void']: byp[x['para']].append(x)
for p,g in sorted(byp.items(),key=lambda x:-len(x[1])):
    w=sum(1 for x in g if x['pnl']>0)
    print(f"  {str(p):<16} n={len(g):>2} WR={w/len(g)*100:>3.0f}% pts={sum(x['pnl'] for x in g):>+7.1f}")

print("\n=== the gate: block low-vol longs in a GEX void (dc None or >15) ===")
keep=[x for x in out if not x['void']]; block=[x for x in out if x['void']]
def tot(g,lab):
    w=sum(1 for x in g if x['pnl']>0) if g else 0
    print(f"  {lab:<20} n={len(g):>3} WR={(w/len(g)*100 if g else 0):>3.0f}% pts={sum(x['pnl'] for x in g):>+7.1f} $@1={sum(x['pnl'] for x in g)*5-len(g):>+6.0f}")
tot(out,"all longs (now)"); tot(keep,"KEEP (near wall)"); tot(block,"BLOCK (void/extended)")
c.close()
