"""Recompute the v3 GEX-Long classifier using TS CHAIN GEX (the LIVE source,
mirroring main.py _gex_long_v3_features) + Volland charm, for today's morning.
Settles whether the live detector's structure was a GEX Long at 9:52."""
import psycopg2, json

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
cur = psycopg2.connect(DB).cursor()

# call cols [0..9], Strike=10, put cols [11..20]
iS, iCOI, iCG, iPG, iPOI = 10, 1, 3, 17, 19

def charm_near(ts):
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s AND greek='charm'
                   ORDER BY ts_utc DESC LIMIT 1""", (ts, ts))
    r = cur.fetchone()
    if not r: return []
    cur.execute("""SELECT strike, value FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='charm' AND strike BETWEEN %s AND %s
                   ORDER BY strike""", (r[0], -1e9, 1e9))
    return [(float(s), float(v)) for s, v in cur.fetchall()]

def classify(f):
    if f is None: return 'NO_DATA'
    if not f['CORE_R3']: return 'BAD'
    if f['R_VETO']: return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']): return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']): return 'A'
    if f['CORE_R2'] or f['R5_align']: return 'B'
    return 'C'

cur.execute("""SELECT ts, (ts AT TIME ZONE 'America/New_York')::time, spot, rows
               FROM chain_snapshots
               WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-02'
                 AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:40' AND '12:30'
                 AND spot IS NOT NULL
               ORDER BY ts""")
snaps = cur.fetchall()
print(f"{'ET':9s} {'spot':>8s} {'verdict':7s} {'sg_above':>9s} {'sg_below':>9s} R3 R2 R5 VETO totGEX totCH")
for ts, et, spot, rows in snaps:
    rows = rows if isinstance(rows, list) else json.loads(rows)
    gex = []
    for rr in rows:
        try: s = float(rr[iS])
        except Exception: continue
        if not (spot-50 <= s <= spot+50): continue
        cg=float(rr[iCG] or 0); coi=float(rr[iCOI] or 0)
        pg=float(rr[iPG] or 0); poi=float(rr[iPOI] or 0)
        gex.append((s, cg*coi - pg*poi))
    charm = charm_near(ts)
    charm = [(s,v) for s,v in charm if spot-50 <= s <= spot+50]
    if not gex or not charm:
        print(f"{str(et)[:8]} {spot:8.1f} NO_DATA"); continue
    gb=[(s,v) for s,v in gex if s<spot]; ga=[(s,v) for s,v in gex if s>spot]
    ca=[(s,v) for s,v in charm if s>spot]
    sgb=max(gb,key=lambda x:abs(x[1])) if gb else (None,0)
    sga=max(ga,key=lambda x:abs(x[1])) if ga else (None,0)
    nca=[(s,v) for s,v in ca if v<0]
    bcm=min(nca,key=lambda x:x[1])[0] if nca else None
    tg=sum(v for _,v in gex); tc=sum(v for _,v in charm)
    acpp=(sum(1 for _,v in ca if v>0)/max(len(ca),1)*100)
    R5=(bcm is not None and sga[0] is not None and sga[1]>0 and abs(bcm-sga[0])<=10)
    f={'CORE_R3':sga[1]>0,'CORE_R2':sgb[1]<0,'R5_align':R5,
       'R_charm_bullish':tc<0,'R_gex_regime_pos':tg>=0,
       'R_VETO':(acpp>=80) and (not R5),'gex_magnet_strike':sga[0]}
    v=classify(f)
    print(f"{str(et)[:8]} {spot:8.1f} {v:7s} {str(sga[0]):>9s} {str(sgb[0]):>9s} "
          f"{f['CORE_R3']!s:5s}{f['CORE_R2']!s:5s}{f['R5_align']!s:5s}{f['R_VETO']!s:5s} "
          f"{tg:+.0f} {tc:+.0f}")
