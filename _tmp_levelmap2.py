import os
from collections import defaultdict
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])
DAY="2026-06-08"
with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    # spot for that day (last)
    spot=conn.execute(text("""SELECT spot FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
        ORDER BY ts DESC LIMIT 1"""),{"d":DAY}).scalar()
    spot=float(spot) if spot else 7405.0
    # latest value per (greek, expiration, strike) within the day
    rows=conn.execute(text("""
        SELECT DISTINCT ON (greek, expiration_option, strike)
               greek, expiration_option, strike, value
        FROM volland_exposure_points
        WHERE ts_utc::date = DATE :d
          AND greek IN ('vanna','gamma')
          AND expiration_option IN ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
          AND ABS(strike - :spot) <= 320
        ORDER BY greek, expiration_option, strike, ts_utc DESC
    """),{"d":DAY,"spot":spot}).fetchall()

van=defaultdict(dict); gam=defaultdict(dict)
for greek,exp,strike,val in rows:
    (van if greek=='vanna' else gam)[float(strike)][exp]=float(val)
print(f"DAY {DAY} | spot ~ {spot:.0f} | strikes loaded: vanna {len(van)}, gamma {len(gam)}\n")

TAGS={'TODAY':'0','THIS_WEEK':'W','THIRTY_NEXT_DAYS':'M'}
def stack(d): return "+".join(TAGS[e] for e in ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS') if abs(d.get(e,0))>5e6) or "-"

strikes=sorted(set(list(van)+list(gam)),reverse=True)
print(f"{'strike':>7}{'  vanna$M':>10}{'  gam$M':>8}  v[0/W/M]            role")
for k in strikes:
    vsum=sum(van.get(k,{}).values()); gsum=sum(gam.get(k,{}).values())
    if abs(vsum)<4e7 and abs(gsum)<3e7: continue
    per=" ".join(f"{TAGS[e]}:{van.get(k,{}).get(e,0)/1e6:+.0f}" for e in ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS'))
    role=""
    if k>spot: role="RESISTANCE (vanna wall above)" if vsum>0 else "repellent above"
    elif k<spot: role="SUPPORT/FLOOR (neg-vanna)" if vsum<0 else "support below"
    mark=" <==SPOT" if abs(k-spot)<8 else ""
    print(f"{k:>7.0f}{vsum/1e6:>+10.0f}{gsum/1e6:>+8.0f}  [{stack(van.get(k,{})):<7}] {per:<22} {role}{mark}")

print("\nHIS PUBLISHED 6/8: 7600 cap | 7575 WALL +588M | 7500 transit | 7375 pin | 7350/7100/7000 floors")
