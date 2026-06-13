"""Reconstruct Dark Matter's multi-expiry level map from volland_exposure_points,
and validate against his published 6/8 Key Levels Table.

His method: aggregate vanna/gamma per strike across expiries; positive vanna above
spot = resistance wall, negative vanna below = support floor; confluence across
expiries (TODAY/THIS_WEEK/THIRTY_NEXT_DAYS) = his term-stack strength.
"""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])

DAY="2026-06-08"  # his plan day
with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    # pick the snapshot nearest his publish (use last ts of that day)
    ts=conn.execute(text("""SELECT MAX(ts_utc) FROM volland_exposure_points
        WHERE ts_utc::date = DATE :d"""),{"d":DAY}).scalar()
    if ts is None:
        # fallback nearest day with data
        ts=conn.execute(text("""SELECT MAX(ts_utc) FROM volland_exposure_points
            WHERE ts_utc::date <= DATE :d"""),{"d":DAY}).scalar()
    print("snapshot ts_utc:", ts)
    rows=conn.execute(text("""
        SELECT greek, expiration_option, strike, value, current_price
        FROM volland_exposure_points
        WHERE ts_utc = :ts AND greek IN ('vanna','gamma')
          AND expiration_option IN ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
    """),{"ts":ts}).fetchall()

spot=None
# strike -> {expiry: vanna}, plus gamma
van=defaultdict(dict); gam=defaultdict(dict)
for greek,exp,strike,val,cp in rows:
    spot=float(cp) if cp else spot
    if greek=='vanna': van[float(strike)][exp]=float(val)
    else: gam[float(strike)][exp]=float(val)

print(f"spot ~ {spot}\n")
TAGS={'TODAY':'0','THIS_WEEK':'W','THIRTY_NEXT_DAYS':'M'}
def termstack(d):
    return "+".join(TAGS[e] for e in ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS') if e in d and abs(d.get(e,0))>5e6)

# build level rows: aggregate vanna across expiries
levels=[]
strikes=sorted(set(list(van.keys())+list(gam.keys())))
for k in strikes:
    if spot and abs(k-spot)>320: continue
    vsum=sum(van.get(k,{}).values()); gsum=sum(gam.get(k,{}).values())
    if abs(vsum)<3e7 and abs(gsum)<2e7: continue
    levels.append((k,vsum,gsum,termstack(van.get(k,{})),termstack(gam.get(k,{}))))

# resistance = positive vanna above spot (his polarity: under EXTREME, +vanna above = resistance)
print("=== RECONSTRUCTED LEVEL MAP (vanna $ aggregated across 0/W/M) ===")
print(f"{'strike':>7} {'vanna$M':>9} {'gamma$M':>9}  v-stack  g-stack  role")
for k,v,g,vs,gs in sorted(levels,key=lambda x:-x[0]):
    role=""
    if spot:
        if k>spot and v>0: role="RESIST (vanna wall above)"
        elif k<spot and v<0: role="SUPPORT (neg-vanna floor below)"
        elif k>spot and v<0: role="repellent/repel above"
        elif k<spot and v>0: role="support-ish below"
    mark=" <== SPOT" if spot and abs(k-spot)<7 else ""
    print(f"{k:>7.0f} {v/1e6:>+9.0f} {g/1e6:>+9.0f}  [{vs:<6}] [{gs:<6}] {role}{mark}")

print("\n=== HIS PUBLISHED 6/8 levels (for comparison) ===")
print("  7600 vanna cap | 7575 WALL +$588M(all)/+$384M(M) | 7500 transit | 7375 delta-pin")
print("  7350 gamma floor | 7100 vanna FLOOR | 7000 delta FLOOR")
