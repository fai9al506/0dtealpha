"""Full-history: can shorts ride the trend DOWN, or do they fade flat days?
SC/AG/DD shorts, outcome_pnl by from_open regime by month. Settles 'ride the
trend down' definitively across eras (not just post-V16).
"""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    spath = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, spot
        FROM setup_log WHERE spot IS NOT NULL
          AND (ts AT TIME ZONE 'America/New_York')::time >= TIME '09:30' ORDER BY ts ASC""")).fetchall()
    day_open={}
    for et,spot in spath:
        d=et.date().isoformat()
        if d not in day_open: day_open[d]=float(spot)
    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, setup_name, grade, spot,
               outcome_pnl, mes_sim_outcome_pnl
        FROM setup_log
        WHERE setup_name IN ('Skew Charm','AG Short','DD Exhaustion')
          AND direction IN ('short','bearish')
          AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
        ORDER BY ts ASC""")).fetchall()
last={}; sig=[]
for et,setup,grade,spot,opnl,mpnl in rows:
    if setup in last and (et-last[setup])<timedelta(minutes=15): continue
    last[setup]=et
    if grade in ('C','LOG',None): continue
    d=et.date().isoformat()
    fo=float(spot)-day_open.get(d,float(spot))
    sig.append({"mo":d[:7],"day":d,"setup":setup,"fo":fo,
                "pnl":float(opnl),"mpnl":float(mpnl) if mpnl is not None else None})
months=sorted(set(t['mo'] for t in sig))
def agg(ts,f='pnl'):
    vals=[t[f] for t in ts if t[f] is not None]
    if not vals: return "n=  0"
    w=sum(1 for v in vals if v>0); s=sum(vals)
    return f"n={len(vals):>3} WR={100*w/len(vals):3.0f}% sum={s:+7.1f}p (${s*5:+6.0f})"
print("SC/AG/DD SHORTS (A+/A/B) by intraday regime, by month:")
print("  DOWN=from_open<=-15 (chase trend down) | FLAT=(-15,+15] (fade) | UP=>+15 (fade strength)\n")
for label,cond in [("DOWN (chase down)",lambda t:t['fo']<=-15),
                   ("FLAT (fade)",lambda t:-15<t['fo']<=15),
                   ("UP (fade strength)",lambda t:t['fo']>15)]:
    print(f"--- {label} ---")
    for mo in months:
        mm=[t for t in sig if t['mo']==mo and cond(t)]
        print(f"  {mo:<8}{agg(mm)}")
    allt=[t for t in sig if cond(t)]
    print(f"  {'TOTAL':<8}{agg(allt)}   mes_sim {agg(allt,'mpnl')}")
    print()
