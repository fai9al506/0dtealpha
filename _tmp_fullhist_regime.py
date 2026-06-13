"""FULL-HISTORY regime backtest (Feb 3 - Jun 9 2026).
Test: do SC/DD LONGS entered on a down intraday regime (from_open<=-15) lose
across EVERY era? P&L = setup_log.outcome_pnl (portal sim, points). Cross-check
with mes_sim (broker-realistic) where available. ES Abs longs = control.

Dedup ~15min per (setup,direction) to approximate distinct placed trades (the
portal cooldown). from_open = spot - session-open spot (first row >=09:30 ET).
No lookahead: from_open at entry uses only spot up to that row.
"""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text
from datetime import timedelta
engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    # full spot path per day from ALL rows (accurate session open)
    spath = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, spot
        FROM setup_log
        WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
        ORDER BY ts ASC""")).fetchall()
    day_open = {}
    for et, spot in spath:
        d = et.date().isoformat()
        if d not in day_open:
            day_open[d] = float(spot)

    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, setup_name, direction,
               grade, spot, greek_alignment, vix, outcome_pnl, mes_sim_outcome_pnl
        FROM setup_log
        WHERE setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption')
          AND direction IN ('long','bullish')
          AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
        ORDER BY ts ASC""")).fetchall()

# dedup 15min per (setup)
last_kept = {}
sig = []
for et, setup, direction, grade, spot, align, vix, opnl, mpnl in rows:
    key = setup
    if key in last_kept and (et - last_kept[key]) < timedelta(minutes=15):
        continue
    last_kept[key] = et
    d = et.date().isoformat()
    fo = float(spot) - day_open.get(d, float(spot))
    sig.append({"et":et,"day":d,"mo":d[:7],"setup":setup,"grade":grade,
                "align":align,"vix":float(vix) if vix is not None else None,
                "fo":fo,"pnl":float(opnl),"mpnl":float(mpnl) if mpnl is not None else None})

def quality(t):
    """Approx tradeable: drop C/LOG; DD long needs align in [0,2] & vix<22."""
    if t['grade'] in ('C','LOG',None): return False
    if t['setup']=='DD Exhaustion':
        a=t['align'] if t['align'] is not None else 0
        if a<0 or a>=3: return False
        if t['vix'] is not None and t['vix']>=22: return False
    return True

def st(ts, field='pnl'):
    n=len(ts)
    if n==0: return "n=  0"
    vals=[t[field] for t in ts if t[field] is not None]
    if not vals: return f"n={n:>3} (no {field})"
    w=sum(1 for v in vals if v>0); tot=sum(vals)
    return f"n={len(vals):>3} WR={100*w/len(vals):4.0f}% sumP={tot:+7.1f} (${tot*5:+6.0f}) mean={tot/len(vals):+5.1f}p"

for scope_name, scope in [("ALL grades", lambda t: True), ("QUALITY (A+/A/B + DD gates)", quality)]:
    print(f"\n================ SC+DD LONGS — {scope_name} ================")
    SCDD=[t for t in sig if t['setup'] in ('Skew Charm','DD Exhaustion') and scope(t)]
    print("BY MONTH  [down=from_open<=-15  vs  up/flat=from_open>-15]:")
    print(f"  {'month':<8}{'DOWN bucket':<48}{'UP/FLAT bucket'}")
    for mo in sorted(set(t['mo'] for t in SCDD)):
        mm=[t for t in SCDD if t['mo']==mo]
        dn=[t for t in mm if t['fo']<=-15]; up=[t for t in mm if t['fo']>-15]
        print(f"  {mo:<8}{st(dn):<48}{st(up)}")
    dn=[t for t in SCDD if t['fo']<=-15]; up=[t for t in SCDD if t['fo']>-15]
    print(f"  {'TOTAL':<8}{st(dn):<48}{st(up)}")
    print(f"  distinct down-days: {len(set(t['day'] for t in dn))}")
    # mes_sim cross-check on down bucket (recent era only)
    dn_m=[t for t in dn if t['mpnl'] is not None]
    print(f"  DOWN bucket mes_sim cross-check: {st(dn_m,'mpnl')}")

# ES Abs longs control
print("\n================ ES ABSORPTION LONGS (control — leave alone) ================")
ES=[t for t in sig if t['setup']=='ES Absorption' and t['grade'] in ('A','A+')]
for mo in sorted(set(t['mo'] for t in ES)):
    mm=[t for t in ES if t['mo']==mo]
    dn=[t for t in mm if t['fo']<=-15]; up=[t for t in mm if t['fo']>-15]
    print(f"  {mo:<8}DOWN {st(dn):<46}UP {st(up)}")
dn=[t for t in ES if t['fo']<=-15]
print(f"  ES Abs A/A+ longs on DOWN days TOTAL: {st(dn)}")
