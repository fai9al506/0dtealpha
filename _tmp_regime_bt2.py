"""Follow-up: day distribution + per-setup + shorts regime split + walk-forward sanity.
Reuses the same broker-fill P&L + regime features as _tmp_regime_bt.py.
"""
import os, json
from collections import defaultdict
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
ERA = "2026-05-18"

with engine.connect() as conn:
    spath = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, spot, vix
        FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE :era
        AND spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
        ORDER BY ts ASC"""), {"era": ERA}).fetchall()
    day_path = defaultdict(list)
    for et, spot, vix in spath:
        day_path[et.date().isoformat()].append((et, float(spot), float(vix) if vix is not None else None))
    def regime_at(day, ts_et):
        path = day_path.get(day, []); prior = [p for p in path if p[0] <= ts_et]
        if not prior: return None
        open_spot = path[0][1]; open_vix = next((v for (_,_,v) in path if v is not None), None)
        cur_spot = prior[-1][1]; cur_vix = next((v for (_,_,v) in reversed(prior) if v is not None), None)
        return {"from_open": cur_spot-open_spot,
                "vix_chg": (cur_vix-open_vix) if (cur_vix is not None and open_vix is not None) else None}
    rows = conn.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York') as et, sl.setup_name, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= DATE :era ORDER BY sl.ts ASC
    """), {"era": ERA}).fetchall()

trades = []
for sid, et, setup, direction, st in rows:
    if not isinstance(st, dict):
        try: st = json.loads(st)
        except Exception: st = {}
    entry = st.get('fill_price'); exitp = st.get('close_fill_price')
    if entry is None or exitp is None: continue
    is_short = ('bear' in (direction or '')) or (direction=='short') or (setup=='AG Short')
    pts = (entry-exitp) if is_short else (exitp-entry)
    reg = regime_at(et.date().isoformat(), et)
    trades.append({"lid":sid,"et":et,"day":et.date().isoformat(),"setup":setup,
                   "is_short":is_short,"pts":pts,"usd":pts*5.0,"reg":reg})

def stats(ts):
    n=len(ts)
    if n==0: return "n=0"
    w=sum(1 for t in ts if t['pts']>0); usd=sum(t['usd'] for t in ts)
    return f"n={n:>3} WR={100*w/n:4.0f}% net=${usd:+8.0f} avg=${usd/n:+6.1f}"

longs=[t for t in trades if not t['is_short'] and t['reg'] and t['reg']['from_open'] is not None]
shorts=[t for t in trades if t['is_short'] and t['reg'] and t['reg']['from_open'] is not None]

# Down-long losses by DAY
print("=== LONGS with from_open<=-15, BY DAY ===")
downL=[t for t in longs if t['reg']['from_open']<=-15]
byday=defaultdict(list)
for t in downL: byday[t['day']].append(t)
for d in sorted(byday):
    print(f"  {d}: {stats(byday[d])}")
print(f"  TOTAL down-longs: {stats(downL)}")
print(f"  # distinct days: {len(byday)}")
print()
# Per-setup within down bucket
print("=== down-longs (from_open<=-15) BY SETUP ===")
bys=defaultdict(list)
for t in downL: bys[t['setup']].append(t)
for s in sorted(bys, key=lambda x:-len(bys[x])): print(f"  {s:<16}{stats(bys[s])}")
print()
# SHORTS regime split — do shorts win MORE on down days?
print("=== SHORTS regime split ===")
print("  shorts from_open<=-15 (down day):", stats([t for t in shorts if t['reg']['from_open']<=-15]))
print("  shorts from_open in (-15,+15]:   ", stats([t for t in shorts if -15<t['reg']['from_open']<=15]))
print("  shorts from_open> +15 (up day):  ", stats([t for t in shorts if t['reg']['from_open']>15]))
print("  shorts BY SETUP:")
bss=defaultdict(list)
for t in shorts: bss[t['setup']].append(t)
for s in sorted(bss, key=lambda x:-len(bss[x])): print(f"    {s:<16}{stats(bss[s])}")
print()
# Walk-forward: split era in half by date
days_sorted=sorted(set(t['day'] for t in longs))
mid=days_sorted[len(days_sorted)//2]
print(f"=== WALK-FORWARD (mid={mid}) — block rule from_open<=-15 on LONGS ===")
for label, cond in [("FIRST half", lambda d:d<mid),("SECOND half", lambda d:d>=mid)]:
    half=[t for t in longs if cond(t['day'])]
    kept=[t for t in half if t['reg']['from_open']>-15]
    blocked=[t for t in half if t['reg']['from_open']<=-15]
    print(f"  {label}: ALL {stats(half)}")
    print(f"            KEPT(>-15) {stats(kept)}  | BLOCKED(<=-15) {stats(blocked)}")
