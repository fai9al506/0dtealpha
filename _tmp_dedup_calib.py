import os, sys
sys.path.insert(0, '.')
import psycopg2
from zoneinfo import ZoneInfo
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; ET = g['ET']
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()
COLS = g['COLS']; KEYS = g['KEYS']

def load_range(d0, d1):
    cur.execute(f"SELECT {COLS} FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= %s AND (ts AT TIME ZONE 'America/New_York')::date <= %s ORDER BY ts", (d0, d1))
    return [dict(zip(KEYS, r)) for r in cur.fetchall()]

def dedup_place(sigs, dedup_min, cap, hold_min):
    placed = []; last = {}; openq = {'L': [], 'S': []}
    for l in sigs:
        ts = l['ts']; sn = l['setup_name'] or ''
        isL = l['direction'] in ('long', 'bullish'); side = 'L' if isL else 'S'
        openq[side] = [t for t in openq[side] if (ts - t).total_seconds() <= hold_min*60]
        k = (sn, side)
        if k in last and (ts - last[k]).total_seconds() < dedup_min*60: continue
        if len(openq[side]) >= cap: continue
        placed.append(l); last[k] = ts; openq[side].append(ts)
    return placed

# Anchor: May 18-31 v16 = 78 real placed trades (MCHK broker-verified)
may = load_range('2026-05-18', '2026-05-31')
v16 = FILTERS['v16']
sigs = [l for l in may if v16(l)]
print(f"May18-31 v16 raw signals: {len(sigs)}   (anchor: 78 real placed)")
for dm, cap, hold in [(15,3,30),(20,3,45),(30,2,45),(30,3,60),(45,2,60)]:
    p = dedup_place(sigs, dm, cap, hold)
    print(f"  dedup={dm}min cap={cap} hold={hold}min -> placed={len(p)}")
