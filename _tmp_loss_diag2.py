"""Loss diagnosis part 2 — direction & setup attribution.

A) Long vs Short WR/P&L from tsrt_daily_stmt trades JSONB (broker truth).
B) Per-setup, per-direction from setup_log + real_trade_orders (attribution).
Winning era (<=2026-06-04) vs Loss window (>=2026-06-05).
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

def era(d):  # d is 'YYYY-MM-DD'
    return 'WIN' if d <= '2026-06-04' else 'LOSS'

# ---------- A) direction split from broker FIFO trades ----------
cur.execute("""SELECT day, trades FROM tsrt_daily_stmt
               WHERE day >= '2026-05-19' ORDER BY day""")
dir_stat = defaultdict(lambda: {'n':0,'w':0,'usd':0.0})  # key (era,dir)
for day, trades in cur.fetchall():
    ds = str(day)
    t = trades if isinstance(trades, list) else (json.loads(trades) if trades else [])
    for it in t:
        d = it.get('dir','?')
        usd = float(it.get('usd') or 0)
        k = (era(ds), d)
        dir_stat[k]['n'] += 1
        dir_stat[k]['usd'] += usd
        if usd > 0: dir_stat[k]['w'] += 1

print("=== A) Direction split (broker FIFO) ===")
print(f"{'era':5} {'dir':6} {'n':>4} {'W':>4} {'WR':>5} {'net$':>10} {'$/trade':>8}")
for k in sorted(dir_stat):
    s = dir_stat[k]
    wr = s['w']/s['n']*100 if s['n'] else 0
    print(f"{k[0]:5} {k[1]:6} {s['n']:>4} {s['w']:>4} {wr:>4.0f}% {s['usd']:>10.2f} {s['usd']/s['n']:>8.2f}")

# ---------- B) per-setup attribution from setup_log + real_trade_orders ----------
MES_PT = 5.0
def real_pts(state, direction):
    if isinstance(state, str): state = json.loads(state)
    fill = state.get('entry_fill_price') or state.get('fill_price')
    exit_p = state.get('stop_fill_price') or state.get('close_fill_price')
    if fill is None or exit_p is None: return None
    fill, exit_p = float(fill), float(exit_p)
    return (exit_p - fill) if direction == 'long' else (fill - exit_p)

cur.execute("""
    SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
           sl.setup_name, sl.direction, rto.state
    FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-05-19'
    ORDER BY sl.ts
""")
setup_stat = defaultdict(lambda: {'n':0,'w':0,'pts':0.0})  # key (era,setup,dir)
for d, setup, direction, state in cur.fetchall():
    ds = str(d)
    pts = real_pts(state, direction)
    if pts is None: continue
    k = (era(ds), setup, direction)
    setup_stat[k]['n'] += 1
    setup_stat[k]['pts'] += pts
    if pts > 0: setup_stat[k]['w'] += 1

print("\n=== B) Per-setup x direction (lid attribution, $ @1MES) ===")
print(f"{'era':5} {'setup':16} {'dir':6} {'n':>4} {'W':>4} {'WR':>5} {'pts':>8} {'$':>9}")
for k in sorted(setup_stat, key=lambda x:(x[0],x[1],x[2])):
    s = setup_stat[k]
    wr = s['w']/s['n']*100 if s['n'] else 0
    print(f"{k[0]:5} {k[1]:16} {k[2]:6} {s['n']:>4} {s['w']:>4} {wr:>4.0f}% {s['pts']:>8.1f} {s['pts']*MES_PT:>9.0f}")

# Per-setup totals across loss window only, sorted by $ damage
print("\n=== Loss window (Jun05-12): setup damage ranked ===")
dmg = defaultdict(lambda: {'n':0,'w':0,'pts':0.0})
for (e,setup,direction),s in setup_stat.items():
    if e!='LOSS': continue
    dmg[setup]['n']+=s['n']; dmg[setup]['w']+=s['w']; dmg[setup]['pts']+=s['pts']
for setup in sorted(dmg, key=lambda x:dmg[x]['pts']):
    s=dmg[setup]; wr=s['w']/s['n']*100 if s['n'] else 0
    print(f"  {setup:16} n={s['n']:>3} WR={wr:>3.0f}% pts={s['pts']:>7.1f} ${s['pts']*MES_PT:>7.0f}")

conn.close()
