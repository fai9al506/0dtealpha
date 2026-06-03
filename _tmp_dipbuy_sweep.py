"""
Dip-Buy parameter/filter sweep on the ES-mirrored 30s SPX path.
Era-split everything (Feb-Mar vs Apr-Jun) to catch regime overfit.
"""
import os, psycopg2
from collections import defaultdict
from datetime import time as dtime, datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
WS, CUTOFF = dtime(9, 30), dtime(16, 0)
D_LO = "2026-02-18"

cur.execute("""select ts, spot from chain_snapshots
               where ts::date >= %s and spot is not null order by ts""", (D_LO,))
chain = defaultdict(list)
for ts, spot in cur.fetchall():
    et = ts.astimezone(ET)
    chain[et.date()].append((et, float(spot)))

def load_bars(tbl, extra=""):
    cur.execute(f"""select ts_end, bar_close from {tbl}
                    where range_pts=5 and ts_end is not null {extra}
                      and ts_end::date >= %s order by ts_end""", (D_LO,))
    out = defaultdict(list)
    for tse, c in cur.fetchall():
        et = tse.astimezone(ET)
        out[et.date()].append((et, float(c)))
    return out

vps  = load_bars("vps_es_range_bars")
rith = load_bars("es_range_bars", "and source='rithmic'")

paths = {}   # date -> list[(et_30s, spx)]
prior_close = {}
chaindays = sorted(chain)
for d in chaindays:
    bars = [b for b in vps.get(d, []) if WS <= b[0].time() <= CUTOFF]
    if len(bars) < 20:
        bars = [b for b in rith.get(d, []) if WS <= b[0].time() <= CUTOFF]
    snaps = chain[d]
    if len(bars) < 20 or len(snaps) < 10:
        continue
    bp = []; bi = 0
    for set_, sp in snaps:
        while bi < len(bars) - 1 and bars[bi+1][0] <= set_:
            bi += 1
        if bars[bi][0] <= set_:
            bp.append((set_, sp - bars[bi][1]))
    if not bp: continue
    out = []; pi = 0
    t = datetime(d.year, d.month, d.day, 9, 30, tzinfo=ET)
    end = datetime(d.year, d.month, d.day, 16, 0, tzinfo=ET)
    j = -1
    while t <= end:
        while j + 1 < len(bars) and bars[j+1][0] <= t:
            j += 1
        if j >= 0:
            while pi < len(bp) - 1 and bp[pi+1][0] <= t:
                pi += 1
            out.append((t, bars[j][1] + bp[pi][1]))
        t += timedelta(seconds=30)
    if out:
        paths[d] = out
        prevs = [x for x in chaindays if x < d and chain[x]]
        prior_close[d] = chain[prevs[-1]][-1][1] if prevs else None

def run(dip, conf, target, stop, w_start, w_end, persist=0,
        gap_lo=None, gap_hi=None, vspc_lo=None, vspc_hi=None, dipmax=None):
    """persist = extra consecutive 30s samples that must hold >= lo+conf before entry."""
    trades = []
    for d, path in sorted(paths.items()):
        pc = prior_close.get(d)
        sess_open = path[0][1]
        gap = (sess_open - pc) if pc else None
        if gap_lo is not None and (gap is None or gap < gap_lo): continue
        if gap_hi is not None and (gap is None or gap > gap_hi): continue
        sess_high = -1e9; in_dip = False; lo = 1e9
        ent = None; hold = 0
        for i, (et, sp) in enumerate(path):
            if et.time() > w_end: break
            if et.time() < w_start:
                sess_high = max(sess_high, sp); continue
            sess_high = max(sess_high, sp)
            if not in_dip:
                if sp <= sess_high - dip:
                    in_dip = True; lo = sp; hold = 0
            else:
                lo = min(lo, sp)
                if sp >= lo + conf:
                    hold += 1
                    if hold > persist:
                        ent = (i, sp, sess_high - lo); break
                else:
                    hold = 0
        if not ent: continue
        i, entry, depth = ent
        if dipmax is not None and depth > dipmax: continue
        vspc = (entry - pc) if pc else None
        if vspc_lo is not None and (vspc is None or vspc < vspc_lo): continue
        if vspc_hi is not None and (vspc is None or vspc > vspc_hi): continue
        pnl, res = None, "EXPIRED"
        last = entry
        for et, sp in path[i+1:]:
            if et.time() > CUTOFF: break
            last = sp
            if sp <= entry - stop:  pnl, res = -stop, "LOSS"; break
            if sp >= entry + target: pnl, res = target, "WIN"; break
        if pnl is None: pnl = round(last - entry, 2)
        trades.append((d, pnl, res))
    return trades

def summ(ts):
    if not ts: return "n=0"
    n = len(ts); w = sum(1 for t in ts if t[2] == "WIN")
    tot = sum(t[1] for t in ts)
    cum = peak = dd = 0
    for t in sorted(ts):
        cum += t[1]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return f"n={n:<3} WR={100*w/n:>5.1f}% tot={tot:>+7.1f} dd={dd:>6.1f}"

def era(ts, lo, hi): return [t for t in ts if lo <= t[0].isoformat() <= hi]

def show(label, ts):
    print(f"{label:<46} FULL {summ(ts)} | FebMar {summ(era(ts,'2026-02-01','2026-03-31'))} | AprJun {summ(era(ts,'2026-04-01','2026-06-30'))}")

print("=== 1) CONFIRM sweep (dip=8, T10/S8, 9:30-11:30) ===")
for conf in (4, 5, 6, 7, 8):
    show(f"conf={conf}", run(8, conf, 10, 8, dtime(9,30), dtime(11,30)))
print("\n=== 2) PERSIST sweep (conf=4) — bounce must hold N extra 30s samples ===")
for p in (0, 2, 4, 8):
    show(f"persist={p} ({p*30}s hold)", run(8, 4, 10, 8, dtime(9,30), dtime(11,30), persist=p))
print("\n=== 3) DIP sweep (conf=4) ===")
for dip in (8, 10, 12, 15):
    show(f"dip={dip}", run(dip, 4, 10, 8, dtime(9,30), dtime(11,30)))
print("\n=== 4) TARGET/STOP sweep (dip=8 conf=4) ===")
for tg, st in ((10,8),(12,8),(15,8),(15,10),(20,10),(10,6),(8,8),(10,10),(10,12)):
    show(f"T={tg}/S={st}", run(8, 4, tg, st, dtime(9,30), dtime(11,30)))
print("\n=== 5) WINDOW sweep (dip=8 conf=4 T10/S8) ===")
for ws, we in ((dtime(9,30),dtime(11,30)),(dtime(9,45),dtime(11,30)),(dtime(10,0),dtime(11,30)),
               (dtime(9,30),dtime(12,30)),(dtime(9,30),dtime(10,30))):
    show(f"win {ws}-{we}", run(8, 4, 10, 8, ws, we))
print("\n=== 6) GAP filter (base config) ===")
show("gap >= -30 (no big gap-down)", run(8,4,10,8,dtime(9,30),dtime(11,30),gap_lo=-30))
show("gap <= +30 (no big gap-up)",  run(8,4,10,8,dtime(9,30),dtime(11,30),gap_hi=30))
show("|gap| <= 30",                 run(8,4,10,8,dtime(9,30),dtime(11,30),gap_lo=-30,gap_hi=30))
print("\n=== 7) entry-vs-prior-close filter ===")
show("vs_pc >= -2 (S196 prior_close_ok)", run(8,4,10,8,dtime(9,30),dtime(11,30),vspc_lo=-2))
show("vs_pc < -2",                        run(8,4,10,8,dtime(9,30),dtime(11,30),vspc_hi=-2.01))
print("\n=== 8) dip-depth cap (skip falling knives) ===")
for dm in (15, 20, 25):
    show(f"dip_depth <= {dm}", run(8,4,10,8,dtime(9,30),dtime(11,30),dipmax=dm))
conn.close()
