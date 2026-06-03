"""Drill into the persist (bounce-hold) finding: dose-response, monthly stability, June check."""
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

paths = {}
for d in sorted(chain):
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
    if out: paths[d] = out

def run(persist, target=10, stop=8, dip=8, conf=4, w_end=dtime(11,30)):
    trades = []
    for d, path in sorted(paths.items()):
        sess_high = -1e9; in_dip = False; lo = 1e9; ent = None; hold = 0
        for i, (et, sp) in enumerate(path):
            if et.time() > w_end: break
            sess_high = max(sess_high, sp)
            if not in_dip:
                if sp <= sess_high - dip:
                    in_dip = True; lo = sp; hold = 0
            else:
                lo = min(lo, sp)
                if sp >= lo + conf:
                    hold += 1
                    if hold > persist:
                        ent = (i, sp, et); break
                else:
                    hold = 0
        if not ent: continue
        i, entry, eet = ent
        pnl, res = None, "EXPIRED"; last = entry
        for et, sp in path[i+1:]:
            if et.time() > CUTOFF: break
            last = sp
            if sp <= entry - stop:  pnl, res = -stop, "LOSS"; break
            if sp >= entry + target: pnl, res = target, "WIN"; break
        if pnl is None: pnl = round(last - entry, 2)
        trades.append((d, pnl, res, eet, entry))
    return trades

def summ(ts):
    if not ts: return "n=0"
    n = len(ts); w = sum(1 for t in ts if t[2] == "WIN")
    tot = sum(t[1] for t in ts)
    cum = peak = dd = 0
    for t in sorted(ts):
        cum += t[1]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return f"n={n:<3} WR={100*w/n:>5.1f}% tot={tot:>+7.1f} dd={dd:>6.1f}"

print("=== persist dose-response (dip8/conf4/T10/S8) ===")
for p in (0, 2, 4, 6, 8, 10, 12, 16, 20):
    print(f"persist={p:<3} ({p*30:>3}s)  {summ(run(p))}")

print("\n=== persist=8 by month ===")
t8 = run(8)
for m in ("2026-02", "2026-03", "2026-04", "2026-05", "2026-06"):
    sub = [t for t in t8 if t[0].isoformat()[:7] == m]
    print(f"{m}  {summ(sub)}")

print("\n=== persist=8 with T/S variants ===")
for tg, st in ((10,8),(12,8),(15,8),(15,10)):
    ts_ = run(8, tg, st)
    full = summ(ts_)
    fm = summ([t for t in ts_ if t[0].isoformat() <= '2026-03-31'])
    aj = summ([t for t in ts_ if t[0].isoformat() >= '2026-04-01'])
    print(f"T={tg}/S={st}  FULL {full} | FebMar {fm} | AprJun {aj}")

print("\n=== persist=8 June days (live = 3/3 WIN) ===")
for t in t8:
    if t[0].isoformat() >= "2026-06-01":
        print(f"  {t[0]} entry {t[3].time()} @{t[4]:.2f} -> {t[2]} {t[1]:+.1f}")

print("\n=== persist=8 trade list (losses, for eyeballing) ===")
for t in t8:
    if t[2] == "LOSS":
        print(f"  {t[0]} {t[3].time()} @{t[4]:.2f}")
conn.close()
