"""
Dip-Buy deep search for higher-WR config.
A) grid sweep with era-stability gate
B) walk-forward: optimize Feb18-Apr30, blind-test May1-Jun3
C) structural variants: retrace limit entry, second-dip entry
Path: ES 5pt range bars mirrored to SPX, 30s resample (cached pickle).
"""
import os, pickle, psycopg2
from collections import defaultdict
from datetime import time as dtime, datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
WS, CUTOFF = dtime(9, 30), dtime(16, 0)
D_LO = "2026-02-18"
CACHE = "_tmp_dipbuy_paths.pkl"

if os.path.exists(CACHE):
    with open(CACHE, "rb") as f:
        paths = pickle.load(f)
else:
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
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
    conn.close()
    with open(CACHE, "wb") as f:
        pickle.dump(paths, f)

DAYS = sorted(paths)
print(f"days cached: {len(DAYS)}  {DAYS[0]} .. {DAYS[-1]}")

def signal_raw(path, dip, conf, persist, w_end):
    """First dip+held-confirm trigger. Returns (i, entry, dip_low, sess_high) or None."""
    sess_high = -1e9; in_dip = False; lo = 1e9; hold = 0
    for i, (et, sp) in enumerate(path):
        if et.time() > w_end: return None
        sess_high = max(sess_high, sp)
        if not in_dip:
            if sp <= sess_high - dip:
                in_dip = True; lo = sp; hold = 0
        else:
            lo = min(lo, sp)
            if sp >= lo + conf:
                hold += 1
                if hold > persist:
                    return (i, sp, lo, sess_high)
            else:
                hold = 0
    return None

def walk(path, i, entry, target, stop):
    last = entry
    for et, sp in path[i+1:]:
        if et.time() > CUTOFF: break
        last = sp
        if sp <= entry - stop:  return -stop, "LOSS"
        if sp >= entry + target: return target, "WIN"
    return round(last - entry, 2), "EXPIRED"

def sim(dip, conf, persist, target, stop, w_end=dtime(11,30), days=None):
    trades = []
    for d in (days or DAYS):
        path = paths[d]
        sig = signal_raw(path, dip, conf, persist, w_end)
        if not sig: continue
        i, entry, lo, hi = sig
        pnl, res = walk(path, i, entry, target, stop)
        trades.append((d, pnl, res))
    return trades

def stats(ts):
    if not ts: return (0, 0.0, 0.0, 0.0)
    n = len(ts); w = sum(1 for t in ts if t[2] == "WIN")
    tot = sum(t[1] for t in ts)
    cum = peak = dd = 0
    for t in sorted(ts):
        cum += t[1]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return (n, 100.0*w/n, tot, dd)

def split(ts, cut="2026-04-01"):
    a = [t for t in ts if t[0].isoformat() < cut]
    b = [t for t in ts if t[0].isoformat() >= cut]
    return a, b

# ============ A) grid sweep with era-stability ============
print("\n===== A) GRID SWEEP (era-stable only: both-era WR>=55%, n>=25) =====")
results = []
for dip in (6, 8, 10, 12):
    for conf in (3, 4, 5, 6):
        for persist in (4, 6, 8, 10):
            for target in (8, 10, 12, 15):
                for stop in (6, 8, 10, 12):
                    ts = sim(dip, conf, persist, target, stop)
                    n, wr, tot, dd = stats(ts)
                    if n < 25: continue
                    a, b = split(ts)
                    na, wra, ta, _ = stats(a)
                    nb, wrb, tb, _ = stats(b)
                    if na < 8 or nb < 12: continue
                    if wra < 55 or wrb < 55: continue
                    if ta <= 0 or tb <= 0: continue
                    results.append((min(wra, wrb), wr, tot, dd, dip, conf, persist, target, stop, n, wra, ta, wrb, tb))
results.sort(key=lambda r: (-r[0], -r[2]))
print(f"{len(results)} era-stable configs. Top 20 by min-era-WR then total:")
print("minWR | FULL wr tot dd n | dip conf per T S | FebMar wr/tot | AprJun wr/tot")
for r in results[:20]:
    print(f"{r[0]:5.1f} | {r[1]:5.1f}% {r[2]:+7.1f} {r[3]:6.1f} n={r[9]:<3} | "
          f"d{r[4]} c{r[5]} p{r[6]} T{r[7]}/S{r[8]} | {r[10]:5.1f}%/{r[11]:+6.1f} | {r[12]:5.1f}%/{r[13]:+6.1f}")

# ============ B) walk-forward ============
print("\n===== B) WALK-FORWARD: optimize Feb18-Apr30 (by WR, n>=20), test May1-Jun3 =====")
train_days = [d for d in DAYS if d.isoformat() <= "2026-04-30"]
test_days  = [d for d in DAYS if d.isoformat() >= "2026-05-01"]
train_res = []
for dip in (6, 8, 10, 12):
    for conf in (3, 4, 5, 6):
        for persist in (4, 6, 8, 10):
            for target in (8, 10, 12, 15):
                for stop in (6, 8, 10, 12):
                    ts = sim(dip, conf, persist, target, stop, days=train_days)
                    n, wr, tot, dd = stats(ts)
                    if n < 20 or tot <= 0: continue
                    train_res.append((wr, tot, dip, conf, persist, target, stop, n))
train_res.sort(key=lambda r: (-r[0], -r[1]))
print("rank | TRAIN wr tot n | config | TEST wr tot n dd")
for k, r in enumerate(train_res[:8]):
    wr, tot, dip, conf, persist, target, stop, n = r
    ts2 = sim(dip, conf, persist, target, stop, days=test_days)
    n2, wr2, tot2, dd2 = stats(ts2)
    print(f"{k+1:>4} | {wr:5.1f}% {tot:+7.1f} n={n:<3} | d{dip} c{conf} p{persist} T{target}/S{stop} "
          f"| {wr2:5.1f}% {tot2:+7.1f} n={n2:<3} dd={dd2:.1f}")

# ============ C) structural variants on persist base ============
print("\n===== C) STRUCTURAL VARIANTS (base d8 c4 p8 T10/S8) =====")

def sim_retrace(retrace, timeout_min, dip=8, conf=4, persist=8, target=10, stop=8, w_end=dtime(11,30)):
    """After held confirm, wait for price to come back to signal - retrace; limit fill."""
    trades = []
    for d in DAYS:
        path = paths[d]
        sig = signal_raw(path, dip, conf, persist, w_end)
        if not sig: continue
        i, sigpx, lo, hi = sig
        limit = sigpx - retrace
        fill = None
        for k in range(i+1, len(path)):
            et, sp = path[k]
            if (et - path[i][0]).total_seconds() > timeout_min*60: break
            if et.time() > CUTOFF: break
            if sp <= limit:
                fill = (k, sp); break
        if not fill: continue
        k, entry = fill
        pnl, res = walk(path, k, entry, target, stop)
        trades.append((d, pnl, res))
    return trades

def sim_nth_dip(nth, dip=8, conf=4, persist=8, target=10, stop=8, w_end=dtime(11,30)):
    """Enter on the Nth dip-confirm of the morning instead of the first."""
    trades = []
    for d in DAYS:
        path = paths[d]
        sess_high = -1e9; in_dip = False; lo = 1e9; hold = 0; count = 0; ent = None
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
                        count += 1
                        if count >= nth:
                            ent = (i, sp); break
                        in_dip = False; hold = 0   # reset, wait for next dip
                else:
                    hold = 0
        if not ent: continue
        i, entry = ent
        pnl, res = walk(path, i, entry, target, stop)
        trades.append((d, pnl, res))
    return trades

def show(label, ts):
    n, wr, tot, dd = stats(ts)
    a, b = split(ts)
    na, wra, ta, _ = stats(a)
    nb, wrb, tb, _ = stats(b)
    print(f"{label:<42} FULL n={n:<3} WR={wr:5.1f}% tot={tot:+7.1f} dd={dd:6.1f} | "
          f"FebMar {wra:5.1f}%/{ta:+6.1f} (n={na}) | AprJun {wrb:5.1f}%/{tb:+6.1f} (n={nb})")

show("base d8c4p8 T10/S8 (reference)", sim(8,4,8,10,8))
for rt, tmo in ((2,15),(3,15),(4,15),(2,30),(3,30),(4,30),(5,30)):
    show(f"retrace {rt}pt limit, {tmo}min timeout", sim_retrace(rt, tmo))
for nth in (2, 3):
    show(f"enter on dip #{nth}", sim_nth_dip(nth))
print()
# retrace on T12
for rt, tmo in ((3,30),(4,30)):
    show(f"retrace {rt}pt/{tmo}min + T12/S8", sim_retrace(rt, tmo, target=12))
