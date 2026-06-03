"""
Dip-Buy high-resolution backtest (S196 follow-up).

Price path: ES 5pt range-bar closes mirrored into SPX space via per-day
time-varying basis (basis = chain_snapshots.spot - concurrent ES close,
stepwise across the day). ~10x finer than the 2-min chain_snapshots path.

Sources: vps_es_range_bars (Sierra, Mar 23+) preferred, es_range_bars
source='rithmic' (Feb 18 - Apr 30) fallback. range_pts=5 only.

Trigger logic = live app/dipbuy_detector.py: session high from 9:30 ET,
dip >= 8 below high, confirm >= 4 off dip low, entry window 9:30-11:30,
one trade/day, T+10 / S-8, EOD expire 16:00.

Exit modes:
  A "sampled"      — walk bar closes only (closest to portal 30s logging)
  B "conservative" — bar H/L adverse-first (closest to real broker stops)

Validation: Jun 1-3 must reproduce the 3 live WINs (lids 3413/3464/3509).
"""
import os, sys, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

DIP, CONF, TARGET, STOP = 8.0, 4.0, 10.0, 8.0
WS, WE, CUTOFF = dtime(9, 30), dtime(11, 30), dtime(16, 0)
D_LO, D_HI = "2026-02-18", "2026-06-03"

# ---------- load chain spot (for basis) ----------
cur.execute("""select ts, spot from chain_snapshots
               where ts::date >= %s and spot is not null order by ts""", (D_LO,))
chain = defaultdict(list)
for ts, spot in cur.fetchall():
    et = ts.astimezone(ET)
    chain[et.date()].append((et, float(spot)))

# ---------- load ES bars ----------
def load_bars(tbl, extra=""):
    cur.execute(f"""select ts_end, bar_close, bar_high, bar_low
                    from {tbl}
                    where range_pts=5 and ts_end is not null {extra}
                      and ts_end::date >= %s
                    order by ts_end""", (D_LO,))
    out = defaultdict(list)
    for tse, c, h, l in cur.fetchall():
        et = tse.astimezone(ET)
        out[et.date()].append((et, float(c), float(h), float(l)))
    return out

vps  = load_bars("vps_es_range_bars")
rith = load_bars("es_range_bars", "and source='rithmic'")

def day_bars(d):
    v = vps.get(d, [])
    sess_v = [b for b in v if WS <= b[0].time() <= CUTOFF]
    if len(sess_v) >= 20:
        return sess_v, "vps"
    r = [b for b in rith.get(d, []) if WS <= b[0].time() <= CUTOFF]
    if len(r) >= 20:
        return r, "rithmic"
    return None, None

# ---------- mirror to SPX ----------
def spx_path(d):
    bars, src = day_bars(d)
    snaps = chain.get(d, [])
    if not bars or len(snaps) < 10:
        return None, None
    # basis points: for each snapshot, ES close of last bar ending <= snap ts
    basis_pts = []
    bi = 0
    for set_, sp in snaps:
        while bi < len(bars) - 1 and bars[bi + 1][0] <= set_:
            bi += 1
        if bars[bi][0] <= set_:
            basis_pts.append((set_, sp - bars[bi][1]))
    if not basis_pts:
        return None, None
    # stepwise basis applied to each bar
    out = []
    pi = 0
    for bet, c, h, l in bars:
        while pi < len(basis_pts) - 1 and basis_pts[pi + 1][0] <= bet:
            pi += 1
        b = basis_pts[pi][1] if basis_pts[pi][0] <= bet else basis_pts[0][1]
        out.append((bet, c + b, h + b, l + b))
    return out, src

# ---------- 30s resample (mimic live 30s spot polling) ----------
def resample_30s(path):
    """Last-known mirrored close at each 30s tick from 9:30 to 16:00."""
    from datetime import datetime, timedelta
    if not path: return []
    d = path[0][0].date()
    t = datetime(d.year, d.month, d.day, 9, 30, tzinfo=ET)
    end = datetime(d.year, d.month, d.day, 16, 0, tzinfo=ET)
    out = []
    j = -1
    while t <= end:
        while j + 1 < len(path) and path[j + 1][0] <= t:
            j += 1
        if j >= 0:
            out.append((t, path[j][1], path[j][1], path[j][1]))  # c,h,l = sample
        t += timedelta(seconds=30)
    return out

# ---------- detector ----------
def find_entry(path):
    sess_high = -1e9; in_dip = False; lo = 1e9; dip_start = None
    for i, (et, c, h, l) in enumerate(path):
        if et.time() > WE:
            break
        sess_high = max(sess_high, c)
        if not in_dip:
            if c <= sess_high - DIP:
                in_dip = True; lo = c; dip_start = et
        else:
            lo = min(lo, c)
            if c >= lo + CONF:
                return dict(i=i, et=et, entry=c, sess_high=sess_high,
                            dip_low=lo, dip_depth=sess_high - lo,
                            dip_start=dip_start)
    return None

def walk_A(path, i, entry):           # closes only (portal-style)
    mfe = mae = 0.0
    for et, c, h, l in path[i + 1:]:
        if et.time() > CUTOFF: break
        mfe = max(mfe, c - entry); mae = min(mae, c - entry)
        if c <= entry - STOP:  return -STOP, "LOSS", mfe, mae
        if c >= entry + TARGET: return TARGET, "WIN", mfe, mae
    last = None
    for et, c, h, l in path[i + 1:]:
        if et.time() > CUTOFF: break
        last = c
    return (round(last - entry, 2) if last is not None else 0.0), "EXPIRED", mfe, mae

def walk_B(path, i, entry):           # H/L adverse-first (broker-style)
    mfe = mae = 0.0
    for et, c, h, l in path[i + 1:]:
        if et.time() > CUTOFF: break
        mae = min(mae, l - entry); mfe = max(mfe, h - entry)
        if l <= entry - STOP:  return -STOP, "LOSS", mfe, mae
        if h >= entry + TARGET: return TARGET, "WIN", mfe, mae
    last = None
    for et, c, h, l in path[i + 1:]:
        if et.time() > CUTOFF: break
        last = c
    return (round(last - entry, 2) if last is not None else 0.0), "EXPIRED", mfe, mae

# ---------- run ----------
trades = []
no_data, no_sig = [], []
for d in sorted(chain):
    if not (D_LO <= d.isoformat() <= D_HI):
        continue
    path, src = spx_path(d)
    if not path:
        no_data.append(d); continue
    samp = resample_30s(path)
    ent = find_entry(samp)
    if not ent:
        no_sig.append(d); continue
    pa, ra, mfa, maa = walk_A(samp, ent["i"], ent["entry"])
    # mode B: raw-bar H/L adverse-first from entry time onward
    bi = next((k for k, b in enumerate(path) if b[0] > ent["et"]), len(path)) - 1
    pb, rb, mfb, mab = walk_B(path, bi, ent["entry"])
    # prior close = last chain spot of previous chain day
    prevs = [x for x in sorted(chain) if x < d]
    prior_close = chain[prevs[-1]][-1][1] if prevs else None
    sess_open = samp[0][1]
    trades.append(dict(d=d, src=src, et=ent["et"], entry=ent["entry"],
                       dip_depth=round(ent["dip_depth"], 1),
                       sess_high=ent["sess_high"],
                       pnlA=pa, resA=ra, mfeA=round(mfa,1), maeA=round(maa,1),
                       pnlB=pb, resB=rb, mfeB=round(mfb,1), maeB=round(mab,1),
                       gap=round(sess_open - prior_close, 1) if prior_close else None,
                       vs_pc=round(ent["entry"] - prior_close, 1) if prior_close else None))

def summ(ts, key_pnl, key_res, label):
    if not ts:
        print(f"{label:<30} n=0"); return
    n = len(ts); w = sum(1 for t in ts if t[key_res] == "WIN")
    l = sum(1 for t in ts if t[key_res] == "LOSS")
    e = n - w - l
    tot = sum(t[key_pnl] for t in ts)
    cum = peak = dd = 0
    for t in sorted(ts, key=lambda x: x["d"]):
        cum += t[key_pnl]; peak = max(peak, cum); dd = min(dd, cum - peak)
    print(f"{label:<30} n={n:<4} W/L/E={w}/{l}/{e} WR={100*w/n:>5.1f}% "
          f"tot={tot:>+7.1f}p avg={tot/n:>+5.2f} maxDD={dd:>6.1f}")

print(f"days: traded={len(trades)} no_signal={len(no_sig)} no_es_data={len(no_data)}")
print(f"no_es_data days: {[d.isoformat() for d in no_data]}\n")

print("== FULL Feb 18 - Jun 3 ==")
summ(trades, "pnlA", "resA", "  mode A (portal-style closes)")
summ(trades, "pnlB", "resB", "  mode B (broker H/L advfirst)")
for lbl, lo, hi in [("FEB-MAR", "2026-02-01", "2026-03-31"),
                    ("APR", "2026-04-01", "2026-04-30"),
                    ("MAY", "2026-05-01", "2026-05-31"),
                    ("JUN", "2026-06-01", "2026-06-30")]:
    sub = [t for t in trades if lo <= t["d"].isoformat() <= hi]
    print(f"\n== {lbl} ==")
    summ(sub, "pnlA", "resA", "  mode A")
    summ(sub, "pnlB", "resB", "  mode B")

print("\n== VALIDATION vs live (Jun 1-3, live = 3/3 WIN @ 7576.11/7588.18/7567.75) ==")
for t in trades:
    if t["d"].isoformat() >= "2026-06-01":
        print(f"  {t['d']} entry {t['et'].time()} @{t['entry']:.2f} A={t['resA']} B={t['resB']} src={t['src']}")

print("\n== per-trade dump ==")
for t in sorted(trades, key=lambda x: x["d"]):
    print(f"{t['d']} {str(t['et'].time())[:8]} @{t['entry']:7.2f} dip={t['dip_depth']:5.1f} "
          f"gap={str(t['gap']):>6} vs_pc={str(t['vs_pc']):>6} "
          f"A:{t['resA'][:1]}{t['pnlA']:>+6.1f} (mfe{t['mfeA']:>5.1f}/mae{t['maeA']:>6.1f}) "
          f"B:{t['resB'][:1]}{t['pnlB']:>+6.1f} src={t['src']}")
conn.close()
