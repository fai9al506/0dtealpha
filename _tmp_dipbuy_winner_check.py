"""Final checks on winner config d8 c3 p8 T8/S12: monthly, June detail, loss anatomy."""
import os, pickle
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
CUTOFF = dtime(16, 0)
with open("_tmp_dipbuy_paths.pkl", "rb") as f:
    paths = pickle.load(f)
DAYS = sorted(paths)

def sim(dip, conf, persist, target, stop, w_end=dtime(11,30)):
    trades = []
    for d in DAYS:
        path = paths[d]
        sess_high = -1e9; in_dip = False; lo = 1e9; hold = 0; ent = None
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
        pnl, res, last = None, "EXPIRED", entry
        for et, sp in path[i+1:]:
            if et.time() > CUTOFF: break
            last = sp
            if sp <= entry - stop:  pnl, res = -stop, "LOSS"; break
            if sp >= entry + target: pnl, res = target, "WIN"; break
        if pnl is None: pnl = round(last - entry, 2)
        trades.append((d, pnl, res, eet, entry))
    return trades

t = sim(8, 3, 8, 8, 12)
print("=== d8 c3 p8 T8/S12 — monthly ===")
for m in ("2026-02", "2026-03", "2026-04", "2026-05", "2026-06"):
    sub = [x for x in t if x[0].isoformat()[:7] == m]
    if not sub: continue
    n = len(sub); w = sum(1 for x in sub if x[2] == "WIN")
    tot = sum(x[1] for x in sub)
    print(f"{m}: n={n:<3} WR={100*w/n:5.1f}% tot={tot:+7.1f}")

print("\n=== consecutive losses / worst streak ===")
streak = worst = 0
for x in sorted(t):
    if x[2] == "LOSS": streak += 1; worst = max(worst, streak)
    else: streak = 0
n = len(t); w = sum(1 for x in t if x[2]=="WIN"); l = sum(1 for x in t if x[2]=="LOSS")
e = n - w - l
print(f"n={n} W/L/E={w}/{l}/{e} worst_loss_streak={worst}")
print(f"breakeven WR for T8/S12 = {100*12/20:.0f}%, observed {100*w/n:.1f}%")

print("\n=== June days detail (live raw-config = 3/3 WIN) ===")
for x in t:
    if x[0].isoformat() >= "2026-06-01":
        print(f"  {x[0]} entry {str(x[3].time())[:8]} @{x[4]:.2f} -> {x[2]} {x[1]:+.1f}")

print("\n=== all losses ===")
for x in t:
    if x[2] == "LOSS":
        print(f"  {x[0]} {str(x[3].time())[:8]} @{x[4]:.2f}")

print("\n=== EXPIRED detail ===")
for x in t:
    if x[2] == "EXPIRED":
        print(f"  {x[0]} {str(x[3].time())[:8]} @{x[4]:.2f} pnl={x[1]:+.1f}")

# sibling config c4 (matches live conf=4) for comparison
t2 = sim(8, 4, 8, 8, 12)
n2 = len(t2); w2 = sum(1 for x in t2 if x[2]=="WIN")
print(f"\nsibling d8 c4 p8 T8/S12: n={n2} WR={100*w2/n2:.1f}% tot={sum(x[1] for x in t2):+.1f}")
