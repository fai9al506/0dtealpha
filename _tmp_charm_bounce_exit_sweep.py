"""Charm-bounce: same entries, EXIT SWEEP (user: T10 too small for a 30pt zone reversal).

Entries (unchanged): first touch/day of dominant below-spot charm bar
(>=0.6*gmax, >=10M), zone [bar-2, bar+3], 10:00-15:30 ET.
Exits tested: fixed T/S grid + trailing (activation/gap) + EOD-hold.
Trail: once profit >= activation, stop = max_profit - gap (continuous).
Also tag entries with the user's Jun-3 signature: near-spot charm empty at entry.
"""
import os
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

cur.execute("""
    SELECT ts_utc, strike, value FROM (
        SELECT ts_utc, strike, value,
               row_number() OVER (PARTITION BY ts_utc ORDER BY abs(value) DESC) rn
        FROM volland_exposure_points
        WHERE greek='charm' AND ticker='SPX' AND expiration_option IS NULL
    ) x WHERE rn <= 10
""")
csnaps = defaultdict(list)
for ts, s, v in cur.fetchall():
    csnaps[ts.astimezone(ET)].append((float(s), float(v)))

cur.execute("SELECT ts, spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts")
path = defaultdict(list)
for ts, spot in cur.fetchall():
    t = ts.astimezone(ET)
    path[t.date()].append((t, float(spot)))

cbyday = defaultdict(list)
for t in csnaps:
    cbyday[t.date()].append(t)

entries = []  # (date, idx, entry_px, sig_empty)
for d, p in sorted(path.items()):
    p = [(t, s) for t, s in p if 10 * 60 <= t.hour * 60 + t.minute <= 16 * 60]
    if len(p) < 50 or len(set(s for _, s in p)) < 10:
        continue
    ctimes = sorted(cbyday.get(d, []))
    if not ctimes:
        continue
    ci = 0
    for i, (t, s) in enumerate(p):
        if t.hour * 60 + t.minute > 15 * 60 + 30:
            break
        while ci + 1 < len(ctimes) and ctimes[ci + 1] <= t:
            ci += 1
        if ctimes[ci] > t:
            continue
        pts = csnaps[ctimes[ci]]
        gmax = max(abs(v) for _, v in pts)
        below = [(st, v) for st, v in pts if st <= s]
        if not below:
            continue
        s_sup, v_sup = max(below, key=lambda x: abs(x[1]))
        if abs(v_sup) < max(0.6 * gmax, 10e6):
            continue
        if s_sup - 2 <= s <= s_sup + 3:
            near = max((abs(v) for st, v in pts if abs(st - s) <= 15), default=0.0)
            entries.append((d, i, s, near < 0.4 * gmax, p))
            break

print(f"entries: {len(entries)}  ({entries[0][0]} .. {entries[-1][0]})  signature-days: {sum(1 for e in entries if e[3])}")

def walk_fixed(p, i, e, tgt, sl):
    for _, s in p[i + 1:]:
        if s <= e - sl: return -sl
        if s >= e + tgt: return tgt
    return p[-1][1] - e

def walk_trail(p, i, e, act, gap, sl):
    mx = 0.0
    for _, s in p[i + 1:]:
        prof = s - e
        if mx >= act and prof <= mx - gap:
            return mx - gap
        if mx < act and prof <= -sl:
            return -sl
        mx = max(mx, prof)
    return p[-1][1] - e

schemes = [
    ("T10/S8 (base)", lambda p, i, e: walk_fixed(p, i, e, 10, 8)),
    ("T15/S8",        lambda p, i, e: walk_fixed(p, i, e, 15, 8)),
    ("T20/S8",        lambda p, i, e: walk_fixed(p, i, e, 20, 8)),
    ("T25/S10",       lambda p, i, e: walk_fixed(p, i, e, 25, 10)),
    ("T30/S10",       lambda p, i, e: walk_fixed(p, i, e, 30, 10)),
    ("trail a10/g5 S8",  lambda p, i, e: walk_trail(p, i, e, 10, 5, 8)),
    ("trail a15/g7 S8",  lambda p, i, e: walk_trail(p, i, e, 15, 7, 8)),
    ("trail a20/g8 S10", lambda p, i, e: walk_trail(p, i, e, 20, 8, 10)),
    ("EOD hold S8",   lambda p, i, e: walk_fixed(p, i, e, 9999, 8)),
    ("EOD hold S12",  lambda p, i, e: walk_fixed(p, i, e, 9999, 12)),
]
print(f"\n{'scheme':18s} {'ALL(n=' + str(len(entries)) + ')':>22s}   {'SIGNATURE-ONLY':>22s}")
for name, fn in schemes:
    allr, sigr = [], []
    for d, i, e, sig, p in entries:
        r = fn(p, i, e)
        allr.append(r)
        if sig:
            sigr.append(r)
    def fmt(rs):
        if not rs: return "n/a"
        w = sum(1 for r in rs if r > 0.2); l = sum(1 for r in rs if r < -0.2)
        return f"{sum(rs):+7.1f}p {100*w/max(w+l,1):3.0f}% n={len(rs)}"
    print(f"{name:18s} {fmt(allr):>22s}   {fmt(sigr):>22s}")

# per-trade detail for best instincts: show the actual max favorable excursion
print("\nMFE per entry (how far the bounce actually ran):")
for d, i, e, sig, p in entries:
    mfe = max((s - e for _, s in p[i + 1:]), default=0)
    mae = min((s - e for _, s in p[i + 1:]), default=0)
    print(f"  {d}  entry {e:7.1f}  MFE {mfe:+6.1f}  MAE {mae:+6.1f}  {'SIG' if sig else ''}")
c.close()
