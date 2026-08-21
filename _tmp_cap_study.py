"""Cap sweep + stack-position study on the FULL history (no basket dependency).

base1 = every V16-base signal, 1 MES, real_trader gate order, CHAIN outcomes.
Answers: (a) does raising the concurrency cap keep adding profit?
         (b) is the Nth stacked same-direction trade better or worse than the 1st?
"""
import os, sys, collections, statistics
from sqlalchemy import create_engine, text
from datetime import timedelta
from zoneinfo import ZoneInfo
from app.live_filter import passes_v16, load_gaps, COLS

ET = ZoneInfo("America/New_York")
START = os.environ.get("CF_START", "2026-03-16"); END = os.environ.get("CF_END", "2026-08-07")
WITH_GEX = "--gex" in sys.argv
DPP, COMM, LIM = 5.0, 1.0, 300.0
WL = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"}
if WITH_GEX:
    WL.add("GEX Long")

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps = load_gaps(c)
    rows = c.execute(text(f"""SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot
        FROM setup_log WHERE ts>=:a AND ts<:b ORDER BY ts"""), {"a": START, "b": END}).mappings().all()
cands = [r for r in rows if r["setup_name"] in WL and r["outcome_pnl"] is not None and passes_v16(r, gaps)]
byday = collections.defaultdict(list)
for r in cands:
    byday[r["ts"].astimezone(ET).date()].append(r)


def run(capL, capS, collect_stack=False):
    daily = {}; NT = NW = 0; stackpos = collections.defaultdict(lambda: [0, 0, 0.0])
    for d in sorted(byday):
        openp = []; realized = 0.0; placed = []; dayp = 0.0
        for r in byday[d]:
            et = r["ts"].astimezone(ET); il = r["direction"] in ("long", "bullish")
            still = []
            for p in openp:
                if p["exit"] <= et: realized += p["pnl"]
                else: still.append(p)
            openp = still
            if any(s == r["setup_name"] and dl == il and (et-t).total_seconds() < 90 for s, dl, t in placed):
                continue
            nopen = sum(1 for p in openp if p["is_long"] == il)
            if nopen >= (capL if il else capS): continue
            if realized <= -LIM: continue
            stack = [p for p in openp if p["setup"] == r["setup_name"] and p["is_long"] == il]
            if len(stack) >= 2:
                sgn = 1.0 if il else -1.0
                if sum((float(r["spot"])-p["entry"])*sgn for p in stack) < 0: continue
            pts = float(r["outcome_pnl"]); pnl = pts*DPP - COMM
            openp.append({"setup": r["setup_name"], "is_long": il, "entry": float(r["spot"]),
                          "exit": et+timedelta(minutes=int(r["outcome_elapsed_min"] or 60)), "pnl": pnl})
            placed.append((r["setup_name"], il, et))
            NT += 1; NW += 1 if pts > 0 else 0; dayp += pnl
            if collect_stack:
                k = min(nopen+1, 5)      # this trade is the (nopen+1)-th concurrent same-direction position
                stackpos[k][0] += 1; stackpos[k][1] += 1 if pts > 0 else 0; stackpos[k][2] += pts
        for p in openp: realized += p["pnl"]
        daily[d] = dayp
    peak = dd = cum = 0
    for d in sorted(daily):
        cum += daily[d]; peak = max(peak, cum); dd = min(dd, cum-peak)
    return dict(total=sum(daily.values()), n=NT, wr=NW/max(NT,1)*100, dd=dd, daily=daily, sp=stackpos)


print(f"=== {START} -> {END}   {len(cands)} V16-base candidates, {len(byday)} sessions, "
      f"GEX {'ON' if WITH_GEX else 'OFF'}, 1 MES flat ===")
print(f"\n{'cap':<8}{'total$':>9}{'trades':>8}{'WR':>6}{'MaxDD':>9}{'$/trade':>9}{'vs prev':>11}")
prev = None
for cap in (1, 2, 3, 4, 5, 8):
    r = run(cap, cap)
    d = "" if prev is None else f"{r['total']-prev:>+11,.0f}"
    print(f"{cap}/{cap:<6}{r['total']:>9,.0f}{r['n']:>8}{r['wr']:>5.0f}%{r['dd']:>9,.0f}"
          f"{r['total']/max(r['n'],1):>9,.1f}{d}")
    prev = r["total"]

print("\n=== STACK POSITION: is the Nth concurrent same-direction trade better? (cap 8/8, raw pts) ===")
r = run(8, 8, collect_stack=True)
print(f"{'position':<12}{'n':>5}{'WR':>7}{'pts':>10}{'pts/trade':>11}")
for k in sorted(r["sp"]):
    v = r["sp"][k]
    lbl = f"{k}th+" if k == 5 else f"{k}{'st' if k==1 else 'nd' if k==2 else 'rd' if k==3 else 'th'}"
    print(f"{lbl:<12}{v[0]:>5}{v[1]/v[0]*100:>6.0f}%{v[2]:>10.1f}{v[2]/v[0]:>11.2f}")

print("\n=== per-month, cap 2/2 vs 3/3 vs 5/5 (1 MES) ===")
res = {c: run(c, c) for c in (2, 3, 5)}
months = sorted({d.strftime('%Y-%m') for d in res[2]["daily"]})
print(f"{'month':<10}" + "".join(f"{'cap'+str(c)+'/'+str(c):>12}" for c in (2, 3, 5)))
for m in months:
    line = f"{m:<10}"
    for c in (2, 3, 5):
        line += f"{sum(v for d,v in res[c]['daily'].items() if d.strftime('%Y-%m')==m):>12,.0f}"
    print(line)
