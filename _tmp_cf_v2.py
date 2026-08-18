"""S231b: V16 BASE vs V16-SB counterfactual, 2026-07-01 (TSRT disable) -> now.

Outcome model = CHAIN (setup_log.outcome_pnl). Verified 2026-08-06 against broker fills:
post-S217 (43 executed trades) chain vs broker MAE 2.69pt, median 1.70pt, bias +0.18pt/trade.
The MES-walk model is WRONG for the current code (MAE 5.06pt, -122pt total error) because
SPX_EXIT_ENABLED makes check_spx_trail_exit() own the exit. Do not use it here.

Candidate set is rebuilt from live_filter.passes_v16 (BASE, no basket) so both policies are
scored on the same universe — `master_kill` rows are already SB-gated and can't serve as a base.

usage: python cf_v2.py <capL> <capS> <policy> [--gex]
   policy: base1 | base012 | sb012 | sb001
"""
import os, sys, json, collections
from sqlalchemy import create_engine, text
from datetime import timedelta
from zoneinfo import ZoneInfo
from app.live_filter import passes_v16, load_gaps, COLS

ET = ZoneInfo("America/New_York")
CAP_L = int(sys.argv[1]); CAP_S = int(sys.argv[2]); POLICY = sys.argv[3]
WITH_GEX = "--gex" in sys.argv
START, END = "2026-07-01", "2026-08-07"
DAILY_LOSS_LIMIT = 300.0
DOLLAR_PER_PT = 5.0; COMM = 1.0
DEADBAND = 0.15

WHITELIST = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"}
if WITH_GEX:
    WHITELIST.add("GEX Long")   # GEX_LONG_V3_REAL_TRADE_ENABLED=true on Railway

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps = load_gaps(c)
    rows = c.execute(text(f"""
        SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot, outcome_result
        FROM setup_log
        WHERE ts >= :a AND ts < :b
        ORDER BY ts"""), {"a": START, "b": END}).mappings().all()

cands, unresolved = [], 0
for r in rows:
    if r["setup_name"] not in WHITELIST:
        continue
    if not passes_v16(r, gaps):
        continue
    if r["outcome_pnl"] is None:
        unresolved += 1
        continue
    cands.append(r)


def basket_state(bp, is_long):
    if bp is None:
        return "nodata"
    bp = float(bp)
    if abs(bp) < DEADBAND:
        return "neutral"
    return "confirm" if ((bp > 0) == is_long) else "contradict"


def policy_take_qty(state):
    """(take?, qty) for each policy."""
    if POLICY == "base1":
        return True, 1
    if POLICY == "base012":
        return True, 2 if state == "confirm" else 1
    if POLICY == "sb012":                       # CURRENT LIVE
        return state != "contradict", (2 if state == "confirm" else 1)
    if POLICY == "sb001":
        return state in ("confirm", "nodata"), (2 if state == "confirm" else 1)
    raise SystemExit("bad policy")


byday = collections.defaultdict(list)
for r in cands:
    byday[r["ts"].astimezone(ET).date()].append(r)

daily = {}; TOT = 0.0; NT = NW = 0
per_setup = collections.defaultdict(lambda: [0, 0, 0.0])
skips = collections.defaultdict(int)
taken = []
for d in sorted(byday):
    open_pos = []; realized = 0.0; placed = []; dayp = 0.0
    for r in byday[d]:
        et = r["ts"].astimezone(ET)
        il = r["direction"] in ("long", "bullish")
        st = basket_state(r["basket_pct"], il)
        still = []
        for p in open_pos:
            if p["exit"] <= et:
                realized += p["pnl"]      # retire finished positions, realize their $
            else:
                still.append(p)
        open_pos = still
        take, qty = policy_take_qty(st)
        if not take:
            skips[f"basket_{st}"] += 1; continue
        if any(s == r["setup_name"] and dl == il and (et - t).total_seconds() < 90
               for s, dl, t in placed):
            skips["dedup_window"] += 1; continue
        if sum(1 for p in open_pos if p["is_long"] == il) >= (CAP_L if il else CAP_S):
            skips[f"cap_{'long' if il else 'short'}_full"] += 1; continue
        if realized <= -DAILY_LOSS_LIMIT:
            skips["daily_loss_limit"] += 1; continue
        stack = [p for p in open_pos if p["setup"] == r["setup_name"] and p["is_long"] == il]
        if len(stack) >= 2:
            sgn = 1.0 if il else -1.0
            if sum((float(r["spot"]) - p["entry"]) * sgn for p in stack) < 0:
                skips["underwater_stack_block"] += 1; continue
        pts = float(r["outcome_pnl"])
        pnl = pts * DOLLAR_PER_PT * qty - COMM * qty
        exit_et = et + timedelta(minutes=int(r["outcome_elapsed_min"] or 60))
        open_pos.append({"setup": r["setup_name"], "is_long": il, "entry": float(r["spot"]),
                         "exit": exit_et, "pnl": pnl})
        placed.append((r["setup_name"], il, et))
        NT += 1; NW += 1 if pts > 0 else 0; dayp += pnl
        k = (r["setup_name"], "LONG" if il else "SHORT")
        per_setup[k][0] += 1; per_setup[k][1] += 1 if pts > 0 else 0; per_setup[k][2] += pnl
        taken.append((str(d), r["id"], r["setup_name"], "L" if il else "S", pts, qty, pnl, st))
    for p in open_pos:
        realized += p["pnl"]
    daily[d] = dayp; TOT += dayp

peak = dd = cum = 0
for d in sorted(daily):
    cum += daily[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
gex = "GEX ON " if WITH_GEX else "GEX OFF"
print(f"=== {POLICY:<8} cap L{CAP_L}/S{CAP_S}  {gex} ===")
print(f"TOTAL ${TOT:,.0f}   trades {NT}   WR {NW/max(NT,1)*100:.0f}%   "
      f"green {sum(1 for v in daily.values() if v>0)}/{len(daily)}   MaxDD ${dd:,.0f}")
mo = collections.defaultdict(float)
for d, v in daily.items():
    mo[d.strftime('%Y-%m')] += v
print("by month:", {k: round(v) for k, v in sorted(mo.items())})
if "-v" in sys.argv:
    print(f"candidates {len(cands)} (unresolved skipped {unresolved})   skips {dict(skips)}")
    for k, v in sorted(per_setup.items(), key=lambda x: x[1][2]):
        print(f"   {k[0]:<20}{k[1]:<7}{v[0]:>4}t WR {v[1]/v[0]*100:>3.0f}%  ${v[2]:>8,.0f}")
    print("daily:", {str(k): round(v) for k, v in sorted(daily.items())})
json.dump(taken, open(f"v2_{POLICY}_{CAP_L}{CAP_S}{'_gex' if WITH_GEX else ''}.json", "w"))
