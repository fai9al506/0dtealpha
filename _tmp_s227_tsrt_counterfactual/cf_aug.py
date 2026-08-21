"""S231: TSRT counterfactual 2026-07-01 (disable) -> today, extending S227 to Aug 6.

Candidate set = setup_log rows with real_trade_skip_reason='master_kill' (the runtime
dispatched them; only the kill switch stopped them).
Gates re-simulated in real_trader.place_trade() order.
Outcome = mes_sim_outcome_pnl (DB) else production-simulator gap-fill (mesfill_cache*.json).
"""
from engine import *
from sqlalchemy import text
import sys, json

CAP_L = int(sys.argv[1]); CAP_S = int(sys.argv[2])
MODE = sys.argv[3] if len(sys.argv) > 3 else "sb012"   # sb012 | flat1
SRC = sys.argv[4] if len(sys.argv) > 4 else "clean"    # clean | db

MESFILL = {}
if SRC == "clean":
    # one consistent basis-clean recompute for EVERY candidate (overrides DB values,
    # 7 of which used the SPX-spot entry fallback → ~29pt phantom basis)
    MESFILL.update({int(k): v for k, v in json.load(open("mesfill_rt.json")).items()})
else:
    for f in ("mesfill_cache.json", "mesfill_cache_aug.json"):
        try:
            MESFILL.update({int(k): v for k, v in json.load(open(f)).items()})
        except Exception:
            pass
DAILY_LOSS_LIMIT = 300.0
END = "2026-08-07"

# real_trader whitelist as deployed:
#   GEX Long   -> GEX_LONG_V3_REAL_TRADE_ENABLED=true  (evidence: 4 GEX Long placed in June)
#   VIX Div    -> false (evidence: whitelist_reject 2026-05-21, none placed since 05-18)
WHITELIST = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption",
             "DD Exhaustion", "GEX Long"}
import os as _os
if _os.environ.get("NO_GEX"):
    WHITELIST.discard("GEX Long")

with conn() as c:
    rows = c.execute(text("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.outcome_pnl,
             s.mes_sim_outcome_pnl,s.basket_pct
      FROM setup_log s
      WHERE s.ts>='2026-07-01' AND s.ts<:end AND s.real_trade_skip_reason='master_kill'
      ORDER BY s.ts"""), {"end": END}).fetchall()
bars = load_bars(sorted({r[1].date() for r in rows}))

byday = defaultdict(list)
for r in rows:
    byday[r[1].date()].append(r)

TOT = 0.0; NT = 0; NW = 0; daily = {}; per_setup = defaultdict(lambda: [0, 0.0])
skips = defaultdict(int); taken_all = []; chain_taken = 0.0; n_2x = 0
for d in sorted(byday):
    open_pos = []; realized = 0.0; placed = []; dayp = 0.0
    for (lid, et, setup, direction, spot, sl, tl, tsl, cp, mp, bp) in byday[d]:
        il = direction.lower() in ("long", "bullish")
        still = []
        for p in open_pos:
            if p["exit_et"] <= et: realized += p["pnl"]
            else: still.append(p)
        open_pos = still
        if setup not in WHITELIST:
            skips["whitelist_reject"] += 1; continue
        if any(s == setup and dl == il and (et - t).total_seconds() < 90 for s, dl, t in placed):
            skips["dedup_window"] += 1; continue
        cap = CAP_L if il else CAP_S
        if sum(1 for p in open_pos if p["is_long"] == il) >= cap:
            skips[f"cap_{'long' if il else 'short'}_full"] += 1; continue
        if realized <= -DAILY_LOSS_LIMIT:
            skips["daily_loss_limit"] += 1; continue
        stack = [p for p in open_pos if p["setup"] == setup and p["is_long"] == il]
        if len(stack) >= 2:
            sgn = 1.0 if il else -1.0
            if sum((float(spot) - p["entry"]) * sgn for p in stack) < 0:
                skips["underwater_stack_block"] += 1; continue
        sp = stop_for(setup, il, tsl, float(spot), float(sl) if sl else None)
        tp = float(tl) if (setup == "Vanna Pivot Bounce" and tl) else None
        epts, res, xet = walk(bars[d], et, float(spot), il, sp, setup, tp)
        if SRC == "clean":
            pts = MESFILL.get(lid) if MESFILL.get(lid) is not None else epts
        else:
            pts = float(mp) if mp is not None else (MESFILL.get(lid) if MESFILL.get(lid) is not None else epts)
        conf = bp is not None and abs(float(bp)) >= 0.15 and ((float(bp) > 0) == il)
        qty = 2 if (MODE == "sb012" and conf) else 1
        if qty == 2: n_2x += 1
        pnl = pts * DOLLAR_PER_PT * qty - COMM_PER_CONTRACT * qty
        open_pos.append({"setup": setup, "is_long": il, "entry": float(spot),
                         "exit_et": xet, "pnl": pnl, "qty": qty})
        placed.append((setup, il, et))
        NT += 1; NW += 1 if pts > 0 else 0; dayp += pnl
        chain_taken += float(cp or 0) * DOLLAR_PER_PT * qty
        per_setup[setup][0] += 1; per_setup[setup][1] += pnl
        taken_all.append((d, lid, setup, "L" if il else "S", pts, qty, pnl,
                          (mp is None and MESFILL.get(lid) is None), float(cp or 0)))
    for p in open_pos: realized += p["pnl"]
    daily[d] = dayp; TOT += dayp

print(f"=== CAP L{CAP_L}/S{CAP_S}  MODE={MODE}  {min(daily)} -> {max(daily)} ===")
cum = 0
for d in sorted(daily):
    cum += daily[d]
    print(f"{str(d):<12}{daily[d]:>9.0f}{cum:>10.0f}")
peak = 0; dd = 0; c2 = 0
for d in sorted(daily):
    c2 += daily[d]; peak = max(peak, c2); dd = min(dd, c2 - peak)
print(f"\nTOTAL ${TOT:,.0f}  trades {NT}  WR {NW/max(NT,1)*100:.0f}%  "
      f"green days {sum(1 for v in daily.values() if v>0)}/{len(daily)}  MaxDD ${dd:,.0f}")
print(f"2x-sized trades: {n_2x}/{NT}")
print(f"CHAIN-sim same taken trades (same qty): ${chain_taken:,.0f}")
print(f"\n{'setup':<22}{'n':>4}{'$':>10}")
for s, a in sorted(per_setup.items(), key=lambda x: -x[1][1]):
    print(f"{s:<22}{a[0]:>4}{a[1]:>10.0f}")
print(f"\nskips: {dict(skips)}")
print(f"engine-fallback rows (no mes at all): {sum(1 for t in taken_all if t[7])}/{NT}")

# month + week split
import collections
mo = collections.defaultdict(float)
for d, v in daily.items(): mo[d.strftime('%Y-%m')] += v
print("\nby month:", {k: round(v) for k, v in sorted(mo.items())})
wk = collections.defaultdict(float)
for d, v in daily.items(): wk[d.isocalendar()[1]] += v
print("by ISO week:", {k: round(v) for k, v in sorted(wk.items())})
json.dump([[str(t[0]), t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8]] for t in taken_all],
          open(f"taken_{CAP_L}{CAP_S}_{MODE}.json", "w"))
