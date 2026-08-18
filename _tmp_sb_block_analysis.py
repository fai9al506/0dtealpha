"""S231c: what has the Semi-Basket BLOCK cost since 2026-07-01, and what fixes it.

Same engine as _tmp_cf_v2.py (V16 BASE candidate set, real_trader gate order, CHAIN outcomes)
but sweeps confirmation POLICIES so the block can be priced and replaced.

Policies
  base1      take everything, 1x                       (no basket at all)
  base012    take everything, 2x on tech-confirm       (SIZING ONLY - no block)
  sb012      skip tech-contradict, 2x on confirm       (CURRENT LIVE)
  sb001      take only tech-confirm                    (legacy Scheme B)
  spx012     take everything, 2x when SPX-from-open confirms   (basket replaced by SPX itself)
  spxblk     skip contradict ONLY when tech agrees with SPX    (divergence fail-open)

usage: python _tmp_sb_block_analysis.py [--gex]
"""
import os, sys, collections
from sqlalchemy import create_engine, text
from datetime import timedelta
from zoneinfo import ZoneInfo
from app.live_filter import passes_v16, load_gaps, COLS

ET = ZoneInfo("America/New_York")
WITH_GEX = "--gex" in sys.argv
START = os.environ.get("CF_START","2026-07-01"); END = os.environ.get("CF_END","2026-08-07")
CAPS = [(3, 3), (2, 2)]
DAILY_LOSS_LIMIT = 300.0
DPP, COMM, DEAD = 5.0, 1.0, 0.15
WL = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"}
if os.environ.get("WITH_VIXDIV"): WL.add("VIX Divergence")   # VIX_DIV_REAL_TRADE_ENABLED=true on Railway
if WITH_GEX:
    WL.add("GEX Long")

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps = load_gaps(c)
    rows = c.execute(text(f"""
        SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot
        FROM setup_log WHERE ts >= :a AND ts < :b ORDER BY ts"""),
        {"a": START, "b": END}).mappings().all()
    # session open + close per day, for the SPX-direction confirmer
    spx = {}
    for d, o, cl in c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date d,
               (array_agg(bar_open  ORDER BY ts ASC ))[1],
               (array_agg(bar_close ORDER BY ts DESC))[1]
        FROM spx_ohlc_1m WHERE ts >= :a AND ts < :b
        GROUP BY 1"""), {"a": START, "b": END}):
        spx[d] = (float(o), float(cl))

cands = [r for r in rows
         if r["setup_name"] in WL and r["outcome_pnl"] is not None and passes_v16(r, gaps)]


def tech_state(bp, il):
    if bp is None:
        return "nodata"
    bp = float(bp)
    if abs(bp) < DEAD:
        return "neutral"
    return "confirm" if ((bp > 0) == il) else "contradict"


def spx_state(r, il):
    """SPX's own %-from-session-open at signal time — the single-asset confirmer."""
    d = r["ts"].astimezone(ET).date()
    if d not in spx or r["spot"] is None:
        return "nodata"
    op = spx[d][0]
    pct = (float(r["spot"]) - op) / op * 100.0
    if abs(pct) < DEAD:
        return "neutral"
    return "confirm" if ((pct > 0) == il) else "contradict"


def decide(policy, r, il):
    """-> (take, qty)"""
    t = tech_state(r["basket_pct"], il)
    if policy == "base1":
        return True, 1
    if policy == "flat2":
        return True, 2
    if policy == "base012":
        return True, 2 if t == "confirm" else 1
    if policy == "sb012":
        return t != "contradict", 2 if t == "confirm" else 1
    if policy == "sb001":
        return t in ("confirm", "nodata"), 2 if t == "confirm" else 1
    if policy == "spx012":
        s = spx_state(r, il)
        return True, 2 if s == "confirm" else 1
    if policy == "spxblk":
        # block a tech-contradict ONLY when tech and SPX point the same way (no divergence).
        # On a divergence day (tech green / SPX red or vice-versa) the gate fails OPEN.
        if t != "contradict":
            return True, 2 if t == "confirm" else 1
        d = r["ts"].astimezone(ET).date()
        bp = r["basket_pct"]
        if d not in spx or bp is None:
            return True, 1
        # SPX direction AT SIGNAL TIME (spot vs session open) — no lookahead.
        spx_up = float(r["spot"]) >= spx[d][0]
        tech_up = float(bp) > 0
        return (spx_up != tech_up), 1          # diverging -> TAKE ; agreeing -> SKIP
    raise SystemExit(policy)


def run(policy, capL, capS):
    byday = collections.defaultdict(list)
    for r in cands:
        byday[r["ts"].astimezone(ET).date()].append(r)
    daily = {}; NT = NW = 0; tot = 0.0
    bydir = collections.defaultdict(lambda: [0, 0, 0.0])
    for d in sorted(byday):
        openp = []; realized = 0.0; placed = []; dayp = 0.0
        for r in byday[d]:
            et = r["ts"].astimezone(ET)
            il = r["direction"] in ("long", "bullish")
            still = []
            for p in openp:
                if p["exit"] <= et:
                    realized += p["pnl"]
                else:
                    still.append(p)
            openp = still
            take, qty = decide(policy, r, il)
            if not take:
                continue
            if any(s == r["setup_name"] and dl == il and (et - t).total_seconds() < 90
                   for s, dl, t in placed):
                continue
            if sum(1 for p in openp if p["is_long"] == il) >= (capL if il else capS):
                continue
            if realized <= -DAILY_LOSS_LIMIT:
                continue
            stack = [p for p in openp if p["setup"] == r["setup_name"] and p["is_long"] == il]
            if len(stack) >= 2:
                sgn = 1.0 if il else -1.0
                if sum((float(r["spot"]) - p["entry"]) * sgn for p in stack) < 0:
                    continue
            pts = float(r["outcome_pnl"]); pnl = pts * DPP * qty - COMM * qty
            openp.append({"setup": r["setup_name"], "is_long": il, "entry": float(r["spot"]),
                          "exit": et + timedelta(minutes=int(r["outcome_elapsed_min"] or 60)),
                          "pnl": pnl})
            placed.append((r["setup_name"], il, et))
            NT += 1; NW += 1 if pts > 0 else 0; dayp += pnl
            k = "LONG" if il else "SHORT"
            bydir[k][0] += 1; bydir[k][1] += 1 if pts > 0 else 0; bydir[k][2] += pnl
        for p in openp:
            realized += p["pnl"]
        daily[d] = dayp; tot += dayp
    peak = dd = cum = 0
    for d in sorted(daily):
        cum += daily[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return dict(total=tot, n=NT, wr=NW/max(NT, 1)*100, dd=dd, daily=daily, bydir=dict(bydir))


POLICIES = ["base1", "flat2", "base012", "sb012"]
print(f"=== Jul 1 - Aug 6, {len(cands)} V16-base candidates, GEX {'ON' if WITH_GEX else 'OFF'} ===")
res = {}
for capL, capS in CAPS:
    print(f"\n--- cap L{capL}/S{capS} ---")
    print(f"{'policy':<10}{'total$':>9}{'trades':>8}{'WR':>6}{'MaxDD':>9}{'LONG$':>9}{'SHORT$':>9}")
    for p in POLICIES:
        r = run(p, capL, capS); res[(p, capL)] = r
        L = r["bydir"].get("LONG", [0, 0, 0])[2]; S = r["bydir"].get("SHORT", [0, 0, 0])[2]
        print(f"{p:<10}{r['total']:>9,.0f}{r['n']:>8}{r['wr']:>5.0f}%{r['dd']:>9,.0f}{L:>9,.0f}{S:>9,.0f}")

# --- what the BLOCK costs, day by day ---
print("\n=== cost of the SB contradict-BLOCK  (base012 minus sb012, cap 3/3) ===")
a, b = res[("base012", 3)], res[("sb012", 3)]
diffs = sorted(((b["daily"].get(d, 0) - a["daily"][d], d) for d in a["daily"]))
tot = sum(x for x, _ in diffs)
print(f"total cost of blocking: ${tot:,.0f}   "
      f"days hurt {sum(1 for x,_ in diffs if x<-1)}  days helped {sum(1 for x,_ in diffs if x>1)}")
print(" worst days for the block (block lost you money):")
for x, d in diffs[:8]:
    print(f"   {d}  ${x:>8,.0f}")
print(" best days for the block (block saved you money):")
for x, d in diffs[-5:]:
    print(f"   {d}  ${x:>8,.0f}")

# --- divergence diagnosis ---
print("\n=== tech-contradict trades: divergence days vs agreeing days (raw pts, 1x, pre-gate) ===")
agg = collections.defaultdict(lambda: [0, 0, 0.0])
for r in cands:
    il = r["direction"] in ("long", "bullish")
    if tech_state(r["basket_pct"], il) != "contradict":
        continue
    d = r["ts"].astimezone(ET).date()
    if d not in spx or r["basket_pct"] is None:
        continue
    spx_up = float(r["spot"]) >= spx[d][0]      # signal-time, no lookahead
    tech_up = float(r["basket_pct"]) > 0
    k = "DIVERGING (tech vs SPX disagree)" if spx_up != tech_up else "agreeing"
    agg[k][0] += 1; agg[k][1] += 1 if float(r["outcome_pnl"]) > 0 else 0
    agg[k][2] += float(r["outcome_pnl"])
for k, v in agg.items():
    print(f"  {k:<34}{v[0]:>4}t  WR {v[1]/v[0]*100:>3.0f}%  {v[2]:>+8.1f} pts  ${v[2]*5:>+8,.0f}")
