"""S275c — price the S250 scaling ladder on the CURRENT basis.

S250's plan: scale the PROVEN setup (Skew Charm short), not the whole account.
Slots first (S249, done) then size, one rung at a time.

This re-runs the same engine as _tmp_s275_projection_rerun.py with a per-setup
size multiplier, and reports what each rung costs in PEAK MARGIN and DRAWDOWN,
which is what actually gates it — not the P&L.
"""
import os, sys, collections
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.abspath("."))
from app.live_filter import passes_v16, v19_blocks, load_gaps, COLS

ET = ZoneInfo("America/New_York")
WIN_START, WIN_END = "2026-03-01", "2026-08-16"
DOLLAR_PER_PT, HAIRCUT_PT, FEE_PER_RT = 5.0, 0.6, 1.92
DEADBAND, DEDUP_S, SESSIONS_PER_MONTH = 0.15, 90, 21
MARGIN_PER_MES = 264.80          # TS intraday, proved 2026-08-11
MARGIN_USE_MAX = 0.70            # never commit more than 70% of equity to margin
WL = {"Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"}

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps = load_gaps(c)
    rows = c.execute(text(f"""
        SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot
        FROM setup_log WHERE ts >= :a AND ts < :b ORDER BY ts"""),
        {"a": WIN_START, "b": WIN_END}).mappings().all()
    sess = [r[0] for r in c.execute(text("""
        SELECT DISTINCT (ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date d
        FROM chain_snapshots WHERE ts >= :a AND ts < :b ORDER BY d"""),
        {"a": WIN_START, "b": WIN_END}).all()]
SESS = [d for d in sess if d.weekday() < 5]

cands = [r for r in rows if r["setup_name"] in WL and r["outcome_pnl"] is not None
         and passes_v16(r, gaps) and not v19_blocks(r)]
print(f"{len(cands)} V16+Friday-gate candidates over {len(SESS)} sessions")


def run(mult, cap_l=2, cap_s=3, breaker=300.0):
    """mult = dict (setup_name, 'LONG'/'SHORT') -> size multiplier, default 1."""
    byday = collections.defaultdict(list)
    for r in cands:
        byday[r["ts"].astimezone(ET).date()].append(r)
    daily = {d: 0.0 for d in SESS}
    peak_side = collections.Counter()      # max simultaneous contracts per side
    nt = 0
    sc_short_pnl = 0.0
    for d in sorted(byday):
        open_pos, realized, placed = [], 0.0, []
        for r in byday[d]:
            et = r["ts"].astimezone(ET)
            il = r["direction"] in ("long", "bullish")
            still = []
            for p in open_pos:
                if p["exit"] <= et:
                    realized += p["pnl"]
                else:
                    still.append(p)
            open_pos = still
            if any(s == r["setup_name"] and dl == il and (et - t).total_seconds() < DEDUP_S
                   for s, dl, t in placed):
                continue
            if sum(1 for p in open_pos if p["is_long"] == il) >= (cap_l if il else cap_s):
                continue
            if realized <= -breaker:
                continue
            stack = [p for p in open_pos if p["setup"] == r["setup_name"] and p["is_long"] == il]
            if len(stack) >= 2:
                sgn = 1.0 if il else -1.0
                if sum((float(r["spot"]) - p["entry"]) * sgn for p in stack) < 0:
                    continue
            bp = r["basket_pct"]
            q = 1
            if bp is not None and abs(float(bp)) >= DEADBAND:
                q = 2 if ((float(bp) > 0) == il) else 1
            q *= mult.get((r["setup_name"], "LONG" if il else "SHORT"), 1)
            pts = float(r["outcome_pnl"])
            pnl = (pts - HAIRCUT_PT) * DOLLAR_PER_PT * q - FEE_PER_RT * q
            open_pos.append({"setup": r["setup_name"], "is_long": il, "qty": q,
                             "entry": float(r["spot"]), "pnl": pnl,
                             "exit": et + timedelta(minutes=int(r["outcome_elapsed_min"] or 60))})
            placed.append((r["setup_name"], il, et))
            for side in (True, False):
                peak_side[side] = max(peak_side[side],
                                      sum(p["qty"] for p in open_pos if p["is_long"] == side))
            nt += 1
            daily[d] += pnl
            if r["setup_name"] == "Skew Charm" and not il:
                sc_short_pnl += pnl
    tot = sum(daily.values())
    pk = cum = dd = 0.0
    for d in sorted(daily):
        cum += daily[d]
        pk = max(pk, cum)
        dd = min(dd, cum - pk)
    return {"total": tot, "per_month": tot / len(SESS) * SESSIONS_PER_MONTH, "trades": nt,
            "maxdd": dd, "peak_long": peak_side[True], "peak_short": peak_side[False],
            "sc_short": sc_short_pnl / len(SESS) * SESSIONS_PER_MONTH}


SC_S = ("Skew Charm", "SHORT")
LADDER = [
    ("rung 0 — TODAY (1x everything)", {}),
    ("rung 1 — SC-short 2x", {SC_S: 2}),
    ("rung 2 — SC-short 3x", {SC_S: 3}),
    ("rung 3 — SC-short 4x", {SC_S: 4}),
    ("(compare) whole book 2x", {k: 2 for k in
     [(s, d) for s in WL for d in ("LONG", "SHORT")]}),
]

print("\n{:<32}{:>9}{:>10}{:>11}{:>8}{:>9}{:>11}{:>11}".format(
    "rung", "$/mo", "SC-sh $/mo", "MaxDD", "pkL", "pkS", "shortMargin", "needEquity"))
print("-" * 102)
base = None
for label, mult in LADDER:
    r = run(mult)
    if base is None:
        base = r
    marg_s = r["peak_short"] * MARGIN_PER_MES
    marg_l = r["peak_long"] * MARGIN_PER_MES
    need_s = marg_s / MARGIN_USE_MAX
    print(f"{label:<32}{r['per_month']:>9,.0f}{r['sc_short']:>10,.0f}"
          f"{r['maxdd']:>11,.0f}{r['peak_long']:>8}{r['peak_short']:>9}"
          f"{marg_s:>11,.0f}{need_s:>11,.0f}")
print("-" * 102)
print(f"long side peak margin at rung 0: ${base['peak_long']*MARGIN_PER_MES:,.0f} "
      f"-> long account needs ${base['peak_long']*MARGIN_PER_MES/MARGIN_USE_MAX:,.0f}")
print(f"\nAccounts today: long 210VYX65, short 210VYX91  (combined equity ~$6,016 at 2026-08-14)")
print(f"MaxDD as % of the $6,000 that can absorb it:")
for label, mult in LADDER:
    r = run(mult)
    print(f"   {label:<32} MaxDD ${r['maxdd']:>8,.0f} = {abs(r['maxdd'])/6000*100:>5.1f}%  "
          f"(+${r['per_month']-base['per_month']:>6,.0f}/mo vs today)")
