"""S275 clean projection re-run — ONE window, ONE haircut, ONE cost model.

Answers: what does the CURRENT live config earn per month, and what did each
recent change (S249 short cap 2->3, Friday gate) contribute on the SAME basis?

Metric = CHAIN (setup_log.outcome_pnl) per CLAUDE.md Gate 0.
Costs   = -0.6 pt/trade/contract execution haircut (S246, structural)
          + $1.92/contract round-turn all-in fees (S266; the API reports only half)

DENOMINATOR RULE: a month is 21 CALENDAR trading sessions. Never divide by
"sessions that had a trade" — the Friday gate and v7 both trade on a subset of
days, and dividing by that subset silently inflates them (Friday gate read
+$892/mo instead of +$236/mo before this was fixed).

usage: railway run -s 0dtealpha python rerun.py
"""
import os, sys, collections, json
from datetime import timedelta, date
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.abspath("."))
from app.live_filter import passes_v16, v19_blocks, load_gaps, COLS

ET = ZoneInfo("America/New_York")

WIN_START, WIN_END = "2026-03-01", "2026-08-16"
S217 = "2026-06-13"
DOLLAR_PER_PT = 5.0
HAIRCUT_PT = 0.6
FEE_PER_RT = 1.92
DEADBAND = 0.15
DEDUP_S = 90
SESSIONS_PER_MONTH = 21
LIVE_WEEK = (date(2026, 8, 10), date(2026, 8, 14))   # only live window on ~this config

MAIN_WHITELIST = {"Skew Charm", "AG Short", "Vanna Pivot Bounce",
                  "ES Absorption", "DD Exhaustion"}          # GEX Long OFF

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps = load_gaps(c)
    rows = c.execute(text(f"""
        SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot, outcome_result, gex_state
        FROM setup_log
        WHERE ts >= :a AND ts < :b
        ORDER BY ts"""), {"a": WIN_START, "b": WIN_END}).mappings().all()
    # calendar trading sessions = days the market data pipeline ran
    sess_all = [r[0] for r in c.execute(text("""
        SELECT DISTINCT (ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date d
        FROM chain_snapshots WHERE ts >= :a AND ts < :b ORDER BY d"""),
        {"a": WIN_START, "b": WIN_END}).all()]
    broker = {r[0]: (float(r[1]), float(r[3])) for r in c.execute(text("""
        SELECT day, gross, comm, net FROM tsrt_daily_stmt
        WHERE day >= :a AND day <= :b"""),
        {"a": LIVE_WEEK[0], "b": LIVE_WEEK[1]}).all()}

SESS_FULL = [d for d in sess_all if d.weekday() < 5]
SESS_S217 = [d for d in SESS_FULL if str(d) >= S217]
print(f"rows {len(rows)}   calendar sessions: full {len(SESS_FULL)}  post-S217 {len(SESS_S217)}")


def basket_qty(bp, is_long, enabled):
    if not enabled or bp is None:
        return 1
    bp = float(bp)
    if abs(bp) < DEADBAND:
        return 1
    return 2 if ((bp > 0) == is_long) else 1


def simulate(cands, cap_l, cap_s, breaker, basket_on, label, universe):
    """Chronological walk with the live risk controls.

    `universe` = the calendar sessions this book is exposed to. Days with no
    candidate still count as a $0 session — that is what makes the per-month
    figure honest for a book that only fires on some days.
    """
    byday = collections.defaultdict(list)
    for r in cands:
        byday[r["ts"].astimezone(ET).date()].append(r)

    daily = {d: 0.0 for d in universe}
    per_setup = collections.defaultdict(lambda: [0, 0, 0.0])
    skips = collections.defaultdict(int)
    nt = nw = 0
    peak_contracts = 0
    for d in sorted(byday):
        if d not in daily:
            daily[d] = 0.0
        open_pos, realized, placed, dayp = [], 0.0, [], 0.0
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
                skips["dedup_90s"] += 1
                continue
            if sum(1 for p in open_pos if p["is_long"] == il) >= (cap_l if il else cap_s):
                skips[f"cap_{'long' if il else 'short'}"] += 1
                continue
            if realized <= -breaker:
                skips["daily_breaker"] += 1
                continue
            stack = [p for p in open_pos if p["setup"] == r["setup_name"] and p["is_long"] == il]
            if len(stack) >= 2:
                sgn = 1.0 if il else -1.0
                if sum((float(r["spot"]) - p["entry"]) * sgn for p in stack) < 0:
                    skips["underwater_stack"] += 1
                    continue
            qty = basket_qty(r["basket_pct"], il, basket_on)
            pts = float(r["outcome_pnl"])
            pnl = (pts - HAIRCUT_PT) * DOLLAR_PER_PT * qty - FEE_PER_RT * qty
            exit_et = et + timedelta(minutes=int(r["outcome_elapsed_min"] or 60))
            open_pos.append({"setup": r["setup_name"], "is_long": il,
                             "entry": float(r["spot"]), "exit": exit_et,
                             "pnl": pnl, "qty": qty})
            placed.append((r["setup_name"], il, et))
            peak_contracts = max(peak_contracts, sum(p["qty"] for p in open_pos))
            nt += 1
            nw += 1 if pnl > 0 else 0
            dayp += pnl
            k = (r["setup_name"], "LONG" if il else "SHORT")
            per_setup[k][0] += 1
            per_setup[k][1] += 1 if pnl > 0 else 0
            per_setup[k][2] += pnl
        daily[d] += dayp

    tot = sum(daily.values())
    peak = dd = cum = 0.0
    for d in sorted(daily):
        cum += daily[d]
        peak = max(peak, cum)
        dd = min(dd, cum - peak)
    n_sess = len(daily)
    months = collections.defaultdict(float)
    for d, v in daily.items():
        months[d.strftime("%Y-%m")] += v
    return {"label": label, "total": tot, "trades": nt,
            "wr": nw / max(nt, 1) * 100, "sessions": n_sess,
            "trading_days": sum(1 for v in daily.values() if v != 0),
            "per_month": tot / max(n_sess, 1) * SESSIONS_PER_MONTH,
            "per_session": tot / max(n_sess, 1),
            "maxdd": dd, "green": sum(1 for v in daily.values() if v > 0),
            "red": sum(1 for v in daily.values() if v < 0),
            "months": dict(sorted(months.items())), "peak_contracts": peak_contracts,
            "per_setup": {f"{k[0]} {k[1]}": v for k, v in per_setup.items()},
            "skips": dict(skips), "daily": {str(k): v for k, v in sorted(daily.items())}}


def main_cands(friday_block, start=WIN_START):
    out = []
    for r in rows:
        if r["setup_name"] not in MAIN_WHITELIST or r["outcome_pnl"] is None:
            continue
        if str(r["ts"].astimezone(ET).date()) < start:
            continue
        if not passes_v16(r, gaps):
            continue
        if friday_block and v19_blocks(r):
            continue
        out.append(r)
    return out


def v7_cands(start=WIN_START):
    return [r for r in rows
            if r["setup_name"] == "GEX Long" and r["outcome_pnl"] is not None
            and str(r["ts"].astimezone(ET).date()) >= start
            and (r["gex_state"] or "") == "SUPPORT"]


def show(res, detail=False):
    r = res
    print(f"\n=== {r['label']}")
    print(f"  total ${r['total']:>9,.0f}   {r['trades']:>4} trades   WR {r['wr']:.0f}%   "
          f"{r['sessions']} sessions ({r['trading_days']} with a trade)")
    print(f"  ${r['per_month']:>7,.0f}/mo = SAR {r['per_month']*3.75:>7,.0f}   "
          f"${r['per_session']:.0f}/session   MaxDD ${r['maxdd']:>8,.0f}   "
          f"green {r['green']} / red {r['red']}   peak {r['peak_contracts']} MES")
    print("  by month:", {k: round(v) for k, v in r["months"].items()})
    if detail:
        print("  skips:", r["skips"])
        for k, v in sorted(r["per_setup"].items(), key=lambda x: x[1][2]):
            print(f"     {k:<28}{v[0]:>4}t  WR {v[1]/max(v[0],1)*100:>3.0f}%  ${v[2]:>9,.0f}")


R = {}
print("\n" + "=" * 78)
print(f"WINDOW {WIN_START} -> {WIN_END}   haircut -{HAIRCUT_PT}pt/t/contract   "
      f"fees ${FEE_PER_RT}/RT/contract   month = {SESSIONS_PER_MONTH} calendar sessions")
print("=" * 78)

R["A"] = simulate(main_cands(False), 2, 2, 300, True,
                  "MAIN  cap 2/2, no Friday gate   (old PROJECTION.md config)", SESS_FULL)
R["B"] = simulate(main_cands(False), 2, 3, 300, True,
                  "MAIN  cap 2/3, no Friday gate   (+ S249 short slot)", SESS_FULL)
R["C"] = simulate(main_cands(True), 2, 3, 300, True,
                  "MAIN  cap 2/3 + Friday gate     ** CURRENT LIVE **", SESS_FULL)
for k in ("A", "B", "C"):
    show(R[k], detail=(k == "C"))

R["V7"] = simulate(v7_cands(), 8, 8, 150, False,
                   "V7    GEX Long @ gex_state=SUPPORT, flat 1 MES, cap 8", SESS_FULL)
show(R["V7"], detail=True)

print("\n" + "-" * 78)
print(f"CROSS-CHECK: post-S217 only ({S217} ->, {len(SESS_S217)} sessions) = current trail era")
print("-" * 78)
R["C217"] = simulate(main_cands(True, S217), 2, 3, 300, True,
                     "MAIN  cap 2/3 + Friday gate   [post-S217]", SESS_S217)
R["V7217"] = simulate(v7_cands(S217), 8, 8, 150, False,
                      "V7    SUPPORT, flat 1 MES, cap 8  [post-S217]", SESS_S217)
show(R["C217"])
show(R["V7217"])

# ---------- the only forward validation that exists: the live week ----------
print("\n" + "-" * 78)
print("LIVE-WEEK CALIBRATION 2026-08-10..14 (config that week = cap 2/2, NO Friday gate)")
print("-" * 78)
sim_wk = {d: v for d, v in R["A"]["daily"].items()
          if str(LIVE_WEEK[0]) <= d <= str(LIVE_WEEK[1])}
sg = st = 0.0
print(f"  {'day':<12}{'SIM $':>10}{'BROKER net $':>14}{'diff':>10}")
for d in sorted(sim_wk):
    b = broker.get(date.fromisoformat(d), (0.0, 0.0))
    sg += sim_wk[d]
    st += b[1]
    print(f"  {d:<12}{sim_wk[d]:>10,.0f}{b[1]:>14,.0f}{sim_wk[d]-b[1]:>10,.0f}")
print(f"  {'TOTAL':<12}{sg:>10,.0f}{st:>14,.0f}{sg-st:>10,.0f}")
ratio = st / sg if sg else float('nan')
print(f"\n  SIM-TO-BROKER RATIO = {ratio:.2f}   (n=5 sessions — directional only)")

print("\n" + "=" * 78)
for tag, m, v in (("full window", R["C"], R["V7"]), ("post-S217 ", R["C217"], R["V7217"])):
    tot = m["per_month"] + v["per_month"]
    print(f"COMBINED, {tag}: main ${m['per_month']:,.0f} + v7 ${v['per_month']:,.0f} "
          f"= ${tot:,.0f}/mo (SAR {tot*3.75:,.0f})   "
          f"| x{ratio:.2f} live ratio = ${tot*ratio:,.0f}/mo (SAR {tot*ratio*3.75:,.0f})")
print(f"S249 short slot 2->3 contributed: ${R['B']['per_month'] - R['A']['per_month']:+,.0f}/mo")
print(f"Friday gate contributed:          ${R['C']['per_month'] - R['B']['per_month']:+,.0f}/mo")
print("=" * 78)

json.dump({k: r for k, r in R.items()}, open("rerun_results.json", "w"), indent=1, default=str)
print("wrote rerun_results.json")
