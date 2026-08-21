# -*- coding: utf-8 -*-
"""S233 shared harness — cached candidate load + portfolio simulator.

Gate order mirrors real_trader.place_trade(): basket sizing -> dedup 90s -> concurrency cap
-> $300 daily loss breaker -> underwater-stack guard.  Outcome model = CHAIN (setup_log.outcome_pnl)
per CLAUDE.md Gate 0 (post-S217 chain bias +0.18 pt/trade vs broker; MES-walk is the wrong model).
"""
import os, pickle, collections, statistics
from datetime import timedelta
from zoneinfo import ZoneInfo

os.environ.setdefault("VPB_REAL_TRADE_ENABLED", "true")
ET = ZoneInfo("America/New_York")
CACHE = "_tmp_s233_cache.pkl"
DPP = 5.0          # $ per SPX/MES point per contract
COMM = 1.0         # $ per contract round-trip (matches prior S231 studies)
DEADBAND = 0.15
DAILY_LOSS_LIMIT = 300.0
START_DEFAULT = "2026-02-01"
END_DEFAULT = "2026-08-07"


def load(start=START_DEFAULT, end=END_DEFAULT, refresh=False):
    """Returns (rows, gaps). Cached on disk — the DB pull is the slow part."""
    if not refresh and os.path.exists(CACHE):
        with open(CACHE, "rb") as f:
            d = pickle.load(f)
        if d["start"] == start and d["end"] == end:
            return d["rows"], d["gaps"]
    from sqlalchemy import create_engine, text
    from app.live_filter import load_gaps, COLS
    E = create_engine(os.environ["DATABASE_URL"])
    with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        gaps = load_gaps(c)
        rows = c.execute(text(f"""
            SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot, outcome_result,
                   outcome_max_profit, outcome_max_loss, score, vix3m, mes_sim_outcome_pnl
            FROM setup_log WHERE ts >= :a AND ts < :b ORDER BY ts"""),
                        {"a": start, "b": end}).mappings().all()
    rows = [dict(r) for r in rows]
    with open(CACHE, "wb") as f:
        pickle.dump({"start": start, "end": end, "rows": rows, "gaps": gaps}, f)
    return rows, gaps


def basket_state(bp, is_long):
    if bp is None:
        return "nodata"
    bp = float(bp)
    if abs(bp) < DEADBAND:
        return "neutral"
    return "confirm" if ((bp > 0) == is_long) else "contradict"


def sim(cands, cap_l=2, cap_s=2, sizing="basket", comm=COMM, dpp=DPP,
        limit=DAILY_LOSS_LIMIT, dedup_s=90, underwater=True, haircut=1.0):
    """cands = list of setup_log dicts that PASSED whatever filter is under test, ts-ordered.

    sizing: 'basket' = 2x when the tech basket confirms (else 1x); 'flat1' = always 1;
            'flat2' = always 2.
    haircut: multiply chain points by this before P&L (e.g. 0.81 broker-capture).
    Returns a stats dict.
    """
    byday = collections.defaultdict(list)
    for r in cands:
        byday[r["ts"].astimezone(ET).date()].append(r)

    daily = {}
    per_setup = collections.defaultdict(lambda: [0, 0, 0.0])
    trades = []
    skips = collections.Counter()
    max_conc = 0
    for d in sorted(byday):
        open_pos = []; realized = 0.0; placed = []; dayp = 0.0
        for r in byday[d]:
            et = r["ts"].astimezone(ET)
            il = r["direction"] in ("long", "bullish")
            st = basket_state(r["basket_pct"], il)
            still = []
            for p in open_pos:
                if p["exit"] <= et:
                    realized += p["pnl"]
                else:
                    still.append(p)
            open_pos = still
            if sizing == "basket":
                qty = 2 if st == "confirm" else 1
            elif sizing == "flat2":
                qty = 2
            else:
                qty = 1
            if any(s == r["setup_name"] and dl == il and (et - t).total_seconds() < dedup_s
                   for s, dl, t in placed):
                skips["dedup"] += 1; continue
            if sum(1 for p in open_pos if p["is_long"] == il) >= (cap_l if il else cap_s):
                skips["cap"] += 1; continue
            if realized <= -limit:
                skips["breaker"] += 1; continue
            if underwater:
                stack = [p for p in open_pos if p["setup"] == r["setup_name"] and p["is_long"] == il]
                if len(stack) >= 2:
                    sgn = 1.0 if il else -1.0
                    if sum((float(r["spot"]) - p["entry"]) * sgn for p in stack) < 0:
                        skips["underwater"] += 1; continue
            pts = float(r["outcome_pnl"]) * haircut
            pnl = pts * dpp * qty - comm * qty
            exit_et = et + timedelta(minutes=int(r["outcome_elapsed_min"] or 60))
            open_pos.append({"setup": r["setup_name"], "is_long": il, "entry": float(r["spot"] or 0),
                             "exit": exit_et, "pnl": pnl})
            max_conc = max(max_conc, len(open_pos))
            placed.append((r["setup_name"], il, et))
            dayp += pnl
            k = (r["setup_name"], "LONG" if il else "SHORT")
            per_setup[k][0] += 1; per_setup[k][1] += 1 if pts > 0 else 0; per_setup[k][2] += pnl
            trades.append({"date": d, "id": r["id"], "setup": r["setup_name"], "long": il,
                           "pts": pts, "qty": qty, "pnl": pnl, "basket": st, "et": et,
                           "grade": r["grade"], "para": r["paradigm"], "vix": r["vix"]})
        for p in open_pos:
            realized += p["pnl"]
        daily[d] = dayp

    peak = dd = cum = 0.0
    for d in sorted(daily):
        cum += daily[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    tot = sum(daily.values())
    n = len(trades); nw = sum(1 for t in trades if t["pts"] > 0)
    dv = sorted(daily.values(), reverse=True)
    top3 = sum(dv[:3])
    mo = collections.defaultdict(float); mos = collections.Counter()
    for d, v in daily.items():
        mo[d.strftime("%Y-%m")] += v; mos[d.strftime("%Y-%m")] += 1
    return {
        "total": tot, "trades": n, "wins": nw, "wr": nw / max(n, 1) * 100,
        "maxdd": dd, "sessions": len(daily), "daily": daily, "per_setup": dict(per_setup),
        "trade_rows": trades, "skips": dict(skips), "max_conc": max_conc,
        "green": sum(1 for v in daily.values() if v > 0),
        "median_day": statistics.median(daily.values()) if daily else 0.0,
        "top3_share": (top3 / tot * 100) if tot else 0.0,
        "ex_top3": tot - top3,
        "month": {k: mo[k] for k in sorted(mo)}, "month_sessions": dict(mos),
        "per_trade": tot / max(n, 1),
        "ret_dd": (tot / abs(dd)) if dd else float("inf"),
        "tpd": n / max(len(daily), 1),
    }


def fmt(s, label=""):
    return (f"{label:<34}{s['total']:>9,.0f}{s['trades']:>7}{s['wr']:>5.0f}%"
            f"{s['maxdd']:>9,.0f}{s['per_trade']:>8.1f}{s['ret_dd']:>7.1f}"
            f"{s['green']:>4}/{s['sessions']:<4}{s['tpd']:>6.1f}")


HDR = (f"{'':<34}{'total$':>9}{'trades':>7}{'WR':>6}{'MaxDD':>9}{'$/t':>8}{'r/DD':>7}{'green':>9}{'t/day':>6}")
