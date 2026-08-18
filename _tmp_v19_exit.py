# -*- coding: utf-8 -*-
"""V19 — EXIT study. Re-optimise stop + trail on clean 1-minute SPX OHLC, Feb 19 -> Aug 6.

Why this and not the entry filter: three separate tests now show entry-selection rules fitted
to this dataset do not generalise. Exits apply to EVERY trade, so the sample per parameter is
the whole book, not a selected slice.

No lookahead: each trade is walked from its signal timestamp to 15:57 ET the same day (the live
EOD flatten). The prior study used `actual elapsed + 30 min` as the horizon, which leaks the
real exit time into the counterfactual — corrected here.
"""
import os, sys, pickle, bisect, collections, statistics
from datetime import timedelta, time as dtime
from zoneinfo import ZoneInfo
sys.path.insert(0, '.')
from app.mes_sim_backfill import mes_walk, _DEFAULT_PARAMS

ET = ZoneInfo("America/New_York")
CACHE = "_tmp_v19_univ.pkl"
SETUPS = ('Skew Charm', 'AG Short', 'DD Exhaustion', 'VIX Divergence',
          'ES Absorption', 'Vanna Pivot Bounce')


def build(refresh=False):
    if not refresh and os.path.exists(CACHE):
        return pickle.load(open(CACHE, "rb"))
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"]); conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT ts, bar_open, bar_high, bar_low, bar_close FROM spx_ohlc_1m ORDER BY ts")
    bars = []
    for ts, o, h, l, c in cur.fetchall():
        if o is None:
            continue
        bars.append((ts, ts + timedelta(minutes=1), float(o), float(h), float(l), float(c)))
    bt = [b[0].timestamp() for b in bars]
    cur.execute("""SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.grade, sl.spot,
        sl.outcome_pnl, sl.outcome_result, sl.outcome_elapsed_min, sl.live_pass, sl.vix,
        sl.paradigm, sl.greek_alignment, sl.basket_pct, sl.overvix
        FROM setup_log sl WHERE sl.setup_name = ANY(%s) AND sl.outcome_result IS NOT NULL
          AND sl.spot IS NOT NULL
          AND date(sl.ts AT TIME ZONE 'America/New_York') >= '2026-02-20'
        ORDER BY sl.ts""", (list(SETUPS),))
    univ = []
    for (sid, ts, sn, d, gr, spot, opnl, ores, oem, lp, vix, para, align, bp, ovx) in cur.fetchall():
        et = ts.astimezone(ET)
        univ.append(dict(id=sid, ts=ts, et=et, date=et.date(), month=et.strftime("%Y-%m"),
                         setup=sn, is_long=(d or '').lower() in ('long', 'bullish'),
                         grade=gr, entry=float(spot), chain_pnl=float(opnl or 0),
                         chain_res=ores, elapsed=int(oem) if oem else 90,
                         live_pass=bool(lp), vix=float(vix or 0), paradigm=para,
                         greek_alignment=align, basket_pct=bp,
                         overvix=float(ovx) if ovx is not None else None,
                         direction=d))
    pickle.dump((univ, bars, bt), open(CACHE, "wb"))
    return univ, bars, bt


UNIV, BARS, BT = build()


def bars_to_eod(u):
    """Bars from the signal until 15:57 ET the same session. No lookahead."""
    e0 = u["ts"].timestamp()
    close_et = u["et"].replace(hour=15, minute=57, second=0, microsecond=0)
    e1 = close_et.timestamp()
    i = bisect.bisect_left(BT, e0)
    out = []
    j = i
    while j < len(BARS) and BT[j] <= e1:
        out.append(BARS[j]); j += 1
    return out


_BARCACHE = {}


def walk(u, sl, be_trigger, be_lock, act, gap):
    key = u["id"]
    if key not in _BARCACHE:
        _BARCACHE[key] = bars_to_eod(u)
    b = _BARCACHE[key]
    if not b:
        return None
    mm = int((b[-1][0] - b[0][0]).total_seconds() / 60) + 2
    return mes_walk(b, u["entry"], u["is_long"], sl, be_trigger, be_lock, act, gap, mm)


def live_params(setup):
    d = _DEFAULT_PARAMS.get(setup, {"sl": 14, "be_trigger": None, "be_lock": 0,
                                    "trail_act": 10, "trail_gap": 5})
    return (d["sl"], d["be_trigger"], d["be_lock"], d["trail_act"], d["trail_gap"])


def run(trades, params_by_setup):
    """params_by_setup: setup -> (sl, be_trigger, be_lock, act, gap). Returns list of pnl pts."""
    out = []
    for u in trades:
        p = params_by_setup.get(u["setup"]) or live_params(u["setup"])
        r = walk(u, *p)
        if r is None:
            continue
        out.append((u, r["pnl"]))
    return out


def stats(pairs):
    if not pairs:
        return dict(n=0, tot=0.0, wr=0.0, ppt=0.0, dd=0.0)
    p = [x[1] for x in pairs]
    byday = collections.defaultdict(float)
    for u, v in pairs:
        byday[u["date"]] += v
    cum = peak = dd = 0.0
    for d in sorted(byday):
        cum += byday[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return dict(n=len(p), tot=sum(p), wr=sum(1 for x in p if x > 0) / len(p) * 100,
                ppt=sum(p) / len(p), dd=dd)


if __name__ == "__main__":
    print(f"### V19 exit study — universe {len(UNIV)} resolved signals, "
          f"{len({u['date'] for u in UNIV})} sessions, 1-min SPX OHLC")
    print(f"    months: {sorted({u['month'] for u in UNIV})}\n")

    # ---- VALIDATION 1: reproduce the chain outcome with LIVE params ----
    print("### VALIDATION 1 — re-sim with LIVE params vs the DB chain outcome")
    live = {s: live_params(s) for s in SETUPS}
    pairs = run(UNIV, live)
    agree = sum(1 for u, v in pairs if (v > 0) == (u["chain_pnl"] > 0))
    print(f"  n={len(pairs)}   direction agreement with DB chain result: {agree/len(pairs)*100:.0f}%")
    print(f"  totals: 1-min re-sim {sum(v for _, v in pairs):+,.1f} pts   "
          f"DB chain {sum(u['chain_pnl'] for u, _ in pairs):+,.1f} pts")
    d = [v - u["chain_pnl"] for u, v in pairs]
    print(f"  mean diff {statistics.mean(d):+.2f} pt/trade   median {statistics.median(d):+.2f}   "
          f"MAE {statistics.mean(abs(x) for x in d):.2f}")
    print("  (a gap is EXPECTED: the DB chain sim samples 2-min snapshots and holds to its own")
    print("   cycle extremes; 1-min bars see more intrabar stop touches, so re-sim reads lower.)")

    # ---- VALIDATION 2: against real broker fills ----
    print("\n### VALIDATION 2 — vs REAL BROKER fills (the only ground truth)")
    import json, psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"]); conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""SELECT rto.setup_log_id, rto.state, sl.direction
                   FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
                   WHERE sl.ts AT TIME ZONE 'America/New_York' >= '2026-06-13'""")
    brk = {}
    for lid, st, dirn in cur.fetchall():
        st = st if isinstance(st, dict) else json.loads(st)
        fp = st.get("fill_price")
        xp = (st.get("stop_fill_price_pre_fifo_reconcile") or st.get("stop_fill_price")
              or st.get("close_fill_price_pre_fifo_reconcile") or st.get("close_fill_price"))
        if fp is None or xp is None:
            continue
        sgn = 1 if str(dirn).lower() in ("long", "bullish") else -1
        brk[lid] = (float(xp) - float(fp)) * sgn
    m = [(u, v) for u, v in pairs if u["id"] in brk]
    if m:
        e1 = [v - brk[u["id"]] for u, v in m]
        e2 = [u["chain_pnl"] - brk[u["id"]] for u, _ in m]
        print(f"  matched trades: {len(m)}")
        print(f"    broker total      {sum(brk[u['id']] for u, _ in m):+8.1f} pts")
        print(f"    1-min re-sim      {sum(v for _, v in m):+8.1f} pts   "
              f"MAE {statistics.mean(abs(x) for x in e1):5.2f}   bias {statistics.mean(e1):+.2f}")
        print(f"    DB chain sim      {sum(u['chain_pnl'] for u, _ in m):+8.1f} pts   "
              f"MAE {statistics.mean(abs(x) for x in e2):5.2f}   bias {statistics.mean(e2):+.2f}")
        better = "1-min re-sim" if statistics.mean(abs(x) for x in e1) < statistics.mean(abs(x) for x in e2) else "DB chain sim"
        print(f"    -> {better} is closer to the broker")

    print("\n### baseline by setup, LIVE exit params, 1-min basis")
    print(f"  {'setup':<22}{'live sl/be/act/gap':<22}{'n':>5}{'WR':>6}{'total':>10}{'pts/t':>8}{'MaxDD':>9}")
    for s in SETUPS:
        sub = [(u, v) for u, v in pairs if u["setup"] == s]
        st = stats(sub)
        p = live_params(s)
        ps = f"{p[0]:g}/{p[1] if p[1] is not None else '-'}/{p[3]:g}/{p[4]:g}"
        print(f"  {s:<22}{ps:<22}{st['n']:>5}{st['wr']:>5.0f}%{st['tot']:>+10.1f}{st['ppt']:>+8.2f}{st['dd']:>9.1f}")
