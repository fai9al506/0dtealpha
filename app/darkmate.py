# -*- coding: utf-8 -*-
"""Dark Mate framework — self-contained, fail-soft (never touches the trade loop).

(A) capture(): 1-min TradeStation tech-basket -> semi_basket table (raw prices + basket %).
(B) results(date): semi/gamma/2-factor sizing vs baseline vs real-TSRT, on the V16 set.
(C) levels(at): live multi-expiry gamma+vanna cluster levels (the framework map), live + history.

Init from main.py: darkmate.init(engine, api_get). Scheduler calls darkmate.capture() every 1 min.
"""
import json, traceback, time as _time
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
from zoneinfo import ZoneInfo
from sqlalchemy import text
from app.live_filter import passes_v16, load_gaps, COLS

ET = ZoneInfo("America/New_York")
SEMI_TK = ['NVDA', 'AMD', 'AVGO', 'META', 'MSFT', 'GOOGL']
EXPS = ('TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS')
PATH = 25      # strikes above/below spot for gamma path
TH = 20.0      # gamma favorability threshold ($M)

_engine = None
_api_get = None
_opens = {}    # {date_iso: {sym: open_price}} session opens, daily reset


def init(engine, api_get_fn):
    global _engine, _api_get
    _engine = engine
    _api_get = api_get_fn
    try:
        _db_init()
    except Exception:
        print(f"[darkmate] db_init failed: {traceback.format_exc()}", flush=True)
    print("[darkmate] initialized", flush=True)


def _db_init():
    if not _engine:
        return
    with _engine.begin() as c:
        c.execute(text("""CREATE TABLE IF NOT EXISTS semi_basket (
            et timestamp PRIMARY KEY, basket_pct numeric, n_names int, details jsonb)"""))


def _now_et():
    return datetime.now(ET)


# ====================== (A) CAPTURE ======================
def capture():
    """1-min: fetch 6 tech quotes, compute basket %-from-session-open, upsert into semi_basket."""
    if not _engine or not _api_get:
        return
    try:
        # retry the quote fetch — TS resets connections under load at the open (transient)
        qlist = None
        for attempt in range(3):
            try:
                r = _api_get(f"/marketdata/quotes/{','.join(SEMI_TK)}", timeout=10)
                qlist = r.json().get("Quotes", [])
                break
            except Exception:
                if attempt == 2:
                    raise
                _time.sleep(1.5)
        quotes = {}
        for q in (qlist or []):
            s = q.get("Symbol", "")
            last = q.get("Last") or q.get("Close")
            if s and last:
                quotes[s] = float(last)
        if not quotes:
            return
        n = _now_et()
        day = n.date().isoformat()
        for d in list(_opens):           # daily reset
            if d != day:
                _opens.pop(d, None)
        opens = _opens.setdefault(day, {})
        pcts, details = {}, {}
        for s, p in quotes.items():
            opens.setdefault(s, p)       # first quote of the day = session open
            o = opens[s]
            pct = (p - o) / o * 100 if o else 0.0
            pcts[s] = pct
            details[s] = {"price": round(p, 2), "open": round(o, 2), "pct": round(pct, 3)}
        basket = sum(pcts.values()) / len(pcts)
        et_min = n.replace(tzinfo=None, second=0, microsecond=0)
        with _engine.begin() as c:
            c.execute(text("""INSERT INTO semi_basket (et, basket_pct, n_names, details)
                VALUES (:et,:b,:n,:d)
                ON CONFLICT (et) DO UPDATE SET basket_pct=:b, n_names=:n, details=:d"""),
                {"et": et_min, "b": round(basket, 4), "n": len(pcts), "d": json.dumps(details)})
    except Exception:
        print(f"[darkmate] capture failed: {traceback.format_exc()}", flush=True)


# ====================== sizing helpers ======================
def _semi_series(conn, day):
    rows = conn.execute(text("SELECT et, basket_pct FROM semi_basket WHERE et::date=:d ORDER BY et"),
                        {"d": day}).fetchall()
    return [(r[0], float(r[1])) for r in rows]


def _semi_at(series, et_naive):
    p = [v for (x, v) in series if x <= et_naive]
    return p[-1] if p else None


def _gamma_fav(conn, ts_utc, spot, isLong):
    """Gamma favorability = gamma_below - gamma_above (mirror short). As-of ts_utc.
    Uses the ALL bucket = the true total exposure. (The TODAY/WEEK/30DAYS buckets are
    CUMULATIVE/nested — summing them double-counts; ALL is the correct full total.)"""
    rows = conn.execute(text("""
        SELECT DISTINCT ON (strike) strike, value
        FROM volland_exposure_points
        WHERE greek='gamma' AND expiration_option='ALL' AND ts_utc<=:t
          AND ts_utc >= :t0 AND strike BETWEEN :lo AND :hi
        ORDER BY strike, ts_utc DESC"""),
        {"t": ts_utc, "t0": ts_utc - timedelta(hours=8),
         "lo": spot - 200, "hi": spot + 200}).fetchall()
    prof = {float(k): float(v) / 1e6 for k, v in rows}
    if not prof:
        return None
    above = sum(v for k, v in prof.items() if spot < k <= spot + PATH)
    below = sum(v for k, v in prof.items() if spot - PATH <= k < spot)
    return (below - above) if isLong else (above - below)


def _semi_mult(isLong, sb):
    if sb is None:
        return 1.0
    if (isLong and sb > 0) or (not isLong and sb < 0):
        return 2.0
    if (isLong and sb < 0) or (not isLong and sb > 0):
        return 0.5
    return 1.0


def _gamma_mult(f):
    return (2.0 if f > TH else (0.5 if f < -TH else 1.0)) if f is not None else 1.0


def _gamma_adj(f):
    return (1.25 if f > TH else (0.75 if f < -TH else 1.0)) if f is not None else 1.0


# ====================== (B) RESULTS ======================
def results(date_iso=None):
    """Per-trade sizing comparison on the V16 set for a date (default today, ET).
    Returns baseline/semi/gamma/2factor (portal pnl x$5) + real-TSRT broker $ + daily totals."""
    if not _engine:
        return {"error": "no engine"}
    if not date_iso:
        date_iso = _now_et().date().isoformat()
    try:
        with _engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            gaps = load_gaps(conn)
            rows = conn.execute(text(f"""
                SELECT {COLS}, spot, outcome_pnl, outcome_result
                FROM setup_log
                WHERE (ts AT TIME ZONE 'America/New_York')::date = :d
                ORDER BY ts"""), {"d": date_iso}).mappings().all()
            v16 = [r for r in rows if passes_v16(r, gaps)]
            series = _semi_series(conn, date_iso)
            # real TSRT fills for these lids
            lids = [r['id'] for r in v16]
            real = {}
            if lids:
                rt = conn.execute(text("""SELECT setup_log_id, state FROM real_trade_orders WHERE setup_log_id=ANY(:ids)"""),
                                  {"ids": lids}).fetchall()
                for lid, st in rt:
                    if isinstance(st, str):
                        try: st = json.loads(st)
                        except: st = {}
                    real[lid] = st or {}
            out = []
            tot = {"base": 0.0, "semi": 0.0, "gamma": 0.0, "two": 0.0, "real": 0.0}
            for r in v16:
                if r['outcome_pnl'] is None or r['spot'] is None:
                    continue
                isLong = r['direction'] in ('long', 'bullish')
                et = r['ts'].astimezone(ET); etn = et.replace(tzinfo=None)
                usd = float(r['outcome_pnl']) * 5
                sb = _semi_at(series, etn)
                f = _gamma_fav(conn, r['ts'], float(r['spot']), isLong)
                sm = _semi_mult(isLong, sb)
                two = max(0.375, min(2.5, sm * _gamma_adj(f)))
                gm = _gamma_mult(f)
                st = real.get(r['id'], {})
                rpnl = None
                en, ex = st.get('fill_price'), st.get('close_fill_price')
                if en is not None and ex is not None:
                    sh = (not isLong)
                    rpnl = ((en - ex) if sh else (ex - en)) * 5
                row = {
                    "time": et.strftime("%H:%M"), "setup": r['setup_name'],
                    "dir": "L" if isLong else "S", "base": round(usd, 0),
                    "semi": round(usd * sm, 0), "gamma": round(usd * gm, 0), "two": round(usd * two, 0),
                    "real": (round(rpnl, 0) if rpnl is not None else None),
                    "semi_mult": sm, "gamma_fav": (round(f, 0) if f is not None else None),
                    "basket": (round(sb, 2) if sb is not None else None),
                    "placed": r['id'] in real, "result": r['outcome_result'],
                }
                out.append(row)
                tot["base"] += usd; tot["semi"] += usd * sm; tot["gamma"] += usd * gm; tot["two"] += usd * two
                if rpnl is not None:
                    tot["real"] += rpnl
            tot = {k: round(v, 0) for k, v in tot.items()}
            return {"date": date_iso, "n": len(out), "trades": out, "totals": tot}
    except Exception:
        return {"error": traceback.format_exc()}


def results_history(days=20):
    """Daily totals (base/semi/gamma/two/real) for the last N trading days with V16 trades."""
    if not _engine:
        return {"error": "no engine"}
    try:
        with _engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            ds = conn.execute(text("""SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date d
                FROM setup_log WHERE live_pass=true ORDER BY d DESC LIMIT :n"""), {"n": days}).fetchall()
            out = []
            for (d,) in reversed(ds):
                rr = results(d.isoformat())
                if "totals" in rr:
                    out.append({"date": d.isoformat(), **rr["totals"], "n": rr["n"]})
            return {"days": out}
    except Exception:
        return {"error": traceback.format_exc()}


# ====================== (C) LEVELS (framework map) ======================
def levels(at_iso=None, greek='gamma', rng=150):
    """Multi-expiry per-strike profile near spot with cluster/confluence detection.
    at_iso = ISO ts (history) or None (latest). greek = gamma or vanna. rng = +/- pts window."""
    try:
        rng = max(40, min(400, int(rng)))
    except Exception:
        rng = 150
    if not _engine:
        return {"error": "no engine"}
    try:
        with _engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            if at_iso:
                tcut = text("ts_utc <= :t")
                params = {"t": at_iso}
            else:
                tcut = text("TRUE")
                params = {}
            # latest snapshot ts per expiration <= cut
            EXPS4 = ['TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL']
            base = conn.execute(text(f"""
                SELECT expiration_option, MAX(ts_utc) mt FROM volland_exposure_points
                WHERE greek=:g AND expiration_option=ANY(:e) AND {tcut.text}
                GROUP BY expiration_option"""), {"g": greek, "e": EXPS4, **params}).fetchall()
            if not base:
                return {"error": "no data"}
            spot = None
            per_exp = {}
            snap_ts = None
            for exp, mt in base:
                rows = conn.execute(text("""SELECT strike, value, current_price FROM volland_exposure_points
                    WHERE greek=:g AND expiration_option=:exp AND ts_utc=:mt"""),
                    {"g": greek, "exp": exp, "mt": mt}).fetchall()
                d = {}
                for k, v, cp in rows:
                    d[float(k)] = float(v) / 1e6
                    if cp and spot is None:
                        spot = float(cp)
                if exp == 'ALL':
                    snap_ts = mt.isoformat()
                per_exp[exp] = d
            if snap_ts is None and base:
                snap_ts = max(mt for _, mt in base).isoformat()
            volland_spot = spot
            # LIVE mode: use the freshest chain spot (30s) for the marker + window centering,
            # NOT the ~2-min Volland snapshot spot. History mode keeps the snapshot's own spot.
            if not at_iso:
                sp = conn.execute(text("SELECT spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts DESC LIMIT 1")).scalar()
                if sp:
                    spot = float(sp)
            if spot is None:
                sp = conn.execute(text("SELECT spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts DESC LIMIT 1")).scalar()
                spot = float(sp) if sp else (volland_spot or 0.0)
            strikes = sorted({k for d in per_exp.values() for k in d if spot - rng <= k <= spot + rng})
            def gv(e, k): return per_exp.get(e, {}).get(k, 0.0)
            prof = []
            for k in strikes:
                t = gv('TODAY', k); w = gv('THIS_WEEK', k); m = gv('THIRTY_NEXT_DAYS', k); a = gv('ALL', k)
                # buckets are CUMULATIVE/nested -> show INCREMENTAL slices that sum to ALL (true total)
                prof.append({"strike": k,
                             "dte0": round(t, 1),
                             "weekly": round(w - t, 1),
                             "monthly": round(m - w, 1),
                             "far": round(a - m, 1),
                             "total": round(a, 1)})
            # key levels from the true total (ALL)
            above = [p for p in prof if p['strike'] > spot]
            below = [p for p in prof if p['strike'] < spot]
            def nearest(lst, pos):
                c = [p for p in lst if (p['total'] > 8 if pos else p['total'] < -8)]
                return c[0]['strike'] if pos and c else (c[-1]['strike'] if (not pos and c) else None)
            key = {
                "barrier_above": nearest(above, True),
                "barrier_below": min((p['strike'] for p in below if p['total'] > 8), default=None) if below else None,
                "accel_above": next((p['strike'] for p in above if p['total'] < -8), None),
            }
            return {"greek": greek, "spot": round(spot, 1), "snap_ts": snap_ts,
                    "volland_spot": (round(volland_spot, 1) if volland_spot else None),
                    "spot_live": (not at_iso),
                    "profile": prof, "key": key,
                    "slices": [["dte0", "0DTE"], ["weekly", "Weekly"], ["monthly", "Monthly"], ["far", "Far"]]}
    except Exception:
        return {"error": traceback.format_exc()}
