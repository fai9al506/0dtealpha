"""DD Exhaustion trail-activation sweep (2026-06-04).

Question: is continuous trail activation=20/gap=5 (set Feb 19 on n=8) still
optimal on the full sample, vs tighter activations that secure profit sooner?

Populations:
  P1 (primary, broker truth): placed+closed real DD trades since May 18.
      Entry = actual broker fill. Used for Gate 2 cross-check + headline.
  P2 (secondary, signal-level): graded non-LOG DD LONG signals
      Apr 18 (V13 DD gates ship) -> Jun 3. Entry = abs_es_price
      (signal-time ES) else first bar open. Used for ranking robustness ONLY,
      not PnL projection. Sub-slice align!=0 mirrors the V16 DD admit profile.

Sim: S55 mes_walk (validated |real-sim| = 2.35pt mean) on vps_es_range_bars
     5pt bars, conservative adverse-first ordering, walk to 15:55 ET.
SL fixed at 12 (current live). Sweep: activation x gap grid + fixed-target
variants for context.
"""
import os, sys, json
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.mes_sim_backfill import mes_walk

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
eng = create_engine(os.environ["DATABASE_URL"], isolation_level="AUTOCOMMIT")

SL = 12.0
GRID = [(a, g) for a in (8, 10, 12, 15, 20, 25) for g in (3, 5, 8)]


def fetch_bars(conn, ts_utc, trade_date_et):
    """Bars from signal ts to 15:55 ET same day."""
    eod_utc = datetime.combine(trade_date_et, dtime(15, 55), tzinfo=ET).astimezone(UTC)
    rows = conn.execute(text("""
        SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close
        FROM vps_es_range_bars
        WHERE range_pts=5 AND ts_start >= :a AND ts_start <= :b
        ORDER BY ts_start
    """), {"a": ts_utc, "b": eod_utc}).fetchall()
    return [(r[0], r[1], float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            for r in rows if r[2] is not None]


def walk_with_target(bars, entry, is_long, sl, target):
    """Fixed target + fixed SL walker (conservative adverse-first), for the
    T10/S12 context variant. Target checked AFTER stop within a bar."""
    if not bars:
        return None
    stop = entry - sl if is_long else entry + sl
    tgt = entry + target if is_long else entry - target
    for ts_s, ts_e, o, h, l, c in bars:
        hit_stop = (l <= stop) if is_long else (h >= stop)
        hit_tgt = (h >= tgt) if is_long else (l <= tgt)
        if hit_stop:
            return -sl
        if hit_tgt:
            return target
    _, _, _, _, _, c = bars[-1]
    return (c - entry) if is_long else (entry - c)


def run_combo(trades, act, gap, be_trigger=None, be_lock=0.0):
    """trades: list of dicts {bars, entry, is_long}. Returns metrics."""
    pnls, mfes = [], []
    for t in trades:
        r = mes_walk(t["bars"], t["entry"], t["is_long"], SL,
                     be_trigger, be_lock, act, gap, max_minutes=100000)
        pnls.append(r["pnl"])
        mfes.append(r["mfe"])
    n = len(pnls)
    if n == 0:
        return None
    tot = sum(pnls)
    wins = sum(1 for p in pnls if p > 0.5)
    eq = mdd = peak = 0.0
    for p in pnls:
        eq += p
        peak = max(peak, eq)
        mdd = min(mdd, eq - peak)
    giveback = [m - p for p, m in zip(pnls, mfes) if m >= 8]
    return {"n": n, "tot": round(tot, 1), "avg": round(tot / n, 2),
            "wr": round(100 * wins / n), "mdd": round(mdd, 1),
            "worst": round(min(pnls), 1),
            "gb": round(sum(giveback) / len(giveback), 1) if giveback else 0.0,
            "pnls": pnls}


def load_p1(conn):
    rows = conn.execute(text("""
        SELECT sl.id, sl.ts, sl.direction,
               (rto.state->>'fill_price')::float,
               (rto.state->>'close_fill_price')::float,
               (rto.state->>'stop_fill_price')::float,
               rto.state->>'close_reason'
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='DD Exhaustion' AND rto.state->>'status'='closed'
        ORDER BY sl.ts
    """)).fetchall()
    out = []
    for lid, ts, d, fill, cf, sf, reason in rows:
        if not fill:
            continue
        is_long = (d or "").lower() in ("long", "bullish")
        exitp = cf or sf
        bpnl = None
        if exitp:
            bpnl = (exitp - fill) if is_long else (fill - exitp)
        date_et = ts.astimezone(ET).date()
        bars = fetch_bars(conn, ts, date_et)
        if not bars:
            print(f"  !! lid={lid} no bars, skipped")
            continue
        out.append({"lid": lid, "ts": ts, "entry": float(fill),
                    "is_long": is_long, "bars": bars,
                    "broker_pnl": bpnl, "reason": reason})
    return out


def load_p2(conn):
    rows = conn.execute(text("""
        SELECT sl.id, sl.ts, sl.abs_es_price, sl.greek_alignment, sl.grade
        FROM setup_log sl
        WHERE sl.setup_name='DD Exhaustion'
          AND lower(sl.direction) IN ('long','bullish')
          AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
          AND sl.ts >= '2026-04-18' AND sl.ts < '2026-06-04'
        ORDER BY sl.ts
    """)).fetchall()
    out, nobar = [], 0
    for lid, ts, es, align, grade in rows:
        date_et = ts.astimezone(ET).date()
        bars = fetch_bars(conn, ts, date_et)
        if not bars:
            nobar += 1
            continue
        entry = float(es) if es and float(es) > 1000 else bars[0][2]
        out.append({"lid": lid, "ts": ts, "entry": entry, "is_long": True,
                    "bars": bars, "align": align, "grade": grade})
    print(f"  P2 loaded {len(out)} (skipped {nobar} no-bars)")
    return out


def print_grid(trades, label):
    print(f"\n=== SWEEP: {label} (n={len(trades)}, SL=12 fixed) ===")
    print(f"{'act':>4} {'gap':>4} | {'tot':>8} {'avg':>6} {'WR%':>4} {'maxDD':>7} {'worst':>6} {'avgGiveback':>11}")
    results = {}
    for a, g in GRID:
        m = run_combo(trades, a, g)
        results[(a, g)] = m
        tag = "  <- LIVE" if (a, g) == (20, 5) else ""
        print(f"{a:>4} {g:>4} | {m['tot']:>8} {m['avg']:>6} {m['wr']:>4} {m['mdd']:>7} {m['worst']:>6} {m['gb']:>11}{tag}")
    # hybrid variants
    for (a, g, bt, bl, name) in [(20, 5, 10, 1, "a20g5 + BE@10lock1"),
                                 (20, 5, 12, 1, "a20g5 + BE@12lock1"),
                                 (15, 5, 10, 1, "a15g5 + BE@10lock1")]:
        m = run_combo(trades, a, g, be_trigger=bt, be_lock=bl)
        print(f"{name:>9} | {m['tot']:>8} {m['avg']:>6} {m['wr']:>4} {m['mdd']:>7} {m['worst']:>6} {m['gb']:>11}")
    # fixed target context
    for tgt in (10, 15):
        pnls = [walk_with_target(t["bars"], t["entry"], t["is_long"], SL, tgt) for t in trades]
        pnls = [p for p in pnls if p is not None]
        tot = sum(pnls); n = len(pnls)
        wins = sum(1 for p in pnls if p > 0.5)
        eq = mdd = peak = 0.0
        for p in pnls:
            eq += p; peak = max(peak, eq); mdd = min(mdd, eq - peak)
        print(f"T{tgt}/S12 fixed | {round(tot,1):>8} {round(tot/n,2):>6} {round(100*wins/n):>4} {round(mdd,1):>7} {round(min(pnls),1):>6}")
    return results


with eng.connect() as conn:
    print("=" * 70)
    print("STEP A: P1 placed real DD trades — Gate 2 cross-check (live params)")
    print("=" * 70)
    p1 = load_p1(conn)
    print(f"P1 n={len(p1)} closed placed trades with fills")
    diffs, signs = [], 0
    print(f"{'lid':>5} {'date':>6} {'entry':>8} {'broker':>7} {'sim':>7} {'diff':>6}  reason")
    for t in p1:
        r = mes_walk(t["bars"], t["entry"], t["is_long"], SL, None, 0, 20, 5, 100000)
        sim = r["pnl"]
        b = t["broker_pnl"]
        d = (sim - b) if b is not None else None
        if b is not None:
            diffs.append(abs(d))
            if (sim > 0.5) == (b > 0.5) or abs(sim - b) < 2:
                signs += 1
        print(f"{t['lid']:>5} {t['ts'].astimezone(ET).strftime('%m-%d'):>6} {t['entry']:>8} "
              f"{(round(b,2) if b is not None else 'n/a'):>7} {round(sim,2):>7} "
              f"{(round(d,2) if d is not None else ''):>6}  {t['reason']}")
    if diffs:
        print(f"\nGate 2: mean |sim-broker| = {round(sum(diffs)/len(diffs),2)}pt, "
              f"outcome agreement = {signs}/{len(diffs)} ({round(100*signs/len(diffs))}%)")

    r1 = print_grid(p1, "P1 placed real trades (broker-fill entries)")

    print("\n" + "=" * 70)
    print("STEP B: P2 signal-level robustness (Apr 18 - Jun 3, graded DD longs)")
    print("=" * 70)
    p2 = load_p2(conn)
    r2 = print_grid(p2, "P2 ALL graded DD longs")

    p2a = [t for t in p2 if t["align"] is not None and float(t["align"]) != 0]
    r3 = print_grid(p2a, "P2 align!=0 (V16 DD admit profile)")

    # era split within P2 align!=0
    cut = datetime(2026, 5, 18, tzinfo=ET)
    pre = [t for t in p2a if t["ts"].astimezone(ET) < cut]
    post = [t for t in p2a if t["ts"].astimezone(ET) >= cut]
    print_grid(pre, "P2 align!=0 PRE-V16 era (Apr18-May17)")
    print_grid(post, "P2 align!=0 POST-V16 era (May18-Jun3)")
