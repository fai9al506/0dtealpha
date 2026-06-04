"""DD trail sweep — V16-PASS ONLY population (post-V16, May 18 - Jun 3).
Placed (12) + risk-blocked-but-filter-passed (cap_long_full / daily_loss_limit
/ master_kill, 9). Excludes live_filter_block entirely. Entry = broker fill
where placed, else abs_es_price.
"""
import os, sys
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.mes_sim_backfill import mes_walk

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
eng = create_engine(os.environ["DATABASE_URL"], isolation_level="AUTOCOMMIT")
SL = 12.0


def fetch_bars(conn, ts_utc, d_et):
    eod = datetime.combine(d_et, dtime(15, 55), tzinfo=ET).astimezone(UTC)
    rows = conn.execute(text("""
        SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close
        FROM vps_es_range_bars WHERE range_pts=5 AND ts_start>=:a AND ts_start<=:b
        ORDER BY ts_start"""), {"a": ts_utc, "b": eod}).fetchall()
    return [(r[0], r[1], float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            for r in rows if r[2] is not None]


with eng.connect() as conn:
    rows = conn.execute(text("""
        SELECT sl.id, sl.ts, sl.abs_es_price, sl.real_trade_skip_reason,
               (rto.state->>'fill_price')::float
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='DD Exhaustion'
          AND lower(sl.direction) IN ('long','bullish')
          AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
          AND sl.ts >= '2026-05-18' AND sl.ts < '2026-06-04'
          AND (rto.setup_log_id IS NOT NULL
               OR sl.real_trade_skip_reason IN ('cap_long_full','daily_loss_limit','master_kill'))
        ORDER BY sl.ts
    """)).fetchall()
    trades = []
    for lid, ts, es, skip, fill in rows:
        bars = fetch_bars(conn, ts, ts.astimezone(ET).date())
        if not bars:
            print(f"  !! lid={lid} no bars")
            continue
        entry = float(fill) if fill else (float(es) if es and float(es) > 1000 else bars[0][2])
        trades.append({"lid": lid, "entry": entry, "bars": bars, "skip": skip or "PLACED"})
    print(f"V16-PASS population n={len(trades)} "
          f"(placed={sum(1 for t in trades if t['skip']=='PLACED')})")

    print(f"\n{'variant':>22} | {'tot':>7} {'avg':>6} {'WR%':>4} {'maxDD':>7}")
    variants = [("a10 g5", 10, 5, None, 0), ("a12 g5", 12, 5, None, 0),
                ("a15 g5", 15, 5, None, 0), ("a20 g5 (LIVE)", 20, 5, None, 0),
                ("a25 g5", 25, 5, None, 0),
                ("a20g5 + BE@10lock1", 20, 5, 10, 1),
                ("a20g5 + BE@12lock1", 20, 5, 12, 1),
                ("a15g5 + BE@10lock1", 15, 5, 10, 1)]
    for name, a, g, bt, bl in variants:
        pnls = [mes_walk(t["bars"], t["entry"], True, SL, bt, bl, a, g, 100000)["pnl"]
                for t in trades]
        n = len(pnls); tot = sum(pnls)
        wins = sum(1 for p in pnls if p > 0.5)
        eq = mdd = peak = 0.0
        for p in pnls:
            eq += p; peak = max(peak, eq); mdd = min(mdd, eq - peak)
        print(f"{name:>22} | {round(tot,1):>7} {round(tot/n,2):>6} {round(100*wins/n):>4} {round(mdd,1):>7}")

    # per-trade detail at the 3 candidate settings for transparency
    print("\nper-trade pnl at [a20g5 | a20g5+BE@12 | a20g5+BE@10]:")
    for t in trades:
        p1 = mes_walk(t["bars"], t["entry"], True, SL, None, 0, 20, 5, 100000)["pnl"]
        p2 = mes_walk(t["bars"], t["entry"], True, SL, 12, 1, 20, 5, 100000)["pnl"]
        p3 = mes_walk(t["bars"], t["entry"], True, SL, 10, 1, 20, 5, 100000)["pnl"]
        print(f"  lid={t['lid']:>5} {t['skip']:>16} entry={t['entry']:>8} | "
              f"{round(p1,1):>6} | {round(p2,1):>6} | {round(p3,1):>6}")
