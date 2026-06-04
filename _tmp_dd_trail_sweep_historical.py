"""DD trail sweep — HISTORICAL V16-pass replication (pre-May-18 era).

Replicates the CURRENT `_passes_live_filter()` DD-LONG path exactly
(main.py:4137-4373) on historical graded DD long signals, since skip_reason
only exists from May 18 (V16 DD admit ship).

VALIDATION FIRST: replicated filter applied to the post-V16 era must
reproduce the skip_reason ground truth (pass set = PLACED + cap_long_full +
daily_loss_limit + master_kill). Mismatch -> abort per Validation Protocol.

DD LONG filter rules replicated (all inputs on setup_log + gap from chain):
  R1  paradigm==GEX-TARGET and t>=13:00            -> block (S180)
  R2  14:30 <= t < 15:00                            -> block (V11 dead zone)
  R3  t >= 15:30                                    -> block (V11 late)
  R4  |daily_gap| > 30 and t < 10:00                -> block (V12-fix, longs)
  R5  paradigm==SIDIAL-EXTREME and hour==14         -> block (S195)
  R6  align < 0                                     -> block (V16.1)
  R7  align >= 3                                    -> block (V13)
  R8  vix >= 22                                     -> block (V13)
  R9  paradigm in GEX-LIS/AG-LIS/AG-PURE/BofA-LIS/BOFA-MESSY -> block (V13)
  R10 grade == 'C'                                  -> block (V13)
  (vix>22 & overvix<2 gate redundant after R8; shorts-only rules N/A)
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
BAD_PARA = ("GEX-LIS", "AG-LIS", "AG-PURE", "BofA-LIS", "BOFA-MESSY")


def load_gaps(conn):
    """daily gap = first spot of day - last spot of prior day (ET), chain_snapshots."""
    rows = conn.execute(text("""
        WITH d AS (
          SELECT date(ts AT TIME ZONE 'America/New_York') dt,
                 (array_agg(spot ORDER BY ts ASC))[1] first_spot,
                 (array_agg(spot ORDER BY ts DESC))[1] last_spot
          FROM chain_snapshots WHERE spot IS NOT NULL AND ts >= '2026-03-15'
          GROUP BY 1)
        SELECT dt, first_spot, LAG(last_spot) OVER (ORDER BY dt) prev_close FROM d ORDER BY dt
    """)).fetchall()
    return {r[0]: (float(r[1]) - float(r[2])) for r in rows if r[1] and r[2]}


def passes_v16_dd_long(ts_et, paradigm, grade, align, vix, gap):
    t = ts_et.time()
    p = paradigm or ""
    a = align if align is not None else 0
    if p == "GEX-TARGET" and t >= dtime(13, 0): return "R1_gextarget_pm"
    if dtime(14, 30) <= t < dtime(15, 0): return "R2_deadzone"
    if t >= dtime(15, 30): return "R3_late"
    if gap is not None and abs(gap) > 30 and t < dtime(10, 0): return "R4_gap"
    if p == "SIDIAL-EXTREME" and ts_et.hour == 14: return "R5_sidial_hr14"
    if a < 0: return "R6_align_neg"
    if a >= 3: return "R7_align3"
    if vix is not None and float(vix) >= 22: return "R8_vix"
    if p in BAD_PARA: return "R9_paradigm"
    if grade == "C": return "R10_gradeC"
    return None  # PASS


def fetch_bars(conn, ts_utc, d_et):
    eod = datetime.combine(d_et, dtime(15, 55), tzinfo=ET).astimezone(UTC)
    rows = conn.execute(text("""
        SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close
        FROM vps_es_range_bars WHERE range_pts=5 AND ts_start>=:a AND ts_start<=:b
        ORDER BY ts_start"""), {"a": ts_utc, "b": eod}).fetchall()
    return [(r[0], r[1], float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            for r in rows if r[2] is not None]


def sweep(trades, label):
    print(f"\n=== {label} (n={len(trades)}) ===")
    print(f"{'variant':>22} | {'tot':>7} {'avg':>6} {'WR%':>4} {'maxDD':>7}")
    out = {}
    for name, a, g, bt, bl in [("a10 g5", 10, 5, None, 0), ("a12 g5", 12, 5, None, 0),
                               ("a15 g5", 15, 5, None, 0), ("a20 g5 (LIVE)", 20, 5, None, 0),
                               ("a25 g5", 25, 5, None, 0),
                               ("a20g5 + BE@10lock1", 20, 5, 10, 1),
                               ("a20g5 + BE@12lock1", 20, 5, 12, 1),
                               ("a20g5 + BE@14lock1", 20, 5, 14, 1)]:
        pnls = [mes_walk(t["bars"], t["entry"], True, SL, bt, bl, a, g, 100000)["pnl"]
                for t in trades]
        n = len(pnls)
        if n == 0: continue
        tot = sum(pnls); wins = sum(1 for p in pnls if p > 0.5)
        eq = mdd = peak = 0.0
        for p in pnls:
            eq += p; peak = max(peak, eq); mdd = min(mdd, eq - peak)
        out[name] = (round(tot, 1), round(100 * wins / n), round(mdd, 1))
        print(f"{name:>22} | {round(tot,1):>7} {round(tot/n,2):>6} {round(100*wins/n):>4} {round(mdd,1):>7}")
    return out


with eng.connect() as conn:
    gaps = load_gaps(conn)

    # ---- STEP 1: VALIDATION on post-V16 era against skip_reason ground truth
    rows = conn.execute(text("""
        SELECT sl.id, sl.ts, sl.paradigm, sl.grade, sl.greek_alignment, sl.vix,
               COALESCE(sl.real_trade_skip_reason,
                        CASE WHEN rto.setup_log_id IS NOT NULL THEN 'PLACED' ELSE 'null' END) truth
        FROM setup_log sl LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='DD Exhaustion' AND lower(sl.direction) IN ('long','bullish')
          AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
          AND sl.ts >= '2026-05-18' AND sl.ts < '2026-06-04'
        ORDER BY sl.ts
    """)).fetchall()
    print("=== STEP 1: filter replication validation (post-V16, n=%d) ===" % len(rows))
    agree = mism = 0
    for lid, ts, p, g, al, vx, truth in rows:
        ts_et = ts.astimezone(ET)
        block = passes_v16_dd_long(ts_et, p, g, al, vx, gaps.get(ts_et.date()))
        pred_pass = block is None
        truth_pass = truth in ("PLACED", "cap_long_full", "daily_loss_limit", "master_kill")
        if truth == "null":
            continue  # ambiguous rows, not counted
        if pred_pass == truth_pass:
            agree += 1
        else:
            mism += 1
            print(f"  MISMATCH lid={lid} {ts_et:%m-%d %H:%M} pred={'PASS' if pred_pass else block} truth={truth} para={p} grade={g} align={al} vix={vx}")
    print(f"  agreement: {agree}/{agree+mism} ({round(100*agree/max(1,agree+mism))}%)")
    if agree / max(1, agree + mism) < 0.9:
        print("  !! REPLICATION FAILED (<90%) — ABORT per Validation Protocol")
        sys.exit(1)

    # ---- STEP 2: historical V16-pass populations + sweep
    for lo, hi, label in [("2026-04-18", "2026-05-18", "PRE-V16 era Apr18-May17 (V13-gates era)"),
                          ("2026-03-23", "2026-04-18", "OLDER era Mar23-Apr17 (pre-V13-gates, caution)")]:
        rows = conn.execute(text("""
            SELECT sl.id, sl.ts, sl.paradigm, sl.grade, sl.greek_alignment, sl.vix, sl.abs_es_price
            FROM setup_log sl
            WHERE sl.setup_name='DD Exhaustion' AND lower(sl.direction) IN ('long','bullish')
              AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
              AND sl.ts >= :lo AND sl.ts < :hi ORDER BY sl.ts
        """), {"lo": lo, "hi": hi}).fetchall()
        passing, blocks = [], {}
        for lid, ts, p, g, al, vx, es in rows:
            ts_et = ts.astimezone(ET)
            block = passes_v16_dd_long(ts_et, p, g, al, vx, gaps.get(ts_et.date()))
            if block:
                blocks[block] = blocks.get(block, 0) + 1
                continue
            bars = fetch_bars(conn, ts, ts_et.date())
            if not bars:
                continue
            entry = float(es) if es and float(es) > 1000 else bars[0][2]
            passing.append({"lid": lid, "entry": entry, "bars": bars})
        print(f"\n--- {label}: {len(rows)} signals -> {len(passing)} V16-pass; blocks: {blocks}")
        if passing:
            sweep(passing, f"V16-PASS {label}")
