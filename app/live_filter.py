# -*- coding: utf-8 -*-
"""CANONICAL live-filter (V16) logic — single source of truth.

Mirrors main.py _tlPassesStrategy(l,'v16') (~line 18833). Used by:
  - app/darkmate.py (live Dark Mate results view)
  - live_filter_recall.py (root; backfills setup_log.live_pass for analysis)

Validated against the portal: 920 trades / +3408.2 pts (all-time Feb 2026+).
WHEN THE LIVE FILTER CHANGES (V17/...): update passes_v16() here + bump LIVE_VER.
"""
import os
from zoneinfo import ZoneInfo
from sqlalchemy import text

ET = ZoneInfo("America/New_York")
LIVE_VER = "v16-sb"   # V16 + Semi-Basket (Scheme B 0/0/1). Bump when the live filter changes.

# Semi-Basket (Scheme B) — 2026-06-16. Tech basket (NVDA/AMD/AVGO/META/MSFT/GOOGL)
# %-from-session-open, stamped on each signal as setup_log.basket_pct at detection.
# 0/0/1 policy: take ONLY basket-CONFIRMED (sign matches direction); skip neutral +
# contradict; FAIL-OPEN when basket_pct is NULL (no/stale data -> take the trade).
BASKET_DEADBAND = 0.15


def _basket_sizing_mode():
    """'012' (default) re-admits neutral as a TAKE (sized 1x); '001' = legacy skip-neutral.
    LOCKSTEP env across all copies — see feedback_filter_three_copies_lockstep."""
    return os.getenv("BASKET_SIZING_MODE", "012").lower()


def basket_blocks(l):
    """True = Semi-Basket says SKIP this trade. Fail-open (False) when basket_pct is NULL.

    Mode '001' (legacy 0/0/1): skip neutral AND contradict.
    Mode '012' (default 0/1/2): skip ONLY contradict; neutral is RE-ADMITTED (sized 1x by the
    trader — sizing is the trader's job, the filter only decides take/skip)."""
    bp = l['basket_pct'] if 'basket_pct' in l else None
    if bp is None:
        return False  # fail-open: no basket data -> take (cannot create a loss)
    bp = float(bp)
    is_long = l['direction'] in ('long', 'bullish')
    if abs(bp) < BASKET_DEADBAND:
        # neutral: '001' skips it, '012' re-admits it
        return _basket_sizing_mode() != "012"
    return (bp > 0) != is_long  # contradict (sign mismatch) -> skip ; confirm -> take


def passes_v16_sb(l, gaps):
    """THE live filter = V16 base AND Semi-Basket confirm. Single source of truth."""
    if not passes_v16(l, gaps):
        return False
    if basket_blocks(l):
        return False
    return True


# columns the live filter needs from setup_log
COLS = ("id, setup_name, direction, greek_alignment, grade, paradigm, vix, overvix, ts, "
        "v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, basket_pct")


def load_gaps(conn):
    """date_iso -> (open - prev_close) gap pts, from chain_snapshots. Mirrors /api/setup/daily_gaps."""
    gaps = {}
    rows = conn.execute(text("""
        WITH closes AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York')) date(ts AT TIME ZONE 'America/New_York') d, spot p FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY date(ts AT TIME ZONE 'America/New_York'), ts DESC),
             opens AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York')) date(ts AT TIME ZONE 'America/New_York') d, spot p FROM chain_snapshots WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time>='09:30' ORDER BY date(ts AT TIME ZONE 'America/New_York'), ts ASC)
        SELECT o.d, o.p-c.p gap FROM opens o JOIN closes c ON c.d=(SELECT MAX(c2.d) FROM closes c2 WHERE c2.d<o.d)""")).fetchall()
    for r in rows:
        if r[1] is not None:
            gaps[str(r[0])] = round(float(r[1]), 1)
    return gaps


def backfill_live_pass(engine):
    """Stamp setup_log.live_pass / live_filter_ver for the whole table. Idempotent.
    Run daily (EOD) so recent signals are recallable via WHERE live_pass=true. Returns count."""
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        c.execute(text("ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS live_pass boolean"))
        c.execute(text("ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS live_filter_ver text"))
        gaps = load_gaps(c)
        c.execute(text("ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS basket_pct DOUBLE PRECISION"))
        rows = c.execute(text(f"SELECT {COLS} FROM setup_log ORDER BY ts")).mappings().all()
        lids = [r['id'] for r in rows if passes_v16_sb(r, gaps)]
        c.execute(text("UPDATE setup_log SET live_pass=false, live_filter_ver=:v WHERE live_pass IS NOT false OR live_pass IS NULL"), {"v": LIVE_VER})
        for i in range(0, len(lids), 1000):
            c.execute(text("UPDATE setup_log SET live_pass=true, live_filter_ver=:v WHERE id=ANY(:ids)"), {"v": LIVE_VER, "ids": lids[i:i+1000]})
        return len(lids)


def passes_v16(l, gaps):
    """Exact mirror of main.py _tlPassesStrategy(l,'v16'). l = mapping with COLS. gaps from load_gaps()."""
    sn = l['setup_name'] or ''
    align = l['greek_alignment'] if l['greek_alignment'] is not None else 0
    isLong = l['direction'] in ('long', 'bullish')
    para = l['paradigm']; grade = l['grade']; ts = l['ts']
    et = ts.astimezone(ET) if ts else None
    mins = (et.hour * 60 + et.minute) if et else None

    def gapFilter():
        if not ts or not isLong: return True
        g = gaps.get(et.date().isoformat())
        if g is not None and abs(g) > 30 and mins is not None and mins < 600: return False
        return True
    def v11():
        if mins is None: return True
        if sn in ('Skew Charm', 'DD Exhaustion') and 870 <= mins < 900: return False
        if sn in ('Skew Charm', 'DD Exhaustion') and mins >= 930: return False
        if sn == 'BofA Scalp' and mins >= 870: return False
        return True
    def v13Bull():
        if isLong or sn not in ('Skew Charm', 'DD Exhaustion'): return False
        return (l['v13_gex_above'] or 0) >= 75 or (l['v13_dd_near'] or 0) >= 3000000000
    def v13Vanna():
        c = l['vanna_cliff_side']; p = l['vanna_peak_side']
        if c is None or isLong: return False
        if sn == 'DD Exhaustion' and c == 'A' and p == 'B': return True
        if sn == 'Skew Charm' and c == 'A' and p == 'B': return True
        if sn == 'AG Short' and c == 'B' and p == 'A': return True
        return False
    def v13DDQ():
        if sn != 'DD Exhaustion': return False
        if isLong:
            if align >= 3 or (l['vix'] or 0) >= 22 or grade == 'C': return True
            if para in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BofA-LIS', 'BOFA-MESSY'): return True
        else:
            if para == 'BOFA-PURE' or grade in ('A+', 'C'): return True
        return False
    def scLongAlignBlock():
        return sn == 'Skew Charm' and isLong and align == 3 and para in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BOFA-MESSY')
    def v10BaseV14():
        if isLong:
            if sn == 'Skew Charm':
                if para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900: return False
                if scLongAlignBlock(): return False
                return True
            if para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900: return False
            if align < 2: return False
            if (l['vix'] or 0) > 22 and (l['overvix'] if l['overvix'] is not None else -99) < 2: return False
            return True
        if sn in ('Skew Charm', 'DD Exhaustion') and para == 'GEX-LIS': return False
        if sn == 'Skew Charm' or sn == 'AG Short': return True
        if sn == 'DD Exhaustion' and align != 0: return True
        return False
    def isOpex():
        return bool(et) and et.weekday() == 4 and 15 <= et.day <= 21

    if sn not in ('Skew Charm', 'AG Short', 'Vanna Pivot Bounce', 'ES Absorption',
                  'DD Exhaustion', 'GEX Long', 'VIX Divergence'): return False
    if sn == 'DD Exhaustion' and not isLong: return False
    if sn == 'AG Short' and para == 'AG-TARGET': return False
    # S180 GEX-TARGET PM block — runtime scopes it to SC/DD/ES Abs (main.py:4191), mirror that.
    if isLong and para == 'GEX-TARGET' and et and et.hour >= 13 and sn in ('Skew Charm', 'DD Exhaustion', 'ES Absorption'): return False
    # ── Carve-outs added 2026-06-11 to re-sync with runtime _passes_live_filter ──
    # These three setups went live AFTER the original mirror and were silently
    # dropped from the V16 dropdown (VPB fell through to v10BaseV14's align>=2 gate;
    # GEX Long / VIX Divergence weren't even in the allowed-setup list above).
    # VIX Divergence (main.py:4233): long + grade!=C + GEX-* paradigm.
    if sn == 'VIX Divergence':
        return isLong and grade != 'C' and bool(para) and para.startswith('GEX-')
    # Vanna Pivot Bounce (main.py:4340, S192): env-gated OFF by default (VPB_REAL_TRADE_ENABLED).
    # Runtime returns False when the env != "true", so TSRT never places VPB; mirror that here so
    # live_pass stops stamping phantom VPB trades (drift fixed 2026-06-25; verified vs portal CSV
    # trade_log_2026-06-24 — 5 VPB longs were the ONLY diff). LOCKSTEP with main.py:4340.
    if sn == 'Vanna Pivot Bounce':
        if os.getenv("VPB_REAL_TRADE_ENABLED", "false").lower() != "true":
            return False
        return isLong and grade == 'B' and not (et and et.hour == 11)
    # GEX Long v6 (main.py:4365): long + (gap filter) + not SIDIAL-EXTREME@hr14 + (align>=0 OR bull paradigm).
    # Detector already enforced the v6 classifier (verdict/magnet-dominance) before logging.
    if sn == 'GEX Long':
        if not isLong: return False
        # GEX-Long gap rule (2026-06-16, backtested Feb-Jun, chain-sim):
        #   gap-up(>30) MORNING(<11:00) = 67% WR / +47.6 (n=9) -> GEX Long EXEMPT from the
        #     generic pre-10:00 gap-long block (do NOT call gapFilter for GEX).
        #   gap-up(>30) AFTER 11:00 = 15% WR / -204 (n=26, the 4042 rally-end zone) -> BLOCK.
        _g = gaps.get(et.date().isoformat()) if et else None
        if _g is not None and _g > 30 and mins is not None and mins >= 660: return False
        if para == 'SIDIAL-EXTREME' and et and et.hour == 14: return False
        return (align >= 0) or (para in ('BofA-LIS', 'GEX-TARGET', 'SIDIAL-MESSY', 'BOFA-PURE'))
    if sn == 'DD Exhaustion' and isLong:
        if para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900: return False
        if align < 0 or align >= 3 or (l['vix'] or 0) >= 22: return False
        if para in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BofA-LIS', 'BOFA-MESSY') or grade == 'C': return False
        return True
    if not gapFilter(): return False
    if sn == 'Skew Charm' and grade in ('C', 'LOG'): return False
    if sn in ('IV Momentum', 'Vanna Butterfly'): return False
    if not v11() or v13Bull() or v13Vanna() or v13DDQ(): return False
    if sn == 'ES Absorption':
        if not isLong: return False  # CUT ES Abs shorts 2026-07-27 (lockstep w/ _passes_live_filter)
        if grade not in ('A', 'A+') or para in ('AG-TARGET', 'AG-LIS'): return False
        if mins is not None and mins >= 945: return False
        if isLong and align < 0: return False
        if not isLong and align > 0: return False
        if not isLong and mins is not None and mins >= 840: return False
        return True
    if sn == 'Skew Charm' and isLong and para == 'GEX-LIS': return False
    if sn == 'Skew Charm' and isLong and isOpex(): return False
    if sn == 'AG Short' and isOpex(): return False
    return v10BaseV14()
