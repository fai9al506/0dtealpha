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
LIVE_VER = "v20-sb"   # V20 + Semi-Basket. Bump when the live filter changes.

# ── V20 (2026-08-17, S277/S278) — THE LIVE FILTER from 2026-08-18. ────────────────
# V20 = V16 rules + ES Absorption only when VIX >= ES_ABS_VIX_FLOOR + no Friday.
# The full change ledger (what each version number means and why) is FILTER_VERSIONS.md.
ES_ABS_VIX_FLOOR = 20.0

# Semi-Basket (Scheme B) — 2026-06-16. Tech basket (NVDA/AMD/AVGO/META/MSFT/GOOGL)
# %-from-session-open, stamped on each signal as setup_log.basket_pct at detection.
# 0/0/1 policy: take ONLY basket-CONFIRMED (sign matches direction); skip neutral +
# contradict; FAIL-OPEN when basket_pct is NULL (no/stale data -> take the trade).
BASKET_DEADBAND = 0.15


def _basket_sizing_mode():
    """'sizeonly' (default, S231): the basket NEVER blocks — it only decides 1x vs 2x.
    '012': re-admits neutral as a TAKE (sized 1x), skips contradict.
    '001': legacy Scheme B — skip neutral AND contradict.
    LOCKSTEP env across all copies — see feedback_filter_three_copies_lockstep."""
    return os.getenv("BASKET_SIZING_MODE", "sizeonly").lower()


def basket_blocks(l):
    """True = Semi-Basket says SKIP this trade. Fail-open (False) when basket_pct is NULL.

    Mode 'sizeonly' (DEFAULT since 2026-08-07, S231): NEVER blocks. The basket is a SIZING
      signal only — confirm -> 2x (real_trader._effective_qty), everything else -> 1x.
      Measured Jul 1 - Aug 6 (GEX off, chain outcomes, real_trader gate order): the
      contradict bucket is 107t / 55% WR / +36.8 pts — PROFITABLE, so blocking it forfeits
      money AND roughly doubles MaxDD (-$424 -> -$944) by thinning the book down to
      correlated confirmed trades. Blocking cost -$547 (Jul 1 - Aug 6) / -$590 (Jun 11 - Aug 6).
      Control test: selective 2x beats FLAT 2 MES on both P&L and drawdown, so the basket is
      genuinely selecting, not just levering. See memory research_s231_tsrt_counterfactual_jul_aug.
    Mode '001' (legacy 0/0/1): skip neutral AND contradict.
    Mode '012' (previous default 0/1/2): skip ONLY contradict; neutral re-admitted."""
    mode = _basket_sizing_mode()
    if mode == "sizeonly":
        return False
    bp = l['basket_pct'] if 'basket_pct' in l else None
    if bp is None:
        return False  # fail-open: no basket data -> take (cannot create a loss)
    bp = float(bp)
    is_long = l['direction'] in ('long', 'bullish')
    if abs(bp) < BASKET_DEADBAND:
        # neutral: '001' skips it, '012' re-admits it
        return mode != "012"
    return (bp > 0) != is_long  # contradict (sign mismatch) -> skip ; confirm -> take


def passes_v20(l, gaps):
    """V20 = V16 rules + ES Absorption gated to VIX >= 20 + no Friday.

    THE live filter from 2026-08-18. ES Absorption's edge is volatility-dependent:
    per trade it is -$2.6 below VIX 18 and -$6.4 at 18-20, but +$21.6 at 20-22
    (t=+2.4) and +$15.2 at 22-26 (t=+2.7). Removing the setup outright measured
    WORSE (-$1,036) because its slots refill with weaker trades under the 2/3 cap.
    Fails CLOSED on a missing VIX. Lockstep with main.py _passes_live_filter and
    both portal mirrors (strat 'v20').
    """
    if not passes_v16(l, gaps):
        return False
    if (l.get('setup_name') if hasattr(l, 'get') else l['setup_name']) == 'ES Absorption':
        v = l['vix'] if 'vix' in l else None
        if v is None or float(v) < ES_ABS_VIX_FLOOR:
            return False
    ts = l['ts'] if 'ts' in l else None
    if ts is not None and ts.astimezone(ET).weekday() == 4:
        return False   # REAL_TRADE_NO_FRIDAY, armed 2026-08-15 (S263)
    return True


def passes_v16_sb(l, gaps):
    """THE live filter = V20 base AND Semi-Basket confirm. Single source of truth.

    Name kept for compatibility with every caller and study; the BASE it applies is
    V20 as of 2026-08-17 (was V16). See FILTER_VERSIONS.md.
    """
    if not passes_v20(l, gaps):
        return False
    if basket_blocks(l):
        return False
    return True


# columns the live filter needs from setup_log
COLS = ("id, setup_name, direction, greek_alignment, grade, paradigm, vix, overvix, ts, "
        "v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, basket_pct, "
        "gex_net_ceiling")


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


# ── V17 (S233, 2026-08-08) — MONITORING ONLY, not wired to the trade path ──────────────
# The structural relaxation from S233/`S233_FILTER_STUDY.md`. V16 discards ~55% of the book
# to pick better trades; measured over 100 sessions that selection costs more than it earns
# because a thin book is a concentrated one (same finding as the basket block).
#
# V17 = V16, except: for Skew Charm / AG Short / ES Absorption / DD Exhaustion / VIX Divergence,
# when the signal's own VIX < 22, the per-setup QUALITY rules are skipped. Three things stay:
#   1. full V16 whenever VIX >= 22        (protects the March-type high-vol regime)
#   2. DD SHORTS still go through the existing V13 quality stack (raw DD shorts lost in April)
#   3. Vanna Pivot Bounce is NEVER relaxed (the only bucket negative in all 6 months)
# GEX Long is excluded (GEX_LONG_V3_REAL_TRADE_ENABLED=false).
#
# Measured (chain, cap 2/2, basket sizing, 100 sessions, x0.81 broker haircut):
#   V16 $1,590/mo, MaxDD -$1,253 (24% of equity)  ->  V17 $2,520/mo, MaxDD -$727 (14%)
# Every risk control (cap, dedup, breaker, underwater guard) is untouched; peak exposure is
# identical. LOCKSTEP with main.py _tlPassesStrategy(l,'v17') and passesStrategy(l,'v17').
V17_RELAXED = ("Skew Charm", "AG Short", "ES Absorption", "DD Exhaustion", "VIX Divergence")
V17_VIX_REARM = 22.0


def passes_v17(l, gaps):
    """V17 monitoring filter. NOT used by the trade path — portal/analysis only."""
    sn = l['setup_name'] or ''
    if sn not in ('Skew Charm', 'AG Short', 'Vanna Pivot Bounce', 'ES Absorption',
                  'DD Exhaustion', 'VIX Divergence'):
        return False
    isLong = l['direction'] in ('long', 'bullish')
    vix = l['vix'] or 0
    # high-vol regime, or a setup we never relax -> plain V16
    if vix >= V17_VIX_REARM or sn not in V17_RELAXED:
        return passes_v16(l, gaps)
    if sn == 'VIX Divergence':
        return isLong                      # long-only stands; grade/paradigm rules dropped
    if sn == 'DD Exhaustion' and not isLong:
        # re-admitted, but only through the V13 quality stack that already exists in the code
        align = l['greek_alignment'] if l['greek_alignment'] is not None else 0
        para = l['paradigm']; grade = l['grade']
        if (l['v13_gex_above'] or 0) >= 75 or (l['v13_dd_near'] or 0) >= 3000000000:
            return False                                            # V13BULL
        if l['vanna_cliff_side'] == 'A' and l['vanna_peak_side'] == 'B':
            return False                                            # V13VANNA
        if para == 'BOFA-PURE' or grade in ('A+', 'C'):
            return False                                            # V13DDQ (short branch)
        if para == 'GEX-LIS':
            return False                                            # SCDD_SHORT_GEXLIS
        return align != 0
    return True


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
        # Env gate (added 2026-08-15, S260). GEX_LONG_V3_REAL_TRADE_ENABLED was named in the
        # header comment of this file but NEVER READ, so this branch stamped live_pass=true on
        # 59 signals TSRT does not place — -108.1 pts of phantom loss in every
        # `WHERE live_pass=true` query, i.e. the book read 607.6 pts when it was really 715.7.
        # Mirrors both portal copies (`if (_GEX_LONG_REAL) _tlV16Allowed.add('GEX Long')`) and
        # the VPB gate 8 lines above. Found by filter_mirror_sweep.py: JS 0 vs Python 59.
        #
        # Note on semantics: this retroactively marks the 8 GEX Longs that WERE traded
        # (2026-06-15 → 07-01, while the env was true) as live_pass=false. That is correct and
        # already how this column behaves — 46 other real trades are stamped false because the
        # filter changed after they were placed (ES Abs shorts cut, SC long rules tightened).
        # `live_pass` answers "would TODAY's config trade this"; for what was ACTUALLY placed,
        # join real_trade_orders. backfill_live_pass() re-stamps the whole table nightly, so
        # history stays consistent by itself.
        if os.getenv("GEX_LONG_V3_REAL_TRADE_ENABLED", "false").lower() != "true":
            return False
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


# ── V18 (S260, 2026-08-15) — MONITORING ONLY, not wired to the trade path ─────────────
# ⚠️ NAME CLASH: "V18" in PROJECTION.md / S233_FILTER_STUDY.md refers to a REJECTED
# 2026-08-08 experiment (refit the entry filter per setup from scratch). That one is
# dead and was never code. THIS is the shipped V18 and the only V18 in the codebase.
#
# V18 = V16 + ONE subtraction: skip a SHORT when a +GEX wall sits close overhead.
#
# WHY. The distance from spot up to the strongest NET-gex strike above it is an upward
# magnet, and it orders BOTH books in opposite directions (V16 longs +3.14 pt/trade at
# the wall falling to +0.80 far from it; V16 shorts the mirror, -0.01 at 5-15pt rising
# to +4.21 beyond 30pt). Only the short half is actionable — cutting far-wall longs
# removes more profit than it saves.
#
# MEASURED at the LIVE cap (2 long / 3 short, 90s dedup, 1 MES, chain sim -0.6 pt
# haircut, 123 sessions Feb 19 - Aug 14):
#     $8,589 -> $9,247 (+$658)   MaxDD -$1,677 -> -$1,348   $/trade 7.51 -> 9.07
#     GREEN days 75 -> 83        RED days 48 -> 39          trade WR 60.3% -> 61.6%
# Leave-one-month-out positive 7/7. Against 400 random blocks of the same size:
# total $ p=0.005, green days gained p=0.010, red days removed p=0.003, and green days
# DESTROYED p=0.015 (a random block of this size destroys ~6; V18 destroys 1).
#
# It works because it fires at the same rate on good and bad sessions (0.89 vs 0.98
# blocked trades) but the trades it catches are +1.92 pt/trade on green sessions and
# -5.83 on red ones: on a day price grinds UP into the walls, near-wall shorts get run
# over repeatedly, and that is the day it stands the short book down.
#
# HONEST LIMITS — read before arming:
#   * buy it for CONSISTENCY, not money. 3 sessions carry 60% of the gain; ex-top-3 it
#     is ~$2.87/session and the mean daily delta is NOT significant (p=0.177).
#   * it does NOTHING for the tails — worst day, best day and longest red streak are
#     all unchanged.
#   * do NOT add a grade-A+ exception. It earns $185 more and costs $209 of drawdown,
#     two red days, and the near-perfect zero-green-days-destroyed record.
#   * do NOT put it on V17 — there the walk-forward test half is -$21 (nothing), LOMO
#     drops to 6/7, and the blocked bucket is net POSITIVE.
#   * do NOT re-hunt a sub-rule to "release the winners". 51 numeric + 7 categorical
#     axes were screened; 2 cleared p<0.05 where chance predicts 2.6, and one of those
#     was MFE (lookahead). There is no separator.
#
# FAIL-OPEN: gex_net_ceiling NULL (not yet stamped, or no +net-gex strike within 60pt
# overhead) -> the trade is TAKEN. Like basket_gate, this filter can only ever REMOVE
# trades, so missing data must never invent one.
#
# LOCKSTEP with main.py _tlPassesStrategy(l,'v18') and passesStrategy(l,'v18').
# Evidence: memory research_overhead_gex_wall_both_sides.
V18_CEILING_PTS = 15.0     # block when the wall is this close overhead, or closer
V18_VIX_MAX = 22.0         # ...and only below this VIX; at/above it, near-wall shorts are fine


def v18_blocks(l):
    """True = V18 says SKIP this short. Fail-open on missing data."""
    if l['direction'] in ('long', 'bullish'):
        return False
    ceil = l['gex_net_ceiling'] if 'gex_net_ceiling' in l else None
    if ceil is None:
        return False                                   # fail-open: no wall data
    vix = l['vix']
    if vix is None or float(vix) >= V18_VIX_MAX:
        return False                                   # high vol -> V16 behaviour
    return 0 <= float(ceil) <= V18_CEILING_PTS


def passes_v18(l, gaps):
    """V18 monitoring filter. NOT used by the trade path — portal/analysis only."""
    if not passes_v16(l, gaps):
        return False
    return not v18_blocks(l)


# ── V19 (S263, 2026-08-15) — MONITORING ONLY, not wired to the trade path ─────────────
# ⚠️ NAME NOTE: "V19" in PROJECTION.md is a REJECTED 2026-08-08 stop/trail refit, renamed
# there to V19-exits. This is the only V19 in code.
#
# V19 = V18 + skip everything on FRIDAY from 11:00 ET.
#
# WHY. Friday is the only losing weekday and it is not close:
#     Mon +$72/day 67% green · Tue +$115 72% · Wed +$77 64% · Thu +$153 73%
#     FRI -$62/day, 26% green
# Since 2026-03-13 only 3 of 20 Fridays were green. Both directions lose (long
# -1.37 pt/trade, short -2.91, against +2.13 / +2.79 on other days) and all three
# live setups lose (Skew Charm -2.31 vs +2.66, DD -2.60 vs +1.84, ES Abs -0.29 vs
# +2.27). It is NOT opex: ordinary Fridays are -$1,218 over 18 sessions, opex
# Fridays only -$213 over 5.
#
# WHY THE WHOLE DAY AND NOT JUST THE AFTERNOON. The first cut was 11:00, because
# Friday morning looks positive (+0.85 pt/trade, 57% WR) while the afternoon is
# clearly bad (-3.10 pt/trade, 43% WR). But the morning bucket is only 47 trades and
# it is NOT distinguishable from zero:
#     Friday before 11:00   n= 47  +0.85 pt/t  95% CI [-2.14, +3.93]  p=0.587
#     Friday after  11:00   n= 96  -3.10 pt/t  95% CI [-5.44, -0.72]  p=0.012
#     Mon-Thu all day       n=945  +2.42 pt/t  95% CI [+1.59, +3.31]  p=0.000
# Mon-Thu is clearly positive, Friday afternoon clearly negative, Friday morning a
# coin flip. Keeping it buys $150 over six months (~$25/mo) for 42 extra trades of
# operational exposure — and $25/mo is inside the noise of a single execution error.
# The user's call, 2026-08-15, and the statistics agree with it. Blocking the whole
# day also removes a timezone/DST edge from the rule.
#
# MEASURED at the live cap (2 long / 3 short, 90s dedup, 1 MES, chain -0.6 haircut,
# 123 sessions), V19 vs V16:
#     $9,077 -> $10,623 (+17%)     $/trade 8.34 -> 12.77 (+53%)
#     MaxDD -$1,598 -> -$955 (-40%)   trade WR 61.5% -> 65.0%
#     GREEN days 75 -> 72          RED days 48 -> 26
# The afternoon-only variant scores $10,774 / MaxDD -$763 / 81 green / 33 red — more
# money and a smaller peak drawdown, but SEVEN more red days and a worse $/trade.
# Whole-day was chosen for consistency and simplicity, not for the headline.
#
# EVIDENCE IT IS NOT DATA MINING — five weekdays were tested, so one landing at
# p<0.05 proves nothing on its own. What does:
#   * leave-one-month-out 7/7, and the SAME rule on Mon/Tue/Wed/Thu is 0/7 on all
#     four AND loses $1,033-$1,763 each. The other weekdays are the control group.
#   * blind walk-forward positive in BOTH halves (+$169 train / +$1,416 test).
#   * against 400 random blocks of the same number of afternoon trades: beaten
#     0/400, p=0.000.
#   * not an outlier effect — dropping the 3 worst Fridays still leaves -$600
#     over the other 20.
#
# LIKELY MECHANISM (a story, not evidence): Friday is weekly-expiry 0DTE, the
# largest OI on the board, so the afternoon pins. Our book needs movement.
#
# HONEST LIMITS: 23 Fridays is a small day sample; Feb's 2 Fridays were positive,
# so the effect starts around March; and as with V18 the gain concentrates in
# June-July (+$940 / +$1,067) while Feb/Mar/May/Aug are slightly negative.
#
# LOCKSTEP with main.py _tlPassesStrategy(l,'v19') and passesStrategy(l,'v19').
# Evidence: memory research_friday_afternoon_gate.
V19_DOW = 4               # Monday=0 ... Friday=4
# Minute of the ET day from which Friday is blocked. 0 = the whole session.
# Was 660 (11:00) until 2026-08-15; moved to 0 because the Friday-morning bucket is
# a coin flip (n=47, p=0.587) and keeping it was worth only ~$25/mo. Set it back to
# 660 to restore the afternoon-only variant.
V19_AFTER_MIN = 0


def v19_blocks(l):
    """True = V19 says SKIP (Friday). Fail-open on a missing ts."""
    ts = l['ts'] if 'ts' in l else None
    if ts is None:
        return False
    try:
        et = ts.astimezone(ET)
    except (AttributeError, ValueError):
        return False
    if et.weekday() != V19_DOW:
        return False
    return (et.hour * 60 + et.minute) >= V19_AFTER_MIN


def passes_v19(l, gaps):
    """V19 monitoring filter. NOT used by the trade path — portal/analysis only."""
    if not passes_v18(l, gaps):
        return False
    return not v19_blocks(l)


def passes_v16_fri(l, gaps):
    """V16 with Fridays removed — "V16 w/Friday Off" in the portal.

    THIS is the one that mirrors what TSRT actually places once
    REAL_TRADE_NO_FRIDAY=true (armed 2026-08-15). V19 additionally applies V18,
    which is NOT in the trade path, so V19 is a research view and this is the
    live view. Keep them apart: measured over 123 sessions at the live cap,
    Friday-only is +$1,432 while V18 adds a further +$115.

    NOTE the one difference from the real gate: v7 is excluded there (its Fridays
    are profitable), but v7 is GEX Long, which the V16 view only admits when
    GEX_LONG_V3_REAL_TRADE_ENABLED is true — false today, so the two agree.
    If GEX Long is ever re-enabled for the main book this view needs a v7 carve-out
    to stay equal to what TSRT places. See feedback_v16_equals_tsrt_placed.
    """
    if not passes_v16(l, gaps):
        return False
    return not v19_blocks(l)
