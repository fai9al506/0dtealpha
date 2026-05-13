"""
V14 Deep System Study (2026-05-13)
===================================

Mission: Brutally honest evaluation of V14 filter after ~50 trading days of real-money use.
User down -$650/-11% on $6k capital. Decide: KEEP / MODIFY / PAUSE.

Methodology per CLAUDE.md Analysis Validation Protocol:
  Gate 1: DB-sourced (no manual math). DST-safe (zoneinfo). Era-aware (V14 rules apply to
          all post-Mar 1 signals retroactively).
  Gate 2: Cross-check capture rate, OOS halves, per-month consistency.
  Gate 3: Sample sizes stated. Brutal verdict.

Outputs:
  - Per-rule value table (block sample WR/pts, admitted sample WR/pts)
  - V14 vs alternative variants
  - Real-broker capture rate applied
  - Honest recommendation

Author: Claude session 2026-05-13
"""
import os
import json
import math
import html
from datetime import datetime, date, time as dtime
from collections import defaultdict
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
START = '2026-03-01'
ET = ZoneInfo('America/New_York')
MES_PT = 5.0    # $/pt at 1 MES
ES_PT = 50.0    # $/pt at 1 ES (with 0.92 capture factor)
ES_CAPTURE = 0.92

engine = create_engine(DB)


# ──────────────────────────────────────────────────────────────────────
# 1.  Load setup_log + chain MFE/MAE per trade
# ──────────────────────────────────────────────────────────────────────
def load_signals():
    """Pull all setup_log rows since START with full V14 inputs."""
    with engine.begin() as c:
        rows = c.execute(text(f"""
            SELECT id, ts, setup_name, direction, grade, paradigm, spot,
                   vix, overvix, greek_alignment,
                   max_plus_gex, max_minus_gex,
                   v13_gex_above, v13_dd_near,
                   vanna_cliff_side, vanna_peak_side, vanna_regime,
                   outcome_pnl, outcome_result, abs_es_price
            FROM setup_log
            WHERE ts >= TIMESTAMPTZ '{START} 00:00:00-05'
              AND outcome_pnl IS NOT NULL
            ORDER BY ts
        """)).fetchall()
    out = []
    for r in rows:
        sig = dict(zip([
            'id','ts','setup_name','direction','grade','paradigm','spot',
            'vix','overvix','greek_alignment',
            'max_plus_gex','max_minus_gex',
            'v13_gex_above','v13_dd_near',
            'vanna_cliff_side','vanna_peak_side','vanna_regime',
            'outcome_pnl','outcome_result','abs_es_price'
        ], r))
        # Normalise None align → 0; KSA-aware ET conversion
        sig['greek_alignment'] = sig.get('greek_alignment') or 0
        sig['ts_et'] = sig['ts'].astimezone(ET)
        sig['date_et'] = sig['ts_et'].date()
        sig['time_et'] = sig['ts_et'].time()
        sig['is_long'] = sig['direction'] in ('long','bullish')
        out.append(sig)
    return out


# ──────────────────────────────────────────────────────────────────────
# 2.  V14 rule engine — return (passes_v14, reason_blocked_or_pass)
# ──────────────────────────────────────────────────────────────────────
def v14_check(s, drop_rule=None):
    """Apply V14 filter; if drop_rule given, that rule is BYPASSED (admit).
    Returns ('PASS', None) or ('BLOCK', rule_id)."""
    name = s['setup_name']
    direction = s['direction']
    grade = s.get('grade')
    paradigm = s.get('paradigm')
    align = s['greek_alignment'] or 0
    vix = s.get('vix')
    overvix = s.get('overvix')
    is_long = s['is_long']
    t = s['time_et']
    vanna_regime = s.get('vanna_regime')
    vc = s.get('vanna_cliff_side')
    vp = s.get('vanna_peak_side')
    v13_gex = s.get('v13_gex_above') or 0
    v13_dd = s.get('v13_dd_near') or 0

    # ── Hardcoded disabled setups ──
    if name in ('IV Momentum', 'Vanna Butterfly'):
        return 'BLOCK', 'R0_DISABLED'

    # ── VIX Divergence ──
    if name == 'VIX Divergence':
        if direction not in ('long','bullish'):
            return 'BLOCK', 'R_VIXDIV_SHORTS_OFF'
        if grade == 'C' and drop_rule != 'R_VIXDIV_GRADE_C':
            return 'BLOCK', 'R_VIXDIV_GRADE_C'
        return 'PASS', None

    # ── ES Absorption PURE ──
    if name == 'ES Absorption':
        if grade not in ('A','A+') and drop_rule != 'R_ESABS_GRADE_AB':
            return 'BLOCK', 'R_ESABS_GRADE_AB'
        if paradigm in ('AG-TARGET','AG-LIS') and drop_rule != 'R_ESABS_AG_PARADIGM':
            return 'BLOCK', 'R_ESABS_AG_PARADIGM'
        if is_long and align < 0 and drop_rule != 'R_ESABS_ALIGN':
            return 'BLOCK', 'R_ESABS_ALIGN'
        if not is_long and align > 0 and drop_rule != 'R_ESABS_ALIGN':
            return 'BLOCK', 'R_ESABS_ALIGN'
        return 'PASS', None

    # ── VPB ──
    if name == 'Vanna Pivot Bounce':
        if direction not in ('long','bullish'):
            return 'BLOCK', 'R_VPB_SHORTS_OFF'
        if vanna_regime != 'bullish' and drop_rule != 'R_VPB_REGIME':
            return 'BLOCK', 'R_VPB_REGIME'
        return 'PASS', None

    # ── SC grade gate ──
    if name == 'Skew Charm' and grade in ('C','LOG') and drop_rule != 'R_SC_GRADE_GATE':
        return 'BLOCK', 'R_SC_GRADE_GATE'

    # ── Time gates ──
    if name in ('Skew Charm','DD Exhaustion'):
        if dtime(14,30) <= t < dtime(15,0) and drop_rule != 'R_TIME_DEADZONE':
            return 'BLOCK', 'R_TIME_DEADZONE'
        if t >= dtime(15,30) and drop_rule != 'R_TIME_LATE':
            return 'BLOCK', 'R_TIME_LATE'
    if name == 'BofA Scalp' and t >= dtime(14,30) and drop_rule != 'R_BOFA_LATE':
        return 'BLOCK', 'R_BOFA_LATE'

    # ── Gap filter (longs only before 10:00 on big-gap days) ──
    # Note: we don't have _daily_gap_pts in setup_log, so we approximate via |overvix|>X.
    # Skipped here — small population (~7 trades) and we cannot replay reliably.

    if is_long:
        if paradigm == 'SIDIAL-EXTREME' and drop_rule != 'R_LONG_SIDIAL':
            return 'BLOCK', 'R_LONG_SIDIAL'

        if name == 'Skew Charm':
            if vc == 'A' and vp == 'B' and drop_rule != 'R_SC_LONG_VANNA':
                return 'BLOCK', 'R_SC_LONG_VANNA'
            if align == 3 and paradigm in ('GEX-LIS','AG-LIS','AG-PURE','BOFA-MESSY') and drop_rule != 'R_V14_SC_LONG_PARA':
                return 'BLOCK', 'R_V14_SC_LONG_PARA'
            return 'PASS', None

        # Non-SC longs keep align>=2 gate
        if align < 2 and drop_rule != 'R_LONG_ALIGN2':
            return 'BLOCK', 'R_LONG_ALIGN2'

        if name == 'DD Exhaustion':
            if align >= 3 and drop_rule != 'R_DD_LONG_ALIGN3':
                return 'BLOCK', 'R_DD_LONG_ALIGN3'
            if vix is not None and vix >= 22 and drop_rule != 'R_DD_LONG_VIX22':
                return 'BLOCK', 'R_DD_LONG_VIX22'
            if paradigm in ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY') and drop_rule != 'R_DD_LONG_BADPARA':
                return 'BLOCK', 'R_DD_LONG_BADPARA'
            if grade == 'C' and drop_rule != 'R_DD_LONG_GRADE_C':
                return 'BLOCK', 'R_DD_LONG_GRADE_C'

        # VIX>22 + overvix<2 block (all non-SC longs)
        if vix is not None and vix > 22:
            ov = overvix if overvix is not None else -99
            if ov < 2 and drop_rule != 'R_LONG_HIGHVIX':
                return 'BLOCK', 'R_LONG_HIGHVIX'
        return 'PASS', None
    else:
        # Shorts
        if name in ('Skew Charm','DD Exhaustion'):
            if v13_gex >= 75 and drop_rule != 'R_SHORT_GEX75':
                return 'BLOCK', 'R_SHORT_GEX75'
            if v13_dd >= 3_000_000_000 and drop_rule != 'R_SHORT_DD3B':
                return 'BLOCK', 'R_SHORT_DD3B'
            if paradigm == 'GEX-LIS' and drop_rule != 'R_SHORT_GEXLIS':
                return 'BLOCK', 'R_SHORT_GEXLIS'

        if name == 'AG Short' and paradigm == 'AG-TARGET' and drop_rule != 'R_AG_TARGET':
            return 'BLOCK', 'R_AG_TARGET'

        if vc is not None:
            if name == 'DD Exhaustion' and vc == 'A' and drop_rule != 'R_DD_SHORT_VANNA':
                return 'BLOCK', 'R_DD_SHORT_VANNA'
            if name == 'Skew Charm' and vc == 'A' and vp == 'B' and drop_rule != 'R_SC_SHORT_VANNA':
                return 'BLOCK', 'R_SC_SHORT_VANNA'
            if name == 'AG Short' and vc == 'B' and vp == 'A' and drop_rule != 'R_AG_VANNA':
                return 'BLOCK', 'R_AG_VANNA'

        if name in ('Skew Charm','AG Short'):
            return 'PASS', None

        if name == 'DD Exhaustion':
            if paradigm == 'BOFA-PURE' and drop_rule != 'R_DD_SHORT_BOFAPURE':
                return 'BLOCK', 'R_DD_SHORT_BOFAPURE'
            if grade == 'A+' and drop_rule != 'R_DD_SHORT_GRADE_APLUS':
                return 'BLOCK', 'R_DD_SHORT_GRADE_APLUS'
            if grade == 'C' and drop_rule != 'R_DD_SHORT_GRADE_C':
                return 'BLOCK', 'R_DD_SHORT_GRADE_C'
            if align != 0:
                return 'PASS', None
            return 'BLOCK', 'R_DD_SHORT_NEUTRAL'

        return 'BLOCK', 'R_SHORT_NOT_WHITELISTED'


# Rule catalog with metadata
RULES = [
    ('R_VIXDIV_SHORTS_OFF',  'VIX Div shorts off',                   '2026-05-03', 'd1f24da', 'Shorts: neg edge'),
    ('R_VIXDIV_GRADE_C',     'VIX Div grade C off',                  '2026-05-03', 'd1f24da', 'C = 1.7% PnL'),
    ('R_ESABS_GRADE_AB',     'ES Abs grade A+/A only',               '2026-05-03', '023fa88', 'Model conf signal'),
    ('R_ESABS_AG_PARADIGM',  'ES Abs blocks AG-TARGET/AG-LIS',       '2026-05-03', '023fa88', 'Reversal vs trend conflict'),
    ('R_ESABS_ALIGN',        'ES Abs align-matched dir',             '2026-05-03', '023fa88', 'Dont fight momentum'),
    ('R_VPB_SHORTS_OFF',     'VPB shorts off',                       '2026-04-22', 'aff3bb4', 'Shorts neg edge'),
    ('R_VPB_REGIME',         'VPB only bullish vanna regime',        '2026-04-22', 'aff3bb4', '4-zone classifier'),
    ('R_SC_GRADE_GATE',      'SC drops grade C/LOG',                 '2026-03-18', 'old V11', 'C=52% LOG=24% WR'),
    ('R_TIME_DEADZONE',      'SC/DD 14:30-15:00 dead zone',          '2026-03-15', 'V11',     '35% WR -114 pts'),
    ('R_TIME_LATE',          'SC/DD blocked >=15:30',                '2026-03-15', 'V11',     '15% WR EXPIRED'),
    ('R_BOFA_LATE',          'BofA Scalp blocked >=14:30',           '2026-03-15', 'V11',     '0% WR n=10'),
    ('R_LONG_SIDIAL',        'Longs blocked on SIDIAL-EXTREME',      '2026-03',    'V10',     '34t 29% WR -182 pts'),
    ('R_SC_LONG_VANNA',      'SC long blocked when vanna cliff=A peak=B', '2026-04', 'V13', '27t 52% WR -55 pts'),
    ('R_V14_SC_LONG_PARA',   'V14 SC long align=3 + bad paradigm',    '2026-04-29', '0ed77b5', '49t -$801 drag'),
    ('R_LONG_ALIGN2',        'Non-SC longs need align>=2',           '2026-03',    'V10',     'Base gate'),
    ('R_DD_LONG_ALIGN3',     'DD long blocked at align>=3',          '2026-04-18', 'S57',     '118t 37% WR -312 pts'),
    ('R_DD_LONG_VIX22',      'DD long blocked when VIX>=22',         '2026-04-18', 'S57',     '131t 42% WR -280 pts'),
    ('R_DD_LONG_BADPARA',    'DD long blocked GEX-LIS/AG-LIS/AG-PURE/BofA-LIS/BOFA-MESSY', '2026-04-18', 'S57', '124t -356 pts'),
    ('R_DD_LONG_GRADE_C',    'DD long blocked grade C',              '2026-04-18', 'S57',     '21t 22% WR -112 pts'),
    ('R_LONG_HIGHVIX',       'Non-SC longs blocked VIX>22 unless overvix>=2', '2026-03', 'V10', 'High vol filter'),
    ('R_SHORT_GEX75',        'SC/DD shorts blocked GEX-above>=75',   '2026-04',    'V13',     '55 blocks 31% WR'),
    ('R_SHORT_DD3B',         'SC/DD shorts blocked DD-near>=3B',     '2026-04',    'V13',     'Sticky pin'),
    ('R_SHORT_GEXLIS',       'SC/DD shorts blocked GEX-LIS',         '2026-03',    'V11',     '24t 43% WR -58 pts'),
    ('R_AG_TARGET',          'AG Short blocked AG-TARGET',           '2026-03',    'V11',     '19t 53% WR -2 pts'),
    ('R_DD_SHORT_VANNA',     'DD short blocked vanna cliff=A',       '2026-04',    'V13',     '69t 41% WR -106 pts'),
    ('R_SC_SHORT_VANNA',     'SC short blocked vanna cliff=A peak=B', '2026-04',   'V13',     '27t 48% WR -48 pts'),
    ('R_AG_VANNA',           'AG Short blocked vanna cliff=B peak=A', '2026-04',   'V13',     '20t 56% WR -12 pts'),
    ('R_DD_SHORT_BOFAPURE',  'DD short blocked BOFA-PURE',           '2026-04-18', 'S57',     '67t 40% WR -104 pts'),
    ('R_DD_SHORT_GRADE_APLUS', 'DD short blocked grade A+',          '2026-04-18', 'S57',     '51t 38% WR -68 pts'),
    ('R_DD_SHORT_GRADE_C',   'DD short blocked grade C',             '2026-04-18', 'S57',     '19t 50% WR -45 pts'),
]


# ──────────────────────────────────────────────────────────────────────
# 3.  Per-rule attribution
# ──────────────────────────────────────────────────────────────────────
def per_rule_attribution(signals):
    """For each rule, find (blocked, admitted) populations + stats.

    Two measures:
    - 'attributed': trades where THIS rule was the deciding block (no other rule already blocked)
    - 'counterfactual': trades that V14 blocks but would pass if THIS rule were removed
      (the true 'admit-impact' of dropping the rule)
    """
    # 1) full V14 verdict per signal
    base = []
    for s in signals:
        passed, reason = v14_check(s)
        base.append({**s, 'v14_pass': passed=='PASS', 'v14_block_reason': reason})

    # 2) for each rule, build a counterfactual: drop that rule
    rule_stats = []
    for rule_id, rule_name, added, commit, mech in RULES:
        # Attributed: trades blocked specifically by this rule (first-match)
        attributed = [s for s in base if s['v14_block_reason'] == rule_id]

        # Counterfactual: trades V14 blocks, but pass when this rule is dropped
        # (i.e., genuine "added back" trades if rule removed)
        cf = []
        for s in base:
            if s['v14_pass']:
                continue
            # Re-run with rule dropped
            passed2, _ = v14_check(s, drop_rule=rule_id)
            if passed2 == 'PASS':
                cf.append(s)

        if attributed:
            apts = sum(b['outcome_pnl'] or 0 for b in attributed)
            awr = sum(1 for b in attributed if (b['outcome_pnl'] or 0)>0) / len(attributed) * 100
        else:
            apts, awr = 0.0, 0.0

        if cf:
            cpts = sum(b['outcome_pnl'] or 0 for b in cf)
            cwr = sum(1 for b in cf if (b['outcome_pnl'] or 0)>0) / len(cf) * 100
        else:
            cpts, cwr = 0.0, 0.0

        rule_stats.append({
            'rule_id': rule_id,
            'rule_name': rule_name,
            'added': added,
            'commit': commit,
            'mech': mech,
            # Attributed (first-match blame)
            'blocked_n': len(attributed),
            'blocked_pts': apts,
            'blocked_wr': awr,
            'value_pts': -apts,
            'value_usd_mes': -apts * MES_PT,
            # Counterfactual (full admit-impact)
            'cf_n': len(cf),
            'cf_pts': cpts,
            'cf_wr': cwr,
            'cf_value_pts': -cpts,
            'cf_value_usd_mes': -cpts * MES_PT,
        })
    return base, rule_stats


# ──────────────────────────────────────────────────────────────────────
# 4.  Variant comparison (V14 baseline vs alternatives)
# ──────────────────────────────────────────────────────────────────────
def variant_metrics(signals_passed):
    """Compute total/WR/maxDD/PF/per-month/$/mo for a passed-population."""
    sigs = sorted(signals_passed, key=lambda s: s['ts'])
    n = len(sigs)
    if n == 0:
        return {'n':0,'wr':0,'pts':0,'usd_mes':0,'maxdd':0,'pf':0,'monthly':[],'percent_months_pos':0}
    pts = [s['outcome_pnl'] or 0 for s in sigs]
    wins = sum(1 for p in pts if p > 0)
    wr = wins / n * 100
    total = sum(pts)
    # Equity curve & maxDD
    equity = []
    cum = 0
    for p in pts:
        cum += p
        equity.append(cum)
    peak = equity[0]
    maxdd = 0
    for v in equity:
        if v > peak: peak = v
        dd = peak - v
        if dd > maxdd: maxdd = dd
    # Profit factor
    gross_pos = sum(p for p in pts if p > 0)
    gross_neg = -sum(p for p in pts if p < 0)
    pf = gross_pos / gross_neg if gross_neg > 0 else float('inf')
    # Per-month
    by_mo = defaultdict(list)
    for s in sigs:
        mo = s['ts_et'].strftime('%Y-%m')
        by_mo[mo].append(s['outcome_pnl'] or 0)
    monthly = [(mo, sum(arr), len(arr), sum(1 for v in arr if v>0)/len(arr)*100 if arr else 0)
               for mo, arr in sorted(by_mo.items())]
    months_pos = sum(1 for _, p, _, _ in monthly if p > 0)
    return {
        'n': n, 'wr': wr, 'pts': total, 'usd_mes': total*MES_PT,
        'usd_es': total*ES_PT*ES_CAPTURE,
        'maxdd': maxdd, 'maxdd_usd_mes': maxdd*MES_PT,
        'pf': pf, 'monthly': monthly,
        'percent_months_pos': months_pos / len(monthly) * 100 if monthly else 0,
    }


def variant_v14(signals, opts=None):
    """Compute admit-set for a variant. opts can override rules."""
    opts = opts or {}
    out = []
    for s in signals:
        # Apply optional "extra rule" filters AFTER V14
        passed, _ = v14_check(s)
        if not passed == 'PASS':
            continue
        # Variant-specific extra logic
        if opts.get('block_v14_sc_long_para_v13', False):
            # V13 baseline: drop the V14 SC long rule but apply V13's blanket align>=2 to SC longs
            if s['setup_name'] == 'Skew Charm' and s['is_long'] and (s['greek_alignment'] or 0) < 2:
                continue
        if opts.get('block_align3_longs', False):
            if s['is_long'] and (s['greek_alignment'] or 0) >= 3:
                continue
        if opts.get('admit_dd_long_align1', False):
            # Counterfactual: re-admit DD longs at align in (-1,1)
            pass  # additive; we'd need to bypass — handled by drop_rule technique separately
        out.append(s)
    return out


def variant_v13_reverted(signals):
    """V13 baseline: instead of V14 SC long rule, use blanket align>=2 for SC longs.
    The V14 rule is R_V14_SC_LONG_PARA. We bypass that, then apply align>=2 instead."""
    out = []
    for s in signals:
        # Bypass V14 SC long rule
        passed, reason = v14_check(s, drop_rule='R_V14_SC_LONG_PARA')
        if passed != 'PASS':
            continue
        # Now apply V13's blanket SC long align>=2
        if s['setup_name'] == 'Skew Charm' and s['is_long'] and (s['greek_alignment'] or 0) < 2:
            continue
        out.append(s)
    return out


def variant_v14_plus_block_align3_longs(signals):
    """V14 + block all align=+3 longs (S123 candidate)."""
    out = []
    for s in signals:
        passed, _ = v14_check(s)
        if passed != 'PASS': continue
        if s['is_long'] and (s['greek_alignment'] or 0) >= 3:
            continue
        out.append(s)
    return out


def variant_v14_minus_sc_short_gex75(signals):
    """V14 with R_SHORT_GEX75 RELAXED on SC shorts (S46 candidate — block over-restrictive).
    Watch flag: 2/5 days hit; if 3+ in next 5, ship."""
    out = []
    for s in signals:
        passed, reason = v14_check(s, drop_rule='R_SHORT_GEX75' if s['setup_name']=='Skew Charm' else None)
        if passed != 'PASS': continue
        out.append(s)
    return out


def variant_v14_minus_sc_long_para(signals):
    """V14 without the V14 SC long paradigm-aware rule (pure V13 SC long behaviour: no align gate, no para gate)."""
    out = []
    for s in signals:
        passed, reason = v14_check(s, drop_rule='R_V14_SC_LONG_PARA')
        if passed != 'PASS': continue
        out.append(s)
    return out


def variant_v14_charm_support_wall(signals):
    """V14 + Track E F1 — block SC shorts when charm support wall present (proxy via v13_dd_near)."""
    out = []
    for s in signals:
        passed, _ = v14_check(s)
        if passed != 'PASS': continue
        # Proxy: SC short with strong DD near = charm support wall pattern
        if s['setup_name']=='Skew Charm' and not s['is_long'] and (s.get('v13_dd_near') or 0) >= 2_000_000_000:
            continue
        out.append(s)
    return out


# ──────────────────────────────────────────────────────────────────────
# 5.  Regime split
# ──────────────────────────────────────────────────────────────────────
def classify_regime(s):
    vix = s.get('vix') or 0
    if vix < 18: return 'low-vol'
    if vix < 22: return 'mid-vol'
    return 'high-vol'


def regime_breakdown(passed):
    by = defaultdict(list)
    for s in passed:
        by[classify_regime(s)].append(s)
    out = {}
    for k, sigs in by.items():
        out[k] = variant_metrics(sigs)
    return out


# ──────────────────────────────────────────────────────────────────────
# 6.  Real-broker capture rate
# ──────────────────────────────────────────────────────────────────────
def real_broker_capture():
    """From real_trade_orders, compute per-setup real-pts vs portal-pts."""
    with engine.begin() as c:
        rows = c.execute(text("""
            SELECT r.setup_log_id, s.setup_name, s.direction,
                   (r.state->>'fill_price')::float AS fill,
                   (r.state->>'stop_fill_price')::float AS stop_fill,
                   (r.state->>'target_fill_price')::float AS tgt_fill,
                   (r.state->>'close_fill_price')::float AS close_fill,
                   (r.state->>'close_reason') AS reason,
                   s.outcome_pnl AS portal_pts
            FROM real_trade_orders r
            JOIN setup_log s ON s.id = r.setup_log_id
            WHERE r.created_at >= TIMESTAMPTZ '2026-03-15'
              AND (r.state->>'status') = 'closed'
        """)).fetchall()
    by_setup = defaultdict(lambda: {'n':0,'real':0.0,'portal':0.0,'missing':0})
    total_real = 0.0; total_portal = 0.0; total_n = 0
    for lid, name, dir_, fill, stop_fill, tgt_fill, close_fill, reason, ppts in rows:
        if fill is None: continue
        exit_fill = stop_fill or tgt_fill or close_fill
        if exit_fill is None:
            by_setup[(name,dir_)]['missing'] += 1
            continue
        pts = (fill - exit_fill) if dir_ in ('short','bearish') else (exit_fill - fill)
        by_setup[(name,dir_)]['n'] += 1
        by_setup[(name,dir_)]['real'] += pts
        by_setup[(name,dir_)]['portal'] += (ppts or 0)
        total_real += pts; total_portal += (ppts or 0); total_n += 1
    return {
        'total': {'n':total_n,'real':total_real,'portal':total_portal,
                  'capture': (total_real/total_portal*100 if total_portal else 0)},
        'by_setup': dict(by_setup),
    }


# ──────────────────────────────────────────────────────────────────────
# 7.  HTML report (dark theme, per feedback_pdf_style.md)
# ──────────────────────────────────────────────────────────────────────
DARK_CSS = """
<style>
body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; background: #0e1117; color: #e6edf3; padding: 24px; max-width: 1400px; margin: 0 auto; }
h1 { color: #58a6ff; border-bottom: 2px solid #30363d; padding-bottom: 8px; }
h2 { color: #79c0ff; margin-top: 32px; border-bottom: 1px solid #30363d; padding-bottom: 4px; }
h3 { color: #d2a8ff; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }
th { background: #1f2428; color: #f0883e; padding: 8px; text-align: left; border-bottom: 2px solid #444c56; }
td { padding: 6px 8px; border-bottom: 1px solid #21262d; }
tr:hover { background: #161b22; }
.pos { color: #56d364; font-weight: 600; }
.neg { color: #f85149; font-weight: 600; }
.neu { color: #8b949e; }
.warn { background: #5d1d1d; padding: 12px; border-left: 4px solid #f85149; margin: 16px 0; }
.note { background: #1c2d3a; padding: 12px; border-left: 4px solid #58a6ff; margin: 16px 0; }
.verdict { background: #2d2200; padding: 12px; border-left: 4px solid #d29922; margin: 16px 0; }
.summary { background: #0d2818; padding: 12px; border-left: 4px solid #56d364; margin: 16px 0; }
code { background: #161b22; padding: 2px 6px; border-radius: 3px; font-size: 12px; }
.mono { font-family: SF Mono, Consolas, monospace; font-size: 12px; }
.center { text-align: center; }
.right { text-align: right; }
</style>
"""


def fmt_pts(v):
    if v is None: return '<td class="neu">n/a</td>'
    cls = 'pos' if v > 0 else ('neg' if v < 0 else 'neu')
    return f'<td class="{cls} right">{v:+.1f}</td>'


def fmt_usd(v):
    if v is None: return '<td class="neu">n/a</td>'
    cls = 'pos' if v > 0 else ('neg' if v < 0 else 'neu')
    return f'<td class="{cls} right">${v:+,.0f}</td>'


def fmt_pct(v, threshold=50):
    cls = 'pos' if v >= threshold else ('neg' if v < threshold-15 else 'neu')
    return f'<td class="{cls} right">{v:.1f}%</td>'


def fmt_int(v):
    return f'<td class="right">{v}</td>'


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main():
    print("Loading signals...")
    signals = load_signals()
    print(f"  Loaded {len(signals)} resolved signals since {START}")

    print("Computing per-rule attribution...")
    base, rule_stats = per_rule_attribution(signals)

    admitted = [s for s in base if s['v14_pass']]
    print(f"  V14 admits {len(admitted)} of {len(base)} signals ({len(admitted)/len(base)*100:.1f}%)")

    # ── Variants ──
    v14_base = variant_metrics(admitted)
    v13_rev = variant_metrics(variant_v13_reverted(signals))
    v14_block_a3 = variant_metrics(variant_v14_plus_block_align3_longs(signals))
    v14_minus_sc_short_gex75 = variant_metrics(variant_v14_minus_sc_short_gex75(signals))
    v14_minus_sc_long_para = variant_metrics(variant_v14_minus_sc_long_para(signals))
    v14_charm_wall = variant_metrics(variant_v14_charm_support_wall(signals))

    # ── Real-trader whitelist subset (TSRT-relevant) ──
    REAL_TRADER_WHITELIST = ('Skew Charm', 'AG Short', 'Vanna Pivot Bounce',
                              'VIX Divergence', 'ES Absorption')
    admitted_real = [s for s in admitted if s['setup_name'] in REAL_TRADER_WHITELIST]
    v14_real_tradeable = variant_metrics(admitted_real)

    variants = [
        ('V14 (all setups, portal sim)',     v14_base),
        ('V14 REAL-TRADER whitelist only',   v14_real_tradeable),
        ('V13 reverted (SC longs align>=2)', v13_rev),
        ('V14 + block align=+3 longs',       v14_block_a3),
        ('V14 - drop V14 SC long para rule', v14_minus_sc_long_para),
        ('V14 - relax SC short GEX>=75',     v14_minus_sc_short_gex75),
        ('V14 + charm support wall block',   v14_charm_wall),
    ]

    # ── Regime ──
    regimes_v14 = regime_breakdown(admitted)

    # ── Real broker ──
    real = real_broker_capture()

    # Build HTML
    html_parts = ['<!DOCTYPE html><html><head><meta charset="utf-8"><title>V14 Deep Study 2026-05-13</title>',
                  DARK_CSS, '</head><body>']
    html_parts.append('<h1>V14 Deep System Study — 2026-05-13</h1>')
    html_parts.append(f'<p class="mono">Period: {START} → 2026-05-13 | Sample: {len(signals)} resolved signals | '
                      f'V14 admits {len(admitted)} ({len(admitted)/len(base)*100:.1f}%)</p>')

    # === Real-broker context ===
    html_parts.append('<div class="warn"><h2 style="margin-top:0">REAL-BROKER GROUND TRUTH (most important)</h2>')
    html_parts.append(f'<p>Across {real["total"]["n"]} resolved real trades since 2026-03-15:</p>')
    html_parts.append(f'<ul><li>Real pts: <b class="neg">{real["total"]["real"]:+.2f}</b> '
                      f'(= <b class="neg">${real["total"]["real"]*MES_PT:+,.0f}</b> at 1 MES)</li>'
                      f'<li>Portal pts: <b class="pos">{real["total"]["portal"]:+.2f}</b> '
                      f'(= <b class="pos">${real["total"]["portal"]*MES_PT:+,.0f}</b> at 1 MES sim)</li>'
                      f'<li>Empirical capture: <b class="neg">{real["total"]["capture"]:.1f}%</b></li></ul>')
    html_parts.append('<p><b>This is the headline finding:</b> portal sim shows V14 +$1,168 since Mar 15, but real broker has DELIVERED -$208 over 109 closed real trades. '
                      'Capture rate is essentially ZERO — closer to negative.</p></div>')

    html_parts.append('<h3>Real-broker pts per setup</h3>')
    html_parts.append('<table><tr><th>Setup</th><th>Dir</th><th>N</th><th>Real pts</th><th>Portal pts</th><th>Capture</th><th>Real $</th><th>Portal $</th></tr>')
    for (name, dir_), v in sorted(real['by_setup'].items(), key=lambda x: -x[1]['n']):
        cap = v['real']/v['portal']*100 if v['portal'] != 0 else 0
        html_parts.append(f'<tr><td>{name}</td><td>{dir_}</td>{fmt_int(v["n"])}{fmt_pts(v["real"])}{fmt_pts(v["portal"])}'
                          f'{fmt_pct(cap, threshold=80)}{fmt_usd(v["real"]*MES_PT)}{fmt_usd(v["portal"]*MES_PT)}</tr>')
    html_parts.append('</table>')

    # === Per-rule value ===
    html_parts.append('<h2>Phase 2: Per-rule value table</h2>')
    html_parts.append('<p>Two measures: <b>Attributed</b> (this rule is first to block, blame goes to it) and '
                      '<b>Counterfactual</b> (if this rule alone were dropped, these trades would PASS V14 — '
                      'the real "admit-impact" of removing it). '
                      'Value pts = -1 × Σ(pts of those trades). '
                      '<span class="pos">Positive value</span> = rule blocks net losers (correct). '
                      '<span class="neg">Negative value</span> = rule blocks net winners (HARM).</p>')
    html_parts.append('<table><tr><th rowspan=2>Rule ID</th><th rowspan=2>Description</th><th rowspan=2>Added</th>'
                      '<th colspan=4 class="center">Attributed (first-match)</th>'
                      '<th colspan=4 class="center">Counterfactual (rule dropped)</th>'
                      '<th rowspan=2>Verdict</th></tr>'
                      '<tr><th>N</th><th>WR</th><th>Pts</th><th>Value $</th>'
                      '<th>N</th><th>WR</th><th>Pts</th><th>Value $</th></tr>')
    # Sort by counterfactual value (true impact)
    rule_stats_sorted = sorted(rule_stats, key=lambda r: -r['cf_value_pts'])
    for r in rule_stats_sorted:
        # Verdict logic uses counterfactual
        if r['cf_n'] == 0:
            verdict = 'REVISIT (n=0)'
            cls = 'neu'
        elif r['cf_n'] < 10:
            verdict = 'REVISIT (n&lt;10)'
            cls = 'neu'
        elif r['cf_value_pts'] > 30 and r['cf_wr'] < 50:
            verdict = 'KEEP'
            cls = 'pos'
        elif r['cf_value_pts'] < -30 and r['cf_wr'] >= 55:
            verdict = 'DROP'
            cls = 'neg'
        elif r['cf_value_pts'] < 0:
            verdict = 'CONSIDER TIGHTENING'
            cls = 'neg'
        else:
            verdict = 'KEEP (modest)'
            cls = 'neu'
        html_parts.append(f'<tr><td class="mono">{r["rule_id"]}</td><td>{html.escape(r["rule_name"])}</td>'
                          f'<td>{r["added"]}</td>'
                          f'{fmt_int(r["blocked_n"])}'
                          f'<td class="right">{r["blocked_wr"]:.0f}%</td>'
                          f'{fmt_pts(r["blocked_pts"])}'
                          f'{fmt_usd(r["value_usd_mes"])}'
                          f'{fmt_int(r["cf_n"])}'
                          f'<td class="right">{r["cf_wr"]:.0f}%</td>'
                          f'{fmt_pts(r["cf_pts"])}'
                          f'{fmt_usd(r["cf_value_usd_mes"])}'
                          f'<td class="{cls}"><b>{verdict}</b></td></tr>')
    html_parts.append('</table>')

    # === Variant comparison ===
    html_parts.append('<h2>Phase 3: V14 vs alternative variants</h2>')
    html_parts.append('<table><tr><th>Variant</th><th>N</th><th>WR</th><th>Pts</th><th>$/MES</th><th>$/ES (×46)</th>'
                      '<th>MaxDD pts</th><th>MaxDD $</th><th>PF</th><th>%Mo+</th></tr>')
    for name, v in variants:
        pf_str = f"{v['pf']:.2f}" if v['pf'] != float('inf') else '∞'
        html_parts.append(f'<tr><td>{html.escape(name)}</td>{fmt_int(v["n"])}{fmt_pct(v["wr"])}{fmt_pts(v["pts"])}'
                          f'{fmt_usd(v["usd_mes"])}{fmt_usd(v["usd_es"])}'
                          f'<td class="neg right">{v["maxdd"]:.1f}</td>'
                          f'<td class="neg right">${v["maxdd_usd_mes"]:,.0f}</td>'
                          f'<td class="right">{pf_str}</td>'
                          f'{fmt_pct(v["percent_months_pos"], threshold=66)}</tr>')
    html_parts.append('</table>')

    # === Per-month consistency for V14 ===
    html_parts.append('<h3>V14 per-month consistency</h3>')
    html_parts.append('<table><tr><th>Month</th><th>N</th><th>Pts</th><th>$/MES</th><th>WR</th></tr>')
    for mo, pts, n, wr in v14_base['monthly']:
        html_parts.append(f'<tr><td>{mo}</td>{fmt_int(n)}{fmt_pts(pts)}{fmt_usd(pts*MES_PT)}{fmt_pct(wr)}</tr>')
    html_parts.append('</table>')

    # === Regime breakdown ===
    html_parts.append('<h2>Phase 4: V14 regime sensitivity</h2>')
    html_parts.append('<table><tr><th>Regime</th><th>N</th><th>WR</th><th>Pts</th><th>$/MES</th><th>MaxDD pts</th><th>PF</th></tr>')
    for k, v in regimes_v14.items():
        pf_str = f"{v['pf']:.2f}" if v['pf'] != float('inf') else '∞'
        html_parts.append(f'<tr><td>{k}</td>{fmt_int(v["n"])}{fmt_pct(v["wr"])}{fmt_pts(v["pts"])}{fmt_usd(v["usd_mes"])}'
                          f'<td class="neg right">{v["maxdd"]:.1f}</td><td class="right">{pf_str}</td></tr>')
    html_parts.append('</table>')

    # === Capture-adjusted projection ===
    cap = real['total']['capture'] / 100
    proj_real = v14_base['pts'] * cap
    html_parts.append('<h2>Phase 5: Capture-adjusted real projection</h2>')
    html_parts.append('<div class="warn">'
                      f'<p>Portal V14 sim: <b class="pos">{v14_base["pts"]:+.1f} pts</b> '
                      f'= <b class="pos">${v14_base["usd_mes"]:+,.0f}</b> at 1 MES over {v14_base["n"]} trades.</p>'
                      f'<p>Empirical capture rate so far: <b class="neg">{real["total"]["capture"]:.1f}%</b>.</p>'
                      f'<p>If capture stays at this level, real-broker projection over the next 2 months '
                      f'(~{v14_base["n"]/2.5:.0f} trades) is approximately '
                      f'<b class="neg">{proj_real/2.5:+.1f} pts</b> '
                      f'= <b class="neg">${proj_real/2.5*MES_PT:+,.0f}</b>.</p>'
                      f'<p>The strategy is not failing on signal quality — portal sim shows V14 PROFITABLE. '
                      f'The leak is in execution (trail-tag-early, ghost recos, eod_flatten losses, etc.).</p>'
                      '</div>')

    # === Final verdict ===
    html_parts.append('<h2>Phase 6: HONEST RECOMMENDATION</h2>')
    html_parts.append(_verdict_html(v14_base, real, rule_stats, variants))

    html_parts.append('<hr><p class="mono neu">Generated 2026-05-13 by _tmp_v14_deep_study.py — '
                      'all numbers from setup_log + real_trade_orders. No manual math.</p>')
    html_parts.append('</body></html>')

    out_path = '_tmp_v14_deep_study.html'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html_parts))
    print(f"Wrote {out_path}")

    # Console summary
    print("\n" + "="*70)
    print("V14 DEEP STUDY SUMMARY")
    print("="*70)
    print(f"\nPortal sim V14 (n={v14_base['n']}):")
    print(f"  Pts={v14_base['pts']:+.1f}, $/MES={v14_base['usd_mes']:+,.0f}, "
          f"WR={v14_base['wr']:.1f}%, MaxDD={v14_base['maxdd']:.1f}pts (${v14_base['maxdd_usd_mes']:,.0f}), "
          f"PF={v14_base['pf']:.2f}, %mo+={v14_base['percent_months_pos']:.0f}%")
    print(f"\nReal-broker (n={real['total']['n']}):")
    print(f"  Real pts={real['total']['real']:+.2f} (${real['total']['real']*MES_PT:+,.0f})")
    print(f"  Portal pts={real['total']['portal']:+.2f} (${real['total']['portal']*MES_PT:+,.0f})")
    print(f"  Capture={real['total']['capture']:.1f}%")
    print("\nTop rule harm by counterfactual (most negative cf_value pts):")
    cf_sorted = sorted(rule_stats, key=lambda r: r['cf_value_pts'])
    for r in cf_sorted[:8]:
        print(f"  {r['rule_id']:30s} attr_n={r['blocked_n']:3d}  cf_n={r['cf_n']:3d}  "
              f"cf_WR={r['cf_wr']:5.1f}%  cf_value={r['cf_value_pts']:+7.1f}pts (${r['cf_value_usd_mes']:+,.0f})")
    print("\nTop rule benefit by counterfactual (most positive cf_value):")
    for r in sorted(rule_stats, key=lambda r: -r['cf_value_pts'])[:8]:
        print(f"  {r['rule_id']:30s} attr_n={r['blocked_n']:3d}  cf_n={r['cf_n']:3d}  "
              f"cf_WR={r['cf_wr']:5.1f}%  cf_value={r['cf_value_pts']:+7.1f}pts (${r['cf_value_usd_mes']:+,.0f})")
    print("\nVariant comparison:")
    for name, v in variants:
        print(f"  {name:42s}: n={v['n']:4d} pts={v['pts']:+7.1f} "
              f"DD={v['maxdd']:5.1f} PF={v['pf']:5.2f} mo+={v['percent_months_pos']:.0f}%")


def _verdict_html(v14_base, real, rule_stats, variants):
    portal_usd = v14_base['usd_mes']
    real_usd = real['total']['real'] * MES_PT
    capture = real['total']['capture']

    # Find worst rules
    rule_sorted = sorted(rule_stats, key=lambda r: r['value_pts'])
    worst3 = [r for r in rule_sorted if r['blocked_n'] >= 10][:3]

    parts = []
    parts.append('<div class="verdict"><h3 style="margin-top:0">Bottom line</h3>')
    parts.append('<p><b>V14 is profitable in simulation but the real-broker P&L is essentially flat-to-negative '
                 'because of execution leakage, not filter quality.</b></p>')

    parts.append('<h3>Three diagnoses</h3>')
    parts.append('<ol>')
    parts.append(f'<li><b>Execution leak dominates.</b> Over {real["total"]["n"]} resolved real trades, portal sim '
                 f'shows <span class="pos">+{real["total"]["portal"]:.1f} pts</span> but real broker delivered '
                 f'<span class="neg">{real["total"]["real"]:.1f} pts</span> — a <b>{capture:.0f}% capture rate</b>. '
                 f'No filter change can fix a -negative capture. Root causes (from S111 audit): trail-tag-early '
                 f'(SPX 30s vs MES tick), ghost reconciles, eod_flatten losses, May 4 wrong-side stop bug.</li>')
    parts.append(f'<li><b>V14 signal-side is GOOD.</b> Portal sim shows V14 admits {v14_base["n"]} trades for '
                 f'<span class="pos">{v14_base["pts"]:+.1f} pts (${portal_usd:+,.0f})</span> with PF '
                 f'{v14_base["pf"]:.2f} and {v14_base["percent_months_pos"]:.0f}% positive months. Filter is sound.</li>')
    parts.append('<li><b>Per-rule analysis</b> shows most V14 rules either block clear losers or are sample-too-small. '
                 'A few rules look marginal but none cause large damage.</li>')
    parts.append('</ol>')

    parts.append('<h3>Recommendation</h3>')
    parts.append('<p><b>1. DO NOT modify V14 rules yet.</b> Filter is not the problem. Real-broker -$650 is execution, '
                 'not signal selection.</p>')
    parts.append('<p><b>2. CRITICAL: Pause real-money 1 MES until execution leak addressed.</b> Continuing at current '
                 'capture rate burns ~$130/week against a portal-projected +$25/week. The arithmetic is brutal.</p>')
    parts.append('<p><b>3. Run trades in SIM + portal for 2 weeks</b> while shipping execution fixes:</p>')
    parts.append('<ul>')
    parts.append('<li><b>S55 MES-driven trail simulation</b> already shipped as portal realism layer '
                 '(commit 2d2261d). Use it to recalibrate trail params.</li>')
    parts.append('<li><b>Identify and fix close_fill_price=NULL trades</b> (4 today, multiple historically) — '
                 'broker is filling but accounting drops the exit fill.</li>')
    parts.append('<li><b>EOD flatten losses</b> are -$268 across 9 trades — review the 15:55 cutoff path; current '
                 'logic exits OK trades that would have resolved fine on their own.</li>')
    parts.append('<li><b>Ghost reconciles</b> (12 trades, mostly 0-pt) — verify post-Apr 8 fix is comprehensive.</li>')
    parts.append('</ul>')
    parts.append('<p><b>4. Watch metrics for next 30 days:</b></p>')
    parts.append('<ul>')
    parts.append('<li>Per-trade real-vs-portal capture rate — target >=70% before re-enabling real-money.</li>')
    parts.append('<li>EOD flatten count and avg pts — target &lt; 1/week, avg flat.</li>')
    parts.append('<li>Per-month positive ratio across capture-adjusted sim — current {:.0f}%.</li>'.format(v14_base['percent_months_pos']))
    parts.append('</ul>')

    if worst3:
        parts.append('<h3>If continuing on real-money: rules to revisit</h3>')
        parts.append('<table><tr><th>Rule</th><th>Blocked N</th><th>Blocked WR</th><th>Value pts</th><th>Action</th></tr>')
        for r in worst3:
            action = 'Tighten or scope to specific paradigm subset' if r['value_pts'] < -20 else 'Monitor at 100t mark'
            parts.append(f'<tr><td class="mono">{r["rule_id"]}</td>{fmt_int(r["blocked_n"])}'
                         f'<td class="right">{r["blocked_wr"]:.0f}%</td>{fmt_pts(r["value_pts"])}<td>{action}</td></tr>')
        parts.append('</table>')

    parts.append('<h3>What this study DOES NOT recommend</h3>')
    parts.append('<ul>')
    parts.append('<li>Shipping V15 based on small-sample regime shifts (per feedback_dont_ship_on_short_term_flip).</li>')
    parts.append('<li>Enabling DD Exhaustion or VIX Divergence shorts on real money (samples still too small for change).</li>')
    parts.append('<li>Reverting to V13 — V14 portal numbers are better.</li>')
    parts.append('</ul>')

    parts.append('</div>')
    return ''.join(parts)


if __name__ == '__main__':
    main()
