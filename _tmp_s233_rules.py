# -*- coding: utf-8 -*-
"""S233 — V16 filter decomposed into individually-toggleable named rules.

`passes(l, gaps, off=frozenset())` with off=empty MUST be bit-identical to
app.live_filter.passes_v16 (verified by _tmp_s233_verify.py).

Each `off` id removes exactly one blocking rule so its marginal value can be measured.
GEX Long is EXCLUDED from the study universe (real-trade flag is false; Tasks S230).
"""
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")

# id -> (short description, what it blocks)
RULES = {
    "DD_SHORT":        "DD Exhaustion SHORTS blocked entirely (V16 top)",
    "AG_TARGET":       "AG Short blocked when paradigm = AG-TARGET",
    "GEXTARGET_PM":    "longs blocked after 13:00 when paradigm = GEX-TARGET (SC/DD/ESAbs)",
    "VIXDIV_GRADEC":   "VIX Divergence grade C blocked",
    "VIXDIV_GEXPARA":  "VIX Divergence blocked unless paradigm starts with GEX-",
    "VPB_GRADEB":      "Vanna Pivot Bounce blocked unless grade == B",
    "VPB_HOUR11":      "Vanna Pivot Bounce blocked during the 11:00 hour",
    "DDLONG_SIDIAL":   "DD long blocked 14:00-15:00 when paradigm = SIDIAL-EXTREME",
    "DDLONG_ALIGN_LO": "DD long blocked when alignment < 0",
    "DDLONG_ALIGN_HI": "DD long blocked when alignment >= 3",
    "DDLONG_VIX22":    "DD long blocked when VIX >= 22",
    "DDLONG_PARA":     "DD long blocked on GEX-LIS/AG-LIS/AG-PURE/BofA-LIS/BOFA-MESSY",
    "DDLONG_GRADEC":   "DD long blocked when grade = C",
    "GAP_LONG":        "longs blocked before 10:00 when |gap| > 30 pts",
    "SC_GRADE":        "Skew Charm grade C / LOG blocked",
    "V11_DEADZONE":    "SC/DD blocked 14:30-15:00 (charm dead zone)",
    "V11_LATE":        "SC/DD blocked from 15:30",
    "V13BULL":         "SC/DD SHORTS blocked when GEX-above >= 75% or DD-near >= $3B",
    "V13VANNA":        "SC/AG/DD shorts blocked on adverse vanna cliff/peak sides",
    "V13DDQ":          "DD quality gate (dead code while DD shorts are blocked)",
    "ESABS_SHORT":     "ES Absorption SHORTS blocked (S229, 2026-07-27)",
    "ESABS_GRADE":     "ES Absorption blocked unless grade A / A+",
    "ESABS_PARA":      "ES Absorption blocked on AG-TARGET / AG-LIS",
    "ESABS_LATE":      "ES Absorption blocked from 15:45",
    "ESABS_ALIGN":     "ES Absorption long blocked when alignment < 0 (short when > 0)",
    "ESABS_SHORT_PM":  "ES Absorption shorts blocked from 14:00 (dead while ESABS_SHORT is on)",
    "SC_LONG_GEXLIS":  "Skew Charm LONG blocked when paradigm = GEX-LIS",
    "SC_LONG_OPEX":    "Skew Charm LONG blocked on opex Friday",
    "AG_OPEX":         "AG Short blocked on opex Friday",
    "SIDIAL_PM":       "longs blocked 14:00-15:00 when paradigm = SIDIAL-EXTREME",
    "SC_LONG_A3PARA":  "SC long blocked when align=3 and paradigm in GEX-LIS/AG-LIS/AG-PURE/BOFA-MESSY",
    "LONG_ALIGN2":     "non-SC longs blocked when alignment < 2",
    "LONG_VIX22":      "longs blocked when VIX > 22 and overvix < 2",
    "SCDD_SHORT_GEXLIS": "SC/DD SHORTS blocked when paradigm = GEX-LIS",
    "SHORT_WHITELIST": "shorts other than SC / AG Short blocked (fallthrough)",
}

WHITELIST = ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption",
             "DD Exhaustion", "VIX Divergence")


def passes(l, gaps, off=frozenset()):
    """Returns (bool_passes, blocking_rule_id_or_None). off = set of rule ids to DISABLE."""
    def on(r):
        return r not in off

    sn = l['setup_name'] or ''
    align = l['greek_alignment'] if l['greek_alignment'] is not None else 0
    isLong = l['direction'] in ('long', 'bullish')
    para = l['paradigm']; grade = l['grade']; ts = l['ts']
    et = ts.astimezone(ET) if ts else None
    mins = (et.hour * 60 + et.minute) if et else None
    vix = l['vix'] or 0

    if sn not in WHITELIST:
        return False, "NOT_WHITELISTED"
    if on("DD_SHORT") and sn == 'DD Exhaustion' and not isLong:
        return False, "DD_SHORT"
    if on("AG_TARGET") and sn == 'AG Short' and para == 'AG-TARGET':
        return False, "AG_TARGET"
    if on("GEXTARGET_PM") and isLong and para == 'GEX-TARGET' and et and et.hour >= 13 \
            and sn in ('Skew Charm', 'DD Exhaustion', 'ES Absorption'):
        return False, "GEXTARGET_PM"

    if sn == 'VIX Divergence':
        if not isLong:
            return False, "VIXDIV_LONGONLY"
        if on("VIXDIV_GRADEC") and grade == 'C':
            return False, "VIXDIV_GRADEC"
        if on("VIXDIV_GEXPARA") and not (para and para.startswith('GEX-')):
            return False, "VIXDIV_GEXPARA"
        return True, None

    if sn == 'Vanna Pivot Bounce':
        if not isLong:
            return False, "VPB_LONGONLY"
        if on("VPB_GRADEB") and grade != 'B':
            return False, "VPB_GRADEB"
        if on("VPB_HOUR11") and et and et.hour == 11:
            return False, "VPB_HOUR11"
        return True, None

    if sn == 'DD Exhaustion' and isLong:
        if on("DDLONG_SIDIAL") and para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900:
            return False, "DDLONG_SIDIAL"
        if on("DDLONG_ALIGN_LO") and align < 0:
            return False, "DDLONG_ALIGN_LO"
        if on("DDLONG_ALIGN_HI") and align >= 3:
            return False, "DDLONG_ALIGN_HI"
        if on("DDLONG_VIX22") and vix >= 22:
            return False, "DDLONG_VIX22"
        if on("DDLONG_PARA") and para in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BofA-LIS', 'BOFA-MESSY'):
            return False, "DDLONG_PARA"
        if on("DDLONG_GRADEC") and grade == 'C':
            return False, "DDLONG_GRADEC"
        return True, None

    # ── gap filter (longs only) ──
    if on("GAP_LONG") and ts and isLong:
        g = gaps.get(et.date().isoformat())
        if g is not None and abs(g) > 30 and mins is not None and mins < 600:
            return False, "GAP_LONG"

    if on("SC_GRADE") and sn == 'Skew Charm' and grade in ('C', 'LOG'):
        return False, "SC_GRADE"

    # ── v11 time rules ──
    if mins is not None:
        if on("V11_DEADZONE") and sn in ('Skew Charm', 'DD Exhaustion') and 870 <= mins < 900:
            return False, "V11_DEADZONE"
        if on("V11_LATE") and sn in ('Skew Charm', 'DD Exhaustion') and mins >= 930:
            return False, "V11_LATE"

    # ── v13 short-quality rules ──
    if on("V13BULL") and not isLong and sn in ('Skew Charm', 'DD Exhaustion'):
        if (l['v13_gex_above'] or 0) >= 75 or (l['v13_dd_near'] or 0) >= 3000000000:
            return False, "V13BULL"
    if on("V13VANNA") and not isLong:
        cs = l['vanna_cliff_side']; ps = l['vanna_peak_side']
        if cs is not None:
            if sn in ('DD Exhaustion', 'Skew Charm') and cs == 'A' and ps == 'B':
                return False, "V13VANNA"
            if sn == 'AG Short' and cs == 'B' and ps == 'A':
                return False, "V13VANNA"
    if on("V13DDQ") and sn == 'DD Exhaustion':
        if isLong:
            if align >= 3 or vix >= 22 or grade == 'C':
                return False, "V13DDQ"
            if para in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BofA-LIS', 'BOFA-MESSY'):
                return False, "V13DDQ"
        else:
            if para == 'BOFA-PURE' or grade in ('A+', 'C'):
                return False, "V13DDQ"

    if sn == 'ES Absorption':
        if on("ESABS_SHORT") and not isLong:
            return False, "ESABS_SHORT"
        if on("ESABS_GRADE") and grade not in ('A', 'A+'):
            return False, "ESABS_GRADE"
        if on("ESABS_PARA") and para in ('AG-TARGET', 'AG-LIS'):
            return False, "ESABS_PARA"
        if on("ESABS_LATE") and mins is not None and mins >= 945:
            return False, "ESABS_LATE"
        if on("ESABS_ALIGN") and isLong and align < 0:
            return False, "ESABS_ALIGN"
        if on("ESABS_ALIGN") and not isLong and align > 0:
            return False, "ESABS_ALIGN"
        if not isLong and mins is not None and mins >= 840 and on("ESABS_SHORT_PM"):
            return False, "ESABS_SHORT_PM"
        return True, None

    def isOpex():
        return bool(et) and et.weekday() == 4 and 15 <= et.day <= 21

    if on("SC_LONG_GEXLIS") and sn == 'Skew Charm' and isLong and para == 'GEX-LIS':
        return False, "SC_LONG_GEXLIS"
    if on("SC_LONG_OPEX") and sn == 'Skew Charm' and isLong and isOpex():
        return False, "SC_LONG_OPEX"
    if on("AG_OPEX") and sn == 'AG Short' and isOpex():
        return False, "AG_OPEX"

    # ── v10 base (V14) ──
    if isLong:
        if sn == 'Skew Charm':
            if on("SIDIAL_PM") and para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900:
                return False, "SIDIAL_PM"
            if on("SC_LONG_A3PARA") and align == 3 and para in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BOFA-MESSY'):
                return False, "SC_LONG_A3PARA"
            return True, None
        if on("SIDIAL_PM") and para == 'SIDIAL-EXTREME' and mins is not None and 840 <= mins < 900:
            return False, "SIDIAL_PM"
        if on("LONG_ALIGN2") and align < 2:
            return False, "LONG_ALIGN2"
        if on("LONG_VIX22") and vix > 22 and (l['overvix'] if l['overvix'] is not None else -99) < 2:
            return False, "LONG_VIX22"
        return True, None

    # shorts
    if on("SCDD_SHORT_GEXLIS") and sn in ('Skew Charm', 'DD Exhaustion') and para == 'GEX-LIS':
        return False, "SCDD_SHORT_GEXLIS"
    if sn in ('Skew Charm', 'AG Short'):
        return True, None
    if sn == 'DD Exhaustion' and align != 0:
        return True, None
    if not on("SHORT_WHITELIST"):
        return True, None
    return False, "SHORT_WHITELIST"
