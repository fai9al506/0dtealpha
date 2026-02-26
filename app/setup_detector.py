"""
Trading Setup Detector — self-contained scoring module.
Evaluates GEX Long, AG Short, BofA Scalp, ES Absorption, Paradigm Reversal,
and DD Exhaustion (log-only) setups.
Receives all data as parameters; no imports from main.py.
"""
from collections import deque
from datetime import date, datetime, time as dtime, timedelta
import re
import pytz

NY = pytz.timezone("US/Eastern")

# ── Default settings (exported so main.py can seed its global) ──────────────
DEFAULT_SETUP_SETTINGS = {
    "gex_long_enabled": True,
    "weight_support": 20,
    "weight_upside": 20,
    "weight_floor_cluster": 20,
    "weight_target_cluster": 20,
    "weight_rr": 20,
    "brackets": {
        "support": [
            [5, 100], [10, 75], [15, 50], [20, 25]
        ],
        "upside": [
            [25, 100], [15, 75], [10, 50]
        ],
        "floor_cluster": [
            [3, 100], [7, 75], [10, 50]
        ],
        "target_cluster": [
            [3, 100], [7, 75], [10, 50]
        ],
        "rr": [
            [3, 100], [2, 75], [1.5, 50], [1, 25]
        ],
    },
    "grade_thresholds": {"A+": 90, "A": 75, "A-Entry": 60},
}

# ── Cooldown state (module-level, resets daily) ─────────────────────────────
_cooldown = {
    "last_grade": None,
    "last_gap_to_lis": None,
    "setup_expired": False,
    "last_date": None,
}

_cooldown_ag = {
    "last_grade": None,
    "last_gap_to_lis": None,
    "setup_expired": False,
    "last_date": None,
}

_GRADE_ORDER = {"A+": 3, "A": 2, "A-Entry": 1}


# ── Scoring helpers ─────────────────────────────────────────────────────────

def score_component_max(value, brackets):
    """Score where *lower* input is better (gap, clustering).
    Brackets: [[threshold, score], …] sorted ascending by threshold.
    Returns score of first bracket where value <= threshold, else 0.
    """
    for threshold, score in brackets:
        if value <= threshold:
            return score
    return 0


def score_component_min(value, brackets):
    """Score where *higher* input is better (upside, R:R).
    Brackets: [[threshold, score], …] sorted descending by threshold.
    Returns score of first bracket where value >= threshold, else 0.
    """
    for threshold, score in brackets:
        if value >= threshold:
            return score
    return 0


def compute_grade(composite, thresholds):
    """Map composite score → grade string (best first) or None."""
    ordered = sorted(thresholds.items(), key=lambda kv: kv[1], reverse=True)
    for grade, cutoff in ordered:
        if composite >= cutoff:
            return grade
    return None


def is_first_hour():
    """True when 09:30–10:30 ET."""
    now = datetime.now(NY)
    return dtime(9, 30) <= now.time() <= dtime(10, 30)


# ── Main evaluation ────────────────────────────────────────────────────────

def evaluate_gex_long(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings):
    """
    Evaluate GEX Long setup.  Returns a result dict or None.

    Parameters are plain floats/strings — caller is responsible for parsing.
    """
    if not settings.get("gex_long_enabled", True):
        return None

    # Base conditions
    if not paradigm or "GEX" not in str(paradigm).upper():
        return None
    if spot is None or lis is None or target is None:
        return None
    if max_plus_gex is None or max_minus_gex is None:
        return None
    if spot < lis:
        return None

    gap = spot - lis
    upside_target = target - spot
    upside_gex = max_plus_gex - spot

    if upside_target < 10:
        return None
    if upside_gex < 10:
        return None
    if gap > 20:
        return None

    # ── Component scores ────────────────────────────────────────────────
    brackets = settings.get("brackets", DEFAULT_SETUP_SETTINGS["brackets"])

    support_score = score_component_max(gap, brackets.get("support", DEFAULT_SETUP_SETTINGS["brackets"]["support"]))
    upside = min(upside_target, upside_gex)
    upside_score = score_component_min(upside, brackets.get("upside", DEFAULT_SETUP_SETTINGS["brackets"]["upside"]))
    floor_cluster_score = score_component_max(abs(lis - max_minus_gex), brackets.get("floor_cluster", DEFAULT_SETUP_SETTINGS["brackets"]["floor_cluster"]))
    target_cluster_score = score_component_max(abs(target - max_plus_gex), brackets.get("target_cluster", DEFAULT_SETUP_SETTINGS["brackets"]["target_cluster"]))

    # If minimum upside is excellent (both targets give 15+ pts), target clustering doesn't matter
    # You'll easily hit your 10pt first target regardless of cluster distance
    if upside >= 15:
        target_cluster_score = max(target_cluster_score, 100)  # Override to 100 if upside is great
    elif upside >= 10:
        target_cluster_score = max(target_cluster_score, 75)   # At least 75 if 10+ pts upside

    rr_ratio = upside / gap if gap > 0 else 99
    rr_score = score_component_min(rr_ratio, brackets.get("rr", DEFAULT_SETUP_SETTINGS["brackets"]["rr"]))

    # ── Weighted composite ──────────────────────────────────────────────
    w_support = settings.get("weight_support", 20)
    w_upside = settings.get("weight_upside", 20)
    w_floor = settings.get("weight_floor_cluster", 20)
    w_target = settings.get("weight_target_cluster", 20)
    w_rr = settings.get("weight_rr", 20)
    total_weight = w_support + w_upside + w_floor + w_target + w_rr

    if total_weight == 0:
        return None

    composite = (
        support_score * w_support
        + upside_score * w_upside
        + floor_cluster_score * w_floor
        + target_cluster_score * w_target
        + rr_score * w_rr
    ) / total_weight

    first_hour = is_first_hour()
    if first_hour:
        composite = min(composite + 10, 100)

    # ── Grade ───────────────────────────────────────────────────────────
    thresholds = settings.get("grade_thresholds", DEFAULT_SETUP_SETTINGS["grade_thresholds"])
    grade = compute_grade(composite, thresholds)

    if grade is None:
        return None

    return {
        "setup_name": "GEX Long",
        "direction": "long",
        "grade": grade,
        "score": round(composite, 1),
        "paradigm": str(paradigm),
        "spot": round(spot, 2),
        "lis": round(lis, 2),
        "target": round(target, 2),
        "max_plus_gex": round(max_plus_gex, 2),
        "max_minus_gex": round(max_minus_gex, 2),
        "gap_to_lis": round(gap, 2),
        "upside": round(upside, 2),
        "rr_ratio": round(rr_ratio, 2),
        "first_hour": first_hour,
        "support_score": support_score,
        "upside_score": upside_score,
        "floor_cluster_score": floor_cluster_score,
        "target_cluster_score": target_cluster_score,
        "rr_score": rr_score,
    }


# ── AG Short evaluation ────────────────────────────────────────────────────

def evaluate_ag_short(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings):
    """
    Evaluate AG Short setup.  Returns a result dict or None.
    Mirror of GEX Long with flipped direction inputs.
    """
    if not settings.get("ag_short_enabled", True):
        return None

    # Base conditions
    if not paradigm or "AG" not in str(paradigm).upper():
        return None
    if spot is None or lis is None or target is None:
        return None
    if max_plus_gex is None or max_minus_gex is None:
        return None
    if spot >= lis:
        return None

    gap = lis - spot                   # gap to resistance (above)
    downside_target = spot - target    # room to downside target
    downside_gex = spot - max_minus_gex  # room to -GEX magnet

    if downside_target < 10:
        return None
    if downside_gex < 10:
        return None
    if gap > 20:
        return None

    # ── Component scores (same brackets, mirrored inputs) ────────────
    brackets = settings.get("brackets", DEFAULT_SETUP_SETTINGS["brackets"])

    support_score = score_component_max(gap, brackets.get("support", DEFAULT_SETUP_SETTINGS["brackets"]["support"]))
    downside = min(downside_target, downside_gex)
    upside_score = score_component_min(downside, brackets.get("upside", DEFAULT_SETUP_SETTINGS["brackets"]["upside"]))
    floor_cluster_score = score_component_max(abs(lis - max_plus_gex), brackets.get("floor_cluster", DEFAULT_SETUP_SETTINGS["brackets"]["floor_cluster"]))
    target_cluster_score = score_component_max(abs(target - max_minus_gex), brackets.get("target_cluster", DEFAULT_SETUP_SETTINGS["brackets"]["target_cluster"]))

    # If minimum downside is excellent (both targets give 15+ pts), target clustering doesn't matter
    # You'll easily hit your 10pt first target regardless of cluster distance
    if downside >= 15:
        target_cluster_score = max(target_cluster_score, 100)
    elif downside >= 10:
        target_cluster_score = max(target_cluster_score, 75)

    rr_ratio = downside / gap if gap > 0 else 99
    rr_score = score_component_min(rr_ratio, brackets.get("rr", DEFAULT_SETUP_SETTINGS["brackets"]["rr"]))

    # ── Weighted composite ──────────────────────────────────────────────
    w_support = settings.get("weight_support", 20)
    w_upside = settings.get("weight_upside", 20)
    w_floor = settings.get("weight_floor_cluster", 20)
    w_target = settings.get("weight_target_cluster", 20)
    w_rr = settings.get("weight_rr", 20)
    total_weight = w_support + w_upside + w_floor + w_target + w_rr

    if total_weight == 0:
        return None

    composite = (
        support_score * w_support
        + upside_score * w_upside
        + floor_cluster_score * w_floor
        + target_cluster_score * w_target
        + rr_score * w_rr
    ) / total_weight

    first_hour = is_first_hour()
    if first_hour:
        composite = min(composite + 10, 100)

    # ── Grade ───────────────────────────────────────────────────────────
    thresholds = settings.get("grade_thresholds", DEFAULT_SETUP_SETTINGS["grade_thresholds"])
    grade = compute_grade(composite, thresholds)

    if grade is None:
        return None

    return {
        "setup_name": "AG Short",
        "direction": "short",
        "grade": grade,
        "score": round(composite, 1),
        "paradigm": str(paradigm),
        "spot": round(spot, 2),
        "lis": round(lis, 2),
        "target": round(target, 2),
        "max_plus_gex": round(max_plus_gex, 2),
        "max_minus_gex": round(max_minus_gex, 2),
        "gap_to_lis": round(gap, 2),
        "upside": round(downside, 2),
        "rr_ratio": round(rr_ratio, 2),
        "first_hour": first_hour,
        "support_score": support_score,
        "upside_score": upside_score,
        "floor_cluster_score": floor_cluster_score,
        "target_cluster_score": target_cluster_score,
        "rr_score": rr_score,
    }


# ── Cooldown / notification gate ───────────────────────────────────────────

def should_notify(result):
    """
    Decide whether to fire a Telegram notification for this result.
    Fires once per grade, re-fires on improvement or after expiry→re-form.
    Returns (fire: bool, reason: str) where reason is one of:
      - "new": First detection of the day
      - "grade_upgrade": Grade improved
      - "gap_improvement": Gap improved by >2 pts
      - "reformed": Setup expired and re-formed
      - None: No notification
    """
    global _cooldown

    today = datetime.now(NY).date()
    if _cooldown["last_date"] != today:
        _cooldown = {"last_grade": None, "last_gap_to_lis": None, "setup_expired": False, "last_date": today}

    grade = result["grade"]
    gap = result["gap_to_lis"]
    grade_rank = _GRADE_ORDER.get(grade, 0)
    last_rank = _GRADE_ORDER.get(_cooldown["last_grade"], 0)

    fire = False
    reason = None

    if _cooldown["last_grade"] is None:
        # First detection of the day
        fire = True
        reason = "new"
    elif grade_rank > last_rank:
        # Grade improved
        fire = True
        reason = "grade_upgrade"
    elif _cooldown["last_gap_to_lis"] is not None and (_cooldown["last_gap_to_lis"] - gap) > 2:
        # Gap improved by >2 pts
        fire = True
        reason = "gap_improvement"
    elif _cooldown["setup_expired"]:
        # Setup had expired and re-formed
        fire = True
        reason = "reformed"

    if fire:
        _cooldown["last_grade"] = grade
        _cooldown["last_gap_to_lis"] = gap
        _cooldown["setup_expired"] = False

    return fire, reason


def mark_setup_expired():
    """Call when paradigm loses GEX or gap > 20."""
    _cooldown["setup_expired"] = True
    _cooldown["last_grade"] = None
    _cooldown["last_gap_to_lis"] = None


def should_notify_ag(result):
    """Cooldown gate for AG Short — same logic, separate state. Returns (fire, reason)."""
    global _cooldown_ag

    today = datetime.now(NY).date()
    if _cooldown_ag["last_date"] != today:
        _cooldown_ag = {"last_grade": None, "last_gap_to_lis": None, "setup_expired": False, "last_date": today}

    grade = result["grade"]
    gap = result["gap_to_lis"]
    grade_rank = _GRADE_ORDER.get(grade, 0)
    last_rank = _GRADE_ORDER.get(_cooldown_ag["last_grade"], 0)

    fire = False
    reason = None

    if _cooldown_ag["last_grade"] is None:
        fire = True
        reason = "new"
    elif grade_rank > last_rank:
        fire = True
        reason = "grade_upgrade"
    elif _cooldown_ag["last_gap_to_lis"] is not None and (_cooldown_ag["last_gap_to_lis"] - gap) > 2:
        fire = True
        reason = "gap_improvement"
    elif _cooldown_ag["setup_expired"]:
        fire = True
        reason = "reformed"

    if fire:
        _cooldown_ag["last_grade"] = grade
        _cooldown_ag["last_gap_to_lis"] = gap
        _cooldown_ag["setup_expired"] = False

    return fire, reason


def mark_ag_expired():
    """Call when paradigm loses AG or gap > 20."""
    _cooldown_ag["setup_expired"] = True
    _cooldown_ag["last_grade"] = None
    _cooldown_ag["last_gap_to_lis"] = None


# ── Message formatting ─────────────────────────────────────────────────────

def format_setup_message(result):
    """Format a Telegram HTML message with score breakdown."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")

    msg = f"{grade_emoji} <b>GEX Long Setup — {result['grade']}</b>\n"
    msg += f"Score: <b>{result['score']}</b>/100\n\n"
    msg += f"SPX: {result['spot']:.0f}\n"
    msg += f"Paradigm: {result['paradigm']}\n"
    msg += f"LIS: {result['lis']:.0f}  |  Target: {result['target']:.0f}\n"
    msg += f"+GEX: {result['max_plus_gex']:.0f}  |  −GEX: {result['max_minus_gex']:.0f}\n\n"
    msg += f"Gap to LIS: {result['gap_to_lis']:.1f}\n"
    msg += f"Upside: {result['upside']:.1f}\n"
    msg += f"R:R: {result['rr_ratio']:.1f}x\n\n"
    msg += "<b>Scores:</b>\n"
    msg += f"  Support: {result['support_score']}\n"
    msg += f"  Upside: {result['upside_score']}\n"
    msg += f"  Floor cluster: {result['floor_cluster_score']}\n"
    msg += f"  Target cluster: {result['target_cluster_score']}\n"
    msg += f"  R:R: {result['rr_score']}\n"
    if result["first_hour"]:
        msg += "\n⏰ First hour bonus applied"
    return msg


def format_ag_short_message(result):
    """Format a Telegram HTML message for AG Short with direction-specific labels."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")

    msg = f"{grade_emoji} <b>AG Short Setup — {result['grade']}</b>\n"
    msg += f"Score: <b>{result['score']}</b>/100\n\n"
    msg += f"SPX: {result['spot']:.0f}\n"
    msg += f"Paradigm: {result['paradigm']}\n"
    msg += f"LIS (resistance): {result['lis']:.0f}  |  Target: {result['target']:.0f}\n"
    msg += f"+GEX: {result['max_plus_gex']:.0f}  |  −GEX: {result['max_minus_gex']:.0f}\n\n"
    msg += f"Gap to LIS: {result['gap_to_lis']:.1f}\n"
    msg += f"Downside: {result['upside']:.1f}\n"
    msg += f"R:R: {result['rr_ratio']:.1f}x\n\n"
    msg += "<b>Scores:</b>\n"
    msg += f"  Resistance: {result['support_score']}\n"
    msg += f"  Downside: {result['upside_score']}\n"
    msg += f"  Ceiling cluster: {result['floor_cluster_score']}\n"
    msg += f"  Target cluster: {result['target_cluster_score']}\n"
    msg += f"  R:R: {result['rr_score']}\n"
    if result["first_hour"]:
        msg += "\n⏰ First hour bonus applied"
    return msg


# ── BofA Scalp — LIS rolling buffer ───────────────────────────────────────

# Rolling buffers: store last 12 LIS readings (60 min at 5-min intervals)
# We use 12 so we can check stability over longer windows; minimum 6 for 30 min
_lis_history_upper = deque(maxlen=12)
_lis_history_lower = deque(maxlen=12)
_lis_buffer_last_date = None
_lis_last_paradigm = None


def update_lis_buffer(lis_lower, lis_upper, paradigm=None):
    """
    Called from main.py each time new Volland stats arrive.
    Appends latest LIS values to rolling buffers.
    Resets daily at market open.
    Resets on paradigm change (old AG values would corrupt BofA stability).
    """
    global _lis_buffer_last_date, _lis_last_paradigm
    today = datetime.now(NY).date()
    if _lis_buffer_last_date != today:
        _lis_history_upper.clear()
        _lis_history_lower.clear()
        _lis_buffer_last_date = today
        _lis_last_paradigm = None

    # Reset buffers on paradigm change so old values don't pollute stability
    if paradigm is not None and _lis_last_paradigm is not None:
        if paradigm != _lis_last_paradigm:
            _lis_history_upper.clear()
            _lis_history_lower.clear()
            print(f"[setup] LIS buffer reset: paradigm {_lis_last_paradigm} → {paradigm}", flush=True)
    if paradigm is not None:
        _lis_last_paradigm = paradigm

    if lis_lower is not None:
        _lis_history_lower.append(lis_lower)
    if lis_upper is not None:
        _lis_history_upper.append(lis_upper)


def get_lis_stability(side):
    """
    Check LIS stability for a given side ("lower" or "upper").
    Returns (is_stable, drift, stable_bars) where:
      - is_stable: True if drift <= threshold over recent readings
      - drift: max - min over recent readings
      - stable_bars: count of consecutive stable bars going back

    After paradigm reset (3-5 readings), uses relaxed criteria:
      3 readings within 5pt drift (instead of 6 readings within 3pt).
    """
    buf = _lis_history_lower if side == "lower" else _lis_history_upper
    n = len(buf)

    # Relaxed stability for fresh buffers (just after paradigm reset)
    if 3 <= n < 6:
        recent = list(buf)[-n:]
        drift = max(recent) - min(recent)
        is_stable = drift <= 5
        return is_stable, round(drift, 2), n if is_stable else 0

    if n < 6:
        return False, 999, 0

    # Check drift over last 6 readings
    recent = list(buf)[-6:]
    drift = max(recent) - min(recent)
    is_stable = drift <= 3

    # Count consecutive stable bars (all within 3pts of each other) going back
    stable_bars = 0
    vals = list(buf)
    for i in range(len(vals), 0, -1):
        window = vals[:i]
        if len(window) < 6:
            break
        last_6 = window[-6:]
        if max(last_6) - min(last_6) <= 3:
            stable_bars = len(window)
        else:
            break

    return is_stable, round(drift, 2), max(stable_bars, 6 if is_stable else 0)


# ── BofA Scalp cooldown state ────────────────────────────────────────────

_cooldown_bofa = {
    "last_grade": None,
    "last_gap_to_lis": None,
    "setup_expired": False,
    "last_date": None,
    "last_trade_time_long": None,   # timestamp of last LONG trade close/alert
    "last_trade_time_short": None,  # timestamp of last SHORT trade close/alert
}

BOFA_SIDE_COOLDOWN_MINUTES = 40


# ── BofA Scalp default settings ──────────────────────────────────────────

DEFAULT_BOFA_SCALP_SETTINGS = {
    "bofa_scalp_enabled": True,
    "bofa_max_proximity": 5,
    "bofa_min_lis_width": 15,
    "bofa_stability_bars": 6,
    "bofa_stability_threshold": 3,
    "bofa_time_start": "10:00",
    "bofa_time_end": "15:30",
    "bofa_stop_distance": 12,
    "bofa_target_distance": 10,
    "bofa_max_hold_minutes": 30,
    "bofa_cooldown_minutes": 40,
    "bofa_weight_stability": 20,
    "bofa_weight_width": 20,
    "bofa_weight_charm": 20,
    "bofa_weight_time": 20,
    "bofa_weight_midpoint": 20,
}


# ── BofA Scalp evaluation ────────────────────────────────────────────────

def evaluate_bofa_scalp(spot, paradigm, lis_lower, lis_upper, aggregated_charm, settings):
    """
    Evaluate BofA Scalp setup. Returns a result dict or None.

    Parameters:
      spot: current SPX price
      paradigm: current paradigm string
      lis_lower: lower LIS level
      lis_upper: upper LIS level
      aggregated_charm: aggregated charm value from Volland stats
      settings: setup settings dict (merged with bofa defaults)
    """
    if not settings.get("bofa_scalp_enabled", True):
        return None

    # Base condition 1: Paradigm contains BOFA but not MISSY
    if not paradigm:
        return None
    p_upper = str(paradigm).upper()
    if "BOFA" not in p_upper and "BOFA" not in p_upper.replace("-", ""):
        return None
    if "MISSY" in p_upper:
        return None

    # Base condition 2: Both LIS values must exist
    if spot is None or lis_lower is None or lis_upper is None:
        return None

    # Base condition 3: Time between 10:00 AM and 3:30 PM ET
    now = datetime.now(NY)
    t = now.time()
    if t < dtime(10, 0) or t > dtime(15, 30):
        return None

    # Base condition 4: LIS width >= min_width
    min_width = settings.get("bofa_min_lis_width", 15)
    width = lis_upper - lis_lower
    if width < min_width:
        return None

    # Base condition 5: Spot within proximity of either LIS
    max_prox = settings.get("bofa_max_proximity", 3)
    near_lower = abs(spot - lis_lower) <= max_prox
    near_upper = abs(spot - lis_upper) <= max_prox

    if not near_lower and not near_upper:
        return None

    # Base condition 6: LIS stability check
    lower_stable, lower_drift, lower_bars = get_lis_stability("lower")
    upper_stable, upper_drift, upper_bars = get_lis_stability("upper")

    # Determine direction
    direction = None
    traded_lis = None
    stability_bars = 0

    if near_lower and near_upper:
        # Both near — pick the more stable side
        if lower_stable and upper_stable:
            if lower_bars >= upper_bars:
                direction = "long"
                traded_lis = lis_lower
                stability_bars = lower_bars
            else:
                direction = "short"
                traded_lis = lis_upper
                stability_bars = upper_bars
        elif lower_stable:
            direction = "long"
            traded_lis = lis_lower
            stability_bars = lower_bars
        elif upper_stable:
            direction = "short"
            traded_lis = lis_upper
            stability_bars = upper_bars
        else:
            return None
    elif near_lower:
        if not lower_stable:
            return None
        direction = "long"
        traded_lis = lis_lower
        stability_bars = lower_bars
    elif near_upper:
        if not upper_stable:
            return None
        direction = "short"
        traded_lis = lis_upper
        stability_bars = upper_bars

    if direction is None:
        return None

    # Per-side cooldown check (40 min after last alert on same side)
    side_key = "last_trade_time_long" if direction == "long" else "last_trade_time_short"
    last_trade = _cooldown_bofa.get(side_key)
    cooldown_min = settings.get("bofa_cooldown_minutes", BOFA_SIDE_COOLDOWN_MINUTES)
    if last_trade is not None:
        elapsed = (datetime.now(NY) - last_trade).total_seconds() / 60
        if elapsed < cooldown_min:
            return None

    # ── Component scores ──────────────────────────────────────────────────

    # Component 1: LIS Stability Duration
    if stability_bars >= 12:
        stability_score = 100
    elif stability_bars >= 9:
        stability_score = 75
    elif stability_bars >= 6:
        stability_score = 50
    else:
        stability_score = 0

    # Component 2: LIS Width Ratio
    if width >= 40:
        width_score = 100
    elif width >= 30:
        width_score = 75
    elif width >= 20:
        width_score = 50
    elif width >= 15:
        width_score = 25
    else:
        width_score = 0

    # Component 3: Charm Neutrality
    # Real aggregatedCharm values range ~10M-6B (median ~80M).
    # Lower abs = more neutral = better for scalping.
    charm_abs = abs(aggregated_charm) if aggregated_charm is not None else 999_999_999_999
    if charm_abs <= 50_000_000:
        charm_score = 100
    elif charm_abs <= 100_000_000:
        charm_score = 75
    elif charm_abs <= 250_000_000:
        charm_score = 50
    elif charm_abs <= 500_000_000:
        charm_score = 25
    else:
        charm_score = 0

    # Component 4: Time of Day
    time_decimal = t.hour + t.minute / 60
    if time_decimal >= 14.0:
        time_score = 100
    elif time_decimal >= 12.0:
        time_score = 75
    elif time_decimal >= 11.0:
        time_score = 50
    elif time_decimal >= 10.0:
        time_score = 25
    else:
        time_score = 0

    # Component 5: Distance to Midpoint
    midpoint = (lis_upper + lis_lower) / 2
    target_dist = settings.get("bofa_target_distance", 10)
    if direction == "long":
        target_price = spot + target_dist
        target_vs_mid = midpoint - target_price
    else:
        target_price = spot - target_dist
        target_vs_mid = target_price - midpoint

    if target_vs_mid >= 5:
        midpoint_score = 100
    elif target_vs_mid >= 0:
        midpoint_score = 75
    elif target_vs_mid >= -5:
        midpoint_score = 50
    elif target_vs_mid >= -10:
        midpoint_score = 25
    else:
        midpoint_score = 0

    # ── Weighted composite ────────────────────────────────────────────────
    w_stab = settings.get("bofa_weight_stability", 20)
    w_width = settings.get("bofa_weight_width", 20)
    w_charm = settings.get("bofa_weight_charm", 20)
    w_time = settings.get("bofa_weight_time", 20)
    w_mid = settings.get("bofa_weight_midpoint", 20)
    total_weight = w_stab + w_width + w_charm + w_time + w_mid

    if total_weight == 0:
        return None

    composite = (
        stability_score * w_stab
        + width_score * w_width
        + charm_score * w_charm
        + time_score * w_time
        + midpoint_score * w_mid
    ) / total_weight

    # Dealer O'Clock bonus (2 PM+)
    if time_decimal >= 14.0:
        composite = min(composite + 10, 100)

    composite = max(0, min(100, composite))

    # ── Grade ─────────────────────────────────────────────────────────────
    thresholds = settings.get("grade_thresholds", DEFAULT_SETUP_SETTINGS["grade_thresholds"])
    grade = compute_grade(composite, thresholds)

    if grade is None:
        return None

    # Calculate stop and target levels for the result
    stop_dist = settings.get("bofa_stop_distance", 12)
    if direction == "long":
        stop_level = lis_lower - stop_dist
        target_level = spot + target_dist
    else:
        stop_level = lis_upper + stop_dist
        target_level = spot - target_dist

    gap_to_lis = abs(spot - traded_lis)
    rr_ratio = target_dist / (gap_to_lis + stop_dist) if (gap_to_lis + stop_dist) > 0 else 99

    return {
        "setup_name": "BofA Scalp",
        "direction": direction,
        "grade": grade,
        "score": round(composite, 1),
        "paradigm": str(paradigm),
        "spot": round(spot, 2),
        "lis": round(traded_lis, 2),
        "lis_lower": round(lis_lower, 2),
        "lis_upper": round(lis_upper, 2),
        "target": round(target_level, 2),
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": round(gap_to_lis, 2),
        "upside": round(target_dist, 2),
        "rr_ratio": round(rr_ratio, 2),
        "first_hour": False,
        "support_score": stability_score,
        "upside_score": width_score,
        "floor_cluster_score": charm_score,
        "target_cluster_score": time_score,
        "rr_score": midpoint_score,
        # BofA-specific extras (for display/outcome)
        "bofa_stop_level": round(stop_level, 2),
        "bofa_target_level": round(target_level, 2),
        "bofa_lis_width": round(width, 2),
        "bofa_stability_bars": stability_bars,
        "bofa_max_hold_minutes": settings.get("bofa_max_hold_minutes", 30),
        "bofa_charm": aggregated_charm,
    }


# ── BofA Scalp cooldown / notification ────────────────────────────────────

def should_notify_bofa(result):
    """Cooldown gate for BofA Scalp. Returns (fire, reason)."""
    global _cooldown_bofa

    today = datetime.now(NY).date()
    if _cooldown_bofa["last_date"] != today:
        _cooldown_bofa = {
            "last_grade": None, "last_gap_to_lis": None,
            "setup_expired": False, "last_date": today,
            "last_trade_time_long": None, "last_trade_time_short": None,
        }

    grade = result["grade"]
    gap = result["gap_to_lis"]
    grade_rank = _GRADE_ORDER.get(grade, 0)
    last_rank = _GRADE_ORDER.get(_cooldown_bofa["last_grade"], 0)

    fire = False
    reason = None

    if _cooldown_bofa["last_grade"] is None:
        fire = True
        reason = "new"
    elif grade_rank > last_rank:
        fire = True
        reason = "grade_upgrade"
    elif _cooldown_bofa["setup_expired"]:
        fire = True
        reason = "reformed"

    if fire:
        _cooldown_bofa["last_grade"] = grade
        _cooldown_bofa["last_gap_to_lis"] = gap
        _cooldown_bofa["setup_expired"] = False
        # Record trade time for per-side cooldown
        side_key = "last_trade_time_long" if result["direction"] == "long" else "last_trade_time_short"
        _cooldown_bofa[side_key] = datetime.now(NY)

    return fire, reason


def mark_bofa_expired():
    """Call when paradigm loses BofA or LIS becomes unstable."""
    _cooldown_bofa["setup_expired"] = True
    _cooldown_bofa["last_grade"] = None
    _cooldown_bofa["last_gap_to_lis"] = None


# ── BofA Scalp message formatting ────────────────────────────────────────

def format_bofa_scalp_message(result):
    """Format a Telegram HTML message for BofA Scalp setup."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")
    dir_label = "LONG at Lower LIS" if result["direction"] == "long" else "SHORT at Upper LIS"
    dir_emoji = "🔵" if result["direction"] == "long" else "🔴"

    lis_lo = result.get("lis_lower", 0)
    lis_hi = result.get("lis_upper", 0)
    width = result.get("bofa_lis_width", 0)

    msg = f"{dir_emoji} <b>BofA Scalp — {dir_label}</b>\n"
    msg += f"Grade: {grade_emoji} {result['grade']} (Score: {result['score']})\n"
    msg += "━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 Spot: {result['spot']:.1f}\n"
    msg += f"📏 LIS: {lis_lo:.0f} — {lis_hi:.0f} ({width:.0f}pt width)\n"
    msg += f"🎯 Target: {result.get('bofa_target_level', 0):.1f} (+{result.get('upside', 15):.0f}pts)\n"
    msg += f"🛡 Stop: {result.get('bofa_stop_level', 0):.1f} (-12pts beyond LIS)\n"
    msg += f"⏱ Max Hold: {result.get('bofa_max_hold_minutes', 30)} minutes\n\n"
    msg += "<b>Scoring:</b>\n"
    msg += f"  🧱 Stability: {result['support_score']} ({result.get('bofa_stability_bars', 0) * 5}min stable)\n"
    msg += f"  ↔ Width: {result['upside_score']} ({width:.0f}pt range)\n"
    msg += f"  ⚖ Charm: {result['floor_cluster_score']}\n"
    msg += f"  🕐 Time: {result['target_cluster_score']}\n"
    msg += f"  🎯 Midpoint: {result['rr_score']}\n"

    stab_min = result.get("bofa_stability_bars", 6) * 5
    msg += f"\n⚡ LIS stable for {stab_min} minutes — dealers defending"
    return msg


# ── ES Absorption — defaults and state ─────────────────────────────────────

DEFAULT_ABSORPTION_SETTINGS = {
    "absorption_enabled": True,
    "abs_pivot_left": 2,
    "abs_pivot_right": 2,
    "abs_vol_window": 10,
    "abs_min_vol_ratio": 1.4,
    "abs_cvd_z_min": 0.5,
    "abs_cvd_std_window": 20,
    "abs_cooldown_bars": 10,
    "abs_weight_divergence": 25,
    "abs_weight_volume": 25,
    "abs_weight_dd": 10,
    "abs_weight_paradigm": 10,
    "abs_weight_lis": 10,
    "abs_weight_lis_side": 10,
    "abs_weight_target_dir": 10,
    "abs_max_trigger_dist": 40,
    "abs_zone_min_away": 5,
    "abs_grade_thresholds": {"A+": 75, "A": 55, "B": 35},
}

_cooldown_absorption = {
    "last_bullish_bar": -100,
    "last_bearish_bar": -100,
    "last_bullish_eval_idx": -1,
    "last_bearish_eval_idx": -1,
    "last_date": None,
}

_swing_tracker = {
    "swings": [],          # [{type, price, cvd, volume, bar_idx, ts}, ...]
    "last_type": None,     # "L" or "H" for alternating enforcement
    "last_pivot_idx": -1,  # last bar idx scanned for pivots
}

# Zone tracker: CVD at price zones for level-revisit divergence
# Key = zone_key (int = bar_low // range_pts), value = {cvd, bar_idx, price}
_zone_tracker = {
    "zones": {},           # {str(zone_key): {"cvd": float, "bar_idx": int, "price": float}}
    "last_update_idx": -1, # last bar idx processed for zone tracking
}


def reset_absorption_session():
    """Reset absorption detector state for a new ES session."""
    _cooldown_absorption["last_bullish_bar"] = -100
    _cooldown_absorption["last_bearish_bar"] = -100
    _cooldown_absorption["last_bullish_eval_idx"] = -1
    _cooldown_absorption["last_bearish_eval_idx"] = -1
    _swing_tracker["swings"] = []
    _swing_tracker["last_type"] = None
    _swing_tracker["last_pivot_idx"] = -1
    _zone_tracker["zones"] = {}
    _zone_tracker["last_update_idx"] = -1


def _add_swing(new_swing):
    """Add swing with alternating enforcement and adaptive invalidation.

    Rules:
    - L-H-L-H alternation enforced
    - Same direction: lower low replaces previous low, higher high replaces previous high
    - Same direction but doesn't replace: skip
    """
    swings = _swing_tracker["swings"]
    last_type = _swing_tracker["last_type"]

    if not swings or last_type is None:
        swings.append(new_swing)
        _swing_tracker["last_type"] = new_swing["type"]
        return

    if new_swing["type"] == last_type:
        # Same direction — adaptive invalidation only
        if new_swing["type"] == "L" and new_swing["price"] <= swings[-1]["price"]:
            swings[-1] = new_swing  # lower low replaces
        elif new_swing["type"] == "H" and new_swing["price"] >= swings[-1]["price"]:
            swings[-1] = new_swing  # higher high replaces
        # else: skip — doesn't qualify for replacement
    else:
        # Different direction — alternation satisfied
        swings.append(new_swing)
        _swing_tracker["last_type"] = new_swing["type"]


def _update_swings(closed, pivot_left, pivot_right):
    """Detect pivot highs/lows and update swing tracker.

    Pivot at position i requires:
    - bar[i].low <= all neighbors within pivot_left/pivot_right (for lows)
    - bar[i].high >= all neighbors within pivot_left/pivot_right (for highs)
    Alternating L-H-L-H with adaptive invalidation.
    """
    last_scanned = _swing_tracker["last_pivot_idx"]
    max_pos = len(closed) - 1 - pivot_right
    if max_pos < pivot_left:
        return

    for pos in range(pivot_left, max_pos + 1):
        bar = closed[pos]
        if bar["idx"] <= last_scanned:
            continue

        # Check swing low: bar.low <= all neighbors
        is_low = True
        for j in range(1, pivot_left + 1):
            if bar["low"] > closed[pos - j]["low"]:
                is_low = False
                break
        if is_low:
            for j in range(1, pivot_right + 1):
                if bar["low"] > closed[pos + j]["low"]:
                    is_low = False
                    break

        # Check swing high: bar.high >= all neighbors
        is_high = True
        for j in range(1, pivot_left + 1):
            if bar["high"] < closed[pos - j]["high"]:
                is_high = False
                break
        if is_high:
            for j in range(1, pivot_right + 1):
                if bar["high"] < closed[pos + j]["high"]:
                    is_high = False
                    break

        if not is_low and not is_high:
            continue

        # If both qualify on same bar, prefer alternation
        if is_low and is_high:
            lt = _swing_tracker["last_type"]
            if lt == "L":
                is_low = False
            elif lt == "H":
                is_high = False
            else:
                is_high = False  # default: low first

        if is_low:
            _add_swing({
                "type": "L", "price": bar["low"], "cvd": bar["cvd"],
                "volume": bar["volume"], "bar_idx": bar["idx"],
                "ts": bar.get("ts_end", ""),
            })
        elif is_high:
            _add_swing({
                "type": "H", "price": bar["high"], "cvd": bar["cvd"],
                "volume": bar["volume"], "bar_idx": bar["idx"],
                "ts": bar.get("ts_end", ""),
            })

    # Update last scanned to the highest bar idx we checked
    if max_pos >= pivot_left and closed[max_pos]["idx"] > last_scanned:
        _swing_tracker["last_pivot_idx"] = closed[max_pos]["idx"]


def _divergence_score(cvd_z, price_atr):
    """Score a divergence for logging (NOT used for gating).

    cvd_z: z-score of CVD gap (higher = stronger divergence)
    price_atr: price distance as ATR multiple (higher = more significant swing)
    Returns score 0-100.
    """
    # Base from CVD z-score: z=0.5->17, z=1->33, z=2->67, z=3+->100
    base = min(100, cvd_z / 3.0 * 100)
    # Price ATR multiplier: atr=0->0.5x, atr=1->1.0x, atr=2+->1.5-2.0x
    mult = min(2.0, 0.5 + price_atr * 0.5)
    return min(100, base * mult)


def _update_zone_tracker(closed, range_pts=5.0):
    """Track CVD at each price zone for level-revisit detection.

    Called on every bar (before volume gate) so zones are tracked even during
    quiet periods. Updates all bars EXCEPT the last (trigger) bar — the trigger
    bar is updated after zone-revisit check so we can compare against the
    previous visit's CVD.
    """
    last_idx = _zone_tracker["last_update_idx"]
    zones = _zone_tracker["zones"]

    # Update all bars except the last one (trigger bar updated later)
    for bar in closed[:-1]:
        if bar["idx"] <= last_idx:
            continue
        zone_key = str(int(bar["low"] // range_pts))
        zones[zone_key] = {
            "cvd": bar["cvd"],
            "bar_idx": bar["idx"],
            "price": bar["low"],
        }

    if len(closed) >= 2:
        _zone_tracker["last_update_idx"] = closed[-2]["idx"]

    # Prune old zones (keep only zones visited in last 200 bars)
    if closed:
        cutoff = closed[-1]["idx"] - 200
        stale = [k for k, v in zones.items() if v["bar_idx"] < cutoff]
        for k in stale:
            del zones[k]


def _finalize_zone_tracker(trigger, range_pts=5.0):
    """Update zone tracker with the trigger bar (called after zone-revisit check)."""
    zone_key = str(int(trigger["low"] // range_pts))
    _zone_tracker["zones"][zone_key] = {
        "cvd": trigger["cvd"],
        "bar_idx": trigger["idx"],
        "price": trigger["low"],
    }
    _zone_tracker["last_update_idx"] = trigger["idx"]


def evaluate_absorption(bars, volland_stats, settings, spx_spot=None):
    """
    Evaluate ES Absorption using swing-to-swing CVD divergence.

    Architecture:
    1. Swing Tracker — pivot detection (left=2, right=2, <=/>= comparison),
       alternating L-H-L-H, adaptive invalidation.
    2. Volume Trigger — fire only when bar volume >= 1.4x of 10-bar avg.
    3. Swing-to-Swing Divergence — compare consecutive same-type swings
       (low-vs-low, high-vs-high) for 4 patterns:
       - Sell exhaustion: lower low + higher CVD → BUY
       - Sell absorption: higher low + lower CVD → BUY
       - Buy exhaustion: higher high + lower CVD → SELL
       - Buy absorption: lower high + higher CVD → SELL
       Trigger bar must be within abs_max_trigger_dist (40) bars of recent swing.

    Parameters:
      bars: list of bar dicts with idx, open, high, low, close, volume, cvd, status
      volland_stats: dict with keys paradigm, delta_decay_hedging, lines_in_sand (or None)
      settings: setup settings dict with abs_* keys

    Returns result dict or None.
    """
    if not settings.get("absorption_enabled", True):
        return None

    pivot_left = settings.get("abs_pivot_left", 2)
    pivot_right = settings.get("abs_pivot_right", 2)
    vol_window = settings.get("abs_vol_window", 10)
    min_vol_ratio = settings.get("abs_min_vol_ratio", 1.4)
    cvd_z_min = settings.get("abs_cvd_z_min", 0.5)
    cvd_std_window = settings.get("abs_cvd_std_window", 20)

    min_bars = max(vol_window, cvd_std_window, pivot_left + pivot_right + 1) + 1

    closed = [b for b in bars if b.get("status") == "closed"]
    if len(closed) < min_bars:
        return None

    trigger = closed[-1]
    trigger_idx = trigger["idx"]

    # --- Step 1: Update swing tracker and zone tracker (always, even pre-10AM) ---
    _update_swings(closed, pivot_left, pivot_right)
    _update_zone_tracker(closed)

    # --- No signals before 10:00 AM ET ---
    # Opening bars (9:30-10:00) have inflated volume from premarket→regular
    # session transition, causing false volume triggers. Swings are still
    # tracked above so they're available as reference points after 10:00.
    now_et = datetime.now(NY)
    if now_et.time() < dtime(10, 0):
        return None

    # --- Step 2: Volume gate ---
    recent_vols = [b["volume"] for b in closed[-(vol_window + 1):-1]]
    if not recent_vols:
        return None
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < min_vol_ratio:
        return None

    # --- Step 3: CVD stats for z-score ---
    start_i = max(1, len(closed) - cvd_std_window)
    deltas = [closed[i]["cvd"] - closed[i - 1]["cvd"] for i in range(start_i, len(closed))]
    if len(deltas) < 5:
        return None
    mean_d = sum(deltas) / len(deltas)
    cvd_std = (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5
    if cvd_std < 1:
        cvd_std = 1  # floor to avoid extreme z-scores

    # ATR proxy: avg |close-to-close| over recent bars
    atr_moves = [abs(closed[i]["close"] - closed[i - 1]["close"])
                 for i in range(start_i, len(closed))]
    atr = sum(atr_moves) / len(atr_moves) if atr_moves else 1.0
    if atr < 0.01:
        atr = 0.01

    # --- Step 4: Swing-to-swing divergence scan ---
    # Compare consecutive same-type swings (low-vs-low, high-vs-high).
    # Trigger bar is volume confirmation only — must be within max_trigger_dist
    # bars of the most recent swing in the pair.
    # 4 patterns:
    #   Bullish sell_exhaustion: lower low + higher CVD → sellers giving up
    #   Bullish sell_absorption: higher low + lower CVD → passive buyers absorbing
    #   Bearish buy_exhaustion: higher high + lower CVD → buyers giving up
    #   Bearish buy_absorption: lower high + higher CVD → passive sellers absorbing
    swings = _swing_tracker["swings"]

    max_trigger_dist = settings.get("abs_max_trigger_dist", 40)

    bullish_divs = []
    bearish_divs = []

    # --- Step 4a: Swing-to-swing divergence scan ---
    # Collect same-type swing pairs
    swing_lows = [s for s in swings if s["type"] == "L"]
    swing_highs = [s for s in swings if s["type"] == "H"]

    # Bullish patterns: compare consecutive swing lows
    for i in range(1, len(swing_lows)):
        s1, s2 = swing_lows[i - 1], swing_lows[i]
        # Trigger bar must be within max_trigger_dist of the more recent swing
        if trigger_idx - s2["bar_idx"] > max_trigger_dist:
            continue

        cvd_gap = abs(s2["cvd"] - s1["cvd"])
        cvd_z = cvd_gap / cvd_std
        if cvd_z < cvd_z_min:
            continue

        price_dist = abs(s2["price"] - s1["price"])
        price_atr = price_dist / atr
        score = _divergence_score(cvd_z, price_atr)

        # Sell exhaustion: lower low + higher CVD → sellers exhausted → BUY
        if s2["price"] < s1["price"] and s2["cvd"] > s1["cvd"]:
            bullish_divs.append({
                "swing": s2, "ref_swing": s1, "pattern": "sell_exhaustion",
                "cvd_gap": round(cvd_gap, 1), "cvd_z": round(cvd_z, 2),
                "price_dist": round(price_dist, 2), "price_atr": round(price_atr, 2),
                "score": round(score, 1),
            })
        # Sell absorption: higher low + lower CVD → passive buyers absorbing → BUY
        elif s2["price"] >= s1["price"] and s2["cvd"] < s1["cvd"]:
            bullish_divs.append({
                "swing": s2, "ref_swing": s1, "pattern": "sell_absorption",
                "cvd_gap": round(cvd_gap, 1), "cvd_z": round(cvd_z, 2),
                "price_dist": round(price_dist, 2), "price_atr": round(price_atr, 2),
                "score": round(score, 1),
            })

    # Bearish patterns: compare consecutive swing highs
    for i in range(1, len(swing_highs)):
        s1, s2 = swing_highs[i - 1], swing_highs[i]
        if trigger_idx - s2["bar_idx"] > max_trigger_dist:
            continue

        cvd_gap = abs(s2["cvd"] - s1["cvd"])
        cvd_z = cvd_gap / cvd_std
        if cvd_z < cvd_z_min:
            continue

        price_dist = abs(s2["price"] - s1["price"])
        price_atr = price_dist / atr
        score = _divergence_score(cvd_z, price_atr)

        # Buy exhaustion: higher high + lower CVD → buyers exhausted → SELL
        if s2["price"] > s1["price"] and s2["cvd"] < s1["cvd"]:
            bearish_divs.append({
                "swing": s2, "ref_swing": s1, "pattern": "buy_exhaustion",
                "cvd_gap": round(cvd_gap, 1), "cvd_z": round(cvd_z, 2),
                "price_dist": round(price_dist, 2), "price_atr": round(price_atr, 2),
                "score": round(score, 1),
            })
        # Buy absorption: lower high + higher CVD → passive sellers absorbing → SELL
        elif s2["price"] <= s1["price"] and s2["cvd"] > s1["cvd"]:
            bearish_divs.append({
                "swing": s2, "ref_swing": s1, "pattern": "buy_absorption",
                "cvd_gap": round(cvd_gap, 1), "cvd_z": round(cvd_z, 2),
                "price_dist": round(price_dist, 2), "price_atr": round(price_atr, 2),
                "score": round(score, 1),
            })

    # --- Step 4b: Zone-revisit divergence scan ---
    # Compare CVD at the same price zone between current visit and previous visit.
    # Lower CVD at same zone → selling absorbed by passive buyers → bullish
    # Higher CVD at same zone → buying absorbed by passive sellers → bearish
    zone_min_away = settings.get("abs_zone_min_away", 5)
    zone_key = str(int(trigger["low"] // 5.0))
    zones = _zone_tracker["zones"]

    if zone_key in zones:
        prev = zones[zone_key]
        bars_away = trigger_idx - prev["bar_idx"]
        if bars_away >= zone_min_away:
            cvd_diff = trigger["cvd"] - prev["cvd"]
            cvd_z = abs(cvd_diff) / cvd_std if cvd_std > 0 else 0
            if cvd_z >= cvd_z_min:
                # Score: use CVD z-score with a 1.0x multiplier (no price_atr
                # for zone revisits since price is approximately the same)
                zone_score = round(min(100, cvd_z / 3.0 * 100), 1)
                zone_div = {
                    "swing": {"type": "Z", "price": trigger["low"],
                              "cvd": trigger["cvd"], "bar_idx": trigger_idx},
                    "ref_swing": {"type": "Z", "price": prev["price"],
                                  "cvd": prev["cvd"], "bar_idx": prev["bar_idx"]},
                    "cvd_gap": round(abs(cvd_diff), 1),
                    "cvd_z": round(cvd_z, 2),
                    "price_dist": round(abs(trigger["low"] - prev["price"]), 2),
                    "price_atr": 0.0,
                    "score": zone_score,
                }
                if cvd_diff < 0:
                    # Lower CVD at same level → selling couldn't push price down
                    # → passive buyers absorbed it → bullish
                    zone_div["pattern"] = "zone_sell_absorption"
                    bullish_divs.append(zone_div)
                else:
                    # Higher CVD at same level → buying couldn't push price up
                    # → passive sellers absorbed it → bearish
                    zone_div["pattern"] = "zone_buy_absorption"
                    bearish_divs.append(zone_div)

    # Finalize zone tracker with trigger bar (after check, so next call sees
    # this bar's CVD as the "previous visit" for this zone)
    _finalize_zone_tracker(trigger)

    # Evaluate each direction independently (per-direction gate, not shared)
    best_bull = max(bullish_divs, key=lambda d: d["score"]) if bullish_divs else None
    best_bear = max(bearish_divs, key=lambda d: d["score"]) if bearish_divs else None

    # Skip directions already evaluated for this trigger bar
    if best_bull and trigger_idx <= _cooldown_absorption.get("last_bullish_eval_idx", -1):
        best_bull = None
    if best_bear and trigger_idx <= _cooldown_absorption.get("last_bearish_eval_idx", -1):
        best_bear = None

    if not best_bull and not best_bear:
        return None

    if best_bull and best_bear:
        if best_bull["score"] >= best_bear["score"]:
            direction, best, all_divs = "bullish", best_bull, bullish_divs
        else:
            direction, best, all_divs = "bearish", best_bear, bearish_divs
    elif best_bull:
        direction, best, all_divs = "bullish", best_bull, bullish_divs
    else:
        direction, best, all_divs = "bearish", best_bear, bearish_divs

    # Mark this direction as evaluated for this trigger bar
    if direction == "bullish":
        _cooldown_absorption["last_bullish_eval_idx"] = trigger_idx
    else:
        _cooldown_absorption["last_bearish_eval_idx"] = trigger_idx

    # Sort all divergences by score descending
    all_divs = sorted(all_divs, key=lambda d: d["score"], reverse=True)

    # --- Volume spike score (raw 1-3) ---
    if vol_ratio >= 3.0:
        vol_raw = 3
    elif vol_ratio >= 2.0:
        vol_raw = 2
    else:
        vol_raw = 1

    # --- Volland confluence (raw: dd 0-1, paradigm 0-1, lis 0-2, lis_side 0-2, target_dir 0-2) ---
    dd_raw = 0
    para_raw = 0
    lis_raw = 0
    lis_side_raw = 0   # price below LIS + bullish = bonus (buying at support)
    target_dir_raw = 0  # Volland target confirms signal direction
    lis_val = None
    lis_dist = None
    target_val = None
    paradigm_str = ""
    dd_str = ""

    if volland_stats and volland_stats.get("has_statistics"):
        paradigm_str = (volland_stats.get("paradigm") or "").upper()
        dd_str = volland_stats.get("delta_decay_hedging") or ""
        lis_raw_str = volland_stats.get("lines_in_sand") or ""
        target_raw_str = volland_stats.get("target") or ""

        if direction == "bullish" and "long" in dd_str.lower():
            dd_raw = 1
        elif direction == "bearish" and "short" in dd_str.lower():
            dd_raw = 1

        if direction == "bullish" and "GEX" in paradigm_str:
            para_raw = 1
        elif direction == "bearish" and "AG" in paradigm_str:
            para_raw = 1

        # LIS proximity scoring (existing)
        lis_match = re.search(r'[\d,]+\.?\d*', lis_raw_str.replace(',', ''))
        if lis_match:
            lis_val = float(lis_match.group())
            # LIS is SPX-based — use SPX spot for distance (not ES price)
            price_for_lis = spx_spot if spx_spot else trigger["close"]
            lis_dist = abs(price_for_lis - lis_val)
            if lis_dist <= 5:
                lis_raw = 2
            elif lis_dist <= 15:
                lis_raw = 1

            # LIS side scoring: buying below support or selling above resistance
            # LIS acts as support (GEX/BofA) — below LIS + bullish = strong,
            # above LIS + bearish = strong
            if direction == "bullish" and price_for_lis < lis_val:
                lis_side_raw = 2  # buying at/below support
            elif direction == "bullish" and price_for_lis <= lis_val + 5:
                lis_side_raw = 1  # just above support
            elif direction == "bearish" and price_for_lis > lis_val:
                lis_side_raw = 2  # selling above support (LIS becomes resistance)
            elif direction == "bearish" and price_for_lis >= lis_val - 5:
                lis_side_raw = 1  # just below resistance

        # Target direction scoring: Volland target confirms signal direction
        target_match = re.search(r'[\d,]+\.?\d*', str(target_raw_str).replace('$', '').replace(',', ''))
        if target_match and spx_spot:
            target_val = float(target_match.group())
            target_above = target_val > spx_spot
            if direction == "bullish" and target_above:
                target_dir_raw = 2  # Volland expects higher, we're buying
            elif direction == "bullish" and not target_above:
                target_dir_raw = 0  # Volland expects lower, we're buying (conflict)
            elif direction == "bearish" and not target_above:
                target_dir_raw = 2  # Volland expects lower, we're selling
            elif direction == "bearish" and target_above:
                target_dir_raw = 0  # Volland expects higher, we're selling (conflict)

    # --- Normalize to 0-100 ---
    div_score = best["score"]  # already 0-100 from _divergence_score
    vol_score = {1: 33, 2: 67, 3: 100}.get(vol_raw, 33)
    dd_score = 100 if dd_raw else 0
    para_score = 100 if para_raw else 0
    lis_score = {0: 0, 1: 50, 2: 100}.get(lis_raw, 0)
    lis_side_score = {0: 0, 1: 50, 2: 100}.get(lis_side_raw, 0)
    target_dir_score = {0: 0, 2: 100}.get(target_dir_raw, 0)

    # --- Weighted composite (for grading/logging only — NOT gating) ---
    w_div = settings.get("abs_weight_divergence", 25)
    w_vol = settings.get("abs_weight_volume", 25)
    w_dd = settings.get("abs_weight_dd", 10)
    w_para = settings.get("abs_weight_paradigm", 10)
    w_lis = settings.get("abs_weight_lis", 10)
    w_lis_side = settings.get("abs_weight_lis_side", 10)
    w_target_dir = settings.get("abs_weight_target_dir", 10)
    total_weight = w_div + w_vol + w_dd + w_para + w_lis + w_lis_side + w_target_dir

    if total_weight == 0:
        composite = div_score
    else:
        composite = (
            div_score * w_div
            + vol_score * w_vol
            + dd_score * w_dd
            + para_score * w_para
            + lis_score * w_lis
            + lis_side_score * w_lis_side
            + target_dir_score * w_target_dir
        ) / total_weight

    composite = max(0, min(100, composite))

    # --- Grade (detection-first: always fire, "C" fallback) ---
    abs_thresholds = settings.get("abs_grade_thresholds",
                                  DEFAULT_ABSORPTION_SETTINGS["abs_grade_thresholds"])
    grade = compute_grade(composite, abs_thresholds)
    if grade is None:
        grade = "C"

    pattern = best.get("pattern", "unknown")

    return {
        "setup_name": "ES Absorption",
        "direction": direction,
        "grade": grade,
        "score": round(composite, 1),
        "paradigm": paradigm_str,
        "spot": round(trigger["close"], 2),
        "lis": round(lis_val, 2) if lis_val is not None else None,
        "target": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": round(lis_dist, 2) if lis_dist is not None else None,
        "upside": None,
        "rr_ratio": None,
        "first_hour": False,
        # Pattern classification
        "pattern": pattern,
        # Mapped component scores
        "support_score": div_score,
        "upside_score": vol_score,
        "floor_cluster_score": dd_score,
        "target_cluster_score": para_score,
        "rr_score": lis_score,
        # Bar data
        "bar_idx": trigger_idx,
        "abs_vol_ratio": round(vol_ratio, 1),
        "abs_es_price": round(trigger["close"], 2),
        "cvd": trigger["cvd"],
        "high": trigger["high"],
        "low": trigger["low"],
        "vol_trigger": trigger["volume"],
        # Swing-specific
        "best_swing": best,
        "all_divergences": all_divs,
        "swing_count": len(swings),
        "cvd_std": round(cvd_std, 1),
        "atr": round(atr, 3),
        # Backward-compat raw scores
        "div_raw": round(best["cvd_z"], 2),
        "vol_raw": vol_raw,
        "dd_raw": dd_raw,
        "para_raw": para_raw,
        "lis_raw": lis_raw,
        "lis_side_raw": lis_side_raw,
        "target_dir_raw": target_dir_raw,
        "dd_hedging": dd_str,
        "lis_val": lis_val,
        "lis_dist": round(lis_dist, 1) if lis_dist is not None else None,
        "target_val": target_val,
        "ts": trigger.get("ts_end", ""),
        "lookback": f"zone-revisit" if pattern.startswith("zone_") else f"swing ({len(swings)} tracked)",
    }


def should_notify_absorption(result):
    """Cooldown gate for ES Absorption. Returns (fire, reason).

    Absorption uses bar-index-based cooldown (not time-based like other setups).
    """
    today = datetime.now(NY).date()
    if _cooldown_absorption["last_date"] != today:
        _cooldown_absorption["last_bullish_bar"] = -100
        _cooldown_absorption["last_bearish_bar"] = -100
        _cooldown_absorption["last_date"] = today

    bar_idx = result["bar_idx"]
    direction = result["direction"]
    # evaluate_absorption uses per-direction eval gates (last_bullish_eval_idx,
    # last_bearish_eval_idx) so both directions can fire on the same trigger bar.
    # This function gates on bar distance for notification dedup.
    cooldown = 10  # default; caller can check settings but evaluate already gates

    if direction == "bullish":
        if bar_idx - _cooldown_absorption["last_bullish_bar"] < cooldown:
            return False, None
        _cooldown_absorption["last_bullish_bar"] = bar_idx
    else:
        if bar_idx - _cooldown_absorption["last_bearish_bar"] < cooldown:
            return False, None
        _cooldown_absorption["last_bearish_bar"] = bar_idx

    return True, "new"


def format_absorption_message(result):
    """Format a Telegram HTML message for ES Absorption setup."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result["grade"]
    score = result["score"]
    strong_tag = " STRONG" if grade == "A+" else ""

    pattern = result.get("pattern", "unknown")
    pattern_labels = {
        "sell_exhaustion": "Sell Exhaustion",
        "sell_absorption": "Sell Absorption",
        "buy_exhaustion": "Buy Exhaustion",
        "buy_absorption": "Buy Absorption",
        "zone_sell_absorption": "Zone Sell Absorption",
        "zone_buy_absorption": "Zone Buy Absorption",
    }
    pattern_label = pattern_labels.get(pattern, pattern)

    parts = [
        f"<b>ES ABSORPTION {side_emoji} {side_label} [{grade}] ({score:.0f}/100){strong_tag}</b>",
        f"Pattern: {pattern_label}",
        "",
        f"Price: {result['abs_es_price']:.2f} | CVD: {result['cvd']:+,}",
        f"Vol spike: {result['vol_trigger']:,} ({result['abs_vol_ratio']:.1f}x avg)",
    ]

    best = result.get("best_swing")
    if best:
        sw = best["swing"]
        ref = best.get("ref_swing")
        if ref and ref.get("type") == "Z":
            # Zone-revisit: show zone level and bars between visits
            bars_gap = sw["bar_idx"] - ref["bar_idx"]
            parts.append(f"Zone revisit: {ref['price']:.2f} (bar #{ref['bar_idx']}) -> bar #{sw['bar_idx']} ({bars_gap} bars apart)")
            parts.append(f"  CVD: {ref['cvd']:+,} -> {sw['cvd']:+,} (z={best['cvd_z']:.2f})")
        elif ref:
            parts.append(f"Swing pair: {ref['type']}@{ref['price']:.2f} \u2192 {sw['type']}@{sw['price']:.2f}")
            parts.append(f"  CVD z: {best['cvd_z']:.2f} | Price: {best['price_atr']:.1f}x ATR")
        else:
            parts.append(f"Swing: {sw['type']}@{sw['price']:.2f} (bar #{sw['bar_idx']})")
            parts.append(f"  CVD z: {best['cvd_z']:.2f} | Price: {best['price_atr']:.1f}x ATR")

    all_divs = result.get("all_divergences", [])
    if len(all_divs) > 1:
        parts.append(f"({len(all_divs)} confirming swing pairs)")

    if result.get("dd_raw"):
        parts.append(f"DD Hedging: {result['dd_hedging']} \u2713")
    if result.get("para_raw"):
        parts.append(f"Paradigm: {result['paradigm']} \u2713")
    if result.get("lis_raw") and result.get("lis_val") is not None:
        lis_side_label = ""
        if result.get("lis_side_raw", 0) >= 2:
            lis_side_label = " (right side \u2713)"
        elif result.get("lis_side_raw", 0) >= 1:
            lis_side_label = " (near right side)"
        parts.append(f"Near LIS: {result['lis_val']:.0f} ({result['lis_dist']:.1f} pts){lis_side_label}")
    if result.get("target_dir_raw", 0) >= 2 and result.get("target_val") is not None:
        parts.append(f"Target: {result['target_val']:.0f} (confirms direction \u2713)")

    parts.append("")
    parts.append(
        f"Vol {result['upside_score']:.0f} | DD {result['floor_cluster_score']:.0f} | "
        f"Para {result['target_cluster_score']:.0f} | LIS {result['rr_score']:.0f} | "
        f"Side {result.get('lis_side_raw', 0)} | Tgt {result.get('target_dir_raw', 0)}"
    )
    parts.append(f"Swings: {result.get('swing_count', 0)} tracked")

    return "\n".join(parts)


# ── Paradigm Reversal — defaults and state ─────────────────────────────────

DEFAULT_PARADIGM_REV_SETTINGS = {
    "paradigm_rev_enabled": True,
    "pr_max_flip_age_s": 180,
    "pr_max_lis_distance": 5,
    "pr_cooldown_minutes": 30,
    "pr_weight_proximity": 25,
    "pr_weight_es_volume": 25,
    "pr_weight_charm": 20,
    "pr_weight_dd": 15,
    "pr_weight_time": 15,
    "pr_grade_thresholds": {"A+": 80, "A": 60, "A-Entry": 45},
}

_cooldown_paradigm_rev = {
    "last_long_time": None,
    "last_short_time": None,
    "last_date": None,
}

_paradigm_tracker = {
    "current": None,
    "previous": None,
    "flip_time": None,
}


# ── DD Exhaustion (log-only) — defaults and state ───────────────────────────

DEFAULT_DD_EXHAUST_SETTINGS = {
    "dd_exhaust_enabled": True,
    "dd_shift_threshold": 200_000_000,   # $200M minimum shift
    "dd_cooldown_minutes": 30,
    "dd_stop_pts": 12,                   # initial SL before trailing kicks in
    # Trailing ladder: +7→SL+5, +12→SL+10, +17→SL+15, ... (no fixed target)
    "dd_market_start": "10:00",          # Avoid first 30 min
    "dd_market_end": "15:30",
}

_cooldown_dd_exhaust = {
    "last_long_time": None,
    "last_short_time": None,
    "last_date": None,
}

_dd_tracker = {
    "prev_dd_value": None,
    "prev_dd_date": None,
}


def update_dd_tracker(dd_value):
    """Track DD hedging changes across cycles. Returns shift or None."""
    today = datetime.now(NY).date()
    if _dd_tracker["prev_dd_date"] != today:
        _dd_tracker["prev_dd_value"] = None
        _dd_tracker["prev_dd_date"] = today

    prev = _dd_tracker["prev_dd_value"]
    _dd_tracker["prev_dd_value"] = dd_value
    _dd_tracker["prev_dd_date"] = today

    if prev is None:
        return None
    return dd_value - prev


def evaluate_dd_exhaustion(spot, dd_value, dd_shift, charm, paradigm, settings):
    """
    Evaluate DD Exhaustion setup.
    LONG: dd_shift < -threshold AND charm > 0  (dealers over-hedged bearish, price bounces)
    SHORT: dd_shift > +threshold AND charm < 0  (dealers over-positioned bullish, price fades)
    Returns result dict or None.
    """
    if not settings.get("dd_exhaust_enabled", True):
        return None
    if spot is None or dd_value is None or dd_shift is None or charm is None:
        return None

    # Time window check
    now = datetime.now(NY)
    start_str = settings.get("dd_market_start", "10:00")
    end_str = settings.get("dd_market_end", "15:30")
    try:
        h, m = map(int, start_str.split(":"))
        market_start = dtime(h, m)
        h, m = map(int, end_str.split(":"))
        market_end = dtime(h, m)
    except Exception:
        market_start, market_end = dtime(10, 0), dtime(15, 30)
    if not (market_start <= now.time() <= market_end):
        return None

    threshold = settings.get("dd_shift_threshold", 200_000_000)
    stop_pts = settings.get("dd_stop_pts", 12)  # initial SL before trailing kicks in

    # Signal detection
    if dd_shift < -threshold and charm > 0:
        direction = "long"
    elif dd_shift > threshold and charm < 0:
        direction = "short"
    else:
        return None

    # Trailing stop — no fixed target; SL adjusted live by _check_setup_outcomes
    if direction == "long":
        stop_price = round(spot - stop_pts, 2)
    else:
        stop_price = round(spot + stop_pts, 2)

    # --- Scoring (5 components, max 100) ---
    # Based on backtest (24 trades, 58% WR, PF 1.55x) + Volland Discord research

    # 1. DD Shift magnitude (0-30 pts): bell-curve, sweet spot $500M-$2B
    #    Backtest: $3B+ trades (#3, #21) showed 0 maxFav → regime change not exhaustion
    abs_shift = abs(dd_shift)
    if abs_shift >= 3_000_000_000:
        shift_score = 15       # extreme: possibly regime change, not just exhaustion
    elif abs_shift >= 2_000_000_000:
        shift_score = 25       # strong but may be overdone
    elif abs_shift >= 1_000_000_000:
        shift_score = 30       # sweet spot: most winners cluster here
    elif abs_shift >= 500_000_000:
        shift_score = 20       # moderate, reliable range
    else:
        shift_score = 10       # minimum threshold met ($200M+)

    # 2. Charm alignment strength (0-25 pts): structural anchor for the divergence
    #    WizOps: "all 0DTE vanna, charm, gamma captured in delta decay"
    #    Apollo: "bearish charm won't effect much with elevated skew"
    #    Backtest: charm <20M → unreliable (trade #24: charm=$3M → LOSS)
    abs_charm = abs(charm)
    if abs_charm < 20_000_000:
        charm_score = 0        # too weak — no structural support
    elif abs_charm < 50_000_000:
        charm_score = 8
    elif abs_charm < 100_000_000:
        charm_score = 15
    elif abs_charm < 250_000_000:
        charm_score = 22
    else:
        charm_score = 25       # strong structural conviction

    # 3. Time-of-day (0-15 pts)
    #    WizOps: "0DTE delta decay is more actionable in the middle of the day"
    #    Dark Matter: "Post 2 pm is dealer o'clock"
    #    Backtest: trades #19 (14:21), #20 (14:52) both won
    t = now.time()
    if t >= dtime(14, 0):
        time_score = 15        # dealer o'clock — highest conviction
    elif t >= dtime(11, 30):
        time_score = 12        # mid-day sweet spot
    elif t >= dtime(10, 30):
        time_score = 8         # settling period
    else:
        time_score = 3         # 10:00-10:30 — opening noise

    # 4. Paradigm context (0-15 pts) — actual Volland paradigm names
    #    Apollo: "Paradigm is AG pure BUT 0dte Delta Decay is +3B"
    #    DK5000: "Sidial = consolidation" → DD signals are noise
    para_score = 0
    if paradigm:
        p = str(paradigm).upper()
        if "BOFA" in p or "BOF" in p:
            para_score = 10    # range-bound: exhaustion at extremes fits well
        elif "GEX" in p and "AG" not in p and "ANTI" not in p:
            # GEX-PURE is bullish regime
            para_score = 15 if direction == "long" else 5
        elif "AG" in p or "ANTI" in p:
            # AG is bearish regime
            para_score = 5 if direction == "long" else 15
        elif "SIDIAL" in p:
            para_score = 3     # consolidation, DD signals are noise
        # Messy / unknown: 0

    # 5. Direction bonus (0-15 pts)
    #    Backtest: shorts avg +4.7/trade vs longs +1.3/trade (3.6x better)
    #    Market microstructure: bullish over-positioning creates stronger fades
    dir_score = 15 if direction == "short" else 8

    total_score = shift_score + charm_score + time_score + para_score + dir_score

    # Grade thresholds
    if total_score >= 75:
        grade = "A+"
    elif total_score >= 55:
        grade = "A"
    else:
        grade = "A-Entry"

    return {
        "setup_name": "DD Exhaustion",
        "direction": direction,
        "grade": grade,
        "score": total_score,
        "dd_shift": dd_shift,
        "dd_current": dd_value,
        "charm": charm,
        "paradigm": str(paradigm) if paradigm else None,
        "spot": round(spot, 2),
        "target_price": None,   # trailing stop — no fixed target
        "stop_price": stop_price,
        "lis": None,
        "target": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        # Sub-scores stored in existing columns for breakdown display
        "gap_to_lis": None,
        "upside": None,
        "rr_ratio": None,
        "first_hour": False,
        "support_score": shift_score,       # DD shift magnitude (0-30)
        "upside_score": charm_score,        # charm strength (0-25)
        "floor_cluster_score": time_score,  # time-of-day (0-15)
        "target_cluster_score": para_score, # paradigm context (0-15)
        "rr_score": dir_score,              # direction bonus (0-15)
    }


def should_notify_dd_exhaust(result):
    """30-min cooldown per direction for DD Exhaustion."""
    if result is None:
        return False, None

    direction = result["direction"]
    now = datetime.now(NY)
    today = now.date()

    if _cooldown_dd_exhaust.get("last_date") != today:
        _cooldown_dd_exhaust["last_long_time"] = None
        _cooldown_dd_exhaust["last_short_time"] = None
        _cooldown_dd_exhaust["last_date"] = today

    side_key = "last_long_time" if direction == "long" else "last_short_time"
    last_fire = _cooldown_dd_exhaust.get(side_key)
    if last_fire is not None:
        elapsed = (now - last_fire).total_seconds() / 60
        if elapsed < 30:
            return False, None

    _cooldown_dd_exhaust[side_key] = now
    return True, "new"


def format_dd_exhaustion_message(result):
    """Format Telegram HTML message for DD Exhaustion."""
    direction = result["direction"]
    dir_label = "LONG" if direction == "long" else "SHORT"
    dir_emoji = "\U0001f535" if direction == "long" else "\U0001f534"
    grade = result.get("grade", "?")
    score = result.get("score", 0)

    shift = result["dd_shift"]
    shift_m = shift / 1_000_000
    charm_m = result["charm"] / 1_000_000

    if direction == "long":
        exhaust_label = "bearish exhaust"
    else:
        exhaust_label = "bullish exhaust"

    msg = f"{dir_emoji} <b>DD EXHAUSTION \u2014 {dir_label} ({grade} / {score})</b>\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"DD Shift: ${shift_m:+,.0f}M ({exhaust_label})\n"
    msg += f"Charm: ${charm_m:+,.0f}M ({'bullish \u2713' if result['charm'] > 0 else 'bearish \u2713'})\n"
    msg += f"Paradigm: {result['paradigm'] or 'N/A'}\n"
    msg += f"Entry: ${result['spot']:,.0f} | SL: ${result['stop_price']:,.0f} | Trail: +7\u2192SL+5, +12\u2192SL+10"
    return msg


def format_setup_outcome(trade: dict, result_type: str, pnl: float, elapsed_min: int) -> str:
    """Format a Telegram HTML message for a setup outcome (WIN/LOSS/EXPIRED).

    Args:
        trade: open trade dict with setup_name, direction, spot, target_level, stop_level, grade, result_data
        result_type: "WIN", "LOSS", or "EXPIRED"
        pnl: points gained/lost
        elapsed_min: minutes from entry to resolution
    """
    emoji = {"WIN": "\u2705", "LOSS": "\u274c", "EXPIRED": "\u23f9"}.get(result_type, "\u2753")
    setup_name = trade["setup_name"]
    direction = trade["direction"].upper()
    grade = trade.get("grade", "?")
    spot = trade["spot"]
    r = trade.get("result_data", {})

    is_dd = setup_name == "DD Exhaustion"

    msg = f"{emoji} <b>{setup_name} \u2014 {direction} \u2192 {result_type}</b> ({pnl:+.1f} pts, {elapsed_min} min)\n"

    if result_type == "WIN":
        tgt = trade.get('target_level')
        tgt_str = f"${tgt:,.0f}" if tgt is not None else "trail"
        msg += f"Entry: ${spot:,.0f} | Target: {tgt_str} | Grade: {grade}"
    elif result_type == "LOSS":
        sl = trade.get('stop_level')
        sl_str = f"${sl:,.0f}" if sl is not None else "?"
        msg += f"Entry: ${spot:,.0f} | Stop: {sl_str} | Grade: {grade}"
    else:  # EXPIRED
        close_price = trade.get("close_price") or (spot + pnl if direction == "LONG" else spot - pnl)
        msg += f"Entry: ${spot:,.0f} | Close: ${close_price:,.0f} | Grade: {grade}"

    # DD Exhaustion extra context
    if is_dd and r:
        shift_m = (r.get("dd_shift") or 0) / 1_000_000
        charm_m = (r.get("charm") or 0) / 1_000_000
        msg += f"\nDD Shift: ${shift_m:+,.0f}M | Charm: ${charm_m:+,.0f}M"

    return msg


def format_setup_daily_summary(trades_list: list) -> str:
    """Format EOD summary Telegram message with all resolved trades.

    Args:
        trades_list: list of resolved trade dicts, each with:
            setup_name, direction, grade, pnl, result_type, elapsed_min, ts
    """
    if not trades_list:
        return ""

    wins = sum(1 for t in trades_list if t["result_type"] == "WIN")
    losses = sum(1 for t in trades_list if t["result_type"] == "LOSS")
    expired = sum(1 for t in trades_list if t["result_type"] == "EXPIRED")
    total = len(trades_list)
    net_pnl = sum(t["pnl"] for t in trades_list)
    win_rate = round(100 * wins / total) if total > 0 else 0

    msg = "\U0001f4ca <b>Setup Alerts \u2014 Daily Summary</b>\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"Trades: {total} | Wins: {wins} | Losses: {losses} | Expired: {expired}\n"
    msg += f"Net P&L: {net_pnl:+.1f} pts | Win Rate: {win_rate}%\n\n"

    for t in trades_list:
        emoji = {"WIN": "\u2705", "LOSS": "\u274c", "EXPIRED": "\u23f9"}.get(t["result_type"], "\u2753")
        ts_str = t.get("ts_str", "")
        name = t["setup_name"]
        # Shorten some names for the summary line
        name_short = {"Paradigm Reversal": "Paradigm Rev", "DD Exhaustion": "DD Exhaust"}.get(name, name)
        direction = t["direction"].upper()
        grade = t.get("grade", "?")
        pnl = t["pnl"]
        elapsed = t.get("elapsed_min", 0)
        if t["result_type"] == "EXPIRED":
            elapsed_str = "(expired)"
        else:
            elapsed_str = f"({elapsed} min)"
        msg += f"{emoji} {ts_str} {name_short} {direction} {grade} {pnl:+.1f} pts {elapsed_str}\n"

    msg += "\u2501" * 18
    return msg


def update_paradigm_tracker(paradigm):
    """Track paradigm changes. Call each cycle from check_setups()."""
    if paradigm is None:
        return
    p = str(paradigm)
    if _paradigm_tracker["current"] is None:
        _paradigm_tracker["current"] = p
        return
    if p != _paradigm_tracker["current"]:
        _paradigm_tracker["previous"] = _paradigm_tracker["current"]
        _paradigm_tracker["current"] = p
        _paradigm_tracker["flip_time"] = datetime.now(NY)
        print(f"[setup] paradigm flip: {_paradigm_tracker['previous']} → {p}", flush=True)


def _paradigm_rev_direction(prev, curr):
    """Determine trade direction from paradigm transition.
    AG → anything = LONG (bearish regime ending)
    GEX → anything = SHORT (bullish regime ending)
    BofA → GEX = LONG, BofA → AG = SHORT
    """
    prev_u = (prev or "").upper()
    curr_u = (curr or "").upper()
    if "AG" in prev_u and "AG" not in curr_u:
        return "long"
    if "GEX" in prev_u and "GEX" not in curr_u:
        return "short"
    if "BOFA" in prev_u:
        if "GEX" in curr_u:
            return "long"
        if "AG" in curr_u:
            return "short"
    return None


def evaluate_paradigm_reversal(spot, paradigm, lis_lower, lis_upper,
                               aggregated_charm, dd_hedging, es_bars, settings):
    """
    Evaluate Paradigm Reversal setup. Returns a result dict or None.

    Fires when paradigm just flipped, price is near LIS, and ES volume confirms.
    """
    if not settings.get("paradigm_rev_enabled", True):
        return None

    # Must have a recent flip
    flip_time = _paradigm_tracker.get("flip_time")
    prev = _paradigm_tracker.get("previous")
    curr = _paradigm_tracker.get("current")
    if flip_time is None or prev is None:
        return None

    max_age = settings.get("pr_max_flip_age_s", 180)
    age = (datetime.now(NY) - flip_time).total_seconds()
    if age > max_age:
        return None

    # Determine direction
    direction = _paradigm_rev_direction(prev, curr)
    if direction is None:
        return None

    # Per-direction cooldown
    now = datetime.now(NY)
    today = now.date()
    if _cooldown_paradigm_rev.get("last_date") != today:
        _cooldown_paradigm_rev["last_long_time"] = None
        _cooldown_paradigm_rev["last_short_time"] = None
        _cooldown_paradigm_rev["last_date"] = today

    cooldown_min = settings.get("pr_cooldown_minutes", 30)
    side_key = "last_long_time" if direction == "long" else "last_short_time"
    last_fire = _cooldown_paradigm_rev.get(side_key)
    if last_fire is not None:
        elapsed = (now - last_fire).total_seconds() / 60
        if elapsed < cooldown_min:
            return None

    # Need LIS values
    if spot is None or lis_lower is None or lis_upper is None:
        return None

    # Price must be near LIS zone
    max_dist = settings.get("pr_max_lis_distance", 5)
    dist_lower = abs(spot - lis_lower)
    dist_upper = abs(spot - lis_upper)
    min_dist = min(dist_lower, dist_upper)
    if min_dist > max_dist:
        return None

    # Determine which LIS we're near
    near_lis = lis_lower if dist_lower <= dist_upper else lis_upper

    # Time check — no signals before 10:00
    t = now.time()
    if t < dtime(10, 0) or t > dtime(15, 45):
        return None

    # ── Component scores ──────────────────────────────────────────────────

    # 1. LIS Proximity (closer = better)
    if min_dist <= 1:
        prox_score = 100
    elif min_dist <= 2:
        prox_score = 85
    elif min_dist <= 3:
        prox_score = 70
    elif min_dist <= 5:
        prox_score = 50
    else:
        prox_score = 0

    # 2. ES Volume ratio (recent bars volume vs average)
    vol_ratio = 0
    vol_score = 25  # default if no ES bars
    if es_bars and len(es_bars) >= 3:
        recent_vols = [b.get("bar_volume", 0) for b in es_bars[-5:]]
        older_vols = [b.get("bar_volume", 0) for b in es_bars[:-5]] if len(es_bars) > 5 else recent_vols
        avg_vol = sum(older_vols) / len(older_vols) if older_vols else 1
        if avg_vol > 0:
            vol_ratio = sum(recent_vols) / len(recent_vols) / avg_vol
        if vol_ratio >= 2.0:
            vol_score = 100
        elif vol_ratio >= 1.5:
            vol_score = 75
        elif vol_ratio >= 1.2:
            vol_score = 50
        else:
            vol_score = 25

    # 3. Charm alignment
    charm_score = 0
    if aggregated_charm is not None:
        if direction == "long" and aggregated_charm > 0:
            charm_score = 100
        elif direction == "short" and aggregated_charm < 0:
            charm_score = 100
        elif abs(aggregated_charm) < 50_000_000:
            charm_score = 50  # neutral charm is OK

    # 4. DD Hedging alignment
    dd_score = 0
    dd_str = str(dd_hedging or "").lower()
    if direction == "long" and "long" in dd_str:
        dd_score = 100
    elif direction == "short" and "short" in dd_str:
        dd_score = 100

    # 5. Time of Day
    time_decimal = t.hour + t.minute / 60
    if time_decimal >= 14.0:
        time_score = 100
    elif time_decimal >= 12.0:
        time_score = 75
    elif time_decimal >= 11.0:
        time_score = 50
    elif time_decimal >= 10.0:
        time_score = 25
    else:
        time_score = 0

    # ── Weighted composite ────────────────────────────────────────────────
    w_prox = settings.get("pr_weight_proximity", 25)
    w_vol = settings.get("pr_weight_es_volume", 25)
    w_charm = settings.get("pr_weight_charm", 20)
    w_dd = settings.get("pr_weight_dd", 15)
    w_time = settings.get("pr_weight_time", 15)
    total_weight = w_prox + w_vol + w_charm + w_dd + w_time

    if total_weight == 0:
        return None

    composite = (
        prox_score * w_prox
        + vol_score * w_vol
        + charm_score * w_charm
        + dd_score * w_dd
        + time_score * w_time
    ) / total_weight

    composite = max(0, min(100, composite))

    # ── Grade ─────────────────────────────────────────────────────────────
    thresholds = settings.get("pr_grade_thresholds",
                              DEFAULT_PARADIGM_REV_SETTINGS["pr_grade_thresholds"])
    grade = compute_grade(composite, thresholds)
    if grade is None:
        return None

    # Record cooldown
    _cooldown_paradigm_rev[side_key] = now

    lis_width = lis_upper - lis_lower

    return {
        "setup_name": "Paradigm Reversal",
        "direction": direction,
        "grade": grade,
        "score": round(composite, 1),
        "paradigm": str(paradigm),
        "spot": round(spot, 2),
        "lis": round(near_lis, 2),
        "lis_lower": round(lis_lower, 2),
        "lis_upper": round(lis_upper, 2),
        "target": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": round(min_dist, 2),
        "upside": None,
        "rr_ratio": None,
        "first_hour": False,
        "support_score": prox_score,
        "upside_score": vol_score,
        "floor_cluster_score": charm_score,
        "target_cluster_score": dd_score,
        "rr_score": time_score,
        # Paradigm Reversal specifics
        "pr_prev_paradigm": prev,
        "pr_curr_paradigm": curr,
        "pr_flip_age_s": round(age, 0),
        "pr_vol_ratio": round(vol_ratio, 2),
        "pr_lis_width": round(lis_width, 2),
        "pr_dd_hedging": str(dd_hedging or ""),
        "pr_charm": aggregated_charm,
    }


def should_notify_paradigm_rev(result):
    """Always fire if evaluate returned non-None (cooldown already in evaluate)."""
    if result is None:
        return False, None
    return True, "new"


def format_paradigm_reversal_message(result):
    """Format a Telegram HTML message for Paradigm Reversal."""
    dir_emoji = "\U0001f535" if result["direction"] == "long" else "\U0001f534"
    dir_label = "LONG" if result["direction"] == "long" else "SHORT"
    grade_emoji = {"A+": "\U0001f7e2", "A": "\U0001f535", "A-Entry": "\U0001f7e1"}.get(result["grade"], "\u26aa")

    prev = result.get("pr_prev_paradigm", "?")
    curr = result.get("pr_curr_paradigm", "?")

    msg = f"{dir_emoji} <b>Paradigm Reversal — {dir_label}</b>\n"
    msg += f"Grade: {grade_emoji} {result['grade']} (Score: {result['score']})\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"\U0001f504 {prev} \u2192 {curr}\n"
    msg += f"\U0001f4cd Spot: {result['spot']:.1f}\n"
    msg += f"\U0001f4cf LIS: {result.get('lis_lower', 0):.0f} \u2014 {result.get('lis_upper', 0):.0f}"
    msg += f" ({result.get('pr_lis_width', 0):.0f}pt width)\n"
    msg += f"Gap to LIS: {result['gap_to_lis']:.1f}pts\n\n"
    msg += "<b>Scoring:</b>\n"
    msg += f"  \U0001f4cd Proximity: {result['support_score']}\n"
    msg += f"  \U0001f4ca ES Volume: {result['upside_score']}"
    if result.get("pr_vol_ratio"):
        msg += f" ({result['pr_vol_ratio']:.1f}x)"
    msg += "\n"
    msg += f"  \u2696 Charm: {result['floor_cluster_score']}\n"
    msg += f"  \U0001f6e1 DD Hedging: {result['target_cluster_score']}"
    if result.get("pr_dd_hedging"):
        msg += f" ({result['pr_dd_hedging']})"
    msg += "\n"
    msg += f"  \U0001f552 Time: {result['rr_score']}\n"
    msg += f"\n\u26a1 Paradigm just flipped {int(result.get('pr_flip_age_s', 0))}s ago"
    return msg


# ── Main entry point ───────────────────────────────────────────────────────

def check_setups(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings,
                 lis_lower=None, lis_upper=None, aggregated_charm=None,
                 dd_hedging=None, es_bars=None,
                 dd_value=None, dd_shift=None):
    """
    Main entry point called from main.py.
    Returns a list of result wrappers (each has keys: result, notify, notify_reason, message).
    List may be empty.

    Kwargs:
      lis_lower, lis_upper: parsed LIS low/high values (BofA Scalp, Paradigm Reversal)
      aggregated_charm: aggregated charm from Volland stats
      dd_hedging: delta decay hedging string from Volland stats (Paradigm Reversal)
      es_bars: list of recent ES 1-min bar dicts from DB (Paradigm Reversal)
      dd_value: numeric DD hedging value (DD Exhaustion)
      dd_shift: change in DD hedging from previous cycle (DD Exhaustion)
    """
    results = []

    # ── Track paradigm changes (must be before setup evaluations) ──
    update_paradigm_tracker(paradigm)

    # ── GEX Long cooldown expiry tracking ──
    if paradigm and "GEX" not in str(paradigm).upper():
        mark_setup_expired()
    elif spot is not None and lis is not None and (spot - lis) > 20:
        mark_setup_expired()

    gex_result = evaluate_gex_long(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings)
    if gex_result is not None:
        notify, reason = should_notify(gex_result)
        results.append({
            "result": gex_result,
            "notify": notify,
            "notify_reason": reason,
            "message": format_setup_message(gex_result),
        })

    # ── AG Short cooldown expiry tracking ──
    if paradigm and "AG" not in str(paradigm).upper():
        mark_ag_expired()
    elif spot is not None and lis is not None and (lis - spot) > 20:
        mark_ag_expired()

    ag_result = evaluate_ag_short(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings)
    if ag_result is not None:
        notify_ag, reason_ag = should_notify_ag(ag_result)
        results.append({
            "result": ag_result,
            "notify": notify_ag,
            "notify_reason": reason_ag,
            "message": format_ag_short_message(ag_result),
        })

    # ── BofA Scalp ──
    p_str = str(paradigm).upper() if paradigm else ""
    if "BOFA" not in p_str:
        mark_bofa_expired()

    # Update LIS buffer (called here so it happens on every check cycle)
    if lis_lower is not None and lis_upper is not None:
        update_lis_buffer(lis_lower, lis_upper, paradigm=str(paradigm) if paradigm else None)

    bofa_result = evaluate_bofa_scalp(spot, paradigm, lis_lower, lis_upper, aggregated_charm, settings)
    if bofa_result is not None:
        notify_bofa, reason_bofa = should_notify_bofa(bofa_result)
        results.append({
            "result": bofa_result,
            "notify": notify_bofa,
            "notify_reason": reason_bofa,
            "message": format_bofa_scalp_message(bofa_result),
        })

    # ── Paradigm Reversal ──
    pr_result = evaluate_paradigm_reversal(
        spot, paradigm, lis_lower, lis_upper,
        aggregated_charm, dd_hedging, es_bars, settings,
    )
    if pr_result is not None:
        notify_pr, reason_pr = should_notify_paradigm_rev(pr_result)
        results.append({
            "result": pr_result,
            "notify": notify_pr,
            "notify_reason": reason_pr,
            "message": format_paradigm_reversal_message(pr_result),
        })

    # ── DD Exhaustion (log-only) ──
    dd_exhaust_result = evaluate_dd_exhaustion(
        spot, dd_value, dd_shift, aggregated_charm, paradigm, settings,
    )
    if dd_exhaust_result is not None:
        notify_dde, reason_dde = should_notify_dd_exhaust(dd_exhaust_result)
        results.append({
            "result": dd_exhaust_result,
            "notify": notify_dde,
            "notify_reason": reason_dde,
            "message": format_dd_exhaustion_message(dd_exhaust_result),
        })

    return results


# ── Cooldown persistence helpers ──────────────────────────────────────────

def export_cooldowns() -> dict:
    """Export all cooldown state as a serializable dict."""
    import copy
    def _serialize(d):
        out = {}
        for k, v in d.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif isinstance(v, date):
                out[k] = v.isoformat()
            else:
                out[k] = v
        return out
    return {
        "gex": _serialize(_cooldown),
        "ag": _serialize(_cooldown_ag),
        "bofa": _serialize(_cooldown_bofa),
        "absorption": _serialize(_cooldown_absorption),
        "swing_tracker": copy.deepcopy(_swing_tracker),
        "zone_tracker": copy.deepcopy(_zone_tracker),
        "paradigm_rev": _serialize(_cooldown_paradigm_rev),
        "paradigm_tracker": _serialize(_paradigm_tracker),
        "dd_exhaust": _serialize(_cooldown_dd_exhaust),
        "dd_tracker": _serialize(_dd_tracker),
    }

def import_cooldowns(data: dict):
    """Restore cooldown state from a dict (loaded from DB)."""
    global _cooldown, _cooldown_ag, _cooldown_bofa, _cooldown_absorption
    if not data:
        return
    def _deserialize(d, has_datetimes=False, dt_keys=None):
        out = dict(d)
        if has_datetimes:
            keys = dt_keys or ("last_trade_time_long", "last_trade_time_short")
            for k in keys:
                if out.get(k) and isinstance(out[k], str):
                    try:
                        out[k] = datetime.fromisoformat(out[k])
                    except Exception:
                        out[k] = None
        return out
    if "gex" in data:
        _cooldown.update(_deserialize(data["gex"]))
    if "ag" in data:
        _cooldown_ag.update(_deserialize(data["ag"]))
    if "bofa" in data:
        _cooldown_bofa.update(_deserialize(data["bofa"], has_datetimes=True))
    if "absorption" in data:
        _cooldown_absorption.update(_deserialize(data["absorption"]))
    if "swing_tracker" in data:
        _swing_tracker.update(data["swing_tracker"])
    if "zone_tracker" in data:
        _zone_tracker.update(data["zone_tracker"])
    if "paradigm_rev" in data:
        _cooldown_paradigm_rev.update(_deserialize(
            data["paradigm_rev"], has_datetimes=True,
            dt_keys=("last_long_time", "last_short_time")))
    if "paradigm_tracker" in data:
        restored = _deserialize(data["paradigm_tracker"], has_datetimes=True,
                                dt_keys=("flip_time",))
        _paradigm_tracker.update(restored)
    if "dd_exhaust" in data:
        _cooldown_dd_exhaust.update(_deserialize(
            data["dd_exhaust"], has_datetimes=True,
            dt_keys=("last_long_time", "last_short_time")))
    if "dd_tracker" in data:
        _dd_tracker.update(data["dd_tracker"])
