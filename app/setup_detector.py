"""
Trading Setup Detector — self-contained scoring module.
Evaluates GEX Long, AG Short, BofA Scalp, and ES Absorption setups.
Receives all data as parameters; no imports from main.py.
"""
from collections import deque
from datetime import datetime, time as dtime, timedelta
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


def update_lis_buffer(lis_lower, lis_upper):
    """
    Called from main.py each time new Volland stats arrive.
    Appends latest LIS values to rolling buffers.
    Resets daily at market open.
    """
    global _lis_buffer_last_date
    today = datetime.now(NY).date()
    if _lis_buffer_last_date != today:
        _lis_history_upper.clear()
        _lis_history_lower.clear()
        _lis_buffer_last_date = today

    if lis_lower is not None:
        _lis_history_lower.append(lis_lower)
    if lis_upper is not None:
        _lis_history_upper.append(lis_upper)


def get_lis_stability(side):
    """
    Check LIS stability for a given side ("lower" or "upper").
    Returns (is_stable, drift, stable_bars) where:
      - is_stable: True if drift <= 3 over last 6 readings
      - drift: max - min over last 6 readings
      - stable_bars: count of consecutive stable bars going back
    """
    buf = _lis_history_lower if side == "lower" else _lis_history_upper
    if len(buf) < 6:
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
    "bofa_max_proximity": 3,
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
    "abs_lookback": 8,
    "abs_vol_window": 20,
    "abs_min_vol_ratio": 1.5,
    "abs_cooldown_bars": 10,
    "abs_weight_divergence": 25,
    "abs_weight_volume": 25,
    "abs_weight_dd": 15,
    "abs_weight_paradigm": 15,
    "abs_weight_lis": 20,
    "abs_grade_thresholds": {"A+": 75, "A": 55, "B": 35},
}

_cooldown_absorption = {
    "last_bullish_bar": -100,
    "last_bearish_bar": -100,
    "last_checked_idx": -1,
    "last_date": None,
}


def reset_absorption_session():
    """Reset absorption detector state for a new ES session."""
    _cooldown_absorption["last_bullish_bar"] = -100
    _cooldown_absorption["last_bearish_bar"] = -100
    _cooldown_absorption["last_checked_idx"] = -1


def evaluate_absorption(bars, volland_stats, settings):
    """
    Evaluate ES Absorption setup on completed range bars.

    Parameters:
      bars: list of bar dicts (must have idx, open, high, low, close, volume, cvd, status)
      volland_stats: dict with keys paradigm, delta_decay_hedging, lines_in_sand (or None)
      settings: setup settings dict with abs_* keys

    Returns result dict or None.
    """
    if not settings.get("absorption_enabled", True):
        return None

    lookback = settings.get("abs_lookback", 8)
    vol_window = settings.get("abs_vol_window", 20)
    min_vol_ratio = settings.get("abs_min_vol_ratio", 1.5)
    min_bars = vol_window + lookback

    if len(bars) < min_bars:
        return None

    closed = [b for b in bars if b.get("status") == "closed"]
    if len(closed) < min_bars:
        return None

    trigger = closed[-1]
    trigger_idx = trigger["idx"]

    # Skip if already checked this bar
    if trigger_idx <= _cooldown_absorption["last_checked_idx"]:
        return None
    _cooldown_absorption["last_checked_idx"] = trigger_idx

    # --- Volume gate ---
    recent_vols = [b["volume"] for b in closed[-(vol_window + 1):-1]]
    if not recent_vols:
        return None
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < min_vol_ratio:
        return None

    # --- Divergence over lookback window ---
    window = closed[-(lookback + 1):]
    lows = [b["low"] for b in window]
    highs = [b["high"] for b in window]
    cvds = [b["cvd"] for b in window]

    cvd_start, cvd_end = cvds[0], cvds[-1]
    cvd_slope = cvd_end - cvd_start
    cvd_range = max(cvds) - min(cvds)
    if cvd_range == 0:
        return None

    price_low_start, price_low_end = lows[0], lows[-1]
    price_high_start, price_high_end = highs[0], highs[-1]
    price_range = max(highs) - min(lows)
    if price_range == 0:
        return None

    cvd_norm = cvd_slope / cvd_range
    price_low_norm = (price_low_end - price_low_start) / price_range
    price_high_norm = (price_high_end - price_high_start) / price_range

    # Detect direction and raw divergence score (0-4)
    direction = None
    div_raw = 0

    if cvd_norm < -0.15:
        gap = price_low_norm - cvd_norm
        if gap > 0.2:
            direction = "bullish"
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    if cvd_norm > 0.15 and direction is None:
        gap = cvd_norm - price_high_norm
        if gap > 0.2:
            direction = "bearish"
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    if direction is None:
        return None

    # --- Volume spike score (raw 1-3) ---
    if vol_ratio >= 3.0:
        vol_raw = 3
    elif vol_ratio >= 2.0:
        vol_raw = 2
    else:
        vol_raw = 1

    # --- Volland confluence (raw: dd 0-1, paradigm 0-1, lis 0-2) ---
    dd_raw = 0
    para_raw = 0
    lis_raw = 0
    lis_val = None
    lis_dist = None
    paradigm_str = ""
    dd_str = ""

    if volland_stats and volland_stats.get("has_statistics"):
        paradigm_str = (volland_stats.get("paradigm") or "").upper()
        dd_str = volland_stats.get("delta_decay_hedging") or ""
        lis_raw_str = volland_stats.get("lines_in_sand") or ""

        if direction == "bullish" and "long" in dd_str.lower():
            dd_raw = 1
        elif direction == "bearish" and "short" in dd_str.lower():
            dd_raw = 1

        if direction == "bullish" and "GEX" in paradigm_str:
            para_raw = 1
        elif direction == "bearish" and "AG" in paradigm_str:
            para_raw = 1

        lis_match = re.search(r'[\d,]+\.?\d*', lis_raw_str.replace(',', ''))
        if lis_match:
            lis_val = float(lis_match.group())
            lis_dist = abs(trigger["close"] - lis_val)
            if lis_dist <= 5:
                lis_raw = 2
            elif lis_dist <= 15:
                lis_raw = 1

    # --- Normalize to 0-100 ---
    div_score = {0: 0, 1: 25, 2: 50, 3: 75, 4: 100}.get(div_raw, 0)
    vol_score = {1: 33, 2: 67, 3: 100}.get(vol_raw, 33)
    dd_score = 100 if dd_raw else 0
    para_score = 100 if para_raw else 0
    lis_score = {0: 0, 1: 50, 2: 100}.get(lis_raw, 0)

    # --- Weighted composite ---
    w_div = settings.get("abs_weight_divergence", 25)
    w_vol = settings.get("abs_weight_volume", 25)
    w_dd = settings.get("abs_weight_dd", 15)
    w_para = settings.get("abs_weight_paradigm", 15)
    w_lis = settings.get("abs_weight_lis", 20)
    total_weight = w_div + w_vol + w_dd + w_para + w_lis

    if total_weight == 0:
        return None

    composite = (
        div_score * w_div
        + vol_score * w_vol
        + dd_score * w_dd
        + para_score * w_para
        + lis_score * w_lis
    ) / total_weight

    composite = max(0, min(100, composite))

    # --- Grade ---
    abs_thresholds = settings.get("abs_grade_thresholds", DEFAULT_ABSORPTION_SETTINGS["abs_grade_thresholds"])
    grade = compute_grade(composite, abs_thresholds)

    if grade is None:
        return None

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
        # Column mapping: support=divergence, upside=volume, floor=dd, target=paradigm, rr=lis
        "support_score": div_score,
        "upside_score": vol_score,
        "floor_cluster_score": dd_score,
        "target_cluster_score": para_score,
        "rr_score": lis_score,
        # Absorption-specific extras
        "bar_idx": trigger_idx,
        "abs_vol_ratio": round(vol_ratio, 1),
        "abs_es_price": round(trigger["close"], 2),
        "cvd": trigger["cvd"],
        "high": trigger["high"],
        "low": trigger["low"],
        "vol_trigger": trigger["volume"],
        "div_raw": div_raw,
        "vol_raw": vol_raw,
        "dd_raw": dd_raw,
        "para_raw": para_raw,
        "lis_raw": lis_raw,
        "dd_hedging": dd_str,
        "lis_val": lis_val,
        "lis_dist": round(lis_dist, 1) if lis_dist is not None else None,
        "ts": trigger.get("ts_end", ""),
        "lookback": lookback,
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
    # Cooldown is enforced inside evaluate_absorption via last_checked_idx,
    # but we also gate on bar distance here for notification dedup
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

    parts = [
        f"<b>ES ABSORPTION {side_emoji} {side_label} [{grade}] ({score:.0f}/100){strong_tag}</b>",
        "",
        f"Price: {result['abs_es_price']:.2f} | CVD: {result['cvd']:+,}",
        f"Vol spike: {result['vol_trigger']:,} ({result['abs_vol_ratio']:.1f}x avg)",
        f"Divergence: {'Price HL \u2191 / CVD \u2193' if result['direction'] == 'bullish' else 'Price LH \u2193 / CVD \u2191'} ({result.get('lookback', 8)} bars)",
    ]

    if result.get("dd_raw"):
        parts.append(f"DD Hedging: {result['dd_hedging']} \u2713")
    if result.get("para_raw"):
        parts.append(f"Paradigm: {result['paradigm']} \u2713")
    if result.get("lis_raw") and result.get("lis_val") is not None:
        parts.append(f"Near LIS: {result['lis_val']:.0f} ({result['lis_dist']:.1f} pts) \u2713")

    parts.append("")
    parts.append(
        f"Scores: Div {result['support_score']:.0f} | Vol {result['upside_score']:.0f} | "
        f"DD {result['floor_cluster_score']:.0f} | Para {result['target_cluster_score']:.0f} | "
        f"LIS {result['rr_score']:.0f}"
    )

    return "\n".join(parts)


# ── Main entry point ───────────────────────────────────────────────────────

def check_setups(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings,
                 lis_lower=None, lis_upper=None, aggregated_charm=None):
    """
    Main entry point called from main.py.
    Returns a list of result wrappers (each has keys: result, notify, notify_reason, message).
    List may be empty.

    New kwargs for BofA Scalp:
      lis_lower, lis_upper: parsed LIS low/high values
      aggregated_charm: aggregated charm from Volland stats
    """
    results = []

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
        update_lis_buffer(lis_lower, lis_upper)

    bofa_result = evaluate_bofa_scalp(spot, paradigm, lis_lower, lis_upper, aggregated_charm, settings)
    if bofa_result is not None:
        notify_bofa, reason_bofa = should_notify_bofa(bofa_result)
        results.append({
            "result": bofa_result,
            "notify": notify_bofa,
            "notify_reason": reason_bofa,
            "message": format_bofa_scalp_message(bofa_result),
        })

    return results
