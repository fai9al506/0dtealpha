"""
Trading Setup Detector — self-contained scoring module.
Evaluates GEX Long, AG Short, BofA Scalp, CVD Divergence, Paradigm Reversal,
DD Exhaustion, and Skew Charm setups.
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
    "gex_max_gap": 5,           # max |spot - LIS| to enter (was 20)
    "gex_min_upside": 10,       # min pts to +GEX and target above spot
    "gex_target_pts": 15,       # outcome tracking target (was 10)
    "gex_stop_pts": 12,         # outcome tracking stop (was 8)
    # AG Short still uses these weights/brackets
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
    "grade_thresholds": {"A+": 85, "A": 70, "A-Entry": 50},
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
    Evaluate GEX Long setup using force alignment framework.

    Forces pushing price UP:
    - LIS below spot (within gap): support floor
    - LIS above spot (within gap): magnet pulling up
    - -GEX below spot: support floor
    - -GEX above spot: magnet pulling up
    - +GEX above spot: magnet pulling up
    - Target above spot: magnet pulling up

    A+ = all forces aligned bullish and close.
    Returns a result dict or None.
    """
    if not settings.get("gex_long_enabled", True):
        return None

    # Base conditions
    if not paradigm or "GEX" not in str(paradigm).upper():
        return None
    # Block toxic paradigm subtypes (GEX-TARGET=no upside room, GEX-MESSY=not clean)
    paradigm_upper = str(paradigm).upper()
    if "TARGET" in paradigm_upper or "MESSY" in paradigm_upper:
        return None
    if spot is None or lis is None or target is None:
        return None
    if max_plus_gex is None or max_minus_gex is None:
        return None

    max_gap = settings.get("gex_max_gap", 5)
    min_upside = settings.get("gex_min_upside", 10)

    # Gap: absolute distance to LIS (allow above OR below)
    gap = abs(spot - lis)
    if gap > max_gap:
        return None

    # +GEX must be above spot with room (magnet up)
    upside_gex = max_plus_gex - spot
    if upside_gex < min_upside:
        return None

    # Target must be above spot with room (magnet up)
    upside_target = target - spot
    if upside_target < min_upside:
        return None

    upside = min(upside_target, upside_gex)

    # ── Force scoring (6 forces, max 100) ────────────────────────────────
    # Each force scores based on proximity and direction

    # 1. LIS proximity (0-25): closer = stronger support/magnet
    if gap <= 2:
        lis_score = 25
    elif gap <= 3:
        lis_score = 20
    elif gap <= 5:
        lis_score = 15
    else:
        lis_score = 5

    # 2. -GEX force (0-20): above spot = magnet up, below spot = support floor
    neg_gex_dist = max_minus_gex - spot
    if neg_gex_dist > 0:
        # -GEX above spot: magnet pulling up (strong bullish)
        neg_gex_score = 20 if neg_gex_dist <= 15 else 15
    elif neg_gex_dist >= -10:
        # -GEX close below: strong support floor
        neg_gex_score = 15
    elif neg_gex_dist >= -20:
        # -GEX moderate below: weaker support
        neg_gex_score = 8
    else:
        # -GEX far below: minimal support effect
        neg_gex_score = 3

    # 3. +GEX magnet (0-20): above spot pulling up, closer = stronger
    if upside_gex >= 20:
        pos_gex_score = 12
    elif upside_gex >= 15:
        pos_gex_score = 16
    elif upside_gex >= 10:
        pos_gex_score = 20    # close magnet = strongest pull
    else:
        pos_gex_score = 5

    # 4. Target magnet (0-15): closer reachable target = better
    if upside_target >= 30:
        target_score = 8
    elif upside_target >= 20:
        target_score = 12
    elif upside_target >= 10:
        target_score = 15
    else:
        target_score = 5

    # 5. LIS type bonus (0-10): support (below) vs magnet (above)
    lis_type = "magnet" if lis > spot else "support"
    if lis_type == "support" and neg_gex_dist > 0:
        # Best combo: LIS supporting from below + -GEX pulling from above
        lis_type_score = 10
    elif lis_type == "magnet":
        # LIS above pulling up
        lis_type_score = 8
    else:
        lis_type_score = 5

    # 6. Time of day (0-10)
    first_hour = is_first_hour()
    t = datetime.now(NY).time()
    if t >= dtime(14, 0):
        time_score = 10
    elif t >= dtime(11, 30):
        time_score = 8
    elif first_hour:
        time_score = 6
    else:
        time_score = 4

    composite = lis_score + neg_gex_score + pos_gex_score + target_score + lis_type_score + time_score

    # ── Grade ───────────────────────────────────────────────────────────
    thresholds = settings.get("grade_thresholds", DEFAULT_SETUP_SETTINGS["grade_thresholds"])
    grade = compute_grade(composite, thresholds)

    if grade is None:
        return None

    rr_ratio = upside / gap if gap > 0 else 99

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
        "lis_type": lis_type,
        # Sub-scores repurposed for force components
        "support_score": lis_score,             # LIS proximity (0-25)
        "upside_score": neg_gex_score,          # -GEX force (0-20)
        "floor_cluster_score": pos_gex_score,   # +GEX magnet (0-20)
        "target_cluster_score": target_score,   # Target magnet (0-15)
        "rr_score": lis_type_score + time_score, # LIS type + time (0-20)
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
    """Call when paradigm loses GEX or gap > 5."""
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
    """Format a Telegram HTML message with force alignment breakdown."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")

    lis_type = result.get("lis_type", "support")
    lis_label = "MAGNET" if lis_type == "magnet" else "SUPPORT"

    msg = f"{grade_emoji} <b>GEX Long Setup — {result['grade']}</b>\n"
    msg += f"Score: <b>{result['score']}</b>/100\n\n"
    msg += f"SPX: {result['spot']:.0f}\n"
    msg += f"Paradigm: {result['paradigm']}\n"
    msg += f"LIS: {result['lis']:.0f} ({lis_label})  |  Target: {result['target']:.0f}\n"
    msg += f"+GEX: {result['max_plus_gex']:.0f}  |  -GEX: {result['max_minus_gex']:.0f}\n\n"
    msg += f"Gap to LIS: {result['gap_to_lis']:.1f}\n"
    msg += f"Upside: {result['upside']:.1f}\n"
    msg += f"R:R: {result['rr_ratio']:.1f}x\n\n"
    msg += "<b>Forces:</b>\n"
    msg += f"  LIS proximity: {result['support_score']}/25\n"
    msg += f"  -GEX force: {result['upside_score']}/20\n"
    msg += f"  +GEX magnet: {result['floor_cluster_score']}/20\n"
    msg += f"  Target magnet: {result['target_cluster_score']}/15\n"
    msg += f"  LIS type + time: {result['rr_score']}/20\n"
    if result["first_hour"]:
        msg += "\n⏰ First hour"
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


# ── CVD Divergence — defaults and state ─────────────────────────────────────
# Replaced ES Absorption (over-filtered: volume gate + z-score removed good signals).
# Simple swing-to-swing CVD divergence. Backtest: 83% WR, +536 pts (SL8/T10).

DEFAULT_ABSORPTION_SETTINGS = {
    "absorption_enabled": True,
    "abs_pivot_n": 2,
    "abs_max_trigger_dist": 40,
    "abs_cooldown_minutes": 15,
    "abs_stop_pts": 8,
    "abs_target_pts": 10,
}

_cooldown_absorption = {
    "last_bullish_swing_idx": -1,
    "last_bearish_swing_idx": -1,
    "last_bullish_time": None,
    "last_bearish_time": None,
    "last_date": None,
}


def reset_absorption_session():
    """Reset CVD Divergence detector state for a new ES session."""
    _cooldown_absorption["last_bullish_swing_idx"] = -1
    _cooldown_absorption["last_bearish_swing_idx"] = -1
    _cooldown_absorption["last_bullish_time"] = None
    _cooldown_absorption["last_bearish_time"] = None




def evaluate_absorption(bars, volland_stats, settings, spx_spot=None):
    """
    CVD Divergence detector — simple swing-to-swing CVD divergence.

    Finds pivot swings (no alternating enforcement, no volume gate, no z-score)
    and compares consecutive same-type swings (low-vs-low, high-vs-high).
    When price and CVD disagree, that's a divergence signal.

    4 patterns:
    - Sell exhaustion: lower low + higher CVD -> BUY
    - Sell absorption: higher low + lower CVD -> BUY
    - Buy exhaustion: higher high + lower CVD -> SELL
    - Buy absorption: lower high + higher CVD -> SELL

    Backtest: 83% WR, +536 pts on rithmic data (SL=8/T=10).

    Parameters:
      bars: list of bar dicts with idx, open, high, low, close, volume, cvd, status
      volland_stats: (unused, kept for API compat)
      settings: setup settings dict with abs_* keys
      spx_spot: (unused, kept for API compat)

    Returns result dict or None.
    """
    if not settings.get("absorption_enabled", True):
        return None

    pivot_n = settings.get("abs_pivot_n", 2)
    max_lookback = settings.get("abs_max_trigger_dist", 40)

    closed = [b for b in bars if b.get("status") == "closed"]
    if len(closed) < 10:
        return None

    trigger = closed[-1]
    trigger_idx = trigger["idx"]

    # No signals before 10:00 or after 15:30 ET
    now_et = datetime.now(NY)
    if now_et.time() < dtime(10, 0) or now_et.time() > dtime(15, 30):
        return None

    # Find ALL swings (no alternating enforcement — allows consecutive lows/highs)
    swings = []
    for i in range(pivot_n, len(closed) - pivot_n):
        bar = closed[i]
        is_low = all(bar["low"] <= closed[i - j]["low"] and bar["low"] <= closed[i + j]["low"]
                     for j in range(1, pivot_n + 1))
        if is_low:
            swings.append({"type": "L", "price": bar["low"], "cvd": bar["cvd"],
                          "bar_idx": bar["idx"], "volume": bar["volume"]})
        is_high = all(bar["high"] >= closed[i - j]["high"] and bar["high"] >= closed[i + j]["high"]
                      for j in range(1, pivot_n + 1))
        if is_high:
            swings.append({"type": "H", "price": bar["high"], "cvd": bar["cvd"],
                          "bar_idx": bar["idx"], "volume": bar["volume"]})

    if len(swings) < 2:
        return None

    # Find divergences within lookback window
    swing_lows = [s for s in swings if s["type"] == "L"]
    swing_highs = [s for s in swings if s["type"] == "H"]

    bullish_divs = []
    bearish_divs = []

    for i in range(1, len(swing_lows)):
        s1, s2 = swing_lows[i - 1], swing_lows[i]
        if trigger_idx - s2["bar_idx"] > max_lookback:
            continue
        if s2["price"] < s1["price"] and s2["cvd"] > s1["cvd"]:
            bullish_divs.append({
                "pattern": "sell_exhaustion", "swing": s2, "ref_swing": s1,
                "cvd_gap": round(abs(s2["cvd"] - s1["cvd"]), 1),
                "price_dist": round(abs(s2["price"] - s1["price"]), 2),
            })
        elif s2["price"] > s1["price"] and s2["cvd"] < s1["cvd"]:
            bullish_divs.append({
                "pattern": "sell_absorption", "swing": s2, "ref_swing": s1,
                "cvd_gap": round(abs(s2["cvd"] - s1["cvd"]), 1),
                "price_dist": round(abs(s2["price"] - s1["price"]), 2),
            })

    for i in range(1, len(swing_highs)):
        s1, s2 = swing_highs[i - 1], swing_highs[i]
        if trigger_idx - s2["bar_idx"] > max_lookback:
            continue
        if s2["price"] > s1["price"] and s2["cvd"] < s1["cvd"]:
            bearish_divs.append({
                "pattern": "buy_exhaustion", "swing": s2, "ref_swing": s1,
                "cvd_gap": round(abs(s2["cvd"] - s1["cvd"]), 1),
                "price_dist": round(abs(s2["price"] - s1["price"]), 2),
            })
        elif s2["price"] < s1["price"] and s2["cvd"] > s1["cvd"]:
            bearish_divs.append({
                "pattern": "buy_absorption", "swing": s2, "ref_swing": s1,
                "cvd_gap": round(abs(s2["cvd"] - s1["cvd"]), 1),
                "price_dist": round(abs(s2["price"] - s1["price"]), 2),
            })

    if not bullish_divs and not bearish_divs:
        return None

    # Direction resolution: pick most recent divergence
    rejected_divergence = None
    if bullish_divs and not bearish_divs:
        direction = "bullish"
        best = max(bullish_divs, key=lambda d: d["swing"]["bar_idx"])
        all_divs = bullish_divs
    elif bearish_divs and not bullish_divs:
        direction = "bearish"
        best = max(bearish_divs, key=lambda d: d["swing"]["bar_idx"])
        all_divs = bearish_divs
    else:
        last_bull = max(bullish_divs, key=lambda d: d["swing"]["bar_idx"])
        last_bear = max(bearish_divs, key=lambda d: d["swing"]["bar_idx"])
        if last_bull["swing"]["bar_idx"] >= last_bear["swing"]["bar_idx"]:
            direction = "bullish"
            best = last_bull
            all_divs = bullish_divs
            rejected_divergence = {"direction": "bearish", "pattern": last_bear["pattern"]}
        else:
            direction = "bearish"
            best = last_bear
            all_divs = bearish_divs
            rejected_divergence = {"direction": "bullish", "pattern": last_bull["pattern"]}

    # Dedup: skip if we already fired for this exact swing pair
    today = now_et.date()
    if _cooldown_absorption.get("last_date") != today:
        _cooldown_absorption["last_bullish_swing_idx"] = -1
        _cooldown_absorption["last_bearish_swing_idx"] = -1
        _cooldown_absorption["last_bullish_time"] = None
        _cooldown_absorption["last_bearish_time"] = None
        _cooldown_absorption["last_date"] = today

    swing_idx = best["swing"]["bar_idx"]
    side_key = "last_bullish_swing_idx" if direction == "bullish" else "last_bearish_swing_idx"
    if swing_idx <= _cooldown_absorption.get(side_key, -1):
        return None
    _cooldown_absorption[side_key] = swing_idx

    pattern = best["pattern"]
    # Simple grading: exhaustion patterns get higher score
    if "exhaustion" in pattern:
        score = 65
        grade = "A"
    else:
        score = 45
        grade = "B"

    # Volume ratio for logging (informational, not gating)
    recent_vols = [b["volume"] for b in closed[-11:-1]] if len(closed) >= 11 else [b["volume"] for b in closed[:-1]]
    vol_avg = sum(recent_vols) / len(recent_vols) if recent_vols else 1
    vol_ratio = trigger["volume"] / max(1, vol_avg)

    return {
        "setup_name": "CVD Divergence",
        "direction": direction,
        "grade": grade,
        "score": score,
        "paradigm": "",
        "spot": round(trigger["close"], 2),
        "lis": None,
        "target": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": None,
        "upside": None,
        "rr_ratio": None,
        "first_hour": False,
        "log_only": False,
        "pattern": pattern,
        "support_score": score,
        "upside_score": 0,
        "floor_cluster_score": 0,
        "target_cluster_score": 0,
        "rr_score": 0,
        "bar_idx": trigger_idx,
        "abs_vol_ratio": round(vol_ratio, 1),
        "abs_es_price": round(trigger["close"], 2),
        "cvd": trigger["cvd"],
        "high": trigger["high"],
        "low": trigger["low"],
        "vol_trigger": trigger["volume"],
        "best_swing": best,
        "all_divergences": all_divs,
        "swing_count": len(swings),
        "cvd_std": 0,
        "atr": 0,
        "div_raw": round(best["cvd_gap"], 1),
        "vol_raw": 1,
        "dd_raw": 0,
        "para_raw": 0,
        "lis_raw": 0,
        "lis_side_raw": 0,
        "target_dir_raw": 0,
        "dd_hedging": "",
        "lis_val": None,
        "lis_dist": None,
        "target_val": None,
        "ts": trigger.get("ts_end", ""),
        "lookback": f"swing ({len(swings)} tracked)",
        "pattern_tier": 2 if "exhaustion" in pattern else 1,
        "resolution_reason": "single_direction" if not rejected_divergence else "recency",
        "rejected_divergence": rejected_divergence,
        "all_bull_divs": [{"pattern": d["pattern"], "cvd_gap": d["cvd_gap"],
                           "swing_type": d["swing"]["type"], "swing_price": d["swing"]["price"]}
                          for d in bullish_divs],
        "all_bear_divs": [{"pattern": d["pattern"], "cvd_gap": d["cvd_gap"],
                           "swing_type": d["swing"]["type"], "swing_price": d["swing"]["price"]}
                          for d in bearish_divs],
    }


def should_notify_absorption(result):
    """15-min time-based cooldown per direction for CVD Divergence."""
    direction = result["direction"]
    now = datetime.now(NY)
    today = now.date()

    if _cooldown_absorption.get("last_date") != today:
        _cooldown_absorption["last_bullish_time"] = None
        _cooldown_absorption["last_bearish_time"] = None
        _cooldown_absorption["last_date"] = today

    time_key = "last_bullish_time" if direction == "bullish" else "last_bearish_time"
    last_fire = _cooldown_absorption.get(time_key)
    if last_fire is not None:
        elapsed = (now - last_fire).total_seconds() / 60
        if elapsed < 15:
            return False, None

    _cooldown_absorption[time_key] = now
    return True, "new"


def format_absorption_message(result):
    """Format a Telegram HTML message for CVD Divergence setup."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result["grade"]
    score = result["score"]

    pattern = result.get("pattern", "unknown")
    pattern_labels = {
        "sell_exhaustion": "Sell Exhaustion",
        "sell_absorption": "Sell Absorption",
        "buy_exhaustion": "Buy Exhaustion",
        "buy_absorption": "Buy Absorption",
    }
    pattern_label = pattern_labels.get(pattern, pattern)

    parts = [
        f"<b>CVD DIVERGENCE {side_emoji} {side_label} [{grade}] ({score}/100)</b>",
        f"{pattern_label}",
        "\u2501" * 18,
    ]

    best = result.get("best_swing")
    if best:
        sw = best["swing"]
        ref = best.get("ref_swing")
        if ref:
            swing_type_label = "Low" if ref["type"] == "L" else "High"
            parts.append("")
            parts.append(f"Swing 1: {ref['price']:.2f} ({swing_type_label}) Bar #{ref['bar_idx']} CVD={ref['cvd']:+,}")
            parts.append(f"Swing 2: {sw['price']:.2f} ({swing_type_label}) Bar #{sw['bar_idx']} CVD={sw['cvd']:+,}")
            parts.append(f"CVD Gap: {best['cvd_gap']:,} | Price: {best['price_dist']:.2f}")

    parts.append("")
    parts.append(f"Entry: {result['abs_es_price']:.2f} | SL: 8pt | T: 10pt")

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


# ── Skew+Charm — defaults and state ──────────────────────────────────────────

DEFAULT_SKEW_CHARM_SETTINGS = {
    "skew_charm_enabled": True,
    "skew_window": 20,              # snapshots to look back (~10 min at 30s cycle)
    "skew_threshold_pct": 3.0,      # minimum % change to fire
    "skew_cooldown_minutes": 30,    # per-direction cooldown
    "skew_target_pts": 10,          # fixed target for outcome tracking
    "skew_stop_pts": 20,            # fixed stop for outcome tracking
    "skew_market_start": "09:45",
    "skew_market_end": "15:45",
}

_cooldown_skew_charm = {
    "last_long_time": None,
    "last_short_time": None,
    "last_date": None,
}

_skew_tracker = {
    "buffer": [],      # list of (timestamp_iso, skew_value) tuples
    "last_date": None,
}


def update_skew_tracker(skew_value, settings=None):
    """Append skew reading to rolling buffer and compute % change.

    Returns (skew_change_pct, window_skew) or (None, None) if insufficient data.
    skew_change_pct is the % change from window-ago entry to current.
    """
    if settings is None:
        settings = DEFAULT_SKEW_CHARM_SETTINGS
    window = settings.get("skew_window", 20)

    today = datetime.now(NY).date()
    if _skew_tracker["last_date"] != today:
        _skew_tracker["buffer"] = []
        _skew_tracker["last_date"] = today

    now_iso = datetime.now(NY).isoformat()
    _skew_tracker["buffer"].append((now_iso, skew_value))

    # Keep buffer bounded (2x window to allow flexibility)
    max_buf = window * 2
    if len(_skew_tracker["buffer"]) > max_buf:
        _skew_tracker["buffer"] = _skew_tracker["buffer"][-max_buf:]

    buf = _skew_tracker["buffer"]
    if len(buf) < window:
        return None, None

    old_skew = buf[-window][1]
    if old_skew is None or old_skew == 0:
        return None, None

    change_pct = ((skew_value - old_skew) / abs(old_skew)) * 100.0
    return round(change_pct, 2), round(old_skew, 4)


def evaluate_skew_charm(spot, skew_value, skew_change_pct, charm, paradigm, settings):
    """
    Evaluate Skew+Charm setup.
    LONG: skew drops >threshold% AND charm > 0
    SHORT: skew rises >threshold% AND charm < 0
    Returns result dict or None.
    """
    if not settings.get("skew_charm_enabled", True):
        return None
    if spot is None or skew_value is None or skew_change_pct is None or charm is None:
        return None

    # Time window check
    now = datetime.now(NY)
    start_str = settings.get("skew_market_start", "09:45")
    end_str = settings.get("skew_market_end", "15:45")
    try:
        h, m = map(int, start_str.split(":"))
        market_start = dtime(h, m)
        h, m = map(int, end_str.split(":"))
        market_end = dtime(h, m)
    except Exception:
        market_start, market_end = dtime(9, 45), dtime(15, 45)
    if not (market_start <= now.time() <= market_end):
        return None

    threshold = settings.get("skew_threshold_pct", 3.0)
    target_pts = settings.get("skew_target_pts", 10)
    stop_pts = settings.get("skew_stop_pts", 20)

    # Signal detection
    # LONG: skew drops (negative change) AND charm bullish (> 0)
    # SHORT: skew rises (positive change) AND charm bearish (< 0)
    if skew_change_pct <= -threshold and charm > 0:
        direction = "long"
    elif skew_change_pct >= threshold and charm < 0:
        direction = "short"
    else:
        return None

    is_long = direction == "long"
    target_price = round(spot + target_pts, 2) if is_long else round(spot - target_pts, 2)
    stop_price = round(spot - stop_pts, 2) if is_long else round(spot + stop_pts, 2)

    # --- Scoring (5 components, max 100) ---

    # 1. Skew magnitude (0-30): bigger skew change = stronger signal
    abs_change = abs(skew_change_pct)
    if abs_change >= 10:
        skew_score = 30
    elif abs_change >= 7:
        skew_score = 25
    elif abs_change >= 5:
        skew_score = 20
    elif abs_change >= 3:
        skew_score = 15
    else:
        skew_score = 5

    # 2. Charm alignment strength (0-25)
    abs_charm = abs(charm)
    if abs_charm < 20_000_000:
        charm_score = 0
    elif abs_charm < 50_000_000:
        charm_score = 8
    elif abs_charm < 100_000_000:
        charm_score = 15
    elif abs_charm < 250_000_000:
        charm_score = 22
    else:
        charm_score = 25

    # 3. Time-of-day (0-15)
    t = now.time()
    if t >= dtime(14, 0):
        time_score = 15
    elif t >= dtime(11, 30):
        time_score = 12
    elif t >= dtime(10, 30):
        time_score = 8
    else:
        time_score = 3

    # 4. Paradigm context (0-15)
    para_score = 0
    if paradigm:
        p = str(paradigm).upper()
        if "GEX" in p and direction == "long":
            para_score = 15
        elif "AG" in p and direction == "short":
            para_score = 15
        elif "BOFA" in p:
            para_score = 10
        elif "MESSY" in p:
            para_score = 8
        else:
            para_score = 5

    # 5. Skew level (0-15): extreme skew values strengthen the signal
    # Low skew (<1.0) + dropping = strong compression momentum
    # High skew (>1.1) + rising = strong expansion momentum
    if (direction == "long" and skew_value < 0.95) or (direction == "short" and skew_value > 1.10):
        level_score = 15
    elif (direction == "long" and skew_value < 1.0) or (direction == "short" and skew_value > 1.05):
        level_score = 10
    else:
        level_score = 5

    total_score = skew_score + charm_score + time_score + para_score + level_score

    # Grade based on composite score (same thresholds as GEX Long / AG Short)
    if total_score >= 90:
        grade = "A+"
    elif total_score >= 75:
        grade = "A"
    elif total_score >= 60:
        grade = "A-Entry"
    elif total_score >= 45:
        grade = "B"
    else:
        grade = "C"

    return {
        "setup_name": "Skew Charm",
        "direction": direction,
        "grade": grade,
        "score": total_score,
        "paradigm": str(paradigm) if paradigm else None,
        "spot": round(spot, 2),
        "target": round(target_price, 2),
        "target_price": round(target_price, 2),
        "stop_price": round(stop_price, 2),
        "lis": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": None,
        "upside": target_pts,
        "rr_ratio": round(target_pts / stop_pts, 2) if stop_pts > 0 else None,
        "first_hour": False,
        # Sub-scores in existing columns
        "support_score": skew_score,          # skew magnitude (0-30)
        "upside_score": charm_score,          # charm strength (0-25)
        "floor_cluster_score": time_score,    # time-of-day (0-15)
        "target_cluster_score": para_score,   # paradigm context (0-15)
        "rr_score": level_score,              # skew level (0-15)
        # Skew-specific fields
        "skew_value": round(skew_value, 4),
        "skew_change_pct": round(skew_change_pct, 2),
        "charm": charm,
        "dd_shift": None,
        "dd_current": None,
        "detail_score": total_score,  # actual composite for analysis
    }


def should_notify_skew_charm(result):
    """30-min cooldown per direction for Skew Charm."""
    if result is None:
        return False, None

    direction = result["direction"]
    now = datetime.now(NY)
    today = now.date()

    if _cooldown_skew_charm.get("last_date") != today:
        _cooldown_skew_charm["last_long_time"] = None
        _cooldown_skew_charm["last_short_time"] = None
        _cooldown_skew_charm["last_date"] = today

    side_key = "last_long_time" if direction == "long" else "last_short_time"
    last_fire = _cooldown_skew_charm.get(side_key)
    if last_fire is not None:
        elapsed = (now - last_fire).total_seconds() / 60
        if elapsed < 30:
            return False, None

    _cooldown_skew_charm[side_key] = now
    return True, "new"


def format_skew_charm_message(result):
    """Format Telegram HTML message for Skew+Charm setup."""
    direction = result["direction"]
    dir_label = "LONG" if direction == "long" else "SHORT"
    grade = result.get("grade", "C")
    grade_emoji = {"A+": "\U0001f7e2", "A": "\U0001f535", "A-Entry": "\U0001f7e1", "B": "\u26aa", "C": "\u26aa"}.get(grade, "\u26aa")
    score = result.get("score", 0)

    skew_val = result.get("skew_value", 0)
    skew_chg = result.get("skew_change_pct", 0)
    charm_m = (result.get("charm") or 0) / 1_000_000

    msg = f"{grade_emoji} <b>Skew Charm \u2014 {dir_label} ({grade})</b>\n"
    msg += f"Score: <b>{score}</b>/100\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"Skew: {skew_val:.4f} ({skew_chg:+.1f}% over 20 snapshots)\n"
    msg += f"Charm: ${charm_m:+,.0f}M ({'bullish \u2713' if result.get('charm', 0) > 0 else 'bearish \u2713'})\n"
    msg += f"Paradigm: {result.get('paradigm') or 'N/A'}\n"
    msg += f"Entry: ${result['spot']:,.0f} | Target: ${result.get('target_price', 0):,.0f} (+10) | Stop: ${result.get('stop_price', 0):,.0f} (-20)\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"Skew {result.get('support_score', 0)} | Charm {result.get('upside_score', 0)} | "
    msg += f"Time {result.get('floor_cluster_score', 0)} | Para {result.get('target_cluster_score', 0)} | "
    msg += f"Level {result.get('rr_score', 0)}"
    return msg


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


# ── Vanna Pivot Bounce ────────────────────────────────────────────────────

DEFAULT_VANNA_PIVOT_SETTINGS = {
    "vanna_pivot_enabled": False,  # Disabled: vanna levels have no edge (random levels bounce equally)
    "vp_proximity_pts": 15,        # max distance from dominant vanna level
    "vp_dominant_pct": 12,         # min % concentration to qualify as dominant
    "vp_cooldown_minutes": 15,     # per-direction cooldown
    "vp_market_start": "10:00",    # skip first 30 min
    "vp_market_end": "15:30",
    "vp_stop_pts": 8,
    "vp_target_pts": 10,
}

_cooldown_vanna_pivot = {
    "last_long_time": None,
    "last_short_time": None,
    "last_date": None,
}


def _vp_find_swings(bars, pivot_n=2):
    """Find swing highs and lows from range bars for Vanna Pivot Bounce.

    Self-contained — does NOT share state with CVD Divergence swing detection.
    bars: list of dicts with bar_low, bar_high, cvd, ts_start keys (DB column names).
    """
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        # Swing low: bar low <= all neighbors
        is_low = True
        for j in range(1, pivot_n + 1):
            if bars[i]["bar_low"] > bars[i - j]["bar_low"] or bars[i]["bar_low"] > bars[i + j]["bar_low"]:
                is_low = False
                break
        if is_low:
            swings.append({
                "type": "low", "price": bars[i]["bar_low"], "cvd": bars[i]["cvd"],
                "ts": bars[i]["ts_start"], "bar_idx": i,
            })

        # Swing high: bar high >= all neighbors
        is_high = True
        for j in range(1, pivot_n + 1):
            if bars[i]["bar_high"] < bars[i - j]["bar_high"] or bars[i]["bar_high"] < bars[i + j]["bar_high"]:
                is_high = False
                break
        if is_high:
            swings.append({
                "type": "high", "price": bars[i]["bar_high"], "cvd": bars[i]["cvd"],
                "ts": bars[i]["ts_start"], "bar_idx": i,
            })

    swings.sort(key=lambda s: s["ts"])
    return swings


def _vp_detect_divergences(bars, swings):
    """Find CVD divergence points (exhaustion + absorption) from swings.

    Returns list of divergence dicts sorted by timestamp.
    """
    divs = []
    lows = [s for s in swings if s["type"] == "low"]
    highs = [s for s in swings if s["type"] == "high"]

    # Sell exhaustion: lower low + higher CVD -> LONG
    for i in range(1, len(lows)):
        prev, curr = lows[i - 1], lows[i]
        if curr["price"] < prev["price"] and curr["cvd"] > prev["cvd"]:
            divs.append({
                "type": "sell_exhaustion", "direction": "long",
                "price": curr["price"], "ts": curr["ts"], "bar_idx": curr["bar_idx"],
                "price_diff": curr["price"] - prev["price"],
                "cvd_diff": curr["cvd"] - prev["cvd"],
            })

    # Sell absorption: higher low + lower CVD -> LONG
    for i in range(1, len(lows)):
        prev, curr = lows[i - 1], lows[i]
        if curr["price"] > prev["price"] and curr["cvd"] < prev["cvd"]:
            divs.append({
                "type": "sell_absorption", "direction": "long",
                "price": curr["price"], "ts": curr["ts"], "bar_idx": curr["bar_idx"],
                "price_diff": curr["price"] - prev["price"],
                "cvd_diff": curr["cvd"] - prev["cvd"],
            })

    # Buy exhaustion: higher high + lower CVD -> SHORT
    for i in range(1, len(highs)):
        prev, curr = highs[i - 1], highs[i]
        if curr["price"] > prev["price"] and curr["cvd"] < prev["cvd"]:
            divs.append({
                "type": "buy_exhaustion", "direction": "short",
                "price": curr["price"], "ts": curr["ts"], "bar_idx": curr["bar_idx"],
                "price_diff": curr["price"] - prev["price"],
                "cvd_diff": curr["cvd"] - prev["cvd"],
            })

    # Buy absorption: lower high + higher CVD -> SHORT
    for i in range(1, len(highs)):
        prev, curr = highs[i - 1], highs[i]
        if curr["price"] < prev["price"] and curr["cvd"] > prev["cvd"]:
            divs.append({
                "type": "buy_absorption", "direction": "short",
                "price": curr["price"], "ts": curr["ts"], "bar_idx": curr["bar_idx"],
                "price_diff": curr["price"] - prev["price"],
                "cvd_diff": curr["cvd"] - prev["cvd"],
            })

    divs.sort(key=lambda d: d["ts"])
    return divs


def evaluate_vanna_pivot_bounce(spot, vanna_levels, range_bars, settings):
    """
    Evaluate Vanna Pivot Bounce setup.

    Uses dominant vanna levels (THIS_WEEK + THIRTY_NEXT_DAYS) as directional bias
    and CVD swing divergence as entry trigger.

    Parameters:
      spot: current SPX price
      vanna_levels: list of dicts {strike, value, timeframe, pct, confluence}
      range_bars: list of ES range bar dicts from DB (bar_low, bar_high, cvd, etc.)
      settings: setup settings dict with vp_* keys

    Returns result dict or None.
    """
    if not settings.get("vanna_pivot_enabled", True):
        return None
    if spot is None or not vanna_levels or not range_bars:
        return None

    # Time window check
    now = datetime.now(NY)
    start_str = settings.get("vp_market_start", "10:00")
    end_str = settings.get("vp_market_end", "15:30")
    try:
        h, m = map(int, start_str.split(":"))
        market_start = dtime(h, m)
        h, m = map(int, end_str.split(":"))
        market_end = dtime(h, m)
    except Exception:
        market_start, market_end = dtime(10, 0), dtime(15, 30)
    if not (market_start <= now.time() <= market_end):
        return None

    proximity_pts = settings.get("vp_proximity_pts", 15)
    target_pts = settings.get("vp_target_pts", 10)
    stop_pts = settings.get("vp_stop_pts", 8)

    # Need enough bars for swing detection (pivot_n=2 needs at least 5 bars)
    if len(range_bars) < 10:
        return None

    # Normalize bar keys: DB uses bar_low/bar_high, ensure cvd exists
    # Range bars from DB have: bar_open, bar_high, bar_low, bar_close, cvd (cumulative_delta alias)
    # Check first bar for expected keys
    sample = range_bars[0]
    if "bar_low" not in sample or "bar_high" not in sample:
        return None
    if "cvd" not in sample and "cumulative_delta" not in sample:
        return None
    # Map cumulative_delta to cvd if needed
    if "cvd" not in sample:
        for b in range_bars:
            b["cvd"] = b.get("cumulative_delta", 0)

    # Find swings and divergences
    swings = _vp_find_swings(range_bars, pivot_n=2)
    if len(swings) < 2:
        return None
    divergences = _vp_detect_divergences(range_bars, swings)
    if not divergences:
        return None

    # Only consider recent divergences (last 40 bars from the end)
    last_bar_idx = len(range_bars) - 1
    max_lookback = 40
    recent_divs = [d for d in divergences if d["bar_idx"] >= last_bar_idx - max_lookback]
    if not recent_divs:
        return None

    # Match divergences to dominant vanna levels
    best_match = None
    best_score = -1

    for div in recent_divs:
        div_price = div["price"]
        div_direction = div["direction"]

        for vl in vanna_levels:
            strike = vl["strike"]
            vanna_value = vl["value"]
            vanna_pct = vl["pct"]
            confluence = vl.get("confluence", False)

            # Proximity check
            dist = abs(div_price - strike)
            if dist > proximity_pts:
                continue

            # Direction agreement: positive vanna + long, negative vanna + short
            if vanna_value > 0 and div_direction != "long":
                continue
            if vanna_value < 0 and div_direction != "short":
                continue

            # ── Scoring (5 components, max 100) ──

            # 1. Vanna concentration (0-25)
            if vanna_pct >= 30:
                conc_score = 25
            elif vanna_pct >= 20:
                conc_score = 18
            elif vanna_pct >= 15:
                conc_score = 14
            else:
                conc_score = 10  # at minimum vp_dominant_pct (12%)

            # 2. Proximity (0-25)
            if dist <= 3:
                prox_score = 25
            elif dist <= 8:
                prox_score = 18
            else:
                prox_score = 10  # 8-15 pts

            # 3. CVD pattern (0-20): exhaustion > absorption
            pattern = div["type"]
            if "exhaustion" in pattern:
                cvd_score = 20
            else:
                cvd_score = 15  # absorption

            # 4. Confluence (0-15): appears in both timeframes
            conf_score = 15 if confluence else 5

            # 5. Time-of-day (0-15)
            t = now.time()
            if dtime(10, 30) <= t <= dtime(14, 0):
                time_score = 15
            elif dtime(14, 0) < t <= dtime(15, 0):
                time_score = 10
            else:
                time_score = 5  # 10:00-10:30 or 15:00-15:30

            total_score = conc_score + prox_score + cvd_score + conf_score + time_score

            if total_score > best_score:
                best_score = total_score
                best_match = {
                    "div": div, "vanna_level": vl,
                    "conc_score": conc_score, "prox_score": prox_score,
                    "cvd_score": cvd_score, "conf_score": conf_score,
                    "time_score": time_score, "total_score": total_score,
                    "proximity": round(dist, 1),
                }

    if best_match is None:
        return None

    div = best_match["div"]
    vl = best_match["vanna_level"]
    direction = div["direction"]
    is_long = direction == "long"
    total_score = best_match["total_score"]

    target_price = round(spot + target_pts, 2) if is_long else round(spot - target_pts, 2)
    stop_price = round(spot - stop_pts, 2) if is_long else round(spot + stop_pts, 2)

    # Grading: A+>=85, A>=70, B>=50, C<50
    if total_score >= 85:
        grade = "A+"
    elif total_score >= 70:
        grade = "A"
    elif total_score >= 50:
        grade = "B"
    else:
        grade = "C"

    return {
        "setup_name": "Vanna Pivot Bounce",
        "direction": direction,
        "grade": grade,
        "score": total_score,
        "paradigm": None,
        "spot": round(spot, 2),
        "target": round(target_price, 2),
        "target_price": round(target_price, 2),
        "stop_price": round(stop_price, 2),
        "lis": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": None,
        "upside": target_pts,
        "rr_ratio": round(target_pts / stop_pts, 2) if stop_pts > 0 else None,
        "first_hour": False,
        # Sub-scores in existing columns
        "support_score": best_match["conc_score"],       # vanna concentration (0-25)
        "upside_score": best_match["prox_score"],         # proximity (0-25)
        "floor_cluster_score": best_match["cvd_score"],   # CVD pattern (0-20)
        "target_cluster_score": best_match["conf_score"], # confluence (0-15)
        "rr_score": best_match["time_score"],             # time-of-day (0-15)
        # Vanna-specific fields
        "vanna_strike": vl["strike"],
        "vanna_pct": round(vl["pct"], 1),
        "vanna_tf": vl["timeframe"],
        "vanna_value": vl["value"],
        "confluence": vl.get("confluence", False),
        "proximity": best_match["proximity"],
        "pattern": div["type"],
        "div_bar_idx": div["bar_idx"],
        "dd_shift": None,
        "dd_current": None,
        "detail_score": total_score,
    }


def should_notify_vanna_pivot(result):
    """15-min cooldown per direction for Vanna Pivot Bounce."""
    if result is None:
        return False, None

    direction = result["direction"]
    now = datetime.now(NY)
    today = now.date()
    cooldown_min = 15

    if _cooldown_vanna_pivot.get("last_date") != today:
        _cooldown_vanna_pivot["last_long_time"] = None
        _cooldown_vanna_pivot["last_short_time"] = None
        _cooldown_vanna_pivot["last_date"] = today

    side_key = "last_long_time" if direction == "long" else "last_short_time"
    last_fire = _cooldown_vanna_pivot.get(side_key)
    if last_fire is not None:
        elapsed = (now - last_fire).total_seconds() / 60
        if elapsed < cooldown_min:
            return False, None

    _cooldown_vanna_pivot[side_key] = now
    return True, "new"


def format_vanna_pivot_message(result):
    """Format Telegram HTML message for Vanna Pivot Bounce setup."""
    direction = result["direction"]
    dir_label = "LONG" if direction == "long" else "SHORT"
    grade = result.get("grade", "C")
    grade_emoji = {"A+": "\U0001f7e2", "A": "\U0001f535", "B": "\u26aa", "C": "\u26aa"}.get(grade, "\u26aa")
    score = result.get("score", 0)

    pattern = result.get("pattern", "unknown").replace("_", " ").title()
    vanna_strike = result.get("vanna_strike", 0)
    vanna_pct = result.get("vanna_pct", 0)
    vanna_tf = result.get("vanna_tf", "?")
    proximity = result.get("proximity", 0)
    confluence = result.get("confluence", False)

    tf_label = {"THIS_WEEK": "Weekly", "THIRTY_NEXT_DAYS": "Monthly"}.get(vanna_tf, vanna_tf)
    conf_tag = " [CONFLUENCE]" if confluence else ""

    msg = f"{grade_emoji} <b>Vanna Pivot Bounce \u2014 {dir_label} ({grade})</b>\n"
    msg += f"Score: <b>{score}</b>/100\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"Vanna Level: ${vanna_strike:,.0f} ({vanna_pct:.0f}% {tf_label}){conf_tag}\n"
    msg += f"Pattern: {pattern}\n"
    msg += f"Proximity: {proximity:.1f} pts\n"
    msg += f"Entry: ${result['spot']:,.0f} | Target: ${result.get('target_price', 0):,.0f} (+{result.get('upside', 10)}) | Stop: ${result.get('stop_price', 0):,.0f} (-{int(abs(result['spot'] - result.get('stop_price', 0)))})\n"
    msg += "\u2501" * 18 + "\n"
    msg += f"Conc {result.get('support_score', 0)} | Prox {result.get('upside_score', 0)} | "
    msg += f"CVD {result.get('floor_cluster_score', 0)} | Conf {result.get('target_cluster_score', 0)} | "
    msg += f"Time {result.get('rr_score', 0)}"
    return msg


# ── Main entry point ───────────────────────────────────────────────────────

def check_setups(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings,
                 lis_lower=None, lis_upper=None, aggregated_charm=None,
                 dd_hedging=None, es_bars=None,
                 dd_value=None, dd_shift=None,
                 skew_value=None, skew_change_pct=None,
                 vanna_levels=None, es_range_bars=None):
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
      skew_value: current IV skew ratio (put IV / call IV for near OTM strikes)
      skew_change_pct: % change in skew over lookback window (Skew+Charm)
      vanna_levels: list of dominant vanna level dicts (Vanna Pivot Bounce)
      es_range_bars: list of ES range bar dicts from DB (Vanna Pivot Bounce)
    """
    results = []

    # ── Track paradigm changes (must be before setup evaluations) ──
    update_paradigm_tracker(paradigm)

    # ── GEX Long cooldown expiry tracking ──
    if paradigm and "GEX" not in str(paradigm).upper():
        mark_setup_expired()
    elif spot is not None and lis is not None and abs(spot - lis) > 5:
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

    # ── Skew Charm ──
    skew_charm_result = evaluate_skew_charm(
        spot, skew_value, skew_change_pct, aggregated_charm, paradigm, settings,
    )
    if skew_charm_result is not None:
        notify_sc, reason_sc = should_notify_skew_charm(skew_charm_result)
        results.append({
            "result": skew_charm_result,
            "notify": notify_sc,
            "notify_reason": reason_sc,
            "message": format_skew_charm_message(skew_charm_result),
        })

    # ── Vanna Pivot Bounce ──
    vp_result = evaluate_vanna_pivot_bounce(spot, vanna_levels, es_range_bars, settings)
    if vp_result is not None:
        notify_vp, reason_vp = should_notify_vanna_pivot(vp_result)
        results.append({
            "result": vp_result,
            "notify": notify_vp,
            "notify_reason": reason_vp,
            "message": format_vanna_pivot_message(vp_result),
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
        "paradigm_rev": _serialize(_cooldown_paradigm_rev),
        "paradigm_tracker": _serialize(_paradigm_tracker),
        "dd_exhaust": _serialize(_cooldown_dd_exhaust),
        "dd_tracker": _serialize(_dd_tracker),
        "skew_charm": _serialize(_cooldown_skew_charm),
        "skew_tracker": copy.deepcopy(_skew_tracker),
        "vanna_pivot": _serialize(_cooldown_vanna_pivot),
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
        _cooldown_absorption.update(_deserialize(
            data["absorption"], has_datetimes=True,
            dt_keys=("last_bullish_time", "last_bearish_time")))
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
    if "skew_charm" in data:
        _cooldown_skew_charm.update(_deserialize(
            data["skew_charm"], has_datetimes=True,
            dt_keys=("last_long_time", "last_short_time")))
    if "skew_tracker" in data:
        _skew_tracker.update(data["skew_tracker"])
    if "vanna_pivot" in data:
        _cooldown_vanna_pivot.update(_deserialize(
            data["vanna_pivot"], has_datetimes=True,
            dt_keys=("last_long_time", "last_short_time")))
