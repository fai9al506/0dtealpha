"""
GEX Long Trading Setup Detector — self-contained scoring module.
Receives all data as parameters; no imports from main.py.
"""
from datetime import datetime, time as dtime
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


# ── Cooldown / notification gate ───────────────────────────────────────────

def should_notify(result):
    """
    Decide whether to fire a Telegram notification for this result.
    Fires once per grade, re-fires on improvement or after expiry→re-form.
    Returns True if notification should be sent.
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

    if _cooldown["last_grade"] is None:
        # First detection of the day
        fire = True
    elif grade_rank > last_rank:
        # Grade improved
        fire = True
    elif _cooldown["last_gap_to_lis"] is not None and (_cooldown["last_gap_to_lis"] - gap) > 2:
        # Gap improved by >2 pts
        fire = True
    elif _cooldown["setup_expired"]:
        # Setup had expired and re-formed
        fire = True

    if fire:
        _cooldown["last_grade"] = grade
        _cooldown["last_gap_to_lis"] = gap
        _cooldown["setup_expired"] = False

    return fire


def mark_setup_expired():
    """Call when paradigm loses GEX or gap > 20."""
    _cooldown["setup_expired"] = True
    _cooldown["last_grade"] = None
    _cooldown["last_gap_to_lis"] = None


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


# ── Main entry point ───────────────────────────────────────────────────────

def check_setups(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings):
    """
    Main entry point called from main.py.
    Returns dict with keys: result, notify, message  — or None.
    """
    # Check if setup conditions have expired (for cooldown tracking)
    if paradigm and "GEX" not in str(paradigm).upper():
        mark_setup_expired()
    elif spot is not None and lis is not None and (spot - lis) > 20:
        mark_setup_expired()

    result = evaluate_gex_long(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings)
    if result is None:
        return None

    notify = should_notify(result)
    message = format_setup_message(result)

    return {
        "result": result,
        "notify": notify,
        "message": message,
    }
