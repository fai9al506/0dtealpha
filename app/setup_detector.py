"""
Trading Setup Detector — self-contained scoring module.
Evaluates GEX Long, AG Short, BofA Scalp, ES Absorption, Single-Bar Absorption,
Paradigm Reversal, DD Exhaustion, and Skew Charm setups.
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
    "gex_max_gap": 5,           # max |spot - LIS| to enter (base, widened by LIS velocity)
    "gex_min_upside": 10,       # min pts to +GEX and target above spot
    "gex_target_pts": 15,       # outcome tracking target (was 10)
    "gex_stop_pts": 8,          # outcome tracking stop (trail backtest optimal)
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
    "_gone_count": 0,
}

_cooldown_ag = {
    "last_grade": None,
    "last_gap_to_lis": None,
    "setup_expired": False,
    "last_date": None,
    "_gone_count": 0,
    "last_fire_time": None,       # 15-min time floor (prevents flicker re-fires)
}
AG_MIN_COOLDOWN_MINUTES = 15

# How many consecutive non-paradigm cycles before marking expired
# 3 cycles = ~90 seconds — survives deploys and brief paradigm flickers
_EXPIRY_DEBOUNCE = 3

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


# ── GEX Velocity evaluation (separate from GEX Long) ──────────────────────

def evaluate_gex_velocity(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings):
    """
    Separate setup: fires when LIS is surging rapidly toward spot but gap is
    still too wide for normal GEX Long (5-10 pts).  Rapid LIS convergence =
    dealer repositioning = price follows before LIS arrives.

    Only fires in the gap 5-10 range that normal GEX Long misses.
    Uses same risk management as GEX Long (SL=8, trail).
    """
    if not settings.get("gex_long_enabled", True):
        return None

    if not paradigm or "GEX" not in str(paradigm).upper():
        return None
    paradigm_upper = str(paradigm).upper()
    if "TARGET" in paradigm_upper or "MESSY" in paradigm_upper:
        return None
    if spot is None or lis is None or target is None:
        return None
    if max_plus_gex is None or max_minus_gex is None:
        return None

    gap = abs(spot - lis)

    # Only fire in the gap range that normal GEX Long misses (5-10)
    if gap <= 5 or gap > 10:
        return None

    # Require strong velocity (LIS must be surging toward spot)
    lis_move, n_readings = get_gex_lis_velocity()
    if n_readings < 3 or lis_move < 25:
        return None

    min_upside = settings.get("gex_min_upside", 10)

    # +GEX must be above spot with room
    upside_gex = max_plus_gex - spot
    if upside_gex < min_upside:
        return None

    # Target must be above spot with room
    upside_target = target - spot
    if upside_target < min_upside:
        return None

    upside = min(upside_target, upside_gex)

    # ── Scoring: velocity-weighted ──
    # Velocity strength (0-30) — the main edge
    if lis_move >= 80:
        vel_score = 30
    elif lis_move >= 50:
        vel_score = 22
    else:
        vel_score = 15   # lis_move >= 25

    # Gap penalty (0-20) — tighter gap is better even for velocity
    if gap <= 6:
        gap_score = 20
    elif gap <= 7:
        gap_score = 15
    elif gap <= 8:
        gap_score = 10
    else:
        gap_score = 5

    # +GEX magnet (0-20)
    if upside_gex >= 20:
        gex_score = 12
    elif upside_gex >= 15:
        gex_score = 16
    elif upside_gex >= 10:
        gex_score = 20
    else:
        gex_score = 5

    # Time (0-10)
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

    # LIS direction bonus (0-10): LIS below spot = support, above = magnet
    lis_type = "magnet" if lis > spot else "support"
    lis_dir_score = 8 if lis_type == "magnet" else 6

    composite = vel_score + gap_score + gex_score + time_score + lis_dir_score

    # Grade: minimum 50 to fire
    thresholds = {"A+": 80, "A": 65, "A-Entry": 50}
    grade = compute_grade(composite, thresholds)
    if grade is None:
        return None

    return {
        "setup_name": "GEX Velocity",
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
        "rr_ratio": round(upside / gap if gap > 0 else 99, 2),
        "first_hour": first_hour,
        "lis_type": lis_type,
        "lis_velocity": round(lis_move, 1),
        "lis_readings": n_readings,
        # Sub-scores
        "support_score": vel_score,
        "upside_score": gap_score,
        "floor_cluster_score": gex_score,
        "target_cluster_score": lis_dir_score,
        "rr_score": time_score,
    }


def format_gex_velocity_message(result, alignment=None):
    """Format Telegram message for GEX Velocity setup."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    vel = result.get("lis_velocity", 0)
    msg = f"{grade_emoji} <b>GEX Velocity LONG [{result['grade']}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} -> {result['target']:.0f} | SL {result['spot'] - 8:.0f} (8pt) | Trail\n"
    msg += f"{result['paradigm']} | LIS {result['lis']:.0f} (gap {result['gap_to_lis']:.1f}) | LIS surged +{vel:.0f} pts"
    return msg


# ── GEX Velocity cooldown ─────────────────────────────────────────────────

_cooldown_gex_vel = {
    "last_grade": None, "last_gap_to_lis": None,
    "setup_expired": False, "last_date": None, "_gone_count": 0,
}


def should_notify_gex_velocity(result):
    """Cooldown gate for GEX Velocity — same pattern as GEX Long."""
    global _cooldown_gex_vel
    today = datetime.now(NY).date()
    if _cooldown_gex_vel["last_date"] != today:
        _cooldown_gex_vel = {"last_grade": None, "last_gap_to_lis": None,
                             "setup_expired": False, "last_date": today, "_gone_count": 0}

    grade = result["grade"]
    gap = result["gap_to_lis"]
    grade_rank = _GRADE_ORDER.get(grade, 0)
    last_rank = _GRADE_ORDER.get(_cooldown_gex_vel["last_grade"], 0)

    fire = False
    reason = None

    if _cooldown_gex_vel["last_grade"] is None:
        fire, reason = True, "new"
    elif grade_rank > last_rank:
        fire, reason = True, "grade_upgrade"
    elif _cooldown_gex_vel["last_gap_to_lis"] is not None and (_cooldown_gex_vel["last_gap_to_lis"] - gap) > 2:
        fire, reason = True, "gap_improvement"
    elif _cooldown_gex_vel["setup_expired"]:
        fire, reason = True, "reformed"

    if fire:
        _cooldown_gex_vel["last_grade"] = grade
        _cooldown_gex_vel["last_gap_to_lis"] = gap
        _cooldown_gex_vel["setup_expired"] = False

    return fire, reason


def mark_gex_velocity_expired():
    _cooldown_gex_vel["setup_expired"] = True
    _cooldown_gex_vel["last_grade"] = None
    _cooldown_gex_vel["last_gap_to_lis"] = None


# ── AG Short evaluation ────────────────────────────────────────────────────

def evaluate_ag_short(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings,
                      vix=None):
    """
    Evaluate AG Short setup. Returns a result dict or None.

    Grading v2 (Mar 22): Data-driven scoring from 52 trades.
    Old grading was anti-predictive (r=-0.105). New grading r=+0.405.
    Key insight: paradigm subtype, VIX, time of day matter most.
    support_score had r=-0.422 (strongest negative in any setup).
    """
    if not settings.get("ag_short_enabled", True):
        return None

    # Base conditions (unchanged)
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

    downside = min(downside_target, downside_gex)
    rr_ratio = downside / gap if gap > 0 else 99
    first_hour = is_first_hour()

    # Keep old component scores for portal display
    brackets = settings.get("brackets", DEFAULT_SETUP_SETTINGS["brackets"])
    support_score = score_component_max(gap, brackets.get("support", DEFAULT_SETUP_SETTINGS["brackets"]["support"]))
    upside_score = score_component_min(downside, brackets.get("upside", DEFAULT_SETUP_SETTINGS["brackets"]["upside"]))
    floor_cluster_score = score_component_max(abs(lis - max_plus_gex), brackets.get("floor_cluster", DEFAULT_SETUP_SETTINGS["brackets"]["floor_cluster"]))
    target_cluster_score = score_component_max(abs(target - max_minus_gex), brackets.get("target_cluster", DEFAULT_SETUP_SETTINGS["brackets"]["target_cluster"]))
    rr_score = score_component_min(rr_ratio, brackets.get("rr", DEFAULT_SETUP_SETTINGS["brackets"]["rr"]))

    # --- Scoring v2: Data-driven (4 components, max 90) ---
    # Based on 52-trade analysis. Old r=-0.105, New r=+0.405.

    # 1. Paradigm subtype (0-25) — AG-PURE 62% WR best, AG-TARGET 39% WR worst
    _p = str(paradigm)
    if _p in ("AG-PURE", "AG-LIS"):
        para_v2 = 25
    elif _p == "AG-TARGET":
        para_v2 = 0
    else:
        para_v2 = 12

    # 2. VIX (0-20) — higher VIX = better for shorts (r=+0.214)
    #    VIX<20: 13% WR (-60 pts). VIX>=25: 62% WR
    _vix = float(vix) if vix is not None else 22.0
    if _vix >= 25:
        vix_v2 = 20
    elif _vix >= 20:
        vix_v2 = 12
    else:
        vix_v2 = 0   # VIX<20 = 13% WR, terrible

    # 3. Time of day (0-25) — 10xx=79% WR, 12xx=20% WR
    now_et = datetime.now(NY)
    et_h = now_et.hour
    if et_h <= 10:
        time_v2 = 25   # 79% WR
    elif et_h == 11:
        time_v2 = 18   # 60% WR
    elif et_h == 13:
        time_v2 = 12   # 50% WR
    elif et_h == 12:
        time_v2 = 0    # 20% WR death zone
    else:
        time_v2 = 5

    # 4. Downside room (0-20) — basic structural quality
    if downside >= 20:
        down_v2 = 20
    elif downside >= 15:
        down_v2 = 15
    elif downside >= 10:
        down_v2 = 8
    else:
        down_v2 = 0

    composite = para_v2 + vix_v2 + time_v2 + down_v2
    composite = max(0, min(100, composite))

    # Grade thresholds
    if composite >= 70:
        grade = "A+"
    elif composite >= 55:
        grade = "A"
    elif composite >= 40:
        grade = "B"
    elif composite >= 25:
        grade = "C"
    else:
        grade = "LOG"

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
        _cooldown = {"last_grade": None, "last_gap_to_lis": None, "setup_expired": False, "last_date": today, "_gone_count": 0}

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
    """Cooldown gate for AG Short — grade/gap/expiry logic + 15-min time floor.
    Returns (fire, reason)."""
    global _cooldown_ag

    now = datetime.now(NY)
    today = now.date()
    if _cooldown_ag["last_date"] != today:
        _cooldown_ag = {"last_grade": None, "last_gap_to_lis": None, "setup_expired": False,
                        "last_date": today, "_gone_count": 0, "last_fire_time": None}

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

    # ── 15-min time floor: block flicker re-fires regardless of reason ──
    if fire and _cooldown_ag.get("last_fire_time") is not None:
        elapsed = (now - _cooldown_ag["last_fire_time"]).total_seconds() / 60
        if elapsed < AG_MIN_COOLDOWN_MINUTES:
            return False, None

    if fire:
        _cooldown_ag["last_grade"] = grade
        _cooldown_ag["last_gap_to_lis"] = gap
        _cooldown_ag["setup_expired"] = False
        _cooldown_ag["last_fire_time"] = now

    return fire, reason


def mark_ag_expired():
    """Call when paradigm loses AG or gap > 20."""
    _cooldown_ag["setup_expired"] = True
    _cooldown_ag["last_grade"] = None
    _cooldown_ag["last_gap_to_lis"] = None


# ── Message formatting ─────────────────────────────────────────────────────

def format_setup_message(result, alignment=None):
    """Format a concise Telegram HTML message for GEX Long."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    msg = f"{grade_emoji} <b>GEX Long LONG [{result['grade']}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} → {result['target']:.0f} | SL {result['spot'] - 8:.0f} (8pt) | Trail\n"
    msg += f"{result['paradigm']} | LIS {result['lis']:.0f} | +GEX {result['max_plus_gex']:.0f} | -GEX {result['max_minus_gex']:.0f}"
    return msg


def format_ag_short_message(result, alignment=None):
    """Format a concise Telegram HTML message for AG Short."""
    grade_emoji = {"A+": "🟢", "A": "🔵", "A-Entry": "🟡"}.get(result["grade"], "⚪")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    msg = f"{grade_emoji} <b>AG Short [{result['grade']}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} → {result['target']:.0f} | SL {result['spot'] + 10:.0f} (10pt) | Trail\n"
    msg += f"{result['paradigm']} | LIS {result['lis']:.0f} | +GEX {result['max_plus_gex']:.0f} | -GEX {result['max_minus_gex']:.0f}"
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


# ── GEX LIS velocity tracker ─────────────────────────────────────────────
# Detects rapid LIS convergence toward spot (e.g., LIS surges +100 pts in
# 20 min).  When LIS is moving fast, price won't pull back to test it before
# rallying — so we widen the gap allowance proportionally.

_gex_lis_history = deque(maxlen=20)   # (datetime, lis_value) — ~40 min at 2-min cycles
_gex_lis_last_date = None


def update_gex_lis_tracker(lis, paradigm):
    """Append LIS reading during GEX paradigm.  Resets daily and on paradigm change."""
    global _gex_lis_last_date
    today = datetime.now(NY).date()
    if _gex_lis_last_date != today:
        _gex_lis_history.clear()
        _gex_lis_last_date = today

    if not paradigm or "GEX" not in str(paradigm).upper():
        _gex_lis_history.clear()
        return

    if lis is not None:
        _gex_lis_history.append((datetime.now(NY), lis))


def get_gex_lis_velocity():
    """
    Calculate how far LIS moved over the buffer window.
    Returns (lis_move, n_readings).
    lis_move > 0 means LIS is surging UP (bullish convergence toward spot).
    """
    if len(_gex_lis_history) < 3:
        return 0, len(_gex_lis_history)
    values = [v for _, v in _gex_lis_history]
    lis_move = values[-1] - values[0]
    return round(lis_move, 2), len(values)


def _lis_velocity_gap_bonus():
    """
    Gap widening based on rapid LIS convergence.
    Stepped thresholds — bigger LIS surge = more gap room.
    """
    lis_move, n = get_gex_lis_velocity()
    if n < 3 or lis_move < 25:
        return 0, lis_move
    if lis_move >= 80:
        return 7, lis_move
    if lis_move >= 50:
        return 5, lis_move
    return 3, lis_move    # lis_move >= 25


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

def format_bofa_scalp_message(result, alignment=None):
    """Format a concise Telegram HTML message for BofA Scalp."""
    dir_emoji = "🔵" if result["direction"] == "long" else "🔴"
    dir_label = "LONG" if result["direction"] == "long" else "SHORT"
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    lis_lo = result.get("lis_lower", 0)
    lis_hi = result.get("lis_upper", 0)
    width = result.get("bofa_lis_width", 0)
    msg = f"{dir_emoji} <b>BofA {dir_label} [{result['grade']}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} → {(result.get('bofa_target_level') or 0):.0f} (+10) | SL {(result.get('bofa_stop_level') or 0):.0f} | {result.get('bofa_max_hold_minutes') or 30}m hold\n"
    msg += f"LIS {lis_lo:.0f}–{lis_hi:.0f} ({width:.0f}pt)"
    return msg


# ── ES Absorption — defaults and state ─────────────────────────────────────
# Restored from original (pre-CVD-rewrite). With alignment >= 0 filter:
# 76 trades, 67% WR, +117.6 pts. At alignment +3: 25 trades, 76% WR, +88.1 pts.

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




def grade_absorption_v3(direction, alignment, vol_ratio, div_raw, vol_raw, dd_raw, lis_raw):
    """Standalone grading for ES Absorption v3 — direction-aware scoring.

    Returns (grade, score) tuple. Can be called from main.py to re-grade
    after alignment is computed.

    399-trade analysis (Feb 19 - Apr 13, 2026). r=+0.184, cross-validated r=+0.141.
    """
    _is_bull = (direction == "bullish")
    _align = alignment if alignment is not None else 0

    s = 50  # base

    # 1. Direction base
    if _is_bull:
        s += 8

    # 2. Alignment (direction-dependent)
    if _is_bull and _align >= 1:
        s += 4
    if not _is_bull and _align <= -2:
        s += 7
    if not _is_bull and _align >= 1:
        s -= 4

    # 3. Time of day (bearish PM is toxic: 37% WR after 14:30)
    now_et = datetime.now(NY)
    et_hour = now_et.hour + now_et.minute / 60.0
    if not _is_bull and et_hour >= 14.5:
        s -= 12
    elif not _is_bull and et_hour < 12:
        s += 4
    elif _is_bull and et_hour >= 14.5:
        s -= 3

    # 4. Volume ratio (bearish high-vol = bonus)
    if vol_ratio is not None:
        if not _is_bull and vol_ratio >= 2.5:
            s += 8
        elif not _is_bull and vol_ratio < 1.75:
            s -= 4

    # 5. div_raw: OPPOSITE per direction
    if div_raw is not None:
        if _is_bull and div_raw >= 2:
            s += 4
        elif not _is_bull and div_raw >= 3:
            s -= 5
        elif not _is_bull and div_raw <= 1:
            s += 3

    # 6. vol_raw: OPPOSITE per direction
    if vol_raw is not None:
        if _is_bull and vol_raw >= 3:
            s -= 5
        elif not _is_bull and vol_raw >= 2:
            s += 3

    # 7. DD alignment
    if dd_raw is not None and dd_raw >= 1:
        s += 4

    # 8. LIS proximity (hurts bulls)
    if lis_raw is not None:
        if _is_bull and lis_raw >= 1:
            s -= 4

    s = max(0, min(100, s))

    if s >= 70:
        grade = "A+"
    elif s >= 62:
        grade = "A"
    elif s >= 54:
        grade = "B"
    elif s >= 46:
        grade = "C"
    else:
        grade = "LOG"

    return grade, round(s, 1)


def evaluate_absorption(bars, volland_stats, settings, spx_spot=None, vix=None, alignment=None):
    """
    Evaluate ES Absorption setup on completed range bars.

    Parameters:
      bars: list of bar dicts (must have idx, open, high, low, close, volume, cvd, status)
      volland_stats: dict with keys paradigm, delta_decay_hedging, lines_in_sand (or None)
      settings: setup settings dict with abs_* keys
      spx_spot: (unused, kept for API compat)
      vix: current VIX value (for grading)
      alignment: greek alignment score (-3 to +3) from main.py

    Returns result dict or None.

    Grading v3 (Apr 13): Direction-aware scoring from 399 trades.
    Key insight: div_raw and vol_raw have OPPOSITE effects for bulls vs bears.
    v2 was anti-predictive (r=-0.024). v3: r=+0.184, cross-validated r=+0.141.
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

    # --- Normalize raw scores to 0-100 (kept for portal display) ---
    div_score = {0: 0, 1: 25, 2: 50, 3: 75, 4: 100}.get(div_raw, 0)
    vol_score = {1: 33, 2: 67, 3: 100}.get(vol_raw, 33)
    dd_score = 100 if dd_raw else 0
    para_score_raw = 100 if para_raw else 0
    lis_score = {0: 0, 1: 50, 2: 100}.get(lis_raw, 0)

    # --- Scoring v3: Direction-aware (uses standalone grade_absorption_v3) ---
    grade, composite = grade_absorption_v3(
        direction, alignment, vol_ratio, div_raw, vol_raw, dd_raw, lis_raw)

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
        # Column mapping (v2): support=divergence, upside=volume, floor=dd, target=paradigm, rr=lis
        "support_score": div_score,
        "upside_score": vol_score,
        "floor_cluster_score": dd_score,
        "target_cluster_score": para_score_raw,
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
    cooldown = 10

    if direction == "bullish":
        if bar_idx - _cooldown_absorption["last_bullish_bar"] < cooldown:
            return False, None
        _cooldown_absorption["last_bullish_bar"] = bar_idx
    else:
        if bar_idx - _cooldown_absorption["last_bearish_bar"] < cooldown:
            return False, None
        _cooldown_absorption["last_bearish_bar"] = bar_idx

    return True, "new"


def format_absorption_message(result, alignment=None):
    """Format a Telegram HTML message for ES Absorption setup."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result["grade"]
    score = result["score"]
    align_str = f" align {alignment:+d}" if alignment is not None else ""

    parts = [
        f"{side_emoji} <b>ES Abs {side_label} [{grade}]{align_str}</b>",
        f"ES {result['abs_es_price']:.2f} | Score {score:.0f} | Vol {result['abs_vol_ratio']:.1f}x",
    ]

    extras = []
    if result.get("dd_raw"):
        extras.append("DD")
    if result.get("para_raw"):
        extras.append("Para")
    if result.get("lis_raw") and result.get("lis_val") is not None:
        extras.append(f"LIS {result['lis_dist']:.0f}pt")
    if extras:
        parts.append(" | ".join(extras))

    return "\n".join(parts)


# ── Single-Bar Absorption (LOG-ONLY) — defaults and state ──────────────────

DEFAULT_SINGLE_BAR_ABS_SETTINGS = {
    "single_bar_abs_enabled": True,
    "sba_vol_mult": 2.0,          # trigger bar volume >= N x 20-bar avg
    "sba_delta_mult": 2.0,        # trigger bar |delta| >= N x 20-bar avg |delta|
    "sba_cvd_lookback": 8,        # bars to compute CVD trend
    "sba_cvd_threshold": 0,       # min |CVD trend| (0 = just require direction alignment)
    "sba_cooldown_bars": 10,      # min bars between same-direction signals
    "sba_stop_pts": 8,
    "sba_target_pts": 10,
}

_cooldown_single_bar_abs = {
    "last_bullish_bar": -100,
    "last_bearish_bar": -100,
    "last_checked_idx": -1,
    "last_date": None,
}

# ── VIX Divergence state ──────────────────────────────────────────────
_vix_history: list = []  # list of (timestamp_str, vix, spot) tuples
_cooldown_vix_divergence = {
    "last_date": "",
    "last_long_time": None,
    "last_short_time": None,
}

# ── IV Momentum state ─────────────────────────────────────────────────
# Apollo's insight: vol-confirmed momentum shorts.
# Track per-strike put IV over time to detect trend + vol alignment.
_iv_momentum_history: list = []  # list of (timestamp_str, spot, {strike: p_iv}) tuples
_cooldown_iv_momentum = {
    "last_date": "",
    "last_short_time": None,
}

DEFAULT_IV_MOMENTUM_SETTINGS = {
    "ivm_lookback_min": 10,        # lookback window (minutes)
    "ivm_min_spot_drop": 5,        # minimum spot drop (pts) to confirm downtrend
    "ivm_min_iv_rise": 0.05,       # minimum avg put IV rise to confirm vol buying
    "ivm_cooldown_min": 30,        # minutes between signals
    "ivm_stop_pts": 8,             # stop loss
    "ivm_target_pts": 20,          # take profit
    "ivm_market_start": "10:00",   # skip the open (11 ET = best hour, skip 9:30-10)
    "ivm_market_end": "15:50",     # stop before close
}


# ── Vanna Butterfly state ──────────────────────────────────────────────
# 30pt call butterfly centered on max absolute 0DTE vanna strike.
# Entered at ~15:00 ET, held to close. Backtest: 59% WR, PF 3.1x.
_cooldown_vanna_butterfly = {
    "last_date": "",
    "fired": False,
}


def reset_single_bar_abs_session():
    """Reset single-bar absorption detector state for a new ES session."""
    _cooldown_single_bar_abs["last_bullish_bar"] = -100
    _cooldown_single_bar_abs["last_bearish_bar"] = -100
    _cooldown_single_bar_abs["last_checked_idx"] = -1


def evaluate_single_bar_absorption(bars, volland_stats, settings, spx_spot=None, cooldown_state=None):
    """
    Detect single-bar absorption: bar closes against delta direction.

    Bearish: bar closes RED despite strongly POSITIVE delta
      -> passive sellers absorbing aggressive buyers -> SHORT signal
    Bullish: bar closes GREEN despite strongly NEGATIVE delta
      -> passive buyers absorbing aggressive sellers -> LONG signal

    Requires CVD trend alignment (buyers exhausting into top, sellers into bottom)
    and SVB >= 0 (normal market correlation).

    Graded A+/A/B/C based on volume, delta, CVD trend, and Volland confluence.
    cooldown_state: optional dict for dedup (default: _cooldown_single_bar_abs).
                    Pass _cooldown_sb10_abs for 10-pt bar variant.
    """
    cd = cooldown_state or _cooldown_single_bar_abs

    if not settings.get("single_bar_abs_enabled", True):
        return None

    vol_mult = settings.get("sba_vol_mult", DEFAULT_SINGLE_BAR_ABS_SETTINGS["sba_vol_mult"])
    delta_mult = settings.get("sba_delta_mult", DEFAULT_SINGLE_BAR_ABS_SETTINGS["sba_delta_mult"])
    cvd_lookback = settings.get("sba_cvd_lookback", DEFAULT_SINGLE_BAR_ABS_SETTINGS["sba_cvd_lookback"])
    cooldown_bars = settings.get("sba_cooldown_bars", DEFAULT_SINGLE_BAR_ABS_SETTINGS["sba_cooldown_bars"])
    vol_window = 20

    min_bars = vol_window + cvd_lookback
    closed = [b for b in bars if b.get("status") == "closed"]
    if len(closed) < min_bars:
        return None

    trigger = closed[-1]
    trigger_idx = trigger["idx"]

    # Dedup: skip if already checked this bar
    if trigger_idx <= cd["last_checked_idx"]:
        return None
    cd["last_checked_idx"] = trigger_idx

    # --- Volume gate ---
    recent_vols = [b["volume"] for b in closed[-(vol_window + 1):-1]]
    if not recent_vols:
        return None
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < vol_mult:
        return None

    # --- Delta gate ---
    recent_deltas = [abs(b.get("delta", 0)) for b in closed[-(vol_window + 1):-1]]
    delta_avg = sum(recent_deltas) / len(recent_deltas) if recent_deltas else 0
    if delta_avg <= 0:
        return None
    delta_ratio = abs(trigger.get("delta", 0)) / delta_avg
    if delta_ratio < delta_mult:
        return None

    # --- Single-bar absorption check ---
    bar_delta = trigger.get("delta", 0)
    is_red = trigger["close"] < trigger["open"]
    is_green = trigger["close"] > trigger["open"]

    direction = None
    if is_red and bar_delta > 0:
        direction = "bearish"
    elif is_green and bar_delta < 0:
        direction = "bullish"
    if direction is None:
        return None

    # --- CVD trend alignment (8-bar) ---
    # Bearish absorption: CVD should be rising into the top (buyers exhausting)
    # Bullish absorption: CVD should be falling into the bottom (sellers exhausting)
    cvd_start = closed[-(cvd_lookback + 1)]["cvd"]
    cvd_end = trigger["cvd"]
    cvd_trend = cvd_end - cvd_start

    cvd_threshold = settings.get("sba_cvd_threshold", 0)
    if direction == "bearish" and cvd_trend <= cvd_threshold:
        return None  # CVD not rising -> no exhaustion
    if direction == "bullish" and cvd_trend >= -cvd_threshold:
        return None  # CVD not falling -> no exhaustion

    # --- SVB filter (from Volland stats) ---
    svb_val = None
    paradigm_str = ""
    dd_str = ""
    dd_numeric = 0
    charm_val = None
    lis_val = None
    lis_dist = None

    if volland_stats and volland_stats.get("has_statistics"):
        paradigm_str = (volland_stats.get("paradigm") or "").upper()
        dd_str = volland_stats.get("delta_decay_hedging") or ""
        # Parse DD numeric
        _dd_clean = dd_str.replace("$", "").replace(",", "")
        try:
            dd_numeric = float(_dd_clean)
        except (ValueError, TypeError):
            dd_numeric = 0
        # Parse charm
        _charm_raw = volland_stats.get("aggregatedCharm")
        if _charm_raw is not None:
            try:
                charm_val = float(_charm_raw)
            except (ValueError, TypeError):
                pass
        # Parse LIS
        lis_raw_str = volland_stats.get("lines_in_sand") or ""
        lis_match = re.search(r'[\d,]+\.?\d*', lis_raw_str.replace(',', ''))
        if lis_match:
            lis_val = float(lis_match.group())
            lis_dist = abs(trigger["close"] - lis_val)
        # Parse SVB
        _svb_raw = volland_stats.get("spot_vol_beta")
        if _svb_raw and isinstance(_svb_raw, dict):
            try:
                svb_val = float(_svb_raw.get("correlation"))
            except (ValueError, TypeError):
                pass
        elif _svb_raw is not None:
            try:
                svb_val = float(_svb_raw)
            except (ValueError, TypeError):
                pass

    # Block negative SVB (dislocation = unreliable absorption)
    if svb_val is not None and svb_val < 0:
        return None

    # --- Price trend context (8-bar) ---
    price_trend = trigger["close"] - closed[-(cvd_lookback + 1)]["close"]

    # --- Scoring (0-100) ---
    # Volume strength (0-25): how much above threshold
    vol_score = min(25, int((vol_ratio - vol_mult) / vol_mult * 25))
    # Delta strength (0-25): how much above threshold
    delta_score = min(25, int((delta_ratio - delta_mult) / delta_mult * 25))
    # CVD trend strength (0-20): stronger trend = more exhaustion
    cvd_range = max(abs(cvd_trend), 1)
    cvd_score = min(20, int(cvd_range / 500 * 20))
    # Volland confluence (0-30): DD alignment, paradigm, LIS proximity
    confluence_score = 0
    if dd_numeric != 0:
        dd_aligns = (direction == "bullish" and dd_numeric > 0) or (direction == "bearish" and dd_numeric < 0)
        if dd_aligns:
            confluence_score += 10
    if paradigm_str:
        para_aligns = (direction == "bullish" and "GEX" in paradigm_str) or (direction == "bearish" and "AG" in paradigm_str)
        if para_aligns:
            confluence_score += 10
    if lis_dist is not None:
        if lis_dist <= 5:
            confluence_score += 10
        elif lis_dist <= 15:
            confluence_score += 5

    total_score = vol_score + delta_score + cvd_score + confluence_score

    # Grade thresholds
    if total_score >= 70:
        grade = "A+"
    elif total_score >= 50:
        grade = "A"
    elif total_score >= 30:
        grade = "B"
    else:
        grade = "C"

    return {
        "setup_name": "SB Absorption",
        "direction": direction,
        "grade": grade,
        "score": total_score,
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
        # Scores: reuse column mapping for setup_log compatibility
        "support_score": round(vol_ratio, 1),      # vol ratio
        "upside_score": round(delta_ratio, 1),      # delta ratio
        "floor_cluster_score": bar_delta,            # raw bar delta
        "target_cluster_score": cvd_trend,           # CVD trend
        "rr_score": round(svb_val, 2) if svb_val is not None else 0,
        # Single-bar-specific extras
        "bar_idx": trigger_idx,
        "abs_es_price": round(trigger["close"], 2),
        "abs_vol_ratio": round(vol_ratio, 1),
        "vol_trigger": trigger["volume"],
        "bar_delta": bar_delta,
        "delta_ratio": round(delta_ratio, 1),
        "cvd_trend": cvd_trend,
        "price_trend": round(price_trend, 2),
        "svb": round(svb_val, 2) if svb_val is not None else None,
        "dd_hedging": dd_str,
        "dd_numeric": dd_numeric,
        "charm": charm_val,
        "lis_val": lis_val,
        "lis_dist": round(lis_dist, 1) if lis_dist is not None else None,
        "ts": trigger.get("ts_end", ""),
        "cvd": trigger["cvd"],
        "high": trigger["high"],
        "low": trigger["low"],
    }


def should_notify_single_bar_abs(result):
    """Cooldown gate for Single-Bar Absorption. Bar-index based."""
    today = datetime.now(NY).date()
    if _cooldown_single_bar_abs["last_date"] != today:
        _cooldown_single_bar_abs["last_bullish_bar"] = -100
        _cooldown_single_bar_abs["last_bearish_bar"] = -100
        _cooldown_single_bar_abs["last_date"] = today

    bar_idx = result["bar_idx"]
    direction = result["direction"]
    cooldown = 10

    if direction == "bullish":
        if bar_idx - _cooldown_single_bar_abs["last_bullish_bar"] < cooldown:
            return False, None
        _cooldown_single_bar_abs["last_bullish_bar"] = bar_idx
    else:
        if bar_idx - _cooldown_single_bar_abs["last_bearish_bar"] < cooldown:
            return False, None
        _cooldown_single_bar_abs["last_bearish_bar"] = bar_idx

    return True, "new"


def format_single_bar_abs_message(result, alignment=None):
    """Format Telegram HTML message for Single-Bar Absorption."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result.get("grade", "?")
    score = result.get("score", 0)
    align_str = f" align {alignment:+d}" if alignment is not None else ""

    parts = [
        f"{side_emoji} <b>SB Abs {side_label} [{grade}]{align_str}</b>",
        f"ES {result['abs_es_price']:.2f} | Score {score} | Vol {result['abs_vol_ratio']:.1f}x | Delta {result['bar_delta']:+d}({result['delta_ratio']:.1f}x)",
    ]

    extras = []
    if result.get("svb") is not None:
        extras.append(f"SVB {result['svb']:.2f}")
    if result.get("paradigm"):
        extras.append(result["paradigm"])
    if extras:
        parts.append(" | ".join(extras))

    return "\n".join(parts)


# ── SB10 Absorption (10-pt bars) — cooldown and formatting ─────────────────

_cooldown_sb10_abs = {
    "last_bullish_bar": -100,
    "last_bearish_bar": -100,
    "last_checked_idx": -1,
    "last_date": None,
}


def reset_sb10_abs_session():
    """Reset SB10 absorption detector state for a new ES session."""
    _cooldown_sb10_abs["last_bullish_bar"] = -100
    _cooldown_sb10_abs["last_bearish_bar"] = -100
    _cooldown_sb10_abs["last_checked_idx"] = -1


def should_notify_sb10_abs(result):
    """Cooldown gate for SB10 Absorption. Bar-index based (5-bar cooldown for 10-pt bars)."""
    today = datetime.now(NY).date()
    if _cooldown_sb10_abs["last_date"] != today:
        _cooldown_sb10_abs["last_bullish_bar"] = -100
        _cooldown_sb10_abs["last_bearish_bar"] = -100
        _cooldown_sb10_abs["last_date"] = today

    bar_idx = result["bar_idx"]
    direction = result["direction"]
    cooldown = 5  # 5 bars of 10-pt = 50 pts between signals

    if direction == "bullish":
        if bar_idx - _cooldown_sb10_abs["last_bullish_bar"] < cooldown:
            return False, None
        _cooldown_sb10_abs["last_bullish_bar"] = bar_idx
    else:
        if bar_idx - _cooldown_sb10_abs["last_bearish_bar"] < cooldown:
            return False, None
        _cooldown_sb10_abs["last_bearish_bar"] = bar_idx

    return True, "new"


def format_sb10_abs_message(result, alignment=None):
    """Format Telegram HTML message for SB10 Absorption (10-pt bars)."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result.get("grade", "?")
    score = result.get("score", 0)
    align_str = f" align {alignment:+d}" if alignment is not None else ""

    parts = [
        f"{side_emoji} <b>SB10 Abs {side_label} [{grade}]{align_str}</b>",
        f"ES {result['abs_es_price']:.2f} | Score {score} | Vol {result['abs_vol_ratio']:.1f}x | Delta {result['bar_delta']:+d}({result['delta_ratio']:.1f}x)",
    ]

    extras = []
    if result.get("svb") is not None:
        extras.append(f"SVB {result['svb']:.2f}")
    if result.get("paradigm"):
        extras.append(result["paradigm"])
    if extras:
        parts.append(" | ".join(extras))

    return "\n".join(parts)


# ── SB2 Absorption — two-bar absorption (flush + recovery) ────────────────

DEFAULT_SB2_ABS_SETTINGS = {
    "sb2_enabled": True,
    "sb2_vol_mult": 1.2,          # flush bar volume >= N x 20-bar avg
    "sb2_delta_mult": 1.3,        # flush bar |delta| >= N x 20-bar avg |delta|
    "sb2_gate_mode": "OR",        # "OR" = vol OR delta passes, "AND" = both must pass
    "sb2_recovery_pct": 0.60,     # recovery bar must reverse >= 60% of flush bar range
    "sb2_cooldown_bars": 20,      # min bars between same-direction signals (was 10)
    "sb2_stop_pts": 10,
    "sb2_target_pts": 20,         # SL=10/T=20 (Apr 13 study: 89t, RunDD 60, PF 1.18)
    "sb2_block_before_et": "09:45", # block signals before 9:45 ET (open noise)
    "sb2_block_after_et": "15:00",  # block signals after 15:00 ET (weak edge)
}

_cooldown_sb2_abs = {
    "last_bullish_bar": -100,
    "last_bearish_bar": -100,
    "last_checked_idx": -1,
    "last_date": None,
}


def reset_sb2_abs_session():
    """Reset SB2 absorption detector state for a new ES session."""
    _cooldown_sb2_abs["last_bullish_bar"] = -100
    _cooldown_sb2_abs["last_bearish_bar"] = -100
    _cooldown_sb2_abs["last_checked_idx"] = -1


def evaluate_sb2_absorption(bars, volland_stats, settings, spx_spot=None, cooldown_state=None):
    """Detect two-bar absorption: flush bar + recovery bar.

    Bullish: Bar N-1 sells down hard (negative delta, full range drop),
             Bar N recovers >= 60% of the move -> sellers absorbed -> LONG
    Bearish: Bar N-1 buys up hard (positive delta, full range rise),
             Bar N pulls back >= 60% of the move -> buyers absorbed -> SHORT

    Gate: OR mode — flush bar volume >= 1.2x avg OR |delta| >= 1.3x avg.
    Catches fast bars with strong delta but low absolute volume.
    SVB < 0 blocks (market dislocation). Cooldown 20 bars. Time 9:45-15:00 ET.
    Backtest: 132 sig/22d, 47.7% WR, +260 pts, PF 1.52, MaxDD -80.
    """
    cs = cooldown_state or _cooldown_sb2_abs
    if not bars or len(bars) < 22:  # need at least 20 lookback + 2 bars
        return None
    if not settings.get("sb2_enabled", True):
        return None

    # Daily reset
    today = datetime.now(NY).date()
    if cs.get("last_date") != today:
        cs["last_bullish_bar"] = -100
        cs["last_bearish_bar"] = -100
        cs["last_checked_idx"] = -1
        cs["last_date"] = today

    recovery_bar = bars[-1]  # bar N (just completed)
    flush_bar = bars[-2]     # bar N-1

    bar_idx = recovery_bar.get("idx", -1)
    if bar_idx <= cs.get("last_checked_idx", -1):
        return None
    cs["last_checked_idx"] = bar_idx

    # Skip open/incomplete bars
    if recovery_bar.get("status") == "open" or flush_bar.get("status") == "open":
        return None

    # Time gate: block signals before/after configured cutoffs
    now_et = datetime.now(NY)
    block_before = settings.get("sb2_block_before_et", "09:45")
    if block_before:
        _h, _m = (int(x) for x in block_before.split(":"))
        if now_et.time() < dtime(_h, _m):
            return None
    block_after = settings.get("sb2_block_after_et", "15:00")
    if block_after:
        _h, _m = (int(x) for x in block_after.split(":"))
        if now_et.time() >= dtime(_h, _m):
            return None

    # ── Volume + Delta gate on flush bar (OR mode) ──
    vol_mult = settings.get("sb2_vol_mult", 1.2)
    delta_mult = settings.get("sb2_delta_mult", 1.3)
    gate_mode = settings.get("sb2_gate_mode", "OR")

    lookback = bars[-22:-2]  # 20 bars before flush bar
    volumes = [b.get("volume", 0) for b in lookback if b.get("volume", 0) > 0]
    if not volumes:
        return None
    avg_vol = sum(volumes) / len(volumes)
    flush_vol = flush_bar.get("volume", 0)
    vol_pass = avg_vol > 0 and flush_vol >= avg_vol * vol_mult
    vol_ratio = round(flush_vol / avg_vol, 1) if avg_vol > 0 else 0.0

    deltas = [abs(b.get("delta", 0)) for b in lookback if b.get("delta", 0) != 0]
    avg_delta = sum(deltas) / len(deltas) if deltas else 1
    flush_delta = flush_bar.get("delta", 0)
    delta_pass = avg_delta > 0 and abs(flush_delta) >= avg_delta * delta_mult
    delta_ratio = round(abs(flush_delta) / avg_delta, 1) if avg_delta > 0 else 0.0

    if gate_mode == "OR":
        if not (vol_pass or delta_pass):
            return None
    else:  # AND
        if not (vol_pass and delta_pass):
            return None

    # ── Flush direction + recovery check ──
    flush_open = flush_bar.get("open", 0)
    flush_close = flush_bar.get("close", 0)
    flush_move = flush_close - flush_open  # negative = sold down, positive = bought up
    flush_range = abs(flush_move)
    if flush_range < 0.5:  # trivial move
        return None

    recovery_close = recovery_bar.get("close", 0)
    recovery_pct_threshold = settings.get("sb2_recovery_pct", 0.70)

    if flush_delta < 0 and flush_move < 0:
        # Flush: sellers pushed price DOWN
        # Recovery: price must come back UP
        recovery_amount = recovery_close - flush_close
        if recovery_amount <= 0:
            return None
        recovery_pct = recovery_amount / flush_range
        if recovery_pct < recovery_pct_threshold:
            return None
        direction = "bullish"
    elif flush_delta > 0 and flush_move > 0:
        # Flush: buyers pushed price UP
        # Recovery: price must come back DOWN
        recovery_amount = flush_close - recovery_close
        if recovery_amount <= 0:
            return None
        recovery_pct = recovery_amount / flush_range
        if recovery_pct < recovery_pct_threshold:
            return None
        direction = "bearish"
    else:
        # Delta and price direction don't agree (not a clean flush)
        return None

    # ── SVB filter: block if negative (dislocation) ──
    svb_val = None
    if volland_stats and isinstance(volland_stats, dict):
        try:
            svb_raw = volland_stats.get("svb_correlation") or volland_stats.get("spotVolBeta") or volland_stats.get("spot_vol_beta")
            if isinstance(svb_raw, dict):
                svb_raw = svb_raw.get("correlation")
            if svb_raw is not None:
                svb_val = float(svb_raw)
                if svb_val < 0:
                    return None
        except (ValueError, TypeError):
            pass

    # ── Scoring (0-100) ──
    # Volume strength (0-25)
    vol_score = min(25, (vol_ratio - vol_mult) / vol_mult * 25) if vol_ratio > vol_mult else 0

    # Delta strength (0-25)
    delta_score = min(25, (delta_ratio - delta_mult) / delta_mult * 25) if delta_ratio > delta_mult else 0

    # Recovery completeness (0-20): 70% = 0, 100%+ = 20
    recovery_score = min(20, max(0, (recovery_pct - recovery_pct_threshold) / (1.0 - recovery_pct_threshold) * 20))

    # Volland confluence (0-30)
    volland_score = 0
    paradigm = None
    lis_val = None
    dd_hedging = None
    dd_numeric = None
    charm_val = None

    if volland_stats and isinstance(volland_stats, dict):
        paradigm = volland_stats.get("paradigm")
        try:
            lis_val = float(volland_stats.get("lis") or 0) or None
        except (ValueError, TypeError):
            pass
        dd_hedging = volland_stats.get("ddHedging") or volland_stats.get("dd_hedging")
        charm_val = volland_stats.get("aggregatedCharm")

        # DD alignment (+10)
        if dd_hedging:
            try:
                dd_str = str(dd_hedging).replace("$", "").replace(",", "").strip()
                neg = dd_str.startswith("-") or dd_str.startswith("(")
                dd_clean = dd_str.replace("-", "").replace("(", "").replace(")", "")
                dd_numeric = float(dd_clean) * (-1 if neg else 1)
            except (ValueError, TypeError):
                pass
            if dd_numeric is not None:
                if (direction == "bullish" and dd_numeric > 0) or \
                   (direction == "bearish" and dd_numeric < 0):
                    volland_score += 10

        # Paradigm alignment (+10)
        if paradigm:
            if (direction == "bullish" and "GEX" in paradigm) or \
               (direction == "bearish" and "AG" in paradigm):
                volland_score += 10

        # LIS proximity (+10 if within 5, +5 if within 15)
        if lis_val and spx_spot:
            lis_dist = abs(spx_spot - lis_val)
            if lis_dist <= 5:
                volland_score += 10
            elif lis_dist <= 15:
                volland_score += 5

    total_score = round(vol_score + delta_score + recovery_score + volland_score)

    # Grade
    if total_score >= 70:
        grade = "A+"
    elif total_score >= 50:
        grade = "A"
    elif total_score >= 30:
        grade = "B"
    else:
        grade = "C"

    entry_price = recovery_close

    return {
        "setup_name": "SB2 Absorption",
        "direction": direction,
        "grade": grade,
        "score": total_score,
        "paradigm": paradigm,
        "spot": spx_spot or 0,
        "lis": lis_val,
        "bar_idx": bar_idx,
        "flush_bar_idx": flush_bar.get("idx", bar_idx - 1),
        "abs_es_price": entry_price,
        "abs_vol_ratio": vol_ratio,
        "bar_delta": flush_delta,
        "delta_ratio": delta_ratio,
        "recovery_pct": round(recovery_pct, 2),
        "flush_open": flush_open,
        "flush_close": flush_close,
        "recovery_close": recovery_close,
        "cvd_trend": 0,  # not used for SB2
        "price_trend": 0,
        "svb": svb_val,
        "dd_hedging": dd_hedging,
        "dd_numeric": dd_numeric,
        "charm": charm_val,
        "vix": None,
        "overvix": None,
    }


def should_notify_sb2_abs(result):
    """Cooldown gate for SB2 Absorption. Bar-index based."""
    today = datetime.now(NY).date()
    if _cooldown_sb2_abs["last_date"] != today:
        _cooldown_sb2_abs["last_bullish_bar"] = -100
        _cooldown_sb2_abs["last_bearish_bar"] = -100
        _cooldown_sb2_abs["last_date"] = today

    bar_idx = result["bar_idx"]
    direction = result["direction"]
    cooldown = DEFAULT_SB2_ABS_SETTINGS.get("sb2_cooldown_bars", 20)

    if direction == "bullish":
        if bar_idx - _cooldown_sb2_abs["last_bullish_bar"] < cooldown:
            return False, None
        _cooldown_sb2_abs["last_bullish_bar"] = bar_idx
    else:
        if bar_idx - _cooldown_sb2_abs["last_bearish_bar"] < cooldown:
            return False, None
        _cooldown_sb2_abs["last_bearish_bar"] = bar_idx

    return True, "new"


def format_sb2_abs_message(result, alignment=None):
    """Format Telegram HTML message for SB2 Absorption (two-bar)."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result.get("grade", "?")
    score = result.get("score", 0)
    align_str = f" align {alignment:+d}" if alignment is not None else ""

    parts = [
        f"{side_emoji} <b>SB2 Abs {side_label} [{grade}]{align_str}</b>",
        f"ES {result['abs_es_price']:.2f} | Score {score} | Vol {result['abs_vol_ratio']:.1f}x | Delta {result['bar_delta']:+d}({result['delta_ratio']:.1f}x) | Rec {(result.get('recovery_pct') or 0):.0%}",
    ]

    extras = []
    if result.get("svb") is not None:
        extras.append(f"SVB {result['svb']:.2f}")
    if result.get("paradigm"):
        extras.append(result["paradigm"])
    if extras:
        parts.append(" | ".join(extras))

    return "\n".join(parts)


# ── Delta Absorption — delta-vs-price divergence on range bars ──────────────

DEFAULT_DELTA_ABS_SETTINGS = {
    "delta_absorption_enabled": True,
    "da_min_delta": 100,            # minimum |delta| to fire
    "da_doji_body": 1.0,            # T1: body < this = doji
    "da_afternoon_start": 12.5,     # T3: start hour (12:30)
    "da_afternoon_end": 15.0,       # T3: end hour (15:00)
    "da_afternoon_min_delta": 200,  # T3: minimum |delta|
    "da_dead_zone_start": 14.0,     # T3: skip 14:00-14:30
    "da_dead_zone_end": 14.5,
    "da_peak_ratio_cap": 2.5,       # max peak ratio (>= this = toxic)
    "da_trend_bars": 5,             # trend precondition window
    "da_trend_min": 3,              # min bars in opposite direction
    "da_cooldown_bars": 5,          # bars between same-direction signals
    "da_stop_pts": 8,
    "da_trail_gap": 8,
}

_cooldown_delta_abs = {
    "last_bullish_bar": -100,
    "last_bearish_bar": -100,
    "last_checked_idx": -1,
    "last_date": None,
    "sig_count_bull": 0,
    "sig_count_bear": 0,
}


def reset_delta_abs_session():
    """Reset Delta Absorption detector state for a new ES session."""
    _cooldown_delta_abs["last_bullish_bar"] = -100
    _cooldown_delta_abs["last_bearish_bar"] = -100
    _cooldown_delta_abs["last_checked_idx"] = -1
    _cooldown_delta_abs["sig_count_bull"] = 0
    _cooldown_delta_abs["sig_count_bear"] = 0


def evaluate_delta_absorption(bars, volland_stats, settings, spx_spot=None, vix=None):
    """
    Delta Absorption: detects delta-vs-price divergence on 5-pt range bars.

    Signal: bar delta opposes bar color (positive delta + red bar = bearish,
    negative delta + green bar = bullish). Doji bars use prior bar's color.

    Tiers:
      T1 (Doji): body < 1.0 pt, any time 9:30-15:00
      T3 (Afternoon): 12:30-15:00, |delta| >= 200, skip 14:00-14:30

    Filters: trend precondition (3/5 bars opposite), peak ratio < 2.5
    Trail: immediate stop = max(maxProfit - 8, -8)
    """
    if not settings.get("delta_absorption_enabled", True):
        return None

    closed = [b for b in bars if b.get("status") == "closed"]
    if len(closed) < 25:
        return None

    trigger = closed[-1]
    trigger_idx = trigger["idx"]

    # Dedup: skip if already checked this bar
    if trigger_idx <= _cooldown_delta_abs["last_checked_idx"]:
        return None
    _cooldown_delta_abs["last_checked_idx"] = trigger_idx

    # Daily reset
    today = datetime.now(NY).date()
    if _cooldown_delta_abs["last_date"] != today:
        _cooldown_delta_abs["last_bullish_bar"] = -100
        _cooldown_delta_abs["last_bearish_bar"] = -100
        _cooldown_delta_abs["sig_count_bull"] = 0
        _cooldown_delta_abs["sig_count_bear"] = 0
        _cooldown_delta_abs["last_date"] = today

    # Extract bar data
    o, h, l, c = trigger["open"], trigger["high"], trigger["low"], trigger["close"]
    delta = trigger.get("delta", 0)
    volume = trigger.get("volume", 0)
    cvd_open = trigger.get("cvd_open", 0)
    cvd_high = trigger.get("cvd_high", 0)
    cvd_low = trigger.get("cvd_low", 0)
    abs_delta = abs(delta)

    # Min delta gate
    min_delta = settings.get("da_min_delta", 100)
    if abs_delta < min_delta:
        return None

    # Bar properties
    color = "GREEN" if c >= o else "RED"
    body = abs(c - o)
    doji_thresh = settings.get("da_doji_body", 1.0)

    # Direction: delta opposes bar color
    direction = None
    if delta > 0 and color == "RED":
        direction = "bearish"
    elif delta < 0 and color == "GREEN":
        direction = "bullish"
    elif body <= doji_thresh and len(closed) >= 2:
        # Doji: delta opposes prior bar's trend direction
        prev = closed[-2]
        prev_color = "GREEN" if prev["close"] >= prev["open"] else "RED"
        if delta > 0 and prev_color == "GREEN":
            direction = "bearish"
        elif delta < 0 and prev_color == "RED":
            direction = "bullish"

    if direction is None:
        return None

    # Cooldown: bar-index based
    cooldown_bars = settings.get("da_cooldown_bars", 5)
    if direction == "bullish":
        if trigger_idx - _cooldown_delta_abs["last_bullish_bar"] < cooldown_bars:
            return None
    else:
        if trigger_idx - _cooldown_delta_abs["last_bearish_bar"] < cooldown_bars:
            return None

    # Trend precondition: >=3 of last 5 bars in opposite direction
    trend_window = settings.get("da_trend_bars", 5)
    trend_min = settings.get("da_trend_min", 3)
    if len(closed) >= trend_window + 1:
        lookback = closed[-(trend_window + 1):-1]  # last N bars before trigger
        greens = sum(1 for b in lookback if b["close"] >= b["open"])
        reds = len(lookback) - greens
        if direction == "bearish" and greens < trend_min:
            return None
        if direction == "bullish" and reds < trend_min:
            return None

    # Tier classification
    ts_str = trigger.get("ts_end", "") or trigger.get("ts_start", "")
    try:
        ts_time = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        if ts_time.tzinfo is None:
            ts_time = ts_time.replace(tzinfo=NY)
        else:
            ts_time = ts_time.astimezone(NY)
        tod = ts_time.hour + ts_time.minute / 60.0
    except Exception:
        tod = 12.0  # fallback

    is_doji = body < doji_thresh
    af_start = settings.get("da_afternoon_start", 12.5)
    af_end = settings.get("da_afternoon_end", 15.0)
    af_min_d = settings.get("da_afternoon_min_delta", 200)
    dz_start = settings.get("da_dead_zone_start", 14.0)
    dz_end = settings.get("da_dead_zone_end", 14.5)
    is_afternoon = af_start <= tod < af_end

    tier = 0
    if is_doji:
        tier = 1
    elif is_afternoon and abs_delta >= af_min_d:
        # Skip 14:00-14:30 dead zone
        if dz_start <= tod < dz_end:
            return None
        tier = 3

    if tier == 0:
        return None

    # Peak ratio filter
    peak_cap = settings.get("da_peak_ratio_cap", 2.5)
    if direction == "bearish":
        peak_abs = max(cvd_high - cvd_open, 0)
    else:
        peak_abs = abs(min(cvd_low - cvd_open, 0))
    peak_ratio = peak_abs / max(abs_delta, 1)
    if peak_ratio >= peak_cap:
        return None

    # Update signal count and cooldown
    if direction == "bullish":
        _cooldown_delta_abs["sig_count_bull"] += 1
        _cooldown_delta_abs["last_bullish_bar"] = trigger_idx
        sig_num = _cooldown_delta_abs["sig_count_bull"]
    else:
        _cooldown_delta_abs["sig_count_bear"] += 1
        _cooldown_delta_abs["last_bearish_bar"] = trigger_idx
        sig_num = _cooldown_delta_abs["sig_count_bear"]

    # ── V7 Grading (data-driven, 4 components, max 100) ──

    # Component 1: Delta magnitude (0-30)
    if 200 <= abs_delta < 500:
        g_delta = 30
    elif 500 <= abs_delta < 700:
        g_delta = 20
    elif 100 <= abs_delta < 200:
        g_delta = 15
    elif abs_delta >= 1000:
        g_delta = 10
    else:  # 700-1000
        g_delta = 3

    # Component 2: Body size (0-25)
    if 0.5 <= body < 1.0:
        g_body = 25
    elif 3.0 <= body < 4.0:
        g_body = 22
    elif 2.0 <= body < 3.0:
        g_body = 18
    elif 1.0 <= body < 2.0:
        g_body = 14
    elif body >= 4.0:
        g_body = 10
    else:  # body < 0.5
        g_body = 2

    # Component 3: Signal freshness (0-20)
    if sig_num <= 2:
        g_fresh = 20
    elif sig_num == 3:
        g_fresh = 8
    else:
        g_fresh = 2

    # Component 4: Time of day (0-25)
    if 12.5 <= tod < 13.0:
        g_time = 25
    elif 14.5 <= tod < 15.0:
        g_time = 20
    elif 10.0 <= tod < 11.0:
        g_time = 15
    elif 9.5 <= tod < 10.0:
        g_time = 15
    elif 11.0 <= tod < 12.0:
        g_time = 12
    elif 13.5 <= tod < 14.0:
        g_time = 12
    elif 13.0 <= tod < 13.5:
        g_time = 5
    else:  # 12:00-12:30 dead zone and other
        g_time = 0

    composite = g_delta + g_body + g_fresh + g_time

    # Grade thresholds (V3 optimized)
    if composite >= 85:
        grade = "A+"
    elif composite >= 70:
        grade = "A"
    elif composite >= 55:
        grade = "B"
    elif composite >= 40:
        grade = "C"
    else:
        grade = "LOG"

    # Extract Volland data
    paradigm = ""
    lis_val = None
    lis_dist = None
    dd_hedging = ""
    if volland_stats and volland_stats.get("has_statistics"):
        paradigm = volland_stats.get("paradigm", "")
        dd_hedging = volland_stats.get("delta_decay_hedging", "")
        lis_str = volland_stats.get("lines_in_sand", "")
        if lis_str:
            import re
            m = re.search(r"[\d,.]+", str(lis_str).replace(",", ""))
            if m:
                try:
                    lis_val = float(m.group())
                    lis_dist = abs(c - lis_val)
                except ValueError:
                    pass

    tier_label = "Doji" if tier == 1 else "Afternoon"

    return {
        "setup_name": "Delta Absorption",
        "direction": direction,
        "grade": grade,
        "score": composite,
        "paradigm": paradigm,
        "spot": c,
        "lis": lis_val,
        "target": None,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": round(lis_dist, 1) if lis_dist is not None else None,
        "upside": None,
        "rr_ratio": None,
        "first_hour": tod < 10.5,
        # Portal display scores
        "support_score": g_delta,
        "upside_score": g_body,
        "floor_cluster_score": g_fresh,
        "target_cluster_score": g_time,
        "rr_score": 0,
        # Delta Absorption specific
        "bar_idx": trigger_idx,
        "abs_es_price": c,
        "tier": tier,
        "tier_label": tier_label,
        "bar_delta": delta,
        "abs_delta": abs_delta,
        "body": round(body, 2),
        "peak_delta": int(peak_abs),
        "peak_ratio": round(peak_ratio, 2),
        "bar_volume": volume,
        "sig_num": sig_num,
        "dd_hedging": dd_hedging,
        "lis_val": lis_val,
        "lis_dist": round(lis_dist, 1) if lis_dist is not None else None,
        "ts": ts_str,
        "cvd": trigger.get("cvd", 0),
        "high": h,
        "low": l,
        "g_delta": g_delta,
        "g_body": g_body,
        "g_fresh": g_fresh,
        "g_time": g_time,
    }


def should_notify_delta_abs(result):
    """Cooldown gate for Delta Absorption. Already handled in evaluate function."""
    # Cooldown is managed inside evaluate_delta_absorption via bar-index spacing.
    # This function is called by main.py for the notification flow.
    return True, "new"


def format_delta_abs_message(result, alignment=None):
    """Format Telegram HTML message for Delta Absorption."""
    side_emoji = "\U0001f7e2" if result["direction"] == "bullish" else "\U0001f534"
    side_label = "BUY" if result["direction"] == "bullish" else "SELL"
    grade = result.get("grade", "?")
    score = result.get("score", 0)
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    tier_label = result.get("tier_label", "")

    parts = [
        f"{side_emoji} <b>Delta Abs {side_label} [{grade}]{align_str}</b>",
        f"ES {result['abs_es_price']:.2f} | Score {score} | {tier_label}",
        f"Delta {result['bar_delta']:+d} | Body {result['body']} | Peak {result['peak_ratio']:.1f}x | Sig #{result['sig_num']}",
    ]

    extras = []
    if result.get("paradigm"):
        extras.append(result["paradigm"])
    if result.get("lis_dist") is not None:
        extras.append(f"LIS {result['lis_dist']:.0f}pts")
    if extras:
        parts.append(" | ".join(extras))

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
    "skew_stop_pts": 14,            # fixed stop for outcome tracking (was 20, optimized Mar 18)
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


def evaluate_skew_charm(spot, skew_value, skew_change_pct, charm, paradigm, settings,
                        vix=None):
    """
    Evaluate Skew+Charm setup.
    LONG: skew drops >threshold% AND charm > 0
    SHORT: skew rises >threshold% AND charm < 0
    Returns result dict or None.

    Grading v2 (Mar 22): Data-driven scoring from 210 trades analysis.
    Old grading was anti-predictive (r=-0.19). New grading r=+0.32.
    Key insight: paradigm subtype is #1 predictor, charm/time were INVERTED.
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

    # --- Scoring v2: Data-driven (5 components, max 100) ---
    # Based on 210-trade analysis (Mar 2-20, 2026). Correlation with outcome:
    #   Old total score: r=-0.193 (anti-predictive)
    #   New total score: r=+0.320 (correctly predictive)

    # 1. Paradigm subtype (0-30) — strongest predictor by far
    #    GOOD paradigms (84% WR, +478 pts): SIDIAL*, GEX-PURE, AG-TARGET, BofA-LIS, AG-PURE
    #    BAD paradigms (45% WR, -164 pts): GEX-LIS, AG-LIS
    #    NEUTRAL (61% WR): BOFA-PURE, GEX-TARGET, others
    _GOOD_PARADIGMS = {"GEX-PURE", "SIDIAL-EXTREME", "SIDIAL-MESSY", "AG-TARGET",
                       "BOFA-LIS", "BofA-LIS", "AG-PURE", "GEX-MESSY"}
    _BAD_PARADIGMS = {"GEX-LIS", "AG-LIS"}
    p_val = str(paradigm) if paradigm else ""
    if p_val in _GOOD_PARADIGMS:
        para_score = 30
    elif p_val in _BAD_PARADIGMS:
        para_score = 0
    else:
        para_score = 15

    # 2. Time of day (0-25) — morning is better (INVERTED from old scoring)
    #    09-12 ET: ~74% WR, 14:30/15:30 ET: 36-43% WR (death zones)
    t = now.time()
    if t < dtime(12, 0):
        time_score = 25
    elif t < dtime(14, 0):
        time_score = 15
    elif t < dtime(14, 30):
        time_score = 10
    elif t < dtime(15, 0):
        time_score = 3   # 14:30 = 43% WR death zone
    elif t < dtime(15, 30):
        time_score = 8
    else:
        time_score = 0   # 15:30+ = 36% WR death zone

    # 3. VIX regime (0-20)
    #    VIX<22: 72% WR (+514 pts). VIX>=25 + bad paradigm: 41% WR (-148 pts)
    _vix = float(vix) if vix is not None else 22.0
    if _vix < 22:
        vix_score = 20
    elif _vix < 25:
        vix_score = 12
    else:
        vix_score = 5

    # 4. Charm alignment INVERTED (0-15)
    #    Low charm (score=0): 78% WR, +310 pts. High charm (score=25): 37% WR, -33 pts.
    #    Mechanism: extreme charm = over-stretched, snaps back against the trade.
    abs_charm = abs(charm)
    if abs_charm < 50_000_000:
        charm_score = 15   # low charm = best
    elif abs_charm < 100_000_000:
        charm_score = 10
    elif abs_charm < 250_000_000:
        charm_score = 5
    else:
        charm_score = 0    # extreme charm = worst

    # 5. Skew magnitude (0-10) — downweighted (r=-0.03, near zero predictive power)
    abs_change = abs(skew_change_pct)
    if abs_change >= 7:
        skew_score = 10
    elif abs_change >= 5:
        skew_score = 7
    else:
        skew_score = 3

    total_score = para_score + time_score + vix_score + charm_score + skew_score

    # Grade thresholds (calibrated to new score distribution)
    if total_score >= 80:
        grade = "A+"
    elif total_score >= 65:
        grade = "A"
    elif total_score >= 50:
        grade = "B"
    elif total_score >= 35:
        grade = "C"
    else:
        grade = "LOG"

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
        # Sub-scores in existing columns (repurposed for v2)
        "support_score": skew_score,          # skew magnitude (0-10)
        "upside_score": charm_score,          # charm INVERTED (0-15)
        "floor_cluster_score": time_score,    # time-of-day INVERTED (0-25)
        "target_cluster_score": para_score,   # paradigm subtype (0-30)
        "rr_score": vix_score,                # VIX regime (0-20)
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


def format_skew_charm_message(result, alignment=None):
    """Format a concise Telegram HTML message for Skew Charm."""
    direction = result["direction"]
    dir_label = "LONG" if direction == "long" else "SHORT"
    grade = result.get("grade", "C")
    grade_emoji = {"A+": "\U0001f7e2", "A": "\U0001f535", "A-Entry": "\U0001f7e1"}.get(grade, "\u26aa")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    skew_chg = result.get("skew_change_pct") or 0
    charm_m = (result.get("charm") or 0) / 1_000_000
    charm_dir = "bullish" if (result.get("charm") or 0) > 0 else "bearish"
    msg = f"{grade_emoji} <b>Skew Charm {dir_label} [{grade}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} \u2192 Trail | SL {(result.get('stop_price') or 0):.0f} (20pt)\n"
    msg += f"Skew {skew_chg:+.1f}% | Charm {charm_m:+,.0f}M {charm_dir}"
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


def update_vix_tracker(vix: float, spot: float):
    """Track VIX+SPX for rolling window detection. Called every 30s cycle from main.py."""
    if vix is None or spot is None:
        return
    now_str = datetime.now(NY).strftime("%Y-%m-%d %H:%M:%S")
    _vix_history.append((now_str, float(vix), float(spot)))
    # Keep last 120 min of data (generous buffer for 45-min window)
    if len(_vix_history) > 240:
        _vix_history[:] = _vix_history[-240:]


def evaluate_vix_divergence(spot, vix, settings, paradigm=None, **kwargs):
    """
    VIX Divergence v2 — Two-phase VIX-SPX divergence detector.
    Replaces VIX Compression (v1). Both LONG and SHORT directions.

    The pattern (user-discovered Mar 27, Discord-validated by Apollo):
      Phase 1 — "VIX suppression": SPX moves >6 pts but VIX doesn't react (<0.20).
                Vol sellers/buyers absorbing the move.
      Phase 2 — "VIX compression": VIX moves >0.25 AGAINST the Phase 1 direction
                while SPX stays flat (<10 pts). Spring loading.
      Signal fires when Phase 2 completes. The explosion follows.

    LONG: Phase 1 = SPX drops, VIX flat. Phase 2 = VIX drops, SPX flat. -> SPX rallies.
    SHORT: Phase 1 = SPX rallies, VIX flat. Phase 2 = VIX rises, SPX flat. -> SPX drops.

    Backtest (24 days, Feb 24 - Mar 27):
      SHORT (BE@8, trail@10/g5): 20 signals, 56% WR, +82 pts, PF 2.28
        B-grade (P1>=8): 100% WR (5/5), +77 pts
        VIX < 26 sweet spot: 67% WR, +50 pts
      LONG (IMM trail gap=8): 23 signals, 39% WR, +50 pts, PF 1.65
        VIX >= 26: 50% WR, +46 pts
      Combined March: +131 pts, PF 2.11, 58% green days

    Grading by Phase 1 SPX move strength:
      A+ (>=12), A (>=10), B (>=8), C (<8)
    """
    if spot is None or vix is None:
        return None
    if len(_vix_history) < 10:
        return None

    now = datetime.now(NY)
    if not (dtime(10, 0) <= now.time() <= dtime(14, 30)):
        return None

    today = now.date()

    # Phase 1/2 thresholds
    P1_SPX_MOVE = 6
    P1_VIX_REACT_MAX = 0.20
    P1_WIN_MIN, P1_WIN_MAX = 10, 30  # minutes
    P2_VIX_COMPRESS = 0.25
    P2_SPX_FLAT = 10
    P2_WIN_MIN, P2_WIN_MAX = 15, 60  # minutes

    results = []

    for direction in ("long", "short"):
        # One signal per day per direction
        cd_key = f"last_{direction}_time"
        if str(_cooldown_vix_divergence.get("last_date", "")) == str(today):
            if _cooldown_vix_divergence.get(cd_key) is not None:
                continue
        else:
            _cooldown_vix_divergence["last_date"] = str(today)
            _cooldown_vix_divergence["last_long_time"] = None
            _cooldown_vix_divergence["last_short_time"] = None

        # VIX gate: shorts only when VIX < 26, longs any VIX
        if direction == "short" and vix >= 26:
            continue

        # ── Phase 1: SPX moves but VIX doesn't react ──
        best_p1 = None
        n = len(_vix_history)
        for i in range(n):
            ts_i_str, vix_i, spot_i = _vix_history[i]
            try:
                ts_i = datetime.strptime(ts_i_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            for j in range(i + 1, n):
                ts_j_str, vix_j, spot_j = _vix_history[j]
                try:
                    ts_j = datetime.strptime(ts_j_str, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    continue
                mins = (ts_j - ts_i).total_seconds() / 60.0
                if mins < P1_WIN_MIN:
                    continue
                if mins > P1_WIN_MAX:
                    break

                if direction == "long":
                    spx_change = spot_i - spot_j   # SPX drop (positive)
                    vix_react = vix_j - vix_i       # VIX rise (should be small)
                else:
                    spx_change = spot_j - spot_i   # SPX rally (positive)
                    vix_react = vix_i - vix_j       # VIX drop (should be small)

                if spx_change >= P1_SPX_MOVE and vix_react <= P1_VIX_REACT_MAX:
                    if best_p1 is None or spx_change > best_p1["spx_move"]:
                        best_p1 = {
                            "end_idx": j,
                            "spx_move": spx_change,
                            "vix_react": vix_react,
                        }

        if best_p1 is None:
            continue

        # ── Phase 2: VIX compresses while SPX flat ──
        p2_start = best_p1["end_idx"]
        found_p2 = False
        p2_vix_compress = 0
        p2_spx_range = 0

        for j in range(p2_start + 1, n):
            ts_s_str = _vix_history[p2_start][0]
            ts_j_str, vix_j, spot_j = _vix_history[j]
            try:
                ts_s = datetime.strptime(ts_s_str, "%Y-%m-%d %H:%M:%S")
                ts_j = datetime.strptime(ts_j_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            mins = (ts_j - ts_s).total_seconds() / 60.0
            if mins < P2_WIN_MIN:
                continue
            if mins > P2_WIN_MAX:
                break

            vix_s = _vix_history[p2_start][1]
            spot_s = _vix_history[p2_start][2]
            if direction == "long":
                vc = vix_s - vix_j       # VIX dropping (positive = compressing)
            else:
                vc = vix_j - vix_s       # VIX rising (positive = building fear)
            sr = abs(spot_j - spot_s)

            if vc >= P2_VIX_COMPRESS and sr <= P2_SPX_FLAT:
                found_p2 = True
                p2_vix_compress = vc
                p2_spx_range = sr
                break

        if not found_p2:
            continue

        # ── Grading: Phase 1 SPX move strength ──
        p1_strength = best_p1["spx_move"]
        if p1_strength >= 12:
            grade = "A+"
        elif p1_strength >= 10:
            grade = "A"
        elif p1_strength >= 8:
            grade = "B"
        else:
            grade = "C"

        # ── Scoring (3 components, max 100) ──
        # 1. Phase 1 strength (0-40): bigger SPX move with flat VIX = stronger
        if p1_strength >= 15:
            p1_score = 40
        elif p1_strength >= 12:
            p1_score = 35
        elif p1_strength >= 10:
            p1_score = 28
        elif p1_strength >= 8:
            p1_score = 20
        else:
            p1_score = 10

        # 2. Phase 2 VIX compression magnitude (0-30)
        if p2_vix_compress >= 0.8:
            p2_score = 30
        elif p2_vix_compress >= 0.5:
            p2_score = 22
        elif p2_vix_compress >= 0.35:
            p2_score = 15
        else:
            p2_score = 8

        # 3. VIX level (0-30): direction-dependent
        if direction == "long":
            # Higher VIX = more compression energy for longs
            if vix >= 30:
                vix_score = 30
            elif vix >= 26:
                vix_score = 25
            elif vix >= 22:
                vix_score = 15
            else:
                vix_score = 5
        else:
            # Lower VIX = better for shorts (vol buying is meaningful signal)
            if vix < 22:
                vix_score = 25
            elif vix < 24:
                vix_score = 20
            elif vix < 26:
                vix_score = 15
            else:
                vix_score = 0  # blocked by gate above, but safety

        composite = p1_score + p2_score + vix_score

        # ── RM per direction ──
        # Stop-entry confirmation: wait for first 1.5pt move in signal direction
        # Backtest: WR 50→68%, PnL +149→+262, MaxDD 29→11, avg MAE 5.3→2.3
        CONFIRM_OFFSET = 1.5
        confirm_timeout_min = 30
        if direction == "short":
            # SL=12, continuous trail activation=15 gap=5
            # (shorts are contrarian, need wider SL to survive initial drawdown)
            stop_pts = 12
            confirm_price = round(spot - CONFIRM_OFFSET, 2)
            target_price = round(spot - 100, 2)  # no fixed TP, trail-only
            stop_price = round(confirm_price + stop_pts, 2)
        else:
            # SL=8, hybrid trail BE@6 activation=8 gap=8
            stop_pts = 8
            confirm_price = round(spot + CONFIRM_OFFSET, 2)
            target_price = round(spot + 100, 2)  # no fixed TP, trail-only
            stop_price = round(confirm_price - stop_pts, 2)

        signal_hour = now.hour + now.minute / 60.0

        results.append({
            "setup_name": "VIX Divergence",
            "direction": direction,
            "grade": grade,
            "score": round(composite, 1),
            "spot": round(spot, 2),
            "vix": round(vix, 2),
            "p1_spx_move": round(best_p1["spx_move"], 1),
            "p1_vix_react": round(best_p1["vix_react"], 2),
            "p2_vix_compress": round(p2_vix_compress, 2),
            "p2_spx_range": round(p2_spx_range, 1),
            "target_pts": 100,  # trail-only, no fixed TP
            "stop_pts": stop_pts,
            "stop_price": stop_price,
            "target_price": target_price,
            # Stop-entry confirmation fields
            "stop_entry_confirm_price": confirm_price,
            "stop_entry_timeout_min": confirm_timeout_min,
            # setup_log columns
            "paradigm": paradigm,
            "lis": None,
            "target": target_price,
            "max_plus_gex": None,
            "max_minus_gex": None,
            "gap_to_lis": None,
            "upside": round(p2_vix_compress, 2),
            "rr_ratio": round(p1_strength, 1),
            "first_hour": signal_hour < 10.5,
            "support_score": p1_score,
            "upside_score": p2_score,
            "floor_cluster_score": vix_score,
            "target_cluster_score": 0,
            "rr_score": 0,
        })

    # Return best signal (highest composite) or None
    if not results:
        return None
    return max(results, key=lambda r: r["score"])


def should_notify_vix_divergence(result):
    """One signal per day per direction for VIX Divergence."""
    if result is None:
        return False, None

    now = datetime.now(NY)
    today = str(now.date())
    direction = result.get("direction", "long")
    cd_key = f"last_{direction}_time"

    if str(_cooldown_vix_divergence.get("last_date", "")) != today:
        _cooldown_vix_divergence["last_long_time"] = None
        _cooldown_vix_divergence["last_short_time"] = None
        _cooldown_vix_divergence["last_date"] = today

    if _cooldown_vix_divergence.get(cd_key) is not None:
        return False, None

    _cooldown_vix_divergence[cd_key] = now
    return True, "new"


def format_vix_divergence_message(result, alignment=None):
    """Format Telegram message for VIX Divergence setup."""
    grade = result.get("grade", "?")
    direction = result.get("direction", "long")
    dir_label = "LONG" if direction == "long" else "SHORT"
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    p1_spx = result.get("p1_spx_move", 0)
    p1_vix = result.get("p1_vix_react", 0)
    p2_vix = result.get("p2_vix_compress", 0)
    vix_level = result.get("vix", 0)
    score = result.get("score", 0)

    confirm = result.get("stop_entry_confirm_price", 0)
    emoji = "\U0001f535" if direction == "long" else "\U0001f534"
    msg = f"{emoji} <b>VIX Divergence {dir_label} [{grade}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} | STOP ENTRY @ {confirm:.0f} | SL {result.get('stop_pts', 8)}pt | trail\n"
    msg += f"P1: SPX {p1_spx:+.0f}pt VIX {p1_vix:+.2f} | P2: VIX {p2_vix:+.2f}\n"
    msg += f"VIX {vix_level:.1f} | sc={score:.0f}"
    return msg


# ── IV Momentum (Apollo) ───────────────────────────────────────────────

def update_iv_momentum_tracker(spot, chain_df):
    """Track per-strike put IV for momentum detection. Called every 30s from main.py."""
    if spot is None or chain_df is None or chain_df.empty:
        return
    now_str = datetime.now(NY).strftime("%Y-%m-%d %H:%M:%S")

    # Extract put IV at ATM and nearby strikes (ATM, ATM-5, ATM-10)
    strike_ivs = {}
    try:
        for _, row in chain_df.iterrows():
            strike = float(row.get("Strike", 0))
            p_iv = row.get("P_IV")
            if strike > 0 and p_iv is not None and float(p_iv) > 0:
                # Only keep strikes within 15 pts of spot
                if abs(strike - spot) <= 15:
                    strike_ivs[strike] = float(p_iv)
    except Exception:
        return

    if not strike_ivs:
        return

    _iv_momentum_history.append((now_str, float(spot), strike_ivs))
    # Keep last 30 min of data (~60 entries at 30s intervals)
    if len(_iv_momentum_history) > 80:
        _iv_momentum_history[:] = _iv_momentum_history[-80:]


def evaluate_iv_momentum(spot, vix, settings):
    """
    Evaluate IV Momentum SHORT setup (Apollo's vol-confirmed downtrend).
    Signal: spot dropped >= X pts in N min AND put IV at nearby strikes rose >= Y.
    SHORTS ONLY — momentum longs tested poorly (34% WR).
    Returns result dict or None.
    """
    if spot is None or len(_iv_momentum_history) < 5:
        return None

    s = {**DEFAULT_IV_MOMENTUM_SETTINGS, **(settings or {})}

    # Time gate
    now = datetime.now(NY)
    try:
        start = dtime(*[int(x) for x in s["ivm_market_start"].split(":")])
        end = dtime(*[int(x) for x in s["ivm_market_end"].split(":")])
    except Exception:
        start, end = dtime(10, 0), dtime(15, 50)
    if not (start <= now.time() <= end):
        return None

    lookback_min = s["ivm_lookback_min"]
    min_spot_drop = s["ivm_min_spot_drop"]
    min_iv_rise = s["ivm_min_iv_rise"]

    # Find the lookback snapshot (~N minutes ago)
    target_ts = now - timedelta(minutes=lookback_min)
    target_str = target_ts.strftime("%Y-%m-%d %H:%M:%S")

    lb_snap = None
    best_diff = 999
    for ts_str, lb_spot, lb_ivs in _iv_momentum_history:
        try:
            diff = abs((datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S") - target_ts).total_seconds())
        except Exception:
            continue
        if diff < best_diff and diff < 180:  # within 3 min of target
            best_diff = diff
            lb_snap = (ts_str, lb_spot, lb_ivs)

    if lb_snap is None:
        return None

    lb_ts_str, lb_spot, lb_ivs = lb_snap
    current_snap = _iv_momentum_history[-1]
    curr_ts_str, curr_spot, curr_ivs = current_snap

    # Check spot drop (SHORT only: spot must have fallen)
    spot_move = curr_spot - lb_spot
    if spot_move > -min_spot_drop:
        return None  # spot didn't drop enough

    # Compute avg put IV change at common strikes
    iv_changes = []
    for strike, curr_iv in curr_ivs.items():
        # Find same strike in lookback (exact match or within 0.5)
        lb_iv = lb_ivs.get(strike)
        if lb_iv is None:
            # Try nearby
            for s_key, s_val in lb_ivs.items():
                if abs(s_key - strike) < 1:
                    lb_iv = s_val
                    break
        if lb_iv is not None and lb_iv > 0:
            iv_changes.append(curr_iv - lb_iv)

    if len(iv_changes) < 2:
        return None

    avg_iv_change = sum(iv_changes) / len(iv_changes)

    # Put IV must have RISEN (vol buyers confirming fear)
    if avg_iv_change < min_iv_rise:
        return None

    # ── Scoring (3 components, max 100) ──

    # 1. Spot momentum magnitude (0-35): bigger drop = stronger signal
    spot_drop = abs(spot_move)
    if spot_drop >= 15:
        mom_score = 35
    elif spot_drop >= 12:
        mom_score = 30
    elif spot_drop >= 10:
        mom_score = 25
    elif spot_drop >= 8:
        mom_score = 20
    elif spot_drop >= 5:
        mom_score = 12
    else:
        mom_score = 5

    # 2. IV confirmation strength (0-35): bigger IV rise = more vol buying
    if avg_iv_change >= 0.20:
        iv_score = 35
    elif avg_iv_change >= 0.15:
        iv_score = 30
    elif avg_iv_change >= 0.10:
        iv_score = 25
    elif avg_iv_change >= 0.08:
        iv_score = 18
    elif avg_iv_change >= 0.05:
        iv_score = 12
    else:
        iv_score = 5

    # 3. VIX environment (0-30): higher VIX = vol moves faster
    vix_val = vix or 0
    if vix_val >= 28:
        vix_score = 30
    elif vix_val >= 25:
        vix_score = 25
    elif vix_val >= 22:
        vix_score = 18
    elif vix_val >= 18:
        vix_score = 12
    else:
        vix_score = 5

    composite = mom_score + iv_score + vix_score

    # Grade (LOG-ONLY for now until validated with live signals)
    if composite >= 80:
        grade = "A+"
    elif composite >= 60:
        grade = "A"
    elif composite >= 45:
        grade = "B"
    elif composite >= 30:
        grade = "C"
    else:
        return None  # below threshold

    stop_pts = s["ivm_stop_pts"]
    target_pts = s["ivm_target_pts"]

    return {
        "setup_name": "IV Momentum",
        "direction": "short",
        "grade": grade,
        "score": round(composite, 1),
        "spot": round(spot, 2),
        "vix": round(vix_val, 2) if vix_val else None,
        "spot_drop": round(spot_drop, 2),
        "avg_iv_change": round(avg_iv_change, 4),
        "iv_strikes_used": len(iv_changes),
        "lookback_min": lookback_min,
        "lb_spot": round(lb_spot, 2),
        "lb_ts": lb_ts_str,
        "mom_score": mom_score,
        "iv_score": iv_score,
        "vix_score": vix_score,
        "target_pts": target_pts,
        "stop_pts": stop_pts,
        "target_price": round(spot - target_pts, 2),  # SHORT: target is below
        "stop_price": round(spot + stop_pts, 2),       # SHORT: stop is above
    }


def should_notify_iv_momentum(result):
    """30-min cooldown for IV Momentum (short-only)."""
    if result is None:
        return False, None

    now = datetime.now(NY)
    today = str(now.date())
    cooldown_min = DEFAULT_IV_MOMENTUM_SETTINGS["ivm_cooldown_min"]

    if str(_cooldown_iv_momentum.get("last_date", "")) != today:
        _cooldown_iv_momentum["last_short_time"] = None
        _cooldown_iv_momentum["last_date"] = today

    last_fire = _cooldown_iv_momentum.get("last_short_time")
    if last_fire is not None:
        elapsed = (now - last_fire).total_seconds() / 60
        if elapsed < cooldown_min:
            return False, None

    _cooldown_iv_momentum["last_short_time"] = now
    return True, "new"


def format_iv_momentum_message(result, alignment=None):
    """Format a concise Telegram HTML message for IV Momentum."""
    grade = result.get("grade", "?")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    spot_drop = result.get("spot_drop", 0)
    iv_chg = result.get("avg_iv_change", 0)
    score = result.get("score", 0)
    msg = f"\U0001f534 <b>IV Momentum SHORT [{grade}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} \u2192 T {(result.get('target_price') or 0):.0f} | SL {(result.get('stop_price') or 0):.0f}\n"
    msg += f"Drop {spot_drop:.0f}pt | IV \u2191{iv_chg:.3f} ({result.get('iv_strikes_used') or 0}stk) | sc={score:.0f}"
    return msg


# ── Vanna Butterfly (Pin Setup) ────────────────────────────────────────

def evaluate_vanna_butterfly(spot, chain_df, vanna_pin_strike, vanna_pin_value, vix,
                             paradigm=None):
    """
    Evaluate Vanna Pin Butterfly setup.
    40pt call butterfly centered on max absolute 0DTE vanna strike.
    Fires once per day at ~15:00 ET.

    Grading v2 (Mar 23): Data-driven scoring from 27 trades.
    Key finding: GREEN vanna (positive) = 72.7% WR, RED = 18.8%.
    GREEN is the #1 predictor — RED gets grade "LOG".
    Width changed 30pt→40pt (higher total P&L, same WR).
    Gap filter widened 20→30 (GREEN pulls price even from 25+ pts).
    """
    if spot is None or chain_df is None or chain_df.empty:
        return None
    if vanna_pin_strike is None or vanna_pin_value is None:
        return None

    # Time gate: only fire 14:55-15:10 ET
    now = datetime.now(NY)
    if not (dtime(14, 55) <= now.time() <= dtime(15, 10)):
        return None

    # One signal per day
    today = str(now.date())
    if _cooldown_vanna_butterfly.get("last_date") == today and _cooldown_vanna_butterfly.get("fired"):
        return None

    pin = round(vanna_pin_strike / 5) * 5  # Round to nearest 5-pt strike
    gap = abs(spot - pin)

    # Gap filter: skip if spot is too far from pin (>30 pts)
    if gap > 30:
        return None

    # Pin sign: GREEN (positive vanna) = magnet, RED (negative) = repel
    is_green = vanna_pin_value > 0

    # ── Price the butterfly from chain bid/ask ──
    # 40pt butterfly: buy call at pin-20, sell 2x call at pin, buy call at pin+20
    width = 20
    lower = pin - width
    center = pin
    upper = pin + width

    def get_call_price(strike, side='ask'):
        """Get call bid or ask at a strike from chain_df."""
        idx_col = 'Strike'
        try:
            row = chain_df[chain_df[idx_col].astype(float) == strike]
            if row.empty:
                return None
            if side == 'ask':
                val = row.iloc[0].get('C_Ask')
            else:
                val = row.iloc[0].get('C_Bid')
            if val is None or float(val) <= 0:
                return None
            return float(val)
        except Exception:
            return None

    lower_ask = get_call_price(lower, 'ask')
    center_bid = get_call_price(center, 'bid')
    upper_ask = get_call_price(upper, 'ask')

    if lower_ask is None or center_bid is None or upper_ask is None:
        return None

    cost = lower_ask - 2 * center_bid + upper_ask
    if cost <= 0 or cost > 15:
        return None  # Invalid or too expensive

    max_payout = float(width)  # Width of one wing
    max_profit = max_payout - cost

    # ── Grading v2: Data-driven scoring (27 trades backtest) ──
    # GREEN vanna is THE gatekeeper: 72.7% WR vs RED 18.8%
    # RED signals get grade "LOG" — collect data but don't recommend

    if not is_green:
        # RED vanna: log-only, score=0
        grade = "LOG"
        composite = 0.0
        gap_score = 0
        vix_score = 0
        vanna_dir_score = 0
        cost_score = 0
    else:
        # GREEN vanna: score based on 3 predictive components
        # 1. Gap proximity (0-30): closer = slightly better, but GREEN works at any gap<=30
        if gap <= 10:
            gap_score = 30
        elif gap <= 15:
            gap_score = 25
        elif gap <= 20:
            gap_score = 20
        else:
            gap_score = 10

        # 2. VIX environment (0-25): lower VIX = calmer = pins better (r=+0.387 with dist)
        vix_val = vix or 0
        if vix_val <= 18:
            vix_score = 25
        elif vix_val <= 22:
            vix_score = 20
        elif vix_val <= 25:
            vix_score = 15
        else:
            vix_score = 5

        # 3. Net vanna direction near pin (0-25): positive sum = stronger magnet (r=+0.438)
        # Check vanna_pin_value sign — already green, but check magnitude
        abs_vanna = abs(vanna_pin_value)
        if abs_vanna >= 50_000_000:  # 50M+ strong magnet
            vanna_dir_score = 25
        elif abs_vanna >= 20_000_000:
            vanna_dir_score = 15
        else:
            vanna_dir_score = 10

        # 4. Cost efficiency (0-20): cheaper = better R:R (r=-0.795 cost vs gap)
        if cost <= 3.0:
            cost_score = 20
        elif cost <= 5.0:
            cost_score = 15
        elif cost <= 8.0:
            cost_score = 10
        else:
            cost_score = 5

        composite = gap_score + vix_score + vanna_dir_score + cost_score

        if composite >= 80:
            grade = "A+"
        elif composite >= 60:
            grade = "A"
        elif composite >= 40:
            grade = "B"
        else:
            grade = "C"

    vix_val = vix or 0
    rr = round(max_profit / cost, 2) if cost > 0 else 0

    # Mark fired immediately so restarts/deploys can't double-fire
    _cooldown_vanna_butterfly["last_date"] = str(now.date())
    _cooldown_vanna_butterfly["fired"] = True

    return {
        "setup_name": "Vanna Butterfly",
        "direction": "long",  # Butterfly is non-directional but logged as "long"
        "grade": grade,
        "score": round(composite, 1),
        "spot": round(spot, 2),
        "vix": round(vix_val, 2) if vix_val else None,
        "pin_strike": pin,
        "pin_sign": "GREEN" if is_green else "RED",
        "entry_gap": round(gap, 1),
        "butterfly_lower": lower,
        "butterfly_center": center,
        "butterfly_upper": upper,
        "butterfly_width": width * 2,
        "butterfly_cost": round(cost, 2),
        "max_profit": round(max_profit, 2),
        "vanna_value": round(vanna_pin_value, 0),
        "gap_score": gap_score,
        "cost_score": cost_score,
        "vanna_dir_score": vanna_dir_score if is_green else 0,
        "vix_bfly_score": vix_score if is_green else 0,
        # Columns required by setup_log INSERT
        "paradigm": paradigm,
        "lis": None,
        "target": pin,
        "max_plus_gex": None,
        "max_minus_gex": None,
        "gap_to_lis": round(gap, 1),
        "upside": round(max_profit, 2),
        "rr_ratio": rr,
        "first_hour": False,
        "support_score": gap_score,
        "upside_score": vix_score if is_green else 0,
        "floor_cluster_score": vanna_dir_score if is_green else 0,
        "target_cluster_score": cost_score,
        "rr_score": 0,
        # For outcome tracking
        "target_price": pin,
        "stop_price": None,  # No stop — max loss is the cost
        "target_pts": max_profit,
        "stop_pts": cost,  # Max loss = cost
    }


def should_notify_vanna_butterfly(result):
    """One signal per day for Vanna Butterfly."""
    if result is None:
        return False, None

    now = datetime.now(NY)
    today = str(now.date())

    if _cooldown_vanna_butterfly.get("last_date") != today:
        _cooldown_vanna_butterfly["fired"] = False
        _cooldown_vanna_butterfly["last_date"] = today

    if _cooldown_vanna_butterfly.get("fired"):
        return False, None

    _cooldown_vanna_butterfly["fired"] = True
    return True, "new"


def format_vanna_butterfly_message(result, alignment=None):
    """Format Telegram message for Vanna Butterfly."""
    grade = result.get("grade", "?")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    pin = result.get("pin_strike", 0)
    gap = result.get("entry_gap", 0)
    cost = result.get("butterfly_cost", 0)
    mx = result.get("max_profit", 0)
    lower = result.get("butterfly_lower", 0)
    upper = result.get("butterfly_upper", 0)
    score = result.get("score", 0)
    sign = result.get("pin_sign", "?")
    width = result.get("butterfly_width", 40)
    msg = f"\U0001f98b <b>Vanna Butterfly [{grade}]{align_str}</b>\n"
    msg += f"Pin {pin:.0f} ({sign}) | {lower:.0f}/{pin:.0f}/{upper:.0f} ({width}pt)\n"
    msg += f"Gap {gap:.0f}pt | Cost ${cost:.2f} | MaxProfit ${mx:.2f} | sc={score:.0f}"
    return msg


def evaluate_dd_exhaustion(spot, dd_value, dd_shift, charm, paradigm, settings,
                           vix=None, greek_alignment=None):
    """
    Evaluate DD Exhaustion setup.
    LONG: dd_shift < -threshold AND charm > 0  (dealers over-hedged bearish, price bounces)
    SHORT: dd_shift > +threshold AND charm < 0  (dealers over-positioned bullish, price fades)
    Returns result dict or None.

    Grading v2 (Mar 22): Data-driven scoring from 289 trades analysis.
    Old grading was random (r=-0.017). New grading r=+0.296.
    Key insight: DD is CONTRARIAN — anti-alignment is best. Paradigm subtype matters.
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

    # --- Scoring v2: Data-driven (5 components, max 100) ---
    # Based on 289-trade analysis (Feb 18 - Mar 20, 2026). Correlation with outcome:
    #   Old total score: r=-0.017 (random — grade meant nothing)
    #   New total score: r=+0.296 (correctly predictive)

    # 1. Paradigm subtype (0-25) — strongest predictor (r=+0.192)
    #    GOOD (60% WR, +251 pts): AG-TARGET (79%), SIDIAL-MESSY (58%), SIDIAL-EXTREME (50%)
    #    BAD (28% WR, -127 pts): AG-LIS (28%), BOFA-MESSY (25%)
    #    NEUTRAL (45% WR): BOFA-PURE, GEX-*, BofA-LIS
    _GOOD_PARADIGMS = {"AG-TARGET", "SIDIAL-MESSY", "SIDIAL-EXTREME", "AG-PURE",
                       "SIDIAL-BALANCE"}
    _BAD_PARADIGMS = {"AG-LIS", "BOFA-MESSY"}
    p_val = str(paradigm) if paradigm else ""
    if p_val in _GOOD_PARADIGMS:
        para_score = 25
    elif p_val in _BAD_PARADIGMS:
        para_score = 0
    else:
        para_score = 12

    # 2. Greek alignment — CONTRARIAN (0-25) — unique to DD (r=+0.150)
    #    DD is a contrarian setup: anti-alignment means the signal is STRONGEST.
    #    short align=-1: 56% WR, +252 pts (anti-aligned short = dealers overextended)
    #    long  align=+2: 55% WR, +118 pts (moderate alignment works for longs)
    #    long  align=+3: 41% WR, -184 pts (over-aligned = exhaustion already resolved)
    #    short align= 0: 26% WR, -97 pts (no conviction either way)
    align = int(greek_alignment) if greek_alignment is not None else 0
    if direction == "short":
        # Shorts: anti-alignment is BEST (dealers overextended bullish)
        if align <= -1:
            align_score = 25
        elif align <= 0:
            align_score = 5    # neutral = no conviction
        elif align <= 2:
            align_score = 15   # moderate alignment
        else:
            align_score = 8    # align=+3 for shorts is mixed
    else:
        # Longs: moderate alignment works, over-alignment = already resolved
        if align == 2:
            align_score = 25   # sweet spot for longs
        elif align == 1:
            align_score = 18
        elif align == 0:
            align_score = 10
        elif align == -1:
            align_score = 5    # anti-aligned long = risky
        else:  # align=+3
            align_score = 0    # over-aligned = worst (41% WR, -184 PnL)

    # 3. VIX sweet spot (0-20) — (r=+0.175)
    #    VIX 21-26: 52% WR, +266 pts — sweet spot for contrarian exhaustion
    #    VIX 18-20: 39% WR — low vol, not enough pressure for exhaustion reversal
    #    VIX 27+: 25% WR, -132 pts — too much vol, exhaustion signals are noise
    _vix = float(vix) if vix is not None else 22.0
    if 21 <= _vix < 26:
        vix_score = 20   # sweet spot
    elif 20 <= _vix < 21:
        vix_score = 12   # borderline
    elif 18 <= _vix < 20:
        vix_score = 5    # low VIX = weak exhaustion signals
    elif 26 <= _vix < 27:
        vix_score = 8    # borderline high
    else:
        vix_score = 0    # VIX >= 27 or < 18

    # 4. DD shift magnitude (0-15) — only positive correlation from old scoring (r=+0.114)
    abs_shift = abs(dd_shift)
    if abs_shift >= 1_000_000_000:
        shift_score = 15   # strong shift = stronger contrarian signal
    elif abs_shift >= 500_000_000:
        shift_score = 10
    elif abs_shift >= 300_000_000:
        shift_score = 5
    else:
        shift_score = 0

    # 5. Time of day (0-15) — (r=+0.108)
    #    10:00-14:00: 49% WR (best window)
    #    14:30: 31% WR (death zone — same as SC)
    #    15:00: 36% WR (near end of day)
    t = now.time()
    if t < dtime(14, 0):
        time_score = 15    # core hours
    elif t < dtime(14, 30):
        time_score = 10    # 14:00-14:30 still OK (52% WR)
    elif t < dtime(15, 0):
        time_score = 0     # 14:30 death zone (31% WR)
    elif t < dtime(15, 30):
        time_score = 3     # 15:00-15:30 (36% WR)
    else:
        time_score = 5     # 15:30+ (50% WR, small sample)

    total_score = para_score + align_score + vix_score + shift_score + time_score

    # Grade thresholds (calibrated to new score distribution)
    if total_score >= 80:
        grade = "A+"
    elif total_score >= 65:
        grade = "A"
    elif total_score >= 50:
        grade = "B"
    elif total_score >= 35:
        grade = "C"
    else:
        grade = "LOG"

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
        # Sub-scores stored in existing columns for breakdown display (repurposed for v2)
        "gap_to_lis": None,
        "upside": None,
        "rr_ratio": None,
        "first_hour": False,
        "support_score": shift_score,       # DD shift magnitude (0-15)
        "upside_score": align_score,        # alignment CONTRARIAN (0-25)
        "floor_cluster_score": time_score,  # time-of-day (0-15)
        "target_cluster_score": para_score, # paradigm subtype (0-25)
        "rr_score": vix_score,              # VIX sweet spot (0-20)
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


def format_dd_exhaustion_message(result, alignment=None):
    """Format a concise Telegram HTML message for DD Exhaustion."""
    direction = result["direction"]
    dir_label = "LONG" if direction == "long" else "SHORT"
    dir_emoji = "\U0001f535" if direction == "long" else "\U0001f534"
    grade = result.get("grade", "?")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    shift_m = result["dd_shift"] / 1_000_000
    charm_m = result["charm"] / 1_000_000
    exhaust_label = "bearish exhaust" if direction == "long" else "bullish exhaust"
    charm_dir = "bullish" if result["charm"] > 0 else "bearish"
    msg = f"{dir_emoji} <b>DD Exhaust {dir_label} [{grade}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} \u2192 Trail | SL {result['stop_price']:.0f} (12pt)\n"
    msg += f"DD {shift_m:+,.0f}M {exhaust_label} | Charm {charm_m:+,.0f}M {charm_dir}"
    return msg


def format_setup_outcome(trade: dict, result_type: str, pnl: float, elapsed_min: int) -> str:
    """Format a concise Telegram HTML message for a setup outcome."""
    emoji = {"WIN": "\u2705", "LOSS": "\u274c", "EXPIRED": "\u23f9"}.get(result_type, "\u2753")
    setup_name = trade["setup_name"]
    # Shorten names
    name_short = {"Paradigm Reversal": "Paradigm", "DD Exhaustion": "DD Exhaust",
                  "ES Absorption": "ES Abs", "BofA Scalp": "BofA"}.get(setup_name, setup_name)
    direction = trade["direction"].upper()[:1]  # L or S/B
    grade = trade.get("grade", "?")
    spot = trade["spot"]
    r = trade.get("result_data", {})
    alignment = r.get("greek_alignment")
    align_str = f" {alignment:+d}" if alignment is not None else ""

    msg = f"{emoji} <b>{name_short} {direction} {pnl:+.1f}pts</b> ({elapsed_min}m) [{grade}]{align_str}\n"

    if result_type == "WIN":
        exit_price = trade.get("close_price") or spot
        msg += f"{spot:.0f} \u2192 exit {exit_price:.0f}"
    elif result_type == "LOSS":
        sl = trade.get("initial_stop_level") or trade.get("stop_level")
        msg += f"{spot:.0f} \u2192 stopped {sl:.0f}" if sl else f"{spot:.0f}"
    else:  # EXPIRED
        close_price = trade.get("close_price") or spot
        msg += f"{spot:.0f} \u2192 close {close_price:.0f}"

    return msg


def format_setup_daily_summary(trades_list: list) -> str:
    """Format a concise EOD summary Telegram message."""
    if not trades_list:
        return ""

    wins = sum(1 for t in trades_list if t["result_type"] == "WIN")
    losses = sum(1 for t in trades_list if t["result_type"] == "LOSS")
    expired = sum(1 for t in trades_list if t["result_type"] == "EXPIRED")
    total = len(trades_list)
    net_pnl = sum(t["pnl"] for t in trades_list)
    win_rate = round(100 * wins / total) if total > 0 else 0

    msg = f"\U0001f4ca <b>Daily Summary</b>\n"
    msg += f"{wins}W {losses}L {expired}E | {net_pnl:+.1f} pts | {win_rate}%\n\n"

    for t in trades_list:
        emoji = {"WIN": "\u2705", "LOSS": "\u274c", "EXPIRED": "\u23f9"}.get(t["result_type"], "\u2753")
        ts_str = t.get("ts_str", "")
        name = t["setup_name"]
        name_short = {"Paradigm Reversal": "Paradigm", "DD Exhaustion": "DD Exhaust",
                      "ES Absorption": "ES Abs", "BofA Scalp": "BofA",
                      "Skew Charm": "Skew Charm", "GEX Long": "GEX Long"}.get(name, name)
        direction = t["direction"].upper()[:1]  # L/S/B
        grade = t.get("grade", "?")
        alignment = t.get("alignment")
        align_str = f"{alignment:+d}" if alignment is not None else ""
        pnl = t["pnl"]
        msg += f"{emoji} {ts_str} {name_short} {direction} {pnl:+.1f} [{grade}]{align_str}\n"

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


def format_paradigm_reversal_message(result, alignment=None):
    """Format a concise Telegram HTML message for Paradigm Reversal."""
    dir_emoji = "\U0001f535" if result["direction"] == "long" else "\U0001f534"
    dir_label = "LONG" if result["direction"] == "long" else "SHORT"
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    prev = result.get("pr_prev_paradigm", "?")
    curr = result.get("pr_curr_paradigm", "?")
    msg = f"{dir_emoji} <b>Paradigm {dir_label} [{result['grade']}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} \u2192 {result['spot'] + 10:.0f} (+10) | SL {result['spot'] - 15:.0f} (15pt)\n" if result["direction"] == "long" else ""
    msg += f"{result['spot']:.0f} \u2192 {result['spot'] - 10:.0f} (+10) | SL {result['spot'] + 15:.0f} (15pt)\n" if result["direction"] != "long" else ""
    msg += f"{prev} \u2192 {curr}"
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

    Self-contained — does NOT share state with ES Absorption swing detection.
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


def format_vanna_pivot_message(result, alignment=None):
    """Format a concise Telegram HTML message for Vanna Pivot Bounce."""
    direction = result["direction"]
    dir_label = "LONG" if direction == "long" else "SHORT"
    grade = result.get("grade", "C")
    grade_emoji = {"A+": "\U0001f7e2", "A": "\U0001f535"}.get(grade, "\u26aa")
    align_str = f" align {alignment:+d}" if alignment is not None else ""
    vanna_strike = result.get("vanna_strike") or 0
    msg = f"{grade_emoji} <b>Vanna Pivot {dir_label} [{grade}]{align_str}</b>\n"
    msg += f"{result['spot']:.0f} \u2192 {(result.get('target_price') or 0):.0f} | SL {(result.get('stop_price') or 0):.0f}\n"
    msg += f"Vanna {vanna_strike:.0f}"
    return msg


# ── Main entry point ───────────────────────────────────────────────────────

def check_setups(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings,
                 lis_lower=None, lis_upper=None, aggregated_charm=None,
                 dd_hedging=None, es_bars=None,
                 dd_value=None, dd_shift=None,
                 skew_value=None, skew_change_pct=None,
                 vanna_levels=None, es_range_bars=None,
                 vix=None,
                 vanna_pin_strike=None, vanna_pin_value=None, chain_df=None,
                 vanna_all=None,
                 svb_correlation=None, vanna_0dte_ratio=None):
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
      vix: current VIX value (VIX Divergence)
      vanna_pin_strike: max absolute 0DTE vanna strike (Vanna Butterfly)
      vanna_pin_value: vanna notional at pin strike (Vanna Butterfly)
      chain_df: current options chain DataFrame (Vanna Butterfly pricing)
      vanna_all: vanna ALL value for greek alignment (DD Exhaustion contrarian scoring)
    """
    results = []

    # ── Track paradigm changes (must be before setup evaluations) ──
    update_paradigm_tracker(paradigm)

    # ── Update GEX LIS velocity tracker (before evaluations) ──
    update_gex_lis_tracker(lis, paradigm)

    # ── GEX Long cooldown expiry tracking (debounced — survives deploys) ──
    _gex_gone = (paradigm and "GEX" not in str(paradigm).upper()) or \
                (spot is not None and lis is not None and abs(spot - lis) > 5)
    if _gex_gone:
        _cooldown["_gone_count"] = _cooldown.get("_gone_count", 0) + 1
        if _cooldown["_gone_count"] >= _EXPIRY_DEBOUNCE:
            mark_setup_expired()
    else:
        _cooldown["_gone_count"] = 0

    gex_result = evaluate_gex_long(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings)
    if gex_result is not None:
        notify, reason = should_notify(gex_result)
        results.append({
            "result": gex_result,
            "notify": notify,
            "notify_reason": reason,
            "message": format_setup_message(gex_result),
        })

    # ── GEX Velocity cooldown expiry (debounced) ──
    _gv_gone = (paradigm and "GEX" not in str(paradigm).upper()) or \
               (spot is not None and lis is not None and abs(spot - lis) > 10)
    if _gv_gone:
        _cooldown_gex_vel["_gone_count"] = _cooldown_gex_vel.get("_gone_count", 0) + 1
        if _cooldown_gex_vel["_gone_count"] >= _EXPIRY_DEBOUNCE:
            mark_gex_velocity_expired()
    else:
        _cooldown_gex_vel["_gone_count"] = 0

    # Only evaluate if normal GEX Long didn't fire (avoid double signal)
    if gex_result is None:
        gex_vel_result = evaluate_gex_velocity(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings)
        if gex_vel_result is not None:
            notify_vel, reason_vel = should_notify_gex_velocity(gex_vel_result)
            results.append({
                "result": gex_vel_result,
                "notify": notify_vel,
                "notify_reason": reason_vel,
                "message": format_gex_velocity_message(gex_vel_result),
            })

    # ── AG Short cooldown expiry tracking (debounced — survives deploys) ──
    _ag_gone = (paradigm and "AG" not in str(paradigm).upper()) or \
               (spot is not None and lis is not None and (lis - spot) > 20)
    if _ag_gone:
        _cooldown_ag["_gone_count"] = _cooldown_ag.get("_gone_count", 0) + 1
        if _cooldown_ag["_gone_count"] >= _EXPIRY_DEBOUNCE:
            mark_ag_expired()
    else:
        _cooldown_ag["_gone_count"] = 0

    ag_result = evaluate_ag_short(spot, paradigm, lis, target, max_plus_gex, max_minus_gex, settings,
                                   vix=vix)
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
    # Pre-compute greek alignment for DD scoring (contrarian: anti-alignment is best)
    # DD direction is deterministic: long if dd_shift<0+charm>0, short if dd_shift>0+charm<0
    _dd_align = None
    if dd_shift is not None and aggregated_charm is not None:
        _dd_dir = "long" if dd_shift < 0 and aggregated_charm > 0 else "short"
        _dd_align_score = 0
        _dd_is_long = _dd_dir == "long"
        if aggregated_charm is not None:
            _dd_align_score += 1 if (aggregated_charm > 0) == _dd_is_long else -1
        if vanna_all is not None:
            _dd_align_score += 1 if (vanna_all > 0) == _dd_is_long else -1
        if spot and max_plus_gex:
            _dd_align_score += 1 if (spot <= max_plus_gex) == _dd_is_long else -1
        _dd_align = _dd_align_score

    dd_exhaust_result = evaluate_dd_exhaustion(
        spot, dd_value, dd_shift, aggregated_charm, paradigm, settings,
        vix=vix, greek_alignment=_dd_align,
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
        vix=vix,
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

    # ── VIX Divergence (replaces VIX Compression) ──
    vd_result = evaluate_vix_divergence(spot, vix, settings, paradigm=paradigm)
    if vd_result is not None:
        notify_vd, reason_vd = should_notify_vix_divergence(vd_result)
        results.append({
            "result": vd_result,
            "notify": notify_vd,
            "notify_reason": reason_vd,
            "message": format_vix_divergence_message(vd_result),
        })

    # ── IV Momentum (Apollo) — SHORT only ──
    ivm_result = evaluate_iv_momentum(spot, vix, settings)
    if ivm_result is not None:
        notify_ivm, reason_ivm = should_notify_iv_momentum(ivm_result)
        results.append({
            "result": ivm_result,
            "notify": notify_ivm,
            "notify_reason": reason_ivm,
            "message": format_iv_momentum_message(ivm_result),
        })

    # ── Vanna Butterfly (Pin Setup) — once per day at ~15:00 ET ──
    vb_result = evaluate_vanna_butterfly(spot, chain_df, vanna_pin_strike, vanna_pin_value, vix,
                                         paradigm=paradigm)
    if vb_result is not None:
        notify_vb, reason_vb = should_notify_vanna_butterfly(vb_result)
        results.append({
            "result": vb_result,
            "notify": notify_vb,
            "notify_reason": reason_vb,
            "message": format_vanna_butterfly_message(vb_result),
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
        "single_bar_abs": _serialize(_cooldown_single_bar_abs),
        "sb10_abs": _serialize(_cooldown_sb10_abs),
        "sb2_abs": _serialize(_cooldown_sb2_abs),
        "gex_velocity": _serialize(_cooldown_gex_vel),
        "delta_abs": _serialize(_cooldown_delta_abs),
        "vix_divergence": _serialize(_cooldown_vix_divergence),
        "vix_history": _vix_history[-60:],  # save last ~30 min for restart recovery
        "iv_momentum": _serialize(_cooldown_iv_momentum),
        "iv_momentum_history": _iv_momentum_history[-30:],  # save last ~15 min
        "vanna_butterfly": _serialize(_cooldown_vanna_butterfly),
    }

def import_cooldowns(data: dict):
    """Restore cooldown state from a dict (loaded from DB)."""
    global _cooldown, _cooldown_ag, _cooldown_bofa, _cooldown_absorption
    if not data:
        return
    def _deserialize(d, has_datetimes=False, dt_keys=None):
        out = dict(d)
        # Fix: convert last_date string back to date object
        # Without this, "2026-03-23" != date(2026,3,23) causes cooldown reset on every deploy
        if "last_date" in out and isinstance(out["last_date"], str) and out["last_date"]:
            try:
                out["last_date"] = date.fromisoformat(out["last_date"])
            except Exception:
                pass
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
        _cooldown_ag.update(_deserialize(data["ag"], has_datetimes=True,
                                         dt_keys=("last_fire_time",)))
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
    if "single_bar_abs" in data:
        _cooldown_single_bar_abs.update(_deserialize(data["single_bar_abs"]))
    if "sb10_abs" in data:
        _cooldown_sb10_abs.update(_deserialize(data["sb10_abs"]))
    if "sb2_abs" in data:
        _cooldown_sb2_abs.update(_deserialize(data["sb2_abs"]))
    if "gex_velocity" in data:
        _cooldown_gex_vel.update(_deserialize(data["gex_velocity"]))
    if "vix_divergence" in data:
        _cooldown_vix_divergence.update(_deserialize(
            data["vix_divergence"], has_datetimes=True,
            dt_keys=("last_long_time",)))
    if "vix_history" in data:
        _vix_history.clear()
        _vix_history.extend(data["vix_history"])
    if "iv_momentum" in data:
        _cooldown_iv_momentum.update(_deserialize(
            data["iv_momentum"], has_datetimes=True,
            dt_keys=("last_short_time",)))
    if "iv_momentum_history" in data:
        _iv_momentum_history.clear()
        _iv_momentum_history.extend(data["iv_momentum_history"])
    if "vanna_butterfly" in data:
        _cooldown_vanna_butterfly.update(_deserialize(data["vanna_butterfly"]))
    if "delta_abs" in data:
        _cooldown_delta_abs.update(_deserialize(data["delta_abs"]))
