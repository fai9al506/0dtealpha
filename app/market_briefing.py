"""Daily market-bias briefing (10:00 + 14:00 ET) — Volland-concept rules, then SCORED.

Two jobs, and the second is the one that makes this worth having:

  1. Tell the user, in plain English, what the data says at 10:00 and 14:00 ET:
     bias, why, the levels that matter, and what would prove the call wrong.
  2. Write every call down with its levels, then check afterwards what actually
     happened — so "where is our understanding gap" becomes a measured number
     instead of an impression.

Because of (2), every rule votes SEPARATELY and every vote is stored. After enough
sessions `score_range()` can say which rules carry the calls and which are noise.
Nothing here is assumed to work: the rules below are the documented Volland
framework plus our own validated findings, and they are on trial like everything
else.

STRICTLY ADVISORY. Nothing in this module touches the trade path, and nothing
reads its output to make a decision. It is fail-soft everywhere: any error is
swallowed and logged, because a briefing must never be able to break the app.

Provenance tags used in the factor list (so a wrong call is traceable):
  [VG]  Volland User Guide / White Paper  (references/volland/)
  [DC]  Volland Discord curated extracts  (references/volland/)
  [US]  our own validated research in this repo (memory / trade-analyses.md)
"""
from __future__ import annotations

import json
import re
import traceback
from datetime import date as _date, datetime, timedelta
from typing import Any, Optional

from sqlalchemy import text

_engine = None
_send_telegram = None

# ── WHICH RULES ARE ALLOWED TO VOTE ────────────────────────────────────────────
# Measured 2026-08-11 over 120 sessions / 239 samples (10:00 + 14:00 ET), forward
# move to the close, no fitting. Baseline "SPX up" = 52.7%.
#
#   rule           n    hit%   avg pts in its own direction
#   TECH_BASKET   176   59.7%   +6.3     <- votes (symmetric: up 59.4%, down 60.0%)
#   LIS           156   59.6%   +7.2     <- votes at half weight (see decay note)
#   VOL_REGIME     17   52.9%   +2.9        context only, n far too small
#   GAMMA_WALLS    59   47.5%   +0.8        context only, below baseline
#   DD_HEDGING    228   47.4%   -2.2        context only, below baseline
#   CHARM         228   46.9%   -0.5        context only, below baseline
#   PARADIGM      134   46.3%   -2.7        context only, WORST of the seven
#
# The first version let all seven vote and summed them. They are not independent —
# LIS and PARADIGM are two readings of the same structure — so they cancelled, 42 of
# 60 scores came out exactly 0, and the thing said RANGE 93% of the time while the
# median session moved 13.8 pts. It scored 40%, worse than a coin flip.
#
# So: a rule votes only if it beat baseline standing alone. The rest still appear in
# the briefing as CONTEXT, because "dealers are short gamma, moves accelerate" is
# genuinely useful for a human reading the tape — it just does not predict direction.
#
# LIS is on probation: 82% in June but 48% in July and 30% in August. Half weight,
# and if the decay continues it should be demoted to context too.
VOTING_RULES = {"TECH_BASKET": 1.0, "LIS": 0.5}

# Thresholds against the reduced vote scale (max magnitude 1.5).
BULL_AT = 1.0
BEAR_AT = -1.0
# Spot within this many points of LIS = pivot/chop, not a directional read. [VG]
LIS_PIVOT_PTS = 10.0
# RANGE is an ABSTENTION, not a forecast. It is still recorded and still checked
# (did the day stay inside the band?), but it is reported separately and never mixed
# into the headline hit-rate — otherwise "we were right 50% of the time" is really
# "we declined to call half the days and got graded on it anyway".
# Band set from the data: median |move| after a call is 13.8 pts, so a genuine
# range day has to mean less than that, not more.
NEUTRAL_BAND_PTS = 12.0


def init(engine, send_telegram_fn) -> None:
    global _engine, _send_telegram
    _engine = engine
    _send_telegram = send_telegram_fn
    try:
        _ensure_table()
    except Exception as e:
        print(f"[briefing] table init skipped: {e}", flush=True)
    print("[briefing] init ok", flush=True)


def _ensure_table() -> None:
    with _engine.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS market_briefing (
                id            BIGSERIAL PRIMARY KEY,
                trade_date    DATE NOT NULL,
                slot          TEXT NOT NULL,          -- '10:00' | '14:00'
                ts            TIMESTAMPTZ NOT NULL,
                spot          DOUBLE PRECISION,
                bias          TEXT,                   -- BULLISH | BEARISH | RANGE
                confidence    INTEGER,                -- 1..5
                score         DOUBLE PRECISION,       -- summed factor score
                support       DOUBLE PRECISION,
                resistance    DOUBLE PRECISION,
                target        DOUBLE PRECISION,
                invalidation  DOUBLE PRECISION,
                factors       JSONB,                  -- per-rule votes + reasons
                inputs        JSONB,                  -- raw inputs, for post-hoc re-runs
                outcome       JSONB,                  -- filled by score_day()
                UNIQUE (trade_date, slot)
            )
        """))


# ────────────────────────────────────────────────────────────────────────────
# parsing helpers — Volland stats arrive as display strings ("$7,727", "Long $2.0B")
# ────────────────────────────────────────────────────────────────────────────

def _num(v) -> Optional[float]:
    """First number in a string like '$7,727' or '7727.5'. None if nothing usable."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    m = re.search(r"-?[\d,]+(?:\.\d+)?", str(v))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _dd_signed(dd_str) -> tuple[Optional[float], Optional[str]]:
    """DD hedging -> (abs magnitude, 'long'|'short').

    Accepts BOTH shapes, which is the whole point: the raw Volland stat is a signed
    number ('$-422,284,376') while main.py's display string is worded ('Long $7.3B').
    The first version only understood the words, so on live data DD never voted at
    all — a silent no-op that looked like a working rule.
    """
    if dd_str is None or dd_str == "":
        return None, None
    s = str(dd_str)
    low = s.lower()
    mag = _num(s)
    if "long" in low:
        side = "long"
    elif "short" in low:
        side = "short"
    elif mag is not None:
        side = "long" if mag > 0 else ("short" if mag < 0 else None)
    else:
        side = None
    if mag is None:
        return None, side
    up = s.upper()
    if "B" in up:
        mag *= 1e9
    elif "M" in up:
        mag *= 1e6
    return abs(mag), side


def _para_family(paradigm: Optional[str]) -> str:
    """GEX / AG / BOFA / SIDIAL / UNKNOWN — the 4-paradigm framework. [VG]"""
    p = (paradigm or "").upper()
    if p.startswith("GEX"):
        return "GEX"
    if p.startswith("AG"):
        return "AG"
    if p.startswith("BOFA") or p.startswith("BOFA-") or "BOFA" in p:
        return "BOFA"
    if p.startswith("SIDIAL"):
        return "SIDIAL"
    return "UNKNOWN"


# ────────────────────────────────────────────────────────────────────────────
# the rules
# ────────────────────────────────────────────────────────────────────────────

def _factors(inp: dict) -> list[dict]:
    """Each rule votes independently. vote > 0 bullish, < 0 bearish, 0 = context only.

    Every entry carries `why` (shown to the user) and `src` (provenance), so a
    losing streak can be attributed to a specific idea rather than to vibes.
    """
    f: list[dict] = []
    spot = inp.get("spot")
    lis = inp.get("lis")
    target = inp.get("target")
    fam = _para_family(inp.get("paradigm"))
    para = inp.get("paradigm") or "N/A"

    # 1. LIS — the line dealers defend. Rarely breached; side of it sets the floor
    #    or the ceiling. Within LIS_PIVOT_PTS it is a pivot, not a direction. [VG]
    #
    #    GATED ON ALL-TENOR VANNA (user's call, 2026-08-12, then measured):
    #    LIS only holds when long-dated vanna is POSITIVE. Measured over 120 sessions,
    #    LIS read + forward move to close:
    #        ALL-tenor vanna > 0 : n=18  66.7%  mean +14.14  median +12.51
    #        ALL-tenor vanna < 0 : n=45  48.9%  mean  +4.04  median  -0.31  <- no edge
    #    It is specifically the ALL tenor. The week+month+all SUM does not separate
    #    (62.7% vs 59.4%) and THIRTY_NEXT_DAYS actually inverts, so do not "improve"
    #    this by adding tenors together.
    #    This also explains the decay that prompted the question: slots with positive
    #    long vanna fell from 80%/78% (Feb/Mar) to 26-31% (May-Aug), which is exactly
    #    when LIS went from 82% (Jun) to 48% (Jul) to 30% (Aug).
    #    n=18 on the positive side — thin. Forward validation owed.
    vanna_all = inp.get("vanna_all")
    if spot and lis and vanna_all is not None and vanna_all <= 0:
        f.append({"rule": "LIS_BLOCKED", "vote": 0.0, "src": "US",
                  "why": f"LIS {lis:.0f} is NOT reliable today — all-time vanna is negative "
                         f"({vanna_all/1e9:+.1f}B). In this regime the LIS read has run 48.9% "
                         f"with a median of -0.3 pts, i.e. no edge. Treat the line as "
                         f"decoration, not support."})
    elif spot and lis:
        d = spot - lis
        if abs(d) < LIS_PIVOT_PTS:
            f.append({"rule": "LIS", "vote": 0.0, "src": "VG",
                      "why": f"Spot is sitting ON the LIS ({lis:.0f}, {d:+.0f}p) — a pivot, "
                             f"not a direction. Expect two-way trade until it picks a side."})
        elif d > 0:
            f.append({"rule": "LIS", "vote": 1.0, "src": "VG",
                      "why": f"Spot {spot:.0f} is {d:+.0f}p ABOVE the LIS {lis:.0f} — "
                             f"LIS acts as the floor. Dips into it get defended."})
        else:
            f.append({"rule": "LIS", "vote": -1.0, "src": "VG",
                      "why": f"Spot {spot:.0f} is {d:+.0f}p BELOW the LIS {lis:.0f} — "
                             f"LIS acts as the ceiling. Rallies into it get sold."})

    # 2. Paradigm — how dealers are positioned, i.e. whether the day mean-reverts
    #    or trends. BofA is the highest-conviction of the four. [VG]
    if fam == "GEX" and spot and target:
        d = target - spot
        f.append({"rule": "PARADIGM", "vote": 1.0 if d > 0 else -1.0, "src": "VG",
                  "why": f"{para}: dealers are long gamma, so the day mean-reverts and the "
                         f"target {target:.0f} is a MAGNET ({d:+.0f}p away). Fade extremes "
                         f"toward it rather than chasing breaks."})
    elif fam == "BOFA" and spot and target:
        d = target - spot
        f.append({"rule": "PARADIGM", "vote": 2.0 if d > 0 else -2.0, "src": "VG",
                  "why": f"{para}: the strongest of the four paradigms (79-82% hit rate in "
                         f"the white paper). Target {target:.0f} is {d:+.0f}p away and should "
                         f"pull price."})
    elif fam == "AG":
        mom = inp.get("mom_pts")
        v = 0.0 if mom is None else (1.0 if mom > 0 else -1.0)
        f.append({"rule": "PARADIGM", "vote": v, "src": "VG",
                  "why": f"{para}: dealers are SHORT gamma — hedging AMPLIFIES the move "
                         f"instead of damping it. Trend-follow, widen stops, and do not fade. "
                         f"Session move so far {('%+.0f' % mom) if mom is not None else 'n/a'}p."})
    elif fam == "SIDIAL":
        f.append({"rule": "PARADIGM", "vote": 0.0, "src": "VG",
                  "why": f"{para}: messy/no clean dealer structure. Lowest-quality day type — "
                         f"size down and demand a level, not a story."})
    else:
        f.append({"rule": "PARADIGM", "vote": 0.0, "src": "VG",
                  "why": f"Paradigm {para}: no directional read."})

    # 3. Delta-decay hedging — which way dealers must trade as the day decays. [VG]+[US]
    dd_mag, dd_side = _dd_signed(inp.get("dd_str"))
    if dd_side:
        strong = dd_mag is not None and dd_mag >= 2e9
        v = (1.0 if dd_side == "long" else -1.0) * (1.5 if strong else 1.0)
        # Show it as $X.XB, not the raw '$-422,284,376' the scraper stores.
        mag_txt = f" ${dd_mag/1e9:.1f}B" if dd_mag else ""
        f.append({"rule": "DD_HEDGING", "vote": v, "src": "VG",
                  "why": f"DD hedging is {dd_side.upper()}{mag_txt}"
                         + (" (large)" if strong else "")
                         + f" — dealers have to {'BUY dips' if dd_side == 'long' else 'SELL rallies'} "
                           f"as the day decays."})

    # 4. Charm — the 0DTE greek: how delta bleeds toward expiry. [VG]
    #    Sign only. This is the least-proven rule here; the scorecard exists to find out.
    charm = inp.get("charm")
    if charm is not None:
        v = 0.5 if charm > 0 else -0.5
        f.append({"rule": "CHARM", "vote": v, "src": "VG",
                  "why": f"Aggregated charm is {'POSITIVE' if charm > 0 else 'NEGATIVE'} "
                         f"({charm/1e9:+.1f}B) — drift into the close leans "
                         f"{'up' if charm > 0 else 'down'}. Weakest rule here; being scored."})

    # 5. Gamma walls (TradeStation chain gamma, NOT Volland gamma — see memory
    #    feedback_gex_means_ts_gamma). +gamma caps, -gamma accelerates. [US]
    gp, gn = inp.get("max_pos_gamma"), inp.get("max_neg_gamma")
    if spot and gp and gn:
        if gn < spot < gp:
            f.append({"rule": "GAMMA_WALLS", "vote": 0.0, "src": "US",
                      "why": f"Price is boxed between -gamma {gn:.0f} and +gamma {gp:.0f} "
                             f"— that band is the day's expected range. Edges are the trade."})
        elif spot >= gp:
            f.append({"rule": "GAMMA_WALLS", "vote": -0.5, "src": "US",
                      "why": f"Spot is at/through the +gamma wall {gp:.0f} — the cap. "
                             f"Chasing longs here is late."})
        else:
            f.append({"rule": "GAMMA_WALLS", "vote": -1.0, "src": "US",
                      "why": f"Spot is below the -gamma strike {gn:.0f} — dealer hedging "
                             f"ACCELERATES downside. Air pocket, not a bargain."})

    # 6. Vol regime. overvix >= +2 = stress; VIX/VIX3M < 0.83 = complacency,
    #    a documented trim-longs signal from the Discord pros. [DC]+[US]
    vix, vix3m = inp.get("vix"), inp.get("vix3m")
    if vix and vix3m and vix3m > 0:
        ov = vix - vix3m
        ratio = vix / vix3m
        if ov >= 2.0:
            f.append({"rule": "VOL_REGIME", "vote": -1.0, "src": "US",
                      "why": f"VIX {vix:.1f} is ABOVE VIX3M {vix3m:.1f} (overvix {ov:+.1f}) — "
                             f"stressed curve. Downside moves travel; long setups need more proof."})
        elif ratio < 0.83:
            # NB: no bare '<' or '&' anywhere in these strings — the message is sent with
            # Telegram parse_mode=HTML, which would swallow the rest of the line as a tag.
            f.append({"rule": "VOL_REGIME", "vote": -0.5, "src": "DC",
                      "why": f"VIX/VIX3M {ratio:.2f} is under 0.83 — complacency. The Discord "
                             f"desks treat this as a TRIM-LONGS signal, not a short signal."})
        else:
            f.append({"rule": "VOL_REGIME", "vote": 0.0, "src": "US",
                      "why": f"Vol curve normal (VIX {vix:.1f} / VIX3M {vix3m:.1f}, "
                             f"ratio {ratio:.2f}) — no regime warning."})

    # 7. Tech basket vs direction. Confirmed 72% WR vs contradicted 54% over Mar-Jun. [US]
    bp = inp.get("basket_pct")
    if bp is not None:
        if abs(bp) < 0.15:
            f.append({"rule": "TECH_BASKET", "vote": 0.0, "src": "US",
                      "why": f"Tech basket flat ({bp:+.2f}%) — no confirmation either way."})
        else:
            f.append({"rule": "TECH_BASKET", "vote": 1.0 if bp > 0 else -1.0, "src": "US",
                      "why": f"Tech basket {bp:+.2f}% — the names that lead SPX are "
                             f"{'UP' if bp > 0 else 'DOWN'}. Trades with this have run 72% vs "
                             f"54% against."})

    # 8. Gap — context only. Our V12 rule blocks gap longs before 10:00; by the
    #    10:00 briefing that window has closed, so this informs rather than votes. [US]
    gap = inp.get("gap_pts")
    if gap is not None and abs(gap) > 30:
        f.append({"rule": "GAP", "vote": 0.0, "src": "US",
                  "why": f"Large {'gap-up' if gap > 0 else 'gap-down'} ({gap:+.0f}p). Our filter "
                         f"distrusts early longs on these; treat morning strength as suspect "
                         f"until the gap holds."})
    return f


def _levels(inp: dict, bias: str) -> dict:
    """Support / resistance / invalidation from the structural levels we have."""
    spot = inp.get("spot")
    lis = inp.get("lis")
    target = inp.get("target")
    gp, gn = inp.get("max_pos_gamma"), inp.get("max_neg_gamma")
    lo, hi = inp.get("sess_low"), inp.get("sess_high")

    below = [x for x in (lis, gn, lo, target) if x is not None and spot and x < spot]
    above = [x for x in (lis, gp, hi, target) if x is not None and spot and x > spot]
    support = max(below) if below else None
    resistance = min(above) if above else None

    # Invalidation = the level that, if lost, means the read was wrong. LIS is the
    # cleanest such line; fall back to the far side of the day's range.
    if lis is not None and spot:
        invalid = lis
    elif bias == "BULLISH":
        invalid = lo
    elif bias == "BEARISH":
        invalid = hi
    else:
        invalid = None
    return {"support": support, "resistance": resistance, "invalidation": invalid}


def build(slot: str, inp: dict) -> dict:
    """Pure function: inputs -> briefing. No I/O, so it is testable and re-runnable."""
    facs = _factors(inp)
    # Only rules that beat baseline standing alone are allowed to move the bias.
    # Everything else keeps its `why` text and is shown as context with vote 0, so
    # the reader still gets the dealer-positioning picture without it steering the call.
    for f in facs:
        w = VOTING_RULES.get(f["rule"])
        if w is None:
            f["context_only"] = True
            f["raw_vote"] = f["vote"]
            f["vote"] = 0.0
        else:
            f["vote"] = f["vote"] * w
    score = sum(f["vote"] for f in facs)
    if score >= BULL_AT:
        bias = "BULLISH"
    elif score <= BEAR_AT:
        bias = "BEARISH"
    else:
        bias = "RANGE"

    # Confidence = how one-sided the voting was, not how big the score is.
    directional = [f for f in facs if f["vote"] != 0]
    if directional:
        agree = sum(1 for f in directional
                    if (f["vote"] > 0) == (score > 0)) if score != 0 else 0
        conf = max(1, min(5, round(1 + 4 * (agree / len(directional))))) if score != 0 else 2
    else:
        conf = 1
    if bias == "RANGE":
        conf = min(conf, 3)

    lv = _levels(inp, bias)
    return {
        "slot": slot, "bias": bias, "confidence": conf, "score": round(score, 2),
        "spot": inp.get("spot"), "target": inp.get("target"),
        "support": lv["support"], "resistance": lv["resistance"],
        "invalidation": lv["invalidation"],
        "factors": facs, "inputs": inp,
    }


# ────────────────────────────────────────────────────────────────────────────
# rendering
# ────────────────────────────────────────────────────────────────────────────

_BIAS_EMOJI = {"BULLISH": "🟢", "BEARISH": "🔴", "RANGE": "🟡"}


def _playbook(b: dict) -> list[str]:
    """Plain-English 'so what'. Levels, not adjectives."""
    inp = b["inputs"]
    fam = _para_family(inp.get("paradigm"))
    sup, res, tgt = b["support"], b["resistance"], b["target"]
    out: list[str] = []
    if b["bias"] == "BULLISH":
        if fam in ("GEX", "BOFA"):
            out.append(f"Buy dips toward {sup:.0f}" if sup else "Buy dips into support")
            if tgt:
                out.append(f"Upside objective {tgt:.0f}")
            if res:
                out.append(f"Take profit into {res:.0f} — do not chase through it")
        else:
            out.append("Trend day setup: buy strength, do NOT fade rallies")
            if res:
                out.append(f"First friction {res:.0f}")
    elif b["bias"] == "BEARISH":
        if fam in ("GEX", "BOFA"):
            out.append(f"Sell rallies into {res:.0f}" if res else "Sell rallies into resistance")
            if tgt:
                out.append(f"Downside objective {tgt:.0f}")
            if sup:
                out.append(f"Cover into {sup:.0f}")
        else:
            out.append("Short-gamma downside: moves accelerate, do NOT catch the knife")
            if sup:
                out.append(f"Next air pocket below {sup:.0f}")
    else:
        if sup and res:
            out.append(f"Range {sup:.0f} – {res:.0f}: fade the edges, no trades mid-range")
        else:
            out.append("No directional edge — wait for a level to be tested")
    return out


def render(b: dict) -> str:
    inp = b["inputs"]
    em = _BIAS_EMOJI.get(b["bias"], "⚪")
    L: list[str] = [
        f"🧭 <b>{b['slot']} ET Market Briefing</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"{em} <b>BIAS: {b['bias']}</b> · confidence {b['confidence']}/5 "
        f"<i>(score {b['score']:+.1f})</i>",
        "",
        f"SPX <b>{inp.get('spot'):.0f}</b>" if inp.get("spot") else "SPX N/A",
    ]
    ctx = []
    if inp.get("paradigm"):
        ctx.append(f"Paradigm {inp['paradigm']}")
    if inp.get("lis"):
        ctx.append(f"LIS {inp['lis']:.0f}")
    if inp.get("target"):
        ctx.append(f"Target {inp['target']:.0f}")
    if ctx:
        L.append(" · ".join(ctx))
    L.append("")

    voting = [f for f in b["factors"] if f["vote"] != 0]
    context = [f for f in b["factors"] if f["vote"] == 0]
    if voting:
        L.append("<b>What drives the call:</b>")
        for f in voting:
            L.append(f"  {'▲' if f['vote'] > 0 else '▼'} {f['why']}")
        L.append("")
    if context:
        L.append("<b>Context</b> <i>(does not move the bias — measured at/below "
                 "baseline as a direction signal):</i>")
        for f in context:
            L.append(f"  • {f['why']}")
        L.append("")

    L.append("<b>Playbook:</b>")
    for p in _playbook(b):
        L.append(f"  → {p}")
    L.append("")

    lv = []
    if b["support"]:
        lv.append(f"Support <b>{b['support']:.0f}</b>")
    if b["resistance"]:
        lv.append(f"Resist <b>{b['resistance']:.0f}</b>")
    if lv:
        L.append(" · ".join(lv))
    if b["invalidation"]:
        side = "below" if b["bias"] == "BULLISH" else ("above" if b["bias"] == "BEARISH" else "outside")
        L.append(f"❌ <b>Wrong if</b> SPX closes 30min {side} <b>{b['invalidation']:.0f}</b>")

    sc = _scorecard_line()
    if sc:
        L += ["", f"<i>{sc}</i>"]
    L += ["", "<i>Advisory only — does not place or block any trade.</i>"]
    return "\n".join(L)


def _scorecard_line() -> Optional[str]:
    """Running hit-rate, so the briefing always shows its own track record."""
    try:
        with _engine.begin() as c:
            r = c.execute(text("""
                SELECT count(*) n, sum(CASE WHEN (outcome->>'direction_correct')::bool
                                            THEN 1 ELSE 0 END) hit
                FROM market_briefing
                WHERE outcome IS NOT NULL AND outcome ? 'direction_correct'
                  AND bias IN ('BULLISH','BEARISH')
            """)).mappings().first()
        if r and r["n"]:
            return (f"Track record: {r['hit']}/{r['n']} directional calls correct "
                    f"({100.0*r['hit']/r['n']:.0f}%)")
    except Exception:
        pass
    return None


# ────────────────────────────────────────────────────────────────────────────
# persistence + delivery
# ────────────────────────────────────────────────────────────────────────────

def save(b: dict, when: datetime) -> None:
    with _engine.begin() as c:
        c.execute(text("""
            INSERT INTO market_briefing
              (trade_date, slot, ts, spot, bias, confidence, score,
               support, resistance, target, invalidation, factors, inputs)
            VALUES (:d, :slot, :ts, :spot, :bias, :conf, :score,
                    :sup, :res, :tgt, :inv, CAST(:fac AS jsonb), CAST(:inp AS jsonb))
            ON CONFLICT (trade_date, slot) DO UPDATE SET
              ts=EXCLUDED.ts, spot=EXCLUDED.spot, bias=EXCLUDED.bias,
              confidence=EXCLUDED.confidence, score=EXCLUDED.score,
              support=EXCLUDED.support, resistance=EXCLUDED.resistance,
              target=EXCLUDED.target, invalidation=EXCLUDED.invalidation,
              factors=EXCLUDED.factors, inputs=EXCLUDED.inputs
        """), {
            "d": when.date(), "slot": b["slot"], "ts": when, "spot": b["spot"],
            "bias": b["bias"], "conf": b["confidence"], "score": b["score"],
            "sup": b["support"], "res": b["resistance"], "tgt": b["target"],
            "inv": b["invalidation"],
            "fac": json.dumps(b["factors"], default=str),
            "inp": json.dumps(b["inputs"], default=str),
        })


def run_and_send(slot: str, ctx: dict, when: Optional[datetime] = None) -> Optional[dict]:
    """Build, store and send one briefing. Never raises."""
    try:
        when = when or datetime.now()
        inp = dict(ctx)
        inp.update(_db_inputs(when))
        b = build(slot, inp)
        try:
            save(b, when)
        except Exception as e:
            print(f"[briefing] save failed (sending anyway): {e}", flush=True)
        if _send_telegram:
            _send_telegram(render(b))
        print(f"[briefing] {slot} {b['bias']} conf={b['confidence']} score={b['score']}",
              flush=True)
        return b
    except Exception as e:
        print(f"[briefing] error: {e}\n{traceback.format_exc()}", flush=True)
        return None


def _db_inputs(when: datetime) -> dict:
    """Session path + tech basket, read here so main.py does not have to."""
    out: dict[str, Any] = {}
    try:
        with _engine.begin() as c:
            r = c.execute(text("""
                SELECT min(bar_low) lo, max(bar_high) hi,
                       (array_agg(bar_open ORDER BY ts))[1] op
                FROM spx_ohlc_1m
                WHERE trade_date = :d
            """), {"d": when.date()}).mappings().first()
            if r and r["lo"] is not None:
                out["sess_low"] = float(r["lo"])
                out["sess_high"] = float(r["hi"])
                out["sess_open"] = float(r["op"]) if r["op"] is not None else None
            # semi_basket.et is `timestamp WITHOUT time zone` holding NAIVE ET, so it
            # must be compared against a naive ET value. Handing Postgres a tz-aware
            # datetime silently shifts the comparison to the UTC wall clock and reads a
            # basket value ~4 hours later — which is how the first backtest of this
            # module "measured" a 79% hit rate that was pure lookahead. Live
            # basket_gate.py:113 already does this correctly; copy it, don't re-invent.
            naive_et = when.replace(tzinfo=None)
            b = c.execute(text("""
                SELECT basket_pct FROM semi_basket
                WHERE et <= :t AND et >= :cut ORDER BY et DESC LIMIT 1
            """), {"t": naive_et, "cut": naive_et - timedelta(minutes=15)}).first()
            if b and b[0] is not None:
                out["basket_pct"] = float(b[0])
            # All-time net vanna — the gate on the LIS rule (see _factors). Read here so
            # main.py needs no change. ts_utc IS timezone-aware, unlike semi_basket.
            snap = c.execute(text("""
                SELECT max(ts_utc) FROM volland_exposure_points
                WHERE greek='vanna' AND ticker='SPX' AND expiration_option='ALL'
                  AND ts_utc <= :t AND ts_utc > :t0
            """), {"t": when, "t0": when - timedelta(minutes=45)}).scalar()
            if snap:
                v = c.execute(text("""
                    SELECT SUM(value) FROM volland_exposure_points
                    WHERE greek='vanna' AND ticker='SPX' AND expiration_option='ALL'
                      AND ts_utc = :s
                """), {"s": snap}).scalar()
                if v is not None:
                    out["vanna_all"] = float(v)
    except Exception as e:
        print(f"[briefing] db inputs partial: {e}", flush=True)
    return out


# ────────────────────────────────────────────────────────────────────────────
# scoring — "where is our understanding gap"
# ────────────────────────────────────────────────────────────────────────────

def score_day(trade_date: _date) -> int:
    """Grade every stored call for a date against what SPX actually did after it.

    Records, per call: was the direction right, how far it ran the right way (MFE)
    and the wrong way (MAE), whether the stated target printed, and whether the
    invalidation broke. Per-rule votes are copied alongside so a later pass can ask
    'which rule is actually carrying these calls?'.
    """
    n = 0
    try:
        with _engine.begin() as c:
            rows = c.execute(text("""
                SELECT id, slot, ts, spot, bias, confidence, target, invalidation, factors
                FROM market_briefing
                WHERE trade_date = :d
            """), {"d": trade_date}).mappings().all()
            for r in rows:
                path = c.execute(text("""
                    SELECT bar_high h, bar_low l, bar_close cl, ts
                    FROM spx_ohlc_1m
                    WHERE trade_date = :d AND ts > :t
                    ORDER BY ts
                """), {"d": trade_date, "t": r["ts"]}).mappings().all()
                if not path or r["spot"] is None:
                    continue
                spot = float(r["spot"])
                hi = max(float(p["h"]) for p in path)
                lo = min(float(p["l"]) for p in path)
                close = float(path[-1]["cl"])
                move = close - spot

                if r["bias"] == "BULLISH":
                    correct = move > 0
                    mfe, mae = hi - spot, spot - lo
                elif r["bias"] == "BEARISH":
                    correct = move < 0
                    mfe, mae = spot - lo, hi - spot
                else:
                    correct = abs(move) <= NEUTRAL_BAND_PTS
                    mfe, mae = 0.0, max(hi - spot, spot - lo)

                tgt = float(r["target"]) if r["target"] is not None else None
                tgt_hit = None
                if tgt is not None:
                    tgt_hit = (hi >= tgt) if tgt > spot else (lo <= tgt)
                inv = float(r["invalidation"]) if r["invalidation"] is not None else None
                inv_hit = None
                if inv is not None and r["bias"] in ("BULLISH", "BEARISH"):
                    inv_hit = (lo <= inv) if r["bias"] == "BULLISH" else (hi >= inv)

                outcome = {
                    "direction_correct": bool(correct),
                    "close": round(close, 2), "move_pts": round(move, 2),
                    "mfe_pts": round(mfe, 2), "mae_pts": round(mae, 2),
                    "sess_high_after": round(hi, 2), "sess_low_after": round(lo, 2),
                    "target_hit": tgt_hit, "invalidation_hit": inv_hit,
                    "bars": len(path),
                    "rule_votes": {f["rule"]: f["vote"] for f in (r["factors"] or [])},
                    "scored_at": datetime.now().isoformat(timespec="seconds"),
                }
                c.execute(text("UPDATE market_briefing SET outcome = CAST(:o AS jsonb) "
                               "WHERE id = :id"),
                          {"o": json.dumps(outcome, default=str), "id": r["id"]})
                n += 1
    except Exception as e:
        print(f"[briefing] score_day error: {e}", flush=True)
    if n:
        print(f"[briefing] scored {n} call(s) for {trade_date}", flush=True)
    return n


def score_range(days: int = 60) -> dict:
    """Aggregate scorecard, incl. per-rule attribution. For analysis, not alerts."""
    out: dict[str, Any] = {"overall": {}, "by_bias": {}, "by_rule": {}}
    try:
        with _engine.begin() as c:
            rows = c.execute(text("""
                SELECT bias, confidence, outcome, factors
                FROM market_briefing
                WHERE outcome IS NOT NULL
                  AND trade_date >= current_date - CAST(:d AS INTEGER)
            """), {"d": days}).mappings().all()
        if not rows:
            return out
        # Headline = DIRECTIONAL calls only. RANGE is an abstention (see NEUTRAL_BAND_PTS).
        dirs = [r for r in rows if r["bias"] in ("BULLISH", "BEARISH")]
        tot = len(dirs)
        hit = sum(1 for r in dirs if (r["outcome"] or {}).get("direction_correct"))
        out["overall"] = {
            "n": tot, "correct": hit,
            "pct": round(100.0 * hit / tot, 1) if tot else None,
            "abstained": len(rows) - tot,
            "called_pct": round(100.0 * tot / len(rows), 1),
        }
        for bias in ("BULLISH", "BEARISH", "RANGE"):
            sub = [r for r in rows if r["bias"] == bias]
            if sub:
                h = sum(1 for r in sub if (r["outcome"] or {}).get("direction_correct"))
                out["by_bias"][bias] = {"n": len(sub), "correct": h,
                                        "pct": round(100.0 * h / len(sub), 1)}
        # Per-rule: when this rule voted WITH a directional call, was the call right?
        # Only directional rows — on a RANGE row "agreed with the bias" is meaningless,
        # and counting it there is what made an earlier pass report nonsense per-rule
        # numbers (every negative vote scored as 'agreeing' with RANGE).
        tally: dict[str, list[int]] = {}
        for r in dirs:
            ok = bool((r["outcome"] or {}).get("direction_correct"))
            for f in (r["factors"] or []):
                if f.get("vote", 0) == 0:
                    continue
                agreed = (f["vote"] > 0) == (r["bias"] == "BULLISH")
                if not agreed:
                    continue
                t = tally.setdefault(f["rule"], [0, 0])
                t[0] += 1
                t[1] += 1 if ok else 0
        out["by_rule"] = {k: {"n": v[0], "correct": v[1],
                              "pct": round(100.0 * v[1] / v[0], 1) if v[0] else None}
                          for k, v in sorted(tally.items())}
    except Exception as e:
        out["error"] = str(e)
    return out
