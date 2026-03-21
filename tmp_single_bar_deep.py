"""
Deep analysis of Single-Bar Absorption with Volland confluence filters.
Explores: paradigm, DD hedging, charm, LIS proximity, time of day,
bar close position, SL/TGT combos, and combined filters.
"""

import json, sys, re
from datetime import datetime, time as dtime, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

# ── Load data ─────────────────────────────────────────────
print("Loading data...")
with open("tmp_rithmic_bars.json") as f:
    raw_bars = json.load(f)
with open("tmp_volland_stats.json") as f:
    raw_volland = json.load(f)

print(f"Bars: {len(raw_bars)}, Volland snapshots: {len(raw_volland)}")

# ── Parse Volland timestamps & index by time ──────────────
volland_by_ts = []
for v in raw_volland:
    try:
        ts = v["ts"]
        if "+" in ts or ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts).replace(tzinfo=ZoneInfo("UTC"))
        volland_by_ts.append({"dt": dt, **v})
    except Exception:
        pass
volland_by_ts.sort(key=lambda x: x["dt"])
print(f"Parsed {len(volland_by_ts)} Volland snapshots with timestamps")


def parse_dd_numeric(s):
    """Parse DD hedging string like '$1,017,442,846' to numeric."""
    if not s or s == "$0":
        return 0
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0


def parse_lis_numeric(s):
    """Parse LIS string like '$6,700' to numeric."""
    if not s or s == "N/A":
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def parse_charm_numeric(v):
    """Parse charm value (can be int, string, or None)."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace("$", "").replace(",", ""))
    except Exception:
        return None


def get_volland_at(dt):
    """Get most recent Volland snapshot before given datetime."""
    # Binary search for efficiency
    lo, hi = 0, len(volland_by_ts) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if volland_by_ts[mid]["dt"] <= dt:
            result = volland_by_ts[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return result


# ── Parse bars ────────────────────────────────────────────
bars_by_date = defaultdict(list)
for r in raw_bars:
    ts_e = r["ts_e"]
    try:
        if "+" in ts_e or ts_e.endswith("Z"):
            dt = datetime.fromisoformat(ts_e.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts_e)
        et = dt.astimezone(NY)
    except Exception:
        et = None

    bars_by_date[r["td"]].append({
        "idx": r["idx"],
        "open": r["o"],
        "high": r["h"],
        "low": r["l"],
        "close": r["c"],
        "volume": r["vol"],
        "delta": r["delta"],
        "cvd": r["cvd"],
        "et": et,
        "dt_utc": dt if et else None,
    })

dates = sorted(bars_by_date.keys())
print(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)\n")

# ── Signal detection with enrichment ─────────────────────
MARKET_START = dtime(10, 0)
MARKET_END = dtime(15, 45)
COOLDOWN_BARS = 10

# Use loose thresholds to capture ALL candidates, then filter afterward
MIN_VOL = 1.5
MIN_DELTA = 1.5

all_signals = []

for td in dates:
    bars = bars_by_date[td]
    last_bull_idx = -100
    last_bear_idx = -100

    for i, bar in enumerate(bars):
        if i < 20:
            continue
        if bar["et"] is None:
            continue
        bar_time = bar["et"].time()
        if not (MARKET_START <= bar_time <= MARKET_END):
            continue

        # Volume gate
        prior_vols = [b["volume"] for b in bars[i - 20:i]]
        vol_avg = sum(prior_vols) / len(prior_vols)
        if vol_avg <= 0:
            continue
        vol_ratio = bar["volume"] / vol_avg
        if vol_ratio < MIN_VOL:
            continue

        # Delta gate
        prior_deltas = [abs(b["delta"]) for b in bars[i - 20:i]]
        delta_avg = sum(prior_deltas) / len(prior_deltas)
        if delta_avg <= 0:
            continue
        delta_ratio = abs(bar["delta"]) / delta_avg
        if delta_ratio < MIN_DELTA:
            continue

        # Single-bar absorption
        is_red = bar["close"] < bar["open"]
        is_green = bar["close"] > bar["open"]
        delta_pos = bar["delta"] > 0
        delta_neg = bar["delta"] < 0

        direction = None
        if is_red and delta_pos:
            direction = "bearish"
        elif is_green and delta_neg:
            direction = "bullish"
        if direction is None:
            continue

        # Cooldown
        if direction == "bearish":
            if bar["idx"] - last_bear_idx < COOLDOWN_BARS:
                continue
            last_bear_idx = bar["idx"]
        else:
            if bar["idx"] - last_bull_idx < COOLDOWN_BARS:
                continue
            last_bull_idx = bar["idx"]

        # Get Volland data at signal time
        vol_data = get_volland_at(bar["dt_utc"]) if bar["dt_utc"] else None
        paradigm = ""
        dd_hedging = ""
        dd_numeric = 0
        charm_val = None
        lis_val = None
        lis_dist = None
        svb = None

        if vol_data:
            paradigm = (vol_data.get("paradigm") or "").upper()
            dd_hedging = vol_data.get("dd_hedging") or ""
            dd_numeric = parse_dd_numeric(dd_hedging)
            charm_val = parse_charm_numeric(vol_data.get("charm"))
            lis_val = parse_lis_numeric(vol_data.get("lis"))
            if lis_val and bar["close"]:
                lis_dist = abs(bar["close"] - lis_val)
            svb_raw = vol_data.get("svb")
            if isinstance(svb_raw, dict):
                svb = svb_raw.get("correlation")
            elif isinstance(svb_raw, (int, float)):
                svb = float(svb_raw)

        # Bar characteristics
        bar_range = bar["high"] - bar["low"]
        close_position = (bar["close"] - bar["low"]) / bar_range if bar_range > 0 else 0.5
        # For bearish: close near bottom = strong rejection. For bullish: close near top = strong.

        # CVD context: CVD trend over prior 8 bars
        if i >= 8:
            cvd_8_ago = bars[i - 8]["cvd"]
            cvd_now = bar["cvd"]
            cvd_trend = cvd_now - cvd_8_ago
        else:
            cvd_trend = 0

        # Price context: price trend over prior 8 bars
        if i >= 8:
            price_8_ago = bars[i - 8]["close"]
            price_trend = bar["close"] - price_8_ago
        else:
            price_trend = 0

        # Forward sim for multiple SL/TGT combos
        entry_price = bar["close"]
        outcomes = {}
        for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (8, 15), (10, 10), (10, 15), (10, 20), (12, 15), (12, 20)]:
            if direction == "bearish":
                tp = entry_price - tgt
                sp = entry_price + sl
            else:
                tp = entry_price + tgt
                sp = entry_price - sl

            oc = "EXPIRED"
            mfe = 0.0
            mae = 0.0
            for j in range(i + 1, len(bars)):
                fb = bars[j]
                if direction == "bearish":
                    mfe = max(mfe, entry_price - fb["low"])
                    mae = max(mae, fb["high"] - entry_price)
                    if fb["high"] >= sp:
                        oc = "LOSS"
                        if fb["low"] <= tp:
                            oc = "LOSS" if fb["open"] >= entry_price else "WIN"
                        break
                    if fb["low"] <= tp:
                        oc = "WIN"
                        break
                else:
                    mfe = max(mfe, fb["high"] - entry_price)
                    mae = max(mae, entry_price - fb["low"])
                    if fb["low"] <= sp:
                        oc = "LOSS"
                        if fb["high"] >= tp:
                            oc = "LOSS" if fb["open"] <= entry_price else "WIN"
                        break
                    if fb["high"] >= tp:
                        oc = "WIN"
                        break

            outcomes[f"sl{sl}_t{tgt}"] = {"oc": oc, "mfe": mfe, "mae": mae}

        all_signals.append({
            "date": td,
            "time": bar_time.strftime("%H:%M"),
            "hour": bar_time.hour,
            "bar_idx": bar["idx"],
            "direction": direction,
            "entry": entry_price,
            "volume": bar["volume"],
            "vol_ratio": round(vol_ratio, 2),
            "delta": bar["delta"],
            "delta_ratio": round(delta_ratio, 2),
            "close_position": round(close_position, 2),
            "paradigm": paradigm,
            "dd_hedging": dd_hedging,
            "dd_numeric": dd_numeric,
            "charm": charm_val,
            "lis_val": lis_val,
            "lis_dist": round(lis_dist, 1) if lis_dist else None,
            "svb": round(svb, 2) if svb is not None else None,
            "cvd_trend": cvd_trend,
            "price_trend": round(price_trend, 2),
            "bar_range": round(bar_range, 2),
            "outcomes": outcomes,
        })

print(f"Total raw signals (vol>={MIN_VOL}x, delta>={MIN_DELTA}x): {len(all_signals)}")


# ── Helper to analyze a filtered subset ──────────────────
def analyze(signals, label, sl_key="sl8_t10"):
    if not signals:
        print(f"  {label}: 0 signals")
        return 0, 0, 0

    wins = sum(1 for s in signals if s["outcomes"][sl_key]["oc"] == "WIN")
    losses = sum(1 for s in signals if s["outcomes"][sl_key]["oc"] == "LOSS")
    total = wins + losses
    wr = wins / total * 100 if total else 0
    pnl_w = float(sl_key.split("_t")[1]) * wins
    pnl_l = float(sl_key.split("_t")[0].replace("sl", "")) * losses
    pnl = pnl_w - pnl_l
    pf = pnl_w / pnl_l if pnl_l > 0 else 999
    print(f"  {label}: {len(signals)} sig, {wins}W/{losses}L, WR={wr:.0f}%, PnL={pnl:+.1f}, PF={pf:.2f}x")
    return len(signals), wr, pnl


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 1: SL/TGT OPTIMIZATION (all signals at vol>=2.0x, delta>=2.0x)")
print("=" * 90)

filtered = [s for s in all_signals if s["vol_ratio"] >= 2.0 and s["delta_ratio"] >= 2.0]
print(f"\nFiltered: {len(filtered)} signals\n")

print(f"{'SL/TGT':<12} {'Signals':>8} {'W':>4} {'L':>4} {'WR%':>6} {'PnL':>8} {'PF':>7}")
print("-" * 55)
for sl_key in ["sl6_t8", "sl6_t10", "sl8_t10", "sl8_t12", "sl8_t15", "sl10_t10", "sl10_t15", "sl10_t20", "sl12_t15", "sl12_t20"]:
    sl = float(sl_key.split("_t")[0].replace("sl", ""))
    tgt = float(sl_key.split("_t")[1])
    w = sum(1 for s in filtered if s["outcomes"][sl_key]["oc"] == "WIN")
    l = sum(1 for s in filtered if s["outcomes"][sl_key]["oc"] == "LOSS")
    t = w + l
    wr = w / t * 100 if t else 0
    pnl = tgt * w - sl * l
    pf = (tgt * w) / (sl * l) if sl * l > 0 else 999
    print(f"SL={sl:.0f}/T={tgt:.0f}  {len(filtered):>8} {w:>4} {l:>4} {wr:>5.0f}% {pnl:>+8.1f} {pf:>6.2f}x")


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 2: DIRECTION ANALYSIS")
print("=" * 90)

for sl_key in ["sl8_t10", "sl10_t15"]:
    sl = float(sl_key.split("_t")[0].replace("sl", ""))
    tgt = float(sl_key.split("_t")[1])
    print(f"\n--- {sl_key} ---")
    analyze([s for s in filtered if s["direction"] == "bearish"], "Bearish (SHORT)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish"], "Bullish (LONG)", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 3: PARADIGM FILTER")
print("=" * 90)

paradigms_seen = set(s["paradigm"] for s in filtered if s["paradigm"])
print(f"Paradigms in data: {paradigms_seen}")

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    for p_filter in sorted(paradigms_seen):
        sigs = [s for s in filtered if p_filter in s["paradigm"]]
        if sigs:
            analyze(sigs, f"Paradigm contains '{p_filter}'", sl_key)

    # Paradigm alignment with direction
    print("\n  -- Paradigm alignment with direction --")
    # Bearish signals in AG paradigm (aligned)
    analyze([s for s in filtered if s["direction"] == "bearish" and "AG" in s["paradigm"]], "SHORT + AG paradigm (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and "GEX" in s["paradigm"]], "SHORT + GEX paradigm (counter)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and "GEX" in s["paradigm"]], "LONG + GEX paradigm (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and "AG" in s["paradigm"]], "LONG + AG paradigm (counter)", sl_key)
    analyze([s for s in filtered if not s["paradigm"]], "No paradigm data", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 4: DD HEDGING FILTER")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    # DD direction
    dd_long = [s for s in filtered if s["dd_numeric"] > 200_000_000]
    dd_short = [s for s in filtered if s["dd_numeric"] < -200_000_000]
    dd_neutral = [s for s in filtered if abs(s["dd_numeric"]) <= 200_000_000]

    analyze(dd_long, "DD Hedging LONG (> $200M)", sl_key)
    analyze(dd_short, "DD Hedging SHORT (< -$200M)", sl_key)
    analyze(dd_neutral, "DD Hedging NEUTRAL (|DD| <= $200M)", sl_key)

    # DD alignment
    print("\n  -- DD alignment with signal direction --")
    analyze([s for s in filtered if s["direction"] == "bullish" and s["dd_numeric"] > 200_000_000],
            "LONG + DD Long (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and s["dd_numeric"] < -200_000_000],
            "LONG + DD Short (counter)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and s["dd_numeric"] < -200_000_000],
            "SHORT + DD Short (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and s["dd_numeric"] > 200_000_000],
            "SHORT + DD Long (counter)", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 5: CHARM FILTER")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    charm_pos = [s for s in filtered if s["charm"] is not None and s["charm"] > 0]
    charm_neg = [s for s in filtered if s["charm"] is not None and s["charm"] < 0]
    charm_none = [s for s in filtered if s["charm"] is None]

    analyze(charm_pos, "Charm POSITIVE", sl_key)
    analyze(charm_neg, "Charm NEGATIVE", sl_key)
    analyze(charm_none, "Charm N/A", sl_key)

    # Charm alignment
    print("\n  -- Charm alignment --")
    analyze([s for s in filtered if s["direction"] == "bullish" and s["charm"] is not None and s["charm"] > 0],
            "LONG + Charm Positive (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and s["charm"] is not None and s["charm"] < 0],
            "LONG + Charm Negative (counter)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and s["charm"] is not None and s["charm"] < 0],
            "SHORT + Charm Negative (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and s["charm"] is not None and s["charm"] > 0],
            "SHORT + Charm Positive (counter)", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 6: LIS PROXIMITY")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    analyze([s for s in filtered if s["lis_dist"] is not None and s["lis_dist"] <= 5], "LIS within 5 pts", sl_key)
    analyze([s for s in filtered if s["lis_dist"] is not None and 5 < s["lis_dist"] <= 15], "LIS 5-15 pts", sl_key)
    analyze([s for s in filtered if s["lis_dist"] is not None and 15 < s["lis_dist"] <= 30], "LIS 15-30 pts", sl_key)
    analyze([s for s in filtered if s["lis_dist"] is not None and s["lis_dist"] > 30], "LIS > 30 pts", sl_key)
    analyze([s for s in filtered if s["lis_dist"] is None], "No LIS data", sl_key)

    # LIS relative to direction
    print("\n  -- LIS above/below spot --")
    analyze([s for s in filtered if s["lis_val"] is not None and s["lis_val"] > s["entry"]],
            "LIS ABOVE entry (resistance)", sl_key)
    analyze([s for s in filtered if s["lis_val"] is not None and s["lis_val"] <= s["entry"]],
            "LIS BELOW entry (support)", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 7: SPOT-VOL BETA (SVB)")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    analyze([s for s in filtered if s["svb"] is not None and s["svb"] > 0.5], "SVB > 0.5 (strong positive)", sl_key)
    analyze([s for s in filtered if s["svb"] is not None and 0 <= s["svb"] <= 0.5], "SVB 0 to 0.5", sl_key)
    analyze([s for s in filtered if s["svb"] is not None and s["svb"] < 0], "SVB < 0 (negative)", sl_key)
    analyze([s for s in filtered if s["svb"] is None], "SVB N/A", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 8: TIME OF DAY")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    analyze([s for s in filtered if s["hour"] == 10], "10:00-10:59", sl_key)
    analyze([s for s in filtered if s["hour"] == 11], "11:00-11:59", sl_key)
    analyze([s for s in filtered if s["hour"] == 12], "12:00-12:59", sl_key)
    analyze([s for s in filtered if s["hour"] == 13], "13:00-13:59", sl_key)
    analyze([s for s in filtered if s["hour"] >= 14], "14:00+", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 9: BAR CLOSE POSITION (within bar range)")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    # For bearish: close near bottom = strong, close near middle = weak
    bears = [s for s in filtered if s["direction"] == "bearish"]
    bulls = [s for s in filtered if s["direction"] == "bullish"]

    print("  Bearish signals (lower close = more selling strength):")
    analyze([s for s in bears if s["close_position"] < 0.3], "  Close in bottom 30%", sl_key)
    analyze([s for s in bears if 0.3 <= s["close_position"] < 0.6], "  Close in middle", sl_key)
    analyze([s for s in bears if s["close_position"] >= 0.6], "  Close in top 30%", sl_key)

    print("  Bullish signals (higher close = more buying strength):")
    analyze([s for s in bulls if s["close_position"] >= 0.7], "  Close in top 30%", sl_key)
    analyze([s for s in bulls if 0.4 <= s["close_position"] < 0.7], "  Close in middle", sl_key)
    analyze([s for s in bulls if s["close_position"] < 0.4], "  Close in bottom 30%", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 10: CVD TREND CONTEXT (8-bar CVD trend before signal)")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    # For bearish absorption: CVD rising into signal = buyers exhausting at top (aligned)
    # For bullish absorption: CVD falling into signal = sellers exhausting at bottom (aligned)
    print("  Bearish signals:")
    analyze([s for s in filtered if s["direction"] == "bearish" and s["cvd_trend"] > 500],
            "  CVD trending UP into signal (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and -500 <= s["cvd_trend"] <= 500],
            "  CVD flat into signal", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and s["cvd_trend"] < -500],
            "  CVD trending DOWN into signal (counter)", sl_key)

    print("  Bullish signals:")
    analyze([s for s in filtered if s["direction"] == "bullish" and s["cvd_trend"] < -500],
            "  CVD trending DOWN into signal (aligned)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and -500 <= s["cvd_trend"] <= 500],
            "  CVD flat into signal", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and s["cvd_trend"] > 500],
            "  CVD trending UP into signal (counter)", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 11: PRICE TREND CONTEXT (8-bar price move before signal)")
print("=" * 90)

for sl_key in ["sl8_t10"]:
    print(f"\n--- {sl_key} ---")
    # Bearish absorption after a rally = better (exhaustion at top)
    # Bullish absorption after a drop = better (exhaustion at bottom)
    print("  Bearish signals (price trend before):")
    analyze([s for s in filtered if s["direction"] == "bearish" and s["price_trend"] > 5],
            "  Price UP >5pts (rally exhaustion)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and -5 <= s["price_trend"] <= 5],
            "  Price FLAT", sl_key)
    analyze([s for s in filtered if s["direction"] == "bearish" and s["price_trend"] < -5],
            "  Price DOWN >5pts (continuation)", sl_key)

    print("  Bullish signals (price trend before):")
    analyze([s for s in filtered if s["direction"] == "bullish" and s["price_trend"] < -5],
            "  Price DOWN >5pts (drop exhaustion)", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and -5 <= s["price_trend"] <= 5],
            "  Price FLAT", sl_key)
    analyze([s for s in filtered if s["direction"] == "bullish" and s["price_trend"] > 5],
            "  Price UP >5pts (continuation)", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 12: VOLUME RATIO SWEEP (with best SL/TGT)")
print("=" * 90)

for vm in [1.5, 2.0, 2.5, 3.0]:
    for dm in [1.5, 2.0, 2.5, 3.0]:
        sigs = [s for s in all_signals if s["vol_ratio"] >= vm and s["delta_ratio"] >= dm]
        if len(sigs) < 3:
            continue
        print(f"\n  Vol>={vm}x  Delta>={dm}x  ({len(sigs)} signals)")
        for sl_key in ["sl8_t10", "sl10_t15"]:
            analyze(sigs, f"    {sl_key}", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 13: COMBINED BEST FILTERS")
print("=" * 90)

for sl_key in ["sl8_t10", "sl10_t15"]:
    sl = float(sl_key.split("_t")[0].replace("sl", ""))
    tgt = float(sl_key.split("_t")[1])
    print(f"\n--- {sl_key} ---")

    # Filter A: vol>=3.0x (strongest volume spike)
    analyze([s for s in all_signals if s["vol_ratio"] >= 3.0], "Vol >= 3.0x only", sl_key)

    # Filter B: vol>=2.0x + paradigm aligned
    aligned = [s for s in filtered if
               (s["direction"] == "bearish" and "AG" in s["paradigm"]) or
               (s["direction"] == "bullish" and "GEX" in s["paradigm"])]
    analyze(aligned, "Vol>=2x + Paradigm aligned", sl_key)

    # Filter C: vol>=2.0x + DD aligned
    dd_aligned = [s for s in filtered if
                  (s["direction"] == "bullish" and s["dd_numeric"] > 200_000_000) or
                  (s["direction"] == "bearish" and s["dd_numeric"] < -200_000_000)]
    analyze(dd_aligned, "Vol>=2x + DD aligned", sl_key)

    # Filter D: vol>=2.0x + charm aligned
    charm_aligned = [s for s in filtered if
                     (s["direction"] == "bullish" and s["charm"] is not None and s["charm"] > 0) or
                     (s["direction"] == "bearish" and s["charm"] is not None and s["charm"] < 0)]
    analyze(charm_aligned, "Vol>=2x + Charm aligned", sl_key)

    # Filter E: vol>=2.0x + any 2 Volland factors aligned
    def volland_alignment(s):
        score = 0
        d = s["direction"]
        if d == "bullish":
            if "GEX" in s["paradigm"]: score += 1
            if s["dd_numeric"] > 200_000_000: score += 1
            if s["charm"] is not None and s["charm"] > 0: score += 1
        else:
            if "AG" in s["paradigm"]: score += 1
            if s["dd_numeric"] < -200_000_000: score += 1
            if s["charm"] is not None and s["charm"] < 0: score += 1
        if s["lis_dist"] is not None and s["lis_dist"] <= 15: score += 1
        return score

    for min_align in [0, 1, 2, 3]:
        sigs = [s for s in filtered if volland_alignment(s) >= min_align]
        analyze(sigs, f"Vol>=2x + Volland alignment >= {min_align}", sl_key)

    # Filter F: vol>=2.5x + Volland alignment >= 1
    sigs = [s for s in all_signals if s["vol_ratio"] >= 2.5 and s["delta_ratio"] >= 2.0 and volland_alignment(s) >= 1]
    analyze(sigs, "Vol>=2.5x + Delta>=2x + Volland align >= 1", sl_key)

    # Filter G: vol>=2.0x + CVD trend aligned
    cvd_aligned = [s for s in filtered if
                   (s["direction"] == "bearish" and s["cvd_trend"] > 0) or
                   (s["direction"] == "bullish" and s["cvd_trend"] < 0)]
    analyze(cvd_aligned, "Vol>=2x + CVD trend aligned", sl_key)

    # Filter H: vol>=2.0x + price trend aligned (exhaustion)
    price_aligned = [s for s in filtered if
                     (s["direction"] == "bearish" and s["price_trend"] > 0) or
                     (s["direction"] == "bullish" and s["price_trend"] < 0)]
    analyze(price_aligned, "Vol>=2x + Price trend exhaustion", sl_key)

    # Filter I: vol>=2.0x + CVD aligned + price aligned
    both_aligned = [s for s in filtered if
                    ((s["direction"] == "bearish" and s["cvd_trend"] > 0 and s["price_trend"] > 0) or
                     (s["direction"] == "bullish" and s["cvd_trend"] < 0 and s["price_trend"] < 0))]
    analyze(both_aligned, "Vol>=2x + CVD + Price both aligned", sl_key)

    # Filter J: Best combo - vol>=2.0x + delta>=2.5x + CVD aligned
    sigs = [s for s in all_signals if s["vol_ratio"] >= 2.0 and s["delta_ratio"] >= 2.5 and
            ((s["direction"] == "bearish" and s["cvd_trend"] > 0) or
             (s["direction"] == "bullish" and s["cvd_trend"] < 0))]
    analyze(sigs, "Vol>=2x + Delta>=2.5x + CVD aligned", sl_key)

    # Filter K: Kitchen sink - vol>=2.0x + delta>=2.0x + (paradigm OR DD OR charm aligned) + CVD aligned
    def kitchen_sink(s):
        d = s["direction"]
        cvd_ok = (d == "bearish" and s["cvd_trend"] > 0) or (d == "bullish" and s["cvd_trend"] < 0)
        vol_ok = (d == "bullish" and "GEX" in s["paradigm"]) or (d == "bearish" and "AG" in s["paradigm"])
        dd_ok = (d == "bullish" and s["dd_numeric"] > 200_000_000) or (d == "bearish" and s["dd_numeric"] < -200_000_000)
        charm_ok = (d == "bullish" and s["charm"] is not None and s["charm"] > 0) or \
                   (d == "bearish" and s["charm"] is not None and s["charm"] < 0)
        return cvd_ok and (vol_ok or dd_ok or charm_ok)

    sigs = [s for s in filtered if kitchen_sink(s)]
    analyze(sigs, "Vol>=2x + CVD aligned + any Volland factor", sl_key)


# ════════════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 14: INDIVIDUAL TRADE DETAIL (vol>=2.0x, delta>=2.0x)")
print("=" * 90)

print(f"\n{'Date':<12} {'Time':<6} {'Dir':<6} {'Entry':>7} {'V':>5} {'D':>7} {'DR':>4} {'CVDtr':>7} {'PTr':>6} {'Paradigm':<12} {'DD($M)':>8} {'Charm':>10} {'LIS':>7} {'LISDist':>7} {'SL8T10':>7} {'MFE':>5} {'MAE':>5}")
print("-" * 155)
for s in filtered:
    oc = s["outcomes"]["sl8_t10"]
    dd_m = round(s["dd_numeric"] / 1e6) if s["dd_numeric"] else 0
    charm_m = round(s["charm"] / 1e6, 1) if s["charm"] else "n/a"
    lis = round(s["lis_val"]) if s["lis_val"] else "n/a"
    lis_d = s["lis_dist"] if s["lis_dist"] else "n/a"
    d_label = "SHORT" if s["direction"] == "bearish" else "LONG"
    pnl = 10 if oc["oc"] == "WIN" else (-8 if oc["oc"] == "LOSS" else 0)
    print(f"{s['date']:<12} {s['time']:<6} {d_label:<6} {s['entry']:>7.0f} {s['vol_ratio']:>4.1f}x {s['delta']:>+7d} {s['delta_ratio']:>3.1f}x {s['cvd_trend']:>+7d} {s['price_trend']:>+5.1f} {s['paradigm'] or 'n/a':<12} {dd_m:>+7d}M {str(charm_m):>10} {str(lis):>7} {str(lis_d):>7} {oc['oc']:>7} {oc['mfe']:>5.1f} {oc['mae']:>5.1f}")
