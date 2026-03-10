"""Greek Filter V2 — Deep analysis of filter failure modes + improved filters.

Tests 6 filter variants on ALL historical data (Feb 3 - Mar 5 2026):
  1. CURRENT:      alignment < 0 -> block (what eval_trader uses now)
  2. OPTIMAL-v1:   charm-aligned + setup-specific guards (Analysis #8 winner)
  3. PARADIGM:     downgrade alignment when paradigm is bearish/stressed
  4. SVB-REGIME:   disable vanna vote when SVB indicates vol regime shift
  5. MOMENTUM:     weaken filter when intraday price momentum opposes Greeks
  6. HYBRID:       combine paradigm + SVB-regime + momentum (best of all)

Also includes deep SVB/VIX analysis and per-day breakdown to find similar
crash-with-bullish-Greeks days in the historical data.
"""
import os, sys, json, statistics
sys.stdout.reconfigure(encoding='utf-8')
from datetime import datetime, timedelta, time as dtime
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

# ============================================================
# STEP 1: Pull all trades + Greek context (same as v1)
# ============================================================
print("Loading trades from setup_log...")
with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.max_plus_gex, s.paradigm,
               s.outcome_result, s.outcome_pnl,
               s.ts::date as trade_date,
               s.greek_alignment,
               s.vanna_all, s.vanna_weekly, s.vanna_monthly,
               s.spot_vol_beta
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.id
    """)).mappings().all()

print(f"Got {len(trades)} trades from DB")

# Enrich with additional context where DB columns are available
enriched = []
missing_charm = 0

with engine.begin() as conn:
    for t in trades:
        trade_ts = t["ts_et"]
        d = dict(t)

        # Get charm from volland_snapshots (not stored in setup_log)
        agg_charm = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND ts <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": trade_ts}).mappings().first()
        if snap:
            payload = snap["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                stats = payload.get("statistics", {})
                charm_val = stats.get("aggregatedCharm")
                if charm_val is not None:
                    try:
                        agg_charm = float(charm_val)
                    except (ValueError, TypeError):
                        pass
                # Also get SVB from snapshot if not in setup_log
                if d.get("spot_vol_beta") is None:
                    svb_data = stats.get("spot_vol_beta", {})
                    if isinstance(svb_data, dict) and svb_data.get("correlation") is not None:
                        try:
                            d["spot_vol_beta"] = float(svb_data["correlation"])
                        except (ValueError, TypeError):
                            pass

        if agg_charm is None:
            missing_charm += 1

        is_long = d["direction"] in ("long", "bullish")

        # Recompute alignment (for older trades missing greek_alignment column)
        alignment = 0
        if agg_charm is not None:
            alignment += 1 if (agg_charm > 0) == is_long else -1
        vanna_all = d.get("vanna_all")
        if vanna_all is not None:
            alignment += 1 if (vanna_all > 0) == is_long else -1
        spot = d["spot"]
        max_plus_gex = d["max_plus_gex"]
        if spot and max_plus_gex:
            gex_bullish = spot <= max_plus_gex
            alignment += 1 if gex_bullish == is_long else -1

        # Charm aligned
        charm_aligned = None
        if agg_charm is not None:
            charm_aligned = (agg_charm > 0) == is_long

        # Vanna divergence
        vanna_weekly = d.get("vanna_weekly")
        vanna_monthly = d.get("vanna_monthly")
        vanna_divergent = None
        if vanna_weekly is not None and vanna_monthly is not None:
            vanna_divergent = (vanna_weekly > 0) != (vanna_monthly > 0)

        d.update({
            "agg_charm": agg_charm,
            "greek_alignment": alignment,
            "charm_aligned": charm_aligned,
            "vanna_divergent": vanna_divergent,
            "is_long": is_long,
        })
        enriched.append(d)

print(f"Enriched {len(enriched)} trades ({missing_charm} missing charm)")

# ============================================================
# STEP 2: Build per-day context for momentum/regime detection
# ============================================================
print("\nBuilding per-day context...")

# Group trades by date
by_date = defaultdict(list)
for t in enriched:
    by_date[t["trade_date"]].append(t)

# For each day, compute:
# - First signal spot (open reference)
# - Max intraday move from first signal
# - Whether paradigm was bearish/stressed
# - SVB trend (falling vs stable)
day_context = {}
for d, day_trades in sorted(by_date.items()):
    sorted_trades = sorted(day_trades, key=lambda x: x["id"])
    spots = [t["spot"] for t in sorted_trades if t["spot"]]
    svbs = [t["spot_vol_beta"] for t in sorted_trades if t["spot_vol_beta"] is not None]
    paradigms = [t["paradigm"] for t in sorted_trades if t["paradigm"]]
    charms = [t["agg_charm"] for t in sorted_trades if t["agg_charm"] is not None]
    vannas = [t["vanna_all"] for t in sorted_trades if t.get("vanna_all") is not None]

    first_spot = spots[0] if spots else None
    last_spot = spots[-1] if spots else None
    min_spot = min(spots) if spots else None
    max_spot = max(spots) if spots else None

    # Intraday range and direction
    intraday_range = (max_spot - min_spot) if (max_spot and min_spot) else 0
    intraday_direction = (last_spot - first_spot) if (first_spot and last_spot) else 0

    # Paradigm classification
    bearish_paradigms = {"AG-PURE", "AG-LIS", "AG-TARGET", "SIDIAL-EXTREME",
                         "SIDIAL-MESSY", "GEX-LIS", "GEX-TARGET"}
    bullish_paradigms = {"GEX-PURE", "BOFA-PURE", "BOFA-LIS", "BOFA-TARGET",
                         "SIDIAL-BALANCE"}
    paradigm_bearish_pct = sum(1 for p in paradigms if p in bearish_paradigms) / len(paradigms) * 100 if paradigms else 0

    # Alignment distribution
    alignments = [t["greek_alignment"] for t in sorted_trades]
    all_positive = all(a >= 1 for a in alignments) if alignments else False
    all_negative = all(a <= -1 for a in alignments) if alignments else False

    # SVB characteristics
    avg_svb = statistics.mean(svbs) if svbs else None
    min_svb = min(svbs) if svbs else None

    # Charm sign consistency
    charm_all_positive = all(c > 0 for c in charms) if charms else None
    vanna_all_positive = all(v > 0 for v in vannas) if vannas else None

    day_context[d] = {
        "n_trades": len(day_trades),
        "first_spot": first_spot,
        "last_spot": last_spot,
        "intraday_range": intraday_range,
        "intraday_direction": intraday_direction,
        "paradigm_bearish_pct": paradigm_bearish_pct,
        "all_align_positive": all_positive,
        "all_align_negative": all_negative,
        "avg_svb": avg_svb,
        "min_svb": min_svb,
        "charm_all_positive": charm_all_positive,
        "vanna_all_positive": vanna_all_positive,
        "n_wins": sum(1 for t in day_trades if t["outcome_result"] == "WIN"),
        "n_losses": sum(1 for t in day_trades if t["outcome_result"] == "LOSS"),
        "day_pnl": sum(t["outcome_pnl"] or 0 for t in day_trades),
    }

# For momentum check, compute running spot delta per trade
for t in enriched:
    d = t["trade_date"]
    ctx = day_context[d]
    first_spot = ctx["first_spot"]
    if first_spot and t["spot"]:
        t["spot_delta_from_open"] = t["spot"] - first_spot
    else:
        t["spot_delta_from_open"] = 0

    # Per-trade: how much price has moved in the last N signals
    day_trades_before = [x for x in by_date[d] if x["id"] < t["id"] and x["spot"]]
    if day_trades_before:
        recent = day_trades_before[-3:]  # last 3 signals
        avg_recent_spot = statistics.mean(x["spot"] for x in recent)
        t["spot_momentum"] = t["spot"] - avg_recent_spot
    else:
        t["spot_momentum"] = 0

    # Paradigm at signal time
    paradigm = (t.get("paradigm") or "").upper()
    t["paradigm_bearish"] = paradigm in bearish_paradigms
    t["paradigm_bullish"] = paradigm in bullish_paradigms


# ============================================================
# METRICS ENGINE (same as v1)
# ============================================================
def compute_metrics(trades_list, label=""):
    if not trades_list:
        return {"label": label, "n": 0, "wins": 0, "losses": 0, "wr": 0,
                "total_pnl": 0, "avg_daily": 0, "trading_days": 0,
                "trades_per_day": 0, "pf": 0, "max_dd": 0, "sharpe": 0,
                "avg_win": 0, "avg_loss": 0, "max_win_streak": 0,
                "max_loss_streak": 0, "monthly_pts": 0, "worst_day": 0,
                "best_day": 0, "pct_winning_days": 0}

    n = len(trades_list)
    wins = [t for t in trades_list if t["outcome_result"] == "WIN"]
    losses = [t for t in trades_list if t["outcome_result"] == "LOSS"]
    pnl = sum(t["outcome_pnl"] or 0 for t in trades_list)
    wr = len(wins) / n * 100

    daily_pnl = defaultdict(float)
    for t in trades_list:
        daily_pnl[t["trade_date"]] += (t["outcome_pnl"] or 0)
    trading_days = len(daily_pnl)
    avg_daily = pnl / trading_days if trading_days else 0
    daily_vals = [daily_pnl[d] for d in sorted(daily_pnl.keys())]

    cum = peak = max_dd = 0
    for d_val in daily_vals:
        cum += d_val
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    gross_wins = sum(t["outcome_pnl"] for t in wins if t["outcome_pnl"])
    gross_losses = abs(sum(t["outcome_pnl"] for t in losses if t["outcome_pnl"]))
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')

    if len(daily_vals) > 1:
        mean_d = statistics.mean(daily_vals)
        std_d = statistics.stdev(daily_vals)
        sharpe = mean_d / std_d if std_d > 0 else 0
    else:
        sharpe = 0

    avg_win = gross_wins / len(wins) if wins else 0
    avg_loss = gross_losses / len(losses) if losses else 0

    max_loss_streak = cur_streak = 0
    cur_type = None
    for t in sorted(trades_list, key=lambda x: x["id"]):
        r = t["outcome_result"]
        if r == cur_type:
            cur_streak += 1
        else:
            cur_streak = 1
            cur_type = r
        if r == "LOSS":
            max_loss_streak = max(max_loss_streak, cur_streak)

    worst_day = min(daily_vals) if daily_vals else 0
    best_day = max(daily_vals) if daily_vals else 0
    winning_days = sum(1 for d_v in daily_vals if d_v > 0)
    pct_winning_days = winning_days / len(daily_vals) * 100 if daily_vals else 0

    return {
        "label": label, "n": n, "wins": len(wins), "losses": len(losses),
        "wr": wr, "total_pnl": pnl, "avg_daily": avg_daily,
        "trading_days": trading_days, "trades_per_day": n/trading_days if trading_days else 0,
        "pf": pf, "max_dd": max_dd, "sharpe": sharpe,
        "avg_win": avg_win, "avg_loss": avg_loss,
        "max_loss_streak": max_loss_streak,
        "worst_day": worst_day, "best_day": best_day,
        "pct_winning_days": pct_winning_days,
        "monthly_pts": avg_daily * 21,
    }


def print_summary_table(results_list):
    """Print compact comparison table of all filter variants."""
    print(f"\n  {'Filter':<35} {'N':>4} {'WR':>6} {'PnL':>8} {'PF':>6} {'MaxDD':>7} {'Sharpe':>7} {'Worst':>7} {'$/mo':>9}")
    print(f"  {'-'*35} {'-'*4} {'-'*6} {'-'*8} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*9}")
    for m in results_list:
        monthly_usd = m['monthly_pts'] * 5 * 10  # 10 MES
        print(f"  {m['label']:<35} {m['n']:>4} {m['wr']:>5.1f}% {m['total_pnl']:>+7.1f} {m['pf']:>5.2f} {m['max_dd']:>6.1f} {m['sharpe']:>6.3f} {m['worst_day']:>+6.1f} ${monthly_usd:>+7,.0f}")


# ============================================================
# STEP 3: DEEP SVB / VANNA REGIME ANALYSIS
# ============================================================
print(f"\n{'='*80}")
print(f" DEEP SVB / VANNA REGIME ANALYSIS")
print(f"{'='*80}")

# SVB distribution vs outcomes
svb_buckets = {
    "< -0.5 (strong neg)": lambda s: s is not None and s < -0.5,
    "-0.5 to 0 (weak neg)": lambda s: s is not None and -0.5 <= s < 0,
    "0 to 0.3 (neutral)": lambda s: s is not None and 0 <= s < 0.3,
    "0.3 to 0.6 (moderate)": lambda s: s is not None and 0.3 <= s < 0.6,
    "> 0.6 (strong pos)": lambda s: s is not None and s >= 0.6,
    "N/A": lambda s: s is None,
}

print(f"\n  SVB Bucket Analysis (outcome when aligned with direction vs opposed):")
print(f"  {'SVB Range':<22} {'N':>4} {'WR':>6} {'PnL':>8} {'Aligned WR':>11} {'Opposed WR':>11}")
print(f"  {'-'*22} {'-'*4} {'-'*6} {'-'*8} {'-'*11} {'-'*11}")

for label, fn in svb_buckets.items():
    bucket = [t for t in enriched if fn(t.get("spot_vol_beta"))]
    if not bucket:
        continue
    n = len(bucket)
    wr = sum(1 for t in bucket if t["outcome_result"] == "WIN") / n * 100
    pnl = sum(t["outcome_pnl"] or 0 for t in bucket)

    aligned = [t for t in bucket if t["greek_alignment"] >= 1]
    opposed = [t for t in bucket if t["greek_alignment"] <= -1]
    a_wr = sum(1 for t in aligned if t["outcome_result"] == "WIN") / len(aligned) * 100 if aligned else 0
    o_wr = sum(1 for t in opposed if t["outcome_result"] == "WIN") / len(opposed) * 100 if opposed else 0

    print(f"  {label:<22} {n:>4} {wr:>5.1f}% {pnl:>+7.1f} {a_wr:>9.1f}%   {o_wr:>9.1f}%")

# Key question: When SVB > 0 (positive correlation = bullish),
# does vanna_all > 0 still predict bullish outcomes?
print(f"\n  CRITICAL: Vanna direction accuracy by SVB regime:")
print(f"  {'SVB Regime':<25} {'Vanna>0 Long WR':>16} {'Vanna>0 Short WR':>17} {'Net signal':>12}")
print(f"  {'-'*25} {'-'*16} {'-'*17} {'-'*12}")

for svb_label, svb_fn in [
    ("SVB < 0 (vol rising)", lambda s: s is not None and s < 0),
    ("SVB 0-0.3 (calm)", lambda s: s is not None and 0 <= s < 0.3),
    ("SVB > 0.3 (bullish)", lambda s: s is not None and s >= 0.3),
]:
    bucket = [t for t in enriched if svb_fn(t.get("spot_vol_beta"))]
    vanna_pos = [t for t in bucket if t.get("vanna_all") is not None and t["vanna_all"] > 0]

    longs = [t for t in vanna_pos if t["is_long"]]
    shorts = [t for t in vanna_pos if not t["is_long"]]
    l_wr = sum(1 for t in longs if t["outcome_result"] == "WIN") / len(longs) * 100 if longs else 0
    s_wr = sum(1 for t in shorts if t["outcome_result"] == "WIN") / len(shorts) * 100 if shorts else 0

    net = "LONG" if l_wr > s_wr + 5 else ("SHORT" if s_wr > l_wr + 5 else "NEUTRAL")
    print(f"  {svb_label:<25} {l_wr:>5.1f}% (N={len(longs):<3}) {s_wr:>5.1f}% (N={len(shorts):<3}) {net:>12}")


# When vanna is strongly positive AND market is falling — what happens?
print(f"\n  When vanna_all > 1B AND price falling (spot_delta < -10pts):")
crisis_trades = [t for t in enriched
                 if t.get("vanna_all") and t["vanna_all"] > 1e9
                 and t.get("spot_delta_from_open", 0) < -10]
if crisis_trades:
    longs = [t for t in crisis_trades if t["is_long"]]
    shorts = [t for t in crisis_trades if not t["is_long"]]
    print(f"  Total: {len(crisis_trades)} trades")
    l_wr = sum(1 for t in longs if t["outcome_result"] == "WIN") / len(longs) * 100 if longs else 0
    s_wr = sum(1 for t in shorts if t["outcome_result"] == "WIN") / len(shorts) * 100 if shorts else 0
    l_pnl = sum(t["outcome_pnl"] or 0 for t in longs)
    s_pnl = sum(t["outcome_pnl"] or 0 for t in shorts)
    print(f"    Longs:  N={len(longs):>3} WR={l_wr:>5.1f}% PnL={l_pnl:>+7.1f}")
    print(f"    Shorts: N={len(shorts):>3} WR={s_wr:>5.1f}% PnL={s_pnl:>+7.1f}")
    print(f"    -> {'SHORTS BETTER' if s_pnl > l_pnl else 'LONGS BETTER'}")
else:
    print(f"  No trades match this criteria in the dataset")


# ============================================================
# STEP 4: PER-DAY REGIME ANALYSIS — Find "crash with bullish Greeks" days
# ============================================================
print(f"\n\n{'='*80}")
print(f" PER-DAY REGIME ANALYSIS")
print(f"{'='*80}")

print(f"\n  {'Date':<12} {'Trades':>6} {'W/L':>5} {'PnL':>7} {'Dir':>6} {'Range':>6} {'Bear%':>6} {'AllPos':>6} {'SVB':>6} {'Charm+':>7} {'Vanna+':>7}")
print(f"  {'-'*12} {'-'*6} {'-'*5} {'-'*7} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*7} {'-'*7}")

for d in sorted(day_context.keys()):
    ctx = day_context[d]
    dir_str = f"{ctx['intraday_direction']:+.0f}" if ctx['intraday_direction'] else "?"
    svb_str = f"{ctx['avg_svb']:.2f}" if ctx['avg_svb'] is not None else "N/A"
    charm_str = "Yes" if ctx['charm_all_positive'] else ("No" if ctx['charm_all_positive'] is not None else "?")
    vanna_str = "Yes" if ctx['vanna_all_positive'] else ("No" if ctx['vanna_all_positive'] is not None else "?")

    # Flag crash-with-bullish-Greeks days
    flag = ""
    if ctx['intraday_direction'] and ctx['intraday_direction'] < -15 and ctx.get('charm_all_positive') and ctx.get('vanna_all_positive'):
        flag = " <-- CRASH + BULLISH GREEKS"
    elif ctx['intraday_direction'] and ctx['intraday_direction'] < -15:
        flag = " <-- CRASH"
    elif ctx.get('all_align_positive'):
        flag = " <-- ALL ALIGN POSITIVE"

    print(f"  {str(d):<12} {ctx['n_trades']:>6} {ctx['n_wins']}/{ctx['n_losses']:<3} {ctx['day_pnl']:>+6.1f} {dir_str:>6} {ctx['intraday_range']:>5.0f} {ctx['paradigm_bearish_pct']:>5.0f}% {('Y' if ctx['all_align_positive'] else 'N'):>6} {svb_str:>6} {charm_str:>7} {vanna_str:>7}{flag}")


# ============================================================
# STEP 5: FILTER DEFINITIONS
# ============================================================

# --- Filter 1: CURRENT (alignment < 0 -> block) ---
def filter_current(t):
    """What eval_trader uses now: block alignment < 0."""
    if t["greek_alignment"] < 0:
        return False
    return True

# --- Filter 2: OPTIMAL-v1 (charm + setup-specific) ---
def filter_optimal_v1(t):
    """Analysis #8 winner: charm veto + setup-specific guards."""
    if t["charm_aligned"] is not None and not t["charm_aligned"]:
        return False
    if t["setup_name"] == "GEX Long" and t["greek_alignment"] < 1:
        return False
    if t["setup_name"] == "AG Short" and t["greek_alignment"] == -3:
        return False
    if t["setup_name"] == "DD Exhaustion":
        svb = t.get("spot_vol_beta")
        if svb is not None and -0.5 <= svb < 0:
            return False
    return True

# --- Filter 3: PARADIGM CROSS-CHECK ---
def filter_paradigm(t):
    """Downgrade alignment when paradigm contradicts direction.
    If paradigm is bearish but alignment says long -> reduce effective alignment.
    If paradigm is bullish but alignment says short -> reduce effective alignment.
    """
    alignment = t["greek_alignment"]
    is_long = t["is_long"]

    # Paradigm tells us market regime
    paradigm_opposes = (t.get("paradigm_bearish") and is_long) or \
                       (t.get("paradigm_bullish") and not is_long)

    if paradigm_opposes:
        # Paradigm contradicts direction — reduce alignment by 1
        effective_alignment = alignment - 1
    else:
        effective_alignment = alignment

    # Block if effective alignment < 0
    if effective_alignment < 0:
        return False
    return True

# --- Filter 4: SVB-REGIME GATE ---
def filter_svb_regime(t):
    """Disable vanna component when SVB indicates vol regime shift.

    Key insight: when SVB > 0 (positive spot-vol correlation), volatility
    is RISING with price. Positive vanna means dealers must sell delta
    as vol rises in a selloff -> vanna becomes bearish accelerant.

    When SVB drops close to 0 or turns very high, the vanna signal
    becomes unreliable because vol dynamics are in transition.

    Approach: Recompute alignment WITHOUT vanna when SVB < 0.2
    (vol stressed / transitional). This removes the permanently-bullish
    vanna vote that traps us in longs during selloffs.
    """
    svb = t.get("spot_vol_beta")
    alignment = t["greek_alignment"]
    is_long = t["is_long"]

    # If SVB is low/unstable, recompute alignment without vanna
    if svb is not None and svb < 0.2:
        # Reconstruct alignment without vanna
        score = 0
        if t["agg_charm"] is not None:
            score += 1 if (t["agg_charm"] > 0) == is_long else -1
        spot = t["spot"]
        max_plus_gex = t["max_plus_gex"]
        if spot and max_plus_gex:
            gex_bullish = spot <= max_plus_gex
            score += 1 if gex_bullish == is_long else -1
        # Use 2-component alignment (charm + GEX only)
        if score < 0:
            return False
        return True

    # Normal: use full alignment
    if alignment < 0:
        return False
    return True

# --- Filter 5: MOMENTUM OVERRIDE ---
def filter_momentum(t):
    """Weaken filter when price momentum contradicts Greek bias.

    If price has fallen >15 pts from session open and Greeks say bullish,
    the Greek thesis is being overwhelmed. Allow shorts through.

    If price has risen >15 pts from session open and Greeks say bearish,
    allow longs through.
    """
    alignment = t["greek_alignment"]
    is_long = t["is_long"]
    spot_delta = t.get("spot_delta_from_open", 0)

    # Momentum contradicts direction
    # Case 1: Greeks bullish (alignment > 0 for long), but price crashing
    #          -> allow shorts even with negative alignment
    if not is_long and alignment < 0 and spot_delta < -15:
        return True  # Override: let shorts through when market crashing

    # Case 2: Greeks bearish (alignment > 0 for short), but price surging
    #          -> allow longs even with negative alignment
    if is_long and alignment < 0 and spot_delta > 15:
        return True  # Override: let longs through when market surging

    # Normal filter
    if alignment < 0:
        return False
    return True

# --- Filter 5b: MOMENTUM OVERRIDE (more aggressive: 10pt threshold) ---
def filter_momentum_10(t):
    """Same as momentum but with 10pt threshold."""
    alignment = t["greek_alignment"]
    is_long = t["is_long"]
    spot_delta = t.get("spot_delta_from_open", 0)

    if not is_long and alignment < 0 and spot_delta < -10:
        return True
    if is_long and alignment < 0 and spot_delta > 10:
        return True
    if alignment < 0:
        return False
    return True

# --- Filter 6: HYBRID (Paradigm + SVB-Regime + Momentum) ---
def filter_hybrid(t):
    """Best of all approaches combined:
    1. Start with full alignment
    2. Downgrade by 1 if paradigm contradicts
    3. Remove vanna vote if SVB < 0.2 (vol stressed)
    4. Override: allow trades through if momentum > 15pts contradicts
    """
    is_long = t["is_long"]
    spot_delta = t.get("spot_delta_from_open", 0)
    svb = t.get("spot_vol_beta")

    # Step 1: Compute effective alignment
    alignment = 0

    # Charm vote (always use)
    if t["agg_charm"] is not None:
        alignment += 1 if (t["agg_charm"] > 0) == is_long else -1

    # Vanna vote (suppress when SVB < 0.2)
    vanna_all = t.get("vanna_all")
    if vanna_all is not None:
        if svb is None or svb >= 0.2:
            alignment += 1 if (vanna_all > 0) == is_long else -1
        # else: skip vanna vote (unreliable in vol regime shift)

    # GEX vote (always use)
    spot = t["spot"]
    max_plus_gex = t["max_plus_gex"]
    if spot and max_plus_gex:
        gex_bullish = spot <= max_plus_gex
        alignment += 1 if gex_bullish == is_long else -1

    # Step 2: Paradigm cross-check
    paradigm_opposes = (t.get("paradigm_bearish") and is_long) or \
                       (t.get("paradigm_bullish") and not is_long)
    if paradigm_opposes:
        alignment -= 1

    # Step 3: Momentum override
    if not is_long and alignment < 0 and spot_delta < -15:
        return True  # Let shorts through in crash
    if is_long and alignment < 0 and spot_delta > 15:
        return True  # Let longs through in surge

    # Final gate
    if alignment < 0:
        return False
    return True

# --- Filter 7: CHARM-ONLY (simplest possible) ---
def filter_charm_only(t):
    """Only use charm alignment. Ignore vanna and GEX entirely.
    Charm is the single strongest individual signal from Analysis #8.
    """
    if t["charm_aligned"] is not None and not t["charm_aligned"]:
        return False
    return True

# --- Filter 8: ALIGNMENT-MINUS-VANNA (2-component) ---
def filter_no_vanna(t):
    """Use alignment but without vanna vote entirely.
    Keeps charm + GEX, drops the component that inverts during selloffs.
    """
    is_long = t["is_long"]
    score = 0
    if t["agg_charm"] is not None:
        score += 1 if (t["agg_charm"] > 0) == is_long else -1
    spot = t["spot"]
    max_plus_gex = t["max_plus_gex"]
    if spot and max_plus_gex:
        gex_bullish = spot <= max_plus_gex
        score += 1 if gex_bullish == is_long else -1
    if score < 0:
        return False
    return True

# --- Filter 9: SVB-AWARE VANNA (flip vanna sign when SVB indicates inversion) ---
def filter_svb_aware_vanna(t):
    """Instead of removing vanna when SVB is low, INVERT vanna's vote.

    Theory: When SVB < 0 (negative spot-vol correlation = VIX rising as
    market falls), positive vanna means dealers are SELLING delta.
    So +vanna should vote BEARISH, not bullish.

    When SVB > 0.3 (normal bullish correlation), +vanna is genuinely bullish.
    When 0 <= SVB < 0.3, vanna is ambiguous — skip it.
    """
    is_long = t["is_long"]
    svb = t.get("spot_vol_beta")
    vanna_all = t.get("vanna_all")

    score = 0
    # Charm vote (always reliable)
    if t["agg_charm"] is not None:
        score += 1 if (t["agg_charm"] > 0) == is_long else -1

    # Vanna vote (SVB-dependent interpretation)
    if vanna_all is not None:
        if svb is not None and svb < 0:
            # Vol rising: INVERT vanna meaning
            # +vanna = dealers sell delta = bearish
            score += 1 if (vanna_all < 0) == is_long else -1  # INVERTED
        elif svb is not None and svb >= 0.3:
            # Normal: +vanna = bullish
            score += 1 if (vanna_all > 0) == is_long else -1
        # else: SVB 0-0.3, skip vanna (ambiguous)

    # GEX vote
    spot = t["spot"]
    max_plus_gex = t["max_plus_gex"]
    if spot and max_plus_gex:
        gex_bullish = spot <= max_plus_gex
        score += 1 if gex_bullish == is_long else -1

    if score < 0:
        return False
    return True


# ============================================================
# STEP 6: RUN ALL FILTERS
# ============================================================
print(f"\n\n{'='*80}")
print(f" FILTER COMPARISON — ALL HISTORICAL DATA ({len(enriched)} trades)")
print(f"{'='*80}")

baseline = compute_metrics(enriched, "0. BASELINE (no filter)")

filters = [
    ("1. CURRENT (align<0 block)", filter_current),
    ("2. OPTIMAL-v1 (charm+setup)", filter_optimal_v1),
    ("3. PARADIGM cross-check", filter_paradigm),
    ("4. SVB-REGIME (drop vanna SVB<0.2)", filter_svb_regime),
    ("5a. MOMENTUM override (15pt)", filter_momentum),
    ("5b. MOMENTUM override (10pt)", filter_momentum_10),
    ("6. HYBRID (para+svb+momentum)", filter_hybrid),
    ("7. CHARM-ONLY (simplest)", filter_charm_only),
    ("8. NO-VANNA (charm+GEX only)", filter_no_vanna),
    ("9. SVB-AWARE VANNA (invert)", filter_svb_aware_vanna),
]

all_results = [baseline]
filter_details = {}

for name, fn in filters:
    passed = [t for t in enriched if fn(t)]
    blocked = [t for t in enriched if not fn(t)]
    m = compute_metrics(passed, name)
    all_results.append(m)
    filter_details[name] = {"passed": passed, "blocked": blocked, "metrics": m}

print_summary_table(all_results)


# ============================================================
# STEP 7: TODAY-ONLY ANALYSIS (Mar 5, 2026)
# ============================================================
print(f"\n\n{'='*80}")
print(f" TODAY ANALYSIS — Mar 5, 2026")
print(f"{'='*80}")

today_trades = [t for t in enriched if str(t["trade_date"]) == "2026-03-05"]
if today_trades:
    today_baseline = compute_metrics(today_trades, "0. BASELINE (no filter)")
    today_results = [today_baseline]

    for name, fn in filters:
        passed = [t for t in today_trades if fn(t)]
        blocked = [t for t in today_trades if not fn(t)]
        m = compute_metrics(passed, name)
        today_results.append(m)

    print_summary_table(today_results)

    # Show what each filter did today
    print(f"\n  Per-signal breakdown for today:")
    print(f"  {'ID':>4} {'Time':>5} {'Setup':<20} {'Dir':>7} {'Align':>5} {'Outcome':>7} {'PnL':>6} | {'Cur':>3} {'Opt':>3} {'Par':>3} {'SVB':>3} {'Mom':>3} {'Hyb':>3} {'Chm':>3} {'NoV':>3} {'SVA':>3}")
    print(f"  {'-'*4} {'-'*5} {'-'*20} {'-'*7} {'-'*5} {'-'*7} {'-'*6} | {'-'*3} {'-'*3} {'-'*3} {'-'*3} {'-'*3} {'-'*3} {'-'*3} {'-'*3} {'-'*3}")

    filter_fns = [
        filter_current, filter_optimal_v1, filter_paradigm,
        filter_svb_regime, filter_momentum, filter_hybrid,
        filter_charm_only, filter_no_vanna, filter_svb_aware_vanna
    ]

    for t in sorted(today_trades, key=lambda x: x["id"]):
        time_str = str(t["ts_et"].time())[:5] if hasattr(t["ts_et"], 'time') else str(t["ts_et"])[11:16]
        pnl = t["outcome_pnl"] or 0
        decisions = " ".join("P" if fn(t) else "X" for fn in filter_fns)
        decisions_spaced = "   ".join(["P" if fn(t) else "X" for fn in filter_fns])
        print(f"  {t['id']:>4} {time_str:>5} {t['setup_name']:<20} {t['direction']:>7} {t['greek_alignment']:>+4d}  {t['outcome_result']:>7} {pnl:>+5.1f} |  {decisions_spaced}")
else:
    print("  No trades found for Mar 5")


# ============================================================
# STEP 8: WORST-DAY ANALYSIS
# ============================================================
print(f"\n\n{'='*80}")
print(f" WORST-DAY COMPARISON — How each filter handles bad days")
print(f"{'='*80}")

# Find the 5 worst days by baseline PnL
worst_days = sorted(day_context.items(), key=lambda x: x[1]["day_pnl"])[:5]
best_days = sorted(day_context.items(), key=lambda x: x[1]["day_pnl"], reverse=True)[:3]

print(f"\n  5 Worst Days (baseline PnL):")
for d, ctx in worst_days:
    day_trades_all = [t for t in enriched if t["trade_date"] == d]
    print(f"\n  {d} | Baseline: {ctx['day_pnl']:>+6.1f} pts ({ctx['n_wins']}W/{ctx['n_losses']}L) | Range: {ctx['intraday_range']:.0f}pts | Bear%: {ctx['paradigm_bearish_pct']:.0f}%")

    for name, fn in filters:
        passed = [t for t in day_trades_all if fn(t)]
        blocked = [t for t in day_trades_all if not fn(t)]
        p_pnl = sum(t["outcome_pnl"] or 0 for t in passed)
        b_pnl = sum(t["outcome_pnl"] or 0 for t in blocked)
        p_wr = sum(1 for t in passed if t["outcome_result"] == "WIN") / len(passed) * 100 if passed else 0
        print(f"    {name:<35} Pass: {len(passed):>2} ({p_pnl:>+6.1f}) Block: {len(blocked):>2} ({b_pnl:>+6.1f})")


# ============================================================
# STEP 9: TRADE-OFF ANALYSIS — What good trades does each filter block?
# ============================================================
print(f"\n\n{'='*80}")
print(f" BLOCKED TRADE QUALITY ANALYSIS")
print(f"{'='*80}")

for name, fn in filters:
    blocked = [t for t in enriched if not fn(t)]
    blocked_wins = [t for t in blocked if t["outcome_result"] == "WIN"]
    blocked_pnl = sum(t["outcome_pnl"] or 0 for t in blocked)
    good_blocked = sum(t["outcome_pnl"] or 0 for t in blocked_wins)
    bad_blocked = sum(t["outcome_pnl"] or 0 for t in blocked if t["outcome_result"] == "LOSS")
    wr = len(blocked_wins) / len(blocked) * 100 if blocked else 0
    print(f"  {name:<35} Blocked: {len(blocked):>3} | WR: {wr:>5.1f}% | Good blocked: {good_blocked:>+7.1f} | Bad blocked: {bad_blocked:>+7.1f} | Net blocked: {blocked_pnl:>+7.1f}")


# ============================================================
# STEP 10: OPTIMAL RECOMMENDATION
# ============================================================
print(f"\n\n{'='*80}")
print(f" RECOMMENDATION")
print(f"{'='*80}")

# Rank filters by composite score: PnL * Sharpe / max_dd
print(f"\n  Composite Score = (PnL / MaxDD) * Sharpe")
print(f"  Higher is better — rewards P&L efficiency with low drawdown\n")

ranked = []
for m in all_results:
    if m['max_dd'] > 0 and m['n'] > 0:
        composite = (m['total_pnl'] / m['max_dd']) * m['sharpe']
    else:
        composite = 0
    ranked.append((composite, m))

ranked.sort(reverse=True)
for i, (score, m) in enumerate(ranked):
    marker = " <<<" if i == 0 else ""
    print(f"  {i+1:>2}. {m['label']:<35} Composite: {score:>8.2f} | PnL: {m['total_pnl']:>+7.1f} | DD: {m['max_dd']:>5.1f} | Sharpe: {m['sharpe']:>5.3f}{marker}")

print(f"\n  NOTE: Composite score favors high PnL/DD ratio + consistency (Sharpe)")
print(f"  A filter that makes slightly less money but avoids the Mar 5 crash")
print(f"  will score higher than one with max PnL but deep drawdowns.")
