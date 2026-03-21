#!/usr/bin/env python3
"""
Comprehensive backtest: SB (Single-Bar) Absorption + Multi-Bar Absorption.

Part 1: Original SB Absorption — single-bar price/delta divergence
Part 2: Multi-Bar Absorption — 2-bar and 3-bar cluster divergence
Part 3: Comparison table

Run via: railway run -s 0dtealpha -- python tmp_sb_backtest.py
"""

import os
import sys
import json
import re
from datetime import datetime, timedelta, time as dtime, date
from collections import defaultdict

import pytz
import sqlalchemy as sa
from sqlalchemy import text

ET = pytz.timezone("US/Eastern")

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

VOL_WINDOW = 20
SB_CVD_LOOKBACK = 8
SB_CVD_THRESHOLD = 0
SB_COOLDOWN_BARS = 10

# Risk management
SL_PTS = 8.0
T1_PTS = 10.0
T2_TRAIL_BE = 10.0
T2_TRAIL_ACT = 20.0
T2_TRAIL_GAP = 10.0

MARKET_START = dtime(10, 0)
MARKET_END = dtime(15, 55)

# ═══════════════════════════════════════════════════════════════════════════
# DB
# ═══════════════════════════════════════════════════════════════════════════

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

engine = sa.create_engine(DATABASE_URL)

# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADING (with caching)
# ═══════════════════════════════════════════════════════════════════════════

_bar_cache = {}
_volland_cache = {}


def load_bars(trade_date):
    key = str(trade_date)
    if key in _bar_cache:
        return _bar_cache[key]
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_delta, cumulative_delta, ts_start, ts_end
            FROM es_range_bars
            WHERE source = 'rithmic' AND status = 'closed' AND trade_date = :td
            ORDER BY bar_idx
        """), {"td": trade_date}).fetchall()
    bars = []
    for r in rows:
        bars.append({
            "idx": r[0], "open": float(r[1]), "high": float(r[2]),
            "low": float(r[3]), "close": float(r[4]),
            "volume": int(r[5]), "delta": int(r[6]), "cvd": int(r[7]),
            "ts_start": r[8], "ts_end": r[9], "status": "closed",
        })
    _bar_cache[key] = bars
    return bars


def load_volland_for_date(trade_date):
    key = str(trade_date)
    if key in _volland_cache:
        return _volland_cache[key]
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ts, payload
            FROM volland_snapshots
            WHERE ts::date = :td
            AND payload->>'error_event' IS NULL
            AND payload->'statistics' IS NOT NULL
            ORDER BY ts
        """), {"td": trade_date}).fetchall()
    snapshots = []
    for r in rows:
        payload = r[1] if isinstance(r[1], dict) else json.loads(r[1])
        stats = payload.get("statistics", {})
        if not stats or not isinstance(stats, dict):
            continue
        if not any(v for k, v in stats.items() if v):
            continue
        svb_val = None
        svb_raw = stats.get("spot_vol_beta")
        if svb_raw and isinstance(svb_raw, dict):
            try:
                svb_val = float(svb_raw.get("correlation"))
            except (ValueError, TypeError):
                pass
        elif svb_raw is not None:
            try:
                svb_val = float(svb_raw)
            except (ValueError, TypeError):
                pass
        paradigm = (stats.get("paradigm") or "").upper()
        dd_str = stats.get("delta_decay_hedging") or ""
        dd_clean = dd_str.replace("$", "").replace(",", "")
        try:
            dd_numeric = float(dd_clean)
        except (ValueError, TypeError):
            dd_numeric = 0.0
        charm_val = None
        charm_raw = stats.get("aggregatedCharm")
        if charm_raw is not None:
            try:
                charm_val = float(charm_raw)
            except (ValueError, TypeError):
                pass
        lis_val = None
        lis_raw = stats.get("lines_in_sand") or ""
        lis_match = re.search(r'[\d,]+\.?\d*', lis_raw.replace(',', ''))
        if lis_match:
            lis_val = float(lis_match.group())
        snapshots.append({
            "ts": r[0], "svb": svb_val, "paradigm": paradigm,
            "dd_numeric": dd_numeric, "dd_str": dd_str,
            "charm": charm_val, "lis": lis_val,
        })
    _volland_cache[key] = snapshots
    return snapshots


def get_nearest_volland(volland_snaps, bar_ts):
    best = None
    for snap in volland_snaps:
        if snap["ts"] <= bar_ts:
            best = snap
        else:
            break
    return best


def get_trade_dates():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT trade_date
            FROM es_range_bars WHERE source = 'rithmic'
            ORDER BY trade_date
        """)).fetchall()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def is_market_hours(ts):
    if ts is None:
        return False
    ts_et = ts.astimezone(ET)
    return MARKET_START <= ts_et.time() <= MARKET_END


def ts_to_et_str(ts):
    if ts is None:
        return "??:??"
    return ts.astimezone(ET).strftime("%H:%M:%S")


# ═══════════════════════════════════════════════════════════════════════════
# OUTCOME SIMULATION
# ═══════════════════════════════════════════════════════════════════════════

def simulate_outcome(bars, signal_bar_pos, direction, entry_price):
    is_long = (direction == "bullish")
    if is_long:
        t1_target = entry_price + T1_PTS
        initial_stop = entry_price - SL_PTS
    else:
        t1_target = entry_price - T1_PTS
        initial_stop = entry_price + SL_PTS

    t1_hit = False
    t2_hit = False
    t1_pnl = 0.0
    t2_pnl = 0.0
    max_profit = 0.0
    max_loss = 0.0
    trail_stop = None

    forward_bars = bars[signal_bar_pos + 1:]
    for bar in forward_bars:
        if not is_market_hours(bar["ts_end"]):
            break
        bh = bar["high"]
        bl = bar["low"]
        if is_long:
            pnl_hi = bh - entry_price
            pnl_lo = bl - entry_price
        else:
            pnl_hi = entry_price - bl
            pnl_lo = entry_price - bh
        max_profit = max(max_profit, pnl_hi)
        max_loss = min(max_loss, pnl_lo)

        if not t1_hit:
            if is_long:
                if bl <= initial_stop:
                    t1_pnl = -SL_PTS
                    if not t2_hit:
                        t2_pnl = -SL_PTS
                    break
                if bh >= t1_target:
                    t1_pnl = T1_PTS
                    t1_hit = True
            else:
                if bh >= initial_stop:
                    t1_pnl = -SL_PTS
                    if not t2_hit:
                        t2_pnl = -SL_PTS
                    break
                if bl <= t1_target:
                    t1_pnl = T1_PTS
                    t1_hit = True

        if t1_hit and not t2_hit:
            if trail_stop is None:
                trail_stop = entry_price  # BE after T1
            if is_long:
                if max_profit >= T2_TRAIL_ACT:
                    trail_stop = max(trail_stop, entry_price + max_profit - T2_TRAIL_GAP)
                elif max_profit >= T2_TRAIL_BE:
                    trail_stop = max(trail_stop, entry_price)
                if bl <= trail_stop:
                    t2_pnl = max(trail_stop - entry_price, 0)
                    t2_hit = True
            else:
                if max_profit >= T2_TRAIL_ACT:
                    trail_stop = min(trail_stop, entry_price - max_profit + T2_TRAIL_GAP)
                elif max_profit >= T2_TRAIL_BE:
                    trail_stop = min(trail_stop, entry_price)
                if bh >= trail_stop:
                    t2_pnl = max(entry_price - trail_stop, 0)
                    t2_hit = True

    # Expired handling
    if not t1_hit:
        if forward_bars:
            lc = forward_bars[-1]["close"]
            t1_pnl = (lc - entry_price) if is_long else (entry_price - lc)
        t2_pnl = t1_pnl
    elif not t2_hit:
        if forward_bars:
            lc = forward_bars[-1]["close"]
            t2_pnl = max((lc - entry_price) if is_long else (entry_price - lc), 0)

    combined_pnl = (t1_pnl + t2_pnl) / 2.0
    return {
        "t1_pnl": round(t1_pnl, 2), "t2_pnl": round(t2_pnl, 2),
        "combined_pnl": round(combined_pnl, 2),
        "max_profit": round(max_profit, 2), "max_loss": round(max_loss, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════
# DETECTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def detect_sb(bars, pos, vsnap, vol_mult=2.0, delta_mult=2.0, svb_filter=True):
    """Single-bar absorption detection."""
    if pos < VOL_WINDOW + SB_CVD_LOOKBACK:
        return None
    trigger = bars[pos]

    recent_vols = [bars[i]["volume"] for i in range(pos - VOL_WINDOW, pos)]
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < vol_mult:
        return None

    recent_deltas = [abs(bars[i]["delta"]) for i in range(pos - VOL_WINDOW, pos)]
    delta_avg = sum(recent_deltas) / len(recent_deltas)
    if delta_avg <= 0:
        return None
    delta_ratio = abs(trigger["delta"]) / delta_avg
    if delta_ratio < delta_mult:
        return None

    bd = trigger["delta"]
    is_red = trigger["close"] < trigger["open"]
    is_green = trigger["close"] > trigger["open"]
    direction = None
    if is_red and bd > 0:
        direction = "bearish"
    elif is_green and bd < 0:
        direction = "bullish"
    if direction is None:
        return None

    cvd_start = bars[pos - SB_CVD_LOOKBACK]["cvd"]
    cvd_trend = trigger["cvd"] - cvd_start
    if direction == "bearish" and cvd_trend <= SB_CVD_THRESHOLD:
        return None
    if direction == "bullish" and cvd_trend >= -SB_CVD_THRESHOLD:
        return None

    svb_val = vsnap.get("svb") if vsnap else None
    if svb_filter and svb_val is not None and svb_val < 0:
        return None

    # Scoring
    vol_score = min(25, int((vol_ratio - vol_mult) / vol_mult * 25))
    delta_score = min(25, int((delta_ratio - delta_mult) / delta_mult * 25))
    cvd_score = min(20, int(max(abs(cvd_trend), 1) / 500 * 20))
    conf_score = _confluence(direction, trigger["close"], vsnap)
    total = vol_score + delta_score + cvd_score + conf_score
    grade = "A+" if total >= 70 else ("A" if total >= 50 else ("B" if total >= 30 else "C"))

    return {
        "type": "SB", "direction": direction, "grade": grade, "score": total,
        "entry_price": trigger["close"], "bar_idx": trigger["idx"], "bar_pos": pos,
        "vol_ratio": round(vol_ratio, 2), "delta_ratio": round(delta_ratio, 2),
        "bar_delta": bd, "cvd_trend": cvd_trend, "svb": svb_val, "ts": trigger["ts_end"],
    }


def detect_mb(bars, pos, cluster_size, vsnap, use_vol_gate=True, use_delta_gate=True,
              svb_filter=True, vol_mult=2.0, delta_mult=2.0):
    """Multi-bar cluster absorption detection."""
    min_req = VOL_WINDOW + SB_CVD_LOOKBACK + cluster_size - 1
    if pos < min_req:
        return None

    cs = pos - cluster_size + 1
    cluster_bars = [bars[i] for i in range(cs, pos + 1)]
    agg_delta = sum(b["delta"] for b in cluster_bars)
    agg_volume = sum(b["volume"] for b in cluster_bars)
    co = cluster_bars[0]["open"]
    cc = cluster_bars[-1]["close"]
    trigger = bars[pos]

    if use_vol_gate:
        recent_vols = [bars[i]["volume"] for i in range(pos - VOL_WINDOW, pos)]
        vol_avg = sum(recent_vols) / len(recent_vols)
        if vol_avg <= 0:
            return None
        vol_threshold = vol_avg * cluster_size * vol_mult
        if agg_volume < vol_threshold:
            return None
        vol_ratio = agg_volume / (vol_avg * cluster_size)
    else:
        vol_ratio = 0

    if use_delta_gate:
        recent_deltas = [abs(bars[i]["delta"]) for i in range(pos - VOL_WINDOW, pos)]
        delta_avg = sum(recent_deltas) / len(recent_deltas)
        if delta_avg <= 0:
            return None
        delta_threshold = delta_avg * cluster_size * delta_mult
        if abs(agg_delta) < delta_threshold:
            return None
        delta_ratio = abs(agg_delta) / (delta_avg * cluster_size)
    else:
        delta_ratio = 0

    is_red = cc < co
    is_green = cc > co
    direction = None
    if is_red and agg_delta > 0:
        direction = "bearish"
    elif is_green and agg_delta < 0:
        direction = "bullish"
    if direction is None:
        return None

    cvd_ref = cs - SB_CVD_LOOKBACK
    if cvd_ref < 0:
        return None
    cvd_trend = bars[cs]["cvd"] - bars[cvd_ref]["cvd"]
    if direction == "bearish" and cvd_trend <= SB_CVD_THRESHOLD:
        return None
    if direction == "bullish" and cvd_trend >= -SB_CVD_THRESHOLD:
        return None

    svb_val = vsnap.get("svb") if vsnap else None
    if svb_filter and svb_val is not None and svb_val < 0:
        return None

    vol_score = min(25, int((vol_ratio - vol_mult) / vol_mult * 25)) if use_vol_gate and vol_ratio > 0 else 0
    delta_score = min(25, int((delta_ratio - delta_mult) / delta_mult * 25)) if use_delta_gate and delta_ratio > 0 else 0
    cvd_score = min(20, int(max(abs(cvd_trend), 1) / 500 * 20))
    conf_score = _confluence(direction, trigger["close"], vsnap)
    total = vol_score + delta_score + cvd_score + conf_score
    grade = "A+" if total >= 70 else ("A" if total >= 50 else ("B" if total >= 30 else "C"))

    return {
        "type": f"MB-{cluster_size}", "direction": direction, "grade": grade, "score": total,
        "entry_price": cc, "bar_idx": trigger["idx"], "bar_pos": pos,
        "vol_ratio": round(vol_ratio, 2), "delta_ratio": round(delta_ratio, 2),
        "agg_delta": agg_delta, "cvd_trend": cvd_trend, "svb": svb_val,
        "cluster_open": co, "cluster_close": cc, "ts": trigger["ts_end"],
    }


def _confluence(direction, price, vsnap):
    score = 0
    if not vsnap:
        return score
    dd = vsnap.get("dd_numeric", 0)
    if dd != 0:
        if (direction == "bullish" and dd > 0) or (direction == "bearish" and dd < 0):
            score += 10
    par = vsnap.get("paradigm", "")
    if par:
        if (direction == "bullish" and "GEX" in par) or (direction == "bearish" and "AG" in par):
            score += 10
    lis = vsnap.get("lis")
    if lis is not None:
        d = abs(price - lis)
        if d <= 5:
            score += 10
        elif d <= 15:
            score += 5
    return score


# ═══════════════════════════════════════════════════════════════════════════
# STATS HELPER
# ═══════════════════════════════════════════════════════════════════════════

def compute_stats(signals, n_days):
    n = len(signals)
    if n == 0:
        return None
    wins = sum(1 for s in signals if s["outcome"]["combined_pnl"] > 0)
    losses = sum(1 for s in signals if s["outcome"]["combined_pnl"] < 0)
    be = n - wins - losses
    wr = wins / n * 100
    total_pnl = sum(s["outcome"]["combined_pnl"] for s in signals)
    avg_pnl = total_pnl / n
    rp = 0; pk = 0; mdd = 0
    for s in signals:
        rp += s["outcome"]["combined_pnl"]
        pk = max(pk, rp)
        mdd = max(mdd, pk - rp)
    gw = sum(s["outcome"]["combined_pnl"] for s in signals if s["outcome"]["combined_pnl"] > 0)
    gl = abs(sum(s["outcome"]["combined_pnl"] for s in signals if s["outcome"]["combined_pnl"] < 0))
    pf = gw / gl if gl > 0 else float('inf')
    mcl = 0; cl = 0
    for s in signals:
        if s["outcome"]["combined_pnl"] < 0:
            cl += 1; mcl = max(mcl, cl)
        else:
            cl = 0
    ppd = total_pnl / n_days if n_days > 0 else 0
    return {"n": n, "wins": wins, "losses": losses, "be": be, "wr": wr,
            "total_pnl": total_pnl, "avg_pnl": avg_pnl, "max_dd": mdd,
            "pf": pf, "pnl_per_day": ppd, "max_consec_loss": mcl}


def print_stats_row(label, st):
    if st is None:
        print(f"{label:<28} {'0':>5}")
        return
    print(f"{label:<28} {st['n']:>5} {st['wr']:>5.1f}% {st['total_pnl']:>+8.1f} "
          f"{st['avg_pnl']:>+6.2f} {st['max_dd']:>6.1f} {st['pf']:>5.2f} "
          f"{st['pnl_per_day']:>+7.2f} {st['max_consec_loss']:>4}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN BACKTEST
# ═══════════════════════════════════════════════════════════════════════════

def run_backtest():
    trade_dates = get_trade_dates()
    n_days = len(trade_dates)
    print(f"\n{'='*110}")
    print(f"  SB & Multi-Bar Absorption Backtest — {n_days} trading days")
    print(f"  Range: {trade_dates[0]} to {trade_dates[-1]}")
    print(f"  SL={SL_PTS} | T1={T1_PTS} | T2=trail(BE@{T2_TRAIL_BE}/act@{T2_TRAIL_ACT}/gap{T2_TRAIL_GAP})")
    print(f"{'='*110}\n")

    # Preload all data into cache
    print("Loading all bar data and Volland snapshots into memory...")
    for td in trade_dates:
        load_bars(td)
        load_volland_for_date(td)
    print(f"  Loaded {sum(len(v) for v in _bar_cache.values())} bars, "
          f"{sum(len(v) for v in _volland_cache.values())} volland snapshots\n")

    variants = ["SB", "MB-2", "MB-3", "MB-2-nogate", "MB-3-nogate"]
    all_signals = {v: [] for v in variants}
    per_date = {v: defaultdict(list) for v in variants}
    mar20_signals = {v: [] for v in variants}

    for td in trade_dates:
        bars = _bar_cache[str(td)]
        volland_snaps = _volland_cache[str(td)]
        if len(bars) < VOL_WINDOW + SB_CVD_LOOKBACK + 5:
            print(f"--- {td} --- Skipping ({len(bars)} bars)")
            continue

        cooldowns = {v: {"bullish": -100, "bearish": -100} for v in variants}
        ds = str(td)
        counts = {v: 0 for v in variants}

        for pos in range(VOL_WINDOW + SB_CVD_LOOKBACK, len(bars)):
            bar = bars[pos]
            if not is_market_hours(bar["ts_end"]):
                continue
            vsnap = get_nearest_volland(volland_snaps, bar["ts_end"])

            # SB
            sig = detect_sb(bars, pos, vsnap)
            if sig:
                d = sig["direction"]
                if bar["idx"] - cooldowns["SB"][d] >= SB_COOLDOWN_BARS:
                    cooldowns["SB"][d] = bar["idx"]
                    sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                    sig["date"] = ds
                    all_signals["SB"].append(sig)
                    per_date["SB"][ds].append(sig)
                    counts["SB"] += 1
                    if ds == "2026-03-20":
                        mar20_signals["SB"].append(sig)

            # MB-2 gated
            if pos >= VOL_WINDOW + SB_CVD_LOOKBACK + 1:
                sig = detect_mb(bars, pos, 2, vsnap)
                if sig:
                    d = sig["direction"]
                    if bar["idx"] - cooldowns["MB-2"][d] >= SB_COOLDOWN_BARS:
                        cooldowns["MB-2"][d] = bar["idx"]
                        sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                        sig["date"] = ds
                        all_signals["MB-2"].append(sig)
                        per_date["MB-2"][ds].append(sig)
                        counts["MB-2"] += 1
                        if ds == "2026-03-20":
                            mar20_signals["MB-2"].append(sig)

            # MB-3 gated
            if pos >= VOL_WINDOW + SB_CVD_LOOKBACK + 2:
                sig = detect_mb(bars, pos, 3, vsnap)
                if sig:
                    d = sig["direction"]
                    if bar["idx"] - cooldowns["MB-3"][d] >= SB_COOLDOWN_BARS:
                        cooldowns["MB-3"][d] = bar["idx"]
                        sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                        sig["date"] = ds
                        all_signals["MB-3"].append(sig)
                        per_date["MB-3"][ds].append(sig)
                        counts["MB-3"] += 1
                        if ds == "2026-03-20":
                            mar20_signals["MB-3"].append(sig)

            # MB-2 no gates
            if pos >= VOL_WINDOW + SB_CVD_LOOKBACK + 1:
                sig = detect_mb(bars, pos, 2, vsnap, use_vol_gate=False, use_delta_gate=False)
                if sig:
                    d = sig["direction"]
                    if bar["idx"] - cooldowns["MB-2-nogate"][d] >= SB_COOLDOWN_BARS:
                        cooldowns["MB-2-nogate"][d] = bar["idx"]
                        sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                        sig["date"] = ds
                        all_signals["MB-2-nogate"].append(sig)
                        per_date["MB-2-nogate"][ds].append(sig)
                        counts["MB-2-nogate"] += 1
                        if ds == "2026-03-20":
                            mar20_signals["MB-2-nogate"].append(sig)

            # MB-3 no gates
            if pos >= VOL_WINDOW + SB_CVD_LOOKBACK + 2:
                sig = detect_mb(bars, pos, 3, vsnap, use_vol_gate=False, use_delta_gate=False)
                if sig:
                    d = sig["direction"]
                    if bar["idx"] - cooldowns["MB-3-nogate"][d] >= SB_COOLDOWN_BARS:
                        cooldowns["MB-3-nogate"][d] = bar["idx"]
                        sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                        sig["date"] = ds
                        all_signals["MB-3-nogate"].append(sig)
                        per_date["MB-3-nogate"][ds].append(sig)
                        counts["MB-3-nogate"] += 1
                        if ds == "2026-03-20":
                            mar20_signals["MB-3-nogate"].append(sig)

        print(f"--- {td} --- SB={counts['SB']} MB2={counts['MB-2']} MB3={counts['MB-3']} "
              f"MB2ng={counts['MB-2-nogate']} MB3ng={counts['MB-3-nogate']}")

    # ═══════════════════════════════════════════════════════════════════
    # SUMMARY TABLE
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  PART 1 & 2: SUMMARY TABLE")
    print(f"{'='*110}")
    hdr = f"{'Variant':<28} {'Sigs':>5} {'WR%':>6} {'TotalPnL':>9} {'AvgPnL':>7} {'MaxDD':>7} {'PF':>6} {'PnL/Day':>8} {'MCL':>4}"
    print(hdr)
    print("-" * 95)
    for v in variants:
        st = compute_stats(all_signals[v], n_days)
        print_stats_row(v, st)

    # ═══════════════════════════════════════════════════════════════════
    # PER-DATE BREAKDOWN
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  PER-DATE BREAKDOWN")
    print(f"{'='*110}")

    for v in ["SB", "MB-2", "MB-3"]:
        sigs = all_signals[v]
        if not sigs:
            continue
        print(f"\n--- {v} ---")
        print(f"{'Date':<12} {'Sigs':>4} {'W':>3} {'L':>3} {'PnL':>8} {'Details'}")
        print("-" * 100)
        for td in trade_dates:
            ds = str(td)
            day_sigs = per_date[v].get(ds, [])
            if not day_sigs:
                print(f"{ds:<12} {'0':>4}")
                continue
            n = len(day_sigs)
            w = sum(1 for s in day_sigs if s["outcome"]["combined_pnl"] > 0)
            l = sum(1 for s in day_sigs if s["outcome"]["combined_pnl"] < 0)
            pnl = sum(s["outcome"]["combined_pnl"] for s in day_sigs)
            details = []
            for s in day_sigs:
                ts = ts_to_et_str(s["ts"])
                dr = "L" if s["direction"] == "bullish" else "S"
                details.append(f"{ts} {dr}{s['outcome']['combined_pnl']:+.1f}")
            print(f"{ds:<12} {n:>4} {w:>3} {l:>3} {pnl:>+7.1f}  {', '.join(details)}")

    # ═══════════════════════════════════════════════════════════════════
    # DIRECTION BREAKDOWN
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  DIRECTION BREAKDOWN")
    print(f"{'='*110}")
    for v in variants:
        sigs = all_signals[v]
        if not sigs:
            continue
        print(f"\n--- {v} ---")
        for dr in ["bullish", "bearish"]:
            ds = [s for s in sigs if s["direction"] == dr]
            n = len(ds)
            if n == 0:
                print(f"  {dr}: 0 signals")
                continue
            w = sum(1 for s in ds if s["outcome"]["combined_pnl"] > 0)
            pnl = sum(s["outcome"]["combined_pnl"] for s in ds)
            print(f"  {dr}: {n} sigs, {w}W/{n-w}L, WR={w/n*100:.1f}%, PnL={pnl:+.1f}")

    # ═══════════════════════════════════════════════════════════════════
    # GRADE BREAKDOWN
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  GRADE BREAKDOWN")
    print(f"{'='*110}")
    for v in ["SB", "MB-2", "MB-3"]:
        sigs = all_signals[v]
        if not sigs:
            continue
        print(f"\n--- {v} ---")
        for g in ["A+", "A", "B", "C"]:
            gs = [s for s in sigs if s["grade"] == g]
            n = len(gs)
            if n == 0:
                continue
            w = sum(1 for s in gs if s["outcome"]["combined_pnl"] > 0)
            pnl = sum(s["outcome"]["combined_pnl"] for s in gs)
            print(f"  {g}: {n} sigs, {w}W/{n-w}L, WR={w/n*100:.1f}%, PnL={pnl:+.1f}")

    # ═══════════════════════════════════════════════════════════════════
    # PART 3: OVERLAP ANALYSIS
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  PART 3: OVERLAP ANALYSIS")
    print(f"{'='*110}")

    def sig_keys(sigs, prox=5):
        return set((s["date"], s["bar_idx"] // prox) for s in sigs)

    for v1 in variants:
        for v2 in variants:
            if v1 >= v2:
                continue
            k1 = sig_keys(all_signals[v1])
            k2 = sig_keys(all_signals[v2])
            both = len(k1 & k2)
            only1 = len(k1 - k2)
            only2 = len(k2 - k1)
            print(f"  {v1:<14} vs {v2:<14}: overlap={both}, {v1}-only={only1}, {v2}-only={only2}")

    print(f"\n--- Unique signals in MB that SB misses ---")
    sb_keys = sig_keys(all_signals["SB"])
    for v in ["MB-2", "MB-3", "MB-2-nogate", "MB-3-nogate"]:
        mb_keys = sig_keys(all_signals[v])
        unique = mb_keys - sb_keys
        usigs = [s for s in all_signals[v] if (s["date"], s["bar_idx"] // 5) in unique]
        if usigs:
            w = sum(1 for s in usigs if s["outcome"]["combined_pnl"] > 0)
            pnl = sum(s["outcome"]["combined_pnl"] for s in usigs)
            n = len(usigs)
            print(f"  {v} unique: {n} sigs, WR={w/n*100:.1f}%, PnL={pnl:+.1f}")
            for s in usigs[:20]:  # Show first 20
                dr = "L" if s["direction"] == "bullish" else "S"
                ts = ts_to_et_str(s["ts"])
                print(f"    {s['date']} {ts} {dr} @ {s['entry_price']:.2f} PnL={s['outcome']['combined_pnl']:+.1f} MFE={s['outcome']['max_profit']:+.1f}")
        else:
            print(f"  {v}: no unique signals vs SB")

    # ═══════════════════════════════════════════════════════════════════
    # MARCH 20 BOTTOM VALIDATION
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  MARCH 20 BOTTOM VALIDATION (all signals)")
    print(f"{'='*110}")
    for v in variants:
        sigs = mar20_signals[v]
        if not sigs:
            print(f"  {v}: NO signals on Mar 20")
            continue
        print(f"\n  --- {v} ---")
        for s in sigs:
            ts = ts_to_et_str(s["ts"])
            dr = "LONG" if s["direction"] == "bullish" else "SHORT"
            o = s["outcome"]
            delta_val = s.get("bar_delta", s.get("agg_delta", 0))
            print(f"  {ts} {dr} @ {s['entry_price']:.2f} | {s['grade']} score={s['score']} "
                  f"| PnL={o['combined_pnl']:+.1f} MFE={o['max_profit']:+.1f} "
                  f"| vol_r={s.get('vol_ratio', 0):.1f}x delta_r={s.get('delta_ratio', 0):.1f}x "
                  f"| delta={delta_val:+d} cvd_t={s['cvd_trend']:+d}")

    # ═══════════════════════════════════════════════════════════════════
    # LATE vs EARLY SESSION
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  LATE SESSION (>=14:00) vs EARLY (<14:00)")
    print(f"{'='*110}")
    cutoff = dtime(14, 0)
    for v in variants:
        sigs = all_signals[v]
        if not sigs:
            continue
        early = [s for s in sigs if s["ts"].astimezone(ET).time() < cutoff]
        late = [s for s in sigs if s["ts"].astimezone(ET).time() >= cutoff]
        for label, group in [("Early", early), ("Late", late)]:
            n = len(group)
            if n == 0:
                continue
            w = sum(1 for g in group if g["outcome"]["combined_pnl"] > 0)
            pnl = sum(g["outcome"]["combined_pnl"] for g in group)
            print(f"  {v:<14} {label:<6}: {n:>3} sigs, WR={w/n*100:.1f}%, PnL={pnl:+.1f}")

    # ═══════════════════════════════════════════════════════════════════
    # INDIVIDUAL TRADE LOG (SB)
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*130}")
    print(f"  INDIVIDUAL TRADE LOG — SB (1-bar)")
    print(f"{'='*130}")
    print(f"{'Date':<12} {'Time':>8} {'Dir':>5} {'Entry':>8} {'Grd':>4} {'Scr':>4} "
          f"{'T1':>6} {'T2':>6} {'PnL':>7} {'MFE':>6} {'MAE':>7} {'Vol':>5} {'DltR':>5} {'Delta':>7} {'CVD':>7}")
    print("-" * 130)
    for s in all_signals["SB"]:
        o = s["outcome"]
        print(f"{s['date']:<12} {ts_to_et_str(s['ts']):>8} "
              f"{'LONG' if s['direction']=='bullish' else 'SHORT':>5} "
              f"{s['entry_price']:>7.2f} {s['grade']:>4} {s['score']:>4} "
              f"{o['t1_pnl']:>+5.1f} {o['t2_pnl']:>+5.1f} {o['combined_pnl']:>+6.1f} "
              f"{o['max_profit']:>+5.1f} {o['max_loss']:>+6.1f} "
              f"{s['vol_ratio']:>4.1f}x {s['delta_ratio']:>4.1f}x {s['bar_delta']:>+6d} {s['cvd_trend']:>+6d}")

    # ═══════════════════════════════════════════════════════════════════
    # MFE ANALYSIS
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  MFE ANALYSIS — How far do signals reach?")
    print(f"{'='*110}")
    for v in ["SB", "MB-2", "MB-3", "MB-2-nogate", "MB-3-nogate"]:
        sigs = all_signals[v]
        n = len(sigs)
        if n == 0:
            continue
        mfes = sorted([s["outcome"]["max_profit"] for s in sigs])
        avg_mfe = sum(mfes) / n
        p50 = mfes[n // 2]
        p75 = mfes[int(n * 0.75)] if n >= 4 else mfes[-1]
        r5 = sum(1 for m in mfes if m >= 5) / n * 100
        r8 = sum(1 for m in mfes if m >= 8) / n * 100
        r10 = sum(1 for m in mfes if m >= 10) / n * 100
        r15 = sum(1 for m in mfes if m >= 15) / n * 100
        r20 = sum(1 for m in mfes if m >= 20) / n * 100
        print(f"\n  {v}: n={n}, avg MFE={avg_mfe:.1f}, median={p50:.1f}, p75={p75:.1f}")
        print(f"    Reach 5pt: {r5:.0f}% | 8pt: {r8:.0f}% | 10pt: {r10:.0f}% | 15pt: {r15:.0f}% | 20pt: {r20:.0f}%")

    # ═══════════════════════════════════════════════════════════════════
    # SENSITIVITY: SB with different thresholds
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  SENSITIVITY: SB with different vol/delta multipliers")
    print(f"{'='*110}")

    alt_configs = [
        ("SB v=1.5 d=1.5", 1.5, 1.5),
        ("SB v=2.0 d=1.5", 2.0, 1.5),
        ("SB v=1.5 d=2.0", 1.5, 2.0),
        ("SB v=2.0 d=2.0 (baseline)", 2.0, 2.0),
        ("SB v=2.5 d=2.0", 2.5, 2.0),
        ("SB v=2.0 d=2.5", 2.0, 2.5),
        ("SB v=3.0 d=2.0", 3.0, 2.0),
        ("SB v=2.0 d=3.0", 2.0, 3.0),
        ("SB v=1.5 d=1.0 (loose)", 1.5, 1.0),
        ("SB v=1.0 d=2.0 (no vol)", 1.0, 2.0),
    ]

    print(f"\n{'Config':<30} {'Sigs':>5} {'WR%':>6} {'PnL':>8} {'Avg':>7} {'MaxDD':>7} {'PF':>6}")
    print("-" * 80)

    for label, vm, dm in alt_configs:
        alt_sigs = []
        for td in trade_dates:
            bars = _bar_cache[str(td)]
            volland_snaps = _volland_cache[str(td)]
            if len(bars) < VOL_WINDOW + SB_CVD_LOOKBACK + 5:
                continue
            cd_bull = -100; cd_bear = -100
            for pos in range(VOL_WINDOW + SB_CVD_LOOKBACK, len(bars)):
                bar = bars[pos]
                if not is_market_hours(bar["ts_end"]):
                    continue
                vsnap = get_nearest_volland(volland_snaps, bar["ts_end"])
                sig = detect_sb(bars, pos, vsnap, vol_mult=vm, delta_mult=dm)
                if sig:
                    d = sig["direction"]
                    if d == "bullish":
                        if bar["idx"] - cd_bull < SB_COOLDOWN_BARS:
                            continue
                        cd_bull = bar["idx"]
                    else:
                        if bar["idx"] - cd_bear < SB_COOLDOWN_BARS:
                            continue
                        cd_bear = bar["idx"]
                    sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                    alt_sigs.append(sig)

        st = compute_stats(alt_sigs, n_days)
        if st:
            print(f"{label:<30} {st['n']:>5} {st['wr']:>5.1f}% {st['total_pnl']:>+7.1f} "
                  f"{st['avg_pnl']:>+6.2f} {st['max_dd']:>6.1f} {st['pf']:>5.2f}")
        else:
            print(f"{label:<30} {'0':>5}")

    # ═══════════════════════════════════════════════════════════════════
    # SB WITHOUT SVB FILTER
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  SB WITHOUT SVB FILTER")
    print(f"{'='*110}")
    sb_nosvb = []
    for td in trade_dates:
        bars = _bar_cache[str(td)]
        volland_snaps = _volland_cache[str(td)]
        if len(bars) < VOL_WINDOW + SB_CVD_LOOKBACK + 5:
            continue
        cd_bull = -100; cd_bear = -100
        for pos in range(VOL_WINDOW + SB_CVD_LOOKBACK, len(bars)):
            bar = bars[pos]
            if not is_market_hours(bar["ts_end"]):
                continue
            vsnap = get_nearest_volland(volland_snaps, bar["ts_end"])
            sig = detect_sb(bars, pos, vsnap, svb_filter=False)
            if sig:
                d = sig["direction"]
                if d == "bullish":
                    if bar["idx"] - cd_bull < SB_COOLDOWN_BARS:
                        continue
                    cd_bull = bar["idx"]
                else:
                    if bar["idx"] - cd_bear < SB_COOLDOWN_BARS:
                        continue
                    cd_bear = bar["idx"]
                sig["outcome"] = simulate_outcome(bars, pos, d, sig["entry_price"])
                sig["date"] = str(td)
                sb_nosvb.append(sig)

    st_orig = compute_stats(all_signals["SB"], n_days)
    st_nosvb = compute_stats(sb_nosvb, n_days)
    print(f"  With SVB filter:    ", end="")
    print_stats_row("", st_orig)
    print(f"  Without SVB filter: ", end="")
    print_stats_row("", st_nosvb)

    if st_nosvb and st_orig:
        blocked = [s for s in sb_nosvb if s.get("svb") is not None and s["svb"] < 0]
        if blocked:
            w = sum(1 for s in blocked if s["outcome"]["combined_pnl"] > 0)
            pnl = sum(s["outcome"]["combined_pnl"] for s in blocked)
            print(f"  SVB-blocked: {len(blocked)} sigs, {w}W/{len(blocked)-w}L, PnL={pnl:+.1f}")

    # ═══════════════════════════════════════════════════════════════════
    # ALTERNATIVE RM: Fixed target only (no trail)
    # ═══════════════════════════════════════════════════════════════════

    print(f"\n{'='*110}")
    print(f"  ALTERNATIVE RM: Fixed SL/TP (no split, no trail)")
    print(f"{'='*110}")

    rm_configs = [
        ("SL=8 TP=8", 8, 8),
        ("SL=8 TP=10", 8, 10),
        ("SL=8 TP=12", 8, 12),
        ("SL=8 TP=15", 8, 15),
        ("SL=10 TP=10", 10, 10),
        ("SL=10 TP=15", 10, 15),
        ("SL=12 TP=10", 12, 10),
        ("SL=5 TP=10", 5, 10),
    ]

    print(f"\n{'Config':<24} {'Sigs':>5} {'WR%':>6} {'PnL':>8} {'Avg':>7} {'MaxDD':>7} {'PF':>6}")
    print("-" * 70)

    for label, sl, tp in rm_configs:
        fixed_sigs = []
        for s in all_signals["SB"]:
            bars = _bar_cache[s["date"]]
            pos = s["bar_pos"]
            ep = s["entry_price"]
            is_long = s["direction"] == "bullish"
            target = ep + tp if is_long else ep - tp
            stop = ep - sl if is_long else ep + sl
            pnl = 0
            for bar in bars[pos + 1:]:
                if not is_market_hours(bar["ts_end"]):
                    break
                if is_long:
                    if bar["low"] <= stop:
                        pnl = -sl; break
                    if bar["high"] >= target:
                        pnl = tp; break
                else:
                    if bar["high"] >= stop:
                        pnl = -sl; break
                    if bar["low"] <= target:
                        pnl = tp; break
            fixed_sigs.append({"outcome": {"combined_pnl": pnl}})

        n = len(fixed_sigs)
        if n == 0:
            print(f"{label:<24} {'0':>5}")
            continue
        w = sum(1 for s in fixed_sigs if s["outcome"]["combined_pnl"] > 0)
        l = sum(1 for s in fixed_sigs if s["outcome"]["combined_pnl"] < 0)
        wr = w / n * 100
        pnl = sum(s["outcome"]["combined_pnl"] for s in fixed_sigs)
        avg = pnl / n
        rp = 0; pk = 0; mdd = 0
        for s in fixed_sigs:
            rp += s["outcome"]["combined_pnl"]
            pk = max(pk, rp)
            mdd = max(mdd, pk - rp)
        gw = sum(s["outcome"]["combined_pnl"] for s in fixed_sigs if s["outcome"]["combined_pnl"] > 0)
        gl = abs(sum(s["outcome"]["combined_pnl"] for s in fixed_sigs if s["outcome"]["combined_pnl"] < 0))
        pf = gw / gl if gl > 0 else float('inf')
        print(f"{label:<24} {n:>5} {wr:>5.1f}% {pnl:>+7.1f} {avg:>+6.2f} {mdd:>6.1f} {pf:>5.2f}")

    print(f"\n{'='*110}")
    print(f"  BACKTEST COMPLETE")
    print(f"{'='*110}\n")


if __name__ == "__main__":
    run_backtest()
