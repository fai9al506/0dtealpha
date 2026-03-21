"""
Stock GEX Backtest v2 — Proper day-by-day simulation.

Strategy: BULLISH ONLY. Buy when price drops below -GEX support levels.
  - Positive GEX = magnets (always pull price toward them)
  - Negative GEX = support when above, magnet when below
  - MMs don't want price to close below -GEX at week end

Grading (data-driven):
  A+: Barely below -GEX (<0.5%) + Tier A/B stock -> ~97% WR
  A:  Moderately below (0.5-1.5%) + Tier A/B     -> ~90% WR
  B:  Deeper (<3%) or 3+ levels breached          -> ~87% WR
  C:  Deep (3%+) + Tier C stock                   -> ~50% WR (skip)

Usage:
  python stock_gex_backtest.py                          # Full backtest + report
  python stock_gex_backtest.py --stock NVDA --verbose   # Single stock detail
  python stock_gex_backtest.py --sweep                  # Stop loss optimization
"""

import json
import os
import re
import sys
import io
import argparse
from datetime import date, timedelta
from collections import defaultdict

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DATA_DIR = os.environ.get("GEX_DATA_DIR", r"C:\Users\Faisa\stock_gex_data")

from app.stock_gex_scanner import _compute_gex, _identify_key_levels

# ── Stock Tiers (from historical performance analysis) ──────────────

TIER_A = {
    "AFRM", "AI", "AMD", "AVGO", "BAC", "CCL", "CVNA", "GOOGL", "INTC",
    "LCID", "MARA", "MU", "PLTR", "PYPL", "QCOM", "ROKU", "SHOP", "SOFI", "TSLA",
}

TIER_C = {"DKNG", "NIO", "PLUG", "RIOT", "XOM"}

# Everything else = Tier B


def get_tier(stock):
    if stock in TIER_A:
        return "A"
    if stock in TIER_C:
        return "C"
    return "B"


# ── Data Loading ────────────────────────────────────────────────────

def load_prices_map(stock):
    path = f"{DATA_DIR}/prices/{stock}.json"
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        bars = json.load(f)
    return {int(b["date"]): b for b in bars}


def load_options(stock, trade_date_str, day_label, label):
    path = f"{DATA_DIR}/options/{stock}/{trade_date_str}_{day_label}_{label}.json"
    if not os.path.exists(path):
        if day_label == "mon":
            path = f"{DATA_DIR}/options/{stock}/{trade_date_str}_{label}.json"
        if not os.path.exists(path):
            return None
    with open(path) as f:
        records = json.load(f)
    if not records:
        return None
    rows = []
    for r in records:
        right = r.get("right", "?")
        if right not in ("C", "P"):
            continue
        strike = r.get("strike_dollars")
        if strike is None:
            strike = r.get("strike", 0) / 1000.0
        oi = r.get("open_interest")
        if oi is None and r.get("_extras"):
            oi = r["_extras"][0]
        gamma = r.get("gamma")
        if gamma is None and r.get("_extras") and len(r["_extras"]) > 2:
            gamma = r["_extras"][1]
        if oi is None or gamma is None:
            continue
        rows.append({"Type": right, "Strike": float(strike),
                     "Gamma": float(gamma) if gamma else 0,
                     "OpenInterest": float(oi) if oi else 0})
    return rows if rows else None


def get_weeks_from_data(stock):
    options_dir = f"{DATA_DIR}/options/{stock}"
    if not os.path.exists(options_dir):
        return []
    mondays = set()
    for f in os.listdir(options_dir):
        if not f.endswith(".json"):
            continue
        m = re.match(r'^(\d{4}-\d{2}-\d{2})_(mon|tue)_(weekly|opex)\.json$', f)
        if m:
            dt = date.fromisoformat(m.group(1))
            day = m.group(2)
            monday = dt if day == "mon" else dt - timedelta(days=1)
            mondays.add(str(monday))
            continue
        m = re.match(r'^(\d{4}-\d{2}-\d{2})_(weekly|opex)\.json$', f)
        if m:
            mondays.add(m.group(1))
    return sorted(mondays)


# ── GEX Level Extraction ───────────────────────────────────────────

def compute_gex_levels(rows, spot):
    """Top 3 positive and top 3 negative GEX strikes, filtered for significance."""
    gex_by_strike = {}
    for r in rows:
        k = r["Strike"]
        g = r["Gamma"] * r["OpenInterest"] * 100
        if r["Type"] == "P":
            g = -g
        gex_by_strike[k] = gex_by_strike.get(k, 0) + g

    if not gex_by_strike:
        return None

    pos = [(k, v) for k, v in gex_by_strike.items() if v > 0]
    neg = [(k, v) for k, v in gex_by_strike.items() if v < 0]
    if not pos or not neg:
        return None

    pos.sort(key=lambda x: x[1], reverse=True)
    neg.sort(key=lambda x: x[1])

    top_pos = pos[:3]
    top_neg = neg[:3]

    # Significance filter: >= 10% of max
    if top_pos:
        mx = top_pos[0][1]
        top_pos = [(k, v) for k, v in top_pos if v >= mx * 0.10]
    if top_neg:
        mx = abs(top_neg[0][1])
        top_neg = [(k, v) for k, v in top_neg if abs(v) >= mx * 0.10]

    if not top_pos or not top_neg:
        return None

    return {
        "pos_levels": [{"strike": k, "gex": v} for k, v in top_pos],
        "neg_levels": [{"strike": k, "gex": v} for k, v in top_neg],
    }


# ── Zone Classification ────────────────────────────────────────────

def classify_zone(spot, neg_levels, pos_levels):
    neg_strikes = sorted([l["strike"] for l in neg_levels])
    pos_strikes = sorted([l["strike"] for l in pos_levels])

    neg_above_spot = [s for s in neg_strikes if s > spot]
    pos_above_spot = [s for s in pos_strikes if s > spot]

    if neg_above_spot:
        return "A", {
            "neg_breached": len(neg_above_spot),
            "neg_above": neg_above_spot,
            "nearest_neg_above": min(neg_above_spot),
            "nearest_pos_above": min(pos_above_spot) if pos_above_spot else None,
        }
    if pos_above_spot:
        return "B", {}
    return "C", {}


# ── New Data-Driven Grading ────────────────────────────────────────

def grade_setup(stock, dist_below_pct, neg_breached, nearest_pos_above, entry_price):
    """Grade based on validated data patterns.

    Key factors (in order of predictive power):
      1. Distance below -GEX (closer = better, rubber band effect)
      2. Stock tier (A = strongest mean reversion)
      3. +GEX magnet proximity (medium distance = sweet spot)
      4. # of -GEX breached (more = deeper dip = higher WR but riskier)
    """
    tier = get_tier(stock)
    score = 0

    # 1. Distance below -GEX (0-40 pts) — MOST IMPORTANT
    if dist_below_pct < 0.3:
        score += 40   # barely below, near-guaranteed snap back
    elif dist_below_pct < 0.5:
        score += 35
    elif dist_below_pct < 1.0:
        score += 28
    elif dist_below_pct < 1.5:
        score += 22
    elif dist_below_pct < 2.5:
        score += 15
    elif dist_below_pct < 4.0:
        score += 8
    else:
        score += 0    # too deep, likely real breakdown

    # 2. Stock tier (0-25 pts)
    if tier == "A":
        score += 25
    elif tier == "B":
        score += 15
    else:
        score += 0    # Tier C

    # 3. +GEX magnet proximity (0-20 pts) — medium is sweet spot
    if nearest_pos_above and entry_price > 0:
        mag_dist_pct = (nearest_pos_above - entry_price) / entry_price * 100
        if 1.0 <= mag_dist_pct <= 3.0:
            score += 20   # close enough to reach, far enough for good move
        elif 3.0 < mag_dist_pct <= 6.0:
            score += 15
        elif 0.3 < mag_dist_pct < 1.0:
            score += 12   # very close, small payoff
        elif 6.0 < mag_dist_pct <= 10.0:
            score += 8
        else:
            score += 3    # too close or too far

    # 4. # of -GEX breached (0-15 pts) — more breached = stronger bounce
    if neg_breached >= 3:
        score += 15
    elif neg_breached == 2:
        score += 10
    else:
        score += 5

    return min(score, 100)


def grade_label(score):
    if score >= 75:
        return "A+"
    elif score >= 55:
        return "A"
    elif score >= 35:
        return "B"
    elif score >= 20:
        return "C"
    else:
        return "D"


# ── Day-by-Day Backtest ────────────────────────────────────────────

def run_backtest_stock(stock, prices_map, stop_pct=2.0, min_grade="D", verbose=False):
    """Proper day-by-day simulation for one stock."""
    weeks = get_weeks_from_data(stock)
    if not weeks:
        return []

    tier = get_tier(stock)
    trades = []

    for monday_str in weeks:
        monday_date = date.fromisoformat(monday_str)
        tuesday_date = monday_date + timedelta(days=1)

        # Load GEX (prefer Tuesday)
        rows = None
        used_day = None
        for day_label, dt in [("tue", str(tuesday_date)), ("mon", monday_str)]:
            for label in ["opex", "weekly"]:
                rows = load_options(stock, dt, day_label, label)
                if rows:
                    used_day = day_label
                    break
            if rows:
                break
        if not rows:
            continue

        gex_date = tuesday_date if used_day == "tue" else monday_date
        gex_bar = prices_map.get(int(gex_date.strftime("%Y%m%d")))
        if not gex_bar:
            for off in range(1, 3):
                alt = gex_date + timedelta(days=off)
                gex_bar = prices_map.get(int(alt.strftime("%Y%m%d")))
                if gex_bar:
                    break
        if not gex_bar:
            continue

        spot_at_gex = gex_bar["close"]
        levels = compute_gex_levels(rows, spot_at_gex)
        if not levels:
            continue

        neg_levels = levels["neg_levels"]
        pos_levels = levels["pos_levels"]

        # Build week bars (day after GEX through Friday)
        start_day = gex_date + timedelta(days=1)
        week_bars = []
        for d_off in range(5):
            day = start_day + timedelta(days=d_off)
            if day.weekday() >= 5:
                continue
            bar = prices_map.get(int(day.strftime("%Y%m%d")))
            if bar:
                week_bars.append({**bar, "date_obj": day})

        if not week_bars:
            continue

        # ── Day-by-day scan ──
        in_trade = False
        trade = None

        for i, bar in enumerate(week_bars):
            day_date = bar["date_obj"]
            day_name = day_date.strftime("%a")
            is_last_bar = (i == len(week_bars) - 1)

            if in_trade:
                # Manage open position
                if bar["low"] <= trade["stop_price"]:
                    trade["exit_price"] = trade["stop_price"]
                    trade["exit_day"] = day_name
                    trade["exit_date"] = str(day_date)
                    trade["exit_reason"] = "STOP"
                    trade["hold_days"] = (day_date - date.fromisoformat(trade["entry_date"])).days
                    in_trade = False
                elif bar["high"] >= trade["t1_price"]:
                    if trade["t2_price"] and bar["high"] >= trade["t2_price"]:
                        trade["exit_price"] = trade["t2_price"]
                        trade["exit_reason"] = "T2_MAGNET"
                    else:
                        trade["exit_price"] = trade["t1_price"]
                        trade["exit_reason"] = "T1_RECOVERY"
                    trade["exit_day"] = day_name
                    trade["exit_date"] = str(day_date)
                    trade["hold_days"] = (day_date - date.fromisoformat(trade["entry_date"])).days
                    in_trade = False
                elif is_last_bar:
                    trade["exit_price"] = bar["close"]
                    trade["exit_day"] = day_name
                    trade["exit_date"] = str(day_date)
                    trade["exit_reason"] = "FRIDAY_CLOSE"
                    trade["hold_days"] = (day_date - date.fromisoformat(trade["entry_date"])).days
                    in_trade = False

                if not in_trade:
                    trade["pnl_pts"] = round(trade["exit_price"] - trade["entry_price"], 2)
                    trade["pnl_pct"] = round(trade["pnl_pts"] / trade["entry_price"] * 100, 2)
                    trade["win"] = trade["pnl_pts"] > 0
                    trades.append(trade)
                    if verbose:
                        w = "WIN" if trade["win"] else "LOSS"
                        print(f"  {stock} {trade['entry_date']}->{trade['exit_date']} "
                              f"[{trade['grade']}] {w} "
                              f"${trade['entry_price']:.2f}->${trade['exit_price']:.2f} "
                              f"{trade['pnl_pct']:+.2f}% ({trade['exit_reason']})")
                    trade = None
            else:
                # Check for new entry
                zone, details = classify_zone(bar["low"], neg_levels, pos_levels)
                if zone != "A":
                    continue

                nearest_neg = details["nearest_neg_above"]
                nearest_pos = details.get("nearest_pos_above")

                # Entry price
                if bar["open"] < nearest_neg:
                    entry_price = bar["open"]
                else:
                    entry_price = min(bar["close"], nearest_neg)

                dist_pct = (nearest_neg - entry_price) / entry_price * 100 if entry_price > 0 else 0

                # Grade
                score = grade_setup(stock, dist_pct, details["neg_breached"],
                                    nearest_pos, entry_price)
                grade = grade_label(score)

                # Filter by min grade
                grade_order = {"A+": 5, "A": 4, "B": 3, "C": 2, "D": 1}
                if grade_order.get(grade, 0) < grade_order.get(min_grade, 0):
                    continue

                # Targets
                t1_price = nearest_neg
                if entry_price >= t1_price:
                    t1_price = nearest_neg * 1.005
                t2_price = nearest_pos

                stop_price = round(entry_price * (1 - stop_pct / 100), 2)

                # Skip gap-down through stop
                if bar["low"] <= stop_price and bar["open"] < nearest_neg:
                    continue

                trade = {
                    "stock": stock, "tier": tier,
                    "monday": monday_str, "gex_day": used_day,
                    "entry_day": day_name, "entry_date": str(day_date),
                    "entry_price": round(entry_price, 2),
                    "stop_price": round(stop_price, 2),
                    "t1_price": round(t1_price, 2),
                    "t2_price": round(t2_price, 2) if t2_price else None,
                    "score": score, "grade": grade,
                    "neg_breached": details["neg_breached"],
                    "dist_below_pct": round(dist_pct, 2),
                    "spot_at_gex": round(spot_at_gex, 2),
                    "neg_strikes": sorted([l["strike"] for l in neg_levels]),
                    "pos_strikes": sorted([l["strike"] for l in pos_levels]),
                }
                in_trade = True

                # Same-bar exit check
                if bar["high"] >= t1_price:
                    if t2_price and bar["high"] >= t2_price:
                        trade["exit_price"] = t2_price
                        trade["exit_reason"] = "T2_MAGNET"
                    else:
                        trade["exit_price"] = t1_price
                        trade["exit_reason"] = "T1_RECOVERY"
                    trade["exit_day"] = day_name
                    trade["exit_date"] = str(day_date)
                    trade["hold_days"] = 0
                    trade["pnl_pts"] = round(trade["exit_price"] - entry_price, 2)
                    trade["pnl_pct"] = round(trade["pnl_pts"] / entry_price * 100, 2)
                    trade["win"] = trade["pnl_pts"] > 0
                    trades.append(trade)
                    if verbose:
                        w = "WIN" if trade["win"] else "LOSS"
                        print(f"  {stock} {day_name} [{grade}] {w} "
                              f"${entry_price:.2f}->${trade['exit_price']:.2f} "
                              f"{trade['pnl_pct']:+.2f}% ({trade['exit_reason']})")
                    trade = None
                    in_trade = False
                elif is_last_bar:
                    trade["exit_price"] = bar["close"]
                    trade["exit_day"] = day_name
                    trade["exit_date"] = str(day_date)
                    trade["exit_reason"] = "FRIDAY_CLOSE"
                    trade["hold_days"] = 0
                    trade["pnl_pts"] = round(bar["close"] - entry_price, 2)
                    trade["pnl_pct"] = round(trade["pnl_pts"] / entry_price * 100, 2)
                    trade["win"] = trade["pnl_pts"] > 0
                    trades.append(trade)
                    if verbose:
                        w = "WIN" if trade["win"] else "LOSS"
                        print(f"  {stock} {day_name} [{grade}] {w} "
                              f"${entry_price:.2f}->${bar['close']:.2f} "
                              f"{trade['pnl_pct']:+.2f}% ({trade['exit_reason']})")
                    trade = None
                    in_trade = False

        if in_trade and trade:
            last_bar = week_bars[-1]
            trade["exit_price"] = last_bar["close"]
            trade["exit_day"] = last_bar["date_obj"].strftime("%a")
            trade["exit_date"] = str(last_bar["date_obj"])
            trade["exit_reason"] = "WEEK_END"
            trade["hold_days"] = (last_bar["date_obj"] - date.fromisoformat(trade["entry_date"])).days
            trade["pnl_pts"] = round(trade["exit_price"] - trade["entry_price"], 2)
            trade["pnl_pct"] = round(trade["pnl_pts"] / trade["entry_price"] * 100, 2)
            trade["win"] = trade["pnl_pts"] > 0
            trades.append(trade)

    return trades


# ── Comprehensive Report ────────────────────────────────────────────

def print_report(all_trades, stop_pct, min_grade):
    if not all_trades:
        print("No trades found.")
        return

    wins = [t for t in all_trades if t["win"]]
    losses = [t for t in all_trades if not t["win"]]
    total_pnl = sum(t["pnl_pct"] for t in all_trades)
    avg_win = sum(t["pnl_pct"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_pct"] for t in losses) / len(losses) if losses else 0
    gross_win = sum(t["pnl_pct"] for t in wins)
    gross_loss = abs(sum(t["pnl_pct"] for t in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else 999
    unique_weeks = sorted(set(t["monday"] for t in all_trades))
    date_range = f"{unique_weeks[0]} to {unique_weeks[-1]}"

    # Equity curve (cumulative by week)
    weekly_pnl = defaultdict(float)
    for t in all_trades:
        weekly_pnl[t["monday"]] += t["pnl_pct"]
    cum = 0
    max_cum = 0
    max_dd = 0
    for w in sorted(weekly_pnl):
        cum += weekly_pnl[w]
        max_cum = max(max_cum, cum)
        dd = max_cum - cum
        max_dd = max(max_dd, dd)

    print()
    print("=" * 80)
    print("  STOCK GEX BACKTEST REPORT")
    print("  Strategy: Buy when price drops below -GEX support levels (bullish only)")
    print("=" * 80)

    print(f"""
  PARAMETERS
  ----------
  Stop loss:        {stop_pct}%
  Min grade:        {min_grade}
  GEX source:       Tuesday EOD (includes Monday positions)
  Target 1:         -GEX level (support recovery)
  Target 2:         +GEX magnet above
  Time exit:        Friday close
  Date range:       {date_range} ({len(unique_weeks)} weeks)
""")

    print(f"""  OVERALL PERFORMANCE
  --------------------
  Total trades:     {len(all_trades)}
  Wins:             {len(wins)} ({len(wins)/len(all_trades)*100:.1f}%)
  Losses:           {len(losses)} ({len(losses)/len(all_trades)*100:.1f}%)
  Win rate:         {len(wins)/len(all_trades)*100:.1f}%

  Avg win:          {avg_win:+.2f}% (stock move)
  Avg loss:         {avg_loss:+.2f}%
  Avg trade:        {total_pnl/len(all_trades):+.2f}%
  Profit factor:    {pf:.2f}

  Total P&L:        {total_pnl:+.1f}% (sum of stock % moves)
  Per week:         {total_pnl/len(unique_weeks):+.1f}%/week
  Max drawdown:     {max_dd:.1f}% (cumulative weekly)

  Trades/week:      {len(all_trades)/len(unique_weeks):.1f}
  Stocks/week:      {len(set((t['monday'],t['stock']) for t in all_trades))/len(unique_weeks):.1f}
""")

    # ── By Grade ──
    print("  BY GRADE")
    print("  " + "-" * 70)
    print(f"  {'Grade':<6} {'Trades':>7} {'Wins':>6} {'WR':>7} {'AvgP&L':>8} "
          f"{'TotalP&L':>9} {'AvgWin':>8} {'AvgLoss':>8} {'PF':>6}")
    for grade in ["A+", "A", "B", "C", "D"]:
        b = [t for t in all_trades if t["grade"] == grade]
        if not b:
            continue
        bw = [t for t in b if t["win"]]
        bl = [t for t in b if not t["win"]]
        bp = sum(t["pnl_pct"] for t in b)
        aw = sum(t["pnl_pct"] for t in bw) / len(bw) if bw else 0
        al = sum(t["pnl_pct"] for t in bl) / len(bl) if bl else 0
        gw = sum(t["pnl_pct"] for t in bw)
        gl = abs(sum(t["pnl_pct"] for t in bl))
        gpf = gw / gl if gl > 0 else 999
        print(f"  {grade:<6} {len(b):>7} {len(bw):>6} {len(bw)/len(b)*100:>6.1f}% "
              f"{bp/len(b):>+7.2f}% {bp:>+8.1f}% {aw:>+7.2f}% {al:>+7.2f}% {gpf:>5.1f}")

    # ── By Exit Reason ──
    print(f"\n  BY EXIT REASON")
    print("  " + "-" * 70)
    print(f"  {'Reason':<16} {'Trades':>7} {'Wins':>6} {'WR':>7} {'AvgP&L':>8} "
          f"{'TotalP&L':>9} {'%ofTrades':>9}")
    for reason in ["T1_RECOVERY", "T2_MAGNET", "FRIDAY_CLOSE", "STOP"]:
        b = [t for t in all_trades if t["exit_reason"] == reason]
        if not b:
            continue
        bw = sum(1 for t in b if t["win"])
        bp = sum(t["pnl_pct"] for t in b)
        print(f"  {reason:<16} {len(b):>7} {bw:>6} {bw/len(b)*100:>6.1f}% "
              f"{bp/len(b):>+7.2f}% {bp:>+8.1f}% {len(b)/len(all_trades)*100:>8.1f}%")

    # ── By Entry Day ──
    print(f"\n  BY ENTRY DAY")
    print("  " + "-" * 70)
    for day in ["Tue", "Wed", "Thu", "Fri"]:
        b = [t for t in all_trades if t["entry_day"] == day]
        if not b:
            continue
        bw = sum(1 for t in b if t["win"])
        bp = sum(t["pnl_pct"] for t in b)
        print(f"  {day:<6} {len(b):>6}t  WR:{bw/len(b)*100:>5.1f}%  "
              f"avg:{bp/len(b):>+5.2f}%  total:{bp:>+8.1f}%")

    # ── By Distance Below -GEX ──
    print(f"\n  BY DISTANCE BELOW -GEX AT ENTRY")
    print("  " + "-" * 70)
    for name, lo, hi in [("Barely (0-0.5%)", 0, 0.5), ("Moderate (0.5-1.5%)", 0.5, 1.5),
                          ("Deep (1.5-3%)", 1.5, 3), ("Very deep (3%+)", 3, 100)]:
        b = [t for t in all_trades if lo <= t["dist_below_pct"] < hi]
        if not b:
            continue
        bw = sum(1 for t in b if t["win"])
        bp = sum(t["pnl_pct"] for t in b)
        print(f"  {name:<24} {len(b):>5}t  WR:{bw/len(b)*100:>5.1f}%  "
              f"avg:{bp/len(b):>+5.2f}%  total:{bp:>+8.1f}%")

    # ── By # Levels Breached ──
    print(f"\n  BY # OF -GEX LEVELS BREACHED")
    print("  " + "-" * 70)
    for n in [1, 2, 3]:
        b = [t for t in all_trades if t["neg_breached"] == n]
        if not b:
            continue
        bw = sum(1 for t in b if t["win"])
        bp = sum(t["pnl_pct"] for t in b)
        print(f"  {n} level(s):  {len(b):>5}t  WR:{bw/len(b)*100:>5.1f}%  "
              f"avg:{bp/len(b):>+5.2f}%  total:{bp:>+8.1f}%")

    # ── By Stock Tier ──
    print(f"\n  BY STOCK TIER")
    print("  " + "-" * 70)
    for tier_name in ["A", "B", "C"]:
        b = [t for t in all_trades if t["tier"] == tier_name]
        if not b:
            continue
        bw = sum(1 for t in b if t["win"])
        bp = sum(t["pnl_pct"] for t in b)
        n_stocks = len(set(t["stock"] for t in b))
        print(f"  Tier {tier_name} ({n_stocks} stocks):  {len(b):>5}t  WR:{bw/len(b)*100:>5.1f}%  "
              f"avg:{bp/len(b):>+5.2f}%  total:{bp:>+8.1f}%")

    # ── Per-Stock Table ──
    print(f"\n  PER-STOCK PERFORMANCE (sorted by P&L)")
    print("  " + "-" * 70)
    print(f"  {'Stock':<7} {'Tier':>4} {'Trades':>7} {'Wins':>5} {'WR':>7} "
          f"{'AvgP&L':>8} {'TotalP&L':>9} {'MaxLoss':>8}")

    stock_data = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0, "pnls": [], "tier": ""})
    for t in all_trades:
        s = stock_data[t["stock"]]
        s["t"] += 1
        if t["win"]:
            s["w"] += 1
        s["pnl"] += t["pnl_pct"]
        s["pnls"].append(t["pnl_pct"])
        s["tier"] = t["tier"]

    for stock in sorted(stock_data, key=lambda s: stock_data[s]["pnl"], reverse=True):
        s = stock_data[stock]
        wr = s["w"] / s["t"] * 100
        avg = s["pnl"] / s["t"]
        ml = min(s["pnls"])
        print(f"  {stock:<7} {s['tier']:>4} {s['t']:>7} {s['w']:>5} {wr:>6.1f}% "
              f"{avg:>+7.2f}% {s['pnl']:>+8.1f}% {ml:>+7.2f}%")

    # ── Weekly P&L ──
    print(f"\n  WEEKLY P&L BREAKDOWN")
    print("  " + "-" * 70)
    print(f"  {'Week':<12} {'Trades':>7} {'Wins':>5} {'WR':>7} {'P&L':>8} {'Cumul':>8}")
    cum = 0
    for w in sorted(weekly_pnl):
        wt = [t for t in all_trades if t["monday"] == w]
        ww = sum(1 for t in wt if t["win"])
        wp = weekly_pnl[w]
        cum += wp
        print(f"  {w:<12} {len(wt):>7} {ww:>5} {ww/len(wt)*100:>6.1f}% "
              f"{wp:>+7.1f}% {cum:>+7.1f}%")

    # ── Top/Bottom Trades ──
    print(f"\n  TOP 10 BEST TRADES")
    print("  " + "-" * 70)
    best = sorted(all_trades, key=lambda t: t["pnl_pct"], reverse=True)[:10]
    for t in best:
        print(f"  {t['stock']:<6} {t['entry_date']} [{t['grade']}] Tier {t['tier']}  "
              f"${t['entry_price']:.2f} -> ${t['exit_price']:.2f}  "
              f"{t['pnl_pct']:+.2f}%  ({t['exit_reason']})")

    print(f"\n  TOP 10 WORST TRADES")
    print("  " + "-" * 70)
    worst = sorted(all_trades, key=lambda t: t["pnl_pct"])[:10]
    for t in worst:
        print(f"  {t['stock']:<6} {t['entry_date']} [{t['grade']}] Tier {t['tier']}  "
              f"${t['entry_price']:.2f} -> ${t['exit_price']:.2f}  "
              f"{t['pnl_pct']:+.2f}%  ({t['exit_reason']})")

    # ── Options P&L Estimate ──
    print(f"\n  ESTIMATED OPTIONS P&L (weekly ATM ~0.30 delta)")
    print("  " + "-" * 70)
    # Rough: stock_move% * leverage ~20-30x for weekly ATM options
    # More conservative: 1% stock move ~ 30-50% option move for ATM weekly
    for name, grade_filter in [("All grades", None), ("A+ only", "A+"),
                                ("A+ and A", {"A+", "A"}), ("B+ (A+, A, B)", {"A+", "A", "B"})]:
        if grade_filter is None:
            b = all_trades
        elif isinstance(grade_filter, str):
            b = [t for t in all_trades if t["grade"] == grade_filter]
        else:
            b = [t for t in all_trades if t["grade"] in grade_filter]
        if not b:
            continue
        bw = sum(1 for t in b if t["win"])
        bp = sum(t["pnl_pct"] for t in b)
        # Conservative: 30x leverage for weekly ATM
        opt_pnl = sum(min(t["pnl_pct"] * 30, 200) if t["win"] else max(t["pnl_pct"] * 30, -100)
                       for t in b)
        avg_opt = opt_pnl / len(b)
        print(f"  {name:<20} {len(b):>5}t  WR:{bw/len(b)*100:>5.1f}%  "
              f"stock:{bp/len(b):>+5.2f}%/t  option:~{avg_opt:>+5.0f}%/t")

    # Save
    results_file = f"{DATA_DIR}/backtest_v2_results.json"
    with open(results_file, "w") as f:
        json.dump(all_trades, f, indent=2, default=str)
    print(f"\n  Results saved to: {results_file}")
    print("=" * 80)


# ── Stop Loss Sweep ────────────────────────────────────────────────

def sweep_stops(all_stocks_data):
    print("\n" + "=" * 80)
    print("  STOP LOSS OPTIMIZATION")
    print("=" * 80)
    print(f"  {'Stop%':>6} {'Trades':>7} {'Wins':>6} {'WR':>7} {'P&L%':>9} "
          f"{'AvgTrade':>9} {'PF':>6} {'MaxDD':>7}")
    print("  " + "-" * 65)

    for stop_pct in [1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.0, 10.0]:
        all_trades = []
        for stock, pm in all_stocks_data:
            all_trades.extend(run_backtest_stock(stock, pm, stop_pct=stop_pct))

        if not all_trades:
            continue

        ws = [t for t in all_trades if t["win"]]
        ls = [t for t in all_trades if not t["win"]]
        tp = sum(t["pnl_pct"] for t in all_trades)
        gw = sum(t["pnl_pct"] for t in ws)
        gl = abs(sum(t["pnl_pct"] for t in ls))
        pf = gw / gl if gl > 0 else 999

        # Max drawdown
        weekly = defaultdict(float)
        for t in all_trades:
            weekly[t["monday"]] += t["pnl_pct"]
        cum = mx = dd = 0
        for w in sorted(weekly):
            cum += weekly[w]
            mx = max(mx, cum)
            dd = max(dd, mx - cum)

        print(f"  {stop_pct:>5.1f}% {len(all_trades):>7} {len(ws):>6} "
              f"{len(ws)/len(all_trades)*100:>6.1f}% {tp:>+8.1f}% "
              f"{tp/len(all_trades):>+8.2f}% {pf:>5.1f} {dd:>6.1f}%")


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Stock GEX Backtest v2")
    parser.add_argument("--stock", type=str, help="Single stock")
    parser.add_argument("--stop-pct", type=float, default=2.0, help="Stop loss %% (default: 2.0)")
    parser.add_argument("--min-grade", type=str, default="D",
                        choices=["A+", "A", "B", "C", "D"],
                        help="Minimum grade to trade (default: D = all)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--sweep", action="store_true", help="Stop loss sweep")
    parser.add_argument("--exclude-tier-c", action="store_true",
                        help="Exclude Tier C stocks")
    args = parser.parse_args()

    if not os.path.exists(f"{DATA_DIR}/prices"):
        print(f"No data in {DATA_DIR}/")
        return

    if args.stock:
        stocks = [args.stock.upper()]
    else:
        stocks_dir = f"{DATA_DIR}/options"
        stocks = sorted(os.listdir(stocks_dir)) if os.path.exists(stocks_dir) else []

    if args.exclude_tier_c:
        stocks = [s for s in stocks if s not in TIER_C]

    # Load prices
    all_stocks_data = []
    for s in stocks:
        pm = load_prices_map(s)
        if pm:
            all_stocks_data.append((s, pm))

    print(f"Stocks: {len(all_stocks_data)} | Stop: {args.stop_pct}% | "
          f"Min grade: {args.min_grade} | Tier C: {'excluded' if args.exclude_tier_c else 'included'}")

    if args.sweep:
        sweep_stops(all_stocks_data)
        return

    all_trades = []
    for i, (stock, pm) in enumerate(all_stocks_data):
        if args.verbose:
            print(f"\n[{i+1}/{len(all_stocks_data)}] {stock} (Tier {get_tier(stock)})")
        trades = run_backtest_stock(stock, pm, stop_pct=args.stop_pct,
                                   min_grade=args.min_grade, verbose=args.verbose)
        all_trades.extend(trades)
        if not args.verbose and (i + 1) % 10 == 0:
            print(f"  Processed {i+1}/{len(all_stocks_data)}, {len(all_trades)} trades")

    print_report(all_trades, args.stop_pct, args.min_grade)


if __name__ == "__main__":
    main()
