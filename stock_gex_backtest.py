"""
Stock GEX Backtest — Tests the GEX support/magnet strategy on historical data.

Reads downloaded ThetaData files and evaluates:
  - Monday: compute GEX levels (support, magnets) from options chain
  - Tue-Fri: did price reach support? Did it move toward magnets?

Usage:
  python stock_gex_backtest.py                    # Run full backtest
  python stock_gex_backtest.py --stock AAPL       # Single stock
  python stock_gex_backtest.py --stock LULU --verbose  # Detailed output
"""

import json
import os
import argparse
from datetime import date, timedelta
from collections import defaultdict

DATA_DIR = "data/stock_gex_historical"

# Reuse GEX computation from scanner module
from app.stock_gex_scanner import _compute_gex, _identify_key_levels


# ── Data Loading ────────────────────────────────────────────────────

def load_prices(stock):
    """Load daily price bars for a stock. Returns {YYYYMMDD: {open, high, low, close}}."""
    path = f"{DATA_DIR}/prices/{stock}.json"
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        bars = json.load(f)
    result = {}
    for bar in bars:
        d = bar.get("date")
        if d:
            result[int(d)] = bar
    return result


def load_options(stock, monday, label):
    """Load options data for a stock on a specific Monday + expiration label.

    Returns list of dicts compatible with _compute_gex():
      [{Type: C/P, Strike: float_dollars, Gamma: float, OpenInterest: float}, ...]
    """
    path = f"{DATA_DIR}/options/{stock}/{monday}_{label}.json"
    if not os.path.exists(path):
        return None
    with open(path) as f:
        records = json.load(f)

    if not records:
        return None

    # Detect field mapping from first record
    sample = records[0]
    tick_len = sample.get("_tick_len", 0)
    has_oi_field = "open_interest" in sample
    extras = sample.get("_extras", [])

    # Map to our format
    rows = []
    for r in records:
        strike_raw = r.get("strike", 0)
        strike_dollars = strike_raw / 1000.0  # ThetaData: 1/10th cent

        right = r.get("right", "?")
        if right not in ("C", "P"):
            continue

        # Get OI — try direct field first, then extras
        oi = r.get("open_interest")
        if oi is None and r.get("_extras"):
            oi = r["_extras"][0]  # First extra field is typically OI

        # Get gamma — try extras
        gamma = r.get("gamma")
        if gamma is None and r.get("_extras") and len(r["_extras"]) > 2:
            # Typical order after OI: gamma, delta, vega, theta, rho
            gamma = r["_extras"][1]  # Position 1 in extras = gamma

        if oi is None or gamma is None:
            continue

        rows.append({
            "Type": right,
            "Strike": strike_dollars,
            "Gamma": float(gamma) if gamma else 0,
            "OpenInterest": float(oi) if oi else 0,
        })

    return rows if rows else None


def get_mondays_from_data(stock):
    """List all Mondays we have data for a given stock."""
    options_dir = f"{DATA_DIR}/options/{stock}"
    if not os.path.exists(options_dir):
        return []

    mondays = set()
    for f in os.listdir(options_dir):
        if f.endswith(".json"):
            # Format: YYYY-MM-DD_label.json
            parts = f.replace(".json", "").split("_")
            if len(parts) >= 2:
                monday_str = parts[0]
                try:
                    mondays.add(monday_str)
                except ValueError:
                    pass
    return sorted(mondays)


# ── Backtest Logic ──────────────────────────────────────────────────

def run_backtest_stock(stock, prices, verbose=False):
    """Run GEX backtest for a single stock.

    For each Monday:
      1. Compute GEX from options data
      2. Identify support (-GEX) and magnet (+GEX) levels
      3. Check Tue-Fri price action vs those levels

    Returns list of trade dicts.
    """
    mondays = get_mondays_from_data(stock)
    if not mondays:
        return []

    trades = []

    for monday_str in mondays:
        # Load options data (try opex first, then weekly)
        for label in ["opex", "weekly"]:
            rows = load_options(stock, monday_str, label)
            if rows:
                break
        if not rows:
            continue

        # Get Monday's spot price
        monday_date = date.fromisoformat(monday_str)
        monday_key = int(monday_date.strftime("%Y%m%d"))
        monday_bar = prices.get(monday_key)
        if not monday_bar:
            # Try adjacent days (holiday Monday → use Tuesday)
            for offset in range(1, 4):
                alt = monday_date + timedelta(days=offset)
                alt_bar = prices.get(int(alt.strftime("%Y%m%d")))
                if alt_bar:
                    monday_bar = alt_bar
                    break
        if not monday_bar:
            continue

        spot = monday_bar["close"]

        # Compute GEX
        gex_data = _compute_gex(rows)
        if not gex_data:
            continue

        levels = _identify_key_levels(gex_data, spot)

        support = levels.get("support", [])
        magnets_above = levels.get("magnets_above", [])
        magnets_below = levels.get("magnets_below", [])
        strongest_pos = levels.get("strongest_positive")
        strongest_neg = levels.get("strongest_negative")

        # Get Tue-Fri price action
        week_bars = []
        for d_offset in range(1, 6):  # Tue through Sat (only market days will have data)
            day = monday_date + timedelta(days=d_offset)
            day_key = int(day.strftime("%Y%m%d"))
            bar = prices.get(day_key)
            if bar:
                week_bars.append(bar)

        if not week_bars:
            continue

        week_low = min(b["low"] for b in week_bars)
        week_high = max(b["high"] for b in week_bars)
        friday_close = week_bars[-1]["close"]

        # ── Check LONG setups: price reaches -GEX support ──
        if support:
            nearest_support = max(support, key=lambda s: s["strike"])
            support_strike = nearest_support["strike"]

            # Did price reach support during the week?
            reached_support = week_low <= support_strike * 1.005

            if reached_support:
                # How far did price bounce toward magnets?
                if magnets_above:
                    nearest_magnet = min(m["strike"] for m in magnets_above)
                    farthest_magnet = max(m["strike"] for m in magnets_above)

                    # Max bounce from support toward magnets
                    bounce_pts = week_high - support_strike
                    magnet_distance = nearest_magnet - support_strike
                    reach_pct = (bounce_pts / magnet_distance * 100) if magnet_distance > 0 else 0

                    trade = {
                        "stock": stock,
                        "monday": monday_str,
                        "type": "LONG",
                        "label": label,
                        "spot_monday": round(spot, 2),
                        "support_strike": support_strike,
                        "nearest_magnet": nearest_magnet,
                        "farthest_magnet": farthest_magnet,
                        "n_magnets_above": len(magnets_above),
                        "week_low": round(week_low, 2),
                        "week_high": round(week_high, 2),
                        "friday_close": round(friday_close, 2),
                        "bounce_pts": round(bounce_pts, 2),
                        "magnet_distance": round(magnet_distance, 2),
                        "reach_pct": round(reach_pct, 1),
                        "reached_magnet": week_high >= nearest_magnet,
                        "move_from_support_pct": round(bounce_pts / support_strike * 100, 2),
                    }
                    trades.append(trade)

                    if verbose:
                        tag = "WIN" if trade["reached_magnet"] else "MISS"
                        print(f"  {monday_str} LONG {tag}: support=${support_strike:.0f} "
                              f"low=${week_low:.2f} high=${week_high:.2f} "
                              f"magnet=${nearest_magnet:.0f} reach={reach_pct:.0f}%")

        # ── Check SHORT setups: price above max +GEX ──
        if strongest_pos:
            ceiling = strongest_pos["strike"]
            above_ceiling = week_high >= ceiling * 1.005

            if above_ceiling and magnets_below:
                nearest_mag_below = max(m["strike"] for m in magnets_below)
                drop_pts = ceiling - week_low
                mag_distance = ceiling - nearest_mag_below
                reach_pct = (drop_pts / mag_distance * 100) if mag_distance > 0 else 0

                trade = {
                    "stock": stock,
                    "monday": monday_str,
                    "type": "SHORT",
                    "label": label,
                    "spot_monday": round(spot, 2),
                    "ceiling_strike": ceiling,
                    "nearest_magnet_below": nearest_mag_below,
                    "n_magnets_below": len(magnets_below),
                    "week_low": round(week_low, 2),
                    "week_high": round(week_high, 2),
                    "friday_close": round(friday_close, 2),
                    "drop_pts": round(drop_pts, 2),
                    "magnet_distance": round(mag_distance, 2),
                    "reach_pct": round(reach_pct, 1),
                    "reached_magnet": week_low <= nearest_mag_below,
                    "move_from_ceiling_pct": round(drop_pts / ceiling * 100, 2),
                }
                trades.append(trade)

                if verbose:
                    tag = "WIN" if trade["reached_magnet"] else "MISS"
                    print(f"  {monday_str} SHORT {tag}: ceiling=${ceiling:.0f} "
                          f"high=${week_high:.2f} low=${week_low:.2f} "
                          f"magnet=${nearest_mag_below:.0f} reach={reach_pct:.0f}%")

    return trades


# ── Report ──────────────────────────────────────────────────────────

def print_report(all_trades):
    """Print backtest summary report."""
    if not all_trades:
        print("No trades found.")
        return

    print("\n" + "=" * 70)
    print("STOCK GEX BACKTEST REPORT")
    print("=" * 70)

    # Overall stats
    longs = [t for t in all_trades if t["type"] == "LONG"]
    shorts = [t for t in all_trades if t["type"] == "SHORT"]

    print(f"\nTotal signals: {len(all_trades)}")
    print(f"  LONG:  {len(longs)}")
    print(f"  SHORT: {len(shorts)}")

    for label, trades in [("LONG", longs), ("SHORT", shorts)]:
        if not trades:
            continue

        wins = [t for t in trades if t.get("reached_magnet")]
        wr = len(wins) / len(trades) * 100 if trades else 0
        avg_reach = sum(t["reach_pct"] for t in trades) / len(trades)

        if label == "LONG":
            avg_bounce = sum(t["bounce_pts"] for t in trades) / len(trades)
            avg_move_pct = sum(t["move_from_support_pct"] for t in trades) / len(trades)
        else:
            avg_bounce = sum(t["drop_pts"] for t in trades) / len(trades)
            avg_move_pct = sum(t["move_from_ceiling_pct"] for t in trades) / len(trades)

        print(f"\n--- {label} Signals ---")
        print(f"  Signals: {len(trades)}")
        print(f"  Reached magnet: {len(wins)} ({wr:.1f}% win rate)")
        print(f"  Avg reach toward magnet: {avg_reach:.1f}%")
        print(f"  Avg move: {avg_bounce:.2f} pts ({avg_move_pct:.2f}%)")

    # Per-stock breakdown
    print(f"\n--- Per-Stock Summary (sorted by signal count) ---")
    stock_stats = defaultdict(lambda: {"total": 0, "wins": 0, "avg_reach": []})

    for t in all_trades:
        s = stock_stats[t["stock"]]
        s["total"] += 1
        if t.get("reached_magnet"):
            s["wins"] += 1
        s["avg_reach"].append(t["reach_pct"])

    sorted_stocks = sorted(stock_stats.items(), key=lambda x: x[1]["total"], reverse=True)

    print(f"  {'Stock':<8} {'Signals':>8} {'Wins':>6} {'WR':>7} {'AvgReach':>9}")
    print(f"  {'-----':<8} {'-------':>8} {'----':>6} {'--':>7} {'--------':>9}")

    for stock, stats in sorted_stocks:
        wr = stats["wins"] / stats["total"] * 100 if stats["total"] else 0
        avg_r = sum(stats["avg_reach"]) / len(stats["avg_reach"]) if stats["avg_reach"] else 0
        print(f"  {stock:<8} {stats['total']:>8} {stats['wins']:>6} {wr:>6.1f}% {avg_r:>8.1f}%")

    # Best trades
    print(f"\n--- Top 10 Trades (by reach %) ---")
    top = sorted(all_trades, key=lambda t: t["reach_pct"], reverse=True)[:10]
    for t in top:
        print(f"  {t['stock']:<6} {t['monday']} {t['type']:<5} reach={t['reach_pct']:>5.1f}% "
              f"{'WIN' if t.get('reached_magnet') else 'MISS'}")

    # Save full results
    results_file = f"{DATA_DIR}/backtest_results.json"
    with open(results_file, "w") as f:
        json.dump(all_trades, f, indent=2)
    print(f"\nFull results saved to: {results_file}")


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backtest stock GEX strategy")
    parser.add_argument("--stock", type=str, help="Single stock to backtest")
    parser.add_argument("--verbose", action="store_true", help="Show each trade")
    args = parser.parse_args()

    # Check data exists
    if not os.path.exists(f"{DATA_DIR}/prices"):
        print(f"No data found in {DATA_DIR}/")
        print("Run stock_gex_downloader.py first.")
        return

    # Determine stocks to backtest
    if args.stock:
        stocks = [args.stock.upper()]
    else:
        stocks_dir = f"{DATA_DIR}/options"
        if os.path.exists(stocks_dir):
            stocks = sorted(os.listdir(stocks_dir))
        else:
            print("No options data found. Run downloader first.")
            return

    print(f"=== Stock GEX Backtest ===")
    print(f"Stocks: {len(stocks)}")
    print()

    all_trades = []

    for i, stock in enumerate(stocks):
        prices = load_prices(stock)
        if not prices:
            continue

        if args.verbose:
            print(f"\n[{i+1}/{len(stocks)}] {stock} ({len(prices)} price bars)")

        trades = run_backtest_stock(stock, prices, verbose=args.verbose)
        all_trades.extend(trades)

        if not args.verbose and (i + 1) % 10 == 0:
            print(f"  Processed {i+1}/{len(stocks)} stocks, {len(all_trades)} trades so far")

    print_report(all_trades)


if __name__ == "__main__":
    main()
