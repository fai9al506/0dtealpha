"""
SPX 0DTE GEX Analysis — Compute daily GEX levels and study price action.

Uses downloaded 10:00 AM SPXW chain snapshots to:
1. Compute per-strike net GEX and identify key levels
2. Study how often price respects GEX levels as S/R
3. Find the SPX GEX support bounce setup (long when spot dips to -GEX)
4. Analyze GEX regime (positive vs negative total GEX)
5. Backtest the rubber-band bounce strategy with parameter sweep

Usage:
  python spx_gex_analysis.py                      # Full analysis report
  python spx_gex_analysis.py --study support       # Just support bounce
  python spx_gex_analysis.py --study regime        # Just regime analysis
  python spx_gex_analysis.py --backtest            # Run bounce backtest
  python spx_gex_analysis.py --date 2024-06-11     # Single day detail
  python spx_gex_analysis.py --export              # Save results to JSON
"""

import json
import os
import sys
import io
import argparse
from datetime import date, timedelta
from collections import defaultdict

# Fix Windows encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DATA_DIR = os.environ.get("GEX_DATA_DIR", r"C:\Users\Faisa\stock_gex_data")
SPX_DIR = os.path.join(DATA_DIR, "spx")


# ── Data Loading ────────────────────────────────────────────────────

def load_spx_prices():
    """Load SPX daily OHLCV bars into {date_int: bar} map."""
    path = os.path.join(SPX_DIR, "prices", "SPX.json")
    if not os.path.exists(path):
        print("ERROR: SPX prices not found. Run spx_gex_downloader.py first.")
        return {}
    with open(path) as f:
        bars = json.load(f)
    return {int(b["date"]): b for b in bars}


def load_chain(trade_date):
    """Load 10:00 AM chain for a date. Returns list of records or None."""
    path = os.path.join(SPX_DIR, "options", f"{trade_date}_0dte.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        records = json.load(f)
    return records if records else None


def get_trading_days():
    """Get all dates that have chain data."""
    options_dir = os.path.join(SPX_DIR, "options")
    if not os.path.exists(options_dir):
        return []
    days = []
    for f in sorted(os.listdir(options_dir)):
        if f.endswith("_0dte.json"):
            d = f.replace("_0dte.json", "")
            days.append(d)
    return days


# ── GEX Computation ────────────────────────────────────────────────

def compute_gex(records):
    """Compute net GEX per strike from chain records.
    Returns dict {strike_dollars: net_gex}."""
    gex = {}
    for r in records:
        k = r.get("strike_dollars")
        if k is None:
            k = r.get("strike", 0) / 1000.0
        gamma = r.get("gamma", 0)
        oi = r.get("open_interest", 0)
        right = r.get("right", "?")

        g = gamma * oi * 100
        if right == "P":
            g = -g
        gex[k] = gex.get(k, 0) + g
    return gex


def extract_levels(gex_by_strike, spot):
    """Extract key GEX levels from per-strike GEX data.
    Returns dict with neg_levels, pos_levels, zero_gamma, total_gex, regime."""
    if not gex_by_strike:
        return None

    neg = [(k, v) for k, v in gex_by_strike.items() if v < 0]
    pos = [(k, v) for k, v in gex_by_strike.items() if v > 0]

    if not neg and not pos:
        return None

    neg.sort(key=lambda x: x[1])  # most negative first
    pos.sort(key=lambda x: x[1], reverse=True)  # most positive first

    # Significance filter: top 5, each >= 10% of strongest
    top_neg = neg[:5]
    if top_neg:
        mx = abs(top_neg[0][1])
        top_neg = [(k, v) for k, v in top_neg if abs(v) >= mx * 0.10]

    top_pos = pos[:5]
    if top_pos:
        mx = top_pos[0][1]
        top_pos = [(k, v) for k, v in top_pos if v >= mx * 0.10]

    # Zero-gamma line (where net GEX flips from neg to pos)
    sorted_gex = sorted(gex_by_strike.items(), key=lambda x: x[0])
    zero_gamma = None
    for i in range(len(sorted_gex) - 1):
        k1, v1 = sorted_gex[i]
        k2, v2 = sorted_gex[i + 1]
        if v1 < 0 and v2 > 0:
            zero_gamma = (k1 + k2) / 2.0
            break

    total_gex = sum(v for _, v in gex_by_strike.items())
    regime = "positive" if total_gex > 0 else "negative"

    return {
        "neg_levels": [{"strike": k, "gex": v} for k, v in top_neg],
        "pos_levels": [{"strike": k, "gex": v} for k, v in top_pos],
        "zero_gamma": zero_gamma,
        "total_gex": total_gex,
        "regime": regime,
        "strongest_neg": top_neg[0][0] if top_neg else None,
        "strongest_pos": top_pos[0][0] if top_pos else None,
    }


# ── Build Daily Dataset ────────────────────────────────────────────

def build_daily_data():
    """Build dataset: for each trading day, compute GEX levels + price action."""
    prices = load_spx_prices()
    days = get_trading_days()

    print(f"Loading {len(days)} trading days...")
    dataset = []
    skipped = 0

    for d in days:
        records = load_chain(d)
        if not records:
            skipped += 1
            continue

        d_int = int(d.replace("-", ""))
        bar = prices.get(d_int)
        if not bar:
            skipped += 1
            continue

        spot = bar["open"]
        gex = compute_gex(records)
        levels = extract_levels(gex, spot)
        if not levels:
            skipped += 1
            continue

        dataset.append({
            "date": d,
            "date_int": d_int,
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "range": bar["high"] - bar["low"],
            "spot": spot,
            **levels,
        })

    print(f"  Loaded: {len(dataset)} days, skipped: {skipped}\n")
    return dataset


# ── Study 1: Level Respect ──────────────────────────────────────────

def study_level_respect(dataset):
    """How often does daily H/L touch GEX levels?"""
    print("=" * 60)
    print("  STUDY 1: GEX Level Respect Rate")
    print("=" * 60)

    neg_touches = 0
    neg_bounces = 0
    neg_total = 0
    pos_touches = 0
    pos_fades = 0
    pos_total = 0

    for day in dataset:
        low = day["low"]
        high = day["high"]
        close = day["close"]

        # Check -GEX levels (support)
        for lev in day["neg_levels"]:
            strike = lev["strike"]
            neg_total += 1
            # Touched = low within 5 pts
            if low <= strike + 5:
                neg_touches += 1
                # Bounced = close above strike
                if close > strike:
                    neg_bounces += 1

        # Check +GEX levels (resistance/magnet)
        for lev in day["pos_levels"]:
            strike = lev["strike"]
            pos_total += 1
            # Touched = high within 5 pts
            if high >= strike - 5:
                pos_touches += 1
                # Faded = close below strike
                if close < strike:
                    pos_fades += 1

    print(f"\n  -GEX Support Levels:")
    print(f"    Total levels across all days: {neg_total}")
    print(f"    Touched (low within 5pts): {neg_touches} ({neg_touches/max(neg_total,1)*100:.0f}%)")
    print(f"    Bounced (close above): {neg_bounces} ({neg_bounces/max(neg_touches,1)*100:.0f}% of touches)")

    print(f"\n  +GEX Magnet/Resistance Levels:")
    print(f"    Total levels: {pos_total}")
    print(f"    Touched (high within 5pts): {pos_touches} ({pos_touches/max(pos_total,1)*100:.0f}%)")
    print(f"    Faded (close below): {pos_fades} ({pos_fades/max(pos_touches,1)*100:.0f}% of touches)")
    print()


# ── Study 2: Support Bounce ─────────────────────────────────────────

def study_support_bounce(dataset):
    """When low breaches strongest -GEX, how often does price recover?"""
    print("=" * 60)
    print("  STUDY 2: -GEX Support Bounce")
    print("=" * 60)

    bounces = []
    for day in dataset:
        neg_strike = day["strongest_neg"]
        if neg_strike is None:
            continue

        low = day["low"]
        close = day["close"]
        high = day["high"]
        spot = day["spot"]

        # Did price touch or breach -GEX?
        if low <= neg_strike + 2:
            breach_pts = neg_strike - low  # positive = breached below
            recovered = close > neg_strike
            rally_from_low = high - low
            close_vs_neg = close - neg_strike

            # Distance from spot to -GEX at open
            gap_at_open = spot - neg_strike

            bounces.append({
                "date": day["date"],
                "neg_strike": neg_strike,
                "spot": spot,
                "low": low,
                "high": high,
                "close": close,
                "breach_pts": breach_pts,
                "recovered": recovered,
                "rally_from_low": rally_from_low,
                "close_vs_neg": close_vs_neg,
                "gap_at_open": gap_at_open,
                "regime": day["regime"],
            })

    total = len(bounces)
    recovered = sum(1 for b in bounces if b["recovered"])
    avg_rally = sum(b["rally_from_low"] for b in bounces) / max(total, 1)
    avg_breach = sum(b["breach_pts"] for b in bounces) / max(total, 1)

    print(f"\n  Days where low touched strongest -GEX: {total}/{len(dataset)} ({total/max(len(dataset),1)*100:.0f}%)")
    print(f"  Recovered (close above -GEX): {recovered}/{total} ({recovered/max(total,1)*100:.0f}%)")
    print(f"  Avg breach below -GEX: {avg_breach:.1f} pts")
    print(f"  Avg rally from low: {avg_rally:.1f} pts")

    # By regime
    for regime in ["positive", "negative"]:
        r_bounces = [b for b in bounces if b["regime"] == regime]
        r_total = len(r_bounces)
        r_recovered = sum(1 for b in r_bounces if b["recovered"])
        if r_total > 0:
            print(f"\n  {regime.upper()} GEX regime ({r_total} days):")
            print(f"    Recovery rate: {r_recovered}/{r_total} ({r_recovered/r_total*100:.0f}%)")
            print(f"    Avg rally: {sum(b['rally_from_low'] for b in r_bounces)/r_total:.1f} pts")

    # By gap at open (was spot already near -GEX?)
    close_gap = [b for b in bounces if b["gap_at_open"] <= 15]
    far_gap = [b for b in bounces if b["gap_at_open"] > 15]
    if close_gap:
        cr = sum(1 for b in close_gap if b["recovered"])
        print(f"\n  Open within 15pts of -GEX ({len(close_gap)} days): {cr/len(close_gap)*100:.0f}% recovered")
    if far_gap:
        fr = sum(1 for b in far_gap if b["recovered"])
        print(f"  Open >15pts above -GEX ({len(far_gap)} days): {fr/len(far_gap)*100:.0f}% recovered")

    print()
    return bounces


# ── Study 3: GEX Regime ─────────────────────────────────────────────

def study_regime(dataset):
    """Compare positive vs negative total GEX days."""
    print("=" * 60)
    print("  STUDY 3: GEX Regime (Positive vs Negative)")
    print("=" * 60)

    pos_days = [d for d in dataset if d["regime"] == "positive"]
    neg_days = [d for d in dataset if d["regime"] == "negative"]

    for label, days in [("POSITIVE (mean-reverting)", pos_days), ("NEGATIVE (trending)", neg_days)]:
        if not days:
            continue
        avg_range = sum(d["range"] for d in days) / len(days)
        avg_return = sum(d["close"] - d["open"] for d in days) / len(days)
        up_days = sum(1 for d in days if d["close"] > d["open"])
        down_days = len(days) - up_days

        print(f"\n  {label}: {len(days)} days ({len(days)/len(dataset)*100:.0f}%)")
        print(f"    Avg daily range: {avg_range:.1f} pts")
        print(f"    Avg O->C return: {avg_return:+.1f} pts")
        print(f"    Up days: {up_days} ({up_days/len(days)*100:.0f}%), Down: {down_days} ({down_days/len(days)*100:.0f}%)")

    print()


# ── Study 4: Zero-Gamma Magnet ───────────────────────────────────────

def study_zero_gamma(dataset):
    """Does price gravitate to the zero-gamma line?"""
    print("=" * 60)
    print("  STUDY 4: Zero-Gamma Magnet")
    print("=" * 60)

    dists_open = []
    dists_close = []
    closer_count = 0
    total = 0

    for day in dataset:
        zg = day.get("zero_gamma")
        if zg is None:
            continue

        dist_open = abs(day["open"] - zg)
        dist_close = abs(day["close"] - zg)
        dists_open.append(dist_open)
        dists_close.append(dist_close)
        total += 1
        if dist_close < dist_open:
            closer_count += 1

    if not dists_open:
        print("  No zero-gamma data available.\n")
        return

    print(f"\n  Days with zero-gamma line: {total}")
    print(f"  Avg distance at open: {sum(dists_open)/total:.1f} pts")
    print(f"  Avg distance at close: {sum(dists_close)/total:.1f} pts")
    print(f"  Close nearer to zero-gamma than open: {closer_count}/{total} ({closer_count/total*100:.0f}%)")
    print()


# ── Intraday Data ────────────────────────────────────────────────────

def load_intraday_bars():
    """Load SPY 5-min bars grouped by date. SPY*10 ~ SPX.
    Returns {date_int: [bars sorted by time]}."""
    path = os.path.join(SPX_DIR, "intraday", "SPY_5min.json")
    if not os.path.exists(path):
        print("ERROR: SPY_5min.json not found. Download intraday data first.")
        return {}
    with open(path) as f:
        bars = json.load(f)

    by_date = defaultdict(list)
    for b in bars:
        by_date[b["date"]].append(b)

    # Sort each day's bars by time
    for d in by_date:
        by_date[d].sort(key=lambda x: x["ms_of_day"])

    return dict(by_date)


# ── Study 5: Intraday Bounce Backtest ────────────────────────────────

def backtest_bounce_intraday(dataset, intraday, stop_pts=10, target_mode="pos_gex",
                              fixed_target=None, entry_time_start="10:00", entry_time_end="15:00"):
    """Bar-by-bar intraday simulation of the GEX support bounce.

    Uses SPY 5-min bars (× ~10 for SPX scale).
    Entry: when a 5-min bar low touches strongest -GEX level (SPX scale).
    Exit: target hit, stop hit, or EOD close.

    SPY -> SPX conversion: SPX_level / SPY_bar ~ ratio computed per day from opens.
    """
    start_ms = int(entry_time_start.split(":")[0]) * 3600000 + int(entry_time_start.split(":")[1]) * 60000
    end_ms = int(entry_time_end.split(":")[0]) * 3600000 + int(entry_time_end.split(":")[1]) * 60000

    trades = []
    for day in dataset:
        neg_strike = day["strongest_neg"]
        pos_strike = day["strongest_pos"]
        if neg_strike is None:
            continue

        d_int = day["date_int"]
        bars = intraday.get(d_int, [])
        if not bars:
            continue

        # SPX/SPY ratio for this day
        spx_open = day["open"]
        spy_open = bars[0]["open"]
        if spy_open <= 0:
            continue
        ratio = spx_open / spy_open  # typically ~10

        # Determine target in SPX space
        if target_mode == "pos_gex" and pos_strike:
            target = pos_strike
        elif target_mode == "zero_gamma" and day.get("zero_gamma"):
            target = day["zero_gamma"]
        elif target_mode == "fixed" and fixed_target:
            target = neg_strike + fixed_target  # target relative to entry
        else:
            target = neg_strike + 15

        stop = neg_strike - stop_pts

        # Bar-by-bar simulation
        entered = False
        entry_price = None
        entry_time = None

        for bar in bars:
            # Convert SPY bar to SPX scale
            bar_low_spx = bar["low"] * ratio
            bar_high_spx = bar["high"] * ratio
            bar_close_spx = bar["close"] * ratio
            bar_ms = bar["ms_of_day"]

            if not entered:
                # Check entry: bar low touches -GEX, within time window
                if bar_ms < start_ms or bar_ms > end_ms:
                    continue
                if bar_low_spx <= neg_strike + 2:
                    entered = True
                    entry_price = neg_strike  # fill at -GEX level
                    entry_time = bar["time"]
                    # Check same bar for stop/target
                    if bar_low_spx <= stop:
                        pnl = -stop_pts
                        trades.append(_make_trade(day, entry_price, target, stop, pnl, "STOP", entry_time, bar["time"]))
                        break
                    if bar_high_spx >= target:
                        pnl = target - entry_price
                        trades.append(_make_trade(day, entry_price, target, stop, pnl, "TARGET", entry_time, bar["time"]))
                        break
            else:
                # Already in trade — check stop then target
                if bar_low_spx <= stop:
                    pnl = -stop_pts
                    trades.append(_make_trade(day, entry_price, target, stop, pnl, "STOP", entry_time, bar["time"]))
                    break
                if bar_high_spx >= target:
                    pnl = target - entry_price
                    trades.append(_make_trade(day, entry_price, target, stop, pnl, "TARGET", entry_time, bar["time"]))
                    break

        # If entered but no stop/target hit — EOD exit
        if entered and (not trades or trades[-1]["date"] != day["date"]):
            last_close_spx = bars[-1]["close"] * ratio
            pnl = last_close_spx - entry_price
            trades.append(_make_trade(day, entry_price, target, stop, pnl, "EOD", entry_time, bars[-1]["time"]))

    return trades


def _make_trade(day, entry, target, stop, pnl, outcome, entry_time, exit_time):
    return {
        "date": day["date"],
        "entry": entry,
        "target": target,
        "stop": stop,
        "target_dist": target - entry,
        "pnl": round(pnl, 1),
        "outcome": outcome,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "regime": day["regime"],
    }


def backtest_bounce(dataset, stop_pts=10, target_mode="pos_gex", fixed_target=None):
    """Daily bar fallback (no intraday data needed)."""
    trades = []
    for day in dataset:
        neg_strike = day["strongest_neg"]
        pos_strike = day["strongest_pos"]
        if neg_strike is None:
            continue

        low = day["low"]
        high = day["high"]
        close = day["close"]

        if low > neg_strike + 2:
            continue

        entry = neg_strike

        if target_mode == "pos_gex" and pos_strike:
            target = pos_strike
        elif target_mode == "zero_gamma" and day.get("zero_gamma"):
            target = day["zero_gamma"]
        elif target_mode == "fixed" and fixed_target:
            target = entry + fixed_target
        else:
            target = entry + 15

        stop = entry - stop_pts

        if low <= stop:
            pnl = -stop_pts
            outcome = "STOP"
        elif high >= target:
            pnl = target - entry
            outcome = "TARGET"
        else:
            pnl = close - entry
            outcome = "EOD"

        trades.append({
            "date": day["date"],
            "entry": entry,
            "target": target,
            "stop": stop,
            "target_dist": target - entry,
            "pnl": pnl,
            "outcome": outcome,
            "regime": day["regime"],
            "low": low,
            "high": high,
            "close": close,
        })

    return trades


def print_backtest_results(trades, label=""):
    """Print backtest summary."""
    if not trades:
        print(f"  {label}: No trades\n")
        return

    total = len(trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)
    losses = sum(1 for t in trades if t["pnl"] < 0)
    flat = total - wins - losses
    wr = wins / total * 100

    total_pnl = sum(t["pnl"] for t in trades)
    avg_win = sum(t["pnl"] for t in trades if t["pnl"] > 0) / max(wins, 1)
    avg_loss = sum(t["pnl"] for t in trades if t["pnl"] < 0) / max(losses, 1)
    gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gross_win / max(gross_loss, 0.01)

    # Max drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for t in trades:
        equity += t["pnl"]
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    by_outcome = defaultdict(int)
    for t in trades:
        by_outcome[t["outcome"]] += 1

    print(f"  {label}")
    print(f"    Trades: {total} | W: {wins} L: {losses} F: {flat} | WR: {wr:.0f}%")
    print(f"    Total PnL: {total_pnl:+.1f} pts | Avg Win: {avg_win:+.1f} | Avg Loss: {avg_loss:+.1f}")
    print(f"    PF: {pf:.2f} | MaxDD: {max_dd:.1f} pts")
    print(f"    Outcomes: TARGET={by_outcome['TARGET']} STOP={by_outcome['STOP']} EOD={by_outcome['EOD']}")

    # Monthly breakdown
    monthly = defaultdict(lambda: {"pnl": 0, "count": 0, "wins": 0})
    for t in trades:
        m = t["date"][:7]
        monthly[m]["pnl"] += t["pnl"]
        monthly[m]["count"] += 1
        if t["pnl"] > 0:
            monthly[m]["wins"] += 1

    print(f"\n    Monthly breakdown:")
    print(f"    {'Month':>8} {'Trades':>6} {'WR':>6} {'PnL':>8}")
    for m in sorted(monthly.keys()):
        d = monthly[m]
        mwr = d["wins"] / d["count"] * 100 if d["count"] else 0
        print(f"    {m:>8} {d['count']:>6} {mwr:>5.0f}% {d['pnl']:>+8.1f}")

    print()


def run_backtest_sweep(dataset, intraday=None):
    """Sweep stop/target parameters using intraday bars."""
    use_intraday = intraday is not None and len(intraday) > 0
    mode = "INTRADAY (5-min SPY bars)" if use_intraday else "DAILY bars"

    print("=" * 60)
    print(f"  BACKTEST: SPX GEX Support Bounce -- {mode}")
    print("=" * 60)
    print()

    def _run(stop, target_mode, fixed_target=None):
        if use_intraday:
            return backtest_bounce_intraday(dataset, intraday, stop_pts=stop,
                                             target_mode=target_mode, fixed_target=fixed_target)
        return backtest_bounce(dataset, stop_pts=stop, target_mode=target_mode, fixed_target=fixed_target)

    def _stats(trades):
        if not trades:
            return None
        total = len(trades)
        wins = sum(1 for t in trades if t["pnl"] > 0)
        wr = wins / total * 100
        total_pnl = sum(t["pnl"] for t in trades)
        gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        pf = gross_win / max(gross_loss, 0.01)
        eq = 0; peak = 0; max_dd = 0
        for t in trades:
            eq += t["pnl"]; peak = max(peak, eq); max_dd = max(max_dd, peak - eq)
        return total, wr, total_pnl, pf, max_dd

    # Fixed target sweep
    print("  --- Fixed Target, varying Stop ---")
    print(f"  {'Stop':>5} {'Target':>7} {'Trades':>6} {'WR':>6} {'PnL':>8} {'PF':>6} {'MaxDD':>7}")
    for stop in [5, 8, 10, 12, 15, 20]:
        for target in [10, 15, 20, 25, 30]:
            trades = _run(stop, "fixed", target)
            s = _stats(trades)
            if not s:
                continue
            total, wr, total_pnl, pf, max_dd = s
            print(f"  {stop:>5} {target:>7} {total:>6} {wr:>5.0f}% {total_pnl:>+8.1f} {pf:>6.2f} {max_dd:>7.1f}")

    # +GEX target
    print(f"\n  --- +GEX Target, varying Stop ---")
    print(f"  {'Stop':>5} {'Target':>7} {'Trades':>6} {'WR':>6} {'PnL':>8} {'PF':>6} {'MaxDD':>7}")
    for stop in [5, 8, 10, 12, 15, 20]:
        trades = _run(stop, "pos_gex")
        s = _stats(trades)
        if not s:
            continue
        total, wr, total_pnl, pf, max_dd = s
        print(f"  {stop:>5} {'  +GEX':>7} {total:>6} {wr:>5.0f}% {total_pnl:>+8.1f} {pf:>6.2f} {max_dd:>7.1f}")

    print()


# ── Single Day Detail ───────────────────────────────────────────────

def show_day_detail(target_date):
    """Show full GEX analysis for one date."""
    prices = load_spx_prices()
    records = load_chain(target_date)

    if not records:
        print(f"No chain data for {target_date}")
        return

    d_int = int(target_date.replace("-", ""))
    bar = prices.get(d_int)
    if not bar:
        print(f"No price data for {target_date}")
        return

    spot = bar["open"]
    gex = compute_gex(records)
    levels = extract_levels(gex, spot)

    print(f"\n{'='*60}")
    print(f"  {target_date} | Spot (open): {spot:.2f}")
    print(f"  Range: {bar['low']:.2f} - {bar['high']:.2f} | Close: {bar['close']:.2f}")
    print(f"{'='*60}")

    if not levels:
        print("  No GEX levels computed")
        return

    print(f"\n  Regime: {levels['regime'].upper()} (total GEX: {levels['total_gex']:,.0f})")
    if levels["zero_gamma"]:
        print(f"  Zero-gamma line: {levels['zero_gamma']:.0f}")

    print(f"\n  Top -GEX (support):")
    for lev in levels["neg_levels"]:
        dist = lev["strike"] - spot
        print(f"    ${lev['strike']:.0f}  GEX={lev['gex']:>12,.0f}  ({dist:+.0f} pts)")

    print(f"\n  Top +GEX (magnets):")
    for lev in levels["pos_levels"]:
        dist = lev["strike"] - spot
        print(f"    ${lev['strike']:.0f}  GEX={lev['gex']:>12,.0f}  ({dist:+.0f} pts)")

    # Strike-by-strike near spot
    sorted_gex = sorted(gex.items(), key=lambda x: x[0])
    max_abs = max(abs(v) for _, v in sorted_gex) if sorted_gex else 1
    print(f"\n  {'Strike':>8} {'Net GEX':>12} {'Bar'}")
    for k, v in sorted_gex:
        if abs(k - spot) <= 50:
            bar_len = int(abs(v) / max_abs * 25)
            c = "+" if v > 0 else "-"
            gex_bar = c * bar_len
            marker = " << SPOT" if abs(k - spot) < 3 else ""
            print(f"  ${k:>7.0f} {v:>12,.0f}  {gex_bar}{marker}")

    # Price action vs levels
    neg_strike = levels["strongest_neg"]
    pos_strike = levels["strongest_pos"]
    if neg_strike and bar["low"] <= neg_strike + 2:
        recovered = bar["close"] > neg_strike
        print(f"\n  >> Price hit -GEX at ${neg_strike:.0f}")
        print(f"  >> {'BOUNCED' if recovered else 'STAYED BELOW'} (close={bar['close']:.2f})")
    if pos_strike and bar["high"] >= pos_strike - 2:
        print(f"  >> Price reached +GEX at ${pos_strike:.0f}")


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SPX 0DTE GEX Analysis")
    parser.add_argument("--study", type=str, choices=["support", "regime", "levels", "zero"],
                        help="Run specific study only")
    parser.add_argument("--backtest", action="store_true", help="Run bounce backtest sweep")
    parser.add_argument("--date", type=str, help="Show single day detail (YYYY-MM-DD)")
    parser.add_argument("--export", action="store_true", help="Export results to JSON")
    args = parser.parse_args()

    if args.date:
        show_day_detail(args.date)
        sys.exit(0)

    dataset = build_daily_data()
    if not dataset:
        print("No data loaded. Run spx_gex_downloader.py first.")
        sys.exit(1)

    # Load intraday data if available
    intraday = load_intraday_bars()
    if intraday:
        print(f"Intraday data: {len(intraday)} days of SPY 5-min bars\n")
    else:
        print("No intraday data — using daily bars for backtest\n")

    if args.study == "support":
        study_support_bounce(dataset)
    elif args.study == "regime":
        study_regime(dataset)
    elif args.study == "levels":
        study_level_respect(dataset)
    elif args.study == "zero":
        study_zero_gamma(dataset)
    elif args.backtest:
        run_backtest_sweep(dataset, intraday)
    else:
        # Full report
        study_level_respect(dataset)
        study_support_bounce(dataset)
        study_regime(dataset)
        study_zero_gamma(dataset)

        print("=" * 60)
        print("  BACKTEST: Best Configs (Intraday)")
        print("=" * 60)
        print()

        if intraday:
            for stop, target in [(8, 15), (10, 20), (12, 25)]:
                trades = backtest_bounce_intraday(dataset, intraday, stop_pts=stop,
                                                   target_mode="fixed", fixed_target=target)
                print_backtest_results(trades, f"SL={stop} / T={target} (intraday)")

            trades = backtest_bounce_intraday(dataset, intraday, stop_pts=10, target_mode="pos_gex")
            print_backtest_results(trades, "SL=10 / T=+GEX (intraday)")
        else:
            for stop, target in [(8, 15), (10, 20), (12, 25)]:
                trades = backtest_bounce(dataset, stop_pts=stop, target_mode="fixed", fixed_target=target)
                print_backtest_results(trades, f"SL={stop} / T={target} (daily)")

            trades = backtest_bounce(dataset, stop_pts=10, target_mode="pos_gex")
            print_backtest_results(trades, "SL=10 / T=+GEX (daily)")

    if args.export:
        out = os.path.join(SPX_DIR, "analysis_results.json")
        # Re-run backtest for export
        trades = backtest_bounce(dataset, stop_pts=10, target_mode="pos_gex")
        with open(out, "w") as f:
            json.dump(trades, f, indent=2)
        print(f"Results exported to {out}")
