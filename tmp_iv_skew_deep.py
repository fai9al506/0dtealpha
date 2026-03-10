"""
IV SKEW DEEP DIVE — Test multiple parameter combinations
Load ALL data once, then sweep parameters in-memory.
"""
import os, sqlalchemy as sa
from datetime import time as dtime, datetime
from collections import defaultdict
import pytz

NY = pytz.timezone("US/Eastern")
engine = sa.create_engine(os.environ["DATABASE_URL"])

MARKET_START = dtime(9, 45)
MARKET_END = dtime(15, 45)


def compute_skew(chain_rows, spot, put_range=(10, 20), call_range=(10, 20)):
    """Compute put IV / call IV ratio for equidistant strikes."""
    put_ivs = []
    call_ivs = []
    for row in chain_rows:
        if len(row) < 21:
            continue
        strike = row[10]
        c_iv = row[2]
        p_iv = row[18]
        dist = strike - spot
        if -put_range[1] <= dist <= -put_range[0] and p_iv and p_iv > 0:
            put_ivs.append(p_iv)
        if call_range[0] <= dist <= call_range[1] and c_iv and c_iv > 0:
            call_ivs.append(c_iv)
    if not put_ivs or not call_ivs:
        return None
    avg_put = sum(put_ivs) / len(put_ivs)
    avg_call = sum(call_ivs) / len(call_ivs)
    if avg_call == 0:
        return None
    return avg_put / avg_call


# ============================================================
# LOAD ALL DATA ONCE
# ============================================================
print("Loading all chain + volland data from DB (this takes a minute)...")

all_days_data = {}  # day -> {"chain": [...], "volland": [...]}

with engine.connect() as c:
    days = c.execute(sa.text(
        "SELECT DISTINCT ts::date FROM chain_snapshots "
        "WHERE ts::date >= '2026-02-01' AND spot IS NOT NULL ORDER BY ts::date"
    )).fetchall()
    days = [d[0] for d in days]
    print(f"Found {len(days)} trading days: {days[0]} to {days[-1]}")

    for i, day in enumerate(days):
        chain = c.execute(sa.text(
            "SELECT ts, spot, vix, rows FROM chain_snapshots "
            "WHERE ts::date = :d AND spot IS NOT NULL ORDER BY ts"
        ), {"d": day}).fetchall()

        volland = c.execute(sa.text("""
            SELECT ts,
                   payload->'statistics'->>'paradigm' as paradigm,
                   payload->'statistics'->>'aggregatedCharm' as charm_str,
                   payload->'statistics'->>'delta_decay_hedging' as dd_str
            FROM volland_snapshots
            WHERE ts::date = :d
            AND payload->'statistics' IS NOT NULL
            ORDER BY ts
        """), {"d": day}).fetchall()

        all_days_data[day] = {"chain": chain, "volland": volland}
        print(f"  Day {i+1}/{len(days)}: {day} — {len(chain)} chain snapshots, {len(volland)} volland snapshots")

print(f"Data loaded. Total: {sum(len(d['chain']) for d in all_days_data.values())} chain rows across {len(days)} days.\n")


def get_volland_at(volland_rows, ts):
    """Find closest volland data at or before timestamp."""
    paradigm = None
    charm_val = None
    for vr in reversed(volland_rows):
        if vr[0] <= ts:
            paradigm = vr[1]
            charm_str = vr[2]
            if charm_str:
                try:
                    charm_val = float(charm_str.replace("$", "").replace(",", "").strip())
                except (ValueError, AttributeError):
                    pass
            break
    return paradigm, charm_val


def run_backtest(params):
    """Run a single backtest with given parameters using cached data."""
    skew_thresh = params["skew_thresh"]
    lookback = params["lookback"]
    price_stable = params["price_stable"]
    target_pts = params["target_pts"]
    stop_pts = params["stop_pts"]
    cooldown_min = params["cooldown_min"]
    put_range = params.get("put_range", (10, 20))
    call_range = params.get("call_range", (10, 20))
    market_start = params.get("market_start", MARKET_START)
    market_end = params.get("market_end", MARKET_END)

    trades = []

    for day in sorted(all_days_data.keys()):
        data = all_days_data[day]
        chain = data["chain"]
        volland = data["volland"]

        if len(chain) < lookback + 2:
            continue

        last_trade_time = None
        skew_history = []

        for i, (ts, spot, vix, chain_rows) in enumerate(chain):
            if spot is None or chain_rows is None:
                continue

            t_et = ts.astimezone(NY)
            t_time = t_et.time()

            skew = compute_skew(chain_rows, spot, put_range, call_range)
            if skew is None:
                continue
            skew_history.append((ts, skew, spot))

            if t_time < market_start or t_time > market_end:
                continue
            if len(skew_history) < lookback + 1:
                continue
            if last_trade_time and (ts - last_trade_time).total_seconds() < cooldown_min * 60:
                continue

            old_ts, old_skew, old_spot = skew_history[-lookback - 1]
            skew_change = (skew - old_skew) / old_skew if old_skew != 0 else 0
            price_change = abs(spot - old_spot)

            if price_change > price_stable:
                continue

            direction = None
            if skew_change < -skew_thresh:
                direction = "LONG"
            elif skew_change > skew_thresh:
                direction = "SHORT"

            if direction is None:
                continue

            # Get volland context
            paradigm, charm_val = get_volland_at(volland, ts)

            entry_price = spot
            result = "EXPIRED"
            pnl = 0
            max_profit = 0
            max_loss = 0
            elapsed_min = 0

            for j in range(i + 1, len(chain)):
                future_ts, future_spot, _, _ = chain[j]
                if future_spot is None:
                    continue
                profit = (future_spot - entry_price) if direction == "LONG" else (entry_price - future_spot)
                max_profit = max(max_profit, profit)
                max_loss = min(max_loss, profit)
                if profit <= -stop_pts:
                    result = "LOSS"
                    pnl = -stop_pts
                    elapsed_min = (future_ts - ts).total_seconds() / 60
                    break
                if profit >= target_pts:
                    result = "WIN"
                    pnl = target_pts
                    elapsed_min = (future_ts - ts).total_seconds() / 60
                    break
            else:
                last_spot = None
                for r in reversed(chain):
                    if r[1] is not None:
                        last_spot = r[1]
                        break
                if last_spot:
                    pnl = (last_spot - entry_price) if direction == "LONG" else (entry_price - last_spot)
                if len(chain) > i + 1:
                    elapsed_min = (chain[-1][0] - ts).total_seconds() / 60

            hour_bucket = t_et.hour
            trades.append({
                "date": str(day), "time": t_time.strftime("%H:%M"),
                "hour": hour_bucket,
                "direction": direction, "entry": round(entry_price, 1),
                "skew": round(skew, 3), "skew_chg_pct": round(skew_change * 100, 1),
                "price_chg": round(price_change, 1),
                "paradigm": paradigm or "?",
                "charm_M": round(charm_val / 1e6, 1) if charm_val else None,
                "result": result, "pnl": round(pnl, 1),
                "max_profit": round(max_profit, 1), "max_loss": round(max_loss, 1),
                "elapsed_min": round(elapsed_min, 1),
            })
            last_trade_time = ts

    return trades


def print_summary(label, trades):
    """Print a compact summary of trades."""
    if not trades:
        print(f"  {label}: 0 trades")
        return
    wins = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    expired = [t for t in trades if t["result"] == "EXPIRED"]
    total_pnl = sum(t["pnl"] for t in trades)
    wr = len(wins) / len(trades) * 100
    print(f"  {label}: {len(trades)} trades | {len(wins)}W/{len(losses)}L/{len(expired)}E | "
          f"WR={wr:.0f}% | P&L={total_pnl:+.1f} | Avg={total_pnl/len(trades):+.1f}/trade")


# ============================================================
# PHASE 1: Parameter sweep
# ============================================================
print("=" * 80)
print("PHASE 1: PARAMETER SWEEP")
print("=" * 80)

# Test different skew thresholds with base params
print("\n--- Skew Threshold Sweep (lookback=5, stable=5pt, T=10/S=8, CD=30min) ---")
for thresh in [0.05, 0.08, 0.10, 0.12, 0.15, 0.20]:
    trades = run_backtest({
        "skew_thresh": thresh, "lookback": 5, "price_stable": 5.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 30,
    })
    print_summary(f"thresh={thresh:.0%}", trades)

# Test different lookback windows
print("\n--- Lookback Sweep (thresh=10%, lookback, stable=5pt, T=10/S=8, CD=30min) ---")
for lb in [3, 5, 8, 10, 15]:
    trades = run_backtest({
        "skew_thresh": 0.10, "lookback": lb, "price_stable": 5.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 30,
    })
    print_summary(f"lookback={lb}", trades)

# Test different price stability thresholds
print("\n--- Price Stability Sweep (thresh=10%, lookback=5, T=10/S=8, CD=30min) ---")
for ps in [3.0, 5.0, 8.0, 10.0, 15.0]:
    trades = run_backtest({
        "skew_thresh": 0.10, "lookback": 5, "price_stable": ps,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 30,
    })
    print_summary(f"stable={ps}pt", trades)

# Test different T/S combos
print("\n--- Target/Stop Sweep (thresh=10%, lookback=5, stable=5pt, CD=30min) ---")
for tgt, stp in [(5, 5), (8, 5), (10, 5), (10, 8), (10, 10), (12, 8), (15, 8), (15, 10), (20, 10)]:
    trades = run_backtest({
        "skew_thresh": 0.10, "lookback": 5, "price_stable": 5.0,
        "target_pts": tgt, "stop_pts": stp, "cooldown_min": 30,
    })
    print_summary(f"T={tgt}/S={stp}", trades)

# Test different cooldowns
print("\n--- Cooldown Sweep (thresh=10%, lookback=5, stable=5pt, T=10/S=8) ---")
for cd in [15, 20, 30, 45, 60]:
    trades = run_backtest({
        "skew_thresh": 0.10, "lookback": 5, "price_stable": 5.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": cd,
    })
    print_summary(f"cd={cd}min", trades)

# Test different strike ranges for IV measurement
print("\n--- Strike Range Sweep (thresh=10%, lookback=5, stable=5pt, T=10/S=8, CD=30) ---")
for pr, cr in [((5, 15), (5, 15)), ((10, 20), (10, 20)), ((5, 10), (5, 10)), ((15, 25), (15, 25)), ((5, 20), (5, 20))]:
    trades = run_backtest({
        "skew_thresh": 0.10, "lookback": 5, "price_stable": 5.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 30,
        "put_range": pr, "call_range": cr,
    })
    print_summary(f"range=P{pr}/C{cr}", trades)


# ============================================================
# PHASE 2: Best combo deep dive
# ============================================================
print("\n" + "=" * 80)
print("PHASE 2: DEEP DIVE ON BEST COMBOS")
print("=" * 80)

# Run best threshold with full detail
for thresh in [0.08, 0.10, 0.12]:
    trades = run_backtest({
        "skew_thresh": thresh, "lookback": 5, "price_stable": 5.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 30,
    })

    if not trades:
        continue

    print(f"\n{'='*70}")
    print(f"DETAILED: thresh={thresh:.0%}, lookback=5, stable=5pt, T=10/S=8, CD=30")
    print(f"{'='*70}")

    wins = [t for t in trades if t["result"] == "WIN"]
    losses = [t for t in trades if t["result"] == "LOSS"]
    expired = [t for t in trades if t["result"] == "EXPIRED"]
    total_pnl = sum(t["pnl"] for t in trades)
    wr = len(wins) / len(trades) * 100

    print(f"Total: {len(trades)} trades | {len(wins)}W/{len(losses)}L/{len(expired)}E | WR={wr:.0f}% | P&L={total_pnl:+.1f}")

    # By direction
    for d in ["LONG", "SHORT"]:
        dt = [t for t in trades if t["direction"] == d]
        if dt:
            dw = [t for t in dt if t["result"] == "WIN"]
            print(f"  {d}: {len(dt)} trades, {len(dw)} wins ({len(dw)/len(dt)*100:.0f}% WR), P&L={sum(t['pnl'] for t in dt):+.1f}")

    # By hour
    print("\n  By hour:")
    hours = sorted(set(t["hour"] for t in trades))
    for h in hours:
        ht = [t for t in trades if t["hour"] == h]
        hw = [t for t in ht if t["result"] == "WIN"]
        print(f"    {h}:00 — {len(ht)} trades, {len(hw)} wins ({len(hw)/len(ht)*100:.0f}% WR), P&L={sum(t['pnl'] for t in ht):+.1f}")

    # By paradigm
    print("\n  By paradigm:")
    paradigms = sorted(set(t["paradigm"] for t in trades))
    for p in paradigms:
        pt = [t for t in trades if t["paradigm"] == p]
        pw = [t for t in pt if t["result"] == "WIN"]
        print(f"    {p}: {len(pt)} trades, {len(pw)} wins ({len(pw)/len(pt)*100:.0f}% WR), P&L={sum(t['pnl'] for t in pt):+.1f}")

    # By charm alignment
    print("\n  By charm alignment:")
    aligned = [t for t in trades if t["charm_M"] is not None and
               ((t["direction"] == "LONG" and t["charm_M"] > 0) or
                (t["direction"] == "SHORT" and t["charm_M"] < 0))]
    opposed = [t for t in trades if t["charm_M"] is not None and
               ((t["direction"] == "LONG" and t["charm_M"] < 0) or
                (t["direction"] == "SHORT" and t["charm_M"] > 0))]
    if aligned:
        aw = [t for t in aligned if t["result"] == "WIN"]
        print(f"    Aligned:  {len(aligned)} trades, {len(aw)} wins ({len(aw)/len(aligned)*100:.0f}% WR), P&L={sum(t['pnl'] for t in aligned):+.1f}")
    if opposed:
        ow = [t for t in opposed if t["result"] == "WIN"]
        print(f"    Opposed:  {len(opposed)} trades, {len(ow)} wins ({len(ow)/len(opposed)*100:.0f}% WR), P&L={sum(t['pnl'] for t in opposed):+.1f}")

    # Skew magnitude analysis
    print("\n  By skew change magnitude:")
    for lo, hi, label in [(5, 10, "5-10%"), (10, 15, "10-15%"), (15, 20, "15-20%"), (20, 30, "20-30%"), (30, 100, "30%+")]:
        mt = [t for t in trades if lo <= abs(t["skew_chg_pct"]) < hi]
        if mt:
            mw = [t for t in mt if t["result"] == "WIN"]
            print(f"    {label}: {len(mt)} trades, {len(mw)} wins ({len(mw)/len(mt)*100:.0f}% WR), P&L={sum(t['pnl'] for t in mt):+.1f}")

    # Max favorable excursion analysis
    print("\n  Max favorable excursion (all trades):")
    for thresh_pts in [5, 8, 10, 12, 15, 20]:
        reached = [t for t in trades if t["max_profit"] >= thresh_pts]
        print(f"    Reached +{thresh_pts}pt: {len(reached)}/{len(trades)} ({len(reached)/len(trades)*100:.0f}%)")

    # Print all trades
    print(f"\n  {'Date':<12} {'Time':<6} {'Dir':<6} {'Entry':<8} {'Skew':<7} {'Chg%':<7} {'PxChg':<6} {'Paradigm':<12} {'ChrmM':<8} {'Result':<8} {'P&L':<7} {'MaxP':<7} {'MaxL':<7} {'Min':<5}")
    print("  " + "-" * 115)
    for t in trades:
        charm_str = f"{t['charm_M']:+.0f}" if t['charm_M'] is not None else "?"
        print(f"  {t['date']:<12} {t['time']:<6} {t['direction']:<6} {t['entry']:<8} {t['skew_chg_pct']:<+7.1f} {t['price_chg']:<6.1f} {t['paradigm']:<12} {charm_str:<8} {t['result']:<8} {t['pnl']:<+7.1f} {t['max_profit']:<+7.1f} {t['max_loss']:<+7.1f} {t['elapsed_min']:<5.0f}")


# ============================================================
# PHASE 3: Wider strike ranges (closer to ATM)
# ============================================================
print("\n" + "=" * 80)
print("PHASE 3: ATM-FOCUSED IV SKEW (strikes 5-15pt from spot)")
print("=" * 80)

for thresh in [0.05, 0.08, 0.10, 0.12, 0.15]:
    trades = run_backtest({
        "skew_thresh": thresh, "lookback": 5, "price_stable": 5.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 30,
        "put_range": (5, 15), "call_range": (5, 15),
    })
    print_summary(f"ATM 5-15, thresh={thresh:.0%}", trades)

# Best ATM combo detail
print("\n--- ATM detail with best threshold ---")
for thresh in [0.08, 0.10]:
    trades = run_backtest({
        "skew_thresh": thresh, "lookback": 5, "price_stable": 8.0,
        "target_pts": 10, "stop_pts": 8, "cooldown_min": 20,
        "put_range": (5, 15), "call_range": (5, 15),
    })
    if trades:
        wins = [t for t in trades if t["result"] == "WIN"]
        losses = [t for t in trades if t["result"] == "LOSS"]
        total_pnl = sum(t["pnl"] for t in trades)
        wr = len(wins) / len(trades) * 100
        print(f"\n  ATM 5-15, thresh={thresh:.0%}, stable=8pt, CD=20min:")
        print(f"  {len(trades)} trades | {len(wins)}W/{len(losses)}L | WR={wr:.0f}% | P&L={total_pnl:+.1f}")

        for d in ["LONG", "SHORT"]:
            dt = [t for t in trades if t["direction"] == d]
            if dt:
                dw = [t for t in dt if t["result"] == "WIN"]
                print(f"    {d}: {len(dt)} trades, {len(dw)} wins ({len(dw)/len(dt)*100:.0f}% WR), P&L={sum(t['pnl'] for t in dt):+.1f}")

        print(f"\n  {'Date':<12} {'Time':<6} {'Dir':<6} {'Entry':<8} {'Chg%':<7} {'Result':<8} {'P&L':<7} {'MaxP':<7}")
        print("  " + "-" * 70)
        for t in trades:
            print(f"  {t['date']:<12} {t['time']:<6} {t['direction']:<6} {t['entry']:<8} {t['skew_chg_pct']:<+7.1f} {t['result']:<8} {t['pnl']:<+7.1f} {t['max_profit']:<+7.1f}")

print("\n\nDONE.")
