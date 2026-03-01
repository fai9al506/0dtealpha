"""Analyze ES Absorption: max profit after entry, drawdown after +10."""
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import pytz

NY = pytz.timezone("US/Eastern")
engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    # Get all ES Absorption signals
    signals = conn.execute(text("""
        SELECT id, ts, direction, grade, score, paradigm, abs_es_price, spot,
               outcome_result, outcome_pnl
        FROM setup_log
        WHERE setup_name = 'ES Absorption'
        ORDER BY ts ASC
    """)).mappings().all()

print(f"Total ES Absorption signals: {len(signals)}")
print()

results = []

for sig in signals:
    sig_id = sig["id"]
    ts = sig["ts"]
    es_entry = sig["abs_es_price"]
    if not es_entry:
        continue
    direction = sig["direction"]
    is_long = direction in ("long", "bullish")

    alert_date = ts.astimezone(NY).date() if ts.tzinfo else NY.localize(ts).date()

    with engine.begin() as conn:
        bars = conn.execute(text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end
            FROM es_range_bars
            WHERE trade_date = :td AND source = 'rithmic' AND status = 'closed'
            ORDER BY bar_idx ASC
        """), {"td": alert_date.isoformat()}).mappings().all()

    if not bars:
        # Try live source
        with engine.begin() as conn:
            bars = conn.execute(text("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end
                FROM es_range_bars
                WHERE trade_date = :td AND source = 'live' AND status = 'closed'
                ORDER BY bar_idx ASC
            """), {"td": alert_date.isoformat()}).mappings().all()

    if not bars:
        print(f"  #{sig_id}: no bars for {alert_date}")
        continue

    # Find signal bar
    signal_bar_idx = None
    for b in bars:
        bar_start = b["ts_start"]
        if hasattr(bar_start, "tzinfo") and bar_start.tzinfo is None:
            bar_start = NY.localize(bar_start)
        if bar_start <= ts:
            signal_bar_idx = b["bar_idx"]
        else:
            break
    if signal_bar_idx is None:
        continue

    # Walk bars after signal
    max_profit = 0.0
    max_loss = 0.0
    hit_10 = False
    profit_at_bar = []  # (bars_after, profit_at_that_bar)
    bars_after = 0

    for b in bars:
        if b["bar_idx"] <= signal_bar_idx:
            continue
        bars_after += 1

        if is_long:
            p_high = b["bar_high"] - es_entry
            p_low = b["bar_low"] - es_entry
        else:
            p_high = es_entry - b["bar_low"]
            p_low = es_entry - b["bar_high"]

        if p_high > max_profit:
            max_profit = p_high
        if p_low < max_loss:
            max_loss = p_low

        if not hit_10 and max_profit >= 10:
            hit_10 = True

        profit_at_bar.append((bars_after, round(p_high, 2), round(p_low, 2), round(max_profit, 2)))

    # After reaching +10, what was the max additional profit and max drawdown from +10?
    post_10_max = 0.0
    post_10_dd = 0.0
    reached_10_bar = None
    if hit_10:
        peak_after_10 = 10.0
        for ba, ph, pl, mp in profit_at_bar:
            if mp >= 10:
                if reached_10_bar is None:
                    reached_10_bar = ba
                if ph > peak_after_10:
                    peak_after_10 = ph
                dd_from_peak = peak_after_10 - pl
                if dd_from_peak > post_10_dd:
                    post_10_dd = dd_from_peak
        post_10_max = peak_after_10

    results.append({
        "id": sig_id,
        "ts": str(ts)[:16],
        "dir": "BUY" if is_long else "SELL",
        "grade": sig["grade"],
        "es": es_entry,
        "outcome": sig["outcome_result"] or "PENDING",
        "outcome_pnl": float(sig["outcome_pnl"] or 0),
        "max_profit": round(max_profit, 1),
        "max_loss": round(max_loss, 1),
        "hit_10": hit_10,
        "bars_to_10": reached_10_bar,
        "post_10_max": round(post_10_max, 1),
        "post_10_dd": round(post_10_dd, 1),
        "bars_after": bars_after,
    })

# Print results
print(f"\n{'#ID':>5} {'Date':16} {'Dir':4} {'G':2} {'Res':7} {'PnL':>6} {'MaxP':>6} {'MaxL':>6} {'Hit10':5} {'BarsTo10':>8} {'Post10Max':>9} {'Post10DD':>8} {'Bars':>5}")
print("-" * 105)
for r in results:
    print(f"#{r['id']:>4} {r['ts']} {r['dir']:<4} {r['grade']:<2} {r['outcome']:<7} {r['outcome_pnl']:+6.1f} {r['max_profit']:+6.1f} {r['max_loss']:+6.1f} {'YES' if r['hit_10'] else 'no':5} {r['bars_to_10'] or '-':>7} {r['post_10_max']:+9.1f} {r['post_10_dd']:+8.1f} {r['bars_after']:>5}")

# Summary stats
hit10 = [r for r in results if r["hit_10"]]
print(f"\n=== Summary ===")
print(f"Total signals: {len(results)}")
print(f"Reached +10: {len(hit10)}/{len(results)} ({100*len(hit10)/len(results):.0f}%)")
if hit10:
    avg_post10_max = sum(r["post_10_max"] for r in hit10) / len(hit10)
    avg_post10_dd = sum(r["post_10_dd"] for r in hit10) / len(hit10)
    max_post10_max = max(r["post_10_max"] for r in hit10)
    max_post10_dd = max(r["post_10_dd"] for r in hit10)
    print(f"After hitting +10:")
    print(f"  Avg peak beyond 10: {avg_post10_max:.1f} pts (max: {max_post10_max:.1f})")
    print(f"  Avg drawdown from peak: {avg_post10_dd:.1f} pts (max: {max_post10_dd:.1f})")

    # Trail simulation: BE at +10, trail with 5pt gap
    print(f"\n=== Trail Simulation: BE@+10, gap=5 ===")
    trail_pnl = 0
    for r in hit10:
        # After reaching +10, trailing stop at peak - 5, min = 10 (breakeven+10)
        trail_stop = 10.0  # starts at +10 (breakeven to +10)
        peak = 10.0
        final = 10.0  # default: took +10
        stopped = False
        # Need to re-walk bars... let's estimate from max
        # If post_10_max > 15 (trail would move), final = post_10_max - 5
        # Otherwise final = 10 (trail never moved from +10)
        if r["post_10_max"] >= 15:
            final = r["post_10_max"] - 5
        else:
            final = 10.0
        trail_pnl += final
        print(f"  #{r['id']}: peak={r['post_10_max']:+.1f} → trail exit ~{final:+.1f}")
    print(f"  Trail total: {trail_pnl:+.1f} vs fixed 10pt: {10*len(hit10):+.1f}")

# All trades max profit distribution
print(f"\n=== Max Profit Distribution (all trades) ===")
for threshold in [5, 10, 15, 20, 25, 30]:
    count = sum(1 for r in results if r["max_profit"] >= threshold)
    print(f"  Reached +{threshold}: {count}/{len(results)} ({100*count/len(results):.0f}%)")
