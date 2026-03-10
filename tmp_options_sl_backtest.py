"""
Backtest: Should 0DTE options have a stop-loss?
Analyze Skew Charm trades to see if 40% SL helps or hurts.
"""
import os
from sqlalchemy import create_engine, text
from datetime import timedelta

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, direction, grade, score, spot,
               outcome_result, outcome_pnl, outcome_first_event,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
               outcome_target_level, outcome_stop_level,
               ts, ts::date as trade_date,
               greek_alignment, paradigm
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IS NOT NULL
          AND outcome_result NOT IN ('OPEN', 'PENDING')
        ORDER BY id ASC
    """)).mappings().all()

print(f"Skew Charm trades: {len(rows)}")
print()

all_trades = []
for r in rows:
    et = r["ts"] - timedelta(hours=5)
    time_str = f"{et.hour}:{et.minute:02d}"
    max_p = float(r["outcome_max_profit"] or 0)
    max_l = float(r["outcome_max_loss"] or 0)
    pnl = float(r["outcome_pnl"] or 0)
    elapsed = float(r["outcome_elapsed_min"] or 0)
    result = r["outcome_result"]

    all_trades.append({
        "id": r["id"], "dir": r["direction"], "result": result,
        "pnl": pnl, "max_profit": max_p, "max_loss": max_l,
        "elapsed": elapsed, "spot": float(r["spot"]),
        "date": str(r["trade_date"]), "time": time_str,
        "grade": r["grade"],
    })

# Print all trades
print(f"{'ID':>4} {'Date':>10} {'Time':>5} {'Dir':>5} {'Grade':>6} {'Result':>7} {'PnL':>7} {'MaxP':>7} {'MaxL':>7} {'Elap':>5}")
print("-" * 80)
for t in all_trades:
    print(f"{t['id']:>4} {t['date']:>10} {t['time']:>5} {t['dir']:>5} {t['grade']:>6} {t['result']:>7} {t['pnl']:>+7.1f} {t['max_profit']:>+7.1f} {t['max_loss']:>+7.1f} {t['elapsed']:>4.0f}m")

print()
print("=" * 80)
print("MAX ADVERSE EXCURSION ANALYSIS")
print("=" * 80)
print()

# How far against did winning trades go?
print("Winning trades by max adverse excursion (underlying SPX pts):")
buckets = [(0, 1, "< 1 pt"), (1, 2, "1-2 pts"), (2, 3, "2-3 pts"), (3, 5, "3-5 pts"),
           (5, 8, "5-8 pts"), (8, 12, "8-12 pts"), (12, 50, "12+ pts")]
for lo, hi, label in buckets:
    wins = [t for t in all_trades if t["result"] == "WIN" and lo <= abs(t["max_loss"]) < hi]
    if wins:
        avg_dip = sum(abs(t["max_loss"]) for t in wins) / len(wins)
        avg_pnl = sum(t["pnl"] for t in wins) / len(wins)
        print(f"  {label:<12} {len(wins):>3} wins  avg_dip={avg_dip:>5.1f} pts  avg_final_pnl={avg_pnl:>+6.1f}")

print()
print("All trades by max adverse excursion:")
for lo, hi, label in buckets:
    sub = [t for t in all_trades if lo <= abs(t["max_loss"]) < hi]
    if sub:
        n = len(sub)
        w = sum(1 for t in sub if t["result"] == "WIN")
        l = sum(1 for t in sub if t["result"] == "LOSS")
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        avg_pnl = sum(t["pnl"] for t in sub) / n
        print(f"  {label:<12} N={n:>3}  W={w:>2}  L={l:>2}  WR={wr:>5.1f}%  avg_pnl={avg_pnl:>+6.1f}")

print()
print("=" * 80)
print("OPTION STOP-LOSS IMPACT SIMULATION")
print("=" * 80)
print()

# 0DTE option at 0.30 delta, entry price ~$3-$8
# Option % drop from adverse SPX move:
# For 0DTE, gamma is extremely high, so delta changes rapidly.
# Rough model for a 0.30 delta, ~$5 option:
#   1 pt against: -$0.30 (delta) -> ~6% drop
#   2 pts against: -$0.55 (delta dropping) -> ~11% drop
#   3 pts against: -$0.75 -> ~15% drop
#   5 pts against: -$1.10 -> ~22% drop + theta -> ~30%
#   7 pts against: -$1.40 -> ~28% + theta -> ~40% <-- 40% SL triggers
#   10 pts against: -$1.70 -> ~34% + theta -> ~55%
#   15 pts against: -> ~70-80% drop
#
# But theta on 0DTE is brutal: option loses ~10-20% per HOUR just from decay
# So the real SL trigger is lower (in SPX pts) than pure delta would suggest.
#
# Conservative estimate: 40% option drop ~ 5-7 pts adverse on underlying
# (depends heavily on time of day - morning gamma < afternoon gamma)

print("0DTE Option Price Model (0.30 delta, ~$5 entry):")
print("  Adverse move -> Estimated option % drop (including theta/gamma):")
print("    2 pts:  ~10-15%  (safe)")
print("    3 pts:  ~15-25%  (safe)")
print("    5 pts:  ~25-40%  (40% SL borderline)")
print("    7 pts:  ~40-55%  (40% SL triggered)")
print("    10 pts: ~55-75%  (deep loss)")
print("    15 pts: ~75-90%  (near total loss)")
print()

# Test various SL levels (in equivalent underlying pts)
# Conservative: assume 40% SL = ~5 pts adverse move
# Aggressive: assume 40% SL = ~7 pts adverse move
for sl_underlying, sl_label in [
    (3, "Tight SL (~20% option, ~3pt underlying)"),
    (5, "40% SL morning (~5pt underlying)"),
    (7, "40% SL afternoon (~7pt underlying)"),
    (10, "60% SL (~10pt underlying)"),
    (999, "No SL (time exit only)"),
]:
    # Trades that hit this SL level
    stopped_wins = [t for t in all_trades if t["result"] == "WIN" and abs(t["max_loss"]) >= sl_underlying]
    kept_wins = [t for t in all_trades if t["result"] == "WIN" and abs(t["max_loss"]) < sl_underlying]
    stopped_losses = [t for t in all_trades if t["result"] == "LOSS" and abs(t["max_loss"]) >= sl_underlying]
    kept_losses = [t for t in all_trades if t["result"] == "LOSS" and abs(t["max_loss"]) < sl_underlying]
    stopped_expired = [t for t in all_trades if t["result"] not in ("WIN", "LOSS") and abs(t["max_loss"]) >= sl_underlying]

    # With SL: stopped trades lose the SL amount, kept trades keep their result
    total_with_sl = (
        sum(t["pnl"] for t in kept_wins) +      # wins that survive
        sum(t["pnl"] for t in kept_losses) +     # losses within SL
        sum(-sl_underlying for t in stopped_wins) +  # would-be wins stopped out (LOST)
        sum(-sl_underlying for t in stopped_losses) +  # losses capped at SL
        sum(t["pnl"] for t in stopped_expired if abs(t["max_loss"]) < sl_underlying) +
        sum(-sl_underlying for t in stopped_expired)
    )

    total_no_sl = sum(t["pnl"] for t in all_trades)

    # Net impact of SL
    lost_profit = sum(t["pnl"] for t in stopped_wins)  # positive PnL that we'd miss
    capped_losses = sum((-sl_underlying - t["pnl"]) for t in stopped_losses if t["pnl"] < -sl_underlying)  # saved by cap

    n_stopped = len(stopped_wins) + len(stopped_losses) + len(stopped_expired)

    print(f"{sl_label}")
    print(f"  Stopped winners: {len(stopped_wins):>2}  (lost profit: {lost_profit:>+7.1f} pts)")
    print(f"  Stopped losers:  {len(stopped_losses):>2}  (saved from worse: {capped_losses:>+6.1f} pts)")
    print(f"  Stopped expired: {len(stopped_expired):>2}")
    print(f"  NET: {'HELPS' if capped_losses > lost_profit else 'HURTS'} by {abs(capped_losses - lost_profit):.1f} pts")
    print()

# List the critical cases: WINS that dipped hard
print("=" * 80)
print("CRITICAL: Winning trades that dipped >= 5 pts (would be stopped by 40% SL)")
print("=" * 80)
big_dip_wins = [t for t in all_trades if t["result"] == "WIN" and abs(t["max_loss"]) >= 5]
if big_dip_wins:
    for t in big_dip_wins:
        print(f"  #{t['id']} {t['date']} {t['time']} {t['dir']:>5}  dipped {t['max_loss']:>+6.1f} -> won {t['pnl']:>+6.1f}  held {t['elapsed']:.0f}m")
    total_lost = sum(t["pnl"] for t in big_dip_wins)
    print(f"  TOTAL PROFIT THAT WOULD BE LOST: {total_lost:+.1f} pts across {len(big_dip_wins)} trades")
else:
    print("  None! All wins had < 5 pts adverse excursion.")

print()
print("Trades that LOST and dipped > 10 pts (SL would have saved):")
big_losers = [t for t in all_trades if t["result"] == "LOSS" and abs(t["max_loss"]) >= 10]
if big_losers:
    for t in big_losers:
        print(f"  #{t['id']} {t['date']} {t['time']} {t['dir']:>5}  dipped {t['max_loss']:>+6.1f}  lost {t['pnl']:>+6.1f}  held {t['elapsed']:.0f}m")
else:
    print("  None!")

# Final verdict
print()
print("=" * 80)
print("VERDICT")
print("=" * 80)
total_pnl = sum(t["pnl"] for t in all_trades)
wins = sum(1 for t in all_trades if t["result"] == "WIN")
losses = sum(1 for t in all_trades if t["result"] == "LOSS")
print(f"Skew Charm: {wins}W/{losses}L, {wins/(wins+losses)*100:.1f}% WR, {total_pnl:+.1f} pts total")
print(f"91.5% WR means for every 10 options bought, ~9 win and ~1 loses.")
print(f"A stop-loss protects against the rare loss but risks killing the frequent winners.")
avg_win_adverse = sum(abs(t["max_loss"]) for t in all_trades if t["result"] == "WIN") / wins if wins > 0 else 0
avg_loss_adverse = sum(abs(t["max_loss"]) for t in all_trades if t["result"] == "LOSS") / losses if losses > 0 else 0
print(f"Avg adverse on WINS: {avg_win_adverse:.1f} pts | Avg adverse on LOSSES: {avg_loss_adverse:.1f} pts")
