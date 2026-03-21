"""
Deep Analysis: Skew Charm Stop Loss Optimization
Is SL=20 optimal? Or does it just make losses bigger?
"""
import os, json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL:
    DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
if "postgresql://" in DB_URL and "postgresql+psycopg" not in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DB_URL)

# ── Pull ALL Skew Charm trades with outcomes ──
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, grade, score, spot, target,
               outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               outcome_first_event, outcome_elapsed_min,
               outcome_target_level, outcome_stop_level,
               greek_alignment, vix, overvix,
               support_score, upside_score, floor_cluster_score,
               target_cluster_score, rr_score
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

trades = [dict(r) for r in rows]
print(f"Total Skew Charm trades with outcomes: {len(trades)}")
print()

# ── Basic Stats ──
wins = [t for t in trades if t['outcome_result'] == 'WIN']
losses = [t for t in trades if t['outcome_result'] == 'LOSS']
expired = [t for t in trades if t['outcome_result'] == 'EXPIRED']
total_pnl = sum(t['outcome_pnl'] or 0 for t in trades)

print("=" * 70)
print("BASIC OVERVIEW")
print("=" * 70)
print(f"  Wins: {len(wins)} ({100*len(wins)/len(trades):.1f}%)")
print(f"  Losses: {len(losses)} ({100*len(losses)/len(trades):.1f}%)")
print(f"  Expired: {len(expired)} ({100*len(expired)/len(trades):.1f}%)")
print(f"  Total PnL: {total_pnl:+.1f} pts")
print(f"  Avg PnL/trade: {total_pnl/len(trades):+.2f} pts")
print()

# ── WINNING TRADES: MAE Analysis (how deep did they dip before winning?) ──
print("=" * 70)
print("WINNING TRADES — Max Adverse Excursion (MAE)")
print("How deep did winning trades dip before recovering?")
print("=" * 70)
win_maes = []
for t in wins:
    mae = abs(t['outcome_max_loss'] or 0)
    win_maes.append(mae)

if win_maes:
    win_maes_sorted = sorted(win_maes)
    print(f"  Count: {len(win_maes)}")
    print(f"  Min MAE: {min(win_maes):.1f} pts")
    print(f"  Max MAE: {max(win_maes):.1f} pts")
    print(f"  Avg MAE: {sum(win_maes)/len(win_maes):.1f} pts")
    print(f"  Median MAE: {win_maes_sorted[len(win_maes_sorted)//2]:.1f} pts")

    # Distribution buckets
    buckets = [(0, 2), (2, 5), (5, 8), (8, 10), (10, 12), (12, 15), (15, 18), (18, 20), (20, 25), (25, 50)]
    print(f"\n  MAE Distribution (winning trades):")
    for lo, hi in buckets:
        count = sum(1 for m in win_maes if lo <= m < hi)
        if count > 0:
            pct = 100 * count / len(win_maes)
            bar = "#" * int(pct / 2)
            print(f"    {lo:>2}-{hi:<2} pts: {count:>3} ({pct:5.1f}%) {bar}")

    # How many winners would be killed by tighter stops?
    print(f"\n  Winners killed by tighter stop:")
    for sl in [8, 10, 12, 14, 15, 16, 18, 20, 25]:
        killed = sum(1 for m in win_maes if m >= sl)
        killed_pnl = sum(t['outcome_pnl'] or 0 for t in wins if abs(t['outcome_max_loss'] or 0) >= sl)
        pct = 100 * killed / len(win_maes)
        print(f"    SL={sl:>2}: kills {killed:>3} winners ({pct:5.1f}%), losing {killed_pnl:+.1f} pts of winning PnL")

print()

# ── LOSING TRADES: MFE Analysis (did they ever go positive before losing?) ──
print("=" * 70)
print("LOSING TRADES — Max Favorable Excursion (MFE)")
print("Did losing trades ever go positive before hitting stop?")
print("=" * 70)
loss_mfes = []
loss_maes = []
for t in losses:
    mfe = abs(t['outcome_max_profit'] or 0)
    mae = abs(t['outcome_max_loss'] or 0)
    loss_mfes.append(mfe)
    loss_maes.append(mae)

if loss_mfes:
    print(f"  Count: {len(loss_mfes)}")
    print(f"  Avg MFE (best point before losing): {sum(loss_mfes)/len(loss_mfes):.1f} pts")
    print(f"  Avg MAE (actual loss depth): {sum(loss_maes)/len(loss_maes):.1f} pts")
    print(f"  Avg PnL: {sum(t['outcome_pnl'] or 0 for t in losses)/len(losses):+.1f} pts")

    # How many losers went positive at all?
    went_positive = sum(1 for m in loss_mfes if m > 0)
    went_5plus = sum(1 for m in loss_mfes if m >= 5)
    went_10plus = sum(1 for m in loss_mfes if m >= 10)
    print(f"\n  Losers that went positive first: {went_positive}/{len(losses)} ({100*went_positive/len(losses):.0f}%)")
    print(f"  Losers that hit +5 pts first: {went_5plus}/{len(losses)} ({100*went_5plus/len(losses):.0f}%)")
    print(f"  Losers that hit +10 pts first: {went_10plus}/{len(losses)} ({100*went_10plus/len(losses):.0f}%)")

    print(f"\n  Losing trade MFE distribution:")
    mfe_buckets = [(0, 1), (1, 3), (3, 5), (5, 8), (8, 10), (10, 15), (15, 20), (20, 30)]
    for lo, hi in mfe_buckets:
        count = sum(1 for m in loss_mfes if lo <= m < hi)
        if count > 0:
            pct = 100 * count / len(loss_mfes)
            bar = "#" * int(pct / 2)
            print(f"    {lo:>2}-{hi:<2} pts: {count:>3} ({pct:5.1f}%) {bar}")

    print(f"\n  Losing trade actual loss (MAE/PnL) distribution:")
    for lo, hi in [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30)]:
        count = sum(1 for m in loss_maes if lo <= m < hi)
        if count > 0:
            pct = 100 * count / len(loss_maes)
            print(f"    {lo:>2}-{hi:<2} pts: {count:>3} ({pct:5.1f}%)")
else:
    print("  No losing trades!")

print()

# ── EXPIRED TRADES: Analysis ──
print("=" * 70)
print("EXPIRED TRADES — Held to market close")
print("=" * 70)
if expired:
    exp_pnls = [t['outcome_pnl'] or 0 for t in expired]
    exp_positive = sum(1 for p in exp_pnls if p > 0)
    exp_negative = sum(1 for p in exp_pnls if p < 0)
    print(f"  Count: {len(expired)}")
    print(f"  Positive at close: {exp_positive}, Negative: {exp_negative}")
    print(f"  Avg PnL: {sum(exp_pnls)/len(exp_pnls):+.1f} pts")
    print(f"  Total PnL: {sum(exp_pnls):+.1f} pts")
    for t in expired:
        print(f"    #{t['id']} {str(t['ts_et'])[:16]} {t['direction']:>5} "
              f"PnL={t['outcome_pnl'] or 0:+.1f} MFE={t['outcome_max_profit'] or 0:+.1f} "
              f"MAE={t['outcome_max_loss'] or 0:+.1f} grade={t['grade']}")
print()

# ── STOP LOSS SIMULATION: What happens at different SL levels? ──
print("=" * 70)
print("STOP LOSS SIMULATION — What-if analysis")
print("For each SL, recalculate: killed winners + modified losses")
print("=" * 70)

# For each SL value, compute:
# - Winners with MAE >= SL -> become LOSS at -SL (killed)
# - Losers: if their MAE >= SL, loss stays -SL (same or smaller loss)
#   if their MAE < current_SL but MAE >= new_SL, still loss at their actual MAE
# - Actually: outcome_max_loss IS the max adverse, outcome_pnl is the ACTUAL exit
#   With SL=X: if MAE >= X, trade exits at -X (whether it was a winner or loser)
#   If MAE < X, trade plays out as it did

# Current SL is 20. Let's simulate different SLs.
# NOTE: For winning trades, we check if MAE ever >= SL. If yes, the trade would have been
# stopped out at -SL BEFORE it could win (since the loss came first in time).
# BUT we don't have the TIME sequence of MFE/MAE. We only know the extremes.
# However, for a trade that WON: the MAE likely happened BEFORE the MFE (dipped then recovered).
# For a trade that LOST: the MFE likely happened BEFORE the MAE (went up then crashed).
# This is a reasonable assumption for simulation.

print(f"\n  Current deployed: SL=20")
print(f"  {'SL':>4} | {'Wins':>5} | {'Losses':>6} | {'Exp':>4} | {'WR%':>6} | {'TotalPnL':>10} | {'AvgPnL':>8} | {'MaxDD':>7} | {'PF':>5}")
print(f"  {'-'*4}-+-{'-'*5}-+-{'-'*6}-+-{'-'*4}-+-{'-'*6}-+-{'-'*10}-+-{'-'*8}-+-{'-'*7}-+-{'-'*5}")

for sl in [6, 8, 10, 12, 14, 15, 16, 18, 20, 22, 25, 30]:
    sim_wins = 0
    sim_losses = 0
    sim_expired = 0
    sim_total_pnl = 0
    sim_gross_win = 0
    sim_gross_loss = 0
    sim_pnls = []  # for drawdown calc

    for t in trades:
        mae = abs(t['outcome_max_loss'] or 0)
        actual_pnl = t['outcome_pnl'] or 0
        result = t['outcome_result']

        if result == 'WIN':
            if mae >= sl:
                # Winner killed — stopped out at -SL before recovery
                sim_losses += 1
                pnl = -sl
                sim_gross_loss += sl
            else:
                # Winner survives — same PnL
                sim_wins += 1
                pnl = actual_pnl
                sim_gross_win += actual_pnl
        elif result == 'LOSS':
            # Loser: cap loss at -SL (might be smaller than actual if SL < 20)
            sim_losses += 1
            # actual loss is actual_pnl (negative). With tighter SL, loss = -min(sl, mae)
            pnl = -min(sl, mae)
            sim_gross_loss += min(sl, mae)
        elif result == 'EXPIRED':
            # Expired: if MAE >= SL, would have been stopped out
            if mae >= sl:
                sim_losses += 1
                pnl = -sl
                sim_gross_loss += sl
            else:
                sim_expired += 1
                pnl = actual_pnl
                if actual_pnl > 0:
                    sim_gross_win += actual_pnl
                else:
                    sim_gross_loss += abs(actual_pnl)

        sim_total_pnl += pnl
        sim_pnls.append(pnl)

    # Calculate max drawdown
    peak = 0
    max_dd = 0
    cumsum = 0
    for p in sim_pnls:
        cumsum += p
        if cumsum > peak:
            peak = cumsum
        dd = peak - cumsum
        if dd > max_dd:
            max_dd = dd

    total = sim_wins + sim_losses + sim_expired
    wr = 100 * sim_wins / total if total > 0 else 0
    avg_pnl = sim_total_pnl / total if total > 0 else 0
    pf = sim_gross_win / sim_gross_loss if sim_gross_loss > 0 else float('inf')

    marker = " <-- CURRENT" if sl == 20 else ""
    print(f"  {sl:>4} | {sim_wins:>5} | {sim_losses:>6} | {sim_expired:>4} | {wr:>5.1f}% | {sim_total_pnl:>+10.1f} | {avg_pnl:>+8.2f} | {max_dd:>7.1f} | {pf:>5.2f}{marker}")

print()

# ── INDIVIDUAL TRADE DETAIL ──
print("=" * 70)
print("ALL TRADES — Individual Detail")
print("=" * 70)
print(f"  {'ID':>5} | {'Date':>16} | {'Dir':>5} | {'Grade':>5} | {'Result':>7} | {'PnL':>7} | {'MFE':>7} | {'MAE':>7} | {'1stEvt':>8} | {'Min':>4} | {'Align':>5} | {'VIX':>5}")
print(f"  {'-'*5}-+-{'-'*16}-+-{'-'*5}-+-{'-'*5}-+-{'-'*7}-+-{'-'*7}-+-{'-'*7}-+-{'-'*7}-+-{'-'*8}-+-{'-'*4}-+-{'-'*5}-+-{'-'*5}")

for t in trades:
    mae = abs(t['outcome_max_loss'] or 0)
    mfe = abs(t['outcome_max_profit'] or 0)
    pnl = t['outcome_pnl'] or 0
    align = t['greek_alignment'] if t['greek_alignment'] is not None else '?'
    vix = f"{t['vix']:.1f}" if t['vix'] else '?'
    elapsed = t['outcome_elapsed_min'] or 0
    first_evt = t['outcome_first_event'] or '?'

    # Highlight if MAE was close to 20 (within 5 pts of stop)
    flag = ""
    if t['outcome_result'] == 'WIN' and mae >= 15:
        flag = " *** CLOSE CALL"
    elif t['outcome_result'] == 'WIN' and mae >= 10:
        flag = " ** deep dip"

    print(f"  {t['id']:>5} | {str(t['ts_et'])[:16]:>16} | {t['direction']:>5} | {t['grade']:>5} | "
          f"{t['outcome_result']:>7} | {pnl:>+7.1f} | {mfe:>+7.1f} | {mae:>7.1f} | {first_evt:>8} | "
          f"{elapsed:>4} | {align:>5} | {vix:>5}{flag}")

print()

# ── KEY INSIGHTS ──
print("=" * 70)
print("KEY INSIGHTS")
print("=" * 70)

# How many winners had MAE > 10?
deep_dip_wins = [t for t in wins if abs(t['outcome_max_loss'] or 0) >= 10]
print(f"\n  Winners with MAE >= 10 pts (needed SL > 10 to survive):")
print(f"    Count: {len(deep_dip_wins)}/{len(wins)} ({100*len(deep_dip_wins)/len(wins):.1f}%)")
total_saved = sum(t['outcome_pnl'] or 0 for t in deep_dip_wins)
print(f"    Total PnL saved by having SL>10: {total_saved:+.1f} pts")

deep_dip_15 = [t for t in wins if abs(t['outcome_max_loss'] or 0) >= 15]
print(f"\n  Winners with MAE >= 15 pts (needed SL > 15 to survive):")
print(f"    Count: {len(deep_dip_15)}/{len(wins)} ({100*len(deep_dip_15)/len(wins):.1f}%)")
total_saved_15 = sum(t['outcome_pnl'] or 0 for t in deep_dip_15)
print(f"    Total PnL saved by having SL>15: {total_saved_15:+.1f} pts")

# Avg win vs avg loss
avg_win = sum(t['outcome_pnl'] or 0 for t in wins) / len(wins) if wins else 0
avg_loss = sum(t['outcome_pnl'] or 0 for t in losses) / len(losses) if losses else 0
print(f"\n  Avg WIN: {avg_win:+.1f} pts")
print(f"  Avg LOSS: {avg_loss:+.1f} pts")
print(f"  Win/Loss ratio: {abs(avg_win/avg_loss):.2f}x" if avg_loss != 0 else "  No losses!")

# By direction
for d in ['long', 'short']:
    dir_trades = [t for t in trades if t['direction'] == d]
    if dir_trades:
        dir_wins = sum(1 for t in dir_trades if t['outcome_result'] == 'WIN')
        dir_pnl = sum(t['outcome_pnl'] or 0 for t in dir_trades)
        print(f"\n  {d.upper()}: {len(dir_trades)} trades, {dir_wins} wins ({100*dir_wins/len(dir_trades):.0f}% WR), {dir_pnl:+.1f} pts")

# Loss details
print(f"\n  Individual losses:")
for t in losses:
    mae = abs(t['outcome_max_loss'] or 0)
    mfe = abs(t['outcome_max_profit'] or 0)
    print(f"    #{t['id']} {str(t['ts_et'])[:16]} {t['direction']:>5} grade={t['grade']} "
          f"PnL={t['outcome_pnl'] or 0:+.1f} MFE={mfe:.1f} MAE={mae:.1f} "
          f"align={t['greek_alignment']} vix={t['vix']}")

# Expired details if any have significant PnL
if expired:
    print(f"\n  Expired trades detail:")
    for t in expired:
        mae = abs(t['outcome_max_loss'] or 0)
        mfe = abs(t['outcome_max_profit'] or 0)
        print(f"    #{t['id']} {str(t['ts_et'])[:16]} {t['direction']:>5} grade={t['grade']} "
              f"PnL={t['outcome_pnl'] or 0:+.1f} MFE={mfe:.1f} MAE={mae:.1f}")

print()
print("=" * 70)
print("RECOMMENDATION ANALYSIS")
print("=" * 70)

# Find optimal SL
best_sl = 20
best_pnl = -9999
results = {}
for sl in range(6, 31):
    sim_pnl = 0
    sim_wins_count = 0
    sim_losses_count = 0
    sim_gross_w = 0
    sim_gross_l = 0

    for t in trades:
        mae = abs(t['outcome_max_loss'] or 0)
        actual_pnl = t['outcome_pnl'] or 0
        result = t['outcome_result']

        if result == 'WIN':
            if mae >= sl:
                pnl = -sl
                sim_losses_count += 1
                sim_gross_l += sl
            else:
                pnl = actual_pnl
                sim_wins_count += 1
                sim_gross_w += actual_pnl
        elif result == 'LOSS':
            pnl = -min(sl, mae)
            sim_losses_count += 1
            sim_gross_l += min(sl, mae)
        elif result == 'EXPIRED':
            if mae >= sl:
                pnl = -sl
                sim_losses_count += 1
                sim_gross_l += sl
            else:
                pnl = actual_pnl
                if actual_pnl > 0:
                    sim_gross_w += actual_pnl
                else:
                    sim_gross_l += abs(actual_pnl)

        sim_pnl += pnl

    total_trades = sim_wins_count + sim_losses_count + len([t for t in trades if t['outcome_result'] == 'EXPIRED' and abs(t['outcome_max_loss'] or 0) < sl])
    wr = 100 * sim_wins_count / total_trades if total_trades > 0 else 0
    pf = sim_gross_w / sim_gross_l if sim_gross_l > 0 else float('inf')
    results[sl] = {'pnl': sim_pnl, 'wr': wr, 'pf': pf, 'wins': sim_wins_count, 'losses': sim_losses_count}

    if sim_pnl > best_pnl:
        best_pnl = sim_pnl
        best_sl = sl

print(f"\n  OPTIMAL SL by total PnL: SL={best_sl} -> {best_pnl:+.1f} pts")
print(f"  Current SL=20 -> {results[20]['pnl']:+.1f} pts")
print(f"  Difference: {best_pnl - results[20]['pnl']:+.1f} pts")

# Find optimal by PF
best_pf_sl = max(range(6, 31), key=lambda s: results[s]['pf'] if results[s]['pf'] != float('inf') else 0)
print(f"\n  OPTIMAL SL by Profit Factor: SL={best_pf_sl} -> PF={results[best_pf_sl]['pf']:.2f}")
print(f"  Current SL=20 -> PF={results[20]['pf']:.2f}")

# Top 5 SLs
print(f"\n  Top 5 SL values by PnL:")
sorted_sls = sorted(results.items(), key=lambda x: x[1]['pnl'], reverse=True)
for sl, r in sorted_sls[:5]:
    print(f"    SL={sl:>2}: PnL={r['pnl']:>+8.1f}, WR={r['wr']:>5.1f}%, PF={r['pf']:>5.2f}, W={r['wins']}, L={r['losses']}")

print()
