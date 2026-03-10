"""Greek Context Filter Simulation - compute before/after impact on PnL,
daily PnL, monthly income, risk factors (profit factor, max DD, Sharpe-like).

Tests individual and combined filters, then finds the optimal filter set.
"""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

# ---- Step 1: Pull all trades + retroactively compute Greek context ----
with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.max_plus_gex, s.paradigm,
               s.outcome_result, s.outcome_pnl,
               s.ts::date as trade_date
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.id
    """)).mappings().all()

print(f"Loading {len(trades)} trades...")

enriched = []
with engine.begin() as conn:
    for t in trades:
        trade_ts = t["ts_et"]

        # Vanna ALL
        vanna_all = None
        row = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'ALL'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'ALL'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if row and row["total"] is not None:
            vanna_all = float(row["total"])

        # Vanna THIS_WEEK
        vanna_weekly = None
        row = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIS_WEEK'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'THIS_WEEK'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if row and row["total"] is not None:
            vanna_weekly = float(row["total"])

        # Vanna THIRTY_NEXT_DAYS
        vanna_monthly = None
        row = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if row and row["total"] is not None:
            vanna_monthly = float(row["total"])

        # SVB + charm from volland_snapshots
        svb_correlation = None
        agg_charm = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics'->'spot_vol_beta' IS NOT NULL
              AND ts <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": trade_ts}).mappings().first()
        if snap:
            payload = snap["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                stats = payload.get("statistics", {})
                svb = stats.get("spot_vol_beta", {})
                if isinstance(svb, dict) and svb.get("correlation") is not None:
                    try:
                        svb_correlation = float(svb["correlation"])
                    except (ValueError, TypeError):
                        pass
                charm_val = stats.get("aggregatedCharm")
                if charm_val is not None:
                    try:
                        agg_charm = float(charm_val)
                    except (ValueError, TypeError):
                        pass

        # Greek alignment
        direction = t["direction"]
        is_long = direction in ("long", "bullish")
        alignment = 0
        if agg_charm is not None:
            alignment += 1 if (agg_charm > 0) == is_long else -1
        if vanna_all is not None:
            alignment += 1 if (vanna_all > 0) == is_long else -1
        spot = t["spot"]
        max_plus_gex = t["max_plus_gex"]
        if spot and max_plus_gex:
            gex_bullish = spot <= max_plus_gex
            alignment += 1 if gex_bullish == is_long else -1

        # Vanna divergence
        vanna_divergent = None
        if vanna_weekly is not None and vanna_monthly is not None:
            vanna_divergent = (vanna_weekly > 0) != (vanna_monthly > 0)

        # Charm aligned
        charm_aligned = None
        if agg_charm is not None:
            charm_aligned = (agg_charm > 0) == is_long

        enriched.append({
            **dict(t),
            "vanna_all": vanna_all,
            "vanna_weekly": vanna_weekly,
            "vanna_monthly": vanna_monthly,
            "svb_correlation": svb_correlation,
            "agg_charm": agg_charm,
            "greek_alignment": alignment,
            "vanna_divergent": vanna_divergent,
            "charm_aligned": charm_aligned,
        })

print(f"Enriched {len(enriched)} trades\n")


# ============================================================
# METRICS ENGINE
# ============================================================

def compute_metrics(trades_list, label=""):
    """Full suite of risk/return metrics."""
    if not trades_list:
        return None

    # Basic
    n = len(trades_list)
    wins = [t for t in trades_list if t["outcome_result"] == "WIN"]
    losses = [t for t in trades_list if t["outcome_result"] == "LOSS"]
    pnl = sum(t["outcome_pnl"] or 0 for t in trades_list)
    wr = len(wins) / n * 100

    # Daily PnL
    daily_pnl = defaultdict(float)
    daily_count = defaultdict(int)
    for t in trades_list:
        d = t["trade_date"]
        daily_pnl[d] += (t["outcome_pnl"] or 0)
        daily_count[d] += 1

    trading_days = len(daily_pnl)
    avg_daily = pnl / trading_days if trading_days else 0

    # Daily P&L list for drawdown / Sharpe
    daily_vals = [daily_pnl[d] for d in sorted(daily_pnl.keys())]

    # Max drawdown (cumulative)
    cum = 0
    peak = 0
    max_dd = 0
    for d in daily_vals:
        cum += d
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)

    # Profit factor
    gross_wins = sum(t["outcome_pnl"] for t in wins if t["outcome_pnl"])
    gross_losses = abs(sum(t["outcome_pnl"] for t in losses if t["outcome_pnl"]))
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')

    # Sharpe-like (daily PnL mean / std)
    import statistics as stat_mod
    if len(daily_vals) > 1:
        mean_d = stat_mod.mean(daily_vals)
        std_d = stat_mod.stdev(daily_vals)
        sharpe = mean_d / std_d if std_d > 0 else 0
    else:
        sharpe = 0

    # Win/loss streaks
    max_win_streak = 0
    max_loss_streak = 0
    cur_streak = 0
    cur_type = None
    for t in sorted(trades_list, key=lambda x: x["id"]):
        r = t["outcome_result"]
        if r == cur_type:
            cur_streak += 1
        else:
            cur_streak = 1
            cur_type = r
        if r == "WIN":
            max_win_streak = max(max_win_streak, cur_streak)
        elif r == "LOSS":
            max_loss_streak = max(max_loss_streak, cur_streak)

    # Average win / average loss
    avg_win = gross_wins / len(wins) if wins else 0
    avg_loss = gross_losses / len(losses) if losses else 0

    # Monthly projection (21 trading days)
    monthly_pts = avg_daily * 21
    # 4 ES contracts at $50/pt
    monthly_usd_4es = monthly_pts * 50 * 4
    # 10 MES at $5/pt
    monthly_usd_10mes = monthly_pts * 5 * 10

    # Trades per day
    trades_per_day = n / trading_days if trading_days else 0

    # Worst day
    worst_day = min(daily_vals) if daily_vals else 0
    best_day = max(daily_vals) if daily_vals else 0

    # % of winning days
    winning_days = sum(1 for d in daily_vals if d > 0)
    pct_winning_days = winning_days / len(daily_vals) * 100 if daily_vals else 0

    return {
        "label": label,
        "n": n, "wins": len(wins), "losses": len(losses), "wr": wr,
        "total_pnl": pnl, "avg_daily": avg_daily, "trading_days": trading_days,
        "trades_per_day": trades_per_day,
        "pf": pf, "max_dd": max_dd, "sharpe": sharpe,
        "avg_win": avg_win, "avg_loss": avg_loss,
        "max_win_streak": max_win_streak, "max_loss_streak": max_loss_streak,
        "monthly_pts": monthly_pts,
        "monthly_usd_4es": monthly_usd_4es,
        "monthly_usd_10mes": monthly_usd_10mes,
        "worst_day": worst_day, "best_day": best_day,
        "pct_winning_days": pct_winning_days,
    }


def print_comparison(baseline, filtered, blocked_trades):
    """Print side-by-side comparison."""
    b, f = baseline, filtered
    blocked = len(blocked_trades)
    blocked_pnl = sum(t["outcome_pnl"] or 0 for t in blocked_trades)
    blocked_wins = sum(1 for t in blocked_trades if t["outcome_result"] == "WIN")
    blocked_losses = sum(1 for t in blocked_trades if t["outcome_result"] == "LOSS")

    print(f"\n  {'Metric':<28} {'Baseline':>12} {'Filtered':>12} {'Change':>12}")
    print(f"  {'-'*28} {'-'*12} {'-'*12} {'-'*12}")
    print(f"  {'Trades':<28} {b['n']:>12}   {f['n']:>12}   {f['n']-b['n']:>+12}")
    print(f"  {'Win Rate':<28} {b['wr']:>11.1f}%  {f['wr']:>11.1f}%  {f['wr']-b['wr']:>+11.1f}%")
    print(f"  {'Total PnL (pts)':<28} {b['total_pnl']:>+11.1f}   {f['total_pnl']:>+11.1f}   {f['total_pnl']-b['total_pnl']:>+11.1f}")
    print(f"  {'Avg Daily PnL (pts)':<28} {b['avg_daily']:>+11.1f}   {f['avg_daily']:>+11.1f}   {f['avg_daily']-b['avg_daily']:>+11.1f}")
    print(f"  {'Trades/Day':<28} {b['trades_per_day']:>11.1f}   {f['trades_per_day']:>11.1f}   {f['trades_per_day']-b['trades_per_day']:>+11.1f}")
    print(f"  {'Profit Factor':<28} {b['pf']:>11.2f}   {f['pf']:>11.2f}   {f['pf']-b['pf']:>+11.2f}")
    print(f"  {'Max Drawdown (pts)':<28} {b['max_dd']:>11.1f}   {f['max_dd']:>11.1f}   {b['max_dd']-f['max_dd']:>+11.1f}")
    print(f"  {'Sharpe (daily)':<28} {b['sharpe']:>11.3f}   {f['sharpe']:>11.3f}   {f['sharpe']-b['sharpe']:>+11.3f}")
    print(f"  {'Avg Win (pts)':<28} {b['avg_win']:>+11.1f}   {f['avg_win']:>+11.1f}   {f['avg_win']-b['avg_win']:>+11.1f}")
    print(f"  {'Avg Loss (pts)':<28} {b['avg_loss']:>11.1f}   {f['avg_loss']:>11.1f}   {b['avg_loss']-f['avg_loss']:>+11.1f}")
    print(f"  {'Max Loss Streak':<28} {b['max_loss_streak']:>12}   {f['max_loss_streak']:>12}   {f['max_loss_streak']-b['max_loss_streak']:>+12}")
    print(f"  {'Worst Day (pts)':<28} {b['worst_day']:>+11.1f}   {f['worst_day']:>+11.1f}   {f['worst_day']-b['worst_day']:>+11.1f}")
    print(f"  {'Best Day (pts)':<28} {b['best_day']:>+11.1f}   {f['best_day']:>+11.1f}   {f['best_day']-b['best_day']:>+11.1f}")
    print(f"  {'% Winning Days':<28} {b['pct_winning_days']:>11.1f}%  {f['pct_winning_days']:>11.1f}%  {f['pct_winning_days']-b['pct_winning_days']:>+11.1f}%")
    print(f"  {'Monthly PnL (pts)':<28} {b['monthly_pts']:>+11.1f}   {f['monthly_pts']:>+11.1f}   {f['monthly_pts']-b['monthly_pts']:>+11.1f}")
    print(f"  {'Monthly $ (4 ES)':<28} ${b['monthly_usd_4es']:>+10,.0f}   ${f['monthly_usd_4es']:>+10,.0f}   ${f['monthly_usd_4es']-b['monthly_usd_4es']:>+10,.0f}")
    print(f"  {'Monthly $ (10 MES)':<28} ${b['monthly_usd_10mes']:>+10,.0f}   ${f['monthly_usd_10mes']:>+10,.0f}   ${f['monthly_usd_10mes']-b['monthly_usd_10mes']:>+10,.0f}")
    print(f"\n  Blocked: {blocked} trades ({blocked_wins}W/{blocked_losses}L, {blocked_pnl:+.1f} pts)")


# ============================================================
# BASELINE
# ============================================================
baseline = compute_metrics(enriched, "Baseline (no filter)")
print(f"\n{'='*75}")
print(f" BASELINE — ALL TRADES (no Greek filters)")
print(f"{'='*75}")
print(f"  Trades: {baseline['n']} | WR: {baseline['wr']:.1f}% | PnL: {baseline['total_pnl']:+.1f} pts")
print(f"  Daily: {baseline['avg_daily']:+.1f} pts/day over {baseline['trading_days']} days")
print(f"  PF: {baseline['pf']:.2f} | Max DD: {baseline['max_dd']:.1f} | Sharpe: {baseline['sharpe']:.3f}")
print(f"  Monthly: {baseline['monthly_pts']:+.1f} pts → ${baseline['monthly_usd_4es']:+,.0f} (4 ES) / ${baseline['monthly_usd_10mes']:+,.0f} (10 MES)")


# ============================================================
# FILTER DEFINITIONS
# ============================================================

def filter_alignment_pos(t):
    """Block trades with alignment <= 0."""
    return t["greek_alignment"] >= 1

def filter_alignment_pos_or_na(t):
    """Block trades with alignment <= 0 (pass if data missing)."""
    if t["agg_charm"] is None and t["vanna_all"] is None:
        return True  # no data = pass
    return t["greek_alignment"] >= 1

def filter_charm_aligned(t):
    """Block trades where charm opposes direction."""
    if t["charm_aligned"] is None:
        return True  # no data = pass
    return t["charm_aligned"]

def filter_charm_aligned_strict(t):
    """Block trades where charm opposes direction (block N/A too)."""
    return t["charm_aligned"] == True

def filter_vanna_divergent(t):
    """Only take trades when weekly/monthly vanna diverge."""
    if t["vanna_divergent"] is None:
        return True  # no data = pass
    return t["vanna_divergent"]

def filter_alignment_ge_plus2(t):
    """Only take trades with alignment >= +2."""
    return t["greek_alignment"] >= 2

def filter_svb_not_weak_neg(t):
    """Block weak negative SVB (-0.5 to 0)."""
    if t["svb_correlation"] is None:
        return True
    return not (-0.5 <= t["svb_correlation"] < 0)

# Setup-specific filters
def filter_gex_alignment(t):
    """GEX Long: block at alignment <= 0."""
    if t["setup_name"] != "GEX Long":
        return True
    return t["greek_alignment"] >= 1

def filter_dd_svb(t):
    """DD Exhaustion: block when SVB weak negative."""
    if t["setup_name"] != "DD Exhaustion":
        return True
    if t["svb_correlation"] is None:
        return True
    return not (-0.5 <= t["svb_correlation"] < 0)

def filter_ag_alignment(t):
    """AG Short: block at alignment -3 (total misalignment)."""
    if t["setup_name"] != "AG Short":
        return True
    return t["greek_alignment"] != -3


# ============================================================
# COMBINED FILTER TESTS
# ============================================================

filters_to_test = [
    ("F1: Alignment >= +1", filter_alignment_pos),
    ("F2: Charm aligned (pass N/A)", filter_charm_aligned),
    ("F3: Vanna weekly/monthly divergent", filter_vanna_divergent),
    ("F4: Alignment >= +2", filter_alignment_ge_plus2),
    ("F5: SVB not weak-neg", filter_svb_not_weak_neg),
    ("F6: GEX alignment >= +1", filter_gex_alignment),
    ("F7: DD SVB not weak-neg", filter_dd_svb),
    ("F8: AG Short alignment != -3", filter_ag_alignment),
]

print(f"\n\n{'='*75}")
print(f" INDIVIDUAL FILTER TESTS")
print(f"{'='*75}")

for name, fn in filters_to_test:
    passed = [t for t in enriched if fn(t)]
    blocked = [t for t in enriched if not fn(t)]
    m = compute_metrics(passed, name)
    if m:
        blocked_pnl = sum(t["outcome_pnl"] or 0 for t in blocked)
        blocked_wr = sum(1 for t in blocked if t["outcome_result"] == "WIN") / len(blocked) * 100 if blocked else 0
        print(f"\n  {name}")
        print(f"    Pass: {m['n']} trades | WR: {m['wr']:.1f}% | PnL: {m['total_pnl']:+.1f} | PF: {m['pf']:.2f} | Daily: {m['avg_daily']:+.1f}")
        print(f"    Block: {len(blocked)} trades | WR: {blocked_wr:.1f}% | PnL: {blocked_pnl:+.1f}")
        print(f"    PnL delta: {m['total_pnl'] - baseline['total_pnl']:+.1f} | WR delta: {m['wr'] - baseline['wr']:+.1f}%")


# ============================================================
# COMBINED FILTERS — Progressive stacking
# ============================================================

print(f"\n\n{'='*75}")
print(f" COMBINED FILTER STACKS")
print(f"{'='*75}")

# Stack A: Charm + Setup-specific
stack_a_filters = [
    ("Charm aligned", filter_charm_aligned),
    ("+ GEX alignment >= +1", filter_gex_alignment),
    ("+ AG Short alignment != -3", filter_ag_alignment),
    ("+ DD SVB not weak-neg", filter_dd_svb),
]

print(f"\n  STACK A: Charm + Setup-Specific Filters")
print(f"  {'-'*65}")
current = enriched[:]
for name, fn in stack_a_filters:
    current = [t for t in current if fn(t)]
    blocked = [t for t in enriched if t not in current]
    m = compute_metrics(current, name)
    blocked_pnl = sum(t["outcome_pnl"] or 0 for t in blocked)
    print(f"    {name:<35} N={m['n']:>3} WR={m['wr']:>5.1f}% PnL={m['total_pnl']:>+7.1f} "
          f"PF={m['pf']:>5.2f} Daily={m['avg_daily']:>+5.1f} Blocked={len(blocked):>3} ({blocked_pnl:>+7.1f})")

# Stack B: Alignment-based
stack_b_filters = [
    ("Alignment >= +1", filter_alignment_pos),
    ("+ SVB not weak-neg", filter_svb_not_weak_neg),
]

print(f"\n  STACK B: Alignment + SVB")
print(f"  {'-'*65}")
current = enriched[:]
for name, fn in stack_b_filters:
    current = [t for t in current if fn(t)]
    blocked = [t for t in enriched if t not in current]
    m = compute_metrics(current, name)
    blocked_pnl = sum(t["outcome_pnl"] or 0 for t in blocked)
    print(f"    {name:<35} N={m['n']:>3} WR={m['wr']:>5.1f}% PnL={m['total_pnl']:>+7.1f} "
          f"PF={m['pf']:>5.2f} Daily={m['avg_daily']:>+5.1f} Blocked={len(blocked):>3} ({blocked_pnl:>+7.1f})")

# Stack C: Vanna divergence + charm
stack_c_filters = [
    ("Vanna divergent", filter_vanna_divergent),
    ("+ Charm aligned", filter_charm_aligned),
]

print(f"\n  STACK C: Vanna Divergence + Charm")
print(f"  {'-'*65}")
current = enriched[:]
for name, fn in stack_c_filters:
    current = [t for t in current if fn(t)]
    blocked = [t for t in enriched if t not in current]
    m = compute_metrics(current, name)
    blocked_pnl = sum(t["outcome_pnl"] or 0 for t in blocked)
    print(f"    {name:<35} N={m['n']:>3} WR={m['wr']:>5.1f}% PnL={m['total_pnl']:>+7.1f} "
          f"PF={m['pf']:>5.2f} Daily={m['avg_daily']:>+5.1f} Blocked={len(blocked):>3} ({blocked_pnl:>+7.1f})")


# ============================================================
# OPTIMAL COMBINED FILTER (best practical combination)
# ============================================================

print(f"\n\n{'='*75}")
print(f" OPTIMAL FILTER — Full Comparison")
print(f"{'='*75}")

# The optimal filter: charm aligned + setup-specific guards
def optimal_filter(t):
    # Charm must be aligned with trade direction (pass if unknown)
    if t["charm_aligned"] is not None and not t["charm_aligned"]:
        return False
    # GEX Long: need alignment >= +1 (vanna must be positive)
    if t["setup_name"] == "GEX Long" and t["greek_alignment"] < 1:
        return False
    # AG Short: block total misalignment (-3)
    if t["setup_name"] == "AG Short" and t["greek_alignment"] == -3:
        return False
    # DD Exhaustion: block weak negative SVB
    if t["setup_name"] == "DD Exhaustion":
        if t["svb_correlation"] is not None and -0.5 <= t["svb_correlation"] < 0:
            return False
    return True

passed = [t for t in enriched if optimal_filter(t)]
blocked = [t for t in enriched if not optimal_filter(t)]
m_opt = compute_metrics(passed, "Optimal")

print_comparison(baseline, m_opt, blocked)

# Per-setup breakdown
print(f"\n\n  PER-SETUP BREAKDOWN (Optimal Filter)")
print(f"  {'Setup':<20} {'Baseline':>30} {'Filtered':>30} {'Blocked':>15}")
print(f"  {'':.<20} {'N   WR%    PnL    PF':>30} {'N   WR%    PnL    PF':>30} {'N    PnL':>15}")
print(f"  {'-'*20} {'-'*30} {'-'*30} {'-'*15}")

for sn in sorted(set(t["setup_name"] for t in enriched)):
    base_s = [t for t in enriched if t["setup_name"] == sn]
    filt_s = [t for t in passed if t["setup_name"] == sn]
    blk_s = [t for t in blocked if t["setup_name"] == sn]
    bm = compute_metrics(base_s)
    fm = compute_metrics(filt_s)
    blk_pnl = sum(t["outcome_pnl"] or 0 for t in blk_s)
    if bm and fm:
        print(f"  {sn:<20} {bm['n']:>3} {bm['wr']:>5.1f}% {bm['total_pnl']:>+7.1f} {bm['pf']:>5.2f}"
              f"   {fm['n']:>3} {fm['wr']:>5.1f}% {fm['total_pnl']:>+7.1f} {fm['pf']:>5.2f}"
              f"   {len(blk_s):>3} {blk_pnl:>+7.1f}")
    elif bm:
        print(f"  {sn:<20} {bm['n']:>3} {bm['wr']:>5.1f}% {bm['total_pnl']:>+7.1f} {bm['pf']:>5.2f}"
              f"   {'ALL BLOCKED':>30}"
              f"   {len(blk_s):>3} {blk_pnl:>+7.1f}")


# ============================================================
# AGGRESSIVE FILTER (alignment >= +1 as universal gate)
# ============================================================

print(f"\n\n{'='*75}")
print(f" AGGRESSIVE FILTER — Alignment >= +1 (Universal Gate)")
print(f"{'='*75}")

passed_agg = [t for t in enriched if t["greek_alignment"] >= 1]
blocked_agg = [t for t in enriched if t["greek_alignment"] < 1]
m_agg = compute_metrics(passed_agg, "Aggressive")

print_comparison(baseline, m_agg, blocked_agg)

# Per-setup breakdown
print(f"\n\n  PER-SETUP BREAKDOWN (Aggressive Filter)")
print(f"  {'Setup':<20} {'Baseline':>30} {'Filtered':>30} {'Blocked':>15}")
print(f"  {'':.<20} {'N   WR%    PnL    PF':>30} {'N   WR%    PnL    PF':>30} {'N    PnL':>15}")
print(f"  {'-'*20} {'-'*30} {'-'*30} {'-'*15}")

for sn in sorted(set(t["setup_name"] for t in enriched)):
    base_s = [t for t in enriched if t["setup_name"] == sn]
    filt_s = [t for t in passed_agg if t["setup_name"] == sn]
    blk_s = [t for t in blocked_agg if t["setup_name"] == sn]
    bm = compute_metrics(base_s)
    fm = compute_metrics(filt_s) if filt_s else None
    blk_pnl = sum(t["outcome_pnl"] or 0 for t in blk_s)
    if bm and fm:
        print(f"  {sn:<20} {bm['n']:>3} {bm['wr']:>5.1f}% {bm['total_pnl']:>+7.1f} {bm['pf']:>5.2f}"
              f"   {fm['n']:>3} {fm['wr']:>5.1f}% {fm['total_pnl']:>+7.1f} {fm['pf']:>5.2f}"
              f"   {len(blk_s):>3} {blk_pnl:>+7.1f}")
    elif bm:
        print(f"  {sn:<20} {bm['n']:>3} {bm['wr']:>5.1f}% {bm['total_pnl']:>+7.1f} {bm['pf']:>5.2f}"
              f"   {'ALL BLOCKED':>30}"
              f"   {len(blk_s):>3} {blk_pnl:>+7.1f}")


# ============================================================
# DAILY EQUITY CURVES — Baseline vs Optimal vs Aggressive
# ============================================================

print(f"\n\n{'='*75}")
print(f" DAILY EQUITY CURVE COMPARISON")
print(f"{'='*75}")

def daily_equity(trades_list):
    daily = defaultdict(float)
    for t in trades_list:
        daily[t["trade_date"]] += (t["outcome_pnl"] or 0)
    return dict(sorted(daily.items()))

eq_base = daily_equity(enriched)
eq_opt = daily_equity(passed)
eq_agg = daily_equity(passed_agg)

all_dates = sorted(set(list(eq_base.keys()) + list(eq_opt.keys()) + list(eq_agg.keys())))

print(f"\n  {'Date':<12} {'Base PnL':>9} {'Base Cum':>9} {'Opt PnL':>9} {'Opt Cum':>9} {'Agg PnL':>9} {'Agg Cum':>9}")
print(f"  {'-'*12} {'-'*9} {'-'*9} {'-'*9} {'-'*9} {'-'*9} {'-'*9}")

cum_b, cum_o, cum_a = 0, 0, 0
for d in all_dates:
    b = eq_base.get(d, 0)
    o = eq_opt.get(d, 0)
    a = eq_agg.get(d, 0)
    cum_b += b
    cum_o += o
    cum_a += a
    print(f"  {str(d):<12} {b:>+8.1f}  {cum_b:>+8.1f}  {o:>+8.1f}  {cum_o:>+8.1f}  {a:>+8.1f}  {cum_a:>+8.1f}")


# ============================================================
# RISK-ADJUSTED SUMMARY
# ============================================================

print(f"\n\n{'='*75}")
print(f" FINAL RISK-ADJUSTED SUMMARY")
print(f"{'='*75}")

for label, m in [("BASELINE", baseline), ("OPTIMAL", m_opt), ("AGGRESSIVE", m_agg)]:
    print(f"\n  {label}:")
    print(f"    Trades: {m['n']} ({m['trades_per_day']:.1f}/day) | WR: {m['wr']:.1f}% | W/L: {m['wins']}/{m['losses']}")
    print(f"    Total PnL: {m['total_pnl']:+.1f} pts over {m['trading_days']} days")
    print(f"    Daily: {m['avg_daily']:+.1f} pts | Worst day: {m['worst_day']:+.1f} | Best day: {m['best_day']:+.1f}")
    print(f"    Profit Factor: {m['pf']:.2f} | Sharpe: {m['sharpe']:.3f}")
    print(f"    Max Drawdown: {m['max_dd']:.1f} pts | DD Recovery ratio: {m['total_pnl']/m['max_dd']:.1f}x" if m['max_dd'] > 0 else f"    Max Drawdown: 0")
    print(f"    % Winning Days: {m['pct_winning_days']:.0f}% | Max loss streak: {m['max_loss_streak']}")
    print(f"    Avg Win: {m['avg_win']:+.1f} pts | Avg Loss: {m['avg_loss']:.1f} pts | Edge: {m['avg_win']-m['avg_loss']:+.1f}")
    print(f"    Monthly projection:")
    print(f"      21 days × {m['avg_daily']:+.1f} = {m['monthly_pts']:+.1f} pts")
    print(f"      4 ES  ($50/pt): ${m['monthly_usd_4es']:+,.0f}/mo → ${m['monthly_usd_4es']*12:+,.0f}/yr")
    print(f"      10 MES ($5/pt): ${m['monthly_usd_10mes']:+,.0f}/mo → ${m['monthly_usd_10mes']*12:+,.0f}/yr")
    print(f"      E2T 50K (risk-adjusted): ~${m['monthly_usd_10mes']*0.8:+,.0f}/mo (80% of MES, compliance drag)")
