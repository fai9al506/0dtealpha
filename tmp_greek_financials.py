"""Greek Filter - Full Financial Projections at multiple contract sizes.
10 MES, 2 ES, 4 ES, 6 ES with all risk/return metrics.
"""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text
from collections import defaultdict
import statistics as stat_mod

engine = create_engine(os.environ["DATABASE_URL"])

# ---- Load & enrich (same as filter sim) ----
with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.max_plus_gex, s.paradigm,
               s.outcome_result, s.outcome_pnl, s.ts::date as trade_date
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.id
    """)).mappings().all()

enriched = []
with engine.begin() as conn:
    for t in trades:
        trade_ts = t["ts_et"]
        vanna_all = None
        r = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total FROM volland_exposure_points
            WHERE greek='vanna' AND expiration_option='ALL'
              AND ts_utc=(SELECT MAX(ts_utc) FROM volland_exposure_points
                         WHERE greek='vanna' AND expiration_option='ALL'
                           AND ts_utc<=(:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC'))
        """), {"ts": trade_ts}).mappings().first()
        if r and r["total"] is not None: vanna_all = float(r["total"])

        svb_correlation = None
        agg_charm = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics'->'spot_vol_beta' IS NOT NULL
              AND ts<=(:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": trade_ts}).mappings().first()
        if snap:
            payload = snap["payload"]
            if isinstance(payload, str): payload = json.loads(payload)
            if isinstance(payload, dict):
                stats = payload.get("statistics", {})
                svb = stats.get("spot_vol_beta", {})
                if isinstance(svb, dict) and svb.get("correlation") is not None:
                    try: svb_correlation = float(svb["correlation"])
                    except: pass
                cv = stats.get("aggregatedCharm")
                if cv is not None:
                    try: agg_charm = float(cv)
                    except: pass

        direction = t["direction"]
        is_long = direction in ("long", "bullish")
        alignment = 0
        if agg_charm is not None: alignment += 1 if (agg_charm > 0) == is_long else -1
        if vanna_all is not None: alignment += 1 if (vanna_all > 0) == is_long else -1
        spot, mpg = t["spot"], t["max_plus_gex"]
        if spot and mpg: alignment += 1 if (spot <= mpg) == is_long else -1

        charm_aligned = None
        if agg_charm is not None: charm_aligned = (agg_charm > 0) == is_long

        enriched.append({**dict(t), "vanna_all": vanna_all, "svb_correlation": svb_correlation,
                         "agg_charm": agg_charm, "greek_alignment": alignment,
                         "charm_aligned": charm_aligned})

print(f"Loaded {len(enriched)} trades\n")

# ---- Optimal filter ----
def optimal_filter(t):
    if t["charm_aligned"] is not None and not t["charm_aligned"]: return False
    if t["setup_name"] == "GEX Long" and t["greek_alignment"] < 1: return False
    if t["setup_name"] == "AG Short" and t["greek_alignment"] == -3: return False
    if t["setup_name"] == "DD Exhaustion":
        if t["svb_correlation"] is not None and -0.5 <= t["svb_correlation"] < 0: return False
    return True

baseline_trades = enriched
filtered_trades = [t for t in enriched if optimal_filter(t)]

# ---- Compute full metrics for both ----
def full_metrics(trades_list):
    n = len(trades_list)
    wins = [t for t in trades_list if t["outcome_result"] == "WIN"]
    losses = [t for t in trades_list if t["outcome_result"] == "LOSS"]
    pnl = sum(t["outcome_pnl"] or 0 for t in trades_list)
    wr = len(wins) / n * 100 if n else 0

    daily_pnl = defaultdict(float)
    for t in trades_list:
        daily_pnl[t["trade_date"]] += (t["outcome_pnl"] or 0)
    trading_days = len(daily_pnl)
    avg_daily = pnl / trading_days if trading_days else 0
    daily_vals = [daily_pnl[d] for d in sorted(daily_pnl.keys())]

    # Drawdown
    cum = 0; peak = 0; max_dd = 0
    for d in daily_vals:
        cum += d; peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    gross_wins = sum(t["outcome_pnl"] for t in wins if t["outcome_pnl"])
    gross_losses = abs(sum(t["outcome_pnl"] for t in losses if t["outcome_pnl"]))
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')

    if len(daily_vals) > 1:
        sharpe = stat_mod.mean(daily_vals) / stat_mod.stdev(daily_vals)
    else: sharpe = 0

    avg_win = gross_wins / len(wins) if wins else 0
    avg_loss = gross_losses / len(losses) if losses else 0

    worst_day = min(daily_vals) if daily_vals else 0
    best_day = max(daily_vals) if daily_vals else 0
    winning_days = sum(1 for d in daily_vals if d > 0)
    pct_winning = winning_days / len(daily_vals) * 100 if daily_vals else 0

    # Max loss streak
    max_ls = 0; cur = 0
    for t in sorted(trades_list, key=lambda x: x["id"]):
        if t["outcome_result"] == "LOSS": cur += 1; max_ls = max(max_ls, cur)
        else: cur = 0

    # Calmar ratio (annualized return / max DD)
    ann_return = avg_daily * 252
    calmar = ann_return / max_dd if max_dd > 0 else 0

    return {
        "n": n, "wins": len(wins), "losses": len(losses), "wr": wr,
        "pnl": pnl, "avg_daily": avg_daily, "trading_days": trading_days,
        "pf": pf, "max_dd": max_dd, "sharpe": sharpe,
        "avg_win": avg_win, "avg_loss": avg_loss,
        "worst_day": worst_day, "best_day": best_day,
        "pct_winning": pct_winning, "max_ls": max_ls,
        "calmar": calmar, "daily_vals": daily_vals,
    }

base = full_metrics(baseline_trades)
filt = full_metrics(filtered_trades)

# ============================================================
# CONTRACT SIZE PROJECTIONS
# ============================================================

sizes = [
    ("10 MES", 10, 5.0),    # 10 micro ES at $5/pt
    ("2 ES", 2, 50.0),      # 2 ES at $50/pt
    ("4 ES", 4, 50.0),      # 4 ES at $50/pt
    ("6 ES", 6, 50.0),      # 6 ES at $50/pt
]

# Margin requirements (day trade margin)
margins = {
    "10 MES": 10 * 50,       # ~$500 day trade margin per MES
    "2 ES": 2 * 500,         # ~$500 day trade margin per ES (intraday)
    "4 ES": 4 * 500,
    "6 ES": 6 * 500,
}

# Recommended account sizes
account_sizes = {
    "10 MES": 25_000,
    "2 ES": 50_000,
    "4 ES": 100_000,
    "6 ES": 150_000,
}

print("=" * 85)
print(" FULL FINANCIAL PROJECTIONS — GREEK OPTIMAL FILTER")
print("=" * 85)

print(f"\n  Period: {filt['trading_days']} trading days | {filt['n']} trades")
print(f"  Win Rate: {filt['wr']:.1f}% | Profit Factor: {filt['pf']:.2f} | Sharpe: {filt['sharpe']:.3f}")
print(f"  Avg Daily: {filt['avg_daily']:+.1f} pts | Max DD: {filt['max_dd']:.1f} pts")
print(f"  Avg Win: {filt['avg_win']:+.1f} pts | Avg Loss: {filt['avg_loss']:.1f} pts")

for label, qty, pt_val in sizes:
    dollar_per_pt = qty * pt_val
    acct = account_sizes[label]
    margin = margins[label]

    # Filtered metrics in dollars
    daily_usd = filt["avg_daily"] * dollar_per_pt
    monthly_usd = daily_usd * 21
    yearly_usd = daily_usd * 252
    max_dd_usd = filt["max_dd"] * dollar_per_pt
    worst_day_usd = filt["worst_day"] * dollar_per_pt
    best_day_usd = filt["best_day"] * dollar_per_pt
    total_pnl_usd = filt["pnl"] * dollar_per_pt

    # Baseline for comparison
    base_daily_usd = base["avg_daily"] * dollar_per_pt
    base_monthly_usd = base_daily_usd * 21
    base_max_dd_usd = base["max_dd"] * dollar_per_pt

    # ROI
    monthly_roi = (monthly_usd / acct) * 100
    yearly_roi = (yearly_usd / acct) * 100
    dd_pct = (max_dd_usd / acct) * 100

    # Risk of ruin (simplified Kelly)
    p = filt["wr"] / 100
    q = 1 - p
    avg_win_usd = filt["avg_win"] * dollar_per_pt
    avg_loss_usd = filt["avg_loss"] * dollar_per_pt
    b = avg_win_usd / avg_loss_usd if avg_loss_usd > 0 else 1
    kelly = (p * b - q) / b if b > 0 else 0

    # Recovery factor (total pnl / max dd)
    recovery = total_pnl_usd / max_dd_usd if max_dd_usd > 0 else float('inf')

    print(f"\n{'='*85}")
    print(f"  {label} ({qty} contracts x ${pt_val:.0f}/pt = ${dollar_per_pt:,.0f}/pt)")
    print(f"  Recommended Account: ${acct:,}")
    print(f"  Day Trade Margin: ${margin:,}")
    print(f"{'='*85}")

    print(f"\n  {'INCOME':^40}")
    print(f"  {'-'*40}")
    print(f"  {'Daily avg:':<25} ${daily_usd:>+12,.0f}")
    print(f"  {'Weekly (5 days):':<25} ${daily_usd*5:>+12,.0f}")
    print(f"  {'Monthly (21 days):':<25} ${monthly_usd:>+12,.0f}")
    print(f"  {'Quarterly:':<25} ${monthly_usd*3:>+12,.0f}")
    print(f"  {'Yearly (252 days):':<25} ${yearly_usd:>+12,.0f}")
    print(f"  {'Monthly ROI:':<25} {monthly_roi:>+11.1f}%")
    print(f"  {'Yearly ROI:':<25} {yearly_roi:>+11.1f}%")

    print(f"\n  {'RISK METRICS':^40}")
    print(f"  {'-'*40}")
    print(f"  {'Max drawdown ($):':<25} ${max_dd_usd:>12,.0f}")
    print(f"  {'Max drawdown (% acct):':<25} {dd_pct:>11.1f}%")
    print(f"  {'Worst day ($):':<25} ${worst_day_usd:>+12,.0f}")
    print(f"  {'Best day ($):':<25} ${best_day_usd:>+12,.0f}")
    print(f"  {'Recovery factor:':<25} {recovery:>11.1f}x")
    print(f"  {'Calmar ratio:':<25} {filt['calmar']:>11.2f}")
    print(f"  {'Kelly fraction:':<25} {kelly*100:>11.1f}%")
    print(f"  {'Max loss streak:':<25} {filt['max_ls']:>11}")
    print(f"  {'Max streak loss ($):':<25} ${filt['max_ls'] * avg_loss_usd:>12,.0f}")
    print(f"  {'% Winning days:':<25} {filt['pct_winning']:>11.0f}%")

    print(f"\n  {'vs BASELINE (no filter)':^40}")
    print(f"  {'-'*40}")
    print(f"  {'Baseline monthly:':<25} ${base_monthly_usd:>+12,.0f}")
    print(f"  {'Filtered monthly:':<25} ${monthly_usd:>+12,.0f}")
    print(f"  {'Monthly improvement:':<25} ${monthly_usd - base_monthly_usd:>+12,.0f}")
    print(f"  {'Baseline max DD:':<25} ${base_max_dd_usd:>12,.0f}")
    print(f"  {'Filtered max DD:':<25} ${max_dd_usd:>12,.0f}")
    print(f"  {'DD reduction:':<25} ${base_max_dd_usd - max_dd_usd:>12,.0f} ({(1-max_dd_usd/base_max_dd_usd)*100:.0f}%)")


# ============================================================
# E2T SPECIFIC PROJECTION
# ============================================================
print(f"\n\n{'='*85}")
print(f"  E2T 50K EVALUATION (10 MES, compliance-adjusted)")
print(f"{'='*85}")

e2t_daily = filt["avg_daily"] * 50  # 10 MES x $5
e2t_max_dd = filt["max_dd"] * 50
e2t_worst = filt["worst_day"] * 50

# E2T rules: $1,100 daily loss limit, $2,000 trailing drawdown
print(f"\n  Projected daily PnL:       ${e2t_daily:>+8,.0f}")
print(f"  Projected monthly PnL:     ${e2t_daily*21:>+8,.0f}")
print(f"  Historical max DD:         ${e2t_max_dd:>8,.0f} (vs $2K trailing limit)")
print(f"  Historical worst day:      ${e2t_worst:>+8,.0f} (vs $1.1K daily limit)")
print(f"  Safety margin (DD):        ${2000 - e2t_max_dd:>+8,.0f}")
print(f"  Safety margin (daily):     ${1100 + e2t_worst:>+8,.0f}")
print(f"  Days to pass eval:         ~{50000 / (e2t_daily * 21) * 21:.0f} trading days" if e2t_daily > 0 else "  N/A")
print(f"  Profit target $3K:         ~{3000 / e2t_daily:.0f} trading days" if e2t_daily > 0 else "  N/A")

# Can we safely size up?
print(f"\n  SIZING ANALYSIS:")
for n_mes in [6, 8, 10, 15, 20]:
    d = filt["avg_daily"] * n_mes * 5
    dd = filt["max_dd"] * n_mes * 5
    w = abs(filt["worst_day"]) * n_mes * 5
    safe_dd = dd < 2000
    safe_daily = w < 1100
    status = "SAFE" if (safe_dd and safe_daily) else "RISKY" if (safe_dd or safe_daily) else "DANGER"
    print(f"    {n_mes:>2} MES: daily=${d:>+6,.0f}  maxDD=${dd:>5,.0f}  worstDay=${-w:>+6,.0f}  [{status}]")


# ============================================================
# CUMULATIVE EQUITY CURVE COMPARISON (DOLLARS)
# ============================================================
print(f"\n\n{'='*85}")
print(f"  DAILY EQUITY CURVE (4 ES = $200/pt)")
print(f"{'='*85}")

daily_base = defaultdict(float)
daily_filt = defaultdict(float)
for t in baseline_trades: daily_base[t["trade_date"]] += (t["outcome_pnl"] or 0)
for t in filtered_trades: daily_filt[t["trade_date"]] += (t["outcome_pnl"] or 0)
all_dates = sorted(set(list(daily_base.keys()) + list(daily_filt.keys())))

print(f"\n  {'Date':<12} {'Base $':>10} {'Base Cum':>11} {'Filt $':>10} {'Filt Cum':>11} {'DD Base':>9} {'DD Filt':>9}")
print(f"  {'-'*12} {'-'*10} {'-'*11} {'-'*10} {'-'*11} {'-'*9} {'-'*9}")

cum_b, cum_f, peak_b, peak_f = 0, 0, 0, 0
for d in all_dates:
    b = daily_base.get(d, 0) * 200  # 4 ES
    f = daily_filt.get(d, 0) * 200
    cum_b += b; cum_f += f
    peak_b = max(peak_b, cum_b); peak_f = max(peak_f, cum_f)
    dd_b = peak_b - cum_b
    dd_f = peak_f - cum_f
    print(f"  {str(d):<12} ${b:>+9,.0f} ${cum_b:>+10,.0f} ${f:>+9,.0f} ${cum_f:>+10,.0f} ${dd_b:>8,.0f} ${dd_f:>8,.0f}")

print(f"\n  Final: Baseline ${cum_b:>+10,.0f} vs Filtered ${cum_f:>+10,.0f} = ${cum_f-cum_b:>+10,.0f} improvement")
