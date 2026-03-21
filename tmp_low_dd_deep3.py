"""Deep low-DD study part 3: Ultimate combined filters + vanna threshold."""
import sys, io, os, json, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from dotenv import load_dotenv; load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL: DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
if "postgresql://" in DB_URL and "postgresql+psycopg" not in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
from sqlalchemy import create_engine, text
engine = create_engine(DB_URL)

def parse_money(s):
    if not s: return None
    s = str(s).strip()
    neg = '-' in s
    digits = re.sub(r'[^0-9.]', '', s)
    if not digits: return None
    val = float(digits)
    return -val if neg else val

def avg(lst):
    valid = [x for x in lst if x is not None]
    return sum(valid) / len(valid) if valid else 0

with engine.connect() as c:
    # Get ALL trades with volland join
    r = c.execute(text("""
        WITH tv AS (
            SELECT s.id, s.ts, s.setup_name, s.direction, s.spot, s.paradigm,
                   s.greek_alignment, s.vix, s.score, s.grade,
                   s.outcome_result, s.outcome_pnl, s.outcome_max_loss, s.outcome_max_profit,
                   s.vanna_all, s.vanna_weekly, s.vanna_monthly, s.spot_vol_beta,
                   v.payload->'statistics'->>'delta_decay_hedging' as dd_hedging,
                   v.payload->'statistics'->>'aggregatedCharm' as agg_charm,
                   ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY ABS(EXTRACT(EPOCH FROM (v.ts - s.ts)))) as rn
            FROM setup_log s
            LEFT JOIN volland_snapshots v ON v.ts BETWEEN s.ts - interval '5 minutes' AND s.ts + interval '5 minutes'
            WHERE s.outcome_result IS NOT NULL AND s.outcome_max_loss IS NOT NULL
        )
        SELECT * FROM tv WHERE rn = 1
    """))
    all_trades = []
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        d['dd_num'] = parse_money(d['dd_hedging'])
        d['charm_num'] = float(d['agg_charm']) if d['agg_charm'] and d['agg_charm'] not in ('null','') else None
        all_trades.append(d)

    def is_dir_long(t):
        return t['direction'] in ('long', 'bullish')

    def dd_aligned(t):
        if t['dd_num'] is None: return 'no_data'
        sign = 1 if is_dir_long(t) else -1
        val = t['dd_num'] * sign
        if val > 200_000_000: return 'ALIGNED'
        elif val < -200_000_000: return 'OPPOSED'
        return 'NEUTRAL'

    def stats(grp, label=""):
        if not grp: return
        n = len(grp)
        wins = sum(1 for t in grp if t['outcome_result'] == 'WIN')
        zero = sum(1 for t in grp if t['outcome_max_loss'] >= -1.0)
        low = sum(1 for t in grp if t['outcome_max_loss'] >= -2.0)
        tot = sum(t['outcome_pnl'] for t in grp)
        pos = sum(t['outcome_pnl'] for t in grp if t['outcome_pnl'] > 0)
        neg = sum(-t['outcome_pnl'] for t in grp if t['outcome_pnl'] < 0)
        pf = pos / neg if neg > 0 else 999
        mae = avg([t['outcome_max_loss'] for t in grp])
        print(f"  {label:<35} n={n:>3} | 0DD:{zero:>3} ({100*zero/n:>5.1f}%) | lowDD:{low:>3} ({100*low/n:>5.1f}%) | WR:{100*wins/n:>5.1f}% | PnL:{tot:>8.1f} | avg:{tot/n:>6.2f} | PF:{pf:>5.2f} | MAE:{mae:>6.1f}")

    # =========================================================
    # PART 9: Vanna_all as DD predictor for each setup
    # =========================================================
    print("=" * 120)
    print("PART 9: VANNA_ALL threshold as DD predictor")
    print("=" * 120)

    for setup in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
        subset = [t for t in all_trades if t['setup_name'] == setup and t['vanna_all'] is not None]
        if len(subset) < 10: continue

        # Find optimal vanna threshold
        vannas = sorted(set(int(t['vanna_all'] / 1e9) for t in subset))
        print(f"\n{setup} — Vanna_all buckets (billions):")
        for threshold in [3, 4, 5, 6, 7]:
            high_v = [t for t in subset if t['vanna_all'] >= threshold * 1e9]
            low_v = [t for t in subset if t['vanna_all'] < threshold * 1e9]
            if len(high_v) >= 3:
                stats(high_v, f"vanna >= {threshold}B")
            if len(low_v) >= 3:
                stats(low_v, f"vanna < {threshold}B")

    # =========================================================
    # PART 10: THE ULTIMATE COMBINED FILTERS
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 10: ULTIMATE COMBINED FILTERS")
    print("=" * 120)

    sc = [t for t in all_trades if t['setup_name'] == 'Skew Charm']
    dd = [t for t in all_trades if t['setup_name'] == 'DD Exhaustion']
    ag = [t for t in all_trades if t['setup_name'] == 'AG Short']

    print("\n--- SKEW CHARM filters ---")
    stats(sc, "SC_baseline")
    stats([t for t in sc if dd_aligned(t) == 'ALIGNED'], "SC + DD_aligned")
    stats([t for t in sc if dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS','AG-LIS')], "SC + DD_al + no_toxic")
    stats([t for t in sc if dd_aligned(t) == 'ALIGNED' and t['vanna_all'] and t['vanna_all'] >= 5e9], "SC + DD_al + vanna>=5B")
    stats([t for t in sc if dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS','AG-LIS') and t['vanna_all'] and t['vanna_all'] >= 5e9], "SC + DD_al + no_toxic + vanna>=5B")
    stats([t for t in sc if t['paradigm'] not in ('GEX-LIS','AG-LIS')], "SC + no_toxic (no DD req)")
    stats([t for t in sc if t['paradigm'] not in ('GEX-LIS','AG-LIS') and t['vanna_all'] and t['vanna_all'] >= 5e9], "SC + no_toxic + vanna>=5B")

    # DD hedging aligned means: for longs DD > +$200M, for shorts DD < -$200M
    # Let's also try a STRONGER DD alignment threshold
    stats([t for t in sc if dd_aligned(t) == 'ALIGNED' and abs(t['dd_num'] or 0) >= 1e9], "SC + DD_al_strong (>$1B)")
    stats([t for t in sc if dd_aligned(t) == 'ALIGNED' and abs(t['dd_num'] or 0) >= 1e9 and t['paradigm'] not in ('GEX-LIS','AG-LIS')], "SC + DD_al_strong + no_toxic")

    print("\n--- DD EXHAUSTION filters ---")
    stats(dd, "DD_baseline")
    stats([t for t in dd if t['direction'] == 'short'], "DD_shorts")
    stats([t for t in dd if t['vanna_all'] and t['vanna_all'] >= 5e9], "DD + vanna>=5B")
    stats([t for t in dd if t['direction'] == 'short' and t['vanna_all'] and t['vanna_all'] >= 5e9], "DD_shorts + vanna>=5B")
    stats([t for t in dd if t['greek_alignment'] and t['greek_alignment'] != 0 and t['vanna_all'] and t['vanna_all'] >= 5e9], "DD align!=0 + vanna>=5B")

    print("\n--- AG SHORT filters ---")
    stats(ag, "AG_baseline")
    stats([t for t in ag if t['greek_alignment'] and t['greek_alignment'] <= -2], "AG + align<=-2")
    stats([t for t in ag if dd_aligned(t) == 'ALIGNED'], "AG + DD_aligned (charm neg)")
    stats([t for t in ag if t['vanna_all'] and t['vanna_all'] >= 5e9], "AG + vanna>=5B")
    stats([t for t in ag if t['greek_alignment'] and t['greek_alignment'] <= -2 and t['vix'] and t['vix'] >= 24], "AG + align<=-2 + VIX>=24")

    # =========================================================
    # PART 11: DAILY SIMULATION of best filter
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 11: DAILY P&L of SC + DD_aligned + no_toxic_paradigm")
    print("=" * 120)

    best = [t for t in sc if dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS','AG-LIS')]
    best.sort(key=lambda t: t['ts'])
    from collections import defaultdict
    daily = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0, 'mae_worst': 0})
    for t in best:
        day = str(t['ts'])[:10]
        daily[day]['trades'] += 1
        daily[day]['pnl'] += t['outcome_pnl']
        if t['outcome_result'] == 'WIN': daily[day]['wins'] += 1
        if t['outcome_max_loss'] < daily[day]['mae_worst']:
            daily[day]['mae_worst'] = t['outcome_max_loss']

    running = 0
    max_dd = 0
    peak = 0
    for day in sorted(daily.keys()):
        d = daily[day]
        running += d['pnl']
        peak = max(peak, running)
        dd = running - peak
        max_dd = min(max_dd, dd)
        print(f"  {day} | trades:{d['trades']:>2} | wins:{d['wins']:>2} | pnl:{d['pnl']:>6.1f} | cumul:{running:>7.1f} | peak:{peak:>7.1f} | DD:{dd:>6.1f} | worst_MAE:{d['mae_worst']:>6.1f}")

    print(f"\n  TOTAL: {sum(d['trades'] for d in daily.values())} trades | {sum(d['wins'] for d in daily.values())} wins | "
          f"+{running:.1f} pts | MaxDD: {max_dd:.1f} | "
          f"WR: {100*sum(d['wins'] for d in daily.values())/sum(d['trades'] for d in daily.values()):.1f}%")

    # =========================================================
    # PART 12: What about for ALL setups combined?
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 12: PORTFOLIO — best filter per setup, combined")
    print("=" * 120)

    # Best filters from above:
    # SC: DD_aligned + no_toxic = 85.7% WR, PF 6.44
    # DD: shorts + vanna>=5B (or align!=0)
    # AG: align<=-2 (or DD_aligned)
    portfolio = (
        [t for t in sc if dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS','AG-LIS')]
        + [t for t in dd if t['greek_alignment'] and t['greek_alignment'] != 0 and t['vanna_all'] and t['vanna_all'] >= 5e9]
        + [t for t in ag if t['greek_alignment'] and t['greek_alignment'] <= -2]
    )
    stats(portfolio, "PORTFOLIO (SC_best + DD_best + AG_best)")

    # Compare to V9-SC baseline
    v9sc = (
        list(sc)
        + list(ag)
        + [t for t in dd if t['greek_alignment'] and t['greek_alignment'] != 0]
    )
    stats(v9sc, "V9-SC baseline (current filter)")

    # Quick sim of portfolio
    portfolio.sort(key=lambda t: t['ts'])
    daily2 = defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0})
    for t in portfolio:
        day = str(t['ts'])[:10]
        daily2[day]['trades'] += 1
        daily2[day]['pnl'] += t['outcome_pnl']
        if t['outcome_result'] == 'WIN': daily2[day]['wins'] += 1

    running2 = 0; peak2 = 0; max_dd2 = 0
    print(f"\nPortfolio daily:")
    for day in sorted(daily2.keys()):
        d = daily2[day]
        running2 += d['pnl']
        peak2 = max(peak2, running2)
        dd = running2 - peak2
        max_dd2 = min(max_dd2, dd)
        print(f"  {day} | n:{d['trades']:>2} | w:{d['wins']:>2} | pnl:{d['pnl']:>6.1f} | cum:{running2:>7.1f} | DD:{dd:>6.1f}")

    n_total = sum(d['trades'] for d in daily2.values())
    n_wins = sum(d['wins'] for d in daily2.values())
    n_days = len(daily2)
    print(f"\n  PORTFOLIO TOTAL: {n_total} trades over {n_days} days | {n_wins}W | "
          f"+{running2:.1f} pts ({running2/n_days:.1f}/day) | MaxDD: {max_dd2:.1f} | "
          f"WR: {100*n_wins/n_total:.1f}%")
