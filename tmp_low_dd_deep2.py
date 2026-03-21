"""Deep low-DD study part 2: per-strike charm, vanna, sub-scores, combined factors."""
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
    # =========================================================
    # PART 4: Per-strike charm near spot at signal time
    # =========================================================
    print("=" * 120)
    print("PART 4: Per-strike CHARM near spot at signal time")
    print("Net charm within 10 pts of spot: positive = call charm dominates, negative = put charm dominates")
    print("=" * 120)

    # Get all SC trades with outcomes
    r = c.execute(text("""
        SELECT s.id, s.ts, s.setup_name, s.direction, s.spot,
               s.outcome_max_loss, s.outcome_pnl, s.outcome_result,
               s.greek_alignment, s.paradigm, s.vix
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL AND s.outcome_max_loss IS NOT NULL
          AND s.setup_name IN ('Skew Charm', 'DD Exhaustion', 'AG Short')
        ORDER BY s.ts
    """))
    all_trades = [dict(zip(r.keys(), row)) for row in r.fetchall()]

    # For each trade, get per-strike charm near spot
    enriched = []
    for t in all_trades:
        r = c.execute(text("""
            SELECT strike, value FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc BETWEEN :ts - interval '5 minutes' AND :ts + interval '5 minutes'
              AND ABS(strike - :spot) <= 15
            ORDER BY ABS(strike - :spot)
            LIMIT 20
        """), {"ts": t['ts'], "spot": t['spot']})
        charm_rows = r.fetchall()
        if not charm_rows:
            t['charm_near_spot'] = None
            t['charm_above'] = None
            t['charm_below'] = None
        else:
            # Net charm within 10 pts
            net = sum(float(row[1]) for row in charm_rows)
            above = sum(float(row[1]) for row in charm_rows if float(row[0]) > t['spot'])
            below = sum(float(row[1]) for row in charm_rows if float(row[0]) <= t['spot'])
            t['charm_near_spot'] = net
            t['charm_above'] = above
            t['charm_below'] = below
        enriched.append(t)

    # Analyze per-strike charm for different DD buckets
    has_charm = [t for t in enriched if t['charm_near_spot'] is not None]
    print(f"Trades with per-strike charm data: {len(has_charm)} / {len(enriched)}")

    for setup in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
        subset = [t for t in has_charm if t['setup_name'] == setup]
        if len(subset) < 5: continue

        golden = [t for t in subset if t['outcome_max_loss'] >= -1.0]
        low_dd = [t for t in subset if -2.0 <= t['outcome_max_loss'] < -1.0]
        high_dd = [t for t in subset if t['outcome_max_loss'] < -2.0]

        print(f"\n{setup}:")
        for label, grp in [("GOLDEN(>=1)", golden), ("LOW_DD(-2..-1)", low_dd), ("HIGH_DD(<-2)", high_dd)]:
            if not grp: continue
            # Direction-aligned charm
            aligned_charm = []
            for t in grp:
                dir_sign = 1 if t['direction'] in ('long', 'bullish') else -1
                aligned_charm.append(t['charm_near_spot'] * dir_sign)

            net_vals = [t['charm_near_spot'] for t in grp]
            above_vals = [t['charm_above'] for t in grp]
            below_vals = [t['charm_below'] for t in grp]
            wins = sum(1 for t in grp if t['outcome_result'] == 'WIN')
            print(f"  {label:<16} n={len(grp):>3} | WR:{100*wins/len(grp):>5.1f}% | "
                  f"net_charm:{avg(net_vals):>12.0f} | charm_above:{avg(above_vals):>12.0f} | "
                  f"charm_below:{avg(below_vals):>12.0f} | aligned_charm:{avg(aligned_charm):>12.0f}")

    # =========================================================
    # PART 5: VANNA patterns for low-DD trades
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 5: VANNA patterns — direction-aligned")
    print("(positive vanna_all = bullish vanna exposure)")
    print("=" * 120)

    r = c.execute(text("""
        SELECT setup_name, direction,
            CASE WHEN outcome_max_loss >= -1.0 THEN 'GOLDEN'
                 WHEN outcome_max_loss >= -2.0 THEN 'LOW_DD'
                 ELSE 'HIGH_DD' END as dd_cat,
            count(*) as cnt,
            round(avg(vanna_all)::numeric, 0) as avg_vanna_all,
            round(avg(vanna_weekly)::numeric, 0) as avg_vanna_wk,
            round(avg(vanna_monthly)::numeric, 0) as avg_vanna_mo,
            round(100.0 * sum(case when outcome_result = 'WIN' then 1 else 0 end) / count(*)::numeric, 1) as wr,
            round(avg(outcome_pnl)::numeric, 2) as avg_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
          AND vanna_all IS NOT NULL
          AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'AG Short')
        GROUP BY 1, 2, 3
        HAVING count(*) >= 3
        ORDER BY 1, 2, 3
    """))
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        print(f"  {d['setup_name']:<16} {d['direction']:<6} {d['dd_cat']:<8} n={d['cnt']:>3} | "
              f"vanna_all:{d['avg_vanna_all']:>14} | vanna_wk:{d['avg_vanna_wk']:>12} | vanna_mo:{d['avg_vanna_mo']:>14} | "
              f"WR:{d['wr']:>5}% | PnL:{d['avg_pnl']:>6}")

    # =========================================================
    # PART 6: Sub-scores as DD predictors (for SC specifically)
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 6: Setup detector SUB-SCORES for SC trades")
    print("=" * 120)

    r = c.execute(text("""
        SELECT
            CASE WHEN outcome_max_loss >= -1.0 THEN 'GOLDEN'
                 WHEN outcome_max_loss >= -2.0 THEN 'LOW_DD'
                 ELSE 'HIGH_DD' END as dd_cat,
            count(*) as cnt,
            round(avg(support_score)::numeric, 1) as avg_sup,
            round(avg(upside_score)::numeric, 1) as avg_up,
            round(avg(floor_cluster_score)::numeric, 1) as avg_floor,
            round(avg(target_cluster_score)::numeric, 1) as avg_tgt,
            round(avg(rr_score)::numeric, 1) as avg_rr,
            round(avg(score)::numeric, 1) as avg_total,
            round(avg(rr_ratio)::numeric, 2) as avg_rr_ratio,
            round(avg(upside)::numeric, 1) as avg_upside
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
          AND setup_name = 'Skew Charm'
        GROUP BY 1 ORDER BY 1
    """))
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        print(f"  {d['dd_cat']:<8} n={d['cnt']:>3} | sup:{d['avg_sup']:>5} | up:{d['avg_up']:>5} | "
              f"floor:{d['avg_floor']:>5} | tgt:{d['avg_tgt']:>5} | rr:{d['avg_rr']:>5} | "
              f"total:{d['avg_total']:>5} | rr_ratio:{d['avg_rr_ratio']:>5} | upside:{d['avg_upside']:>6}")

    # =========================================================
    # PART 7: SC + DD_ALIGNED: the combined killer filter
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 7: COMBINED FACTORS for SC — DD aligned + paradigm + charm")
    print("=" * 120)

    r = c.execute(text("""
        WITH tv AS (
            SELECT s.id, s.ts, s.setup_name, s.direction, s.spot, s.paradigm,
                   s.greek_alignment, s.vix,
                   s.outcome_result, s.outcome_pnl, s.outcome_max_loss, s.outcome_max_profit,
                   v.payload->'statistics'->>'delta_decay_hedging' as dd_hedging,
                   v.payload->'statistics'->>'aggregatedCharm' as agg_charm,
                   ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY ABS(EXTRACT(EPOCH FROM (v.ts - s.ts)))) as rn
            FROM setup_log s
            LEFT JOIN volland_snapshots v ON v.ts BETWEEN s.ts - interval '5 minutes' AND s.ts + interval '5 minutes'
            WHERE s.outcome_result IS NOT NULL AND s.outcome_max_loss IS NOT NULL
              AND s.setup_name = 'Skew Charm'
        )
        SELECT * FROM tv WHERE rn = 1
    """))
    sc_trades = []
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        d['dd_num'] = parse_money(d['dd_hedging'])
        d['charm_num'] = float(d['agg_charm']) if d['agg_charm'] and d['agg_charm'] not in ('null','') else None
        sc_trades.append(d)

    # DD direction alignment for SC
    def dd_aligned(t):
        if t['dd_num'] is None: return 'no_data'
        dir_sign = 1 if t['direction'] in ('long', 'bullish') else -1
        val = t['dd_num'] * dir_sign
        if val > 200_000_000: return 'DD_ALIGNED'
        elif val < -200_000_000: return 'DD_OPPOSED'
        else: return 'DD_NEUTRAL'

    # Test combinations
    combos = {
        "SC_ALL": lambda t: True,
        "SC_DD_ALIGNED": lambda t: dd_aligned(t) == 'DD_ALIGNED',
        "SC_DD_OPPOSED": lambda t: dd_aligned(t) == 'DD_OPPOSED',
        "SC_DD_AL+no_toxic_para": lambda t: dd_aligned(t) == 'DD_ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS'),
        "SC_DD_AL+SIDIAL/GEX": lambda t: dd_aligned(t) == 'DD_ALIGNED' and t['paradigm'] in ('SIDIAL-MESSY', 'GEX-PURE', 'AG-TARGET', 'SIDIAL-EXTREME'),
        "SC_DD_AL+VIX20-26": lambda t: dd_aligned(t) == 'DD_ALIGNED' and t['vix'] and 20 <= t['vix'] <= 26,
        "SC_DD_OPP+no_toxic": lambda t: dd_aligned(t) == 'DD_OPPOSED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS'),
        "SC_no_toxic+VIX20-26": lambda t: t['paradigm'] not in ('GEX-LIS', 'AG-LIS') and t['vix'] and 20 <= t['vix'] <= 26,
    }

    print(f"{'Filter':<30} | {'n':>3} | {'0DD%':>6} | {'lowDD%':>6} | {'WR':>6} | {'tot_PnL':>8} | {'avg_PnL':>7} | {'PF':>5} | {'avgMAE':>7}")
    print("-" * 110)
    for name, fn in combos.items():
        grp = [t for t in sc_trades if fn(t)]
        if len(grp) < 3: continue
        wins = sum(1 for t in grp if t['outcome_result'] == 'WIN')
        zero = sum(1 for t in grp if t['outcome_max_loss'] >= -1.0)
        low = sum(1 for t in grp if t['outcome_max_loss'] >= -2.0)
        pnl_pos = sum(t['outcome_pnl'] for t in grp if t['outcome_pnl'] > 0)
        pnl_neg = sum(-t['outcome_pnl'] for t in grp if t['outcome_pnl'] < 0)
        pf = pnl_pos / pnl_neg if pnl_neg > 0 else 999
        tot = sum(t['outcome_pnl'] for t in grp)
        mae = avg([t['outcome_max_loss'] for t in grp])
        print(f"{name:<30} | {len(grp):>3} | {100*zero/len(grp):>5.1f}% | {100*low/len(grp):>5.1f}% | {100*wins/len(grp):>5.1f}% | {tot:>8.1f} | {tot/len(grp):>7.2f} | {pf:>5.2f} | {mae:>7.2f}")

    # =========================================================
    # PART 8: GEX levels at signal time — distance matters?
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 8: GEX LEVELS distance from spot (all setups)")
    print("=" * 120)

    r = c.execute(text("""
        SELECT
            CASE WHEN outcome_max_loss >= -1.0 THEN 'GOLDEN'
                 WHEN outcome_max_loss >= -2.0 THEN 'LOW_DD'
                 ELSE 'HIGH_DD' END as dd_cat,
            count(*) as cnt,
            round(avg(CASE WHEN max_plus_gex > 0 THEN max_plus_gex - spot ELSE NULL END)::numeric, 1) as avg_plus_gex_dist,
            round(avg(CASE WHEN max_minus_gex > 0 THEN spot - max_minus_gex ELSE NULL END)::numeric, 1) as avg_minus_gex_dist,
            round(avg(CASE WHEN target > 0 THEN target - spot ELSE NULL END)::numeric, 1) as avg_target_dist,
            round(avg(gap_to_lis)::numeric, 1) as avg_gap_lis,
            round(avg(upside)::numeric, 1) as avg_upside
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
          AND setup_name NOT IN ('ES Absorption', 'SB Absorption')
        GROUP BY 1 ORDER BY 1
    """))
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        print(f"  {d['dd_cat']:<8} n={d['cnt']:>3} | +GEX_dist:{d['avg_plus_gex_dist']:>6} | "
              f"-GEX_dist:{d['avg_minus_gex_dist']:>6} | tgt_dist:{d['avg_target_dist']:>6} | "
              f"gap_LIS:{d['avg_gap_lis']:>6} | upside:{d['avg_upside']:>6}")
