"""Deep low-DD study: correlate Volland snapshot data at signal time."""
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
    """Parse '$7,298,110,681' or '-$721,000,000' into float."""
    if not s: return None
    s = str(s).strip()
    neg = '-' in s
    digits = re.sub(r'[^0-9.]', '', s)
    if not digits: return None
    val = float(digits)
    return -val if neg else val

with engine.connect() as c:
    # =========================================================
    # PART 1: Get closest volland snapshot for each setup_log trade
    # Join on nearest timestamp (within 5 min)
    # =========================================================
    print("=" * 120)
    print("PART 1: Volland state at signal time — LOW DD vs HIGH DD")
    print("=" * 120)

    r = c.execute(text("""
        WITH trade_volland AS (
            SELECT s.id, s.ts, s.setup_name, s.direction, s.spot, s.grade, s.score,
                   s.greek_alignment, s.vix, s.paradigm,
                   s.outcome_result, s.outcome_pnl, s.outcome_max_loss, s.outcome_max_profit,
                   s.vanna_all, s.vanna_weekly, s.vanna_monthly, s.spot_vol_beta,
                   s.support_score, s.upside_score, s.floor_cluster_score,
                   s.target_cluster_score, s.rr_score,
                   s.gap_to_lis, s.lis, s.target, s.max_plus_gex, s.max_minus_gex,
                   s.upside, s.rr_ratio,
                   v.payload->'statistics'->>'aggregatedCharm' as agg_charm,
                   v.payload->'statistics'->>'delta_decay_hedging' as dd_hedging,
                   v.payload->'statistics'->>'paradigm' as v_paradigm,
                   v.payload->'statistics'->>'lines_in_sand' as v_lis,
                   v.payload->'statistics'->'spot_vol_beta'->>'correlation' as v_svb,
                   ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY ABS(EXTRACT(EPOCH FROM (v.ts - s.ts)))) as rn
            FROM setup_log s
            LEFT JOIN volland_snapshots v ON v.ts BETWEEN s.ts - interval '5 minutes' AND s.ts + interval '5 minutes'
            WHERE s.outcome_result IS NOT NULL AND s.outcome_max_loss IS NOT NULL
        )
        SELECT * FROM trade_volland WHERE rn = 1
        ORDER BY outcome_max_loss DESC
    """))
    rows = r.fetchall()
    cols = list(r.keys())

    # Parse into dicts
    trades = []
    for row in rows:
        d = dict(zip(cols, row))
        d['agg_charm_num'] = float(d['agg_charm']) if d['agg_charm'] and d['agg_charm'] not in ('null', 'None', '') else None
        d['dd_hedging_num'] = parse_money(d['dd_hedging'])
        trades.append(d)

    print(f"Total trades with volland match: {len(trades)}")

    # Categorize
    golden = [t for t in trades if t['outcome_max_loss'] is not None and t['outcome_max_loss'] >= -1.0]
    low_dd = [t for t in trades if t['outcome_max_loss'] is not None and -2.0 <= t['outcome_max_loss'] < -1.0]
    med_dd = [t for t in trades if t['outcome_max_loss'] is not None and -5.0 <= t['outcome_max_loss'] < -2.0]
    high_dd = [t for t in trades if t['outcome_max_loss'] is not None and t['outcome_max_loss'] < -5.0]

    def avg(lst):
        valid = [x for x in lst if x is not None]
        return sum(valid) / len(valid) if valid else 0

    def med(lst):
        valid = sorted([x for x in lst if x is not None])
        if not valid: return 0
        n = len(valid)
        return valid[n//2]

    for label, group in [("GOLDEN (MAE >= -1)", golden), ("LOW_DD (-2 to -1)", low_dd),
                          ("MED_DD (-5 to -2)", med_dd), ("HIGH_DD (< -5)", high_dd)]:
        if not group: continue
        ac = [t['agg_charm_num'] for t in group]
        dd = [t['dd_hedging_num'] for t in group]
        va = [t['vanna_all'] for t in group]
        vw = [t['vanna_weekly'] for t in group]
        vm = [t['vanna_monthly'] for t in group]
        svb = [t['spot_vol_beta'] for t in group]

        # Direction-aligned aggcharm: positive = aligned with trade direction
        ac_aligned = []
        for t in group:
            if t['agg_charm_num'] is not None:
                dir_sign = 1 if t['direction'] in ('long', 'bullish') else -1
                # Positive aggcharm = bullish (charm supports upside)
                # For a long, we want positive aggcharm
                # For a short, we want negative aggcharm
                ac_aligned.append(t['agg_charm_num'] * dir_sign)

        # Direction-aligned DD
        dd_aligned = []
        for t in group:
            if t['dd_hedging_num'] is not None:
                dir_sign = 1 if t['direction'] in ('long', 'bullish') else -1
                dd_aligned.append(t['dd_hedging_num'] * dir_sign)

        print(f"\n{label} ({len(group)} trades):")
        print(f"  AggCharm:     avg={avg(ac):>12.0f}  median={med(ac):>12.0f}")
        print(f"  AggCharm (dir-aligned): avg={avg(ac_aligned):>12.0f}  median={med(ac_aligned):>12.0f}")
        print(f"  DD Hedging:   avg={avg(dd):>14.0f}  median={med(dd):>14.0f}")
        print(f"  DD (dir-aligned):       avg={avg(dd_aligned):>14.0f}  median={med(dd_aligned):>14.0f}")
        print(f"  Vanna All:    avg={avg(va):>14.0f}  median={med(va):>14.0f}")
        print(f"  Vanna Weekly: avg={avg(vw):>14.0f}  median={med(vw):>14.0f}")
        print(f"  Vanna Monthly:avg={avg(vm):>14.0f}  median={med(vm):>14.0f}")
        print(f"  SVB:          avg={avg(svb):>8.3f}  median={med(svb):>8.3f}")

    # =========================================================
    # PART 2: AggCharm direction alignment as predictor
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 2: AggCharm DIRECTION ALIGNMENT as low-DD predictor")
    print("(positive = charm aligned WITH trade direction)")
    print("=" * 120)

    # Bucket trades by aggcharm alignment
    for setup in ['Skew Charm', 'DD Exhaustion', 'AG Short', 'ES Absorption', 'ALL']:
        subset = trades if setup == 'ALL' else [t for t in trades if t['setup_name'] == setup]
        subset = [t for t in subset if t['agg_charm_num'] is not None]
        if len(subset) < 10: continue

        aligned = []
        anti = []
        zero_charm = []
        for t in subset:
            dir_sign = 1 if t['direction'] in ('long', 'bullish') else -1
            charm_val = t['agg_charm_num'] * dir_sign
            if charm_val > 5_000_000:  # > 5M aligned
                aligned.append(t)
            elif charm_val < -5_000_000:  # > 5M anti-aligned
                anti.append(t)
            else:
                zero_charm.append(t)

        print(f"\n{setup}:")
        for label, grp in [("CHARM_ALIGNED", aligned), ("CHARM_NEUTRAL", zero_charm), ("CHARM_OPPOSED", anti)]:
            if not grp: continue
            wins = sum(1 for t in grp if t['outcome_result'] == 'WIN')
            low = sum(1 for t in grp if t['outcome_max_loss'] >= -2.0)
            zero = sum(1 for t in grp if t['outcome_max_loss'] >= -1.0)
            pnl = sum(t['outcome_pnl'] for t in grp)
            mae = avg([t['outcome_max_loss'] for t in grp])
            print(f"  {label:<16} n={len(grp):>3} | 0-DD:{zero:>3} ({100*zero/len(grp):>5.1f}%) | lowDD:{low:>3} ({100*low/len(grp):>5.1f}%) | WR:{100*wins/len(grp):>5.1f}% | tot_PnL:{pnl:>7.1f} | avgMAE:{mae:>6.1f}")

    # =========================================================
    # PART 3: DD Hedging direction alignment
    # =========================================================
    print("\n" + "=" * 120)
    print("PART 3: DD HEDGING direction alignment as low-DD predictor")
    print("(positive DD = bullish, aligned with longs)")
    print("=" * 120)

    for setup in ['Skew Charm', 'DD Exhaustion', 'AG Short', 'ALL']:
        subset = trades if setup == 'ALL' else [t for t in trades if t['setup_name'] == setup]
        subset = [t for t in subset if t['dd_hedging_num'] is not None]
        if len(subset) < 10: continue

        dd_aligned_grp = []
        dd_opposed_grp = []
        dd_neutral_grp = []
        for t in subset:
            dir_sign = 1 if t['direction'] in ('long', 'bullish') else -1
            dd_val = t['dd_hedging_num'] * dir_sign
            if dd_val > 200_000_000:  # > $200M aligned
                dd_aligned_grp.append(t)
            elif dd_val < -200_000_000:  # > $200M opposed
                dd_opposed_grp.append(t)
            else:
                dd_neutral_grp.append(t)

        print(f"\n{setup}:")
        for label, grp in [("DD_ALIGNED", dd_aligned_grp), ("DD_NEUTRAL", dd_neutral_grp), ("DD_OPPOSED", dd_opposed_grp)]:
            if not grp: continue
            wins = sum(1 for t in grp if t['outcome_result'] == 'WIN')
            low = sum(1 for t in grp if t['outcome_max_loss'] >= -2.0)
            zero = sum(1 for t in grp if t['outcome_max_loss'] >= -1.0)
            pnl = sum(t['outcome_pnl'] for t in grp)
            mae = avg([t['outcome_max_loss'] for t in grp])
            print(f"  {label:<14} n={len(grp):>3} | 0-DD:{zero:>3} ({100*zero/len(grp):>5.1f}%) | lowDD:{low:>3} ({100*low/len(grp):>5.1f}%) | WR:{100*wins/len(grp):>5.1f}% | tot_PnL:{pnl:>7.1f} | avgMAE:{mae:>6.1f}")
