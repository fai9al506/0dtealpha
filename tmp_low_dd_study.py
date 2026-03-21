"""Low-DD deep study: what makes trades achieve near-zero drawdown?"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from dotenv import load_dotenv
load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL:
    DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
if "postgresql://" in DB_URL and "postgresql+psycopg" not in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
from sqlalchemy import create_engine, text
engine = create_engine(DB_URL)

with engine.connect() as c:
    # SVB patterns for SC
    print("=" * 100)
    print("SC LONGS: SVB sign as predictor of low DD")
    print("=" * 100)
    r = c.execute(text("""
        SELECT
            CASE WHEN spot_vol_beta < 0 THEN 'SVB_neg' ELSE 'SVB_pos' END as svb_sign,
            count(*) as cnt,
            round(avg(outcome_max_loss)::numeric, 2) as avg_mae,
            round(100.0 * sum(case when outcome_max_loss >= -2.0 then 1 else 0 end) / count(*)::numeric, 1) as pct_low_dd,
            round(100.0 * sum(case when outcome_result = 'WIN' then 1 else 0 end) / count(*)::numeric, 1) as wr,
            round(avg(outcome_pnl)::numeric, 2) as avg_pnl,
            round(sum(outcome_pnl)::numeric, 1) as total_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
          AND setup_name = 'Skew Charm' AND direction = 'long'
          AND spot_vol_beta IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """))
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        print(f"  {d['svb_sign']:<8} | cnt:{d['cnt']:>3} | MAE:{d['avg_mae']:>6} | low_DD:{d['pct_low_dd']:>5}% | WR:{d['wr']:>5}% | avg_PnL:{d['avg_pnl']:>6} | tot:{d['total_pnl']:>6}")

    print()
    print("SC SHORTS: SVB sign as predictor of low DD")
    r = c.execute(text("""
        SELECT
            CASE WHEN spot_vol_beta < 0 THEN 'SVB_neg' ELSE 'SVB_pos' END as svb_sign,
            count(*) as cnt,
            round(avg(outcome_max_loss)::numeric, 2) as avg_mae,
            round(100.0 * sum(case when outcome_max_loss >= -2.0 then 1 else 0 end) / count(*)::numeric, 1) as pct_low_dd,
            round(100.0 * sum(case when outcome_result = 'WIN' then 1 else 0 end) / count(*)::numeric, 1) as wr,
            round(avg(outcome_pnl)::numeric, 2) as avg_pnl,
            round(sum(outcome_pnl)::numeric, 1) as total_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
          AND setup_name = 'Skew Charm' AND direction = 'short'
          AND spot_vol_beta IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """))
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        print(f"  {d['svb_sign']:<8} | cnt:{d['cnt']:>3} | MAE:{d['avg_mae']:>6} | low_DD:{d['pct_low_dd']:>5}% | WR:{d['wr']:>5}% | avg_PnL:{d['avg_pnl']:>6} | tot:{d['total_pnl']:>6}")

    # FILTER SIMULATIONS
    print()
    print("=" * 100)
    print("FILTER SIMULATIONS: Comparing strategies for low DD")
    print("=" * 100)

    filters = [
        ("SC_ALL", "setup_name = 'Skew Charm'"),
        ("SC_no_toxic_paradigm", "setup_name = 'Skew Charm' AND COALESCE(paradigm,'') NOT IN ('GEX-LIS', 'AG-LIS')"),
        ("SC_SIDIAL+GEX_paradigm", "setup_name = 'Skew Charm' AND paradigm IN ('SIDIAL-MESSY', 'GEX-PURE', 'AG-TARGET')"),
        ("SC_clean_VIX20-26", "setup_name = 'Skew Charm' AND COALESCE(paradigm,'') NOT IN ('GEX-LIS', 'AG-LIS') AND vix >= 20 AND vix <= 26"),
        ("DD_shorts_align!0", "setup_name = 'DD Exhaustion' AND direction = 'short' AND greek_alignment != 0"),
        ("DD_all_align!0", "setup_name = 'DD Exhaustion' AND greek_alignment != 0"),
        ("AG_align<=-2_VIX>=24", "setup_name = 'AG Short' AND greek_alignment <= -2 AND vix >= 24"),
        ("AG_all", "setup_name = 'AG Short'"),
        ("SC+DD+AG_combined", """(setup_name = 'Skew Charm' AND COALESCE(paradigm,'') NOT IN ('GEX-LIS', 'AG-LIS'))
            OR (setup_name = 'DD Exhaustion' AND direction = 'short' AND greek_alignment != 0)
            OR (setup_name = 'AG Short' AND greek_alignment <= -2)"""),
        ("SC+DD+AG_V9SC", """(setup_name = 'Skew Charm')
            OR (setup_name = 'AG Short')
            OR (setup_name = 'DD Exhaustion' AND greek_alignment != 0)"""),
    ]

    for name, where in filters:
        q = f"""
            SELECT count(*) as cnt,
                round(avg(outcome_max_loss)::numeric, 2) as avg_mae,
                round(100.0 * sum(case when outcome_max_loss >= -2.0 then 1 else 0 end) / NULLIF(count(*),0)::numeric, 1) as pct_low_dd,
                round(100.0 * sum(case when outcome_max_loss >= -1.0 then 1 else 0 end) / NULLIF(count(*),0)::numeric, 1) as pct_zero_dd,
                round(100.0 * sum(case when outcome_result = 'WIN' then 1 else 0 end) / NULLIF(count(*),0)::numeric, 1) as wr,
                round(sum(outcome_pnl)::numeric, 1) as total_pnl,
                round(avg(outcome_pnl)::numeric, 2) as avg_pnl,
                round(CASE WHEN sum(case when outcome_pnl < 0 then -outcome_pnl else 0 end) = 0 THEN 999
                      ELSE sum(case when outcome_pnl > 0 then outcome_pnl else 0 end) / sum(case when outcome_pnl < 0 then -outcome_pnl else 0 end)
                END::numeric, 2) as pf
            FROM setup_log
            WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
              AND ({where})
        """
        r = c.execute(text(q))
        d = dict(zip(r.keys(), r.fetchone()))
        print(f"{name:<30} | n:{d['cnt']:>3} | 0DD:{d['pct_zero_dd']:>5}% | lowDD:{d['pct_low_dd']:>5}% | WR:{d['wr']:>5}% | tot:{d['total_pnl']:>7} | avg:{d['avg_pnl']:>6} | PF:{d['pf']:>5} | MAE:{d['avg_mae']:>6}")

    # ============ WHAT DO ZERO-DD TRADES LOOK LIKE? ============
    print()
    print("=" * 100)
    print("THE 0.1-0.5 MAE TRADES: What happened?")
    print("=" * 100)
    r = c.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade, score, spot,
               COALESCE(paradigm,'') as paradigm,
               COALESCE(greek_alignment, 0) as align,
               COALESCE(vix, 0) as vix,
               outcome_pnl, outcome_max_profit, outcome_max_loss,
               COALESCE(outcome_elapsed_min, 0) as elapsed,
               COALESCE(outcome_first_event, '') as first_event,
               COALESCE(lis, 0) as lis,
               COALESCE(gap_to_lis, 0) as gap
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_max_loss IS NOT NULL
          AND outcome_max_loss >= -0.5
        ORDER BY outcome_max_loss DESC
    """))
    rows = r.fetchall()
    print(f"Total trades with MAE >= -0.5 (half a point or less DD): {len(rows)}")
    for row in rows:
        d = dict(zip(r.keys(), row))
        print(f"  ID:{d['id']:>4} {str(d['ts_et'])[:16]} {d['setup_name']:<16} {d['direction']:<8} {d['grade']:<4} "
              f"spot:{d['spot']:>7.1f} {d['paradigm']:<14} al:{d['align']:>2} VIX:{d['vix']:>5.1f} "
              f"| MAE:{d['outcome_max_loss']:>5.2f} MFE:{d['outcome_max_profit']:>5.1f} PnL:{d['outcome_pnl']:>5.1f} {d['elapsed']:>3}min {d['first_event']}")
