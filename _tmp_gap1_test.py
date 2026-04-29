"""Gap 1: VIX Direction full March backtest"""
import psycopg2, sys
sys.stdout.reconfigure(encoding='utf-8')
DB='postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

print("=" * 70)
print("GAP 1: VIX DIRECTION MODIFIER - FULL MARCH 2026")
print("Rule: Allow non-SC longs when VIX>22 AND VIX falling 1pt+ from open")
print("These trades are currently ALL BLOCKED by V12-fix.")
print("=" * 70)

# VIX > 22 days in March
cur.execute("""
WITH daily_vix AS (
    SELECT (ts AT TIME ZONE 'America/New_York')::date as td,
           (array_agg(vix ORDER BY ts))[1] as vix_open,
           (array_agg(vix ORDER BY ts DESC))[1] as vix_close
    FROM chain_snapshots
    WHERE vix IS NOT NULL AND vix > 0
    AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
    GROUP BY td)
SELECT td, vix_open, vix_close,
       ROUND((vix_close-vix_open)::numeric,2) as chg,
       CASE WHEN vix_close < vix_open - 1 THEN 'CRUSH' ELSE 'rise/flat' END
FROM daily_vix WHERE vix_open > 22 ORDER BY td
""")
print(f"\n{'Date':12s} {'Open':>7s} {'Close':>7s} {'Chg':>7s} {'Type':>10s}")
for r in cur.fetchall():
    print(f"{str(r[0]):12s} {r[1]:7.1f} {r[2]:7.1f} {float(r[3]):+7.2f} {r[4]:>10s}")

# Summary: CRUSH vs rise/flat
print("\n--- Summary: Non-SC Longs (align>=2) on VIX>22 days ---")
cur.execute("""
WITH daily_vix AS (
    SELECT (ts AT TIME ZONE 'America/New_York')::date as td,
           (array_agg(vix ORDER BY ts))[1] as vo,
           (array_agg(vix ORDER BY ts DESC))[1] as vc
    FROM chain_snapshots
    WHERE vix IS NOT NULL AND vix > 0
    AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
    GROUP BY td)
SELECT CASE WHEN vc < vo - 1 THEN 'CRUSH' ELSE 'rise/flat' END as regime,
       COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'),
       COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log s JOIN daily_vix d ON (s.ts AT TIME ZONE 'America/New_York')::date = d.td
WHERE outcome_result IS NOT NULL AND direction IN ('long','bullish')
AND setup_name NOT IN ('Skew Charm','VIX Divergence','IV Momentum','Vanna Butterfly')
AND greek_alignment >= 2 AND vo > 22
GROUP BY regime ORDER BY regime
""")
print(f"{'Regime':12s} {'Trades':>7s} {'W':>4s} {'L':>4s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{r[0]:12s} {r[1]:>5d}   {r[2]:>3d} {r[3]:>3d} {float(r[4]):>+9.1f} {str(r[5]):>5s}%")

# Daily breakdown on CRUSH days
print("\n--- Daily P&L on VIX CRUSH days (trades we would ADD) ---")
cur.execute("""
WITH daily_vix AS (
    SELECT (ts AT TIME ZONE 'America/New_York')::date as td,
           (array_agg(vix ORDER BY ts))[1] as vo,
           (array_agg(vix ORDER BY ts DESC))[1] as vc
    FROM chain_snapshots
    WHERE vix IS NOT NULL AND vix > 0
    AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
    GROUP BY td)
SELECT d.td, d.vo, d.vc,
       COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'),
       COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log s JOIN daily_vix d ON (s.ts AT TIME ZONE 'America/New_York')::date = d.td
WHERE outcome_result IS NOT NULL AND direction IN ('long','bullish')
AND setup_name NOT IN ('Skew Charm','VIX Divergence','IV Momentum','Vanna Butterfly')
AND greek_alignment >= 2 AND d.vo > 22 AND d.vc < d.vo - 1
GROUP BY d.td, d.vo, d.vc ORDER BY d.td
""")
total = 0
print(f"{'Date':12s} {'VIX':>14s} {'T':>3s} {'W/L':>6s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    p = float(r[6])
    total += p
    print(f"{str(r[0]):12s} {r[1]:.1f} -> {r[2]:.1f} {r[3]:>3d} {r[4]:>2d}/{r[5]:>2d}  {p:>+9.1f} {str(r[7]):>5s}%")
print(f"{'TOTAL':12s} {'':>14s} {'':>3s} {'':>6s} {total:>+9.1f}")

# By setup on crush days
print("\n--- By Setup on CRUSH days ---")
cur.execute("""
WITH daily_vix AS (
    SELECT (ts AT TIME ZONE 'America/New_York')::date as td,
           (array_agg(vix ORDER BY ts))[1] as vo,
           (array_agg(vix ORDER BY ts DESC))[1] as vc
    FROM chain_snapshots
    WHERE vix IS NOT NULL AND vix > 0
    AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
    GROUP BY td)
SELECT s.setup_name, COUNT(*),
       COUNT(*) FILTER (WHERE outcome_result='WIN'),
       COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log s JOIN daily_vix d ON (s.ts AT TIME ZONE 'America/New_York')::date = d.td
WHERE outcome_result IS NOT NULL AND direction IN ('long','bullish')
AND setup_name NOT IN ('Skew Charm','VIX Divergence','IV Momentum','Vanna Butterfly')
AND greek_alignment >= 2 AND d.vo > 22 AND d.vc < d.vo - 1
GROUP BY s.setup_name ORDER BY 5 DESC
""")
print(f"{'Setup':22s} {'T':>3s} {'W/L':>6s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{r[0]:22s} {r[1]:>3d} {r[2]:>2d}/{r[3]:>2d}  {float(r[4]):>+9.1f} {str(r[5]):>5s}%")

conn.close()
