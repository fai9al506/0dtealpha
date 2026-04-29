"""Gap 2: ES Absorption BOTH directions, full March"""
import psycopg2, sys
sys.stdout.reconfigure(encoding='utf-8')
DB='postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

print("=" * 70)
print("GAP 2: ES ABSORPTION - BOTH DIRECTIONS - MARCH 2026")
print("User concern: March was bearish, bearish setups biased.")
print("Testing both bullish+bearish together as a single setup.")
print("=" * 70)

# Overall by direction
print("\n--- By Direction ---")
cur.execute("""
SELECT direction, COUNT(*),
       COUNT(*) FILTER (WHERE outcome_result='WIN'),
       COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       COUNT(*) FILTER (WHERE outcome_result='EXPIRED'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY direction ORDER BY direction
""")
print(f"{'Dir':10s} {'T':>4s} {'W':>4s} {'L':>4s} {'E':>4s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{r[0]:10s} {r[1]:>3d}  {r[2]:>3d} {r[3]:>3d} {r[4]:>3d} {float(r[5]):>+9.1f} {str(r[6]):>5s}%")

# Combined
print("\n--- Combined ---")
cur.execute("""
SELECT COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'),
       COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
""")
r = cur.fetchone()
print(f"  {r[0]} trades, {r[1]}W/{r[2]}L, PnL={float(r[3]):+.1f}, WR={r[4]}%")

# Daily P&L with bull/bear split
print("\n--- Daily P&L (Bull + Bear shown separately) ---")
cur.execute("""
SELECT (ts AT TIME ZONE 'America/New_York')::date as td,
       COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(COALESCE(SUM(outcome_pnl) FILTER (WHERE direction='bullish'),0)::numeric,1),
       ROUND(COALESCE(SUM(outcome_pnl) FILTER (WHERE direction='bearish'),0)::numeric,1),
       COUNT(*) FILTER (WHERE direction='bullish'),
       COUNT(*) FILTER (WHERE direction='bearish'),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY td ORDER BY td
""")
total=0; tb=0; tbr=0
print(f"{'Date':12s} {'T':>3s} {'Bu/Be':>6s} {'W/L':>6s} {'Total':>8s} {'Bull$':>8s} {'Bear$':>8s} {'WR':>6s}")
for r in cur.fetchall():
    p=float(r[4]); bp=float(r[5]); brp=float(r[6])
    total+=p; tb+=bp; tbr+=brp
    print(f"{str(r[0]):12s} {r[1]:>3d} {r[7]:>2d}/{r[8]:>2d}  {r[2]:>2d}/{r[3]:>2d}  {p:>+8.1f} {bp:>+8.1f} {brp:>+8.1f}  {r[9]}%")
print(f"{'TOTAL':12s} {'':>3s} {'':>6s} {'':>6s} {total:>+8.1f} {tb:>+8.1f} {tbr:>+8.1f}")

# By alignment
print("\n--- By Alignment (both dirs) ---")
cur.execute("""
SELECT greek_alignment, COUNT(*),
       COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY greek_alignment ORDER BY 1
""")
print(f"{'Align':>6s} {'T':>4s} {'W/L':>6s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{r[0]:>+5d}  {r[1]:>3d}  {r[2]:>2d}/{r[3]:>2d}  {float(r[4]):>+9.1f}  {r[5]}%")

# By paradigm
print("\n--- By Paradigm (both dirs) ---")
cur.execute("""
SELECT paradigm, COUNT(*),
       COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY paradigm ORDER BY 5 DESC
""")
print(f"{'Paradigm':18s} {'T':>4s} {'W/L':>6s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{str(r[0]):18s} {r[1]:>3d}  {r[2]:>2d}/{r[3]:>2d}  {float(r[4]):>+9.1f}  {r[5]}%")

# By grade
print("\n--- By Grade (both dirs) ---")
cur.execute("""
SELECT grade, COUNT(*),
       COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY grade ORDER BY 5 DESC
""")
print(f"{'Grade':>6s} {'T':>4s} {'W/L':>6s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{str(r[0]):>6s} {r[1]:>3d}  {r[2]:>2d}/{r[3]:>2d}  {float(r[4]):>+9.1f}  {r[5]}%")

# By hour
print("\n--- By Hour (both dirs) ---")
cur.execute("""
SELECT EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York')::int,
       COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'),
       ROUND(SUM(outcome_pnl)::numeric,1),
       ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY 1 ORDER BY 1
""")
print(f"{'Hour':>5s} {'T':>4s} {'W/L':>6s} {'PnL':>9s} {'WR':>6s}")
for r in cur.fetchall():
    print(f"{r[0]:>4d}h {r[1]:>3d}  {r[2]:>2d}/{r[3]:>2d}  {float(r[4]):>+9.1f}  {r[5]}%")

# MaxDD running
print("\n--- MaxDD (both dirs combined) ---")
cur.execute("""
SELECT (ts AT TIME ZONE 'America/New_York')::date, SUM(outcome_pnl)::numeric(10,1),
       SUM(SUM(outcome_pnl)) OVER (ORDER BY (ts AT TIME ZONE 'America/New_York')::date)::numeric(10,1)
FROM setup_log WHERE setup_name='ES Absorption' AND outcome_result IS NOT NULL
AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY 1 ORDER BY 1
""")
peak=0; maxdd=0
for r in cur.fetchall():
    run=float(r[2])
    if run>peak: peak=run
    dd=run-peak
    if dd<maxdd: maxdd=dd
print(f"Peak: {peak:.1f}, MaxDD: {maxdd:.1f}")

conn.close()
