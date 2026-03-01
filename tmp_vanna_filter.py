"""Query GEX Long trades with vanna ALL filter simulation."""
import psycopg2
from decimal import Decimal

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# ── Query B: All GEX Long trades with nearest vanna ALL sum ──
sql_detail = """
WITH gex_trades AS (
    SELECT id, ts, grade, score, spot, paradigm,
           direction, outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss, outcome_first_event
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
),
nearest_ts AS (
    SELECT g.id as trade_id,
           (SELECT vep.ts_utc
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna'
            AND vep.expiration_option = 'ALL'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes'
                                AND g.ts + interval '5 minutes'
            GROUP BY vep.ts_utc
            ORDER BY ABS(EXTRACT(EPOCH FROM (vep.ts_utc - g.ts)))
            LIMIT 1
           ) as nearest_vep_ts
    FROM gex_trades g
),
vanna_sums AS (
    SELECT nt.trade_id,
           ROUND(SUM(vep.value)::numeric, 2) as vanna_all_sum
    FROM nearest_ts nt
    JOIN volland_exposure_points vep ON vep.ts_utc = nt.nearest_vep_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'ALL'
    GROUP BY nt.trade_id
)
SELECT g.id,
       (g.ts AT TIME ZONE 'America/New_York')::date as trade_date,
       (g.ts AT TIME ZONE 'America/New_York')::time as time_et,
       g.grade, g.score, g.spot,
       g.outcome_result, g.outcome_pnl, g.outcome_max_profit, g.outcome_max_loss,
       g.outcome_first_event, g.paradigm,
       vs.vanna_all_sum,
       CASE WHEN vs.vanna_all_sum IS NULL THEN 'NO_DATA'
            WHEN vs.vanna_all_sum < 0 THEN 'BLOCKED'
            ELSE 'ALLOWED' END as filter_decision
FROM gex_trades g
LEFT JOIN vanna_sums vs ON vs.trade_id = g.id
ORDER BY g.ts
"""

cur.execute(sql_detail)
rows = cur.fetchall()

print("=" * 170)
print("QUERY B: ALL GEX LONG TRADES WITH VANNA ALL FILTER")
print("=" * 170)
hdr = (f"{'ID':>4} | {'Date':>10} | {'Time ET':>10} | {'Grd':>7} | {'Score':>5} | "
       f"{'Spot':>8} | {'Result':>7} | {'PnL':>7} | {'MaxP':>6} | {'MaxL':>6} | "
       f"{'1stEvt':>7} | {'Paradigm':>10} | {'Vanna ALL Sum':>16} | {'Filter':>8}")
print(hdr)
print("-" * len(hdr))
for r in rows:
    tid, dt, tm, grd, sc, spot, res, pnl, mp, ml, fe, para, vsum, filt = r
    tm_str = str(tm)[:8] if tm else "N/A"
    para_str = (para or "N/A")[:10]
    fe_str = (fe or "N/A")[:7]
    vsum_str = f"{float(vsum):>16,.0f}" if vsum is not None else "             N/A"
    print(
        f"{tid:>4} | {str(dt):>10} | {tm_str:>10} | {grd or '':>7} | "
        f"{float(sc or 0):>5.1f} | {float(spot or 0):>8.1f} | {res:>7} | "
        f"{float(pnl or 0):>7.1f} | {float(mp or 0):>6.1f} | {float(ml or 0):>6.1f} | "
        f"{fe_str:>7} | {para_str:>10} | {vsum_str} | {filt:>8}"
    )
print(f"\nTotal trades: {len(rows)}")

# ── Query C: Summary by filter decision ──
sql_summary = """
WITH gex_trades AS (
    SELECT id, ts, outcome_result, outcome_pnl
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
),
nearest_ts AS (
    SELECT g.id as trade_id,
           (SELECT vep.ts_utc
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna'
            AND vep.expiration_option = 'ALL'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes'
                                AND g.ts + interval '5 minutes'
            GROUP BY vep.ts_utc
            ORDER BY ABS(EXTRACT(EPOCH FROM (vep.ts_utc - g.ts)))
            LIMIT 1
           ) as nearest_vep_ts
    FROM gex_trades g
),
trade_vanna AS (
    SELECT nt.trade_id,
           nt.nearest_vep_ts,
           COALESCE(SUM(vep.value), 0) as vanna_sum,
           CASE WHEN nt.nearest_vep_ts IS NULL THEN 'NO_DATA'
                WHEN SUM(vep.value) < 0 THEN 'BLOCKED'
                ELSE 'ALLOWED' END as filter_decision
    FROM nearest_ts nt
    LEFT JOIN volland_exposure_points vep ON vep.ts_utc = nt.nearest_vep_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'ALL'
    GROUP BY nt.trade_id, nt.nearest_vep_ts
)
SELECT tv.filter_decision,
       COUNT(*) as trades,
       SUM(CASE WHEN sl.outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
       SUM(CASE WHEN sl.outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
       SUM(CASE WHEN sl.outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
       ROUND(SUM(sl.outcome_pnl)::numeric, 1) as total_pnl,
       ROUND(AVG(sl.outcome_pnl)::numeric, 1) as avg_pnl,
       ROUND(100.0 * SUM(CASE WHEN sl.outcome_result = 'WIN' THEN 1 ELSE 0 END) /
             NULLIF(SUM(CASE WHEN sl.outcome_result IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0), 1) as win_rate
FROM trade_vanna tv
JOIN setup_log sl ON sl.id = tv.trade_id
GROUP BY tv.filter_decision
ORDER BY tv.filter_decision
"""

cur.execute(sql_summary)
rows_s = cur.fetchall()

print("\n")
print("=" * 100)
print("QUERY C: SUMMARY BY FILTER DECISION")
print("=" * 100)
print(f"{'Filter':>10} | {'Trades':>6} | {'Wins':>4} | {'Loss':>4} | {'Exp':>3} | {'Total PnL':>10} | {'Avg PnL':>8} | {'Win Rate':>8}")
print("-" * 80)
for r in rows_s:
    filt, trades, wins, losses, expired, tpnl, apnl, wr = r
    wr_str = f"{float(wr):.1f}%" if wr is not None else "N/A"
    print(
        f"{filt:>10} | {trades:>6} | {wins:>4} | {losses:>4} | {expired:>3} | "
        f"{float(tpnl or 0):>10.1f} | {float(apnl or 0):>8.1f} | {wr_str:>8}"
    )

# ── Filter Impact Analysis ──
print("\n")
print("=" * 100)
print("FILTER IMPACT ANALYSIS")
print("=" * 100)

# Build lookup from summary
summary_map = {}
for r in rows_s:
    filt, trades, wins, losses, expired, tpnl, apnl, wr = r
    summary_map[filt] = {
        'trades': trades, 'wins': wins, 'losses': losses,
        'expired': expired, 'pnl': float(tpnl or 0), 'wr': float(wr or 0)
    }

# Current total
total_trades = sum(v['trades'] for v in summary_map.values())
total_pnl = sum(v['pnl'] for v in summary_map.values())
total_wins = sum(v['wins'] for v in summary_map.values())
total_losses = sum(v['losses'] for v in summary_map.values())
total_wr = 100 * total_wins / max(total_wins + total_losses, 1)

print(f"WITHOUT filter:  {total_trades} trades, {total_wins}W/{total_losses}L, "
      f"WR={total_wr:.1f}%, PnL = {total_pnl:.1f} pts")

# With filter
allowed = summary_map.get('ALLOWED', {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0})
no_data = summary_map.get('NO_DATA', {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0})
blocked = summary_map.get('BLOCKED', {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0})

# Allowed + NO_DATA (conservative: keep NO_DATA trades since we can't filter them)
kept_trades = allowed['trades'] + no_data['trades']
kept_pnl = allowed['pnl'] + no_data['pnl']
kept_wins = allowed['wins'] + no_data['wins']
kept_losses = allowed['losses'] + no_data['losses']
kept_wr = 100 * kept_wins / max(kept_wins + kept_losses, 1)

print(f"WITH filter:     {kept_trades} trades (ALLOWED+NO_DATA), {kept_wins}W/{kept_losses}L, "
      f"WR={kept_wr:.1f}%, PnL = {kept_pnl:.1f} pts")
print(f"Trades blocked:  {blocked['trades']}, {blocked['wins']}W/{blocked['losses']}L, "
      f"Blocked PnL = {blocked['pnl']:.1f} pts")

improvement = total_pnl - kept_pnl
if blocked['pnl'] < 0:
    print(f"\n>>> FILTER SAVES {-blocked['pnl']:.1f} pts by blocking losing trades!")
    print(f">>> Net PnL improvement: {total_pnl:.1f} -> {kept_pnl:.1f} = +{kept_pnl - total_pnl:.1f} pts")
else:
    print(f"\n>>> WARNING: Filter would LOSE {blocked['pnl']:.1f} pts of winning trades!")
    print(f">>> Net PnL impact: {total_pnl:.1f} -> {kept_pnl:.1f} = {kept_pnl - total_pnl:.1f} pts")

conn.close()
