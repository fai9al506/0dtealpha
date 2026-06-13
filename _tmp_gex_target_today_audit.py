"""Audit today's 3-loss cluster: which paradigm was active when each fired?"""
import psycopg2

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

# Pull every TSRT trade today with paradigm + entry/exit + outcome
cur.execute("""
    SELECT sl.id, sl.setup_name, sl.direction, sl.grade,
           sl.paradigm,
           sl.greek_alignment, sl.vix,
           sl.spot, sl.lis, sl.target, sl.max_plus_gex, sl.max_minus_gex,
           (sl.ts AT TIME ZONE 'America/New_York') as et_ts,
           sl.outcome_result, sl.outcome_pnl,
           rto.state->>'fill_price',
           rto.state->>'close_fill_price',
           rto.state->>'close_reason',
           sl.real_trade_skip_reason
    FROM setup_log sl
    LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE sl.ts::date = '2026-05-22'
      AND rto.state->>'account_id' = '210VYX65'
    ORDER BY sl.id
""")
rows = cur.fetchall()
print(f"TSRT longs today on 210VYX65: {len(rows)} (matched in real_trade_orders)\n")
print(f"{'lid':>5s} {'setup':24s} {'grade':5s} {'paradigm':18s} {'align':>5s} {'vix':>5s} "
      f"{'spot':>8s} {'lis':>8s} {'target':>8s} {'+gex':>7s} {'-gex':>7s} {'et_time':19s} "
      f"{'fill':>8s} {'close':>8s} {'reason':24s} {'pnl':>7s}")
for r in rows:
    (lid, setup, direction, grade, paradigm, align, vix, spot, lis, tgt, plus_gex, minus_gex,
     et_ts, outcome, pnl, fill, close, reason, skip) = r
    print(f"{lid:5d} {setup:24s} {grade or '-':5s} {paradigm or '-':18s} "
          f"{(str(align) if align is not None else '-'):>5s} {vix or 0:5.1f} "
          f"{spot or 0:8.2f} {lis or 0:8.2f} {tgt or 0:8.2f} "
          f"{plus_gex or 0:7.0f} {minus_gex or 0:7.0f} "
          f"{str(et_ts)[:19]} "
          f"{fill or '-':>8s} {close or '-':>8s} {(reason or '-'):24s} "
          f"{(f'{pnl:+.2f}' if pnl is not None else '-'):>7s}")

print()

# Now also check the paradigm history through the day from chain_snapshots
print("\nParadigm timeline today (chain_snapshots):")
cur.execute("""
    SELECT (ts AT TIME ZONE 'America/New_York') as et_ts,
           paradigm
    FROM chain_snapshots
    WHERE ts::date = '2026-05-22'
      AND paradigm IS NOT NULL
    ORDER BY ts
""")
# Just show paradigm changes
prev = None
for r in cur.fetchall():
    et, p = r
    if p != prev:
        print(f"  {str(et)[:19]}  {p}")
        prev = p

conn.close()
