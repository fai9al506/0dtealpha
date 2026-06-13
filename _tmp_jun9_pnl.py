import os, json
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

# ---- Jun 9 per-trade MES P&L from broker fills (entry vs close_fill) ----
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York')::text as et,
               sl.setup_name, sl.direction, sl.grade, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = DATE '2026-06-08'
        ORDER BY sl.ts ASC
    """)).fetchall()

print("Jun 9 per-trade (broker fills, MES $5/pt):\n")
print(f"{'lid':>5} {'time':<6}{'setup':<14}{'dir':<8}{'gr':<3}{'acct':<10}{'entry':>9}{'exit':>9}{'pts':>8}{'$':>9}")
tot_pts = tot_usd = 0.0
n=w=l=0
for r in rows:
    sid, et, setup, direction, grade, st = r
    if not isinstance(st, dict):
        st = json.loads(st)
    entry = st.get('fill_price')
    exitp = st.get('close_fill_price')
    acct = st.get('account_id')
    # short side: bearish ES Abs, short SC, AG Short, on VYX91
    is_short = ('bear' in (direction or '')) or (direction == 'short') or (setup == 'AG Short')
    if entry is None or exitp is None:
        print(f"{sid:>5} {et[11:16]:<6}{setup[:13]:<14}{str(direction)[:7]:<8}{str(grade):<3}{str(acct):<10} (no fills)")
        continue
    pts = (entry - exitp) if is_short else (exitp - entry)
    usd = pts * 5.0
    tot_pts += pts; tot_usd += usd; n+=1
    if pts>0: w+=1
    else: l+=1
    print(f"{sid:>5} {et[11:16]:<6}{setup[:13]:<14}{('SHORT' if is_short else 'LONG'):<8}{str(grade):<3}{str(acct):<10}{entry:>9.2f}{exitp:>9.2f}{pts:>+8.2f}{usd:>+9.2f}")

comm = n * 1.34  # ~$1.34/RT MES roundtrip est
print(f"\nGROSS: {tot_pts:+.2f} pts = ${tot_usd:+.2f}  ({w}W/{l}L, {n} trades)")
print(f"Est commission (~$1.34/RT x{n}): -${comm:.2f}")
print(f"NET (est): ${tot_usd-comm:+.2f}")

# ---- broker-truth statement table, recent ----
print("\n=== tsrt_daily_stmt (broker truth, if present) ===")
with engine.connect() as conn:
    try:
        srows = conn.execute(text("""
            SELECT day, gross, comm, net, n_trades, n_wins
            FROM tsrt_daily_stmt ORDER BY day DESC LIMIT 12
        """)).fetchall()
        for s in srows:
            print(f"  {s[0]}  n={s[4]:>2} W={s[5]:>2}  gross={float(s[1]):+.2f} comm={float(s[2]):.2f} net={float(s[3]):+.2f}")
    except Exception as e:
        print("  table/query error:", e)
