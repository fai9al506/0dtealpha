"""Reconcile 2026-06-02: portal sim vs TSRT placed vs broker realized."""
import psycopg2, json
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
cur=psycopg2.connect(DB).cursor()

print("="*100)
print("ALL setup_log rows today (2026-06-02) — portal side")
print("="*100)
cur.execute("""
  SELECT id, setup_name, direction, grade,
         (ts AT TIME ZONE 'America/New_York')::time as et,
         outcome_result, outcome_pnl, real_trade_skip_reason
  FROM setup_log
  WHERE ts::date='2026-06-02'
  ORDER BY id
""")
rows=cur.fetchall()
tot_all=0.0; tot_placed=0.0; n_placed=0; w=0; l=0
for r in rows:
    pnl=float(r[6] or 0); tot_all+=pnl
    placed = (r[7] is None)
    if placed:
        tot_placed+=pnl; n_placed+=1
        if r[5]=='WIN': w+=1
        elif r[5]=='LOSS': l+=1
    tag = "PLACED" if placed else f"skip={r[7]}"
    print(f"lid={r[0]} {r[1]:14s} {r[2]:5s} g={r[3] or '-':3s} {str(r[4])[:8]} "
          f"{r[5] or '-':8s} pnl={pnl:6.2f}  {tag}")

print(f"\nALL signals: n={len(rows)} sum_outcome_pnl={tot_all:.1f}pt")
print(f"PLACED (skip_reason IS NULL): n={n_placed} W={w} L={l} sum={tot_placed:.1f}pt (~${tot_placed*5:.0f} @1MES)")

print("\n"+"="*100)
print("real_trade_orders today — TSRT actually placed (broker side)")
print("="*100)
cur.execute("""
  SELECT setup_log_id, state->>'setup_name', state->>'direction',
         state->>'status', state->>'fill_price', state->>'close_fill_price',
         state->>'close_reason', state->>'account_id'
  FROM real_trade_orders WHERE created_at::date='2026-06-02'
  ORDER BY setup_log_id
""")
real=cur.fetchall()
for r in real:
    fp=r[4]; cp=r[5]
    pnl=None
    try:
        if fp and cp:
            d=(float(cp)-float(fp))
            if (r[2] or '').lower() not in ('long','bullish'): d=-d
            pnl=d*5
    except: pnl=None
    print(f"lid={r[0]} {r[1]:14s} {r[2]:5s} acct={r[7]} status={r[3]:8s} "
          f"entry={fp} close={cp} reason={r[6]} ~pnl={('$%.2f'%pnl) if pnl is not None else 'n/a'}")
print(f"\nTSRT placed lids: {[r[0] for r in real]}")
