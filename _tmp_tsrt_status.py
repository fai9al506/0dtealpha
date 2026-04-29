import os
import requests
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

# --- Recent real trades ---
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT sl.id,
               (sl.ts AT TIME ZONE 'America/New_York')::text as et_ts,
               sl.setup_name,
               sl.direction,
               sl.grade,
               rto.state->>'result' as result,
               rto.state->>'pnl_pts' as pnl_pts,
               rto.state->>'account' as account,
               rto.state->>'close_reason' as close_reason
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.ts >= NOW() - INTERVAL '12 days'
        ORDER BY sl.ts DESC
        LIMIT 50
    """)).fetchall()

print("RECENT REAL TRADES (last 12d):")
print(f"{'id':>5} {'et_time':<19} {'setup':<16} {'dir':<6} {'gr':<3} {'res':<10} {'pnl':<8} {'acct':<11} reason")
total_pnl = 0.0
wins = losses = expired = other = 0
for r in rows:
    pnl = r[6]
    try:
        if pnl is not None:
            total_pnl += float(pnl)
    except Exception:
        pass
    res = r[5] or ''
    if res == 'WIN':
        wins += 1
    elif res == 'LOSS':
        losses += 1
    elif res == 'EXPIRED':
        expired += 1
    elif res:
        other += 1
    print(f"{r[0]:>5} {str(r[1])[:19]:<19} {str(r[2])[:16]:<16} {str(r[3])[:6]:<6} {str(r[4])[:3]:<3} {res[:10]:<10} {str(pnl)[:8]:<8} {str(r[7])[:11]:<11} {str(r[8])[:50] if r[8] else ''}")

print()
print(f"SUMMARY 12d: {wins}W / {losses}L / {expired}E / {other}? | net pts = {total_pnl:+.2f}")

# --- Open positions / active orders ---
with engine.connect() as conn:
    open_rows = conn.execute(text("""
        SELECT setup_log_id, state->>'account' as acct, state->>'status' as status,
               state->>'setup_name' as setup, state->>'direction' as dir,
               state->>'entry_price' as entry, state->>'created_at' as created
        FROM real_trade_orders
        WHERE (state->>'result') IS NULL
        ORDER BY setup_log_id DESC
    """)).fetchall()
print()
print(f"OPEN/ACTIVE REAL ORDERS: {len(open_rows)}")
for r in open_rows:
    print(f"  id={r[0]} acct={r[1]} status={r[2]} setup={r[3]} dir={r[4]} entry={r[5]} created={r[6]}")

# --- Balances ---
tok_resp = requests.post(
    'https://signin.tradestation.com/oauth/token',
    data={'grant_type': 'refresh_token',
          'client_id': os.environ['TS_CLIENT_ID'],
          'client_secret': os.environ['TS_CLIENT_SECRET'],
          'refresh_token': os.environ['TS_REFRESH_TOKEN']},
    timeout=10
).json()
tok = tok_resp.get('access_token')
print()
print("ACCOUNT BALANCES:")
for acct in ['210VYX65', '210VYX91']:
    try:
        r = requests.get(f'https://api.tradestation.com/v3/brokerage/accounts/{acct}/balances',
                         headers={'Authorization': f'Bearer {tok}'}, timeout=10)
        d = r.json()
        if 'Balances' in d and d['Balances']:
            b = d['Balances'][0]
            print(f"  {acct}: Cash={b.get('CashBalance')} BuyingPower={b.get('BuyingPower')} Equity={b.get('Equity')}")
        else:
            print(f"  {acct}: {d}")
    except Exception as e:
        print(f"  {acct}: ERROR {e}")
