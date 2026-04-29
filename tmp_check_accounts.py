#!/usr/bin/env python3
"""Query real-trade accounts via TS API."""
import os, json, sys, requests

# Get fresh token
resp = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
})
token = resp.json().get('access_token')
if not token:
    print(f"AUTH FAILED: {resp.text}", flush=True)
    sys.exit(1)

headers = {'Authorization': f'Bearer {token}'}
BASE = 'https://api.tradestation.com/v3'

for acct_id in ['210VYX65', '210VYX91']:
    print(f"\n{'='*50}", flush=True)
    print(f"ACCOUNT: {acct_id}", flush=True)
    print(f"{'='*50}", flush=True)

    # Balances
    r = requests.get(f'{BASE}/brokerage/accounts/{acct_id}/balances', headers=headers)
    if r.status_code == 200:
        b = r.json().get('Balances', [{}])
        b = b[0] if isinstance(b, list) and b else b
        detail = b.get('BalanceDetail', {})
        print(f"  Cash:        ${float(b.get('CashBalance', 0)):,.2f}", flush=True)
        print(f"  Equity:      ${float(b.get('Equity', 0)):,.2f}", flush=True)
        print(f"  BuyingPower: ${float(b.get('BuyingPower', 0)):,.2f}", flush=True)
        print(f"  Today P&L:   ${float(b.get('TodaysProfitLoss', 0)):,.2f}", flush=True)
        print(f"  Realized:    ${float(detail.get('RealizedProfitLoss', 0)):,.2f}", flush=True)
        print(f"  Unrealized:  ${float(detail.get('UnrealizedProfitLoss', 0)):,.2f}", flush=True)
        print(f"  DayTradeEx:  ${float(detail.get('DayTradeExcess', 0)):,.2f}", flush=True)
        # Margin calc
        bp = float(b.get('BuyingPower', 0))
        margin_per = 500.0
        buffer = bp - margin_per
        losses = int(buffer / 70) if buffer > 0 else 0
        can_trade = bp >= margin_per
        print(f"  Can Trade:   {'YES' if can_trade else 'NO (MARGIN BLOCKED)'}", flush=True)
        print(f"  Buffer:      ${buffer:,.2f}", flush=True)
        print(f"  Losses until blocked: {losses}", flush=True)
    else:
        print(f"  Balance error: {r.status_code} {r.text[:200]}", flush=True)

    # Positions
    r = requests.get(f'{BASE}/brokerage/accounts/{acct_id}/positions', headers=headers)
    positions = r.json().get('Positions', []) if r.status_code == 200 else []
    if positions:
        for p in positions:
            print(f"  POSITION: {p.get('LongShort')} {p.get('Quantity')} {p.get('Symbol')} "
                  f"avg={p.get('AveragePrice')} unrealized=${p.get('UnrealizedProfitLoss')}", flush=True)
    else:
        print(f"  Position:    FLAT", flush=True)

    # Open orders
    r = requests.get(f'{BASE}/brokerage/accounts/{acct_id}/orders', headers=headers)
    orders = r.json().get('Orders', []) if r.status_code == 200 else []
    open_orders = [o for o in orders if o.get('Status') in ('OPN', 'ACK', 'DON')]
    if open_orders:
        for o in open_orders:
            side = o.get('Legs', [{}])[0].get('BuyOrSell', '?')
            sym = o.get('Legs', [{}])[0].get('Symbol', '?')
            price = o.get('LimitPrice') or o.get('StopPrice') or '?'
            print(f"  ORDER: {o['OrderID']} {o['Status']} {side} {o.get('Type','')} "
                  f"{sym} @ {price}", flush=True)
    else:
        print(f"  Orders:      NONE", flush=True)

# Real trade orders from DB
print(f"\n{'='*50}", flush=True)
print(f"REAL TRADE HISTORY (from DB)", flush=True)
print(f"{'='*50}", flush=True)
import sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(sa.text("""
        SELECT setup_log_id, state FROM real_trade_orders ORDER BY setup_log_id
    """)).fetchall()
    for r in rows:
        s = json.loads(r.state) if isinstance(r.state, str) else r.state
        fill = s.get('fill_price', '?')
        stop_fill = s.get('stop_fill_price', '')
        tgt_fill = s.get('target_fill_price', '')
        close = s.get('close_reason', '')
        acct = s.get('account_id', '?')
        print(f"  #{r.setup_log_id} {s.get('setup_name','')} {s.get('direction',''):5s} "
              f"fill={fill} stop_fill={stop_fill} tgt_fill={tgt_fill} "
              f"close={close} acct={acct}", flush=True)
