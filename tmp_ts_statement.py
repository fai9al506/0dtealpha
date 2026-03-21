import requests, json, sys

url = 'https://0dtealpha.com/api/debug/sim-orders'
try:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
except Exception as e:
    print(f"ERROR fetching: {e}")
    sys.exit(1)

with open('sim_statement_latest.json', 'w') as f:
    json.dump(data, f, indent=2)

def show_account(label, acct):
    print("=" * 60)
    print(label)
    print("=" * 60)

    bal = acct.get('balance', {})
    bod_list = acct.get('bod_balance', [])
    bod = bod_list[0] if bod_list else {}

    # BOD balance is nested in BalanceDetail
    bod_detail = bod.get('BalanceDetail', {})
    bod_equity = bod_detail.get('AccountBalance', bod_detail.get('RealTimeAccountBalance', '?'))

    print(f"BOD Equity:      ${bod_equity}")
    print(f"Current Equity:  ${bal.get('Equity', '?')}")
    print(f"TodaysPnL:       ${bal.get('TodaysProfitLoss', '?')}")

    # Commission
    comm = bal.get('Commission', '?')
    print(f"Commission:      ${comm}")

    # Balance detail
    detail = bal.get('BalanceDetail', {})
    if detail:
        for k in ['RealizedProfitLoss', 'UnrealizedProfitLoss', 'RealTimeAccountBalance', 'TodayRealTimeTradeEquity']:
            if k in detail:
                print(f"  {k}: ${detail[k]}")
    print()

    # Positions
    positions = acct.get('positions', [])
    if positions:
        print("Open Positions:")
        for p in positions:
            print(f"  {p.get('Qty','?')} {p.get('Symbol','?')} @ {p.get('AvgPrice','?')} | UnrlzPnL: ${p.get('UnrealizedPnL','?')} | Last: {p.get('Last','?')}")
        print()

    # Orders
    orders = acct.get('todays_orders', [])
    filled = [o for o in orders if o.get('Status') == 'FLL']
    cancelled = [o for o in orders if o.get('Status') == 'CAN']
    active = [o for o in orders if o.get('Status') not in ('FLL', 'CAN', 'REJ', 'EXP')]

    print(f"Total Orders: {len(orders)} | Filled: {len(filled)} | Cancelled: {len(cancelled)} | Active: {len(active)}")
    print()

    if filled:
        print(f"{'ID':<8} {'Closed':<22} {'Side':<6} {'Qty':<5} {'Symbol':<32} {'Type':<10} {'FillPx':<10} {'Limit':<10} {'Stop':<10}")
        print("-" * 125)
        for o in sorted(filled, key=lambda x: x.get('ClosedDateTime') or x.get('OpenedDateTime') or ''):
            oid = str(o.get('OrderID', ''))
            t = (o.get('ClosedDateTime') or o.get('OpenedDateTime') or '')[:19]
            side = o.get('Side', '')
            qty = o.get('QtyFilled', o.get('QtyOrdered', ''))
            sym = o.get('Symbol', '')
            otype = o.get('Type', '')
            fill = o.get('FilledPrice', o.get('AvgFillPrice', ''))
            limit_px = o.get('LimitPrice', '') or ''
            stop_px = o.get('StopPrice', '') or ''
            print(f"{oid:<8} {t:<22} {side:<6} {qty:<5} {sym:<32} {otype:<10} {fill:<10} {limit_px:<10} {stop_px:<10}")

    if active:
        print(f"\nActive Orders:")
        for o in active:
            print(f"  ID={o.get('OrderID','')} {o.get('Side','')} {o.get('QtyOrdered','')} {o.get('Symbol','')} {o.get('Type','')} Limit={o.get('LimitPrice','')} Stop={o.get('StopPrice','')} Status={o.get('Status','')}/{o.get('StatusDesc','')}")

    print()


show_account("FUTURES SIM (SIM2609239F)", data.get('futures_sim', {}))
print()
show_account("OPTIONS SIM (SIM2609238M)", data.get('options_sim', {}))

print("\nDone. Full JSON saved to sim_statement_latest.json")
