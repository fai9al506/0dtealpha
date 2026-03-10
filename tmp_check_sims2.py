"""Check TS SIM accounts via API"""
import os, requests, json, sys

CID = os.environ.get('TS_CLIENT_ID', '')
CSEC = os.environ.get('TS_CLIENT_SECRET', '')
RTOK = os.environ.get('TS_REFRESH_TOKEN', '')
r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token', 'client_id': CID,
    'client_secret': CSEC, 'refresh_token': RTOK
})
token = r.json()['access_token']
headers = {'Authorization': 'Bearer ' + token}
BASE = 'https://sim-api.tradestation.com/v3'

for acct in ['SIM2609239F', 'SIM2609238M']:
    sys.stdout.write('\n' + '='*60 + '\n')
    sys.stdout.write('ACCOUNT: %s\n' % acct)
    sys.stdout.write('='*60 + '\n')

    # Raw balances dump
    r = requests.get(BASE + '/brokerage/accounts/' + acct + '/balances', headers=headers)
    if r.ok:
        b = r.json()
        sys.stdout.write('  RAW BALANCE:\n')
        sys.stdout.write('  %s\n' % json.dumps(b, indent=2)[:1500])

    # Positions
    r2 = requests.get(BASE + '/brokerage/accounts/' + acct + '/positions', headers=headers)
    if r2.ok:
        pos = r2.json()
        positions = pos.get('Positions', pos) if isinstance(pos, dict) else pos
        if isinstance(positions, list) and positions:
            sys.stdout.write('\n  POSITIONS:\n')
            for p in positions:
                sys.stdout.write('    %s\n' % json.dumps(p)[:300])
        else:
            sys.stdout.write('\n  No positions\n')

    # Orders
    r3 = requests.get(BASE + '/brokerage/accounts/' + acct + '/orders', headers=headers)
    if r3.ok:
        data = r3.json()
        orders = data.get('Orders', data) if isinstance(data, dict) else data
        if isinstance(orders, list) and orders:
            # Sort by time
            orders.sort(key=lambda o: o.get('OpenedDateTime', ''))
            sys.stdout.write('\n  ORDERS (%d total):\n' % len(orders))
            for o in orders:
                legs = o.get('Legs', [{}])
                leg = legs[0] if legs else {}
                sym = leg.get('Symbol', '?')
                bs = leg.get('BuyOrSell', '?')
                qty = leg.get('QuantityOrdered', '?')
                filled = o.get('FilledPrice', '?')
                status = o.get('StatusDescription', '?')
                otype = o.get('OrderType', '?')
                opened = o.get('OpenedDateTime', '?')[:19]
                comm = o.get('CommissionFee', '0')
                sys.stdout.write('    %s %-10s %-4s %-25s qty=%-3s filled=%-10s %-10s comm=%s\n' % (
                    opened, otype, bs, sym, qty, filled, status, comm))
        else:
            sys.stdout.write('\n  No orders\n')

sys.stdout.flush()
