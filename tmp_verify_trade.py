"""Verify Skew Charm 09:48 trade — portal vs SIM"""
import os, sys, requests, json
from sqlalchemy import create_engine, text

# 1. Portal data
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

sys.stdout.write('='*70 + '\n')
sys.stdout.write('PORTAL: Skew Charm 09:48 ET on Mar 10\n')
sys.stdout.write('='*70 + '\n')

trade = c.execute(text("""
    SELECT id, ts, setup_name, direction, grade, score, spot,
           outcome_result, outcome_pnl,
           outcome_target_level, outcome_stop_level,
           outcome_max_profit, outcome_max_loss,
           outcome_first_event, outcome_elapsed_min,
           greek_alignment, paradigm, lis, target
    FROM setup_log
    WHERE ts::date = '2026-03-10'
      AND setup_name = 'Skew Charm'
      AND direction = 'long'
    ORDER BY ts
    LIMIT 5
""")).fetchall()

for t in trade:
    sys.stdout.write('  ID: %s\n' % t[0])
    sys.stdout.write('  Time: %s\n' % t[1])
    sys.stdout.write('  Direction: %s  Grade: %s  Score: %s\n' % (t[3], t[4], t[5]))
    sys.stdout.write('  SPX Spot: %s\n' % t[6])
    sys.stdout.write('  Result: %s  PnL: %s pts\n' % (t[7], t[8]))
    sys.stdout.write('  Target level: %s  Stop level: %s\n' % (t[9], t[10]))
    sys.stdout.write('  Max profit: %s  Max loss: %s\n' % (t[11], t[12]))
    sys.stdout.write('  First event: %s  Elapsed: %s min\n' % (t[13], t[14]))
    sys.stdout.write('  Alignment: %s  Paradigm: %s\n' % (t[15], t[16]))
    sys.stdout.write('  LIS: %s  Target: %s\n' % (t[17], t[18]))
    sys.stdout.write('\n')

c.close()

# 2. TS SIM orders
sys.stdout.write('='*70 + '\n')
sys.stdout.write('TS SIM: Futures SIM2609239F orders\n')
sys.stdout.write('='*70 + '\n')

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

r = requests.get(BASE + '/brokerage/accounts/SIM2609239F/orders', headers=headers,
                 params={'pageSize': 50})
data = r.json()
orders = data.get('Orders', data) if isinstance(data, dict) else data
orders.sort(key=lambda o: o.get('OpenedDateTime', ''))

for o in orders:
    legs = o.get('Legs', [{}])
    leg = legs[0] if legs else {}
    sys.stdout.write('  %s  %-12s %-4s %-15s qty=%-3s filled=%-10s %-10s comm=%s\n' % (
        o.get('OpenedDateTime', '?')[:19],
        o.get('OrderType', '?'),
        leg.get('BuyOrSell', '?'),
        leg.get('Symbol', '?'),
        leg.get('QuantityOrdered', '?'),
        o.get('FilledPrice', '?'),
        o.get('StatusDescription', '?'),
        o.get('CommissionFee', '0')))

# Also check options SIM
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('TS SIM: Options SIM2609238M orders\n')
sys.stdout.write('='*70 + '\n')

r2 = requests.get(BASE + '/brokerage/accounts/SIM2609238M/orders', headers=headers,
                  params={'pageSize': 50})
data2 = r2.json()
orders2 = data2.get('Orders', data2) if isinstance(data2, dict) else data2
orders2.sort(key=lambda o: o.get('OpenedDateTime', ''))

for o in orders2:
    legs = o.get('Legs', [{}])
    leg = legs[0] if legs else {}
    sys.stdout.write('  %s  %-12s %-4s %-25s qty=%-3s filled=%-10s %-10s comm=%s\n' % (
        o.get('OpenedDateTime', '?')[:19],
        o.get('OrderType', '?'),
        leg.get('BuyOrSell', '?'),
        leg.get('Symbol', '?'),
        leg.get('QuantityOrdered', '?'),
        o.get('FilledPrice', '?'),
        o.get('StatusDescription', '?'),
        o.get('CommissionFee', '0')))

sys.stdout.flush()
