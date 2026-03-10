"""Full SIM analysis - actual PnL from TS API orders"""
import os, requests

CID = os.environ.get("TS_CLIENT_ID", "")
CSEC = os.environ.get("TS_CLIENT_SECRET", "")
RTOK = os.environ.get("TS_REFRESH_TOKEN", "")
r = requests.post("https://signin.tradestation.com/oauth/token", data={
    "grant_type": "refresh_token", "client_id": CID,
    "client_secret": CSEC, "refresh_token": RTOK
})
token = r.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}
BASE = "https://sim-api.tradestation.com/v3"

# ============================================================
# FUTURES SIM - Proper trade-by-trade PnL
# ============================================================
print("=" * 70)
print("FUTURES SIM (SIM2609239F)")
print("  Balance: $46,628.50 | RealizedPnL: -$3,371.50 | Commission: $104")
print("=" * 70)

r = requests.get(f"{BASE}/brokerage/accounts/SIM2609239F/orders", headers=headers,
                 params={"pageSize": 200})
data = r.json()
orders = data.get('Orders', data) if isinstance(data, dict) else data
orders.sort(key=lambda o: o.get('OpenedDateTime', ''))

# Group: each Market order starts a trade, followed by Stop/Limit exits
trades = []
i = 0
while i < len(orders):
    o = orders[i]
    legs = o.get('Legs', [{}])
    leg = legs[0] if legs else {}
    if o.get('OrderType') == 'Market' and o.get('StatusDescription') == 'Filled':
        entry_price = float(o.get('FilledPrice', 0))
        entry_qty = int(leg.get('QuantityOrdered', 0))
        direction = 'LONG' if leg.get('BuyOrSell') == 'Buy' else 'SHORT'
        time = o.get('OpenedDateTime', '')[:16]
        comm = float(o.get('CommissionFee', 0))
        exits = []

        # Collect subsequent non-Market orders as exits
        j = i + 1
        while j < len(orders) and orders[j].get('OrderType') != 'Market':
            eo = orders[j]
            el = eo.get('Legs', [{}])
            eleg = el[0] if el else {}
            exits.append({
                'type': eo.get('OrderType', ''),
                'price': float(eo.get('FilledPrice', 0)),
                'qty': int(eleg.get('QuantityOrdered', 0)),
                'status': eo.get('StatusDescription', ''),
                'comm': float(eo.get('CommissionFee', 0))
            })
            comm += float(eo.get('CommissionFee', 0))
            j += 1

        # Calculate PnL
        pnl_pts = 0
        for ex in exits:
            if ex['status'] != 'Filled':
                continue
            if direction == 'LONG':
                pnl_pts += (ex['price'] - entry_price) * ex['qty']
            else:
                pnl_pts += (entry_price - ex['price']) * ex['qty']

        pnl_dollars = pnl_pts * 5  # MES = $5/pt
        net = pnl_dollars - comm
        result = 'WIN' if net > 0 else 'LOSS'

        print(f"  {time}  {direction:5s}  entry={entry_price:.2f}  pnl={pnl_pts:+.1f}pts  ${pnl_dollars:+.0f}  comm=${comm:.0f}  net=${net:+.0f}  {result}")

        trades.append({'time': time, 'dir': direction, 'net': net, 'pnl_pts': pnl_pts, 'comm': comm})
        i = j
    else:
        i += 1

fut_total = sum(t['net'] for t in trades)
fut_wins = sum(1 for t in trades if t['net'] > 0)
fut_losses = len(trades) - fut_wins
print(f"\n  TOTAL: ${fut_total:+.0f}  ({fut_wins}W/{fut_losses}L, {len(trades)} trades)")

# ============================================================
# OPTIONS SIM - Proper trade-by-trade PnL
# ============================================================
print(f"\n{'='*70}")
print("OPTIONS SIM (SIM2609238M)")
print("  Balance: $52,859 | RealizedPnL: +$2,859 | Commission: ~$24")
print("=" * 70)

r2 = requests.get(f"{BASE}/brokerage/accounts/SIM2609238M/orders", headers=headers,
                  params={"pageSize": 200})
data2 = r2.json()
orders2 = data2.get('Orders', data2) if isinstance(data2, dict) else data2
orders2.sort(key=lambda o: o.get('OpenedDateTime', ''))

# Match buys to sells by symbol (chronological order)
filled = [o for o in orders2 if o.get('StatusDescription') == 'Filled']
open_positions = {}  # symbol -> list of buys
opt_trades = []
running_pnl = 0
peak = 0
max_dd = 0

for o in filled:
    legs = o.get('Legs', [{}])
    leg = legs[0] if legs else {}
    bs = leg.get('BuyOrSell', '')
    sym = leg.get('Symbol', '')
    price = float(o.get('FilledPrice', 0))
    time = o.get('OpenedDateTime', '')[:16]
    comm = float(o.get('CommissionFee', 0))

    if bs == 'Buy':
        if sym not in open_positions:
            open_positions[sym] = []
        open_positions[sym].append({'time': time, 'price': price, 'comm': comm})
    elif bs == 'Sell' and sym in open_positions and open_positions[sym]:
        buy = open_positions[sym].pop(0)
        pnl = (price - buy['price']) * 100  # SPXW multiplier
        total_comm = buy['comm'] + comm
        net = pnl - total_comm
        result = 'WIN' if net > 0 else 'LOSS'
        running_pnl += net
        if running_pnl > peak:
            peak = running_pnl
        dd = peak - running_pnl
        if dd > max_dd:
            max_dd = dd

        print(f"  {buy['time']}  {sym:25s}  buy=${buy['price']:.2f}  sell=${price:.2f}  pnl=${pnl:+.0f}  net=${net:+.0f}  {result}  running=${running_pnl:+.0f}")
        opt_trades.append({'net': net, 'pnl': pnl})

opt_total = sum(t['net'] for t in opt_trades)
opt_wins = sum(1 for t in opt_trades if t['net'] > 0)
opt_losses = len(opt_trades) - opt_wins
print(f"\n  TOTAL: ${opt_total:+.0f}  ({opt_wins}W/{opt_losses}L, {len(opt_trades)} trades)")
print(f"  Peak: ${peak:+.0f}  |  Max Drawdown: ${max_dd:.0f}")

# ============================================================
# SUMMARY
# ============================================================
print(f"\n{'='*70}")
print("SUMMARY")
print("=" * 70)
print(f"  Futures SIM:  ${fut_total:+,.0f}  ({fut_wins}W/{fut_losses}L)")
print(f"  Options SIM:  ${opt_total:+,.0f}  ({opt_wins}W/{opt_losses}L)")
print(f"  Options MaxDD: ${max_dd:,.0f}")
print(f"\n  Portal (all signals): -16.1 pts = theoretical")
print(f"  Futures SIM (13 trades): -$3,372 = ACTUAL")
print(f"  Gap: portal tracks all 32 signals independently,")
print(f"       auto-trader only executed 13 (stacking/blocking)")
