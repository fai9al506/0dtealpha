import requests, json, sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 1) Get TS orders
r = requests.get('https://0dtealpha.com/api/debug/sim-orders', timeout=30)
data = r.json()
opt = data.get('options_sim', {})
orders = opt.get('todays_orders', [])
filled = [o for o in orders if o.get('Status') == 'FLL']
filled.sort(key=lambda x: x.get('ClosedDateTime') or x.get('OpenedDateTime') or '')

# 2) Get our DB options log
r2 = requests.get('https://0dtealpha.com/api/options/log', timeout=30)
db_raw = r2.text
try:
    db_trades = r2.json()
    if isinstance(db_trades, str):
        db_trades = json.loads(db_trades)
except:
    db_trades = []
    print(f"DB response type: {type(r2.json())}, first 200 chars: {db_raw[:200]}")

# 3) Pair buys and sells FIFO per symbol
from datetime import datetime
buys_by_sym = {}
sells_by_sym = {}
for o in filled:
    sym = o.get('Symbol', '')
    side = o.get('Side', '')
    if side in ('Buy', 'BuyToOpen'):
        buys_by_sym.setdefault(sym, []).append(o)
    elif side in ('Sell', 'SellToClose'):
        sells_by_sym.setdefault(sym, []).append(o)

all_symbols = sorted(set(list(buys_by_sym.keys()) + list(sells_by_sym.keys())))

paired = []
still_open = []
for sym in all_symbols:
    buy_list = buys_by_sym.get(sym, [])
    sell_list = sells_by_sym.get(sym, [])
    for i, b in enumerate(buy_list):
        buy_px = float(b.get('FilledPrice', b.get('AvgFillPrice', 0)))
        buy_time = (b.get('ClosedDateTime') or b.get('OpenedDateTime') or '')[:19]
        buy_limit = b.get('LimitPrice', '')
        if i < len(sell_list):
            s = sell_list[i]
            sell_px = float(s.get('FilledPrice', s.get('AvgFillPrice', 0)))
            sell_time = (s.get('ClosedDateTime') or s.get('OpenedDateTime') or '')[:19]
            sell_limit = s.get('LimitPrice', '')
            pnl = (sell_px - buy_px) * 100
            try:
                bt = datetime.strptime(buy_time, '%Y-%m-%dT%H:%M:%S')
                st = datetime.strptime(sell_time, '%Y-%m-%dT%H:%M:%S')
                hold_min = (st - bt).total_seconds() / 60
            except:
                hold_min = 0
            paired.append({
                'sym': sym, 'buy_px': buy_px, 'buy_time': buy_time, 'buy_limit': buy_limit,
                'sell_px': sell_px, 'sell_time': sell_time, 'sell_limit': sell_limit,
                'pnl': pnl, 'hold_min': hold_min,
                'result': 'WIN' if pnl > 0 else 'LOSS' if pnl < 0 else 'FLAT'
            })
        else:
            still_open.append({'sym': sym, 'buy_px': buy_px, 'buy_time': buy_time})

# Sort paired by buy_time
paired.sort(key=lambda x: x['buy_time'])

# 4) Now get DB trades for delta comparison
# Find matching DB trades by setup_log_id
print("=" * 140)
print("OPTIONS TRADES - TS STATEMENT (Today 2026-03-13)")
print("=" * 140)
print(f"{'#':<4} {'Symbol':<18} {'Entry':<8} {'Exit':<8} {'P&L':<8} {'Hold':<8} {'Result':<6} {'BuyTime':<20} {'SellTime':<20} {'BuyLmt':<8} {'SellLmt':<8}")
print("-" * 140)

total_pnl = 0
wins = 0
losses = 0
for i, t in enumerate(paired, 1):
    total_pnl += t['pnl']
    if t['result'] == 'WIN': wins += 1
    elif t['result'] == 'LOSS': losses += 1
    hold = f"{t['hold_min']:.0f}m"
    bl = f"${float(t['buy_limit']):.2f}" if t['buy_limit'] else "---"
    sl = f"${float(t['sell_limit']):.2f}" if t['sell_limit'] else "---"
    print(f"{i:<4} {t['sym']:<18} ${t['buy_px']:<7.2f} ${t['sell_px']:<7.2f} ${t['pnl']:+6.0f}  {hold:<8} {t['result']:<6} {t['buy_time']:<20} {t['sell_time']:<20} {bl:<8} {sl:<8}")

print("-" * 140)
print(f"TOTAL: {len(paired)} trades | Wins: {wins} | Losses: {losses} | WR: {wins/(wins+losses)*100:.0f}% | Realized P&L: ${total_pnl:+,.0f}")

if still_open:
    print(f"\nStill Open ({len(still_open)}):")
    for s in still_open:
        print(f"  1x {s['sym']} @ ${s['buy_px']:.2f} (bought {s['buy_time']})")

# 5) CRITICAL CHECK: Sell fill vs sell limit
print("\n\n" + "=" * 140)
print("SELL FILL vs LIMIT CHECK (detecting unrealistic SIM fills)")
print("=" * 140)
suspicious = 0
for t in paired:
    if t['sell_limit']:
        sl = float(t['sell_limit'])
        if t['sell_px'] > sl * 2:
            suspicious += 1
            print(f"  SUSPICIOUS: {t['sym']} sold at ${t['sell_px']:.2f} but limit was ${sl:.2f} (fill is {t['sell_px']/sl:.1f}x limit) -- P&L impact: ${(t['sell_px'] - sl)*100:+.0f} vs ${(sl - t['buy_px'])*100:+.0f} at limit")

if suspicious == 0:
    print("  All fills look reasonable.")
else:
    print(f"\n  {suspicious} suspicious fills found. SIM may be filling at market price instead of limit.")
    # Calculate what P&L would be at limit prices
    adj_pnl = 0
    for t in paired:
        if t['sell_limit']:
            sl = float(t['sell_limit'])
            if t['sell_px'] > sl * 2:
                # Use limit price instead
                adj_pnl += (sl - t['buy_px']) * 100
            else:
                adj_pnl += t['pnl']
        else:
            adj_pnl += t['pnl']
    print(f"  Reported P&L: ${total_pnl:+,.0f} | If fills were at limit: ${adj_pnl:+,.0f} | Difference: ${total_pnl - adj_pnl:+,.0f}")

# 6) TS balance for verification
bal = opt.get('balance', {})
detail = bal.get('BalanceDetail', {})
print(f"\nTS API RealizedProfitLoss: ${detail.get('RealizedProfitLoss', '?')}")
print(f"Our FIFO calc:            ${total_pnl:+,.2f}")
diff = total_pnl - float(detail.get('RealizedProfitLoss', 0))
if abs(diff) > 1:
    print(f"MISMATCH: ${diff:+,.2f} difference -- TS may use different pairing method")

# 7) DB comparison
print("\n\n" + "=" * 140)
print("DB OPTIONS LOG (our system's records)")
print("=" * 140)
if isinstance(db_trades, list) and len(db_trades) > 0:
    today_db = [t for t in db_trades if isinstance(t, dict) and t.get('ts', '').startswith('2026-03-13')]
    print(f"DB trades today: {len(today_db)}")
    print(f"{'#':<4} {'ID':<6} {'Setup':<18} {'Symbol':<18} {'Dir':<6} {'SimIn':<8} {'SimOut':<8} {'SimP&L':<8} {'TheoIn':<8} {'TheoOut':<8} {'TheoP&L':<8} {'Status':<8}")
    print("-" * 120)
    for i, t in enumerate(today_db, 1):
        sid = str(t.get('setup_log_id', ''))[:5]
        setup = (t.get('setup_name', '') or '')[:16]
        sym = (t.get('symbol', '') or '')[:16]
        d = t.get('direction', '')[:4]
        si = f"${t['sim_entry_price']:.2f}" if t.get('sim_entry_price') else "---"
        so = f"${t['sim_exit_price']:.2f}" if t.get('sim_exit_price') else "---"
        sp = f"${t['sim_pnl']:+.0f}" if t.get('sim_pnl') is not None else "---"
        ti = f"${t['theo_entry_price']:.2f}" if t.get('theo_entry_price') else "---"
        to_ = f"${t['theo_exit_price']:.2f}" if t.get('theo_exit_price') else "---"
        tp = f"${t['theo_pnl']:+.0f}" if t.get('theo_pnl') is not None else "---"
        st = t.get('status', '')
        print(f"{i:<4} {sid:<6} {setup:<18} {sym:<18} {d:<6} {si:<8} {so:<8} {sp:<8} {ti:<8} {to_:<8} {tp:<8} {st:<8}")
else:
    print(f"Could not parse DB trades. Response type: {type(db_trades)}")
    if isinstance(db_trades, list) and len(db_trades) > 0:
        print(f"First item type: {type(db_trades[0])}, value: {str(db_trades[0])[:200]}")
    else:
        print(f"Raw: {db_raw[:500]}")

print("\nDone.")
