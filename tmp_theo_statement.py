import requests, json, sys, time
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Wait for deploy then pull
for attempt in range(6):
    try:
        r = requests.get('https://0dtealpha.com/api/debug/sim-orders', timeout=30)
        data = r.json()
        if 'options_db' in data:
            break
        print(f"Attempt {attempt+1}: no options_db yet, waiting...")
        time.sleep(15)
    except Exception as e:
        print(f"Attempt {attempt+1}: {e}")
        time.sleep(15)

if 'options_db' not in data:
    print("ERROR: options_db not available after deploy. Check logs.")
    sys.exit(1)

db_trades = data['options_db']
if isinstance(db_trades, dict) and 'error' in db_trades:
    print(f"DB ERROR: {db_trades['error']}")
    sys.exit(1)

print("=" * 130)
print("TODAY'S OPTIONS TRADES - THEORETICAL P&L (Live API Prices)")
print("=" * 130)
print(f"{'#':<4} {'Setup':<18} {'Symbol':<18} {'Dir':<6} {'TheoIn':<8} {'TheoOut':<8} {'ThP&L':<8} {'SimIn':<8} {'SimOut':<8} {'SimP&L':<8} {'Delta':<6} {'Time':<16} {'Status':<8}")
print("-" * 130)

total_theo_pnl = 0
total_sim_pnl = 0
theo_count = 0
sim_count = 0
wins = 0
losses = 0
flat = 0

for i, t in enumerate(db_trades, 1):
    setup = (t.get('setup_name') or '')[:16]
    sym = (t.get('symbol') or '')[:16]
    direction = (t.get('direction') or '')[:4]
    status = (t.get('status') or '')[:8]
    qty = int(t.get('qty') or 1)
    delta = t.get('delta_at_entry') or ''

    # Theo prices
    theo_in = float(t['theo_entry']) if t.get('theo_entry') else None
    if not theo_in:
        theo_in = float(t['ask_at_entry']) if t.get('ask_at_entry') else None
    theo_out = float(t['theo_exit']) if t.get('theo_exit') else None

    # SIM prices
    sim_in = float(t['sim_entry']) if t.get('sim_entry') else None
    sim_out = float(t['sim_exit']) if t.get('sim_exit') else None

    # Calculate P&L
    theo_pnl = None
    if theo_in and theo_out:
        theo_pnl = (theo_out - theo_in) * 100 * qty
        total_theo_pnl += theo_pnl
        theo_count += 1
        if theo_pnl > 0: wins += 1
        elif theo_pnl < 0: losses += 1
        else: flat += 1

    sim_pnl = None
    if sim_in and sim_out:
        sim_pnl = (sim_out - sim_in) * 100 * qty
        total_sim_pnl += sim_pnl
        sim_count += 1

    # Format
    ti_s = f"${theo_in:.2f}" if theo_in else "---"
    to_s = f"${theo_out:.2f}" if theo_out else "---"
    tp_s = f"${theo_pnl:+.0f}" if theo_pnl is not None else "---"
    si_s = f"${sim_in:.2f}" if sim_in else "---"
    so_s = f"${sim_out:.2f}" if sim_out else "---"
    sp_s = f"${sim_pnl:+.0f}" if sim_pnl is not None else "---"
    d_s = f"{float(delta):.2f}" if delta else "---"
    ts = (t.get('ts_placed') or '')[:16]

    print(f"{i:<4} {setup:<18} {sym:<18} {direction:<6} {ti_s:<8} {to_s:<8} {tp_s:<8} {si_s:<8} {so_s:<8} {sp_s:<8} {d_s:<6} {ts:<16} {status:<8}")

print("-" * 130)
wr = f"{wins/(wins+losses)*100:.0f}%" if (wins+losses) > 0 else "--"
print(f"TOTAL: {len(db_trades)} trades | Closed: {theo_count} | Wins: {wins} | Losses: {losses} | WR: {wr}")
print(f"  Theo P&L: ${total_theo_pnl:+,.0f} (REAL market prices)")
print(f"  SIM P&L:  ${total_sim_pnl:+,.0f} (unreliable SIM fills)")
print(f"  Gap:      ${total_sim_pnl - total_theo_pnl:+,.0f} (SIM inflation)")

# Open positions
open_trades = [t for t in db_trades if t.get('status') == 'filled']
if open_trades:
    print(f"\nOpen Positions ({len(open_trades)}):")
    for t in open_trades:
        sym = t.get('symbol', '')
        theo_in = float(t['theo_entry']) if t.get('theo_entry') else (float(t['ask_at_entry']) if t.get('ask_at_entry') else None)
        ti_s = f"${theo_in:.2f}" if theo_in else "?"
        print(f"  {t.get('qty',1)}x {sym} theo_entry={ti_s}")
