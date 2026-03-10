"""Skew Charm with 0.30 delta OTM options — the sweet spot for quick winners"""
import os, sys, json, bisect
sys.stdout.reconfigure(encoding='utf-8')
import sqlalchemy as sa
from datetime import timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

CALL_BID, CALL_ASK, CALL_DELTA, CALL_GAMMA = 5, 7, 4, 3
PUT_BID, PUT_ASK, PUT_DELTA, PUT_GAMMA = 14, 12, 16, 17
CALL_IV, PUT_IV = 2, 18
STRIKE_IDX = 10

with engine.connect() as c:
    # Skew Charm trades only
    trades = c.execute(sa.text("""
        SELECT id, setup_name, direction, spot, target,
               outcome_result, outcome_pnl, ts, outcome_elapsed_min,
               outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE outcome_result IN ('WIN','LOSS') AND setup_name = 'Skew Charm'
        ORDER BY ts
    """)).fetchall()
    trades = [dict(r._mapping) for r in trades]
    print(f"Skew Charm trades: {len(trades)}")

    # Load chain snapshots
    print("Loading chain snapshots...")
    snaps = c.execute(sa.text("""
        SELECT ts, rows FROM chain_snapshots
        WHERE ts::date >= '2026-02-03' ORDER BY ts
    """)).fetchall()
    snaps = [(r[0], json.loads(r[1]) if isinstance(r[1], str) else r[1]) for r in snaps]
    snap_times = [s[0] for s in snaps]
    print(f"Snapshots: {len(snaps)}")

def find_snap(ts):
    idx = bisect.bisect_left(snap_times, ts)
    best, best_dt = None, 301
    for i in [idx-1, idx]:
        if 0 <= i < len(snap_times):
            dt = abs((snap_times[i] - ts).total_seconds())
            if dt < best_dt:
                best_dt = dt
                best = snaps[i][1]
    return best

def find_option_by_delta(rows, direction, target_delta):
    """Find option closest to target delta.
    LONG signal -> buy OTM call (delta ~0.30)
    SHORT signal -> buy OTM put (delta ~-0.30, abs ~0.30)
    """
    candidates = []
    for row in rows:
        strike = row[STRIKE_IDX]
        if strike is None:
            continue
        if direction == 'long':
            bid, ask = row[CALL_BID] or 0, row[CALL_ASK] or 0
            delta = row[CALL_DELTA] or 0
            gamma = row[CALL_GAMMA] or 0
        else:
            bid, ask = row[PUT_BID] or 0, row[PUT_ASK] or 0
            delta = row[PUT_DELTA] or 0
            gamma = row[PUT_GAMMA] or 0

        if bid > 0 and ask > 0 and abs(delta) > 0.05:
            candidates.append({
                'strike': strike, 'bid': bid, 'ask': ask,
                'delta': abs(delta), 'raw_delta': delta, 'gamma': gamma,
                'dist': abs(abs(delta) - target_delta)
            })

    if not candidates:
        return None
    candidates.sort(key=lambda x: x['dist'])
    return candidates[0]

def find_atm(rows, direction):
    """ATM option for comparison"""
    best, best_dist = None, 999999
    for row in rows:
        strike = row[STRIKE_IDX]
        if strike is None:
            continue
        # ATM = closest to 0.50 delta
        if direction == 'long':
            delta = abs(row[CALL_DELTA] or 0)
            bid, ask = row[CALL_BID] or 0, row[CALL_ASK] or 0
            gamma = row[CALL_GAMMA] or 0
        else:
            delta = abs(row[PUT_DELTA] or 0)
            bid, ask = row[PUT_BID] or 0, row[PUT_ASK] or 0
            gamma = row[PUT_GAMMA] or 0

        dist = abs(delta - 0.50)
        if bid > 0 and ask > 0 and dist < best_dist:
            best = {'strike': strike, 'bid': bid, 'ask': ask, 'delta': delta, 'gamma': gamma}
            best_dist = dist
    return best

# Process each Skew Charm trade at multiple deltas
deltas_to_test = [0.50, 0.40, 0.30, 0.20, 0.15, 0.10]
all_results = {d: [] for d in deltas_to_test}

for t in trades:
    snap_rows = find_snap(t['ts'])
    if not snap_rows:
        continue

    spot = float(t['spot'])
    direction = t['direction']
    outcome = t['outcome_result']
    pts = float(t['outcome_pnl']) if t['outcome_pnl'] else 0
    max_profit = float(t['outcome_max_profit']) if t['outcome_max_profit'] else 0
    hold_min = float(t['outcome_elapsed_min']) if t['outcome_elapsed_min'] else 15
    move = abs(pts)

    for target_d in deltas_to_test:
        opt = find_option_by_delta(snap_rows, direction, target_d)
        if not opt or opt['ask'] <= 0.10:
            continue

        entry = opt['ask']
        delta = opt['delta']
        gamma = opt['gamma']
        spread_cost = (opt['ask'] - opt['bid']) / 2

        # Gamma-adjusted P&L
        gamma_boost = 0.5 * gamma * move * move

        # Theta (rough, based on time)
        et_hour = (t['ts'].hour - 5) % 24
        if et_hour < 11: theta_rate = 0.015
        elif et_hour < 13: theta_rate = 0.025
        elif et_hour < 14: theta_rate = 0.04
        else: theta_rate = 0.06
        theta = theta_rate * entry * (hold_min / 60)

        if outcome == 'WIN':
            # OTM options benefit MORE from gamma on winners (convexity!)
            # As price moves toward strike, delta increases -> accelerating gains
            option_gain = delta * move + gamma_boost - theta - spread_cost
            option_pnl = max(option_gain, -entry) * 100  # floor at -premium

            # % return on premium
            pct_return = option_gain / entry * 100 if entry > 0 else 0
        else:
            raw_loss = delta * move - gamma_boost + theta + spread_cost
            option_pnl = -min(raw_loss, entry) * 100
            pct_return = -min(raw_loss, entry) / entry * 100 if entry > 0 else 0

        all_results[target_d].append({
            'id': t['id'], 'outcome': outcome, 'pts': pts,
            'strike': opt['strike'], 'delta': delta, 'entry': entry,
            'option_pnl': option_pnl, 'pct_return': pct_return,
            'hold_min': hold_min, 'futures_pnl': pts * 50,
            'capital': entry * 100, 'max_profit_pts': max_profit,
            'direction': direction, 'spot': spot,
        })

print()
print("=" * 85)
print("  SKEW CHARM — OPTIONS BY DELTA (0DTE SPX)")
print("=" * 85)
print(f"  Skew Charm profile: 95.8% WR, quick winners (avg ~7 pts), immediate resolution")
print()

# ─── Comparison table ───
print("  DELTA COMPARISON (per contract)")
print("  " + "-" * 78)
print(f"  {'Delta':>6} {'Entry$':>8} {'#':>4} {'WR':>6} {'TotPnL':>10} {'AvgPnL':>8} {'AvgWin%':>8} {'AvgLoss%':>9} {'PF':>6}")
print("  " + "-" * 78)

# Add futures row first
fut_tot = sum(r['futures_pnl'] for r in all_results[0.50])
fut_wins = [r for r in all_results[0.50] if r['outcome'] == 'WIN']
fut_losses = [r for r in all_results[0.50] if r['outcome'] == 'LOSS']
wr = len(fut_wins) / len(all_results[0.50]) * 100 if all_results[0.50] else 0
gw = sum(r['futures_pnl'] for r in fut_wins)
gl = abs(sum(r['futures_pnl'] for r in fut_losses))
pf = gw / gl if gl else 999
print(f"  {'FUT':>6} {'$15900':>8} {len(all_results[0.50]):>4} {wr:>5.1f}% ${fut_tot:>+8,.0f} ${fut_tot/len(all_results[0.50]) if all_results[0.50] else 0:>+6,.0f}   {'N/A':>7}   {'N/A':>8} {pf:>5.1f}x")

for d in deltas_to_test:
    res = all_results[d]
    if not res:
        continue
    w = [r for r in res if r['outcome'] == 'WIN']
    l = [r for r in res if r['outcome'] == 'LOSS']
    tot = sum(r['option_pnl'] for r in res)
    avg = tot / len(res) if res else 0
    wr = len(w) / len(res) * 100 if res else 0
    avg_entry = sum(r['entry'] for r in res) / len(res)
    avg_win_pct = sum(r['pct_return'] for r in w) / len(w) if w else 0
    avg_loss_pct = sum(r['pct_return'] for r in l) / len(l) if l else 0
    gw = sum(r['option_pnl'] for r in w)
    gl = abs(sum(r['option_pnl'] for r in l))
    pf = gw / gl if gl else 999
    print(f"  {d:>5.2f}d ${avg_entry*100:>6,.0f} {len(res):>4} {wr:>5.1f}% ${tot:>+8,.0f} ${avg:>+6,.0f} {avg_win_pct:>+6.0f}%  {avg_loss_pct:>+7.0f}% {pf:>5.1f}x")

print()

# ─── Capital efficiency ───
print("  CAPITAL EFFICIENCY (same $15,900 budget)")
print("  " + "-" * 78)
print(f"  {'Delta':>6} {'$/Contract':>11} {'Contracts':>10} {'Total P&L':>12} {'vs Futures':>11} {'Monthly':>12}")
print("  " + "-" * 78)

print(f"  {'FUT':>6} {'$15,900':>11} {'1':>10} ${fut_tot:>+10,.0f} {'baseline':>11} ${fut_tot/21*21:>+10,.0f}")

for d in deltas_to_test:
    res = all_results[d]
    if not res:
        continue
    tot = sum(r['option_pnl'] for r in res)
    avg_cap = sum(r['capital'] for r in res) / len(res)
    n_contracts = 15900 / avg_cap if avg_cap > 0 else 1
    scaled = tot * n_contracts
    vs_fut = scaled / fut_tot if fut_tot else 0
    monthly = scaled / 21 * 21  # already per period
    print(f"  {d:>5.2f}d ${avg_cap:>9,.0f} {n_contracts:>8.1f}x ${scaled:>+10,.0f} {vs_fut:>9.1f}x  ${monthly:>+10,.0f}")

print()

# ─── Detail: 0.30 delta trades ───
print("  DETAIL: 0.30 DELTA OPTION — TRADE BY TRADE")
print("  " + "-" * 78)
res30 = all_results[0.30]
print(f"  {'ID':>4} {'Dir':>5} {'Spot':>8} {'Strike':>8} {'Entry':>7} {'SPXpts':>7} {'OptPnL':>8} {'Return%':>8} {'Hold':>6}")
print("  " + "-" * 78)

for r in res30:
    pct = r['pct_return']
    print(f"  {r['id']:>4} {r['direction']:>5} {r['spot']:>8.1f} {r['strike']:>8.0f} ${r['entry']:>5.2f} {r['pts']:>+6.1f} ${r['option_pnl']:>+6,.0f} {pct:>+6.0f}%  {r['hold_min']:>4.0f}m")

tot30 = sum(r['option_pnl'] for r in res30)
avg_cap30 = sum(r['capital'] for r in res30) / len(res30) if res30 else 1
print(f"  {'':>4} {'':>5} {'':>8} {'':>8} {'TOTAL':>7} {'':>7} ${tot30:>+6,.0f}")
print(f"  Avg capital per contract: ${avg_cap30:,.0f}")
print(f"  ROI on capital deployed: {tot30/avg_cap30*100:+.0f}% per trade avg" if avg_cap30 else "")

print()

# ─── Skew Charm with max_profit analysis ───
# Skew Charm winners often overshoot the target — check max_profit
print("  BONUS: MAX PROFIT POTENTIAL (if held to peak instead of target)")
print("  " + "-" * 78)
w30 = [r for r in res30 if r['outcome'] == 'WIN']
if w30:
    for r in w30:
        mp = r['max_profit_pts']
        # At max profit, option would have gained more
        max_gain = (r['delta'] * mp + 0.5 * 0.005 * mp * mp) * 100  # rough gamma
        target_gain = r['option_pnl']
        print(f"  #{r['id']:>3} MaxProfit={mp:>5.1f}pts  AtTarget=${target_gain:>+6,.0f}  AtPeak=~${max_gain:>+6,.0f}  Extra=~${max_gain-target_gain:>+5,.0f}")
