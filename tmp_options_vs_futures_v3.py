"""Options vs Futures V3 — batch query approach for speed"""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
import sqlalchemy as sa
from datetime import timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

CALL_BID, CALL_ASK, CALL_DELTA, CALL_GAMMA = 5, 7, 4, 3
PUT_BID, PUT_ASK, PUT_DELTA, PUT_GAMMA = 14, 12, 16, 17
STRIKE_IDX = 10

with engine.connect() as c:
    # 1) Fetch ALL trades
    trades = c.execute(sa.text("""
        SELECT id, setup_name, direction, spot,
               outcome_result, outcome_pnl, ts, outcome_elapsed_min
        FROM setup_log WHERE outcome_result IN ('WIN','LOSS') ORDER BY ts
    """)).fetchall()
    trades = [dict(r._mapping) for r in trades]
    print(f"Trades: {len(trades)}")

    # 2) Fetch ALL chain snapshots (just ts + rows) - batch
    print("Loading chain snapshots...")
    snaps = c.execute(sa.text("""
        SELECT ts, rows FROM chain_snapshots
        WHERE ts::date >= '2026-02-03' ORDER BY ts
    """)).fetchall()
    snaps = [(r[0], json.loads(r[1]) if isinstance(r[1], str) else r[1]) for r in snaps]
    print(f"Snapshots loaded: {len(snaps)}")

# 3) Build time-indexed lookup
snap_times = [s[0] for s in snaps]

def find_nearest_snap(trade_ts, max_delta_sec=300):
    """Binary search for nearest snapshot"""
    import bisect
    idx = bisect.bisect_left(snap_times, trade_ts)
    best = None
    best_dt = max_delta_sec + 1
    for i in [idx - 1, idx]:
        if 0 <= i < len(snap_times):
            dt = abs((snap_times[i] - trade_ts).total_seconds())
            if dt < best_dt:
                best_dt = dt
                best = i
    return snaps[best][1] if best is not None and best_dt <= max_delta_sec else None

def get_atm(rows, spot, direction):
    best, best_dist = None, 999999
    for row in rows:
        strike = row[STRIKE_IDX]
        if strike is None:
            continue
        dist = abs(strike - spot)
        if dist < best_dist:
            if direction == 'long':
                bid, ask = row[CALL_BID] or 0, row[CALL_ASK] or 0
                delta, gamma = abs(row[CALL_DELTA] or 0), row[CALL_GAMMA] or 0
            else:
                bid, ask = row[PUT_BID] or 0, row[PUT_ASK] or 0
                delta, gamma = abs(row[PUT_DELTA] or 0), row[PUT_GAMMA] or 0
            if bid > 0 and ask > 0:
                best = {'strike': strike, 'bid': bid, 'ask': ask, 'delta': delta, 'gamma': gamma}
                best_dist = dist
    return best

def get_otm(rows, spot, direction, offset_pts=5):
    """Get OTM option for spread short leg"""
    target_strike = spot + offset_pts if direction == 'long' else spot - offset_pts
    best, best_dist = None, 999999
    for row in rows:
        strike = row[STRIKE_IDX]
        if strike is None:
            continue
        dist = abs(strike - target_strike)
        if dist < best_dist:
            if direction == 'long':
                bid, ask = row[CALL_BID] or 0, row[CALL_ASK] or 0
            else:
                bid, ask = row[PUT_BID] or 0, row[PUT_ASK] or 0
            if bid > 0:
                best = {'strike': strike, 'bid': bid, 'ask': ask}
                best_dist = dist
    return best

# 4) Process trades
results = []
skipped = 0

for t in trades:
    snap_rows = find_nearest_snap(t['ts'])
    if not snap_rows:
        skipped += 1
        continue

    spot = float(t['spot'])
    direction = t['direction']
    outcome = t['outcome_result']
    pts = float(t['outcome_pnl']) if t['outcome_pnl'] else 0
    hold_min = float(t['outcome_elapsed_min']) if t['outcome_elapsed_min'] else 15
    move = abs(pts)

    atm = get_atm(snap_rows, spot, direction)
    if not atm or atm['ask'] <= 0.5:
        skipped += 1
        continue

    otm = get_otm(snap_rows, spot, direction, 5)

    entry_cost = atm['ask']
    spread = atm['ask'] - atm['bid']
    delta = atm['delta']
    gamma = atm['gamma']

    # === MODEL A: Naked ATM (delta only) ===
    if outcome == 'WIN':
        naked_pnl = (delta * move - spread * 0.5) * 100
    else:
        naked_pnl = -min(delta * move + spread * 0.5, entry_cost) * 100

    # === MODEL B: Naked ATM with gamma ===
    gamma_boost = 0.5 * gamma * move * move
    # Theta: rough model based on time of day
    et_hour = (t['ts'].hour - 5) % 24
    if et_hour < 11:
        theta_rate = 0.015
    elif et_hour < 13:
        theta_rate = 0.025
    elif et_hour < 14:
        theta_rate = 0.04
    else:
        theta_rate = 0.06
    theta_cost = theta_rate * entry_cost * (hold_min / 60)

    if outcome == 'WIN':
        gamma_pnl = (delta * move + gamma_boost - theta_cost - spread * 0.5) * 100
    else:
        gamma_pnl = -min(delta * move - gamma_boost + theta_cost + spread * 0.5, entry_cost) * 100

    # === MODEL C: 5pt Debit Spread ===
    if otm:
        spr_debit = max(entry_cost - otm['bid'], 0.50)
        spr_max_profit = 5.0 - spr_debit
        if outcome == 'WIN':
            # Spread value at expiry = min(move, width) if ITM
            intrinsic = min(move, 5.0)
            spread_pnl = min(intrinsic - spr_debit, spr_max_profit) * 100
            spread_pnl = max(spread_pnl, -spr_debit * 100)
        else:
            spread_pnl = -spr_debit * 100
    else:
        spr_debit = entry_cost * 0.4
        spr_max_profit = 5.0 - spr_debit
        if outcome == 'WIN':
            spread_pnl = min(min(move, 5.0) - spr_debit, spr_max_profit) * 100
        else:
            spread_pnl = -spr_debit * 100

    results.append({
        'id': t['id'], 'setup': t['setup_name'], 'direction': direction,
        'outcome': outcome, 'pts': pts, 'hold_min': hold_min,
        'entry_cost': entry_cost, 'delta': delta, 'et_hour': et_hour,
        'futures_pnl': pts * 50,
        'naked_pnl': naked_pnl,
        'gamma_pnl': gamma_pnl,
        'spread_pnl': spread_pnl,
        'spread_debit': spr_debit if otm else entry_cost * 0.4,
        'opt_cap': entry_cost * 100,
        'spr_cap': (spr_debit if otm else entry_cost * 0.4) * 100,
    })

wins = [r for r in results if r['outcome'] == 'WIN']
losses = [r for r in results if r['outcome'] == 'LOSS']

first_ts = trades[0]['ts']
last_ts = trades[-1]['ts']
trading_days = max(1, (last_ts - first_ts).days * 5 / 7)

avg_opt_cap = sum(r['opt_cap'] for r in results) / len(results)
avg_spr_cap = sum(r['spr_cap'] for r in results) / len(results)
es_margin = 15900

print(f"\nMatched: {len(results)}, Skipped: {skipped}")
print()
print("=" * 80)
print("       ES FUTURES vs SPX 0DTE OPTIONS — FULL COMPARISON")
print("=" * 80)
print(f"  {len(results)} trades | {first_ts.strftime('%b %d')} - {last_ts.strftime('%b %d %Y')} | ~{trading_days:.0f} trading days")
print()

# ─── TABLE 1: Per-contract P&L ───
print("  TABLE 1: P&L PER CONTRACT")
print("  " + "-" * 72)
hdr = f"  {'Strategy':30} {'Total':>10} {'/Day':>8} {'PF':>6} {'AvgW':>8} {'AvgL':>8}"
print(hdr)
print("  " + "-" * 72)

for label, key in [
    ("ES Futures (1 contract)", 'futures_pnl'),
    ("SPX Naked ATM (delta)", 'naked_pnl'),
    ("SPX ATM (delta+gamma-theta)", 'gamma_pnl'),
    ("SPX 5pt Debit Spread", 'spread_pnl'),
]:
    tot = sum(r[key] for r in results)
    gw = sum(r[key] for r in wins)
    gl = abs(sum(r[key] for r in losses))
    pf = gw / gl if gl else 999
    aw = gw / len(wins) if wins else 0
    al = -gl / len(losses) if losses else 0
    daily = tot / trading_days
    print(f"  {label:30} ${tot:>+8,.0f} ${daily:>+6,.0f} {pf:>5.2f}x ${aw:>+6,.0f} ${al:>+6,.0f}")

print()

# ─── TABLE 2: Capital Efficiency ───
print("  TABLE 2: SAME $15,900 CAPITAL — HOW MANY CONTRACTS?")
print("  " + "-" * 72)
print(f"  {'Strategy':30} {'Contracts':>10} {'Total P&L':>12} {'ROI':>10} {'Monthly':>12}")
print("  " + "-" * 72)

for label, key, cap in [
    ("ES Futures", 'futures_pnl', es_margin),
    ("SPX Naked ATM", 'naked_pnl', avg_opt_cap),
    ("SPX ATM+Gamma", 'gamma_pnl', avg_opt_cap),
    ("SPX 5pt Spread", 'spread_pnl', avg_spr_cap),
]:
    tot = sum(r[key] for r in results)
    n = es_margin / cap if cap > 0 else 1
    scaled = tot * n
    roi = scaled / es_margin * 100
    monthly = scaled / trading_days * 21
    print(f"  {label:30} {n:>8.1f}x  ${scaled:>+10,.0f} {roi:>+8.1f}%  ${monthly:>+10,.0f}")

print()

# ─── TABLE 3: Per-Setup ───
print("  TABLE 3: PER-SETUP — WHICH INSTRUMENT WINS?")
print("  " + "-" * 72)
print(f"  {'Setup':22} {'#':>4} {'Futures':>10} {'NakedATM':>10} {'Spread':>10} {'Best':>8}")
print("  " + "-" * 72)

for s in sorted(set(r['setup'] for r in results)):
    sr = [r for r in results if r['setup'] == s]
    fp = sum(r['futures_pnl'] for r in sr)
    np_ = sum(r['naked_pnl'] for r in sr)
    sp = sum(r['spread_pnl'] for r in sr)
    best = max([(fp, 'FUT'), (np_, 'ATM'), (sp, 'SPREAD')], key=lambda x: x[0])
    print(f"  {s:22} {len(sr):>4} ${fp:>+8,.0f} ${np_:>+8,.0f} ${sp:>+8,.0f}   {best[1]:>6}")

print()

# ─── TABLE 4: Loss capping ───
print("  TABLE 4: LOSS CAPPING — WHERE OPTIONS PROTECT YOU")
print("  " + "-" * 72)
for label, lo, hi in [("Small (<5 pts)", 0, 5), ("Medium (5-10 pts)", 5, 10), ("Large (10-15 pts)", 10, 15), ("Huge (>=15 pts)", 15, 999)]:
    bucket = [r for r in losses if lo <= abs(r['pts']) < hi]
    if not bucket:
        continue
    fl = sum(abs(r['futures_pnl']) for r in bucket)
    nl = sum(abs(r['naked_pnl']) for r in bucket)
    sl = sum(abs(r['spread_pnl']) for r in bucket)
    print(f"  {label:20} {len(bucket):>3} trades  Fut ${fl:>8,.0f}  ATM ${nl:>8,.0f} ({(nl-fl)/fl*100:>+.0f}%)  Spr ${sl:>8,.0f} ({(sl-fl)/fl*100:>+.0f}%)")

print()

# ─── TABLE 5: Hold duration ───
print("  TABLE 5: BY HOLD DURATION")
print("  " + "-" * 72)
for label, lo, hi in [("<10 min", 0, 10), ("10-30 min", 10, 30), ("30-60 min", 30, 60), ("1-2 hr", 60, 120), (">2 hr", 120, 9999)]:
    bucket = [r for r in results if lo <= r['hold_min'] < hi]
    if not bucket:
        continue
    fp = sum(r['futures_pnl'] for r in bucket)
    np_ = sum(r['naked_pnl'] for r in bucket)
    sp = sum(r['spread_pnl'] for r in bucket)
    best = "FUT" if fp >= np_ and fp >= sp else ("ATM" if np_ >= sp else "SPREAD")
    print(f"  {label:>10} ({len(bucket):>3})  Fut ${fp:>+8,.0f}  ATM ${np_:>+8,.0f}  Spr ${sp:>+8,.0f}  -> {best}")

print()

# ─── VERDICT ───
tot_f = sum(r['futures_pnl'] for r in results)
tot_n = sum(r['naked_pnl'] for r in results)
tot_s = sum(r['spread_pnl'] for r in results)

n_atm = es_margin / avg_opt_cap
n_spr = es_margin / avg_spr_cap

print("  " + "=" * 72)
print("  VERDICT")
print("  " + "=" * 72)
print()
print(f"  Per-contract P&L:     Futures ${tot_f:>+,.0f} vs ATM ${tot_n:>+,.0f} vs Spread ${tot_s:>+,.0f}")
print(f"  -> Futures win per contract by ${tot_f - tot_n:,.0f}")
print()
print(f"  Same capital ($15,900):")
print(f"    Futures: 1 ES          = ${tot_f:>+12,.0f}")
print(f"    ATM:     {n_atm:.0f} contracts    = ${tot_n*n_atm:>+12,.0f}  ({tot_n*n_atm/tot_f:.1f}x futures)")
print(f"    Spread:  {n_spr:.0f} contracts    = ${tot_s*n_spr:>+12,.0f}  ({tot_s*n_spr/tot_f:.1f}x futures)")
print()
print(f"  Options capital per trade: ${avg_opt_cap:.0f} (ATM) / ${avg_spr_cap:.0f} (spread)")
print(f"  Futures margin per trade:  ${es_margin:,}")
print(f"  Leverage advantage: {es_margin/avg_opt_cap:.0f}x (ATM) / {es_margin/avg_spr_cap:.0f}x (spread)")
print()
print(f"  BOTTOM LINE:")
print(f"  - Per contract: Futures slightly better (no theta/spread drag)")
print(f"  - Per dollar deployed: Options MASSIVELY better ({n_atm:.0f}x leverage)")
print(f"  - Risk: Options loss CAPPED at premium. Futures can gap through stops.")
print(f"  - Spread: Cheapest entry, max loss = debit, but profit capped at 5 pts")
