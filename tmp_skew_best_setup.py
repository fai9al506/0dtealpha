"""
SKEW BEST SETUP FINDER
Test multiple skew-based ideas:
  A) Skew as portfolio filter (block elevated skew)
  B) Per-setup optimal skew bands
  C) Skew regime change as standalone setup
  D) Skew + charm alignment combo setup
  E) Skew mean-reversion (extreme skew → contrarian)
  F) Skew breakout (skew crossing thresholds)
"""
import os, sqlalchemy as sa
from datetime import time as dtime, datetime, timedelta
from collections import defaultdict
import pytz

NY = pytz.timezone("US/Eastern")
engine = sa.create_engine(os.environ["DATABASE_URL"])

MARKET_START = dtime(9, 45)
MARKET_END = dtime(15, 45)


def compute_skew(chain_rows, spot, put_range=(10, 20), call_range=(10, 20)):
    put_ivs, call_ivs = [], []
    for row in chain_rows:
        if len(row) < 21:
            continue
        strike = row[10]
        c_iv = row[2]
        p_iv = row[18]
        dist = strike - spot
        if -put_range[1] <= dist <= -put_range[0] and p_iv and p_iv > 0:
            put_ivs.append(p_iv)
        if call_range[0] <= dist <= call_range[1] and c_iv and c_iv > 0:
            call_ivs.append(c_iv)
    if not put_ivs or not call_ivs:
        return None
    avg_put = sum(put_ivs) / len(put_ivs)
    avg_call = sum(call_ivs) / len(call_ivs)
    if avg_call == 0:
        return None
    return avg_put / avg_call


def get_volland_at(volland_rows, ts):
    paradigm = None
    charm_val = None
    dd_val = None
    for vr in reversed(volland_rows):
        if vr[0] <= ts:
            paradigm = vr[1]
            if vr[2]:
                try:
                    charm_val = float(vr[2].replace("$", "").replace(",", "").strip())
                except:
                    pass
            if vr[3]:
                try:
                    dd_val = float(vr[3].replace("$", "").replace(",", "").strip())
                except:
                    pass
            break
    return paradigm, charm_val, dd_val


# ============================================================
# LOAD ALL DATA
# ============================================================
print("Loading all data...")

all_days = {}
with engine.connect() as c:
    days = c.execute(sa.text(
        "SELECT DISTINCT ts::date FROM chain_snapshots "
        "WHERE ts::date >= '2026-02-01' AND spot IS NOT NULL ORDER BY ts::date"
    )).fetchall()
    days = [d[0] for d in days]

    for day in days:
        chain = c.execute(sa.text(
            "SELECT ts, spot, rows FROM chain_snapshots "
            "WHERE ts::date = :d AND spot IS NOT NULL ORDER BY ts"
        ), {"d": day}).fetchall()

        volland = c.execute(sa.text("""
            SELECT ts,
                   payload->'statistics'->>'paradigm',
                   payload->'statistics'->>'aggregatedCharm',
                   payload->'statistics'->>'delta_decay_hedging'
            FROM volland_snapshots
            WHERE ts::date = :d AND payload->'statistics' IS NOT NULL
            ORDER BY ts
        """), {"d": day}).fetchall()

        all_days[day] = {"chain": chain, "volland": volland}

print(f"Loaded {len(days)} days, {sum(len(d['chain']) for d in all_days.values())} chain rows.\n")

# Also load setup_log for filter testing
with engine.connect() as c:
    setup_trades = c.execute(sa.text("""
        SELECT id, setup_name, ts::date as trade_date, ts as fired_at, direction, spot,
               outcome_result, outcome_pnl, grade
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_result != 'OPEN'
        ORDER BY setup_log.ts
    """)).fetchall()
print(f"Loaded {len(setup_trades)} setup trades.\n")


def sim_trade(chain, i, direction, target_pts, stop_pts):
    """Simulate a trade from chain index i. Returns (result, pnl, max_profit, max_loss, elapsed_min)."""
    entry = chain[i][1]
    for j in range(i + 1, len(chain)):
        future_ts, future_spot, _ = chain[j]
        if future_spot is None:
            continue
        profit = (future_spot - entry) if direction == "LONG" else (entry - future_spot)
        if profit <= -stop_pts:
            return "LOSS", -stop_pts, profit, profit, (future_ts - chain[i][0]).total_seconds() / 60
        if profit >= target_pts:
            return "WIN", target_pts, profit, 0, (future_ts - chain[i][0]).total_seconds() / 60
    # EOD
    last_spot = None
    for r in reversed(chain):
        if r[1] is not None:
            last_spot = r[1]
            break
    pnl = 0
    if last_spot:
        pnl = (last_spot - entry) if direction == "LONG" else (entry - last_spot)
    elapsed = (chain[-1][0] - chain[i][0]).total_seconds() / 60 if len(chain) > i + 1 else 0
    return "EXPIRED", pnl, 0, 0, elapsed


def stats(label, trades_list):
    if not trades_list:
        print(f"  {label}: 0 trades")
        return 0
    wins = [t for t in trades_list if t["result"] == "WIN"]
    losses = [t for t in trades_list if t["result"] == "LOSS"]
    expired = [t for t in trades_list if t["result"] == "EXPIRED"]
    total_pnl = sum(t["pnl"] for t in trades_list)
    wr = len(wins) / len(trades_list) * 100
    print(f"  {label}: {len(trades_list)} trades | {len(wins)}W/{len(losses)}L/{len(expired)}E | "
          f"WR={wr:.0f}% | P&L={total_pnl:+.1f} | Avg={total_pnl/len(trades_list):+.1f}/trade")
    return total_pnl


# ============================================================
# PART A: Portfolio filter — block existing setups at elevated skew
# ============================================================
print("=" * 80)
print("PART A: PORTFOLIO FILTER — Block existing setups when skew > threshold")
print("=" * 80)

# Enrich setup trades with skew
enriched_setups = []
for t in setup_trades:
    tid, setup, td, ts, direction, spot, result, pnl, grade = t
    if td not in all_days:
        continue
    chain = all_days[td]["chain"]
    best_idx = None
    best_diff = 999999
    for i, (cts, cs, _) in enumerate(chain):
        diff = abs((cts - ts).total_seconds())
        if diff < best_diff:
            best_diff = diff
            best_idx = i
    if best_idx is None or best_diff > 120:
        continue
    skew = compute_skew(chain[best_idx][2], chain[best_idx][1])
    if skew is None:
        continue
    enriched_setups.append({
        "id": tid, "setup": setup, "direction": direction,
        "result": result, "pnl": float(pnl) if pnl else 0,
        "skew": skew,
    })

print(f"\n{len(enriched_setups)} trades enriched with skew.\n")

# Test block thresholds
for block_thresh in [1.15, 1.18, 1.20, 1.22, 1.25]:
    passed = [t for t in enriched_setups if t["skew"] <= block_thresh]
    blocked = [t for t in enriched_setups if t["skew"] > block_thresh]
    pass_pnl = sum(t["pnl"] for t in passed)
    block_pnl = sum(t["pnl"] for t in blocked)
    bw = len([t for t in blocked if t["result"] == "WIN"])
    bl = len([t for t in blocked if t["result"] == "LOSS"])
    all_pnl = sum(t["pnl"] for t in enriched_setups)
    pw = len([t for t in passed if t["result"] == "WIN"])
    pt = len(passed)
    print(f"  Block skew>{block_thresh:.2f}: {pt} pass ({pw}W, {pw/pt*100:.0f}% WR, "
          f"P&L={pass_pnl:+.1f}) | blocked {len(blocked)} ({bw}W/{bl}L, "
          f"P&L={block_pnl:+.1f}) | net={pass_pnl-all_pnl:+.1f}")

# Per-setup optimal skew band
print(f"\n--- Per-setup: best skew band ---")
for setup in ["GEX Long", "AG Short", "BofA Scalp", "ES Absorption", "DD Exhaustion", "Paradigm Reversal"]:
    st = [t for t in enriched_setups if t["setup"] == setup]
    if len(st) < 5:
        continue
    print(f"\n  {setup}:")
    for lo, hi in [(0, 1.10), (1.10, 1.15), (1.15, 1.20), (1.20, 1.30), (0, 1.15), (0, 1.20), (1.10, 1.20), (1.10, 1.25)]:
        band = [t for t in st if lo <= t["skew"] < hi]
        if not band:
            continue
        bw = len([t for t in band if t["result"] == "WIN"])
        bp = sum(t["pnl"] for t in band)
        wr = bw / len(band) * 100
        print(f"    skew {lo:.2f}-{hi:.2f}: {len(band)} trades, {bw}W ({wr:.0f}% WR), P&L={bp:+.1f}")


# ============================================================
# PART B: Skew regime change as standalone setup
# ============================================================
print("\n" + "=" * 80)
print("PART B: SKEW REGIME CHANGE — Trade when skew crosses a threshold")
print("Signal: skew drops below 1.10 = LONG (fear unwinding)")
print("Signal: skew rises above 1.20 = SHORT (fear spiking)")
print("=" * 80)

for target, stop in [(10, 8), (10, 5), (8, 5), (5, 5)]:
    for cooldown_min in [30, 60]:
        trades = []
        for day in sorted(all_days.keys()):
            chain = all_days[day]["chain"]
            volland = all_days[day]["volland"]
            if len(chain) < 10:
                continue
            last_trade_time = None
            prev_skew = None
            for i, (ts, spot, rows) in enumerate(chain):
                if spot is None or rows is None:
                    continue
                t_et = ts.astimezone(NY)
                if t_et.time() < MARKET_START or t_et.time() > MARKET_END:
                    prev_skew = compute_skew(rows, spot)
                    continue
                skew = compute_skew(rows, spot)
                if skew is None:
                    continue
                if prev_skew is None:
                    prev_skew = skew
                    continue
                if last_trade_time and (ts - last_trade_time).total_seconds() < cooldown_min * 60:
                    prev_skew = skew
                    continue

                direction = None
                # Skew drops below threshold (was above, now below)
                if prev_skew >= 1.12 and skew < 1.10:
                    direction = "LONG"
                # Skew rises above threshold
                elif prev_skew <= 1.18 and skew > 1.20:
                    direction = "SHORT"

                prev_skew = skew
                if direction is None:
                    continue

                paradigm, charm, dd = get_volland_at(volland, ts)
                result, pnl, mp, ml, elapsed = sim_trade(chain, i, direction, target, stop)
                trades.append({
                    "date": str(day), "time": t_et.strftime("%H:%M"),
                    "direction": direction, "entry": round(spot, 1),
                    "skew": round(skew, 3),
                    "paradigm": paradigm or "?",
                    "result": result, "pnl": round(pnl, 1),
                    "max_profit": round(mp, 1),
                })
                last_trade_time = ts

        stats(f"Regime cross T={target}/S={stop} CD={cooldown_min}", trades)


# ============================================================
# PART C: Skew mean-reversion (extreme → snap back)
# ============================================================
print("\n" + "=" * 80)
print("PART C: SKEW MEAN REVERSION — Contrarian at extremes")
print("LONG when skew > 1.25 (extreme fear = oversold)")
print("SHORT when skew < 1.05 (extreme complacency = overbought)")
print("=" * 80)

for hi_thresh, lo_thresh in [(1.25, 1.05), (1.22, 1.07), (1.20, 1.08), (1.30, 1.05)]:
    for target, stop in [(10, 8), (10, 5), (5, 5)]:
        trades = []
        for day in sorted(all_days.keys()):
            chain = all_days[day]["chain"]
            if len(chain) < 10:
                continue
            last_trade_time = None
            for i, (ts, spot, rows) in enumerate(chain):
                if spot is None or rows is None:
                    continue
                t_et = ts.astimezone(NY)
                if t_et.time() < MARKET_START or t_et.time() > MARKET_END:
                    continue
                skew = compute_skew(rows, spot)
                if skew is None:
                    continue
                if last_trade_time and (ts - last_trade_time).total_seconds() < 3600:
                    continue

                direction = None
                if skew > hi_thresh:
                    direction = "LONG"  # contrarian: extreme fear = buy
                elif skew < lo_thresh:
                    direction = "SHORT"  # contrarian: extreme complacency = sell

                if direction is None:
                    continue

                result, pnl, mp, ml, elapsed = sim_trade(chain, i, direction, target, stop)
                trades.append({
                    "date": str(day), "time": t_et.strftime("%H:%M"),
                    "direction": direction, "entry": round(spot, 1),
                    "skew": round(skew, 3),
                    "result": result, "pnl": round(pnl, 1),
                })
                last_trade_time = ts

        if trades:
            stats(f"Revert hi>{hi_thresh}/lo<{lo_thresh} T={target}/S={stop}", trades)


# ============================================================
# PART D: Skew + Charm alignment combo
# ============================================================
print("\n" + "=" * 80)
print("PART D: SKEW + CHARM COMBO (Apollo's rule)")
print("LONG: skew compressing (dropping) + charm positive (bullish)")
print("SHORT: skew expanding (rising) + charm negative (bearish)")
print("=" * 80)

for skew_window in [10, 15, 20]:  # lookback in snapshots
    for skew_chg_thresh in [0.03, 0.05, 0.08]:
        for charm_thresh_M in [0, 50]:  # charm in millions
            trades = []
            for day in sorted(all_days.keys()):
                chain = all_days[day]["chain"]
                volland = all_days[day]["volland"]
                if len(chain) < skew_window + 2:
                    continue
                last_trade_time = None
                for i, (ts, spot, rows) in enumerate(chain):
                    if spot is None or rows is None:
                        continue
                    t_et = ts.astimezone(NY)
                    if t_et.time() < MARKET_START or t_et.time() > MARKET_END:
                        continue
                    if i < skew_window:
                        continue
                    if last_trade_time and (ts - last_trade_time).total_seconds() < 1800:
                        continue

                    skew_now = compute_skew(rows, spot)
                    if skew_now is None:
                        continue
                    skew_prev = compute_skew(chain[i - skew_window][2], chain[i - skew_window][1])
                    if skew_prev is None or skew_prev == 0:
                        continue

                    skew_chg = (skew_now - skew_prev) / skew_prev
                    paradigm, charm, dd = get_volland_at(volland, ts)

                    if charm is None:
                        continue

                    charm_M = charm / 1e6
                    direction = None

                    # Apollo's rule: skew compressing + charm bullish = LONG
                    if skew_chg < -skew_chg_thresh and charm_M > charm_thresh_M:
                        direction = "LONG"
                    # Inverse: skew expanding + charm bearish = SHORT
                    elif skew_chg > skew_chg_thresh and charm_M < -charm_thresh_M:
                        direction = "SHORT"

                    if direction is None:
                        continue

                    result, pnl, mp, ml, elapsed = sim_trade(chain, i, direction, 10, 8)
                    trades.append({
                        "date": str(day), "time": t_et.strftime("%H:%M"),
                        "direction": direction, "entry": round(spot, 1),
                        "skew_chg": round(skew_chg * 100, 1),
                        "charm_M": round(charm_M, 0),
                        "paradigm": paradigm or "?",
                        "result": result, "pnl": round(pnl, 1),
                        "max_profit": round(mp, 1),
                    })
                    last_trade_time = ts

            if trades and len(trades) >= 5:
                stats(f"Charm+Skew win={skew_window} chg>{skew_chg_thresh:.0%} charm>{charm_thresh_M}M", trades)


# ============================================================
# PART E: Skew + DD combo (DD shift + skew direction)
# ============================================================
print("\n" + "=" * 80)
print("PART E: SKEW + DD COMBO")
print("LONG: DD bearish shift + skew dropping (fear unwinding despite DD selling)")
print("SHORT: DD bullish shift + skew rising (fear building despite DD buying)")
print("=" * 80)

for skew_window in [10, 15]:
    for skew_chg_thresh in [0.03, 0.05]:
        for dd_thresh_B in [0.2, 0.5, 1.0]:
            trades = []
            for day in sorted(all_days.keys()):
                chain = all_days[day]["chain"]
                volland = all_days[day]["volland"]
                if len(chain) < skew_window + 2 or len(volland) < 2:
                    continue
                last_trade_time = None

                # Build DD shift series
                dd_values = []
                for vr in volland:
                    if vr[3]:
                        try:
                            dd_values.append((vr[0], float(vr[3].replace("$", "").replace(",", "").strip())))
                        except:
                            pass

                for i, (ts, spot, rows) in enumerate(chain):
                    if spot is None or rows is None:
                        continue
                    t_et = ts.astimezone(NY)
                    if t_et.time() < dtime(10, 0) or t_et.time() > dtime(15, 30):
                        continue
                    if i < skew_window:
                        continue
                    if last_trade_time and (ts - last_trade_time).total_seconds() < 1800:
                        continue

                    skew_now = compute_skew(rows, spot)
                    if skew_now is None:
                        continue
                    skew_prev = compute_skew(chain[i - skew_window][2], chain[i - skew_window][1])
                    if skew_prev is None or skew_prev == 0:
                        continue
                    skew_chg = (skew_now - skew_prev) / skew_prev

                    # Get DD at current time and ~5 min ago
                    dd_now = None
                    dd_prev = None
                    for dts, dval in reversed(dd_values):
                        if dts <= ts:
                            if dd_now is None:
                                dd_now = dval
                            elif dd_prev is None and (dd_now != dval):
                                dd_prev = dval
                                break

                    if dd_now is None or dd_prev is None:
                        continue

                    dd_shift = (dd_now - dd_prev) / 1e9  # in billions

                    direction = None
                    # DD shifts bearish (negative) + skew dropping = LONG (fear unwinding)
                    if dd_shift < -dd_thresh_B and skew_chg < -skew_chg_thresh:
                        direction = "LONG"
                    # DD shifts bullish (positive) + skew rising = SHORT (fear building)
                    elif dd_shift > dd_thresh_B and skew_chg > skew_chg_thresh:
                        direction = "SHORT"

                    if direction is None:
                        continue

                    result, pnl, mp, ml, elapsed = sim_trade(chain, i, direction, 10, 8)
                    trades.append({
                        "date": str(day), "time": t_et.strftime("%H:%M"),
                        "direction": direction, "entry": round(spot, 1),
                        "skew_chg": round(skew_chg * 100, 1),
                        "dd_shift_B": round(dd_shift, 2),
                        "result": result, "pnl": round(pnl, 1),
                    })
                    last_trade_time = ts

            if trades and len(trades) >= 3:
                stats(f"DD+Skew win={skew_window} skew>{skew_chg_thresh:.0%} dd>{dd_thresh_B}B", trades)


# ============================================================
# PART F: Skew velocity (rate of change acceleration)
# ============================================================
print("\n" + "=" * 80)
print("PART F: SKEW VELOCITY — Fast skew moves (acceleration)")
print("Detect when skew is moving FAST in one direction")
print("=" * 80)

for window in [5, 8, 10]:
    for vel_thresh in [0.02, 0.03, 0.05]:
        trades = []
        for day in sorted(all_days.keys()):
            chain = all_days[day]["chain"]
            if len(chain) < window + 2:
                continue
            last_trade_time = None
            skew_series = []

            for i, (ts, spot, rows) in enumerate(chain):
                if spot is None or rows is None:
                    continue
                skew = compute_skew(rows, spot)
                if skew is None:
                    continue
                skew_series.append((i, ts, spot, skew))

                t_et = ts.astimezone(NY)
                if t_et.time() < MARKET_START or t_et.time() > MARKET_END:
                    continue
                if len(skew_series) < window + 1:
                    continue
                if last_trade_time and (ts - last_trade_time).total_seconds() < 1800:
                    continue

                # Compute skew velocity (change per snapshot over window)
                recent = skew_series[-window:]
                if len(recent) < window:
                    continue
                skew_start = recent[0][3]
                skew_end = recent[-1][3]
                if skew_start == 0:
                    continue
                velocity = (skew_end - skew_start) / skew_start

                # Price stability check (< 5 pts move over window)
                price_chg = abs(recent[-1][2] - recent[0][2])
                if price_chg > 8:
                    continue

                direction = None
                if velocity < -vel_thresh:
                    direction = "LONG"
                elif velocity > vel_thresh:
                    direction = "SHORT"

                if direction is None:
                    continue

                result, pnl, mp, ml, elapsed = sim_trade(chain, i, direction, 10, 8)
                trades.append({
                    "date": str(day), "time": t_et.strftime("%H:%M"),
                    "direction": direction, "entry": round(spot, 1),
                    "velocity": round(velocity * 100, 2),
                    "price_chg": round(price_chg, 1),
                    "result": result, "pnl": round(pnl, 1),
                })
                last_trade_time = ts

        if trades:
            stats(f"Velocity win={window} thresh>{vel_thresh:.0%} stable<8pt", trades)


# ============================================================
# PART G: Deep dive on best performers
# ============================================================
print("\n" + "=" * 80)
print("PART G: DEEP DIVE — Best setup from above")
print("=" * 80)

# Run the best skew+charm combo with detail
best_configs = [
    # (skew_window, skew_chg_thresh, charm_thresh_M, label)
    (10, 0.03, 0, "win=10 chg>3% charm>0"),
    (10, 0.05, 0, "win=10 chg>5% charm>0"),
    (15, 0.03, 0, "win=15 chg>3% charm>0"),
    (15, 0.05, 0, "win=15 chg>5% charm>0"),
    (20, 0.03, 0, "win=20 chg>3% charm>0"),
    (20, 0.05, 0, "win=20 chg>5% charm>0"),
    (10, 0.03, 50, "win=10 chg>3% charm>50M"),
    (15, 0.05, 50, "win=15 chg>5% charm>50M"),
]

for skew_window, skew_chg_thresh, charm_thresh_M, label in best_configs:
    trades = []
    for day in sorted(all_days.keys()):
        chain = all_days[day]["chain"]
        volland = all_days[day]["volland"]
        if len(chain) < skew_window + 2:
            continue
        last_trade_time = None
        for i, (ts, spot, rows) in enumerate(chain):
            if spot is None or rows is None:
                continue
            t_et = ts.astimezone(NY)
            if t_et.time() < MARKET_START or t_et.time() > MARKET_END:
                continue
            if i < skew_window:
                continue
            if last_trade_time and (ts - last_trade_time).total_seconds() < 1800:
                continue

            skew_now = compute_skew(rows, spot)
            if skew_now is None:
                continue
            skew_prev = compute_skew(chain[i - skew_window][2], chain[i - skew_window][1])
            if skew_prev is None or skew_prev == 0:
                continue
            skew_chg = (skew_now - skew_prev) / skew_prev
            paradigm, charm, dd = get_volland_at(volland, ts)
            if charm is None:
                continue
            charm_M = charm / 1e6

            direction = None
            if skew_chg < -skew_chg_thresh and charm_M > charm_thresh_M:
                direction = "LONG"
            elif skew_chg > skew_chg_thresh and charm_M < -charm_thresh_M:
                direction = "SHORT"

            if direction is None:
                continue

            # Test multiple T/S combos
            for tgt, stp in [(10, 8), (10, 5), (8, 5), (5, 5)]:
                result, pnl, mp, ml, elapsed = sim_trade(chain, i, direction, tgt, stp)
                trades.append({
                    "config": f"{label} T={tgt}/S={stp}",
                    "date": str(day), "time": t_et.strftime("%H:%M"),
                    "direction": direction, "entry": round(spot, 1),
                    "skew": round(skew_now, 3),
                    "skew_chg": round(skew_chg * 100, 1),
                    "charm_M": round(charm_M, 0),
                    "paradigm": paradigm or "?",
                    "result": result, "pnl": round(pnl, 1),
                    "max_profit": round(mp, 1),
                    "ts_combo": f"T={tgt}/S={stp}",
                })
            last_trade_time = ts

    # Print grouped by T/S combo
    if not trades:
        continue
    print(f"\n--- {label} ---")
    for ts_combo in ["T=10/S=8", "T=10/S=5", "T=8/S=5", "T=5/S=5"]:
        combo_trades = [t for t in trades if t["ts_combo"] == ts_combo]
        if combo_trades:
            stats(f"  {ts_combo}", combo_trades)

    # Detailed trade list for best T/S
    best_ts = [t for t in trades if t["ts_combo"] == "T=10/S=8"]
    if best_ts and len(best_ts) <= 40:
        print(f"\n  {'Date':<12} {'Time':<6} {'Dir':<6} {'Entry':<8} {'Skew':<7} {'Chg%':<7} {'ChrmM':<8} {'Para':<14} {'Result':<8} {'P&L':<7} {'MaxP':<7}")
        print("  " + "-" * 100)
        for t in best_ts:
            print(f"  {t['date']:<12} {t['time']:<6} {t['direction']:<6} {t['entry']:<8} {t['skew']:<7.3f} {t['skew_chg']:<+7.1f} {t['charm_M']:<+8.0f} {t['paradigm']:<14} {t['result']:<8} {t['pnl']:<+7.1f} {t['max_profit']:<+7.1f}")


print("\n\nDONE.")
