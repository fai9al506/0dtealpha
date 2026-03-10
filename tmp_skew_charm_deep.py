"""
SKEW + CHARM COMBO — Deep dive on best configs
Show all trades, breakdown by direction/hour/paradigm/skew level
"""
import os, sqlalchemy as sa
from datetime import time as dtime
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


def sim_trade_detailed(chain, i, direction, target_pts, stop_pts):
    """Simulate trade, return detailed result."""
    entry = chain[i][1]
    max_profit = 0
    max_loss = 0
    for j in range(i + 1, len(chain)):
        future_ts, future_spot, _ = chain[j]
        if future_spot is None:
            continue
        profit = (future_spot - entry) if direction == "LONG" else (entry - future_spot)
        max_profit = max(max_profit, profit)
        max_loss = min(max_loss, profit)
        if profit <= -stop_pts:
            elapsed = (future_ts - chain[i][0]).total_seconds() / 60
            return "LOSS", -stop_pts, max_profit, max_loss, elapsed
        if profit >= target_pts:
            elapsed = (future_ts - chain[i][0]).total_seconds() / 60
            return "WIN", target_pts, max_profit, max_loss, elapsed
    last_spot = None
    for r in reversed(chain):
        if r[1] is not None:
            last_spot = r[1]
            break
    pnl = 0
    if last_spot:
        pnl = (last_spot - entry) if direction == "LONG" else (entry - last_spot)
    elapsed = (chain[-1][0] - chain[i][0]).total_seconds() / 60 if len(chain) > i + 1 else 0
    return "EXPIRED", pnl, max_profit, max_loss, elapsed


# ============================================================
# LOAD DATA
# ============================================================
print("Loading data...")
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

print(f"Loaded {len(days)} days.\n")


def run_combo(skew_window, skew_chg_thresh, charm_thresh_M, cooldown_min=30):
    """Run skew+charm combo, return detailed trades."""
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
            if last_trade_time and (ts - last_trade_time).total_seconds() < cooldown_min * 60:
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

            result, pnl, mp, ml, elapsed = sim_trade_detailed(chain, i, direction, 10, 8)
            trades.append({
                "date": str(day),
                "time": t_et.strftime("%H:%M"),
                "hour": t_et.hour,
                "direction": direction,
                "entry": round(spot, 1),
                "skew": round(skew_now, 3),
                "skew_chg": round(skew_chg * 100, 1),
                "charm_M": round(charm_M, 0),
                "dd_B": round(dd / 1e9, 1) if dd else None,
                "paradigm": paradigm or "?",
                "result": result,
                "pnl": round(pnl, 1),
                "max_profit": round(mp, 1),
                "max_loss": round(ml, 1),
                "elapsed_min": round(elapsed, 0),
            })
            last_trade_time = ts
    return trades


def stats(label, tl):
    if not tl:
        print(f"  {label}: 0 trades")
        return
    wins = [t for t in tl if t["result"] == "WIN"]
    losses = [t for t in tl if t["result"] == "LOSS"]
    expired = [t for t in tl if t["result"] == "EXPIRED"]
    total_pnl = sum(t["pnl"] for t in tl)
    wr = len(wins) / len(tl) * 100
    print(f"  {label}: {len(tl)} trades | {len(wins)}W/{len(losses)}L/{len(expired)}E | "
          f"WR={wr:.0f}% | P&L={total_pnl:+.1f} | Avg={total_pnl/len(tl):+.1f}")


# ============================================================
# TOP 3 CONFIGS — Full detail
# ============================================================
configs = [
    (20, 0.03, 0, 30, "A: win=20, chg>3%, charm>0, CD=30"),
    (15, 0.05, 50, 30, "B: win=15, chg>5%, charm>50M, CD=30"),
    (10, 0.03, 50, 30, "C: win=10, chg>3%, charm>50M, CD=30"),
]

for skew_win, skew_thr, charm_thr, cd, label in configs:
    trades = run_combo(skew_win, skew_thr, charm_thr, cd)
    if not trades:
        continue

    print("=" * 110)
    print(f"CONFIG {label}")
    print("=" * 110)

    # Overall
    stats("TOTAL", trades)

    # By direction
    print("\n  --- By Direction ---")
    for d in ["LONG", "SHORT"]:
        dt = [t for t in trades if t["direction"] == d]
        stats(f"  {d}", dt)

    # By hour
    print("\n  --- By Hour ---")
    for h in sorted(set(t["hour"] for t in trades)):
        ht = [t for t in trades if t["hour"] == h]
        hw = len([t for t in ht if t["result"] == "WIN"])
        hp = sum(t["pnl"] for t in ht)
        wr = hw / len(ht) * 100
        print(f"    {h:02d}:00 — {len(ht)} trades, {hw}W ({wr:.0f}% WR), P&L={hp:+.1f}")

    # By paradigm
    print("\n  --- By Paradigm ---")
    for p in sorted(set(t["paradigm"] for t in trades)):
        pt = [t for t in trades if t["paradigm"] == p]
        if len(pt) < 2:
            continue
        pw = len([t for t in pt if t["result"] == "WIN"])
        pp = sum(t["pnl"] for t in pt)
        wr = pw / len(pt) * 100
        print(f"    {p:<16} {len(pt)} trades, {pw}W ({wr:.0f}% WR), P&L={pp:+.1f}")

    # By skew level
    print("\n  --- By Skew Level ---")
    for lo, hi, lbl in [(0, 1.10, "<1.10"), (1.10, 1.15, "1.10-1.15"), (1.15, 1.20, "1.15-1.20"), (1.20, 1.30, "1.20-1.30"), (1.30, 9.0, ">1.30")]:
        st = [t for t in trades if lo <= t["skew"] < hi]
        if not st:
            continue
        sw = len([t for t in st if t["result"] == "WIN"])
        sp = sum(t["pnl"] for t in st)
        wr = sw / len(st) * 100
        print(f"    skew {lbl:<10} {len(st)} trades, {sw}W ({wr:.0f}% WR), P&L={sp:+.1f}")

    # By charm magnitude
    print("\n  --- By Charm Magnitude ---")
    for lo, hi, lbl in [(0, 50, "0-50M"), (50, 100, "50-100M"), (100, 250, "100-250M"), (250, 500, "250-500M"), (500, 99999, "500M+")]:
        ct = [t for t in trades if abs(t["charm_M"]) >= lo and abs(t["charm_M"]) < hi]
        if not ct:
            continue
        cw = len([t for t in ct if t["result"] == "WIN"])
        cp = sum(t["pnl"] for t in ct)
        wr = cw / len(ct) * 100
        print(f"    |charm| {lbl:<10} {len(ct)} trades, {cw}W ({wr:.0f}% WR), P&L={cp:+.1f}")

    # By skew change magnitude
    print("\n  --- By Skew Change Magnitude ---")
    for lo, hi, lbl in [(3, 5, "3-5%"), (5, 8, "5-8%"), (8, 12, "8-12%"), (12, 20, "12-20%"), (20, 100, "20%+")]:
        mt = [t for t in trades if lo <= abs(t["skew_chg"]) < hi]
        if not mt:
            continue
        mw = len([t for t in mt if t["result"] == "WIN"])
        mp = sum(t["pnl"] for t in mt)
        wr = mw / len(mt) * 100
        print(f"    |chg| {lbl:<10} {len(mt)} trades, {mw}W ({wr:.0f}% WR), P&L={mp:+.1f}")

    # MFE
    print("\n  --- Max Favorable Excursion ---")
    for pt in [3, 5, 8, 10, 12, 15, 20]:
        reached = len([t for t in trades if t["max_profit"] >= pt])
        print(f"    Reached +{pt}pt: {reached}/{len(trades)} ({reached/len(trades)*100:.0f}%)")

    # Daily P&L
    print("\n  --- Daily P&L ---")
    daily = defaultdict(list)
    for t in trades:
        daily[t["date"]].append(t)
    for d in sorted(daily.keys()):
        dt = daily[d]
        dw = len([t for t in dt if t["result"] == "WIN"])
        dl = len([t for t in dt if t["result"] == "LOSS"])
        dp = sum(t["pnl"] for t in dt)
        print(f"    {d}: {len(dt)} trades ({dw}W/{dl}L), P&L={dp:+.1f}")

    # Drawdown analysis
    running_pnl = 0
    peak = 0
    max_dd = 0
    for t in trades:
        running_pnl += t["pnl"]
        peak = max(peak, running_pnl)
        dd = running_pnl - peak
        max_dd = min(max_dd, dd)
    print(f"\n  Max Drawdown: {max_dd:+.1f} pts")
    print(f"  Final P&L: {running_pnl:+.1f} pts")

    # Consecutive losses
    max_consec_loss = 0
    cur_consec = 0
    for t in trades:
        if t["result"] == "LOSS":
            cur_consec += 1
            max_consec_loss = max(max_consec_loss, cur_consec)
        else:
            cur_consec = 0
    print(f"  Max Consecutive Losses: {max_consec_loss}")

    # Print all trades
    print(f"\n  {'#':<4} {'Date':<12} {'Time':<6} {'Dir':<6} {'Entry':<8} {'Skew':<7} {'Chg%':<7} "
          f"{'ChrmM':<8} {'DD_B':<6} {'Paradigm':<16} {'Result':<8} {'P&L':<7} {'MaxP':<7} {'MaxL':<7} {'Min':<5}")
    print("  " + "-" * 125)
    for idx, t in enumerate(trades, 1):
        dd_str = f"{t['dd_B']:+.1f}" if t['dd_B'] is not None else "?"
        print(f"  {idx:<4} {t['date']:<12} {t['time']:<6} {t['direction']:<6} {t['entry']:<8} "
              f"{t['skew']:<7.3f} {t['skew_chg']:<+7.1f} {t['charm_M']:<+8.0f} {dd_str:<6} "
              f"{t['paradigm']:<16} {t['result']:<8} {t['pnl']:<+7.1f} {t['max_profit']:<+7.1f} "
              f"{t['max_loss']:<+7.1f} {t['elapsed_min']:<5.0f}")

    print()


# ============================================================
# T/S OPTIMIZATION for best config
# ============================================================
print("\n" + "=" * 110)
print("T/S OPTIMIZATION for Config A (win=20, chg>3%, charm>0)")
print("=" * 110)

# Re-run with different T/S combos
for tgt, stp in [(5, 3), (5, 5), (8, 5), (8, 8), (10, 5), (10, 8), (10, 10), (12, 8), (15, 8), (15, 10), (20, 10)]:
    trades = []
    for day in sorted(all_days.keys()):
        chain = all_days[day]["chain"]
        volland = all_days[day]["volland"]
        if len(chain) < 22:
            continue
        last_trade_time = None
        for i, (ts, spot, rows) in enumerate(chain):
            if spot is None or rows is None:
                continue
            t_et = ts.astimezone(NY)
            if t_et.time() < MARKET_START or t_et.time() > MARKET_END:
                continue
            if i < 20:
                continue
            if last_trade_time and (ts - last_trade_time).total_seconds() < 1800:
                continue
            skew_now = compute_skew(rows, spot)
            if skew_now is None:
                continue
            skew_prev = compute_skew(chain[i - 20][2], chain[i - 20][1])
            if skew_prev is None or skew_prev == 0:
                continue
            skew_chg = (skew_now - skew_prev) / skew_prev
            paradigm, charm, dd = get_volland_at(volland, ts)
            if charm is None:
                continue
            charm_M = charm / 1e6
            direction = None
            if skew_chg < -0.03 and charm_M > 0:
                direction = "LONG"
            elif skew_chg > 0.03 and charm_M < 0:
                direction = "SHORT"
            if direction is None:
                continue
            result, pnl, mp, ml, elapsed = sim_trade_detailed(chain, i, direction, tgt, stp)
            trades.append({"result": result, "pnl": round(pnl, 1)})
            last_trade_time = ts

    if trades:
        wins = len([t for t in trades if t["result"] == "WIN"])
        losses = len([t for t in trades if t["result"] == "LOSS"])
        total = sum(t["pnl"] for t in trades)
        wr = wins / len(trades) * 100
        pf = (wins * tgt) / (losses * stp) if losses > 0 else 999
        print(f"  T={tgt:>2}/S={stp:>2}: {len(trades)} trades | {wins}W/{losses}L | "
              f"WR={wr:.0f}% | P&L={total:+.1f} | Avg={total/len(trades):+.1f} | PF={pf:.2f}")


print("\n\nDONE.")
