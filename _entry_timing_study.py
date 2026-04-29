"""
VIX Divergence — Entry Timing Optimization
============================================
Instead of filtering signals, find the exact moment before the explosion.

Ideas to test:
1. IMMEDIATE: current approach — enter when Phase 2 conditions met
2. BREAKOUT: wait for price to break Phase 2 range (consolidation breakout)
3. VIX ACCELERATION: wait for VIX to accelerate its move beyond Phase 2
4. FIRST STRONG BAR: wait for first 1-min bar with body > 2 pts in signal direction
5. DELAYED: enter N minutes after signal (give Phase 2 more time to load)
6. SPX RANGE SHRINK: enter when 5-bar range hits minimum (max compression just before pop)

Uses 1-min OHLC for precise entry, chain_snapshots for signal detection.
"""
import psycopg2
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

P1_SPX_MOVE = 6; P1_VIX_REACT_MAX = 0.20; P1_WIN_MIN = 10; P1_WIN_MAX = 30
P2_VIX_COMPRESS = 0.25; P2_SPX_FLAT = 10; P2_WIN_MIN = 15; P2_WIN_MAX = 60
BAD_DATES = {"2026-03-26"}

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT ts AT TIME ZONE 'America/New_York' as et, spot, vix
    FROM chain_snapshots WHERE spot IS NOT NULL AND vix IS NOT NULL
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '15:30'
    ORDER BY ts;
""")
snaps = [{"ts": r[0], "date": r[0].date(), "spot": float(r[1]), "vix": float(r[2])} for r in cur.fetchall()]

cur.execute("""
    SELECT ts AT TIME ZONE 'America/New_York' as et, bar_open, bar_high, bar_low, bar_close
    FROM spx_ohlc_1m WHERE (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
    ORDER BY ts;
""")
ohlc = [{"ts": r[0], "date": r[0].date(), "open": float(r[1]), "high": float(r[2]),
         "low": float(r[3]), "close": float(r[4])} for r in cur.fetchall()]
conn.close()


def group_by_date(data):
    g = defaultdict(list)
    for d in data: g[d["date"]].append(d)
    return dict(g)

snaps_by_date = group_by_date(snaps)
ohlc_by_date = group_by_date(ohlc)
dates = sorted(set(snaps_by_date.keys()) & set(ohlc_by_date.keys()))


def detect_signals(snaps_day, direction):
    n = len(snaps_day)
    phase1_events = []
    for i in range(n):
        for j in range(i + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[i]["ts"]).total_seconds() / 60
            if mins < P1_WIN_MIN: continue
            if mins > P1_WIN_MAX: break
            if direction == "long":
                sc = snaps_day[i]["spot"] - snaps_day[j]["spot"]
                vr = snaps_day[j]["vix"] - snaps_day[i]["vix"]
            else:
                sc = snaps_day[j]["spot"] - snaps_day[i]["spot"]
                vr = snaps_day[i]["vix"] - snaps_day[j]["vix"]
            if sc >= P1_SPX_MOVE and vr <= P1_VIX_REACT_MAX:
                phase1_events.append({"end_idx": j, "spx_move": sc, "vix_react": vr,
                                      "end_spot": snaps_day[j]["spot"]})
    if not phase1_events: return []
    used = set()
    for p1 in phase1_events:
        if p1["end_idx"] in used: continue
        p2s = p1["end_idx"]
        for j in range(p2s + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[p2s]["ts"]).total_seconds() / 60
            if mins < P2_WIN_MIN: continue
            if mins > P2_WIN_MAX: break
            if direction == "long":
                vc = snaps_day[p2s]["vix"] - snaps_day[j]["vix"]
            else:
                vc = snaps_day[j]["vix"] - snaps_day[p2s]["vix"]
            sr = abs(snaps_day[j]["spot"] - snaps_day[p2s]["spot"])
            if vc >= P2_VIX_COMPRESS and sr <= P2_SPX_FLAT:
                # Compute Phase 2 price range (consolidation zone)
                p2_spots = [snaps_day[k]["spot"] for k in range(p2s, j + 1)]
                p2_high = max(p2_spots)
                p2_low = min(p2_spots)
                p1s = p1["spx_move"]
                grade = "A+" if p1s >= 12 else ("A" if p1s >= 10 else ("B" if p1s >= 8 else "C"))
                used.add(p1["end_idx"])
                return [{"ts": snaps_day[j]["ts"], "spot": snaps_day[j]["spot"], "vix": snaps_day[j]["vix"],
                         "p1_spx": p1["spx_move"], "p1_vix": p1["vix_react"], "p2_vix": vc,
                         "direction": direction, "grade": grade,
                         "p2_high": p2_high, "p2_low": p2_low,
                         "p2_range": p2_high - p2_low}]
    return []


def simulate(ohlc_day, entry_idx, direction, sl, be_trigger=None, activation=None, gap=None, imm_gap=None, max_hold=120):
    if entry_idx is None or entry_idx >= len(ohlc_day): return None
    entry = ohlc_day[entry_idx]["open"]
    stop = -sl; max_p = 0.0; cur_stop = stop; mfe = 0.0; mae = 0.0
    for i in range(entry_idx, min(entry_idx + max_hold, len(ohlc_day))):
        bar = ohlc_day[i]; elapsed = i - entry_idx
        if direction == "long":
            hp = bar["high"] - entry; lp = bar["low"] - entry
        else:
            hp = entry - bar["low"]; lp = entry - bar["high"]
        mfe = max(mfe, hp); mae = max(mae, -lp if lp < 0 else 0)
        if lp <= cur_stop:
            pnl = cur_stop
            reason = "STOP" if cur_stop == stop else ("BE" if cur_stop == 0 else "TRAIL")
            return {"outcome": "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "BE"),
                    "pnl": pnl, "mfe": mfe, "mae": mae, "bars": elapsed, "reason": reason,
                    "entry": entry}
        max_p = max(max_p, hp)
        if be_trigger and max_p >= be_trigger and cur_stop < 0: cur_stop = 0
        if imm_gap is not None:
            tl = max_p - imm_gap
            if tl > cur_stop: cur_stop = tl
        elif activation and gap and max_p >= activation:
            tl = max_p - gap
            if tl > cur_stop: cur_stop = tl
    last = ohlc_day[min(entry_idx + max_hold - 1, len(ohlc_day) - 1)]
    pnl = (last["close"] - entry) if direction == "long" else (entry - last["close"])
    return {"outcome": "EXPIRED", "pnl": pnl, "mfe": mfe, "mae": mae, "bars": max_hold,
            "reason": "EXPIRED", "entry": entry}


def find_signal_bar_idx(ohlc_day, signal_ts):
    """Find the bar index at or after signal_ts."""
    for i, bar in enumerate(ohlc_day):
        if bar["ts"] >= signal_ts:
            return i
    return None


def find_entry_breakout(ohlc_day, sig_idx, direction, p2_high, p2_low, timeout=30):
    """Wait for price to break Phase 2 range. Return bar idx or None."""
    for i in range(sig_idx, min(sig_idx + timeout, len(ohlc_day))):
        if direction == "long" and ohlc_day[i]["high"] > p2_high + 1:
            return i
        if direction == "short" and ohlc_day[i]["low"] < p2_low - 1:
            return i
    return None


def find_entry_strong_bar(ohlc_day, sig_idx, direction, min_body=2.0, timeout=30):
    """Wait for first 1-min bar with body >= min_body in signal direction."""
    for i in range(sig_idx, min(sig_idx + timeout, len(ohlc_day))):
        bar = ohlc_day[i]
        body = bar["close"] - bar["open"]
        if direction == "long" and body >= min_body:
            return i
        if direction == "short" and body <= -min_body:
            return i
    return None


def find_entry_range_squeeze(ohlc_day, sig_idx, direction, lookback=5, timeout=30):
    """Wait for minimum 5-bar range (max compression). Enter on next bar."""
    min_range = 999
    min_range_idx = None
    for i in range(sig_idx + lookback, min(sig_idx + timeout, len(ohlc_day))):
        window = ohlc_day[i - lookback:i]
        hi = max(b["high"] for b in window)
        lo = min(b["low"] for b in window)
        rng = hi - lo
        if rng < min_range:
            min_range = rng
            min_range_idx = i
    return min_range_idx


def find_entry_delayed(ohlc_day, sig_idx, delay_bars):
    """Simple delay: enter N bars after signal."""
    idx = sig_idx + delay_bars
    if idx < len(ohlc_day):
        return idx
    return None


def eval_strategy(name, all_entries):
    """Compute stats for a list of (signal, result) pairs."""
    valid = [(sig, res) for sig, res in all_entries if res is not None]
    filled = len(valid)
    total = len(all_entries)
    if not valid:
        print(f"  {name:>35}: 0 fills / {total} signals")
        return

    wins = sum(1 for _, r in valid if r["pnl"] > 0)
    losses = sum(1 for _, r in valid if r["pnl"] < 0)
    total_pnl = sum(r["pnl"] for _, r in valid)
    avg_mae = sum(r["mae"] for _, r in valid) / len(valid)
    avg_mfe = sum(r["mfe"] for _, r in valid) / len(valid)
    gw = sum(r["pnl"] for _, r in valid if r["pnl"] > 0)
    gl = abs(sum(r["pnl"] for _, r in valid if r["pnl"] < 0))
    pf = gw / max(0.01, gl)
    wr = wins / max(1, wins + losses) * 100

    run = 0; pk = 0; mdd = 0
    for _, r in sorted(valid, key=lambda x: x[0]["ts"]):
        run += r["pnl"]; pk = max(pk, run); mdd = max(mdd, pk - run)

    fill_pct = filled / total * 100
    print(f"  {name:>35}: {filled:>3}t({fill_pct:.0f}%) {wins}W/{losses}L WR={wr:>4.0f}% PnL={total_pnl:>+7.1f} PF={pf:>5.2f} MaxDD={mdd:>5.1f} avgMAE={avg_mae:>4.1f} avgMFE={avg_mfe:>5.1f}")


# ── Collect signals ──
all_signals = []
for date in dates:
    if str(date) in BAD_DATES: continue
    for direction in ["short", "long"]:
        sigs = detect_signals(snaps_by_date[date], direction)
        for sig in sigs:
            if direction == "short" and sig["vix"] >= 26: continue
            all_signals.append(sig)

all_signals.sort(key=lambda x: x["ts"])
print(f"Total signals: {len(all_signals)}\n")

# ── STUDY 1: What happens bar-by-bar after signal? ──
print("=== BAR-BY-BAR FORWARD ACTION (first 20 bars after signal) ===")
print(f"  {'Bar':>4} {'Avg Fav':>8} {'Avg Adv':>8} {'%Fav>0':>7} {'AvgBody':>8}")

bar_data = {i: {"favs": [], "advs": [], "bodies": []} for i in range(20)}

for sig in all_signals:
    date = sig["ts"].date()
    if date not in ohlc_by_date: continue
    ohlc_day = ohlc_by_date[date]
    si = find_signal_bar_idx(ohlc_day, sig["ts"])
    if si is None: continue

    entry = ohlc_day[si]["open"]
    for offset in range(20):
        idx = si + offset
        if idx >= len(ohlc_day): break
        bar = ohlc_day[idx]
        if sig["direction"] == "long":
            fav = bar["close"] - entry
            body = bar["close"] - bar["open"]
        else:
            fav = entry - bar["close"]
            body = bar["open"] - bar["close"]
        adv = -fav if fav < 0 else 0
        bar_data[offset]["favs"].append(fav)
        bar_data[offset]["advs"].append(adv)
        bar_data[offset]["bodies"].append(body)

for i in range(20):
    if not bar_data[i]["favs"]: continue
    avg_fav = sum(bar_data[i]["favs"]) / len(bar_data[i]["favs"])
    avg_adv = sum(bar_data[i]["advs"]) / len(bar_data[i]["advs"])
    pct_fav = sum(1 for f in bar_data[i]["favs"] if f > 0) / len(bar_data[i]["favs"]) * 100
    avg_body = sum(bar_data[i]["bodies"]) / len(bar_data[i]["bodies"])
    print(f"  {i+1:>4} {avg_fav:>+7.2f} {avg_adv:>7.2f} {pct_fav:>6.0f}% {avg_body:>+7.2f}")

# ── STUDY 2: Compare entry strategies ──
print(f"\n{'='*80}")
print("=== ENTRY STRATEGY COMPARISON ===")
print(f"{'='*80}\n")

strategies = {}

for sig in all_signals:
    date = sig["ts"].date()
    if date not in ohlc_by_date: continue
    ohlc_day = ohlc_by_date[date]
    si = find_signal_bar_idx(ohlc_day, sig["ts"])
    if si is None: continue

    d = sig["direction"]
    if d == "short":
        sl, be, act, gp, imm = 8, 8, 10, 5, None
    else:
        sl, be, act, gp, imm = 8, None, None, None, 8

    # 1. IMMEDIATE (baseline)
    res = simulate(ohlc_day, si, d, sl, be, act, gp, imm)
    strategies.setdefault("1_IMMEDIATE", []).append((sig, res))

    # 2. BREAKOUT: wait for price to break Phase 2 range
    for timeout in [15, 30]:
        bo_idx = find_entry_breakout(ohlc_day, si, d, sig["p2_high"], sig["p2_low"], timeout)
        res_bo = simulate(ohlc_day, bo_idx, d, sl, be, act, gp, imm) if bo_idx else None
        strategies.setdefault(f"2_BREAKOUT_t{timeout}", []).append((sig, res_bo))

    # 3. STRONG BAR: wait for first body >= Npt in direction
    for min_body in [1.5, 2.0, 3.0]:
        sb_idx = find_entry_strong_bar(ohlc_day, si, d, min_body, 30)
        res_sb = simulate(ohlc_day, sb_idx, d, sl, be, act, gp, imm) if sb_idx else None
        strategies.setdefault(f"3_STRONG_BAR_{min_body}pt", []).append((sig, res_sb))

    # 4. DELAYED: wait N bars
    for delay in [3, 5, 8, 10, 15]:
        dl_idx = find_entry_delayed(ohlc_day, si, delay)
        res_dl = simulate(ohlc_day, dl_idx, d, sl, be, act, gp, imm) if dl_idx else None
        strategies.setdefault(f"4_DELAY_{delay}bar", []).append((sig, res_dl))

    # 5. RANGE SQUEEZE: wait for minimum 5-bar range
    sq_idx = find_entry_range_squeeze(ohlc_day, si, d, 5, 30)
    res_sq = simulate(ohlc_day, sq_idx, d, sl, be, act, gp, imm) if sq_idx else None
    strategies.setdefault("5_RANGE_SQUEEZE", []).append((sig, res_sq))

    # 6. BREAKOUT + confirmation: breakout bar must close in signal direction
    for timeout in [15, 30]:
        bo_idx = find_entry_breakout(ohlc_day, si, d, sig["p2_high"], sig["p2_low"], timeout)
        if bo_idx and bo_idx < len(ohlc_day):
            bar = ohlc_day[bo_idx]
            body = bar["close"] - bar["open"]
            if (d == "long" and body > 0) or (d == "short" and body < 0):
                res_boc = simulate(ohlc_day, bo_idx + 1, d, sl, be, act, gp, imm) if bo_idx + 1 < len(ohlc_day) else None
            else:
                res_boc = None
        else:
            res_boc = None
        strategies.setdefault(f"6_BREAKOUT_CONFIRM_t{timeout}", []).append((sig, res_boc))

    # 7. BREAKOUT + enter on NEXT bar open (don't chase the breakout bar)
    for timeout in [15, 30]:
        bo_idx = find_entry_breakout(ohlc_day, si, d, sig["p2_high"], sig["p2_low"], timeout)
        if bo_idx and bo_idx + 1 < len(ohlc_day):
            res_bon = simulate(ohlc_day, bo_idx + 1, d, sl, be, act, gp, imm)
        else:
            res_bon = None
        strategies.setdefault(f"7_BREAKOUT_NEXT_t{timeout}", []).append((sig, res_bon))

for name in sorted(strategies.keys()):
    eval_strategy(name, strategies[name])

# ── STUDY 3: Per-signal detail for best strategy ──
# Find best strategy first
print(f"\n{'='*80}")
print("=== PER-SIGNAL DETAIL: IMMEDIATE vs BREAKOUT_t15 ===")
print(f"{'='*80}\n")

print(f"  {'Date':>12} {'Time':>6} {'Dir':>6} {'Gr':>3} {'IMM PnL':>8} {'IMM MAE':>8} {'BO PnL':>8} {'BO MAE':>8} {'BO Wait':>8} {'P2 Rng':>7}")

imm_entries = {sig["ts"]: (sig, res) for sig, res in strategies["1_IMMEDIATE"]}
bo_entries = {sig["ts"]: (sig, res) for sig, res in strategies["2_BREAKOUT_t15"]}

for sig in all_signals:
    _, imm_res = imm_entries.get(sig["ts"], (None, None))
    _, bo_res = bo_entries.get(sig["ts"], (None, None))
    if imm_res is None: continue

    imm_pnl = imm_res["pnl"]
    imm_mae = imm_res["mae"]
    bo_pnl = bo_res["pnl"] if bo_res else 0
    bo_mae = bo_res["mae"] if bo_res else 0
    bo_wait = bo_res.get("bars", 0) if bo_res else -1

    # How many bars until breakout?
    date = sig["ts"].date()
    ohlc_day = ohlc_by_date.get(date, [])
    si = find_signal_bar_idx(ohlc_day, sig["ts"])
    bo_idx = find_entry_breakout(ohlc_day, si, sig["direction"], sig["p2_high"], sig["p2_low"], 15) if si else None
    wait_bars = (bo_idx - si) if bo_idx and si else -1

    marker = " *" if bo_res and bo_res["pnl"] > imm_res["pnl"] else ""
    fill = "FILL" if bo_res else "MISS"

    print(f"  {str(sig['ts'].date()):>12} {str(sig['ts'].time())[:5]:>6} {sig['direction']:>6} {sig['grade']:>3} "
          f"{imm_pnl:>+7.1f} {imm_mae:>7.1f} {bo_pnl:>+7.1f} {bo_mae:>7.1f} {wait_bars:>7}b {sig['p2_range']:>6.1f}{marker}")
