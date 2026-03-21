"""
Single-Bar Absorption Backtest (local JSON data)

Detects single-bar absorption patterns on ES 5-pt range bars:
- BEARISH (sell absorption): Bar closes RED (close < open) despite strongly POSITIVE delta
  -> passive sellers absorbing aggressive buyers at a top -> SHORT signal
- BULLISH (buy absorption): Bar closes GREEN (close > open) despite strongly NEGATIVE delta
  -> passive buyers absorbing aggressive sellers at a bottom -> LONG signal

Volume gate: bar volume >= Nx average (configurable)
Delta gate: |delta| >= Mx average |delta| (configurable)

Forward simulation: SL=8, T=10 on subsequent range bars.
"""

import json, sys
from datetime import datetime, time as dtime
from collections import defaultdict
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

# ── Configuration ──────────────────────────────────────────
VOL_MULT    = 2.5   # trigger bar volume >= N x 20-bar avg
DELTA_MULT  = 2.0   # trigger bar |delta| >= N x 20-bar avg |delta|
SL_PTS      = 8.0
TGT_PTS     = 10.0
COOLDOWN_BARS = 10  # min bars between same-direction signals
MARKET_START = dtime(10, 0)  # ET
MARKET_END   = dtime(15, 45)

# ── Load local data ───────────────────────────────────────
print("Loading tmp_rithmic_bars.json...")
with open("tmp_rithmic_bars.json") as f:
    raw = json.load(f)
print(f"Loaded {len(raw)} bars")

# ── Group by date ──────────────────────────────────────────
bars_by_date = defaultdict(list)
for r in raw:
    # Parse timestamp for market hours filter
    ts_e = r["ts_e"]
    try:
        if "+" in ts_e or ts_e.endswith("Z"):
            dt = datetime.fromisoformat(ts_e.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts_e)
        et = dt.astimezone(NY)
    except Exception:
        et = None

    bars_by_date[r["td"]].append({
        "idx":    r["idx"],
        "open":   r["o"],
        "high":   r["h"],
        "low":    r["l"],
        "close":  r["c"],
        "volume": r["vol"],
        "delta":  r["delta"],
        "cvd":    r["cvd"],
        "et":     et,
    })

dates = sorted(bars_by_date.keys())
print(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)\n")

# ── Detection + Forward Sim ────────────────────────────────
all_trades = []

for td in dates:
    bars = bars_by_date[td]
    last_bull_idx = -100
    last_bear_idx = -100

    for i, bar in enumerate(bars):
        # Need at least 20 prior bars for averages
        if i < 20:
            continue

        # Market hours filter (ET)
        if bar["et"] is None:
            continue
        bar_time = bar["et"].time()
        if not (MARKET_START <= bar_time <= MARKET_END):
            continue

        # ── Volume gate ──
        prior_vols = [b["volume"] for b in bars[i-20:i]]
        vol_avg = sum(prior_vols) / len(prior_vols)
        if vol_avg <= 0:
            continue
        vol_ratio = bar["volume"] / vol_avg
        if vol_ratio < VOL_MULT:
            continue

        # ── Delta gate ──
        prior_deltas = [abs(b["delta"]) for b in bars[i-20:i]]
        delta_avg = sum(prior_deltas) / len(prior_deltas)
        if delta_avg <= 0:
            continue
        delta_ratio = abs(bar["delta"]) / delta_avg
        if delta_ratio < DELTA_MULT:
            continue

        # ── Single-bar absorption check ──
        is_red   = bar["close"] < bar["open"]
        is_green = bar["close"] > bar["open"]
        delta_positive = bar["delta"] > 0
        delta_negative = bar["delta"] < 0

        direction = None
        if is_red and delta_positive:
            # Bar closes red despite aggressive buying -> passive sellers absorbing -> SHORT
            direction = "bearish"
        elif is_green and delta_negative:
            # Bar closes green despite aggressive selling -> passive buyers absorbing -> LONG
            direction = "bullish"

        if direction is None:
            continue

        # ── Cooldown ──
        if direction == "bearish":
            if bar["idx"] - last_bear_idx < COOLDOWN_BARS:
                continue
            last_bear_idx = bar["idx"]
        else:
            if bar["idx"] - last_bull_idx < COOLDOWN_BARS:
                continue
            last_bull_idx = bar["idx"]

        # ── Forward simulation on subsequent bars ──
        entry_price = bar["close"]
        if direction == "bearish":
            target_price = entry_price - TGT_PTS
            stop_price   = entry_price + SL_PTS
        else:
            target_price = entry_price + TGT_PTS
            stop_price   = entry_price - SL_PTS

        outcome = "EXPIRED"
        exit_price = None
        max_favorable = 0.0
        max_adverse = 0.0

        for j in range(i + 1, len(bars)):
            fb = bars[j]
            if direction == "bearish":
                excursion_fav = entry_price - fb["low"]
                excursion_adv = fb["high"] - entry_price
                max_favorable = max(max_favorable, excursion_fav)
                max_adverse = max(max_adverse, excursion_adv)

                # Check stop first (worst case)
                if fb["high"] >= stop_price:
                    outcome = "LOSS"
                    exit_price = stop_price
                    if fb["low"] <= target_price:
                        if fb["open"] >= entry_price:
                            outcome = "LOSS"
                        else:
                            outcome = "WIN"
                            exit_price = target_price
                    break
                if fb["low"] <= target_price:
                    outcome = "WIN"
                    exit_price = target_price
                    break
            else:  # bullish
                excursion_fav = fb["high"] - entry_price
                excursion_adv = entry_price - fb["low"]
                max_favorable = max(max_favorable, excursion_fav)
                max_adverse = max(max_adverse, excursion_adv)

                if fb["low"] <= stop_price:
                    outcome = "LOSS"
                    exit_price = stop_price
                    if fb["high"] >= target_price:
                        if fb["open"] <= entry_price:
                            outcome = "LOSS"
                        else:
                            outcome = "WIN"
                            exit_price = target_price
                    break
                if fb["high"] >= target_price:
                    outcome = "WIN"
                    exit_price = target_price
                    break

        pnl = 0.0
        if outcome == "WIN":
            pnl = TGT_PTS
        elif outcome == "LOSS":
            pnl = -SL_PTS

        all_trades.append({
            "date":       td,
            "time":       bar_time.strftime("%H:%M"),
            "bar_idx":    bar["idx"],
            "direction":  direction,
            "entry":      entry_price,
            "volume":     bar["volume"],
            "vol_ratio":  round(vol_ratio, 1),
            "delta":      bar["delta"],
            "delta_ratio": round(delta_ratio, 1),
            "outcome":    outcome,
            "pnl":        pnl,
            "mfe":        round(max_favorable, 2),
            "mae":        round(max_adverse, 2),
        })

# ── Results ────────────────────────────────────────────────
print("=" * 120)
print(f"SINGLE-BAR ABSORPTION BACKTEST  |  Vol>={VOL_MULT}x  Delta>={DELTA_MULT}x  SL={SL_PTS}  T={TGT_PTS}  CD={COOLDOWN_BARS} bars")
print("=" * 120)

if not all_trades:
    print("No signals found!")
    sys.exit(0)

wins   = [t for t in all_trades if t["outcome"] == "WIN"]
losses = [t for t in all_trades if t["outcome"] == "LOSS"]
expired = [t for t in all_trades if t["outcome"] == "EXPIRED"]
total_pnl = sum(t["pnl"] for t in all_trades)

bears = [t for t in all_trades if t["direction"] == "bearish"]
bulls = [t for t in all_trades if t["direction"] == "bullish"]
bear_wins = [t for t in bears if t["outcome"] == "WIN"]
bull_wins = [t for t in bulls if t["outcome"] == "WIN"]

print(f"\nTotal signals: {len(all_trades)}  |  Wins: {len(wins)}  Losses: {len(losses)}  Expired: {len(expired)}")
print(f"Win Rate: {len(wins)/max(1,len(wins)+len(losses))*100:.1f}%")
print(f"Total PnL: {total_pnl:+.1f} pts")
print(f"Avg PnL/trade: {total_pnl/len(all_trades):+.2f} pts")
if losses:
    print(f"Profit Factor: {sum(t['pnl'] for t in wins)/abs(sum(t['pnl'] for t in losses)):.2f}x")

print(f"\nBearish (shorts): {len(bears)} signals, {len(bear_wins)} wins ({len(bear_wins)/max(1,len(bears))*100:.0f}% WR)")
print(f"Bullish (longs):  {len(bulls)} signals, {len(bull_wins)} wins ({len(bull_wins)/max(1,len(bulls))*100:.0f}% WR)")

# MFE/MAE analysis
avg_mfe = sum(t["mfe"] for t in all_trades) / len(all_trades)
avg_mae = sum(t["mae"] for t in all_trades) / len(all_trades)
print(f"\nAvg MFE: {avg_mfe:.1f} pts  |  Avg MAE: {avg_mae:.1f} pts")

win_mfe = sum(t["mfe"] for t in wins) / max(1, len(wins))
loss_mfe = sum(t["mfe"] for t in losses) / max(1, len(losses))
win_mae = sum(t["mae"] for t in wins) / max(1, len(wins))
loss_mae = sum(t["mae"] for t in losses) / max(1, len(losses))
print(f"Winners:  avg MFE={win_mfe:.1f}  avg MAE={win_mae:.1f}")
print(f"Losers:   avg MFE={loss_mfe:.1f}  avg MAE={loss_mae:.1f}")

# Daily breakdown
print(f"\n{'Date':<14} {'Signals':>8} {'W':>4} {'L':>4} {'PnL':>8} {'CumPnL':>8} {'Trades'}")
print("-" * 120)
by_date = defaultdict(list)
for t in all_trades:
    by_date[t["date"]].append(t)
cum_pnl = 0
for d in sorted(by_date.keys()):
    trades = by_date[d]
    day_w = sum(1 for t in trades if t["outcome"] == "WIN")
    day_l = sum(1 for t in trades if t["outcome"] == "LOSS")
    day_pnl = sum(t["pnl"] for t in trades)
    cum_pnl += day_pnl
    trade_strs = []
    for t in trades:
        arrow = "SHORT" if t["direction"] == "bearish" else "LONG"
        trade_strs.append(f"{t['time']} {arrow} @{t['entry']:.0f} v={t['vol_ratio']}x d={t['delta']:+d}({t['delta_ratio']}x) -> {t['outcome']}({t['pnl']:+.0f})")
    print(f"{str(d):<14} {len(trades):>8} {day_w:>4} {day_l:>4} {day_pnl:>+8.1f} {cum_pnl:>+8.1f}  {' | '.join(trade_strs)}")
print("-" * 120)
print(f"{'TOTAL':<14} {len(all_trades):>8} {len(wins):>4} {len(losses):>4} {total_pnl:>+8.1f}")

# MaxDD
running_pnl = 0
peak_pnl = 0
max_dd = 0
for t in all_trades:
    running_pnl += t["pnl"]
    peak_pnl = max(peak_pnl, running_pnl)
    dd = peak_pnl - running_pnl
    max_dd = max(max_dd, dd)
print(f"\nMax Drawdown: {max_dd:.1f} pts")

# Volume ratio distribution
print(f"\n--- Volume Ratio Distribution ---")
for bucket_min, bucket_max in [(2.0, 2.5), (2.5, 3.0), (3.0, 4.0), (4.0, 5.0), (5.0, 100)]:
    bucket = [t for t in all_trades if bucket_min <= t["vol_ratio"] < bucket_max]
    if bucket:
        bw = sum(1 for t in bucket if t["outcome"] == "WIN")
        bl = sum(1 for t in bucket if t["outcome"] == "LOSS")
        bpnl = sum(t["pnl"] for t in bucket)
        print(f"  Vol {bucket_min:.1f}-{bucket_max:.1f}x: {len(bucket)} trades, {bw}W/{bl}L, WR={bw/max(1,bw+bl)*100:.0f}%, PnL={bpnl:+.1f}")

# Delta ratio distribution
print(f"\n--- Delta Ratio Distribution ---")
for bucket_min, bucket_max in [(2.0, 3.0), (3.0, 4.0), (4.0, 6.0), (6.0, 100)]:
    bucket = [t for t in all_trades if bucket_min <= t["delta_ratio"] < bucket_max]
    if bucket:
        bw = sum(1 for t in bucket if t["outcome"] == "WIN")
        bl = sum(1 for t in bucket if t["outcome"] == "LOSS")
        bpnl = sum(t["pnl"] for t in bucket)
        print(f"  Delta {bucket_min:.1f}-{bucket_max:.1f}x: {len(bucket)} trades, {bw}W/{bl}L, WR={bw/max(1,bw+bl)*100:.0f}%, PnL={bpnl:+.1f}")

# ── Sweep: try different thresholds ──────────────────────
print(f"\n{'='*80}")
print("PARAMETER SWEEP")
print(f"{'='*80}")
print(f"{'VolMult':>8} {'DeltaMult':>10} {'Signals':>8} {'W':>4} {'L':>4} {'WR%':>6} {'PnL':>8} {'PF':>6} {'MaxDD':>7}")
print("-" * 70)

for vm in [1.5, 2.0, 2.5, 3.0, 3.5, 4.0]:
    for dm in [1.5, 2.0, 2.5, 3.0, 4.0]:
        sweep_trades = []
        for td in dates:
            bars = bars_by_date[td]
            lb = -100
            ls = -100
            for i, bar in enumerate(bars):
                if i < 20:
                    continue
                if bar["et"] is None:
                    continue
                bt = bar["et"].time()
                if not (MARKET_START <= bt <= MARKET_END):
                    continue
                pv = [b["volume"] for b in bars[i-20:i]]
                va = sum(pv)/len(pv)
                if va <= 0:
                    continue
                vr = bar["volume"]/va
                if vr < vm:
                    continue
                pd = [abs(b["delta"]) for b in bars[i-20:i]]
                da = sum(pd)/len(pd)
                if da <= 0:
                    continue
                dr = abs(bar["delta"])/da
                if dr < dm:
                    continue
                ir = bar["close"] < bar["open"]
                ig = bar["close"] > bar["open"]
                dp = bar["delta"] > 0
                dn = bar["delta"] < 0
                d = None
                if ir and dp:
                    d = "bearish"
                elif ig and dn:
                    d = "bullish"
                if d is None:
                    continue
                if d == "bearish":
                    if bar["idx"] - ls < COOLDOWN_BARS:
                        continue
                    ls = bar["idx"]
                else:
                    if bar["idx"] - lb < COOLDOWN_BARS:
                        continue
                    lb = bar["idx"]
                ep = bar["close"]
                tp_ = ep - TGT_PTS if d == "bearish" else ep + TGT_PTS
                sp_ = ep + SL_PTS if d == "bearish" else ep - SL_PTS
                oc = "EXPIRED"
                for j in range(i+1, len(bars)):
                    fb = bars[j]
                    if d == "bearish":
                        if fb["high"] >= sp_:
                            oc = "LOSS"
                            if fb["low"] <= tp_:
                                oc = "LOSS" if fb["open"] >= ep else "WIN"
                            break
                        if fb["low"] <= tp_:
                            oc = "WIN"
                            break
                    else:
                        if fb["low"] <= sp_:
                            oc = "LOSS"
                            if fb["high"] >= tp_:
                                oc = "LOSS" if fb["open"] <= ep else "WIN"
                            break
                        if fb["high"] >= tp_:
                            oc = "WIN"
                            break
                p = TGT_PTS if oc == "WIN" else (-SL_PTS if oc == "LOSS" else 0)
                sweep_trades.append({"outcome": oc, "pnl": p})

        sw = sum(1 for t in sweep_trades if t["outcome"] == "WIN")
        sl = sum(1 for t in sweep_trades if t["outcome"] == "LOSS")
        sp = sum(t["pnl"] for t in sweep_trades)
        if sw + sl == 0:
            continue
        wr = sw / (sw + sl) * 100
        pf = sum(t["pnl"] for t in sweep_trades if t["pnl"] > 0) / max(0.01, abs(sum(t["pnl"] for t in sweep_trades if t["pnl"] < 0)))
        # MaxDD
        rp = 0; pp = 0; md = 0
        for t in sweep_trades:
            rp += t["pnl"]; pp = max(pp, rp); md = max(md, pp - rp)
        print(f"{vm:>8.1f} {dm:>10.1f} {len(sweep_trades):>8} {sw:>4} {sl:>4} {wr:>5.0f}% {sp:>+8.1f} {pf:>5.2f}x {md:>6.1f}")
