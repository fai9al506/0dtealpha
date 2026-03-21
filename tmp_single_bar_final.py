"""
Final targeted sweep: Single-Bar Absorption with best filters.
Focus on CVD alignment + SVB + SL optimization.
"""

import json, sys
from datetime import datetime, time as dtime
from collections import defaultdict
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

# Load data
with open("tmp_rithmic_bars.json") as f:
    raw_bars = json.load(f)
with open("tmp_volland_stats.json") as f:
    raw_volland = json.load(f)

# Parse Volland
volland_by_ts = []
for v in raw_volland:
    try:
        ts = v["ts"]
        if "+" in ts or ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts).replace(tzinfo=ZoneInfo("UTC"))
        volland_by_ts.append({"dt": dt, **v})
    except Exception:
        pass
volland_by_ts.sort(key=lambda x: x["dt"])


def parse_dd_numeric(s):
    if not s or s == "$0":
        return 0
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0


def parse_charm_numeric(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace("$", "").replace(",", ""))
    except Exception:
        return None


def parse_lis_numeric(s):
    if not s or s == "N/A":
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def get_volland_at(dt):
    lo, hi = 0, len(volland_by_ts) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if volland_by_ts[mid]["dt"] <= dt:
            result = volland_by_ts[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return result


# Parse bars
MARKET_START = dtime(10, 0)
MARKET_END = dtime(15, 45)
COOLDOWN_BARS = 10

bars_by_date = defaultdict(list)
for r in raw_bars:
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
        "idx": r["idx"], "open": r["o"], "high": r["h"], "low": r["l"],
        "close": r["c"], "volume": r["vol"], "delta": r["delta"],
        "cvd": r["cvd"], "et": et, "dt_utc": dt if et else None,
    })

dates = sorted(bars_by_date.keys())


def run_backtest(vol_min, delta_min, sl, tgt, require_cvd_aligned=False,
                 require_svb_positive=False, require_price_exhaustion=False,
                 require_charm_aligned=False, block_price_continuation=False,
                 require_volland_factor=False):
    """Run a single backtest with given parameters."""
    trades = []
    for td in dates:
        bars = bars_by_date[td]
        lb, ls = -100, -100
        for i, bar in enumerate(bars):
            if i < 20 or bar["et"] is None:
                continue
            bt = bar["et"].time()
            if not (MARKET_START <= bt <= MARKET_END):
                continue

            # Volume + delta gates
            pv = [b["volume"] for b in bars[i - 20:i]]
            va = sum(pv) / len(pv)
            if va <= 0:
                continue
            vr = bar["volume"] / va
            if vr < vol_min:
                continue

            pd = [abs(b["delta"]) for b in bars[i - 20:i]]
            da = sum(pd) / len(pd)
            if da <= 0:
                continue
            dr = abs(bar["delta"]) / da
            if dr < delta_min:
                continue

            # Absorption check
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

            # Cooldown
            if d == "bearish":
                if bar["idx"] - ls < COOLDOWN_BARS:
                    continue
                ls = bar["idx"]
            else:
                if bar["idx"] - lb < COOLDOWN_BARS:
                    continue
                lb = bar["idx"]

            # CVD trend (8-bar)
            cvd_trend = bar["cvd"] - bars[i - 8]["cvd"] if i >= 8 else 0
            price_trend = bar["close"] - bars[i - 8]["close"] if i >= 8 else 0

            # CVD alignment filter
            if require_cvd_aligned:
                if d == "bearish" and cvd_trend <= 0:
                    continue
                if d == "bullish" and cvd_trend >= 0:
                    continue

            # Price exhaustion filter (block continuation)
            if require_price_exhaustion:
                if d == "bearish" and price_trend <= 0:
                    continue
                if d == "bullish" and price_trend >= 0:
                    continue

            # Block price continuation (bearish during downtrend, bullish during uptrend)
            if block_price_continuation:
                if d == "bearish" and price_trend < -5:
                    continue
                if d == "bullish" and price_trend > 5:
                    continue

            # Volland filters
            vol_data = get_volland_at(bar["dt_utc"]) if bar["dt_utc"] else None
            paradigm = ""
            dd_num = 0
            charm = None
            svb = None

            if vol_data:
                paradigm = (vol_data.get("paradigm") or "").upper()
                dd_num = parse_dd_numeric(vol_data.get("dd_hedging") or "")
                charm = parse_charm_numeric(vol_data.get("charm"))
                svb_raw = vol_data.get("svb")
                if isinstance(svb_raw, dict):
                    svb = svb_raw.get("correlation")
                elif isinstance(svb_raw, (int, float)):
                    svb = float(svb_raw)

            if require_svb_positive:
                if svb is None or svb < 0:
                    continue

            if require_charm_aligned:
                if d == "bullish" and (charm is None or charm <= 0):
                    continue
                if d == "bearish" and (charm is None or charm >= 0):
                    continue

            if require_volland_factor:
                has_any = False
                if d == "bullish":
                    if "GEX" in paradigm: has_any = True
                    if dd_num > 200_000_000: has_any = True
                    if charm is not None and charm > 0: has_any = True
                else:
                    if "AG" in paradigm: has_any = True
                    if dd_num < -200_000_000: has_any = True
                    if charm is not None and charm < 0: has_any = True
                if not has_any:
                    continue

            # Forward sim
            ep = bar["close"]
            tp_ = ep - tgt if d == "bearish" else ep + tgt
            sp_ = ep + sl if d == "bearish" else ep - sl
            oc = "EXPIRED"
            mfe, mae = 0.0, 0.0

            for j in range(i + 1, len(bars)):
                fb = bars[j]
                if d == "bearish":
                    mfe = max(mfe, ep - fb["low"])
                    mae = max(mae, fb["high"] - ep)
                    if fb["high"] >= sp_:
                        oc = "LOSS"
                        if fb["low"] <= tp_:
                            oc = "LOSS" if fb["open"] >= ep else "WIN"
                        break
                    if fb["low"] <= tp_:
                        oc = "WIN"
                        break
                else:
                    mfe = max(mfe, fb["high"] - ep)
                    mae = max(mae, ep - fb["low"])
                    if fb["low"] <= sp_:
                        oc = "LOSS"
                        if fb["high"] >= tp_:
                            oc = "LOSS" if fb["open"] <= ep else "WIN"
                        break
                    if fb["high"] >= tp_:
                        oc = "WIN"
                        break

            pnl = tgt if oc == "WIN" else (-sl if oc == "LOSS" else 0)
            trades.append({
                "date": td, "time": bt.strftime("%H:%M"), "dir": d,
                "entry": ep, "vr": vr, "delta": bar["delta"], "dr": dr,
                "cvd_trend": cvd_trend, "price_trend": price_trend,
                "paradigm": paradigm, "dd": dd_num, "charm": charm, "svb": svb,
                "oc": oc, "pnl": pnl, "mfe": mfe, "mae": mae,
            })

    return trades


def report(trades, label, sl, tgt, show_trades=False):
    if not trades:
        print(f"  {label}: 0 signals")
        return
    w = sum(1 for t in trades if t["oc"] == "WIN")
    l = sum(1 for t in trades if t["oc"] == "LOSS")
    total_pnl = sum(t["pnl"] for t in trades)
    wr = w / (w + l) * 100 if (w + l) else 0
    pf = (tgt * w) / (sl * l) if (sl * l) > 0 else 999
    # MaxDD
    rp, pp, md = 0, 0, 0
    for t in trades:
        rp += t["pnl"]; pp = max(pp, rp); md = max(md, pp - rp)
    avg_mfe = sum(t["mfe"] for t in trades) / len(trades)
    avg_mae = sum(t["mae"] for t in trades) / len(trades)
    bears = sum(1 for t in trades if t["dir"] == "bearish")
    bulls = sum(1 for t in trades if t["dir"] == "bullish")
    print(f"  {label}: {len(trades)} sig ({bears}S/{bulls}L), {w}W/{l}L, WR={wr:.0f}%, PnL={total_pnl:+.1f}, PF={pf:.2f}x, MaxDD={md:.0f}, MFE={avg_mfe:.1f}, MAE={avg_mae:.1f}")

    if show_trades:
        for t in trades:
            d_label = "SHORT" if t["dir"] == "bearish" else "LONG "
            charm_s = f"{t['charm']/1e6:+.1f}M" if t["charm"] else "n/a"
            dd_s = f"{t['dd']/1e6:+.0f}M" if t["dd"] else "0"
            svb_s = f"{t['svb']:.2f}" if t["svb"] is not None else "n/a"
            print(f"    {t['date']} {t['time']} {d_label} @{t['entry']:.0f} v={t['vr']:.1f}x d={t['delta']:+d}({t['dr']:.1f}x) cvd={t['cvd_trend']:+d} pt={t['price_trend']:+.1f} {t['paradigm'] or 'n/a':<14} dd={dd_s:>7} charm={charm_s:>8} svb={svb_s:>5} -> {t['oc']}({t['pnl']:+.0f}) MFE={t['mfe']:.1f} MAE={t['mae']:.1f}")


# ==========================================================
print("=" * 100)
print("FINAL SWEEP: BEST FILTER COMBINATIONS")
print("=" * 100)

# BASELINE
print("\n-- BASELINE (vol>=2.0x, delta>=2.0x, no filters) --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER A: CVD aligned only
print("\n-- FILTER A: CVD trend aligned --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, require_cvd_aligned=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER B: SVB >= 0
print("\n-- FILTER B: SVB >= 0 --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, require_svb_positive=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER C: CVD aligned + SVB >= 0
print("\n-- FILTER C: CVD aligned + SVB >= 0 --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, require_cvd_aligned=True, require_svb_positive=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER D: Block price continuation (no shorts in downtrend, no longs in uptrend)
print("\n-- FILTER D: Block price continuation --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, block_price_continuation=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER E: CVD aligned + block price continuation
print("\n-- FILTER E: CVD aligned + block continuation --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, require_cvd_aligned=True, block_price_continuation=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER F: CVD aligned + any Volland factor
print("\n-- FILTER F: CVD aligned + any Volland factor --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, require_cvd_aligned=True, require_volland_factor=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER G: CVD aligned + charm aligned
print("\n-- FILTER G: CVD aligned + charm aligned --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.0, 2.0, sl, tgt, require_cvd_aligned=True, require_charm_aligned=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER H: Higher vol threshold + CVD aligned
print("\n-- FILTER H: Vol>=2.5x + CVD aligned --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.5, 2.0, sl, tgt, require_cvd_aligned=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER I: Higher vol + higher delta + CVD aligned
print("\n-- FILTER I: Vol>=2.5x + Delta>=2.5x + CVD aligned --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(2.5, 2.5, sl, tgt, require_cvd_aligned=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER J: Vol>=3.0x only (volume is king)
print("\n-- FILTER J: Vol>=3.0x only --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(3.0, 1.5, sl, tgt)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER K: Vol>=3.0x + CVD aligned
print("\n-- FILTER K: Vol>=3.0x + CVD aligned --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(3.0, 1.5, sl, tgt, require_cvd_aligned=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)

# FILTER L: Looser vol but strict CVD + SVB + block continuation
print("\n-- FILTER L: Vol>=1.5x + Delta>=2.0x + CVD aligned + SVB>=0 + block continuation --")
for sl, tgt in [(6, 8), (6, 10), (8, 10), (8, 12), (10, 12), (10, 15)]:
    t = run_backtest(1.5, 2.0, sl, tgt, require_cvd_aligned=True, require_svb_positive=True, block_price_continuation=True)
    report(t, f"SL={sl}/T={tgt}", sl, tgt)


# ==========================================================
# DETAILED TRADES for the top candidates
print("\n" + "=" * 100)
print("DETAILED TRADES: Best candidates")
print("=" * 100)

print("\n-- CVD aligned + SVB>=0, SL=6/T=8 --")
t = run_backtest(2.0, 2.0, 6, 8, require_cvd_aligned=True, require_svb_positive=True)
report(t, "CVD+SVB SL6/T8", 6, 8, show_trades=True)

print("\n-- CVD aligned + SVB>=0, SL=8/T=10 --")
t = run_backtest(2.0, 2.0, 8, 10, require_cvd_aligned=True, require_svb_positive=True)
report(t, "CVD+SVB SL8/T10", 8, 10, show_trades=True)

print("\n-- CVD aligned + block continuation, SL=6/T=8 --")
t = run_backtest(2.0, 2.0, 6, 8, require_cvd_aligned=True, block_price_continuation=True)
report(t, "CVD+NoContinu SL6/T8", 6, 8, show_trades=True)

print("\n-- CVD aligned + any Volland factor, SL=8/T=10 --")
t = run_backtest(2.0, 2.0, 8, 10, require_cvd_aligned=True, require_volland_factor=True)
report(t, "CVD+Volland SL8/T10", 8, 10, show_trades=True)

print("\n-- Vol>=3.0x + CVD aligned, SL=6/T=8 --")
t = run_backtest(3.0, 1.5, 6, 8, require_cvd_aligned=True)
report(t, "Vol3+CVD SL6/T8", 6, 8, show_trades=True)


# ==========================================================
# MFE analysis for optimal SL/TGT
print("\n" + "=" * 100)
print("MFE/MAE ANALYSIS for CVD-aligned signals (vol>=2.0x, delta>=2.0x)")
print("=" * 100)

t = run_backtest(2.0, 2.0, 100, 100, require_cvd_aligned=True)  # infinite SL/TGT to see raw MFE/MAE
print(f"\n{len(t)} CVD-aligned signals with uncapped SL/TGT:")
for trade in t:
    d_label = "SHORT" if trade["dir"] == "bearish" else "LONG "
    print(f"  {trade['date']} {trade['time']} {d_label} @{trade['entry']:.0f} v={trade['vr']:.1f}x d={trade['delta']:+d}({trade['dr']:.1f}x) MFE={trade['mfe']:.1f} MAE={trade['mae']:.1f}")

mfes = [t["mfe"] for t in t]
maes = [t["mae"] for t in t]
print(f"\nMFE distribution: min={min(mfes):.1f} p25={sorted(mfes)[len(mfes)//4]:.1f} median={sorted(mfes)[len(mfes)//2]:.1f} p75={sorted(mfes)[3*len(mfes)//4]:.1f} max={max(mfes):.1f}")
print(f"MAE distribution: min={min(maes):.1f} p25={sorted(maes)[len(maes)//4]:.1f} median={sorted(maes)[len(maes)//2]:.1f} p75={sorted(maes)[3*len(maes)//4]:.1f} max={max(maes):.1f}")

# Percentage reaching various targets
for target in [5, 8, 10, 12, 15, 20]:
    reached = sum(1 for m in mfes if m >= target)
    print(f"  MFE >= {target:>2} pts: {reached}/{len(mfes)} ({reached/len(mfes)*100:.0f}%)")

print()
for stop in [4, 6, 8, 10, 12]:
    breached = sum(1 for m in maes if m >= stop)
    print(f"  MAE >= {stop:>2} pts: {breached}/{len(maes)} ({breached/len(maes)*100:.0f}%)")
