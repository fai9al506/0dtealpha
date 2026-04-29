"""
ES Absorption Context-Features Detector v3 — Comprehensive Backtest
March 2026 rithmic 5-pt range bars, market hours only.

KEY INSIGHT: On 5-pt range bars, single-bar metrics (DvP, body ratio) are NOT
discriminating because ALL bars have tiny bodies relative to delta. The user
reads STRUCTURAL patterns across multiple bars:
  1. Price at a swing high/low (structural level)
  2. Multi-bar delta divergence from price (3-8 bar trend)
  3. Delta cluster (multiple bars with same-sign delta at extreme)
  4. Volume climax at end of directional move

This version computes STRUCTURAL features and uses a grid over them.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlalchemy as sa
import warnings, os, time as _time

warnings.filterwarnings("ignore")

CSV_PATH = "G:/My Drive/Python/MyProject/GitHub/0dtealpha/exports/es_range_bars_march_volrate.csv"
DB_URL = os.environ.get("DATABASE_URL",
    "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")
EXPORT_PATH = "G:/My Drive/Python/MyProject/GitHub/0dtealpha/exports/manual_absorption_study_v3_context.csv"

SL_PTS = 8.0; TRAIL_GAP = 8.0; TRAIL_ACTIVATION = 0.0; TIMEOUT_BARS = 100

MANUAL_SIGNALS = [
    {"date":"2026-03-27","time_start":"15:57","time_end":"15:59","direction":"BEAR"},
    {"date":"2026-03-27","time_start":"15:04","time_end":"15:04","direction":"BULL"},
    {"date":"2026-03-27","time_start":"13:00","time_end":"13:00","direction":"BULL"},
    {"date":"2026-03-27","time_start":"12:37","time_end":"12:40","direction":"BULL"},
    {"date":"2026-03-27","time_start":"11:37","time_end":"11:37","direction":"BEAR"},
    {"date":"2026-03-27","time_start":"11:17","time_end":"11:20","direction":"BULL"},
    {"date":"2026-03-27","time_start":"11:06","time_end":"11:06","direction":"BEAR"},
    {"date":"2026-03-27","time_start":"10:45","time_end":"10:46","direction":"BEAR"},
    {"date":"2026-03-27","time_start":"09:47","time_end":"09:48","direction":"BEAR"},
    {"date":"2026-03-27","time_start":"09:35","time_end":"09:36","direction":"BULL"},
    {"date":"2026-03-16","time_start":"09:57","time_end":"09:57","direction":"BULL"},
    {"date":"2026-03-16","time_start":"10:59","time_end":"10:59","direction":"BEAR"},
    {"date":"2026-03-16","time_start":"11:22","time_end":"11:37","direction":"BEAR"},
    {"date":"2026-03-16","time_start":"11:58","time_end":"11:58","direction":"BULL"},
]

print("=" * 80)
print("ES ABSORPTION v3 — STRUCTURAL FEATURES")
print("Swing detection + multi-bar divergence + delta clustering + volume climax")
print("=" * 80)
t0 = _time.time()

# ── STEP 1: Load Data ──────────────────────────────────────────────────
print("\n[1] Loading data...")
df = pd.read_csv(CSV_PATH)
df["ts_utc"] = pd.to_datetime(df["ts_start_utc"])
df["ts_et"] = df["ts_utc"].dt.tz_localize("UTC").dt.tz_convert("America/New_York")
df["hour_et"] = df["ts_et"].dt.hour
df["minute_et"] = df["ts_et"].dt.minute
df["time_et_str"] = df["ts_et"].dt.strftime("%H:%M")
df["trade_date"] = df["ts_et"].dt.strftime("%Y-%m-%d")

mask = (((df["hour_et"]==9)&(df["minute_et"]>=30))|((df["hour_et"]>=10)&(df["hour_et"]<16)))
df = df[mask].reset_index(drop=True)
n_days = df["trade_date"].nunique()
print(f"  {len(df)} market-hours bars, {n_days} days")

for col in ["open","high","low","close","volume","delta","buy_volume","sell_volume",
            "duration_sec","vol_per_sec","body_size","cvd","cvd_open","cvd_high","cvd_low","cvd_close"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
df["bar_direction"] = np.where(df["close"]>df["open"],1,np.where(df["close"]<df["open"],-1,0))

# ── STEP 2: Compute STRUCTURAL Features ────────────────────────────────
print("\n[2] Computing structural features...")

feat_cols = [
    # Swing detection
    "is_swing_high", "is_swing_low",
    "bars_since_swing_high", "bars_since_swing_low",
    # Multi-bar divergence (CVD vs price over N bars)
    "cvd_price_div_5", "cvd_price_div_8",
    # Price trend
    "price_trend_5", "price_trend_8",
    "trend_consistency_5", "trend_bars",
    # Delta clustering (N consecutive same-sign delta bars)
    "delta_cluster_len", "delta_cluster_sum",
    # Delta extremity
    "delta_zscore", "delta_percentile",
    # Volume
    "vol_rate_zscore", "is_volume_climax",
    # Price structure
    "is_at_extreme_10", "is_at_extreme_20",
    "bar_position_in_range_20",
    # Body/absorption
    "body_ratio",
    # Composite: trend reversal score
    "trend_reversal_score",
]

for c in feat_cols:
    df[c] = 0.0

for date, grp in df.groupby("trade_date"):
    n = len(grp); idx = grp.index.values
    c_ = grp["close"].values.astype(float)
    o_ = grp["open"].values.astype(float)
    h_ = grp["high"].values.astype(float)
    l_ = grp["low"].values.astype(float)
    d_ = grp["delta"].values.astype(float)
    vr_ = grp["vol_per_sec"].values.astype(float)
    dirs_ = grp["bar_direction"].values.astype(float)
    bs_ = grp["body_size"].values.astype(float)
    cvd_ = grp["cvd_close"].values.astype(float)
    br_raw = h_ - l_
    br_ = np.where(br_raw==0, 0.25, br_raw)

    r = {fc: np.zeros(n) for fc in feat_cols}

    for i in range(n):
        # ── Swing Detection (3-bar pivot) ──
        # Swing high: high[i] >= high[i-1] and high[i] >= high[i-2] and high[i] >= high[i+1] and high[i] >= high[i+2]
        if 2 <= i < n-2:
            if h_[i] >= h_[i-1] and h_[i] >= h_[i-2] and h_[i] >= h_[i+1] and h_[i] >= h_[i+2]:
                r["is_swing_high"][i] = 1
            if l_[i] <= l_[i-1] and l_[i] <= l_[i-2] and l_[i] <= l_[i+1] and l_[i] <= l_[i+2]:
                r["is_swing_low"][i] = 1

        # bars_since_swing (look back up to 30 bars)
        for j in range(1, min(i+1, 30)+1):
            if r["is_swing_high"][i-j] if (i-j >= 0 and i-j < n) else False:
                r["bars_since_swing_high"][i] = j; break
        else:
            r["bars_since_swing_high"][i] = 30

        for j in range(1, min(i+1, 30)+1):
            pi = i - j
            if pi >= 0 and r["is_swing_low"][pi]:
                r["bars_since_swing_low"][i] = j; break
        else:
            r["bars_since_swing_low"][i] = 30

        # ── Multi-bar CVD vs Price Divergence ──
        # Compute normalized slopes over lookback
        for lb, col_name in [(5, "cvd_price_div_5"), (8, "cvd_price_div_8")]:
            if i >= lb:
                # Price slope (normalized by range)
                price_change = c_[i] - c_[i-lb]
                price_range = max(h_[i-lb:i+1].max() - l_[i-lb:i+1].min(), 1)
                price_norm = price_change / price_range

                # CVD slope (normalized by std)
                cvd_vals = cvd_[i-lb:i+1]
                if not np.any(np.isnan(cvd_vals)):
                    cvd_change = cvd_vals[-1] - cvd_vals[0]
                    cvd_std = max(np.std(cvd_vals), 1)
                    cvd_norm = cvd_change / (np.abs(cvd_vals).max() + 1)

                    # Divergence = price going one way, CVD going the other
                    # Positive div = price up but CVD down (bearish divergence)
                    # Negative div = price down but CVD up (bullish divergence)
                    if price_norm > 0.1 and cvd_norm < -0.1:
                        r[col_name][i] = price_norm - cvd_norm  # positive = bearish div
                    elif price_norm < -0.1 and cvd_norm > 0.1:
                        r[col_name][i] = price_norm - cvd_norm  # negative = bullish div
                    else:
                        r[col_name][i] = 0

        # ── Price Trend ──
        if i >= 5: r["price_trend_5"][i] = c_[i] - c_[i-5]
        if i >= 8: r["price_trend_8"][i] = c_[i] - c_[i-8]

        if i >= 5:
            l5 = dirs_[i-5:i]
            r["trend_consistency_5"][i] = max(np.sum(l5>0), np.sum(l5<0)) / 5.0

        if i >= 1:
            pd_val = dirs_[i-1]; cnt = 0
            for j in range(i-1, -1, -1):
                if dirs_[j] == pd_val and pd_val != 0: cnt += 1
                else: break
            r["trend_bars"][i] = cnt

        # ── Delta Clustering ──
        # Count consecutive bars with same-sign delta ending at this bar
        dsign = 1 if d_[i] > 0 else (-1 if d_[i] < 0 else 0)
        cluster_len = 1; cluster_sum = d_[i]
        for j in range(i-1, max(-1, i-20), -1):
            jsign = 1 if d_[j] > 0 else (-1 if d_[j] < 0 else 0)
            if jsign == dsign:
                cluster_len += 1
                cluster_sum += d_[j]
            else:
                break
        r["delta_cluster_len"][i] = cluster_len
        r["delta_cluster_sum"][i] = cluster_sum

        # ── Delta Extremity ──
        lb20 = max(0, i-20); lb30 = max(0, i-30)
        dw = d_[lb20:i] if i > 0 else np.array([0.0])
        dw30 = d_[lb30:i] if i > 0 else np.array([0.0])
        md = np.mean(dw) if len(dw)>1 else 0
        sd = np.std(dw) if len(dw)>1 else 1
        if sd == 0: sd = 1
        r["delta_zscore"][i] = (d_[i] - md) / sd
        r["delta_percentile"][i] = np.mean(np.abs(dw30) <= abs(d_[i])) if len(dw30)>0 else 0.5

        # ── Volume ──
        vrw = vr_[lb20:i] if i>0 else np.array([1.0])
        mvr = np.mean(vrw) if len(vrw)>0 else 1
        svr = np.std(vrw) if len(vrw)>0 else 1
        if svr == 0: svr = 1
        if mvr == 0: mvr = 1
        r["vol_rate_zscore"][i] = (vr_[i] - mvr) / svr
        r["is_volume_climax"][i] = 1.0 if vr_[i] >= 2*mvr else 0.0

        # ── Price Structure ──
        lb10 = max(0, i-10)
        h10 = np.max(h_[lb10:i+1]); l10 = np.min(l_[lb10:i+1])
        h20 = np.max(h_[lb20:i+1]); l20 = np.min(l_[lb20:i+1])
        r["is_at_extreme_10"][i] = 1 if c_[i]>=h10 else (-1 if c_[i]<=l10 else 0)
        r["is_at_extreme_20"][i] = 1 if c_[i]>=h20 else (-1 if c_[i]<=l20 else 0)
        rng20 = h20 - l20
        r["bar_position_in_range_20"][i] = (c_[i]-l20)/rng20 if rng20>0 else 0.5

        r["body_ratio"][i] = bs_[i]/br_[i] if br_[i]>0 else 0

        # ── Trend Reversal Score (composite) ──
        # High score = more likely a reversal point
        # Components:
        #   1. At extreme (10-bar): gives direction
        #   2. Trend consistency (prior trend exists)
        #   3. |delta_zscore| (unusual delta)
        #   4. Volume climax
        #   5. CVD divergence from price
        tc5 = r["trend_consistency_5"][i]
        dz_n = min(abs(r["delta_zscore"][i])/3.0, 1.0)
        vc = r["is_volume_climax"][i]
        div5 = min(abs(r["cvd_price_div_5"][i])/0.5, 1.0)  # normalize to 0-1
        at_ext = abs(r["is_at_extreme_10"][i])  # 0 or 1
        dcl_n = min(r["delta_cluster_len"][i]/5.0, 1.0)
        r["trend_reversal_score"][i] = (0.20*tc5 + 0.20*dz_n + 0.15*vc + 0.20*div5 +
                                         0.10*at_ext + 0.15*dcl_n)

    for fc in feat_cols:
        df.loc[idx, fc] = r[fc]

print(f"  Done in {_time.time()-t0:.1f}s")

# Feature distributions
print(f"\n  Feature distributions (all {len(df)} bars):")
for fc in ["trend_reversal_score", "cvd_price_div_5", "cvd_price_div_8",
           "delta_cluster_len", "delta_zscore", "trend_consistency_5",
           "is_at_extreme_10", "is_volume_climax"]:
    vals = df[fc].values
    print(f"    {fc:<28} P10={np.percentile(vals,10):>6.2f}  P50={np.percentile(vals,50):>6.2f}  "
          f"P90={np.percentile(vals,90):>6.2f}  P99={np.percentile(vals,99):>6.2f}")

# ── STEP 3: Match Manual Signals ───────────────────────────────────────
print("\n[3] Matching manual signals...")

manual_matches = []
for sig in MANUAL_SIGNALS:
    day_df = df[df["trade_date"]==sig["date"]]
    if day_df.empty: continue
    t_s, t_e = sig["time_start"], sig["time_end"]
    matched = day_df[(day_df["time_et_str"]>=t_s)&(day_df["time_et_str"]<=t_e)]
    if matched.empty:
        ts_obj = datetime.strptime(t_s,"%H:%M")
        t_b = (ts_obj-timedelta(minutes=2)).strftime("%H:%M")
        te_obj = datetime.strptime(t_e,"%H:%M")
        t_a = (te_obj+timedelta(minutes=2)).strftime("%H:%M")
        matched = day_df[(day_df["time_et_str"]>=t_b)&(day_df["time_et_str"]<=t_a)]
    if matched.empty: continue
    best_idx = matched["delta"].abs().idxmax()
    bar = df.loc[best_idx]
    delta = bar["delta"]; direction = sig["direction"]
    sig_type = "ABS" if ((direction=="BULL" and delta<0) or (direction=="BEAR" and delta>0)) else "EXH"
    manual_matches.append({
        "signal": f"{sig['date']} {t_s} {sig['direction']}",
        "bar_idx": int(bar["bar_idx"]), "bar_time": bar["time_et_str"],
        "close": bar["close"], "delta": bar["delta"],
        "direction": sig["direction"], "sig_type": sig_type, "global_idx": best_idx,
        **{c: bar[c] for c in feat_cols}
    })

print(f"\n{'Signal':<32} {'Tp':<4} {'Time':<6} {'Delta':>7} {'TRS':>5} "
      f"{'Div5':>6} {'Div8':>6} {'TrC5':>4} {'TrBr':>4} {'DZsc':>5} "
      f"{'DClL':>4} {'DClS':>7} {'VClm':>4} {'Ext10':>5} {'BPR20':>5}")
print("-"*130)
for m in manual_matches:
    print(f"{m['signal']:<32} {m['sig_type']:<4} {m['bar_time']:<6} {m['delta']:>7.0f} "
          f"{m['trend_reversal_score']:>5.3f} "
          f"{m['cvd_price_div_5']:>6.2f} {m['cvd_price_div_8']:>6.2f} "
          f"{m['trend_consistency_5']:>4.2f} {m['trend_bars']:>4.0f} "
          f"{m['delta_zscore']:>5.1f} "
          f"{m['delta_cluster_len']:>4.0f} {m['delta_cluster_sum']:>7.0f} "
          f"{m['is_volume_climax']:>4.0f} {m['is_at_extreme_10']:>5.0f} "
          f"{m['bar_position_in_range_20']:>5.2f}")

# Compute stats for manual signals vs all bars
print(f"\n  Manual vs All comparison:")
for fc in ["trend_reversal_score", "cvd_price_div_5", "cvd_price_div_8",
           "delta_cluster_len", "delta_zscore", "trend_consistency_5"]:
    man_vals = [m[fc] for m in manual_matches]
    all_vals = df[fc].values
    print(f"    {fc:<28} Manual: med={np.median(man_vals):>6.2f}  All: med={np.median(all_vals):>6.2f}  "
          f"(Manual {np.median(man_vals)/max(np.median(all_vals),0.001):.1f}x)")

# ── STEP 4: Outcome Simulator ──────────────────────────────────────────
print("\n[4] Outcome simulator ready.")
date_groups = {d: g for d, g in df.groupby("trade_date")}

def simulate_outcomes(signal_df):
    results = []
    for _, sig in signal_df.iterrows():
        gi = int(sig["global_idx"]); d = sig["direction"]
        entry = df.at[gi,"close"]; td = sig["trade_date"]
        day = date_groups.get(td)
        if day is None: results.append({"outcome":"EXPIRED","pnl":0.0,"bars_held":0,"max_fav":0.0}); continue
        fwd = day[day.index>gi]
        if fwd.empty: results.append({"outcome":"EXPIRED","pnl":0.0,"bars_held":0,"max_fav":0.0}); continue
        fh=fwd["high"].values; fl=fwd["low"].values; fc=fwd["close"].values
        nf=min(len(fwd),TIMEOUT_BARS); mf=0.0; ts=-SL_PTS; oc="EXPIRED"; pnl=0.0; bh=0
        for j in range(nf):
            bh=j+1
            bhi=(fh[j]-entry) if d=="BULL" else (entry-fl[j])
            blo=(fl[j]-entry) if d=="BULL" else (entry-fh[j])
            if blo<=ts: pnl=ts; oc="WIN" if pnl>0 else "LOSS"; break
            if bhi>mf: mf=bhi
            if mf>=TRAIL_ACTIVATION:
                nt=mf-TRAIL_GAP
                if nt>ts: ts=nt
        else:
            lc=fc[min(bh-1,nf-1)]; pnl=(lc-entry) if d=="BULL" else (entry-lc); oc="EXPIRED"
        results.append({"outcome":oc,"pnl":round(pnl,2),"bars_held":bh,"max_fav":round(mf,2)})
    return pd.DataFrame(results)

# ── STEP 5: Grid Search over Structural Features ───────────────────────
print("\n[5] Grid search over structural features...")

# Direction: based on structural context
# At swing high (or near it, or price at 10-bar extreme high) → BEAR signal
# At swing low (or near it, or price at 10-bar extreme low) → BULL signal
# If neither: use price trend reversal (trend was up → BEAR, trend was down → BULL)
pt5 = df["price_trend_5"].values
pt8 = df["price_trend_8"].values
ext10 = df["is_at_extreme_10"].values
delta_v = df["delta"].values
cvd_div5 = df["cvd_price_div_5"].values

# Direction logic:
# Priority 1: at 10-bar extreme → reverse it
# Priority 2: price trend (use 8-bar for more stability)
# Priority 3: delta sign (as fallback)
sig_dir = np.full(len(df), "BULL", dtype=object)
for i in range(len(df)):
    if ext10[i] == 1:     # at high → expect reversal down
        sig_dir[i] = "BEAR"
    elif ext10[i] == -1:  # at low → expect reversal up
        sig_dir[i] = "BULL"
    elif pt8[i] > 3:      # uptrend → expect reversal down
        sig_dir[i] = "BEAR"
    elif pt8[i] < -3:     # downtrend → expect reversal up
        sig_dir[i] = "BULL"
    elif cvd_div5[i] > 0.1:  # bearish CVD divergence
        sig_dir[i] = "BEAR"
    elif cvd_div5[i] < -0.1:  # bullish CVD divergence
        sig_dir[i] = "BULL"
    elif delta_v[i] > 0:  # positive delta → expect reversal (absorption)
        sig_dir[i] = "BEAR"
    else:
        sig_dir[i] = "BULL"

bar_idxs = df["bar_idx"].values
trade_dates = df["trade_date"].values
trs_arr = df["trend_reversal_score"].values
div5_arr = np.abs(df["cvd_price_div_5"].values)
div8_arr = np.abs(df["cvd_price_div_8"].values)
dcl_arr = df["delta_cluster_len"].values
dz_arr = np.abs(df["delta_zscore"].values)
tc5_arr = df["trend_consistency_5"].values
vc_arr = df["is_volume_climax"].values
ext10_arr = np.abs(df["is_at_extreme_10"].values)
ext20_arr = np.abs(df["is_at_extreme_20"].values)

# Grid parameters
trs_thresholds = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55]
div_thresholds = [0.0, 0.05, 0.10, 0.15, 0.20]  # |cvd_price_div_5|
dcl_min_vals = [0, 2, 3, 4]  # min delta cluster length
dz_min_vals = [0.0, 0.5, 1.0, 1.5]
extra_gates = ["none", "vol_climax", "at_extreme", "trend_cons_60"]

total_combos = len(trs_thresholds)*len(div_thresholds)*len(dcl_min_vals)*len(dz_min_vals)*len(extra_gates)
print(f"  {total_combos} combinations")

grid_results = []
combo = 0
t_gs = _time.time()

for trs_t in trs_thresholds:
    for div_t in div_thresholds:
        for dcl_min in dcl_min_vals:
            for dz_min in dz_min_vals:
                for gate in extra_gates:
                    combo += 1
                    mask = trs_arr >= trs_t
                    if div_t > 0: mask = mask & (div5_arr >= div_t)
                    if dcl_min > 0: mask = mask & (dcl_arr >= dcl_min)
                    if dz_min > 0: mask = mask & (dz_arr >= dz_min)
                    if gate == "vol_climax": mask = mask & (vc_arr > 0)
                    elif gate == "at_extreme": mask = mask & (ext10_arr > 0)
                    elif gate == "trend_cons_60": mask = mask & (tc5_arr >= 0.6)
                    if not mask.any(): continue

                    si = np.where(mask)[0]
                    filtered = []; bcd = {}; ecd = {}
                    for s in si:
                        td = trade_dates[s]; di = sig_dir[s]; bi = bar_idxs[s]
                        if di == "BULL":
                            if bi - bcd.get(td,-100) < 10: continue
                            bcd[td] = bi
                        else:
                            if bi - ecd.get(td,-100) < 10: continue
                            ecd[td] = bi
                        filtered.append((s,di))

                    ns = len(filtered)
                    if ns == 0: continue
                    spd = ns / n_days
                    if spd < 1.0 or spd > 8.0: continue

                    sd = pd.DataFrame([{"global_idx":s,"direction":d,"trade_date":trade_dates[s]} for s,d in filtered])
                    oc = simulate_outcomes(sd)
                    sd = pd.concat([sd.reset_index(drop=True), oc], axis=1)

                    tp = sd["pnl"].sum(); w = (sd["pnl"]>0).sum(); wr = w/ns
                    gw = sd.loc[sd["pnl"]>0,"pnl"].sum()
                    gl = abs(sd.loc[sd["pnl"]<=0,"pnl"].sum())
                    pf = gw/gl if gl>0 else 99
                    cs = sd["pnl"].cumsum(); mdd = (cs-cs.cummax()).min()
                    q = tp*pf if tp>0 else tp/max(pf,0.01)

                    grid_results.append({
                        "trs_t":trs_t,"div_t":div_t,"dcl_min":dcl_min,"dz_min":dz_min,"gate":gate,
                        "n_sigs":ns,"spd":round(spd,1),"wr":round(wr,3),
                        "pnl":round(tp,1),"max_dd":round(mdd,1),"pf":round(pf,2),"quality":round(q,1)
                    })

                    if combo % 500 == 0:
                        print(f"    {combo}/{total_combos} ({_time.time()-t_gs:.0f}s, {len(grid_results)} valid)")

print(f"  Grid done: {_time.time()-t_gs:.1f}s, {len(grid_results)} valid combos")

if len(grid_results) == 0:
    print("  WARNING: No valid combos found! Adjusting sigs/day range...")
    # Retry with wider range
    for r in grid_results:
        pass
    print("  Exiting.")
    import sys; sys.exit(1)

grid_df = pd.DataFrame(grid_results)
grid_df = grid_df.sort_values("quality", ascending=False).reset_index(drop=True)

# Print results
def print_grid_table(title, gdf, n=20):
    print(f"\n{title}")
    print(f"{'#':<3} {'TRS':>5} {'Div5':>5} {'DCL':>4} {'DZ':>4} {'Gate':<15} | "
          f"{'Sigs':>5} {'S/D':>4} {'WR':>6} {'PnL':>8} {'MxDD':>7} {'PF':>5} {'Qual':>7}")
    print("-"*100)
    for i,r in gdf.head(n).iterrows():
        print(f"{i+1:<3} {r['trs_t']:>5.2f} {r['div_t']:>5.2f} {r['dcl_min']:>4} {r['dz_min']:>4.1f} {r['gate']:<15} | "
              f"{r['n_sigs']:>5.0f} {r['spd']:>4.1f} {r['wr']:>6.1%} {r['pnl']:>8.1f} {r['max_dd']:>7.1f} "
              f"{r['pf']:>5.2f} {r['quality']:>7.1f}")

print_grid_table("=" * 100 + "\nTOP 20 BY QUALITY (PnL * PF)", grid_df, 20)

wr_f = grid_df[grid_df["n_sigs"]>=30].sort_values("wr", ascending=False)
if len(wr_f)>0: print_grid_table("\nTOP 10 BY WR (min 30 sigs)", wr_f, 10)

pos_f = grid_df[(grid_df["pnl"]>0)&(grid_df["n_sigs"]>=20)].sort_values("pnl", ascending=False)
if len(pos_f)>0: print_grid_table("\nTOP 10 POSITIVE PNL (min 20 sigs)", pos_f, 10)

# ── STEP 6: Recall Check ──────────────────────────────────────────────
print("\n\n[6] Recall on 14 manual signals...")

def check_recall(trs_t, div_t, dcl_min, dz_min, gate, manual_matches, df):
    caught = []; missed = []
    for m in manual_matches:
        gi = m["global_idx"]; found = False
        for off in [0,-1,1,-2,2]:
            ni = gi + off
            if ni<0 or ni>=len(df) or df.at[ni,"trade_date"]!=m["signal"].split()[0]: continue
            passes = df.at[ni,"trend_reversal_score"] >= trs_t
            if div_t > 0: passes = passes and abs(df.at[ni,"cvd_price_div_5"]) >= div_t
            if dcl_min > 0: passes = passes and df.at[ni,"delta_cluster_len"] >= dcl_min
            if dz_min > 0: passes = passes and abs(df.at[ni,"delta_zscore"]) >= dz_min
            if gate == "vol_climax": passes = passes and df.at[ni,"is_volume_climax"] > 0
            elif gate == "at_extreme": passes = passes and abs(df.at[ni,"is_at_extreme_10"]) > 0
            elif gate == "trend_cons_60": passes = passes and df.at[ni,"trend_consistency_5"] >= 0.6
            if passes: found = True; break
        (caught if found else missed).append(m["signal"])
    return caught, missed

print(f"\n{'Config':<65} {'Caught':>6} {'Missed':>6} {'Recall':>7}")
print("-"*90)
for i in range(min(10, len(grid_df))):
    r = grid_df.iloc[i]
    caught, missed = check_recall(r["trs_t"],r["div_t"],r["dcl_min"],r["dz_min"],r["gate"],manual_matches,df)
    recall = len(caught)/len(manual_matches)
    cfg = f"TRS>={r['trs_t']:.2f} Div>={r['div_t']:.2f} DCL>={r['dcl_min']} DZ>={r['dz_min']:.1f} {r['gate']}"
    print(f"{cfg:<65} {len(caught):>6} {len(missed):>6} {recall:>7.0%}")
    if i < 2: print(f"  Missed: {missed}")

# Recall-optimized
print("\n[7] Recall-optimized (positive PnL only)...")
rc = []
for i,r in grid_df.iterrows():
    if r["pnl"]<=0: continue
    caught, missed = check_recall(r["trs_t"],r["div_t"],r["dcl_min"],r["dz_min"],r["gate"],manual_matches,df)
    recall = len(caught)/len(manual_matches) if manual_matches else 0
    rc.append({**r.to_dict(),"recall":recall,"caught_n":len(caught),"missed_list":missed})

rc_df = pd.DataFrame(rc)
if len(rc_df)>0:
    rc_df = rc_df.sort_values(["recall","quality"],ascending=[False,False]).reset_index(drop=True)
    print(f"\n{'#':<3} {'TRS':>5} {'Div5':>5} {'DCL':>4} {'DZ':>4} {'Gate':<15} | "
          f"{'Sigs':>5} {'WR':>6} {'PnL':>8} {'PF':>5} {'Recall':>7} {'Caug':>5}")
    print("-"*100)
    for i,r in rc_df.head(15).iterrows():
        print(f"{i+1:<3} {r['trs_t']:>5.2f} {r['div_t']:>5.2f} {r['dcl_min']:>4.0f} {r['dz_min']:>4.1f} {r['gate']:<15} | "
              f"{r['n_sigs']:>5.0f} {r['wr']:>6.1%} {r['pnl']:>8.1f} {r['pf']:>5.2f} {r['recall']:>7.0%} {r['caught_n']:>5.0f}")
    print(f"\n  Best recall config missed: {rc_df.iloc[0].get('missed_list',[])}")

# ── STEP 8: Best Config ───────────────────────────────────────────────
print("\n\n[8] Best config selection...")

if len(rc_df)>0:
    good = rc_df[rc_df["recall"]>=0.50]
    if len(good)==0: good = rc_df.head(5)
    best = good.sort_values("quality", ascending=False).iloc[0]
else:
    best = grid_df.iloc[0]

btrs = best["trs_t"]; bdiv = best["div_t"]; bdcl = best["dcl_min"]; bdz = best["dz_min"]; bgate = best["gate"]
print(f"\n  BEST: TRS>={btrs:.2f}, |Div5|>={bdiv:.2f}, DCL>={bdcl:.0f}, |DZ|>={bdz:.1f}, gate={bgate}")
print(f"  {best['n_sigs']:.0f} sigs ({best['spd']:.1f}/day), WR={best['wr']:.1%}, PnL={best['pnl']:.1f}, "
      f"MaxDD={best['max_dd']:.1f}, PF={best['pf']:.2f}")
if "recall" in best: print(f"  Recall: {best['recall']:.0%} ({best['caught_n']:.0f}/14)")

# Generate all signals
mask = trs_arr >= btrs
if bdiv > 0: mask = mask & (div5_arr >= bdiv)
if bdcl > 0: mask = mask & (dcl_arr >= bdcl)
if bdz > 0: mask = mask & (dz_arr >= bdz)
if bgate == "vol_climax": mask = mask & (vc_arr > 0)
elif bgate == "at_extreme": mask = mask & (ext10_arr > 0)
elif bgate == "trend_cons_60": mask = mask & (tc5_arr >= 0.6)

si = np.where(mask)[0]
best_signals = []; bcd = {}; ecd = {}
for s in si:
    td = trade_dates[s]; di = sig_dir[s]; bi = bar_idxs[s]
    if di == "BULL":
        if bi - bcd.get(td,-100) < 10: continue
        bcd[td] = bi
    else:
        if bi - ecd.get(td,-100) < 10: continue
        ecd[td] = bi
    best_signals.append({"global_idx":s,"direction":di,"trade_date":td})

bsdf = pd.DataFrame(best_signals)
bsoc = simulate_outcomes(bsdf)
bsdf = pd.concat([bsdf.reset_index(drop=True), bsoc], axis=1)
for col in ["time_et_str","close","delta","bar_idx"]+feat_cols:
    bsdf[col] = [df.at[gi,col] for gi in bsdf["global_idx"]]

# Daily breakdown
print(f"\n  {'Date':<12} {'S':>3} {'W':>3} {'L':>3} {'PnL':>8} {'WR':>6}")
print(f"  {'-'*40}")
for dt in sorted(bsdf["trade_date"].unique()):
    g = bsdf[bsdf["trade_date"]==dt]; w = (g["pnl"]>0).sum()
    print(f"  {dt:<12} {len(g):>3} {w:>3} {len(g)-w:>3} {g['pnl'].sum():>8.1f} {w/len(g):>6.0%}")
tp = bsdf["pnl"].sum(); tw = (bsdf["pnl"]>0).sum()
print(f"  {'TOTAL':<12} {len(bsdf):>3} {tw:>3} {len(bsdf)-tw:>3} {tp:>8.1f} {tw/len(bsdf):>6.0%}")
for d in ["BULL","BEAR"]:
    dm = bsdf[bsdf["direction"]==d]
    if len(dm)>0:
        dw = (dm["pnl"]>0).sum()
        print(f"  {d+':':<12} {len(dm):>3} {dw:>3} {len(dm)-dw:>3} {dm['pnl'].sum():>8.1f} {dw/len(dm):>6.0%}")

# ── STEP 9: Volland + Comparison ──────────────────────────────────────
print("\n\n[9] Volland filters + current detector comparison...")
try:
    engine = sa.create_engine(DB_URL)

    vol_df = pd.read_sql("""
    SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
           payload->'statistics'->>'paradigm' as paradigm,
           payload->'statistics'->>'lines_in_sand' as lis_str,
           (payload->'statistics'->'spot_vol_beta'->>'correlation')::float as svb,
           (payload->>'current_price')::float as spot
    FROM volland_snapshots
    WHERE ts >= '2026-03-01' AND ts < '2026-03-28'
      AND payload->'statistics' IS NOT NULL AND (payload->>'current_price') IS NOT NULL
    ORDER BY ts
    """, engine)
    print(f"  {len(vol_df)} Volland snapshots")

    if len(vol_df)>0:
        vol_df["ts_et"] = pd.to_datetime(vol_df["ts_et"])
        vol_df["trade_date"] = vol_df["ts_et"].dt.strftime("%Y-%m-%d")
        vol_df["svb"] = pd.to_numeric(vol_df["svb"], errors="coerce")
        def parse_lis(s):
            if pd.isna(s) or not s: return np.nan
            try: return float(s.replace("$","").replace(",",""))
            except: return np.nan
        vol_df["lis"] = vol_df["lis_str"].apply(parse_lis)

        def match_vol(row):
            td = row["trade_date"]; bt = row["time_et_str"]
            vd = vol_df[vol_df["trade_date"]==td]
            if vd.empty: return {"paradigm":None,"lis":None,"svb":None,"lis_dist":None}
            sig_dt = pd.to_datetime(f"{td} {bt}")
            vd = vd.copy(); vd["td_diff"] = abs((vd["ts_et"]-sig_dt).dt.total_seconds())
            cl = vd.loc[vd["td_diff"].idxmin()]
            lis = cl["lis"]
            return {"paradigm":cl["paradigm"],"lis":lis,"svb":cl["svb"],
                    "lis_dist":abs(row["close"]-lis) if pd.notna(lis) else None}

        vc = bsdf.apply(match_vol, axis=1, result_type="expand")
        bsdf = pd.concat([bsdf, vc], axis=1)

        def pf_row(label, sub):
            if len(sub)==0: print(f"  {label:<35} {'(0)':>5}"); return
            n=len(sub); w=(sub["pnl"]>0).sum(); wr=w/n; p=sub["pnl"].sum()
            gw=sub.loc[sub["pnl"]>0,"pnl"].sum(); gl=abs(sub.loc[sub["pnl"]<=0,"pnl"].sum())
            pf=gw/gl if gl>0 else 99; cs_v=sub["pnl"].cumsum(); md=(cs_v-cs_v.cummax()).min()
            print(f"  {label:<35} {n:>5} {wr:>6.0%} {p:>8.1f} {pf:>6.2f} {md:>7.1f}")

        print(f"\n  {'Filter':<35} {'Sigs':>5} {'WR':>6} {'PnL':>8} {'PF':>6} {'MaxDD':>7}")
        print(f"  {'-'*75}")
        pf_row("Baseline", bsdf)
        for lm in [5,10,15,20,30]:
            pf_row(f"LIS within {lm}", bsdf[bsdf["lis_dist"].notna()&(bsdf["lis_dist"]<=lm)])
        for sv in [0,0.5,1.0]:
            pf_row(f"SVB >= {sv}", bsdf[bsdf["svb"].notna()&(bsdf["svb"]>=sv)])
        pf_row("SVB < 0", bsdf[bsdf["svb"].notna()&(bsdf["svb"]<0)])
        for ps in ["GEX","AG","SIDIAL","BofA"]:
            pf_row(f"Paradigm: {ps}", bsdf[bsdf["paradigm"].notna()&bsdf["paradigm"].str.contains(ps)])
        pf_row("BULL only", bsdf[bsdf["direction"]=="BULL"])
        pf_row("BEAR only", bsdf[bsdf["direction"]=="BEAR"])

        # Combined filters
        svb_pos = bsdf[bsdf["svb"].notna()&(bsdf["svb"]>=0)]
        if len(svb_pos)>=5: pf_row("SVB>=0 + BEAR", svb_pos[svb_pos["direction"]=="BEAR"])
        svb1 = bsdf[bsdf["svb"].notna()&(bsdf["svb"]>=1.0)]
        if len(svb1)>=5: pf_row("SVB>=1.0", svb1)

        # Paradigm detail
        print(f"\n  Paradigm breakdown:")
        for p, g in bsdf.groupby("paradigm"):
            if pd.isna(p) or p=="": continue
            n=len(g); w=(g["pnl"]>0).sum()
            print(f"    {str(p):<25} {n:>3}  {w/n if n>0 else 0:>5.0%} WR  {g['pnl'].sum():>+7.1f}")

    # Current detector comparison
    print("\n  Current ES Absorption detector (setup_log):")
    cdf = pd.read_sql("""
    SELECT id, direction, grade, score, spot, abs_es_price, outcome_result, outcome_pnl,
           ts AT TIME ZONE 'America/New_York' as fired_et
    FROM setup_log
    WHERE setup_name = 'ES Absorption' AND ts >= '2026-03-01' AND ts < '2026-03-28'
    ORDER BY ts
    """, engine)
    print(f"  {len(cdf)} signals")
    if len(cdf)>0:
        cdf["trade_date"] = pd.to_datetime(cdf["fired_et"]).dt.strftime("%Y-%m-%d")
        cdf["time_et_str"] = pd.to_datetime(cdf["fired_et"]).dt.strftime("%H:%M")
        res = cdf[cdf["outcome_result"].notna()]
        if len(res)>0:
            cw = (res["outcome_result"]=="WIN").sum()
            cwr = cw/len(res); cpnl = res["outcome_pnl"].sum()
            print(f"  Resolved: {len(res)}, WR={cwr:.0%}, PnL={cpnl:+.1f}")

        cr = 0
        for m in manual_matches:
            md = m["signal"].split()[0]; mt = m["bar_time"]
            mt_dt = pd.to_datetime(f"{md} {mt}")
            for _,cs in cdf.iterrows():
                ct_dt = pd.to_datetime(f"{cs['trade_date']} {cs['time_et_str']}")
                if abs((mt_dt-ct_dt).total_seconds())<300: cr+=1; break
        print(f"  Manual recall: {cr}/{len(manual_matches)}")

        print(f"\n  {'Metric':<25} {'Current':>12} {'New':>12}")
        print(f"  {'-'*55}")
        print(f"  {'Signals':<25} {len(cdf):>12} {len(bsdf):>12}")
        print(f"  {'Sigs/Day':<25} {len(cdf)/n_days:>12.1f} {len(bsdf)/n_days:>12.1f}")
        if len(res)>0:
            print(f"  {'WR':<25} {cwr:>12.0%} {tw/len(bsdf):>12.0%}")
            print(f"  {'PnL':<25} {cpnl:>+12.1f} {tp:>+12.1f}")
        print(f"  {'Manual Recall':<25} {cr:>12} {int(best.get('caught_n',0)):>12}")

    engine.dispose()
except Exception as e:
    import traceback; print(f"  DB error: {e}"); traceback.print_exc()

# ── Save ───────────────────────────────────────────────────────────────
print("\n\n[10] Saving...")
ecols = ["trade_date","time_et_str","direction","bar_idx","close","delta","outcome","pnl","bars_held","max_fav"]+feat_cols
if "paradigm" in bsdf.columns: ecols += ["paradigm","lis","svb","lis_dist"]
bsdf[ecols].to_csv(EXPORT_PATH, index=False)
grid_df.to_csv(EXPORT_PATH.replace(".csv","_grid.csv"), index=False)
print(f"  {len(bsdf)} signals -> {EXPORT_PATH}")

print(f"\n{'='*80}\nFINAL SUMMARY\n{'='*80}")
print(f"  Config: TRS>={btrs:.2f}, |Div5|>={bdiv:.2f}, DCL>={bdcl:.0f}, |DZ|>={bdz:.1f}, gate={bgate}")
print(f"  Trail: SL={SL_PTS}, Gap={TRAIL_GAP}, Timeout={TIMEOUT_BARS}")
print(f"  {len(bsdf)} sigs ({best['spd']:.1f}/day), WR={best['wr']:.1%}, PnL={best['pnl']:.1f}")
if "recall" in best: print(f"  Manual recall: {best['recall']:.0%}")
print(f"  Runtime: {_time.time()-t0:.0f}s\nDone.")
