"""
DD Filter Impact Analysis
=========================
Take ALL completed setup trades from DB, match to DD snapshots,
and show exact before/after PnL impact of each filter.
"""
import json, os, psycopg2, statistics
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import pytz

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()
NY = pytz.timezone("US/Eastern")

out = []
def p(s=""):
    out.append(str(s))

# Load DD snapshots
p("Loading DD exposure data...")
cur.execute("""
    SELECT ts_utc, strike::float, value::float, current_price::float
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    ORDER BY ts_utc, strike
""")
raw = cur.fetchall()

snapshots = defaultdict(list)
snap_spots = {}
for ts, strike, value, spot in raw:
    snapshots[ts].append((strike, value))
    snap_spots[ts] = spot

# Build per-snapshot analysis
snap_analysis = []
for ts in sorted(snapshots.keys()):
    points = snapshots[ts]
    spot = snap_spots[ts]
    ts_et = ts.astimezone(NY)

    total_dd = sum(v for _, v in points)
    total_abs = sum(abs(v) for _, v in points)
    near_abs = sum(abs(v) for s, v in points if abs(s - spot) <= 25)
    concentration = near_abs / total_abs * 100 if total_abs > 0 else 0

    snap_analysis.append({
        "ts": ts, "ts_et": ts_et, "date": ts_et.date(),
        "spot": spot, "total_dd": total_dd, "concentration": concentration,
    })

p(f"DD snapshots: {len(snap_analysis)}")

# Load ALL completed setup trades
cur.execute("""
    SELECT id, ts, setup_name, direction, spot, grade, score,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE outcome_result IS NOT NULL
      AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
    ORDER BY ts
""")
all_trades = cur.fetchall()
p(f"Total completed trades: {len(all_trades)}")

# Match each trade to nearest DD snapshot
def find_dd(trade_ts):
    trade_ts_et = trade_ts.astimezone(NY) if trade_ts.tzinfo else NY.localize(trade_ts)
    trade_date = trade_ts_et.date()
    best = None
    best_delta = float('inf')
    for sa in snap_analysis:
        if sa["date"] != trade_date:
            continue
        delta = abs((sa["ts_et"] - trade_ts_et).total_seconds())
        if delta < best_delta:
            best_delta = delta
            best = sa
    if best and best_delta < 600:
        return best["concentration"], best["total_dd"]
    return None, None

trades = []
for row in all_trades:
    sid, ts, name, direction, spot, grade, score, result, pnl = row
    ts_et = ts.astimezone(NY) if ts.tzinfo else NY.localize(ts)
    conc, dd_total = find_dd(ts)
    trades.append({
        "id": sid, "ts_et": ts_et, "date": ts_et.date(),
        "name": name, "direction": direction, "spot": spot,
        "grade": grade, "score": float(score) if score else 0,
        "result": result, "pnl": float(pnl) if pnl else 0,
        "conc": conc, "dd": dd_total,
    })

has_dd = [t for t in trades if t["conc"] is not None]
no_dd = [t for t in trades if t["conc"] is None]
p(f"Trades with DD data: {len(has_dd)}")
p(f"Trades without DD data (pre-DD era): {len(no_dd)}")

# ============================================================
# BASELINE: Current Results (No Filter)
# ============================================================
p("\n" + "=" * 130)
p("BASELINE: Current Results (No Filter)")
p("=" * 130)

def summary(trade_list, label=""):
    if not trade_list:
        return
    wins = sum(1 for t in trade_list if t["result"] == "WIN")
    losses = sum(1 for t in trade_list if t["result"] == "LOSS")
    expired = sum(1 for t in trade_list if t["result"] == "EXPIRED")
    total_pnl = sum(t["pnl"] for t in trade_list)
    wr = wins / max(1, wins + losses) * 100
    gross_w = sum(t["pnl"] for t in trade_list if t["pnl"] > 0)
    gross_l = abs(sum(t["pnl"] for t in trade_list if t["pnl"] < 0))
    pf = gross_w / gross_l if gross_l > 0 else 999
    avg = total_pnl / len(trade_list)

    # Max drawdown
    running = 0
    max_dd = 0
    peak = 0
    for t in trade_list:
        running += t["pnl"]
        peak = max(peak, running)
        max_dd = min(max_dd, running - peak)

    p(f"  {label:25} n={len(trade_list):3}  W={wins:2} L={losses:2} E={expired:2}  "
      f"WR={wr:5.1f}%  PnL={total_pnl:+8.1f}  Avg={avg:+6.1f}  PF={pf:5.2f}  MaxDD={max_dd:+7.1f}")
    return total_pnl, wr, len(trade_list)

p("\nAll trades (including pre-DD era):")
summary(trades, "TOTAL")
for name in sorted(set(t["name"] for t in trades)):
    subset = [t for t in trades if t["name"] == name]
    summary(subset, name)

p("\nTrades WITH DD data only (DD era, Feb 11+):")
baseline_total = summary(has_dd, "TOTAL (DD era)")
for name in sorted(set(t["name"] for t in has_dd)):
    subset = [t for t in has_dd if t["name"] == name]
    summary(subset, name)


# ============================================================
# FILTER 1: Block DD Exhaustion when Concentration > 75%
# ============================================================
p("\n\n" + "=" * 130)
p("FILTER 1: Block DD Exhaustion when DD Concentration > 75%")
p("=" * 130)

f1_kept = [t for t in has_dd if not (t["name"] == "DD Exhaustion" and t["conc"] is not None and t["conc"] > 75)]
f1_blocked = [t for t in has_dd if t["name"] == "DD Exhaustion" and t["conc"] is not None and t["conc"] > 75]

p(f"\nBlocked trades: {len(f1_blocked)}")
p(f"{'ID':>5} {'Date':12} {'Time':6} {'Dir':6} {'Spot':>8} {'Result':>8} {'PnL':>8} {'Conc':>6} {'DD':>12}")
p("-" * 90)
for t in f1_blocked:
    p(f"{t['id']:5} {t['date']} {t['ts_et'].strftime('%H:%M'):6} {t['direction']:6} "
      f"{t['spot']:8.1f} {t['result']:>8} {t['pnl']:+8.1f} {t['conc']:5.0f}% {t['dd']/1e9:+11.1f}B")

blocked_pnl = sum(t["pnl"] for t in f1_blocked)
blocked_w = sum(1 for t in f1_blocked if t["result"] == "WIN")
blocked_l = sum(1 for t in f1_blocked if t["result"] == "LOSS")
p(f"\nBlocked summary: W={blocked_w} L={blocked_l} PnL={blocked_pnl:+.1f}")

p("\nAFTER Filter 1:")
f1_total = summary(f1_kept, "TOTAL (filtered)")
for name in sorted(set(t["name"] for t in f1_kept)):
    subset = [t for t in f1_kept if t["name"] == name]
    summary(subset, name)


# ============================================================
# FILTER 2: Block ALL setups when Concentration > 85%
# ============================================================
p("\n\n" + "=" * 130)
p("FILTER 2: Block ALL Setups when DD Concentration > 85%")
p("=" * 130)

f2_kept = [t for t in has_dd if t["conc"] is None or t["conc"] <= 85]
f2_blocked = [t for t in has_dd if t["conc"] is not None and t["conc"] > 85]

p(f"\nBlocked trades: {len(f2_blocked)}")
p(f"{'ID':>5} {'Date':12} {'Time':6} {'Setup':20} {'Dir':6} {'Result':>8} {'PnL':>8} {'Conc':>6}")
p("-" * 90)
for t in f2_blocked:
    p(f"{t['id']:5} {t['date']} {t['ts_et'].strftime('%H:%M'):6} {t['name']:20} {t['direction']:6} "
      f"{t['result']:>8} {t['pnl']:+8.1f} {t['conc']:5.0f}%")

blocked_pnl = sum(t["pnl"] for t in f2_blocked)
blocked_w = sum(1 for t in f2_blocked if t["result"] == "WIN")
blocked_l = sum(1 for t in f2_blocked if t["result"] == "LOSS")
p(f"\nBlocked summary: W={blocked_w} L={blocked_l} PnL={blocked_pnl:+.1f}")

p("\nAFTER Filter 2:")
f2_total = summary(f2_kept, "TOTAL (filtered)")
for name in sorted(set(t["name"] for t in f2_kept)):
    subset = [t for t in f2_kept if t["name"] == name]
    summary(subset, name)


# ============================================================
# FILTER 3: Block setups when DD > +$2B (bullish DD = bearish for setups)
# ============================================================
p("\n\n" + "=" * 130)
p("FILTER 3: Block ALL Setups when DD Total > +$2B (Bullish DD = Poor Setup Environment)")
p("=" * 130)

f3_kept = [t for t in has_dd if t["dd"] is None or t["dd"] <= 2e9]
f3_blocked = [t for t in has_dd if t["dd"] is not None and t["dd"] > 2e9]

p(f"\nBlocked trades: {len(f3_blocked)}")
p(f"{'ID':>5} {'Date':12} {'Time':6} {'Setup':20} {'Dir':6} {'Result':>8} {'PnL':>8} {'DD':>12}")
p("-" * 100)
for t in f3_blocked:
    p(f"{t['id']:5} {t['date']} {t['ts_et'].strftime('%H:%M'):6} {t['name']:20} {t['direction']:6} "
      f"{t['result']:>8} {t['pnl']:+8.1f} {t['dd']/1e9:+11.1f}B")

blocked_pnl = sum(t["pnl"] for t in f3_blocked)
blocked_w = sum(1 for t in f3_blocked if t["result"] == "WIN")
blocked_l = sum(1 for t in f3_blocked if t["result"] == "LOSS")
p(f"\nBlocked summary: W={blocked_w} L={blocked_l} PnL={blocked_pnl:+.1f}")

p("\nAFTER Filter 3:")
f3_total = summary(f3_kept, "TOTAL (filtered)")
for name in sorted(set(t["name"] for t in f3_kept)):
    subset = [t for t in f3_kept if t["name"] == name]
    summary(subset, name)


# ============================================================
# FILTER 4: COMBINED — F1 + F3 (DD Exhaust conc>75% + all DD>+2B)
# ============================================================
p("\n\n" + "=" * 130)
p("FILTER 4: COMBINED — Block DD Exhaustion Conc>75% + Block ALL when DD>+$2B")
p("=" * 130)

def is_blocked_f4(t):
    if t["conc"] is None or t["dd"] is None:
        return False
    # F1: DD Exhaustion + concentration > 75%
    if t["name"] == "DD Exhaustion" and t["conc"] > 75:
        return True
    # F3: Any setup + DD > +$2B
    if t["dd"] > 2e9:
        return True
    return False

f4_kept = [t for t in has_dd if not is_blocked_f4(t)]
f4_blocked = [t for t in has_dd if is_blocked_f4(t)]

p(f"\nBlocked trades: {len(f4_blocked)}")
p(f"{'ID':>5} {'Date':12} {'Time':6} {'Setup':20} {'Dir':6} {'Result':>8} {'PnL':>8} {'Conc':>6} {'DD':>12} {'Reason':>12}")
p("-" * 120)
for t in f4_blocked:
    reason = ""
    if t["name"] == "DD Exhaustion" and t["conc"] is not None and t["conc"] > 75:
        reason = "DD_CONC>75"
    elif t["dd"] is not None and t["dd"] > 2e9:
        reason = "DD>+2B"
    p(f"{t['id']:5} {t['date']} {t['ts_et'].strftime('%H:%M'):6} {t['name']:20} {t['direction']:6} "
      f"{t['result']:>8} {t['pnl']:+8.1f} {t['conc']:5.0f}% {t['dd']/1e9:+11.1f}B {reason:>12}")

blocked_pnl = sum(t["pnl"] for t in f4_blocked)
blocked_w = sum(1 for t in f4_blocked if t["result"] == "WIN")
blocked_l = sum(1 for t in f4_blocked if t["result"] == "LOSS")
blocked_e = sum(1 for t in f4_blocked if t["result"] == "EXPIRED")
p(f"\nBlocked: {len(f4_blocked)} trades  W={blocked_w} L={blocked_l} E={blocked_e}  PnL removed={blocked_pnl:+.1f}")

p("\nAFTER Filter 4 (Combined):")
f4_total = summary(f4_kept, "TOTAL (filtered)")
for name in sorted(set(t["name"] for t in f4_kept)):
    subset = [t for t in f4_kept if t["name"] == name]
    summary(subset, name)


# ============================================================
# FILTER 5: COMBINED — F1 + F2 (DD Exhaust conc>75% + ALL conc>85%)
# ============================================================
p("\n\n" + "=" * 130)
p("FILTER 5: COMBINED — Block DD Exhaustion Conc>75% + Block ALL Setups Conc>85%")
p("=" * 130)

def is_blocked_f5(t):
    if t["conc"] is None:
        return False
    if t["name"] == "DD Exhaustion" and t["conc"] > 75:
        return True
    if t["conc"] > 85:
        return True
    return False

f5_kept = [t for t in has_dd if not is_blocked_f5(t)]
f5_blocked = [t for t in has_dd if is_blocked_f5(t)]

p(f"\nBlocked trades: {len(f5_blocked)}")
p(f"{'ID':>5} {'Date':12} {'Time':6} {'Setup':20} {'Dir':6} {'Result':>8} {'PnL':>8} {'Conc':>6} {'Reason':>15}")
p("-" * 110)
for t in f5_blocked:
    reason = "DD_CONC>75" if t["name"] == "DD Exhaustion" and t["conc"] > 75 else "ALL_CONC>85"
    p(f"{t['id']:5} {t['date']} {t['ts_et'].strftime('%H:%M'):6} {t['name']:20} {t['direction']:6} "
      f"{t['result']:>8} {t['pnl']:+8.1f} {t['conc']:5.0f}% {reason:>15}")

blocked_pnl = sum(t["pnl"] for t in f5_blocked)
blocked_w = sum(1 for t in f5_blocked if t["result"] == "WIN")
blocked_l = sum(1 for t in f5_blocked if t["result"] == "LOSS")
blocked_e = sum(1 for t in f5_blocked if t["result"] == "EXPIRED")
p(f"\nBlocked: {len(f5_blocked)} trades  W={blocked_w} L={blocked_l} E={blocked_e}  PnL removed={blocked_pnl:+.1f}")

p("\nAFTER Filter 5 (Combined):")
f5_total = summary(f5_kept, "TOTAL (filtered)")
for name in sorted(set(t["name"] for t in f5_kept)):
    subset = [t for t in f5_kept if t["name"] == name]
    summary(subset, name)


# ============================================================
# GRAND COMPARISON
# ============================================================
p("\n\n" + "=" * 130)
p("GRAND COMPARISON: All Filters Side-by-Side (DD era trades only)")
p("=" * 130)

base_pnl = sum(t["pnl"] for t in has_dd)
base_n = len(has_dd)
base_w = sum(1 for t in has_dd if t["result"] == "WIN")
base_l = sum(1 for t in has_dd if t["result"] == "LOSS")
base_wr = base_w / max(1, base_w + base_l) * 100

def filter_stats(kept, label):
    n = len(kept)
    pnl = sum(t["pnl"] for t in kept)
    w = sum(1 for t in kept if t["result"] == "WIN")
    l = sum(1 for t in kept if t["result"] == "LOSS")
    wr = w / max(1, w + l) * 100
    blocked = base_n - n
    delta = pnl - base_pnl
    gw = sum(t["pnl"] for t in kept if t["pnl"] > 0)
    gl = abs(sum(t["pnl"] for t in kept if t["pnl"] < 0))
    pf = gw / gl if gl > 0 else 999

    # Max drawdown
    running = 0
    max_dd = 0
    peak = 0
    for t in sorted(kept, key=lambda x: x["ts_et"]):
        running += t["pnl"]
        peak = max(peak, running)
        max_dd = min(max_dd, running - peak)

    p(f"  {label:50} n={n:3} (-{blocked:2})  W={w:2} L={l:2}  "
      f"WR={wr:5.1f}%  PnL={pnl:+8.1f} ({delta:+7.1f})  PF={pf:5.2f}  MaxDD={max_dd:+7.1f}")

p(f"\n  {'Filter':50} {'Trades':>6} {'Blocked':>9} {'W/L':>8} "
  f"{'WR':>7} {'PnL':>10} {'Delta':>9} {'PF':>7} {'MaxDD':>9}")
p(f"  {'-'*120}")

# Baseline
gw = sum(t["pnl"] for t in has_dd if t["pnl"] > 0)
gl = abs(sum(t["pnl"] for t in has_dd if t["pnl"] < 0))
bpf = gw / gl if gl > 0 else 999
running = 0
bdd = 0
peak = 0
for t in sorted(has_dd, key=lambda x: x["ts_et"]):
    running += t["pnl"]
    peak = max(peak, running)
    bdd = min(bdd, running - peak)

p(f"  {'BASELINE (no filter)':50} n={base_n:3} (-{0:2})  W={base_w:2} L={base_l:2}  "
  f"WR={base_wr:5.1f}%  PnL={base_pnl:+8.1f} ({0:+7.1f})  PF={bpf:5.2f}  MaxDD={bdd:+7.1f}")

filter_stats(f1_kept, "F1: Block DD Exhaust when Conc > 75%")
filter_stats(f2_kept, "F2: Block ALL when Conc > 85%")
filter_stats(f3_kept, "F3: Block ALL when DD > +$2B")
filter_stats(f4_kept, "F4: F1 + F3 (DD Exhaust Conc>75% + DD>+$2B)")
filter_stats(f5_kept, "F5: F1 + F2 (DD Exhaust Conc>75% + ALL Conc>85%)")

# ============================================================
# ALSO: Impact on Grand Total (including pre-DD era trades)
# ============================================================
p("\n\n" + "=" * 130)
p("GRAND TOTAL IMPACT (Including Pre-DD Era Trades)")
p("=" * 130)
p("Pre-DD trades (before Feb 11) are UNAFFECTED by filters.")

pre_dd_pnl = sum(t["pnl"] for t in no_dd)
pre_dd_n = len(no_dd)
pre_dd_w = sum(1 for t in no_dd if t["result"] == "WIN")
pre_dd_l = sum(1 for t in no_dd if t["result"] == "LOSS")

p(f"\n  Pre-DD trades: n={pre_dd_n}  W={pre_dd_w} L={pre_dd_l}  PnL={pre_dd_pnl:+.1f}")
p(f"  DD-era trades: n={base_n}  W={base_w} L={base_l}  PnL={base_pnl:+.1f}")
p(f"  Grand total baseline: n={len(trades)}  PnL={sum(t['pnl'] for t in trades):+.1f}")

for label, kept in [
    ("F1: DD Exhaust Conc>75%", f1_kept),
    ("F2: ALL Conc>85%", f2_kept),
    ("F3: ALL DD>+$2B", f3_kept),
    ("F4: F1+F3 Combined", f4_kept),
    ("F5: F1+F2 Combined", f5_kept),
]:
    filtered_pnl = pre_dd_pnl + sum(t["pnl"] for t in kept)
    filtered_n = pre_dd_n + len(kept)
    blocked = len(has_dd) - len(kept)
    delta = sum(t["pnl"] for t in kept) - base_pnl
    all_w = pre_dd_w + sum(1 for t in kept if t["result"] == "WIN")
    all_l = pre_dd_l + sum(1 for t in kept if t["result"] == "LOSS")
    all_wr = all_w / max(1, all_w + all_l) * 100

    p(f"  {label:30} n={filtered_n:3} (-{blocked:2})  W={all_w:2} L={all_l:2}  "
      f"WR={all_wr:5.1f}%  GrandPnL={filtered_pnl:+8.1f} (delta={delta:+7.1f})")

# Wins that would be killed
p("\n\n--- Wins That Would Be KILLED By Each Filter ---")
for label, blocked_list in [
    ("F1", [t for t in has_dd if t["name"] == "DD Exhaustion" and t["conc"] is not None and t["conc"] > 75]),
    ("F2", [t for t in has_dd if t["conc"] is not None and t["conc"] > 85]),
    ("F3", [t for t in has_dd if t["dd"] is not None and t["dd"] > 2e9]),
    ("F4", [t for t in has_dd if is_blocked_f4(t)]),
    ("F5", [t for t in has_dd if is_blocked_f5(t)]),
]:
    killed_wins = [t for t in blocked_list if t["result"] == "WIN"]
    killed_losses = [t for t in blocked_list if t["result"] == "LOSS"]
    killed_expired = [t for t in blocked_list if t["result"] == "EXPIRED"]
    p(f"\n  {label}: blocks {len(blocked_list)} trades  "
      f"(kills {len(killed_wins)}W, saves {len(killed_losses)}L, removes {len(killed_expired)}E)")
    if killed_wins:
        for t in killed_wins:
            p(f"    KILLED WIN: #{t['id']} {t['date']} {t['name']} {t['direction']} PnL={t['pnl']:+.1f} "
              f"Conc={t['conc']:.0f}% DD={t['dd']/1e9:+.1f}B")
    if killed_losses:
        for t in killed_losses:
            p(f"    SAVED LOSS: #{t['id']} {t['date']} {t['name']} {t['direction']} PnL={t['pnl']:+.1f} "
              f"Conc={t['conc']:.0f}% DD={t['dd']/1e9:+.1f}B")

cur.close()
conn.close()

with open("tmp_dd_impact_output.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
print(f"Done. {len(out)} lines -> tmp_dd_impact_output.txt")
