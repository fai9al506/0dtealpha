"""Deep regime analysis: why last week lost ~$200-300/day, why today +$388?
Find loss-cluster patterns. Propose V17 block rules if validated.

Methodology:
  1. Daily BROKER TRUTH P&L (fill→close × $5 × direction). Not setup_log labels.
  2. Per-setup per-week contribution (find the bleeders).
  3. Regime characterization on bad days (VIX, paradigm dominance, vanna).
  4. Bad-trade pattern hunting (paradigm × align × grade × time).
  5. V17 candidate rules: each backtested against May data.

Sample window: May 1-20, 14 trading days. 100 real_trade_orders placed.
Bad week reference: May 12-16 (Mon-Fri). Good day reference: May 20.
"""
import os, json, psycopg2
from collections import defaultdict
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()


# ────────────────────────────────────────────────────────────────────
# 1. DAILY BROKER TRUTH P&L (fill→close, not setup_log labels)
# ────────────────────────────────────────────────────────────────────
cur.execute("""
    SELECT rto.setup_log_id, rto.created_at::date AS d,
           sl.setup_name, sl.direction, sl.paradigm, sl.grade,
           sl.greek_alignment, sl.vix, sl.vanna_regime, sl.ts,
           rto.state
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE rto.created_at >= '2026-05-01' AND rto.created_at < '2026-05-21'
      AND rto.state->>'status' = 'closed'
    ORDER BY rto.created_at
""")

trades = []
for r in cur.fetchall():
    sid, d, name, dir_, para, grade, align, vix, vanna, ts, state = r
    if isinstance(state, str): state = json.loads(state)
    fill = state.get("fill_price")
    close = state.get("close_fill_price")
    qty = state.get("quantity") or 1
    if fill is None or close is None:
        continue  # unresolved ghost
    is_long = dir_ in ("long", "bullish")
    pts = (float(close) - float(fill)) * (1 if is_long else -1)
    dollars = pts * 5.0 * qty
    trades.append({
        "lid": sid, "d": d, "ts": ts, "setup": name, "dir": dir_, "is_long": is_long,
        "paradigm": para, "grade": grade, "align": align or 0,
        "vix": float(vix) if vix else None, "vanna_regime": vanna,
        "fill": float(fill), "close": float(close), "pts": pts, "dollars": dollars,
        "qty": qty,
    })

print(f"Loaded {len(trades)} resolved real-broker trades May 1-20\n")

# Daily P&L
daily = defaultdict(float)
daily_n = defaultdict(int)
for t in trades:
    daily[t["d"]] += t["dollars"]
    daily_n[t["d"]] += 1

# Bad days = bottom 5, good days = top 5
sorted_days = sorted(daily.items(), key=lambda x: x[1])
print("=" * 70)
print("DAILY BROKER P&L — May 1-20 (sorted worst to best)")
print("=" * 70)
print(f"{'Date':<12} {'Day':<10} {'P&L':>10} {'Trades':>7}")
for d, p in sorted_days:
    dow = d.strftime("%a")
    print(f"  {d}  {dow:<8} ${p:>+8.2f}   {daily_n[d]:>4}")

# Identify "bad week" — find worst consecutive 5-day window
sorted_dates = sorted(daily.keys())
worst_week_pnl = 0
worst_week_days = []
for i in range(len(sorted_dates) - 4):
    win = sorted_dates[i:i+5]
    win_pnl = sum(daily[d] for d in win)
    if win_pnl < worst_week_pnl:
        worst_week_pnl = win_pnl
        worst_week_days = win

print()
print(f"Worst 5-day window: {worst_week_days[0]} to {worst_week_days[-1]} = ${worst_week_pnl:+.2f}")
print(f"  ({sum(daily_n[d] for d in worst_week_days)} trades)")


# ────────────────────────────────────────────────────────────────────
# 2. PER-SETUP × PER-WEEK BREAKDOWN
# ────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("PER-SETUP CONTRIBUTION BY WEEK")
print("=" * 70)

def week_label(d):
    """Return week label e.g. 'W1 May 1-3'."""
    # Group by ISO week within May
    iso = d.isocalendar()
    return f"W{iso.week} {d.strftime('%b %d')}"

weekly_setup = defaultdict(lambda: defaultdict(lambda: {"n":0, "pnl":0.0, "wins":0, "losses":0}))
for t in trades:
    iso = t["d"].isocalendar()
    wk = f"W{iso.week}"
    key = (t["setup"], t["dir"])
    weekly_setup[wk][key]["n"] += 1
    weekly_setup[wk][key]["pnl"] += t["dollars"]
    if t["dollars"] > 0: weekly_setup[wk][key]["wins"] += 1
    elif t["dollars"] < 0: weekly_setup[wk][key]["losses"] += 1

for wk in sorted(weekly_setup):
    week_dates = sorted(set(t["d"] for t in trades if f"W{t['d'].isocalendar().week}" == wk))
    if not week_dates: continue
    total = sum(d["pnl"] for d in weekly_setup[wk].values())
    print(f"\n  {wk} ({week_dates[0]} → {week_dates[-1]}): total ${total:+.2f}")
    for (setup, dir_), s in sorted(weekly_setup[wk].items(), key=lambda x: x[1]["pnl"]):
        wr = s["wins"] / max(1, s["wins"]+s["losses"]) * 100
        print(f"    {setup:<18} {dir_:<8} n={s['n']:>3} WR={wr:>4.0f}% ${s['pnl']:+8.2f}")


# ────────────────────────────────────────────────────────────────────
# 3. REGIME CHARACTERIZATION — bad days vs good days
# ────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("REGIME COMPARISON — bad days (bottom 5) vs good days (top 5)")
print("=" * 70)

bad_days = set(d for d, p in sorted_days[:5])
good_days = set(d for d, p in sorted_days[-5:])

def summarize_regime(day_set, label):
    ts_in = [t for t in trades if t["d"] in day_set]
    if not ts_in:
        return
    n = len(ts_in)
    pnl = sum(t["dollars"] for t in ts_in)
    wins = sum(1 for t in ts_in if t["dollars"] > 0)
    losses = sum(1 for t in ts_in if t["dollars"] < 0)
    avg_vix = sum(t["vix"] for t in ts_in if t["vix"]) / max(1, sum(1 for t in ts_in if t["vix"]))
    paradigm_dist = defaultdict(int)
    vanna_dist = defaultdict(int)
    align_dist = defaultdict(int)
    long_n = sum(1 for t in ts_in if t["is_long"])
    short_n = n - long_n
    for t in ts_in:
        paradigm_dist[t["paradigm"]] += 1
        vanna_dist[t["vanna_regime"]] += 1
        align_dist[t["align"]] += 1
    print(f"\n[{label}]   n={n}, P&L=${pnl:+.2f}, WR={wins/max(1,wins+losses)*100:.0f}%")
    print(f"  long/short:    {long_n}/{short_n}")
    print(f"  avg VIX:       {avg_vix:.1f}")
    print(f"  paradigms:     " + ", ".join(f"{p}={c}" for p,c in sorted(paradigm_dist.items(), key=lambda x: -x[1])[:5]))
    print(f"  vanna regime:  " + ", ".join(f"{v}={c}" for v,c in sorted(vanna_dist.items(), key=lambda x: -x[1])))
    print(f"  align dist:    " + ", ".join(f"{a:+d}={c}" for a,c in sorted(align_dist.items())))

summarize_regime(bad_days, f"BAD DAYS {sorted(bad_days)}")
summarize_regime(good_days, f"GOOD DAYS {sorted(good_days)}")


# ────────────────────────────────────────────────────────────────────
# 4. BAD-TRADE PATTERN HUNTING — what consistently loses?
# ────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("BAD-TRADE PATTERN HUNTING — buckets sorted by loss")
print("=" * 70)

# Pattern: (setup, dir, paradigm)
pat_pnl = defaultdict(lambda: {"n":0,"pnl":0.0,"wins":0,"losses":0})
for t in trades:
    key = (t["setup"], t["dir"], t["paradigm"])
    pat_pnl[key]["n"] += 1
    pat_pnl[key]["pnl"] += t["dollars"]
    if t["dollars"] > 0: pat_pnl[key]["wins"] += 1
    elif t["dollars"] < 0: pat_pnl[key]["losses"] += 1

print(f"\nTop 10 bleeder patterns (worst $ contribution, n>=2):")
sorted_pats = sorted([(k,v) for k,v in pat_pnl.items() if v["n"] >= 2], key=lambda x: x[1]["pnl"])
for (setup, dir_, para), s in sorted_pats[:10]:
    wr = s["wins"]/max(1,s["wins"]+s["losses"])*100
    print(f"  {setup:<18} {dir_:<8} {para:<18} n={s['n']:>2} WR={wr:>4.0f}% ${s['pnl']:+8.2f}")

print(f"\nTop 10 winning patterns:")
for (setup, dir_, para), s in sorted_pats[-10:]:
    wr = s["wins"]/max(1,s["wins"]+s["losses"])*100
    print(f"  {setup:<18} {dir_:<8} {para:<18} n={s['n']:>2} WR={wr:>4.0f}% ${s['pnl']:+8.2f}")


# Pattern: (setup, dir, align)
print(f"\nBy alignment:")
align_pnl = defaultdict(lambda: {"n":0,"pnl":0.0,"wins":0,"losses":0})
for t in trades:
    key = (t["setup"], t["dir"], t["align"])
    align_pnl[key]["n"] += 1
    align_pnl[key]["pnl"] += t["dollars"]
    if t["dollars"] > 0: align_pnl[key]["wins"] += 1
    elif t["dollars"] < 0: align_pnl[key]["losses"] += 1

for (setup, dir_, a), s in sorted(align_pnl.items(), key=lambda x: x[1]["pnl"])[:8]:
    wr = s["wins"]/max(1,s["wins"]+s["losses"])*100
    print(f"  {setup:<18} {dir_:<8} align={a:+d} n={s['n']:>2} WR={wr:>4.0f}% ${s['pnl']:+8.2f}")


# Pattern: (setup, dir, hour)
print(f"\nBy hour (ET):")
hour_pnl = defaultdict(lambda: {"n":0,"pnl":0.0})
for t in trades:
    h = t["ts"].astimezone(ET).hour
    key = (t["setup"], t["dir"], h)
    hour_pnl[key]["n"] += 1
    hour_pnl[key]["pnl"] += t["dollars"]

bleeder_hours = sorted([(k,v) for k,v in hour_pnl.items() if v["n"] >= 3], key=lambda x: x[1]["pnl"])[:8]
for (setup, dir_, h), s in bleeder_hours:
    print(f"  {setup:<18} {dir_:<8} hour={h:02d}:00 ET n={s['n']:>2} ${s['pnl']:+8.2f}")


# ────────────────────────────────────────────────────────────────────
# 5. V17 CANDIDATE RULES — backtest each
# ────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("V17 CANDIDATE BLOCK RULES — backtest May 1-20 (real broker P&L)")
print("=" * 70)

current_total = sum(t["dollars"] for t in trades)
print(f"\nBASELINE (V16.1 actual): ${current_total:+.2f} over {len(trades)} trades")

candidates = []

def test_rule(rule_label, block_fn):
    kept = [t for t in trades if not block_fn(t)]
    blocked = [t for t in trades if block_fn(t)]
    block_n = len(blocked)
    block_pnl = sum(t["dollars"] for t in blocked)
    new_total = sum(t["dollars"] for t in kept)
    delta = new_total - current_total
    if block_n == 0:
        return
    block_wr = sum(1 for t in blocked if t["dollars"] > 0) / block_n * 100
    print(f"\n  {rule_label}")
    print(f"    Blocked: n={block_n}, WR={block_wr:.0f}%, P&L=${block_pnl:+.2f}")
    print(f"    Kept:    n={len(kept)}, total ${new_total:+.2f}")
    print(f"    Delta:   ${delta:+.2f}")
    candidates.append((rule_label, block_n, block_pnl, delta))

# Build candidates from worst patterns
print("\n--- Direct paradigm blocks ---")
test_rule("Block: SC long paradigm=AG-PURE",
          lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["paradigm"]=="AG-PURE")
test_rule("Block: SC long paradigm=AG-LIS",
          lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["paradigm"]=="AG-LIS")
test_rule("Block: SC short paradigm=BOFA-MESSY",
          lambda t: t["setup"]=="Skew Charm" and not t["is_long"] and t["paradigm"]=="BOFA-MESSY")
test_rule("Block: DD long paradigm=BOFA-MESSY",
          lambda t: t["setup"]=="DD Exhaustion" and t["is_long"] and t["paradigm"]=="BOFA-MESSY")
test_rule("Block: ES Abs bearish paradigm=BOFA-PURE",
          lambda t: t["setup"]=="ES Absorption" and not t["is_long"] and t["paradigm"]=="BOFA-PURE")
test_rule("Block: ES Abs bearish paradigm=GEX-LIS",
          lambda t: t["setup"]=="ES Absorption" and not t["is_long"] and t["paradigm"]=="GEX-LIS")
test_rule("Block: ES Abs bullish paradigm=AG-LIS",
          lambda t: t["setup"]=="ES Absorption" and t["is_long"] and t["paradigm"]=="AG-LIS")

print("\n--- Alignment-based blocks ---")
test_rule("Block: SC long align=-1",
          lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["align"]==-1)
test_rule("Block: DD long align=0",
          lambda t: t["setup"]=="DD Exhaustion" and t["is_long"] and t["align"]==0)
test_rule("Block: any setup align=-3 (deeply contrarian)",
          lambda t: t["align"] == -3)

print("\n--- Hour-based blocks (extending V16's R10) ---")
def hr(t): return t["ts"].astimezone(ET).hour
test_rule("Block: SC long hour=15 (last hour)",
          lambda t: t["setup"]=="Skew Charm" and t["is_long"] and hr(t)==15)
test_rule("Block: SC long hour=14 (pm chop)",
          lambda t: t["setup"]=="Skew Charm" and t["is_long"] and hr(t)==14)
test_rule("Block: SC short hour=15",
          lambda t: t["setup"]=="Skew Charm" and not t["is_long"] and hr(t)==15)

print("\n--- VIX regime blocks ---")
test_rule("Block: longs when VIX < 18 (calm regime, contrarian longs bleed)",
          lambda t: t["is_long"] and t["vix"] is not None and t["vix"] < 18)
test_rule("Block: shorts when VIX < 16",
          lambda t: not t["is_long"] and t["vix"] is not None and t["vix"] < 16)

print("\n--- Vanna regime blocks ---")
vanna_buckets = defaultdict(lambda: {"n":0,"pnl":0.0})
for t in trades:
    vanna_buckets[(t["setup"], t["dir"], t["vanna_regime"])]["n"] += 1
    vanna_buckets[(t["setup"], t["dir"], t["vanna_regime"])]["pnl"] += t["dollars"]
worst_vanna = sorted([(k,v) for k,v in vanna_buckets.items() if v["n"] >= 3], key=lambda x: x[1]["pnl"])[:5]
for (setup, dir_, vr), s in worst_vanna:
    if s["pnl"] < -50:
        print(f"  Block: {setup:<18} {dir_:<8} vanna={vr}  n={s['n']} ${s['pnl']:+.2f}")

# Top combined V17 candidates ranked by delta
print()
print("=" * 70)
print("TOP V17 RULES — ranked by P&L improvement")
print("=" * 70)
candidates.sort(key=lambda x: -x[3])
for rule, blocked_n, blocked_pnl, delta in candidates[:8]:
    print(f"  +${delta:>7.2f}  (blocks {blocked_n} trades, ${blocked_pnl:+.2f}) -- {rule}")

cur.close(); c.close()
