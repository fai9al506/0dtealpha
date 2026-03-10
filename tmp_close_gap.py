"""Deep analysis: Close the gap between Greek-filtered PnL and single-pos execution.
Test multiple strategies to recover skipped profits.
"""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta
import statistics as stat_mod

engine = create_engine(os.environ["DATABASE_URL"])

# ---- Pull all trades + enrich with charm ----
with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.max_plus_gex, s.paradigm,
               s.outcome_result, s.outcome_pnl, s.ts::date as trade_date,
               s.vanna_all, s.spot_vol_beta, s.greek_alignment,
               s.outcome_elapsed_min
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.ts
    """)).mappings().all()

enriched = []
with engine.begin() as conn:
    for t in trades:
        ts = t["ts_et"]
        agg_charm = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics' IS NOT NULL
              AND ts<=(:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": ts}).mappings().first()
        if snap:
            p = snap["payload"]
            if isinstance(p, str): p = json.loads(p)
            if isinstance(p, dict):
                cv = p.get("statistics", {}).get("aggregatedCharm")
                if cv is not None:
                    try: agg_charm = float(cv)
                    except: pass
        charm_aligned = None
        if agg_charm is not None:
            is_long = t["direction"] in ("long", "bullish")
            charm_aligned = (agg_charm > 0) == is_long
        enriched.append({**dict(t), "agg_charm": agg_charm, "charm_aligned": charm_aligned})

def greek_filter(t):
    if t["charm_aligned"] is not None and not t["charm_aligned"]: return False
    if t["setup_name"] == "GEX Long" and t["greek_alignment"] < 1: return False
    if t["setup_name"] == "AG Short" and t["greek_alignment"] == -3: return False
    if t["setup_name"] == "DD Exhaustion":
        svb = t["spot_vol_beta"]
        if svb is not None and -0.5 <= svb < 0: return False
    return True

filtered = [t for t in enriched if greek_filter(t)]

def get_duration(t):
    if t["outcome_elapsed_min"] and t["outcome_elapsed_min"] > 0:
        return t["outcome_elapsed_min"]
    defaults = {"BofA Scalp": 30, "DD Exhaustion": 45, "GEX Long": 40,
                "AG Short": 40, "ES Absorption": 30, "Paradigm Reversal": 30, "Skew Charm": 30}
    return defaults.get(t["setup_name"], 30)

def norm_dir(d):
    return "long" if d in ("long", "bullish") else "short"

# ---- METRICS HELPER ----
def metrics(tl):
    n = len(tl)
    if not n: return {"n":0,"pnl":0,"wr":0,"pf":0,"avg_d":0,"max_dd":0,"sharpe":0,"days":0,"tpd":0,"worst":0,"best":0,"pct_w":0}
    wins = [t for t in tl if t["outcome_result"] == "WIN"]
    losses = [t for t in tl if t["outcome_result"] == "LOSS"]
    pnl = sum(t["outcome_pnl"] or 0 for t in tl)
    wr = len(wins) / n * 100
    daily = defaultdict(float)
    for t in tl: daily[t["trade_date"]] += (t["outcome_pnl"] or 0)
    days = len(daily)
    avg_d = pnl / days if days else 0
    dv = [daily[d] for d in sorted(daily.keys())]
    cum = peak = max_dd = 0
    for d in dv:
        cum += d; peak = max(peak, cum); max_dd = max(max_dd, peak - cum)
    gw = sum(t["outcome_pnl"] for t in wins if t["outcome_pnl"])
    gl = abs(sum(t["outcome_pnl"] for t in losses if t["outcome_pnl"]))
    pf = gw / gl if gl > 0 else float('inf')
    sharpe = stat_mod.mean(dv) / stat_mod.stdev(dv) if len(dv) > 1 and stat_mod.stdev(dv) > 0 else 0
    worst = min(dv) if dv else 0
    best = max(dv) if dv else 0
    wd = sum(1 for d in dv if d > 0)
    return {"n": n, "pnl": pnl, "wr": wr, "pf": pf, "avg_d": avg_d, "days": days,
            "max_dd": max_dd, "sharpe": sharpe, "tpd": n/days if days else 0,
            "worst": worst, "best": best, "pct_w": wd/len(dv)*100 if dv else 0}

# ============================================================
# PART 1: Anatomy of the skipped trades
# ============================================================
print("=" * 100)
print(" PART 1: ANATOMY OF SKIPPED TRADES — What are we leaving on the table?")
print("=" * 100)

# Baseline single-pos sim
executed_base = []
skipped_base = []
pos_busy_until = None
pos_direction = None

for t in filtered:
    entry_time = t["ts_et"]
    if pos_busy_until is not None and entry_time < pos_busy_until:
        skipped_base.append({**t, "active_dir": pos_direction})
        continue
    duration = get_duration(t)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = norm_dir(t["direction"])
    executed_base.append(t)

print(f"\nGreek filtered: {len(filtered)} -> Executed: {len(executed_base)} -> Skipped: {len(skipped_base)}")

# Skipped by same vs opposite direction
same_dir = [t for t in skipped_base if norm_dir(t["direction"]) == t["active_dir"]]
opp_dir = [t for t in skipped_base if norm_dir(t["direction"]) != t["active_dir"]]

m_same = metrics(same_dir)
m_opp = metrics(opp_dir)

print(f"\n  Skipped SAME direction:     {len(same_dir):>4} trades | WR: {m_same['wr']:>5.1f}% | PnL: {m_same['pnl']:>+8.1f} pts")
print(f"  Skipped OPPOSITE direction: {len(opp_dir):>4} trades | WR: {m_opp['wr']:>5.1f}% | PnL: {m_opp['pnl']:>+8.1f} pts")
print(f"  Total skipped PnL:          {m_same['pnl'] + m_opp['pnl']:>+8.1f} pts (this is the gap)")

# Skipped by setup
print(f"\n  Skipped by setup (same-dir):")
for setup in sorted(set(t["setup_name"] for t in same_dir)):
    sub = [t for t in same_dir if t["setup_name"] == setup]
    m = metrics(sub)
    print(f"    {setup:<20} {m['n']:>3} trades | WR: {m['wr']:>5.1f}% | PnL: {m['pnl']:>+7.1f}")

print(f"\n  Skipped by setup (opposite-dir):")
for setup in sorted(set(t["setup_name"] for t in opp_dir)):
    sub = [t for t in opp_dir if t["setup_name"] == setup]
    m = metrics(sub)
    print(f"    {setup:<20} {m['n']:>3} trades | WR: {m['wr']:>5.1f}% | PnL: {m['pnl']:>+7.1f}")

# Skipped by outcome
print(f"\n  Skipped wins we missed:  {sum(1 for t in skipped_base if t['outcome_result']=='WIN'):>4} ({sum(t['outcome_pnl'] or 0 for t in skipped_base if t['outcome_result']=='WIN'):+.1f} pts)")
print(f"  Skipped losses we dodged:{sum(1 for t in skipped_base if t['outcome_result']=='LOSS'):>4} ({sum(t['outcome_pnl'] or 0 for t in skipped_base if t['outcome_result']=='LOSS'):+.1f} pts)")

# Time gap analysis — how soon after position opens do skipped signals fire?
print(f"\n  Time gap: skipped signal fires how long after position opened?")
gaps = []
# Rebuild to track when position was opened
pos_open_time = None
pos_busy_until2 = None
pos_dir2 = None
for t in filtered:
    entry = t["ts_et"]
    if pos_busy_until2 is not None and entry < pos_busy_until2:
        gap_min = (entry - pos_open_time).total_seconds() / 60
        gaps.append(gap_min)
        continue
    pos_open_time = entry
    dur = get_duration(t)
    pos_busy_until2 = entry + timedelta(minutes=dur)
    pos_dir2 = norm_dir(t["direction"])

if gaps:
    print(f"    Median gap: {stat_mod.median(gaps):.0f} min | Mean: {stat_mod.mean(gaps):.0f} min | Min: {min(gaps):.0f} | Max: {max(gaps):.0f}")
    buckets = {"0-5 min": 0, "5-15 min": 0, "15-30 min": 0, "30+ min": 0}
    for g in gaps:
        if g <= 5: buckets["0-5 min"] += 1
        elif g <= 15: buckets["5-15 min"] += 1
        elif g <= 30: buckets["15-30 min"] += 1
        else: buckets["30+ min"] += 1
    for label, count in buckets.items():
        print(f"    {label}: {count} signals ({count/len(gaps)*100:.0f}%)")


# ============================================================
# PART 2: STRATEGY SIMULATIONS
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 2: STRATEGY SIMULATIONS")
print(f"{'='*100}")

# --- Strategy A: Same-direction stacking (add to position) ---
# When same-direction signal fires while in a position, count it as additional PnL
# (simulates adding contracts or taking a 2nd correlated position)
print(f"\n{'='*100}")
print(f" STRATEGY A: SAME-DIRECTION STACKING")
print(f" Add-to-position when same direction fires. Opposite direction = skip.")
print(f"{'='*100}")

exec_a = []
pos_busy_until = None
pos_direction = None

for t in filtered:
    entry_time = t["ts_et"]
    if pos_busy_until is not None and entry_time < pos_busy_until:
        # Same direction? Stack it (count as executed)
        if norm_dir(t["direction"]) == pos_direction:
            exec_a.append(t)
            # Extend busy time by this trade's duration
            new_end = entry_time + timedelta(minutes=get_duration(t))
            pos_busy_until = max(pos_busy_until, new_end)
        # Opposite? Skip
        continue
    duration = get_duration(t)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = norm_dir(t["direction"])
    exec_a.append(t)

m_a = metrics(exec_a)

# --- Strategy B: Direction-locked 2 positions ---
# Allow up to 2 positions but only same direction
print(f"\n{'='*100}")
print(f" STRATEGY B: DIRECTION-LOCKED 2-SLOT")
print(f" Allow up to 2 concurrent positions, must be same direction. Opposite = skip.")
print(f"{'='*100}")

exec_b = []
slots = []  # list of (busy_until, direction)
MAX_SLOTS = 2

for t in filtered:
    entry_time = t["ts_et"]
    # Clean expired slots
    slots = [(bu, d) for bu, d in slots if entry_time < bu]

    if len(slots) >= MAX_SLOTS:
        continue  # all slots busy

    td = norm_dir(t["direction"])
    # Check direction lock: if any slot occupied, must match direction
    if slots and any(d != td for _, d in slots):
        continue  # opposite direction, skip

    duration = get_duration(t)
    slots.append((entry_time + timedelta(minutes=duration), td))
    exec_b.append(t)

m_b = metrics(exec_b)

# --- Strategy B2: Direction-locked 3 positions ---
print(f"\n{'='*100}")
print(f" STRATEGY B2: DIRECTION-LOCKED 3-SLOT")
print(f"{'='*100}")

exec_b2 = []
slots = []
MAX_SLOTS_3 = 3

for t in filtered:
    entry_time = t["ts_et"]
    slots = [(bu, d) for bu, d in slots if entry_time < bu]
    if len(slots) >= MAX_SLOTS_3:
        continue
    td = norm_dir(t["direction"])
    if slots and any(d != td for _, d in slots):
        continue
    duration = get_duration(t)
    slots.append((entry_time + timedelta(minutes=duration), td))
    exec_b2.append(t)

m_b2 = metrics(exec_b2)

# --- Strategy C: Close + Reopen on opposite (reversal with Greek filter) ---
# If opposite-direction signal fires while in position, close current at current PnL
# rate (time-prorated) and open new. Since Greek filter cleaned up, reversals should be rare.
# SIMPLIFIED: just count both trades (old trade's actual PnL + new trade's actual PnL)
print(f"\n{'='*100}")
print(f" STRATEGY C: SMART REVERSAL (close current + open opposite)")
print(f" When opposite fires: close current trade, open new one.")
print(f" (We use actual outcomes for both since we can't simulate partial PnL)")
print(f"{'='*100}")

exec_c = []
pos_busy_until = None
pos_direction = None
pos_trade = None
reversals = 0

for t in filtered:
    entry_time = t["ts_et"]
    td = norm_dir(t["direction"])

    if pos_busy_until is not None and entry_time < pos_busy_until:
        if td == pos_direction:
            # Same direction: stack
            exec_c.append(t)
            new_end = entry_time + timedelta(minutes=get_duration(t))
            pos_busy_until = max(pos_busy_until, new_end)
        else:
            # Opposite: close current (already counted), open new
            reversals += 1
            duration = get_duration(t)
            pos_busy_until = entry_time + timedelta(minutes=duration)
            pos_direction = td
            pos_trade = t
            exec_c.append(t)
        continue

    duration = get_duration(t)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = td
    pos_trade = t
    exec_c.append(t)

m_c = metrics(exec_c)

# --- Strategy D: Skip opposite signals entirely (same as A but let's see if opposite are net negative) ---
# Already computed in Strategy A. Let's just also test: what if we ONLY skip same-dir (take opposites)?

# --- Strategy E: Reduced hold time — use max 30 min for all setups ---
print(f"\n{'='*100}")
print(f" STRATEGY E: FASTER EXITS (cap all hold times at 30 min)")
print(f" Frees up position slot faster.")
print(f"{'='*100}")

exec_e = []
pos_busy_until = None

for t in filtered:
    entry_time = t["ts_et"]
    if pos_busy_until is not None and entry_time < pos_busy_until:
        continue
    duration = min(get_duration(t), 30)  # Cap at 30 min
    pos_busy_until = entry_time + timedelta(minutes=duration)
    exec_e.append(t)

m_e = metrics(exec_e)

# --- Strategy F: HYBRID — same-dir stacking + faster exits (30 min cap) ---
print(f"\n{'='*100}")
print(f" STRATEGY F: HYBRID (same-dir stacking + 30-min cap)")
print(f"{'='*100}")

exec_f = []
pos_busy_until = None
pos_direction = None

for t in filtered:
    entry_time = t["ts_et"]
    if pos_busy_until is not None and entry_time < pos_busy_until:
        if norm_dir(t["direction"]) == pos_direction:
            exec_f.append(t)
            new_end = entry_time + timedelta(minutes=min(get_duration(t), 30))
            pos_busy_until = max(pos_busy_until, new_end)
        continue
    duration = min(get_duration(t), 30)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = norm_dir(t["direction"])
    exec_f.append(t)

m_f = metrics(exec_f)

# --- Strategy G: HYBRID + smart reversal (same-dir stack + opp reversal + 30min cap) ---
print(f"\n{'='*100}")
print(f" STRATEGY G: FULL HYBRID (same-dir stack + opp reversal + 30-min cap)")
print(f"{'='*100}")

exec_g = []
pos_busy_until = None
pos_direction = None
rev_g = 0

for t in filtered:
    entry_time = t["ts_et"]
    td = norm_dir(t["direction"])

    if pos_busy_until is not None and entry_time < pos_busy_until:
        if td == pos_direction:
            exec_g.append(t)
            new_end = entry_time + timedelta(minutes=min(get_duration(t), 30))
            pos_busy_until = max(pos_busy_until, new_end)
        else:
            # Reversal
            rev_g += 1
            duration = min(get_duration(t), 30)
            pos_busy_until = entry_time + timedelta(minutes=duration)
            pos_direction = td
            exec_g.append(t)
        continue

    duration = min(get_duration(t), 30)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = td
    exec_g.append(t)

m_g = metrics(exec_g)

# --- Strategy H: Direction-locked 2-slot + 30min cap ---
print(f"\n{'='*100}")
print(f" STRATEGY H: 2-SLOT DIRECTION-LOCKED + 30-MIN CAP")
print(f"{'='*100}")

exec_h = []
slots = []

for t in filtered:
    entry_time = t["ts_et"]
    slots = [(bu, d) for bu, d in slots if entry_time < bu]
    if len(slots) >= 2:
        continue
    td = norm_dir(t["direction"])
    if slots and any(d != td for _, d in slots):
        continue
    duration = min(get_duration(t), 30)
    slots.append((entry_time + timedelta(minutes=duration), td))
    exec_h.append(t)

m_h = metrics(exec_h)


# ============================================================
# PART 3: MASTER COMPARISON TABLE
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 3: MASTER COMPARISON — ALL STRATEGIES")
print(f"{'='*100}")

strategies = [
    ("Greek Filter (no limit)", metrics(filtered)),
    ("Single-Pos (current)", metrics(executed_base)),
    ("A: Same-dir stacking", m_a),
    ("B: Dir-locked 2-slot", m_b),
    ("B2: Dir-locked 3-slot", m_b2),
    ("C: Smart reversal+stack", m_c),
    ("E: Faster exits (30m)", m_e),
    ("F: Stack + 30m cap", m_f),
    ("G: Full hybrid", m_g),
    ("H: 2-slot + 30m cap", m_h),
]

print(f"\n  {'Strategy':<28} {'Trades':>7} {'T/Day':>6} {'WR':>7} {'PnL':>9} {'Avg/Day':>8} {'PF':>6} {'MaxDD':>7} {'Sharpe':>7} {'W Days':>7}")
print(f"  {'-'*28} {'-'*7} {'-'*6} {'-'*7} {'-'*9} {'-'*8} {'-'*6} {'-'*7} {'-'*7} {'-'*7}")

for label, m in strategies:
    print(f"  {label:<28} {m['n']:>7} {m['tpd']:>5.1f} {m['wr']:>6.1f}% {m['pnl']:>+8.1f} {m['avg_d']:>+7.1f} {m['pf']:>5.2f} {m['max_dd']:>6.1f} {m['sharpe']:>6.3f} {m['pct_w']:>6.0f}%")

# ============================================================
# PART 4: DOLLAR PROJECTIONS FOR TOP STRATEGIES
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 4: DOLLAR PROJECTIONS (3 MES = SAR 56.25/pt)")
print(f"{'='*100}")

# Find top 3 strategies by avg daily PnL (excluding the unlimited reference)
ranked = sorted(strategies[1:], key=lambda x: -x[1]["avg_d"])  # skip Greek Filter reference

print(f"\n  {'Strategy':<28} {'Daily $':>9} {'Daily SAR':>11} {'Monthly $':>11} {'Monthly SAR':>13} {'MaxDD $':>9} {'MaxDD SAR':>11}")
print(f"  {'-'*28} {'-'*9} {'-'*11} {'-'*11} {'-'*13} {'-'*9} {'-'*11}")

for label, m in ranked:
    dpp = 15  # 3 MES x $5
    sar = dpp * 3.75
    daily_usd = m["avg_d"] * dpp
    daily_sar = m["avg_d"] * sar
    monthly_usd = daily_usd * 21
    monthly_sar = daily_sar * 21
    dd_usd = m["max_dd"] * dpp
    dd_sar = m["max_dd"] * sar
    print(f"  {label:<28} ${daily_usd:>+7,.0f} SAR{daily_sar:>+8,.0f} ${monthly_usd:>+9,.0f} SAR{monthly_sar:>+10,.0f} ${dd_usd:>7,.0f} SAR{dd_sar:>8,.0f}")

# 10 MES projections for top strategies
print(f"\n  10 MES (= 1 ES) projections:")
print(f"  {'Strategy':<28} {'Daily $':>9} {'Daily SAR':>11} {'Monthly $':>11} {'Monthly SAR':>13} {'MaxDD $':>9} {'MaxDD SAR':>11}")
print(f"  {'-'*28} {'-'*9} {'-'*11} {'-'*11} {'-'*13} {'-'*9} {'-'*11}")

for label, m in ranked[:5]:
    dpp = 50  # 10 MES x $5
    sar = dpp * 3.75
    daily_usd = m["avg_d"] * dpp
    daily_sar = m["avg_d"] * sar
    monthly_usd = daily_usd * 21
    monthly_sar = daily_sar * 21
    dd_usd = m["max_dd"] * dpp
    dd_sar = m["max_dd"] * sar
    print(f"  {label:<28} ${daily_usd:>+7,.0f} SAR{daily_sar:>+8,.0f} ${monthly_usd:>+9,.0f} SAR{monthly_sar:>+10,.0f} ${dd_usd:>7,.0f} SAR{dd_sar:>8,.0f}")


# ============================================================
# PART 5: DEEP DIVE ON OPPOSITE-DIRECTION SIGNALS
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 5: OPPOSITE-DIRECTION SIGNALS — ARE THEY WORTH REVERSING FOR?")
print(f"{'='*100}")

print(f"\n  When we're in a LONG position and a SHORT signal fires:")
long_pos_short_sig = [t for t in opp_dir if t["active_dir"] == "long"]
m_ls = metrics(long_pos_short_sig)
print(f"    {m_ls['n']} trades | WR: {m_ls['wr']:.1f}% | PnL: {m_ls['pnl']:+.1f}")

print(f"\n  When we're in a SHORT position and a LONG signal fires:")
short_pos_long_sig = [t for t in opp_dir if t["active_dir"] == "short"]
m_sl = metrics(short_pos_long_sig)
print(f"    {m_sl['n']} trades | WR: {m_sl['wr']:.1f}% | PnL: {m_sl['pnl']:+.1f}")

# How many opposite signals fire per day?
opp_by_day = defaultdict(int)
for t in opp_dir:
    opp_by_day[t["trade_date"]] += 1
if opp_by_day:
    print(f"\n  Opposite signals per day: avg={stat_mod.mean(opp_by_day.values()):.1f}, max={max(opp_by_day.values())}")

# ============================================================
# PART 6: SAME-DIR STACKING DETAIL
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 6: SAME-DIRECTION STACKING — THE EASY WIN")
print(f"{'='*100}")

print(f"\n  Same-direction signals while in position: {len(same_dir)}")
print(f"  These are CONFIRMATIONS — the market is agreeing with our direction.")
print(f"  PnL if we took them: {m_same['pnl']:+.1f} pts | WR: {m_same['wr']:.1f}%")

# How many same-dir signals fire per active trade on average?
# Group same_dir by the trade they were blocked by
print(f"\n  Same-dir by outcome:")
same_wins = [t for t in same_dir if t["outcome_result"] == "WIN"]
same_losses = [t for t in same_dir if t["outcome_result"] == "LOSS"]
print(f"    Winners we missed: {len(same_wins)} trades ({sum(t['outcome_pnl'] or 0 for t in same_wins):+.1f} pts)")
print(f"    Losses we avoided: {len(same_losses)} trades ({sum(t['outcome_pnl'] or 0 for t in same_losses):+.1f} pts)")

# What setups produce the best same-dir stacking results?
print(f"\n  Same-dir stacking by setup:")
for setup in sorted(set(t["setup_name"] for t in same_dir)):
    sub = [t for t in same_dir if t["setup_name"] == setup]
    m = metrics(sub)
    marker = " <-- PROFITABLE" if m["pnl"] > 0 else " <-- toxic" if m["pnl"] < -20 else ""
    print(f"    {setup:<20} {m['n']:>3} | WR: {m['wr']:>5.1f}% | PnL: {m['pnl']:>+7.1f}{marker}")


# ============================================================
# PART 7: SELECTIVE STACKING — only stack profitable setups
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 7: SELECTIVE STACKING — Only stack high-quality same-dir signals")
print(f"{'='*100}")

# Determine which setups are profitable when stacked
profitable_stack_setups = set()
for setup in sorted(set(t["setup_name"] for t in same_dir)):
    sub = [t for t in same_dir if t["setup_name"] == setup]
    m = metrics(sub)
    if m["pnl"] > 0:
        profitable_stack_setups.add(setup)
print(f"  Profitable stack setups: {profitable_stack_setups}")

exec_sel = []
pos_busy_until = None
pos_direction = None

for t in filtered:
    entry_time = t["ts_et"]
    if pos_busy_until is not None and entry_time < pos_busy_until:
        if norm_dir(t["direction"]) == pos_direction and t["setup_name"] in profitable_stack_setups:
            exec_sel.append(t)
            new_end = entry_time + timedelta(minutes=get_duration(t))
            pos_busy_until = max(pos_busy_until, new_end)
        continue
    duration = get_duration(t)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = norm_dir(t["direction"])
    exec_sel.append(t)

m_sel = metrics(exec_sel)
print(f"\n  Selective stacking: {m_sel['n']} trades | PnL: {m_sel['pnl']:+.1f} | Avg/Day: {m_sel['avg_d']:+.1f} | WR: {m_sel['wr']:.1f}% | PF: {m_sel['pf']:.2f} | MaxDD: {m_sel['max_dd']:.1f}")

# And with 30-min cap
exec_sel30 = []
pos_busy_until = None
pos_direction = None

for t in filtered:
    entry_time = t["ts_et"]
    if pos_busy_until is not None and entry_time < pos_busy_until:
        if norm_dir(t["direction"]) == pos_direction and t["setup_name"] in profitable_stack_setups:
            exec_sel30.append(t)
            new_end = entry_time + timedelta(minutes=min(get_duration(t), 30))
            pos_busy_until = max(pos_busy_until, new_end)
        continue
    duration = min(get_duration(t), 30)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    pos_direction = norm_dir(t["direction"])
    exec_sel30.append(t)

m_sel30 = metrics(exec_sel30)
print(f"  Selective + 30m cap:  {m_sel30['n']} trades | PnL: {m_sel30['pnl']:+.1f} | Avg/Day: {m_sel30['avg_d']:+.1f} | WR: {m_sel30['wr']:.1f}% | PF: {m_sel30['pf']:.2f} | MaxDD: {m_sel30['max_dd']:.1f}")


# ============================================================
# PART 8: RISK-ADJUSTED RANKING
# ============================================================
print(f"\n\n{'='*100}")
print(f" PART 8: FINAL RANKING (by PnL / MaxDD ratio — return per unit of risk)")
print(f"{'='*100}")

all_strats = [
    ("Single-Pos (current)", metrics(executed_base)),
    ("A: Same-dir stacking", m_a),
    ("B: Dir-locked 2-slot", m_b),
    ("B2: Dir-locked 3-slot", m_b2),
    ("C: Smart reversal+stack", m_c),
    ("E: Faster exits (30m)", m_e),
    ("F: Stack + 30m cap", m_f),
    ("G: Full hybrid", m_g),
    ("H: 2-slot + 30m cap", m_h),
    ("Selective stacking", m_sel),
    ("Selective + 30m cap", m_sel30),
]

# Add PnL/DD ratio
for label, m in all_strats:
    m["pnl_dd"] = m["pnl"] / m["max_dd"] if m["max_dd"] > 0 else float('inf')

ranked_risk = sorted(all_strats, key=lambda x: -x[1]["pnl_dd"])

print(f"\n  {'Rank':>4} {'Strategy':<28} {'PnL':>8} {'MaxDD':>7} {'PnL/DD':>7} {'Avg/Day':>8} {'WR':>6} {'Sharpe':>7} {'PF':>6}")
print(f"  {'-'*4} {'-'*28} {'-'*8} {'-'*7} {'-'*7} {'-'*8} {'-'*6} {'-'*7} {'-'*6}")

for rank, (label, m) in enumerate(ranked_risk, 1):
    star = " ***" if rank <= 3 else ""
    print(f"  {rank:>4} {label:<28} {m['pnl']:>+7.1f} {m['max_dd']:>6.1f} {m['pnl_dd']:>6.2f} {m['avg_d']:>+7.1f} {m['wr']:>5.1f}% {m['sharpe']:>6.3f} {m['pf']:>5.2f}{star}")

# Winner recommendation
winner = ranked_risk[0]
print(f"\n  RECOMMENDED: {winner[0]}")
w = winner[1]
dpp = 15
sar = dpp * 3.75
print(f"  At 3 MES: Daily ${w['avg_d']*dpp:+,.0f} / SAR {w['avg_d']*sar:+,.0f} | Monthly ${w['avg_d']*dpp*21:+,.0f} / SAR {w['avg_d']*sar*21:+,.0f}")
print(f"  Max DD: ${w['max_dd']*dpp:,.0f} / SAR {w['max_dd']*sar:,.0f}")
print(f"  Improvement over single-pos: {w['avg_d'] - metrics(executed_base)['avg_d']:+.1f} pts/day ({(w['avg_d']/metrics(executed_base)['avg_d']-1)*100:+.0f}%)")
