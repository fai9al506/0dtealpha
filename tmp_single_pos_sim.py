"""Simulate single-position mode with Greek filter on historical trades.
Only take 1 trade at a time — skip any signal that fires while a position is open.
"""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta
import statistics as stat_mod

engine = create_engine(os.environ["DATABASE_URL"])

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

# Enrich with charm alignment
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

        svb = t["spot_vol_beta"]
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
print(f"Total trades: {len(enriched)} -> Greek filtered: {len(filtered)}")

# ---- Simulate single-position mode ----
# Estimate trade duration from outcome_elapsed_min, default 30 min
def get_duration(t):
    if t["outcome_elapsed_min"] and t["outcome_elapsed_min"] > 0:
        return t["outcome_elapsed_min"]
    # Defaults by setup
    defaults = {"BofA Scalp": 30, "DD Exhaustion": 45, "GEX Long": 40,
                "AG Short": 40, "ES Absorption": 30, "Paradigm Reversal": 30, "Skew Charm": 30}
    return defaults.get(t["setup_name"], 30)

# Run simulation
executed = []
skipped_single_pos = []
pos_busy_until = None

for t in filtered:
    entry_time = t["ts_et"]

    # Check if position is busy
    if pos_busy_until is not None and entry_time < pos_busy_until:
        skipped_single_pos.append(t)
        continue

    # Take trade
    duration = get_duration(t)
    pos_busy_until = entry_time + timedelta(minutes=duration)
    executed.append(t)

print(f"Greek filtered: {len(filtered)} -> Single-pos executed: {len(executed)} -> Skipped (busy): {len(skipped_single_pos)}")
print()

# ---- Metrics ----
def metrics(tl):
    n = len(tl)
    if not n: return None
    wins = [t for t in tl if t["outcome_result"] == "WIN"]
    losses = [t for t in tl if t["outcome_result"] == "LOSS"]
    pnl = sum(t["outcome_pnl"] or 0 for t in tl)
    wr = len(wins) / n * 100
    daily = defaultdict(float)
    daily_count = defaultdict(int)
    for t in tl:
        daily[t["trade_date"]] += (t["outcome_pnl"] or 0)
        daily_count[t["trade_date"]] += 1
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
    return {"n": n, "w": len(wins), "l": len(losses), "wr": wr, "pnl": pnl,
            "avg_d": avg_d, "days": days, "pf": pf, "max_dd": max_dd,
            "sharpe": sharpe, "worst": worst, "best": best,
            "pct_w": wd/len(dv)*100 if dv else 0, "tpd": n/days if days else 0,
            "daily": daily, "daily_count": daily_count}

m_all = metrics(enriched)
m_filt = metrics(filtered)
m_exec = metrics(executed)

print("=" * 90)
print(" SINGLE-POSITION SIMULATION vs GREEK FILTER vs BASELINE")
print("=" * 90)
print(f"\n  {'':45} {'BASELINE':>14} {'GREEK FILT':>14} {'SINGLE-POS':>14}")
print(f"  {'-'*45} {'-'*14} {'-'*14} {'-'*14}")
print(f"  {'Trades':.<45} {m_all['n']:>14} {m_filt['n']:>14} {m_exec['n']:>14}")
print(f"  {'Trades/day':.<45} {m_all['tpd']:>13.1f} {m_filt['tpd']:>13.1f} {m_exec['tpd']:>13.1f}")
print(f"  {'Win Rate':.<45} {m_all['wr']:>13.1f}% {m_filt['wr']:>13.1f}% {m_exec['wr']:>13.1f}%")
print(f"  {'Total PnL (pts)':.<45} {m_all['pnl']:>+13.1f} {m_filt['pnl']:>+13.1f} {m_exec['pnl']:>+13.1f}")
print(f"  {'Avg Daily PnL (pts)':.<45} {m_all['avg_d']:>+13.1f} {m_filt['avg_d']:>+13.1f} {m_exec['avg_d']:>+13.1f}")
print(f"  {'Profit Factor':.<45} {m_all['pf']:>13.2f} {m_filt['pf']:>13.2f} {m_exec['pf']:>13.2f}")
print(f"  {'Max Drawdown (pts)':.<45} {m_all['max_dd']:>13.1f} {m_filt['max_dd']:>13.1f} {m_exec['max_dd']:>13.1f}")
print(f"  {'Sharpe':.<45} {m_all['sharpe']:>13.3f} {m_filt['sharpe']:>13.3f} {m_exec['sharpe']:>13.3f}")
print(f"  {'Worst Day (pts)':.<45} {m_all['worst']:>+13.1f} {m_filt['worst']:>+13.1f} {m_exec['worst']:>+13.1f}")
print(f"  {'Best Day (pts)':.<45} {m_all['best']:>+13.1f} {m_filt['best']:>+13.1f} {m_exec['best']:>+13.1f}")
print(f"  {'Winning Days %':.<45} {m_all['pct_w']:>13.0f}% {m_filt['pct_w']:>13.0f}% {m_exec['pct_w']:>13.0f}%")

# Dollar projections
print(f"\n{'='*90}")
print(f" DOLLAR PROJECTIONS — SINGLE-POSITION MODE")
print(f"{'='*90}")

for label, qty, ptval in [("3 MES (your size)", 3, 5), ("10 MES (=1 ES)", 10, 5),
                           ("2 ES", 2, 50), ("4 ES", 4, 50)]:
    dpp = qty * ptval
    sar = dpp * 3.75
    daily = m_exec["avg_d"] * dpp
    daily_sar = m_exec["avg_d"] * sar
    monthly = daily * 21
    monthly_sar = daily_sar * 21
    dd = m_exec["max_dd"] * dpp
    dd_sar = m_exec["max_dd"] * sar
    worst = m_exec["worst"] * dpp
    worst_sar = m_exec["worst"] * sar

    print(f"\n  {label} (${dpp}/pt = SAR {sar:.0f}/pt):")
    print(f"    Daily:    ${daily:>+8,.0f}  /  SAR {daily_sar:>+10,.0f}")
    print(f"    Monthly:  ${monthly:>+8,.0f}  /  SAR {monthly_sar:>+10,.0f}")
    print(f"    Yearly:   ${monthly*12:>+8,.0f}  /  SAR {monthly_sar*12:>+10,.0f}")
    print(f"    Max DD:   ${dd:>8,.0f}  /  SAR {dd_sar:>10,.0f}")
    print(f"    Worst day:${worst:>+8,.0f}  /  SAR {worst_sar:>+10,.0f}")

# Daily breakdown
print(f"\n{'='*90}")
print(f" DAILY EQUITY CURVE (single-position mode)")
print(f"{'='*90}")
print(f"\n  {'Date':<12} {'Trades':>7} {'PnL pts':>9} {'Cum pts':>9} {'$ (3MES)':>10} {'Cum $':>10} {'SAR':>12} {'Cum SAR':>12}")
print(f"  {'-'*12} {'-'*7} {'-'*9} {'-'*9} {'-'*10} {'-'*10} {'-'*12} {'-'*12}")

cum = 0
for d in sorted(m_exec["daily"].keys()):
    p = m_exec["daily"][d]
    n = m_exec["daily_count"][d]
    cum += p
    usd = p * 15  # 3 MES x $5
    cum_usd = cum * 15
    sar = usd * 3.75
    cum_sar = cum_usd * 3.75
    print(f"  {str(d):<12} {n:>7} {p:>+8.1f} {cum:>+8.1f} ${usd:>+9,.0f} ${cum_usd:>+9,.0f} SAR{sar:>+9,.0f} SAR{cum_sar:>+9,.0f}")

# Setup mix in executed trades
print(f"\n{'='*90}")
print(f" SETUP MIX (single-position executed trades)")
print(f"{'='*90}")
setup_exec = defaultdict(list)
for t in executed: setup_exec[t["setup_name"]].append(t)
print(f"\n  {'Setup':<20} {'N':>4} {'WR':>6} {'PnL':>8} {'PF':>6} {'Avg':>6}")
print(f"  {'-'*20} {'-'*4} {'-'*6} {'-'*8} {'-'*6} {'-'*6}")
for sn in sorted(setup_exec.keys(), key=lambda x: -sum(t["outcome_pnl"] or 0 for t in setup_exec[x])):
    sl = setup_exec[sn]
    w = sum(1 for t in sl if t["outcome_result"] == "WIN")
    l = sum(1 for t in sl if t["outcome_result"] == "LOSS")
    p = sum(t["outcome_pnl"] or 0 for t in sl)
    gw = sum(t["outcome_pnl"] for t in sl if t["outcome_result"] == "WIN" and t["outcome_pnl"])
    gl = abs(sum(t["outcome_pnl"] for t in sl if t["outcome_result"] == "LOSS" and t["outcome_pnl"]))
    pf = gw/gl if gl > 0 else float('inf')
    wr = w/len(sl)*100
    avg = p/len(sl)
    print(f"  {sn:<20} {len(sl):>4} {wr:>5.1f}% {p:>+7.1f} {pf:>5.2f} {avg:>+5.1f}")
