"""Side-by-side: Baseline (no filter) vs Greek Optimal Filter at every contract size."""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text
from collections import defaultdict
import statistics as stat_mod

engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.setup_name, s.direction, s.spot, s.max_plus_gex,
               s.outcome_result, s.outcome_pnl, s.ts::date as trade_date
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_result != 'EXPIRED'
          AND s.grade != 'LOG'
        ORDER BY s.id
    """)).mappings().all()

enriched = []
with engine.begin() as conn:
    for t in trades:
        ts = t["ts_et"]
        vanna_all = None
        r = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total FROM volland_exposure_points
            WHERE greek='vanna' AND expiration_option='ALL'
              AND ts_utc=(SELECT MAX(ts_utc) FROM volland_exposure_points
                         WHERE greek='vanna' AND expiration_option='ALL'
                           AND ts_utc<=(:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC'))
        """), {"ts": ts}).mappings().first()
        if r and r["total"] is not None: vanna_all = float(r["total"])

        svb_correlation, agg_charm = None, None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics'->'spot_vol_beta' IS NOT NULL
              AND ts<=(:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": ts}).mappings().first()
        if snap:
            p = snap["payload"]
            if isinstance(p, str): p = json.loads(p)
            if isinstance(p, dict):
                st = p.get("statistics", {})
                svb = st.get("spot_vol_beta", {})
                if isinstance(svb, dict) and svb.get("correlation") is not None:
                    try: svb_correlation = float(svb["correlation"])
                    except: pass
                cv = st.get("aggregatedCharm")
                if cv is not None:
                    try: agg_charm = float(cv)
                    except: pass

        direction = t["direction"]
        is_long = direction in ("long", "bullish")
        alignment = 0
        if agg_charm is not None: alignment += 1 if (agg_charm > 0) == is_long else -1
        if vanna_all is not None: alignment += 1 if (vanna_all > 0) == is_long else -1
        if t["spot"] and t["max_plus_gex"]: alignment += 1 if (t["spot"] <= t["max_plus_gex"]) == is_long else -1

        charm_aligned = None
        if agg_charm is not None: charm_aligned = (agg_charm > 0) == is_long

        enriched.append({**dict(t), "vanna_all": vanna_all, "svb_correlation": svb_correlation,
                         "agg_charm": agg_charm, "greek_alignment": alignment,
                         "charm_aligned": charm_aligned})

def optimal_filter(t):
    if t["charm_aligned"] is not None and not t["charm_aligned"]: return False
    if t["setup_name"] == "GEX Long" and t["greek_alignment"] < 1: return False
    if t["setup_name"] == "AG Short" and t["greek_alignment"] == -3: return False
    if t["setup_name"] == "DD Exhaustion":
        if t["svb_correlation"] is not None and -0.5 <= t["svb_correlation"] < 0: return False
    return True

baseline = enriched
filtered = [t for t in enriched if optimal_filter(t)]

def metrics(tl):
    n = len(tl)
    if not n: return None
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
    aw = gw / len(wins) if wins else 0
    al = gl / len(losses) if losses else 0
    worst = min(dv) if dv else 0
    best = max(dv) if dv else 0
    wd = sum(1 for d in dv if d > 0)
    pct_w = wd / len(dv) * 100 if dv else 0
    mls = cur = 0
    for t in sorted(tl, key=lambda x: x["id"]):
        if t["outcome_result"] == "LOSS": cur += 1; mls = max(mls, cur)
        else: cur = 0

    return {"n": n, "w": len(wins), "l": len(losses), "wr": wr, "pnl": pnl,
            "avg_d": avg_d, "days": days, "pf": pf, "max_dd": max_dd,
            "sharpe": sharpe, "aw": aw, "al": al, "worst": worst, "best": best,
            "pct_w": pct_w, "mls": mls, "tpd": n/days if days else 0}

b = metrics(baseline)
f = metrics(filtered)

sizes = [
    ("10 MES (=1 ES)", 50),
    ("20 MES (=2 ES)", 100),
    ("4 ES", 200),
    ("6 ES", 300),
]

print("=" * 100)
print(" SIDE-BY-SIDE COMPARISON: CURRENT SYSTEM vs GREEK OPTIMAL FILTER")
print("=" * 100)

print(f"\n  {'':50} {'CURRENT':>20} {'WITH FILTER':>20} {'CHANGE':>15}")
print(f"  {'':50} {'(no filter)':>20} {'(Greek Optimal)':>20} {'':>15}")
print(f"  {'-'*50} {'-'*20} {'-'*20} {'-'*15}")
print(f"  {'Trades':.<50} {b['n']:>20} {f['n']:>20} {f['n']-b['n']:>+15}")
print(f"  {'Win Rate':.<50} {b['wr']:>19.1f}% {f['wr']:>19.1f}% {f['wr']-b['wr']:>+14.1f}%")
bwl = f"{b['w']}W / {b['l']}L"
fwl = f"{f['w']}W / {f['l']}L"
print(f"  {'Wins / Losses':.<50} {bwl:>20} {fwl:>20}")
print(f"  {'Total PnL (SPX pts)':.<50} {b['pnl']:>+19.1f} {f['pnl']:>+19.1f} {f['pnl']-b['pnl']:>+14.1f}")
print(f"  {'Avg Daily PnL (pts)':.<50} {b['avg_d']:>+19.1f} {f['avg_d']:>+19.1f} {f['avg_d']-b['avg_d']:>+14.1f}")
print(f"  {'Trades per Day':.<50} {b['tpd']:>19.1f} {f['tpd']:>19.1f} {f['tpd']-b['tpd']:>+14.1f}")
print(f"  {'Profit Factor':.<50} {b['pf']:>19.2f} {f['pf']:>19.2f} {f['pf']-b['pf']:>+14.2f}")
print(f"  {'Max Drawdown (pts)':.<50} {b['max_dd']:>19.1f} {f['max_dd']:>19.1f} {f['max_dd']-b['max_dd']:>+14.1f}")
print(f"  {'Sharpe Ratio (daily)':.<50} {b['sharpe']:>19.3f} {f['sharpe']:>19.3f} {f['sharpe']-b['sharpe']:>+14.3f}")
print(f"  {'Avg Win (pts)':.<50} {b['aw']:>+19.1f} {f['aw']:>+19.1f} {f['aw']-b['aw']:>+14.1f}")
print(f"  {'Avg Loss (pts)':.<50} {b['al']:>19.1f} {f['al']:>19.1f} {b['al']-f['al']:>+14.1f}")
print(f"  {'Worst Day (pts)':.<50} {b['worst']:>+19.1f} {f['worst']:>+19.1f} {f['worst']-b['worst']:>+14.1f}")
print(f"  {'Best Day (pts)':.<50} {b['best']:>+19.1f} {f['best']:>+19.1f} {f['best']-b['best']:>+14.1f}")
print(f"  {'Winning Days %':.<50} {b['pct_w']:>19.0f}% {f['pct_w']:>19.0f}% {f['pct_w']-b['pct_w']:>+14.0f}%")
print(f"  {'Max Loss Streak':.<50} {b['mls']:>20} {f['mls']:>20} {f['mls']-b['mls']:>+15}")
print(f"  {'Trading Days':.<50} {b['days']:>20} {f['days']:>20}")

for label, dpp in sizes:
    print(f"\n\n{'='*100}")
    print(f" {label} (${dpp}/pt)")
    print(f"{'='*100}")

    b_daily = b["avg_d"] * dpp
    f_daily = f["avg_d"] * dpp
    b_monthly = b_daily * 21
    f_monthly = f_daily * 21
    b_yearly = b_daily * 252
    f_yearly = f_daily * 252
    b_dd = b["max_dd"] * dpp
    f_dd = f["max_dd"] * dpp
    b_worst = b["worst"] * dpp
    f_worst = f["worst"] * dpp
    b_best = b["best"] * dpp
    f_best = f["best"] * dpp

    print(f"\n  {'':50} {'CURRENT':>15} {'FILTERED':>15} {'IMPROVEMENT':>15}")
    print(f"  {'-'*50} {'-'*15} {'-'*15} {'-'*15}")
    print(f"  {'Daily Income':.<50} ${b_daily:>+13,.0f} ${f_daily:>+13,.0f} ${f_daily-b_daily:>+13,.0f}")
    print(f"  {'Weekly Income (5 days)':.<50} ${b_daily*5:>+13,.0f} ${f_daily*5:>+13,.0f} ${(f_daily-b_daily)*5:>+13,.0f}")
    print(f"  {'Monthly Income (21 days)':.<50} ${b_monthly:>+13,.0f} ${f_monthly:>+13,.0f} ${f_monthly-b_monthly:>+13,.0f}")
    print(f"  {'Quarterly Income':.<50} ${b_monthly*3:>+13,.0f} ${f_monthly*3:>+13,.0f} ${(f_monthly-b_monthly)*3:>+13,.0f}")
    print(f"  {'Yearly Income (252 days)':.<50} ${b_yearly:>+13,.0f} ${f_yearly:>+13,.0f} ${f_yearly-b_yearly:>+13,.0f}")
    print(f"  {'':.<50} {'':>15} {'':>15} {'':>15}")
    print(f"  {'Max Drawdown':.<50} ${b_dd:>13,.0f} ${f_dd:>13,.0f} ${b_dd-f_dd:>13,.0f} saved")
    print(f"  {'Worst Day':.<50} ${b_worst:>+13,.0f} ${f_worst:>+13,.0f} ${f_worst-b_worst:>+13,.0f}")
    print(f"  {'Best Day':.<50} ${b_best:>+13,.0f} ${f_best:>+13,.0f} ${f_best-b_best:>+13,.0f}")
    print(f"  {'Monthly Improvement':.<50} {'':>15} {'':>15}   +{(f_monthly-b_monthly)/b_monthly*100:.0f}%")
    print(f"  {'DD Reduction':.<50} {'':>15} {'':>15}   -{(b_dd-f_dd)/b_dd*100:.0f}%")
