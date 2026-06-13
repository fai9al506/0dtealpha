"""Prevention rules backtest on ACTUALLY-PLACED TSRT trades (real_trade_orders).

Per-lid $ = (close_fill - fill) * sign * $5 * qty  (MCHK-validated method)
Fallback when close fill missing: portal outcome_pnl * $5 (counted + reported).

Rules tested (each replays the placed-trade stream per day, removing blocked trades):
  A1: after 2 consecutive >=  $20 losses on SAME setup+direction -> block that setup+dir til EOD
  A2: same but 120-min cooldown instead of EOD
  B : after 3 consecutive >= $20 losses on SAME DIRECTION (any setup) -> block direction 120 min
  C : daily loss cap sweep: stop all new entries when cum day P&L <= -X (200/250/300)

Eras: full Apr 1 - Jun 3, and post-V16 (May 18+) separately.
"""
import os, json
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from datetime import timedelta, date

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT r.setup_log_id, r.state, l.setup_name, l.direction, l.ts, l.outcome_pnl, l.paradigm, l.grade
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-04-01'
    ORDER BY l.ts
""")
trades = []
fallbacks = 0
for lid, state, name, d, ts, pnl, para, grade in cur.fetchall():
    st = state if isinstance(state, dict) else json.loads(state or "{}")
    t = ts.astimezone(ET)
    sign = 1 if (d or "").lower() in ("long", "bullish", "buy") else -1
    fill = st.get("fill_price"); close_p = st.get("close_fill_price")
    qty = float(st.get("qty") or 1)
    if fill is not None and close_p is not None:
        dollars = (float(close_p) - float(fill)) * sign * 5.0 * qty
    elif pnl is not None:
        dollars = float(pnl) * 5.0
        fallbacks += 1
    else:
        continue
    trades.append(dict(lid=lid, name=name, dir="L" if sign > 0 else "S", t=t,
                       d=t.date(), usd=dollars, para=para, grade=grade))
print(f"placed trades Apr 1 - Jun 3: {len(trades)} (fallback-$ for {fallbacks})")
base_total = sum(x["usd"] for x in trades)
print(f"baseline total: ${base_total:+.2f}")

by_day = defaultdict(list)
for x in trades:
    by_day[x["d"]].append(x)

LOSS_THRESH = -20.0

def replay(rule, era_start=None):
    """returns (kept_total, removed list)"""
    kept, removed = 0.0, []
    for d, xs in sorted(by_day.items()):
        if era_start and d < era_start:
            continue
        consec = defaultdict(int)         # (setup,dir) -> consecutive losses
        consec_dir = defaultdict(int)     # dir -> consecutive losses
        block_until = {}                  # key -> ET datetime or 'EOD'
        cum = 0.0
        for x in sorted(xs, key=lambda z: z["t"]):
            key_sd = (x["name"], x["dir"]); key_d = x["dir"]
            blocked = False
            if rule == "A1" and block_until.get(key_sd) == "EOD":
                blocked = True
            if rule == "A2" and key_sd in block_until and block_until[key_sd] != "EOD" and x["t"] < block_until[key_sd]:
                blocked = True
            if rule == "B" and key_d in block_until and x["t"] < block_until[key_d]:
                blocked = True
            if rule.startswith("C"):
                cap = float(rule[1:])
                if cum <= -cap:
                    blocked = True
            if blocked:
                removed.append(x)
                continue
            kept += x["usd"]; cum += x["usd"]
            is_loss = x["usd"] <= LOSS_THRESH
            if is_loss:
                consec[key_sd] += 1; consec_dir[key_d] += 1
            else:
                consec[key_sd] = 0; consec_dir[key_d] = 0
            if rule == "A1" and consec[key_sd] >= 2:
                block_until[key_sd] = "EOD"
            if rule == "A2" and consec[key_sd] >= 2:
                block_until[key_sd] = x["t"] + timedelta(minutes=120)
            if rule == "B" and consec_dir[key_d] >= 3:
                block_until[key_d] = x["t"] + timedelta(minutes=120)
    return kept, removed

for era_name, era_start in (("FULL Apr1-Jun3", None), ("POST-V16 May18+", date(2026, 5, 18))):
    base = sum(x["usd"] for d, xs in by_day.items() if not era_start or d >= era_start for x in xs)
    n = sum(len(xs) for d, xs in by_day.items() if not era_start or d >= era_start)
    print(f"\n================ {era_name}: n={n}, baseline ${base:+.2f} ================")
    for rule in ("A1", "A2", "B", "C200", "C250", "C300"):
        kept, removed = replay(rule, era_start)
        rem_usd = sum(x["usd"] for x in removed)
        rem_w = sum(1 for x in removed if x["usd"] > 0.5)
        print(f"  {rule:4s}: kept ${kept:+9.2f}  (delta {kept-base:+8.2f})  removed {len(removed):3d} trades (${rem_usd:+8.2f}, {rem_w} winners)")

# what happens AFTER 2 consecutive same setup+dir losses historically? (the 3rd trade)
print("\n=== The '3rd trade' after 2 consecutive same setup+dir losses (no rule) ===")
third = []
for d, xs in sorted(by_day.items()):
    consec = defaultdict(int)
    for x in sorted(xs, key=lambda z: z["t"]):
        key = (x["name"], x["dir"])
        if consec[key] >= 2:
            third.append(x)
        if x["usd"] <= LOSS_THRESH:
            consec[key] += 1
        else:
            consec[key] = 0
w = sum(1 for x in third if x["usd"] > 0.5)
l = sum(1 for x in third if x["usd"] < -0.5)
print(f"n={len(third)}  W{w}/L{l}  total ${sum(x['usd'] for x in third):+.2f}  avg ${sum(x['usd'] for x in third)/max(len(third),1):+.2f}")
for x in third:
    print(f"  {x['d']} {x['t'].strftime('%H:%M')} {x['name']:15s} {x['dir']} {x['para']} -> ${x['usd']:+.2f}")

# SC longs by paradigm (placed)
print("\n=== Skew Charm LONGS by paradigm (placed, Apr 1+) ===")
agg = defaultdict(list)
for x in trades:
    if x["name"] == "Skew Charm" and x["dir"] == "L":
        agg[(x["para"] or "?").split("-")[0]].append(x["usd"])
for p, xs in sorted(agg.items()):
    w = sum(1 for u in xs if u > 0.5); l = sum(1 for u in xs if u < -0.5)
    print(f"  {p:8s} n={len(xs):3d}  W{w}/L{l}  total ${sum(xs):+8.2f}  avg ${sum(xs)/len(xs):+.2f}")
c.close()
