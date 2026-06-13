# AG Short x vanna tenor study (2026-06-07): net vanna (sum of per-strike values)
# at signal time, per tenor (TODAY/THIS_WEEK/THIRTY_NEXT_DAYS/ALL), vs outcome.
# Hypothesis (Apollo): negative vanna = vol-up -> dealers sell -> good for shorts.
import json
import sqlalchemy as sa

e = sa.create_engine(
    "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway",
    isolation_level="AUTOCOMMIT")  # DB discipline: no long-held transaction (2026-06-03 outage rule)

trades = json.load(open("_tmp_ag_regime_data.json"))

VQ = """
SELECT expiration_option, sum(value) AS net
FROM volland_exposure_points
WHERE greek = 'vanna'
  AND ts_utc BETWEEN (:t)::timestamptz - interval '12 minutes' AND (:t)::timestamptz
  AND ts_utc = (
      SELECT max(ts_utc) FROM volland_exposure_points
      WHERE greek='vanna'
        AND ts_utc BETWEEN (:t)::timestamptz - interval '12 minutes' AND (:t)::timestamptz
  )
GROUP BY 1
"""

# trade ts in _tmp_ag_regime_data.json is ET naive string; need UTC. Re-pull ids->utc ts.
with e.connect() as c:
    rows = c.execute(sa.text(
        "SELECT id, ts FROM setup_log WHERE setup_name='AG Short' AND grade != 'LOG' AND outcome_result IS NOT NULL"
    )).fetchall()
ts_map = {r[0]: r[1] for r in rows}

import sys
out = []
with e.connect() as c:
    for i, t in enumerate(trades):
        ts = ts_map.get(t["id"])
        if ts is None:
            continue
        vr = c.execute(sa.text(VQ), {"t": ts.isoformat()}).fetchall()
        v = {r[0]: float(r[1]) for r in vr}
        t["v_today"] = v.get("TODAY")
        t["v_week"] = v.get("THIS_WEEK")
        t["v_month"] = v.get("THIRTY_NEXT_DAYS")
        t["v_all"] = v.get("ALL")
        out.append(t)
        if (i+1) % 25 == 0:
            print(f"  ...{i+1}/{len(trades)}", flush=True)

json.dump(out, open("_tmp_ag_vanna_data.json", "w"))
have = sum(1 for t in out if t.get("v_all") is not None)
print(f"vanna found for {have}/{len(out)} trades")

# Gate 2 cross-check: sign of computed v_all vs setup_log.vanna_all (stored at signal)
match = mismatch = 0
for t in out:
    sl_v = t.get("svb")  # not vanna; skip
for t in out:
    pass
with e.connect() as c:
    rows = c.execute(sa.text(
        "SELECT id, vanna_all, vanna_weekly, vanna_monthly FROM setup_log WHERE setup_name='AG Short' AND vanna_all IS NOT NULL"
    )).fetchall()
slv = {r[0]: (float(r[1]), float(r[2]), float(r[3])) for r in rows}
for t in out:
    if t["id"] in slv and t.get("v_all") is not None:
        a = slv[t["id"]][0]
        if a == 0:
            continue
        if (a > 0) == (t["v_all"] > 0):
            match += 1
        else:
            mismatch += 1
print(f"sign check computed v_all vs setup_log.vanna_all: match={match} mismatch={mismatch}")

def seg(label, items):
    n = len(items)
    if n == 0:
        print(f"  {label:<30} n=0")
        return
    w = sum(1 for x in items if x["res"] == "WIN")
    l = sum(1 for x in items if x["res"] == "LOSS")
    pnl = sum(x["pnl"] for x in items if x["pnl"] is not None)
    print(f"  {label:<30} n={n:>3}  WR={100*w/max(1,w+l):>3.0f}%  pnl={pnl:>+7.1f}  avg={pnl/n:>+5.2f}")

for key, name in [("v_today", "VANNA TODAY (0DTE)"), ("v_week", "VANNA THIS_WEEK"),
                  ("v_month", "VANNA 30D (monthly)"), ("v_all", "VANNA ALL")]:
    print(f"\n=== {name} ===")
    pos = [t for t in out if t.get(key) is not None and t[key] > 0]
    neg = [t for t in out if t.get(key) is not None and t[key] < 0]
    seg("positive", pos)
    seg("negative", neg)
    # magnitude terciles among non-null
    nn = sorted([t for t in out if t.get(key) is not None], key=lambda x: x[key])
    if len(nn) >= 9:
        k = len(nn)//3
        seg("most negative tercile", nn[:k])
        seg("middle tercile", nn[k:2*k])
        seg("most positive tercile", nn[2*k:])

print("\n=== INTERACTION WITH VIX GATE ===")
hi = [t for t in out if t.get("vix") and t["vix"] >= 20]
lo = [t for t in out if t.get("vix") and t["vix"] < 20]
for key, name in [("v_today", "0DTE"), ("v_week", "week"), ("v_month", "month"), ("v_all", "ALL")]:
    print(f" -- {name} --")
    seg(f"VIX>=20 & vanna_{name} neg", [t for t in hi if t.get(key) is not None and t[key] < 0])
    seg(f"VIX>=20 & vanna_{name} pos", [t for t in hi if t.get(key) is not None and t[key] > 0])
    seg(f"VIX<20  & vanna_{name} neg", [t for t in lo if t.get(key) is not None and t[key] < 0])
    seg(f"VIX<20  & vanna_{name} pos", [t for t in lo if t.get(key) is not None and t[key] > 0])

print("\n=== MAY-JUN ONLY (can vanna rescue AG in low-VIX?) ===")
mj = [t for t in out if t["et"][:7] >= "2026-05"]
for key, name in [("v_today", "0DTE"), ("v_week", "week"), ("v_month", "month"), ("v_all", "ALL")]:
    seg(f"May-Jun vanna_{name} neg", [t for t in mj if t.get(key) is not None and t[key] < 0])
    seg(f"May-Jun vanna_{name} pos", [t for t in mj if t.get(key) is not None and t[key] > 0])
