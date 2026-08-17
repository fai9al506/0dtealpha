# -*- coding: utf-8 -*-
"""S260 SWEEP — run the ACTUAL portal JS filter against every signal and diff it
against the Python filter that stamps live_pass.

Any disagreement is a mirror bug of the VPB / VIX-Divergence class: TSRT and the
portal view telling different stories about the same trade.

This is a real execution test, not a code read. It extracts _tlPassesStrategy
verbatim from main.py, runs it in Node with the same env flags Railway has, and
compares to live_filter.passes_v16.
"""
import json
import os
import re
import subprocess
import sys
import tempfile

import psycopg2
import psycopg2.extras

REPO = os.path.dirname(os.path.abspath(__file__))
SCR = tempfile.mkdtemp(prefix="filter_sweep_")   # was a hard-coded session scratchpad
sys.path.insert(0, REPO)

# which filter version to sweep: v16 (the live one), v17 or v18 (both monitoring)
STRAT = (sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "v16").lower()
if STRAT not in ("v16", "v16fri", "v17", "v18", "v19"):
    print(f"usage: python filter_mirror_sweep.py [v16|v16fri|v17|v18|v19]   (got {STRAT!r})")
    sys.exit(2)

# env flags exactly as Railway has them
FLAGS = {"_GEX_LONG_REAL": False, "_VIX_DIV_REAL": True, "_VPB_REAL": True,
         "_ES_ABS_REAL": False}   # S277 2026-08-17: ES Absorption parked

# The JS harness gets these as consts, but live_filter.py reads them from the OS at
# call time. Set BOTH from one place or the sweep reports phantom mismatches that are
# really just the operator's shell (this produced 32 fake VPB diffs on 2026-08-15).
_ENV_FOR_FLAG = {"_GEX_LONG_REAL": "GEX_LONG_V3_REAL_TRADE_ENABLED",
                 "_VPB_REAL": "VPB_REAL_TRADE_ENABLED",
                 "_ES_ABS_REAL": "ES_ABS_REAL_TRADE_ENABLED"}
for _f, _envname in _ENV_FOR_FLAG.items():
    os.environ[_envname] = "true" if FLAGS[_f] else "false"
SINCE = "2026-05-19"          # post-V16.1 era

# ---------- 1. extract the portal function verbatim ----------
src = open(f"{REPO}/app/main.py", encoding="utf-8").read().split("\n")
start = next(i for i, l in enumerate(src) if "function _tlPassesStrategy(l, strat) {" in l)
depth = 0
for i in range(start, len(src)):
    depth += src[i].count("{") - src[i].count("}")
    if i > start and depth <= 0:
        end = i
        break
js_fn = "\n".join(src[start:end + 1])
print(f"extracted _tlPassesStrategy: lines {start+1}-{end+1}")

# ---------- 2. pull signals + daily gaps ----------
conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm, greek_alignment, vix, overvix,
           spot, max_plus_gex, max_minus_gex, basket_pct,
           v13_dd_near, v13_gex_above, vanna_cliff_side, vanna_peak_side,
           gex_net_ceiling,
           to_char(ts AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ts, ts AS ts_dt,
           live_pass, outcome_pnl
    FROM setup_log
    WHERE ts::date >= DATE %s
    ORDER BY id""", (SINCE,))
rows = [dict(r) for r in cur.fetchall()]
print(f"sweeping {STRAT.upper()}")
print(f"signals since {SINCE}: {len(rows)}")

cur.execute("""
    WITH closes AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York'))
                      date(ts AT TIME ZONE 'America/New_York') d, spot p
                    FROM chain_snapshots WHERE spot IS NOT NULL
                    ORDER BY 1, ts DESC),
         opens  AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York'))
                      date(ts AT TIME ZONE 'America/New_York') d, spot p
                    FROM chain_snapshots WHERE spot IS NOT NULL
                    ORDER BY 1, ts ASC)
    SELECT o.d::text, (o.p - c.p) gap
    FROM opens o JOIN closes c ON c.d = (SELECT max(d) FROM closes WHERE d < o.d)""")
gaps = {r["d"]: float(r["gap"]) for r in cur.fetchall() if r["gap"] is not None}
print(f"daily gaps: {len(gaps)} days")

cur.execute("SELECT setup_log_id FROM real_trade_orders")
traded = {r["setup_log_id"] for r in cur.fetchall()}

# ---------- 3. run the real JS ----------
for r in rows:
    for k in ("greek_alignment", "vix", "overvix", "spot", "max_plus_gex",
              "max_minus_gex", "basket_pct", "v13_gex_above", "gex_net_ceiling"):
        if r.get(k) is not None:
            r[k] = float(r[k])
    r["outcome_pnl"] = float(r["outcome_pnl"]) if r["outcome_pnl"] is not None else None

harness = f"""
const _tlDailyGaps = {json.dumps(gaps)};
const _GEX_LONG_REAL = {str(FLAGS['_GEX_LONG_REAL']).lower()};
const _VIX_DIV_REAL  = {str(FLAGS['_VIX_DIV_REAL']).lower()};
const _VPB_REAL      = {str(FLAGS['_VPB_REAL']).lower()};
const _ES_ABS_REAL   = {str(FLAGS['_ES_ABS_REAL']).lower()};
const __BASKET_SIZING_MODE__ = 'sizeonly';
{js_fn}
const rows = JSON.parse(require('fs').readFileSync(process.argv[2],'utf8'));
const out = {{}};
for (const l of rows) {{
  try {{ out[l.id] = !!_tlPassesStrategy(l, '{STRAT}'); }}
  catch (e) {{ out[l.id] = 'ERR:' + e.message; }}
}}
console.log(JSON.stringify(out));
"""
open(f"{SCR}/harness.js", "w", encoding="utf-8").write(harness)
open(f"{SCR}/rows.json", "w", encoding="utf-8").write(json.dumps([{k:v for k,v in r.items() if k!="ts_dt"} for r in rows], default=str))
p = subprocess.run(["node", f"{SCR}/harness.js", f"{SCR}/rows.json"],
                   capture_output=True, text=True)
if p.returncode != 0:
    print("NODE FAILED:\n", p.stderr[:3000]); sys.exit(1)
js_res = json.loads(p.stdout)
errs = {k: v for k, v in js_res.items() if isinstance(v, str)}
if errs:
    print(f"JS errors on {len(errs)} rows, e.g. {list(errs.items())[:2]}")

# ---------- 4. Python filter ----------
from app.live_filter import (passes_v16, passes_v16_fri, passes_v17,
                             passes_v18, passes_v19)
_PY = {"v16": passes_v16, "v16fri": passes_v16_fri, "v17": passes_v17,
       "v18": passes_v18, "v19": passes_v19}[STRAT]
py_res = {}
for r in rows:
    row = dict(r)
    row["ts"] = r["ts_dt"]      # the filter wants a real datetime, not the JS string
    try:
        py_res[str(r["id"])] = bool(_PY(row, gaps))
    except Exception as e:
        py_res[str(r["id"])] = f"ERR:{e}"

# ---------- 5. diff ----------
by_setup = {}
mismatches = []
for r in rows:
    i = str(r["id"])
    j, y = js_res.get(i), py_res.get(i)
    if isinstance(j, str) or isinstance(y, str):
        continue
    s = r["setup_name"]
    b = by_setup.setdefault(s, {"n": 0, "js": 0, "py": 0, "diff": 0})
    b["n"] += 1; b["js"] += j; b["py"] += y
    if j != y:
        b["diff"] += 1
        mismatches.append((r["id"], s, r["direction"], r["grade"], j, y,
                           r["id"] in traded, r["outcome_pnl"]))

print("\n=== PORTAL JS vs PYTHON live_filter, per setup ===")
print(f"{'setup':<24}{'signals':>8}{'JS pass':>9}{'PY pass':>9}{'MISMATCH':>10}")
for s, b in sorted(by_setup.items(), key=lambda x: -x[1]["diff"]):
    flag = "  <<<" if b["diff"] else ""
    print(f"{s[:23]:<24}{b['n']:>8}{b['js']:>9}{b['py']:>9}{b['diff']:>10}{flag}")

tot = sum(b["diff"] for b in by_setup.values())
print(f"\nTOTAL MISMATCHES: {tot}")

if mismatches:
    print("\n=== worst offenders (traded ones first) ===")
    mismatches.sort(key=lambda m: (not m[6], m[0]))
    for lid, s, d, g, j, y, t, pnl in mismatches[:30]:
        who = "JS hides it" if (y and not j) else "JS over-admits"
        print(f"  lid {lid:<6}{s[:20]:<22}{d:<7}grade={str(g):<5}"
              f"traded={str(t):<6}pnl={str(pnl):<7}{who}")

if STRAT != "v16":
    # The invariant below only holds for the LIVE filter. V17/V18 are monitoring
    # versions: a traded lid that they REJECT is the whole point of them, not a bug.
    print(f"\n(skipping the traded-lid invariant — {STRAT.upper()} is monitoring-only; "
          f"TSRT places on V16)")
    sys.exit(0 if tot == 0 else 1)

print("\n=== THE INVARIANT: every TRADED lid must pass BOTH ===")
bad = [(r['id'], r['setup_name']) for r in rows
       if r["id"] in traded and (js_res.get(str(r['id'])) is not True
                                 or py_res.get(str(r['id'])) is not True)]
print(f"  traded lids since {SINCE}: {sum(1 for r in rows if r['id'] in traded)}")
print(f"  hidden by one of the filters: {len(bad)}")
for lid, s in bad[:20]:
    print(f"    lid {lid} {s}  js={js_res.get(str(lid))} py={py_res.get(str(lid))}")
