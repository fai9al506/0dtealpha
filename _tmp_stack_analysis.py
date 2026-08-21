import os, psycopg2
from collections import defaultdict
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True
cur = c.cursor()

# V16 placed universe (live_pass = the stamped portal V16 recall)
cur.execute("""
  SELECT id, ts, setup_name, direction, outcome_pnl, mes_sim_outcome_pnl,
         outcome_result, outcome_elapsed_min, basket_pct
  FROM setup_log
  WHERE live_pass = true AND outcome_elapsed_min IS NOT NULL
  ORDER BY ts
""")
rows = cur.fetchall()

def norm(d):
    return "long" if d in ("long", "bullish") else "short"

# group by (ET date, direction)
byday = defaultdict(list)
for r in rows:
    id_, ts, nm, d, op, mp, res, elapsed, bp = r
    t = ts.astimezone(ET)
    byday[(t.date(), norm(d))].append({
        "id": id_, "ts": t, "nm": nm, "dir": norm(d),
        "chain": float(op) if op is not None else 0.0,
        "mes": float(mp) if mp is not None else None,
        "res": res, "open_min": elapsed or 0, "bp": bp,
    })

# compute stack depth: how many same-dir trades already open at this entry
for key, lst in byday.items():
    lst.sort(key=lambda x: x["ts"])
    for i, tr in enumerate(lst):
        depth = 1
        for prev in lst[:i]:
            close = prev["ts"].timestamp() + prev["open_min"] * 60
            if prev["ts"].timestamp() <= tr["ts"].timestamp() < close:
                depth += 1
        tr["depth"] = depth

all_tr = [tr for lst in byday.values() for tr in lst]

def bucket(trs, label, mes_only=False):
    agg = defaultdict(lambda: {"n":0,"w":0,"chain":0.0,"mes":0.0,"mesn":0})
    for tr in trs:
        dk = tr["depth"] if tr["depth"] < 3 else 3   # 1,2,3+(=3)
        a = agg[dk]
        a["n"] += 1
        if tr["res"] in ("WIN",): a["w"] += 1
        a["chain"] += tr["chain"]
        if tr["mes"] is not None:
            a["mes"] += tr["mes"]; a["mesn"] += 1
    print(f"\n=== {label} ===")
    print(f"  {'depth':6} {'n':>4} {'WR':>6} {'chainSum':>9} {'chainAvg':>9} {'mesSum':>8} {'mesAvg':>8} {'mesN':>5}")
    for dk in sorted(agg):
        a = agg[dk]
        wr = 100*a["w"]/a["n"] if a["n"] else 0
        cavg = a["chain"]/a["n"] if a["n"] else 0
        mavg = a["mes"]/a["mesn"] if a["mesn"] else 0
        dlab = "3+" if dk==3 else str(dk)
        print(f"  {dlab:6} {a['n']:>4} {wr:>5.0f}% {a['chain']:>9.1f} {cavg:>9.2f} {a['mes']:>8.1f} {mavg:>8.2f} {a['mesn']:>5}")

print(f"V16 placed universe: {len(all_tr)} trades, "
      f"{rows[0][1].astimezone(ET).date()} -> {rows[-1][1].astimezone(ET).date()}")
bucket(all_tr, "ALL ERAS (chain all; mes Apr15+)")

# Era split (mes_sim only meaningful Apr15+)
apr_jun = [tr for tr in all_tr if tr["ts"].date().isoformat() >= "2026-04-15"]
bucket(apr_jun, "Apr15 -> Jun18 (mes-sim era)")

june = [tr for tr in all_tr if tr["ts"].strftime("%Y-%m") == "2026-06"]
bucket(june, "June only")

# The specific question: 3+ stacked trades, listed
print("\n=== EVERY 3+ STACKED TRADE (depth>=3), Apr15+ ===")
deep = sorted([tr for tr in apr_jun if tr["depth"] >= 3], key=lambda x: x["ts"])
print(f"  {'date':11} {'time':6} {'setup':14} {'dir':5} {'depth':>5} {'chain':>7} {'mes':>7} {'res':>6}")
sc=sm=0; smn=0
for tr in deep:
    mes = f"{tr['mes']:.1f}" if tr["mes"] is not None else "  -"
    sc += tr["chain"]
    if tr["mes"] is not None: sm += tr["mes"]; smn+=1
    print(f"  {tr['ts'].date()} {tr['ts'].strftime('%H:%M')} {tr['nm'][:14]:14} {tr['dir']:5} {tr['depth']:>5} {tr['chain']:>7.1f} {mes:>7} {tr['res'] or '-':>6}")
print(f"  --- {len(deep)} trades: chainSum={sc:.1f}  mesSum={sm:.1f} (n={smn})")
