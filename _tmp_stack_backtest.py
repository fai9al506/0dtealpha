"""Confluence/stacking backtest: when multiple DISTINCT setup families fire same-direction
within a look-back window, do the tradeable signals perform better? Validation Protocol.
Look-back only (no lookahead) = causally valid for live sizing."""
import os, psycopg2
from collections import defaultdict
from datetime import timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MES = 5.0

FAMILY = {
    "Skew Charm": "SC", "AG Short": "AG", "DD Exhaustion": "DD",
    "VIX Divergence": "VIX", "VIX Compression": "VIX", "IV Momentum": "VIX",
    "ES Absorption": "ABS", "SB Absorption": "ABS", "SB10 Absorption": "ABS",
    "SB2 Absorption": "ABS", "Delta Absorption": "ABS",
    "GEX Long": "GEX", "GEX Velocity": "GEX",
    "Vanna Pivot Bounce": "VANNA", "Vanna Butterfly": "VANNA",
    "BofA Scalp": "BOFA", "Paradigm Reversal": "PARA", "Dip-Buy": "DIP",
}
# tradeable universe we EVALUATE (what V16 actually places)
WHITELIST = {"Skew Charm", "AG Short", "Vanna Pivot Bounce",
             "VIX Divergence", "ES Absorption", "DD Exhaustion"}

def ndir(d):
    return "long" if (d or "").lower() in ("long", "bullish") else "short"

c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""SELECT id, (ts AT TIME ZONE 'America/New_York') AS et, setup_name,
                      direction, outcome_pnl, real_trade_skip_reason
               FROM setup_log
               WHERE outcome_pnl IS NOT NULL
               ORDER BY ts""")
rows = []
for lid, et, name, d, pnl, skip in cur.fetchall():
    rows.append(dict(lid=lid, et=et, date=et.date(), name=name, fam=FAMILY.get(name),
                     dir=ndir(d), pnl=float(pnl), skip=skip))

def confluence(rows, W_min):
    """Tag each row with #distinct families same-dir in prior W minutes (same date)."""
    by_date = defaultdict(list)
    for r in rows:
        by_date[r["date"]].append(r)
    for d, day in by_date.items():
        day.sort(key=lambda x: x["et"])
        for i, r in enumerate(day):
            fams = set()
            if r["fam"]:
                fams.add(r["fam"])
            lo = r["et"] - timedelta(minutes=W_min)
            for j in range(i):
                p = day[j]
                if p["et"] >= lo and p["dir"] == r["dir"] and p["fam"]:
                    fams.add(p["fam"])
            r["conf"] = len(fams)

def stats(sub):
    if not sub: return None
    pnls = [r["pnl"] for r in sub]
    n = len(pnls); wins = sum(1 for p in pnls if p > 0); tot = sum(pnls)
    days = len(set(r["date"] for r in sub))
    return dict(n=n, wr=100*wins/n, tot=tot, avg=tot/n, days=days)

def show(tag, sub):
    s = stats(sub)
    if not s: print(f"    {tag:16s} (0)"); return
    print(f"    {tag:16s} n={s['n']:4d}  WR={s['wr']:5.1f}%  avg={s['avg']:+5.2f}p"
          f"  tot={s['tot']:+8.1f}p (${s['tot']*MES:+7.0f})  days={s['days']}")

def report(label, pool):
    # pool already filtered to evaluated/era. Bucket by confluence.
    solo = [r for r in pool if r["conf"] == 1]
    two = [r for r in pool if r["conf"] == 2]
    three = [r for r in pool if r["conf"] >= 3]
    stk = [r for r in pool if r["conf"] >= 2]
    print(f"\n  -- {label} --")
    show("conf=1 (solo)", solo)
    show("conf=2", two)
    show("conf>=3", three)
    show("conf>=2 (stacked)", stk)

for W in (15, 30):
    confluence(rows, W)
    print(f"\n================= WINDOW = {W} min (look-back) =================")
    evaluated = [r for r in rows if r["name"] in WHITELIST]
    report(f"ALL HISTORY  whitelist", evaluated)
    report(f"POST-V16 (>=2026-05-18)", [r for r in evaluated if str(r["date"]) >= "2026-05-18"])
    report(f"POST-V16 + skip IS NULL (placed)",
           [r for r in evaluated if str(r["date"]) >= "2026-05-18" and r["skip"] is None])

# Gate2 cross-check
cur.execute("""SELECT count(*), COALESCE(SUM(outcome_pnl),0) FROM setup_log
               WHERE outcome_pnl IS NOT NULL AND setup_name IN %s""",
            (tuple(WHITELIST),))
dbn, dbsum = cur.fetchone()
mysum = sum(r["pnl"] for r in rows if r["name"] in WHITELIST)
myn = sum(1 for r in rows if r["name"] in WHITELIST)
print(f"\n[GATE2] DB whitelist n={dbn} sum={float(dbsum):+.1f} | computed n={myn} sum={mysum:+.1f}"
      f" -> {'MATCH' if dbn==myn and abs(float(dbsum)-mysum)<1 else 'MISMATCH'}")
cur.close(); c.close()
