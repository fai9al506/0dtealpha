"""Vol-regime forward-tracking review (S216, 2026-06-11).

Re-runs the cage/VIX vs MES-sim analysis on accumulating real data so we get a
clean OUT-OF-SAMPLE read before sizing real money on the vol-regime signal.

WHY: chain-sim (portal outcome_pnl) overstates P&L on high-vol days because its
smooth SPX 30s path never gets whipsawed by the wicks that stop real MES fills.
On the realistic MES-sim baseline, high-VIX (>=19) and wide-cage (>90) days are
net LOSERS that the chain-sim baseline HID. Judge execution-sensitive rules on
MES-sim, never chain-sim. See memory research_chainsim_baseline_flaw_vol_regime.md.

Data collection is AUTOMATIC (chain_snapshots = cage+VIX; mes_sim_outcome_pnl
populated live for V14-whitelist trades). Just run this to refresh the read:

    python vol_regime_review.py [START_DATE=2026-04-15]

Decision gate (S216): once ~15-20 NEW high-vol days have accrued past 2026-06-11,
check whether wide-cage / VIX>=19 days STAY net-negative on MES-sim out-of-sample.
If yes -> build a vol-regime size-down (drawdown control). If they flip -> drop it.
"""
import os, sys, json, statistics
from collections import defaultdict
from datetime import date
import psycopg


def walls(rows, spot):
    lv = []
    for r in rows:
        try:
            lv.append((float(r[10]), ((r[1] or 0) - (r[19] or 0)) * (r[3] or 0) * 100))
        except Exception:
            pass
    ab = [(s, g) for s, g in lv if s > spot]
    be = [(s, g) for s, g in lv if s < spot]
    res = max(ab, key=lambda x: x[1], default=None)
    sup = min(be, key=lambda x: x[1], default=None)
    return (res[0] - sup[0]) if (res and sup and res[1] > 0 and sup[1] < 0) else None


def main(start="2026-04-15"):
    conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
    cur = conn.cursor()
    # daily chain & MES-sim net (only live_pass trades that HAVE mes_sim = apples-to-apples)
    cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d,
        sum(outcome_pnl) chain, sum(mes_sim_outcome_pnl) mes
        FROM setup_log WHERE live_pass=true AND mes_sim_outcome_pnl IS NOT NULL
          AND ts::date >= %s GROUP BY 1""", (start,))
    pnl = {d: (float(c), float(m)) for d, c, m in cur.fetchall()}
    # cage width per day (~10:00 ET chain)
    cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, spot, rows FROM (
        SELECT ts,spot,rows,row_number() OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York')
          ORDER BY abs(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'America/New_York')::time - TIME '10:00:00'))) rn
        FROM chain_snapshots WHERE ts::date >= %s AND spot IS NOT NULL AND rows IS NOT NULL) q
        WHERE rn=1""", (start,))
    cage = {}
    for d, spot, rows in cur.fetchall():
        rows = rows if isinstance(rows, list) else json.loads(rows)
        cage[d] = walls(rows, float(spot))
    # VIX at open
    cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, (array_agg(vix ORDER BY ts))[1]
        FROM chain_snapshots WHERE ts::date >= %s AND spot IS NOT NULL GROUP BY 1""", (start,))
    vix = {d: (float(v) if v else None) for d, v in cur.fetchall()}

    days = sorted(d for d in pnl if d in cage and cage[d] is not None)
    print(f"vol-regime review: {start} -> {days[-1] if days else '?'}  n={len(days)} days (MES-sim baseline)\n")

    def grp(name, pred):
        g = [d for d in days if pred(d)]
        if not g:
            print(f"  {name:<18} n=0"); return
        m = sum(pnl[d][1] for d in g) / len(g)
        c = sum(pnl[d][0] for d in g) / len(g)
        print(f"  {name:<18} n={len(g):>2}  MES {m:+5.1f}p/d   chain {c:+5.1f}p/d   (chain overstates {c-m:+4.1f})")

    print("CAGE (10am gamma walls):")
    grp("narrow <=90", lambda d: cage[d] <= 90)
    grp("wide >90", lambda d: cage[d] > 90)
    print("VIX @ open:")
    grp("VIX<19", lambda d: (vix.get(d) or 0) < 19)
    grp("VIX>=19", lambda d: (vix.get(d) or 0) >= 19)

    # sizing variants on MES-sim
    def stats(series):
        tot = sum(series); peak = eq = dd = 0
        for v in series:
            eq += v; peak = max(peak, eq); dd = min(dd, eq - peak)
        return tot, dd
    base = [pnl[d][1] for d in days]
    half = [pnl[d][1] * (0.5 if cage[d] > 90 else 1.0) for d in days]
    skip = [pnl[d][1] * (0.0 if cage[d] > 90 else 1.0) for d in days]
    print("\nCAGE SIZING (MES-sim):")
    for nm, s in [("baseline 1x", base), ("wide 0.5x", half), ("wide 0x skip", skip)]:
        t, dd = stats(s)
        print(f"  {nm:<14} tot {t:+6.0f}p (${t*5:+6.0f})  maxDD {dd:+5.0f}p")
    print("\nDECISION GATE: are wide-cage / VIX>=19 STILL net-negative on the NEW days "
          "past 2026-06-11? If yes -> size-down for drawdown control. If flipped -> drop.")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2026-04-15")
