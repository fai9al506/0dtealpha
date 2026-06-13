"""TEST: can a dominant negative DD floor 'promote' basis GEX Long trades that
v3.1 rejects into winners?  Reuses canonical app/gex_long_v3.py classifier + exit
sim so results match the portal's trusted v3.1 view exactly.

For EVERY logged GEX Long signal (grade != LOG):
  - v3.1 verdict + pass  (app.gex_long_v3)
  - v3.1-simulated outcome (SL14 / magnet target / trail15-5) computed for ALL
    trades (even rejected) so promotion is apples-to-apples
  - DD floor feature at entry (dominant negative deltaDecay strike vs spot)

Buckets:
  A. v3.1 PASS (trusted set)               -> the baseline we trust
  C. v3.1 REJECT & DD-support               -> the 'promote via DD' candidate
  D. v3.1 REJECT & NO DD-support
"""
import os, sys, psycopg2
from datetime import date
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import gex_long_v3 as g

DD_RATIO_MIN = 2.0
DD_NEAR = 15.0     # dominant neg strike within this many pts of spot
DD_VAL_MIN = 1.0e9 # floor must be at least this big (|deltaDecay|)
DD_MAX_ABOVE = 15.0 # spot no more than this far above the strike

c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

def dd_support(t_utc, spot):
    """Return (is_support, dom_strike, dom_val, ratio, dist) at nearest deltaDecay snap."""
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s
                     AND greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   ORDER BY ts_utc DESC LIMIT 1""", (t_utc, t_utc))
    snap = cur.fetchone()
    if not snap: return (None,)*5
    cur.execute("""SELECT strike::float, value::float FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='deltaDecay' AND expiration_option='TODAY'
                     AND ticker='SPX'""", (snap[0],))
    pts = cur.fetchall()
    negs = sorted([(s, v) for s, v in pts if v < 0], key=lambda x: x[1])
    if not negs: return (None,)*5
    ds, dv = negs[0]
    second = abs(negs[1][1]) if len(negs) >= 2 else 1.0
    ratio = abs(dv) / max(second, 1.0)
    dist = float(spot) - ds
    is_sup = (ratio >= DD_RATIO_MIN and abs(dv) >= DD_VAL_MIN
              and 0 < dist <= DD_MAX_ABOVE and abs(ds - float(spot)) <= DD_NEAR)
    return is_sup, ds, dv, ratio, dist

# Pull every GEX Long graded signal
cur.execute("""SELECT id, ts, ts AT TIME ZONE 'America/New_York' as t_et,
                      greek_alignment, spot, grade, paradigm,
                      outcome_result, outcome_pnl
               FROM setup_log
               WHERE setup_name='GEX Long' AND grade!='LOG' AND grade IS NOT NULL
                 AND spot IS NOT NULL
               ORDER BY ts""")
rows = cur.fetchall()
print(f"Total GEX Long graded signals: {len(rows)}\n")

def newstat(): return {"n":0,"w":0,"simpnl":0.0,"dbpnl":0.0,"dbw":0}
def add(st, sim_res, sim_pnl, db_res, db_pnl):
    st["n"]+=1
    if sim_res=="WIN": st["w"]+=1
    if sim_pnl is not None: st["simpnl"]+=sim_pnl
    if db_res=="WIN": st["dbw"]+=1
    if db_pnl is not None: st["dbpnl"]+=float(db_pnl)
def line(name, st):
    if st["n"]==0: return f"  {name:<40} n=0"
    return (f"  {name:<40} n={st['n']:<3} "
            f"v3.1sim WR={100*st['w']/st['n']:>4.0f}% pts={st['simpnl']:>+7.1f}   "
            f"| DBactual WR={100*st['dbw']/st['n']:>4.0f}% pts={st['dbpnl']:>+7.1f}")

A = newstat()             # v3.1 PASS
C = newstat()             # v3.1 REJECT + DD support
D = newstat()             # v3.1 REJECT + no DD support
A_dd = newstat()          # v3.1 PASS + DD support (does DD also confirm trusted set?)
no_feat = 0
today_rows = []

for (lid, t_utc, t_et, al, spot, grade, para, db_res, db_pnl) in rows:
    try:
        f = g._features(cur, t_utc, spot)
    except Exception:
        f = None
    verdict = g._classify(f)
    align = al if al is not None else 0
    hour = t_et.hour if t_et else 99
    v3_pass = (verdict in ('A++','A','B')) and (align >= 0) and (hour < 15)

    # v3.1 exit sim for ALL trades that have features (so rejects are comparable)
    sim_res, sim_pnl = None, None
    if f is not None:
        magnet = f['gex_magnet_strike']
        target = max(magnet or 0, float(spot) + g.TARGET_FLOOR)
        try:
            sim_res, sim_pnl, _mf, _r = g._simulate_exit(cur, t_utc, float(spot), target)
        except Exception:
            pass

    sup, ds, dv, ratio, dist = dd_support(t_utc, spot)
    if sup is None:
        no_feat += 1

    if v3_pass:
        add(A, sim_res, sim_pnl, db_res, db_pnl)
        if sup: add(A_dd, sim_res, sim_pnl, db_res, db_pnl)
    else:
        if sup: add(C, sim_res, sim_pnl, db_res, db_pnl)
        elif sup is False: add(D, sim_res, sim_pnl, db_res, db_pnl)

    if t_et and t_et.date() == date(2026,6,1):
        today_rows.append((lid, t_et.strftime('%H:%M'), grade, para, verdict, v3_pass,
                           db_res, db_pnl, sup, ds, dv, ratio, dist))

print(f"(DD feature missing for {no_feat} trades)\n")
print(f"DD-support def: ratio>={DD_RATIO_MIN}, |val|>={DD_VAL_MIN/1e9}B, spot 0-{DD_MAX_ABOVE}pt above, within {DD_NEAR}pt\n")
print(line("A. v3.1 PASS (trusted set)", A))
print(line("A2. v3.1 PASS & DD-support", A_dd))
print(line("C. v3.1 REJECT & DD-support  <<promote?", C))
print(line("D. v3.1 REJECT & NO DD-support", D))

print("\n=== TODAY 2026-06-01 GEX Long signals ===")
print("  lid   time  grade para         v3.1verdict pass  DBres   DBpnl  DDsup ratio dist  floor")
for (lid, tm, grade, para, verdict, vp, dbr, dbp, sup, ds, dv, ratio, dist) in today_rows:
    dvb = f"{dv/1e9:+.1f}B" if dv is not None else "n/a"
    print(f"  {lid:<5} {tm} {str(grade):<4} {str(para)[:12]:<12} {verdict:<10} {str(vp):<5} "
          f"{str(dbr):<6} {str(dbp):<6} {str(sup):<5} {ratio if ratio else 0:>4.1f} "
          f"{dist if dist else 0:>+5.0f} {dvb}")

cur.close(); c.close()
