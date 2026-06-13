"""Raw per-strike GEX above spot at ~9:52 today, BOTH sources, to settle the
CORE_R3 question. User says all gamma above 7600 was positive, largest at 7620."""
import psycopg2, json

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
cur = psycopg2.connect(DB).cursor()

# ---- Source A: VOLLAND gamma TODAY (what gex_long_v3._features uses) ----
cur.execute("""SELECT ts_utc FROM volland_exposure_points
               WHERE greek='gamma' AND expiration_option='TODAY'
                 AND (ts_utc AT TIME ZONE 'America/New_York')::date='2026-06-02'
                 AND (ts_utc AT TIME ZONE 'America/New_York')::time BETWEEN '09:50' AND '09:54'
               ORDER BY ts_utc LIMIT 1""")
r = cur.fetchone()
print("=== Source A: VOLLAND gamma TODAY @", r[0] if r else None, "===")
if r:
    cur.execute("""SELECT strike, value FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='gamma' AND expiration_option='TODAY'
                     AND strike BETWEEN 7595 AND 7645 ORDER BY strike""", (r[0],))
    for s, v in cur.fetchall():
        print(f"   {s:8.0f}  {v:+15.1f}  {'POS' if v>0 else 'NEG'}")

# Also THIS_WEEK gamma (Volland's default chart is often this-week/all)
cur.execute("""SELECT ts_utc FROM volland_exposure_points
               WHERE greek='gamma' AND expiration_option='THIS_WEEK'
                 AND (ts_utc AT TIME ZONE 'America/New_York')::date='2026-06-02'
                 AND (ts_utc AT TIME ZONE 'America/New_York')::time BETWEEN '09:50' AND '09:54'
               ORDER BY ts_utc LIMIT 1""")
r2 = cur.fetchone()
print("\n=== Source A2: VOLLAND gamma THIS_WEEK @", r2[0] if r2 else None, "===")
if r2:
    cur.execute("""SELECT strike, value FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='gamma' AND expiration_option='THIS_WEEK'
                     AND strike BETWEEN 7595 AND 7645 ORDER BY strike""", (r2[0],))
    for s, v in cur.fetchall():
        print(f"   {s:8.0f}  {v:+15.1f}  {'POS' if v>0 else 'NEG'}")

# ---- Source B: TS chain GEX (what live _gex_long_v3_features uses) ----
cur.execute("""SELECT ts, spot, columns, rows FROM chain_snapshots
               WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-02'
                 AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:50' AND '09:54'
               ORDER BY ts LIMIT 1""")
row = cur.fetchone()
print("\n=== Source B: TS chain GEX (C_Gamma*C_OI - P_Gamma*P_OI) @", row[0] if row else None, "===")
if row:
    ts, spot, cols, rows = row
    cols = cols if isinstance(cols, list) else json.loads(cols)
    rows = rows if isinstance(rows, list) else json.loads(rows)
    print(f"   spot={spot}")
    # Mirrored layout: call cols [0..9], Strike=10, put cols [11..20]
    iS = 10
    iCOI, iCG = 1, 3      # call Open Int, call Gamma (left side)
    iPG, iPOI = 17, 19    # put Gamma, put Open Int (right side)
    print(f"   using idx: Strike={iS} C_Gamma={iCG} C_OI={iCOI} P_Gamma={iPG} P_OI={iPOI}")
    out = []
    for rr in rows:
        try:
            s = float(rr[iS])
        except Exception:
            continue
        if not (7595 <= s <= 7645):
            continue
        cg = float(rr[iCG] or 0); coi = float(rr[iCOI] or 0)
        pg = float(rr[iPG] or 0); poi = float(rr[iPOI] or 0)
        net = cg*coi - pg*poi
        out.append((s, cg*coi*100, -pg*poi*100, net*100))
    for s, cgex, pgex, net in sorted(out):
        print(f"   {s:8.0f}  call_gex={cgex:+13.0f}  put_gex={pgex:+13.0f}  NET={net:+13.0f}  {'POS' if net>0 else 'NEG'}")
