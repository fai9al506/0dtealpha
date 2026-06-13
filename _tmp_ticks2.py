import os, psycopg
from datetime import datetime
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York"); UTC=ZoneInfo("UTC")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='vps_es_range_bars' ORDER BY ordinal_position")
cols=[r[0] for r in cur.fetchall()]; print("cols:",cols)
# pick OHLC-ish columns
def pick(*cands):
    for c in cands:
        if c in cols: return c
    return None
o=pick('bar_open','open_px','o','open'); h=pick('bar_high','high_px','h','high'); l=pick('bar_low','low_px','l','low'); c=pick('bar_close','close_px','c','close'); tcol=pick('ts_start','ts','bar_ts')
print("using",tcol,o,h,l,c)
def bars(lid,h1,m1,h2,m2,entry,stop,direction):
    s=datetime(2026,6,11,h1,m1,tzinfo=ET).astimezone(UTC); e=datetime(2026,6,11,h2,m2,tzinfo=ET).astimezone(UTC)
    cur.execute(f"SELECT {tcol},{o},{h},{l},{c} FROM vps_es_range_bars WHERE range_pts=5 AND {tcol}>=%s AND {tcol}<=%s ORDER BY {tcol}",(s,e))
    print(f"\n=== lid {lid} {direction} entry={entry} stop={stop} ({h1:02d}:{m1:02d}-{h2:02d}:{m2:02d} ET) ===")
    short = direction=='short'
    for r in cur.fetchall():
        t=r[0].astimezone(ET).strftime("%H:%M:%S")
        hi,lo=float(r[2]),float(r[3])
        # for short: adverse = high (toward stop above); fav = low
        hit = "  <-- STOP HIT" if (short and hi>=stop) else ""
        print(f"   {t}  O {float(r[1]):7.2f} H {hi:7.2f} L {lo:7.2f} C {float(r[4]):7.2f}{hit}")
bars(3900,12,7,12,40,7306.75,7320.75,'short')
bars(3905,12,38,13,5,7310.75,None,'short')
bars(3926,13,38,13,45,7359.25,7373.75,'short')
