import os, psycopg
from datetime import datetime
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York"); UTC=ZoneInfo("UTC")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
def bars(lid,h1,m1,h2,m2,entry,stop,direction,note):
    s=datetime(2026,6,11,h1,m1,tzinfo=ET).astimezone(UTC); e=datetime(2026,6,11,h2,m2,tzinfo=ET).astimezone(UTC)
    cur.execute("SELECT ts_start,bar_open,bar_high,bar_low,bar_close FROM vps_es_range_bars WHERE range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start",(s,e))
    print(f"\n=== lid {lid} {direction} entry={entry} stop={stop} :: {note} ===")
    short = direction=='short'
    best=0.0
    for r in cur.fetchall():
        t=r[0].astimezone(ET).strftime("%H:%M:%S"); hi,lo=float(r[2]),float(r[3])
        fav = (entry-lo) if short else (hi-entry)   # best favorable excursion
        best=max(best,fav)
        hit = "  <== STOP" if (stop and short and hi>=stop) else ("  <== STOP" if (stop and not short and lo<=stop) else "")
        print(f"   {t}  H {hi:7.2f} L {lo:7.2f} C {float(r[4]):7.2f}   fav={fav:+5.1f}{hit}")
    print(f"   >> max favorable excursion on MES = {best:+.1f}pt")
bars(3905,12,38,13,5,7310.75,7304.75,'short','trail-exit +6.75 broker / portal +25.3')
bars(3926,13,38,13,42,7359.25,7373.75,'short','ghost_reconcile -14.75 broker / portal -8')
