import os, psycopg, json
from datetime import date
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
cur.execute("""SELECT id,ts,direction,grade,paradigm,greek_alignment,
   outcome_result,outcome_pnl,real_trade_skip_reason,live_pass,abs_details
   FROM setup_log WHERE ts::date=%s AND setup_name='ES Absorption' ORDER BY ts""",(date(2026,6,11),))
print(f"{'lid':>4} {'ET':>5} {'dir':<8}{'gr':<3}{'para':<10}{'algn':>4} {'outcome':<8}{'skip':<18}{'live_pass'}")
for r in cur.fetchall():
    lid,ts,dirn,grade,para,algn,res,pnl,skip,lp,absd=r
    t=ts.astimezone(ET).strftime("%H:%M")
    placed = "PLACED" if skip is None else ""
    print(f"{lid:>4} {t:>5} {str(dirn):<8}{str(grade):<3}{str(para):<10}{str(algn):>4} {str(res):<8}{str(skip):<18}{lp} {placed}")
    # check abs_details for align_at_trigger if present
    if absd:
        d = absd if isinstance(absd,dict) else json.loads(absd)
        for k in ('align_at_trigger','greek_alignment','log_only'):
            if k in d: print(f"        abs_details.{k}={d[k]}")
