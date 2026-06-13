import os
from sqlalchemy import create_engine, text
from app.live_filter import backfill_live_pass
eng = create_engine(os.environ["DATABASE_URL"].replace("postgresql://","postgresql+psycopg://"))
n = backfill_live_pass(eng)
print("re-stamped live_pass=true count:", n)
from datetime import date
with eng.connect() as c:
    for lid in (3930,3935,3940,3905):
        r=c.execute(text("SELECT setup_name,direction,grade,live_pass FROM setup_log WHERE id=:i"),{"i":lid}).fetchone()
        print(f"  lid {lid}: {r[0]} {r[1]} {r[2]} -> live_pass={r[3]}")
    cnt=c.execute(text("SELECT count(*),round(sum(outcome_pnl)::numeric,1) FROM setup_log WHERE ts::date=:d AND live_pass=true"),{"d":date(2026,6,11)}).fetchone()
    print(f"  TODAY V16 dropdown now: {cnt[0]} trades, sum {cnt[1]}pt")
    # VPB count change
    vpb=c.execute(text("SELECT count(*) FROM setup_log WHERE setup_name='Vanna Pivot Bounce' AND live_pass=true")).scalar()
    gl=c.execute(text("SELECT count(*) FROM setup_log WHERE setup_name='GEX Long' AND live_pass=true")).scalar()
    vd=c.execute(text("SELECT count(*) FROM setup_log WHERE setup_name='VIX Divergence' AND live_pass=true")).scalar()
    print(f"  now-included all-time: VPB={vpb} GEX Long={gl} VIX Div={vd}")
