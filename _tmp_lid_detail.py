import os, psycopg, json
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
# all columns of setup_log
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' ORDER BY ordinal_position")
allcols=[r[0] for r in cur.fetchall()]
print("setup_log columns:", allcols)
lids=[3900,3905,3926,3930,3935,3940]
cur.execute(f"SELECT * FROM setup_log WHERE id = ANY(%s) ORDER BY id",(lids,))
for row in cur.fetchall():
    rec=dict(zip(allcols,row))
    print("\n"+"="*80)
    print(f"lid {rec['id']} {rec['setup_name']} {rec.get('direction')} grade={rec.get('grade')}")
    for k in ['ts','setup_name','direction','grade','paradigm','greek_alignment','alignment','vix','overvix','live_pass','live_filter_ver','real_trade_skip_reason','outcome_result','outcome_pnl','mes_sim_outcome_pnl','gap','gap_pts','es_price']:
        if k in rec: print(f"   {k} = {rec[k]}")
