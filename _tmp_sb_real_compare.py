import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
cur=c.cursor()
DEAD=0.15
WL = ('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption')

# direction column?
cols=[r[0] for r in cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='setup_log'") or cur.fetchall()]
print("has direction col:", 'direction' in cols)

def blocks(bp, direction):
    if bp is None: return None  # fail-open (no data) -> taken, excluded from confirm/block split
    bp=float(bp); is_long = direction in ('long','bullish')
    if abs(bp)<DEAD: return True          # neutral -> skip
    return (bp>0)!=is_long                 # contradict -> skip ; confirm -> take

def window(label, start, end):
    cur.execute("""
      SELECT direction, basket_pct,
             COALESCE(mes_sim_outcome_pnl, outcome_pnl) AS pnl
      FROM setup_log
      WHERE ts >= %s AND ts < %s
        AND setup_name = ANY(%s)
        AND COALESCE(mes_sim_outcome_pnl, outcome_pnl) IS NOT NULL
    """, (start, end, list(WL)))
    rows=cur.fetchall()
    conf=[]; blok=[]; nodata=[]
    for d,bp,pnl in rows:
        pnl=float(pnl)
        b=blocks(bp,d)
        if b is None: nodata.append(pnl)
        elif b: blok.append(pnl)
        else: conf.append(pnl)
    def stat(x):
        if not x: return "n=0"
        w=sum(1 for v in x if v>0)
        return f"n={len(x):3d}  net={sum(x):+8.1f}p  WR={100*w/len(x):4.0f}%  avg={sum(x)/len(x):+.2f}"
    base = conf+blok+nodata
    print(f"\n--- {label} ({start} -> {end}) ---  whitelist signals w/ outcome")
    print(f"  BASELINE (take all)         : {stat(base)}")
    print(f"  V16-SB CONFIRM (taken)      : {stat(conf)}")
    print(f"  SB BLOCKED (neutral+contra) : {stat(blok)}   <- what SB removes")
    print(f"  no-basket-data (fail-open)  : {stat(nodata)}")

window("FULL SB ERA",   '2026-03-16','2026-06-20')
window("JUNE",          '2026-06-01','2026-06-20')
window("LAST WEEK",     '2026-06-15','2026-06-20')
c.close()
