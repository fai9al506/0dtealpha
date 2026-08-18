import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
cur=c.cursor()
DEAD=0.15
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption')

def sb_confirm(bp, d):
    if bp is None: return True  # fail-open -> taken
    bp=float(bp); is_long = d in ('long','bullish')
    if abs(bp)<DEAD: return False
    return (bp>0)==is_long

def sim_semi(start,end):
    cur.execute("""SELECT direction, basket_pct, COALESCE(mes_sim_outcome_pnl,outcome_pnl)
                   FROM setup_log WHERE setup_name=ANY(%s) AND ts>=%s AND ts<%s
                   AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL""",(list(WL),start,end))
    take=[float(p) for d,bp,p in cur.fetchall() if sb_confirm(bp,d)]
    return len(take), sum(take), sum(take)*5

def real(start,end):
    cur.execute("SELECT COALESCE(SUM(net),0) FROM tsrt_daily_stmt WHERE day>=%s AND day<%s",(start,end))
    return float(cur.fetchone()[0])

print("APPLES-TO-APPLES: post-fix SEMI sim vs ACTUAL real money\n")
for label,s,e in [("SB live era Jun16-19",'2026-06-16','2026-06-20'),
                  ("Jun16-18 (3 supervised)",'2026-06-16','2026-06-19'),
                  ("Last week Jun15-19",'2026-06-15','2026-06-20')]:
    n,pts,usd=sim_semi(s,e); r=real(s,e)
    cap = f"{100*r/usd:.0f}%" if usd>0 else "n/a"
    print(f"  {label:24} SEMI sim: {n:>3} trades  +{pts:6.1f}p = +${usd:>6.0f}   |  REAL: ${r:+8.2f}   |  capture(real/sim): {cap}")

print("\n  (SEMI sim = V16-SB-confirmed whitelist signals, same filter that's live now.)")
print("  (REAL = broker tsrt_daily_stmt, actual fills, the fixed system live.)")
c.close()
