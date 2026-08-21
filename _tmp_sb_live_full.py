import os, psycopg2, json
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
cur=c.cursor()
DEAD=0.15
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption')
def confirm(bp,d):
    if bp is None: return True
    bp=float(bp); il=d in ('long','bullish')
    if abs(bp)<DEAD: return False
    return (bp>0)==il
def is_long(d): return d in ('long','bullish')

# live period extent
cur.execute("SELECT MAX(day) FROM tsrt_daily_stmt"); last=cur.fetchone()[0]
print(f"V16-SB live: 2026-06-16  ->  latest broker day on file: {last}\n")

LIVE_START='2026-06-16'; LIVE_END=str(last)  # inclusive handled below
days = [str(r[0]) for r in cur.execute("SELECT DISTINCT day FROM tsrt_daily_stmt WHERE day>=%s ORDER BY day",(LIVE_START,)) or cur.fetchall()]

print(f"{'day':12} | {'SIM confirmed':^22} | {'REAL placed':^20} | per-dir capture")
print(f"{'':12} | {'shorts':>10} {'longs':>10} | {'shorts':>9} {'longs':>9} |")
print("-"*86)
tot_ss=tot_sl=tot_rs=tot_rl=0
for day in days:
    nd=day+' 23:59'
    # sim confirmed for the day
    cur.execute("""SELECT direction, basket_pct, COALESCE(mes_sim_outcome_pnl,outcome_pnl)
                   FROM setup_log WHERE setup_name=ANY(%s) AND ts::date=%s
                   AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL""",(list(WL),day))
    ss=sl=0
    for d,bp,p in cur.fetchall():
        if not confirm(bp,d): continue
        if is_long(d): sl+=float(p)
        else: ss+=float(p)
    # real placed for the day
    cur.execute("SELECT trades FROM tsrt_daily_stmt WHERE day=%s",(day,))
    row=cur.fetchone(); rs=rl=0
    if row and row[0]:
        arr=row[0] if isinstance(row[0],list) else json.loads(row[0])
        for t in arr:
            p=float(t.get('pts',0))
            if t.get('dir','').upper()=='LONG': rl+=p
            else: rs+=p
    cap_s = f"{100*rs/ss:.0f}%" if ss>0 else "n/a"
    cap_l = f"{100*rl/sl:.0f}%" if sl>0 else ("n/a" if sl>=0 else f"{100*rl/sl:.0f}%")
    print(f"{day:12} | {ss:>+9.1f}p {sl:>+9.1f}p | {rs:>+8.1f}p {rl:>+8.1f}p | S:{cap_s:>5}  L:{cap_l:>6}")
    tot_ss+=ss; tot_sl+=sl; tot_rs+=rs; tot_rl+=rl

print("-"*86)
print(f"{'TOTAL':12} | {tot_ss:>+9.1f}p {tot_sl:>+9.1f}p | {tot_rs:>+8.1f}p {tot_rl:>+8.1f}p |")
print(f"\nSIM confirmed total:  shorts {tot_ss:+.1f}p  longs {tot_sl:+.1f}p  = {tot_ss+tot_sl:+.1f}p (${(tot_ss+tot_sl)*5:+.0f})")
print(f"REAL placed total:    shorts {tot_rs:+.1f}p  longs {tot_rl:+.1f}p  = {tot_rs+tot_rl:+.1f}p (${(tot_rs+tot_rl)*5:+.0f} gross)")
print(f"\nSHORT-side capture (real/sim): {100*tot_rs/tot_ss:.0f}%" if tot_ss>0 else "")
print(f"LONG-side: sim {tot_sl:+.1f}p, real {tot_rl:+.1f}p  (both directions of a losing side)")
c.close()
