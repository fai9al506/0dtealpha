"""Loss diagnosis part 4 — breaker behavior + ES Abs execution gap.

1) Per-day intraday cumulative broker P&L timeline (by exit time) — did the
   $300 breaker actually halt trading, or did losses pile on after -300?
2) ES Absorption bullish: per-trade chain-sim vs broker to localize the gap.
3) Vol-regime sizing what-if: halve size on high-range days, recompute loss window.
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# 1) intraday cumulative from tsrt_daily_stmt trades (sorted by exit_et)
print("=== 1) Intraday cumulative broker $ by exit time (breaker check) ===")
cur.execute("""SELECT day, trades FROM tsrt_daily_stmt
               WHERE day >= '2026-06-05' ORDER BY day""")
for day, trades in cur.fetchall():
    t = trades if isinstance(trades,list) else (json.loads(trades) if trades else [])
    t = sorted(t, key=lambda x: x.get('exit_et') or '')
    cum=0.0; line=[]; first_cross=None
    for it in t:
        cum+=float(it.get('usd') or 0)
        if first_cross is None and cum<=-300: first_cross=it.get('exit_et')
        line.append(f"{it.get('exit_et')}:{cum:+.0f}")
    print(f"\n{str(day)} ({len(t)} trades)  first<=-300 at {first_cross}")
    # print every 3rd to keep compact
    print("  "+"  ".join(line))

# 2) ES Absorption bullish per-trade
print("\n\n=== 2) ES Absorption bullish: per-trade chain vs broker ===")
cur.execute("""
   SELECT (sl.ts AT TIME ZONE 'America/New_York') AS et, sl.outcome_result,
          sl.outcome_pnl, sl.mes_sim_outcome_pnl, sl.grade, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name='ES Absorption' AND sl.direction='bullish'
     AND (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-06-05'
   ORDER BY sl.ts""")
def real_pts(state, d='long'):
    if isinstance(state,str): state=json.loads(state)
    fill=state.get('entry_fill_price') or state.get('fill_price')
    ex=state.get('stop_fill_price') or state.get('close_fill_price')
    if fill is None or ex is None: return None,None,None
    return float(ex)-float(fill), fill, ex
print(f"{'et':17} {'grade':5} {'res':8} {'chain':>7} {'mes':>7} {'broker':>7} {'fill':>8} {'exit':>8}")
for et,res,opnl,mpnl,grade,state in cur.fetchall():
    rp,fill,ex = real_pts(state)
    print(f"{str(et)[:16]:17} {str(grade):5} {str(res):8} {float(opnl or 0):>7.1f} {float(mpnl or 0):>7.1f} "
          f"{(rp if rp is not None else 0):>7.1f} {str(fill):>8} {str(ex):>8}")

# 3) what-if: halve all sizes on days with intraday SPX range>=120 (high-vol regime)
print("\n=== 3) Vol-regime halve-size what-if (range>=120 days) ===")
cur.execute("""
  WITH d AS (SELECT (ts AT TIME ZONE 'America/New_York')::date dd, spot FROM chain_snapshots
             WHERE (ts AT TIME ZONE 'America/New_York')::date>='2026-06-05' AND spot IS NOT NULL)
  SELECT dd, max(spot)-min(spot) rng FROM d GROUP BY dd ORDER BY dd""")
rng={str(dd):float(r) for dd,r in cur.fetchall()}
cur.execute("""SELECT day,net,trades FROM tsrt_daily_stmt WHERE day>='2026-06-05' ORDER BY day""")
tot_real=tot_half=0.0
for day,net,trades in cur.fetchall():
    ds=str(day); r=rng.get(ds,0); net=float(net or 0)
    half = net*0.5 if r>=120 else net   # halve only high-range days
    tot_real+=net; tot_half+=half
    print(f"  {ds} range={r:>5.0f} real={net:>+8.2f} halved={half:>+8.2f} {'<halved' if r>=120 else ''}")
print(f"\n  TOTAL real={tot_real:+.2f}  with-halve-on-highvol={tot_half:+.2f}  saved={tot_half-tot_real:+.2f}")
conn.close()
