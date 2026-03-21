"""Portfolio comparison: Low-DD filter vs V9-SC baseline."""
import sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from dotenv import load_dotenv; load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL: DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
if "postgresql://" in DB_URL and "postgresql+psycopg" not in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
from sqlalchemy import create_engine, text
from collections import defaultdict
engine = create_engine(DB_URL)

def parse_money(s):
    if not s: return None
    s = str(s).strip(); neg = '-' in s
    digits = re.sub(r'[^0-9.]', '', s)
    if not digits: return None
    val = float(digits)
    return -val if neg else val

def avg(lst):
    v = [x for x in lst if x is not None]
    return sum(v)/len(v) if v else 0

SQL = """
WITH tv AS (
    SELECT s.id, s.ts, s.setup_name, s.direction, s.spot, s.paradigm,
           s.greek_alignment, s.vix,
           s.outcome_result, s.outcome_pnl, s.outcome_max_loss, s.outcome_max_profit,
           s.vanna_all,
           v.payload->'statistics'->>'delta_decay_hedging' as dd_hedging,
           ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY ABS(EXTRACT(EPOCH FROM (v.ts - s.ts)))) as rn
    FROM setup_log s
    LEFT JOIN volland_snapshots v ON v.ts BETWEEN s.ts - interval '5 minutes' AND s.ts + interval '5 minutes'
    WHERE s.outcome_result IS NOT NULL AND s.outcome_max_loss IS NOT NULL
)
SELECT * FROM tv WHERE rn = 1
"""

with engine.connect() as c:
    r = c.execute(text(SQL))
    all_trades = []
    for row in r.fetchall():
        d = dict(zip(r.keys(), row))
        d['dd_num'] = parse_money(d['dd_hedging'])
        all_trades.append(d)

    def dd_aligned(t):
        if t['dd_num'] is None: return 'no_data'
        sign = 1 if t['direction'] in ('long','bullish') else -1
        val = t['dd_num'] * sign
        if val > 200_000_000: return 'ALIGNED'
        elif val < -200_000_000: return 'OPPOSED'
        return 'NEUTRAL'

    def stats(grp, label):
        if not grp: return
        n = len(grp)
        wins = sum(1 for t in grp if t['outcome_result'] == 'WIN')
        zero = sum(1 for t in grp if t['outcome_max_loss'] >= -1.0)
        low = sum(1 for t in grp if t['outcome_max_loss'] >= -2.0)
        tot = sum(t['outcome_pnl'] for t in grp)
        pos = sum(t['outcome_pnl'] for t in grp if t['outcome_pnl'] > 0)
        neg = sum(-t['outcome_pnl'] for t in grp if t['outcome_pnl'] < 0)
        pf = pos/neg if neg > 0 else 999
        mae = avg([t['outcome_max_loss'] for t in grp])
        print(f"  {label:<42} n={n:>3} | 0DD:{100*zero/n:>5.1f}% | lowDD:{100*low/n:>5.1f}% | WR:{100*wins/n:>5.1f}% | PnL:{tot:>8.1f} | avg:{tot/n:>6.2f} | PF:{pf:>5.2f} | MAE:{mae:>6.1f}")

    sc = [t for t in all_trades if t['setup_name'] == 'Skew Charm']
    dd = [t for t in all_trades if t['setup_name'] == 'DD Exhaustion']
    ag = [t for t in all_trades if t['setup_name'] == 'AG Short']

    # Portfolio: best low-DD filters
    sc_best = [t for t in sc if dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS','AG-LIS')]
    dd_best = [t for t in dd if t['direction'] == 'short' and t['vanna_all'] is not None and t['vanna_all'] >= 5e9]
    ag_best = [t for t in ag if t['greek_alignment'] is not None and t['greek_alignment'] <= -2]
    portfolio = sc_best + dd_best + ag_best

    # V9-SC baseline
    v9sc = sc + ag + [t for t in dd if t['greek_alignment'] is not None and t['greek_alignment'] != 0]

    print("PORTFOLIO vs V9-SC COMPARISON")
    print("=" * 130)
    stats(v9sc, "V9-SC baseline (current filter)")
    stats(portfolio, "LOW-DD PORTFOLIO (SC_best+DD_best+AG_best)")
    stats(sc_best, "  -> SC: DD_aligned + no_toxic_paradigm")
    stats(dd_best, "  -> DD: shorts + vanna>=5B")
    stats(ag_best, "  -> AG: align<=-2")

    # Daily simulation
    portfolio.sort(key=lambda t: t['ts'])
    daily = defaultdict(lambda: {'n':0, 'w':0, 'pnl':0})
    for t in portfolio:
        day = str(t['ts'])[:10]
        daily[day]['n'] += 1
        daily[day]['pnl'] += t['outcome_pnl']
        if t['outcome_result'] == 'WIN': daily[day]['w'] += 1

    running = 0; peak = 0; max_dd = 0
    print(f"\nPORTFOLIO DAILY EQUITY:")
    for day in sorted(daily.keys()):
        d = daily[day]
        running += d['pnl']
        peak = max(peak, running)
        dd = running - peak
        max_dd = min(max_dd, dd)
        bar = "+" * int(max(0, d['pnl']) / 3) + "-" * int(max(0, -d['pnl']) / 3)
        print(f"  {day} | n:{d['n']:>2} w:{d['w']:>2} | pnl:{d['pnl']:>7.1f} | cum:{running:>7.1f} | DD:{dd:>6.1f} | {bar}")

    n_total = sum(d['n'] for d in daily.values())
    n_wins = sum(d['w'] for d in daily.values())
    n_days = len(daily)
    tot_pnl = sum(t['outcome_pnl'] for t in portfolio)

    print(f"\n  PORTFOLIO: {n_total} trades over {n_days} days")
    print(f"  Total PnL: +{tot_pnl:.1f} pts ({tot_pnl/n_days:.1f} pts/day)")
    print(f"  Win rate: {100*n_wins/n_total:.1f}%")
    print(f"  Max drawdown: {max_dd:.1f} pts")
    print(f"  Avg PnL/trade: {tot_pnl/n_total:.2f} pts")

    v9_pnl = sum(t['outcome_pnl'] for t in v9sc)
    v9_days = len(set(str(t['ts'])[:10] for t in v9sc))
    print(f"\n  V9-SC: {len(v9sc)} trades over {v9_days} days")
    print(f"  Total PnL: +{v9_pnl:.1f} pts ({v9_pnl/v9_days:.1f} pts/day)")

    print(f"\n  Efficiency: Portfolio {tot_pnl/n_total:.2f} pts/trade vs V9-SC {v9_pnl/len(v9sc):.2f} pts/trade")
    print(f"  = {(tot_pnl/n_total) / (v9_pnl/len(v9sc)):.1f}x better per trade")

    # What does SC_best look like on losing days?
    print(f"\n\nSC_BEST LOSING TRADES ({sum(1 for t in sc_best if t['outcome_result'] != 'WIN')} total):")
    for t in sorted(sc_best, key=lambda t: t['outcome_pnl']):
        if t['outcome_result'] == 'WIN': continue
        print(f"  ID:{t['id']:>4} {str(t['ts'])[:16]} {t['direction']:<6} spot:{t['spot']:>7.1f} "
              f"{t['paradigm']:<14} al:{t['greek_alignment']:>2} VIX:{t['vix'] or 0:>5.1f} "
              f"DD:{t['dd_hedging'] or 'n/a'} | MAE:{t['outcome_max_loss']:>6.1f} PnL:{t['outcome_pnl']:>6.1f}")
