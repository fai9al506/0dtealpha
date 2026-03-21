"""Full comparison: Current V9-SC filter vs proposed DD-alignment filter."""
import sys, io, os, re, math
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

SQL = """
WITH tv AS (
    SELECT s.id, s.ts, s.setup_name, s.direction, s.spot, s.paradigm,
           s.greek_alignment, s.vix, s.overvix, s.grade, s.score,
           s.outcome_result, s.outcome_pnl, s.outcome_max_loss, s.outcome_max_profit,
           s.outcome_elapsed_min, s.outcome_first_event,
           s.vanna_all, s.spot_vol_beta,
           v.payload->'statistics'->>'delta_decay_hedging' as dd_hedging,
           v.payload->'statistics'->>'aggregatedCharm' as agg_charm,
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
        d['charm_num'] = float(d['agg_charm']) if d['agg_charm'] and d['agg_charm'] not in ('null','') else None
        d['day'] = str(d['ts'])[:10]
        d['hour'] = d['ts'].hour if d['ts'] else 0
        all_trades.append(d)

    def is_long(t):
        return t['direction'] in ('long', 'bullish')

    def dd_aligned(t):
        if t['dd_num'] is None: return 'no_data'
        sign = 1 if is_long(t) else -1
        val = t['dd_num'] * sign
        if val > 200_000_000: return 'ALIGNED'
        elif val < -200_000_000: return 'OPPOSED'
        return 'NEUTRAL'

    # =====================================================
    # DEFINE FILTERS
    # =====================================================

    # Current V9-SC filter (from memory):
    # Longs: alignment >= +2 AND (Skew Charm OR VIX <= 22 OR overvix >= +2)
    # Shorts whitelist: SC (all), AG (all), DD (align!=0). No VIX gate on shorts.
    def passes_v9sc(t):
        name = t['setup_name']
        direction = t['direction']
        align = t['greek_alignment'] or 0
        vix = t['vix'] or 0
        overvix = t['overvix'] or 0

        # Shorts whitelist
        if direction in ('short', 'bearish'):
            if name == 'Skew Charm': return True
            if name == 'AG Short': return True
            if name == 'DD Exhaustion' and align != 0: return True
            return False

        # Longs: alignment >= +2 AND (SC OR VIX<=22 OR overvix>=+2)
        if direction in ('long', 'bullish'):
            if align < 2: return False
            if name == 'Skew Charm': return True
            if vix <= 22: return True
            if overvix >= 2: return True
            return False

        return False

    # Proposed DD-alignment filter:
    # SC: DD_aligned + no toxic paradigm (GEX-LIS, AG-LIS)
    # DD: shorts + vanna >= 5B
    # AG: align <= -2
    def passes_dd_filter(t):
        name = t['setup_name']
        if name == 'Skew Charm':
            return dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS')
        if name == 'DD Exhaustion':
            return t['direction'] == 'short' and t['vanna_all'] is not None and t['vanna_all'] >= 5e9
        if name == 'AG Short':
            return (t['greek_alignment'] or 0) <= -2
        return False

    # Also test: SC_DD_aligned_only (just SC component, no DD/AG)
    def passes_sc_dd_only(t):
        if t['setup_name'] != 'Skew Charm': return False
        return dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS')

    # Hybrid: V9-SC but ADD DD-alignment gate on SC
    def passes_hybrid(t):
        if not passes_v9sc(t): return False
        # Additional: if SC, require DD alignment
        if t['setup_name'] == 'Skew Charm':
            return dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS')
        return True

    # =====================================================
    # COMPUTE ALL METRICS
    # =====================================================
    def full_metrics(trades, label):
        if not trades:
            return None

        n = len(trades)
        wins = [t for t in trades if t['outcome_result'] == 'WIN']
        losses = [t for t in trades if t['outcome_result'] != 'WIN']
        n_win = len(wins)
        n_loss = len(losses)
        wr = 100 * n_win / n

        tot_pnl = sum(t['outcome_pnl'] for t in trades)
        avg_pnl = tot_pnl / n
        pnl_pos = sum(t['outcome_pnl'] for t in trades if t['outcome_pnl'] > 0)
        pnl_neg = sum(-t['outcome_pnl'] for t in trades if t['outcome_pnl'] < 0)
        pf = pnl_pos / pnl_neg if pnl_neg > 0 else 999

        avg_win = sum(t['outcome_pnl'] for t in wins) / n_win if n_win else 0
        avg_loss = sum(t['outcome_pnl'] for t in losses) / n_loss if n_loss else 0

        avg_mae = sum(t['outcome_max_loss'] for t in trades) / n
        avg_mfe = sum(t['outcome_max_profit'] for t in trades) / n

        zero_dd = sum(1 for t in trades if t['outcome_max_loss'] >= -1.0)
        low_dd = sum(1 for t in trades if t['outcome_max_loss'] >= -2.0)
        worst_mae = min(t['outcome_max_loss'] for t in trades)

        avg_elapsed = sum((t['outcome_elapsed_min'] or 0) for t in trades) / n

        # Daily stats
        daily = defaultdict(lambda: {'pnl': 0, 'n': 0, 'w': 0})
        for t in sorted(trades, key=lambda x: x['ts']):
            day = t['day']
            daily[day]['pnl'] += t['outcome_pnl']
            daily[day]['n'] += 1
            if t['outcome_result'] == 'WIN': daily[day]['w'] += 1

        n_days = len(daily)
        avg_per_day = tot_pnl / n_days if n_days else 0
        avg_trades_per_day = n / n_days if n_days else 0

        daily_pnls = [daily[d]['pnl'] for d in sorted(daily.keys())]
        win_days = sum(1 for p in daily_pnls if p > 0)
        loss_days = sum(1 for p in daily_pnls if p <= 0)
        best_day = max(daily_pnls)
        worst_day = min(daily_pnls)

        # Equity curve and max drawdown
        running = 0; peak = 0; max_dd = 0
        for p in daily_pnls:
            running += p
            peak = max(peak, running)
            dd = running - peak
            max_dd = min(max_dd, dd)

        # Sharpe-like: avg daily / stdev daily
        if len(daily_pnls) > 1:
            mean_d = sum(daily_pnls) / len(daily_pnls)
            var_d = sum((p - mean_d)**2 for p in daily_pnls) / (len(daily_pnls) - 1)
            std_d = math.sqrt(var_d) if var_d > 0 else 0.001
            sharpe = mean_d / std_d if std_d > 0 else 0
        else:
            sharpe = 0

        # Recovery factor = total PnL / |maxDD|
        recovery = tot_pnl / abs(max_dd) if max_dd != 0 else 999

        # By setup breakdown
        by_setup = defaultdict(lambda: {'n': 0, 'w': 0, 'pnl': 0})
        for t in trades:
            by_setup[t['setup_name']]['n'] += 1
            by_setup[t['setup_name']]['pnl'] += t['outcome_pnl']
            if t['outcome_result'] == 'WIN': by_setup[t['setup_name']]['w'] += 1

        # By direction
        longs = [t for t in trades if is_long(t)]
        shorts = [t for t in trades if not is_long(t)]

        # Consecutive losses
        max_consec_loss = 0; cur = 0
        for t in sorted(trades, key=lambda x: x['ts']):
            if t['outcome_result'] != 'WIN': cur += 1; max_consec_loss = max(max_consec_loss, cur)
            else: cur = 0

        return {
            'label': label, 'n': n, 'n_win': n_win, 'n_loss': n_loss, 'wr': wr,
            'tot_pnl': tot_pnl, 'avg_pnl': avg_pnl, 'pf': pf,
            'avg_win': avg_win, 'avg_loss': avg_loss,
            'avg_mae': avg_mae, 'avg_mfe': avg_mfe,
            'zero_dd_pct': 100*zero_dd/n, 'low_dd_pct': 100*low_dd/n,
            'worst_mae': worst_mae, 'avg_elapsed': avg_elapsed,
            'n_days': n_days, 'avg_per_day': avg_per_day,
            'avg_trades_per_day': avg_trades_per_day,
            'win_days': win_days, 'loss_days': loss_days,
            'best_day': best_day, 'worst_day': worst_day,
            'max_dd': max_dd, 'sharpe': sharpe, 'recovery': recovery,
            'by_setup': dict(by_setup),
            'n_longs': len(longs), 'pnl_longs': sum(t['outcome_pnl'] for t in longs),
            'n_shorts': len(shorts), 'pnl_shorts': sum(t['outcome_pnl'] for t in shorts),
            'max_consec_loss': max_consec_loss,
        }

    # Apply filters
    v9sc_trades = [t for t in all_trades if passes_v9sc(t)]
    dd_filter_trades = [t for t in all_trades if passes_dd_filter(t)]
    sc_dd_only_trades = [t for t in all_trades if passes_sc_dd_only(t)]
    hybrid_trades = [t for t in all_trades if passes_hybrid(t)]

    filters = [
        full_metrics(v9sc_trades, "V9-SC (CURRENT)"),
        full_metrics(dd_filter_trades, "DD-ALIGN PORTFOLIO"),
        full_metrics(sc_dd_only_trades, "SC DD-ALIGNED ONLY"),
        full_metrics(hybrid_trades, "HYBRID (V9SC + SC DD gate)"),
    ]

    # =====================================================
    # PRINT COMPARISON TABLE
    # =====================================================
    print("=" * 130)
    print("FULL COMPARISON: CURRENT vs PROPOSED FILTERS")
    print("=" * 130)

    metrics = [
        ("TRADE VOLUME", None),
        ("Total trades", 'n'),
        ("Trading days", 'n_days'),
        ("Trades/day", 'avg_trades_per_day'),
        ("Longs", 'n_longs'),
        ("Shorts", 'n_shorts'),
        ("", None),
        ("PROFITABILITY", None),
        ("Total PnL (pts)", 'tot_pnl'),
        ("Avg PnL/trade", 'avg_pnl'),
        ("Avg PnL/day", 'avg_per_day'),
        ("Profit Factor", 'pf'),
        ("Avg winner", 'avg_win'),
        ("Avg loser", 'avg_loss'),
        ("PnL from longs", 'pnl_longs'),
        ("PnL from shorts", 'pnl_shorts'),
        ("", None),
        ("WIN RATE", None),
        ("Win rate", 'wr'),
        ("Winners", 'n_win'),
        ("Losers", 'n_loss'),
        ("Win days", 'win_days'),
        ("Loss days", 'loss_days'),
        ("Max consecutive losses", 'max_consec_loss'),
        ("", None),
        ("DRAWDOWN / RISK", None),
        ("Avg MAE (drawdown)", 'avg_mae'),
        ("Worst single MAE", 'worst_mae'),
        ("Zero-DD rate (MAE >= -1)", 'zero_dd_pct'),
        ("Low-DD rate (MAE >= -2)", 'low_dd_pct'),
        ("Max portfolio DD", 'max_dd'),
        ("Best day", 'best_day'),
        ("Worst day", 'worst_day'),
        ("", None),
        ("EFFICIENCY", None),
        ("Avg MFE", 'avg_mfe'),
        ("Avg hold time (min)", 'avg_elapsed'),
        ("Sharpe (daily)", 'sharpe'),
        ("Recovery factor", 'recovery'),
    ]

    # Header
    header = f"{'Metric':<30}"
    for f in filters:
        header += f" | {f['label']:>22}"
    print(header)
    print("-" * 130)

    for name, key in metrics:
        if key is None:
            if name:
                print(f"\n  {name}")
                print(f"  {'-'*len(name)}")
            continue

        row = f"  {name:<28}"
        for f in filters:
            val = f[key]
            if isinstance(val, float):
                if 'pct' in key or key == 'wr':
                    row += f" | {val:>21.1f}%"
                elif key in ('pf', 'sharpe', 'recovery', 'avg_trades_per_day'):
                    row += f" | {val:>22.2f}"
                else:
                    row += f" | {val:>22.1f}"
            elif isinstance(val, int):
                row += f" | {val:>22}"
            else:
                row += f" | {str(val):>22}"
        print(row)

    # =====================================================
    # SETUP BREAKDOWN
    # =====================================================
    print("\n\n" + "=" * 130)
    print("SETUP BREAKDOWN")
    print("=" * 130)

    all_setups = set()
    for f in filters:
        all_setups.update(f['by_setup'].keys())

    header = f"{'Setup':<20}"
    for f in filters:
        header += f" | {f['label']:>22}"
    print(header)
    print("-" * 130)

    for setup in sorted(all_setups):
        row = f"  {setup:<18}"
        for f in filters:
            s = f['by_setup'].get(setup, {'n': 0, 'w': 0, 'pnl': 0})
            if s['n'] > 0:
                wr = 100 * s['w'] / s['n']
                row += f" | {s['n']:>3}t {wr:>4.0f}%WR {s['pnl']:>+7.1f}"
            else:
                row += f" |                    ---"
        print(row)

    # =====================================================
    # DAILY EQUITY SIDE BY SIDE
    # =====================================================
    print("\n\n" + "=" * 130)
    print("DAILY EQUITY CURVE COMPARISON")
    print("=" * 130)

    all_days = set()
    for f_trades in [v9sc_trades, dd_filter_trades, sc_dd_only_trades, hybrid_trades]:
        for t in f_trades:
            all_days.add(t['day'])

    def daily_stats(trades):
        d = defaultdict(lambda: {'pnl': 0, 'n': 0, 'w': 0})
        for t in trades:
            d[t['day']]['pnl'] += t['outcome_pnl']
            d[t['day']]['n'] += 1
            if t['outcome_result'] == 'WIN': d[t['day']]['w'] += 1
        return d

    d_v9 = daily_stats(v9sc_trades)
    d_dd = daily_stats(dd_filter_trades)
    d_sc = daily_stats(sc_dd_only_trades)
    d_hy = daily_stats(hybrid_trades)

    cum_v9 = 0; cum_dd = 0; cum_sc = 0; cum_hy = 0
    pk_v9 = 0; pk_dd = 0; pk_sc = 0; pk_hy = 0

    print(f"{'Date':<12} | {'V9-SC':>30} | {'DD-ALIGN':>30} | {'SC-DD-ONLY':>30} | {'HYBRID':>30}")
    print("-" * 140)

    for day in sorted(all_days):
        pv = d_v9[day]['pnl']; nv = d_v9[day]['n']
        pd = d_dd[day]['pnl']; nd = d_dd[day]['n']
        ps = d_sc[day]['pnl']; ns = d_sc[day]['n']
        ph = d_hy[day]['pnl']; nh = d_hy[day]['n']

        cum_v9 += pv; cum_dd += pd; cum_sc += ps; cum_hy += ph
        pk_v9 = max(pk_v9, cum_v9); pk_dd = max(pk_dd, cum_dd)
        pk_sc = max(pk_sc, cum_sc); pk_hy = max(pk_hy, cum_hy)
        dd_v9 = cum_v9 - pk_v9; dd_dd = cum_dd - pk_dd
        dd_sc = cum_sc - pk_sc; dd_hy = cum_hy - pk_hy

        def fmt(pnl, cum, n, dd):
            if n == 0: return f"{'---':>30}"
            return f"{n:>2}t {pnl:>+6.1f} cum:{cum:>6.1f} dd:{dd:>5.1f}"

        print(f"  {day} | {fmt(pv,cum_v9,nv,dd_v9):>30} | {fmt(pd,cum_dd,nd,dd_dd):>30} | {fmt(ps,cum_sc,ns,dd_sc):>30} | {fmt(ph,cum_hy,nh,dd_hy):>30}")

    print(f"\n  {'FINAL':>10} | {'cum:':>5}{cum_v9:>6.1f}{'':>19} | {'cum:':>5}{cum_dd:>6.1f}{'':>19} | {'cum:':>5}{cum_sc:>6.1f}{'':>19} | {'cum:':>5}{cum_hy:>6.1f}")

    # =====================================================
    # $ PROJECTIONS (1 MES = $5/pt, 8 MES = $40/pt)
    # =====================================================
    print("\n\n" + "=" * 130)
    print("DOLLAR PROJECTIONS (8 MES = $40/pt)")
    print("=" * 130)

    for f in filters:
        daily_dollar = f['avg_per_day'] * 40
        monthly_dollar = daily_dollar * 21
        yearly_dollar = daily_dollar * 252
        max_dd_dollar = f['max_dd'] * 40
        print(f"\n  {f['label']}:")
        print(f"    Daily avg:   ${daily_dollar:>8,.0f}/day")
        print(f"    Monthly est: ${monthly_dollar:>8,.0f}/month")
        print(f"    Yearly est:  ${yearly_dollar:>8,.0f}/year")
        print(f"    Max DD:      ${max_dd_dollar:>8,.0f}")
        print(f"    Trades/day:  {f['avg_trades_per_day']:.1f}")
