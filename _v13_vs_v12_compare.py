"""Full V12-fix vs V13 comparison — PnL, trades, DD, weekly, streaks, consistency."""
import psycopg2, json
from collections import defaultdict
from datetime import datetime, date, timedelta

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

START = '2026-03-01'
END = '2026-04-17'

cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       (ts AT TIME ZONE 'America/New_York')::date as d,
       outcome_max_profit, outcome_max_loss
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""", (START, END))
raw = cur.fetchall()

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m, d, mfe, mae = t
    if setup in ("VIX Divergence", "IV Momentum", "Vanna Butterfly"): return False
    if setup == 'Skew Charm' and grade and grade in ('C', 'LOG'): return False
    if setup in ('Skew Charm', 'DD Exhaustion'):
        if (h == 14 and m >= 30) or h == 15: return False
    if setup == 'BofA Scalp' and ((h == 14 and m >= 30) or h >= 15): return False
    is_long = dirx in ('long', 'bullish')
    if is_long and paradigm == 'SIDIAL-EXTREME': return False
    if is_long:
        if align is None or align < 2: return False
        if setup == 'Skew Charm': return True
        vix_f = float(vix) if vix else None
        ovx_f = float(ovx) if ovx else -99
        if vix_f is not None and vix_f > 22 and ovx_f < 2: return False
        return True
    else:
        if setup in ('Skew Charm','DD Exhaustion') and paradigm == 'GEX-LIS': return False
        if setup == 'AG Short' and paradigm == 'AG-TARGET': return False
        if setup in ('Skew Charm', 'AG Short'): return True
        if setup == 'DD Exhaustion' and align != 0: return True
        return False

v12 = [t for t in raw if passes_v12fix(t)]
print(f"V12-fix eligible: {len(v12)}")

# Enrich with v13 block status
def get_v13_gex(ts, spot):
    cur.execute("SELECT columns, rows FROM chain_snapshots WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL ORDER BY ts DESC LIMIT 1", (ts, ts))
    r = cur.fetchone()
    if not r: return 0.0
    cols, rows = r
    try:
        s_i = cols.index('Strike'); c_oi = cols.index('Open Int'); c_g = cols.index('Gamma')
        p_g = cols.index('Gamma', c_g+1); p_oi = cols.index('Open Int', c_oi+1)
        mg = 0
        for row in rows:
            s = row[s_i]
            if s is None or float(s) <= float(spot): continue
            ng = float(row[c_g] or 0)*float(row[c_oi] or 0) - float(row[p_g] or 0)*float(row[p_oi] or 0)
            if ng > mg: mg = ng
        return mg
    except: return 0.0

def get_v13_dd(ts, spot):
    cur.execute("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc=(SELECT mts FROM lts) AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    r = cur.fetchone()
    return float(r[0]) if r and r[0] else 0.0

def get_vanna(ts, spot):
    cur.execute("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='vanna' AND expiration_option='THIS_WEEK'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc = (SELECT mts FROM lts) ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
    if not pts: return None, None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None, None
    s0 = sorted(near); cr = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1-v0 != 0: cr.append(x0 + (-v0/(v1-v0))*(x1-x0))
    cs = None
    if cr:
        nearest = min(cr, key=lambda s: abs(s - float(spot)))
        cs = 'A' if nearest > float(spot) else 'B'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    ps = 'A' if pk > float(spot) else 'B'
    return cs, ps

print("Enriching...", flush=True)
enr = []
for i, t in enumerate(v12):
    gx = get_v13_gex(t[1], t[5])
    dd = get_v13_dd(t[1], t[5])
    vc, vp = get_vanna(t[1], t[5])
    enr.append({'t': t, 'gex': gx, 'dd': dd, 'vc': vc, 'vp': vp})
    if (i+1) % 50 == 0: print(f"  {i+1}/{len(v12)}", flush=True)

def v13_block(r):
    t = r['t']; setup = t[2]; dirx = t[11]; c = r['vc']; p = r['vp']
    if dirx in ('short','bearish') and setup in ('Skew Charm','DD Exhaustion'):
        if r['gex'] >= 75: return True
        if r['dd'] >= 3_000_000_000: return True
    if c is not None:
        if dirx in ('short','bearish'):
            if setup == 'DD Exhaustion' and c == 'A': return True
            if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
            if setup == 'AG Short' and c == 'B' and p == 'A': return True
        if dirx in ('long','bullish'):
            if setup == 'Skew Charm' and c == 'A' and p == 'B': return True
    return False

# Build two sequential trade lists: V12 (all enr) and V13 (enr without blocks)
v12_trades = sorted(enr, key=lambda r: r['t'][1])
v13_trades = [r for r in v12_trades if not v13_block(r)]

def trade_pnl(r): return float(r['t'][7] or 0)
def trade_outcome(r): return r['t'][6]
def trade_date(r): return r['t'][14]
def trade_setup(r): return r['t'][2]
def trade_direction(r): return r['t'][11]

def compute_maxdd(trades):
    """Max peak-to-trough drawdown on cumulative PnL curve."""
    cum = 0; peak = 0; maxdd = 0; peak_ts = None; trough_ts = None
    cur_peak_ts = None
    dd_start_ts = None
    for r in trades:
        cum += trade_pnl(r)
        if cum > peak:
            peak = cum
            cur_peak_ts = r['t'][1]
        dd = peak - cum
        if dd > maxdd:
            maxdd = dd
            peak_ts = cur_peak_ts
            trough_ts = r['t'][1]
    return maxdd, peak_ts, trough_ts

def compute_streaks(trades):
    """Longest losing streak and longest winning streak (by count)."""
    max_loss_streak = 0; max_win_streak = 0
    cur_loss = 0; cur_win = 0
    max_loss_amt = 0; cur_loss_amt = 0
    max_win_amt = 0; cur_win_amt = 0
    for r in trades:
        o = trade_outcome(r); p = trade_pnl(r)
        if o == 'LOSS' or (o == 'EXPIRED' and p < 0):
            cur_loss += 1; cur_win = 0
            cur_loss_amt += p
            if cur_loss > max_loss_streak: max_loss_streak = cur_loss
            if cur_loss_amt < max_loss_amt: max_loss_amt = cur_loss_amt
        elif o == 'WIN' or (o == 'EXPIRED' and p > 0):
            cur_win += 1; cur_loss = 0
            cur_win_amt += p
            if cur_win > max_win_streak: max_win_streak = cur_win
            if cur_win_amt > max_win_amt: max_win_amt = cur_win_amt
            if o == 'LOSS': cur_loss_amt = 0
            else: cur_loss_amt = 0
        else:  # EXPIRED zero
            if p == 0: pass
        if o == 'WIN': cur_loss_amt = 0
        if o == 'LOSS': cur_win_amt = 0
    return {'max_loss_streak': max_loss_streak, 'max_win_streak': max_win_streak,
            'max_loss_amt': max_loss_amt, 'max_win_amt': max_win_amt}

def stats(trades, label):
    pnl = sum(trade_pnl(r) for r in trades)
    n = len(trades)
    wins = [r for r in trades if trade_outcome(r) == 'WIN']
    losses = [r for r in trades if trade_outcome(r) == 'LOSS']
    exps = [r for r in trades if trade_outcome(r) == 'EXPIRED']
    wr = 100*len(wins)/max(1, len(wins)+len(losses))
    avg_win = sum(trade_pnl(r) for r in wins)/max(1,len(wins))
    avg_loss = sum(trade_pnl(r) for r in losses)/max(1,len(losses))
    maxdd, peak_ts, tr_ts = compute_maxdd(trades)
    streaks = compute_streaks(trades)
    # Profit factor
    gross_p = sum(trade_pnl(r) for r in trades if trade_pnl(r) > 0)
    gross_l = abs(sum(trade_pnl(r) for r in trades if trade_pnl(r) < 0))
    pf = gross_p / gross_l if gross_l > 0 else 0
    return {
        'label': label, 'n': n, 'pnl': pnl,
        'wins': len(wins), 'losses': len(losses), 'exps': len(exps),
        'wr': wr, 'avg_win': avg_win, 'avg_loss': avg_loss,
        'maxdd': maxdd, 'pf': pf,
        'avg_per_trade': pnl/max(1,n),
        'max_loss_streak': streaks['max_loss_streak'],
        'max_win_streak': streaks['max_win_streak'],
        'worst_streak_pts': streaks['max_loss_amt'],
        'best_streak_pts': streaks['max_win_amt'],
        'peak_ts': str(peak_ts) if peak_ts else None,
        'trough_ts': str(tr_ts) if tr_ts else None,
    }

s12 = stats(v12_trades, 'V12-fix')
s13 = stats(v13_trades, 'V13 combined')

# Weekly breakdown
def iso_week(d):
    return f"{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"
weekly_v12 = defaultdict(float); weekly_v13 = defaultdict(float)
weekly_v12_n = defaultdict(int); weekly_v13_n = defaultdict(int)
for r in v12_trades:
    w = iso_week(trade_date(r))
    weekly_v12[w] += trade_pnl(r); weekly_v12_n[w] += 1
for r in v13_trades:
    w = iso_week(trade_date(r))
    weekly_v13[w] += trade_pnl(r); weekly_v13_n[w] += 1
weeks = sorted(set(list(weekly_v12.keys()) + list(weekly_v13.keys())))

# Daily breakdown (for DD timeline)
daily_v12_cum = []; daily_v13_cum = []
cum12 = 0; cum13 = 0
days = sorted(set(trade_date(r) for r in v12_trades))
by_day_v12 = defaultdict(float); by_day_v13 = defaultdict(float)
for r in v12_trades:
    by_day_v12[trade_date(r)] += trade_pnl(r)
for r in v13_trades:
    by_day_v13[trade_date(r)] += trade_pnl(r)
for d in days:
    cum12 += by_day_v12.get(d, 0)
    cum13 += by_day_v13.get(d, 0)
    daily_v12_cum.append({'d': str(d), 'cum': round(cum12, 1), 'day': round(by_day_v12.get(d, 0), 1)})
    daily_v13_cum.append({'d': str(d), 'cum': round(cum13, 1), 'day': round(by_day_v13.get(d, 0), 1)})

# Per-setup breakdown
per_setup = {}
for setup in sorted(set(trade_setup(r) for r in v12_trades)):
    v12_s = [r for r in v12_trades if trade_setup(r) == setup]
    v13_s = [r for r in v13_trades if trade_setup(r) == setup]
    blocked_s = [r for r in v12_s if v13_block(r)]
    per_setup[setup] = {
        'v12': {'n': len(v12_s), 'pnl': sum(trade_pnl(r) for r in v12_s),
                'w': sum(1 for r in v12_s if trade_outcome(r)=='WIN'),
                'l': sum(1 for r in v12_s if trade_outcome(r)=='LOSS')},
        'v13': {'n': len(v13_s), 'pnl': sum(trade_pnl(r) for r in v13_s),
                'w': sum(1 for r in v13_s if trade_outcome(r)=='WIN'),
                'l': sum(1 for r in v13_s if trade_outcome(r)=='LOSS')},
        'blocked': {'n': len(blocked_s), 'pnl': sum(trade_pnl(r) for r in blocked_s)},
    }

# Monthly
monthly = {}
for m in ['2026-03', '2026-04']:
    mv12 = [r for r in v12_trades if str(trade_date(r)).startswith(m)]
    mv13 = [r for r in v13_trades if str(trade_date(r)).startswith(m)]
    mb = [r for r in v12_trades if str(trade_date(r)).startswith(m) and v13_block(r)]
    monthly[m] = {
        'v12_n': len(mv12), 'v12_pnl': sum(trade_pnl(r) for r in mv12),
        'v13_n': len(mv13), 'v13_pnl': sum(trade_pnl(r) for r in mv13),
        'blocked_n': len(mb), 'blocked_pnl': sum(trade_pnl(r) for r in mb),
    }

# Direction breakdown
dir_summary = {}
for dname, setups in [('LONG (total)', lambda r: trade_direction(r) in ('long','bullish')),
                      ('SHORT (total)', lambda r: trade_direction(r) in ('short','bearish'))]:
    v12_d = [r for r in v12_trades if setups(r)]
    v13_d = [r for r in v13_trades if setups(r)]
    dir_summary[dname] = {
        'v12_n': len(v12_d), 'v12_pnl': sum(trade_pnl(r) for r in v12_d),
        'v13_n': len(v13_d), 'v13_pnl': sum(trade_pnl(r) for r in v13_d),
    }

# Days positive / negative
pos_days_v12 = sum(1 for d in days if by_day_v12.get(d, 0) > 0)
neg_days_v12 = sum(1 for d in days if by_day_v12.get(d, 0) < 0)
pos_days_v13 = sum(1 for d in days if by_day_v13.get(d, 0) > 0)
neg_days_v13 = sum(1 for d in days if by_day_v13.get(d, 0) < 0)

# Weekly positive/negative
pos_w_v12 = sum(1 for w in weeks if weekly_v12.get(w, 0) > 0)
neg_w_v12 = sum(1 for w in weeks if weekly_v12.get(w, 0) < 0)
pos_w_v13 = sum(1 for w in weeks if weekly_v13.get(w, 0) > 0)
neg_w_v13 = sum(1 for w in weeks if weekly_v13.get(w, 0) < 0)

# Worst day
worst_v12_day = min(days, key=lambda d: by_day_v12.get(d, 0))
worst_v13_day = min(days, key=lambda d: by_day_v13.get(d, 0))
best_v12_day = max(days, key=lambda d: by_day_v12.get(d, 0))
best_v13_day = max(days, key=lambda d: by_day_v13.get(d, 0))

data = {
    'period': f"{START} to {END}",
    'trading_days': len(days),
    'v12': s12, 'v13': s13,
    'weekly': {w: {'v12_n': weekly_v12_n[w], 'v12_pnl': weekly_v12[w],
                   'v13_n': weekly_v13_n[w], 'v13_pnl': weekly_v13[w]} for w in weeks},
    'daily_v12': daily_v12_cum, 'daily_v13': daily_v13_cum,
    'per_setup': per_setup, 'monthly': monthly, 'direction': dir_summary,
    'pos_days_v12': pos_days_v12, 'neg_days_v12': neg_days_v12,
    'pos_days_v13': pos_days_v13, 'neg_days_v13': neg_days_v13,
    'pos_weeks_v12': pos_w_v12, 'neg_weeks_v12': neg_w_v12,
    'pos_weeks_v13': pos_w_v13, 'neg_weeks_v13': neg_w_v13,
    'worst_day_v12': {'d': str(worst_v12_day), 'pnl': by_day_v12.get(worst_v12_day, 0)},
    'worst_day_v13': {'d': str(worst_v13_day), 'pnl': by_day_v13.get(worst_v13_day, 0)},
    'best_day_v12': {'d': str(best_v12_day), 'pnl': by_day_v12.get(best_v12_day, 0)},
    'best_day_v13': {'d': str(best_v13_day), 'pnl': by_day_v13.get(best_v13_day, 0)},
}

with open('_v13_compare.json', 'w') as f:
    def conv(o):
        if hasattr(o, 'item'): return o.item()
        if hasattr(o, 'isoformat'): return o.isoformat()
        return str(o)
    json.dump(data, f, indent=2, default=conv)

print()
print("=" * 60)
print(f"V12-fix: {s12['pnl']:+.1f} pts / {s12['n']} trades / WR {s12['wr']:.1f}% / MaxDD {s12['maxdd']:.1f} / PF {s12['pf']:.2f}")
print(f"V13:     {s13['pnl']:+.1f} pts / {s13['n']} trades / WR {s13['wr']:.1f}% / MaxDD {s13['maxdd']:.1f} / PF {s13['pf']:.2f}")
print(f"Δ PnL: {s13['pnl']-s12['pnl']:+.1f} / Δ MaxDD: {s13['maxdd']-s12['maxdd']:+.1f}")
print(f"Worst streak V12: {s12['max_loss_streak']} losses in a row ({s12['worst_streak_pts']:.1f} pts)")
print(f"Worst streak V13: {s13['max_loss_streak']} losses in a row ({s13['worst_streak_pts']:.1f} pts)")
print(f"Pos weeks V12/V13: {pos_w_v12}/{len(weeks)} vs {pos_w_v13}/{len(weeks)}")
print(f"Pos days V12/V13:  {pos_days_v12}/{len(days)} vs {pos_days_v13}/{len(days)}")
