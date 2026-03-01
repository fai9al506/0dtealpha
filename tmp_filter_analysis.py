"""Full filter analysis across ALL trades — test every proposed filter."""
import os, sys, json, psycopg
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from psycopg.rows import dict_row
from datetime import datetime, timedelta
import pytz, re

NY = pytz.timezone("America/New_York")
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row)

# ── Load ALL resolved trades ──
trades = c.execute("""
    SELECT s.id, s.setup_name, s.direction, s.grade, s.score,
           s.outcome_result, s.outcome_pnl, s.outcome_first_event,
           s.outcome_max_profit, s.outcome_max_loss,
           s.spot, s.lis, s.target, s.paradigm, s.comments,
           s.abs_es_price, s.abs_vol_ratio,
           s.ts AT TIME ZONE 'America/New_York' as ts_et,
           s.ts::date as trade_date
    FROM setup_log s
    WHERE s.outcome_result IS NOT NULL
    ORDER BY s.id
""").fetchall()

print(f"Total resolved trades: {len(trades)}")

# ── Load vanna data for GEX Long filter ──
# Get aggregated vanna ALL sum per snapshot timestamp, then match to trade
vanna_data = c.execute("""
    SELECT ts_utc, SUM(value) as total_vanna
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'ALL'
    GROUP BY ts_utc
    ORDER BY ts_utc
""").fetchall()

# Build a lookup: for each trade, find nearest vanna snapshot
vanna_list = [(v['ts_utc'], float(v['total_vanna'])) for v in vanna_data]

def get_nearest_vanna(trade_ts):
    """Find nearest vanna snapshot value before trade timestamp."""
    if not vanna_list:
        return None
    best = None
    for vts, vval in vanna_list:
        if vts <= trade_ts:
            best = vval
        else:
            break
    return best

# ── Load DD shift data for threshold filter ──
# Parse DD from comments (dd_shift is stored in comments for DD trades)
def parse_dd_from_comments(comments):
    """Extract DD value from comments like 'DD: $-5,231,144,679'"""
    if not comments:
        return None
    m = re.search(r'DD:\s*\$([+-]?[\d,]+)', comments)
    if m:
        try:
            return int(m.group(1).replace(',', ''))
        except:
            pass
    return None

# ── Helper functions ──
def stats(trade_list):
    """Compute W/L/E, WR, PnL, PF, MaxDD for a list of trades."""
    wins = sum(1 for t in trade_list if t['outcome_result'] == 'WIN')
    losses = sum(1 for t in trade_list if t['outcome_result'] == 'LOSS')
    expired = sum(1 for t in trade_list if t['outcome_result'] == 'EXPIRED')
    decided = wins + losses
    wr = wins / decided * 100 if decided > 0 else 0
    pnl = sum(float(t['outcome_pnl'] or 0) for t in trade_list)

    # Profit factor
    gross_win = sum(float(t['outcome_pnl'] or 0) for t in trade_list if (t['outcome_pnl'] or 0) > 0)
    gross_loss = abs(sum(float(t['outcome_pnl'] or 0) for t in trade_list if (t['outcome_pnl'] or 0) < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')

    # Max drawdown
    cum = 0
    peak = 0
    max_dd = 0
    for t in trade_list:
        cum += float(t['outcome_pnl'] or 0)
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    return {
        'count': len(trade_list), 'wins': wins, 'losses': losses, 'expired': expired,
        'wr': wr, 'pnl': round(pnl, 1), 'pf': round(pf, 2), 'max_dd': round(max_dd, 1),
        'avg_win': round(gross_win / wins, 1) if wins > 0 else 0,
        'avg_loss': round(-gross_loss / losses, 1) if losses > 0 else 0,
    }

def print_stats(label, s, baseline=None):
    delta = f" ({s['pnl'] - baseline['pnl']:+.1f})" if baseline else ""
    print(f"  {label}")
    print(f"    {s['count']} trades | {s['wins']}W/{s['losses']}L/{s['expired']}E | "
          f"WR={s['wr']:.1f}% | PnL={s['pnl']:+.1f}{delta} | PF={s['pf']:.2f} | MaxDD={s['max_dd']:.1f}")
    print(f"    Avg win={s['avg_win']:+.1f} | Avg loss={s['avg_loss']:.1f}")

# ═══════════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"  FILTER ANALYSIS — ALL {len(trades)} RESOLVED TRADES")
print(f"{'='*80}")

# ── BASELINE ──
baseline = stats(trades)
print(f"\n{'─'*80}")
print(f"  BASELINE (no filters)")
print(f"{'─'*80}")
print_stats("All trades", baseline)

# Per-setup baseline
print(f"\n  Per-setup:")
for name in ['DD Exhaustion', 'ES Absorption', 'BofA Scalp', 'AG Short', 'GEX Long', 'Paradigm Reversal']:
    st = [t for t in trades if t['setup_name'] == name]
    if st:
        s = stats(st)
        print(f"    {name:20s} {s['count']:3d} | {s['wins']}W/{s['losses']}L | WR={s['wr']:5.1f}% | PnL={s['pnl']:+7.1f} | PF={s['pf']:.2f}")

# ═══════════════════════════════════════════════════════════════════
# FILTER 1: DD 14:00 cutoff
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  FILTER 1: DD cutoff 14:00 ET (was 15:30)")
print(f"{'─'*80}")

dd_blocked_f1 = [t for t in trades if t['setup_name'] == 'DD Exhaustion'
                 and t['ts_et'] and t['ts_et'].hour >= 14]
dd_kept_f1 = [t for t in trades if not (t['setup_name'] == 'DD Exhaustion'
              and t['ts_et'] and t['ts_et'].hour >= 14)]

blocked_s = stats(dd_blocked_f1)
kept_s = stats(dd_kept_f1)
print(f"  Blocked: {blocked_s['count']} DD trades after 14:00")
print(f"    Blocked stats: {blocked_s['wins']}W/{blocked_s['losses']}L WR={blocked_s['wr']:.1f}% PnL={blocked_s['pnl']:+.1f}")
print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# FILTER 2: Block DD in BOFA-PURE paradigm
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  FILTER 2: Block DD in BOFA-PURE paradigm")
print(f"{'─'*80}")

dd_blocked_f2 = [t for t in trades if t['setup_name'] == 'DD Exhaustion'
                 and t['paradigm'] and 'BOFA' in t['paradigm'].upper() and 'PURE' in t['paradigm'].upper()]
dd_kept_f2 = [t for t in trades if not (t['setup_name'] == 'DD Exhaustion'
              and t['paradigm'] and 'BOFA' in t['paradigm'].upper() and 'PURE' in t['paradigm'].upper())]

blocked_s = stats(dd_blocked_f2)
kept_s = stats(dd_kept_f2)
print(f"  Blocked: {blocked_s['count']} DD trades in BOFA-PURE")
print(f"    Blocked stats: {blocked_s['wins']}W/{blocked_s['losses']}L WR={blocked_s['wr']:.1f}% PnL={blocked_s['pnl']:+.1f}")
print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# FILTER 3: Raise DD threshold to $500M
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  FILTER 3: DD shift threshold $200M -> $500M")
print(f"{'─'*80}")

# Parse DD from comments
dd_with_shift = []
for t in trades:
    if t['setup_name'] != 'DD Exhaustion':
        continue
    dd_val = parse_dd_from_comments(t['comments'])
    if dd_val is not None:
        dd_with_shift.append((t, abs(dd_val)))

dd_blocked_f3 = [t for t, dd in dd_with_shift if dd < 500_000_000]
dd_kept_f3 = [t for t in trades if not any(t['id'] == bt['id'] for bt in dd_blocked_f3)]

if dd_with_shift:
    blocked_s = stats(dd_blocked_f3)
    kept_s = stats(dd_kept_f3)
    print(f"  DD trades with parseable shift: {len(dd_with_shift)}")
    print(f"  Blocked: {blocked_s['count']} DD trades with |shift| < $500M")
    if blocked_s['count'] > 0:
        print(f"    Blocked stats: {blocked_s['wins']}W/{blocked_s['losses']}L WR={blocked_s['wr']:.1f}% PnL={blocked_s['pnl']:+.1f}")
        print_stats("After filter", kept_s, baseline)
    else:
        print(f"    No trades blocked (all DD shifts >= $500M)")
else:
    print(f"  Cannot parse DD shift from comments — skipping")

# ═══════════════════════════════════════════════════════════════════
# FILTER 4: GEX Long vanna filter (block when vanna ALL < 0)
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  FILTER 4: GEX Long — block when aggregated vanna ALL < 0")
print(f"{'─'*80}")

gex_trades = [t for t in trades if t['setup_name'] == 'GEX Long']
gex_with_vanna = []
for t in gex_trades:
    v = get_nearest_vanna(t['ts_et'].replace(tzinfo=NY) if t['ts_et'].tzinfo is None else t['ts_et'])
    if v is not None:
        gex_with_vanna.append((t, v))

gex_neg_vanna = [t for t, v in gex_with_vanna if v < 0]
gex_pos_vanna = [t for t, v in gex_with_vanna if v >= 0]

print(f"  GEX Long trades with vanna data: {len(gex_with_vanna)}/{len(gex_trades)}")
if gex_neg_vanna:
    neg_s = stats(gex_neg_vanna)
    pos_s = stats(gex_pos_vanna) if gex_pos_vanna else None
    print(f"  Negative vanna (BLOCK): {neg_s['count']} trades | {neg_s['wins']}W/{neg_s['losses']}L WR={neg_s['wr']:.1f}% PnL={neg_s['pnl']:+.1f}")
    if pos_s:
        print(f"  Positive vanna (KEEP):  {pos_s['count']} trades | {pos_s['wins']}W/{pos_s['losses']}L WR={pos_s['wr']:.1f}% PnL={pos_s['pnl']:+.1f}")

    # Impact on total
    kept_f4 = [t for t in trades if not (t['setup_name'] == 'GEX Long' and t['id'] in [x['id'] for x in gex_neg_vanna])]
    kept_s = stats(kept_f4)
    print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# FILTER 5: Disable GEX Long entirely
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  FILTER 5: Disable GEX Long entirely")
print(f"{'─'*80}")

no_gex = [t for t in trades if t['setup_name'] != 'GEX Long']
gex_s = stats(gex_trades)
kept_s = stats(no_gex)
print(f"  GEX Long removed: {gex_s['count']} trades | PnL={gex_s['pnl']:+.1f}")
print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# COMBINED: F1 + F2 (DD 14:00 cutoff + block BOFA-PURE)
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  COMBINED: F1 + F2 (DD 14:00 + block BOFA-PURE)")
print(f"{'─'*80}")

def is_blocked_f1f2(t):
    if t['setup_name'] != 'DD Exhaustion':
        return False
    if t['ts_et'] and t['ts_et'].hour >= 14:
        return True
    if t['paradigm'] and 'BOFA' in t['paradigm'].upper() and 'PURE' in t['paradigm'].upper():
        return True
    return False

blocked_f1f2 = [t for t in trades if is_blocked_f1f2(t)]
kept_f1f2 = [t for t in trades if not is_blocked_f1f2(t)]

blocked_s = stats(blocked_f1f2)
kept_s = stats(kept_f1f2)
print(f"  Blocked: {blocked_s['count']} DD trades")
print(f"    Blocked stats: {blocked_s['wins']}W/{blocked_s['losses']}L WR={blocked_s['wr']:.1f}% PnL={blocked_s['pnl']:+.1f}")
print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# COMBINED: F1 + F2 + F4 (DD filters + GEX vanna)
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  COMBINED: F1 + F2 + F4 (DD filters + GEX vanna block)")
print(f"{'─'*80}")

gex_neg_ids = set(t['id'] for t in gex_neg_vanna) if gex_neg_vanna else set()
kept_all = [t for t in trades if not is_blocked_f1f2(t)
            and not (t['setup_name'] == 'GEX Long' and t['id'] in gex_neg_ids)]
blocked_all = [t for t in trades if t not in kept_all]

blocked_s = stats(blocked_all)
kept_s = stats(kept_all)
print(f"  Total blocked: {blocked_s['count']} trades")
print(f"    Blocked stats: {blocked_s['wins']}W/{blocked_s['losses']}L WR={blocked_s['wr']:.1f}% PnL={blocked_s['pnl']:+.1f}")
print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# COMBINED: F1 + F2 + F5 (DD filters + disable GEX Long)
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  COMBINED: F1 + F2 + F5 (DD filters + disable GEX)")
print(f"{'─'*80}")

kept_no_gex = [t for t in trades if not is_blocked_f1f2(t)
               and t['setup_name'] != 'GEX Long']
kept_s = stats(kept_no_gex)
print_stats("After filter", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# NUCLEAR: Only keep ES Absorption + AG Short + Paradigm
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  NUCLEAR: Only ES Absorption + AG Short + Paradigm")
print(f"{'─'*80}")

best_only = [t for t in trades if t['setup_name'] in ('ES Absorption', 'AG Short', 'Paradigm Reversal')]
kept_s = stats(best_only)
print_stats("Best 3 only", kept_s, baseline)

# ═══════════════════════════════════════════════════════════════════
# PER-FILTER IMPACT SUMMARY
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"  FILTER IMPACT SUMMARY")
print(f"{'='*80}")
print(f"  {'Filter':<45s} {'Trades':>7s} {'PnL':>8s} {'Delta':>8s} {'WR':>6s} {'PF':>5s} {'MaxDD':>6s}")
print(f"  {'─'*85}")

combos = [
    ("Baseline (no filters)", trades),
    ("F1: DD cutoff 14:00", dd_kept_f1),
    ("F2: Block DD BOFA-PURE", dd_kept_f2),
    ("F4: GEX vanna < 0 block", kept_f4 if gex_neg_vanna else trades),
    ("F5: Disable GEX Long", no_gex),
    ("F1+F2: DD 14:00 + BOFA-PURE", kept_f1f2),
    ("F1+F2+F4: + GEX vanna", kept_all),
    ("F1+F2+F5: + disable GEX", kept_no_gex),
    ("Nuclear: Abs+AG+Para only", best_only),
]

for label, tlist in combos:
    s = stats(tlist)
    delta = s['pnl'] - baseline['pnl']
    print(f"  {label:<45s} {s['count']:>7d} {s['pnl']:>+8.1f} {delta:>+8.1f} {s['wr']:>5.1f}% {s['pf']:>5.2f} {s['max_dd']:>6.1f}")

# ═══════════════════════════════════════════════════════════════════
# DD time-of-day breakdown (hourly, all time)
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  DD EXHAUSTION: HOURLY BREAKDOWN (ALL TIME)")
print(f"{'─'*80}")

dd_all = [t for t in trades if t['setup_name'] == 'DD Exhaustion']
dd_hourly = {}
for t in dd_all:
    h = t['ts_et'].hour if t['ts_et'] else 0
    if h not in dd_hourly:
        dd_hourly[h] = []
    dd_hourly[h].append(t)

for h in sorted(dd_hourly):
    s = stats(dd_hourly[h])
    print(f"  {h:02d}:00  {s['count']:3d} trades | {s['wins']}W/{s['losses']}L | WR={s['wr']:5.1f}% | PnL={s['pnl']:+7.1f} | AvgW={s['avg_win']:+.1f} AvgL={s['avg_loss']:.1f}")

# ═══════════════════════════════════════════════════════════════════
# DD paradigm breakdown (all time)
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  DD EXHAUSTION: PARADIGM BREAKDOWN (ALL TIME)")
print(f"{'─'*80}")

dd_para = {}
for t in dd_all:
    p = t['paradigm'] or 'Unknown'
    # Normalize
    p_upper = p.upper()
    if 'BOFA' in p_upper and 'PURE' in p_upper:
        key = 'BOFA-PURE'
    elif 'BOFA' in p_upper and 'MESSY' in p_upper:
        key = 'BOFA-MESSY'
    elif 'BOFA' in p_upper and 'LIS' in p_upper:
        key = 'BofA-LIS'
    elif 'GEX' in p_upper:
        key = 'GEX-*'
    elif 'AG' in p_upper or 'ANTI' in p_upper:
        key = 'AG/Anti-GEX'
    elif 'MESSY' in p_upper:
        key = 'MESSY'
    else:
        key = p
    if key not in dd_para:
        dd_para[key] = []
    dd_para[key].append(t)

for p in sorted(dd_para, key=lambda x: -stats(dd_para[x])['pnl']):
    s = stats(dd_para[p])
    print(f"  {p:20s} {s['count']:3d} trades | {s['wins']}W/{s['losses']}L | WR={s['wr']:5.1f}% | PnL={s['pnl']:+7.1f}")

# ═══════════════════════════════════════════════════════════════════
# Feb 27 filter impact specifically
# ═══════════════════════════════════════════════════════════════════
print(f"\n{'─'*80}")
print(f"  FEB 27 SPECIFIC FILTER IMPACT")
print(f"{'─'*80}")

feb27 = [t for t in trades if t['trade_date'] and str(t['trade_date']) == '2026-02-27']
if feb27:
    feb27_base = stats(feb27)
    print_stats("Feb 27 baseline", feb27_base)

    # F1 on Feb 27
    feb27_f1 = [t for t in feb27 if not (t['setup_name'] == 'DD Exhaustion' and t['ts_et'] and t['ts_et'].hour >= 14)]
    feb27_f1_blocked = [t for t in feb27 if t['setup_name'] == 'DD Exhaustion' and t['ts_et'] and t['ts_et'].hour >= 14]
    s = stats(feb27_f1)
    blocked_pnl = sum(float(t['outcome_pnl'] or 0) for t in feb27_f1_blocked)
    print(f"\n  F1 (DD 14:00 cutoff) on Feb 27:")
    print(f"    Would block {len(feb27_f1_blocked)} trades worth {blocked_pnl:+.1f} pts")
    print(f"    Remaining: {s['count']} trades, PnL={s['pnl']:+.1f} ({s['pnl'] - feb27_base['pnl']:+.1f} impact)")

    # F2 on Feb 27
    feb27_f2 = [t for t in feb27 if not (t['setup_name'] == 'DD Exhaustion'
                and t['paradigm'] and 'BOFA' in t['paradigm'].upper() and 'PURE' in t['paradigm'].upper())]
    feb27_f2_blocked = [t for t in feb27 if t['setup_name'] == 'DD Exhaustion'
                        and t['paradigm'] and 'BOFA' in t['paradigm'].upper() and 'PURE' in t['paradigm'].upper()]
    if feb27_f2_blocked:
        blocked_pnl = sum(float(t['outcome_pnl'] or 0) for t in feb27_f2_blocked)
        s = stats(feb27_f2)
        print(f"\n  F2 (Block DD BOFA-PURE) on Feb 27:")
        print(f"    Would block {len(feb27_f2_blocked)} trades worth {blocked_pnl:+.1f} pts")
        for bt in feb27_f2_blocked:
            print(f"      #{bt['id']} {bt['ts_et'].strftime('%H:%M')} {bt['outcome_result']} {float(bt['outcome_pnl'] or 0):+.1f}")
        print(f"    Remaining: {s['count']} trades, PnL={s['pnl']:+.1f} ({s['pnl'] - feb27_base['pnl']:+.1f} impact)")

c.close()
