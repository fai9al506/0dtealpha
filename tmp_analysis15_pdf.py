"""Generate Analysis #15 PDF: Zero-Drawdown Study — DD Hedging Alignment Discovery."""
import sys, io, os, re, math
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from dotenv import load_dotenv; load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL: DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
if "postgresql://" in DB_URL and "postgresql+psycopg" not in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
from sqlalchemy import create_engine, text
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

engine = create_engine(DB_URL)

def parse_money(s):
    if not s: return None
    s = str(s).strip(); neg = '-' in s
    digits = re.sub(r'[^0-9.]', '', s)
    if not digits: return None
    val = float(digits)
    return -val if neg else val

# ============================================================
# FETCH ALL DATA
# ============================================================
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
        all_trades.append(d)

print(f"Loaded {len(all_trades)} trades")

def is_long(t):
    return t['direction'] in ('long', 'bullish')

def dd_aligned(t):
    if t['dd_num'] is None: return 'no_data'
    sign = 1 if is_long(t) else -1
    val = t['dd_num'] * sign
    if val > 200_000_000: return 'ALIGNED'
    elif val < -200_000_000: return 'OPPOSED'
    return 'NEUTRAL'

# ============================================================
# FILTER DEFINITIONS
# ============================================================
def passes_v9sc(t):
    name = t['setup_name']; direction = t['direction']
    align = t['greek_alignment'] or 0; vix = t['vix'] or 0; overvix = t['overvix'] or 0
    if direction in ('short', 'bearish'):
        if name == 'Skew Charm': return True
        if name == 'AG Short': return True
        if name == 'DD Exhaustion' and align != 0: return True
        return False
    if direction in ('long', 'bullish'):
        if align < 2: return False
        if name == 'Skew Charm': return True
        if vix <= 22: return True
        if overvix >= 2: return True
        return False
    return False

def passes_sc_dd(t):
    if t['setup_name'] != 'Skew Charm': return False
    return dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS')

def passes_hybrid(t):
    if not passes_v9sc(t): return False
    if t['setup_name'] == 'Skew Charm':
        return dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS')
    return True

def passes_dd_portfolio(t):
    name = t['setup_name']
    if name == 'Skew Charm':
        return dd_aligned(t) == 'ALIGNED' and t['paradigm'] not in ('GEX-LIS', 'AG-LIS')
    if name == 'DD Exhaustion':
        return t['direction'] == 'short' and t['vanna_all'] is not None and t['vanna_all'] >= 5e9
    if name == 'AG Short':
        return (t['greek_alignment'] or 0) <= -2
    return False

# Apply filters
filters = {
    'V9-SC (Current)': [t for t in all_trades if passes_v9sc(t)],
    'SC DD-Aligned Only': [t for t in all_trades if passes_sc_dd(t)],
    'DD-Align Portfolio': [t for t in all_trades if passes_dd_portfolio(t)],
    'Hybrid': [t for t in all_trades if passes_hybrid(t)],
}

# ============================================================
# COMPUTE METRICS
# ============================================================
def compute_metrics(trades):
    if not trades: return {}
    n = len(trades)
    wins = sum(1 for t in trades if t['outcome_result'] == 'WIN')
    tot = sum(t['outcome_pnl'] for t in trades)
    pos = sum(t['outcome_pnl'] for t in trades if t['outcome_pnl'] > 0)
    neg = sum(-t['outcome_pnl'] for t in trades if t['outcome_pnl'] < 0)
    pf = pos / neg if neg > 0 else 999
    mae_avg = sum(t['outcome_max_loss'] for t in trades) / n
    mfe_avg = sum(t['outcome_max_profit'] for t in trades) / n
    zero_dd = sum(1 for t in trades if t['outcome_max_loss'] >= -1.0)
    low_dd = sum(1 for t in trades if t['outcome_max_loss'] >= -2.0)

    daily = defaultdict(float)
    for t in trades: daily[t['day']] += t['outcome_pnl']
    dpnls = [daily[d] for d in sorted(daily.keys())]
    n_days = len(dpnls)
    avg_day = tot / n_days if n_days else 0

    running = 0; peak = 0; max_dd = 0
    equity = []
    for p in dpnls:
        running += p
        peak = max(peak, running)
        dd = running - peak
        max_dd = min(max_dd, dd)
        equity.append(running)

    if len(dpnls) > 1:
        mean_d = sum(dpnls) / len(dpnls)
        var_d = sum((p - mean_d)**2 for p in dpnls) / (len(dpnls) - 1)
        sharpe = mean_d / math.sqrt(var_d) if var_d > 0 else 0
    else:
        sharpe = 0

    recovery = tot / abs(max_dd) if max_dd != 0 else 999
    max_consec = 0; cur = 0
    for t in sorted(trades, key=lambda x: x['ts']):
        if t['outcome_result'] != 'WIN': cur += 1; max_consec = max(max_consec, cur)
        else: cur = 0

    return {
        'n': n, 'wins': wins, 'wr': 100*wins/n, 'tot_pnl': tot, 'avg_pnl': tot/n,
        'pf': pf, 'mae_avg': mae_avg, 'mfe_avg': mfe_avg,
        'zero_dd_pct': 100*zero_dd/n, 'low_dd_pct': 100*low_dd/n,
        'max_dd': max_dd, 'n_days': n_days, 'avg_day': avg_day,
        'sharpe': sharpe, 'recovery': recovery, 'max_consec': max_consec,
        'equity': equity, 'days': sorted(daily.keys()), 'dpnls': dpnls,
        'worst_day': min(dpnls) if dpnls else 0, 'best_day': max(dpnls) if dpnls else 0,
        'avg_win': sum(t['outcome_pnl'] for t in trades if t['outcome_result']=='WIN')/wins if wins else 0,
        'avg_loss': sum(t['outcome_pnl'] for t in trades if t['outcome_result']!='WIN')/(n-wins) if n-wins else 0,
    }

metrics = {k: compute_metrics(v) for k, v in filters.items()}

# ============================================================
# STYLING
# ============================================================
DARK_BG = '#1a1a2e'
PANEL_BG = '#16213e'
CARD_BG = '#0f3460'
ACCENT_GREEN = '#00e676'
ACCENT_RED = '#ff5252'
ACCENT_BLUE = '#448aff'
ACCENT_GOLD = '#ffd740'
ACCENT_PURPLE = '#e040fb'
TEXT_WHITE = '#ffffff'
TEXT_LIGHT = '#b0bec5'
TEXT_DIM = '#607d8b'

plt.rcParams.update({
    'figure.facecolor': DARK_BG,
    'axes.facecolor': PANEL_BG,
    'axes.edgecolor': TEXT_DIM,
    'axes.labelcolor': TEXT_LIGHT,
    'text.color': TEXT_WHITE,
    'xtick.color': TEXT_LIGHT,
    'ytick.color': TEXT_LIGHT,
    'grid.color': '#263859',
    'grid.alpha': 0.5,
    'font.family': 'sans-serif',
    'font.size': 9,
})

FILTER_COLORS = {
    'V9-SC (Current)': ACCENT_BLUE,
    'SC DD-Aligned Only': ACCENT_GOLD,
    'DD-Align Portfolio': ACCENT_GREEN,
    'Hybrid': ACCENT_PURPLE,
}

pdf_path = os.path.join(os.path.dirname(__file__), 'Analysis_15_Zero_DD_Study.pdf')

with PdfPages(pdf_path) as pdf:

    # ============================================================
    # PAGE 1: TITLE + EXECUTIVE SUMMARY
    # ============================================================
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(DARK_BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100); ax.set_ylim(0, 100)
    ax.axis('off'); ax.set_facecolor(DARK_BG)

    # Title
    ax.text(50, 92, 'ANALYSIS #15', fontsize=28, fontweight='bold', ha='center', color=ACCENT_GOLD, fontfamily='sans-serif')
    ax.text(50, 86, 'Zero-Drawdown Study: DD Hedging Alignment Discovery', fontsize=16, ha='center', color=TEXT_WHITE)
    ax.text(50, 82, 'March 20, 2026  |  900 trades analyzed  |  Feb 3 - Mar 19, 2026', fontsize=10, ha='center', color=TEXT_DIM)

    # Divider
    ax.plot([10, 90], [79, 79], color=ACCENT_GOLD, linewidth=1.5, alpha=0.6)

    # Key finding box
    from matplotlib.patches import FancyBboxPatch
    box = FancyBboxPatch((8, 62), 84, 15, boxstyle="round,pad=0.5", facecolor=CARD_BG, edgecolor=ACCENT_GOLD, linewidth=2, alpha=0.9)
    ax.add_patch(box)
    ax.text(50, 73.5, 'KEY DISCOVERY', fontsize=14, fontweight='bold', ha='center', color=ACCENT_GOLD)
    ax.text(50, 70, 'When DD Hedging aligns with Skew Charm direction + non-toxic paradigm:', fontsize=10, ha='center', color=TEXT_LIGHT)
    ax.text(25, 66, '85.7% WR', fontsize=18, fontweight='bold', ha='center', color=ACCENT_GREEN)
    ax.text(50, 66, 'PF 6.44', fontsize=18, fontweight='bold', ha='center', color=ACCENT_GREEN)
    ax.text(75, 66, 'MaxDD -10 pts', fontsize=18, fontweight='bold', ha='center', color=ACCENT_GREEN)

    # Executive summary
    lines = [
        ("The Problem:", "What makes some trades achieve near-zero drawdown (MAE 0.1 pts)?"),
        ("Dataset:", "900 completed trades across 7 setups, 26 trading days"),
        ("Method:", "Correlated every Volland metric (DD hedging, aggCharm, per-strike charm,"),
        ("", "vanna, paradigm, VIX, alignment, sub-scores) with MAE outcomes"),
        ("Finding:", "DD Hedging direction alignment is the single strongest predictor"),
        ("", "of zero-drawdown entries for Skew Charm (PF 6.44 vs 1.53 baseline)"),
        ("Mechanism:", "SC catches charm mispricing. DD alignment = dealers also hedging your"),
        ("", "way. Two concurrent flows = instant favorable price movement = zero DD"),
    ]
    y = 57
    for label, text in lines:
        if label:
            ax.text(12, y, label, fontsize=9, fontweight='bold', color=ACCENT_GOLD)
        ax.text(28, y, text, fontsize=9, color=TEXT_LIGHT)
        y -= 3.5

    # Bottom note
    ax.text(50, 5, '0DTE Alpha  |  Confidential Trading Research', fontsize=8, ha='center', color=TEXT_DIM, style='italic')
    pdf.savefig(fig, facecolor=DARK_BG); plt.close()

    # ============================================================
    # PAGE 2: MAE BUCKET ANALYSIS
    # ============================================================
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle('MAE (Maximum Adverse Excursion) Analysis — 900 Trades', fontsize=14, fontweight='bold', color=ACCENT_GOLD, y=0.97)

    # Chart 1: MAE bucket WR and PnL
    ax = axes[0, 0]
    buckets = ['0 to -1', '-1 to -2', '-2 to -3', '-3 to -5', '-5 to -8', '-8+']
    bucket_trades = [146, 42, 37, 84, 102, 450]
    bucket_wr = [86.3, 78.6, 78.4, 78.6, 74.5, 21.1]
    bucket_pnl = [1325, 457, 339, 791, 697, -3031]
    colors = [ACCENT_GREEN if p > 0 else ACCENT_RED for p in bucket_pnl]
    bars = ax.bar(range(len(buckets)), bucket_wr, color=colors, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(len(buckets))); ax.set_xticklabels(buckets, fontsize=7)
    ax.set_ylabel('Win Rate %'); ax.set_title('Win Rate by MAE Bucket', fontsize=10, color=TEXT_WHITE)
    ax.axhline(y=50, color=TEXT_DIM, linestyle='--', alpha=0.5)
    for i, (wr, pnl, nt) in enumerate(zip(bucket_wr, bucket_pnl, bucket_trades)):
        ax.text(i, wr + 2, f'{wr:.0f}%\n{nt}t\n{pnl:+.0f}', ha='center', fontsize=7, color=TEXT_WHITE)
    ax.set_ylim(0, 105); ax.grid(axis='y', alpha=0.3)

    # Chart 2: Cumulative PnL contribution
    ax = axes[0, 1]
    cum = np.cumsum(bucket_pnl)
    ax.bar(range(len(buckets)), bucket_pnl, color=colors, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(len(buckets))); ax.set_xticklabels(buckets, fontsize=7)
    ax.set_ylabel('Total PnL (pts)'); ax.set_title('PnL Contribution by MAE Bucket', fontsize=10, color=TEXT_WHITE)
    ax.axhline(y=0, color=TEXT_DIM, linewidth=1)
    ax.grid(axis='y', alpha=0.3)
    for i, p in enumerate(bucket_pnl):
        ax.text(i, p + (80 if p > 0 else -200), f'{p:+,.0f}', ha='center', fontsize=7, color=TEXT_WHITE)

    # Chart 3: Zero-DD rate by setup
    ax = axes[1, 0]
    setups = ['SC', 'ES Abs', 'GEX L', 'BofA', 'AG', 'DD']
    zero_rates = [22.2, 23.2, 19.2, 18.5, 17.6, 6.4]
    setup_pnl = [510, -84, -67, -18, 176, 107]
    cols = [ACCENT_GREEN if p > 50 else ACCENT_BLUE if p > 0 else ACCENT_RED for p in setup_pnl]
    ax.barh(range(len(setups)), zero_rates, color=cols, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_yticks(range(len(setups))); ax.set_yticklabels(setups)
    ax.set_xlabel('Zero-DD Rate %'); ax.set_title('Zero-DD Rate by Setup (MAE >= -1)', fontsize=10, color=TEXT_WHITE)
    for i, (r, p) in enumerate(zip(zero_rates, setup_pnl)):
        ax.text(r + 0.5, i, f'{r:.1f}% | PnL:{p:+.0f}', va='center', fontsize=7, color=TEXT_WHITE)
    ax.grid(axis='x', alpha=0.3)

    # Chart 4: Golden trades breakdown
    ax = axes[1, 1]
    ax.axis('off')
    ax.set_title('146 "Golden Trades" Profile (MAE >= -1.0)', fontsize=10, color=ACCENT_GOLD, pad=10)
    info = [
        ('Win Rate', '91.1%', ACCENT_GREEN),
        ('Total PnL', '+1,325 pts', ACCENT_GREEN),
        ('% of All Profits', '47%', ACCENT_GOLD),
        ('% of All Trades', '16%', TEXT_LIGHT),
        ('Avg MFE', '+17.7 pts', ACCENT_GREEN),
        ('Avg PnL', '+9.1 pts', ACCENT_GREEN),
        ('Avg Hold Time', '25 min', TEXT_LIGHT),
        ('Target Hit', '86 of 146', TEXT_LIGHT),
    ]
    for i, (label, val, col) in enumerate(info):
        y_pos = 0.88 - i * 0.11
        ax.text(0.1, y_pos, label + ':', fontsize=9, color=TEXT_LIGHT, transform=ax.transAxes)
        ax.text(0.65, y_pos, val, fontsize=11, fontweight='bold', color=col, transform=ax.transAxes)

    plt.tight_layout(rect=[0, 0.02, 1, 0.94])
    pdf.savefig(fig, facecolor=DARK_BG); plt.close()

    # ============================================================
    # PAGE 3: DD HEDGING ALIGNMENT — THE DISCOVERY
    # ============================================================
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(DARK_BG)
    gs = gridspec.GridSpec(3, 2, hspace=0.45, wspace=0.3, left=0.08, right=0.95, top=0.92, bottom=0.06)
    fig.suptitle('The Discovery: DD Hedging Direction Alignment', fontsize=14, fontweight='bold', color=ACCENT_GOLD, y=0.97)

    # Chart 1: SC filter comparison bars
    ax = fig.add_subplot(gs[0, :])
    filter_names = ['SC Baseline', 'SC+DD Aligned', 'SC+DD Al\n+No Toxic', 'SC+DD Strong\n(>$1B)+No Toxic']
    filter_wr = [67.2, 80.0, 85.7, 81.2]
    filter_pf = [1.53, 3.99, 6.44, 4.71]
    filter_mae = [-8.84, -6.17, -5.53, -5.4]
    filter_n = [198, 40, 35, 16]

    x = np.arange(len(filter_names))
    w = 0.25
    bars1 = ax.bar(x - w, filter_wr, w, label='Win Rate %', color=ACCENT_GREEN, alpha=0.8)
    ax2 = ax.twinx()
    bars2 = ax2.bar(x, [p * 10 for p in filter_pf], w, label='PF x10', color=ACCENT_GOLD, alpha=0.8)
    bars3 = ax2.bar(x + w, [-m * 5 for m in filter_mae], w, label='|MAE| x5', color=ACCENT_BLUE, alpha=0.8)

    ax.set_xticks(x); ax.set_xticklabels(filter_names, fontsize=8)
    ax.set_ylabel('Win Rate %', color=ACCENT_GREEN); ax.set_ylim(0, 110)
    ax2.set_ylabel('PF x10 / |MAE| x5', color=TEXT_LIGHT)
    ax.set_title('SC Filter Comparison: WR, PF, MAE', fontsize=10, color=TEXT_WHITE)

    for i, (wr, pf, n) in enumerate(zip(filter_wr, filter_pf, filter_n)):
        ax.text(i - w, wr + 2, f'{wr:.0f}%', ha='center', fontsize=7, color=ACCENT_GREEN, fontweight='bold')
        ax.text(i, 3, f'PF {pf:.1f}', ha='center', fontsize=7, color=ACCENT_GOLD, fontweight='bold')
        ax.text(i, 100, f'n={n}', ha='center', fontsize=7, color=TEXT_DIM)

    ax.legend(loc='upper left', fontsize=7, framealpha=0.3)
    ax2.legend(loc='upper right', fontsize=7, framealpha=0.3)

    # Chart 2: DD aligned vs opposed for SC
    ax = fig.add_subplot(gs[1, 0])
    cats = ['DD Aligned\n(n=40)', 'DD Neutral\n(n=12)', 'DD Opposed\n(n=146)']
    wr_cats = [80.0, 50.0, 65.1]
    pnl_cats = [270.2, 4.6, 235.6]
    mae_cats = [-6.2, -10.6, -9.4]
    colors_cats = [ACCENT_GREEN, ACCENT_BLUE, ACCENT_RED]

    bars = ax.bar(range(3), wr_cats, color=colors_cats, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(3)); ax.set_xticklabels(cats, fontsize=8)
    ax.set_ylabel('Win Rate %')
    ax.set_title('SC: DD Hedging Alignment Effect', fontsize=10, color=TEXT_WHITE)
    for i, (wr, pnl, mae) in enumerate(zip(wr_cats, pnl_cats, mae_cats)):
        ax.text(i, wr + 2, f'{wr:.0f}%\nPnL:{pnl:+.0f}\nMAE:{mae:.1f}', ha='center', fontsize=7, color=TEXT_WHITE)
    ax.set_ylim(0, 100); ax.grid(axis='y', alpha=0.3)

    # Chart 3: Toxic vs clean paradigms
    ax = fig.add_subplot(gs[1, 1])
    paras = ['SIDIAL-MESSY', 'GEX-PURE', 'AG-TARGET', 'BOFA-PURE', 'AG-LIS', 'GEX-LIS']
    para_ldd = [54.5, 41.7, 36.4, 21.1, 25.0, 26.7]
    para_wr = [90.9, 91.7, 90.9, 61.8, 50.0, 40.0]
    para_pnl = [9.2, 6.25, 5.47, 2.04, -3.93, -5.35]
    cols = [ACCENT_GREEN if p > 5 else ACCENT_BLUE if p > 0 else ACCENT_RED for p in para_pnl]

    ax.barh(range(len(paras)), para_wr, color=cols, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_yticks(range(len(paras))); ax.set_yticklabels(paras, fontsize=8)
    ax.set_xlabel('Win Rate %')
    ax.set_title('SC by Paradigm (WR + low-DD rate)', fontsize=10, color=TEXT_WHITE)
    for i, (wr, ldd, pnl) in enumerate(zip(para_wr, para_ldd, para_pnl)):
        ax.text(wr + 1, i, f'WR:{wr:.0f}% | lowDD:{ldd:.0f}% | PnL:{pnl:+.1f}', va='center', fontsize=7, color=TEXT_WHITE)
    ax.axvline(x=65, color=ACCENT_GOLD, linestyle='--', alpha=0.5, label='V9-SC avg')
    ax.grid(axis='x', alpha=0.3)
    # Mark toxic
    ax.text(5, 4, 'TOXIC', fontsize=8, fontweight='bold', color=ACCENT_RED)
    ax.text(5, 5, 'TOXIC', fontsize=8, fontweight='bold', color=ACCENT_RED)

    # Chart 4: Mechanism explanation
    ax = fig.add_subplot(gs[2, :])
    ax.axis('off')
    ax.set_facecolor(CARD_BG)

    ax.text(0.5, 0.95, 'WHY DD ALIGNMENT CREATES ZERO DRAWDOWN', fontsize=12, fontweight='bold',
            ha='center', color=ACCENT_GOLD, transform=ax.transAxes)

    mechanism = [
        "1. Skew Charm detects a CHARM MISPRICING:  options expiry pressure is about to move the underlying",
        "2. DD Hedging aligned means DEALERS ARE ALSO HEDGING in your direction:  delta-decay flow supports the trade",
        "3. TWO CONCURRENT FLOWS push price your way:  charm reversal + DD hedging = instant favorable movement",
        "4. Result: price moves immediately in trade direction = NO adverse excursion = ZERO drawdown",
        "",
        "When DD OPPOSES direction: SC is right eventually (65% WR) but DD flow fights the reversal first = higher MAE",
        "Toxic paradigms (GEX-LIS, AG-LIS): strong dealer positioning regimes that RESIST the charm reversal = lower WR",
    ]
    for i, line in enumerate(mechanism):
        col = ACCENT_GREEN if line.startswith(('1.','2.','3.','4.')) else ACCENT_RED if 'OPPOSES' in line or 'Toxic' in line else TEXT_LIGHT
        ax.text(0.05, 0.78 - i * 0.12, line, fontsize=8.5, color=col, transform=ax.transAxes)

    pdf.savefig(fig, facecolor=DARK_BG); plt.close()

    # ============================================================
    # PAGE 4: FULL FILTER COMPARISON
    # ============================================================
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(DARK_BG)
    gs = gridspec.GridSpec(3, 2, hspace=0.45, wspace=0.3, left=0.08, right=0.95, top=0.92, bottom=0.06)
    fig.suptitle('Full Filter Comparison: V9-SC vs DD-Alignment Filters', fontsize=14, fontweight='bold', color=ACCENT_GOLD, y=0.97)

    filter_order = ['V9-SC (Current)', 'DD-Align Portfolio', 'SC DD-Aligned Only', 'Hybrid']
    f_cols = [FILTER_COLORS[f] for f in filter_order]

    # Chart 1: Equity curves
    ax = fig.add_subplot(gs[0, :])
    for fname, col in zip(filter_order, f_cols):
        m = metrics[fname]
        ax.plot(range(len(m['equity'])), m['equity'], color=col, linewidth=2, label=f"{fname} (+{m['tot_pnl']:.0f})", alpha=0.9)
    ax.axhline(y=0, color=TEXT_DIM, linewidth=0.5)
    ax.set_ylabel('Cumulative PnL (pts)'); ax.set_xlabel('Trading Day')
    ax.set_title('Equity Curves', fontsize=10, color=TEXT_WHITE)
    ax.legend(fontsize=7, loc='upper left', framealpha=0.3)
    ax.grid(alpha=0.3)

    # Chart 2: Key metrics comparison - grouped bars
    ax = fig.add_subplot(gs[1, 0])
    metric_labels = ['WR%', 'PF', 'Sharpe', 'Recovery\nFactor/5']
    x = np.arange(len(metric_labels))
    w = 0.18
    for i, (fname, col) in enumerate(zip(filter_order, f_cols)):
        m = metrics[fname]
        vals = [m['wr'], m['pf'] * 10, m['sharpe'] * 50, m['recovery'] / 5]
        ax.bar(x + i * w - 1.5*w, vals, w, color=col, alpha=0.8, label=fname, edgecolor='white', linewidth=0.3)
    ax.set_xticks(x); ax.set_xticklabels(metric_labels, fontsize=8)
    ax.set_title('Performance Metrics', fontsize=10, color=TEXT_WHITE)
    ax.legend(fontsize=6, framealpha=0.3)
    ax.grid(axis='y', alpha=0.3)

    # Chart 3: Risk metrics - MaxDD, worst day, avg MAE
    ax = fig.add_subplot(gs[1, 1])
    risk_labels = ['Max DD', 'Worst Day', 'Avg MAE']
    x = np.arange(len(risk_labels))
    for i, (fname, col) in enumerate(zip(filter_order, f_cols)):
        m = metrics[fname]
        vals = [abs(m['max_dd']), abs(m['worst_day']), abs(m['mae_avg']) * 5]
        ax.bar(x + i * w - 1.5*w, vals, w, color=col, alpha=0.8, label=fname, edgecolor='white', linewidth=0.3)
    ax.set_xticks(x); ax.set_xticklabels(risk_labels, fontsize=8)
    ax.set_ylabel('Points (lower = better)')
    ax.set_title('Risk Metrics (lower = better)', fontsize=10, color=TEXT_WHITE)
    ax.legend(fontsize=6, framealpha=0.3)
    ax.grid(axis='y', alpha=0.3)

    # Chart 4: Summary table
    ax = fig.add_subplot(gs[2, :])
    ax.axis('off')
    ax.set_facecolor(PANEL_BG)

    cols_header = ['Metric'] + filter_order
    table_data = [
        ['Trades', *[f"{metrics[f]['n']}" for f in filter_order]],
        ['Total PnL', *[f"+{metrics[f]['tot_pnl']:.0f}" for f in filter_order]],
        ['PnL/trade', *[f"+{metrics[f]['avg_pnl']:.1f}" for f in filter_order]],
        ['PnL/day', *[f"+{metrics[f]['avg_day']:.1f}" for f in filter_order]],
        ['Win Rate', *[f"{metrics[f]['wr']:.1f}%" for f in filter_order]],
        ['Profit Factor', *[f"{metrics[f]['pf']:.2f}" for f in filter_order]],
        ['Max DD', *[f"{metrics[f]['max_dd']:.1f}" for f in filter_order]],
        ['Sharpe', *[f"{metrics[f]['sharpe']:.2f}" for f in filter_order]],
        ['Recovery', *[f"{metrics[f]['recovery']:.1f}" for f in filter_order]],
        ['Max Consec Loss', *[f"{metrics[f]['max_consec']}" for f in filter_order]],
        ['Zero-DD Rate', *[f"{metrics[f]['zero_dd_pct']:.1f}%" for f in filter_order]],
    ]

    table = ax.table(cellText=table_data, colLabels=cols_header, loc='center',
                     cellLoc='center', colWidths=[0.16, 0.21, 0.21, 0.21, 0.21])
    table.auto_set_font_size(False); table.set_fontsize(8)
    for key, cell in table.get_celld().items():
        cell.set_facecolor(PANEL_BG); cell.set_edgecolor(TEXT_DIM)
        cell.set_text_props(color=TEXT_WHITE)
        if key[0] == 0:
            cell.set_facecolor(CARD_BG); cell.set_text_props(color=ACCENT_GOLD, fontweight='bold')
        if key[1] == 0:
            cell.set_text_props(color=TEXT_LIGHT, fontweight='bold')
        # Highlight best values
        if key[0] > 0 and key[1] == 3:  # SC DD-Aligned column
            cell.set_text_props(color=ACCENT_GREEN, fontweight='bold')

    pdf.savefig(fig, facecolor=DARK_BG); plt.close()

    # ============================================================
    # PAGE 5: DOLLAR PROJECTIONS + DAILY EQUITY
    # ============================================================
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(DARK_BG)
    gs = gridspec.GridSpec(2, 2, hspace=0.4, wspace=0.3, left=0.08, right=0.95, top=0.92, bottom=0.06)
    fig.suptitle('Dollar Projections & Daily Performance (8 MES = $40/pt)', fontsize=14, fontweight='bold', color=ACCENT_GOLD, y=0.97)

    # Chart 1: Monthly $ projection
    ax = fig.add_subplot(gs[0, 0])
    monthly = [metrics[f]['avg_day'] * 40 * 21 for f in filter_order]
    bars = ax.bar(range(4), monthly, color=f_cols, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(4)); ax.set_xticklabels([f.replace(' ', '\n') for f in filter_order], fontsize=7)
    ax.set_ylabel('$/month')
    ax.set_title('Estimated Monthly Income', fontsize=10, color=TEXT_WHITE)
    for i, m in enumerate(monthly):
        ax.text(i, m + 500, f'${m:,.0f}', ha='center', fontsize=9, fontweight='bold', color=TEXT_WHITE)
    ax.grid(axis='y', alpha=0.3)

    # Chart 2: Max DD in dollars
    ax = fig.add_subplot(gs[0, 1])
    dd_dollars = [abs(metrics[f]['max_dd']) * 40 for f in filter_order]
    bars = ax.bar(range(4), dd_dollars, color=f_cols, alpha=0.8, edgecolor='white', linewidth=0.5)
    ax.set_xticks(range(4)); ax.set_xticklabels([f.replace(' ', '\n') for f in filter_order], fontsize=7)
    ax.set_ylabel('Max Drawdown ($)')
    ax.set_title('Maximum Drawdown (lower = better)', fontsize=10, color=TEXT_WHITE)
    for i, d in enumerate(dd_dollars):
        ax.text(i, d + 50, f'-${d:,.0f}', ha='center', fontsize=9, fontweight='bold', color=ACCENT_RED)
    ax.grid(axis='y', alpha=0.3)

    # Chart 3: Daily PnL distribution - V9-SC
    ax = fig.add_subplot(gs[1, 0])
    dpnls_v9 = metrics['V9-SC (Current)']['dpnls']
    dpnls_sc = metrics['SC DD-Aligned Only']['dpnls']
    cols_v9 = [ACCENT_GREEN if p > 0 else ACCENT_RED for p in dpnls_v9]
    ax.bar(range(len(dpnls_v9)), dpnls_v9, color=cols_v9, alpha=0.8, edgecolor='white', linewidth=0.3)
    ax.axhline(y=0, color=TEXT_DIM, linewidth=0.5)
    ax.set_ylabel('Daily PnL (pts)'); ax.set_xlabel('Trading Day')
    ax.set_title('V9-SC Daily PnL', fontsize=10, color=ACCENT_BLUE)
    ax.grid(axis='y', alpha=0.3)

    # Chart 4: Daily PnL distribution - SC DD
    ax = fig.add_subplot(gs[1, 1])
    cols_sc = [ACCENT_GREEN if p > 0 else ACCENT_RED for p in dpnls_sc]
    ax.bar(range(len(dpnls_sc)), dpnls_sc, color=cols_sc, alpha=0.8, edgecolor='white', linewidth=0.3)
    ax.axhline(y=0, color=TEXT_DIM, linewidth=0.5)
    ax.set_ylabel('Daily PnL (pts)'); ax.set_xlabel('Trading Day')
    ax.set_title('SC DD-Aligned Daily PnL', fontsize=10, color=ACCENT_GOLD)
    ax.grid(axis='y', alpha=0.3)
    # Same y-axis scale for comparison
    yl = max(abs(min(dpnls_v9)), abs(max(dpnls_v9)), abs(min(dpnls_sc)), abs(max(dpnls_sc))) * 1.2
    axes_list = fig.get_axes()
    axes_list[2].set_ylim(-yl, yl)
    axes_list[3].set_ylim(-yl, yl)

    pdf.savefig(fig, facecolor=DARK_BG); plt.close()

    # ============================================================
    # PAGE 6: EXPECTATIONS & RECOMMENDATIONS
    # ============================================================
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(DARK_BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100); ax.set_ylim(0, 100)
    ax.axis('off'); ax.set_facecolor(DARK_BG)

    ax.text(50, 95, 'EXPECTATIONS & RECOMMENDATIONS', fontsize=18, fontweight='bold', ha='center', color=ACCENT_GOLD)
    ax.plot([10, 90], [93, 93], color=ACCENT_GOLD, linewidth=1.5, alpha=0.6)

    # Option A
    box = FancyBboxPatch((5, 68), 42, 22, boxstyle="round,pad=0.5", facecolor=CARD_BG, edgecolor=ACCENT_GOLD, linewidth=2)
    ax.add_patch(box)
    ax.text(26, 87, 'OPTION A: SC DD-Aligned Only', fontsize=11, fontweight='bold', ha='center', color=ACCENT_GOLD)
    ax.text(26, 84, '"Sleep at Night" Mode', fontsize=9, ha='center', color=TEXT_DIM, style='italic')
    details_a = [
        ('Trades/day:', '~3', TEXT_LIGHT),
        ('Expected $/month:', '$19,226', ACCENT_GREEN),
        ('Max drawdown:', '-$404', ACCENT_GREEN),
        ('Win rate:', '85.7%', ACCENT_GREEN),
        ('Risk:', 'MINIMAL', ACCENT_GREEN),
    ]
    for i, (k, v, c) in enumerate(details_a):
        ax.text(10, 81 - i * 2.5, k, fontsize=8, color=TEXT_LIGHT)
        ax.text(30, 81 - i * 2.5, v, fontsize=9, fontweight='bold', color=c)

    # Option B
    box = FancyBboxPatch((53, 68), 42, 22, boxstyle="round,pad=0.5", facecolor=CARD_BG, edgecolor=ACCENT_PURPLE, linewidth=2)
    ax.add_patch(box)
    ax.text(74, 87, 'OPTION B: Hybrid', fontsize=11, fontweight='bold', ha='center', color=ACCENT_PURPLE)
    ax.text(74, 84, 'V9-SC + DD gate on SC only', fontsize=9, ha='center', color=TEXT_DIM, style='italic')
    details_b = [
        ('Trades/day:', '~9', TEXT_LIGHT),
        ('Expected $/month:', '$28,533', ACCENT_GREEN),
        ('Max drawdown:', '-$2,005', ACCENT_GOLD),
        ('Win rate:', '60.5%', TEXT_LIGHT),
        ('Risk:', 'MODERATE', ACCENT_GOLD),
    ]
    for i, (k, v, c) in enumerate(details_b):
        ax.text(58, 81 - i * 2.5, k, fontsize=8, color=TEXT_LIGHT)
        ax.text(78, 81 - i * 2.5, v, fontsize=9, fontweight='bold', color=c)

    # Comparison section
    ax.text(50, 64, 'HEAD-TO-HEAD vs CURRENT V9-SC', fontsize=12, fontweight='bold', ha='center', color=TEXT_WHITE)
    ax.plot([10, 90], [62.5, 62.5], color=TEXT_DIM, linewidth=0.5)

    comparisons = [
        ('', 'V9-SC (Current)', 'SC DD-Aligned', 'Hybrid', ''),
        ('Monthly income', '$36,099', '$19,226 (-47%)', '$28,533 (-21%)', ''),
        ('Max drawdown', '-$4,296', '-$404 (10x better)', '-$2,005 (2x better)', ''),
        ('Profit per trade', '$112', '$314 (2.8x)', '$148 (1.3x)', ''),
        ('Worst day', '-$4,296', '-$336 (12x better)', '-$2,005 (2x better)', ''),
        ('Max consec losses', '8', '2 (4x better)', '8 (same)', ''),
        ('DD Recovery time', 'Multi-day', 'Same day', '1-2 days', ''),
    ]
    y = 60
    for row in comparisons:
        is_header = row[0] == ''
        for j, val in enumerate(row[:4]):
            x_pos = 8 + j * 22
            if is_header:
                ax.text(x_pos, y, val, fontsize=8, fontweight='bold', color=ACCENT_GOLD if j > 0 else TEXT_WHITE)
            else:
                col = TEXT_LIGHT if j == 0 else (ACCENT_BLUE if j == 1 else ACCENT_GREEN if 'better' in val or '2.8x' in val else TEXT_WHITE)
                ax.text(x_pos, y, val, fontsize=8, color=col)
        y -= 3.5

    # Bottom expectations
    ax.text(50, 30, 'REALISTIC EXPECTATIONS', fontsize=12, fontweight='bold', ha='center', color=ACCENT_GOLD)
    ax.plot([10, 90], [28.5, 28.5], color=TEXT_DIM, linewidth=0.5)

    expectations = [
        "SC DD-Aligned fires ~3 trades/day. Some days will have 0 signals (DD hedging doesn't always align).",
        "The 85.7% WR will regress toward 75-80% with more data. PF 6.44 will likely settle at 3-4. Still excellent.",
        "Max DD of -10 pts is over 12 days. With 50+ days, expect -20 to -30 pts max DD (still 5x better than V9-SC).",
        "The filter REDUCES opportunity — V9-SC catches 163 more SC trades. Some of those 163 are profitable trades you miss.",
        "Hybrid is the pragmatic choice: keep all DD/AG trades, just add DD-alignment gate to SC. Minimal code change.",
        "",
        "BOTTOM LINE: SC DD-Aligned = best risk-adjusted returns we've ever found.",
        "Hybrid = practical implementation that captures 79% of V9-SC profits with 53% less max drawdown.",
    ]
    y = 26
    for line in expectations:
        if not line:
            y -= 1.5; continue
        col = ACCENT_GREEN if 'BOTTOM LINE' in line or 'Hybrid = practical' in line else TEXT_LIGHT
        bullet = '\u2022 ' if line[0] != ' ' and 'BOTTOM' not in line and 'Hybrid =' not in line else '  '
        ax.text(10, y, bullet + line, fontsize=8, color=col)
        y -= 3

    ax.text(50, 3, '0DTE Alpha  |  Analysis #15  |  March 20, 2026', fontsize=8, ha='center', color=TEXT_DIM, style='italic')

    pdf.savefig(fig, facecolor=DARK_BG); plt.close()

print(f"\nPDF saved: {pdf_path}")
