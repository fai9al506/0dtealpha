"""Generate Weekly Trading Report PDF - Feb 24-28, 2026."""
import os, sys, psycopg
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from psycopg.rows import dict_row
from fpdf import FPDF
from datetime import datetime
from collections import defaultdict

class TradingPDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 16)
        self.set_text_color(30, 60, 120)
        self.cell(0, 10, '0DTE Alpha', new_x="LMARGIN", new_y="NEXT")
        self.set_font('Helvetica', '', 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, self._header_subtitle, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(30, 60, 120)
        self.set_line_width(0.5)
        self.line(10, self.get_y() + 2, 200, self.get_y() + 2)
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'0DTE Alpha Trading System  |  Page {self.page_no()}/{{nb}}', align='C')

    def section_title(self, title):
        self.set_font('Helvetica', 'B', 13)
        self.set_text_color(30, 60, 120)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def sub_title(self, title):
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(60, 60, 60)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font('Helvetica', '', 10)
        self.set_text_color(40, 40, 40)
        # Sanitize unicode chars that latin-1 can't handle
        text = text.replace('\u2014', '-').replace('\u2013', '-').replace('\u2019', "'").replace('\u2018', "'").replace('\u201c', '"').replace('\u201d', '"').replace('\u2192', '->')
        self.multi_cell(0, 5, text)
        self.ln(2)

    def stat_box(self, label, value, color=(30, 60, 120), w=43):
        x, y = self.get_x(), self.get_y()
        self.set_fill_color(240, 245, 255)
        self.rect(x, y, w, 18, 'F')
        self.set_font('Helvetica', '', 8)
        self.set_text_color(100, 100, 100)
        self.set_xy(x + 2, y + 2)
        self.cell(w - 4, 4, label)
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(*color)
        self.set_xy(x + 2, y + 7)
        self.cell(w - 4, 9, str(value))
        self.set_xy(x + w + 2, y)

    def add_table(self, headers, data, col_widths=None, highlight_col=None):
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Check if we need a new page
        needed = 7 + len(data) * 6 + 10
        if self.get_y() + needed > 270:
            self.add_page()
        self.set_font('Helvetica', 'B', 9)
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align='C')
        self.ln()
        self.set_font('Helvetica', '', 9)
        for row_idx, row in enumerate(data):
            if self.get_y() > 270:
                self.add_page()
                self.set_font('Helvetica', 'B', 9)
                self.set_fill_color(30, 60, 120)
                self.set_text_color(255, 255, 255)
                for i, h in enumerate(headers):
                    self.cell(col_widths[i], 7, h, border=1, fill=True, align='C')
                self.ln()
                self.set_font('Helvetica', '', 9)
            if row_idx % 2 == 0:
                self.set_fill_color(248, 248, 255)
            else:
                self.set_fill_color(255, 255, 255)
            for i, cell in enumerate(row):
                cell_str = str(cell)
                if highlight_col is not None and i == highlight_col:
                    try:
                        val = float(cell_str.replace('+', '').replace('%', ''))
                        if val > 0:
                            self.set_text_color(0, 130, 0)
                        elif val < 0:
                            self.set_text_color(200, 0, 0)
                        else:
                            self.set_text_color(40, 40, 40)
                    except:
                        self.set_text_color(40, 40, 40)
                else:
                    self.set_text_color(40, 40, 40)
                self.cell(col_widths[i], 6, cell_str, border=1, fill=True, align='C')
            self.ln()
        self.ln(3)

    def colored_box(self, text, bg_color, text_color=(255,255,255)):
        x, y = self.get_x(), self.get_y()
        self.set_fill_color(*bg_color)
        self.set_text_color(*text_color)
        self.set_font('Helvetica', 'B', 10)
        w = self.get_string_width(text) + 8
        self.cell(w, 8, text, fill=True, align='C')
        self.set_xy(x + w + 3, y)


# Connect and fetch data
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row)

# Weekly trades
trades = c.execute("""
    SELECT id, setup_name, direction, score, grade, outcome_result, outcome_pnl,
           outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
           abs_es_price, abs_vol_ratio,
           ts AT TIME ZONE 'America/New_York' as ts_et,
           ts::date as trade_date,
           comments
    FROM setup_log
    WHERE ts::date >= '2026-02-24' AND ts::date <= '2026-02-28'
    ORDER BY id
""").fetchall()

# All-time stats
alltime = c.execute("""
    SELECT setup_name,
           COUNT(*) as total,
           COUNT(*) FILTER (WHERE outcome_result = 'WIN') as wins,
           COUNT(*) FILTER (WHERE outcome_result = 'LOSS') as losses,
           COUNT(*) FILTER (WHERE outcome_result = 'EXPIRED') as expired,
           COALESCE(SUM(outcome_pnl), 0) as total_pnl,
           MIN(ts::date) as first_date,
           MAX(ts::date) as last_date
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    GROUP BY setup_name
    ORDER BY total_pnl DESC
""").fetchall()

alltime_total = c.execute("""
    SELECT COUNT(*) as total,
           COUNT(*) FILTER (WHERE outcome_result = 'WIN') as wins,
           COUNT(*) FILTER (WHERE outcome_result = 'LOSS') as losses,
           COALESCE(SUM(outcome_pnl), 0) as total_pnl,
           COUNT(DISTINCT ts::date) as trading_days,
           MIN(ts::date) as first_date
    FROM setup_log
    WHERE outcome_result IS NOT NULL
""").fetchone()

# Trading days count
trading_days_count = c.execute("""
    SELECT COUNT(DISTINCT ts::date) as days
    FROM setup_log
    WHERE outcome_result IS NOT NULL
""").fetchone()['days']

c.close()

# --- Compute stats ---
resolved = [t for t in trades if t['outcome_result'] in ('WIN', 'LOSS', 'EXPIRED')]
wins_w = sum(1 for t in resolved if t['outcome_result'] == 'WIN')
losses_w = sum(1 for t in resolved if t['outcome_result'] == 'LOSS')
expired_w = sum(1 for t in resolved if t['outcome_result'] == 'EXPIRED')
open_w = len(trades) - len(resolved)
pnl_w = sum(float(t['outcome_pnl'] or 0) for t in resolved)
wr_w = (wins_w / (wins_w + losses_w) * 100) if (wins_w + losses_w) > 0 else 0
gross_win = sum(float(t['outcome_pnl'] or 0) for t in resolved if t['outcome_result'] == 'WIN')
gross_loss = abs(sum(float(t['outcome_pnl'] or 0) for t in resolved if t['outcome_result'] in ('LOSS', 'EXPIRED') and (t['outcome_pnl'] or 0) < 0))
pf = (gross_win / gross_loss) if gross_loss > 0 else float('inf')

# Daily breakdown
daily = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'pnl': 0.0})
for t in resolved:
    d = str(t['trade_date'])
    daily[d]['count'] += 1
    daily[d]['pnl'] += float(t['outcome_pnl'] or 0)
    if t['outcome_result'] == 'WIN':
        daily[d]['wins'] += 1
    elif t['outcome_result'] == 'LOSS':
        daily[d]['losses'] += 1
    else:
        daily[d]['expired'] += 1

# Setup breakdown
setups = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'pnl': 0.0, 'open': 0})
for t in trades:
    sn = t['setup_name']
    setups[sn]['count'] += 1
    r = t['outcome_result']
    if r == 'WIN':
        setups[sn]['wins'] += 1
        setups[sn]['pnl'] += float(t['outcome_pnl'] or 0)
    elif r == 'LOSS':
        setups[sn]['losses'] += 1
        setups[sn]['pnl'] += float(t['outcome_pnl'] or 0)
    elif r == 'EXPIRED':
        setups[sn]['expired'] += 1
        setups[sn]['pnl'] += float(t['outcome_pnl'] or 0)
    else:
        setups[sn]['open'] += 1

# Hourly
hours = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl': 0.0})
for t in resolved:
    h = t['ts_et'].hour
    hours[h]['count'] += 1
    hours[h]['pnl'] += float(t['outcome_pnl'] or 0)
    if t['outcome_result'] == 'WIN':
        hours[h]['wins'] += 1

# Direction per setup
dir_setup = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl': 0.0}))
for t in resolved:
    sn = t['setup_name']
    d = 'LONG' if t['direction'].lower() in ('long', 'bullish') else 'SHORT'
    dir_setup[sn][d]['count'] += 1
    dir_setup[sn][d]['pnl'] += float(t['outcome_pnl'] or 0)
    if t['outcome_result'] == 'WIN':
        dir_setup[sn][d]['wins'] += 1

# Best/worst trades
best = max(resolved, key=lambda t: float(t['outcome_pnl'] or 0)) if resolved else None
worst = min(resolved, key=lambda t: float(t['outcome_pnl'] or 0)) if resolved else None

# ===== GENERATE PDF =====
pdf = TradingPDF()
pdf._header_subtitle = 'Weekly Trading Report  |  Feb 24-28, 2026'
pdf.alias_nb_pages()
pdf.add_page()

# --- Page 1: Executive Summary ---
pdf.section_title('Executive Summary')
pnl_color = (0, 130, 0) if pnl_w >= 0 else (200, 0, 0)
pdf.stat_box('Week P&L', f'{pnl_w:+.1f} pts', pnl_color)
pdf.stat_box('Trades', str(len(trades)))
pdf.stat_box('Win Rate', f'{wr_w:.0f}%')
pdf.stat_box('Profit Factor', f'{pf:.2f}x')
pdf.ln(22)

pdf.body_text(
    f"This week the system generated {len(trades)} signals across 4 trading days (Feb 24-27, market closed Feb 28). "
    f"{len(resolved)} resolved with {wins_w} wins, {losses_w} losses, {expired_w} expired. "
    f"Net P&L: {pnl_w:+.1f} pts with a profit factor of {pf:.2f}x."
)

if best:
    pdf.body_text(
        f"Best trade: #{best['id']} {best['setup_name']} ({best['direction']}) = "
        f"{float(best['outcome_pnl']):+.1f} pts. "
        f"Worst trade: #{worst['id']} {worst['setup_name']} ({worst['direction']}) = "
        f"{float(worst['outcome_pnl']):+.1f} pts."
    )

# Narrative
if pnl_w < 0:
    pdf.body_text(
        "The week was challenging with significant drawdowns in the first two days (Mon/Tue). "
        "Wednesday and Thursday showed recovery as DD Exhaustion afternoon signals and ES Absorption "
        "continued their strong streak. AG Short struggled all week (0% WR) and GEX Long remains "
        "the weakest setup."
    )
else:
    pdf.body_text(
        "A positive week with strong contributions from ES Absorption and DD Exhaustion. "
        "The system demonstrated resilience with good afternoon trading performance."
    )

# --- Daily Progression ---
pdf.section_title('Daily Progression')
day_names = {'2026-02-24': 'Mon', '2026-02-25': 'Tue', '2026-02-26': 'Wed', '2026-02-27': 'Thu'}
d_headers = ['Date', 'Day', 'Trades', 'W', 'L', 'E', 'PnL', 'WR%', 'Cumul']
d_widths = [24, 14, 16, 12, 12, 12, 22, 18, 22]
d_data = []
cumul = 0.0
for date_str in sorted(daily.keys()):
    dd = daily[date_str]
    cumul += dd['pnl']
    wl = dd['wins'] + dd['losses']
    wr_d = f"{dd['wins']/wl*100:.0f}" if wl > 0 else '--'
    short_date = date_str[5:]  # MM-DD
    d_data.append([short_date, day_names.get(date_str, ''), str(dd['count']),
                   str(dd['wins']), str(dd['losses']), str(dd['expired']),
                   f"{dd['pnl']:+.1f}", wr_d, f"{cumul:+.1f}"])
d_data.append(['TOTAL', '', str(len(resolved)), str(wins_w), str(losses_w),
               str(expired_w), f'{pnl_w:+.1f}', f'{wr_w:.0f}', f'{cumul:+.1f}'])
pdf.add_table(d_headers, d_data, d_widths, highlight_col=6)

# --- Setup Performance ---
pdf.section_title('Setup Performance')
s_headers = ['Setup', 'Trades', 'W', 'L', 'E', 'PnL', 'WR%', 'Avg']
s_widths = [38, 16, 14, 14, 14, 24, 18, 20]
s_data = []
for sn in sorted(setups.keys(), key=lambda x: setups[x]['pnl'], reverse=True):
    s = setups[sn]
    wl = s['wins'] + s['losses']
    wr_s = f"{s['wins']/wl*100:.0f}" if wl > 0 else '--'
    avg = f"{s['pnl']/s['count']:+.1f}" if s['count'] > 0 else '--'
    s_data.append([sn, str(s['count']), str(s['wins']), str(s['losses']),
                   str(s['expired']), f"{s['pnl']:+.1f}", wr_s, avg])
pdf.add_table(s_headers, s_data, s_widths, highlight_col=5)

# Direction per setup
pdf.sub_title('Direction Breakdown by Setup')
ds_headers = ['Setup', 'Direction', 'Trades', 'Wins', 'PnL', 'WR%']
ds_widths = [38, 22, 20, 20, 26, 20]
ds_data = []
for sn in sorted(dir_setup.keys()):
    for d_name in ['LONG', 'SHORT']:
        if d_name in dir_setup[sn]:
            dd = dir_setup[sn][d_name]
            wr_ds = f"{dd['wins']/dd['count']*100:.0f}" if dd['count'] > 0 else '--'
            ds_data.append([sn, d_name, str(dd['count']), str(dd['wins']),
                           f"{dd['pnl']:+.1f}", wr_ds])
pdf.add_table(ds_headers, ds_data, ds_widths, highlight_col=4)

# --- Hourly Heatmap ---
pdf.section_title('Hourly Performance Heatmap')
h_headers = ['Hour (ET)', 'Trades', 'Wins', 'PnL', 'WR%']
h_widths = [30, 25, 25, 30, 25]
h_data = []
for h in sorted(hours.keys()):
    hr = hours[h]
    wr_h = f"{hr['wins']/hr['count']*100:.0f}" if hr['count'] > 0 else '--'
    h_data.append([f'{h}:00-{h+1}:00', str(hr['count']), str(hr['wins']),
                   f"{hr['pnl']:+.1f}", wr_h])
pdf.add_table(h_headers, h_data, h_widths, highlight_col=3)
pdf.body_text(
    "Morning hours (9-12 ET) were consistently negative this week. "
    "The afternoon window (13-15 ET) was highly profitable, particularly for DD Exhaustion continuation signals. "
    "This supports the proposed 14:00 ET cutoff filter for DD - but with a caveat that this week's afternoon DD trades were the most profitable."
)

# ===== FILTERS SECTION =====
pdf.add_page()
pdf.section_title('Filter Analysis & Recommendations')
pdf.body_text(
    "Based on comprehensive analysis of 216+ resolved trades (Feb 3-27), the following filters have been "
    "identified to improve system performance. All findings are in-sample and require out-of-sample validation."
)

# Filter table
f_headers = ['Priority', 'Filter', 'Impact', 'Trades Removed', 'Wins Lost']
f_widths = [18, 60, 28, 36, 28]
f_data = [
    ['1', 'GEX Long: Block vanna ALL < 0', '+114.4 pts', '17 trades (0% WR)', '0'],
    ['2', 'DD Exhaust: Cutoff after 14:00 ET', '+82.1 pts', '13 trades (0% WR)', '0'],
    ['3', 'DD Exhaust: Block BOFA-PURE paradigm', '+21.5 pts', '11 trades (18% WR)', '2'],
    ['4', 'GEX Long: Exclude MESSY paradigm', '+32.0 pts', '4 trades (0% WR)', '0'],
    ['5', 'DD Exhaust: Charm ceiling $200M', '+8.5 pts', '3 trades (0% WR)', '0'],
    ['6', 'DD Exhaust: Raise shift to $500M', '+5 pts est', '~5 marginal trades', '0-1'],
]
pdf.add_table(f_headers, f_data, f_widths, highlight_col=2)

# Filter details
pdf.sub_title('Filter 1: GEX Long Vanna Gate (HIGHEST IMPACT)')
pdf.body_text(
    "When aggregated vanna exposure (ALL expirations) is negative, GEX Long has 0% win rate across 17 trades "
    "(-114.4 pts). When positive: 42.9% WR, +3.3 pts. This is the strongest single filter discovered. "
    "Zero wins lost, all losses removed. Implementation: check volland_exposure_points vanna ALL sum before "
    "allowing GEX Long signals."
)

pdf.sub_title('Filter 2: DD Exhaustion Afternoon Cutoff')
pdf.body_text(
    "13 DD trades fired after 14:00 ET with 0% win rate and -82.1 pts total. The scoring model awarded "
    "maximum 'dealer o clock' bonus for 14:00+ which was counterproductive. Best DD window: 11:00-13:00 "
    "(76.9% WR). Change dd_market_end from 15:30 to 14:00."
)
pdf.body_text(
    "CAUTION: This week (Feb 27) had 4 winning DD longs in the 13-15 window (+72.5 pts). "
    "A 14:00 cutoff would have cost some of those. Consider 14:30 as a compromise."
)

pdf.sub_title('Filter 3: DD BOFA-PURE Paradigm Block')
pdf.body_text(
    "BOFA-PURE paradigm DD trades: 18.2% WR, -21.5 pts (11 trades). MESSY paradigms: 100% WR, +200.8 pts. "
    "When the paradigm is clean BofA, the DD-Charm divergence gets overwhelmed by existing regime. "
    "When messy, DD divergence becomes dominant and works brilliantly. NOTE: Recent data shows this filter "
    "weakening (+3.8 pts with latest trades). Needs more data."
)

pdf.sub_title('Combined Filter Impact')
pdf.body_text(
    "Filters 1+2+3 combined: estimated +200 to +218 pts improvement. Would bring all-time PnL from "
    "+390 to approximately +590-608 pts. Reduces trade count by ~35-40 trades (virtually all losers). "
    "Projected win rate improvement: 35% -> 50%+."
)

# ===== SUGGESTIONS =====
pdf.add_page()
pdf.section_title('Strategic Suggestions')

pdf.sub_title('1. Implement Top Filters Immediately')
pdf.body_text(
    "Priority: GEX Long vanna gate (F1) and DD 14:00 cutoff (F2). These have the strongest statistical "
    "backing with 0% WR on blocked trades and zero false positives. Combined impact: +196.5 pts."
)

pdf.sub_title('2. Reduce ES Absorption Signal Frequency')
pdf.body_text(
    "ES Absorption fired 14 signals on Feb 27 alone (target: 1-3/day). Despite high win rate, excessive "
    "signals increase execution risk and correlation between trades. Consider raising abs_min_vol_ratio "
    "from 1.4 to 2.0 or abs_cvd_z_min from 0.5 to 1.0 to filter marginal divergences."
)

pdf.sub_title('3. Consider Disabling AG Short')
pdf.body_text(
    "AG Short went 0% WR this week (-91.0 pts, 8 trades). While historically positive (+71.5 pts all-time), "
    "recent performance is deteriorating. Monitor for 2 more weeks before deciding."
)

pdf.sub_title('4. Morning Trading Caution')
pdf.body_text(
    "Morning hours (9-12 ET) were negative every single day this week. Consider reducing position sizing "
    "or requiring higher grade thresholds for morning signals. The afternoon edge is consistently stronger."
)

pdf.sub_title('5. Continue ES Absorption Data Collection')
pdf.body_text(
    "With only 16 resolved trades, ES Absorption needs 30+ more before the 62.5% WR is statistically reliable. "
    "The split-target RM is well-designed, but the true edge may settle to 55-60% WR with regression to mean."
)

# ===== EXPECTED MONTHLY INCOME =====
pdf.add_page()
pdf.section_title('Expected Monthly Income Projection')

# All-time stats
at_total = int(alltime_total['total'])
at_wins = int(alltime_total['wins'])
at_losses = int(alltime_total['losses'])
at_pnl = float(alltime_total['total_pnl'])
at_days = int(alltime_total['trading_days'] or trading_days_count)

avg_daily = at_pnl / at_days if at_days > 0 else 0
monthly_days = 21  # typical trading days per month

# Scenarios
base_monthly = avg_daily * monthly_days

# With filters
filter_delta = 196.5  # F1+F2 combined
at_pnl_filtered = at_pnl + filter_delta
avg_daily_filtered = at_pnl_filtered / at_days if at_days > 0 else 0
filtered_monthly = avg_daily_filtered * monthly_days

# MES dollar conversion (1 SPX pt ~ 1 ES pt ~ 5 MES ticks, $5/pt for MES, 10 contracts)
mes_per_pt = 5.0 * 10  # $50 per SPX pt with 10 MES

pdf.sub_title('Historical Performance (All-Time)')
pdf.body_text(
    f"Dataset: {at_total} resolved trades across {at_days} trading days (Feb 3 - Feb 27, 2026)\n"
    f"Total P&L: {at_pnl:+.1f} pts | Win Rate: {at_wins/(at_wins+at_losses)*100:.1f}%\n"
    f"Average Daily P&L: {avg_daily:+.1f} pts/day"
)

pdf.sub_title('Scenario Analysis')
sc_headers = ['Scenario', 'Daily Avg', 'Monthly (21d)', 'MES $/month*', 'Annual']
sc_widths = [42, 24, 30, 32, 30]
sc_data = [
    ['Current (no filters)',
     f'{avg_daily:+.1f} pts',
     f'{base_monthly:+.1f} pts',
     f'${base_monthly * mes_per_pt:+,.0f}',
     f'${base_monthly * mes_per_pt * 12:+,.0f}'],
    ['With F1+F2 filters',
     f'{avg_daily_filtered:+.1f} pts',
     f'{filtered_monthly:+.1f} pts',
     f'${filtered_monthly * mes_per_pt:+,.0f}',
     f'${filtered_monthly * mes_per_pt * 12:+,.0f}'],
    ['Conservative (70%)',
     f'{avg_daily_filtered * 0.7:+.1f} pts',
     f'{filtered_monthly * 0.7:+.1f} pts',
     f'${filtered_monthly * 0.7 * mes_per_pt:+,.0f}',
     f'${filtered_monthly * 0.7 * mes_per_pt * 12:+,.0f}'],
    ['Bear case (40%)',
     f'{avg_daily_filtered * 0.4:+.1f} pts',
     f'{filtered_monthly * 0.4:+.1f} pts',
     f'${filtered_monthly * 0.4 * mes_per_pt:+,.0f}',
     f'${filtered_monthly * 0.4 * mes_per_pt * 12:+,.0f}'],
]
pdf.add_table(sc_headers, sc_data, sc_widths, highlight_col=3)
pdf.body_text(
    f"* MES calculation: 10 MES contracts x $5/point = $50 per SPX point. "
    f"Commissions not included (~$5 RT per contract = $50/trade)."
)

# ES contract scaling
pdf.sub_title('Scaling Potential (ES Full-Size)')
es_per_pt = 50.0  # $50/pt for ES
pdf.body_text(
    f"With 1 ES contract ($50/pt): Current monthly = ${base_monthly * es_per_pt:+,.0f} | "
    f"Filtered = ${filtered_monthly * es_per_pt:+,.0f}\n"
    f"With 2 ES contracts: Current = ${base_monthly * es_per_pt * 2:+,.0f} | "
    f"Filtered = ${filtered_monthly * es_per_pt * 2:+,.0f}\n"
    f"With 5 ES contracts: Current = ${base_monthly * es_per_pt * 5:+,.0f} | "
    f"Filtered = ${filtered_monthly * es_per_pt * 5:+,.0f}"
)

pdf.sub_title('Important Caveats')
pdf.body_text(
    "1. SMALL SAMPLE: Only 18 trading days of data. True edge may differ from observed.\n"
    "2. IN-SAMPLE: Filter impacts are calculated on the same data used to discover them. "
    "Out-of-sample performance will be lower.\n"
    "3. REGIME DEPENDENT: These results span a specific volatility regime. Major market shifts "
    "(FOMC, CPI) can temporarily invalidate signals.\n"
    "4. SLIPPAGE: Live execution adds slippage (0.25-0.50 pts per trade on MES).\n"
    "5. CONSERVATIVE ESTIMATE: The '70% of filtered' scenario (~70% of backtest) is the most "
    "realistic expectation for live trading.\n"
    "6. DRAWDOWNS: The system had a -164.8 pts drawdown (Feb 24-25). Max monthly drawdown "
    "could reach -200+ pts in adverse conditions."
)

# All-time breakdown
pdf.add_page()
pdf.section_title('All-Time Setup Performance')
at_headers = ['Setup', 'Trades', 'W', 'L', 'E', 'PnL', 'WR%', 'Avg/Trade']
at_widths = [38, 16, 14, 14, 14, 24, 18, 22]
at_data = []
for row in alltime:
    wl = int(row['wins']) + int(row['losses'])
    wr_at = f"{int(row['wins'])/wl*100:.0f}" if wl > 0 else '--'
    avg_at = f"{float(row['total_pnl'])/int(row['total']):+.1f}"
    at_data.append([
        row['setup_name'], str(row['total']), str(row['wins']), str(row['losses']),
        str(row['expired']), f"{float(row['total_pnl']):+.1f}", wr_at, avg_at
    ])
at_data.append(['TOTAL', str(at_total), str(at_wins), str(at_losses),
                str(int(alltime_total['total']) - at_wins - at_losses),
                f'{at_pnl:+.1f}',
                f'{at_wins/(at_wins+at_losses)*100:.0f}',
                f'{at_pnl/at_total:+.1f}'])
pdf.add_table(at_headers, at_data, at_widths, highlight_col=5)

# Equity curve narrative
pdf.sub_title('Equity Curve Summary')
pdf.body_text(
    f"System started Feb 3, 2026. Over {at_days} trading days, generated {at_pnl:+.1f} pts total. "
    f"Average {avg_daily:+.1f} pts/day. Maximum observed drawdown: ~164.8 pts (Feb 24-25 cluster). "
    f"Recovery time: 2 days. The system shows positive expectancy across most setups, "
    f"with GEX Long being the only consistently negative contributor."
)

pdf.sub_title('Next Steps')
pdf.body_text(
    "1. Deploy GEX Long vanna filter and DD 14:00 cutoff (top priority)\n"
    "2. Continue ES Absorption data collection (target: 50 resolved trades)\n"
    "3. Monitor AG Short performance - suspend if 0% WR persists next week\n"
    "4. Consider reducing ES Absorption signal frequency (abs_cvd_z_min 0.5 -> 1.0)\n"
    "5. Build daily regime classifier (trend vs range) for dynamic setup selection\n"
    "6. Implement real-time equity curve tracking on dashboard"
)

# Save
out_path = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\Weekly_Report_Feb24-28.pdf"
pdf.output(out_path)
print(f"PDF saved to: {out_path}")
