"""Generate Expected Income PDF Report from actual trading data"""
import os, datetime
from fpdf import FPDF
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)

# ── Fetch data ─────────────────────────────────────────────
with engine.begin() as conn:
    daily = conn.execute(text("""
        SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
               COUNT(*) as trades,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
               ROUND(SUM(outcome_pnl)::numeric, 1) as daily_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY DATE(ts AT TIME ZONE 'America/New_York')
        ORDER BY trade_date ASC
    """)).mappings().all()

    setup_perf = conn.execute(text("""
        SELECT setup_name,
               COUNT(*) as cnt,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
               ROUND(SUM(outcome_pnl)::numeric, 1) as pnl,
               ROUND((100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / COUNT(*))::numeric, 1) as wr
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name
        ORDER BY SUM(outcome_pnl) DESC
    """)).mappings().all()

    total_row = conn.execute(text("""
        SELECT COUNT(*) as cnt,
               ROUND(SUM(outcome_pnl)::numeric, 1) as pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
    """)).mappings().first()

# ── Compute stats ──────────────────────────────────────────
trading_days = len(daily)
total_pts = float(total_row['pnl'])
total_trades = int(total_row['cnt'])
avg_daily = total_pts / trading_days
first_date = daily[0]['trade_date']
last_date = daily[-1]['trade_date']

ES_PT = 50.0
SAR = 3.75
TRADING_DAYS_MO = 21

# Daily PnL list for drawdown calc
daily_pnls = [float(d['daily_pnl']) for d in daily]
cumulative = []
s = 0
peak = 0
max_dd = 0
for p in daily_pnls:
    s += p
    cumulative.append(s)
    if s > peak:
        peak = s
    dd = peak - s
    if dd > max_dd:
        max_dd = dd

# ── Build PDF ──────────────────────────────────────────────
class PDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, '0DTE Alpha Trading System', align='R', new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f'Generated {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}  |  Page {self.page_no()}/{{nb}}', align='C')

    def section_title(self, title):
        self.set_font('Helvetica', 'B', 13)
        self.set_text_color(30, 60, 120)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(30, 60, 120)
        self.line(10, self.get_y(), 100, self.get_y())
        self.ln(4)

    def sub_title(self, title):
        self.set_font('Helvetica', 'B', 10)
        self.set_text_color(60, 60, 60)
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, txt):
        self.set_font('Helvetica', '', 9)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5, txt)
        self.ln(2)

    def table_header(self, cols, widths):
        self.set_font('Helvetica', 'B', 8)
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 6, col, border=1, fill=True, align='C')
        self.ln()

    def table_row(self, cols, widths, aligns=None, bold=False, fill=False):
        self.set_font('Helvetica', 'B' if bold else '', 8)
        self.set_text_color(40, 40, 40)
        if fill:
            self.set_fill_color(240, 245, 255)
        for i, col in enumerate(cols):
            a = aligns[i] if aligns else 'C'
            self.cell(widths[i], 5.5, str(col), border=1, align=a, fill=fill)
        self.ln()

    def kpi_box(self, label, value, x, y, w=42, h=18):
        self.set_xy(x, y)
        self.set_fill_color(240, 245, 255)
        self.set_draw_color(30, 60, 120)
        self.rect(x, y, w, h, style='DF')
        self.set_xy(x, y + 2)
        self.set_font('Helvetica', '', 7)
        self.set_text_color(100, 100, 100)
        self.cell(w, 4, label, align='C', new_x="LMARGIN", new_y="NEXT")
        self.set_xy(x, y + 7)
        self.set_font('Helvetica', 'B', 12)
        self.set_text_color(30, 60, 120)
        self.cell(w, 8, value, align='C')


pdf = PDF()
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=20)
pdf.add_page()

# ── Title ──────────────────────────────────────────────────
pdf.set_font('Helvetica', 'B', 22)
pdf.set_text_color(30, 60, 120)
pdf.cell(0, 14, 'Expected Income Report', align='C', new_x="LMARGIN", new_y="NEXT")
pdf.set_font('Helvetica', '', 10)
pdf.set_text_color(100, 100, 100)
pdf.cell(0, 7, f'Based on live signal data: {first_date} to {last_date}  ({trading_days} trading days, {total_trades} trades)', align='C', new_x="LMARGIN", new_y="NEXT")
pdf.ln(6)

# ── KPI Boxes ──────────────────────────────────────────────
y = pdf.get_y()
pdf.kpi_box('Total P&L', f'+{total_pts:.0f} pts', 12, y)
pdf.kpi_box('Avg Daily', f'+{avg_daily:.1f} pts', 56, y)
pdf.kpi_box('Total Trades', str(total_trades), 100, y)
pdf.kpi_box('Trading Days', str(trading_days), 144, y)
pdf.set_y(y + 24)

# ── Setup Performance ──────────────────────────────────────
pdf.section_title('Setup Performance Breakdown')
cols = ['Setup', 'Trades', 'Wins', 'Losses', 'Win Rate', 'P&L (pts)']
widths = [40, 18, 18, 18, 22, 25]
aligns = ['L', 'C', 'C', 'C', 'C', 'R']
pdf.table_header(cols, widths)
for i, r in enumerate(setup_perf):
    pdf.table_row([
        r['setup_name'], str(r['cnt']), str(r['wins']), str(r['losses']),
        f"{r['wr']}%", f"+{r['pnl']}" if float(r['pnl']) >= 0 else str(r['pnl'])
    ], widths, aligns, fill=(i % 2 == 0))
pdf.table_row(['TOTAL', str(total_trades), '', '', '', f'+{total_pts:.1f}'], widths, aligns, bold=True)
pdf.ln(4)

# ── Daily P&L ─────────────────────────────────────────────
pdf.section_title('Daily P&L History')
cols = ['Date', 'Trades', 'Wins', 'Losses', 'Day P&L', 'Cumulative']
widths = [28, 16, 16, 16, 22, 25]
aligns = ['C', 'C', 'C', 'C', 'R', 'R']
pdf.table_header(cols, widths)
cumul = 0
for i, d in enumerate(daily):
    pnl = float(d['daily_pnl'])
    cumul += pnl
    losses = int(d['trades']) - int(d['wins'])
    pdf.table_row([
        str(d['trade_date']), str(d['trades']), str(d['wins']), str(losses),
        f"{pnl:+.1f}", f"{cumul:+.1f}"
    ], widths, aligns, fill=(i % 2 == 0))
pdf.ln(4)

# ── Risk metrics ──────────────────────────────────────────
pdf.sub_title('Risk Metrics')
worst_day = min(daily_pnls)
best_day = max(daily_pnls)
win_days = sum(1 for p in daily_pnls if p > 0)
pdf.body_text(
    f"Best day: +{best_day:.1f} pts  |  Worst day: {worst_day:+.1f} pts  |  "
    f"Max drawdown: {max_dd:.1f} pts  |  Profitable days: {win_days}/{trading_days} ({100*win_days//trading_days}%)"
)

# ── NEW PAGE: Income Projections ───────────────────────────
pdf.add_page()
pdf.section_title('Monthly Income Projections')
pdf.body_text(
    f"Based on average daily P&L of +{avg_daily:.1f} pts over {trading_days} trading days. "
    f"Assumes 21 trading days/month. ES = $50/point. MES = $5/point. USD/SAR = 3.75."
)

# Full-rate table
pdf.sub_title(f'Scenario A: Full Rate ({avg_daily:.1f} pts/day)')
cols = ['Contracts', 'Monthly (pts)', 'Monthly (USD)', 'Monthly (SAR)', 'Yearly (USD)', 'Margin Req.']
widths = [24, 26, 28, 28, 28, 26]
aligns = ['C', 'R', 'R', 'R', 'R', 'R']
pdf.table_header(cols, widths)
mo_pts = avg_daily * TRADING_DAYS_MO
for c in [2, 3, 4, 5, 6]:
    mo_usd = mo_pts * ES_PT * c
    mo_sar = mo_usd * SAR
    yr_usd = mo_usd * 12
    margin = c * 15400
    pdf.table_row([
        f'{c} ES', f'{mo_pts:+,.0f}', f'${mo_usd:+,.0f}', f'{mo_sar:+,.0f} SAR',
        f'${yr_usd:+,.0f}', f'${margin:,}'
    ], widths, aligns, fill=(c % 2 == 0))
pdf.ln(4)

# Conservative table (15 pts/day)
pdf.sub_title('Scenario B: Conservative (15 pts/day)')
pdf.body_text('Assumes performance normalizes after the initial high-signal period (Feb 19-20 were outlier days with 50 combined trades).')
cols = ['Contracts', 'Monthly (pts)', 'Monthly (USD)', 'Monthly (SAR)', 'Yearly (USD)']
widths = [24, 28, 30, 30, 30]
aligns = ['C', 'R', 'R', 'R', 'R']
pdf.table_header(cols, widths)
for c in [2, 3, 4, 5, 6]:
    mo = 15 * TRADING_DAYS_MO
    mo_usd = mo * ES_PT * c
    mo_sar = mo_usd * SAR
    yr_usd = mo_usd * 12
    pdf.table_row([
        f'{c} ES', f'{mo:+,.0f}', f'${mo_usd:+,.0f}', f'{mo_sar:+,.0f} SAR', f'${yr_usd:+,.0f}'
    ], widths, aligns, fill=(c % 2 == 0))
pdf.ln(4)

# Moderate table (25 pts/day)
pdf.sub_title('Scenario C: Moderate (25 pts/day)')
pdf.body_text('Assumes strong setups (DD Exhaustion + AG Short) continue to perform. GEX Long disabled or fixed.')
cols = ['Contracts', 'Monthly (pts)', 'Monthly (USD)', 'Monthly (SAR)', 'Yearly (USD)']
widths = [24, 28, 30, 30, 30]
pdf.table_header(cols, widths)
for c in [2, 3, 4, 5, 6]:
    mo = 25 * TRADING_DAYS_MO
    mo_usd = mo * ES_PT * c
    mo_sar = mo_usd * SAR
    yr_usd = mo_usd * 12
    pdf.table_row([
        f'{c} ES', f'{mo:+,.0f}', f'${mo_usd:+,.0f}', f'{mo_sar:+,.0f} SAR', f'${yr_usd:+,.0f}'
    ], widths, aligns, fill=(c % 2 == 0))
pdf.ln(6)

# ── Time to $1M ───────────────────────────────────────────
pdf.section_title('Time to $1,000,000 USD')
cols = ['Contracts', 'Conservative (15/day)', 'Moderate (25/day)', f'Full Rate ({avg_daily:.0f}/day)']
widths = [30, 42, 42, 42]
aligns = ['C', 'C', 'C', 'C']
pdf.table_header(cols, widths)
for c in [2, 3, 4, 5, 6]:
    vals = []
    for rate in [15, 25, avg_daily]:
        mo_usd = rate * TRADING_DAYS_MO * ES_PT * c
        if mo_usd > 0:
            months = 1_000_000 / mo_usd
            vals.append(f'{months:.1f} months ({months/12:.1f} yr)')
        else:
            vals.append('N/A')
    pdf.table_row([f'{c} ES'] + vals, widths, aligns, fill=(c % 2 == 0))
pdf.ln(6)

# ── Worst Case / Drawdown ─────────────────────────────────
pdf.section_title('Risk & Drawdown Analysis')
pdf.body_text(
    'Worst-case daily losses based on historical data. '
    'Actual drawdowns could be larger on days with more losing setups or volatile conditions.'
)
cols = ['Scenario', '2 ES', '4 ES', '6 ES']
widths = [50, 30, 30, 30]
aligns = ['L', 'R', 'R', 'R']
pdf.table_header(cols, widths)
pdf.table_row([f'Worst day ({worst_day:+.1f} pts)', f'${worst_day*ES_PT*2:+,.0f}', f'${worst_day*ES_PT*4:+,.0f}', f'${worst_day*ES_PT*6:+,.0f}'], widths, aligns)
pdf.table_row([f'Max drawdown ({max_dd:.1f} pts)', f'${-max_dd*ES_PT*2:,.0f}', f'${-max_dd*ES_PT*4:,.0f}', f'${-max_dd*ES_PT*6:,.0f}'], widths, aligns, fill=True)
pdf.table_row(['Hypothetical bad day (-50 pts)', f'${-50*ES_PT*2:,.0f}', f'${-50*ES_PT*4:,.0f}', f'${-50*ES_PT*6:,.0f}'], widths, aligns)
pdf.table_row(['Hypothetical bad week (-120 pts)', f'${-120*ES_PT*2:,.0f}', f'${-120*ES_PT*4:,.0f}', f'${-120*ES_PT*6:,.0f}'], widths, aligns, fill=True)
pdf.ln(6)

# ── Disclaimers ────────────────────────────────────────────
pdf.section_title('Important Disclaimers')
pdf.set_font('Helvetica', '', 8)
pdf.set_text_color(80, 80, 80)
disclaimers = [
    "1. These projections are based on only 11 trading days of live signal data. This is a very small sample and results may not be representative of future performance.",
    "2. Feb 19-20 contributed +410 of +506 total points (81%). These were outlier trending days. Removing them reduces average to ~10 pts/day.",
    "3. Slippage and commissions are NOT included. ES round-trip cost is ~$4.50/contract. At 8 trades/day with 4 contracts = ~$144/day overhead.",
    "4. Past performance does not guarantee future results. The system has not been tested through extended flat/choppy markets, flash crashes, or regime changes.",
    "5. The Conservative scenario (15 pts/day) is the most realistic starting assumption. Performance should be validated over 60+ trading days before scaling.",
    "6. Capital requirements shown are overnight margin. Intraday margin may be lower but varies by broker. A risk reserve of 2-3x margin is recommended.",
    "7. DD Exhaustion (largest P&L contributor) is still in LOG-ONLY mode and has not been live-traded at scale.",
]
for d in disclaimers:
    pdf.multi_cell(0, 4, d)
    pdf.ln(1)

# ── Save ──────────────────────────────────────────────────
out_path = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\0DTE_Alpha_Expected_Income_Report.pdf"
pdf.output(out_path)
print(f"PDF saved to: {out_path}")
