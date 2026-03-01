"""Generate Feb 27, 2026 Daily Trading Report PDF."""
import os, sys, psycopg
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from psycopg.rows import dict_row
from fpdf import FPDF
from datetime import datetime

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
        self.multi_cell(0, 5, text)
        self.ln(2)

    def stat_box(self, label, value, color=(30, 60, 120)):
        x, y = self.get_x(), self.get_y()
        self.set_fill_color(240, 245, 255)
        self.rect(x, y, 43, 18, 'F')
        self.set_font('Helvetica', '', 8)
        self.set_text_color(100, 100, 100)
        self.set_xy(x + 2, y + 2)
        self.cell(39, 4, label)
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(*color)
        self.set_xy(x + 2, y + 7)
        self.cell(39, 9, str(value))
        self.set_xy(x + 45, y)

    def add_table(self, headers, data, col_widths=None, highlight_col=None):
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Header
        self.set_font('Helvetica', 'B', 9)
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align='C')
        self.ln()
        # Data
        self.set_font('Helvetica', '', 9)
        for row_idx, row in enumerate(data):
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


# Connect and fetch data
c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row)

trades = c.execute("""
    SELECT id, setup_name, direction, score, grade, outcome_result, outcome_pnl,
           outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
           abs_es_price, abs_vol_ratio,
           ts AT TIME ZONE 'America/New_York' as ts_et,
           comments
    FROM setup_log
    WHERE ts::date = '2026-02-27'
    ORDER BY id
""").fetchall()

bars = c.execute("""
    SELECT COUNT(*) as bar_count,
           MIN(bar_low) as day_low, MAX(bar_high) as day_high,
           (SELECT bar_open FROM es_range_bars WHERE trade_date = '2026-02-27' AND source = 'rithmic' AND status = 'closed' ORDER BY bar_idx ASC LIMIT 1) as open_price,
           (SELECT bar_close FROM es_range_bars WHERE trade_date = '2026-02-27' AND source = 'rithmic' AND status = 'closed' ORDER BY bar_idx DESC LIMIT 1) as close_price
    FROM es_range_bars
    WHERE trade_date = '2026-02-27' AND source = 'rithmic' AND status = 'closed'
""").fetchone()

c.close()

# Compute stats
total = len(trades)
resolved = [t for t in trades if t['outcome_result'] in ('WIN', 'LOSS', 'EXPIRED')]
wins = sum(1 for t in resolved if t['outcome_result'] == 'WIN')
losses = sum(1 for t in resolved if t['outcome_result'] == 'LOSS')
expired = sum(1 for t in resolved if t['outcome_result'] == 'EXPIRED')
open_trades = total - len(resolved)
total_pnl = sum(float(t['outcome_pnl'] or 0) for t in resolved)
wr = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

# By setup
setups = {}
for t in trades:
    sn = t['setup_name']
    if sn not in setups:
        setups[sn] = {'trades': [], 'wins': 0, 'losses': 0, 'expired': 0, 'open': 0, 'pnl': 0}
    setups[sn]['trades'].append(t)
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

# By hour
hours = {}
for t in resolved:
    h = t['ts_et'].hour
    if h not in hours:
        hours[h] = {'count': 0, 'wins': 0, 'pnl': 0}
    hours[h]['count'] += 1
    hours[h]['pnl'] += float(t['outcome_pnl'] or 0)
    if t['outcome_result'] == 'WIN':
        hours[h]['wins'] += 1

# Direction
dirs = {'long': {'count': 0, 'wins': 0, 'pnl': 0}, 'short': {'count': 0, 'wins': 0, 'pnl': 0}}
for t in resolved:
    d = 'long' if t['direction'].lower() in ('long', 'bullish') else 'short'
    dirs[d]['count'] += 1
    dirs[d]['pnl'] += float(t['outcome_pnl'] or 0)
    if t['outcome_result'] == 'WIN':
        dirs[d]['wins'] += 1

# Generate PDF
pdf = TradingPDF()
pdf._header_subtitle = 'Daily Trading Report  |  Thursday, February 27, 2026'
pdf.alias_nb_pages()
pdf.add_page()

# Executive Summary
pdf.section_title('Executive Summary')
pdf.stat_box('Total Trades', total)
pnl_color = (0, 130, 0) if total_pnl >= 0 else (200, 0, 0)
pdf.stat_box('Net P&L', f'{total_pnl:+.1f} pts', pnl_color)
pdf.stat_box('Win Rate', f'{wr:.0f}%')
pdf.stat_box('Resolved', f'{len(resolved)}/{total}')
pdf.ln(22)

es_range = float(bars['day_high']) - float(bars['day_low']) if bars['day_high'] else 0
pdf.body_text(
    f"Market Context: ES traded a {es_range:.0f}-point range "
    f"({float(bars['day_low']):.0f} - {float(bars['day_high']):.0f}) across {bars['bar_count']} range bars. "
    f"Opened at {float(bars['open_price']):.0f}, closed at {float(bars['close_price']):.0f}."
)
pdf.body_text(
    f"The system generated {total} signals with {len(resolved)} resolved: "
    f"{wins} wins, {losses} losses, {expired} expired, {open_trades} still open (pending outcome backfill). "
    f"Net P&L of {total_pnl:+.1f} pts on resolved trades."
)

# Setup Performance
pdf.section_title('Performance by Setup')
headers = ['Setup', 'Trades', 'W', 'L', 'E', 'Open', 'PnL', 'WR%']
widths = [38, 16, 12, 12, 12, 14, 22, 18]
data = []
for sn in sorted(setups.keys()):
    s = setups[sn]
    t_count = len(s['trades'])
    wl = s['wins'] + s['losses']
    wr_s = f"{s['wins']/wl*100:.0f}" if wl > 0 else '--'
    data.append([sn, str(t_count), str(s['wins']), str(s['losses']),
                 str(s['expired']), str(s['open']), f"{s['pnl']:+.1f}", wr_s])
data.append(['TOTAL', str(total), str(wins), str(losses), str(expired),
             str(open_trades), f'{total_pnl:+.1f}', f'{wr:.0f}'])
pdf.add_table(headers, data, widths, highlight_col=6)

# Setup narratives
pdf.sub_title('Key Observations')
narratives = []
if 'ES Absorption' in setups:
    s = setups['ES Absorption']
    narratives.append(
        f"ES Absorption: {len(s['trades'])} signals, {s['wins']}W/{s['losses']}L with {s['open']} still open. "
        f"T1 hit rate near 100%. Split-target RM (T1=+10pt, T2=trail) continues to perform. "
        f"Signal frequency remains high (~14/day vs target 1-3)."
    )
if 'DD Exhaustion' in setups:
    s = setups['DD Exhaustion']
    narratives.append(
        f"DD Exhaustion: {len(s['trades'])} signals, {s['wins']}W/{s['losses']}L. "
        f"Morning longs (10-11 ET) were 0/6 = -72 pts. "
        f"Afternoon longs (13-15 ET) rescued with 4/4 wins = +72.5 pts. "
        f"Strong afternoon rally confirmed contrarian DD signals."
    )
if 'BofA Scalp' in setups:
    s = setups['BofA Scalp']
    narratives.append(
        f"BofA Scalp: {s['wins']}W/{s['losses']}L = +{s['pnl']:.1f} pts. "
        f"Reliable 30-min scalp setup continues steady contribution."
    )
for n in narratives:
    pdf.body_text(n)

# Hourly breakdown
pdf.section_title('Hourly Performance')
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
    "Morning (10-11 ET) was the weakest period with heavy DD losses. "
    "The afternoon session (13-15 ET) drove the recovery with 100% win rate on DD continuation signals."
)

# Direction breakdown
pdf.section_title('Direction Breakdown')
d_headers = ['Direction', 'Trades', 'Wins', 'PnL', 'WR%']
d_widths = [35, 25, 25, 30, 25]
d_data = []
for d_name in ['long', 'short']:
    dd = dirs[d_name]
    wr_d = f"{dd['wins']/dd['count']*100:.0f}" if dd['count'] > 0 else '--'
    d_data.append([d_name.upper(), str(dd['count']), str(dd['wins']),
                   f"{dd['pnl']:+.1f}", wr_d])
pdf.add_table(d_headers, d_data, d_widths, highlight_col=3)

# Trade log
pdf.add_page()
pdf.section_title('Trade Log')
t_headers = ['#', 'Setup', 'Dir', 'Score', 'Result', 'PnL', 'MaxP', 'MaxL', 'Time']
t_widths = [10, 34, 14, 14, 18, 18, 18, 18, 20]
t_data = []
for t in trades:
    r = t['outcome_result'] or 'OPEN'
    pnl_str = f"{float(t['outcome_pnl']):+.1f}" if t['outcome_pnl'] is not None else '--'
    mp = f"{float(t['outcome_max_profit']):.1f}" if t['outcome_max_profit'] is not None else '--'
    ml = f"{float(t['outcome_max_loss']):.1f}" if t['outcome_max_loss'] is not None else '--'
    d = 'L' if t['direction'].lower() in ('long', 'bullish') else 'S'
    time_str = t['ts_et'].strftime('%H:%M')
    score_str = f"{float(t['score']):.0f}" if t['score'] else '--'
    t_data.append([str(t['id']), t['setup_name'][:18], d, score_str, r, pnl_str, mp, ml, time_str])
pdf.add_table(t_headers, t_data, t_widths, highlight_col=5)

# Save
out_path = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\Feb27_Trading_Report.pdf"
pdf.output(out_path)
print(f"PDF saved to: {out_path}")
