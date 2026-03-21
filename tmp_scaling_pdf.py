"""
Generate PDF: V10 Performance & Realistic Scaling Plan
Dark theme matching Analysis #15 style.
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
from datetime import datetime

# Dark theme colors
BG = '#1a1a2e'
CARD_BG = '#16213e'
TEXT = '#e0e0e0'
GREEN = '#00e676'
RED = '#ff5252'
BLUE = '#42a5f5'
GOLD = '#ffd740'
CYAN = '#00e5ff'
ORANGE = '#ff9800'
GRID = '#2a2a4a'
ACCENT = '#7c4dff'

plt.rcParams.update({
    'figure.facecolor': BG, 'axes.facecolor': CARD_BG,
    'text.color': TEXT, 'axes.labelcolor': TEXT,
    'xtick.color': TEXT, 'ytick.color': TEXT,
    'axes.edgecolor': GRID, 'grid.color': GRID,
    'font.family': 'sans-serif', 'font.size': 10,
})

pdf_path = "V10_Scaling_Plan.pdf"

with PdfPages(pdf_path) as pdf:

    # ===== PAGE 1: V10 March Performance =====
    fig = plt.figure(figsize=(11, 8.5))
    fig.suptitle("V10 Filter — March 2026 Performance", fontsize=18, fontweight='bold', color=GOLD, y=0.97)

    # March daily PnL data (V10 all setups)
    dates = ['Mar 2', 'Mar 3', 'Mar 4', 'Mar 5', 'Mar 6', 'Mar 9', 'Mar 10',
             'Mar 11', 'Mar 12', 'Mar 13', 'Mar 16', 'Mar 17', 'Mar 18', 'Mar 19', 'Mar 20']
    daily_pnl = [98.7, 115.3, 169.1, 68.0, 117.0, -55.4, 137.6,
                 -8.7, -40.9, -60.4, 28.0, 140.5, 96.6, 86.5, 30.9]
    cumulative = np.cumsum(daily_pnl)

    # Top left: cumulative equity curve
    ax1 = fig.add_axes([0.08, 0.52, 0.55, 0.38])
    ax1.fill_between(range(len(cumulative)), cumulative, alpha=0.3, color=GREEN)
    ax1.plot(cumulative, color=GREEN, linewidth=2.5)
    ax1.set_xticks(range(len(dates)))
    ax1.set_xticklabels(dates, rotation=45, fontsize=7)
    ax1.set_ylabel('Cumulative PnL (pts)', fontsize=9)
    ax1.set_title('Equity Curve — V10 All Setups', fontsize=11, color=CYAN)
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color=TEXT, alpha=0.3, linewidth=0.5)

    # Top right: key metrics card
    ax2 = fig.add_axes([0.68, 0.52, 0.28, 0.38])
    ax2.axis('off')
    metrics = [
        ("Total PnL", "+922.8 pts", GREEN),
        ("Win Rate", "72.3%", GREEN),
        ("Profit Factor", "1.67x", GREEN),
        ("Max Drawdown", "202.8 pts", RED),
        ("PnL/MaxDD", "4.55x", CYAN),
        ("Worst Day", "-60.4 pts", RED),
        ("Win Days", "11 / 15", GREEN),
        ("Trades", "302", TEXT),
    ]
    for i, (label, value, color) in enumerate(metrics):
        y = 0.92 - i * 0.115
        ax2.text(0.05, y, label, fontsize=9, color=TEXT, alpha=0.7, transform=ax2.transAxes)
        ax2.text(0.95, y, value, fontsize=11, fontweight='bold', color=color,
                 ha='right', transform=ax2.transAxes)

    # Bottom: daily bar chart
    ax3 = fig.add_axes([0.08, 0.08, 0.88, 0.35])
    colors = [GREEN if p >= 0 else RED for p in daily_pnl]
    bars = ax3.bar(range(len(daily_pnl)), daily_pnl, color=colors, alpha=0.8, width=0.7)
    ax3.set_xticks(range(len(dates)))
    ax3.set_xticklabels(dates, rotation=45, fontsize=7)
    ax3.set_ylabel('Daily PnL (pts)', fontsize=9)
    ax3.set_title('Daily PnL — V10 All Setups, March 2026', fontsize=11, color=CYAN)
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.axhline(y=0, color=TEXT, alpha=0.5, linewidth=1)
    for bar, val in zip(bars, daily_pnl):
        y = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2, y + (3 if y >= 0 else -8),
                 f'{val:+.0f}', ha='center', fontsize=6.5, color=TEXT, alpha=0.8)

    pdf.savefig(fig)
    plt.close()

    # ===== PAGE 2: Setup Contribution + Risk =====
    fig = plt.figure(figsize=(11, 8.5))
    fig.suptitle("V10 Setup Contribution & Risk Profile", fontsize=18, fontweight='bold', color=GOLD, y=0.97)

    # Setup breakdown
    setups = ['Skew Charm', 'DD Exhaustion', 'AG Short', 'GEX Long', 'ES Absorption']
    setup_pnl = [553.0, 277.9, 97.6, 11.7, -17.5]
    setup_trades = [188, 83, 27, 1, 3]
    setup_wr = [77.8, 61.5, 72.7, 100, 33.3]
    setup_colors = [GREEN, BLUE, ORANGE, CYAN, RED]

    # Left: PnL contribution bar
    ax1 = fig.add_axes([0.08, 0.52, 0.4, 0.38])
    bars = ax1.barh(range(len(setups)), setup_pnl, color=setup_colors, alpha=0.8, height=0.6)
    ax1.set_yticks(range(len(setups)))
    ax1.set_yticklabels(setups, fontsize=9)
    ax1.set_xlabel('PnL (pts)', fontsize=9)
    ax1.set_title('PnL by Setup', fontsize=11, color=CYAN)
    ax1.grid(True, alpha=0.3, axis='x')
    ax1.axvline(x=0, color=TEXT, alpha=0.5)
    for bar, val in zip(bars, setup_pnl):
        x = bar.get_width()
        ax1.text(x + 5, bar.get_y() + bar.get_height()/2,
                 f'{val:+.0f}', va='center', fontsize=8, color=TEXT)

    # Right: WR + trades
    ax2 = fig.add_axes([0.55, 0.52, 0.4, 0.38])
    ax2.axis('off')
    ax2.text(0.5, 0.95, 'Setup Detail', fontsize=11, color=CYAN, ha='center',
             fontweight='bold', transform=ax2.transAxes)

    headers = ['Setup', 'Trades', 'WR', 'PnL']
    for j, h in enumerate(headers):
        ax2.text(0.02 + j*0.26, 0.85, h, fontsize=8, fontweight='bold',
                 color=GOLD, transform=ax2.transAxes)

    for i, (sn, tr, wr, pnl) in enumerate(zip(setups, setup_trades, setup_wr, setup_pnl)):
        y = 0.75 - i * 0.12
        c = GREEN if pnl > 0 else RED
        ax2.text(0.02, y, sn[:15], fontsize=8, color=TEXT, transform=ax2.transAxes)
        ax2.text(0.28, y, str(tr), fontsize=8, color=TEXT, transform=ax2.transAxes)
        ax2.text(0.54, y, f'{wr:.0f}%', fontsize=8, color=c, transform=ax2.transAxes)
        ax2.text(0.78, y, f'{pnl:+.0f}', fontsize=8, fontweight='bold', color=c, transform=ax2.transAxes)

    # Bottom: Risk assessment card
    ax3 = fig.add_axes([0.08, 0.08, 0.88, 0.35])
    ax3.axis('off')
    ax3.text(0.5, 0.95, 'Risk Assessment — Real Money Implications', fontsize=13,
             color=GOLD, ha='center', fontweight='bold', transform=ax3.transAxes)

    risk_data = [
        ['', '1 MES', '1 ES', '2 ES'],
        ['Monthly PnL', '$1,938', '$19,379', '$38,758'],
        ['Max Drawdown', '$1,014', '$10,140', '$20,280'],
        ['Worst Day', '-$302', '-$3,020', '-$6,040'],
        ['Capital Needed', '$4,056', '$40,560', '$81,120'],
        ['DD % Capital', '25%', '25%', '25%'],
        ['Monthly ROI', '48%', '48%', '48%'],
    ]

    for i, row in enumerate(risk_data):
        y = 0.80 - i * 0.11
        for j, val in enumerate(row):
            x = 0.12 + j * 0.22
            if i == 0:
                ax3.text(x, y, val, fontsize=10, fontweight='bold', color=CYAN,
                         ha='center', transform=ax3.transAxes)
            else:
                c = TEXT
                if 'Drawdown' in row[0] or 'Worst' in row[0]:
                    c = RED if j > 0 else TEXT
                elif 'PnL' in row[0] and 'DD' not in row[0]:
                    c = GREEN if j > 0 else TEXT
                ax3.text(x, y, val, fontsize=9, color=c, ha='center', transform=ax3.transAxes)

    ax3.text(0.5, 0.05, '* Based on 30% of March backtest (conservative). Full MaxDD preserved for risk.',
             fontsize=7, color=TEXT, alpha=0.5, ha='center', transform=ax3.transAxes)

    pdf.savefig(fig)
    plt.close()

    # ===== PAGE 3: 1-Year Scaling Plan =====
    fig = plt.figure(figsize=(11, 8.5))
    fig.suptitle("Realistic 1-Year Scaling Plan", fontsize=18, fontweight='bold', color=GOLD, y=0.97)
    fig.text(0.5, 0.93, '30% of March PnL  |  Full MaxDD  |  4x Safety Buffer  |  2 Flat Months/Year',
             fontsize=9, color=TEXT, alpha=0.6, ha='center')

    # Monthly capital growth
    months = list(range(1, 13))
    capitals = [5000, 6550, 8488, 10813, 11007, 14883, 18371, 24766, 30967, 31936, 41625, 64880]
    end_caps = [6550, 8488, 10813, 11007, 14883, 18371, 24766, 30967, 31936, 41625, 64880, 86197]
    monthly_pnl_data = [1550, 1938, 2325, 194, 3876, 3488, 6395, 6201, 969, 9689, 23255, 21317]
    configs = ['1 MES', '1 MES', '1 MES', '1 MES', '2 MES', '2 MES',
               '3 MES', '4 MES', '5 MES', '5 MES', '1 ES', '11 MES']

    # Top: capital growth curve
    ax1 = fig.add_axes([0.08, 0.52, 0.88, 0.35])
    ax1.fill_between(months, [c/1000 for c in end_caps], alpha=0.3, color=GREEN)
    ax1.plot(months, [c/1000 for c in end_caps], color=GREEN, linewidth=2.5, marker='o', markersize=6)
    for i, (m, c) in enumerate(zip(months, end_caps)):
        offset = 2 if i % 2 == 0 else -4
        ax1.annotate(f'${c/1000:.0f}K', (m, c/1000), textcoords="offset points",
                     xytext=(0, 10+offset), fontsize=7, color=TEXT, ha='center')
    ax1.set_xlabel('Month', fontsize=9)
    ax1.set_ylabel('Capital ($K)', fontsize=9)
    ax1.set_title('Capital Growth — $5K Start', fontsize=11, color=CYAN)
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(months)

    # Bottom: monthly statement table
    ax2 = fig.add_axes([0.05, 0.05, 0.92, 0.40])
    ax2.axis('off')

    headers = ['Month', 'Start Cap', 'Config', 'Monthly PnL', 'MaxDD', 'DD%', 'End Capital']
    for j, h in enumerate(headers):
        x = 0.02 + j * 0.14
        ax2.text(x, 0.95, h, fontsize=8, fontweight='bold', color=GOLD, transform=ax2.transAxes)

    dd_per_config = {
        '1 MES': 1014, '2 MES': 2028, '3 MES': 3042, '4 MES': 4056,
        '5 MES': 5070, '11 MES': 11154, '1 ES': 10140
    }

    for i in range(12):
        y = 0.87 - i * 0.068
        dd = dd_per_config.get(configs[i], 1014)
        dd_pct = round(dd / capitals[i] * 100)
        pnl_color = GREEN if monthly_pnl_data[i] > 500 else (ORANGE if monthly_pnl_data[i] > 0 else RED)

        vals = [
            (f'{i+1}', TEXT),
            (f'${capitals[i]:,}', TEXT),
            (configs[i], CYAN),
            (f'${monthly_pnl_data[i]:+,}', pnl_color),
            (f'${dd:,}', RED),
            (f'{dd_pct}%', RED if dd_pct > 25 else ORANGE),
            (f'${end_caps[i]:,}', GREEN),
        ]
        for j, (val, color) in enumerate(vals):
            x = 0.02 + j * 0.14
            ax2.text(x, y, val, fontsize=7.5, color=color, transform=ax2.transAxes)

    pdf.savefig(fig)
    plt.close()

    # ===== PAGE 4: Path to $80K/month + Honest Assessment =====
    fig = plt.figure(figsize=(11, 8.5))
    fig.suptitle("Path to $80K/Month & Honest Assessment", fontsize=18, fontweight='bold', color=GOLD, y=0.97)

    # Scale reference chart
    ax1 = fig.add_axes([0.08, 0.55, 0.88, 0.35])
    scale_names = ['1 MES\nSC', '1 MES\nAll', '2 MES', '4 MES', '1 ES', '2 ES', '3 ES', '5 ES']
    scale_monthly = [1163, 1938, 3876, 7752, 19379, 38758, 58136, 96894]
    scale_dd = [640, 1014, 2028, 4056, 10140, 20280, 30420, 50700]

    x_pos = np.arange(len(scale_names))
    width = 0.35

    bars1 = ax1.bar(x_pos - width/2, [m/1000 for m in scale_monthly], width,
                    label='Monthly PnL ($K)', color=GREEN, alpha=0.8)
    bars2 = ax1.bar(x_pos + width/2, [d/1000 for d in scale_dd], width,
                    label='Max DD ($K)', color=RED, alpha=0.8)

    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(scale_names, fontsize=7)
    ax1.set_ylabel('$K', fontsize=9)
    ax1.set_title('Monthly PnL vs Max Drawdown at Each Scale', fontsize=11, color=CYAN)
    ax1.legend(fontsize=8, loc='upper left')
    ax1.grid(True, alpha=0.3, axis='y')

    # Target line
    ax1.axhline(y=80, color=GOLD, linewidth=1.5, linestyle='--', alpha=0.7)
    ax1.text(len(scale_names)-1, 83, '$80K TARGET', fontsize=8, color=GOLD, ha='right')

    # Bottom: honest assessment
    ax2 = fig.add_axes([0.08, 0.05, 0.84, 0.42])
    ax2.axis('off')

    ax2.text(0.5, 0.95, 'Honest Assessment', fontsize=14, color=CYAN,
             ha='center', fontweight='bold', transform=ax2.transAxes)

    points = [
        ("THE EDGE IS REAL", "V10 filter shows consistent alpha across 300+ trades in March. "
         "72% WR, 1.67x PF, positive on 11/15 days.", GREEN),
        ("30% BASELINE IS CONSERVATIVE", "March was bearish + high VIX — a tough month for "
         "our primarily bullish system. Normal months may perform better.", BLUE),
        ("BUT: 15 DAYS ≠ 12 MONTHS", "We're projecting a year from 15 trading days. "
         "The edge might be larger or smaller. Prove it month by month.", ORANGE),
        ("RISK FIRST, ALWAYS", "MaxDD doesn't scale down with PnL. A -200pt day can happen "
         "anytime. Never risk more than you can lose in a worst-case day.", RED),
        ("REALISTIC YEAR 1", "$30-50K total profit. Ending at $10-20K/month. "
         "Still 600-1000% return on $5K. Exceptional by any standard.", TEXT),
        ("$80K/MONTH TARGET", "Achievable at 5 ES ($250/pt). Needs ~$200K capital. "
         "Reachable month 16-24 with disciplined compounding.", GOLD),
    ]

    for i, (title, desc, color) in enumerate(points):
        y = 0.82 - i * 0.14
        ax2.text(0.02, y, "●", fontsize=12, color=color, transform=ax2.transAxes)
        ax2.text(0.06, y, title, fontsize=9, fontweight='bold', color=color, transform=ax2.transAxes)
        ax2.text(0.06, y - 0.055, desc, fontsize=7.5, color=TEXT, alpha=0.8,
                 transform=ax2.transAxes, wrap=True)

    ax2.text(0.5, 0.01, f'Generated {datetime.now().strftime("%B %d, %Y")} — 0DTE Alpha V10',
             fontsize=7, color=TEXT, alpha=0.4, ha='center', transform=ax2.transAxes)

    pdf.savefig(fig)
    plt.close()

print(f"PDF saved: {pdf_path}")
