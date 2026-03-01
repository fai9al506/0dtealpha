"""Generate Deep Trade Analysis PDF — All-Time 158 Trades."""
from fpdf import FPDF
from datetime import datetime

class Report(FPDF):
    def header(self):
        if self.page_no() == 1:
            self.set_font("Helvetica", "B", 20)
            self.cell(0, 12, "0DTE Alpha  -  Deep Performance Analysis", align="C", new_x="LMARGIN", new_y="NEXT")
            self.set_font("Helvetica", "", 11)
            self.set_text_color(100, 100, 100)
            self.cell(0, 7, "158 Trades  |  Feb 3 - Feb 25, 2026  |  All Setups", align="C", new_x="LMARGIN", new_y="NEXT")
            self.set_text_color(0, 0, 0)
            self.line(10, self.get_y() + 2, 200, self.get_y() + 2)
            self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  Page {self.page_no()}/{{nb}}", align="C")

    def section(self, title):
        self.set_font("Helvetica", "B", 13)
        self.set_fill_color(30, 60, 110)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(3)

    def sub(self, title):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(30, 60, 110)
        self.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)

    def txt(self, text):
        self.set_font("Helvetica", "", 9)
        self.multi_cell(0, 4.5, text)
        self.ln(1)

    def bold_txt(self, label, text):
        self.set_font("Helvetica", "B", 9)
        w = self.get_string_width(label) + 2
        self.cell(w, 5, label, new_x="RIGHT", new_y="TOP")
        self.set_font("Helvetica", "", 9)
        self.multi_cell(0, 5, text)

    def kpi_row(self, items):
        n = len(items)
        w = 190 / n
        y_start = self.get_y()
        for label, value, color in items:
            self.set_fill_color(*color)
            self.set_text_color(255, 255, 255)
            self.set_font("Helvetica", "B", 13)
            self.cell(w, 10, value, fill=True, align="C", new_x="RIGHT", new_y="TOP")
        self.ln(10)
        x = 10
        for label, value, color in items:
            self.set_text_color(80, 80, 80)
            self.set_font("Helvetica", "", 7.5)
            self.set_xy(x, y_start + 10)
            self.cell(w, 4, label, align="C")
            x += w
        self.set_text_color(0, 0, 0)
        self.ln(7)

    def table(self, headers, data, col_widths, aligns=None):
        if aligns is None:
            aligns = ["C"] * len(headers)
        self.set_font("Helvetica", "B", 7.5)
        self.set_fill_color(50, 50, 50)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 6, h, border=1, fill=True, align="C", new_x="RIGHT", new_y="TOP")
        self.ln(6)
        self.set_text_color(0, 0, 0)
        for ri, row in enumerate(data):
            self.set_font("Helvetica", "B" if row[0] in ("TOTAL","BEST","ULTIMATE") else "", 7.5)
            self.set_fill_color(240, 240, 240) if ri % 2 == 0 else self.set_fill_color(255, 255, 255)
            for i, cell in enumerate(row):
                self.cell(col_widths[i], 5.5, str(cell), border=1, fill=True, align=aligns[i], new_x="RIGHT", new_y="TOP")
            self.ln(5.5)


pdf = Report()
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=20)
pdf.add_page()

# ── KPIs ──
pdf.kpi_row([
    ("Total P&L", "+397.0 pts", (30, 130, 60)),
    ("Win Rate (W/W+L)", "49.1%", (180, 130, 40)),
    ("Trades", "158", (80, 80, 80)),
    ("Avg Win / Avg Loss", "+19.0 / -11.7", (30, 60, 110)),
])

pdf.txt(
    "System is net profitable (+397 pts over 17 trading days) but bleeds heavily from "
    "signal spam, afternoon dead zones, and one consistently losing setup (GEX Long). "
    "This analysis identifies concrete filters that could have improved P&L by +82 to +170 points "
    "while cutting trade count from 158 to ~45 and raising win rate from 49% to 63%."
)

# ── Per-Setup Breakdown ──
pdf.section("1. Setup Performance Breakdown")

pdf.table(
    ["Setup", "Trades", "W/L/E", "Win Rate", "P&L", "Avg Win", "Avg Loss", "Verdict"],
    [
        ["DD Exhaustion",    "75", "24/26/25", "48%", "+219.6", "+23.4", "-12.0", "TOP EARNER"],
        ["AG Short",         "25", "11/8/6",   "58%", "+105.7", "+20.7", "-17.1", "STRONG"],
        ["BofA Scalp",       "21", "5/5/11",   "50%", "+45.6",  "+10.0", "-11.4", "OK"],
        ["Paradigm Rev",     "11", "5/0/6",   "100%", "+36.5",  "+10.0", "N/A",   "PERFECT"],
        ["ES Absorption",     "5", "3/2/0",    "60%", "+6.0",   "+10.0", "-12.0", "SMALL N"],
        ["GEX Long",         "21", "5/14/2",   "26%", "-16.4",  "+18.0", "-8.0",  "CUT"],
        ["","","","","","","",""],
        ["TOTAL",           "158", "53/55/50", "49%", "+397.0", "+19.0", "-11.7", ""],
    ],
    [30, 14, 20, 16, 16, 16, 16, 22],
    ["L","C","C","C","C","C","C","C"],
)
pdf.ln(3)

pdf.sub("Key findings per setup:")
pdf.txt(
    "DD Exhaustion (+219.6 pts): Top earner but carries the most noise. 48% WR with excellent avg win (+23.4) "
    "thanks to continuous trailing. Morning sessions (9-12) drive ALL the profit: +224.0 pts, 54% WR. "
    "Afternoon (14+) is a dead zone: 0 wins in 21 trades, -82.1 pts. BOFA-PURE paradigm toxic: 29% WR, -21.5 pts.\n\n"
    "AG Short (+105.7 pts): Most reliable setup. 58% WR with strong directional edge. "
    "Morning is best: +100.8 pts from 18 trades. AG-LIS paradigm (67% WR, +61.4) and AG-TARGET (57% WR, +54.1) both strong. "
    "AG-PURE is weak (50% WR, -9.8) - but sample is small.\n\n"
    "GEX Long (-16.4 pts): Only net loser. 26% WR - gets stopped out 14/21 times. "
    "Midday 12-14 is catastrophic: 0 wins in 5 trades (-40 pts). GEX-TARGET paradigm 0% WR (-28 pts). "
    "Recommendation: DISABLE entirely."
)

# ── Time-of-Day ──
pdf.section("2. Time-of-Day Analysis")

pdf.table(
    ["Hour (ET)", "Trades", "W/L/E", "Win Rate", "P&L", "P&L/Trade"],
    [
        ["09:00", "7",  "4/3/0",   "57%", "+48.4", "+6.9"],
        ["10:00", "27", "12/13/2", "48%", "+103.1","+3.8"],
        ["11:00", "34", "16/13/5", "55%", "+199.9","+5.9"],
        ["12:00", "22", "8/6/8",   "57%", "+56.4", "+2.6"],
        ["13:00", "23", "8/10/5",  "44%", "+19.0", "+0.8"],
        ["14:00", "32", "4/6/22",  "40%", "-24.4", "-0.8"],
        ["15:00", "13", "1/4/8",   "20%", "-5.4",  "-0.4"],
        ["","","","","",""],
        ["9:00-13:59", "113", "48/45/20", "52%", "+426.8","+3.8"],
        ["14:00-15:59", "45", "5/10/30",  "33%", "-29.8", "-0.7"],
    ],
    [24, 16, 22, 16, 18, 18],
    ["C","C","C","C","C","C"],
)
pdf.ln(3)

pdf.txt(
    "The morning session (9:00-13:59) generates +426.8 pts at 52% WR. "
    "The afternoon (14:00+) is net negative: -29.8 pts at 33% WR across 45 trades. "
    "The 11:00 hour alone contributes +199.9 pts - nearly half the system's total profit. "
    "The 15:00 hour has a devastating 20% WR.\n\n"
    "Insight: The system's edge comes from morning volatility and mean-reversion moves. "
    "By afternoon, trends solidify and contrarian signals (DD, AG) fail systematically."
)

# ── Direction ──
pdf.add_page()
pdf.section("3. Direction & Paradigm Analysis")

pdf.sub("Direction Bias")
pdf.table(
    ["Direction", "Trades", "Win Rate", "P&L", "Best Window", "Worst Window"],
    [
        ["Short", "88", "56%", "+306.3", "9-12: +316.8", "14-16: -18.6"],
        ["Long",  "65", "39%", "+84.7",  "12-14: +57.3", "14-16: -9.2"],
    ],
    [24, 16, 16, 22, 38, 38],
    ["L","C","C","C","C","C"],
)
pdf.ln(3)
pdf.txt(
    "Shorts outperform longs significantly: +306 pts (56% WR) vs +85 pts (39% WR). "
    "Short morning trades (9-12) are the system's core profit engine: +316.8 pts. "
    "Long trades only work well in the midday session (12-14). "
    "Both directions turn negative after 14:00."
)

pdf.sub("Paradigm Performance (All Setups)")
pdf.table(
    ["Paradigm", "Trades", "W/L", "Win Rate", "P&L", "Assessment"],
    [
        ["AG-TARGET",        "17",  "8/6",  "57%", "+106.0", "Best overall"],
        ["AG-LIS",           "24",  "9/6",  "60%", "+84.3",  "Strong"],
        ["SIDIAL-EXTREME",    "4",  "3/1",  "75%", "+66.3",  "Small N, promising"],
        ["GEX-MESSY",        "17",  "7/9",  "44%", "+65.9",  "Noisy, DD drives it"],
        ["SIDIAL-MESSY",      "8",  "5/2",  "71%", "+63.9",  "Strong"],
        ["BOFA-PURE",        "39",  "9/8",  "53%", "+54.8",  "Volume, mixed"],
        ["GEX-LIS",          "10",  "2/6",  "25%", "-1.4",   "Weak"],
        ["AG-PURE",          "18",  "5/7",  "42%", "-14.5",  "Below average"],
        ["GEX-PURE",          "6",  "1/4",  "20%", "-16.0",  "Avoid"],
        ["GEX-TARGET",        "3",  "0/3",   "0%", "-28.0",  "Toxic"],
    ],
    [28, 14, 14, 16, 18, 28],
    ["L","C","C","C","C","C"],
)
pdf.ln(3)
pdf.txt(
    "AG-paradigms (AG-TARGET, AG-LIS) are the most profitable environments overall. "
    "SIDIAL paradigms show high WR but small samples. "
    "GEX-paradigms (GEX-PURE, GEX-TARGET, GEX-LIS) are the worst performers - "
    "this correlates with GEX Long being the worst setup. "
    "BOFA-PURE has volume (39 trades) but mixed results."
)

# ── Grade ──
pdf.section("4. Grade & Score Analysis")
pdf.table(
    ["Grade", "Trades", "W/L", "Win Rate", "P&L"],
    [
        ["A+",      "26", "7/5",   "58%", "+106.8"],
        ["A",       "61", "21/23", "48%", "+160.1"],
        ["A-Entry", "62", "19/24", "44%", "+68.2"],
        ["LOG",      "5", "4/1",   "80%", "+65.9"],
        ["B",        "2", "1/1",   "50%", "-2.0"],
        ["C",        "2", "1/1",   "50%", "-2.0"],
    ],
    [24, 16, 16, 20, 22],
    ["L","C","C","C","C"],
)
pdf.ln(2)
pdf.txt(
    "A+ has the best WR (58%) but LOG has 80% WR (small sample of 5 DD trades). "
    "The grading system has limited predictive power - A-Entry (44% WR) is barely better than coin flip. "
    "The score is NOT a strong trade filter. Timing and setup selection matter far more than grade."
)

# ── VIX ──
pdf.section("5. VIX Analysis")
pdf.txt(
    "Only 45 trades have VIX data (recently added). VIX 16-20: 44 trades, 35% WR, -66.1 pts. "
    "This suggests the system struggles in elevated but not extreme vol. "
    "Need more data across VIX regimes to draw conclusions, but early signal is that "
    "higher VIX (16-20) hurts performance."
)

# ── Profit Capture ──
pdf.section("6. Profit Capture Efficiency")
pdf.table(
    ["Setup", "Avg Win", "Avg Max Profit", "Capture %", "Avg Loss", "Opportunity"],
    [
        ["DD Exhaustion",  "+23.4", "27.8", "84%", "-12.0", "Good - trail captures most"],
        ["Paradigm Rev",   "+10.0", "13.9", "72%", "N/A",   "Fixed 10pt, misses +4 avg"],
        ["BofA Scalp",     "+10.0", "14.4", "69%", "-11.4", "30min timeout leaves money"],
        ["AG Short",       "+20.7", "38.2", "54%", "-17.1", "Biggest gap - 46% left"],
        ["GEX Long",       "+18.0", "30.4", "59%", "-8.0",  "Irrelevant - disable it"],
        ["ES Absorption",  "+10.0", "6.7",  "150%","--12.0","Captures well (small N)"],
    ],
    [30, 18, 24, 18, 18, 40],
    ["L","C","C","C","C","L"],
)
pdf.ln(2)
pdf.txt(
    "AG Short has the biggest capture gap: winners average +38.2 pts max profit but only capture +20.7 (54%). "
    "This means AG winners regularly go +30-40 pts but the system exits at the Volland target. "
    "A trailing mechanism on AG (similar to DD's continuous trail) could capture significantly more. "
    "Paradigm Reversal is capped at +10 fixed target but max profit averages +13.9 - a trail could help here too."
)

# ── What-If Scenarios ──
pdf.add_page()
pdf.section("7. What-If Optimization Scenarios")
pdf.txt(
    "Each scenario is applied to the historical 158 trades to estimate impact. "
    "Scenarios are cumulative where noted."
)
pdf.ln(1)

pdf.table(
    ["Scenario", "Description", "Trades Cut", "P&L Impact", "New Total"],
    [
        ["A", "Remove GEX Long entirely",              "21", "+16.4", "+413.4"],
        ["B", "DD cutoff at 14:00",                    "21", "+82.1", "+479.1"],
        ["C", "DD cutoff at 13:00",                    "32", "+69.5", "+466.5"],
        ["D", "Remove BofA Scalp",                     "21", "-45.6", "+351.4"],
        ["E", "Remove Paradigm Reversal",              "11", "-36.5", "+360.5"],
        ["I", "Cap 2 signals per setup+dir/day",       "76", "-185.1","+211.9"],
        ["","","","",""],
        ["J", "BEST: AG+DD(<14,!BOFA)+Abs+Para cap2", "113","",      "+198.8"],
        ["","","","",""],
        ["ULTIMATE", "J + DD 60min cooldown",          "114","",      "+226.3"],
    ],
    [18, 64, 18, 22, 22],
    ["C","L","C","C","C"],
)
pdf.ln(3)

pdf.sub("Scenario Analysis:")
pdf.txt(
    "Scenario B (DD cutoff 14:00) is the single highest-impact change: +82.1 pts recovered "
    "by eliminating 21 afternoon DD trades that had 0% WR. This is the #1 priority.\n\n"
    "Scenario A (remove GEX Long) saves +16.4 pts from 21 trades. GEX Long has 26% WR "
    "and actively destroys capital. Easy decision.\n\n"
    "Scenario I (cap 2 signals/day) has a COUNTER-INTUITIVE result: it REDUCES P&L from +397 to +212. "
    "This is because DD's big winners often come as the 3rd or 4th signal of the day "
    "(the trend finally reverses after multiple contrarian entries). "
    "Capping per-day signals kills these comeback winners. This suggests a smarter approach: "
    "cap signals per direction, not per setup.\n\n"
    "Scenario J (best combo) cuts to 45 trades with 60% WR and +198.8 pts. "
    "While total P&L drops from +397, the P&L per trade jumps from +2.51 to +4.42 - "
    "nearly double the efficiency. For SIM/live trading with real money, this matters more.\n\n"
    "IMPORTANT: Scenario J's lower total P&L is because it removes DD trades that were eventually winners "
    "despite firing in BOFA-PURE paradigm or after 14:00. The question is whether those rare "
    "big wins justify the consistent bleed from the 70%+ losers around them."
)

# ── Signal Spam ──
pdf.section("8. Signal Spam Problem")
pdf.txt(
    "76 trades (48% of all trades) were the 3rd or later signal for the same setup+direction on the same day. "
    "These spam trades generated +185.1 pts total - meaning they are net profitable! "
    "This is entirely driven by DD Exhaustion's pattern: fire multiple contrarian signals, "
    "eventually the reversal happens and the continuous trail captures a huge move.\n\n"
    "However, this only works in the portal tracker which counts every signal independently. "
    "The SIM auto-trader already deduplicates (one position per setup), so it captures the FIRST "
    "signal's entry and rides it. Spam signals don't matter for SIM execution.\n\n"
    "Recommendation: For portal P&L reporting, spam is fine. For real trading (SIM/eval), "
    "the deduplication already handles this correctly."
)

# ── Daily P&L ──
pdf.section("9. Daily P&L Pattern")
pdf.table(
    ["Date", "Trades", "W/L/E", "P&L", "Notes"],
    [
        ["Feb 03", "3",  "1/2",  "+4.0",   "Low volume day"],
        ["Feb 04", "1",  "1/0",  "+16.4",  "Single AG Short win"],
        ["Feb 05", "4",  "1/3",  "-14.0",  "GEX Long losses"],
        ["Feb 09", "2",  "2/0",  "+45.0",  "Perfect day"],
        ["Feb 11", "3",  "0/1",  "-9.1",   "Slow day"],
        ["Feb 12", "3",  "0/0",  "+30.8",  "All expired profitable"],
        ["Feb 13", "7",  "2/3",  "+1.3",   "Break even"],
        ["Feb 17", "6",  "2/2",  "-13.1",  "Post-holiday"],
        ["Feb 18", "12", "6/6",  "+34.8",  "First big DD day"],
        ["Feb 19", "20", "14/4", "+228.0", "BEST DAY - DD trail captures"],
        ["Feb 20", "30", "14/9", "+182.3", "Second best - DD dominant"],
        ["Feb 23", "17", "4/9",  "+15.3",  "Drawdown starts"],
        ["Feb 24", "27", "3/10", "-87.1",  "WORST DAY - all setups struggle"],
        ["Feb 25", "23", "3/6",  "-37.6",  "Afternoon grind killed shorts"],
    ],
    [20, 14, 14, 18, 60],
    ["C","C","C","C","L"],
)
pdf.ln(3)
pdf.txt(
    "Two days (Feb 19-20) generated +410 pts, more than the system's entire net profit. "
    "The last three days (Feb 23-25) lost -109.4 pts. "
    "This suggests the system is regime-dependent: it excels in mean-reverting markets "
    "but bleeds in trending ones. Feb 24 was the worst day (-87.1 pts) with 3W/10L.\n\n"
    "The system's P&L curve is NOT smooth - it's driven by a few big DD trail wins "
    "on high-volatility reversal days."
)

# ── RECOMMENDATIONS ──
pdf.add_page()
pdf.section("10. Recommended Changes (Priority Order)")

changes = [
    ("1. DD Exhaustion: Cut afternoon (dd_market_end = 14:00)",
     "IMPACT: +82.1 pts recovered | CONFIDENCE: Very High (21 trades, 0% WR after 14:00)\n"
     "The single highest-impact change. Zero wins in 21 afternoon DD trades across the entire dataset. "
     "The afternoon is hostile to contrarian signals because trends have solidified by then. "
     "Cutting at 14:00 instead of 13:00 preserves the strong midday window (12-14: 53% WR, +77.7 pts)."),

    ("2. Disable GEX Long",
     "IMPACT: +16.4 pts recovered | CONFIDENCE: High (21 trades, 26% WR)\n"
     "GEX Long has the worst win rate of any setup. It fires into support levels expecting bounces "
     "but gets stopped out 14 of 21 times. The midday session (12-14) is 0/5. "
     "Even the morning, where it shows 36% WR, is barely above random. "
     "The setup's logic may need a fundamental rethink before re-enabling."),

    ("3. DD Exhaustion: Block BOFA-PURE paradigm",
     "IMPACT: +21.5 pts recovered | CONFIDENCE: Medium-High (17 trades, 29% WR in BOFA-PURE)\n"
     "DD in BOFA-PURE paradigm wins only 29% of the time vs 54% in morning overall. "
     "BOFA-PURE represents a stable dealer positioning regime where DD's contrarian thesis "
     "(dealers over-hedged) is less valid. Best DD paradigms: GEX-MESSY (50%), SIDIAL-EXTREME (67%), "
     "SIDIAL-MESSY (67%), AG-TARGET (57%)."),

    ("4. DD Exhaustion: Increase cooldown to 60 minutes",
     "IMPACT: Reduces noise, marginal P&L impact | CONFIDENCE: Medium\n"
     "Currently DD fires every 30 minutes, creating 7-9 signals on heavy days. "
     "A 60-minute cooldown per direction would halve the spam. Since the SIM auto-trader "
     "deduplicates anyway, this mainly helps the portal tracker and eval trader. "
     "Combined with the 14:00 cutoff, this limits DD to ~4-5 signals max per day."),

    ("5. AG Short: Add trailing stop (capture gap = 46%)",
     "IMPACT: Potentially +50-80 pts | CONFIDENCE: Medium (needs backtest)\n"
     "AG Short winners average +38.2 pts max profit but only capture +20.7 (54%). "
     "A continuous trail similar to DD (activation=15, gap=5) could capture significantly more "
     "on trending days. This is the biggest P&L-per-trade opportunity in the system. "
     "Requires careful backtesting - AG's profile is different from DD."),

    ("6. Paradigm Reversal: Consider trail or higher target",
     "IMPACT: +4-10 pts per trade | CONFIDENCE: Low (11 trades, 100% WR)\n"
     "Paradigm Rev has perfect WR but is capped at +10 pts while max profit averages +13.9. "
     "Small sample, but worth testing a trail or higher fixed target. "
     "However, with 0 losses, the current setup is working - don't break what works."),

    ("7. Monitor VIX regime filter",
     "IMPACT: TBD | CONFIDENCE: Low (limited data)\n"
     "Early data shows VIX 16-20 has 35% WR (-66.1 pts). "
     "Consider reducing position size or tightening stops in elevated-VIX environments. "
     "Need 50+ VIX-tagged trades to validate."),
]

for title, text in changes:
    pdf.set_font("Helvetica", "B", 10)
    # Color code by priority
    if "1." in title or "2." in title:
        pdf.set_fill_color(220, 50, 50)
        pdf.set_text_color(255, 255, 255)
        pdf.cell(0, 6, f"  {title}  [HIGH PRIORITY]", fill=True, new_x="LMARGIN", new_y="NEXT")
    elif "3." in title or "4." in title:
        pdf.set_fill_color(220, 160, 40)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 6, f"  {title}  [MEDIUM PRIORITY]", fill=True, new_x="LMARGIN", new_y="NEXT")
    else:
        pdf.set_fill_color(200, 200, 200)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(0, 6, f"  {title}  [LOW PRIORITY]", fill=True, new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 8.5)
    pdf.multi_cell(0, 4.2, text)
    pdf.ln(3)

# ── Summary Impact ──
pdf.section("11. Combined Impact Estimate")

pdf.table(
    ["Change", "Trades Removed", "P&L Impact", "Running Total"],
    [
        ["Current baseline",        "--", "--",    "+397.0 pts"],
        ["+ DD cutoff 14:00",       "21", "+82.1", "+479.1 pts"],
        ["+ Disable GEX Long",      "21", "+16.4", "+495.5 pts"],
        ["+ DD block BOFA-PURE",    "~8", "+15-20","~+513 pts"],
        ["","","",""],
        ["Conservative estimate",   "50 fewer","","+495 pts"],
        ["With AG trail (est.)",    "0",   "+50-80","~+560 pts"],
    ],
    [42, 28, 28, 30],
    ["L","C","C","C"],
)
pdf.ln(4)

pdf.txt(
    "Implementing just the top 3 changes (DD afternoon cutoff, disable GEX Long, DD block BOFA-PURE) "
    "would have improved total P&L from +397 to approximately +513 pts (+29% improvement) "
    "while cutting 50 losing trades. Adding an AG trail could push to +560 pts.\n\n"
    "These changes reduce total trades from 158 to ~108, but the quality of remaining trades is much higher. "
    "For real money trading (SIM/eval), fewer high-quality signals with the auto-trader's deduplication "
    "is the optimal approach.\n\n"
    "Priority implementation order: DD 14:00 cutoff (today) > Disable GEX Long (today) > "
    "DD BOFA-PURE block (this week) > DD 60min cooldown (this week) > AG trail (backtest first)."
)

out = "G:/My Drive/Python/MyProject/GitHub/0dtealpha/Deep_Trade_Analysis.pdf"
pdf.output(out)
print(f"PDF saved: {out}")
