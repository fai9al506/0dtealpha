"""
Generate Trend-Following Research PDF for 0DTE Alpha.
Run: python _gen_trend_research_pdf.py
Output: Trend_Following_Research_0DTE_Alpha.pdf
"""
from fpdf import FPDF
from datetime import datetime

class ResearchPDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=20)
        # Use Windows system fonts for Unicode support
        self.add_font("DejaVu", "", fname="C:/Windows/Fonts/arial.ttf")
        self.add_font("DejaVu", "B", fname="C:/Windows/Fonts/arialbd.ttf")
        self.add_font("DejaVu", "I", fname="C:/Windows/Fonts/ariali.ttf")
        self.add_font("DejaVu", "BI", fname="C:/Windows/Fonts/arialbi.ttf")

    def header(self):
        if self.page_no() > 1:
            self.set_font("DejaVu", "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 8, "0DTE Alpha -- Trend-Following Research  |  March 2026", align="C")
            self.ln(4)
            self.set_draw_color(200, 200, 200)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def title_page(self):
        self.add_page()
        self.ln(50)
        self.set_font("DejaVu", "B", 28)
        self.set_text_color(20, 60, 120)
        self.cell(0, 15, "From Mean-Reversion to", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 15, "Trend-Riding", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(8)
        self.set_font("DejaVu", "", 16)
        self.set_text_color(80, 80, 80)
        self.cell(0, 10, "A Deep Research Analysis for 0DTE Alpha", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(12)
        self.set_draw_color(20, 60, 120)
        self.set_line_width(0.8)
        self.line(60, self.get_y(), 150, self.get_y())
        self.ln(12)
        self.set_font("DejaVu", "", 11)
        self.set_text_color(100, 100, 100)
        self.cell(0, 7, "Sources: Volland Discord (35,713 messages), Volland White Paper,", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 7, "Volland User Guide, SpotGamma, OptionMetrics, Academic Research", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(6)
        self.cell(0, 7, f"Compiled: March 27, 2026", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 7, "Classification: Internal / Proprietary", align="C", new_x="LMARGIN", new_y="NEXT")

    def section_title(self, num, title):
        self.ln(6)
        self.set_font("DejaVu", "B", 16)
        self.set_text_color(20, 60, 120)
        self.cell(0, 10, f"{num}. {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(20, 60, 120)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def subsection(self, title):
        self.ln(3)
        self.set_font("DejaVu", "B", 12)
        self.set_text_color(40, 40, 40)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def sub_subsection(self, title):
        self.ln(2)
        self.set_font("DejaVu", "BI", 11)
        self.set_text_color(60, 60, 60)
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body(self, text):
        self.set_font("DejaVu", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bold_body(self, text):
        self.set_font("DejaVu", "B", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def quote(self, who, text):
        self.set_font("DejaVu", "I", 9.5)
        self.set_text_color(60, 60, 60)
        x = self.get_x()
        self.set_x(x + 8)
        self.set_draw_color(20, 60, 120)
        self.set_line_width(0.4)
        y_start = self.get_y()
        self.multi_cell(170, 5, f'{who}: "{text}"')
        y_end = self.get_y()
        self.line(16, y_start, 16, y_end)
        self.ln(2)

    def bullet(self, text, bold_prefix=""):
        self.set_font("DejaVu", "", 10)
        self.set_text_color(30, 30, 30)
        x = self.get_x()
        self.set_x(x + 6)
        self.cell(4, 5.5, chr(8226) + " ")
        if bold_prefix:
            self.set_font("DejaVu", "B", 10)
            self.write(5.5, bold_prefix + " ")
            self.set_font("DejaVu", "", 10)
        self.multi_cell(168, 5.5, text)
        self.ln(0.5)

    def table_header(self, cols, widths):
        self.set_font("DejaVu", "B", 9)
        self.set_fill_color(20, 60, 120)
        self.set_text_color(255, 255, 255)
        for i, col in enumerate(cols):
            self.cell(widths[i], 7, col, border=1, fill=True, align="C")
        self.ln()

    def table_row(self, cells, widths, fill=False):
        self.set_font("DejaVu", "", 9)
        self.set_text_color(30, 30, 30)
        if fill:
            self.set_fill_color(240, 245, 255)
        else:
            self.set_fill_color(255, 255, 255)
        max_h = 7
        for i, cell in enumerate(cells):
            self.cell(widths[i], max_h, str(cell), border=1, fill=True, align="C" if i > 0 else "L")
        self.ln()

    def key_insight_box(self, text):
        self.ln(2)
        self.set_fill_color(255, 248, 230)
        self.set_draw_color(220, 180, 50)
        self.set_line_width(0.4)
        x = self.get_x()
        y = self.get_y()
        self.rect(x, y, 190, 14, style="DF")
        self.set_xy(x + 4, y + 2)
        self.set_font("DejaVu", "B", 10)
        self.set_text_color(120, 80, 0)
        self.multi_cell(182, 5, f"KEY INSIGHT: {text}")
        self.set_y(y + 16)


def build_pdf():
    pdf = ResearchPDF()
    pdf.alias_nb_pages()

    # ── TITLE PAGE ──
    pdf.title_page()

    # ── TABLE OF CONTENTS ──
    pdf.add_page()
    pdf.set_font("DejaVu", "B", 18)
    pdf.set_text_color(20, 60, 120)
    pdf.cell(0, 12, "Table of Contents", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    toc = [
        ("1", "The Problem: All Setups Are Mean-Reversion"),
        ("2", "Discord Intelligence: What Experts Say About Trends"),
        ("3", "Volland Framework: Trend Mechanics"),
        ("4", "Academic & Industry Research"),
        ("5", "Proposed Setup: Paradigm Trend Rider (PTR)"),
        ("6", "Proposed Setup: LIS Cascade"),
        ("7", "Comparison: PTR vs Current Setups"),
        ("8", "Expected Edge & Risk Analysis"),
        ("9", "Implementation Roadmap"),
        ("10", "Sources & References"),
    ]
    for num, title in toc:
        pdf.set_font("DejaVu", "", 11)
        pdf.set_text_color(30, 30, 30)
        pdf.cell(0, 7, f"  {num}.   {title}", new_x="LMARGIN", new_y="NEXT")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 1
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("1", "The Problem: All Setups Are Mean-Reversion")

    pdf.body(
        "The 0DTE Alpha system currently runs 7 active setup detectors. Every single one is "
        "designed to capture mean-reversion or reversal moves with fixed 8-10 point targets. "
        "While these setups are profitable, they systematically leave money on the table on "
        "trend days -- the exact days where the biggest profits are available."
    )

    # Table of current setups
    cols = ["Setup", "Mechanic", "Target", "Style"]
    widths = [38, 62, 25, 30]
    pdf.ln(2)
    pdf.table_header(cols, widths)
    rows = [
        ("GEX Long", "Buy at LIS support", "+10 pts", "Scalp"),
        ("AG Short", "Sell near LIS resistance", "+10 pts", "Scalp"),
        ("BofA Scalp", "LIS level bounce", "+10 pts", "Scalp"),
        ("ES Absorption", "CVD divergence reversal", "+10 pts", "Reversal"),
        ("DD Exhaustion", "Contrarian exhaustion", "+10 trail", "Contrarian"),
        ("Skew Charm", "Charm/skew inflection", "+10 pts", "Reversal"),
        ("Paradigm Rev.", "Paradigm shift", "+10 pts", "Reversal"),
    ]
    for i, row in enumerate(rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    pdf.key_insight_box(
        "AG Short and GEX Long fire on TREND paradigms (AG = bearish trend day, "
        "GEX = bullish trend day per Volland), but exit after just 10 points. "
        "The AG target may be 30-40 pts away. This is the core missed opportunity."
    )

    pdf.ln(4)
    pdf.body(
        "On March 26, 2026, SPX dropped 90 points in a single session. AG Short signaled correctly "
        "but exited after +10 pts. A trend-riding setup with a trailing stop could have captured "
        "40-50+ points from the same signal. One trend day like this per week would transform the "
        "system's risk-adjusted returns."
    )

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("2", "Discord Intelligence: What Experts Say About Trends")

    pdf.body(
        "Analysis of 35,713 messages from the Volland Discord 'Daytrading Central' channel "
        "(Nov 2025 - Feb 2026) reveals that the community overwhelmingly trades mean-reversion. "
        "However, several expert contributors describe specific trend-riding mechanics that are "
        "underutilized. Key findings follow."
    )

    pdf.subsection("2.1  The Break + Retest Pattern (Most Repeated Setup)")
    pdf.body("This is the single most frequently discussed entry pattern across all 13 expert contributors:")
    pdf.quote("Dark Matter",
        "Break past 6875 SPX, then break below it -- new 30 min candle goes and retests it "
        "and you enter on that retest and manage your risk accordingly."
    )
    pdf.quote("Dark Matter",
        "Taking the breakdown of 6950 SPX and then the retest. We broke below 6950, "
        "popped back to retest it, then had 2 red 30-min candles to test 6935."
    )
    pdf.quote("Toto",
        "Wait for 2-way trade, let a range form, let price breakout to the other side, "
        "then start looking for retraces."
    )
    pdf.body(
        "This is a classic trend-continuation entry: wait for breakout, wait for pullback "
        "to broken level, enter when pullback fails. The broken level becomes the stop. "
        "This is NOT a mean-reversion pattern -- it explicitly rides the trend."
    )

    pdf.subsection("2.2  Negative Gamma as Trend Accelerant")
    pdf.quote("Wizard of Ops",
        "It will propel the move in the short term... but that's a negative gamma zone too, "
        "so it can break out with the intent of needing to watch for a good time to sell the rip. "
        "That rip is going to be fierce though, so it would feel like trying to catch a rising knife."
    )
    pdf.quote("Jettins",
        "VX1 expansion counters the magnetic hold of the Gamma. In the absence of VX1 expansion, "
        "Gamma acts as a repelling orbiting force that can slingshot away when VIX is stable."
    )
    pdf.body(
        "Key mechanic: Positive gamma = mean reversion (dealers dampen moves). "
        "Negative gamma = momentum (dealers amplify moves). VIX expansion breaks gamma's "
        "hold, enabling trend moves that would normally be contained."
    )

    pdf.subsection("2.3  Post-2PM Dealer O'Clock = Trend Acceleration")
    pdf.quote("Dark Matter",
        "Post 2 PM is dealer o'clock where those charm moves can come in. Just note on high "
        "volatility days OR where we have an event catalyst, 0DTE data will give way to higher "
        "timeframes -- i.e. vanna on a weekly timeframe will play a bigger role than 0DTE charm."
    )
    pdf.quote("Wizard of Ops",
        "Towards the end of the day you see the delta decay hedging changing BEFORE "
        "the market does -- a good opportunity to throw on a cheap call or put."
    )
    pdf.body(
        "Dealers warehouse intraday risk until 2-3 PM ET. When they finally hedge, it "
        "ACCELERATES the existing trend. Morning signals + afternoon execution force = "
        "the trend-rider's optimal timing framework."
    )

    pdf.subsection("2.4  The 'Top Greek' Regime Concept")
    pdf.quote("Dark Matter",
        "I am starting to hone in on what the 'top' greek of the day is. For me vanna and "
        "vol -- that's what I have my eyes on right now until vanna has been met. "
        "Each greek can push a certain way depending on which one is top. Gamma will provide "
        "the structure but we broke it because vanna and vol led the way today."
    )
    pdf.body(
        "This is critical for trend detection: when vanna or vol is the dominant force, gamma "
        "structure breaks and trends emerge. When gamma is dominant, mean-reversion holds. "
        "Identifying which Greek is 'top' determines whether to trade mean-reversion or trend."
    )

    pdf.subsection("2.5  TheEdge's 15-Year Lesson (Long Bias Trend)")
    pdf.quote("TheEdge",
        "Looking back at my trading since inception (15 years now), my P&L would have been "
        "way less volatile and profitable if I simply longed, and de-risked at times, instead "
        "of trying to be a smart ass and make money from both sides."
    )
    pdf.quote("Phoenix",
        "Stocks rally only total of max 14 days per year. If you miss that, you miss lots of money."
    )
    pdf.body(
        "The asymmetry: rallies are concentrated in a few days. Missing them is devastating. "
        "A trend-following setup that captures these days is worth more than many scalps."
    )

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("3", "Volland Framework: Trend Mechanics")

    pdf.body(
        "The Volland White Paper and User Guide explicitly describe GEX and AG as trend "
        "paradigms. This section extracts the precise mechanics that enable trend-following."
    )

    pdf.subsection("3.1  GEX Paradigm = Bullish Trend Day")
    pdf.body(
        "From the White Paper: 'Negative net charm both above and below price is how this "
        "shows. This means that on the downside there is a line in the sand, but with strong "
        "net negative charm, option dealers must buy to hedge their position up to a charm "
        "balance point referred to as a target. Targets are out-of-the-money strikes that "
        "should be hit during the day. GEX paradigms manifest on bullish trend days.'"
    )
    pdf.body(
        "The trend mechanic: Charm is negative everywhere. As price rises, bars below become "
        "farther OTM and fade (Volland Principle 4). Bars above spot flip sign as they become "
        "ITM. Trend continues until aggregate charm reaches neutral = the target."
    )

    pdf.subsection("3.2  Anti-GEX Paradigm = Bearish Trend Day")
    pdf.body(
        "From the White Paper: 'Customers buy calls and sell puts, leaving the option dealers "
        "with a bearish risk reversal position... positive charm on both sides of spot, "
        "resulting in a bearish trend. Like the GEX paradigm, there is a line in the sand "
        "and a target; however, the line in the sand is above spot, while the target is below spot.'"
    )

    pdf.key_insight_box(
        "AG target hits are MORE reliable than GEX targets (White Paper). AG transitions "
        "to Sidial paradigm more frequently indicate target completion. AG needs late "
        "morning (vs GEX's first-hour acceleration) but delivers more consistent trends."
    )

    pdf.subsection("3.3  LIS Break = Gamma Cascade (10-15 pt Impulse)")
    pdf.body(
        "From the User Guide: 'When a line in the sand breaks, you will notice a 10-15 point "
        "move in a roughly 5 minute timeframe. It will create a new low/high that represents "
        "the new line in the sand where another round of gamma hedging would occur.'"
    )
    pdf.body(
        "From the User Guide: 'The gamma hedge tends to be done in triples, hedging triple "
        "the amount of gamma needed assuming a trend.'"
    )
    pdf.body(
        "This is the staircase breakout mechanic: LIS break triggers 3x gamma hedge, "
        "causing 10-15 pt impulse. New LIS forms. If that breaks, another cascade. "
        "Each step is a trend continuation signal."
    )

    pdf.subsection("3.4  Paradigm Timing Statistics (White Paper)")
    pdf.bullet("GEX paradigm sweet spot: 10:30 AM -- paradigm has stabilized, high target probability")
    pdf.bullet("AG paradigm needs late morning to maximize -- more time than GEX to play out")
    pdf.bullet("GEX/AG -> Sidial transition = target hit (trend complete)")
    pdf.bullet("GEX/AG -> BofA transition = trend dying, mean-reversion regime starting")
    pdf.bullet("Paradigms typically stabilize by 10:30 AM (far from guaranteed but statistically robust)")

    pdf.subsection("3.5  Dealer Hedging Timing")
    pdf.body(
        "From the White Paper: 'Dealers do not dynamically hedge because of increased costs. "
        "In fact, option dealers warehouse their risk until the end of the day, typically "
        "1-2.5 hours until expiration, independent of events that happen in the afternoon.'"
    )
    pdf.body(
        "From the User Guide: 'Dealers may hedge their exposure sooner if there is strong "
        "volatility. If the market goes far out of bounds (based on the premiums dealers have "
        "collected), dealers will hedge before dealer o'clock. Consider out of bounds greater "
        "than 1.5x the opening straddle price as a rule of thumb.'"
    )
    pdf.body(
        "Implication: The morning paradigm signal predicts the afternoon execution force. "
        "GEX/AG signal at 10:30 AM -> dealer hedging force at 2-3 PM AMPLIFIES the trend."
    )

    pdf.subsection("3.6  Vanna Extremes: The '---' / '+++' Signal")
    pdf.body(
        "From the User Guide: 'If you see a --- in the support extreme or a +++ in the "
        "resistance extreme, that means there is not enough option volume at any price to "
        "stop market momentum based on vanna at that tenor.'"
    )
    pdf.body(
        "When vanna extremes show no support or no resistance, there is literally no dealer "
        "force to contain the move. This is the strongest possible trend signal from the "
        "Volland framework -- unimpeded momentum with no structural ceiling/floor."
    )

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("4", "Academic & Industry Research")

    pdf.subsection("4.1  Baltussen et al. (2021) -- Intraday Momentum on Negative Gamma Days")
    pdf.body(
        "Published in the Journal of Financial Economics (the top-3 finance journal), this "
        "paper analyzes hedging demand and intraday momentum across 60+ futures markets "
        "from 1974-2020."
    )
    pdf.bold_body("Key findings:")
    pdf.bullet("The return in the last 30 minutes of trading is predicted by the rest-of-day return")
    pdf.bullet("This momentum effect is 'significantly more prominent on days with negative Net Gamma Exposure'")
    pdf.bullet("Simple momentum strategy produces annualized Sharpe ratios of 0.87 to 1.73")
    pdf.bullet("The effect REVERSES over the next few days (mean reversion kicks in)")
    pdf.body(
        "Direct implication: On AG paradigm days (negative gamma), if price has established "
        "a clear direction by 3:00 PM, there is peer-reviewed evidence for a momentum push "
        "into the close. This validates holding trend positions into the afternoon rather "
        "than taking quick scalps."
    )

    pdf.subsection("4.2  SpotGamma -- Volatility Trigger Framework")
    pdf.body(
        "SpotGamma distinguishes between Zero Gamma (where net dealer gamma crosses zero) "
        "and the Volatility Trigger (where negative feedback loops ignite). "
        "The regime framework:"
    )
    pdf.bullet("Above Volatility Trigger = positive gamma = mean-reversion regime", bold_prefix="Regime A:")
    pdf.bullet("Below Volatility Trigger = negative gamma = momentum/trend regime", bold_prefix="Regime B:")
    pdf.bullet("VT level itself = the stop-loss for trend trades (if reclaimed, thesis is broken)", bold_prefix="Risk:")
    pdf.body(
        "This maps directly to Volland paradigms: GEX/BofA with price near LIS = Regime A "
        "(your current setups). AG with price breaking through levels = Regime B (the proposed "
        "trend setups)."
    )

    pdf.subsection("4.3  SpotGamma -- Put Wall / Call Wall Break Mechanics")
    pdf.body(
        "The Put Wall (highest put gamma strike) acts as mechanical support because dealers "
        "buy the underlying as price approaches. The Call Wall acts as mechanical resistance. "
        "When these walls BREAK:"
    )
    pdf.bullet("Put Wall break: mechanical buying support vanishes, dealers must now SELL to re-hedge, price accelerates lower")
    pdf.bullet("Call Wall break: mechanical selling resistance vanishes, dealers must now BUY, price accelerates higher")
    pdf.bullet("The gap between Put Wall and Call Wall defines the 'expected range' -- breaks outside this range are trend signals")
    pdf.body(
        "This maps to your -GEX / +GEX levels. Currently, GEX Long uses -GEX as SUPPORT. "
        "A trend setup would fire when -GEX BREAKS -- the opposite logic."
    )

    pdf.subsection("4.4  OptionMetrics -- Gamma Type Matters")
    pdf.body(
        "OptionMetrics research adds a critical nuance: 'The short call regime posts the "
        "highest returns, along with the lowest volatility.' Simple 'negative GEX = volatile' "
        "is too crude -- the TYPE of short gamma matters:"
    )
    pdf.bullet("Short calls (customers sold calls) = different behavior from short puts (customers sold puts)")
    pdf.bullet("'Gamma exposure alone does not adequately explain realized volatility'")
    pdf.bullet("Negative gamma regimes persist for an average of 3-7 trading sessions")
    pdf.bullet("Intraday ranges expand 50-200% above average during negative gamma")

    pdf.subsection("4.5  Gamma Flip = Highest-Edge Transition")
    pdf.body(
        "Multiple sources (AI FlowTrader, SpotGamma) confirm that the TRANSITION from "
        "positive to negative gamma is the highest-edge entry point. Not just being in "
        "a negative gamma regime, but the moment of crossing."
    )
    pdf.body(
        "This maps to paradigm shifts (GEX->AG, BofA->AG). Your Paradigm Reversal setup "
        "already detects these transitions but treats them as 10-point scalps."
    )

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("5", "Proposed Setup: Paradigm Trend Rider (PTR)")

    pdf.body(
        "The primary trend-following setup. Uses 100% existing data infrastructure -- "
        "no new scrapers, API calls, or DB tables. Only new evaluation logic in "
        "setup_detector.py and wiring in main.py."
    )

    pdf.subsection("5.1  Core Thesis")
    pdf.body(
        "GEX and AG paradigms are TREND paradigms by definition. The paradigm target is "
        "where charm neutralizes -- often 25-40 pts from entry. Current setups take 10 pts "
        "and exit. PTR rides the paradigm to its natural target using a trailing stop."
    )

    pdf.subsection("5.2  Entry Conditions")
    pdf.bold_body("All 6 conditions must be met:")
    pdf.bullet("Paradigm = AG-PURE or AG-LIS (short) or GEX-PURE (long), confirmed at or after 10:30 AM ET")
    pdf.bullet("Room to target >= 20 pts (distance from spot to paradigm target)")
    pdf.bullet("Charm alignment: charm direction matches trade direction (negative charm for shorts, positive for longs)")
    pdf.bullet("DD alignment: DD hedging direction matches trade direction")
    pdf.bullet("Entry timing: 10:00 - 13:00 ET (paradigm confirmed, max edge per White Paper)")
    pdf.bullet("VIX: >= 20 for shorts (your data: VIX<20 = 13% WR for AG). <= 22 for longs")

    pdf.subsection("5.3  Scoring System (5 Components, Max 100)")
    cols = ["Component", "Weight", "Logic"]
    widths = [40, 20, 100]
    pdf.ln(2)
    pdf.table_header(cols, widths)
    score_rows = [
        ("Paradigm Quality", "0-25", "PURE/LIS=25, MESSY=12, TARGET=0"),
        ("Room to Target", "0-25", ">=30pts=25, >=25=20, >=20=10"),
        ("Charm Alignment", "0-20", "Aligned=20, Neutral=10, Opposing=0"),
        ("DD Alignment", "0-15", "Aligned=15, Neutral=8, Opposing=0"),
        ("VIX Regime", "0-15", "Sweet spot=15, Acceptable=8, Poor=0"),
    ]
    for i, row in enumerate(score_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    pdf.ln(2)
    pdf.bold_body("Grading: A+ >= 75  |  A >= 55  |  B >= 40  |  Below B = No Signal")

    pdf.subsection("5.4  Risk Management (The Core Innovation)")
    pdf.body("This is where PTR fundamentally differs from all current setups:")
    pdf.ln(2)
    cols = ["Parameter", "Current AG Short", "PTR"]
    widths = [45, 50, 50]
    pdf.table_header(cols, widths)
    rm_rows = [
        ("Stop Loss", "8 pts", "15 pts (trends need room)"),
        ("T1 (partial)", "10 pts (all out)", "10 pts (50% position)"),
        ("T2 (runner)", "None", "Paradigm target (trail)"),
        ("Trail", "BE at +10", "10pt gap after T1"),
        ("Hold time", "~30 min", "Hours (until target)"),
        ("Time stop", "None", "15:30 ET -> trail to EOD"),
        ("Kill condition", "None", "Paradigm shift -> 5pt trail"),
    ]
    for i, row in enumerate(rm_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    pdf.ln(3)
    pdf.bold_body("Key RM Design Rationale:")
    pdf.bullet("15pt stop: Your data shows AG Short shakeouts at 8-12 pts that reverse and hit target. Wider stop survives the shakeouts that kill scalps.")
    pdf.bullet("T1 at 10pts: Locks in base profit and provides psychological anchor. Same proven scalp target.")
    pdf.bullet("T2 trailing: After T1, the runner costs nothing (stop at BE). Let dealer o'clock (2-3 PM) push it to target.")
    pdf.bullet("Kill condition: Paradigm shift to BofA/Sidial = trend dying. Tighten trail instead of hard exit.")

    pdf.subsection("5.5  How PTR Differs from AG Short")
    pdf.body(
        "PTR does NOT replace AG Short. They fire on different conditions:"
    )
    pdf.bullet("AG Short fires when spot is near LIS in any AG subtype -- quick scalp for 10 pts")
    pdf.bullet("PTR fires only when room to target >= 20 pts and paradigm is high quality -- holds for full move")
    pdf.bullet("Both can fire on the same day: AG Short takes the first 10 pts, PTR separately rides the continuation")
    pdf.bullet("PTR's wider stop prevents overlap: if AG Short stops out at -8, PTR may still be alive at -8 (within its 15pt stop)")

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 6
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("6", "Proposed Setup: LIS Cascade")

    pdf.body(
        "The breakout continuation setup. Captures the trend AFTER key levels break, "
        "riding dealer gamma-hedging cascades. More novel than PTR and complementary to it."
    )

    pdf.subsection("6.1  Core Thesis")
    pdf.body(
        "When price breaks through LIS, dealers gamma-hedge 3x (Volland User Guide), "
        "causing a 10-15 pt impulse in ~5 minutes. After the impulse, price typically "
        "retests the broken LIS. If the retest fails (broken LIS becomes resistance/support), "
        "the trend continues toward the next structural level."
    )
    pdf.body(
        "This is the #1 most repeated pattern in the Volland Discord (Dark Matter, Toto, "
        "and others describe it repeatedly). It is fundamentally different from all current "
        "setups because it enters AFTER levels break, not AT levels."
    )

    pdf.subsection("6.2  Entry Conditions")
    pdf.bold_body("All 5 conditions must be met:")
    pdf.bullet("LIS Break: Price moves >= 8 pts past LIS (confirmed break, not noise)")
    pdf.bullet("Retest: Price retraces back toward broken LIS (within 3 pts of the level)")
    pdf.bullet("Rejection: ES range bar closes in the break direction (retest fails)")
    pdf.bullet("Volume: Break bar volume >= 1.5x 20-bar average (real conviction, same gate as ES Absorption)")
    pdf.bullet("Time: 10:00 - 14:30 ET (need time for continuation, avoid open + last 90 min)")

    pdf.subsection("6.3  Detection Using Existing Data")
    pdf.body("All required data already exists in the system:")
    pdf.bullet("LIS level: from Volland (volland_snapshots table)", bold_prefix="LIS:")
    pdf.bullet("5-pt range bars with volume: from ES quote stream (es_range_bars table)", bold_prefix="Range bars:")
    pdf.bullet("from Volland (used for scoring, not gating)", bold_prefix="Paradigm:")
    pdf.bullet("from Volland (used for alignment scoring)", bold_prefix="Charm/DD:")
    pdf.body(
        "The new component is the retest detection logic: tracking whether price has "
        "broken LIS, pulled back, and been rejected. This is a state machine on the "
        "range bar stream (break detected -> pullback phase -> rejection confirmed)."
    )

    pdf.subsection("6.4  Scoring (4 Components, Max 100)")
    cols = ["Component", "Weight", "Logic"]
    widths = [45, 20, 95]
    pdf.ln(2)
    pdf.table_header(cols, widths)
    cascade_rows = [
        ("Break Magnitude", "0-30", ">=12pts=30, >=10=20, >=8=10"),
        ("Volume Quality", "0-25", ">=2x avg=25, >=1.5x=15"),
        ("Paradigm Support", "0-25", "AG for shorts / GEX for longs=25, BofA=15, Sidial=0"),
        ("Charm Alignment", "0-20", "Aligned with break=20, Neutral=10, Opposing=0"),
    ]
    for i, row in enumerate(cascade_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    pdf.subsection("6.5  Risk Management")
    pdf.bullet("Stop Loss: 10 pts past the broken LIS (if LIS reclaimed, thesis is dead)")
    pdf.bullet("T1: 15 pts (larger than scalps -- riding impulse + continuation)")
    pdf.bullet("Trail: After T1, 8pt gap from high/low water mark")
    pdf.bullet("Cascade bonus: If price breaks through a SECOND level (new -GEX or +GEX), reset trail activation")

    pdf.subsection("6.6  The Cascade Mechanic")
    pdf.body(
        "The User Guide describes a staircase pattern: LIS break -> 10-15 pt impulse -> "
        "new LIS forms -> if that breaks, another impulse. Each cascade step is a "
        "continuation signal. With trailing stop, PTR captures multiple steps automatically. "
        "LIS Cascade is designed to ENTER on the first step and trail through subsequent steps."
    )

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 7
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("7", "Comparison: PTR vs Current Setups")

    pdf.subsection("7.1  When Each Setup Type Is Optimal")

    cols = ["Market Condition", "PTR", "LIS Cascade", "Current Setups"]
    widths = [45, 35, 35, 40]
    pdf.ln(2)
    pdf.table_header(cols, widths)
    cmp_rows = [
        ("AG paradigm, at LIS", "FIRES", "Not yet", "AG Short (scalp)"),
        ("LIS breaks", "Holds (trail)", "FIRES (new)", "Stopped/exited"),
        ("Price cascades down", "T2 trail captures", "Re-entries ok", "Missed entirely"),
        ("2-3PM charm accel.", "Still holding", "Trail captures", "Time-filtered out"),
        ("Paradigm -> BofA", "Kill (tighten)", "No new signal", "BofA Scalp fires"),
        ("BofA range-bound", "No signal", "No signal", "BofA Scalp (ideal)"),
        ("Sidial whipsaw", "No signal", "No signal", "No signal (correct)"),
    ]
    for i, row in enumerate(cmp_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    pdf.key_insight_box(
        "PTR and LIS Cascade fill the gap between 'correctly identified trend day' "
        "and 'actually capturing the trend.' Current setups correctly detect AND scalp "
        "these days. The new setups hold through the full move."
    )

    pdf.subsection("7.2  Head-to-Head: PTR Short vs AG Short")
    cols = ["Metric", "AG Short", "PTR Short"]
    widths = [50, 45, 50]
    pdf.ln(2)
    pdf.table_header(cols, widths)
    h2h_rows = [
        ("Entry location", "Near LIS (resistance)", "Near LIS at 10:30+"),
        ("Stop loss", "8 pts", "15 pts"),
        ("Primary target", "10 pts (100% exit)", "10 pts (50% exit)"),
        ("Runner target", "None", "Paradigm target"),
        ("Expected best case", "+10 pts", "+10 (T1) + 20-30 (T2)"),
        ("Expected worst case", "-8 pts", "-15 pts"),
        ("Avg trades/week", "5-8", "2-3"),
        ("Quality filter", "Medium", "High (paradigm + room)"),
    ]
    for i, row in enumerate(h2h_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 8
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("8", "Expected Edge & Risk Analysis")

    pdf.subsection("8.1  PTR Expected Value Calculation")
    pdf.body("Conservative estimates based on your existing AG Short data:")
    pdf.ln(2)

    pdf.bold_body("Assumptions:")
    pdf.bullet("T1 hit rate: 65% (same as AG Short at A+ grade)")
    pdf.bullet("T2 average capture (when T1 hit): +15 pts beyond T1")
    pdf.bullet("T2 hit rate (conditional on T1): 50%")
    pdf.bullet("Loss rate: 35%, average loss: -15 pts")
    pdf.bullet("Frequency: ~2-3 trades/week")

    pdf.ln(2)
    pdf.bold_body("Expected value per trade:")
    pdf.body(
        "Win scenario T1 only (32.5%): +10 pts\n"
        "Win scenario T1+T2 (32.5%): +10 + 15 = +25 pts\n"
        "Loss scenario (35%): -15 pts\n\n"
        "E[PnL] = (0.325 x 10) + (0.325 x 25) - (0.35 x 15) = +6.1 pts/trade\n\n"
        "vs AG Short E[PnL] = (0.62 x 10) - (0.38 x 8) = +3.2 pts/trade"
    )

    pdf.key_insight_box(
        "PTR's expected value is ~1.9x AG Short per trade, but the real edge is "
        "tail capture. On Mar 26 (-90pt day), PTR could yield +40-50 pts vs AG Short's +10."
    )

    pdf.subsection("8.2  Risk Analysis")
    pdf.bold_body("Risks and Mitigations:")
    pdf.bullet("Wider stop (-15 vs -8): Mitigated by higher selectivity (only A/A+ quality, >=20pt room). Fewer but better trades.", bold_prefix="Risk 1:")
    pdf.bullet("Paradigm shifts mid-trade: Mitigated by kill condition -- tighten trail to 5pt gap on paradigm shift. T1 is already locked.", bold_prefix="Risk 2:")
    pdf.bullet("Afternoon reversal after morning trend: Academic evidence (Baltussen) shows last-30-min momentum on negative gamma days, not reversal.", bold_prefix="Risk 3:")
    pdf.bullet("Overlap with AG Short: By design -- AG Short takes quick profit, PTR separately holds for more. Different position.", bold_prefix="Risk 4:")
    pdf.bullet("Backtest overfitting: Start in LOG-ONLY mode (like DD Exhaustion). Collect 30+ signals before enabling live.", bold_prefix="Risk 5:")

    pdf.subsection("8.3  Weekly Scenario Analysis")
    cols = ["Scenario", "PTR Trades", "PTR P&L", "AG-Only P&L"]
    widths = [55, 25, 35, 35]
    pdf.ln(2)
    pdf.table_header(cols, widths)
    scenario_rows = [
        ("2 trend days + 3 chop", "2", "+50 to +70", "+20 (2x10)"),
        ("1 big trend + 4 chop", "1", "+30 to +50", "+10"),
        ("0 trend days (all chop)", "0", "$0 (no signals)", "$0"),
        ("1 trend + 1 fake (loss)", "2", "+25 to -5", "+10 to -8"),
    ]
    for i, row in enumerate(scenario_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))
    pdf.ln(2)
    pdf.body(
        "PTR only fires on high-conviction setups (>=20pt room, quality paradigm). "
        "On weeks with no trend days, it simply produces zero signals -- no harm. "
        "It cannot make chop weeks worse because it doesn't trade them."
    )

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 9
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("9", "Implementation Roadmap")

    pdf.subsection("Phase 1: Backtest (1-2 days)")
    pdf.bullet("Query setup_log for historical AG Short and GEX Long signals where room to target >= 20 pts")
    pdf.bullet("Simulate PTR risk management (15pt stop, 10pt T1, trail T2) on those entries")
    pdf.bullet("Compare: PTR P&L vs actual AG Short/GEX Long P&L on same signals")
    pdf.bullet("Validate: Does the trailing T2 capture significant additional profit?")

    pdf.subsection("Phase 2: LOG-ONLY Mode (2-4 weeks)")
    pdf.bullet("Add evaluate_paradigm_trend_rider() to setup_detector.py")
    pdf.bullet("Wire into main.py alongside existing setups")
    pdf.bullet("Grade always 'LOG', Telegram tagged [LOG-ONLY]")
    pdf.bullet("Track: entry price, max favorable excursion, actual outcome at target")
    pdf.bullet("Target: 30+ logged signals before enabling")

    pdf.subsection("Phase 3: Live Enable (after 30+ signals)")
    pdf.bullet("Review log-only results: actual win rates, T2 capture, drawdown")
    pdf.bullet("Calibrate stop/trail parameters based on real data")
    pdf.bullet("Enable on eval_trader first (SIM), then real_trader")
    pdf.bullet("Uses existing Flow B (split-target) in auto_trader.py -- T1 partial + T2 trail")

    pdf.subsection("Phase 4: LIS Cascade (after PTR validated)")
    pdf.bullet("Implement retest detection state machine on range bar stream")
    pdf.bullet("More complex than PTR -- defer until PTR is proven")
    pdf.bullet("Can be added as a separate setup or as a re-entry trigger within PTR")

    pdf.ln(6)
    pdf.bold_body("Estimated Implementation Effort:")
    cols = ["Component", "Complexity", "Lines of Code"]
    widths = [70, 40, 40]
    pdf.table_header(cols, widths)
    effort_rows = [
        ("evaluate_paradigm_trend_rider()", "Medium", "~80-100"),
        ("Wiring in main.py", "Low", "~30-40"),
        ("Backtest query script", "Low", "~50-60"),
        ("Auto-trader integration", "Low", "Reuse Flow B"),
        ("LIS Cascade (Phase 4)", "High", "~150-200"),
    ]
    for i, row in enumerate(effort_rows):
        pdf.table_row(row, widths, fill=(i % 2 == 0))

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 10
    # ══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("10", "Sources & References")

    pdf.subsection("Primary Sources (Direct)")
    pdf.bullet("Volland Discord 'Daytrading Central' -- 35,713 messages, Nov 2025 - Feb 2026. 13 expert contributors including Apollo, Dark Matter, Wizard of Ops, Simple Jack, Phoenix, Jettins, TheEdge, Toto.")
    pdf.bullet("Volland White Paper -- 'Impact of Option Dealer Flows on Equity Returns' by Jason DeLorenzo (Dec 2023). GEX/AG paradigm statistics, charm mechanics, vanna stochastic.")
    pdf.bullet("Volland User Guide (Dec 2025, 37 pages) -- Paradigm trading rules, LIS break mechanics, 3x gamma hedging, dealer o'clock, 1.5x straddle rule.")

    pdf.subsection("Academic Research")
    pdf.bullet("Baltussen, Bekkerman, Grient & Zhu (2021). 'Hedging Demand and Market Intraday Momentum.' Journal of Financial Economics. Negative gamma days show strongest last-30-min momentum. Sharpe 0.87-1.73.")
    pdf.bullet("OptionMetrics (2022). 'Gamma Gravity: Negative Gamma is Not a Volatility Black Hole.' Short-call vs short-put gamma distinction. Gamma exposure alone doesn't explain realized volatility.")

    pdf.subsection("Industry Sources")
    pdf.bullet("SpotGamma -- Volatility Trigger framework, Zero Gamma level, Put Wall / Call Wall break mechanics. support.spotgamma.com")
    pdf.bullet("AI FlowTrader -- Gamma flip as highest-edge transition event. 'Trading Gamma Flips: A Professional Options Strategy.' aiflowtrader.com")
    pdf.bullet("MenthorQ -- Charm acceleration into close framework. 'Charm, Decay, and Flow.' menthorq.com")
    pdf.bullet("GammaEdge -- Market structure analysis, True Delta Zero / True Gamma Zero. gammaedge.us")
    pdf.bullet("SqueezeMetrics -- GEX White Paper. squeezemetrics.com")
    pdf.bullet("StrikeWatch -- Zero Gamma Level analysis, negative gamma regime persistence data. strike-watch.com")

    pdf.subsection("Internal Data (0DTE Alpha)")
    pdf.bullet("setup_log table -- Historical AG Short, GEX Long, Skew Charm signals with outcomes")
    pdf.bullet("trade-analyses.md -- 9 trade analyses, DD observations, parameter tuning decisions")
    pdf.bullet("V11 win rates by hour -- 73% WR 9:30-11AM, afternoon decay patterns")
    pdf.bullet("AG Short v2 scoring -- 52-trade analysis showing r=+0.405 predictive correlation")

    # ── Final page ──
    pdf.ln(10)
    pdf.set_draw_color(20, 60, 120)
    pdf.set_line_width(0.8)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(8)
    pdf.set_font("DejaVu", "I", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, "End of Research Document", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "Compiled March 27, 2026 -- 0DTE Alpha Project", align="C", new_x="LMARGIN", new_y="NEXT")

    # ── Save ──
    output_path = "Trend_Following_Research_0DTE_Alpha.pdf"
    pdf.output(output_path)
    print(f"PDF generated: {output_path}")
    return output_path


if __name__ == "__main__":
    build_pdf()
