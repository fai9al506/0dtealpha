"""
Generate a comprehensive PDF guide explaining all 0DTE Alpha trading setups.
Run: python 0DTE_Alpha_Trading_Setups_Guide.py
Output: 0DTE_Alpha_Trading_Setups_Guide.pdf
"""
from fpdf import FPDF
import os

class GuidePDF(FPDF):
    """Dark-themed PDF guide."""

    BG = (18, 20, 26)
    FG = (230, 230, 230)
    ACCENT = (99, 102, 241)   # indigo
    GREEN = (34, 197, 94)
    RED = (239, 68, 68)
    MUTED = (156, 163, 175)
    SURFACE = (30, 33, 41)
    YELLOW = (250, 204, 21)

    def header(self):
        self.set_fill_color(*self.BG)
        self.rect(0, 0, 210, 297, "F")

    def footer(self):
        self.set_y(-12)
        self.set_font("Helvetica", "", 7)
        self.set_text_color(*self.MUTED)
        self.cell(0, 10, f"0DTE Alpha Trading Setups Guide  |  Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title, y_pad=6):
        self.ln(y_pad)
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*self.ACCENT)
        self.cell(0, 9, title, new_x="LMARGIN", new_y="NEXT")
        # underline
        self.set_draw_color(*self.ACCENT)
        self.set_line_width(0.5)
        x = self.l_margin
        self.line(x, self.get_y(), x + 180, self.get_y())
        self.ln(4)

    def sub_title(self, title, y_pad=4):
        self.ln(y_pad)
        self.set_font("Helvetica", "B", 12)
        self.set_text_color(*self.YELLOW)
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text, bold=False):
        style = "B" if bold else ""
        self.set_font("Helvetica", style, 9.5)
        self.set_text_color(*self.FG)
        self.multi_cell(0, 5, text)
        self.ln(1)

    def muted_text(self, text):
        self.set_font("Helvetica", "I", 8.5)
        self.set_text_color(*self.MUTED)
        self.multi_cell(0, 4.5, text)
        self.ln(1)

    def bullet(self, text, indent=8):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*self.FG)
        self.set_x(self.l_margin + indent)
        w = self.w - self.l_margin - self.r_margin - indent
        self.multi_cell(w, 5, f"-  {text}")
        self.ln(0.5)

    def key_value(self, key, value, indent=8):
        self.set_x(self.l_margin + indent)
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*self.ACCENT)
        kw = self.get_string_width(key + ": ") + 2
        self.cell(kw, 5, key + ": ")
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*self.FG)
        w = self.w - self.l_margin - self.r_margin - indent - kw
        self.multi_cell(w, 5, value)
        self.ln(0.5)

    def info_box(self, text):
        self.ln(2)
        self.set_fill_color(*self.SURFACE)
        x = self.get_x()
        self.set_x(x + 5)
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*self.FG)
        self.multi_cell(170, 5.5, text, fill=True)
        self.ln(2)

    def stat_line(self, label, value, color=None):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*self.MUTED)
        lw = self.get_string_width(label + "  ") + 2
        self.cell(lw + 8, 5, "    " + label + "  ")
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*(color or self.GREEN))
        self.cell(0, 5, value, new_x="LMARGIN", new_y="NEXT")

    def check_page_space(self, needed=60):
        if self.get_y() > 297 - needed:
            self.add_page()


def build_pdf():
    pdf = GuidePDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    # ═══════════════════════════════════════════════════════════════════════
    # COVER PAGE
    # ═══════════════════════════════════════════════════════════════════════
    pdf.ln(50)
    pdf.set_font("Helvetica", "B", 32)
    pdf.set_text_color(*pdf.ACCENT)
    pdf.cell(0, 15, "0DTE Alpha", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 18)
    pdf.set_text_color(*pdf.FG)
    pdf.cell(0, 12, "Trading Setups Guide", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(*pdf.MUTED)
    pdf.cell(0, 7, "A Complete Beginner's Guide to Every Setup", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 7, "From Zero to Understanding Dealer-Driven 0DTE Strategies", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(30)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "March 2026", align="C", new_x="LMARGIN", new_y="NEXT")

    # ═══════════════════════════════════════════════════════════════════════
    # TABLE OF CONTENTS
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("Table of Contents")
    toc = [
        "1. What is 0DTE Trading?",
        "2. Core Concepts You Need to Know",
        "   2.1  Options Greeks (Delta, Gamma, Charm, Vanna)",
        "   2.2  GEX - Gamma Exposure",
        "   2.3  LIS - Largest In Strike",
        "   2.4  Paradigm - Market Regime",
        "   2.5  Volland - The Data Source",
        "   2.6  CVD - Cumulative Volume Delta",
        "   2.7  Greek Alignment - The Macro Filter",
        "3. The Trading Setups",
        "   3.1  Skew Charm (MVP - 96% Win Rate)",
        "   3.2  Paradigm Reversal (89% Win Rate)",
        "   3.3  DD Exhaustion (Workhorse)",
        "   3.4  AG Short (Bearish Dealer Fade)",
        "   3.5  GEX Long (Bullish Force Alignment)",
        "   3.6  BofA Scalp (Range Bounce)",
        "   3.7  CVD Divergence (Order Flow Contrarian)",
        "4. The Greek Alignment Filter",
        "5. Risk Management",
        "6. Performance Summary",
    ]
    for item in toc:
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(*pdf.FG)
        pdf.cell(0, 6, item, new_x="LMARGIN", new_y="NEXT")

    # ═══════════════════════════════════════════════════════════════════════
    # CHAPTER 1: What is 0DTE Trading?
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("1. What is 0DTE Trading?")

    pdf.body_text(
        "0DTE stands for \"Zero Days to Expiration.\" These are options contracts on the S&P 500 index (SPX) "
        "that expire on the same day they are traded. Every trading day, new SPX options are born in the morning "
        "and die by 4:00 PM Eastern Time."
    )
    pdf.body_text(
        "Why does this matter? Because 0DTE options create massive, predictable forces in the market. "
        "Market makers (dealers) who sell these options must constantly hedge their positions by buying or selling "
        "the underlying index. This hedging activity moves the market in predictable patterns."
    )
    pdf.body_text(
        "0DTE Alpha is a system that detects these dealer-driven patterns in real time and generates trading signals. "
        "Instead of guessing where the market will go, we read what dealers are FORCED to do based on their options exposure. "
        "Think of it as reading the footprints of the biggest players in the room."
    )
    pdf.info_box(
        "Key insight: We don't predict the market. We detect conditions where dealer hedging creates "
        "predictable price movements, then trade in that direction."
    )

    # ═══════════════════════════════════════════════════════════════════════
    # CHAPTER 2: Core Concepts
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("2. Core Concepts You Need to Know")

    pdf.body_text(
        "Before diving into the setups, you need to understand the building blocks. "
        "Don't worry - we'll explain each one simply."
    )

    # 2.1 Options Greeks
    pdf.sub_title("2.1  Options Greeks")
    pdf.body_text(
        "Options Greeks are measurements of how an option's price changes in response to different market conditions. "
        "Think of them as sensors that tell us what dealers are experiencing."
    )
    pdf.bullet("Delta: How much the option price moves when the stock moves $1. A delta of 0.50 means the option gains $0.50 for every $1 move in the stock.")
    pdf.bullet("Gamma: How fast delta changes. High gamma means dealers need to hedge MORE aggressively as price moves. This is the key to 0DTE - gamma is highest on expiration day.")
    pdf.bullet("Charm: How delta changes as TIME passes (also called delta decay). This is THE most important Greek for 0DTE. As the day progresses, charm forces dealers to adjust hedges, creating directional pressure.")
    pdf.bullet("Vanna: How delta changes when VOLATILITY changes. When fear rises/falls, vanna forces dealers to buy or sell, amplifying moves.")

    pdf.info_box(
        "Why Charm matters most for 0DTE: Options lose value as time passes (time decay). But the rate of this decay "
        "changes throughout the day. Charm measures this acceleration. When charm is positive, dealers are forced to BUY "
        "as time passes (bullish pressure). When negative, they must SELL (bearish pressure). This creates a \"gravitational pull\" "
        "that dominates 0DTE price action, especially after 2:00 PM (\"dealer o'clock\")."
    )

    # 2.2 GEX
    pdf.check_page_space(80)
    pdf.sub_title("2.2  GEX - Gamma Exposure")
    pdf.body_text(
        "GEX (Gamma Exposure) measures the total gamma that market makers hold across all strikes. "
        "It tells us WHERE dealers are most exposed and HOW they'll react to price changes."
    )
    pdf.bullet("+GEX (Positive Gamma): Dealers are \"long gamma\" - they buy dips and sell rallies. This creates a STABILIZING effect. Price tends to bounce between +GEX levels like a pinball.")
    pdf.bullet("-GEX (Negative Gamma): Dealers are \"short gamma\" - they sell into dips and buy into rallies. This AMPLIFIES moves. Price accelerates when it hits -GEX zones.")

    pdf.body_text(
        "On a chart, +GEX levels act as magnets (price gravitates toward them) and -GEX levels act as accelerators "
        "(price speeds through them). Knowing where these levels are gives you a roadmap of likely price behavior."
    )

    # 2.3 LIS
    pdf.check_page_space(70)
    pdf.sub_title("2.3  LIS - Largest In Strike")
    pdf.body_text(
        "LIS is the strike price with the most options activity. Think of it as the market's \"center of gravity.\" "
        "Dealers have their biggest positions at this level, so they hedge most aggressively around it."
    )
    pdf.bullet("When price is NEAR LIS: Dealers are actively defending it, creating strong support/resistance.")
    pdf.bullet("When price is FAR from LIS: Dealer influence weakens and price moves more freely.")
    pdf.bullet("LIS can act as SUPPORT (below price, preventing drops) or as a MAGNET (above price, pulling price up).")

    # 2.4 Paradigm
    pdf.check_page_space(80)
    pdf.sub_title("2.4  Paradigm - Market Regime")
    pdf.body_text(
        "Paradigm is the current market regime - it tells you what TYPE of dealer positioning dominates. "
        "Think of it as the \"weather\" for the trading day. Each paradigm creates different price behavior."
    )
    pdf.bullet("GEX Paradigm (Bullish): Dealers hold positive gamma. Market is supported, dips get bought. LIS acts as strong support. Price tends to grind higher.")
    pdf.bullet("AG Paradigm (Bearish): \"Anti-GEX\" - dealers hold negative gamma. Market is vulnerable, rallies get sold. LIS acts as resistance. Price tends to fade lower.")
    pdf.bullet("BofA Paradigm (Range-Bound): Balanced positioning. Market consolidates between upper and lower LIS levels. Price bounces within a defined range. Named after Bank of America's options flow patterns.")
    pdf.bullet("Subtypes: GEX-LIS, GEX-PURE, GEX-TARGET, GEX-MESSY, AG-LIS, BOFA-PURE, etc. These refine the regime with additional context about dealer positioning quality.")

    # 2.5 Volland
    pdf.check_page_space(60)
    pdf.sub_title("2.5  Volland - The Data Source")
    pdf.body_text(
        "Volland (vol.land) is a specialized analytics platform that processes real-time options flow data "
        "and outputs the Greeks, paradigm, LIS, GEX levels, and other metrics our system uses. "
        "It scrapes options chain data, calculates dealer exposure, and identifies the current market regime."
    )
    pdf.body_text(
        "Our system captures Volland's data every 2 minutes via an automated scraper (Playwright browser automation). "
        "This data feeds directly into the setup detector that generates our trading signals."
    )

    # 2.6 CVD
    pdf.check_page_space(70)
    pdf.sub_title("2.6  CVD - Cumulative Volume Delta")
    pdf.body_text(
        "CVD tracks the running total of buying vs selling pressure in ES futures (S&P 500 futures). "
        "Every trade is classified as a BUY (buyer was the aggressor) or SELL (seller was the aggressor)."
    )
    pdf.bullet("Rising CVD: More aggressive buying than selling. Bulls are in control.")
    pdf.bullet("Falling CVD: More aggressive selling. Bears are in control.")
    pdf.bullet("CVD Divergence: When price makes a new extreme (higher high or lower low) but CVD moves the opposite direction, it signals that the move is running out of steam. This is a powerful reversal indicator.")

    # 2.7 Greek Alignment
    pdf.check_page_space(70)
    pdf.sub_title("2.7  Greek Alignment - The Macro Filter")
    pdf.body_text(
        "Greek Alignment is our master filter. It measures how many Greeks agree on the trade direction, "
        "scored from -3 (all bearish) to +3 (all bullish). It combines three components:"
    )
    pdf.bullet("Charm direction: Is time decay forcing dealers to buy (+1) or sell (-1)?")
    pdf.bullet("Vanna direction: Is volatility positioning bullish (+1) or bearish (-1)?")
    pdf.bullet("Spot-Vol-Beta (SVB): Is the market \"overvixing\" (fear elevated = bullish contrarian, +1) or \"undervixing\" (complacent = bearish contrarian, -1)?")

    pdf.info_box(
        "The +3 alignment rule: We only trade when ALL three Greeks agree (alignment = +3 for longs, -3 for shorts). "
        "This single filter improved our win rate from 53% to 61%, increased P&L by 61%, "
        "and cut maximum drawdown by 59%. It's the most important rule in the system."
    )

    # ═══════════════════════════════════════════════════════════════════════
    # CHAPTER 3: The Trading Setups
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("3. The Trading Setups")
    pdf.body_text(
        "Now let's dive into each setup. They are ordered by performance (best first). "
        "Each setup detects a different type of dealer-driven pattern."
    )

    # ── 3.1 SKEW CHARM ──────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.1  Skew Charm  (MVP Setup)")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "Skew Charm detects moments when the options market's fear gauge (skew) shifts sharply "
        "while dealer hedging pressure (charm) confirms the same direction. It's like seeing the crowd "
        "suddenly stop panicking while the house is already betting on a recovery."
    )

    pdf.sub_title("How Skew Works")
    pdf.body_text(
        "Skew measures the price difference between put options (downside protection) and call options (upside bets). "
        "High skew = the market is scared (puts are expensive). Low skew = the market is calm."
    )
    pdf.bullet("When skew DROPS sharply (3%+ over 20 snapshots): Fear is evaporating. If charm is positive (bullish dealer pressure), this confirms a BUY signal - the market is about to rally.")
    pdf.bullet("When skew RISES sharply: Fear is building. If charm is negative (bearish dealer pressure), this confirms a SELL signal - the market is about to drop.")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("Skew change >= 3% (either direction)")
    pdf.bullet("Charm aligns with direction (positive for long, negative for short)")
    pdf.bullet("Time window: 9:45 AM - 3:45 PM ET")
    pdf.bullet("Scoring: skew magnitude (30pts), charm strength (25pts), time (15pts), paradigm (15pts), skew level (15pts)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At current SPX price")
    pdf.key_value("Initial Stop", "20 points (generous - high conviction trades need room)")
    pdf.key_value("Trail", "Breakeven at +10pts profit, then continuous trail (activation=10, gap=8)")
    pdf.key_value("Target", "No fixed target - let trail capture the full move")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "Skew compression + charm alignment is a double confirmation: the crowd is shifting sentiment (skew) "
        "AND dealers are structurally positioned for the same move (charm). Unlike most setups that fight "
        "the crowd, Skew Charm rides WITH the momentum shift. The 20-point stop gives room for intraday noise - "
        "high-conviction signals rarely breach this level."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "96% (24 trades)")
    pdf.stat_line("Total P&L", "+156.9 points")
    pdf.stat_line("Avg per trade", "+6.5 pts")
    pdf.stat_line("Profit Factor", "7.15")
    pdf.muted_text("MVP setup. Highest win rate and profit factor of all setups.")

    # ── 3.2 PARADIGM REVERSAL ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.2  Paradigm Reversal")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "Paradigm Reversal catches the exact moment when the market's regime changes. "
        "Imagine a traffic light switching from green to red - everyone is still moving, "
        "but the signal just changed. This setup trades the first moments of the new regime "
        "before the rest of the market adjusts."
    )

    pdf.sub_title("How It Works")
    pdf.body_text(
        "The system tracks the Volland paradigm label every 2 minutes. When it detects a flip "
        "(e.g., GEX -> AG, meaning bullish support just collapsed into bearish pressure), "
        "it fires within 3 minutes of the change."
    )
    pdf.bullet("GEX/BofA -> AG paradigm = SHORT (bullish support just died, bearish regime starting)")
    pdf.bullet("AG -> GEX/BofA paradigm = LONG (bearish pressure just ended, bullish regime starting)")
    pdf.bullet("Price must be within 5 points of LIS (entry near the critical dealer level)")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("Paradigm just flipped (within last 180 seconds)")
    pdf.bullet("Price near LIS (within 5 points)")
    pdf.bullet("Time window: 10:00 AM - 3:45 PM ET")
    pdf.bullet("Scoring: LIS proximity (25pts), ES volume spike (25pts), charm alignment (20pts), DD hedging (15pts), time (15pts)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At current SPX price (near LIS)")
    pdf.key_value("Stop", "15 points")
    pdf.key_value("Target", "+10 points (fixed)")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "Paradigm shifts are the most significant events in dealer positioning. When the regime changes, "
        "ALL dealer hedging behavior changes direction simultaneously. Traders who positioned for the old regime "
        "are suddenly wrong, and their stop-outs create the momentum for the new direction. "
        "The 3-minute window catches this transition before it becomes obvious."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "89% (18 trades)")
    pdf.stat_line("Total P&L", "+29.3 points")
    pdf.stat_line("Profit Factor", "5.33")
    pdf.muted_text("Highest confidence setup. Small sample size but extremely reliable when it fires.")

    # ── 3.3 DD EXHAUSTION ────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.3  DD Exhaustion  (Workhorse)")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "DD Exhaustion detects when dealers have over-committed in one direction and the market is about "
        "to snap back. \"DD\" stands for Dealer Delta - the total directional exposure of market makers. "
        "When this shifts by hundreds of millions while charm says the opposite, dealers are wrong "
        "and about to get squeezed."
    )

    pdf.sub_title("How It Works")
    pdf.body_text(
        "Every 2 minutes, the system tracks how much Dealer Delta has shifted from the previous reading. "
        "A large negative shift means dealers just added massive bearish hedges. If charm is simultaneously "
        "positive (saying \"the market should go up\"), dealers are over-hedged and wrong."
    )
    pdf.bullet("LONG signal: DD shifts bearish (< -$200M) while charm is positive. Dealers over-hedged for a drop that won't happen. When price bounces, their hedges unwind and AMPLIFY the rally.")
    pdf.bullet("SHORT signal: DD shifts bullish (> +$200M) while charm is negative. Dealers over-positioned for a rally that charm says won't happen. Fade the crowd.")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("DD shift >= $200M in either direction")
    pdf.bullet("Charm opposes the DD shift (positive charm for long, negative for short)")
    pdf.bullet("Time window: 10:00 AM - 3:30 PM ET")
    pdf.bullet("Scoring: DD shift magnitude (30pts), charm strength (25pts), time (15pts), paradigm (15pts), direction bias (15pts)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At current SPX price")
    pdf.key_value("Initial Stop", "12 points")
    pdf.key_value("Trail", "Continuous trail - activates at +20pts profit, then locks at (max profit - 5pts)")
    pdf.key_value("Target", "No fixed target - trail only, lets winners run")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "This is a CONTRARIAN setup. It trades against dealer positioning when structural forces (charm) "
        "say they're wrong. The wide 20-point trail activation gives contrarian trades room to develop - "
        "these moves take time as the market digests the mismatch. Once the move is confirmed (+20pts), "
        "the trail locks in profits while allowing further extension."
    )
    pdf.body_text(
        "An important finding: SHORT signals are 3.6x more profitable than LONG signals (+4.7 pts/trade vs +1.3). "
        "When dealers are over-positioned bullishly, the unwinding cascade is more violent."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "50% (155 trades)")
    pdf.stat_line("Total P&L", "+368.5 points")
    pdf.stat_line("Profit Factor", "1.54")
    pdf.stat_line("Avg per trade", "+2.4 pts")
    pdf.muted_text("Workhorse setup. Fires most frequently, consistent positive expectancy. Score/grade is NOT predictive (LOG grade has 80% WR, A+ has 22%).")

    # ── 3.4 AG SHORT ─────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.4  AG Short  (Bearish Dealer Fade)")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "AG Short detects conditions where bearish dealer positioning (Anti-GEX paradigm) creates "
        "downward pressure. When dealers hold negative gamma with LIS acting as resistance above, "
        "and targets sitting below, ALL forces point down."
    )

    pdf.sub_title("How It Works")
    pdf.body_text(
        "In an AG paradigm, dealers are short gamma. This means they must SELL when price drops "
        "(amplifying the move) and BUY when price rises (capping the rally). The LIS level acts as "
        "a ceiling, and the -GEX level below acts as a magnet pulling price down."
    )
    pdf.bullet("AG paradigm active (dealers positioned bearishly)")
    pdf.bullet("Price below LIS (under the ceiling)")
    pdf.bullet("-GEX magnet sits at least 10 points below (room to fall)")
    pdf.bullet("Gap to LIS <= 20 points (still in dealers' hedging zone)")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("Paradigm contains \"AG\" or \"ANTI-GEX\"")
    pdf.bullet("Downside room >= 10 points to target")
    pdf.bullet("Scoring: LIS proximity (20pts), downside room (20pts), GEX clustering (20pts), target clustering (20pts), risk-reward (20pts)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At current SPX price")
    pdf.key_value("Initial Stop", "10 points above entry")
    pdf.key_value("Trail", "Breakeven at +10pts, then continuous (activation=15, gap=5)")
    pdf.key_value("Split Target", "T1: +10pts (half position), T2: trail to Volland target")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "AG Short exploits the self-reinforcing nature of negative gamma. Each tick lower forces dealers "
        "to sell more (to maintain their hedge), which pushes price lower, which forces more selling. "
        "This cascade continues until price reaches the -GEX level where dealer exposure peaks. "
        "The cluster analysis ensures dealers' key levels are concentrated (focused) rather than scattered."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "56% baseline, 82% with Greek filter (32 trades)")
    pdf.stat_line("Total P&L", "+30.6 points")
    pdf.stat_line("Profit Factor", "1.71 (with filter)")
    pdf.muted_text("Greek alignment is critical for AG Short. Charm-opposed trades drop to 36% WR.")

    # ── 3.5 GEX LONG ─────────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.5  GEX Long  (Bullish Force Alignment)")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "GEX Long detects when ALL dealer forces align upward in a GEX (positive gamma) paradigm. "
        "Every price level is classified as either SUPPORT (below spot) or MAGNET (above spot). "
        "When LIS supports, -GEX pulls up, +GEX attracts, and target beckons - ALL forces point UP."
    )

    pdf.sub_title("The Force Framework")
    pdf.body_text("Every level gets classified relative to current price:")
    pdf.bullet("LIS below spot = SUPPORT (dealers defend it). LIS above spot = MAGNET (pulls price up). Both are bullish.")
    pdf.bullet("-GEX above spot = MAGNET (dealers' negative gamma pulls price toward it)")
    pdf.bullet("+GEX above spot = MAGNET (stabilizing gamma attracts price)")
    pdf.bullet("Target above spot = MAGNET (Volland's projected move endpoint)")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("GEX paradigm active (not GEX-TARGET or GEX-MESSY - these are toxic)")
    pdf.bullet("Gap to LIS <= 5 points")
    pdf.bullet("+GEX magnet at least 10 points above spot")
    pdf.bullet("Target at least 10 points above spot")
    pdf.bullet("6-component force scoring (LIS proximity 25pts, -GEX force 20pts, +GEX magnet 20pts, target magnet 15pts, LIS type 10pts, time 10pts)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At current SPX price")
    pdf.key_value("Stop", "8 points (tight - force alignment should hold)")
    pdf.key_value("Trail", "Breakeven at +8pts, continuous trail (activation=10, gap=5)")
    pdf.key_value("Split Target", "T1: +10pts (half position), T2: trail to full target")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "When all dealer forces align upward, price faces a \"path of least resistance\" higher. "
        "Each level above acts as a magnet while levels below provide support. This creates a one-way "
        "escalator effect. The tight 8-point stop works because when force alignment is truly present, "
        "price shouldn't violate the support structure."
    )
    pdf.body_text(
        "Critical insight: Paradigm subtype matters enormously. GEX-LIS (100% WR historically) and "
        "GEX-PURE (67% WR) are the only tradeable subtypes. GEX-TARGET (25% WR) and GEX-MESSY (0% WR) "
        "are blocked by the detector."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "~50% SPX-only (rewritten Mar 8)", color=pdf.YELLOW)
    pdf.stat_line("Total P&L", "+45.4 pts (SL=8/T=10 backtest)")
    pdf.stat_line("Profit Factor", "1.86")
    pdf.muted_text("Recently rewritten. Old code was broken (29% WR). New force framework is promising but needs more live data.")

    # ── 3.6 BOFA SCALP ──────────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.6  BofA Scalp  (Range Bounce)")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "BofA Scalp detects when price touches the edge of a stable consolidation range (BofA paradigm) "
        "and is likely to bounce back. Think of it as playing ping-pong between the floor and ceiling "
        "that dealers are actively defending."
    )

    pdf.sub_title("How It Works")
    pdf.body_text(
        "In a BofA paradigm, the market has established two LIS levels (upper and lower) that form "
        "a trading range. The system monitors how STABLE these levels are (have they held for 30+ minutes?) "
        "and fires when price touches one edge."
    )
    pdf.bullet("Price within 3 points of lower LIS = BUY (bounce off floor)")
    pdf.bullet("Price within 3 points of upper LIS = SELL (bounce off ceiling)")
    pdf.bullet("LIS must be stable (held for at least 6 readings = 30 minutes)")
    pdf.bullet("Range width must be at least 15 points (real consolidation, not noise)")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("BofA paradigm active")
    pdf.bullet("LIS stability >= 6 bars (30 minutes)")
    pdf.bullet("Range width >= 15 points")
    pdf.bullet("Scoring: stability duration (20pts), width (20pts), charm neutrality (20pts), time (20pts), midpoint distance (20pts)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At current SPX price (near LIS edge)")
    pdf.key_value("Stop", "12 points beyond LIS (gives room for false breakout)")
    pdf.key_value("Target", "+10 points")
    pdf.key_value("Time Exit", "30 minutes maximum hold (protective!)")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "In a BofA regime, dealers are balanced and actively defending both edges. Each touch of the LIS "
        "triggers hedging activity that pushes price back into the range. The 30-minute time exit is crucial - "
        "scalps must be quick. Testing showed that holding BofA trades longer HURTS performance because "
        "ranges eventually break and the bounce reverses."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "53% (35 trades)", color=pdf.YELLOW)
    pdf.stat_line("Total P&L", "+16.1 points")
    pdf.stat_line("Profit Factor", "1.11")
    pdf.muted_text("Marginal edge. Works best with Greek alignment filter (64% WR). Not recommended as a standalone setup.")

    # ── 3.7 CVD DIVERGENCE ───────────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3.7  CVD Divergence  (Order Flow Contrarian)")

    pdf.sub_title("What It Detects")
    pdf.body_text(
        "CVD Divergence detects when price makes a new extreme (new high or new low) but the "
        "underlying buying/selling pressure (CVD) moves in the opposite direction. "
        "This signals exhaustion - the trend is running out of fuel and about to reverse."
    )

    pdf.sub_title("The Four Patterns")
    pdf.body_text("The detector scans ES futures 5-point range bars for swing divergences:", bold=True)
    pdf.ln(1)
    pdf.bullet("Sell Exhaustion (BUY): Price makes a LOWER LOW but CVD makes a HIGHER LOW. Sellers are giving up - each push lower requires less selling pressure. Reversal imminent.")
    pdf.bullet("Sell Absorption (BUY): Price makes a HIGHER LOW but CVD makes a LOWER LOW. Despite heavy selling (falling CVD), price holds higher. Buyers are absorbing the selling pressure.")
    pdf.bullet("Buy Exhaustion (SELL): Price makes a HIGHER HIGH but CVD makes a LOWER HIGH. Buyers are giving up - each push higher has less conviction. Reversal imminent.")
    pdf.bullet("Buy Absorption (SELL): Price makes a LOWER HIGH but CVD makes a HIGHER HIGH. Despite heavy buying (rising CVD), price can't make a new high. Sellers are absorbing buying pressure.")

    pdf.sub_title("Entry Conditions")
    pdf.bullet("Swing detection: pivot with 2-bar lookback/forward")
    pdf.bullet("Consecutive same-type swings compared (low vs low, high vs high)")
    pdf.bullet("Price and CVD must diverge (move opposite directions)")
    pdf.bullet("Time window: 10:00 AM - 3:30 PM ET")
    pdf.bullet("Exhaustion patterns grade A (score 65), Absorption grade B (score 45)")

    pdf.sub_title("Risk Management")
    pdf.key_value("Entry", "At ES futures close price of trigger bar")
    pdf.key_value("Stop", "8 points (fixed)")
    pdf.key_value("Target", "10 points (fixed)")
    pdf.key_value("Trail", "None - simple fixed bracket")

    pdf.sub_title("Why It Works")
    pdf.body_text(
        "CVD Divergence is a pure CONTRARIAN signal based on order flow physics. When price makes "
        "a new extreme but volume delta doesn't confirm, the move is hollow - it's being driven by "
        "stop runs or liquidity grabs rather than genuine conviction. The reversal typically happens "
        "quickly as trapped traders exit, creating momentum in the opposite direction."
    )
    pdf.body_text(
        "Important design note: This setup was intentionally kept SIMPLE. The original version had "
        "volume gates (1.4x), z-score minimums, and Volland confluence scoring. All were REMOVED "
        "because testing showed they hurt performance by filtering out good signals."
    )

    pdf.sub_title("Performance")
    pdf.stat_line("Win Rate", "57% baseline (138 trades)", color=pdf.YELLOW)
    pdf.stat_line("Total P&L", "-38.9 pts baseline, +93.9 pts with alignment >= 0 filter")
    pdf.stat_line("Backtest (Rithmic data)", "83% WR, +536 pts", color=pdf.GREEN)
    pdf.muted_text("Live performance below backtest due to execution gaps. Alignment filter is critical for this setup.")

    # ═══════════════════════════════════════════════════════════════════════
    # CHAPTER 4: Greek Alignment Filter
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("4. The Greek Alignment Filter")

    pdf.body_text(
        "The Greek Alignment Filter is the single most impactful improvement to the system. "
        "It acts as a macro context layer on top of all individual setups."
    )

    pdf.sub_title("How It Works")
    pdf.body_text("Three components are scored +1 (bullish) or -1 (bearish) and summed:")
    pdf.bullet("Charm: Is time decay forcing dealers to buy (+1) or sell (-1)?")
    pdf.bullet("Vanna: Is volatility positioning bullish (+1) or bearish (-1)?")
    pdf.bullet("SVB (Spot-Vol-Beta): Is the VIX overvixing (+1 contrarian bullish) or undervixing (-1)?")
    pdf.body_text("Result: alignment ranges from -3 (strongly bearish) to +3 (strongly bullish).")

    pdf.sub_title("The +3 / -3 Rule")
    pdf.body_text(
        "We ONLY trade when alignment = +3 (for long trades) or -3 (for short trades). "
        "This means ALL three Greeks must agree on direction."
    )

    pdf.sub_title("Impact on Results (266 trades analyzed)")
    pdf.stat_line("Win Rate", "53% -> 61% (+15% relative improvement)")
    pdf.stat_line("P&L", "+374 -> +602 pts (+61% improvement)")
    pdf.stat_line("Max Drawdown", "86 -> 35 pts (-59% reduction)")
    pdf.stat_line("Sharpe Ratio", "0.37 -> 0.61 (+65% improvement)")
    pdf.stat_line("Profit Factor", "1.28 -> 1.80 (+41% improvement)")

    pdf.info_box(
        "Why it works: Individual setups can fire in bad macro conditions. A perfect GEX Long setup "
        "is worthless if charm, vanna, and SVB all point down. The alignment filter ensures you're "
        "not fighting the entire Greek structure. Think of it as checking the wind direction before sailing."
    )

    # ═══════════════════════════════════════════════════════════════════════
    # CHAPTER 5: Risk Management
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("5. Risk Management")

    pdf.sub_title("Stop Loss Types")
    pdf.bullet("Fixed Stop: Set at entry. Never moves. Used by BofA Scalp (12pt), CVD Divergence (8pt), Paradigm Reversal (15pt).")
    pdf.bullet("Trailing Stop: Starts as a fixed stop, then follows price as the trade moves in your favor. Used by DD Exhaustion, GEX Long, AG Short, Skew Charm.")

    pdf.sub_title("How Trailing Stops Work")
    pdf.body_text("Our trailing stops have three phases:")
    pdf.bullet("Phase 1 - Initial Stop: Fixed stop (e.g., -8pts for GEX Long). Stays here until profit reaches the breakeven trigger.")
    pdf.bullet("Phase 2 - Breakeven: When profit reaches the BE trigger (e.g., +8pts for GEX Long), stop moves to entry price (breakeven). Risk eliminated.")
    pdf.bullet("Phase 3 - Continuous Trail: When profit reaches activation level (e.g., +10pts), stop follows at (max profit - gap). Example: if GEX Long reaches +15pts profit, stop sits at +15 - 5 = +10pts profit.")

    pdf.sub_title("Split Target Execution")
    pdf.body_text("Flow B setups (GEX Long, AG Short, DD Exhaustion, Skew Charm) use split targets:")
    pdf.bullet("T1 (First Target): 50% of position closes at +10 points profit. Locks in guaranteed gain.")
    pdf.bullet("T2 (Second Target): Remaining 50% stays open with trailing stop. Captures the big moves.")
    pdf.bullet("When T1 fills, the stop automatically moves to breakeven for the remaining position.")

    pdf.sub_title("Trail Parameters by Setup")
    pdf.info_box(
        "GEX Long:     SL=8pt,  BE@+8pts,  Trail activation=10, gap=5\n"
        "AG Short:     SL=10pt, BE@+10pts, Trail activation=15, gap=5\n"
        "DD Exhaust:   SL=12pt, No BE,     Trail activation=20, gap=5\n"
        "Skew Charm:   SL=20pt, BE@+10pts, Trail activation=10, gap=8\n"
        "BofA Scalp:   SL=12pt, Fixed target +10, 30min time exit\n"
        "CVD Divergence: SL=8pt,  Fixed target +10\n"
        "Paradigm Rev: SL=15pt, Fixed target +10"
    )

    pdf.sub_title("Position Sizing")
    pdf.body_text("The auto-trader uses 10 MES (Micro E-mini S&P 500) contracts per trade:")
    pdf.bullet("Each MES point = $5. So 10 MES = $50 per point.")
    pdf.bullet("Max risk per trade (worst case 20pt SL): $1,000")
    pdf.bullet("Average risk per trade (8pt SL): $400")

    # ═══════════════════════════════════════════════════════════════════════
    # CHAPTER 6: Performance Summary
    # ═══════════════════════════════════════════════════════════════════════
    pdf.add_page()
    pdf.section_title("6. Performance Summary")

    pdf.body_text(
        "Based on 476+ tracked trades across 21 trading days (as of March 2026). "
        "All figures are in SPX points (1 point = $50 per ES contract, $5 per MES)."
    )

    pdf.sub_title("Setup Rankings (with Greek Alignment Filter)")
    pdf.ln(2)

    # Table header
    pdf.set_fill_color(*pdf.SURFACE)
    pdf.set_font("Helvetica", "B", 8.5)
    pdf.set_text_color(*pdf.ACCENT)
    cols = [("Setup", 38), ("Trades", 16), ("WR", 14), ("P&L", 22), ("PF", 14), ("Type", 30), ("Status", 46)]
    for label, w in cols:
        pdf.cell(w, 7, label, fill=True, align="C")
    pdf.ln()

    # Table rows
    rows = [
        ("Skew Charm",      "56",  "92%", "+408.2", "4.53", "Momentum",    "MVP - enabled everywhere"),
        ("DD Exhaustion",   "155", "50%", "+368.5", "1.54", "Contrarian",  "Workhorse - most volume"),
        ("Paradigm Rev",    "18",  "89%", "+29.3",  "5.33", "Regime shift", "High confidence, rare"),
        ("AG Short",        "32",  "56%", "+30.6",  "1.71", "Bearish fade", "Good with filter"),
        ("BofA Scalp",      "35",  "53%", "+16.1",  "1.11", "Range scalp",  "Marginal, needs filter"),
        ("CVD Divergence",  "138", "57%", "-38.9",  "0.94", "Order flow",   "Needs align >= 0 filter"),
        ("GEX Long",        "42",  "29%", "-103.5", "0.49", "Bullish force","Rewritten Mar 8"),
    ]
    for i, (setup, trades, wr, pnl, pf, type_, status) in enumerate(rows):
        bg = pdf.SURFACE if i % 2 == 0 else pdf.BG
        pdf.set_fill_color(*bg)
        pdf.set_font("Helvetica", "B", 8)
        pnl_val = float(pnl)
        pnl_color = pdf.GREEN if pnl_val > 0 else pdf.RED
        pdf.set_text_color(*pdf.FG)
        pdf.cell(38, 6, setup, fill=True)
        pdf.cell(16, 6, trades, fill=True, align="C")
        pdf.cell(14, 6, wr, fill=True, align="C")
        pdf.set_text_color(*pnl_color)
        pdf.cell(22, 6, pnl, fill=True, align="C")
        pdf.set_text_color(*pdf.FG)
        pdf.set_font("Helvetica", "", 8)
        pdf.cell(14, 6, pf, fill=True, align="C")
        pdf.cell(30, 6, type_, fill=True, align="C")
        pdf.set_text_color(*pdf.MUTED)
        pdf.cell(46, 6, status, fill=True)
        pdf.ln()

    pdf.ln(6)
    pdf.sub_title("Grand Total")
    pdf.stat_line("Total Tracked", "476+ trades over 21 days")
    pdf.stat_line("Unfiltered P&L", "+710.4 points (+33.8 pts/day)")
    pdf.stat_line("With Greek Filter", "+1,030.3 points (+49.1 pts/day)")
    pdf.stat_line("Projected Monthly (2 ES)", "$37,000 - $74,000")

    pdf.ln(6)
    pdf.info_box(
        "Key takeaway: The system's edge comes from combining dealer positioning data (Volland) with "
        "real-time order flow (CVD/ES delta) and filtering through Greek alignment. No single setup is "
        "magic - the alignment filter turns mediocre setups into consistent winners and blocks the losers. "
        "The best approach is trading ALL setups that pass the +3 alignment gate."
    )

    # Save
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "0DTE_Alpha_Trading_Setups_Guide.pdf")
    pdf.output(out_path)
    print(f"PDF saved: {out_path}")
    return out_path


if __name__ == "__main__":
    build_pdf()
