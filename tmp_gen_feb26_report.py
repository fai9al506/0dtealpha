"""Generate Feb 26, 2026 Trading Analysis PDF."""
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "Feb26_Trade_Analysis.pdf")

doc = SimpleDocTemplate(OUTPUT, pagesize=letter,
                        leftMargin=0.6*inch, rightMargin=0.6*inch,
                        topMargin=0.5*inch, bottomMargin=0.5*inch)

styles = getSampleStyleSheet()
title_style = ParagraphStyle("Title2", parent=styles["Title"], fontSize=18, spaceAfter=6)
h1 = ParagraphStyle("H1", parent=styles["Heading1"], fontSize=14, spaceAfter=4, spaceBefore=10)
h2 = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=12, spaceAfter=3, spaceBefore=8)
h3 = ParagraphStyle("H3", parent=styles["Heading3"], fontSize=10, spaceAfter=2, spaceBefore=6)
body = ParagraphStyle("Body2", parent=styles["BodyText"], fontSize=9, spaceAfter=4)
small = ParagraphStyle("Small", parent=styles["BodyText"], fontSize=8, spaceAfter=2)
bold_body = ParagraphStyle("BoldBody", parent=body, fontName="Helvetica-Bold")
red_body = ParagraphStyle("RedBody", parent=body, textColor=colors.red)
green_body = ParagraphStyle("GreenBody", parent=body, textColor=colors.darkgreen)

def make_table(data, col_widths=None, header=True):
    t = Table(data, colWidths=col_widths, repeatRows=1 if header else 0)
    style_cmds = [
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    if header:
        style_cmds += [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]
    # Alternate row colors
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#ecf0f1")))
    t.setStyle(TableStyle(style_cmds))
    return t

def hr():
    return HRFlowable(width="100%", thickness=1, color=colors.grey, spaceAfter=6, spaceBefore=6)

story = []

# ======================== TITLE ========================
story.append(Paragraph("Feb 26, 2026 — Daily Trading Analysis", title_style))
story.append(Paragraph("0DTE Alpha — Portal Setup Tracking", body))
story.append(hr())

# ======================== DAY SUMMARY ========================
story.append(Paragraph("Day Summary", h1))
story.append(make_table([
    ["Metric", "Value"],
    ["Total Signals", "40"],
    ["Result", "14W / 18L / 8E"],
    ["Net P&L", "+32.9 pts"],
    ["All-Time Cumulative (corrected)", "+389.9 pts (198 trades)"],
], col_widths=[2.5*inch, 3*inch]))
story.append(Spacer(1, 8))
story.append(Paragraph(
    "A positive day dragged down by GEX Long (-69.7 pts). Without GEX Long, "
    "the other 4 setups delivered +102.6 pts on 29 trades.", body))

# ======================== PER-SETUP ========================
story.append(Paragraph("Per-Setup Performance", h1))
story.append(make_table([
    ["Setup", "Trades", "W/L/E", "WR", "P&L", "Notes"],
    ["DD Exhaustion", "15", "5/6/4", "33%", "+48.8", "One monster +48.4 win carried"],
    ["ES Absorption", "7", "5/1/1", "71%", "+35.8", "Best WR, 5/7 wins"],
    ["BofA Scalp", "2", "1/0/1", "100%", "+13.0", "Clean, low volume"],
    ["Paradigm Rev.", "5", "2/1/2", "40%", "+5.0", "Modest"],
    ["GEX Long", "11", "1/10/0", "9%", "-69.7", "Catastrophic"],
], col_widths=[1.2*inch, 0.6*inch, 0.7*inch, 0.5*inch, 0.6*inch, 2.8*inch]))

# ======================== GEX LONG ========================
story.append(Paragraph("GEX Long Disaster (11 trades, -69.7 pts)", h1))
story.append(Paragraph(
    "All 11 trades were long into a selloff. SPX opened ~5935 and plunged to ~5863 (-72 pts). "
    "GEX Long kept firing buy signals as price fell through support levels. "
    "6 trades fired in 26 minutes during the initial waterfall drop (09:49-10:15).", body))

story.append(make_table([
    ["ID", "Time", "Grade", "Score", "Result", "PnL", "MaxP", "Paradigm"],
    ["250", "09:49", "A-Entry", "60", "LOSS", "-8.0", "6.5", "GEX-PURE"],
    ["251", "10:01", "A-Entry", "65", "LOSS", "-8.0", "5.1", "GEX-PURE"],
    ["253", "10:05", "A-Entry", "60", "LOSS", "-8.0", "2.3", "GEX-PURE"],
    ["254", "10:08", "A-Entry", "60", "LOSS", "-8.0", "2.0", "GEX-PURE"],
    ["255", "10:09", "A-Entry", "65", "LOSS", "-8.0", "4.3", "GEX-PURE"],
    ["256", "10:15", "A+", "90", "LOSS", "-8.0", "-3.6", "GEX-PURE"],
    ["260", "10:52", "A+", "95", "WIN", "+10.3", "15.3", "GEX-PURE"],
    ["261", "11:19", "A", "80", "LOSS", "-8.0", "3.9", "GEX-PURE"],
    ["263", "11:38", "A", "75", "LOSS", "-8.0", "-0.8", "GEX-PURE"],
    ["264", "11:40", "A", "85", "LOSS", "-8.0", "1.4", "GEX-PURE"],
    ["268", "11:51", "A+", "95", "LOSS", "-8.0", "-2.3", "GEX-PURE"],
], col_widths=[0.4*inch, 0.5*inch, 0.7*inch, 0.5*inch, 0.5*inch, 0.5*inch, 0.5*inch, 1.2*inch]))

# ======================== VANNA FILTER ========================
story.append(PageBreak())
story.append(Paragraph("GEX Long — Vanna Regime Filter Discovery", h1))
story.append(Paragraph(
    "Investigated whether aggregated vanna (all expirations) from Volland exposure data "
    "can predict GEX Long failure. Data from volland_exposure_points, greek='vanna', "
    "exposure_option='ALL'. Available from Feb 11 onwards.", body))

story.append(Paragraph("Vanna ALL Sign vs GEX Long Outcome (all 32 historical trades)", h2))
story.append(make_table([
    ["Vanna ALL Sign", "Trades", "Wins", "Losses", "Expired", "WR", "P&L"],
    ["NEGATIVE", "17", "0", "15", "2", "0.0%", "-114.4"],
    ["POSITIVE", "7", "3", "4", "0", "42.9%", "+3.3"],
    ["NO_DATA (pre-Feb 11)", "8", "3", "5", "0", "37.5%", "+25.0"],
], col_widths=[1.5*inch, 0.6*inch, 0.5*inch, 0.6*inch, 0.6*inch, 0.5*inch, 0.7*inch]))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "<b>When aggregated vanna is negative, GEX Long has ZERO wins out of 17 trades.</b> "
    "Not one. Every single trade was a loss or expired. This is the strongest filter signal "
    "in the entire dataset.", bold_body))

story.append(Paragraph("Filter Impact", h2))
story.append(make_table([
    ["Metric", "Without Filter", "With Vanna Filter", "Change"],
    ["Trades", "32", "15", "-17 blocked"],
    ["Wins", "6", "6", "Zero wins lost"],
    ["Losses", "24", "9", "-15 fewer losses"],
    ["Win Rate", "20.0%", "40.0%", "+20 pp"],
    ["Total PnL", "-86.1 pts", "+28.3 pts", "+114.4 pts"],
], col_widths=[1.2*inch, 1.3*inch, 1.3*inch, 1.5*inch]))

story.append(Paragraph("Cross-Setup Vanna Impact", h2))
story.append(Paragraph(
    "Vanna regime affects setups differently. DD Exhaustion performs BETTER in negative vanna "
    "(contrarian signal works when dealers are heavily hedged). GEX Long is directional and "
    "gets crushed.", body))
story.append(make_table([
    ["Setup", "Neg Vanna WR", "Neg Vanna PnL", "Pos Vanna WR", "Pos Vanna PnL"],
    ["GEX Long", "0% (17)", "-114.4", "42.9% (7)", "+3.3"],
    ["DD Exhaustion", "45.9% (37)", "+300.2", "22.6% (53)", "-31.7"],
    ["AG Short", "42.9% (7)", "+35.9", "40.0% (15)", "+63.4"],
    ["BofA Scalp", "25.0% (16)", "+51.3", "33.3% (6)", "-0.6"],
    ["ES Absorption", "50% (2)", "+7.8", "70% (10)", "+34.0"],
    ["Paradigm Rev.", "37.5% (8)", "+30.0", "50% (8)", "+11.5"],
], col_widths=[1.2*inch, 1*inch, 1*inch, 1*inch, 1*inch]))

story.append(Paragraph("Status: PENDING — saved for implementation. Needs 15+ more trades with positive vanna to validate.", small))

# ======================== DD EXHAUSTION ========================
story.append(PageBreak())
story.append(Paragraph("DD Exhaustion Analysis (15 trades, +48.8 pts)", h1))

story.append(make_table([
    ["ID", "Time", "Dir", "Grade", "Score", "Result", "PnL", "MaxP", "Paradigm"],
    ["252", "10:04", "short", "A+", "75", "WIN", "+48.4", "53.4", "GEX-PURE"],
    ["257", "10:24", "long", "A-Entry", "29", "LOSS", "-12.0", "6.3", "SIDIAL-MESSY"],
    ["258", "10:28", "short", "A", "59", "LOSS", "-12.0", "8.5", "SIDIAL-MESSY"],
    ["259", "10:47", "short", "A", "58", "LOSS", "-12.0", "-0.5", "GEX-MESSY"],
    ["262", "11:18", "short", "A", "60", "WIN", "+15.8", "20.8", "GEX-PURE"],
    ["266", "11:44", "short", "A", "62", "WIN", "+16.5", "21.5", "GEX-PURE"],
    ["271", "12:14", "short", "A-Entry", "48", "LOSS", "-12.0", "17.5", "SIDIAL-EXT"],
    ["272", "12:25", "short", "A-Entry", "40", "LOSS", "-12.0", "15.2", "SIDIAL-EXT"],
    ["274", "12:39", "long", "A-Entry", "48", "WIN", "+17.0", "22.0", "SIDIAL-EXT"],
    ["276", "13:13", "long", "A-Entry", "50", "WIN", "+23.1", "28.1", "AG-LIS"],
    ["281", "13:58", "short", "A", "74", "LOSS", "-12.0", "1.8", "BOFA-PURE"],
    ["282", "14:10", "short", "A+", "80", "EXPIRED", "0.0", "3.4", "BofA-LIS"],
    ["286", "14:59", "short", "A", "68", "EXPIRED", "0.0", "4.9", "SIDIAL-EXT"],
    ["287", "15:01", "short", "A+", "90", "EXPIRED", "0.0", "3.0", "AG-LIS"],
    ["289", "15:26", "short", "A+", "75", "EXPIRED", "0.0", "3.5", "BOFA-PURE"],
], col_widths=[0.35*inch, 0.45*inch, 0.45*inch, 0.6*inch, 0.4*inch, 0.55*inch, 0.5*inch, 0.45*inch, 1.0*inch]))

story.append(Paragraph("DD Filter Validation (proposed from Analysis #5)", h2))
story.append(make_table([
    ["Filter", "Trades Removed", "Wins Lost", "PnL Saved", "Remaining"],
    ["14:00 cutoff", "4 (all expired)", "0", "+0 (neutral)", "11 trades"],
    ["+ Block BOFA-PURE", "1 more (#281 LOSS)", "0", "+12.0", "10 trades"],
    ["Combined", "5 removed", "0 wins lost", "+12.0 saved", "5W/5L = 50% WR, +60.8"],
], col_widths=[1.3*inch, 1.2*inch, 0.8*inch, 0.9*inch, 1.5*inch]))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "MESSY paradigm finding from Analysis #5 (100% WR) INVERTED today — "
    "both SIDIAL-MESSY trades lost (-24 pts). Small samples both ways. "
    "GEX-PURE was the winner today: 3/3 wins, +80.7 pts.", body))

# ======================== ES ABSORPTION ========================
story.append(Paragraph("ES Absorption — Standout Day (7 trades, +35.8 pts, 71% WR)", h1))
story.append(make_table([
    ["ID", "Time", "Dir", "Grade", "Score", "Result", "PnL", "Max Profit"],
    ["270", "12:11", "bearish", "B", "51.3", "WIN", "+10.0", "20.3"],
    ["275", "13:10", "bearish", "A", "58.2", "LOSS", "-12.0", "8.5"],
    ["280", "13:56", "bullish", "C", "30.9", "WIN", "+10.0", "66.0"],
    ["283", "14:15", "bullish", "C", "26.1", "WIN", "+10.0", "55.0"],
    ["284", "14:47", "bearish", "B", "44.7", "WIN", "+10.0", "49.8"],
    ["291", "15:42", "bullish", "B", "37.3", "WIN", "+4.3", "3.5"],
    ["292", "15:59", "bullish", "B", "53.8", "EXPIRED", "+3.5", "2.8"],
], col_widths=[0.4*inch, 0.5*inch, 0.7*inch, 0.5*inch, 0.5*inch, 0.55*inch, 0.5*inch, 0.8*inch]))

story.append(Paragraph("Trailing Potential", h2))
story.append(Paragraph(
    "3 trades had massive max profits (50-66 pts) but we exited at +10 each. "
    "Total profit left on the table from 6 winners: 151 pts. "
    "Current capture rate across all 12 ES Absorption trades: 17.9% (41.8 of 233 theoretical pts).", body))
story.append(make_table([
    ["ID", "Max Profit", "Captured", "Left on Table"],
    ["280", "66.0", "+10", "56.0"],
    ["283", "55.0", "+10", "45.0"],
    ["284", "49.75", "+10", "39.8"],
    ["270", "20.25", "+10", "10.3"],
    ["147 (Feb 19)", "10.0", "+10", "0.0"],
    ["150 (Feb 19)", "10.0", "+10", "0.0"],
    ["TOTAL", "211.0", "+60", "151.1"],
], col_widths=[1.2*inch, 1*inch, 1*inch, 1.2*inch]))
story.append(Paragraph(
    "Sample too small (12 trades, 3 mega-runners all from one afternoon) to implement trailing. "
    "Need more data to confirm this is typical vs outlier.", small))

# ======================== BUG FIX ========================
story.append(PageBreak())
story.append(Paragraph("Bug Fix: EXPIRED Trades PnL = 0 (31 trades corrected)", h1))
story.append(Paragraph(
    "Discovered that EXPIRED trades from Feb 24+ had outcome_pnl = 0.0 instead of actual P&L. "
    "Root cause: EOD summary at 16:05 ET could not get spot price because last_run_status was "
    "already overwritten to 'outside market hours'.", body))

story.append(Paragraph("Fix Applied", h2))
story.append(Paragraph(
    "Changed market_closed threshold from 16:00 to 15:57 ET. All open trades now close "
    "3 minutes before market end while spot price is still available from the live tracker. "
    "Added _last_known_spot cache as safety net.", body))

story.append(Paragraph("Backfill Impact", h2))
story.append(make_table([
    ["Metric", "Before (inflated)", "After (corrected)"],
    ["All-time PnL", "+429.9 pts", "+389.9 pts"],
    ["Trades affected", "31 with pnl=0", "All corrected"],
    ["Correction", "", "-40.0 pts (were hiding losses)"],
], col_widths=[1.5*inch, 1.8*inch, 2*inch]))

# ======================== CORRECTED TOTALS ========================
story.append(Paragraph("Corrected All-Time Totals (198 trades)", h1))
story.append(make_table([
    ["Setup", "Trades", "W/L/E", "Total PnL"],
    ["DD Exhaustion", "90", "29W/32L/29E", "+288.0"],
    ["AG Short", "25", "11W/8L/6E", "+71.5"],
    ["BofA Scalp", "23", "6W/5L/12E", "+58.6"],
    ["ES Absorption", "12", "8W/3L/1E", "+41.8"],
    ["Paradigm Reversal", "16", "7W/1L/8E", "+16.2"],
    ["GEX Long", "32", "6W/24L/2E", "-86.1"],
    ["TOTAL", "198", "67W/73L/58E", "+389.9"],
], col_widths=[1.5*inch, 0.8*inch, 1.5*inch, 1*inch]))

# ======================== RECOMMENDATIONS ========================
story.append(Paragraph("Pending Filters — Priority List", h1))
story.append(make_table([
    ["#", "Filter", "Impact", "Confidence", "Status"],
    ["1", "GEX Long: block when vanna ALL < 0", "+114.4 pts, 0 wins lost", "VERY HIGH", "PENDING"],
    ["2", "DD: cut after 14:00 ET", "+82 pts saved", "VERY HIGH", "PENDING"],
    ["3", "DD: block BOFA-PURE paradigm", "+21 pts saved", "HIGH", "PENDING"],
    ["4", "DD: raise threshold to $500M", "Removes weak signals", "MEDIUM", "PENDING"],
    ["5", "DD: charm ceiling $200M", "Safety filter", "MEDIUM", "PENDING"],
    ["6", "ES Absorption: add trailing", "+151 pts potential", "MEDIUM (small sample)", "PENDING"],
], col_widths=[0.3*inch, 2.2*inch, 1.4*inch, 1*inch, 0.8*inch]))

story.append(Spacer(1, 12))
story.append(Paragraph(
    "All filters pending user decision to accumulate more data before implementation.", small))

doc.build(story)
print(f"PDF saved to: {OUTPUT}")
