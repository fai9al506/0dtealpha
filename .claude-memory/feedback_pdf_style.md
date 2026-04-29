---
name: PDF report dark style
description: User loves the Analysis #15 PDF dark theme — use it as default for all future PDF reports and analysis illustrations
type: feedback
---

Always use the Analysis #15 dark theme for PDF reports and charts. Reference: `tmp_analysis15_pdf.py`.

**Why:** User said "wow! i like this PDF, make it the default style for all analysis and illustration"

**How to apply:** When generating any PDF report, chart, or analysis illustration:

**Color palette:**
- `DARK_BG = '#1a1a2e'` (figure background)
- `PANEL_BG = '#16213e'` (axes background)
- `CARD_BG = '#0f3460'` (info boxes, table headers)
- `ACCENT_GREEN = '#00e676'` (positive values, wins, good metrics)
- `ACCENT_RED = '#ff5252'` (negative values, losses, risk)
- `ACCENT_BLUE = '#448aff'` (neutral/baseline data)
- `ACCENT_GOLD = '#ffd740'` (titles, headers, highlights, key findings)
- `ACCENT_PURPLE = '#e040fb'` (alternative/hybrid options)
- `TEXT_WHITE = '#ffffff'` (main text)
- `TEXT_LIGHT = '#b0bec5'` (secondary text, labels)
- `TEXT_DIM = '#607d8b'` (subtle text, dividers, grid)

**Layout conventions:**
- Figure size: `(11, 8.5)` (letter landscape)
- matplotlib `Agg` backend, `PdfPages` for multi-page
- `plt.rcParams` set for dark theme (facecolor, edgecolor, text, grid, ticks)
- Title page with `FancyBboxPatch` key-finding box
- Section headers in ACCENT_GOLD, bold
- Divider lines: `ax.plot([10,90], [y,y], color=ACCENT_GOLD, linewidth=1.5, alpha=0.6)`
- Bar charts with `edgecolor='white', linewidth=0.5, alpha=0.8`
- Value labels on bars (above or inside)
- Grid: `alpha=0.3` on relevant axis
- Tables: `cell.set_facecolor(PANEL_BG)`, header row `CARD_BG` with `ACCENT_GOLD` text
- Footer: `'0DTE Alpha | Confidential Trading Research'` in `TEXT_DIM`, italic
- Gridspec for complex layouts, tight_layout for simple ones
- Sans-serif font, base size 9
