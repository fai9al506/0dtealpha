---
name: Dashboard Design System
description: Approved dark dashboard style (Outfit + JetBrains Mono) — user wants to apply to main 0DTE dashboard later
type: project
---

User approved the stock-gex-live dashboard redesign (commit `8e2455a`, 2026-03-24) and wants to apply the same style to the main 0DTE Alpha dashboard (`/`) later.

**Why:** User found this design clean, comfortable, and professional for data-dense dashboards.

**How to apply:** When user asks to restyle the main dashboard, use these exact specs:

## Fonts
- **Google Fonts import:** `Outfit:wght@300;400;500;600;700` + `JetBrains+Mono:wght@400;500;600`
- **Body/UI text:** `'Outfit', system-ui, sans-serif` — 12px, line-height 1.5, font-weight 400
- **Numeric data** (prices, ratios, KPI values, chart axes, status pills, sidebar prices): `'JetBrains Mono', monospace`
- **NO serif fonts** on data dashboards (DM Serif Display is for content/editorial pages only)
- `-webkit-font-smoothing: antialiased`

## Color Palette
```css
--bg-0: #0b0e14;   /* deepest background (main panel) */
--bg-1: #131720;   /* cards, sidebar, tables */
--bg-2: #1a1f2e;   /* hover states, table headers, secondary surfaces */
--bg-3: #232a3b;   /* active states, count badges */
--border: #1c2333;   /* subtle but visible — distinct from ALL bg layers */
--border-l: #2a3548; /* hover borders */
--text: #dfe4ed;     /* primary text */
--text-2: #94a3b8;   /* secondary text */
--text-3: #64748b;   /* tertiary/labels */
--blue: #3b82f6;     /* active tabs, links, selected items */
--green: #22c55e;    /* positive data, win, watchlist dots */
--green-glow: rgba(34,197,94,0.35); /* status dot glow */
--red: #ef4444;      /* negative data, loss */
--amber: #f59e0b;    /* warnings, tier-A */
--purple: #a78bfa;   /* T2 targets */
```

## Component Specs
- **Header:** bg-1, 10px 24px padding, h1 15px font-weight 700
- **Tabs:** Outfit 12px/500, blue active + blue underline (NOT green, NOT monospace)
- **Sidebar:** 210px, bg-1, blue selected highlight
- **Tables:** th on bg-2 (visible header separation), 8px 0 0 0 / 0 8px 0 0 radius, 11px body
- **Cards:** bg-1, border-radius 12px, 16px padding
- **KPI values:** JetBrains Mono 17px/600
- **Badges:** 4px radius, 10px font, 600 weight (NOT pill-shaped — too much for data)
- **Filter buttons:** 6px radius, 11px, blue active state
- **Plotly charts:** paper/plot bg #131720, grid #1c2333, zeroline #2a3548, font Outfit, tick font JetBrains Mono
- **Status dot:** 6px, green with `box-shadow: 0 0 6px var(--green-glow)`
- **Scrollbar:** 5px, transparent track, border-l thumb

## Key Design Rules
1. JetBrains Mono ONLY for numeric data — never for tabs, labels, or body text
2. Blue for navigation/interaction, green/red for data meaning
3. Each background layer must be visibly distinct from its neighbors
4. Border color must be distinct from all background layers
5. Keep data-dense spacing (12px body, tight padding) — don't bloat for aesthetics
