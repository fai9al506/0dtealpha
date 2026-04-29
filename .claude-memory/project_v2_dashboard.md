---
name: V2 Dashboard — modern trading cockpit
description: New dashboard at /v2 with actionable design for manual trading. Separate file, easy to delete.
type: project
---

**V2 Dashboard deployed 2026-03-15** at `/v2` route.

**File:** `app/dashboard_v2.py` (self-contained, ~1000 lines)
**Integration:** 31 lines added to `app/main.py` (router import + init)
**Commits:** `a549265` (initial), `98febfe` (layout fix), `82845eb` (actionability)

**TO REVERT V2 COMPLETELY:**
1. Delete `app/dashboard_v2.py`
2. In `app/main.py` remove:
   - 3 lines after `app = FastAPI()`: comment + `from app.dashboard_v2 import router` + `app.include_router`
   - ~18 lines in `on_startup()`: the `# Initialize V2 dashboard` try/except block
3. Or just: `git revert 82845eb 98febfe a549265`

**Design:** Plus Jakarta Sans + JetBrains Mono, navy grain bg, KPI cards with colored accents, pill toggle bar, fade-up animations. Reference: `C:\Users\Faisa\Downloads\trading-dashboard (2).html`

**Tabs:** Overview (NEW), Exposure, Charts, ES Delta, Trade Log, Historical (placeholder)

**Key actionability features:**
- Signal bar with audio alert (Web Audio API, two rising tones) on new signal
- Signal shows: setup, grade, direction, entry, target, stop, alignment
- KPI cards: Spot+LIS distance, Paradigm, DD+SVB, Charm, VIX+Overvix, Today P&L
- Overview: full-width price chart with levels + 3 mini exposure charts + signals table
- Exposure: horizontal bars with shared Y-axis (strikes synced across all 5 charts)

**Why:** User wants manual trading dashboard alongside automated system. V1 is monitoring-focused; V2 is trading-focused.

**How to apply:** Original dashboard at `/dashboard` completely untouched. User switches to V2 when comfortable, or deletes it.
