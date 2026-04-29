---
name: Discord research ideas from Mar 23 analysis
description: Research-tier ideas from comparing Volland Discord with our V11 trades on Mar 23. Includes fixed strike vol, panic vs structural put buying, volatility spike pause.
type: project
---

## Research Ideas from Discord Analysis (Mar 23, 2026)

### 1. Fixed Strike Vol for Vanna Interpretation (Apollo, Mar 21)
- Vanna support only holds when fixed strike vol is declining
- Positive vanna below spot + falling IV = confirmed support
- Positive vanna below spot + rising IV = support will fail
- We already capture chain IV per strike — could compute trend
- **Effort:** High (IV tracking per strike over time)

### 2. Panic vs Structural Put Buying (Wizard, Mar 20)
- Geopolitical panic put buying → contrarian reversal signal
- Structural put buying (institutional) → trend-following, not contrarian
- Wizard called green close on selloff day using this distinction
- **Effort:** High (new signal type, need to classify put volume surges)

### 3. Volatility Spike Pause (Dynamic)
- If ES range bar volatility exceeds 3x normal, pause entries 15-30 min
- Would avoid post-headline chop (Trump-Iran Mar 23 caused 200pt futures spike pre-market)
- **Effort:** Medium

### 4. Gap-Day Charm-Limit Delay — HIGH PRIORITY
- On gap days (opening > 30 pts from prior close), delay charm-limit entries 30-60 min
- Mar 23: all 4 morning losses (09:45-10:38 ET) were charm-limit entries = -51.9 pts
- Charm S/R levels stale from overnight, misaligned with gap opening
- User confirmed: "charm always misleading when we open with a big gap"
- **Effort:** Low (time check + gap detection)

### 5. FOMC Event Day Filter
- Wizard: "0DTE analysis is not effective on FOMC days"
- Known FOMC dates = no trading or reduced sizing
- **Effort:** Low (date list)
