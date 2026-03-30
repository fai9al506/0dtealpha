---
name: Copilot Reference Knowledge — Deep Trading Mechanics
description: Detailed trading mechanics from Volland references and Discord expert analysis. Background knowledge for the copilot. Read when analyzing unfamiliar patterns or when rules need deeper context.
type: reference
---

# Copilot Reference Knowledge

Source: Volland User Guide (37pp), White Paper (23pp), Discord Intelligence (583 lines), Discord exports Mar 19-25 2026.

---

## PARADIGM DETAILS

### BofA Paradigm (~40% of days)
- Customers long calls + long puts (strangles); dealers short both sides
- Charm: negative below spot, positive above = RANGE-BOUND
- LIS heavily defended. Lines breach <5% of time
- Best trade: Iron condors at LIS. Entry 10:05-10:35 (93-100% WR)
- Put line at -0.65% (93% hold), Call line at -0.30% (97% hold)

### GEX Paradigm (~25%)
- Customers long puts + short calls; dealers net short calls, long puts
- Charm: negative on BOTH sides = BULLISH TREND to target
- Target ~0.73% above spot. LIS below = support
- Best entry 10:30-11:05 (54-62% target hit). Often transitions to BofA after 1 PM

### Anti-GEX (AG) Paradigm (~20%)
- Customers short puts + long calls; dealers net long puts, short calls
- Charm: positive on BOTH sides = BEARISH TREND to target
- Target ~0.70% below spot. LIS above = resistance
- Delayed start vs GEX. Best 11:00-11:35 (50-70%)

### Sidial (~5%)
- Customers long calls + long puts (diff from BofA); dealers long both
- Single-point charm neutral. Mean-revert to single target
- Rare standalone. Usually post-GEX/AG target hit
- Wizard: Sidial = vol selling regime, expect vol expansion on catalyst (FOMC)

---

## GAMMA MECHANICS

- Positive gamma = friction (support/resistance). Negative gamma = acceleration zone
- VIX expansion WEAKENS gamma magnetic hold (momentum overrides structure)
- VIX compression + positive gamma = tightest ranges, hardest to break
- CRITICAL: Gamma is SECONDARY to vega. Dealer vega hedging ~100x larger than gamma
- Gamma correlations are ARTIFACTS of vega exposure, not independent drivers
- Don't trade gamma alone; pair with vega/vanna bias

---

## VEGA/SPOT-VOL CORRELATION

- Dealers are PERPETUALLY SHORT VEGA (profitable to warehouse)
- As spot falls → customers buy puts → dealer short vega increases → forced to BUY vol → vol rises reflexively
- Only ~25% of SPX vega hedged in VIX instruments; rest carried as risk
- Spot-vol correlation is DRIVEN by dealer vega hedging, not gamma
- Monitor dealer vega exposure: when it increases, spot-vol correlation strengthens

---

## DISCORD EXPERT PROFILES

### Apollo — "The Worm Master"
- 3-pillar framework: 0DTE positioning + worm (vol activity) + order flow
- Sums SPX+SPY DD for combined conviction
- Watches CL (crude), VX for multi-asset confirmation
- Uses anchored VWAPs from event days (Trump tweets, headlines)
- Takes entries at open with order flow confirmation (I can't replicate this)
- Plays call/put spreads and lottos for convex payoffs
- Mar 25: best month in trading. Shorted at open, banked 20 pts immediately

### Wizard of Ops — "The Vol Strategist"
- Specializes in skew curve analysis and vol mispricing
- Identifies panic vs structural put buying (different trading responses)
- Calls paradigm regime shifts early
- "2 PM reversal" pattern on panic put days
- Bold calls: "long gamma long vega" when skew undervalued (validated by VIX spike)
- "0DTE analysis not effective on FOMC days" — sit out

### LordHelmet — "The Disciplined Scalper"
- "Sell pops while DD stays negative" — his core rule
- Sits out when no clear edge (DD grind)
- Takes deliberate breaks after good days (avoids giveback)
- Uses puts as asymmetric portfolio hedge
- "In 2026, fewer trades = higher P/L"

### TheEdge — "The DD Tracker"
- 15-year trader. Tracks DD turns at price extremes as reversal signals
- "Being short at the wrong times does a lot of damage to mental capital"
- Notes SPX vs SPY DD divergence

### Dragonboys — "The Patient Waiter"
- Sits on hands when no edge. No overnight exposure in headline markets
- Waits for key support levels before entering (6525 example)
- Identifies 200 SMA as key level

### Zack — "The Structure Analyst"
- Identifies quarterly put decay as hidden support
- Spots regime similarities across years ("vibes from last year")
- Uses 50-wide butterflies on swing expirations as cheap lottos

---

## KEY DISCORD PATTERNS (Multi-Expert Consensus)

1. **DD direction after 10am = primary short-term bias** (LordHelmet, Abcdefg, TheEdge)
2. **Vol seller/buyer flip = key catalyst signal** (Apollo, Johannes, Disciple3)
3. **Negative gamma above = explosive fuel but needs ignition** (Wizard, Apollo, LordHelmet, Phoenix)
4. **CL drives equities in geopolitical regimes** (Apollo, jay, dauma)
5. **Vanna support CONDITIONAL on fixed-strike vol declining** (Apollo, Miyaka)
6. **Panic put buying days = reversal setup at 2 PM** (Wizard of Ops)

---

## VALIDATED EXPERT CALLS

| Date | Expert | Call | Result |
|------|--------|------|--------|
| Mar 19 | Wizard | Bearish below 6600, no overvixing | Correct — market failed at 6600 |
| Mar 20 | Wizard | Panic puts → 2pm reversal | Partially correct — bounced into close |
| Mar 20 | Wizard | Skew undervalued, long vega | Correct — VIX spiked following week |
| Mar 23 | Apollo | Rally toward 6625 SPX (positive vanna + negative gamma) | Correct — rallied from 6500s to 6600+ |
| Mar 23 | Apollo | 6650 ceiling today | Correct — stayed below 6650 |
| Mar 23 | Apollo | Combined DD -6B = bearish | Correct — 60pt drop followed |
| Mar 25 | Apollo | Short at open, target 20pts | Correct — 20pts in 18 minutes |
| Mar 25 | LordHelmet | Sell pops while DD negative | Correct — shorts worked all morning |
| Mar 25 | Apollo | Squeeze higher coming (13:35) | Partially — DD flipped, choppy bounce |
| Mar 26 | Apollo | Globex long → TP at 6608.5 by 09:48 | Correct — hit TP1 in 18 min, "no need to trade" |
| Mar 26 | Apollo | Vol buyside at 10:10 → bearish catalyst | Correct — 90pt drop followed (6568→6475) |
| Mar 26 | Apollo | Liquidity below = pull, not support (13:20) | Correct — price swept through to 6475 |
| Mar 26 | Apollo | DD flip at 12:35 | Correct — brief bounce attempt, sellers won |
| Mar 26 | Apollo | 6475 ES target (13:54) | Correct — hit by close |
| Mar 26 | Wizard | "Up 13% this month" — long vega paying | Validated — VIX >20 all March |
| Mar 26 | Phoenix | 6320/6340 eventual target | Pending — not yet tested |
| Mar 26 | Yahya Z | VIX/VIX3M = 1.03, term structure inverted | Correct — confirmed our overvix reading |
