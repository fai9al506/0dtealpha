---
name: Apollo Vol Seller/Buyer Detection Research
description: All Discord insights on how Apollo and others detect vol sellers/buyers — for Sierra backtesting and system integration
type: reference
---

## Apollo's Vol Detection Method — Complete Discord Evidence

### His Core Method (Direct Quotes)

**"Track time and sales. Best method and LOB shifts."** (Mar 21, 00:07)

He watches:
1. **Time & Sales** — raw trade tape, who is aggressing (buying at ask vs selling at bid)
2. **LOB shifts** — Level of Book / order book depth changes (bids/asks stacking or pulling)
3. **Fixed strike IV changes** — vol sellers show as IV dropping at specific strikes, vol buyers show as IV rising

### The "Tapeworm" (Oct 1, 2025)
- Wizard of Ops: "One spot I have seen a couple of bounces already is where the vol sellers end and vol buyers begin."
- l0rd.helmet: "is that the 'tapeworm' Apollo has been using?"
- **Definition:** The boundary strike/price level where vol selling activity transitions to vol buying activity. It's a dynamic S/R level.
- Apollo never publicly explained the exact method — likely proprietary. But the concept is: find where IV change flips sign across strikes.

### Apollo's Fixed Strike Vol Insight (Mar 21, 01:35)
Full explanation of how vanna support breaks:
- "Vanna becomes very supportive with fixed strike vols rolling down"
- "Look at the fixed strike vols. As charm and vanna are supportive from an exposure side but when fixed strike vols come down, that is where the support really plays out"
- When IV at a fixed strike RISES → delta increases → more aggressive hedging → support WEAKENS
- When IV at a fixed strike FALLS → delta doesn't increase → support HOLDS
- macki9917 asked: "note the vols at key strikes at open and watch when price approaches key vanna level if that vol is rising or dropping to know if that support will hold or not" → Apollo confirmed this is the right approach

### Apollo's Greeks Priority for 0DTE (Nov 10, 2025)
1. **Delta** first — "I can sort of see how Gamma and Charm will change as spot moves to those strikes"
2. **Charm** — primary payoff mechanism for 0DTE
3. **Gamma** — hedging urgency
4. **Theta** — "Gamma/Theta Ratio as a proxy for hedging needs" (conceptual, not a Volland widget)
5. **Vanna** — "miniscule in comparison to Gamma and Charm of 0dte option. Vanna is more effective for 5-45 DTE range"

### Apollo's Preferred Data Sources for 0DTE (Feb 10, 2026)
- "Primarily Charm and 0dteDeltDecay.. then This Week Vanna"
- "0dteDD exposure by strike is structured just like Charm is"

### How Vol Sellers/Buyers Show Up in ES Futures
- Customer buys puts → dealer sells puts (short puts = long delta) → dealer hedges by SELLING ES → sell-side aggressor volume
- Customer sells puts (vol selling) → dealer buys puts (long puts = short delta) → dealer hedges by BUYING ES → buy-side aggressor volume
- So in ES futures: persistent buy aggression at a level = possible vol sellers providing support
- Persistent sell aggression at a level = possible vol buyers creating resistance
- **Caveat:** mixed with directional futures trading — not pure vol signal

### Related Insights from Other Traders

**Ingram (Oct 23, 2025):** "After 2pm charm, before gamma and vanna. Gamma for low vol days, vanna for event catalysts."

**Wizard of Ops (Oct 1, 2025):** "We just have started to see the vol sellers more clearly" — referring to improved Volland data categorization.

**l0rd.helmet (Oct 1, 2025):** "LIS bounce is a fantastic play. I've made lots of money that way. Buy butterflies at the target on a LIS test. Big RR."

**Johannes (Nov 15, 2025):** "When [cumulative charm] is negative dealers will need to buy as time passes, when it's positive they need to sell" — this is what creates the hedging flows that vol detection catches.

**DK5000 (Nov 12, 2025):** "The behavior of vanna is dependent upon spot-vol behavior. When spot vol acts correctly (negative correlation), vanna acts OPPOSITE of volatility. When spot vol is off, Vanna acts WEIRD or SPOT UP-VOL UP."

### Execution Scale
Apollo: "I can manually execute 390 trades a day off 1 minute candles and that's not considered even medium frequency" — ultra-high-frequency manual trader.

### Tools Mentioned for Flow Detection
- **Time & Sales** (Sierra Chart, NinjaTrader) — Apollo's #1
- **LOB / DOM** — order book depth, bid/ask stacking
- **Anchored VWAP** — "I would put that at the top of the moving average list. I like using them where key orderflow events shift"
- **Volume Leaders** — dark pool block trades (15-min delay, limited for intraday)
- **Financial Juice + Tickstrike** — news/flow alerts ("geiger counter" for prints)
- **Footprint charts** — used by Hunter Edmonds (Volland + footprint combo)

### What We Built to Capture This

**Sierra Study:** `C:\SierraChart\ACS_Source\VolDetector.cpp`
- Delta bars (buyer/seller aggression per bar)
- CVD line (cumulative delta)
- Absorption detection (high volume + price holds = vol absorbed)
- CVD-price divergence arrows
- Bar color companion study

### Future Plan: Backtest & Compare
1. **Build DLL on Monday** when market opens
2. **Add to ES chart** alongside our existing orderflow setup
3. **Observe for 1 week:** Do absorption signals align with our ES Absorption / SB Absorption automated signals?
4. **Compare timing:** Does Sierra Vol Detector spot setups BEFORE our automated system fires?
5. **Quantify:** Track absorption diamonds vs our setup_log — how many overlap? How many unique?
6. **If valuable:** Consider adding Rithmic Level 2 data (market depth) for LOB shift detection
7. **If very valuable:** Automate absorption detection from Rithmic tick data in our system (we already have aggressor data)

### What We Tested and Found (from this session)
- **Fixed strike IV direction as a FILTER:** REJECTED for our contrarian system (reversed — rising IV = better for our longs). But Apollo trades momentum/trend-following, so it works for HIS style.
- **IV Momentum as standalone strategy:** Marginal (+13 pts/month). Not worth automating beyond LOG-ONLY.
- **The visual approach (Sierra) is the right path:** Apollo's method is discretionary tape reading + IV monitoring. Can't be fully automated, but Sierra provides the visualization layer.
