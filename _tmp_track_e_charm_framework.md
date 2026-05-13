# Track E — Charm Interpretation Framework
*Synthesized from Volland White Paper, User Guide, and DC Discord (Apollo, Wizard, Dark Matter, Simple Jack, Big Bill, LordHelmet).*

---

## 1. Mathematical Definition

- **Charm** = ∂Δ/∂t (delta's sensitivity to passage of time).
- Volland convention: charm calculated as **+1 day passing** (NOT days remaining).
  - **Negative charm = bullish** (dealers must buy underlying as time passes)
  - **Positive charm = bearish** (dealers must sell underlying as time passes)
- Time horizon: effective 1-2 days. **Charm is most potent 0-1 days from expiration** — drives 0DTE.
- White Paper: "0DTE option hedging primarily uses charm as its driving greek."
- Dark Matter scanning protocol: **charm is always 0DTE** (vanna/gamma checked across multiple expirations, charm only 0DTE).

## 2. Per-Strike Sign Convention (from User Guide table p.13)

| Charm sign × position | Implication |
|---|---|
| `+` above spot | **Bearish** — dealers will sell as time passes |
| `+` below spot | **Bearish** — dealers will sell as time passes |
| `-` above spot | **Bullish** — dealers will buy as time passes |
| `-` below spot | **Bullish** — dealers will buy as time passes |

**Critical:** The sign matters more than the position. Aggregate net charm = directional bias. Per-strike sign flips happen at "balance" zones — those are charm walls.

## 3. How Dealers Position by Charm

- Dealers hedge to deltas, NOT to PnL. Vega/theta don't drive 0DTE — charm and vanna do.
- Dealers warehouse short vol intraday; dynamically hedge long vol.
- Charm/vanna BALANCE (equal aggregate offsetting signs) = less end-of-day hedging needed.
- White Paper Principle 3.a (the existing-position hedging formula):
  ```
  (Gamma Exposure × Underlying Change) + (Vanna × IV Change) + (Charm × Days Passed) = Total Delta Notional Hedged
  ```

## 4. The Four Paradigms (Charm Profiles)

| Paradigm | Per-strike charm profile | Dealer Position | Trade Bias |
|---|---|---|---|
| **BofA** | Negative below spot, Positive above spot | Short strangles, defend the range | Range-bound. LIS = where dealers flip to gamma hedge. Trade fades to LIS, breakouts ABOVE call line gain dealer assist. |
| **GEX** | Negative on BOTH sides | Hedged short on both sides, must buy up | Bullish trend. Target = OTM call strike where charm shrinks to flip. LIS below spot. |
| **Anti-GEX (AG)** | Positive on BOTH sides | Hedged long on both sides, must sell down | Bearish trend. Target = OTM put strike where charm shrinks/flips. LIS above spot. |
| **Sidial** | Charm flips at single point | Long both sides | Mean-reverts to neutral. Rarest. Only fires near GEX/AG targets. |
| **Messy** | No uniform pattern | Mixed | Trade strike-by-strike charm to find S/R. Hardest paradigm. |

## 5. Intraday Charm Mechanics — Apollo / Wizard / Dark Matter

### Skew filter (Apollo's #1 rule)
> "Bearish charm won't effect much with elevated skew. IF skew comes down then you can realize the bearishness."

**Rule:** Charm needs **skew compression** to manifest. Elevated skew BLOCKS charm effects.

### Time-of-day decay (User Guide & Dark Matter)
> "Before 2:00 PM ET, delta and gamma have the largest effect on 0DTE. Afterward, charm and vanna have a larger effect."
> "Post 2 PM is dealer o'clock where those charm moves can come in."

**This contradicts our brief's H4 — charm gets MORE potent toward EOD, not less.** The exposure value shrinks (because there's less time left), but its hedging IMPACT becomes dominant (each remaining hour matters exponentially more).

### Charm flip = trend trigger
> "If an outside party trades strongly in one direction or the other, the charm bars will flip their sign and price can begin to trend."

The **flip-zone strikes** are critical — once charm flips at a strike, dealers reverse hedging direction.

### Charm-Vanna interaction
- Charm and vanna are **opposite signs on the same strike** (positive vanna at strike X = negative charm at strike X).
- **Cooperate** when IV is decreasing (vol falling → both push same direction).
- **Conflict** when IV is rising.
- Apollo: "Vanna props delta up, charm beats delta down."

### Charm and Premium
- Per Big Bill: "Make sure Charm, Vanna (and sometimes Gamma) line up with your thesis."
- Per Apollo: BofA and Sidial paradigms are "premium-skew filters" — dealers profit from premium decay only if charm holds the range.

## 6. Practical Charm Reading (Simple Jack)

- "Above a level = charm support; below it = negative charm, negative gamma in size."
- "Charm weakens OTM deltas and strengthens ITM deltas."
- "Put charm and ITM short call charm should keep us well supported unless we get a bad headline."

## 7. Concepts to Test in Track E

### Concept A — Charm Wall as S/R
**A charm wall** = a strike near spot where charm changes sign by significant magnitude. The wall acts as resistance (above spot, charm flips from `-` to `+`) or support (below spot, charm flips from `+` to `-`).

- **Short setups** should target a charm wall ABOVE spot within 5-15 pts.
- **Long setups** should target a charm wall BELOW spot within 5-15 pts.
- Walls beyond 30 pts are too far to provide intraday edge in 0DTE timeframe.

### Concept B — Charm Symmetry
- **Symmetric charm** (|sum_above| ≈ |sum_below|) = BofA-like, range-bound. Scalps win, trends fail.
- **Asymmetric charm** = directional, trend continuation likely.

### Concept C — Charm Magnitude vs LIS
- Per `reference_volland_lis.md`: LIS is Volland-proprietary.
- BUT — Apollo's rule: charm validity depends on magnitude at the LIS strike.
- Hypothesis: a "weak LIS" (low charm magnitude at LIS strike) is breakable; "strong LIS" (high charm magnitude) holds.

### Concept D — Charm Gradient
- Steep charm slope across strikes near spot = strong directional pull.
- Flat charm = no immediate directional force from charm hedging.

### Concept E — Skew Filter Already in V14
- Our V13/V14 already filters on `align`, `VIX`, paradigm. Skew compression not explicitly tested. But "Skew Charm" SETUP is built on charm direction + skew, so partially captured.

## 8. Cross-Reference to Existing System

- **Skew Charm** setup: already direction-aware charm signal (charm shift + skew direction).
- **DD Exhaustion**: uses DD (which is influenced by charm/vanna/gamma combined).
- **Greek alignment (`greek_alignment`)**: already incorporates a charm component (per `_compute_greek_alignment`).
- **Charm S/R limit entry** (real_trader, currently disabled): finds top positive charm strike above spot (resistance) and most-negative below (support) — this IS the charm wall concept, but only for short-side entries. We've seen it provides edge at 3-15pt distance (per `project_charm_sr_analysis.md`).

**This means our system already partly captures charm, but only via aggregate signals and the per-strike S/R wall for shorts.** What we have NOT done:
- Per-strike charm features for ALL setups (not just shorts)
- Charm symmetry ratio
- Charm gradient
- Charm wall distance for longs
- Charm magnitude near LIS as a validity filter
- Charm/vanna alignment (since they're opposite-signed, checking their joint sign agreement is a quality filter)
