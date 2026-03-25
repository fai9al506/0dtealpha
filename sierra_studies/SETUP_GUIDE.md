# Apollo Momentum — Sierra Chart Study Setup Guide

## Overview

Two-part system for visualizing Apollo's momentum strategy in Sierra Chart:

| Approach | What it does | Complexity |
|----------|-------------|------------|
| **#4 — ACSIL Study** | 15-min spot momentum overlay + bar coloring. Trader manually checks IV on Volland. | Simple (C++ compile in Sierra) |
| **#5 — Python IV Bridge** | Reads IV from our PostgreSQL DB, computes IV confirmation signals, writes to file that the ACSIL study reads. | Medium (Python script + file I/O) |

You can run Approach #4 standalone (80% of the value) or add Approach #5 for full IV-confirmed signals.

---

## Approach #4: ACSIL Momentum Study (Standalone)

### What You Get

1. **Apollo Momentum** (Region 1 — below price chart):
   - Blue line: 15-min rate of change in points
   - Green up-arrows: momentum >= +5 pts (bullish)
   - Red down-arrows: momentum <= -5 pts (bearish)
   - Gray zero line

2. **Apollo Momentum Bar Color** (Region 0 — main price chart):
   - Green bars when 15-min momentum >= +5 pts
   - Red bars when 15-min momentum <= -5 pts
   - Normal bars when below threshold

### Installation

1. **Copy the source file:**
   ```
   Copy: sierra_studies\ApolloMomentum.cpp
   To:   C:\SierraChart\ACS_Source\ApolloMomentum.cpp
   ```

2. **Build the DLL in Sierra Chart:**
   - Open Sierra Chart
   - Menu: `Analysis` > `Build Custom Studies DLL`
   - Select `ApolloMomentum.cpp` from the list
   - Click `Build`
   - Should show "Build succeeded" in the output window
   - If errors: check Sierra Chart version >= 2500

3. **Add the momentum study to your chart:**
   - Open an ES/MES or SPX chart (any timeframe — 1min, 5min, tick, range bars all work)
   - Menu: `Analysis` > `Studies` (or press `F6`)
   - In the "Available Studies" list, find `Apollo Momentum`
   - Click `Add` to move it to "Studies to Graph"
   - Click `Settings` to configure:

   | Setting | Default | Recommended for ES 1-min | Recommended for ES 5-min |
   |---------|---------|--------------------------|--------------------------|
   | Lookback Minutes | 15 | 15 | 15 |
   | Spot Move Threshold | 5.0 | 5.0 | 5.0 |
   | Show ROC Line | Yes | Yes | Yes |
   | IV Bridge File Path | (empty) | (empty for standalone) | (empty for standalone) |

   - Click `OK`

4. **Add the bar coloring study (optional):**
   - Same Studies dialog
   - Find `Apollo Momentum Bar Color`
   - Add it and configure with same Lookback/Threshold values
   - Click `OK`

### Chart Timeframe Notes

The study auto-converts "Lookback Minutes" to bars:
- **1-min chart**: 15 minutes = 15 bars lookback
- **5-min chart**: 15 minutes = 3 bars lookback
- **30-sec chart**: 15 minutes = 30 bars lookback
- **Range/tick charts**: "Lookback Minutes" is used as bar count directly (adjust accordingly)

For range bars (e.g., 10-pt range bars on ES), set Lookback Minutes to the approximate number of bars you want to look back (typically 8-15 for a 15-minute equivalent window).

### How to Use (Standalone)

1. Watch the ROC line — when it crosses above +5 (green arrow appears), momentum is bullish
2. **Manually check Volland** — look at put IV at ATM/-5/-10 strikes:
   - If put IV dropped while spot rose: **CONFIRMED LONG** (vol sellers aligned)
   - If put IV held/rose while spot rose: momentum WITHOUT vol confirmation (weaker)
3. Watch for green arrow + red arrow transitions — these are the inflection points
4. Bar coloring gives instant visual: all-green bars = sustained momentum

---

## Approach #5: Python IV Bridge (Full Automation)

### What You Get

- Python script polls our Railway PostgreSQL database every 30 seconds
- Computes IV changes at fixed ATM/ATM-5/ATM-10 put strikes over 15-min window
- Writes `apollo_signal.txt` that the ACSIL study reads
- When BOTH momentum AND IV confirm: bright green/pink arrows appear on chart (brighter than momentum-only arrows)

### Prerequisites

```bash
pip install pandas sqlalchemy psycopg2-binary
```

### Configuration

You need the `DATABASE_URL` from Railway. Set it as an environment variable:

```bash
# Windows (PowerShell)
$env:DATABASE_URL = "postgresql://user:pass@host:port/dbname"

# Windows (cmd)
set DATABASE_URL=postgresql://user:pass@host:port/dbname

# Linux/Mac
export DATABASE_URL="postgresql://user:pass@host:port/dbname"
```

### Running the Bridge

```bash
# Basic usage (polls every 30 seconds, writes to default Sierra Chart Data folder)
python sierra_studies/apollo_iv_bridge.py

# Custom output directory
python sierra_studies/apollo_iv_bridge.py --output-dir "C:\SierraChart\Data"

# Custom thresholds
python sierra_studies/apollo_iv_bridge.py --spot-threshold 3.0 --iv-threshold 0.03

# Test mode (run once and exit)
python sierra_studies/apollo_iv_bridge.py --once

# Full customization
python sierra_studies/apollo_iv_bridge.py \
    --db-url "postgresql://..." \
    --output-dir "C:\SierraChart\Data" \
    --interval 30 \
    --lookback 15 \
    --spot-threshold 5.0 \
    --iv-threshold 0.05
```

### Connecting to Sierra Chart ACSIL Study

Once the bridge is running and producing `apollo_signal.txt`:

1. Open Sierra Chart Studies dialog (`F6`)
2. Select the `Apollo Momentum` study
3. Click `Settings`
4. Set **IV Bridge File Path** to the full path of the signal file:
   ```
   C:\Users\YourName\Documents\SierraChart\Data\apollo_signal.txt
   ```
5. Set **IV Change Threshold** to match your Python bridge setting (default 0.05)
6. Click `OK`

Now you will see:
- **Regular green/red arrows**: Momentum-only signals (spot moved >= threshold)
- **Bright spring-green / hot-pink arrows**: BOTH momentum AND IV confirmed (full Apollo signal)

### Output Files

| File | Description | Used By |
|------|-------------|---------|
| `apollo_signal.txt` | Single-line signal for Sierra Chart | ACSIL study reads this |
| `apollo_iv_log.csv` | Full history with IV details | Your analysis / review |

**Signal file format:** `direction,iv_change,spot_price`
- `1,0.0700,5890.50` = LONG signal (put IV dropped 0.07, spot at 5890.50)
- `-1,0.0900,5885.25` = SHORT signal (put IV rose 0.09, spot at 5885.25)
- `0,0.0200,5888.00` = No signal (conditions not met)

**Log file columns:**
```
timestamp, spot, spot_15m_ago, spot_change,
atm_strike, iv_atm, iv_atm5, iv_atm10,
iv_atm_15m, iv_atm5_15m, iv_atm10_15m,
iv_change_avg, direction, signal
```

---

## Research Notes: Alternative Approaches

### Approach #1 — Sierra Chart HTTP to Railway API

Sierra Chart's ACSIL does not have native HTTP client functions. You would need to:
- Write a custom DLL using WinHTTP/libcurl
- Or use the Spreadsheet study with an external Excel sheet that fetches HTTP

**Verdict:** Overly complex. The file-based bridge (Approach #5) is simpler and equally fast.

### Approach #2 — Sierra Chart Native Options IV

Sierra Chart CAN subscribe to individual option symbols via Rithmic and display IV. However:
- You would need to manually add each SPXW option symbol
- SPX options are cash-settled, only available on CBOE — Rithmic supports this but setup is manual
- No built-in "IV change over N minutes" study exists in Sierra
- You would need to write ACSIL code to track IV history anyway

**Verdict:** Possible but requires manual symbol management daily (strikes change with spot). Our DB already has this data — use the bridge.

### Approach #3 — Sierra Chart DTC Protocol for Options

The DTC protocol (which our `sierra_bridge.py` already uses for MES trading) supports `MarketDataRequest` which can subscribe to option symbols. However:
- Sierra's DTC server passes through whatever the connected data feed provides
- Rithmic does carry CBOE SPX options data
- You would need to build a separate DTC client that subscribes to specific option symbols and computes IV internally

**Verdict:** Most complex approach. Only worth it if you want real-time tick-level IV changes (sub-second). For 30-second polling, the Python bridge is vastly simpler.

### Recommendation

**Start with Approach #4 alone** (momentum study + manual Volland check). This gives you 80% of the signal value with zero external dependencies.

**Add Approach #5** when you want hands-free IV confirmation. Run the Python bridge on your trading PC (same machine that runs `eval_trader.py`) — it will poll the Railway DB just like the eval trader does.

---

## Troubleshooting

### Study not appearing in Sierra Chart
- Make sure the `.cpp` file is in `C:\SierraChart\ACS_Source\`
- Rebuild: `Analysis` > `Build Custom Studies DLL`
- Check the build output for errors

### Arrows not showing
- The study defaults to Region 1 (below price chart). Make sure you can see Region 1.
- Check the Spot Move Threshold — on a low-volatility day, 5.0 pts might rarely trigger. Try lowering to 3.0.
- For range/tick charts, adjust Lookback Minutes (it becomes bar count).

### IV Bridge file not reading
- Verify the file path is exact (no typos, use backslashes on Windows)
- Check that `apollo_signal.txt` exists and has content
- The ACSIL study re-reads the file every 10 bars (not every single bar) to avoid I/O overhead

### Python bridge errors
- `DB query failed`: Check DATABASE_URL is correct and Railway DB is accessible from your PC
- `No lookback data`: The chain_snapshots table needs at least 15 minutes of data for comparison
- `No valid IV data at reference strikes`: The reference strikes might be outside the saved chain range (±125 pts from spot)
