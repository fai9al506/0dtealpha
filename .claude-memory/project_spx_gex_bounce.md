---
name: SPX 0DTE GEX Bounce Study
description: SPX GEX support bounce backtest results — 70% T1 recovery on 10pt dips, options-based strategy
type: project
---

## SPX 0DTE GEX Support Bounce — Research Results (Mar 23, 2026)

### Setup
- Download 10:00 AM SPXW 0DTE chains from ThetaData (Value plan, $40/mo)
- Compute GEX per strike, identify strongest -GEX (support) and +GEX (magnet)
- Track when SPX dips below -GEX during the day

### Key Finding: 10pt Dip Recovery
**When spot opens above -GEX and dips 10+ pts below during the day:**
- **68 events in 12 months (5.7/month, 1.3/week)**
- **48/68 recovered above -GEX = 70.6% T1 hit rate**
- **Median recovery time: 35 min from hitting -10**
- 44% recover within 30 min, 70% within 1 hour

### Failure Pattern
- 20/68 did NOT recover (29.4%)
- **12 of 20 failures had max dip at 14:30-15:55** (late-day crash into close)
- Only 3 failures had max dip before 12:00
- **How to apply:** Don't enter after 13:00 ET. Morning dips recover much better.

### Trade Plan (Options)
- Entry: Buy 0DTE call when SPX dips 10+ pts below strongest -GEX
- T1: Recovery to -GEX level (10+ pts move in call)
- T2: Reach +GEX magnet (25% hit rate, bigger move)
- Time window: 10:00-13:00 ET only
- Max loss: option premium (no stop needed)
- Expected: ~5/week across SPX+SPY+QQQ+IWM, 70%+ WR

### Data Infrastructure
- `spx_gex_downloader.py` — downloads 10AM chains from ThetaData for SPX/SPY/QQQ/IWM
- `C:\Users\Faisa\stock_gex_data\spx\` — 243 days of chain data + SPY 5-min bars
- Analysis scripts: `tmp_spx_dip10.py` (main study), `tmp_spx_filters.py`, `tmp_spx_raw_study.py`
- SPY/QQQ/IWM download pending (terminal keeps crashing, user needs to run manually)

### Download Status (as of Mar 23)
- SPX: DONE (243 days chains + intraday)
- SPY: 47/~250 (partial)
- QQQ: Not started
- IWM: Not started

### Next Steps
1. Complete SPY/QQQ/IWM downloads
2. Run same dip study on all 4 symbols
3. Test with 10:00-13:00 time filter
4. Calculate options P&L (not just pts — actual call premium gain)
5. Consider adding Volland data overlay for Feb-Mar 2026 period
