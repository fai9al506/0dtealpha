# Volland Scraper V2 - Implementation Notes

**Created:** 2026-02-10
**Status:** Ready for market hours testing
**Purpose:** Capture all Greeks (Charm, Vanna, Gamma) + Statistics + Spot-Vol Beta from single workspace

---

## DETAILED COMPARISON: Current vs V2

### Current volland_worker.py

| Aspect | Current Implementation |
|--------|------------------------|
| **Pages visited** | 2 per cycle (STATS_URL + VOLLAND_URL) |
| **Data captured** | Charm 0DTE only |
| **Greek field** | Hardcoded `greek="charm"` |
| **expiration_option** | NOT USED (always NULL) |
| **Statistics** | DOM text parsing (fragile) |
| **Rows per cycle** | ~230 (1 chart × 230 strikes) |
| **Env vars** | VOLLAND_URL, VOLLAND_STATS_URL |

### V2 Approach (0dtealpha-all workspace)

| Aspect | V2 Implementation |
|--------|-------------------|
| **Pages visited** | 1 per cycle (all-in-one workspace) |
| **Data captured** | Charm, Vanna (4), Gamma (4), Delta Decay |
| **Greek field** | Dynamic: 'charm', 'vanna', 'gamma' |
| **expiration_option** | '0dte', 'weekly', 'monthly', 'all' |
| **Statistics** | API endpoint (reliable) |
| **Rows per cycle** | ~2,300+ (10 charts × 230 strikes avg) |
| **Env vars** | Only needs VOLLAND_URL (new workspace) |

---

## POTENTIAL ISSUES & MITIGATIONS

### Issue 1: Database Write Volume (10x increase)
**Risk:** LOW (RESOLVED)
**Current:** ~230 rows/minute → ~13,800 rows/hour
**V2:** ~2,300 rows/2 minutes → ~69,000 rows/hour

**Resolution:**
- Use 120-second interval (matches Volland's 2-minute update frequency)
- Current DB: 1.56 GB after 21 days
- Projected 5-year cost: ~$15/month (acceptable)
- No data filtering needed - storage is affordable

### Issue 2: main.py Hardcodes `greek='charm'`
**Risk:** LOW (backward compatible)
**Location:** `app/main.py` lines 684, 691, 696, 714

```python
WHERE greek = 'charm'  # Hardcoded in multiple queries
```

**Mitigation:**
- Existing Charm queries will continue to work
- New Vanna/Gamma data will be stored but not displayed
- Update main.py later to add new charts for Vanna/Gamma

### Issue 3: `expiration_option` Column Not Used Currently
**Risk:** LOW
**Current:** Column exists but is always NULL
**V2:** Will populate with '0dte', 'weekly', 'monthly', 'all'

**Mitigation:**
- No schema change needed
- Existing queries don't filter by this column
- V2 queries can use it for filtering

### Issue 4: Statistics Parsing Method Change
**Risk:** MEDIUM
**Current:** DOM text extraction (fragile, depends on page layout)
**V2:** API endpoint `/api/v1/data/paradigms/0dte` (reliable)

**Mitigation:**
- V2 method is actually MORE reliable
- API returns structured JSON
- No risk of breaking due to UI changes

### Issue 5: Railway Environment Variables
**Risk:** LOW
**Current env vars:**
- `VOLLAND_URL` - current Charm workspace
- `VOLLAND_STATS_URL` - statistics workspace

**V2 needs:**
- `VOLLAND_URL` - change to new 0dtealpha-all workspace
- `VOLLAND_STATS_URL` - can be removed (stats included in workspace)

**Mitigation:**
- Just update VOLLAND_URL env var in Railway
- Keep VOLLAND_STATS_URL for fallback (optional)

### Issue 6: Greek Identification Reliability
**Risk:** LOW-MEDIUM
**Method:** Value magnitude sorting (Vanna > Gamma for same expiration)

**Tested scenarios:**
- End of day: Works correctly
- Market hours: Need to verify

**Mitigation:**
- Test during market hours before deployment
- Log Greek assignments for monitoring
- Fallback: All data still captured, just labeling might be wrong

---

## RAILWAY DEPLOYMENT NOTES

### Current Setup
```
Procfile: web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
Dockerfile: Uses playwright/python image
```

**volland_worker.py runs as separate Railway service** (not in Procfile)

### V2 Deployment Steps
1. Create `volland_worker_v2.py` (don't modify original)
2. Deploy V2 as NEW service on Railway (parallel testing)
3. Set env vars for V2 service:
   - `DATABASE_URL` - same as current
   - `VOLLAND_EMAIL` - same as current
   - `VOLLAND_PASSWORD` - same as current
   - `VOLLAND_URL` - NEW workspace URL
4. Monitor both services
5. When V2 proven stable, stop V1 service

### Environment Variables Reference
```bash
# Current (V1)
VOLLAND_URL=https://vol.land/app/workspace/[CHARM_WORKSPACE_ID]
VOLLAND_STATS_URL=https://vol.land/app/workspace/696fcf236547cfa9b4d09267

# V2 (new)
VOLLAND_URL=https://vol.land/app/workspace/698a5560ea3d7b5155f88e67
# VOLLAND_STATS_URL not needed (included in workspace)
```

---

## DATABASE ANALYSIS

### Table: volland_exposure_points
```sql
CREATE TABLE IF NOT EXISTS volland_exposure_points (
  id BIGSERIAL PRIMARY KEY,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  ticker VARCHAR(20),           -- 'SPX'
  greek VARCHAR(20),            -- 'charm', 'vanna', 'gamma'
  expiration_option VARCHAR(30),-- NULL (current) → '0dte','weekly','monthly','all' (V2)
  strike NUMERIC,
  value NUMERIC,
  current_price NUMERIC,
  last_modified TIMESTAMPTZ,
  source_url TEXT
);

-- Indexes exist:
CREATE INDEX idx_volland_exposure_points_ts ON volland_exposure_points(ts_utc DESC);
CREATE INDEX idx_volland_exposure_points_greek ON volland_exposure_points(greek);
```

**V2 Recommendation:** Add index for expiration_option
```sql
CREATE INDEX idx_volland_exposure_points_exp ON volland_exposure_points(expiration_option);
```

### Table: volland_snapshots
```sql
CREATE TABLE IF NOT EXISTS volland_snapshots (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload JSONB NOT NULL
);
```

**Payload structure (V2 enhanced):**
```json
{
  "ts_utc": "2026-02-10T12:00:00Z",
  "page_url": "https://vol.land/app/workspace/...",
  "current_price": 6965.44,
  "statistics": {
    "paradigm": "BOFA-PURE",
    "target": 7000,
    "lis": [6937, 6966],
    "totalZeroDteOptionVolume": 445918,
    "aggregatedCharm": -4171250,
    "aggregatedDeltaDecay": 0
  },
  "spot_vol_beta": {
    "correlation": 0.06,
    "vixEvents": []
  },
  "exposure_summary": {
    "charm_0dte": 230,
    "vanna_0dte": 230,
    "vanna_weekly": 275,
    "vanna_monthly": 519,
    "vanna_all": 702,
    "gamma_0dte": 230,
    "gamma_weekly": 275,
    "gamma_monthly": 519,
    "gamma_all": 702
  }
}
```

---

## BACKWARD COMPATIBILITY CHECKLIST

| Component | Compatible? | Notes |
|-----------|-------------|-------|
| volland_exposure_points table | YES | Schema unchanged |
| volland_snapshots table | YES | JSONB is flexible |
| main.py Charm queries | YES | `greek='charm'` still works |
| main.py Statistics queries | YES | payload structure same |
| Existing Charm history | PRESERVED | No data modified |
| API endpoints | YES | No changes needed |
| Dashboard | YES | Shows Charm, ignores new data |

---

## Overview

We successfully tested capturing data from the `0dtealpha-all` workspace which contains all widgets in one page. This replaces the current approach of visiting 2 separate pages (Stats + Charm).

### Current vs New Approach

| Aspect | Current (volland_worker.py) | New (V2) |
|--------|----------------------------|----------|
| Pages per cycle | 2 (Stats + Charm) | 1 (all-in-one) |
| Greeks captured | Charm only | Charm, Vanna, Gamma |
| Expiration types | 0DTE only | 0DTE, Weekly, Monthly, All |
| Statistics | Separate page | Same page |
| Spot-Vol Beta | Not captured | Captured |
| Delta Decay | Not captured | Captured |
| Traffic | More requests | Fewer requests |

---

## Workspace Configuration

**Workspace:** `0dtealpha-all`
**URL:** `https://vol.land/app/workspace/698a5560ea3d7b5155f88e67`
**ID:** `698a5560ea3d7b5155f88e67`

### Widgets (in order):

| # | Widget | Greek | Expiration | Ticker |
|---|--------|-------|------------|--------|
| 1 | Chart | CHARM | 0DTE (CUSTOM) | SPX |
| 2 | Chart | VANNA | 0DTE (CUSTOM) | SPX |
| 3 | Chart | VANNA | Weekly (THIS_WEEK) | SPX |
| 4 | Chart | VANNA | Monthly (THIRTY_NEXT_DAYS) | SPX |
| 5 | Chart | VANNA | All (ALL) | SPX |
| 6 | Spot-Vol Beta | - | - | SPX |
| 7 | Chart | GAMMA | 0DTE (CUSTOM) | SPX |
| 8 | Chart | GAMMA | Weekly (THIS_WEEK) | SPX |
| 9 | Chart | GAMMA | Monthly (THIRTY_NEXT_DAYS) | SPX |
| 10 | Chart | GAMMA | All (ALL) | SPX |
| 11 | Delta Decay | - | 0DTE | SPX |
| 12 | Statistics | - | - | SPX |

---

## API Endpoints Captured

### 1. Exposure Data (Charts)
**URL:** `https://api.vol.land/api/v1/data/exposure`
**Method:** POST
**Response Keys:** `items`, `data`, `lastModified`, `currentPrice`, `expirations`

Each chart makes a separate call. Response contains:
- `items`: Array of `{x: strike, y: value}` points
- `currentPrice`: Current SPX price
- `expirations`: Array of expiration dates

### 2. Statistics
**URL:** `https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX`
**Method:** GET
**Response Keys:**
- `paradigm`: e.g., "BOFA-PURE", "GOLDMAN", etc.
- `target`: Target price (null when no target)
- `lis`: Lines in Sand array, e.g., [6937, 6966]
- `totalZeroDteOptionVolume`: Total 0DTE option volume
- `aggregatedCharm`: Aggregated charm value
- `aggregatedDeltaDecay`: Aggregated delta decay value

### 3. Spot-Vol Beta
**URL:** `https://api.vol.land/api/v1/data/volhacks/spot-vol-beta?ticker=SPX`
**Method:** GET
**Response Keys:**
- `correlation`: Correlation value (e.g., 0.06)
- `vixEvents`: Array of VIX events (empty when no events)

When there IS a vol event, additional fields appear:
- Trigger Date
- Target Price
- Deadline

---

## Greek Identification Method

The API response does NOT include a `greek` field. We identify Greeks by:

### 1. Expiration Count
| Count | Type |
|-------|------|
| 1 | 0DTE |
| 4-5 | Weekly |
| 20-25 | Monthly |
| 50+ | All |
| 0 | Empty (Delta Decay end-of-day) |

### 2. Value Magnitude (for same expiration type)
For charts with the same expiration count, **Vanna has larger absolute values than Gamma**.

**Sorting Logic:**
```python
# Sort by absolute max value - larger = Vanna, smaller = Gamma
sorted_by_magnitude = sorted(exposures, key=lambda x: -x['max_abs_value'])
```

### 3. 0DTE Identification
For 0DTE charts (3 expected: Charm, Vanna, Gamma):
- Sort by absolute max value
- Largest = **Charm**
- Second = **Vanna**
- Third = **Gamma**
- Any with value 0 = **Empty/End-of-day**

---

## Test Results (2026-02-10, End of Day)

### Exposure Data Captured:

| Greek | Expiration | Strikes | Top Strike | Top Value |
|-------|------------|---------|------------|-----------|
| CHARM | 0DTE | 230 | 6970 | -46,590,070 |
| VANNA | 0DTE | 230 | 6970 | 33,446,900 |
| VANNA | Weekly | 275 | 7080 | -86,208,479 |
| VANNA | Monthly | 519 | 7000 | -355,682,560 |
| VANNA | All | 702 | 6000 | 1,318,853,499 |
| GAMMA | 0DTE | 230 | 6965 | 21,375,597 |
| GAMMA | Weekly | 275 | 6970 | 32,094,604 |
| GAMMA | Monthly | 519 | 7000 | -148,342,946 |
| GAMMA | All | 702 | 6900 | 50,279,176 |
| Delta Decay | 0DTE | 0 | - | (empty) |

### Statistics (end of day - some fields empty):
- Paradigm: (empty)
- Target: (none)
- Lines in Sand: (empty)
- 0DTE Opt Volume: 445,918
- Aggregated Charm: -4,171,250
- Delta Decay: 0

### Spot-Vol Beta:
- Correlation: 0.06
- VIX Events: (none)

---

## Database Schema

### Existing Table: `volland_exposure_points`
```sql
CREATE TABLE IF NOT EXISTS volland_exposure_points (
  id BIGSERIAL PRIMARY KEY,
  ts_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  ticker VARCHAR(20),
  greek VARCHAR(20),           -- 'charm', 'vanna', 'gamma'
  expiration_option VARCHAR(30), -- '0dte', 'weekly', 'monthly', 'all'
  strike NUMERIC,
  value NUMERIC,
  current_price NUMERIC,
  last_modified TIMESTAMPTZ,
  source_url TEXT
);
```

**Note:** `greek` and `expiration_option` fields already exist - perfect for new data.

### Existing Table: `volland_snapshots`
```sql
CREATE TABLE IF NOT EXISTS volland_snapshots (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload JSONB NOT NULL
);
```

The payload can store Statistics, Spot-Vol Beta, and Delta Decay data.

---

## Implementation Plan

### Phase 1: Market Hours Testing
1. Run `test_scraper.py` during market hours (9:30 AM - 4:00 PM ET)
2. Verify all data is captured correctly
3. Compare Statistics data (Paradigm, Target, Lines in Sand should have values)
4. Verify Delta Decay has data during market hours
5. Check Spot-Vol Beta during a vol event (if any)

### Phase 2: Create volland_worker_v2.py
1. Copy current `volland_worker.py` structure
2. Update to use `0dtealpha-all` workspace URL
3. Add Greek identification logic
4. Save all Greeks to `volland_exposure_points` with proper labels
5. Save Statistics to `volland_snapshots` payload
6. Add Spot-Vol Beta to payload
7. Add Delta Decay to payload (if needed)

### Phase 3: Parallel Testing
1. Run both workers simultaneously
2. Compare Charm data between V1 and V2
3. Verify Statistics data matches
4. Monitor for any issues

### Phase 4: Switch to V2
1. Stop V1 worker
2. Deploy V2 worker
3. Keep V1 as backup

---

## Files in volland_test/

| File | Purpose |
|------|---------|
| `test_scraper.py` | Main test script with DOM detection |
| `analyze_captures.py` | Analyze captured JSON files |
| `check_db_size.py` | Database size and cost report |
| `run_test.bat` | Quick run script (double-click) |
| `README.md` | Basic setup instructions |
| `IMPLEMENTATION_NOTES.md` | This file - detailed implementation notes |
| `captures/` | Captured JSON files from test runs |

---

## Credentials (for testing only)

Hardcoded in `test_scraper.py`:
- Email: `faisal.a.d@msn.com`
- Password: `Fad2024506!`
- Workspace URL: `https://vol.land/app/workspace/698a5560ea3d7b5155f88e67`

**Note:** These are in an untracked folder, not committed to git.

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Greek misidentification | Value magnitude sorting is reliable; test during market hours |
| Breaking existing data | Create V2 file, don't modify V1 |
| Network response order | Use expiration count + value magnitude, not arrival order |
| Missing data | End-of-day data is sparse; market hours will have full data |

---

## Next Steps

1. **[ ] Test during market hours** (9:30 AM - 4:00 PM ET)
2. **[ ] Verify all data fields populated**
3. **[ ] Create volland_worker_v2.py**
4. **[ ] Run parallel testing**
5. **[ ] Update main.py to use new data (optional - add Vanna/Gamma charts)**
6. **[ ] Deploy to production**

---

## Current Database Status (2026-02-10)

### Size Report

| Metric | Value |
|--------|-------|
| **Total Size** | 1.56 GB |
| **Monthly Cost** | $0.39 |

### Table Breakdown

| Table | Size | Rows |
|-------|------|------|
| volland_snapshots | 933 MB | 54,425 |
| volland_exposure_points | 574 MB | 2,753,932 |
| chain_snapshots | 79 MB | 23,845 |
| playback_snapshots | 2.4 MB | - |
| Other tables | < 1 MB | - |

### Current Data Analysis

- **Greek field:** Only `charm` populated (V1 behavior)
- **expiration_option field:** All `NULL` (V2 will populate)
- **Data span:** 21 days (2026-01-19 to 2026-02-09)
- **Growth rate:** ~74 MB/day with V1

### Projected Costs with V2

V2 adds ~10x more data per cycle. With 2-minute intervals (matching Volland update frequency):

| Period | Projected Size | Monthly Cost |
|--------|----------------|--------------|
| Now | 1.6 GB | $0.39 |
| 1 month | 2.6 GB | $0.64 |
| 6 months | 7.6 GB | $1.89 |
| 1 year | 13.6 GB | $3.39 |
| 2 years | 25.6 GB | $6.39 |
| 5 years | 61.6 GB | $15.39 |

**Conclusion:** Costs remain very affordable even with V2's 10x data increase.

---

## Contact/Notes

- volland_test folder is untracked in git (safe for credentials)
- Main worker warning in CLAUDE.md - don't modify directly
- **Recommended scraping interval: 120 seconds** (Volland updates every 2 minutes, not 1)
- Market hours: 9:20 AM - 4:10 PM ET (current worker setting)
