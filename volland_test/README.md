# Volland Test Scraper

**Isolated test environment** - NO connection to production database.

## Setup

1. Set environment variables:
```bash
# Windows (PowerShell)
$env:VOLLAND_EMAIL = "your-email@example.com"
$env:VOLLAND_PASSWORD = "your-password"
$env:VOLLAND_TEST_URL = "https://vol.land/app/workspace/YOUR_TEST_WORKSPACE_ID"

# Windows (CMD)
set VOLLAND_EMAIL=your-email@example.com
set VOLLAND_PASSWORD=your-password
set VOLLAND_TEST_URL=https://vol.land/app/workspace/YOUR_TEST_WORKSPACE_ID

# Linux/Mac
export VOLLAND_EMAIL="your-email@example.com"
export VOLLAND_PASSWORD="your-password"
export VOLLAND_TEST_URL="https://vol.land/app/workspace/YOUR_TEST_WORKSPACE_ID"
```

2. Make sure Playwright is installed:
```bash
pip install playwright
playwright install chromium
```

## Run Test

```bash
cd volland_test
python test_scraper.py
```

The browser will open (not headless) so you can:
1. Watch it login
2. See your workspace with all widgets
3. **Interact with widgets** (zoom in/out) to trigger data loads
4. Watch what gets captured

## Output

Files are saved to `captures/` folder:
- `raw_captures_YYYYMMDD_HHMMSS.json` - All captured network requests
- `analysis_YYYYMMDD_HHMMSS.json` - Analyzed/categorized data

## Analyze Results

```bash
python analyze_captures.py
```

This will show:
- What endpoints were captured
- What data each contains
- How to identify different widget types (charm vs vanna, 0DTE vs weekly, etc.)

## Delete Everything

If test fails or you want to clean up:
```bash
# Delete entire test folder
rm -rf volland_test/

# Or just delete captures
rm -rf volland_test/captures/*
```

**Zero impact on production!**
