---
name: Run analysis scripts locally, not via deploy
description: Never deploy debug/analysis endpoints to Railway — download data once and analyze locally with Python scripts
type: feedback
---

Run analysis scripts LOCALLY, not by adding debug endpoints to the production app.

**Why:** Each deploy takes 3+ minutes, risks shutting down the live service (losing signals during market hours), and requires multiple iterations for bug fixes. Local scripts are instant to iterate and carry zero production risk.

**How to apply:**
- For data analysis: create a simple one-time endpoint to dump raw data (or use existing API), download as CSV/JSON, then run Python scripts locally
- Never add complex analysis logic to main.py — keep it in tmp_*.py scripts
- If DB access is needed: use `railway run python script.py` or download via existing API endpoints
- Reserve deploys for actual feature changes, not data exploration
