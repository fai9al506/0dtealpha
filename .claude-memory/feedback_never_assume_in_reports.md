---
name: Never assume in reports — only show what was researched
description: CRITICAL — never add branches/conclusions to reports that weren't in the actual research data. User was furious when gap-down was shown as "normal trading" without any data backing it.
type: feedback
---

When building HTML research reports or flowcharts, ONLY include what was actually studied and backed by data. Never fill in "obvious" branches with assumed conclusions.

**Why:** User was furious. In the V12 gap filter report, I added a "Gap Down → Normal Trading" branch to the decision tree, but the research ONLY studied gap-UP longs. Gap-down was never analyzed. I assumed it was fine — that's fabrication, not research.

**How to apply:**
- Every node/branch in a flowchart must trace back to actual backtest data
- If a condition wasn't studied, DON'T include it — or explicitly label it "NOT STUDIED"
- This is the same core rule as "never assume values" but applied to visual reports
- If the tree feels incomplete without an unstudied branch, that's a research gap to flag to the user, NOT something to fill in with assumptions
- This applies to ALL report types: HTML, PDF, text summaries
