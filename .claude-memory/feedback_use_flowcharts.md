---
name: Use flowcharts for complex logic
description: Always use decision tree / hierarchy flowcharts in HTML reports to explain complex filter logic, not just text or ASCII
type: feedback
---

When presenting complex decision logic (filters, gates, conditions), always use a **visual decision tree / flowchart** — not just text descriptions or ASCII diagrams.

**Why:** User said "why didn't you explain it as a hierarchy chart? This kind of illustrative is best for complex things." Plain text or tables don't convey branching logic as clearly as a visual tree.

**How to apply:**
- Any filter with branching conditions (if X → then check Y → if Z...) gets a CSS flowchart
- Use the flowchart CSS classes from the research report template (fc-node, fc-branch, fc-arrow, etc.)
- Nodes: decisions (blue border), block (red), allow (green), start (gray rounded)
- Include data at leaf nodes (WR, PnL) so the chart tells the full story
- Use flowcharts in ALL HTML research reports, not just gap filter
- Also good for: setup detection flow, outcome tracking logic, trail stop decision tree, filter evolution
