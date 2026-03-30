---
name: AI Trading Co-Pilot — future feature
description: Plan to use Claude Code skills (included in sub) or Claude API (paid) for morning briefs, signal commentary, Discord monitoring, news filtering
type: project
---

User wants to leverage AI subscription for trading assistance beyond auto-execution. Discussed Mar 22, deferred to later.

**Why:** System already auto-trades. AI layer would be advisory — market context, signal commentary, anomaly detection.

**Preferred approach (zero extra cost):** Claude Code custom skills using existing subscription:
1. `/morning-brief` — 10 AM market read (VIX, paradigm, LIS, charm, DD, economic calendar, overnight ES move)
2. `/review-trades` — mid-day or EOD analysis of today's signals + outcomes
3. `/check-discord` — read trading Discord channels, filter for actionable info

**If fully automated (needs Claude API, ~$1-3/day):**
- Morning brief → Telegram (scheduled on Railway)
- Signal commentary → enhanced Telegram alerts with AI context
- EOD AI narrative → added to existing PDF report
- Discord monitor bot → filtered summaries to Telegram
- News sentinel → RSS/API → Claude filters for SPX-relevant events
- Mid-day checkpoint at 12:30 PM
- Trade journal AI — weekly pattern digest
- Anomaly detector — unusual data combinations

**Architecture:** Railway app → Claude API calls at key moments → Telegram delivery

**How to apply:** When user says "let's do the AI copilot" or "morning brief", refer to this plan and start with Option B (Claude Code skills, zero cost).
