---
name: Never deploy during market hours
description: Code deploys during live trading caused missed signals, disconnects, and lost money on Day 1
type: feedback
---

Never deploy code changes during market hours (9:30-16:00 ET).

**Why:** Day 1 real money (Mar 24): 5+ deploys caused Rithmic ForcedLogout, ES quote stream drops, position tracking gaps, missed SC signals (cost +$43.50 on #1179), and ghost position false alarms. Each deploy restarts the service, killing all live connections.

**How to apply:** Queue all code fixes for pre-market (before 9:20 ET) or post-market (after 16:10 ET) deployment. If a critical bug is found during market hours, assess whether it's safer to let it run with the bug or risk a deploy. Only deploy mid-session for truly position-threatening bugs (e.g., stops not working).
