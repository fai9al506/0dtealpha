---
name: Railway CLI login
description: Always use Railway CLI for logs/deploys. If unauthorized, immediately ask user to run 'railway login' manually (interactive mode required).
type: feedback
---

Always use Railway CLI to check logs, deploys, and service status — never skip it or guess.

**Why:** User wants accurate data from Railway at all times. CLI session expires periodically.

**How to apply:** Before any Railway command, if you get "Unauthorized", immediately ask the user to run `railway login` in their terminal (it requires interactive browser auth — cannot be done from Claude). Don't try workarounds or skip the check.
