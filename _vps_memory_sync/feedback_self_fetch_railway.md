---
name: Self-fetch from Railway
description: Don't ask user for values available via Railway CLI — fetch them yourself
type: feedback
---

Don't ask the user for env vars, tokens, or config values that are available on Railway. Use `railway variables -s <service> --json` to fetch them.

**Why:** User got frustrated being asked for the Telegram bot token when it was right there on Railway. "Don't ask me every time about something you can get yourself."

**How to apply:** Before asking the user for any config value, check if it's available via Railway CLI, local config files, or .env files. Only ask if truly unavailable.
