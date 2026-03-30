---
name: Use PowerShell for ET time, not Git Bash TZ
description: Git Bash TZ=America/New_York is broken on Windows — shows GMT not EDT. Use PowerShell instead.
type: feedback
---

Never use `TZ=America/New_York date` in Git Bash on Windows — it silently resolves to GMT, giving wrong time (off by 4h).

**Why:** Mar 25 session — told user it was 19:58 ET when it was actually 15:58 ET. Almost pushed during market hours.

**How to apply:** For accurate ET time, use:
```
powershell -c "[System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId((Get-Date), 'Eastern Standard Time').ToString('HH:mm')"
```
