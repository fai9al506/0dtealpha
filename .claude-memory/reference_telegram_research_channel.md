---
name: Telegram Research Channel
description: 0DTE Alpha Researchs Telegram channel for sending HTML research reports and deep analysis
type: reference
---

**Channel:** 0DTE Alpha Researchs
**Chat ID:** -1003792574755
**Bot:** Same TELEGRAM_BOT_TOKEN as general alerts
**Type:** channel

**Workflow:**
- When user asks to "send to Tel Res" or similar, generate a well-illustrated HTML report
- HTML should be dark-themed (Analysis #15 style), with charts, tables, color-coded sections
- Send as HTML document via Telegram Bot API `sendDocument` endpoint
- Use the same bot token from env vars or from the Railway deployment

**How to send:**
```python
import requests
url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
files = {"document": ("report.html", open(path, "rb"), "text/html")}
data = {"chat_id": "-1003792574755", "caption": "Report title"}
requests.post(url, files=files, data=data)
```
