# Discord Worker Plan

Standalone worker service that scrapes a Discord channel via Playwright, filters messages through Claude, and forwards trading-relevant signals to Telegram.

## External Setup Required

### 1. Telegram Bot
- Message @BotFather on Telegram, `/newbot`, follow prompts
- Create "Discord Daily Chat" channel, add the bot as admin
- Get the channel's chat ID via `https://api.telegram.org/bot<token>/getUpdates`

### 2. Anthropic API
- Get an `ANTHROPIC_API_KEY`
- Use Haiku model (fast, cheap, sufficient for chat filtering)

### 3. Discord Session (no bot needed)
- Log in manually via headed Playwright on your local machine
- Export browser state (cookies + localStorage) to JSON
- Upload to Railway for the worker to reuse

## Environment Variables

```
DISCORD_EMAIL
DISCORD_PASSWORD
DISCORD_CHANNEL_URL     # e.g. https://discord.com/channels/{server}/{channel}
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
ANTHROPIC_API_KEY
ANTHROPIC_MODEL          # optional, default claude-haiku
DATABASE_URL             # existing
```

## Process Flow

### One-time setup (local, manual)

1. Run Playwright headed (visible browser) on your machine
2. Log into Discord manually (handle CAPTCHA/2FA yourself)
3. Navigate to the target channel
4. Export browser state (cookies + localStorage) to a JSON file
5. Upload that state file to Railway as a persistent volume or env var

This gives the worker a valid session without ever automating the login flow.

### Worker loop (every 3 minutes)

```
 1. Launch headless browser with saved session
    (Playwright + stored cookies)

 2. Navigate to Discord channel URL

 3. Wait for messages to render in DOM

 4. Scrape all visible messages
    → author, timestamp, content for each

 5. Query Postgres for last_processed_timestamp
    → filter out already-seen messages

 6. If no new messages → sleep 3 min, goto 1

 7. Send new messages to Claude (Anthropic API)
    "Which of these are trading-relevant?"
    → returns: important/not + summary

 8. Log ALL results to discord_summaries table
    (message, author, timestamp, important flag,
     claude's reasoning, summary)

 9. If any marked important →
    POST summary to Telegram channel

10. Update cursor in Postgres
    (last_processed_timestamp = newest message)

11. Sleep 3 minutes → goto 1
    (keep browser open, just re-navigate/refresh)
```

### Key detail: browser stays open

Keep the browser open across iterations. Refresh the page or scroll to load new messages. Avoids re-login and is faster. Restart the browser only on error.

## Database Schema

```sql
CREATE TABLE discord_summaries (
    id SERIAL PRIMARY KEY,
    message_id TEXT,
    author TEXT,
    content TEXT,
    timestamp TIMESTAMPTZ,
    is_important BOOLEAN,
    claude_reasoning TEXT,
    summary TEXT,
    sent_to_telegram BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE discord_cursor (
    channel_id TEXT PRIMARY KEY,
    last_message_timestamp TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Telegram Output Example

```
Discord Signal (2 of 12 messages flagged)

@TraderMike: "5950 put wall just got lifted,
gamma flip now at 5980, watching for squeeze"
→ GEX level shift: put wall removed at 5950,
  gamma flip moved up to 5980

@FlowGuy: "massive 0DTE call sweep 5990-6000,
$8M notional in 2 minutes"
→ Unusual options activity: large call sweep
  near 6000 strike
```

## Gotchas

- **Discord CAPTCHA**: Railway's IP may trigger CAPTCHA on first login. Do initial login locally (headed mode), export session state, then use that on Railway.
- **Session persistence**: Store browser state via Playwright's `storage_state` so it doesn't re-login every restart.
- **2FA**: If your Discord account has 2FA, the manual login handles it. No need to automate TOTP.
- **Telegram message limit**: 4096 chars max per message. Truncate or split if needed.
- **Railway Procfile**: Add a second process entry: `worker: python discord_worker.py`

## Complexity

Low. ~150-200 lines of Python. One file, same pattern as volland_worker.py but simpler (no JS injection, no network interception). Dependencies already in the project (Playwright, psycopg, requests).
