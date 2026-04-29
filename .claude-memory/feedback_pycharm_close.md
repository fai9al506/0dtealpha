---
name: Close PyCharm before editing config/state files
description: User must close PyCharm before Claude edits eval_trader config/state/position JSON files — Google Drive sync reverts changes otherwise
type: feedback
---

Always ask the user to close PyCharm before editing any eval_trader JSON files (config, state, position, api_state).

**Why:** Google Drive sync + PyCharm file watchers revert edits made while PyCharm is open. This has happened multiple times with eval_trader_config_sierra.json — edits were silently reverted back to the old content.

**How to apply:** Before any Edit/Write to `eval_trader_config*.json`, `eval_trader_state*.json`, `eval_trader_position*.json`, or `eval_trader_api_state*.json` — ask: "Close PyCharm first, then I'll edit the file."
