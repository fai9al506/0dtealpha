# ES DOM Pipeline — Deploy Guide

End-to-end build for capturing the ES limit-order book on VPS, mirroring the existing VX DOM pipeline.

## Components shipped

| File | What changed |
|---|---|
| `sierra_studies/ESDomSnapshot.cpp` | NEW — Sierra ACSIL study that writes top-10 bid/ask levels to `es_dom.jsonl` every 1s. Mirrors `VXDomSnapshot.cpp` exactly. |
| `vps_data_bridge.py` | Added `_check_es_dom()` tailer + `post_es_dom_batch()` + config fields + heartbeat field. Mirrors VX DOM path. |
| `app/main.py` | Added `POST /api/vps/es/dom` (Bearer auth, batch insert) + `GET /api/vps/es/dom` (session auth, last N hours) + `vps_es_dom_snapshots` table auto-create in `db_init()`. |

## Deploy sequence (post 16:10 ET, no live trading risk)

### 1. Push repo (laptop)

```bash
cd C:\Users\Administrator\0dtealpha
git add sierra_studies/ESDomSnapshot.cpp sierra_studies/ES_DOM_DEPLOY.md vps_data_bridge.py app/main.py
git commit -m "S173: ES DOM capture pipeline (Sierra study + bridge tailer + Railway endpoints)"
git push origin main
```

Railway auto-deploys the API/table changes (~2 min). `vps_es_dom_snapshots` is auto-created on next startup.

### 2. Sync VPS bridge code

```powershell
# On VPS PowerShell:
cd C:\0dtealpha
git pull
```

### 3. Compile Sierra study (on VPS Sierra)

1. Open Sierra Chart on the VPS (RDP session).
2. Copy `C:\Users\Administrator\0dtealpha\sierra_studies\ESDomSnapshot.cpp` → `C:\SierraChart\ACS_Source\ESDomSnapshot.cpp`.
3. **Analysis** menu → **Build Custom Studies DLL** → select **ESDomSnapshot.cpp** only → **Build**.
4. Wait for "Build complete" message. Verify `C:\SierraChart\Data\ESDomSnapshot_64.dll` exists (or similar name).
5. **Build-worker tip** (from VX experience): Sierra build server randomly assigns build workers; if the resulting DLL crashes Sierra on load, just delete the DLL and retry the build — gets a different worker.

### 4. Attach study to ES chart

1. Open the existing **ESM26-CME** chart (the one feeding `vps_data_bridge` — chart that's already getting depth on Rithmic/CME feed).
2. **Studies** menu → **Add Custom Study** → **ES DOM Snapshot** → **Add**.
3. In settings dialog, accept defaults:
   - Enabled = Yes
   - Snapshot Interval = 1 sec
   - Max Levels Each Side = 10
   - DOM Output File Path = `C:\SierraChart\Data\es_dom.jsonl`
   - Also Emit Derived Features = Yes
   - Features Output File Path = `C:\SierraChart\Data\es_dom_features.csv`
4. **OK** to add. Chart should immediately start writing to `es_dom.jsonl`.

### 5. Verify Sierra is writing

```powershell
# Wait 10 sec then check file growth:
Get-Item C:\SierraChart\Data\es_dom.jsonl | Select-Object Length, LastWriteTime
Get-Content C:\SierraChart\Data\es_dom.jsonl -Tail 2
```

Expected output: a JSON line per second like:
```json
{"ts":"2026-05-22T18:55:01","s":"ESM26-CME","bid":[[7491.50,42,7],[7491.25,18,4],...],"ask":[[7491.75,33,6],...]}
```

If file size stays at 0 — common cause: study doesn't have a depth subscription. Check the chart's data feed includes **CME Market Depth**.

### 6. Restart VPS bridge

```powershell
# Find current bridge PID:
Get-Process python | Where-Object { (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine -like "*vps_data_bridge*" }

# Kill it:
taskkill /F /PID <pid>

# Relaunch (off-market window only):
wscript C:\0dtealpha\run_bridge.vbs
```

### 7. Verify end-to-end

```powershell
# Within 1 min of bridge restart:
Get-Content C:\0dtealpha\logs\vps_bridge.log -Tail 20 | Select-String "es_dom"
```

Expected: heartbeat logs now include `es_dom_snaps_posted=N` and N > 0.

```bash
# From any machine with session cookie to dashboard:
curl -s "https://0dtealpha.com/api/vps/es/dom?hours=1&limit=5" -b "session=..."
```

Or from DB directly:
```sql
SELECT COUNT(*), MIN(ts), MAX(ts) FROM vps_es_dom_snapshots;
SELECT ts, bid_levels->0, ask_levels->0 FROM vps_es_dom_snapshots ORDER BY ts DESC LIMIT 3;
```

Expected: ~1 row/sec during market hours (RTH window).

## What runs in production after deploy

- Sierra study writes to `es_dom.jsonl` every 1s during market hours when ESM26-CME depth is subscribed.
- Bridge polls the file every 5s, batches up to 50 snapshots, POSTs to `/api/vps/es/dom`.
- Railway inserts into `vps_es_dom_snapshots` table.
- ~78,000 rows/day during RTH (matches VX DOM Day 1 throughput).

## Phase 2 — DOM ladder detector (next sprint)

This pipeline gets us the **data**. The signal that captures Apollo's edge needs a detector that:
1. Reads recent ES DOM snapshots (last 5 min).
2. Finds clustered queues: ≥3 consecutive 5pt strikes (e.g., 7510/7515/7520) on one side with depth ≥ N contracts each.
3. Confirms ladder has a "wall break" — the topmost queue has empty book past it (next strike < threshold).
4. Fires `LADDER_BULL @ 7525` (the cap) or `LADDER_BEAR @ 7460` (the floor) signal.
5. Tracks signal outcome: did price gravitate to the cap and reverse?

Will be a separate module `app/dom_ladder_detector.py`. Not blocking this data-capture deploy — collect first, build detector once we have 1-2 weeks of data.

## Rollback

Three layers — each independent:

| To remove | How |
|---|---|
| Sierra study | Remove study from ES chart settings OR set Enabled=No in study config |
| Bridge tailer | Set `es_dom_poll_seconds=999999` in `vps_bridge_config.json` (file still grows on Sierra side but is harmless) |
| API + table | Revert the main.py commit + redeploy. Table can stay (read-only artifact). |

Each layer is non-destructive to existing trading. ES Absorption, VX Apollo Pipeline, real_trader, eval_trader are completely untouched by this change.

## Open items for next session

- Day-1 verification after first market open with study active (Mon 2026-05-25 09:30 ET).
- Watch for "no depth" empty levels — if `bid_levels` always `[[0.0,0,0]]` (as VX shows post-market today), the chart isn't subscribed to depth. Check Rithmic plan covers CME L2.
- Build DOM ladder detector once ~3 days of data is collected.
- Add `vps_es_dom_snapshots` freshness to pipeline health monitoring (mirror VX freshness rules).
