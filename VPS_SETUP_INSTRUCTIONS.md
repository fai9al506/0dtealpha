# VPS Setup Instructions for Claude Code

Run these steps in order on the VPS (0dte-vps, 103.54.56.210).

## Prerequisites (manual — do in Chrome/GUI)
- [x] Sierra Chart installed + Denali connected
- [ ] Sierra DTC Protocol Server enabled (Global Settings → SC Server Settings → DTC Protocol Server → Enable, Port 11099, Allow Trading YES, Encoding JSON)
- [ ] ES symbol added (@ES or ESM26.CME)
- [ ] VX symbol added (VXM26_FUT_CFE)
- [ ] NinjaTrader 8 installed + Rithmic connection configured
- [ ] IBKR TWS installed (for Sierra monthly auth)

## Step 1: Install Python 3.12+

```bash
# Download Python installer
curl -L -o python_installer.exe https://www.python.org/ftp/python/3.12.9/python-3.12.9-amd64.exe

# Install silently with PATH enabled
./python_installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_pip=1

# Verify (open NEW terminal after install)
python --version
pip --version
```

## Step 2: Clone the repo

```bash
cd /c
git clone https://github.com/<your-username>/0dtealpha.git
cd 0dtealpha
```

## Step 3: Install Python dependencies

```bash
pip install requests websocket-client pytz
```

## Step 4: Configure eval_trader for VPS

Create `eval_trader_config_vps.json` by copying from the real config, but change:
```json
{
  "nt8_incoming_folder": "C:\\Users\\Administrator\\Documents\\NinjaTrader 8\\incoming"
}
```
All other settings (API URL, API key, setup rules, compliance) stay the same as eval_trader_config_real.json.

## Step 5: Test eval_trader

```bash
python eval_trader.py --config eval_trader_config_vps.json --test buy
```

Verify OIF file appears in NT8 incoming folder.

## Step 6: Configure auto-start (Windows Task Scheduler)

Create scheduled tasks for:
1. Sierra Chart — at logon, restart on failure
2. NinjaTrader 8 — at logon, restart on failure
3. eval_trader.py — at logon + 30s delay, restart on failure
4. vps_data_bridge.py — at logon + 30s delay, restart on failure (after Phase 2)

## Step 7: Disable Windows auto-restart during market hours

```powershell
# Run in PowerShell as Administrator
Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\WindowsUpdate\UX\Settings" -Name "ActiveHoursStart" -Value 8
Set-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\WindowsUpdate\UX\Settings" -Name "ActiveHoursEnd" -Value 17
```

## Step 8: Configure Windows auto-logon

```powershell
# Run in PowerShell as Administrator — so VPS resumes after reboot without RDP
$RegPath = "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Winlogon"
Set-ItemProperty -Path $RegPath -Name "AutoAdminLogon" -Value "1"
Set-ItemProperty -Path $RegPath -Name "DefaultUserName" -Value "Administrator"
# Password must be set manually (don't store in scripts)
```

## Phase 2: Data Bridge (after market validation)

The `vps_data_bridge.py` script will be created in the main repo. It:
1. Connects to Sierra DTC server (localhost:11099)
2. Subscribes to @ES and /VX market data
3. Builds 5pt ES range bars (same as rithmic_es_stream.py)
4. POSTs completed bars to Railway: `POST /api/vps/es/bar`
5. Batches VIX ticks and POSTs to Railway: `POST /api/vps/vix/ticks`
6. Sends heartbeat every 60s: `POST /api/vps/heartbeat`

## Phase 2: VPS Monitor

The `vps_monitor.py` script monitors all processes and auto-restarts crashed ones:
- Sierra Chart (SierraChart.exe)
- NinjaTrader 8 (NinjaTrader.exe)
- eval_trader (python.exe with eval_trader in args)
- vps_data_bridge (python.exe with vps_data_bridge in args)

Sends heartbeat to Railway every 60s with component status.

## Key Paths on VPS

| Item | Path |
|------|------|
| Repo | `C:\0dtealpha\` |
| Sierra Chart | `C:\SierraChart\` |
| Sierra Data | `C:\SierraChart\Data\` |
| NT8 | `C:\Users\Administrator\Documents\NinjaTrader 8\` |
| NT8 Incoming | `C:\Users\Administrator\Documents\NinjaTrader 8\incoming\` |
| eval_trader config | `C:\0dtealpha\eval_trader_config_vps.json` |
| eval_trader state | `C:\0dtealpha\eval_trader_state_vps.json` |
| Python | `C:\Program Files\Python312\` |
