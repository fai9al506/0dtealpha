import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN"); chat="-1003792574755"
if not token: raise SystemExit("NO TOKEN")
path="daily_trade_logs/gamma_explorer_jun34.html"
cap=("Jun 3 vs Jun 4 — Interactive 0DTE Gamma Explorer.\n"
     "Slide the time bar to see the per-strike gamma profile change; setups marked on price with "
     "gamma-at-entry in the table. Jun3 longs lost (neg gamma + down-drift), Jun4 longs won (up-trend). "
     "Open in a browser (needs internet for the chart). Add your comments to refine the gamma rule.")
with open(path,"rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id":chat,"caption":cap}, files={"document":("gamma_explorer_jun34.html",f,"text/html")},timeout=90)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),r.json().get("description"))
