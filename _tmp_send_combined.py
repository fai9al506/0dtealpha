import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN"); chat="-1003792574755"  # Researchs
if not token: raise SystemExit("NO TOKEN")
path="daily_trade_logs/semi_gamma_sizing_validated.html"
cap=("Semi + Gamma Sizing — VALIDATED (updated: +REAL per-day post-V16).\n"
     "1,267 trades Mar17-Jun10 PORTAL: base +$12,685 -> Semi +$20,984 -> 2-factor +$22,475. "
     "Gamma adds independent info (within semi-confirmed: gamma-fav 64% vs unfav 56%).\n"
     "NEW Section 3 = REAL TSRT broker per-day. Note: the cumulative chart is PORTAL; on Jun9/10 the portal "
     "book was green (+$250/+$120, shorts+ES Abs+V-reversal) but the REAL broker bled (-$335/-$291, longs+breaker). "
     "Section 3 shows the honest broker view.")
with open(path,"rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id":chat,"caption":cap}, files={"document":("semi_gamma_sizing_validated.html",f,"text/html")},timeout=90)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),r.json().get("description"))
