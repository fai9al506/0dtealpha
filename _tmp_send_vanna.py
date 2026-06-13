import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN"); chat="-1003792574755"
if not token: raise SystemExit("NO TOKEN")
cap=("Vanna — How It Works & How To Trade It (guide).\n"
 "Synthesized from Volland white paper + Discord (Wizard of Ops, Apollo, Dark Matter, official alerts).\n"
 "Core: vanna is a MAGNET but VOL-CONDITIONAL — +vanna pulls price in normal/falling vol, INVERTS to "
 "resistance when VIX rises. Covers the sign table, levels (magnet/pivot/wall/flip/cascade), multi-expiry, "
 "vanna vs gamma, practical rules, + today's live 7400 magnet example (+107pt pin). Open in a browser.")
f=open("daily_trade_logs/vanna_guide.html","rb")
r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",data={"chat_id":chat,"caption":cap},files={"document":("vanna_guide.html",f,"text/html")},timeout=90)
print("HTTP",r.status_code,r.json().get("ok"),r.json().get("description"))
