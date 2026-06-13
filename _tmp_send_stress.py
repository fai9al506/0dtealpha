import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN")
chat="-1003792574755"  # Researchs
if not token: print("NO TOKEN"); raise SystemExit(1)
path="daily_trade_logs/tsrt_2factor_stresstest.html"
cap=("2-Factor Sizing — HONEST STRESS TEST. Verdict: edge is FRAGILE, not robust.\n"
     "(1) Defensive half alone = NET LOSER -$2,538 (June-only). (2) Full edge concentrated: top day = "
     "49-59% of uplift, minus top-3 ~flat/negative, more losing days than winning. (3) Jun3 lost / Jun4 "
     "won = the TAPE, not signals; bar-by-bar gamma shows neg-gamma amplifies BOTH directions (factor unreliable). "
     "(4) $300 cap = slight net negative. Not safe to scale. Full detail + charts inside.")
with open(path,"rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id":chat,"caption":cap},
        files={"document":("tsrt_2factor_stresstest.html", f, "text/html")}, timeout=60)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),"desc=",r.json().get("description"))
