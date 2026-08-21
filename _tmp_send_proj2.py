# -*- coding: utf-8 -*-
import os, requests

token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat = "-1003792574755"  # 0DTE Alpha Researchs
assert token, "NO TOKEN"

cap = (
    "V21 PROJECTION - updated with 1 / 2 / 5 / 10 contract sizing.\n\n"
    "1 MES   +$2,372/mo   SAR  8,896   MaxDD  -$906\n"
    "2 MES   +$4,745/mo   SAR 17,792   MaxDD -$1,812\n"
    "5 MES  +$11,861/mo   SAR 44,480   MaxDD -$4,531\n"
    "1 ES   +$23,723/mo   SAR 88,960   MaxDD -$9,061\n\n"
    "The money scales linearly and so does the damage. Read the capital column first:\n"
    "5 MES needs $18,929 and a 40-point gap costs $10,000 = 165% of the account.\n"
    "1 ES needs $37,857 and a gap costs $20,000 = 329%.\n\n"
    "Those two rows are arithmetic, not a plan - unfundable today and they would not\n"
    "survive one bad open. The real next step is September: 2 and 3 contracts on stacked\n"
    "Skew Charm shorts only, which needs $3,029 and is already funded."
)

with open("0DTE_Projection_V21.html", "rb") as f:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id": chat, "caption": cap},
        files={"document": ("0DTE_Projection_V21.html", f, "text/html")},
        timeout=120,
    )
j = r.json()
print("HTTP", r.status_code, "ok=", j.get("ok"), j.get("description"))
