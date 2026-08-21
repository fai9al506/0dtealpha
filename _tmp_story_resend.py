import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN"); chat="-1003792574755"
assert token
msg="""<b>SIX MONTHS, FLAT — corrected</b>

Two errors in the first version, both caught by you:

<b>1.</b> Equity read $6,016. It is <b>$6,076.27</b> ($2,611.96 long + $3,464.31 short).
<b>2.</b> The chart plotted <b>+$710</b> against a line labelled "break-even". That $710 is the
STATEMENT ERA only, from 14 May. The account lost <b>$633.87 before that</b>, so the honest
all-time position against the $6,000 deposit is <b>+$76.27</b>.
<b>3.</b> And the profit was drawn BELOW the break-even line — an SVG y-axis mistake.

The curve is now generated from the 37 daily statement rows instead of drawn by hand, which
shows what the smooth line hid:
<pre>
peak     +$482   4 June
trough  -$1,307  24 June
today      +$76
</pre>
A +$482 to -$1,307 round trip in three weeks — that is the real shape of the June drawdown.

https://claude.ai/code/artifact/3bf35438-8dac-4bdc-85d7-b3e4bc885421"""
r=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
  data={"chat_id":chat,"text":msg,"parse_mode":"HTML","disable_web_page_preview":True},timeout=60)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),r.json().get("description"))
