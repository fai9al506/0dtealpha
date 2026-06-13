"""Tel Res — GEX Long paradigm-gate investigation (2026-06-02)."""
import os, requests

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN") or "8544971756:AAGsdiBWXCZtPtKiUfhPddsd3M93Vwv8Xuw"
CHAT = "-1003792574755"

msg = """<b>🔎 GEX Long — why it didn't fire today (2026-06-02)</b>

<b>Diagnosis (you were right):</b>
On <b>TS GEX</b> (TS Gamma Exposure, chain C_Gamma·OI − P_Gamma·OI) the 9:46–9:50 ET structure was a clean <b>A++ GEX Long</b>: −GEX support <b>7570</b>, +GEX magnet <b>7620</b>. The classifier graded it A++. The <b>only</b> thing that blocked the trade was the hard paradigm gate (<code>BOFA-PURE</code>) at <code>setup_detector.py:179</code>, which runs before the v3 classifier ever sees the structure.

⚠️ My first pass used <b>Volland gamma</b> by mistake (opposite sign today: 7615 = −34M) and wrongly said "structure was BAD". Corrected — <b>GEX = TS Gamma Exposure, always</b>. Also found a code bug: portal harness <code>gex_long_v3.py</code> grades on Volland gamma (wrong source) vs live detector on TS GEX.

<b>Backtest — gate removed, TS GEX (Feb 23–Jun 2):</b>
• GEX-* (current):  101t / 45% WR / +203p / PF 1.30
• non-GEX, <b>align≥0</b>:  160t / <b>52% WR</b> / +849p / PF 2.02
• BOFA-PURE raw:  196t / 40% WR / +601p (trail-dependent, high var)

<b>Beta-controlled takeaway:</b> same uptrend both buckets → beta cancels. Non-GEX signals with align≥0 perform ≥ GEX-paradigm ones (52% vs 45%). <b>The paradigm label carries no protective signal once align≥0 is required.</b>

<b>Today, gate removed:</b>
09:36 A (+1) <b>WIN +30.8p</b> · 10:08 B WIN +24.9 · 10:24 B WIN +22.6 · 10:40 B WIN +13.2 · 10:56 EXP −0.7 · 11:22 (−1) LOSS −14.0
→ 4W/1flat/1L. Only loser was align −1. The 9:36 A-grade rode +30.8p — your move.

<b>Recommendation:</b> drop the hard paradigm gate, keep <b>align≥0</b> as the filter (don't let a bull-paradigm label substitute for alignment). GEX Long is already <b>portal-only</b> (no real $), so shipping this just logs signals for forward validation. Watch 30–50 live non-GEX signals before any real-money enable.

<i>Caveats: generator over-fires vs live should_notify → absolute PnL is directional, not a $ forecast. Re-run with real cadence before trusting dollars.</i>"""

r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                  json={"chat_id": CHAT, "text": msg, "parse_mode": "HTML",
                        "disable_web_page_preview": True})
print(r.status_code, r.text[:300])
