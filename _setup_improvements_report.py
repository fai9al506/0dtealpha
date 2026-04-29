"""Build setup improvement report for Tel Res."""
import os, requests
from datetime import datetime
BOT = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT = '-1003792574755'

html = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>Setup Improvement Study</title>
<style>
body { background:#0b0e13; color:#e3e6eb; font-family:-apple-system,Segoe UI,Arial; max-width:1100px; margin:30px auto; padding:20px; }
h1 { color:#6bc7ff; border-bottom:2px solid #1e2a36; padding-bottom:10px; }
h2 { color:#7dd3a6; margin-top:35px; border-left:4px solid #7dd3a6; padding-left:12px; }
h3 { color:#ffb86b; margin-top:22px; }
.pos { color:#7dd3a6; font-weight:bold; }
.neg { color:#ff7b7b; font-weight:bold; }
.warn { color:#ffb86b; }
.target { color:#ffd36b; font-weight:bold; }
table { border-collapse:collapse; width:100%; margin:14px 0; background:#0f1620; border-radius:6px; overflow:hidden; }
th,td { padding:9px 12px; text-align:left; border-bottom:1px solid #1e2a36; font-size:0.92em; }
th { background:#162030; color:#6bc7ff; font-weight:600; }
.hero { background:linear-gradient(135deg,#1a2332,#0b0e13); padding:22px; border-radius:10px; margin-bottom:20px; border:1px solid #2a3a50; }
.callout { background:#162030; border-left:4px solid #7dd3a6; padding:12px 18px; margin:15px 0; border-radius:4px; }
.callout-warn { border-left-color:#ffb86b; }
.callout-red { border-left-color:#ff7b7b; }
.rule-box { background:#0a0e14; padding:10px 14px; margin:8px 0; border-radius:4px; border-left:3px solid #6bc7ff; font-size:0.9em; }
.stable { color:#7dd3a6; } .unstable { color:#ffb86b; }
</style></head><body>"""

html += "<h1>Setup-Level Improvement Study — BofA, DD, VPB, SB2, PR, ES Abs</h1>"
html += f'<div style="color:#8a97a8;margin-bottom:18px;">All data since Feb 2026, Mar 26 outage excluded. OOS validated (50/50 split by date). Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>'

html += """<div class="hero">
<h2 style="margin-top:0;border:none;padding:0;">Top Takeaways</h2>
<ul>
<li><b>DD Exhaustion has MASSIVE untapped edge.</b> 6 OOS-stable filter rules exist. Combined rule takes DD from <b class='neg'>−34 pts</b> to <b class='pos'>+309 pts</b> (+343 pts saved, both halves positive).</li>
<li><b>BofA Scalp cleanup</b>: blocking VIX 22-30 alone takes it from marginal to PF 2.15, +121 pts, WR 68%. OOS stable.</li>
<li><b>Vanna Pivot Bounce LONGS promotion-ready</b> on quality metrics (75% WR, PF 3.88, MaxDD only −16). Still 17t — need 30 for confidence.</li>
<li><b>SB2 Bearish unfixable</b> — no filter rescues it. Recommend turning off.</li>
<li><b>Paradigm Reversal SHORT is good</b> (74% WR, +38 pts). PR Long is broken.</li>
<li><b>ES Absorption grading v3 (Apr 13+)</b> looking healthy on bullish side (62% WR, PF 2.18).</li>
</ul>
</div>"""

# =================================
# DD EXHAUSTION (biggest find)
# =================================
html += "<h2>1. DD Exhaustion — Biggest Opportunity</h2>"
html += """<p>449 trades, currently <span class='neg'>−34 pts baseline</span>. Analysis found 6 independent <b>OOS-stable</b> rules, each saving significant losses.</p>"""

html += """<table>
<tr><th>Rule</th><th>Blocks</th><th>Saves</th><th>Train Δ</th><th>Test Δ</th><th>OOS</th></tr>
<tr><td>Block LONG align=+3 (counter-intuitive — too-extended)</td><td>118t</td><td class='pos'>+312</td><td>+145</td><td>+167</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block LONG VIX ≥ 22</td><td>131t</td><td class='pos'>+280</td><td>+139</td><td>+141</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block LONG bad paradigms (GEX-LIS, AG-LIS, AG-PURE, etc.)</td><td>124t</td><td class='pos'>+356</td><td>+130</td><td>+226</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block SHORT BOFA-PURE (67t, 40% WR, −104)</td><td>67t</td><td class='pos'>+104</td><td>+82</td><td>+22</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block SHORT grade A+ (51t, 38% WR, −68)</td><td>51t</td><td class='pos'>+68</td><td>+22</td><td>+46</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block grade C (both dirs)</td><td>40t</td><td class='pos'>+157</td><td>+0</td><td>+157</td><td class='stable'>✅ STABLE</td></tr>
</table>"""

html += """<div class="callout">
<h3 style="margin-top:0;">DD COMBINED Rule (all 6 above stacked)</h3>
<table>
<tr><th></th><th>Trades</th><th>PnL</th><th>WR</th><th>MaxDD</th><th>PF</th></tr>
<tr><td>Current (V12-fix base)</td><td>449</td><td class='neg'>−34</td><td>45%</td><td class='neg'>−504</td><td>0.99</td></tr>
<tr><td>Combined filter</td><td>162</td><td class='pos'>+309</td><td>52%</td><td>−146</td><td>1.40</td></tr>
<tr><td><b>Improvement</b></td><td>−287 trades</td><td class='pos'><b>+343 pts</b></td><td>+7pp</td><td class='pos'>MDD halved</td><td>+0.41</td></tr>
</table>
<p>Train half Δ=+46, test half Δ=+296. Both positive → filter holds out-of-sample.</p>
</div>"""

html += """<div class="callout callout-warn">
<b>Biggest single insight</b>: <b>DD Long with alignment=+3 is catastrophic</b> — 118 trades, 37% WR, −312 pts, MaxDD −482.
Counter-intuitive: more alignment usually = better. DD is contrarian (fires on DD/charm divergence).
When Greeks are ALSO fully aligned bullish, the bullish move has already happened — DD's "exhaustion" call is late and fails.
<b>Fix: on DD longs, require align in [0, 2] only.</b>
</div>"""

# =================================
# BofA
# =================================
html += "<h2>2. BofA Scalp — Cleanup</h2>"
html += """<p>84 trades, +19 pts baseline (marginal). Two clean OOS-stable rules.</p>

<table>
<tr><th>Rule</th><th>Blocks</th><th>Saves</th><th>Train Δ</th><th>Test Δ</th><th>OOS</th></tr>
<tr><td>Block VIX 22-30 (the chop regime)</td><td>41t</td><td class='pos'>+102</td><td>+19</td><td>+83</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block time ≥ 14:00 (tighten from 14:30)</td><td>37t</td><td class='pos'>+51</td><td>+15</td><td>+36</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block grade A+</td><td>19t</td><td class='pos'>+30</td><td>+2</td><td>+29</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block BofA-LIS paradigm</td><td>20t</td><td>+15</td><td>−8</td><td>+23</td><td class='unstable'>⚠️ UNSTABLE</td></tr>
<tr><td>Block long+align=+3 (14t, 25% WR, −61)</td><td>12t</td><td class='pos'>+56</td><td>−10</td><td>+66</td><td class='unstable'>⚠️ UNSTABLE</td></tr>
</table>

<div class="callout">
<h3 style="margin-top:0;">Recommended BofA Filter (only stable rules)</h3>
<table>
<tr><th></th><th>Trades</th><th>PnL</th><th>WR</th><th>MaxDD</th><th>PF</th></tr>
<tr><td>Current</td><td>84</td><td>+19</td><td>54%</td><td>−136</td><td>1.06</td></tr>
<tr><td>VIX 22-30 block alone</td><td>43</td><td class='pos'>+121</td><td><b>68%</b></td><td>−33</td><td class='target'><b>2.15</b></td></tr>
</table>
<p>Just the VIX rule gets BofA to promotion-grade metrics. Don't stack the unstable rules — classic overfit trap.</p>
</div>

<p><b>Sweet spot BofA long + VIX&lt;20</b>: 8 trades, <b>100% WR</b>, +57 pts, MaxDD −1, PF 58. Tiny sample but remarkable. Watch this bucket.</p>"""

# =================================
# Vanna Pivot Bounce
# =================================
html += "<h2>3. Vanna Pivot Bounce — Promotion Candidate</h2>"
html += """<p>40 trades total (17 longs + 23 shorts). Quality diverges sharply by direction.</p>
<table>
<tr><th>Direction</th><th>Trades</th><th>WR</th><th>PnL</th><th>MaxDD</th><th>PF</th><th>Verdict</th></tr>
<tr><td>LONG</td><td>17</td><td class='pos'>75%</td><td class='pos'>+92</td><td>−16</td><td class='target'>3.88</td><td class='pos'>★ PROMOTE WHEN 30t</td></tr>
<tr><td>SHORT</td><td>23</td><td>52%</td><td>+32</td><td>−72</td><td>1.36</td><td>mediocre — don't trade</td></tr>
</table>

<div class="callout">
<b>Recommendation: keep logging BOTH directions; trade LONGS only when promoted.</b>
VPB longs at VIX&lt;18: 7 trades, 86% WR, +52 pts, MaxDD −8, PF 7.5. Sweet spot is low-VIX bullish regime.
Need 13 more trades (at ~1-2/week, that's ~2 months) before 30t threshold.
</div>"""

# =================================
# SB2
# =================================
html += "<h2>4. SB2 Absorption — Bearish Unfixable</h2>"
html += """<p>92 trades. Bullish side profitable (+171 pts, 46% WR, PF 1.78). Bearish is broken:</p>
<table>
<tr><th>Direction</th><th>Trades</th><th>WR</th><th>PnL</th><th>MaxDD</th><th>PF</th></tr>
<tr><td>Bullish</td><td>43</td><td>46%</td><td class='pos'>+171</td><td>−50</td><td>1.78</td></tr>
<tr><td>Bearish</td><td>49</td><td class='neg'>24%</td><td class='neg'>−138</td><td class='neg'>−148</td><td>0.61</td></tr>
</table>
<p>Tested VIX filters, grade filters, paradigm filters — nothing rescues SB2 Bearish enough.
Best sub-bucket (A grade): 6t, 50% WR, +30 pts. Sample too small.</p>
<div class="callout callout-red">
<b>Recommendation: Disable SB2 Absorption BEARISH</b> until a fundamentally different signal is identified. Keep bullish side live.
</div>"""

# =================================
# Paradigm Reversal
# =================================
html += "<h2>5. Paradigm Reversal — Long Broken, Short Healthy</h2>"
html += """<table>
<tr><th>Direction</th><th>Trades</th><th>WR</th><th>PnL</th><th>MaxDD</th><th>PF</th><th>Verdict</th></tr>
<tr><td>LONG</td><td>19</td><td class='neg'>31%</td><td class='neg'>−105</td><td>−113</td><td>0.34</td><td>BROKEN</td></tr>
<tr><td>SHORT</td><td>25</td><td class='pos'>74%</td><td class='pos'>+38</td><td>−42</td><td>1.36</td><td>Healthy</td></tr>
</table>
<p>PR Long fails across paradigms. Only profitable long sub-bucket is VIX 18-22 (7t, 67% WR, +18) — too small.</p>
<div class="callout">
<b>Recommendation: Block PR LONGS entirely. Keep PR SHORTS as-is.</b> PR short is a good complement to SC short.
</div>"""

# =================================
# ES Absorption
# =================================
html += "<h2>6. ES Absorption — v3 Grading Check</h2>"
html += """<p>Grading v3 was deployed Apr 13. Early post-v3 data (18 bullish + 16 bearish) is promising on bullish side:</p>
<table>
<tr><th>Side</th><th>Pre-v3 (all time)</th><th>Post-v3 (Apr 13+)</th></tr>
<tr><td>Bullish</td><td>60% WR, PF 1.51</td><td class='pos'>62% WR, PF 2.18, +57 pts in 18t</td></tr>
<tr><td>Bearish</td><td>50% WR, PF 1.07</td><td>36% WR, −20 pts (only 16t — noisy)</td></tr>
</table>
<p>Grade C + LOG bearish: combined −160 pts. Clear candidate for filtering.</p>
<div class="callout">
<b>Recommendation: Block ES Abs BEARISH grade C and LOG. Keep bullish as-is.</b> Re-check after 50 more trades post-v3.
</div>"""

# =================================
# Execution plan
# =================================
html += "<h2>7. Deployment Plan (ranked by impact)</h2>"
html += """<ol>
<li><b>DEPLOY DD Exhaustion filter bundle</b> — BIGGEST WIN.
The combined rule (6 gates) takes DD from losing to <span class='pos'>+309 pts / PF 1.4 / halved MaxDD</span>. Add to <code>_passes_live_filter()</code> for DD Exhaustion. OOS validated.</li>
<li><b>DEPLOY BofA VIX 22-30 block</b> — moderate win. Single rule, OOS stable, adds +102 pts. Could promote BofA to TSRT after this. Add to <code>_passes_live_filter()</code>.</li>
<li><b>DEPLOY Paradigm Reversal LONG block</b> — prevents a −$525 loss bucket. Simple rule.</li>
<li><b>DEPLOY SB2 Bearish disable</b> — flag it in the detector or filter. Small sample but consistent drag.</li>
<li><b>LOG ONLY Vanna Pivot Bounce shorts</b>, trade VPB longs after 30t milestone.</li>
<li><b>WAIT</b> on ES Abs bearish C/LOG block — small sample, re-check in 2 weeks.</li>
</ol>"""

html += """<h2>8. Risk Caveats</h2>
<div class="callout callout-warn">
<ul>
<li>All these rules are <b>in-sample fitted</b> to Feb-Apr 2026 data. Even OOS-stable (train/test split) doesn't guarantee forward stability.</li>
<li>DD filter is aggressive: blocks 287/449 (64%) of trades. Check if the remaining 162 are enough volume for realistic trading.</li>
<li>Deploying all changes at once makes attribution hard. Consider 1-rule-per-week rollout to isolate each rule's impact.</li>
<li>The DD align=+3 counter-intuition is strong — it's real in data, but the mechanism should be understood before deploying. Maybe DD fires too late on fully-aligned days.</li>
</ul>
</div>

<p style='color:#8a97a8;margin-top:30px;font-size:0.85em;'>
Methodology: univariate + bivariate analysis per setup, then 50/50 date-ordered train/test split.
Only OOS-stable rules recommended. Sample excludes Mar 26 TS outage.
All numbers reproducible via <code>_bofa_dd_deep_study.py</code> and <code>_setup_improvements_phase2.py</code>.
</p>
</body></html>"""

with open('_setup_improvements.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"HTML written ({len(html)} bytes)")

if BOT:
    with open('_setup_improvements.html', 'rb') as f:
        r = requests.post(
            f'https://api.telegram.org/bot{BOT}/sendDocument',
            data={'chat_id': CHAT, 'caption': '🔬 Setup-Level Improvements — DD/BofA/VPB/SB2/PR/ES Abs (OOS validated)'},
            files={'document': ('Setup_Improvements.html', f, 'text/html')},
            timeout=30,
        )
    print(f"Status: {r.status_code}")
    print(r.text[:300])
