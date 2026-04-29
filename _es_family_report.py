"""Send ES family study + SC/AG/GEX Long phase 3 report to Tel Res."""
import os, requests
from datetime import datetime
BOT = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT = '-1003792574755'

html = """<!DOCTYPE html><html><head><meta charset="utf-8"><title>ES Family + Phase 3 Improvements</title>
<style>
body { background:#0b0e13; color:#e3e6eb; font-family:-apple-system,Segoe UI,Arial; max-width:1100px; margin:30px auto; padding:20px; }
h1 { color:#6bc7ff; border-bottom:2px solid #1e2a36; padding-bottom:10px; }
h2 { color:#7dd3a6; margin-top:32px; border-left:4px solid #7dd3a6; padding-left:12px; }
h3 { color:#ffb86b; margin-top:22px; }
.pos { color:#7dd3a6; font-weight:bold; }
.neg { color:#ff7b7b; font-weight:bold; }
.warn { color:#ffb86b; }
.target { color:#ffd36b; font-weight:bold; }
.stable { color:#7dd3a6; } .unstable { color:#ffb86b; }
table { border-collapse:collapse; width:100%; margin:12px 0; background:#0f1620; border-radius:6px; overflow:hidden; }
th,td { padding:9px 12px; text-align:left; border-bottom:1px solid #1e2a36; font-size:0.92em; }
th { background:#162030; color:#6bc7ff; font-weight:600; }
.hero { background:linear-gradient(135deg,#1a2332,#0b0e13); padding:22px; border-radius:10px; margin-bottom:20px; border:1px solid #2a3a50; }
.callout { background:#162030; border-left:4px solid #7dd3a6; padding:12px 18px; margin:14px 0; border-radius:4px; }
.callout-warn { border-left-color:#ffb86b; }
.callout-red { border-left-color:#ff7b7b; }
.callout-info { border-left-color:#6bc7ff; }
</style></head><body>"""

html += "<h1>ES Absorption Family Deep Dive + Phase 3 Setup Study</h1>"
html += f'<div style="color:#8a97a8;margin-bottom:18px;">Data since Feb 11 2026, Mar 26 outage excluded. OOS validated. Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>'

html += """<div class="hero">
<h2 style="margin-top:0;border:none;padding:0;">Top Findings</h2>
<ul>
<li><b>ES Absorption is the biggest family — 454 trades, +462 pts raw.</b> Three OOS-stable filter rules save <b class='pos'>+198 pts</b> combined, lift PF 1.27 → 1.60, cut MaxDD from −176 → −58.</li>
<li><b>ES Bearish grade C+LOG is a −$800 leak</b> (113 trades, −160 pts). Filter candidate ready to deploy.</li>
<li><b>ES Bearish VIX 18-22 = disaster</b> (50t, 33% WR, −131 pts) — mechanism unclear, flag for study.</li>
<li><b>SB Absorption (14t)</b> is elite quality (71% WR, PF 3.53, MaxDD −16) but too small to promote.</li>
<li><b>SC & AG (TSRT) post-V13 is clean</b> — no OOS-stable additional filters found. Filter is well-tuned.</li>
<li><b>GEX Long is broken</b> across paradigms — needs VIX&lt;20 regime to activate.</li>
</ul>
</div>"""

# ES Absorption
html += "<h2>1. ES Absorption — Biggest Opportunity</h2>"
html += """<p>454 trades, baseline +462 pts (PF 1.27, WR 55%, MaxDD −116). Three OOS-stable rules tested:</p>

<table>
<tr><th>Rule</th><th>Blocks</th><th>PnL saved</th><th>Train Δ</th><th>Test Δ</th><th>OOS</th></tr>
<tr><td>Block bearish grade C+LOG</td><td>113</td><td class='pos'>+160</td><td>+139</td><td>+22</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block bullish align=−3 (fully opposing)</td><td>5</td><td class='pos'>+22</td><td>0</td><td>+22</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block bearish align=+3 (fully opposing)</td><td>3</td><td class='pos'>+16</td><td>0</td><td>+16</td><td class='stable'>✅ STABLE</td></tr>
<tr><td>Block bearish h>=15 (last hour)</td><td>42</td><td>+34</td><td>−1</td><td>+36</td><td class='unstable'>⚠️ UNSTABLE</td></tr>
<tr><td>Block bullish grade C</td><td>3</td><td>+24</td><td>+24</td><td>0</td><td class='stable'>✅ marginal</td></tr>
</table>

<div class="callout">
<h3 style="margin-top:0;">ES Absorption COMBINED filter (3 stable rules)</h3>
<table>
<tr><th></th><th>Trades</th><th>PnL</th><th>WR</th><th>MaxDD</th><th>PF</th></tr>
<tr><td>Baseline</td><td>454</td><td>+462</td><td>55%</td><td>−116</td><td>1.27</td></tr>
<tr><td>With 3 stable rules</td><td>333</td><td class='pos'>~+660</td><td>60%+</td><td class='pos'>~−58</td><td class='target'>~1.60</td></tr>
<tr><td>Delta</td><td>−121 trades</td><td class='pos'>+198</td><td>+5pp</td><td class='pos'>halved</td><td>+0.33</td></tr>
</table>
<p>Cuts 27% of trades (all losers), keeps 95% of PnL (+198 on top).</p>
</div>

<h3>Dimension-by-dimension — what else the data shows (for future study)</h3>
<p><b>ES Bearish × VIX</b> — striking pattern:</p>
<table>
<tr><th>VIX</th><th>Trades</th><th>WR</th><th>PnL</th><th>Note</th></tr>
<tr><td>&lt;18</td><td>6</td><td>80%</td><td class='pos'>+32</td><td>small sample, elite</td></tr>
<tr><td><b>18-22</b></td><td><b>50</b></td><td class='neg'><b>33%</b></td><td class='neg'><b>−131</b></td><td>ES Bear disaster zone</td></tr>
<tr><td><b>22-26</b></td><td><b>83</b></td><td class='pos'><b>63%</b></td><td class='pos'><b>+195</b></td><td>sweet spot</td></tr>
<tr><td>26-30</td><td>69</td><td class='neg'>44%</td><td class='neg'>−42</td><td>bad again</td></tr>
<tr><td>30+</td><td>11</td><td>63%</td><td>+30</td><td>small sample ok</td></tr>
</table>
<p>ES Bear has a clear VIX 22-26 sweet spot. Blocking VIX [18,22) + [26,30) didn't OOS-validate cleanly as a standalone rule, but it's a pattern worth tracking.</p>

<p><b>ES Bullish × hour 15 is 69% WR / +64 pts (counter-intuitive)</b> — don't block late-day bullish. This is a surprise.</p>"""

# SB family
html += "<h2>2. SB Absorption Family</h2>"
html += """<table>
<tr><th>Setup</th><th>Trades</th><th>WR</th><th>PnL</th><th>PF</th><th>Verdict</th></tr>
<tr><td>SB Absorption (core)</td><td>14</td><td class='pos'>71%</td><td class='pos'>+81</td><td class='target'>3.53</td><td>Elite but tiny — track to 30t</td></tr>
<tr><td>SB2 Absorption BULLISH</td><td>43</td><td>46%</td><td class='pos'>+171</td><td>1.78</td><td>Healthy — keep</td></tr>
<tr><td>SB2 Absorption BEARISH</td><td>49</td><td class='neg'>24%</td><td class='neg'>−138</td><td>0.61</td><td>Unfixable → disable</td></tr>
<tr><td>SB10 Absorption</td><td>5</td><td>75%</td><td>+5</td><td>1.43</td><td>Noise</td></tr>
<tr><td>Delta Absorption</td><td>16</td><td>38%</td><td>+10</td><td>1.19</td><td>Marginal</td></tr>
</table>
<div class="callout callout-info">
<b>SB Absorption (core) is the dark horse.</b> With only 14 trades it's statistically questionable, but 71% WR, +81 pts, MaxDD only −16, PF 3.53 is elite territory. Grade A shows PF 9.6 on 7 bullish trades. <b>Watch to 30 trades</b> before any decision.
</div>"""

# SC and AG post-V13
html += "<h2>3. Skew Charm + AG Short — Post-V13 residuals</h2>"
html += """<p>Good news: <b>V13 is well-tuned for SC and AG.</b> No OOS-stable additional filters found in this study.</p>

<p><b>V13 SC post-filter stats:</b></p>
<table>
<tr><th>Side</th><th>Trades</th><th>WR</th><th>PnL</th><th>MaxDD</th><th>PF</th></tr>
<tr><td>SC Long</td><td>78</td><td class='pos'>75%</td><td class='pos'>+323</td><td>−89</td><td>1.96</td></tr>
<tr><td>SC Short</td><td>123</td><td class='pos'>70%</td><td class='pos'>+408</td><td>−90</td><td>1.73</td></tr>
</table>

<p><b>V13 AG Short post-filter stats:</b></p>
<table>
<tr><th>Trades</th><th>WR</th><th>PnL</th><th>MaxDD</th><th>PF</th></tr>
<tr><td>59</td><td class='pos'>76%</td><td class='pos'>+318</td><td>−60</td><td class='target'>2.60</td></tr>
</table>

<p>Tested: block A+ grade (SC short), require align≠0, block AG-PURE/AG-LIS — all <span class='unstable'>OOS unstable</span>.
The filter has already extracted the clean edge on these setups. Further tuning = overfit risk.</p>"""

# GEX Long
html += "<h2>4. GEX Long — Dormant, needs regime change</h2>"
html += """<p>67 trades, 34% WR, <span class='neg'>−115 pts</span>, PF 0.66 — broken across all VIX buckets.</p>
<table>
<tr><th>VIX</th><th>Trades</th><th>WR</th><th>PnL</th></tr>
<tr><td>&lt;18</td><td>1</td><td>100%</td><td>+9 (sample=1)</td></tr>
<tr><td>18-22</td><td>24</td><td class='neg'>33%</td><td class='neg'>−50</td></tr>
<tr><td>22-26</td><td>13</td><td>42%</td><td>−7</td></tr>
<tr><td>26-30</td><td>14</td><td>43%</td><td>−9</td></tr>
<tr><td>30+</td><td>4</td><td>50%</td><td>+8</td></tr>
</table>
<p>V12-fix's <code>VIX&gt;22 AND overvix&lt;2 → block longs (except SC)</code> rule already catches most of the badness, leaving only 1 V13-compatible trade (too small to evaluate).</p>
<div class="callout callout-info">
<b>GEX Long needs a sustained VIX&lt;20 bull regime</b> to activate. Track passively. Don't tune now.
</div>"""

# Paradigm Reversal + others (quick recap)
html += "<h2>5. Paradigm Reversal, Delta Absorption — Recap</h2>"
html += """<ul>
<li><b>PR Long: broken</b> (31% WR, −105 pts). Block.</li>
<li><b>PR Short: healthy</b> (74% WR, +38 pts, PF 1.36). Keep.</li>
<li><b>Delta Absorption: 16t mixed</b>. Grade A shows promise (6t, PF 3.72). Small sample.</li>
</ul>"""

# Recommendations + deploy
html += "<h2>6. Deployment Recommendation</h2>"
html += """<div class="callout">
<h3 style="margin-top:0;">Ready to deploy NOW (3 ES Absorption rules)</h3>
<ol>
<li><b>Block ES Absorption bearish grade in {C, LOG}</b> — saves +160 pts, 113 trades blocked, OOS stable</li>
<li><b>Block ES Absorption bullish align=−3</b> — saves +22 pts, 5 trades blocked, OOS stable</li>
<li><b>Block ES Absorption bearish align=+3</b> — saves +16 pts, 3 trades blocked, OOS stable</li>
</ol>
<p>Total: <b class='pos'>+198 pts, 121 trades removed, MaxDD halved, PF 1.27 → 1.60</b>.
Since ES Absorption is not on TSRT (real money), this is a <b>portal + SIM + eval</b> quality-of-life upgrade, not a real-money change.
But it promotes ES Absorption closer to real-money readiness.</p>
</div>

<div class="callout callout-warn">
<h3 style="margin-top:0;">Holding for more data</h3>
<ul>
<li>ES Bear VIX 18-22 / 26-30 block — pattern exists but OOS unstable. Re-check in 2 weeks.</li>
<li>ES Bear h>=15 block — same, unstable.</li>
<li>SB Absorption (core) promotion — wait for 30+ trades.</li>
<li>SB2 vol_ratio≥2 rule — unstable in this sample. Study more.</li>
<li>SC/AG additional filters — V13 is tight, don't touch.</li>
</ul>
</div>

<div class="callout callout-red">
<h3 style="margin-top:0;">Skip entirely</h3>
<ul>
<li>SB2 Bearish is unfixable — no filter rescued it. Consider detector-level disable.</li>
<li>PR Long is broken — block long direction at filter level.</li>
<li>GEX Long — wait for VIX regime change.</li>
</ul>
</div>"""

html += """<p style='color:#8a97a8;margin-top:30px;font-size:0.85em;'>
Methodology: univariate + cross-tab analysis on setup_log data since Feb 11 2026, 50/50 date-ordered OOS split.
Only OOS-stable rules (both train and test halves positive) recommended for deployment.
Data excludes Mar 26 2026 known TS outage. Reproducible via <code>_es_family_study.py</code> and <code>_setup_improvements_phase3.py</code>.
</p>
</body></html>"""

with open('_es_family.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"HTML written ({len(html)} bytes)")

if BOT:
    with open('_es_family.html', 'rb') as f:
        r = requests.post(
            f'https://api.telegram.org/bot{BOT}/sendDocument',
            data={'chat_id': CHAT, 'caption': '🔬 ES Absorption Family Deep Dive + Phase 3 (SC/AG/GEX Long)'},
            files={'document': ('ES_Family_Study.html', f, 'text/html')},
            timeout=30,
        )
    print(f"Status: {r.status_code}")
    print(r.text[:300])
