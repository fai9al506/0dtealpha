"""
V2 Dashboard — Modern trading cockpit design.
Separate from the original dashboard. Access at /v2.
Delete this file + remove 15 lines from main.py to revert.
"""
from fastapi import APIRouter, Cookie
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()

_get_context = None


def init(get_context_fn):
    global _get_context
    _get_context = get_context_fn


DASH_V2_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>0DTE Alpha — Trading Cockpit</title>
  <link rel="icon" type="image/png" href="/favicon.png">
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-primary: #0a0e17;
      --bg-card: #111827;
      --bg-card-hover: #1a2332;
      --bg-surface: #0d1520;
      --border: #1e2d3d;
      --text-primary: #e8edf5;
      --text-secondary: #7a8ba3;
      --text-muted: #4a5568;
      --green: #00e396;
      --red: #ff4560;
      --blue: #008ffb;
      --gold: #feb019;
      --purple: #9b59b6;
      --cyan: #00d4ff;
    }
    * { margin:0; padding:0; box-sizing:border-box; }
    body {
      background: var(--bg-primary);
      color: var(--text-primary);
      font-family: 'Plus Jakarta Sans', -apple-system, sans-serif;
      min-height: 100vh;
      overflow-x: hidden;
    }
    .grain {
      position:fixed; inset:0; z-index:0; pointer-events:none; opacity:0.03;
      background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
    }
    .container { position:relative; z-index:1; max-width:1440px; margin:0 auto; padding:20px 24px; }

    /* Signal Bar */
    .signal-bar {
      background: linear-gradient(135deg, rgba(0,143,251,0.08) 0%, rgba(0,227,150,0.05) 100%);
      border: 1px solid rgba(0,143,251,0.2);
      border-radius: 10px;
      padding: 10px 18px;
      margin-bottom: 16px;
      display: none;
      align-items: center;
      gap: 14px;
      animation: fadeUp 0.4s ease;
    }
    .signal-bar.active { display: flex; }
    .signal-bar.signal-long {
      background: linear-gradient(135deg, rgba(0,227,150,0.1) 0%, rgba(0,227,150,0.03) 100%);
      border-color: rgba(0,227,150,0.3);
    }
    .signal-bar.signal-short {
      background: linear-gradient(135deg, rgba(255,69,96,0.1) 0%, rgba(255,69,96,0.03) 100%);
      border-color: rgba(255,69,96,0.3);
    }
    .signal-pulse {
      width: 10px; height: 10px; border-radius: 50%;
      background: var(--blue);
      animation: pulse 1.5s infinite;
    }
    .signal-bar.signal-long .signal-pulse { background: var(--green); }
    .signal-bar.signal-short .signal-pulse { background: var(--red); }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.5;transform:scale(1.3)} }
    .signal-text {
      font-family: 'JetBrains Mono', monospace;
      font-size: 13px;
      font-weight: 600;
      flex: 1;
    }
    .signal-meta {
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      color: var(--text-secondary);
    }

    /* Header */
    header {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      margin-bottom: 18px;
      flex-wrap: wrap;
      gap: 12px;
    }
    h1 {
      font-size: 24px;
      font-weight: 800;
      letter-spacing: -0.5px;
      background: linear-gradient(135deg, var(--text-primary) 0%, var(--blue) 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .subtitle { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }
    .badge {
      display: inline-block;
      font-family: 'JetBrains Mono', monospace;
      font-size: 10px;
      padding: 3px 8px;
      border-radius: 4px;
      margin-left: 6px;
    }
    .badge-open { background: rgba(0,227,150,0.15); color: var(--green); border: 1px solid rgba(0,227,150,0.3); }
    .badge-closed { background: rgba(255,69,96,0.15); color: var(--red); border: 1px solid rgba(255,69,96,0.3); }
    .header-right {
      display: flex;
      align-items: center;
      gap: 14px;
      font-size: 12px;
      color: var(--text-secondary);
    }
    .header-right a { color: var(--text-muted); text-decoration: none; }
    .header-right a:hover { color: var(--text-primary); }
    .freshness-bar {
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
    }

    /* KPI Cards */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .kpi-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px 16px;
      position: relative;
      overflow: hidden;
      transition: all 0.25s ease;
    }
    .kpi-card:hover { background: var(--bg-card-hover); transform: translateY(-1px); }
    .kpi-card::before {
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0;
      height: 2px;
    }
    .kpi-card.green::before { background: var(--green); }
    .kpi-card.red::before { background: var(--red); }
    .kpi-card.blue::before { background: var(--blue); }
    .kpi-card.gold::before { background: var(--gold); }
    .kpi-card.purple::before { background: var(--purple); }
    .kpi-card.cyan::before { background: var(--cyan); }
    .kpi-label {
      font-size: 9px;
      text-transform: uppercase;
      letter-spacing: 1.2px;
      color: var(--text-muted);
      margin-bottom: 4px;
      font-weight: 600;
    }
    .kpi-value {
      font-family: 'JetBrains Mono', monospace;
      font-size: 20px;
      font-weight: 700;
    }
    .kpi-sub {
      font-family: 'JetBrains Mono', monospace;
      font-size: 10px;
      color: var(--text-secondary);
      margin-top: 3px;
    }
    .kpi-value.positive { color: var(--green); }
    .kpi-value.negative { color: var(--red); }
    .kpi-value.neutral { color: var(--blue); }
    .kpi-value.warn { color: var(--gold); }
    .kpi-value.info { color: var(--purple); }

    /* Toggle Bar */
    .toggle-bar {
      display: flex;
      gap: 0;
      margin-bottom: 18px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 4px;
      width: fit-content;
    }
    .toggle-btn {
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px;
      font-weight: 600;
      padding: 8px 20px;
      border-radius: 7px;
      border: none;
      cursor: pointer;
      background: transparent;
      color: var(--text-muted);
      transition: all 0.25s ease;
      letter-spacing: 0.5px;
    }
    .toggle-btn:hover { color: var(--text-secondary); }
    .toggle-btn.active {
      background: var(--blue);
      color: #fff;
      box-shadow: 0 2px 12px rgba(0,143,251,0.3);
    }

    /* Content cards */
    .chart-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 18px;
      transition: all 0.25s ease;
    }
    .chart-card:hover { border-color: #2a3a50; }
    .chart-title {
      font-size: 13px;
      font-weight: 700;
      margin-bottom: 4px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .chart-title .dot { width: 8px; height: 8px; border-radius: 50%; }
    .chart-desc { font-size: 10px; color: var(--text-muted); margin-bottom: 12px; }

    /* Tab Content Panels */
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }

    /* Overview layout — stacked: price on top, 3 mini charts below */
    .overview-price { min-height: 420px; margin-bottom: 14px; }
    .overview-row {
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 14px;
      margin-bottom: 14px;
    }
    .overview-row .chart-card { min-height: 280px; }

    /* Exposure view */
    .exposure-grid {
      display: grid;
      grid-template-columns: 2fr 1fr 1fr 1fr 1fr;
      gap: 10px;
      height: calc(100vh - 280px);
      min-height: 450px;
    }
    .exposure-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    .exposure-card h3 {
      margin: 0 0 4px;
      font-size: 10px;
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .exposure-plot { flex:1; width:100%; min-height:0; }

    /* Charts grid */
    .charts-2x4 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .charts-2x4 > div { height: 360px; }
    .ht-grid {
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 10px;
    }
    .ht-grid > div { height: 360px; }

    /* Sub-tabs */
    .subtabs {
      display: flex;
      gap: 4px;
      padding: 10px 0;
    }
    .subtab-btn {
      font-family: 'JetBrains Mono', monospace;
      padding: 5px 14px;
      font-size: 11px;
      font-weight: 600;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: transparent;
      color: var(--text-muted);
      cursor: pointer;
      transition: all .15s;
    }
    .subtab-btn:hover { border-color: #444; color: var(--text-primary); }
    .subtab-btn.active { background: rgba(0,143,251,0.15); border-color: var(--blue); color: var(--blue); }

    /* Trade Log */
    .tl-filters {
      display: flex;
      gap: 8px;
      padding: 10px 0;
      flex-wrap: wrap;
      align-items: center;
    }
    .tl-filters select, .tl-filters input {
      background: var(--bg-surface);
      color: var(--text-primary);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 5px 10px;
      font-size: 11px;
      font-family: 'JetBrains Mono', monospace;
    }
    .tl-stats-row {
      display: flex;
      gap: 18px;
      padding: 8px 0;
      font-size: 12px;
      color: var(--text-secondary);
      border-bottom: 1px solid var(--border);
      margin-bottom: 8px;
    }
    .tl-stats-row .stat-val { font-weight: 700; color: var(--text-primary); font-family: 'JetBrains Mono', monospace; }

    /* Table styling */
    .data-table { width: 100%; border-collapse: collapse; font-size: 12px; }
    .data-table th {
      text-align: left;
      padding: 8px 10px;
      font-size: 9px;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--text-muted);
      border-bottom: 2px solid var(--border);
      font-weight: 600;
      white-space: nowrap;
      position: sticky;
      top: 0;
      background: var(--bg-card);
      z-index: 1;
    }
    .data-table td {
      padding: 7px 10px;
      border-bottom: 1px solid var(--bg-surface);
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      white-space: nowrap;
    }
    .data-table tr:hover td { background: var(--bg-surface); }
    .td-green { color: var(--green); }
    .td-red { color: var(--red); }
    .td-blue { color: var(--blue); }
    .td-gold { color: var(--gold); }
    .setup-pill {
      font-size: 9px;
      font-weight: 600;
      padding: 2px 6px;
      border-radius: 4px;
      white-space: nowrap;
      display: inline-block;
    }
    .win-bar { height: 5px; border-radius: 3px; background: var(--border); position: relative; min-width: 50px; display: inline-block; vertical-align: middle; }
    .win-bar-fill { height: 100%; border-radius: 3px; background: var(--green); }

    /* Signals table on overview */
    .signals-table { width: 100%; }
    .signals-table td { padding: 6px 10px; font-size: 11px; }

    /* Placeholder */
    .placeholder-panel {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 400px;
      color: var(--text-muted);
      font-size: 14px;
    }
    .placeholder-panel a { color: var(--blue); text-decoration: none; }
    .placeholder-panel a:hover { text-decoration: underline; }

    /* Strike buttons */
    .strike-btn {
      font-family: 'JetBrains Mono', monospace;
      padding: 4px 10px;
      font-size: 11px;
      border: 1px solid var(--border);
      border-radius: 6px;
      background: transparent;
      color: var(--text-muted);
      cursor: pointer;
    }
    .strike-btn:hover { border-color: #444; color: var(--text-primary); }
    .strike-btn.active { background: rgba(0,143,251,0.15); border-color: var(--blue); color: var(--blue); }

    /* Animations */
    @keyframes fadeUp { from { opacity:0; transform:translateY(12px); } to { opacity:1; transform:translateY(0); } }
    .animate { animation: fadeUp 0.4s ease forwards; opacity: 0; }
    .d1{animation-delay:.05s} .d2{animation-delay:.1s} .d3{animation-delay:.15s} .d4{animation-delay:.2s} .d5{animation-delay:.25s}

    /* Responsive */
    @media (max-width: 900px) {
      .container { padding: 12px; }
      .kpi-grid { grid-template-columns: repeat(3, 1fr); }
      .overview-row { grid-template-columns: 1fr; }
      .exposure-grid { grid-template-columns: 1fr; height: auto; }
      .exposure-card { min-height: 280px; }
      .charts-2x4 { grid-template-columns: 1fr; }
      .ht-grid { grid-template-columns: 1fr; }
      .toggle-bar { flex-wrap: wrap; }
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: var(--bg-primary); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #3a4a5a; }
  </style>
</head>
<body>
<div class="grain"></div>
<div class="container">

  <!-- Signal Bar -->
  <div class="signal-bar" id="signalBar">
    <div class="signal-pulse"></div>
    <div class="signal-text" id="signalText"></div>
    <div class="signal-meta" id="signalMeta"></div>
  </div>

  <!-- Header -->
  <header class="animate d1">
    <div>
      <h1>0DTE Alpha</h1>
      <div class="subtitle">Trading Cockpit <span class="badge" id="marketBadge">__STATUS_TEXT__</span></div>
    </div>
    <div class="header-right">
      <div class="freshness-bar" id="freshnessBar">Loading...</div>
      <span id="userEmail">__USER_EMAIL__</span>
      <a href="/dashboard" title="Original dashboard">V1</a>
      <a href="/logout">Sign out</a>
    </div>
  </header>

  <!-- KPI Cards -->
  <div class="kpi-grid animate d2" id="kpiGrid">
    <div class="kpi-card blue" id="kpiSpot">
      <div class="kpi-label">SPX Spot</div>
      <div class="kpi-value neutral" id="kpiSpotVal">--</div>
      <div class="kpi-sub" id="kpiSpotSub">loading</div>
    </div>
    <div class="kpi-card purple" id="kpiParadigm">
      <div class="kpi-label">Paradigm</div>
      <div class="kpi-value info" id="kpiParadigmVal">--</div>
      <div class="kpi-sub" id="kpiParadigmSub">&nbsp;</div>
    </div>
    <div class="kpi-card cyan" id="kpiDD">
      <div class="kpi-label">DD Hedging</div>
      <div class="kpi-value" id="kpiDDVal" style="color:var(--cyan)">--</div>
      <div class="kpi-sub" id="kpiDDSub">&nbsp;</div>
    </div>
    <div class="kpi-card gold" id="kpiCharm">
      <div class="kpi-label">Agg. Charm</div>
      <div class="kpi-value warn" id="kpiCharmVal">--</div>
      <div class="kpi-sub" id="kpiCharmSub">&nbsp;</div>
    </div>
    <div class="kpi-card red" id="kpiVIX">
      <div class="kpi-label">VIX / Overvix</div>
      <div class="kpi-value" id="kpiVIXVal" style="color:var(--red)">--</div>
      <div class="kpi-sub" id="kpiVIXSub">&nbsp;</div>
    </div>
    <div class="kpi-card green" id="kpiPnL">
      <div class="kpi-label">Today P&L</div>
      <div class="kpi-value positive" id="kpiPnLVal">--</div>
      <div class="kpi-sub" id="kpiPnLSub">&nbsp;</div>
    </div>
  </div>

  <!-- Toggle Bar -->
  <div class="toggle-bar animate d3">
    <button class="toggle-btn active" data-tab="overview">OVERVIEW</button>
    <button class="toggle-btn" data-tab="exposure">EXPOSURE</button>
    <button class="toggle-btn" data-tab="charts">CHARTS</button>
    <button class="toggle-btn" data-tab="esdelta">ES DELTA</button>
    <button class="toggle-btn" data-tab="tradelog">TRADE LOG</button>
    <button class="toggle-btn" data-tab="historical">HISTORICAL</button>
  </div>

  <!-- ===== OVERVIEW TAB ===== -->
  <div class="tab-panel active" id="panelOverview">
    <!-- Price chart — full width -->
    <div class="chart-card overview-price animate d4">
      <div class="chart-title"><span class="dot" style="background:var(--blue)"></span> SPX Price + Key Levels</div>
      <div class="chart-desc">3-min candles with Target, LIS, GEX levels</div>
      <div id="overviewPricePlot" style="width:100%;height:380px"></div>
    </div>
    <!-- 3 mini exposure charts — equal width row -->
    <div class="overview-row animate d4">
      <div class="chart-card">
        <div class="chart-title"><span class="dot" style="background:var(--green)"></span> Net GEX</div>
        <div id="overviewGexPlot" style="width:100%;height:240px"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title"><span class="dot" style="background:var(--gold)"></span> Charm</div>
        <div id="overviewCharmPlot" style="width:100%;height:240px"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title"><span class="dot" style="background:var(--purple)"></span> Delta Decay</div>
        <div id="overviewDDPlot" style="width:100%;height:240px"></div>
      </div>
    </div>
    <!-- Recent signals -->
    <div class="chart-card animate d5">
      <div class="chart-title"><span class="dot" style="background:var(--cyan)"></span> Today's Signals</div>
      <div class="chart-desc">Most recent setup detections</div>
      <div style="max-height:280px;overflow-y:auto">
        <table class="data-table signals-table" id="overviewSignalsTable">
          <thead><tr><th>Time</th><th>Setup</th><th>Dir</th><th>Grade</th><th>Entry</th><th>Target</th><th>Stop</th><th>Align</th><th>Result</th><th>P&L</th></tr></thead>
          <tbody id="overviewSignalsBody"><tr><td colspan="10" style="text-align:center;color:var(--text-muted);padding:20px">Loading signals...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ===== EXPOSURE TAB ===== -->
  <div class="tab-panel" id="panelExposure">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <div class="chart-title"><span class="dot" style="background:var(--blue)"></span> Exposure View</div>
      <div style="display:flex;gap:6px;align-items:center">
        <span style="font-size:10px;color:var(--text-muted)">Strikes:</span>
        <button class="strike-btn" data-strikes="20">20</button>
        <button class="strike-btn active" data-strikes="30">30</button>
        <button class="strike-btn" data-strikes="40">40</button>
      </div>
    </div>
    <!-- Statistics Plot (price candles + levels) -->
    <div class="chart-card" style="margin-bottom:14px">
      <div id="exposureStatisticsPlot" style="width:100%;height:460px"></div>
      <div style="margin-top:6px;font-size:10px;color:var(--text-muted);display:flex;gap:18px;flex-wrap:wrap">
        <span><span style="color:var(--blue)">&#9632;</span> Target</span>
        <span><span style="color:var(--gold)">&#9632;</span> LIS</span>
        <span><span style="color:var(--green)">&#9632;</span> Max +Gamma</span>
        <span><span style="color:var(--red)">&#9632;</span> Max -Gamma</span>
      </div>
    </div>
    <!-- 5-column exposure grid -->
    <div class="exposure-grid">
      <div class="exposure-card"><h3>SPX 3m</h3><div id="exposureSpxPlot" class="exposure-plot"></div></div>
      <div class="exposure-card"><h3>Net GEX</h3><div id="exposureGexPlot" class="exposure-plot"></div></div>
      <div class="exposure-card"><h3>Charm</h3><div id="exposureCharmPlot" class="exposure-plot"></div></div>
      <div class="exposure-card"><h3>Delta Decay</h3><div id="exposureDDPlot" class="exposure-plot"></div></div>
      <div class="exposure-card"><h3>Volume</h3><div id="exposureVolPlot" class="exposure-plot"></div></div>
    </div>
  </div>

  <!-- ===== CHARTS TAB ===== -->
  <div class="tab-panel" id="panelCharts">
    <div class="subtabs" id="chartsSubtabs">
      <button class="subtab-btn active" data-subtab="0dte">0DTE Charts</button>
      <button class="subtab-btn" data-subtab="htf">HTF Charts</button>
    </div>
    <div id="chartsView0dte">
      <div class="charts-2x4">
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--green)"></span> Net GEX</div><div id="chartGexNet" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--green)"></span> GEX (Call &amp; Put)</div><div id="chartGexCP" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--gold)"></span> Charm</div><div id="chartCharm" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--gold)"></span> Vanna 0DTE</div><div id="chartVanna0dte" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--purple)"></span> Delta Decay</div><div id="chartDD" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--cyan)"></span> Gamma 0DTE</div><div id="chartGamma0dte" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--blue)"></span> Open Interest</div><div id="chartOI" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title"><span class="dot" style="background:var(--blue)"></span> Volume</div><div id="chartVol" style="width:100%;height:320px"></div></div>
      </div>
    </div>
    <div id="chartsViewHTF" style="display:none">
      <div class="ht-grid">
        <div class="chart-card"><div class="chart-title">Weekly Vanna</div><div id="chartHTFWeeklyVanna" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title">Monthly Vanna</div><div id="chartHTFMonthlyVanna" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title">All-Exp Vanna</div><div id="chartHTFAllVanna" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title">Weekly Gamma</div><div id="chartHTFWeeklyGamma" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title">Monthly Gamma</div><div id="chartHTFMonthlyGamma" style="width:100%;height:320px"></div></div>
        <div class="chart-card"><div class="chart-title">All-Exp Gamma</div><div id="chartHTFAllGamma" style="width:100%;height:320px"></div></div>
      </div>
    </div>
  </div>

  <!-- ===== ES DELTA TAB ===== -->
  <div class="tab-panel" id="panelEsDelta">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <div class="chart-title"><span class="dot" style="background:var(--green)"></span> ES Range Bars + CVD</div>
      <span id="esDeltaStatus" style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text-muted)">Loading...</span>
    </div>
    <div class="chart-card">
      <div id="esDeltaPlot" style="width:100%;height:calc(100vh - 260px);min-height:500px"></div>
    </div>
  </div>

  <!-- ===== TRADE LOG TAB ===== -->
  <div class="tab-panel" id="panelTradeLog">
    <div class="subtabs" id="tlSubtabs">
      <button class="subtab-btn active" data-subtab="portal">Portal Log</button>
      <button class="subtab-btn" data-subtab="tssim">TS SIM Log</button>
      <button class="subtab-btn" data-subtab="eval">Eval Log</button>
      <button class="subtab-btn" data-subtab="options">Options Log</button>
    </div>
    <div class="tl-filters">
      <select id="tlFilterSetup"><option value="">All Setups</option><option>GEX Long</option><option>AG Short</option><option>BofA Scalp</option><option>ES Absorption</option><option>DD Exhaustion</option><option>Paradigm Reversal</option><option>Skew Charm</option></select>
      <select id="tlFilterResult"><option value="">All Results</option><option value="WIN">WIN</option><option value="LOSS">LOSS</option><option value="EXPIRED">EXPIRED</option><option value="TIMEOUT">TIMEOUT</option><option value="OPEN">OPEN</option></select>
      <select id="tlFilterGrade"><option value="">All Grades</option><option>A+</option><option>A</option><option>A-Entry</option></select>
      <select id="tlFilterDate"><option value="">All Dates</option><option value="today">Today</option><option value="week">This Week</option><option value="month">This Month</option></select>
      <select id="tlFilterAlign"><option value="">All Align</option><option value="3">+3</option><option value="2">+2</option><option value="1">+1</option><option value="0">0</option><option value="-1">-1</option><option value="-2">-2</option><option value="-3">-3</option></select>
      <select id="tlFilterStrategy"><option value="">All Strategies</option><option value="v8">V8 (live)</option><option value="v7ag">V7+AG</option><option value="sc">SC Only</option><option value="r1">R1 (basic)</option></select>
      <input type="text" id="tlSearch" placeholder="Search...">
    </div>
    <div class="tl-stats-row" id="tlStats"></div>
    <div class="chart-card" style="padding:0;overflow:auto;max-height:calc(100vh - 340px)">
      <table class="data-table" id="tlTable">
        <thead id="tlHeader"></thead>
        <tbody id="tlBody"><tr><td colspan="14" style="text-align:center;color:var(--text-muted);padding:30px">Loading trade log...</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- ===== HISTORICAL TAB ===== -->
  <div class="tab-panel" id="panelHistorical">
    <div class="chart-card placeholder-panel">
      <div style="text-align:center">
        <div style="font-size:18px;margin-bottom:8px">Historical Playback + Regime Map</div>
        <div>Use <a href="/dashboard">original dashboard</a> for playback controls — coming to V2 soon.</div>
      </div>
    </div>
  </div>

</div>

<script>
// ===== Configuration =====
const PULL_MS = __PULL_MS__;
const IS_ADMIN = __IS_ADMIN__;
const ET_TZ = 'America/New_York';

// ===== Helpers =====
async function fetchJSON(url) {
  const r = await fetch(url, {cache: 'no-store'});
  return r.json();
}

function fmtTimeET(iso) {
  if (!iso) return '--:--';
  try {
    return new Date(iso).toLocaleTimeString('en-US', {timeZone: ET_TZ, hour:'2-digit', minute:'2-digit', hour12:false});
  } catch { return '--:--'; }
}

function fmtDateTimeET(iso) {
  if (!iso) return '--';
  try {
    const d = new Date(iso);
    const date = d.toLocaleDateString('en-US', {timeZone:ET_TZ, month:'2-digit', day:'2-digit'});
    const time = d.toLocaleTimeString('en-US', {timeZone:ET_TZ, hour:'2-digit', minute:'2-digit', hour12:false});
    return date + ' ' + time;
  } catch { return '--'; }
}

function fmtPnl(v) {
  if (v == null || isNaN(v)) return '--';
  const sign = v >= 0 ? '+' : '';
  return sign + v.toFixed(1);
}

function setupColor(name) {
  const colors = {
    'GEX Long': '#22c55e', 'AG Short': '#ef4444', 'BofA Scalp': '#3b82f6',
    'ES Absorption': '#f59e0b', 'DD Exhaustion': '#a855f7', 'Paradigm Reversal': '#06b6d4',
    'Skew Charm': '#ec4899'
  };
  return colors[name] || '#7a8ba3';
}

// ===== Plotly Theme =====
const PL = {
  paper: '#111827',
  plot: '#0d1520',
  grid: '#1a2538',
  font: {color:'#e8edf5', size:11, family:"'Plus Jakarta Sans', sans-serif"},
  tickfont: {size:10, family:"'JetBrains Mono', monospace", color:'#7a8ba3'},
  margin: {t:8, r:10, b:32, l:48},
  marginSmall: {t:4, r:6, b:24, l:40},
  config: {displayModeBar:false, responsive:true}
};

function baseLayout(opts) {
  return {
    paper_bgcolor: PL.paper,
    plot_bgcolor: PL.plot,
    font: PL.font,
    margin: opts.small ? PL.marginSmall : PL.margin,
    xaxis: {gridcolor:PL.grid, tickfont:PL.tickfont, dtick:opts.dtick||10, ...(opts.xaxis||{})},
    yaxis: {gridcolor:PL.grid, tickfont:PL.tickfont, ...(opts.yaxis||{})},
    shapes: opts.shapes || [],
    barmode: opts.barmode || 'group',
    showlegend: opts.showlegend != null ? opts.showlegend : false,
    ...(opts.extra || {})
  };
}

function spotLine(spot, yMin, yMax) {
  if (spot == null) return null;
  return {type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax, xref:'x', yref:'y', line:{color:'#4a5a6a', width:1.5, dash:'dot'}};
}

function barTrace(strikes, values, name, posColor, negColor) {
  const colors = values.map(v => v >= 0 ? (posColor || '#00e396') : (negColor || '#ff4560'));
  return {type:'bar', x:strikes, y:values, marker:{color:colors}, name: name || '',
    hovertemplate: "Strike %{x}<br>%{y:.2f}<extra></extra>"};
}

function emptyAnnotation(msg) {
  return {text:msg, x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#7a8ba3', size:13}};
}

// ===== State =====
let activeTab = 'overview';
let chartsSubTab = '0dte';
let tlSubTab = 'portal';
let timers = {};
let _spotPrice = null;
let _vixValue = null;
let _exposureStrikes = 30;
let _lastSignalId = null; // track last signal to detect new ones

// ===== Tab Management =====
function switchTab(tab) {
  activeTab = tab;
  // Update toggle buttons
  document.querySelectorAll('.toggle-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  // Show/hide panels
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  const panel = document.getElementById('panel' + tab.charAt(0).toUpperCase() + tab.slice(1));
  if (panel) panel.classList.add('active');
  // Map tab names to panel IDs
  const panelMap = {
    overview: 'panelOverview', exposure: 'panelExposure', charts: 'panelCharts',
    esdelta: 'panelEsDelta', tradelog: 'panelTradeLog', historical: 'panelHistorical'
  };
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  const target = document.getElementById(panelMap[tab]);
  if (target) target.classList.add('active');

  // Stop all tab-specific timers
  Object.keys(timers).forEach(k => { if(k !== 'global') { clearInterval(timers[k]); delete timers[k]; } });

  // Start tab-specific polling
  if (tab === 'overview') startOverview();
  else if (tab === 'exposure') startExposure();
  else if (tab === 'charts') startCharts();
  else if (tab === 'esdelta') startEsDelta();
  else if (tab === 'tradelog') startTradeLog();

  try { sessionStorage.setItem('v2_tab', tab); } catch(e) {}
}

// Wire up toggle buttons
document.querySelectorAll('.toggle-btn').forEach(btn => {
  btn.addEventListener('click', () => switchTab(btn.dataset.tab));
});

// ===== Global Polling (runs regardless of tab) =====
async function updateFreshness() {
  try {
    const d = await fetchJSON('/api/data_freshness');
    const sc = {ok:'#00e396', stale:'#feb019', error:'#ff4560', closed:'#4a5568'};
    const ts = d.ts_api || {};
    const vl = d.volland || {};
    const tsC = sc[ts.status] || sc.error;
    const vlC = sc[vl.status] || sc.error;
    const spotStr = d.spot ? d.spot.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}) : '--';
    const vixStr = d.vix ? d.vix.toFixed(2) : '--';

    document.getElementById('freshnessBar').innerHTML =
      'SPX <span style="color:var(--text-primary);font-weight:600">' + spotStr + '</span>' +
      ' &middot; VIX <span style="color:var(--text-primary);font-weight:600">' + vixStr + '</span>' +
      ' &middot; <span style="color:' + tsC + '">TS:' + fmtTimeET(ts.last_update) + '</span>' +
      ' <span style="color:' + vlC + '">Vol:' + fmtTimeET(vl.last_update) + '</span>';

    // Update market badge
    const badge = document.getElementById('marketBadge');
    if (d.market_open) {
      badge.textContent = 'OPEN';
      badge.className = 'badge badge-open';
    } else {
      badge.textContent = 'CLOSED';
      badge.className = 'badge badge-closed';
    }

    if (d.spot) _spotPrice = d.spot;
    if (d.vix) _vixValue = d.vix;
  } catch(e) {
    document.getElementById('freshnessBar').innerHTML = '<span style="color:var(--red)">Error</span>';
  }
}

async function updateKPIs() {
  try {
    const data = await fetchJSON('/api/volland/stats');
    if (!data || data.error) return;
    const s = data.stats || {};

    // Spot
    if (_spotPrice) {
      document.getElementById('kpiSpotVal').textContent = _spotPrice.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2});
    }

    // Paradigm
    if (s.paradigm) {
      const p = s.paradigm;
      const el = document.getElementById('kpiParadigmVal');
      el.textContent = p;
      // Color based on paradigm type
      if (p.includes('GEX') && !p.includes('Anti')) { el.style.color = 'var(--green)'; }
      else if (p.includes('Anti') || p.includes('AG')) { el.style.color = 'var(--red)'; }
      else if (p.includes('BofA')) { el.style.color = 'var(--blue)'; }
      else { el.style.color = 'var(--purple)'; }
    }
    if (s.target) document.getElementById('kpiParadigmSub').textContent = 'T: ' + s.target;

    // DD Hedging
    if (s.delta_decay_hedging) {
      const dd = s.delta_decay_hedging;
      const el = document.getElementById('kpiDDVal');
      el.textContent = dd.replace(/\\s*\\(.*\\)/, '').substring(0, 12);
      el.style.color = (dd.includes('-') || dd.toLowerCase().includes('short')) ? 'var(--red)' : 'var(--green)';
    }

    // Charm
    if (s.aggregated_charm != null) {
      const c = parseFloat(s.aggregated_charm);
      if (!isNaN(c)) {
        const el = document.getElementById('kpiCharmVal');
        const cM = (c / 1e6).toFixed(0);
        el.textContent = (c >= 0 ? '+' : '') + cM + 'M';
        el.style.color = c >= 0 ? 'var(--green)' : 'var(--red)';
      }
    }

    // VIX / Overvix
    const vixEl = document.getElementById('kpiVIXVal');
    const vixSub = document.getElementById('kpiVIXSub');
    // Show actual VIX value from freshness data
    if (_vixValue) {
      vixEl.textContent = _vixValue.toFixed(2);
      vixEl.style.color = _vixValue > 26 ? 'var(--red)' : _vixValue > 20 ? 'var(--gold)' : 'var(--green)';
    }
    if (s.overvix != null) {
      const ov = parseFloat(s.overvix);
      if (!isNaN(ov)) {
        const ovTag = ov >= 2 ? ' [OVERVIX]' : '';
        vixSub.textContent = 'OV: ' + (ov >= 0 ? '+' : '') + ov.toFixed(2) + ovTag;
        vixSub.style.color = ov >= 2 ? 'var(--green)' : ov <= -2 ? 'var(--red)' : '';
      }
    }

    // SVB (Spot-Vol Beta)
    if (s.svb_correlation != null) {
      const svb = parseFloat(s.svb_correlation);
      if (!isNaN(svb)) {
        document.getElementById('kpiDDSub').textContent = 'SVB: ' + (svb >= 0 ? '+' : '') + svb.toFixed(2);
      }
    }

    // LIS in spot sub — show distance from spot
    if (s.lines_in_sand && _spotPrice) {
      // Parse LIS range (e.g. "$5,900 - $5,940" or "5900/5940")
      const nums = s.lines_in_sand.replace(/[$,]/g, '').match(/[0-9]+/g);
      if (nums && nums.length >= 1) {
        const lisLow = parseFloat(nums[0]);
        const lisHigh = nums.length >= 2 ? parseFloat(nums[1]) : lisLow;
        const lisMid = (lisLow + lisHigh) / 2;
        const dist = (_spotPrice - lisMid).toFixed(1);
        document.getElementById('kpiSpotSub').textContent = 'LIS: ' + s.lines_in_sand + ' (' + (dist >= 0 ? '+' : '') + dist + ')';
      } else {
        document.getElementById('kpiSpotSub').textContent = 'LIS: ' + s.lines_in_sand;
      }
    }
  } catch(e) {}

  // Today P&L
  try {
    const logs = await fetchJSON('/api/setup/log?limit=200&date_range=today');
    if (Array.isArray(logs)) {
      let wins = 0, losses = 0, pnl = 0;
      logs.filter(t => t.notified).forEach(t => {
        if (t.outcome_result === 'WIN') { wins++; pnl += (t.outcome_pnl || 0); }
        else if (t.outcome_result === 'LOSS') { losses++; pnl += (t.outcome_pnl || 0); }
        else if (t.outcome_result === 'EXPIRED') { losses++; pnl += (t.outcome_pnl || 0); }
      });
      const total = wins + losses;
      const el = document.getElementById('kpiPnLVal');
      el.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(1) + ' pts';
      el.className = 'kpi-value ' + (pnl >= 0 ? 'positive' : 'negative');
      document.getElementById('kpiPnLSub').textContent = wins + 'W / ' + losses + 'L' + (total ? ' · ' + ((wins/total)*100).toFixed(0) + '%' : '');
    }
  } catch(e) {}
}

// Audio alert for new signals (uses Web Audio API — no file needed)
function playSignalAlert() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    // Play two rising tones
    [0, 0.15].forEach((delay, i) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.value = i === 0 ? 600 : 900;
      osc.type = 'sine';
      gain.gain.value = 0.15;
      gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + delay + 0.3);
      osc.start(ctx.currentTime + delay);
      osc.stop(ctx.currentTime + delay + 0.3);
    });
  } catch(e) {}
}

async function updateSignals() {
  try {
    const logs = await fetchJSON('/api/setup/log?limit=5&date_range=today');
    if (!Array.isArray(logs) || !logs.length) {
      document.getElementById('signalBar').classList.remove('active');
      return;
    }
    // Check if most recent signal is within 5 minutes
    const latest = logs[0];
    const age = Date.now() - new Date(latest.ts).getTime();
    const ageMin = age / 60000;
    const signalId = latest.id || latest.ts;

    if (ageMin < 5 && !latest.outcome_result) {
      const bar = document.getElementById('signalBar');
      bar.classList.add('active');
      bar.classList.remove('signal-long', 'signal-short');
      bar.classList.add(latest.direction === 'LONG' ? 'signal-long' : 'signal-short');

      const dir = latest.direction === 'LONG' ? '&#9650;' : '&#9660;';
      const alignStr = latest.alignment != null ? ' [align ' + (latest.alignment >= 0 ? '+' : '') + latest.alignment + ']' : '';
      const stopStr = latest.stop_level ? ' SL:' + latest.stop_level.toFixed(0) : '';
      document.getElementById('signalText').innerHTML =
        '<span style="color:' + setupColor(latest.setup_name) + '">' + latest.setup_name + '</span> ' +
        latest.grade + ' ' + dir + ' ' +
        (latest.spot ? latest.spot.toFixed(1) : '--') +
        (latest.target_level ? ' &rarr; ' + latest.target_level.toFixed(1) : '') +
        stopStr + alignStr;
      document.getElementById('signalMeta').textContent = Math.floor(ageMin) + 'm ago';

      // Play sound on NEW signal only
      if (signalId !== _lastSignalId) {
        _lastSignalId = signalId;
        playSignalAlert();
      }
    } else {
      document.getElementById('signalBar').classList.remove('active');
    }
  } catch(e) {
    document.getElementById('signalBar').classList.remove('active');
  }
}

// Start global polling
updateFreshness();
updateKPIs();
updateSignals();
timers.global = setInterval(() => { updateFreshness(); updateSignals(); updateKPIs(); }, PULL_MS);

// ===== OVERVIEW TAB =====
async function renderOverview() {
  try {
    const [candles, levels, series, charm, dd] = await Promise.all([
      fetchJSON('/api/spx_candles?bars=80'),
      fetchJSON('/api/statistics_levels'),
      fetchJSON('/api/series'),
      fetchJSON('/api/volland/vanna_window?limit=30'),
      fetchJSON('/api/volland/delta_decay_window?limit=30')
    ]);

    // --- Price Chart with Levels ---
    const c = candles.candles || [];
    if (c.length) {
      const times = c.map(b => b.time);
      const opens = c.map(b => b.open);
      const highs = c.map(b => b.high);
      const lows = c.map(b => b.low);
      const closes = c.map(b => b.close);

      const traces = [{
        type: 'candlestick', x: times, open: opens, high: highs, low: lows, close: closes,
        increasing: {line:{color:'#00e396',width:1.5}, fillcolor:'rgba(0,227,150,0.4)'},
        decreasing: {line:{color:'#ff4560',width:1.5}, fillcolor:'rgba(255,69,96,0.4)'},
        hoverinfo: 'x+open+high+low+close'
      }];

      const shapes = [];
      const annots = [];
      function addLevel(val, color, label) {
        if (val == null) return;
        shapes.push({type:'line', x0:times[0], x1:times[times.length-1], y0:val, y1:val, xref:'x', yref:'y', line:{color:color, width:1.5, dash:'dash'}});
        annots.push({x:times[times.length-1], y:val, xref:'x', yref:'y', text:label + ' ' + val.toFixed(0), showarrow:false, font:{size:10, color:color, family:"'JetBrains Mono'"}, xanchor:'left', bgcolor:'rgba(10,14,23,0.8)', borderpad:2});
      }
      if (levels) {
        addLevel(levels.target, '#008ffb', 'TGT');
        addLevel(levels.lis_low, '#feb019', 'LIS');
        if (levels.lis_high && levels.lis_high !== levels.lis_low) addLevel(levels.lis_high, '#feb019', 'LIS');
        addLevel(levels.max_pos_gamma, '#00e396', '+GEX');
        addLevel(levels.max_neg_gamma, '#ff4560', '-GEX');
      }

      Plotly.react('overviewPricePlot', traces, {
        ...baseLayout({xaxis:{rangeslider:{visible:false}, type:'date'}, shapes:shapes}),
        annotations: annots,
        margin: {t:8, r:80, b:32, l:48},
        yaxis: {gridcolor:PL.grid, tickfont:PL.tickfont, side:'right'}
      }, PL.config);
    }

    // --- Mini GEX ---
    if (series && series.strikes && series.netGEX) {
      const s = series.strikes, g = series.netGEX;
      const spot = series.spot;
      const yMin = Math.min(...g), yMax = Math.max(...g);
      const sh = spotLine(spot, yMin, yMax);
      Plotly.react('overviewGexPlot', [barTrace(s, g, 'Net GEX')], baseLayout({small:true, shapes:sh?[sh]:[], dtick:20}), PL.config);
    }

    // --- Mini Charm ---
    if (charm && charm.points && charm.points.length) {
      const pts = charm.points;
      const s = pts.map(p => p.strike), v = pts.map(p => p.vanna);
      const spot = series ? series.spot : null;
      const yMin = Math.min(...v), yMax = Math.max(...v);
      const sh = spotLine(spot, yMin, yMax);
      Plotly.react('overviewCharmPlot', [barTrace(s, v, 'Charm')], baseLayout({small:true, shapes:sh?[sh]:[], dtick:20}), PL.config);
    }

    // --- Mini Delta Decay ---
    if (dd && dd.points && dd.points.length) {
      const pts = dd.points;
      const s = pts.map(p => p.strike), v = pts.map(p => p.delta_decay || p.vanna || 0);
      const spot = series ? series.spot : null;
      const yMin = Math.min(...v), yMax = Math.max(...v);
      const sh = spotLine(spot, yMin, yMax);
      Plotly.react('overviewDDPlot', [barTrace(s, v, 'DD')], baseLayout({small:true, shapes:sh?[sh]:[], dtick:20}), PL.config);
    }

  } catch(e) {
    console.error('Overview render error:', e);
  }

  // --- Recent signals table ---
  try {
    const logs = await fetchJSON('/api/setup/log?limit=15&date_range=today');
    const tbody = document.getElementById('overviewSignalsBody');
    if (!Array.isArray(logs) || !logs.length) {
      tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--text-muted);padding:20px">No signals today</td></tr>';
      return;
    }
    let html = '';
    logs.forEach(t => {
      const dirC = t.direction === 'LONG' ? 'td-green' : 'td-red';
      const dirS = t.direction === 'LONG' ? '&#9650;' : '&#9660;';
      const resC = t.outcome_result === 'WIN' ? 'td-green' : t.outcome_result === 'LOSS' ? 'td-red' : 'td-gold';
      const bgColor = setupColor(t.setup_name);
      html += '<tr>' +
        '<td>' + fmtTimeET(t.ts) + '</td>' +
        '<td><span class="setup-pill" style="background:' + bgColor + '22;color:' + bgColor + ';border:1px solid ' + bgColor + '44">' + (t.setup_name || '--') + '</span></td>' +
        '<td class="' + dirC + '">' + dirS + '</td>' +
        '<td>' + (t.grade || '--') + '</td>' +
        '<td>' + (t.spot ? t.spot.toFixed(1) : '--') + '</td>' +
        '<td>' + (t.target_level ? t.target_level.toFixed(1) : '--') + '</td>' +
        '<td>' + (t.stop_level ? t.stop_level.toFixed(1) : '--') + '</td>' +
        '<td>' + (t.alignment != null ? (t.alignment >= 0 ? '+' : '') + t.alignment : '--') + '</td>' +
        '<td class="' + resC + '">' + (t.outcome_result || 'OPEN') + '</td>' +
        '<td class="' + (t.outcome_pnl >= 0 ? 'td-green' : 'td-red') + '">' + fmtPnl(t.outcome_pnl) + '</td>' +
        '</tr>';
    });
    tbody.innerHTML = html;
  } catch(e) {}
}

function startOverview() {
  renderOverview();
  timers.overview = setInterval(renderOverview, PULL_MS);
}

// ===== EXPOSURE TAB =====
// Shared Y range for synced strike axis across all 5 exposure charts
let _expYRange = null;
let _expBaseYRange = null;
let _expZoomSyncing = false;

function computeExposureYRange(strikes, spot) {
  if (_expYRange) return _expYRange; // preserve user zoom
  if (!strikes || !strikes.length) return null;
  const half = Math.floor(_exposureStrikes / 2);
  const above = strikes.filter(s => s >= spot).slice(0, half);
  const below = strikes.filter(s => s < spot).slice(-half);
  const selected = [...below, ...above].sort((a,b) => a-b);
  if (!selected.length) return {min: Math.min(...strikes), max: Math.max(...strikes)};
  const yMin = Math.min(...selected), yMax = Math.max(...selected);
  const pad = (yMax - yMin) * 0.02 || 1;
  const range = {min: yMin - pad, max: yMax + pad};
  _expBaseYRange = range;
  return range;
}

function horizontalSpot(spot, xMin, xMax) {
  if (spot == null) return null;
  return {type:'line', y0:spot, y1:spot, x0:xMin, x1:xMax, xref:'x', yref:'y', line:{color:'#008ffb', width:1.5, dash:'dot'}};
}

function setupExposureZoomSync() {
  const divIds = ['exposureSpxPlot','exposureGexPlot','exposureCharmPlot','exposureDDPlot','exposureVolPlot'];
  divIds.forEach(id => {
    const el = document.getElementById(id);
    if (!el || el._v2ZoomWired) return;
    el._v2ZoomWired = true;
    el.on('plotly_relayout', function(ev) {
      if (_expZoomSyncing) return;
      const y0 = ev['yaxis.range[0]'], y1 = ev['yaxis.range[1]'];
      if (y0 !== undefined && y1 !== undefined) {
        _expZoomSyncing = true;
        _expYRange = {min: y0, max: y1};
        divIds.forEach(otherId => {
          if (otherId !== id) {
            const otherEl = document.getElementById(otherId);
            if (otherEl && otherEl._fullLayout) Plotly.relayout(otherEl, {'yaxis.range': [y0, y1]});
          }
        });
        setTimeout(() => { _expZoomSyncing = false; }, 100);
      }
      if (ev['yaxis.autorange']) {
        _expYRange = null;
        _expZoomSyncing = true;
        divIds.forEach(otherId => {
          if (otherId !== id && _expBaseYRange) {
            const otherEl = document.getElementById(otherId);
            if (otherEl && otherEl._fullLayout) Plotly.relayout(otherEl, {'yaxis.range': [_expBaseYRange.min, _expBaseYRange.max]});
          }
        });
        setTimeout(() => { _expZoomSyncing = false; }, 100);
      }
    });
  });
}

function expBarLayout(yRange, hasLabels) {
  return {
    paper_bgcolor: PL.paper, plot_bgcolor: PL.plot, font: PL.font,
    margin: hasLabels ? {l:50, r:8, t:4, b:24} : {l:8, r:8, t:4, b:24},
    xaxis: {gridcolor:PL.grid, tickfont:PL.tickfont, zeroline:true, zerolinecolor:'#333'},
    yaxis: {gridcolor:PL.grid, tickfont:{...PL.tickfont, size:9}, range:[yRange.min, yRange.max],
      showticklabels: hasLabels, dtick:5, fixedrange:false, side: hasLabels ? 'left' : 'left'},
    showlegend: false
  };
}

async function renderExposure() {
  try {
    const [series, candles, levels, charm, dd] = await Promise.all([
      fetchJSON('/api/series'),
      fetchJSON('/api/spx_candles?bars=60'),
      fetchJSON('/api/statistics_levels'),
      fetchJSON('/api/volland/vanna_window?limit=' + _exposureStrikes),
      fetchJSON('/api/volland/delta_decay_window?limit=' + _exposureStrikes)
    ]);

    const spot = series ? series.spot : null;
    const strikes = series ? series.strikes : [];

    // --- Statistics Plot (price candles + levels) ---
    const c = candles.candles || [];
    if (c.length) {
      const times = c.map(b => b.time);
      const traces = [{
        type:'candlestick', x:times, open:c.map(b=>b.open), high:c.map(b=>b.high), low:c.map(b=>b.low), close:c.map(b=>b.close),
        increasing:{line:{color:'#00e396'}}, decreasing:{line:{color:'#ff4560'}},
      }];
      const shapes = [];
      function addLine(v, col) { if(v) shapes.push({type:'line',x0:times[0],x1:times[times.length-1],y0:v,y1:v,xref:'x',yref:'y',line:{color:col,width:1.5,dash:'dash'}}); }
      if(levels) { addLine(levels.target,'#008ffb'); addLine(levels.lis_low,'#feb019'); addLine(levels.lis_high,'#feb019'); addLine(levels.max_pos_gamma,'#00e396'); addLine(levels.max_neg_gamma,'#ff4560'); }
      Plotly.react('exposureStatisticsPlot', traces, baseLayout({xaxis:{rangeslider:{visible:false},type:'date'}, shapes:shapes, extra:{margin:{t:8,r:60,b:32,l:48}}}), PL.config);
    }

    // Compute shared Y range from strikes
    const yRange = computeExposureYRange(strikes, spot);
    if (!yRange) return;

    // --- SPX 3m candles (Y = price/strikes, X = time) ---
    if (c.length) {
      const times = c.map(b => fmtTimeET(b.time));
      const shapes = [];
      const annots = [];
      function addLvl(val, color, label) {
        if (!val) return;
        shapes.push({type:'line', y0:val, y1:val, x0:0, x1:1, xref:'paper', yref:'y', line:{color:color, width:1.5}});
        annots.push({x:0.01, y:val, xref:'paper', yref:'y', text:label+' '+Math.round(val), showarrow:false, font:{color:color, size:9, family:"'JetBrains Mono'"}, xanchor:'left', yanchor:'bottom', bgcolor:'rgba(10,14,23,0.8)', borderpad:2});
      }
      if (levels) { addLvl(levels.target,'#008ffb','TGT'); addLvl(levels.lis_low,'#feb019','LIS'); if(levels.lis_high && levels.lis_high !== levels.lis_low) addLvl(levels.lis_high,'#feb019','LIS'); addLvl(levels.max_pos_gamma,'#00e396','+G'); addLvl(levels.max_neg_gamma,'#ff4560','-G'); }
      Plotly.react('exposureSpxPlot', [{
        type:'candlestick', x:times, open:c.map(b=>b.open), high:c.map(b=>b.high), low:c.map(b=>b.low), close:c.map(b=>b.close),
        increasing:{line:{color:'#00e396'}, fillcolor:'#00e396'}, decreasing:{line:{color:'#ff4560'}, fillcolor:'#ff4560'},
        hoverinfo:'x+y'
      }], {
        ...expBarLayout(yRange, true),
        xaxis:{gridcolor:PL.grid, tickfont:{size:9}, rangeslider:{visible:false}, type:'category', nticks:8, tickangle:-45},
        shapes: shapes, annotations: annots
      }, {...PL.config, scrollZoom:true});
    }

    // --- Net GEX (horizontal bars, Y = strikes) ---
    if (strikes.length && series.netGEX) {
      const g = series.netGEX;
      const gMax = Math.max(1, ...g.map(v => Math.abs(v))) * 1.1;
      const sh = horizontalSpot(spot, -gMax, gMax);
      Plotly.react('exposureGexPlot', [{
        type:'bar', orientation:'h', x:g, y:strikes,
        marker:{color: g.map(v => v >= 0 ? '#00e396' : '#ff4560')},
        hovertemplate:'Strike %{y}<br>GEX %{x:,.0f}<extra></extra>'
      }], {...expBarLayout(yRange, false), shapes: sh ? [sh] : []}, {...PL.config, scrollZoom:true});
    }

    // --- Charm (horizontal bars) ---
    if (charm && charm.points && charm.points.length) {
      const pts = charm.points, s = pts.map(p=>p.strike), v = pts.map(p=>p.vanna);
      const vMax = Math.max(1, ...v.map(x => Math.abs(x))) * 1.1;
      const sh = horizontalSpot(spot, -vMax, vMax);
      Plotly.react('exposureCharmPlot', [{
        type:'bar', orientation:'h', x:v, y:s,
        marker:{color: v.map(x => x >= 0 ? '#00e396' : '#ff4560')},
        hovertemplate:'Strike %{y}<br>Charm %{x:,.0f}<extra></extra>'
      }], {...expBarLayout(yRange, false), shapes: sh ? [sh] : []}, {...PL.config, scrollZoom:true});
    }

    // --- Delta Decay (horizontal bars) ---
    if (dd && dd.points && dd.points.length) {
      const pts = dd.points, s = pts.map(p=>p.strike), v = pts.map(p=>p.delta_decay||p.vanna||0);
      const vMax = Math.max(1, ...v.map(x => Math.abs(x))) * 1.1;
      const sh = horizontalSpot(spot, -vMax, vMax);
      Plotly.react('exposureDDPlot', [{
        type:'bar', orientation:'h', x:v, y:s,
        marker:{color: v.map(x => x >= 0 ? '#00e396' : '#ff4560')},
        hovertemplate:'Strike %{y}<br>DD %{x:,.0f}<extra></extra>'
      }], {...expBarLayout(yRange, false), shapes: sh ? [sh] : []}, {...PL.config, scrollZoom:true});
    }

    // --- Volume (horizontal bars, mirrored: calls right, puts left) ---
    if (strikes.length && series.callVol && series.putVol) {
      const negPuts = series.putVol.map(v => -v);
      const sh = horizontalSpot(spot, Math.min(...negPuts)*1.1, Math.max(...series.callVol)*1.1);
      Plotly.react('exposureVolPlot', [
        {type:'bar', orientation:'h', x:series.callVol, y:strikes, marker:{color:'rgba(0,227,150,0.7)'}, name:'Calls',
          hovertemplate:'Strike %{y}<br>Call Vol %{x:,.0f}<extra></extra>'},
        {type:'bar', orientation:'h', x:negPuts, y:strikes, marker:{color:'rgba(255,69,96,0.7)'}, name:'Puts',
          hovertemplate:'Strike %{y}<br>Put Vol %{customdata:,.0f}<extra></extra>', customdata:series.putVol}
      ], {...expBarLayout(yRange, false), barmode:'overlay'}, {...PL.config, scrollZoom:true});
    }

    // Wire up zoom sync after first render
    setupExposureZoomSync();

  } catch(e) {
    console.error('Exposure render error:', e);
  }
}

function startExposure() {
  renderExposure();
  timers.exposure = setInterval(renderExposure, PULL_MS);
}

// Strike button handlers
document.querySelectorAll('[data-strikes]').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('[data-strikes]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    _exposureStrikes = parseInt(btn.dataset.strikes);
    _expYRange = null; _expBaseYRange = null; // reset zoom on strike change
    if (activeTab === 'exposure') renderExposure();
  });
});

// ===== CHARTS TAB =====
async function renderCharts0dte() {
  try {
    const [series, charm, dd] = await Promise.all([
      fetchJSON('/api/series'),
      fetchJSON('/api/volland/vanna_window?limit=40'),
      fetchJSON('/api/volland/delta_decay_window?limit=40')
    ]);

    const spot = series ? series.spot : null;
    const strikes = series ? series.strikes : [];

    function renderBar(divId, strikeArr, valArr, title) {
      if (!strikeArr || !strikeArr.length) {
        Plotly.react(divId, [], baseLayout({extra:{annotations:[emptyAnnotation('No data')]}}), PL.config);
        return;
      }
      const yMin = Math.min(...valArr), yMax = Math.max(...valArr);
      const sh = spotLine(spot, yMin, yMax);
      Plotly.react(divId, [barTrace(strikeArr, valArr, title)], baseLayout({shapes:sh?[sh]:[]}), PL.config);
    }

    function renderCallPut(divId, strikeArr, callArr, putArr, title) {
      if (!strikeArr || !strikeArr.length) {
        Plotly.react(divId, [], baseLayout({extra:{annotations:[emptyAnnotation('No data')]}}), PL.config);
        return;
      }
      Plotly.react(divId, [
        {type:'bar', x:strikeArr, y:callArr, marker:{color:'#00e396'}, name:'Calls', offsetgroup:'c'},
        {type:'bar', x:strikeArr, y:putArr, marker:{color:'#ff4560'}, name:'Puts', offsetgroup:'p'}
      ], baseLayout({barmode:'group', showlegend:true, extra:{legend:{font:{size:10,color:'#7a8ba3'}}}}), PL.config);
    }

    // Net GEX
    if (strikes.length && series.netGEX) renderBar('chartGexNet', strikes, series.netGEX, 'GEX');
    // GEX Call & Put
    if (strikes.length && series.callGEX) renderCallPut('chartGexCP', strikes, series.callGEX, series.putGEX, 'GEX');
    // Charm
    if (charm && charm.points) {
      const pts = charm.points;
      renderBar('chartCharm', pts.map(p=>p.strike), pts.map(p=>p.vanna), 'Charm');
    }
    // OI
    if (strikes.length && series.callOI) renderCallPut('chartOI', strikes, series.callOI, series.putOI, 'OI');
    // Volume
    if (strikes.length && series.callVol) renderCallPut('chartVol', strikes, series.callVol, series.putVol, 'Vol');
    // DD
    if (dd && dd.points) {
      const pts = dd.points;
      renderBar('chartDD', pts.map(p=>p.strike), pts.map(p=>p.delta_decay||p.vanna||0), 'DD');
    }

    // Vanna 0DTE + Gamma 0DTE from exposure_window
    try {
      const ew = await fetchJSON('/api/volland/exposure_window');
      if (ew && ew.exposures) {
        const v0 = ew.exposures.find(e => e.type === 'vanna_0dte');
        const g0 = ew.exposures.find(e => e.type === 'gamma_0dte');
        if (v0 && v0.points) renderBar('chartVanna0dte', v0.points.map(p=>p.strike), v0.points.map(p=>p.value), 'Vanna');
        if (g0 && g0.points) renderBar('chartGamma0dte', g0.points.map(p=>p.strike), g0.points.map(p=>p.value), 'Gamma');
      }
    } catch(e) {}

  } catch(e) {
    console.error('Charts 0DTE render error:', e);
  }
}

async function renderChartsHTF() {
  try {
    const ew = await fetchJSON('/api/volland/exposure_window');
    if (!ew || !ew.exposures) return;

    const typeMap = {
      'vanna_weekly': 'chartHTFWeeklyVanna', 'vanna_monthly': 'chartHTFMonthlyVanna', 'vanna_all': 'chartHTFAllVanna',
      'gamma_weekly': 'chartHTFWeeklyGamma', 'gamma_monthly': 'chartHTFMonthlyGamma', 'gamma_all': 'chartHTFAllGamma'
    };

    const spot = ew.spot || _spotPrice;
    ew.exposures.forEach(exp => {
      const divId = typeMap[exp.type];
      if (!divId || !exp.points || !exp.points.length) return;
      const s = exp.points.map(p=>p.strike), v = exp.points.map(p=>p.value);
      const yMin = Math.min(...v), yMax = Math.max(...v);
      const sh = spotLine(spot, yMin, yMax);
      Plotly.react(divId, [barTrace(s,v,exp.type)], baseLayout({shapes:sh?[sh]:[]}), PL.config);
    });
  } catch(e) {
    console.error('HTF render error:', e);
  }
}

function startCharts() {
  if (chartsSubTab === '0dte') {
    renderCharts0dte();
    timers.charts = setInterval(renderCharts0dte, PULL_MS);
  } else {
    renderChartsHTF();
    timers.charts = setInterval(renderChartsHTF, PULL_MS);
  }
}

// Charts sub-tab switching
document.querySelectorAll('#chartsSubtabs .subtab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    chartsSubTab = btn.dataset.subtab;
    document.querySelectorAll('#chartsSubtabs .subtab-btn').forEach(b => b.classList.toggle('active', b.dataset.subtab === chartsSubTab));
    document.getElementById('chartsView0dte').style.display = chartsSubTab === '0dte' ? '' : 'none';
    document.getElementById('chartsViewHTF').style.display = chartsSubTab === 'htf' ? '' : 'none';
    if (timers.charts) { clearInterval(timers.charts); delete timers.charts; }
    startCharts();
  });
});

// ===== ES DELTA TAB =====
async function renderEsDelta() {
  try {
    const data = await fetchJSON('/api/es/delta/rangebars');
    if (!data || !Array.isArray(data) || !data.length) {
      document.getElementById('esDeltaStatus').textContent = 'No range bars';
      Plotly.react('esDeltaPlot', [], baseLayout({extra:{annotations:[emptyAnnotation('No ES range bar data')]}}), PL.config);
      return;
    }

    document.getElementById('esDeltaStatus').textContent = data.length + ' bars · ' + (data[data.length-1].source || 'live');

    const idx = data.map(b => b.idx);
    const traces = [
      // Price candlestick
      {type:'candlestick', x:idx, open:data.map(b=>b.open), high:data.map(b=>b.high), low:data.map(b=>b.low), close:data.map(b=>b.close),
        increasing:{line:{color:'#00e396'}}, decreasing:{line:{color:'#ff4560'}},
        yaxis:'y', xaxis:'x', name:'ES Price'},
      // CVD OHLC
      {type:'candlestick', x:idx,
        open:data.map(b=>b.cvd_open||0), high:data.map(b=>b.cvd_high||0), low:data.map(b=>b.cvd_low||0), close:data.map(b=>b.cvd_close||0),
        increasing:{line:{color:'#008ffb'}}, decreasing:{line:{color:'#ff6b35'}},
        yaxis:'y2', xaxis:'x', name:'CVD'},
      // Volume bars
      {type:'bar', x:idx, y:data.map(b=>b.volume||0),
        marker:{color:data.map(b=>(b.delta||0)>=0?'rgba(0,227,150,0.5)':'rgba(255,69,96,0.5)')},
        yaxis:'y3', xaxis:'x', name:'Volume'}
    ];

    Plotly.react('esDeltaPlot', traces, {
      paper_bgcolor:PL.paper, plot_bgcolor:PL.plot, font:PL.font,
      margin:{t:10,r:60,b:30,l:60},
      showlegend:false,
      xaxis:{gridcolor:PL.grid, tickfont:PL.tickfont, rangeslider:{visible:false}},
      yaxis:{gridcolor:PL.grid, tickfont:PL.tickfont, domain:[0.4,1], title:{text:'ES Price',font:{size:10,color:'#7a8ba3'}}},
      yaxis2:{gridcolor:PL.grid, tickfont:PL.tickfont, domain:[0.15,0.38], title:{text:'CVD',font:{size:10,color:'#7a8ba3'}}},
      yaxis3:{gridcolor:'transparent', tickfont:PL.tickfont, domain:[0,0.13], title:{text:'Vol',font:{size:10,color:'#7a8ba3'}}},
      grid:{rows:3, columns:1, subplots:[['xy'],['xy2'],['xy3']]}
    }, PL.config);

  } catch(e) {
    console.error('ES Delta render error:', e);
  }
}

function startEsDelta() {
  renderEsDelta();
  timers.esdelta = setInterval(renderEsDelta, PULL_MS);
}

// ===== TRADE LOG TAB =====
let _tlActiveSubTab = 'portal';

const TL_COLUMNS = {
  portal: ['#','Setup','Dir','Grade','Score','Entry','Gap','Align','Tgt/Stp','Result','P&L','Dur','Time'],
  tssim: ['#','Setup','Dir','Grade','Time','Entry','Stop','T1','T2','Result','P&L($)','Dur','Status'],
  eval: ['#','Setup','Dir','Grade','Time','Qty','Entry','Stop','Result','P&L($)','Dur','Status'],
  options: ['#','Setup','Dir','Symbol','TheoIn','TheoOut','TheoPnl','SIMIn','SIMOut','SIMPnl','Hold','Time']
};

const TL_ENDPOINTS = {
  portal: '/api/setup/log',
  tssim: '/api/auto-trade/log',
  eval: '/api/eval/log',
  options: '/api/options/log'
};

async function renderTradeLog() {
  const sub = _tlActiveSubTab;
  const endpoint = TL_ENDPOINTS[sub];
  const cols = TL_COLUMNS[sub];

  // Build query params from filters
  const params = new URLSearchParams();
  const setup = document.getElementById('tlFilterSetup').value;
  const result = document.getElementById('tlFilterResult').value;
  const grade = document.getElementById('tlFilterGrade').value;
  const dateRange = document.getElementById('tlFilterDate').value;
  const align = document.getElementById('tlFilterAlign').value;
  const search = document.getElementById('tlSearch').value;

  if (setup) params.set('setup', setup);
  if (result) params.set('result', result);
  if (grade) params.set('grade', grade);
  if (dateRange) params.set('date_range', dateRange);
  if (align) params.set('alignment', align);
  if (search) params.set('search', search);
  params.set('limit', '100');

  const url = endpoint + '?' + params.toString();

  try {
    const logs = await fetchJSON(url);

    // Header
    document.getElementById('tlHeader').innerHTML = '<tr>' + cols.map(c => '<th>' + c + '</th>').join('') + '</tr>';

    // Stats
    if (Array.isArray(logs)) {
      let wins=0,losses=0,pnl=0;
      logs.forEach(t => {
        const r = t.outcome_result || t.result;
        const p = t.outcome_pnl || t.pnl || 0;
        if(r==='WIN'){wins++;pnl+=p;}
        else if(r==='LOSS'||r==='EXPIRED'){losses++;pnl+=p;}
      });
      const total=wins+losses;
      document.getElementById('tlStats').innerHTML =
        '<span>Showing <span class="stat-val">' + logs.length + '</span></span>' +
        '<span class="td-green">' + wins + 'W</span>' +
        '<span class="td-red">' + losses + 'L</span>' +
        (total ? '<span>WR <span class="stat-val">' + ((wins/total)*100).toFixed(0) + '%</span></span>' : '') +
        '<span>P&L <span class="stat-val ' + (pnl>=0?'td-green':'td-red') + '">' + fmtPnl(pnl) + '</span></span>';

      // Rows
      const tbody = document.getElementById('tlBody');
      if (!logs.length) {
        tbody.innerHTML = '<tr><td colspan="' + cols.length + '" style="text-align:center;color:var(--text-muted);padding:30px">No trades found</td></tr>';
        return;
      }

      if (sub === 'portal') {
        tbody.innerHTML = logs.map((t,i) => {
          const dirC = t.direction==='LONG' ? 'td-green' : 'td-red';
          const dirS = t.direction==='LONG' ? '&#9650;' : '&#9660;';
          const resC = t.outcome_result==='WIN' ? 'td-green' : t.outcome_result==='LOSS' ? 'td-red' : 'td-gold';
          const bgC = setupColor(t.setup_name);
          const dur = t.outcome_duration_min != null ? t.outcome_duration_min + 'm' : '--';
          return '<tr>' +
            '<td>' + (i+1) + '</td>' +
            '<td><span class="setup-pill" style="background:'+bgC+'22;color:'+bgC+';border:1px solid '+bgC+'44">'+t.setup_name+'</span></td>' +
            '<td class="'+dirC+'">'+dirS+'</td>' +
            '<td>'+t.grade+'</td>' +
            '<td>'+(t.score||'--')+'</td>' +
            '<td>'+(t.spot?t.spot.toFixed(1):'--')+'</td>' +
            '<td>'+(t.gap_to_lis!=null?t.gap_to_lis.toFixed(1):'--')+'</td>' +
            '<td>'+(t.alignment!=null?(t.alignment>=0?'+':'')+t.alignment:'--')+'</td>' +
            '<td style="font-size:10px">'+(t.target_level?t.target_level.toFixed(0):'--')+'/'+(t.stop_level?t.stop_level.toFixed(0):'--')+'</td>' +
            '<td class="'+resC+'">'+(t.outcome_result||'OPEN')+'</td>' +
            '<td class="'+(t.outcome_pnl>=0?'td-green':'td-red')+'">'+fmtPnl(t.outcome_pnl)+'</td>' +
            '<td>'+dur+'</td>' +
            '<td>'+fmtDateTimeET(t.ts)+'</td>' +
            '</tr>';
        }).join('');
      } else {
        // Generic row rendering for SIM/Eval/Options
        tbody.innerHTML = logs.map((t,i) => {
          const cells = cols.map((col,ci) => {
            if (ci === 0) return '<td>' + (i+1) + '</td>';
            // Try to map common fields
            const val = t[Object.keys(t)[ci]] || '--';
            return '<td>' + val + '</td>';
          }).join('');
          return '<tr>' + cells + '</tr>';
        }).join('');
      }
    }
  } catch(e) {
    document.getElementById('tlBody').innerHTML = '<tr><td colspan="' + cols.length + '" style="text-align:center;color:var(--red);padding:30px">Error: ' + e.message + '</td></tr>';
  }
}

function startTradeLog() {
  renderTradeLog();
  timers.tradelog = setInterval(renderTradeLog, 30000);
}

// Trade log sub-tab switching
document.querySelectorAll('#tlSubtabs .subtab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    _tlActiveSubTab = btn.dataset.subtab;
    document.querySelectorAll('#tlSubtabs .subtab-btn').forEach(b => b.classList.toggle('active', b.dataset.subtab === _tlActiveSubTab));
    renderTradeLog();
  });
});

// Trade log filter change handlers
['tlFilterSetup','tlFilterResult','tlFilterGrade','tlFilterDate','tlFilterAlign','tlFilterStrategy','tlSearch'].forEach(id => {
  const el = document.getElementById(id);
  if (el) el.addEventListener('change', renderTradeLog);
});
document.getElementById('tlSearch').addEventListener('input', renderTradeLog);

// ===== INIT =====
// Restore last tab
try {
  const saved = sessionStorage.getItem('v2_tab');
  if (saved) switchTab(saved);
  else switchTab('overview');
} catch(e) {
  switchTab('overview');
}
</script>
</body>
</html>
"""


@router.get("/v2", response_class=HTMLResponse)
def dashboard_v2(session: str = Cookie(default=None)):
    if not _get_context:
        return HTMLResponse("Dashboard v2 not initialized", status_code=500)
    ctx = _get_context(session)
    if ctx is None:
        return RedirectResponse(url="/", status_code=302)
    html = DASH_V2_TEMPLATE
    for key, val in ctx.items():
        html = html.replace(f"__{key}__", str(val))
    return HTMLResponse(html)
