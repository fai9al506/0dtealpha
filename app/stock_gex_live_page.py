"""Stock GEX Live Scanner Dashboard — Support Bounce Strategy."""

STOCK_GEX_LIVE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock GEX Live Scanner</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#0d1117; color:#c9d1d9; font-family:'Segoe UI',system-ui,sans-serif; font-size:13px; }
.header { background:#161b22; border-bottom:1px solid #30363d; padding:12px 20px; display:flex; justify-content:space-between; align-items:center; }
.header h1 { font-size:18px; color:#58a6ff; }
.tabs { display:flex; gap:4px; background:#161b22; padding:8px 20px; border-bottom:1px solid #30363d; }
.tab { padding:6px 16px; border-radius:6px; cursor:pointer; color:#8b949e; font-size:12px; user-select:none; }
.tab:hover { background:#21262d; color:#c9d1d9; }
.tab.active { background:#1f6feb; color:#fff; }
.layout { display:flex; height:calc(100vh - 90px); }
.sidebar { width:220px; background:#161b22; border-right:1px solid #30363d; overflow-y:auto; flex-shrink:0; }
.sidebar .stock-item { padding:8px 12px; cursor:pointer; border-bottom:1px solid #21262d; display:flex; justify-content:space-between; align-items:center; }
.sidebar .stock-item:hover { background:#21262d; }
.sidebar .stock-item.selected { background:#1f6feb33; border-left:3px solid #1f6feb; }
.sidebar .stock-item .sym { font-weight:700; font-size:13px; }
.sidebar .stock-item .meta { font-size:10px; color:#8b949e; }
.sidebar .stock-item .ratio { font-size:11px; }
.sidebar .stock-item .ratio.good { color:#3fb950; }
.sidebar .stock-item .ratio.bad { color:#f85149; }
.main-panel { flex:1; overflow-y:auto; padding:16px; }
table { width:100%; border-collapse:collapse; font-size:12px; }
th { background:#161b22; color:#8b949e; text-align:left; padding:8px 10px; border-bottom:1px solid #30363d; font-weight:600; position:sticky; top:0; }
td { padding:6px 10px; border-bottom:1px solid #21262d; }
tr:hover { background:#161b22; }
.win { color:#3fb950; }
.loss { color:#f85149; }
.tier-a { color:#f0883e; font-weight:700; }
.tier-b { color:#8b949e; }
.badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:10px; font-weight:600; }
.badge-pass { background:#1a3a2a; color:#3fb950; }
.badge-fail { background:#3a1a1a; color:#f85149; }
.badge-active { background:#1a2a3a; color:#58a6ff; }
.badge-t1 { background:#1a3a2a; color:#3fb950; }
.badge-t2 { background:#1a2a3a; color:#a371f7; }
.badge-eod { background:#3a2a1a; color:#d29922; }
.card { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px; margin-bottom:12px; }
.card h3 { color:#58a6ff; font-size:14px; margin-bottom:8px; }
.kpi-row { display:flex; gap:12px; margin-bottom:16px; flex-wrap:wrap; }
.kpi { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:10px 16px; min-width:120px; }
.kpi .label { color:#8b949e; font-size:10px; text-transform:uppercase; }
.kpi .value { color:#c9d1d9; font-size:18px; font-weight:700; margin-top:2px; }
.kpi .value.green { color:#3fb950; }
.kpi .value.red { color:#f85149; }
.refresh-btn { background:#21262d; border:1px solid #30363d; color:#c9d1d9; padding:6px 14px; border-radius:6px; cursor:pointer; font-size:11px; }
.refresh-btn:hover { background:#30363d; }
.chart-container { height:350px; margin-bottom:16px; }
.info-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:16px; }
.info-item { background:#0d1117; border:1px solid #21262d; border-radius:6px; padding:8px 12px; }
.info-item .lbl { color:#8b949e; font-size:10px; }
.info-item .val { font-size:14px; font-weight:600; margin-top:2px; }
#lastUpdate { color:#484f58; font-size:11px; }
</style>
</head>
<body>
<div class="header">
  <h1>Stock GEX Live Scanner</h1>
  <div>
    <button class="refresh-btn" onclick="doRefresh()">Refresh</button>
    <button class="refresh-btn" onclick="triggerScan()">Force Scan</button>
    <span id="lastUpdate"></span>
  </div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('chart',this)">GEX Chart</div>
  <div class="tab" onclick="showTab('watchlist',this)">Watchlist</div>
  <div class="tab" onclick="showTab('active',this)">Active</div>
  <div class="tab" onclick="showTab('log',this)">Trade Log</div>
  <div class="tab" onclick="showTab('levels',this)">All Levels</div>
</div>
<div class="layout">
  <div class="sidebar" id="sidebar"></div>
  <div class="main-panel" id="main"></div>
</div>

<script>
let currentTab = 'chart';
let selectedSym = null;
let data = {};

function showTab(tab, el) {
  currentTab = tab;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  render();
}

function selectStock(sym) {
  selectedSym = sym;
  document.querySelectorAll('.stock-item').forEach(el => el.classList.remove('selected'));
  const el = document.querySelector('[data-sym="'+sym+'"]');
  if (el) el.classList.add('selected');
  render();
}

async function doRefresh() {
  try {
    const results = await Promise.allSettled([
      fetch('/api/stock-gex-live/watchlist').then(r=>r.json()),
      fetch('/api/stock-gex-live/active').then(r=>r.json()),
      fetch('/api/stock-gex-live/trades?days=30').then(r=>r.json()),
      fetch('/api/stock-gex-live/levels').then(r=>r.json()),
      fetch('/api/stock-gex-live/status').then(r=>r.json()),
    ]);
    const wl = results[0].status==='fulfilled' ? results[0].value : {};
    const active = results[1].status==='fulfilled' ? results[1].value : [];
    const trades = results[2].status==='fulfilled' ? results[2].value : [];
    const levels = results[3].status==='fulfilled' ? results[3].value : {};
    const status = results[4].status==='fulfilled' ? results[4].value : {};
    // Handle error responses
    data = {
      watchlist: (wl && !wl.error) ? wl : {},
      active: Array.isArray(active) ? active : [],
      trades: Array.isArray(trades) ? trades : [],
      levels: (levels && !levels.error) ? levels : {},
      status: (status && !status.error) ? status : {},
    };
    renderSidebar();
    render();
    document.getElementById('lastUpdate').textContent =
      new Date().toLocaleTimeString() +
      ' | WL:' + (data.status.watchlist_count||0) +
      ' | Active:' + (data.status.active_trades||0);
  } catch(e) {
    console.error('Refresh error:', e);
    render();
  }
}

function renderSidebar() {
  const levels = data.levels || {};
  const wl = data.watchlist || {};
  const keys = Object.keys(levels).sort();
  let html = '';
  for (const sym of keys) {
    const s = levels[sym];
    const onWL = sym in wl;
    const cls = sym === selectedSym ? 'stock-item selected' : 'stock-item';
    const rCls = (s.ratio||0) >= 3 ? 'ratio good' : 'ratio bad';
    html += '<div class="'+cls+'" data-sym="'+sym+'" onclick="selectStock(\''+sym+'\')">';
    html += '<div><span class="sym">'+sym+'</span>';
    if (onWL) html += ' <span class="badge badge-pass" style="font-size:8px">WL</span>';
    html += '<br><span class="meta">$'+(s.spot||0).toFixed(1)+'</span></div>';
    html += '<span class="'+rCls+'">'+(s.ratio||0)+'x</span>';
    html += '</div>';
  }
  document.getElementById('sidebar').innerHTML = html;
  if (!selectedSym && keys.length) {
    // Auto-select first watchlist stock or first stock
    const wlKeys = Object.keys(wl);
    selectedSym = wlKeys.length ? wlKeys[0] : keys[0];
    const el = document.querySelector('[data-sym="'+selectedSym+'"]');
    if (el) el.classList.add('selected');
  }
}

function render() {
  const el = document.getElementById('main');
  if (currentTab === 'chart') el.innerHTML = renderChart();
  else if (currentTab === 'watchlist') el.innerHTML = renderWatchlist();
  else if (currentTab === 'active') el.innerHTML = renderActive();
  else if (currentTab === 'log') el.innerHTML = renderLog();
  else if (currentTab === 'levels') el.innerHTML = renderLevels();
}

function renderChart() {
  if (!selectedSym) return '<div class="card"><h3>Select a stock from the sidebar</h3></div>';
  const levels = data.levels || {};
  const s = levels[selectedSym];
  if (!s) return '<div class="card"><h3>No GEX data for '+selectedSym+'</h3></div>';

  const wl = (data.watchlist||{})[selectedSym];
  const negLevels = s.neg_levels || [];
  const posLevels = s.pos_levels || [];
  const allLevels = [...negLevels, ...posLevels].sort((a,b) => a.strike - b.strike);

  // Info cards
  let html = '<div class="info-grid">';
  html += '<div class="info-item"><div class="lbl">Spot</div><div class="val">$'+(s.spot||0).toFixed(2)+'</div></div>';
  html += '<div class="info-item"><div class="lbl">GEX Ratio</div><div class="val" style="color:'+((s.ratio||0)>=3?'#3fb950':'#f85149')+'">'+(s.ratio||0)+'x</div></div>';
  html += '<div class="info-item"><div class="lbl">Highest -GEX</div><div class="val" style="color:#f85149">$'+(s.highest_neg||0).toFixed(0)+'</div></div>';
  html += '<div class="info-item"><div class="lbl">Lowest +GEX</div><div class="val" style="color:#3fb950">$'+(s.lowest_pos||0).toFixed(0)+'</div></div>';
  html += '<div class="info-item"><div class="lbl">Zone Width</div><div class="val">'+(s.zone_width||0).toFixed(1)+'%</div></div>';
  html += '<div class="info-item"><div class="lbl">Trigger (-1%)</div><div class="val" style="color:#d29922">$'+(wl?wl.trigger_price.toFixed(2):'n/a')+'</div></div>';
  html += '<div class="info-item"><div class="lbl">Support Below</div><div class="val">'+(s.n_support_below||0)+'</div></div>';
  html += '<div class="info-item"><div class="lbl">Magnets Above</div><div class="val">'+(s.n_magnets_above||0)+'</div></div>';
  html += '</div>';

  // GEX bar chart container
  html += '<div id="gexChart" class="chart-container"></div>';

  // After render, draw Plotly chart
  setTimeout(() => drawGexChart(selectedSym, s), 50);
  return html;
}

function drawGexChart(sym, s) {
  const negLevels = s.neg_levels || [];
  const posLevels = s.pos_levels || [];
  const allLevels = [...negLevels, ...posLevels].sort((a,b) => a.strike - b.strike);

  if (!allLevels.length) return;

  const strikes = allLevels.map(l => '$'+l.strike.toFixed(0));
  const gexValues = allLevels.map(l => l.gex);
  const colors = allLevels.map(l => l.gex >= 0 ? '#3fb950' : '#f85149');

  const barTrace = {
    x: strikes,
    y: gexValues,
    type: 'bar',
    marker: { color: colors },
    name: 'GEX',
    hovertemplate: '%{x}<br>GEX: %{y:,.0f}<extra></extra>',
  };

  // Spot line as annotation
  const spotStrike = '$' + (s.spot||0).toFixed(0);

  const layout = {
    paper_bgcolor: '#0d1117',
    plot_bgcolor: '#161b22',
    font: { color: '#c9d1d9', size: 11 },
    title: { text: sym + ' GEX Exposure', font: { color: '#58a6ff', size: 15 } },
    xaxis: { title: 'Strike', gridcolor: '#21262d', tickangle: -45 },
    yaxis: { title: 'Gamma Exposure', gridcolor: '#21262d', zeroline: true, zerolinecolor: '#30363d' },
    margin: { t: 40, b: 60, l: 60, r: 20 },
    shapes: [
      // Spot vertical line
      {
        type: 'line', yref: 'paper', y0: 0, y1: 1,
        x0: spotStrike, x1: spotStrike,
        line: { color: '#58a6ff', width: 2, dash: 'dot' },
      },
    ],
    annotations: [
      {
        x: spotStrike, y: 1, yref: 'paper',
        text: 'Spot $'+(s.spot||0).toFixed(2),
        showarrow: false, font: { color: '#58a6ff', size: 10 },
        yanchor: 'bottom',
      },
    ],
  };

  // Add trigger line if on watchlist
  const wl = (data.watchlist||{})[sym];
  if (wl) {
    const trigStrike = '$' + wl.trigger_price.toFixed(0);
    layout.shapes.push({
      type: 'line', yref: 'paper', y0: 0, y1: 1,
      x0: trigStrike, x1: trigStrike,
      line: { color: '#d29922', width: 2, dash: 'dash' },
    });
    layout.annotations.push({
      x: trigStrike, y: 0.95, yref: 'paper',
      text: 'TRIGGER $'+wl.trigger_price.toFixed(2),
      showarrow: false, font: { color: '#d29922', size: 10 },
    });
  }

  Plotly.react('gexChart', [barTrace], layout, {displayModeBar: false, responsive: true});
}

function renderWatchlist() {
  const wl = data.watchlist || {};
  const keys = Object.keys(wl).sort();
  if (!keys.length) return '<div class="card"><h3>No stocks on watchlist</h3><p>Waiting for GEX scan (every 30 min during market hours)...</p></div>';
  let html = '<div class="kpi-row">';
  html += '<div class="kpi"><div class="label">Watchlist</div><div class="value">'+keys.length+'</div></div>';
  html += '<div class="kpi"><div class="label">Active</div><div class="value">'+(data.active||[]).length+'</div></div>';
  html += '</div>';
  html += '<table><tr><th>Stock</th><th>Tier</th><th>Spot</th><th>-GEX</th><th>+GEX</th><th>Trigger</th><th>Dist</th><th>Ratio</th><th>Zone</th><th>Chart</th></tr>';
  for (const sym of keys) {
    const s = wl[sym];
    const dist = ((s.spot-s.trigger_price)/s.spot*100).toFixed(1);
    const tier = s.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    html += '<tr><td><b>'+sym+'</b></td><td>'+tier+'</td>';
    html += '<td>$'+s.spot.toFixed(2)+'</td><td style="color:#f85149">$'+s.highest_neg.toFixed(0)+'</td>';
    html += '<td style="color:#3fb950">$'+s.lowest_pos.toFixed(0)+'</td>';
    html += '<td style="color:#d29922">$'+s.trigger_price.toFixed(2)+'</td>';
    html += '<td>'+dist+'%</td><td>'+s.ratio+'x</td><td>'+s.zone_width.toFixed(1)+'%</td>';
    html += '<td><span style="color:#58a6ff;cursor:pointer" onclick="selectedSym=\\''+sym+'\\';showTab(\\'chart\\',document.querySelector(\\'.tab\\'))">View</span></td></tr>';
  }
  html += '</table>';
  return html;
}

function renderActive() {
  const trades = data.active || [];
  if (!trades.length) return '<div class="card"><h3>No active trades</h3><p>Monitoring watchlist every 2 min...</p></div>';
  let html = '<table><tr><th>Stock</th><th>Tier</th><th>Entry</th><th>Spot</th><th>Strike</th><th>T1</th><th>T2</th><th>Delta</th><th>Bid/Ask</th><th>Ratio</th></tr>';
  for (const t of trades) {
    const tier = t.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    const ts = t.entry_ts ? new Date(t.entry_ts).toLocaleTimeString() : '?';
    html += '<tr><td><b>'+t.symbol+'</b> <span class="badge badge-active">OPEN</span></td>';
    html += '<td>'+tier+'</td><td>'+ts+'</td>';
    html += '<td>$'+(t.entry_spot||0).toFixed(2)+'</td><td>$'+(t.strike||0).toFixed(0)+'</td>';
    html += '<td>$'+(t.t1_price||0).toFixed(0)+'</td><td>$'+(t.t2_price||0).toFixed(0)+'</td>';
    html += '<td>'+(t.call_delta?t.call_delta.toFixed(2):'?')+'</td>';
    html += '<td>$'+(t.call_bid||0).toFixed(2)+'/$'+(t.call_ask||0).toFixed(2)+'</td>';
    html += '<td>'+(t.ratio||'?')+'x</td></tr>';
  }
  html += '</table>';
  return html;
}

function renderLog() {
  const trades = data.trades || [];
  if (!trades.length) return '<div class="card"><h3>No trades yet</h3></div>';
  const wins = trades.filter(t=>(t.option_pnl_pct||0)>0).length;
  const tot = trades.reduce((s,t)=>s+(t.option_pnl_pct||0),0);
  let html = '<div class="kpi-row">';
  html += '<div class="kpi"><div class="label">Trades</div><div class="value">'+trades.length+'</div></div>';
  html += '<div class="kpi"><div class="label">Win Rate</div><div class="value '+(wins/trades.length>0.7?'green':'red')+'">'+(wins/trades.length*100).toFixed(0)+'%</div></div>';
  html += '<div class="kpi"><div class="label">Avg ROI</div><div class="value '+(tot>0?'green':'red')+'">'+(tot/trades.length).toFixed(0)+'%</div></div>';
  html += '<div class="kpi"><div class="label">Total P&L</div><div class="value '+(tot>0?'green':'red')+'">'+tot.toFixed(0)+'%</div></div>';
  html += '</div>';
  html += '<table><tr><th>Date</th><th>Stock</th><th>Tier</th><th>Entry</th><th>Exit</th><th>Strike</th><th>-GEX</th><th>+GEX</th><th>R</th><th>Exit</th><th>Stk%</th><th>Opt%</th><th>Hold</th></tr>';
  for (const t of trades) {
    const p=t.option_pnl_pct||0;
    const c=p>0?'win':(p<0?'loss':'neutral');
    const tier=t.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    const bg=t.exit_reason==='T2'?'badge-t2':(t.exit_reason==='T1'?'badge-t1':'badge-eod');
    html += '<tr><td>'+(t.trade_date||'?')+'</td><td><b>'+(t.symbol||'?')+'</b></td><td>'+tier+'</td>';
    html += '<td>$'+(t.entry_spot||0).toFixed(2)+'</td><td>$'+(t.exit_spot||0).toFixed(2)+'</td>';
    html += '<td>$'+(t.strike||0).toFixed(0)+'</td><td>$'+(t.highest_neg||0).toFixed(0)+'</td>';
    html += '<td>$'+(t.lowest_pos||0).toFixed(0)+'</td><td>'+(t.gex_ratio||'?')+'</td>';
    html += '<td><span class="badge '+bg+'">'+(t.exit_reason||'?')+'</span></td>';
    html += '<td class="'+c+'">'+(t.stock_pnl_pct||0).toFixed(2)+'%</td>';
    html += '<td class="'+c+'" style="font-weight:700">'+p.toFixed(0)+'%</td>';
    html += '<td>'+(t.hold_minutes||'?')+'m</td></tr>';
  }
  html += '</table>';
  return html;
}

function renderLevels() {
  const levels = data.levels || {};
  const keys = Object.keys(levels).sort();
  if (!keys.length) return '<div class="card"><h3>No levels yet</h3></div>';
  let html = '<table><tr><th>Stock</th><th>Spot</th><th>-GEX 1</th><th>-GEX 2</th><th>-GEX 3</th><th>+GEX 1</th><th>+GEX 2</th><th>+GEX 3</th><th>Ratio</th><th>Zone</th><th>Filter</th><th>Chart</th></tr>';
  for (const sym of keys) {
    const s = levels[sym];
    const neg=s.neg_strikes||[]; const pos=s.pos_strikes||[];
    const pass=s.ratio>=3&&s.spot>=s.highest_neg;
    html += '<tr><td><b>'+sym+'</b></td><td>$'+(s.spot||0).toFixed(2)+'</td>';
    for(let i=0;i<3;i++) html+='<td style="color:#f85149">'+(neg[i]?'$'+neg[i].toFixed(0):'-')+'</td>';
    for(let i=0;i<3;i++) html+='<td style="color:#3fb950">'+(pos[i]?'$'+pos[i].toFixed(0):'-')+'</td>';
    html += '<td>'+(s.ratio||0)+'x</td><td>'+(s.zone_width||0).toFixed(1)+'%</td>';
    html += '<td><span class="badge '+(pass?'badge-pass':'badge-fail')+'">'+(pass?'PASS':'FAIL')+'</span></td>';
    html += '<td><span style="color:#58a6ff;cursor:pointer" onclick="selectedSym=\\''+sym+'\\';showTab(\\'chart\\',document.querySelector(\\'.tab\\'))">View</span></td></tr>';
  }
  html += '</table>';
  return html;
}

async function triggerScan() {
  await fetch('/api/stock-gex-live/scan',{method:'POST'});
  setTimeout(doRefresh, 5000);
}

doRefresh();
setInterval(doRefresh, 30000);
</script>
</body>
</html>"""
