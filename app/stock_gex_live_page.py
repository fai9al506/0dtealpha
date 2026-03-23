"""Stock GEX Live Scanner Dashboard — Support Bounce Strategy.
Design inspired by Unusual Whales: dark, data-dense, professional."""

STOCK_GEX_LIVE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock GEX Live Scanner</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg-0:#0a0e17;--bg-1:#111827;--bg-2:#1a2236;--bg-3:#243049;
  --border:#1e2d45;--border-l:#2a3f63;
  --text:#e2e8f0;--text-2:#94a3b8;--text-3:#64748b;
  --blue:#3b82f6;--blue-d:#1d4ed8;--blue-bg:rgba(59,130,246,0.12);
  --green:#22c55e;--green-bg:rgba(34,197,94,0.12);
  --red:#ef4444;--red-bg:rgba(239,68,68,0.12);
  --amber:#f59e0b;--amber-bg:rgba(245,158,11,0.12);
  --purple:#a78bfa;--purple-bg:rgba(167,139,250,0.12);
}
body{background:var(--bg-0);color:var(--text);font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:13px;line-height:1.5}
::selection{background:var(--blue);color:#fff}
::-webkit-scrollbar{width:6px}
::-webkit-scrollbar-track{background:var(--bg-1)}
::-webkit-scrollbar-thumb{background:var(--border-l);border-radius:3px}

/* ── Header ────────────────────────────────── */
.header{background:var(--bg-1);border-bottom:1px solid var(--border);padding:10px 24px;display:flex;justify-content:space-between;align-items:center}
.header-left{display:flex;align-items:center;gap:12px}
.header-left h1{font-size:16px;font-weight:700;color:#fff;letter-spacing:-0.3px}
.header-left .logo{width:28px;height:28px;border-radius:6px;background:linear-gradient(135deg,var(--blue),var(--purple));display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:#fff}
.header-right{display:flex;align-items:center;gap:10px}
.status-pill{display:flex;align-items:center;gap:6px;background:var(--bg-2);border:1px solid var(--border);border-radius:20px;padding:4px 12px;font-size:11px;color:var(--text-2)}
.status-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.btn{background:var(--bg-2);border:1px solid var(--border);color:var(--text-2);padding:6px 14px;border-radius:6px;cursor:pointer;font-size:11px;font-family:inherit;font-weight:500;transition:all .15s}
.btn:hover{background:var(--bg-3);color:var(--text);border-color:var(--border-l)}
.btn-primary{background:var(--blue-d);border-color:var(--blue);color:#fff}
.btn-primary:hover{background:var(--blue)}

/* ── Tabs ──────────────────────────────────── */
.tabs{display:flex;gap:2px;background:var(--bg-1);padding:0 24px;border-bottom:1px solid var(--border)}
.tab{padding:10px 18px;cursor:pointer;color:var(--text-3);font-size:12px;font-weight:500;user-select:none;border-bottom:2px solid transparent;transition:all .15s}
.tab:hover{color:var(--text-2)}
.tab.active{color:var(--blue);border-bottom-color:var(--blue);font-weight:600}
.tab .count{display:inline-block;background:var(--bg-3);color:var(--text-2);font-size:10px;padding:1px 6px;border-radius:10px;margin-left:5px;font-weight:600}
.tab.active .count{background:var(--blue-bg);color:var(--blue)}

/* ── Layout ────────────────────────────────── */
.layout{display:flex;height:calc(100vh - 89px)}

/* ── Sidebar ───────────────────────────────── */
.sidebar{width:240px;background:var(--bg-1);border-right:1px solid var(--border);overflow-y:auto;flex-shrink:0}
.sidebar-header{padding:12px 14px 8px;font-size:10px;font-weight:600;color:var(--text-3);text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid var(--border)}
.stock-item{padding:10px 14px;cursor:pointer;border-bottom:1px solid rgba(30,45,69,0.5);display:flex;justify-content:space-between;align-items:center;transition:all .1s}
.stock-item:hover{background:var(--bg-2)}
.stock-item.selected{background:var(--blue-bg);border-left:3px solid var(--blue)}
.stock-item .sym{font-weight:600;font-size:13px;color:#fff}
.stock-item .price{font-size:11px;color:var(--text-2);margin-top:1px}
.stock-item .right{text-align:right}
.stock-item .ratio{font-size:12px;font-weight:600}
.stock-item .ratio.good{color:var(--green)}
.stock-item .ratio.bad{color:var(--text-3)}
.wl-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);margin-left:5px;vertical-align:middle}

/* ── Main Panel ────────────────────────────── */
.main-panel{flex:1;overflow-y:auto;padding:20px 24px;background:var(--bg-0)}

/* ── Tables ────────────────────────────────── */
table{width:100%;border-collapse:separate;border-spacing:0;font-size:12px}
thead{position:sticky;top:0;z-index:1}
th{background:var(--bg-1);color:var(--text-3);text-align:left;padding:10px 12px;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--border)}
th:first-child{border-radius:8px 0 0 0}
th:last-child{border-radius:0 8px 0 0}
td{padding:10px 12px;border-bottom:1px solid rgba(30,45,69,0.3)}
tr{transition:background .1s}
tbody tr:hover{background:var(--bg-2)}
.tbl-wrap{background:var(--bg-1);border:1px solid var(--border);border-radius:10px;overflow:hidden}

/* ── Cards / KPIs ──────────────────────────── */
.card{background:var(--bg-1);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:16px}
.card h3{color:#fff;font-size:14px;margin-bottom:6px;font-weight:600}
.card p{color:var(--text-3);font-size:12px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:18px}
.kpi{background:var(--bg-1);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.kpi .label{color:var(--text-3);font-size:10px;text-transform:uppercase;letter-spacing:.5px;font-weight:600}
.kpi .value{color:#fff;font-size:22px;font-weight:700;margin-top:4px;letter-spacing:-0.5px}
.kpi .value.green{color:var(--green)}
.kpi .value.red{color:var(--red)}

/* ── Badges ────────────────────────────────── */
.badge{display:inline-flex;align-items:center;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:.3px}
.badge-pass{background:var(--green-bg);color:var(--green)}
.badge-fail{background:var(--red-bg);color:var(--red)}
.badge-active{background:var(--blue-bg);color:var(--blue)}
.badge-t1{background:var(--green-bg);color:var(--green)}
.badge-t2{background:var(--purple-bg);color:var(--purple)}
.badge-eod{background:var(--amber-bg);color:var(--amber)}
.badge-wl{background:var(--green-bg);color:var(--green);font-size:9px;padding:1px 5px}

/* ── Colors ────────────────────────────────── */
.c-green{color:var(--green)}.c-red{color:var(--red)}.c-blue{color:var(--blue)}.c-amber{color:var(--amber)}.c-purple{color:var(--purple)}
.c-muted{color:var(--text-3)}
.tier-a{color:var(--amber);font-weight:700}
.tier-b{color:var(--text-3)}
.win{color:var(--green)}.loss{color:var(--red)}
.link{color:var(--blue);cursor:pointer;font-weight:500}
.link:hover{text-decoration:underline}

/* ── GEX Chart ─────────────────────────────── */
.chart-container{height:380px;margin-bottom:16px;background:var(--bg-1);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.info-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:14px}
.info-item{background:var(--bg-1);border:1px solid var(--border);border-radius:8px;padding:10px 14px}
.info-item .lbl{color:var(--text-3);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.4px}
.info-item .val{font-size:16px;font-weight:700;margin-top:3px;color:#fff}

/* ── Empty state ───────────────────────────── */
.empty{text-align:center;padding:60px 20px}
.empty h3{color:var(--text-2);font-size:16px;margin-bottom:8px}
.empty p{color:var(--text-3);font-size:13px}
</style>
</head>
<body>
<div class="header">
  <div class="header-left">
    <div class="logo">G</div>
    <h1>Stock GEX Scanner</h1>
  </div>
  <div class="header-right">
    <div class="status-pill"><span class="status-dot"></span><span id="lastUpdate">Loading...</span></div>
    <button class="btn" onclick="doRefresh()">Refresh</button>
    <button class="btn btn-primary" onclick="triggerScan()">Force Scan</button>
  </div>
</div>
<div class="tabs" id="tabBar">
  <div class="tab active" onclick="showTab('chart',this)">GEX Chart</div>
  <div class="tab" onclick="showTab('watchlist',this)">Watchlist <span class="count" id="wlCount">0</span></div>
  <div class="tab" onclick="showTab('active',this)">Active <span class="count" id="actCount">0</span></div>
  <div class="tab" onclick="showTab('log',this)">Trade Log</div>
  <div class="tab" onclick="showTab('levels',this)">All Levels</div>
</div>
<div class="layout">
  <div class="sidebar">
    <div class="sidebar-header">Stocks <span id="stockCount"></span></div>
    <div id="sidebar"></div>
  </div>
  <div class="main-panel" id="main"></div>
</div>

<script>
let currentTab='chart',selectedSym=null,data={};

function showTab(tab,el){
  currentTab=tab;
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  if(el)el.classList.add('active');
  render();
}
function selectStock(sym){
  selectedSym=sym;
  document.querySelectorAll('.stock-item').forEach(el=>el.classList.remove('selected'));
  const el=document.querySelector('[data-sym="'+sym+'"]');
  if(el)el.classList.add('selected');
  render();
}

async function doRefresh(){
  try{
    const results=await Promise.allSettled([
      fetch('/api/stock-gex-live/watchlist').then(r=>r.json()),
      fetch('/api/stock-gex-live/active').then(r=>r.json()),
      fetch('/api/stock-gex-live/trades?days=30').then(r=>r.json()),
      fetch('/api/stock-gex-live/levels').then(r=>r.json()),
      fetch('/api/stock-gex-live/status').then(r=>r.json()),
    ]);
    const wl=results[0].status==='fulfilled'?results[0].value:{};
    const active=results[1].status==='fulfilled'?results[1].value:[];
    const trades=results[2].status==='fulfilled'?results[2].value:[];
    const levels=results[3].status==='fulfilled'?results[3].value:{};
    const status=results[4].status==='fulfilled'?results[4].value:{};
    data={
      watchlist:(wl&&!wl.error)?wl:{},
      active:Array.isArray(active)?active:[],
      trades:Array.isArray(trades)?trades:[],
      levels:(levels&&!levels.error)?levels:{},
      status:(status&&!status.error)?status:{},
    };
    renderSidebar();render();
    const wlC=Object.keys(data.watchlist).length;
    const actC=(data.active||[]).length;
    const levC=Object.keys(data.levels).length;
    document.getElementById('lastUpdate').textContent=new Date().toLocaleTimeString()+' | '+levC+' stocks';
    document.getElementById('wlCount').textContent=wlC;
    document.getElementById('actCount').textContent=actC;
    document.getElementById('stockCount').textContent='('+levC+')';
  }catch(e){console.error('Refresh error:',e);render()}
}

function renderSidebar(){
  const levels=data.levels||{};const wl=data.watchlist||{};
  const keys=Object.keys(levels).sort();
  let html='';
  for(const sym of keys){
    const s=levels[sym];const onWL=sym in wl;
    const cls=sym===selectedSym?'stock-item selected':'stock-item';
    const rCls=(s.ratio||0)>=3?'ratio good':'ratio bad';
    html+='<div class="'+cls+'" data-sym="'+sym+'" onclick="selectStock(\\''+sym+'\\')">';
    html+='<div><span class="sym">'+sym+'</span>';
    if(onWL)html+='<span class="wl-dot"></span>';
    html+='<div class="price">$'+(s.spot||0).toFixed(2)+'</div></div>';
    html+='<div class="right"><div class="'+rCls+'">'+(s.ratio||0).toFixed(1)+'x</div></div>';
    html+='</div>';
  }
  document.getElementById('sidebar').innerHTML=html;
  if(!selectedSym&&keys.length){
    const wlKeys=Object.keys(wl);
    selectedSym=wlKeys.length?wlKeys[0]:keys[0];
    const el=document.querySelector('[data-sym="'+selectedSym+'"]');
    if(el)el.classList.add('selected');
  }
}

function render(){
  const el=document.getElementById('main');
  if(currentTab==='chart')el.innerHTML=renderChart();
  else if(currentTab==='watchlist')el.innerHTML=renderWatchlist();
  else if(currentTab==='active')el.innerHTML=renderActive();
  else if(currentTab==='log')el.innerHTML=renderLog();
  else if(currentTab==='levels')el.innerHTML=renderLevels();
}

function renderChart(){
  if(!selectedSym)return '<div class="empty"><h3>Select a stock from the sidebar</h3><p>Click any stock to view its GEX profile</p></div>';
  const levels=data.levels||{};const s=levels[selectedSym];
  if(!s)return '<div class="empty"><h3>No GEX data for '+selectedSym+'</h3></div>';
  const wl=(data.watchlist||{})[selectedSym];

  let html='<div class="info-grid">';
  html+='<div class="info-item"><div class="lbl">Spot Price</div><div class="val">$'+(s.spot||0).toFixed(2)+'</div></div>';
  html+='<div class="info-item"><div class="lbl">GEX Ratio</div><div class="val" style="color:'+((s.ratio||0)>=3?'var(--green)':'var(--red)')+'">'+(s.ratio||0).toFixed(1)+'x</div></div>';
  html+='<div class="info-item"><div class="lbl">-GEX (Support)</div><div class="val c-red">$'+(s.highest_neg||0).toFixed(0)+'</div></div>';
  html+='<div class="info-item"><div class="lbl">+GEX (Magnet)</div><div class="val c-green">$'+(s.lowest_pos||0).toFixed(0)+'</div></div>';
  html+='<div class="info-item"><div class="lbl">Zone Width</div><div class="val">'+(s.zone_width||0).toFixed(1)+'%</div></div>';
  html+='<div class="info-item"><div class="lbl">Trigger (-1%)</div><div class="val c-amber">$'+(wl?wl.trigger_price.toFixed(2):'n/a')+'</div></div>';
  html+='<div class="info-item"><div class="lbl">Support Below</div><div class="val">'+(s.n_support_below||0)+'</div></div>';
  html+='<div class="info-item"><div class="lbl">Magnets Above</div><div class="val">'+(s.n_magnets_above||0)+'</div></div>';
  html+='</div>';
  html+='<div id="gexChart" class="chart-container"></div>';
  setTimeout(()=>drawGexChart(selectedSym,s),50);
  return html;
}

function drawGexChart(sym,s){
  const negL=s.neg_levels||[];const posL=s.pos_levels||[];
  const all=[...negL,...posL].sort((a,b)=>a.strike-b.strike);
  if(!all.length)return;
  const strikes=all.map(l=>'$'+l.strike.toFixed(0));
  const vals=all.map(l=>l.gex);
  const colors=all.map(l=>l.gex>=0?'rgba(34,197,94,0.8)':'rgba(239,68,68,0.7)');
  const trace={x:strikes,y:vals,type:'bar',marker:{color:colors,line:{color:colors.map(c=>c.replace('0.8','1').replace('0.7','1')),width:1}},name:'GEX',hovertemplate:'%{x}<br>GEX: %{y:,.0f}<extra></extra>'};
  const spot='$'+(s.spot||0).toFixed(0);
  const layout={
    paper_bgcolor:'#111827',plot_bgcolor:'#111827',
    font:{color:'#94a3b8',size:11,family:'Inter'},
    margin:{t:10,b:50,l:60,r:20},
    xaxis:{gridcolor:'#1e2d45',tickangle:-45,tickfont:{size:10}},
    yaxis:{gridcolor:'#1e2d45',zeroline:true,zerolinecolor:'#2a3f63',title:{text:'Gamma Exposure',font:{size:11}}},
    shapes:[{type:'line',yref:'paper',y0:0,y1:1,x0:spot,x1:spot,line:{color:'#3b82f6',width:2,dash:'dot'}}],
    annotations:[{x:spot,y:1,yref:'paper',text:'SPOT $'+(s.spot||0).toFixed(2),showarrow:false,font:{color:'#3b82f6',size:10,family:'Inter'},yanchor:'bottom',bgcolor:'#111827'}],
  };
  const wl=(data.watchlist||{})[sym];
  if(wl){
    const trig='$'+wl.trigger_price.toFixed(0);
    layout.shapes.push({type:'line',yref:'paper',y0:0,y1:1,x0:trig,x1:trig,line:{color:'#f59e0b',width:2,dash:'dash'}});
    layout.annotations.push({x:trig,y:0.92,yref:'paper',text:'TRIGGER $'+wl.trigger_price.toFixed(2),showarrow:false,font:{color:'#f59e0b',size:10},bgcolor:'#111827'});
  }
  Plotly.react('gexChart',[trace],layout,{displayModeBar:false,responsive:true});
}

function renderWatchlist(){
  const wl=data.watchlist||{};const keys=Object.keys(wl).sort();
  if(!keys.length)return '<div class="empty"><h3>No stocks on watchlist</h3><p>Waiting for GEX scan (every 30 min during market hours)</p></div>';
  let html='<div class="kpi-row">';
  html+='<div class="kpi"><div class="label">Watchlist</div><div class="value">'+keys.length+'</div></div>';
  html+='<div class="kpi"><div class="label">Active Trades</div><div class="value">'+(data.active||[]).length+'</div></div>';
  html+='</div>';
  html+='<div class="tbl-wrap"><table><thead><tr><th>Stock</th><th>Tier</th><th>Spot</th><th>-GEX</th><th>+GEX</th><th>Trigger</th><th>Dist</th><th>Ratio</th><th>Zone</th><th></th></tr></thead><tbody>';
  for(const sym of keys){
    const s=wl[sym];
    const dist=((s.spot-s.trigger_price)/s.spot*100).toFixed(1);
    const tier=s.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    html+='<tr><td><b>'+sym+'</b></td><td>'+tier+'</td>';
    html+='<td>$'+s.spot.toFixed(2)+'</td><td class="c-red">$'+s.highest_neg.toFixed(0)+'</td>';
    html+='<td class="c-green">$'+s.lowest_pos.toFixed(0)+'</td>';
    html+='<td class="c-amber">$'+s.trigger_price.toFixed(2)+'</td>';
    html+='<td>'+dist+'%</td><td>'+(s.ratio||0).toFixed(1)+'x</td><td>'+(s.zone_width||0).toFixed(1)+'%</td>';
    html+='<td><span class="link" onclick="selectedSym=\\''+sym+'\\';showTab(\\'chart\\',document.querySelector(\\'.tab\\'))">View</span></td></tr>';
  }
  html+='</tbody></table></div>';
  return html;
}

function renderActive(){
  const trades=data.active||[];
  if(!trades.length)return '<div class="empty"><h3>No active trades</h3><p>Monitoring watchlist every 2 min for bounce entries</p></div>';
  let html='<div class="tbl-wrap"><table><thead><tr><th>Stock</th><th>Tier</th><th>Entry</th><th>Spot</th><th>Strike</th><th>T1</th><th>T2</th><th>Delta</th><th>Bid/Ask</th><th>Ratio</th></tr></thead><tbody>';
  for(const t of trades){
    const tier=t.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    const ts=t.entry_ts?new Date(t.entry_ts).toLocaleTimeString():'?';
    html+='<tr><td><b>'+t.symbol+'</b> <span class="badge badge-active">OPEN</span></td>';
    html+='<td>'+tier+'</td><td>'+ts+'</td>';
    html+='<td>$'+(t.entry_spot||0).toFixed(2)+'</td><td>$'+(t.strike||0).toFixed(0)+'</td>';
    html+='<td class="c-green">$'+(t.t1_price||0).toFixed(0)+'</td><td class="c-purple">$'+(t.t2_price||0).toFixed(0)+'</td>';
    html+='<td>'+(t.call_delta?t.call_delta.toFixed(2):'?')+'</td>';
    html+='<td>$'+(t.call_bid||0).toFixed(2)+' / $'+(t.call_ask||0).toFixed(2)+'</td>';
    html+='<td>'+(t.ratio||'?')+'x</td></tr>';
  }
  html+='</tbody></table></div>';
  return html;
}

function renderLog(){
  const trades=data.trades||[];
  if(!trades.length)return '<div class="empty"><h3>No trades yet</h3><p>Trades will appear here once the scanner enters positions</p></div>';
  const wins=trades.filter(t=>(t.option_pnl_pct||0)>0).length;
  const tot=trades.reduce((s,t)=>s+(t.option_pnl_pct||0),0);
  let html='<div class="kpi-row">';
  html+='<div class="kpi"><div class="label">Total Trades</div><div class="value">'+trades.length+'</div></div>';
  html+='<div class="kpi"><div class="label">Win Rate</div><div class="value '+(wins/trades.length>0.6?'green':'red')+'">'+(wins/trades.length*100).toFixed(0)+'%</div></div>';
  html+='<div class="kpi"><div class="label">Avg ROI</div><div class="value '+(tot>0?'green':'red')+'">'+(tot/trades.length).toFixed(0)+'%</div></div>';
  html+='<div class="kpi"><div class="label">Total P&L</div><div class="value '+(tot>0?'green':'red')+'">'+tot.toFixed(0)+'%</div></div>';
  html+='</div>';
  html+='<div class="tbl-wrap"><table><thead><tr><th>Date</th><th>Stock</th><th>Tier</th><th>Entry</th><th>Exit</th><th>Strike</th><th>-GEX</th><th>+GEX</th><th>R</th><th>Exit</th><th>Stk%</th><th>Opt%</th><th>Hold</th></tr></thead><tbody>';
  for(const t of trades){
    const p=t.option_pnl_pct||0;const c=p>0?'win':(p<0?'loss':'');
    const tier=t.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    const bg=t.exit_reason==='T2'?'badge-t2':(t.exit_reason==='T1'?'badge-t1':'badge-eod');
    html+='<tr><td>'+(t.trade_date||'?')+'</td><td><b>'+(t.symbol||'?')+'</b></td><td>'+tier+'</td>';
    html+='<td>$'+(t.entry_spot||0).toFixed(2)+'</td><td>$'+(t.exit_spot||0).toFixed(2)+'</td>';
    html+='<td>$'+(t.strike||0).toFixed(0)+'</td><td class="c-red">$'+(t.highest_neg||0).toFixed(0)+'</td>';
    html+='<td class="c-green">$'+(t.lowest_pos||0).toFixed(0)+'</td><td>'+(t.gex_ratio||'?')+'</td>';
    html+='<td><span class="badge '+bg+'">'+(t.exit_reason||'?')+'</span></td>';
    html+='<td class="'+c+'">'+(t.stock_pnl_pct||0).toFixed(2)+'%</td>';
    html+='<td class="'+c+'" style="font-weight:700">'+p.toFixed(0)+'%</td>';
    html+='<td>'+(t.hold_minutes||'?')+'m</td></tr>';
  }
  html+='</tbody></table></div>';
  return html;
}

function renderLevels(){
  const levels=data.levels||{};const keys=Object.keys(levels).sort();
  if(!keys.length)return '<div class="empty"><h3>No levels yet</h3><p>Waiting for first GEX scan</p></div>';
  let html='<div class="tbl-wrap"><table><thead><tr><th>Stock</th><th>Spot</th><th>-GEX 1</th><th>-GEX 2</th><th>-GEX 3</th><th>+GEX 1</th><th>+GEX 2</th><th>+GEX 3</th><th>Ratio</th><th>Zone</th><th>Filter</th><th></th></tr></thead><tbody>';
  for(const sym of keys){
    const s=levels[sym];const neg=s.neg_strikes||[];const pos=s.pos_strikes||[];
    const pass=s.ratio>=3&&s.spot>=s.highest_neg;
    html+='<tr><td><b>'+sym+'</b></td><td>$'+(s.spot||0).toFixed(2)+'</td>';
    for(let i=0;i<3;i++)html+='<td class="c-red">'+(neg[i]?'$'+neg[i].toFixed(0):'-')+'</td>';
    for(let i=0;i<3;i++)html+='<td class="c-green">'+(pos[i]?'$'+pos[i].toFixed(0):'-')+'</td>';
    html+='<td>'+(s.ratio||0).toFixed(1)+'x</td><td>'+(s.zone_width||0).toFixed(1)+'%</td>';
    html+='<td><span class="badge '+(pass?'badge-pass':'badge-fail')+'">'+(pass?'PASS':'FAIL')+'</span></td>';
    html+='<td><span class="link" onclick="selectedSym=\\''+sym+'\\';showTab(\\'chart\\',document.querySelector(\\'.tab\\'))">View</span></td></tr>';
  }
  html+='</tbody></table></div>';
  return html;
}

async function triggerScan(){
  document.querySelector('.btn-primary').textContent='Scanning...';
  await fetch('/api/stock-gex-live/scan',{method:'POST',credentials:'include'});
  setTimeout(()=>{doRefresh();document.querySelector('.btn-primary').textContent='Force Scan'},8000);
}

doRefresh();
setInterval(doRefresh,30000);
</script>
</body>
</html>"""
