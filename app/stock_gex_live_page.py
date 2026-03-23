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
body{background:var(--bg-0);color:var(--text);font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:12px;line-height:1.4;font-weight:400}
::selection{background:var(--blue);color:#fff}
::-webkit-scrollbar{width:6px}
::-webkit-scrollbar-track{background:var(--bg-1)}
::-webkit-scrollbar-thumb{background:var(--border-l);border-radius:3px}

/* ── Header ────────────────────────────────── */
.header{background:var(--bg-1);border-bottom:1px solid var(--border);padding:10px 24px;display:flex;justify-content:space-between;align-items:center}
.header-left{display:flex;align-items:center;gap:12px}
.header-left h1{font-size:14px;font-weight:600;color:var(--text);letter-spacing:-0.2px}
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
.sidebar{width:210px;background:var(--bg-1);border-right:1px solid var(--border);overflow-y:auto;flex-shrink:0}
.sidebar-header{padding:8px 12px 6px;font-size:9px;font-weight:500;color:var(--text-3);text-transform:uppercase;letter-spacing:.6px;border-bottom:1px solid var(--border)}
.stock-item{padding:7px 12px;cursor:pointer;border-bottom:1px solid rgba(30,45,69,0.4);display:flex;justify-content:space-between;align-items:center;transition:all .1s}
.stock-item:hover{background:var(--bg-2)}
.stock-item.selected{background:var(--blue-bg);border-left:3px solid var(--blue)}
.stock-item .sym{font-weight:500;font-size:12px;color:var(--text)}
.stock-item .price{font-size:10px;color:var(--text-3);margin-top:1px}
.stock-item .right{text-align:right}
.stock-item .ratio{font-size:11px;font-weight:500}
.stock-item .ratio.good{color:var(--green)}
.stock-item .ratio.bad{color:var(--text-3)}
.wl-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);margin-left:5px;vertical-align:middle}

/* ── Main Panel ────────────────────────────── */
.main-panel{flex:1;overflow-y:auto;padding:20px 24px;background:var(--bg-0)}

/* ── Tables ────────────────────────────────── */
table{width:100%;border-collapse:separate;border-spacing:0;font-size:11px}
thead{position:sticky;top:0;z-index:1}
th{background:var(--bg-1);color:var(--text-3);text-align:left;padding:7px 10px;font-weight:500;font-size:10px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid var(--border)}
th:first-child{border-radius:8px 0 0 0}
th:last-child{border-radius:0 8px 0 0}
td{padding:6px 10px;border-bottom:1px solid rgba(30,45,69,0.3);font-weight:400}
tr{transition:background .1s}
tbody tr:hover{background:var(--bg-2)}
.tbl-wrap{background:var(--bg-1);border:1px solid var(--border);border-radius:10px;overflow:hidden}

/* ── Cards / KPIs ──────────────────────────── */
.card{background:var(--bg-1);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:16px}
.card h3{color:var(--text);font-size:13px;margin-bottom:6px;font-weight:500}
.card p{color:var(--text-3);font-size:12px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:18px}
.kpi{background:var(--bg-1);border:1px solid var(--border);border-radius:8px;padding:10px 14px}
.kpi .label{color:var(--text-3);font-size:9px;text-transform:uppercase;letter-spacing:.4px;font-weight:500}
.kpi .value{color:var(--text);font-size:16px;font-weight:600;margin-top:2px}
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
.chart-container{height:260px;margin-bottom:12px;background:var(--bg-1);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.info-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-bottom:10px}
.info-item{background:var(--bg-1);border:1px solid var(--border);border-radius:6px;padding:7px 10px}
.info-item .lbl{color:var(--text-3);font-size:9px;font-weight:500;text-transform:uppercase;letter-spacing:.3px}
.info-item .val{font-size:13px;font-weight:600;margin-top:2px;color:var(--text)}

/* ── Empty state ───────────────────────────── */
/* ── Filter bar + sort ──────────────────────── */
.filter-bar{display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.fbtn{display:inline-flex;align-items:center;gap:4px;padding:5px 12px;border-radius:6px;font-size:11px;font-weight:500;color:var(--text-3);background:var(--bg-1);border:1px solid var(--border);cursor:pointer;transition:all .12s}
.fbtn:hover{color:var(--text);border-color:var(--border-l)}
.fbtn-act{color:var(--blue);border-color:var(--blue);background:var(--blue-bg)}
.fbtn .count{font-size:10px;font-weight:600;opacity:.7}
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:var(--text)}

.empty{text-align:center;padding:50px 20px}
.empty h3{color:var(--text-2);font-size:13px;font-weight:500;margin-bottom:6px}
.empty p{color:var(--text-3);font-size:11px}
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
let lvlSort='gexdist',lvlSortAsc=true,lvlFilter='all';

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
    document.getElementById('lastUpdate').textContent=new Date().toLocaleTimeString('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',second:'2-digit'})+' ET | '+levC+' stocks';
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

function fmtK(v){return v%1===0?'$'+v.toFixed(0):'$'+v.toFixed(1)}

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
  html+='<div class="info-item"><div class="lbl">-GEX (Support)</div><div class="val c-red">'+fmtK(s.highest_neg||0)+'</div></div>';
  html+='<div class="info-item"><div class="lbl">+GEX (Magnet)</div><div class="val c-green">'+fmtK(s.lowest_pos||0)+'</div></div>';
  const _hn=s.highest_neg||0;const _gDist=_hn>0?((s.spot-_hn)/_hn*100):0;const _below=s.spot<_hn;
  html+='<div class="info-item"><div class="lbl">Spot vs -GEX</div><div class="val" style="color:'+(_below?'var(--red)':(_gDist<2?'var(--amber)':'var(--green)'))+'">'+ (_below?'':'+')+ _gDist.toFixed(1)+'%</div></div>';
  html+='<div class="info-item"><div class="lbl">Trigger (-1%)</div><div class="val c-amber">$'+(wl?wl.trigger_price.toFixed(2):'n/a')+'</div></div>';
  html+='<div class="info-item"><div class="lbl">Support Below</div><div class="val">'+(s.n_support_below||0)+'</div></div>';
  html+='<div class="info-item"><div class="lbl">Magnets Above</div><div class="val">'+(s.n_magnets_above||0)+'</div></div>';
  html+='</div>';
  html+='<div id="gexChart" class="chart-container"></div>';
  setTimeout(()=>drawGexChart(selectedSym,s),50);
  return html;
}

function drawGexChart(sym,s){
  let all;
  if(s.all_levels&&s.all_levels.length){
    all=s.all_levels;
  }else{
    // Fallback: merge neg+pos and net-deduplicate by strike
    const byStrike={};
    for(const l of [...(s.neg_levels||[]),...(s.pos_levels||[])]){
      byStrike[l.strike]=(byStrike[l.strike]||0)+l.gex;
    }
    all=Object.entries(byStrike).map(([k,v])=>({strike:parseFloat(k),gex:v})).sort((a,b)=>a.strike-b.strike);
  }
  if(!all.length)return;
  const strikes=all.map(l=>fmtK(l.strike));
  const vals=all.map(l=>l.gex);
  const colors=all.map(l=>l.gex>=0?'rgba(34,197,94,0.8)':'rgba(239,68,68,0.7)');
  const trace={x:strikes,y:vals,type:'bar',marker:{color:colors,line:{color:colors.map(c=>c.replace('0.8','1').replace('0.7','1')),width:1}},name:'GEX',hovertemplate:'%{x}<br>GEX: %{y:,.0f}<extra></extra>'};
  const spot='$'+(s.spot||0).toFixed(0);
  const layout={
    paper_bgcolor:'#111827',plot_bgcolor:'#111827',
    font:{color:'#94a3b8',size:10,family:'Inter'},
    margin:{t:6,b:40,l:50,r:14},
    xaxis:{gridcolor:'#1e2d45',tickangle:-45,tickfont:{size:9}},
    yaxis:{gridcolor:'#1e2d45',zeroline:true,zerolinecolor:'#2a3f63',title:{text:'GEX',font:{size:9}}},
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
  html+='<div class="tbl-wrap"><table><thead><tr><th>Stock</th><th>Tier</th><th>Spot</th><th>-GEX</th><th>+GEX</th><th>Trigger</th><th>To Trigger</th><th>Spot vs -GEX</th><th>Ratio</th><th></th></tr></thead><tbody>';
  for(const sym of keys){
    const s=wl[sym];
    const trigDist=((s.spot-s.trigger_price)/s.spot*100).toFixed(1);
    const hn=s.highest_neg||0;
    const gexDist=hn>0?((s.spot-hn)/hn*100):0;
    const below=s.spot<hn;const close=!below&&gexDist<2;
    const gexCls=below?'c-red':(close?'c-amber':'c-green');
    const gexTxt=(below?'':'+')+ gexDist.toFixed(1)+'%';
    const tier=s.tier==='A'?'<span class="tier-a">A</span>':'<span class="tier-b">B</span>';
    html+='<tr><td><b>'+sym+'</b></td><td>'+tier+'</td>';
    html+='<td>$'+s.spot.toFixed(2)+'</td><td class="c-red">'+fmtK(hn)+'</td>';
    html+='<td class="c-green">'+fmtK(s.lowest_pos||0)+'</td>';
    html+='<td class="c-amber">$'+s.trigger_price.toFixed(2)+'</td>';
    html+='<td>'+trigDist+'%</td><td class="'+gexCls+'" style="font-weight:500">'+gexTxt+'</td><td>'+(s.ratio||0).toFixed(1)+'x</td>';
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
    const ts=t.entry_ts?new Date(t.entry_ts).toLocaleTimeString('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',second:'2-digit'}):'?';
    html+='<tr><td><b>'+t.symbol+'</b> <span class="badge badge-active">OPEN</span></td>';
    html+='<td>'+tier+'</td><td>'+ts+'</td>';
    html+='<td>$'+(t.entry_spot||0).toFixed(2)+'</td><td>'+fmtK(t.strike||0)+'</td>';
    html+='<td class="c-green">'+fmtK(t.t1_price||0)+'</td><td class="c-purple">'+fmtK(t.t2_price||0)+'</td>';
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
    html+='<td>'+fmtK(t.strike||0)+'</td><td class="c-red">'+fmtK(t.highest_neg||0)+'</td>';
    html+='<td class="c-green">'+fmtK(t.lowest_pos||0)+'</td><td>'+(t.gex_ratio||'?')+'</td>';
    html+='<td><span class="badge '+bg+'">'+(t.exit_reason||'?')+'</span></td>';
    html+='<td class="'+c+'">'+(t.stock_pnl_pct||0).toFixed(2)+'%</td>';
    html+='<td class="'+c+'" style="font-weight:700">'+p.toFixed(0)+'%</td>';
    html+='<td>'+(t.hold_minutes||'?')+'m</td></tr>';
  }
  html+='</tbody></table></div>';
  return html;
}

function _lvlData(levels){
  // Build enriched array for sorting/filtering
  // Watching = ratio >= 3 (strong setup). Below -GEX with good ratio = TRADE ZONE (most interesting)
  return Object.keys(levels).map(sym=>{
    const s=levels[sym];const hn=s.highest_neg||0;const sp=s.spot||0;
    const distPct=hn>0?((sp-hn)/hn*100):0;
    const below=sp<hn;const ratio=s.ratio||0;
    const goodRatio=ratio>=3;
    const status=goodRatio?'watching':'low';
    return{sym,s,hn,sp,distPct,below,ratio,status,goodRatio,neg:s.neg_strikes||[],pos:s.pos_strikes||[]};
  });
}
function setLvlSort(col){
  if(lvlSort===col)lvlSortAsc=!lvlSortAsc;
  else{lvlSort=col;lvlSortAsc=col==='gexdist'||col==='sym';}
  render();
}
function setLvlFilter(f){lvlFilter=f;render();}

function renderLevels(){
  const levels=data.levels||{};
  let rows=_lvlData(levels);
  if(!rows.length)return '<div class="empty"><h3>No levels yet</h3><p>Waiting for first GEX scan</p></div>';

  // Filter
  if(lvlFilter==='watching')rows=rows.filter(r=>r.goodRatio);
  else if(lvlFilter==='tradezone')rows=rows.filter(r=>r.below&&r.goodRatio);
  else if(lvlFilter==='below')rows=rows.filter(r=>r.below);
  else if(lvlFilter==='low')rows=rows.filter(r=>!r.goodRatio);
  else if(lvlFilter==='close')rows=rows.filter(r=>!r.below&&r.distPct<3&&r.goodRatio);

  // Sort
  const dir=lvlSortAsc?1:-1;
  if(lvlSort==='gexdist')rows.sort((a,b)=>(a.distPct-b.distPct)*dir);
  else if(lvlSort==='ratio')rows.sort((a,b)=>(a.ratio-b.ratio)*dir);
  else if(lvlSort==='sym')rows.sort((a,b)=>a.sym.localeCompare(b.sym)*dir);
  else if(lvlSort==='spot')rows.sort((a,b)=>(a.sp-b.sp)*dir);

  const total=_lvlData(levels);
  const cW=total.filter(r=>r.goodRatio).length;
  const cTZ=total.filter(r=>r.below&&r.goodRatio).length;
  const cB=total.filter(r=>r.below).length;
  const cL=total.filter(r=>!r.goodRatio).length;
  const cC=total.filter(r=>!r.below&&r.distPct<3&&r.goodRatio).length;

  // Filter bar
  const fb=(id,label,cnt)=>{const act=lvlFilter===id;return '<span class="fbtn'+(act?' fbtn-act':'')+'" onclick="setLvlFilter(\\''+id+'\\')">'+label+' <span class="count">'+cnt+'</span></span>'};
  let html='<div class="filter-bar">';
  html+=fb('all','All',total.length)+fb('watching','Watching',cW)+fb('tradezone','Trade Zone',cTZ)+fb('close','Near -GEX',cC)+fb('below','Below -GEX',cB)+fb('low','Low Ratio',cL);
  html+='</div>';

  // Sort arrow helper
  const sa=(col)=>lvlSort===col?(lvlSortAsc?' \\u25B2':' \\u25BC'):'';

  html+='<div style="font-size:10px;color:var(--text-3);margin-bottom:6px">Showing '+rows.length+' of '+total.length+' stocks</div>';
  html+='<div class="tbl-wrap"><table><thead><tr>';
  html+='<th class="sortable" onclick="setLvlSort(\\'sym\\')">Stock'+sa('sym')+'</th>';
  html+='<th class="sortable" onclick="setLvlSort(\\'spot\\')">Spot'+sa('spot')+'</th>';
  html+='<th>-GEX 1</th><th>-GEX 2</th><th>-GEX 3</th><th>+GEX 1</th><th>+GEX 2</th><th>+GEX 3</th>';
  html+='<th class="sortable" onclick="setLvlSort(\\'ratio\\')">Ratio'+sa('ratio')+'</th>';
  html+='<th class="sortable" onclick="setLvlSort(\\'gexdist\\')">Spot vs -GEX'+sa('gexdist')+'</th>';
  html+='<th>Status</th><th></th></tr></thead><tbody>';

  for(const r of rows){
    const close=!r.below&&r.distPct<2;
    const distCls=r.below?'c-red':(close?'c-amber':'c-green');
    const distTxt=(r.below?'':'+')+r.distPct.toFixed(1)+'%';
    const statusBadge=(r.below&&r.goodRatio)?'<span class="badge badge-active">TRADE ZONE</span>':(r.below?'<span class="badge badge-fail">BELOW -GEX</span>':(r.goodRatio?'<span class="badge badge-pass">WATCHING</span>':'<span class="badge" style="background:var(--bg-3);color:var(--text-3)">LOW RATIO</span>'));
    html+='<tr><td><b>'+r.sym+'</b></td><td>$'+r.sp.toFixed(2)+'</td>';
    for(let i=0;i<3;i++)html+='<td class="c-red">'+(r.neg[i]?fmtK(r.neg[i]):'-')+'</td>';
    for(let i=0;i<3;i++)html+='<td class="c-green">'+(r.pos[i]?fmtK(r.pos[i]):'-')+'</td>';
    html+='<td>'+r.ratio.toFixed(1)+'x</td><td class="'+distCls+'" style="font-weight:500">'+distTxt+'</td>';
    html+='<td>'+statusBadge+'</td>';
    html+='<td><span class="link" onclick="selectedSym=\\''+r.sym+'\\';showTab(\\'chart\\',document.querySelector(\\'.tab\\'))">View</span></td></tr>';
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
