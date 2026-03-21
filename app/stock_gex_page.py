"""
Stock GEX Dashboard — Separate standalone page.
Completely independent from the 0DTE SPX dashboard.
Served at /stock-gex route.
"""

STOCK_GEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock GEX Scanner</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0b0c10;color:#e6e7e9;font-family:'Segoe UI',system-ui,-apple-system,sans-serif}
.wrap{display:flex;height:100vh;overflow:hidden}

/* ── Sidebar ── */
.sb{width:230px;background:#121417;border-right:1px solid #23262b;display:flex;flex-direction:column;flex-shrink:0}
.sb-hdr{padding:14px 16px;border-bottom:1px solid #23262b}
.sb-hdr h2{font-size:15px;color:#60a5fa;margin-bottom:10px;letter-spacing:.5px}
.sb-hdr input{width:100%;padding:7px 10px;background:#1a1d22;border:1px solid #23262b;border-radius:5px;color:#e6e7e9;font-size:13px;outline:none}
.sb-hdr input:focus{border-color:#60a5fa}
.sb-hdr input::placeholder{color:#6b7280}
.exp-tog{display:flex;gap:4px;margin-top:10px}
.exp-tog button{flex:1;padding:5px;background:#1a1d22;border:1px solid #23262b;border-radius:5px;color:#6b7280;font-size:11px;cursor:pointer;font-weight:600;transition:all .15s}
.exp-tog button:hover{border-color:#4b5563;color:#9ca3af}
.exp-tog button.on{background:#1e3a5f;border-color:#60a5fa;color:#60a5fa}
.slist{flex:1;overflow-y:auto;padding:4px 0}
.slist::-webkit-scrollbar{width:6px}
.slist::-webkit-scrollbar-thumb{background:#23262b;border-radius:3px}
.si{padding:7px 16px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;font-size:13px;transition:background .1s}
.si:hover{background:#1a1d22}
.si.sel{background:#1e3a5f;color:#60a5fa}
.si .tk{font-weight:600;min-width:48px}
.si .pr{font-size:11px;color:#9ca3af}
.si .dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.dot-g{background:#22c55e}
.dot-x{background:#3f3f46}

/* ── Main Panel ── */
.mn{flex:1;display:flex;flex-direction:column;overflow:hidden}
.mn-hdr{padding:14px 24px;border-bottom:1px solid #23262b;display:flex;justify-content:space-between;align-items:center}
.mn-hdr h1{font-size:20px;font-weight:700}
.mn-hdr .sub{font-size:12px;color:#9ca3af;margin-top:2px}
.mn-hdr .acts{display:flex;gap:8px;align-items:center}
.btn{background:#1a1d22;border:1px solid #23262b;color:#9ca3af;padding:6px 14px;border-radius:5px;cursor:pointer;font-size:12px;transition:all .15s}
.btn:hover{border-color:#4b5563;color:#e6e7e9}

/* ── Chart Tabs ── */
.ctabs{padding:12px 24px 0;display:flex;gap:6px}
.ctabs button{padding:6px 18px;background:#1a1d22;border:1px solid #23262b;border-radius:5px 5px 0 0;color:#6b7280;font-size:12px;cursor:pointer;font-weight:600;transition:all .15s;border-bottom:2px solid transparent}
.ctabs button:hover{color:#9ca3af}
.ctabs button.on{background:#0f1115;border-color:#23262b;border-bottom-color:#60a5fa;color:#60a5fa}

/* ── Chart Area ── */
.chart-area{flex:1;padding:0 24px 8px;min-height:0;position:relative}
.chart-area>div{height:100%}
.empty{display:flex;align-items:center;justify-content:center;height:100%;color:#4b5563;font-size:15px;flex-direction:column;gap:8px}
.empty .hint{font-size:12px;color:#3f3f46}

/* ── Levels Panel ── */
.lvl{padding:12px 24px 14px;border-top:1px solid #23262b;overflow-x:auto}
.lvl-grid{display:flex;gap:8px;flex-wrap:wrap}
.lc{background:#181b20;border-radius:6px;padding:10px 14px;min-width:160px;flex:1}
.lc .lb{font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.lc .val{font-size:14px;font-weight:700}
.lc .sub{font-size:11px;color:#6b7280;margin-top:3px}

/* ── Status Bar ── */
.sbar{padding:8px 24px;background:#0f1115;border-top:1px solid #23262b;font-size:11px;color:#4b5563;display:flex;justify-content:space-between;flex-shrink:0}
</style>
</head>
<body>
<div class="wrap">
  <!-- Sidebar -->
  <div class="sb">
    <div class="sb-hdr">
      <h2>Stock GEX</h2>
      <input type="text" id="search" placeholder="Search ticker..." oninput="filterList()">
      <div class="exp-tog">
        <button id="bW" class="on" onclick="setExp('weekly')">Weekly</button>
        <button id="bO" onclick="setExp('opex')">OpEx</button>
      </div>
    </div>
    <div class="slist" id="stockList"></div>
  </div>

  <!-- Main -->
  <div class="mn">
    <div class="mn-hdr">
      <div>
        <h1 id="title">Stock GEX Scanner</h1>
        <div class="sub" id="meta"></div>
      </div>
      <div class="acts">
        <button class="btn" onclick="triggerScan()">Scan Now</button>
        <button class="btn" onclick="refreshAll()">Refresh</button>
      </div>
    </div>
    <div class="ctabs" id="tabs" style="display:none">
      <button class="on" onclick="setView('net',this)">Net GEX</button>
      <button onclick="setView('exposure',this)">Exposure</button>
    </div>
    <div class="chart-area">
      <div id="chart" style="display:none"></div>
      <div id="empty" class="empty">
        <span>Select a stock from the sidebar</span>
        <span class="hint">GEX data updates every 30 min during market hours</span>
      </div>
    </div>
    <div class="lvl" id="levels" style="display:none">
      <div class="lvl-grid" id="levelsGrid"></div>
    </div>
    <div class="sbar">
      <span id="statusL">Loading...</span>
      <span id="statusR"></span>
    </div>
  </div>
</div>

<script>
/* ── State ── */
let allStocks = [];
let levelsData = {};
let curSym = null;
let curDetail = null;
let curExp = 'weekly';
let curView = 'net';

/* ── Init ── */
async function init(){
  await Promise.all([loadStatus(), loadLevels()]);
  setInterval(()=>{ loadLevels(); loadStatus(); }, 5*60*1000);
}

async function loadStatus(){
  try{
    const s = await (await fetch('/api/stock-gex/status')).json();
    allStocks = s.stock_list || [];
    document.getElementById('statusL').textContent =
      s.last_scan?.ts ? 'Last scan: '+s.last_scan.ts.slice(0,19)+' | '+s.last_scan.msg : 'No scans yet';
    document.getElementById('statusR').textContent = (s.stocks_tracked||0)+' stocks with data';
    renderList();
  }catch(e){ document.getElementById('statusL').textContent='Error: '+e; }
}

async function loadLevels(){
  try{
    levelsData = await (await fetch('/api/stock-gex/levels')).json();
    renderList();
    if(curSym && levelsData[curSym]) loadDetail(curSym);
  }catch(e){}
}

/* ── Stock List ── */
function renderList(){
  const el = document.getElementById('stockList');
  const q = document.getElementById('search').value.toUpperCase();
  const syms = (allStocks.length ? allStocks : Object.keys(levelsData)).sort();

  el.innerHTML = syms.filter(s=>!q||s.includes(q)).map(s=>{
    const d = levelsData[s];
    const has = d && d[curExp];
    const spot = has ? '$'+has.spot.toFixed(2) : '';
    const cls = s===curSym ? 'si sel' : 'si';
    const dot = has ? 'dot dot-g' : 'dot dot-x';
    return '<div class="'+cls+'" onclick="pick(\\''+s+'\\')">'+
      '<span class="tk">'+s+'</span>'+
      '<span style="display:flex;align-items:center;gap:6px">'+
        '<span class="pr">'+spot+'</span>'+
        '<span class="'+dot+'"></span>'+
      '</span></div>';
  }).join('');
}

function filterList(){ renderList(); }

function setExp(e){
  curExp=e;
  document.getElementById('bW').className = e==='weekly'?'on':'';
  document.getElementById('bO').className = e==='opex'?'on':'';
  renderList();
  if(curSym) loadDetail(curSym);
}

/* ── Detail ── */
function pick(s){ curSym=s; renderList(); loadDetail(s); }

async function loadDetail(sym){
  try{
    const r = await fetch('/api/stock-gex/detail?symbol='+sym);
    curDetail = await r.json();
    if(curDetail.error){ showEmpty(curDetail.error); return; }
    render();
  }catch(e){ showEmpty('Error: '+e); }
}

function showEmpty(msg){
  document.getElementById('chart').style.display='none';
  const em=document.getElementById('empty');
  em.style.display='flex'; em.querySelector('span').textContent=msg;
  document.getElementById('tabs').style.display='none';
  document.getElementById('levels').style.display='none';
}

function render(){
  const d = curDetail[curExp];
  if(!d){ showEmpty('No '+curExp+' data for '+curSym); return; }

  document.getElementById('empty').style.display='none';
  document.getElementById('chart').style.display='block';
  document.getElementById('tabs').style.display='flex';
  document.getElementById('levels').style.display='block';

  document.getElementById('title').textContent = d.symbol+' $'+d.spot.toFixed(2);
  document.getElementById('meta').textContent =
    'Exp: '+d.expiration+' ('+d.exp_label+') | Scanned: '+(d.scanned_at||'').slice(0,19);

  drawChart(d);
  drawLevels(d.levels);
}

/* ── Chart ── */
function drawChart(d){
  const gex=d.gex_data, spot=d.spot;
  const strikes = gex.map(g=>g.strike);

  let yVals;
  if(curView==='net') yVals = gex.map(g=>g.net_gex);
  else yVals = gex.map(g=>Math.max(Math.abs(g.call_gex),Math.abs(g.put_gex)));

  const allY = curView==='net'
    ? gex.map(g=>g.net_gex)
    : gex.flatMap(g=>[g.call_gex, g.put_gex]);
  const yMin=Math.min(...allY), yMax=Math.max(...allY);
  const pad=(yMax-yMin)*0.08||1;

  const spotLine={type:'line',x0:spot,x1:spot,y0:yMin-pad,y1:yMax+pad,
    line:{color:'#fbbf24',width:2,dash:'dot'},xref:'x',yref:'y'};

  const spotAnnot={x:spot,y:yMax+pad,text:'Spot $'+spot.toFixed(2),
    font:{size:11,color:'#fbbf24'},xref:'x',yref:'y',showarrow:false,yanchor:'bottom'};

  const layout={
    paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
    font:{color:'#e6e7e9',size:11},
    margin:{t:44,r:16,b:50,l:65},
    xaxis:{title:'Strike',gridcolor:'#20242a',tickfont:{size:10}},
    yaxis:{title:'GEX',gridcolor:'#20242a',tickfont:{size:10},
      tickformat:',', separatethousands:true},
    barmode:'group', shapes:[spotLine], annotations:[spotAnnot],
    showlegend:curView==='exposure',
    legend:{x:0,y:1.12,orientation:'h',font:{size:11}},
  };

  let traces;
  if(curView==='net'){
    const colors=gex.map(g=>g.net_gex>=0?'#22c55e':'#ef4444');
    traces=[{type:'bar',x:strikes,y:gex.map(g=>g.net_gex),
      marker:{color:colors},name:'Net GEX',
      hovertemplate:'Strike $%{x}<br>Net GEX: %{y:,.0f}<extra></extra>'}];
    layout.title={text:d.symbol+' \u2014 Net GEX ('+d.exp_label+')',font:{size:14}};
  } else {
    traces=[
      {type:'bar',x:strikes,y:gex.map(g=>g.call_gex),name:'Call GEX',
       marker:{color:'#22c55e',opacity:0.85},
       hovertemplate:'Strike $%{x}<br>Call GEX: %{y:,.0f}<extra></extra>'},
      {type:'bar',x:strikes,y:gex.map(g=>g.put_gex),name:'Put GEX',
       marker:{color:'#ef4444',opacity:0.85},
       hovertemplate:'Strike $%{x}<br>Put GEX: %{y:,.0f}<extra></extra>'},
    ];
    layout.title={text:d.symbol+' \u2014 Call/Put Exposure ('+d.exp_label+')',font:{size:14}};
  }

  Plotly.react('chart',traces,layout,{responsive:true,displayModeBar:false});
}

function setView(v,btn){
  curView=v;
  document.querySelectorAll('.ctabs button').forEach(b=>b.className='');
  btn.className='on';
  if(curDetail && curDetail[curExp]) drawChart(curDetail[curExp]);
}

/* ── Levels Panel ── */
function drawLevels(lv){
  if(!lv){document.getElementById('levelsGrid').innerHTML='';return;}
  const cards=[];

  const sup=lv.support||[];
  if(sup.length) cards.push(lc('Support (-GEX)', sup.map(s=>'$'+s.strike).join(', '),
    '#ef4444', sup.length+' level'+(sup.length>1?'s':'')));

  const ma=lv.magnets_above||[];
  if(ma.length) cards.push(lc('Magnets Above (+GEX)', ma.map(s=>'$'+s.strike).join(', '),
    '#22c55e', ma.length+' level'+(ma.length>1?'s':'')));

  const mb=lv.magnets_below||[];
  if(mb.length) cards.push(lc('Magnets Below (+GEX)', mb.map(s=>'$'+s.strike).join(', '),
    '#60a5fa', mb.length+' level'+(mb.length>1?'s':'')));

  const ra=lv.resistance_above||[];
  if(ra.length) cards.push(lc('Resistance Above (-GEX)', ra.map(s=>'$'+s.strike).join(', '),
    '#f59e0b', ra.length+' level'+(ra.length>1?'s':'')));

  if(lv.strongest_positive)
    cards.push(lc('Strongest +GEX','$'+lv.strongest_positive.strike,
      '#22c55e','GEX: '+(+lv.strongest_positive.gex).toLocaleString()));

  if(lv.strongest_negative)
    cards.push(lc('Strongest -GEX','$'+lv.strongest_negative.strike,
      '#ef4444','GEX: '+(+lv.strongest_negative.gex).toLocaleString()));

  cards.push(lc('GEX Above Spot',
    lv.gex_above_spot!=null?(+lv.gex_above_spot).toLocaleString():'n/a',
    lv.gex_above_spot>=0?'#22c55e':'#ef4444'));

  cards.push(lc('GEX Below Spot',
    lv.gex_below_spot!=null?(+lv.gex_below_spot).toLocaleString():'n/a',
    lv.gex_below_spot>=0?'#22c55e':'#ef4444'));

  document.getElementById('levelsGrid').innerHTML=cards.join('');
}

function lc(label,value,color,sub){
  return '<div class="lc"><div class="lb">'+label+'</div>'+
    '<div class="val" style="color:'+color+'">'+value+'</div>'+
    (sub?'<div class="sub">'+sub+'</div>':'')+
    '</div>';
}

/* ── Actions ── */
async function refreshAll(){ await Promise.all([loadStatus(),loadLevels()]); }
async function triggerScan(){
  try{ await fetch('/api/stock-gex/scan',{method:'POST'});
    document.getElementById('statusL').textContent='Scan triggered...';
    setTimeout(()=>{ loadStatus(); loadLevels(); }, 30000);
  }catch(e){}
}

init();
</script>
</body>
</html>"""
