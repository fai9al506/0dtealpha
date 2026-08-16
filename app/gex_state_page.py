# -*- coding: utf-8 -*-
"""GEX Dealer Positioning page (S244) — Exelza-style card layout for SPX 0DTE.

MONITORING ONLY. Manual-trade map, same role as /darkmate-fw. Nothing here places,
sizes or blocks a trade.
"""

GEX_STATE_HTML = r"""<!doctype html><html><head><meta charset="utf-8">
<title>GEX Dealer Positioning</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body{background:#0e1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:1180px;margin:0 auto;padding:16px;line-height:1.4}
h1{font-size:20px;margin:4px 0} a{color:#58a6ff}
.bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:8px 0}
select,button,input{background:#161b22;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:5px 9px}
button{cursor:pointer} .mut{color:#8b949e;font-size:12px}
.cards{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin:10px 0}
.c{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:9px 11px}
.c .v{font-size:19px;font-weight:700;line-height:1.25} .c .t{font-size:10.5px;color:#8b949e;letter-spacing:.06em}
.c .s{font-size:11px;color:#8b949e}
.sig{display:grid;grid-template-columns:2fr 1fr;gap:8px;margin:8px 0}
.sig .c{display:flex;align-items:center;justify-content:space-between;gap:12px}
.badge{border-radius:6px;padding:3px 10px;font-size:12px;font-weight:700}
.pos{color:#3fb950} .neg{color:#f85149} .amb{color:#d29922}
.lab{font-weight:700;color:#bc8cff;margin:14px 0 4px;font-size:14px}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{border-bottom:1px solid #21262d;padding:4px 7px;text-align:left}
th{color:#8b949e;font-weight:600}
.note{background:#161b22;border-left:3px solid #d29922;border-radius:0 6px 6px 0;padding:8px 12px;margin:12px 0;font-size:12.5px;color:#8b949e}
</style></head><body>
<h1>&#129518; GEX Dealer Positioning <span class="mut">SPX 0DTE &middot; monitoring only</span></h1>
<div class="bar">
  <label><input type="checkbox" id="live" checked> Live 60s</label>
  <label>History <input type="datetime-local" id="at"></label>
  <button onclick="load()">Load</button>
  <span id="status" class="mut"></span>
</div>

<div class="cards" id="cards"></div>
<div class="sig">
  <div class="c"><div><div class="t">SIGNAL</div><div class="v" id="sigv">&mdash;</div>
    <div class="s" id="sigs"></div></div><div id="sigb"></div></div>
  <div class="c"><div class="t">NET DEX</div><div class="v" id="dexv">&mdash;</div>
    <div class="s">positive = buying pressure</div></div>
</div>

<div class="lab">GAMMA EXPOSURE BY STRIKE</div>
<div id="chart" style="height:520px"></div>

<div class="lab">STATE THROUGH THE DAY</div>
<div id="tl" style="height:150px"></div>
<table id="hist"><thead><tr><th>time</th><th>state</th><th>spot</th><th>zero gamma</th>
<th>net GEX</th><th>net DEX</th></tr></thead><tbody></tbody></table>

<div class="note"><b>How to read it.</b> Positive net GEX = dealers dampen moves (range-y);
negative = they amplify (trend/violence). <b>Zero Gamma is the line that matters</b> &mdash; above it
the regime is calm, below it volatile. Call/Put Wall are the biggest gamma concentrations, which
dealers tend to defend. The guide's own ranking, which held up on our trades: the quiet states
(SUPPORT / RESISTANCE / MEAN REVERSION) are the most reliable; SQUEEZE / ACCELERATION move hardest
but are less dependable; BREAKOUT / BREAKDOWN TEST are coin flips without confirmation.
<b>Nothing on this page places or blocks a trade.</b></div>

<script>
const TIERC={'quiet-reliable':'#3fb950','explosive':'#f85149','conditional':'#d29922',
             'energy-no-direction':'#58a6ff','cautionary':'#8b949e'};
const f1=(x,d)=>x==null?'&mdash;':Number(x).toFixed(d===undefined?2:d);
function card(t,v,s,cls){return `<div class="c"><div class="t">${t}</div>
  <div class="v ${cls||''}">${v}</div><div class="s">${s||''}</div></div>`;}

function render(j){
  if(!j||!j.profile){document.getElementById('status').textContent='no data';return;}
  const spot=j.spot, zg=j.zero_gamma, cw=j.call_wall, pw=j.put_wall, mg=j.max_gamma;
  const ngPos=j.net_gex_m>0;
  document.getElementById('cards').innerHTML=
    card('NET GEX',(ngPos?'+':'')+f1(j.net_gex_m,0)+'M',ngPos?'dampening':'amplifying',ngPos?'pos':'neg')+
    card('SPOT',f1(spot,2),new Date(j.ts).toLocaleTimeString())+
    card('ZERO GAMMA',f1(zg,2),zg==null?'outside window':(spot>zg?'spot ABOVE (calm)':'spot BELOW (volatile)'),
         zg==null?'':(spot>zg?'pos':'neg'))+
    card('MAX GAMMA',f1(mg,0),'magnet / pin')+
    card('PUT WALL',f1(pw,0),pw?((spot-pw>=0?'-':'+')+f1(Math.abs(spot-pw),0)+' pt from spot'):'')+
    card('CALL WALL',f1(cw,0),cw?((cw-spot>=0?'+':'-')+f1(Math.abs(cw-spot),0)+' pt from spot'):'');
  const tier=j.tier||'', col=TIERC[tier]||'#8b949e';
  document.getElementById('sigv').innerHTML=`<span style="color:${col}">${(j.state||'—').replace(/_/g,' ')}</span>`;
  document.getElementById('sigs').textContent=j.state_bias?('bias: '+j.state_bias.toUpperCase()):'no directional bias';
  document.getElementById('sigb').innerHTML=tier?`<span class="badge" style="background:${col}22;color:${col};border:1px solid ${col}66">${tier}</span>`:'';
  const dx=j.net_dex_m;
  const de=document.getElementById('dexv');
  de.textContent=(dx>0?'+':'')+f1(dx,0)+'M'; de.className='v '+(dx>0?'pos':'neg');

  const p=j.profile, ks=p.map(s=>s.strike);
  Plotly.newPlot('chart',[
    {y:ks,x:p.map(s=>s.call_gex),type:'bar',orientation:'h',name:'Call GEX',marker:{color:'#3fb950'},
     hovertemplate:'%{y} · call %{x:.1f}M<extra></extra>'},
    {y:ks,x:p.map(s=>s.put_gex),type:'bar',orientation:'h',name:'Put GEX',marker:{color:'#f85149'},
     hovertemplate:'%{y} · put %{x:.1f}M<extra></extra>'}],
    {paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},barmode:'relative',
     margin:{t:8,b:36,l:62,r:120},legend:{orientation:'h',y:1.08},
     xaxis:{title:'$M per 1% move',gridcolor:'#30363d',zerolinecolor:'#484f58'},
     yaxis:{title:'strike',gridcolor:'#21262d',dtick:10},
     shapes:[
       hl(spot,'#d29922','solid'), zg!=null?hl(zg,'#e6edf3','dot'):null,
       cw!=null?hl(cw,'#3fb950','dash'):null, pw!=null?hl(pw,'#f85149','dash'):null
     ].filter(Boolean),
     annotations:[
       an(spot,'spot '+f1(spot,0),'#d29922'), zg!=null?an(zg,'zero gamma','#e6edf3'):null,
       cw!=null?an(cw,'call wall','#3fb950'):null, pw!=null?an(pw,'put wall','#f85149'):null
     ].filter(Boolean)},{responsive:true});
  document.getElementById('status').textContent='updated '+new Date().toLocaleTimeString();
}
const hl=(y,c,d)=>({type:'line',xref:'paper',x0:0,x1:1,y0:y,y1:y,line:{color:c,width:1.5,dash:d}});
const an=(y,t,c)=>({xref:'paper',x:1,xanchor:'left',y:y,text:t,showarrow:false,
                    font:{size:10.5,color:c},bgcolor:'#0e1117'});

async function loadHist(){
  try{
    const r=await fetch('/api/gex-state/history',{credentials:'same-origin'});
    const j=await r.json(); const rows=(j.rows||[]);
    const tb=document.querySelector('#hist tbody'); tb.innerHTML='';
    rows.slice().reverse().slice(0,40).forEach(x=>{
      const c=TIERC[x.tier]||'#8b949e';
      tb.insertAdjacentHTML('beforeend',`<tr><td>${x.et.slice(11,16)}</td>
        <td style="color:${c}">${(x.state||'').replace(/_/g,' ')}</td><td>${f1(x.spot,2)}</td>
        <td>${f1(x.zero_gamma,2)}</td><td>${f1(x.net_gex,0)}</td><td>${f1(x.net_dex,0)}</td></tr>`);});
    if(rows.length){
      Plotly.newPlot('tl',[{x:rows.map(x=>x.et),y:rows.map(x=>x.spot),type:'scatter',mode:'markers',
        marker:{size:7,color:rows.map(x=>TIERC[x.tier]||'#8b949e')},
        text:rows.map(x=>(x.state||'').replace(/_/g,' ')),
        hovertemplate:'%{x|%H:%M} · %{text}<br>spot %{y:.2f}<extra></extra>'},
        {x:rows.map(x=>x.et),y:rows.map(x=>x.zero_gamma),type:'scatter',mode:'lines',
         name:'zero gamma',line:{color:'#e6edf3',width:1,dash:'dot'},hoverinfo:'skip'}],
        {paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},showlegend:false,
         margin:{t:6,b:28,l:56,r:10},xaxis:{gridcolor:'#30363d'},
         yaxis:{title:'spot',gridcolor:'#30363d'}},{responsive:true});
    }
  }catch(e){}
}
async function load(){
  const at=document.getElementById('at').value;
  document.getElementById('status').textContent='loading...';
  try{
    const q=at?('?at='+encodeURIComponent(new Date(at).toISOString())):'';
    const r=await fetch('/api/gex-state/profile'+q,{credentials:'same-origin'});
    render(await r.json());
  }catch(e){document.getElementById('status').textContent='error: '+e;}
  loadHist();
}
load();
setInterval(()=>{if(document.getElementById('live').checked && !document.getElementById('at').value) load();},60000);
</script></body></html>"""
