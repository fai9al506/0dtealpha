# -*- coding: utf-8 -*-
"""Dark Mate FW — framework map: Gamma + Vanna shown together (incremental multi-expiry),
live + history. Manual-trade map."""

DARKMATE_FW_HTML = r"""<!doctype html><html><head><meta charset="utf-8">
<title>Dark Mate FW</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body{background:#0e1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:1120px;margin:0 auto;padding:16px;line-height:1.4}
h1{font-size:20px;margin:4px 0} a{color:#58a6ff}
.bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:8px 0}
select,button,input{background:#161b22;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:5px 9px}
button{cursor:pointer} .mut{color:#8b949e;font-size:12px}
.key{display:flex;gap:8px;flex-wrap:wrap;margin:4px 0 2px}
.kb{background:#161b22;border:1px solid #30363d;border-radius:7px;padding:5px 10px;font-size:12.5px}
.lab{font-weight:700;color:#bc8cff;margin:8px 0 0;font-size:14px}
</style></head><body>
<h1>⚛️ Dark Mate FW</h1>
<div class="bar">
  <label>Range <select id="rng"><option value="100">±100</option><option value="150" selected>±150</option><option value="200">±200</option><option value="250">±250</option></select></label>
  <label><input type="checkbox" id="live" checked> Live 60s</label>
  <label>History <input type="datetime-local" id="at"></label>
  <button onclick="load()">Load</button>
  <span id="status" class="mut"></span>
</div>

<div class="lab">GAMMA</div>
<div class="key" id="keyG"></div>
<div id="chartG" style="height:360px"></div>

<div class="lab">VANNA</div>
<div class="key" id="keyV"></div>
<div id="chartV" style="height:360px"></div>

<script>
const EXP=[['dte0','0DTE','#3fb950'],['weekly','Weekly','#58a6ff'],['monthly','Monthly','#bc8cff'],['far','Far (>30d)','#8b949e']];
function renderOne(divId,j,showLegend){
  const p=j.profile, spot=j.spot;
  const traces=EXP.map(([k,nm,c])=>({x:p.map(s=>s.strike),y:p.map(s=>s[k]),type:'bar',name:nm,marker:{color:c},
     showlegend:showLegend,hovertemplate:'%{x} · '+nm+': %{y}M<extra></extra>'}));
  Plotly.newPlot(divId,traces,
    {paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},barmode:'relative',hovermode:'x unified',
     margin:{t:10,b:34,l:50,r:10},legend:{orientation:'h',y:1.12},
     xaxis:{gridcolor:'#30363d'},yaxis:{title:'$M',gridcolor:'#30363d'},
     shapes:[{type:'line',x0:spot,x1:spot,yref:'paper',y0:0,y1:1,line:{color:'#d29922',width:2,dash:'dot'}}]},{responsive:true});
}
function keyChips(elId,j){
  const k=j.key;
  document.getElementById(elId).innerHTML=
    `<div class="kb">spot <b>${j.spot}</b>${j.spot_live?' <span class="mut">live</span>':''}</div>`+
    `<div class="kb">barrier above <b>${k.barrier_above||'—'}</b></div>`+
    `<div class="kb">support below <b>${k.barrier_below||'—'}</b></div>`+
    `<div class="kb">accel above <b>${k.accel_above||'—'}</b></div>`;
}
async function load(){
  const rng=document.getElementById('rng').value;
  const at=document.getElementById('at').value;
  const q='&rng='+rng+(at?('&at='+encodeURIComponent(new Date(at).toISOString())):'');
  document.getElementById('status').textContent='loading...';
  try{
    const [gr,vr]=await Promise.all([
      fetch('/api/darkmate/levels?greek=gamma'+q,{cache:'no-store'}).then(r=>r.json()),
      fetch('/api/darkmate/levels?greek=vanna'+q,{cache:'no-store'}).then(r=>r.json())
    ]);
    if(gr.error||vr.error){document.getElementById('status').textContent='err';return;}
    renderOne('chartG',gr,true); keyChips('keyG',gr);
    renderOne('chartV',vr,false); keyChips('keyV',vr);
    document.getElementById('status').textContent='spot '+gr.spot+(gr.spot_live?' (live)':'')+' · '+(gr.snap_ts?gr.snap_ts.slice(11,16)+'Z':'');
  }catch(e){document.getElementById('status').textContent='err: '+e;}
}
function tick(){ if(document.getElementById('live').checked && !document.getElementById('at').value) load(); }
document.getElementById('rng').addEventListener('change',load);
load(); setInterval(tick,60000);
</script></body></html>"""
