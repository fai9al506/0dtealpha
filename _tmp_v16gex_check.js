const rows = require('./_tmp_v17_rows.json');
// mirror of the patched allowed-set logic
function allowed(sn, gexReal) {
  const s = new Set(['Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion','VIX Divergence']);
  if (gexReal) s.add('GEX Long');
  return s.has(sn);
}
let off=0, on=0;
for (const l of rows) { if (allowed(l.setup_name,false)) off++; if (allowed(l.setup_name,true)) on++; }
console.log(`rows passing the allowed-set: GEX off ${off}, GEX on ${on} (of ${rows.length})`);
console.log('note: _tmp_v17_rows.json contains no GEX Long rows, so equal counts are expected here');
