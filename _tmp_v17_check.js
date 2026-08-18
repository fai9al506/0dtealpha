// Verifies the V17 JS branch (copied verbatim from app/main.py) against the Python version.
const rows = require('./_tmp_v17_rows.json');
function v17(l, v16delegate) {
  const _v17Allowed = new Set(['Skew Charm', 'AG Short', 'Vanna Pivot Bounce',
                               'ES Absorption', 'DD Exhaustion', 'VIX Divergence']);
  const _sn17 = l.setup_name || '';
  if (!_v17Allowed.has(_sn17)) return false;
  const _long17 = l.direction === 'long' || l.direction === 'bullish';
  const _vix17 = l.vix != null ? l.vix : 0;
  const _relaxed17 = new Set(['Skew Charm', 'AG Short', 'ES Absorption',
                              'DD Exhaustion', 'VIX Divergence']);
  if (_vix17 >= 22 || !_relaxed17.has(_sn17)) return v16delegate;
  if (_sn17 === 'VIX Divergence') return _long17;
  if (_sn17 === 'DD Exhaustion' && !_long17) {
    const _ga17 = l.v13_gex_above != null ? l.v13_gex_above : 0;
    const _dn17 = l.v13_dd_near != null ? l.v13_dd_near : 0;
    if (_ga17 >= 75 || _dn17 >= 3000000000) return false;
    if (l.vanna_cliff_side === 'A' && l.vanna_peak_side === 'B') return false;
    const _g17 = l.grade;
    if (l.paradigm === 'BOFA-PURE' || _g17 === 'A+' || _g17 === 'C') return false;
    if (l.paradigm === 'GEX-LIS') return false;
    return (l.greek_alignment != null ? l.greek_alignment : 0) !== 0;
  }
  return true;
}
let diff = 0, pass = 0;
for (const l of rows) {
  const js = v17(l, l.v16);
  if (js) pass++;
  if (js !== l.py17) {
    if (diff < 8) console.log('DIFF', l.setup_name, l.direction, 'js', js, 'py', l.py17,
                              'vix', l.vix, 'grade', l.grade, 'para', l.paradigm);
    diff++;
  }
}
console.log(`checked ${rows.length}  js_pass=${pass}  diffs=${diff}`);
console.log(diff === 0 ? 'JS/PYTHON PARITY OK' : '*** MISMATCH ***');
