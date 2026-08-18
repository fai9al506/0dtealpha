import os, sys, importlib.util, io, contextlib
sys.argv=['x']
def load(gex=False):
    if gex: sys.argv=['x','--gex']
    spec=importlib.util.spec_from_file_location("m"+str(gex),"_tmp_sb_block_analysis.py")
    m=importlib.util.module_from_spec(spec)
    with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)
    sys.argv=['x']; return m

for gex in (False,True):
    m=load(gex)
    print(f"\n===== GEX Long {'ON' if gex else 'OFF'} =====")
    for pol,cap in [("base012",2),("base012",3),("sb012",3)]:
        # rerun counting contracts
        r=m.run(pol,cap,cap)
        # contracts: recount
        import collections
        from datetime import timedelta
        contracts=0
        byday=collections.defaultdict(list)
        for x in m.cands: byday[x["ts"].astimezone(m.ET).date()].append(x)
        # cheap: re-derive qty distribution from policy on taken trades is complex; approximate via run
        hc_lo = r["n"]*0.18*5*1.0
        hc_hi = r["n"]*0.18*5*2.0
        print(f"  {pol:<8} cap{cap}/{cap}: gross ${r['total']:>7,.0f}  {r['n']:>3}t  WR {r['wr']:.0f}%  MaxDD ${r['dd']:>6,.0f}"
              f"   net after chain haircut: ${r['total']-hc_hi:,.0f} .. ${r['total']-hc_lo:,.0f}")
