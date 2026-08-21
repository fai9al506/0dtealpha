# -*- coding: utf-8 -*-
"""Dip-Buy feature study: which indicator boosts WR? Tests VIX level, VIX movement
(pre-signal + during dip), VX divergence, charm regime/sign, paradigm, vanna(svb),
DD hedging, dip depth, time-of-day, gap, vs-prior-close. Baseline exit T10/S8.
Reconstruction caveat: ~67% trade-match to live; aggregate WR matches (55% vs 53%)."""
import os, pickle, json, bisect, psycopg2
from datetime import timezone
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()
sigs,days,daylist=pickle.load(open("_tmp_dipbuy_sigs.pkl","rb"))

def ep(dt_et_naive):  # ET-naive wall time -> utc epoch
    return dt_et_naive.replace(tzinfo=ET).timestamp()

# VIX ticks
cur.execute("SELECT extract(epoch from ts), price FROM vps_vix_ticks WHERE price IS NOT NULL ORDER BY ts")
vt=cur.fetchall(); vte=[float(x[0]) for x in vt]; vtp=[float(x[1]) for x in vt]
def vix_at(epoch):
    i=bisect.bisect_left(vte,epoch)
    best=None
    for j in (i-1,i):
        if 0<=j<len(vte) and abs(vte[j]-epoch)<=300:
            if best is None or abs(vte[j]-epoch)<abs(vte[best]-epoch): best=j
    return vtp[best] if best is not None else None
def vix_max_between(e0,e1):
    i=bisect.bisect_left(vte,e0); j=bisect.bisect_right(vte,e1)
    return max(vtp[i:j]) if j>i else None

# volland snapshots
cur.execute("SELECT extract(epoch from ts), payload FROM volland_snapshots WHERE payload IS NOT NULL ORDER BY ts")
vs=[]
for e,p in cur.fetchall():
    p=p if isinstance(p,dict) else json.loads(p); st=p.get('statistics',{}) or {}
    svb=st.get('spot_vol_beta',{}) or {}
    vs.append((float(e), st.get('paradigm'), st.get('aggregatedCharm'),
               (svb.get('correlation') if isinstance(svb,dict) else None),
               st.get('delta_decay_hedging')))
vse=[x[0] for x in vs]
def voll_at(epoch):
    i=bisect.bisect_right(vse,epoch)-1  # last snapshot <= entry
    if 0<=i<len(vs) and epoch-vse[i]<=900: return vs[i]
    return None

# attach features
for s in sigs:
    een=ep(s['entry_ts']); shn=ep(s['sh_ts']) if s.get('sh_ts') else None
    s['vix']=vix_at(een)
    s['vix_at_sh']=vix_at(shn) if shn else None
    s['vix_chg_dip']=(s['vix']-s['vix_at_sh']) if (s['vix'] and s['vix_at_sh']) else None  # VIX move during dip
    s['vix_pre30']=(s['vix_at_sh']-vix_at(shn-1800)) if (shn and s['vix_at_sh'] and vix_at(shn-1800)) else None
    vmax=vix_max_between(shn,een) if shn else None
    s['vx_diverge']=((vmax-s['vix_at_sh'])<=0.10) if (vmax is not None and s['vix_at_sh']) else None
    v=voll_at(een)
    s['paradigm']=v[1] if v else None
    s['charm']=float(v[2]) if (v and v[2] is not None) else None
    s['svb_corr']=float(v[3]) if (v and v[3] is not None) else None
    # day context
    op=days[s['date']][0][1]; pc=s['prior_close']
    s['gap']=(op-pc) if pc else None
    s['prior_close_ok']=(pc is not None and s['entry']>=pc-2)

def wr(subset):
    n=len(subset);
    if not n: return (0,0,0.0)
    w=sum(1 for x in subset if x['win']); return (n,w,w/n*100)

print(f"n={len(sigs)} reconstructed signals | baseline T10/S8 WR={wr(sigs)[2]:.1f}%\n")
print("="*72); print("CATEGORICAL FEATURES — WR by group"); print("="*72)
def cat(name, keyfn):
    groups={}
    for s in sigs:
        k=keyfn(s)
        if k is None: continue
        groups.setdefault(k,[]).append(s)
    print(f"\n[{name}]")
    for k in sorted(groups, key=lambda k:-wr(groups[k])[2]):
        n,w,p=wr(groups[k]);
        if n>=4: print(f"   {str(k):<22} n={n:<3} WR={p:>5.1f}%  ({w}W/{n-w}L)")
cat("prior_close_ok", lambda s:s['prior_close_ok'])
cat("vx_diverge (no new VIX high in dip)", lambda s:s['vx_diverge'])
cat("charm sign", lambda s:("charm+" if s['charm']>0 else "charm-") if s['charm'] is not None else None)
cat("paradigm", lambda s:s['paradigm'])
cat("gap dir", lambda s:("gap_up" if s['gap']>3 else "gap_dn" if s['gap']<-3 else "flat") if s['gap'] is not None else None)

print("\n"+"="*72); print("CONTINUOUS FEATURES — WR low-half vs high-half (median split)"); print("="*72)
def cont(name, key):
    vals=[(s[key],s) for s in sigs if s.get(key) is not None]
    if len(vals)<12: print(f"\n[{name}] n={len(vals)} too few"); return
    vals.sort(key=lambda x:x[0]); m=len(vals)//2
    lo=[s for _,s in vals[:m]]; hi=[s for _,s in vals[m:]]
    med=vals[m][0]
    nl,wl,pl=wr(lo); nh,wh,ph=wr(hi)
    print(f"\n[{name}] median={med:.2f}  LOW n={nl} WR={pl:.1f}%  |  HIGH n={nh} WR={ph:.1f}%  lift={ph-pl:+.1f}")
for nm,k in [("dip_depth","dip_depth"),("mins_from_open","mins"),("vs_prior_close","vs_prior_close"),
             ("gap","gap"),("VIX level","vix"),("VIX chg during dip","vix_chg_dip"),
             ("VIX pre-30min chg","vix_pre30"),("charm magnitude","charm"),("svb correlation","svb_corr")]:
    cont(nm,k)

# best single threshold filter (n>=20) maximizing WR
print("\n"+"="*72); print("BEST SINGLE FILTERS (subset n>=20, ranked by WR)"); print("="*72)
cands=[]
import itertools
for key in ['dip_depth','mins','vs_prior_close','gap','vix','vix_chg_dip','vix_pre30','charm','svb_corr']:
    vals=sorted(set(s[key] for s in sigs if s.get(key) is not None))
    for thr in vals:
        for op,lbl in [('>=','>='),('<=','<=')]:
            sub=[s for s in sigs if s.get(key) is not None and (s[key]>=thr if op=='>=' else s[key]<=thr)]
            if len(sub)>=20:
                n,w,p=wr(sub); cands.append((p,n,f"{key} {lbl} {thr:.2f}"))
cands.sort(reverse=True)
for p,n,lbl in cands[:12]:
    print(f"   {lbl:<26} n={n:<3} WR={p:.1f}%")

pickle.dump((sigs,days,daylist),open("_tmp_dipbuy_enriched.pkl","wb"))

# ---- OUT-OF-SAMPLE validation of the top hypotheses ----
print("\n"+"="*72); print("OUT-OF-SAMPLE robustness (train<=2026-04-30, test>=2026-05-01)"); print("="*72)
tr=[s for s in sigs if s['date']<='2026-04-30']; te=[s for s in sigs if s['date']>='2026-05-01']
def f_wr(sub):
    n=len(sub); w=sum(1 for x in sub if x['win']); return (n, w/n*100 if n else 0)
FILT={
 "ALL (no filter)":          lambda s:True,
 "above prior close (>=0)":  lambda s:s['vs_prior_close'] is not None and s['vs_prior_close']>=0,
 "well above pc (>=10)":     lambda s:s['vs_prior_close'] is not None and s['vs_prior_close']>=10,
 "gap up (>=0)":             lambda s:s['gap'] is not None and s['gap']>=0,
 "after 15min":              lambda s:s['mins']>=15,
 "VIX rose in dip (>=0)":    lambda s:s['vix_chg_dip'] is not None and s['vix_chg_dip']>=0,
 "not GEX-TARGET/LIS":       lambda s:s['paradigm'] not in ('GEX-TARGET','GEX-LIS'),
 "uptrend(pc>=0)+after15":   lambda s:(s['vs_prior_close'] is not None and s['vs_prior_close']>=0) and s['mins']>=15,
}
print(f"  {'filter':<26}{'TRAIN n/WR':>16}{'TEST n/WR':>16}")
for name,fn in FILT.items():
    tn,tw=f_wr([s for s in tr if fn(s)]); en,ew=f_wr([s for s in te if fn(s)])
    print(f"  {name:<26}{f'{tn}/{tw:.0f}%':>16}{f'{en}/{ew:.0f}%':>16}")

# ---- refined spec: best robust filter + SL/TP on filtered set ----
print("\n"+"="*72); print("SL/TP on UPTREND-filtered set (vs_prior_close>=0 AND mins>=15)"); print("="*72)
def sim_outcome(sig,T,S):
    bars=days[sig['date']]; e=sig['entry']; et0=sig['entry_ts']
    for et,o,h,l,c in bars:
        if et<=et0: continue
        if c<=e-S: return round(c-e,2)
        if c>=e+T: return round(c-e,2)
        if et.time()>=__import__('datetime').time(16,0): return round(c-e,2)
    return round(bars[-1][4]-e,2)
filt=[s for s in sigs if (s['vs_prior_close'] is not None and s['vs_prior_close']>=0) and s['mins']>=15]
print(f"filtered n={len(filt)} (of {len(sigs)})")
def maxdd(seq):
    cum=peak=dd=0
    for x in seq: cum+=x; peak=max(peak,cum); dd=min(dd,cum-peak)
    return dd
for T,S in [(10,8),(8,8),(8,12),(10,12),(8,15),(6,12),(10,15)]:
    pnls=[sim_outcome(s,T,S) for s in filt]; n=len(pnls); w=sum(1 for p in pnls if p>0)
    print(f"  T{T}/S{S}: WR={w/n*100:.0f}%  PnL={sum(pnls):+.0f}  exp={sum(pnls)/n:+.1f}  maxDD={maxdd(pnls):.0f}")
conn.close()
