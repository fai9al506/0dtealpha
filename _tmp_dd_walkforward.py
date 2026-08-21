# -*- coding: utf-8 -*-
"""Proper walk-forward test of DD Exhaustion trail params.
Clean 1-min SPX basis. Train on earlier period, test out-of-sample on later.
Current live = activation 20 / gap 5 / SL 20 (continuous trail)."""
import os, sys, pickle, bisect
sys.path.insert(0,'.')
from app.mes_sim_backfill import mes_walk
univ,bars,bt=pickle.load(open("_tmp_1min_univ.pkl","rb"))
def bars_after(ts,mm):
    e0=ts.timestamp(); e1=e0+mm*60
    i=bisect.bisect_left(bt,e0); out=[]
    j=i
    while j<len(bars) and bt[j]<=e1: out.append(bars[j]); j+=1
    return out

SL=12
def sim(trades, act, gap):
    pnls=[]
    for u in trades:
        mm=max(u['elapsed']+30,60)
        r=mes_walk(bars_after(u['ts'],mm),u['entry'],u['is_long'],SL,None,0,act,gap,mm)
        pnls.append(r['pnl'])
    return pnls
def stats(pnls):
    n=len(pnls)
    if not n: return (0,0,0,0)
    tot=sum(pnls); wr=sum(1 for p in pnls if p>0)/n*100
    cum=peak=dd=0
    for p in pnls:
        cum+=p; peak=max(peak,cum); dd=min(dd,cum-peak)
    return (round(tot,1),round(wr,1),round(dd,1),n)

CUR=(20,5)  # current live
GRID=[(a,g) for a in (8,10,12,15,20) for g in (3,5,8,10)]

for label, only_sb in [("V16-SB DD trades (go-forward set)", True), ("ALL DD trades (max sample)", False)]:
    dd=[u for u in univ if u['setup']=='DD Exhaustion' and (u['live_pass'] or not only_sb)]
    dd.sort(key=lambda u:u['ts'])
    n=len(dd)
    print("\n"+"="*78)
    print(f"{label}: n={n}, {dd[0]['ts'].date()} -> {dd[-1]['ts'].date()}")
    print("="*78)
    if n<30:
        print("  too few for walk-forward."); continue

    # 3 contiguous folds: train on fold[i], test on fold[i+1]
    import math
    k=3; sz=math.ceil(n/k)
    folds=[dd[i*sz:(i+1)*sz] for i in range(k)]
    for i in range(k-1):
        train, test = folds[i], folds[i+1]
        # best by PnL on train
        best=max(GRID, key=lambda ag: sum(sim(train,*ag)))
        bt_tr=stats(sim(train,*best)); bt_te=stats(sim(test,*best))
        cur_te=stats(sim(test,*CUR))
        print(f"\n FOLD {i+1}: train {train[0]['ts'].date()}..{train[-1]['ts'].date()} (n={len(train)}) | test {test[0]['ts'].date()}..{test[-1]['ts'].date()} (n={len(test)})")
        print(f"   train-best param: a{best[0]}/g{best[1]}")
        print(f"   OUT-OF-SAMPLE test:  train-best a{best[0]}/g{best[1]} -> PnL {bt_te[0]:+}, WR {bt_te[1]}%, DD {bt_te[2]}")
        print(f"                        current   a20/g5            -> PnL {cur_te[0]:+}, WR {cur_te[1]}%, DD {cur_te[2]}")
        win = "TRAIN-BEST WINS OOS" if bt_te[0]>cur_te[0] else "current wins / no improvement"
        print(f"   => {win}")

    # single 60/40 split (more samples per side)
    cut=int(n*0.6); train,test=dd[:cut],dd[cut:]
    best=max(GRID, key=lambda ag: sum(sim(train,*ag)))
    print(f"\n 60/40 SPLIT: train n={len(train)} ({train[0]['ts'].date()}..{train[-1]['ts'].date()}), test n={len(test)} ({test[0]['ts'].date()}..{test[-1]['ts'].date()})")
    print(f"   train-best: a{best[0]}/g{best[1]}")
    print(f"   OOS test  a{best[0]}/g{best[1]}: {stats(sim(test,*best))}")
    print(f"   OOS test  a20/g5 (current): {stats(sim(test,*CUR))}")
    print(f"   (stats = PnL, WR%, MaxDD, n)")

    # candidate a15/g10 specifically (my earlier proposal) OOS on the 40% test
    print(f"   OOS test  a15/g10 (my proposal): {stats(sim(test,15,10))}")
    print(f"   OOS test  a10/g10 (max-WR cand): {stats(sim(test,10,10))}")
