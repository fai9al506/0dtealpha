// =============================================================================
// ES DOM Snapshot — Apollo-style depth capture for E-mini S&P futures
// =============================================================================
// Snapshots top N bid/ask depth levels every N seconds, writes JSONL for
// vps_data_bridge.py pickup.
//
// Add to an ES futures chart (e.g., ESM26-CME) that has depth subscription.
// Requires sc.UsesMarketDepthData = 1 + CME Market Depth (US$) on Rithmic.
//
// Output file format (es_dom.jsonl — APPEND mode, one JSON per line):
//   {"ts":"2026-05-22T14:55:01","s":"ESM26-CME","bid":[[7503.50,42,7],...],"ask":[[7503.75,18,4],...]}
//   Each level array = [price, quantity, num_orders]
//
// Mirrors VXDomSnapshot.cpp 1:1 — same fopen("ab") pattern, same throttle,
// same features CSV optional output. The ONLY differences are:
//   * default output path: es_dom.jsonl / es_dom_features.csv
//   * default chart symbol: ESM26-CME (whichever ES contract the chart is on)
//   * features CSV row identical schema (so bridge processing is uniform)
//
// To build:
//   1. Drop this file into C:\SierraChart\ACS_Source\
//   2. Analysis menu → Build Custom Studies DLL → select this file
//   3. After build, "ES DOM Snapshot" appears in Studies → Add Custom Study
//   4. Add to the same ESM26-CME chart used by vps_data_bridge (depth required)
//   5. Settings: leave defaults (1s interval, 10 levels each side)
//   6. Restart vps_data_bridge.py to pick up the JSONL
//
// Build worker note (from VX experience): if the build server lands on ARM64
// and the resulting DLL crashes Sierra on load, just delete the DLL + retry
// the build. Build worker is randomly assigned by Sierra build server.
// =============================================================================

#include "sierrachart.h"
#include <cstdio>
#include <ctime>

SCDLLName("ES DOM Snapshot")

SCSFExport scsf_ESDomSnapshot(SCStudyInterfaceRef sc)
{
    SCInputRef InEnabled            = sc.Input[0];
    SCInputRef InSnapIntervalSec    = sc.Input[1];
    SCInputRef InMaxLevels          = sc.Input[2];
    SCInputRef InOutputFile         = sc.Input[3];
    SCInputRef InEmitFeatures       = sc.Input[4];
    SCInputRef InFeaturesFile       = sc.Input[5];

    if (sc.SetDefaults)
    {
        sc.GraphName            = "ES DOM Snapshot";
        sc.StudyDescription     = "Snapshots top N bid/ask depth levels every N seconds. "
                                  "Writes JSONL for VPS bridge pickup. Add to ES futures chart with depth feed.";
        sc.AutoLoop             = 0;
        sc.UpdateAlways         = 1;
        sc.MaintainAdditionalChartDataArrays = 0;
        sc.UsesMarketDepthData  = 1;
        sc.GraphRegion          = 0;

        InEnabled.Name = "Enabled";
        InEnabled.SetYesNo(1);

        InSnapIntervalSec.Name = "Snapshot Interval (sec)";
        InSnapIntervalSec.SetInt(1);
        InSnapIntervalSec.SetIntLimits(1, 60);

        InMaxLevels.Name = "Max Levels Each Side";
        InMaxLevels.SetInt(10);
        InMaxLevels.SetIntLimits(1, 50);

        InOutputFile.Name = "DOM Output File Path";
        InOutputFile.SetPathAndFileName("C:\\SierraChart\\Data\\es_dom.jsonl");

        InEmitFeatures.Name = "Also Emit Derived Features";
        InEmitFeatures.SetYesNo(1);

        InFeaturesFile.Name = "Features Output File Path";
        InFeaturesFile.SetPathAndFileName("C:\\SierraChart\\Data\\es_dom_features.csv");

        return;
    }

    if (InEnabled.GetYesNo() == 0)
        return;

    // ---- Throttle: one snap per InSnapIntervalSec (epoch-second based) ----
    int& LastSnapEpoch = sc.GetPersistentInt(1);

    int NowEpoch = (int)time(NULL);
    int Interval = InSnapIntervalSec.GetInt();

    if (LastSnapEpoch > 0 && (NowEpoch - LastSnapEpoch) < Interval)
        return;

    LastSnapEpoch = NowEpoch;

    // ---- Build JSONL line ----
    int MaxLevels = InMaxLevels.GetInt();

    SCDateTime Now = sc.CurrentSystemDateTime;
    int Y, M, D, H, Mi, S;
    Now.GetDateTimeYMDHMS(Y, M, D, H, Mi, S);

    SCString Line;
    Line.Format("{\"ts\":\"%04d-%02d-%02dT%02d:%02d:%02d\",\"s\":\"%s\",\"bid\":[",
                Y, M, D, H, Mi, S, sc.Symbol.GetChars());

    int BidCount = sc.GetBidMarketDepthNumberOfLevels();
    if (BidCount > MaxLevels) BidCount = MaxLevels;

    double BidTopPrice = 0.0, BidTopQty = 0.0;
    double BidSumQty5 = 0.0;
    double BidWeightedPx = 0.0;

    for (int i = 0; i < BidCount; i++)
    {
        s_MarketDepthEntry e;
        if (!sc.GetBidMarketDepthEntryAtLevel(e, i)) break;
        if (i > 0) Line += ",";
        SCString Entry;
        Entry.Format("[%.4f,%.0f,%d]", e.Price, e.Quantity, e.NumOrders);
        Line += Entry;
        if (i == 0) { BidTopPrice = e.Price; BidTopQty = e.Quantity; }
        if (i < 5)  { BidSumQty5 += e.Quantity; BidWeightedPx += e.Price * e.Quantity; }
    }

    Line += "],\"ask\":[";

    int AskCount = sc.GetAskMarketDepthNumberOfLevels();
    if (AskCount > MaxLevels) AskCount = MaxLevels;

    double AskTopPrice = 0.0, AskTopQty = 0.0;
    double AskSumQty5 = 0.0;
    double AskWeightedPx = 0.0;

    for (int i = 0; i < AskCount; i++)
    {
        s_MarketDepthEntry e;
        if (!sc.GetAskMarketDepthEntryAtLevel(e, i)) break;
        if (i > 0) Line += ",";
        SCString Entry;
        Entry.Format("[%.4f,%.0f,%d]", e.Price, e.Quantity, e.NumOrders);
        Line += Entry;
        if (i == 0) { AskTopPrice = e.Price; AskTopQty = e.Quantity; }
        if (i < 5)  { AskSumQty5 += e.Quantity; AskWeightedPx += e.Price * e.Quantity; }
    }

    Line += "]}\n";

    // ---- Write raw JSONL (append mode via standard C fopen) ----
    SCString DomPath = InOutputFile.GetPathAndFileName();
    if (DomPath.GetLength() > 0)
    {
        FILE* f = fopen(DomPath.GetChars(), "ab");
        if (f != NULL)
        {
            fwrite(Line.GetChars(), 1, Line.GetLength(), f);
            fclose(f);
        }
    }

    // ---- Optionally emit compact derived features ----
    if (InEmitFeatures.GetYesNo() == 1 && BidCount > 0 && AskCount > 0)
    {
        double SpreadPx   = AskTopPrice - BidTopPrice;
        double MidPx      = (AskTopPrice + BidTopPrice) / 2.0;
        double TopImb     = (BidTopQty + AskTopQty > 0)
                            ? (BidTopQty - AskTopQty) / (BidTopQty + AskTopQty) : 0.0;
        double Top5Imb    = (BidSumQty5 + AskSumQty5 > 0)
                            ? (BidSumQty5 - AskSumQty5) / (BidSumQty5 + AskSumQty5) : 0.0;
        double WMid       = (BidSumQty5 + AskSumQty5 > 0)
                            ? (BidWeightedPx + AskWeightedPx) / (BidSumQty5 + AskSumQty5) : MidPx;

        SCString FeatLine;
        FeatLine.Format("%04d-%02d-%02dT%02d:%02d:%02d,%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.0f,%.0f,%.0f,%.0f\n",
                        Y, M, D, H, Mi, S, sc.Symbol.GetChars(),
                        BidTopPrice, AskTopPrice, MidPx, WMid, SpreadPx,
                        TopImb, BidTopQty, AskTopQty, BidSumQty5, AskSumQty5);
        // CSV cols: ts, sym, bid_top, ask_top, mid, wmid, spread, top_imb, top5_imb,
        //           bid_top_qty, ask_top_qty, bid_sum5, ask_sum5

        SCString FeatPath = InFeaturesFile.GetPathAndFileName();
        if (FeatPath.GetLength() > 0)
        {
            FILE* fFeat = fopen(FeatPath.GetChars(), "ab");
            if (fFeat != NULL)
            {
                fwrite(FeatLine.GetChars(), 1, FeatLine.GetLength(), fFeat);
                fclose(fFeat);
            }
        }
    }
}
