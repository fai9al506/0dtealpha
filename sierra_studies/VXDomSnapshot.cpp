// =============================================================================
// VX DOM Snapshot — Apollo-style depth capture for VIX futures
// =============================================================================
// Snapshots top N bid/ask depth levels every N seconds, writes JSONL for
// vps_data_bridge.py pickup.
//
// Add to a VX futures chart (e.g., VXM26_FUT_CFE) that has depth subscription.
// Requires sc.UsesMarketDepthData = 1.
//
// Output file format (vx_dom.jsonl — APPEND mode, one JSON per line):
//   {"ts":"2026-05-17T19:30:01","s":"VXM26_FUT_CFE","bid":[[21.40,15,3],...],"ask":[[21.50,12,2],...]}
//   Each level array = [price, quantity, num_orders]
//
// Bug-fix note: Sierra sc.OpenFile() returns 1 on success, 0 on failure.
// (VolDetector.cpp uses == 0 which is the inverted check — also needs fix.)
// =============================================================================

#include "sierrachart.h"
#include <cstdio>
#include <ctime>

SCDLLName("VX DOM Snapshot")

SCSFExport scsf_VXDomSnapshot(SCStudyInterfaceRef sc)
{
    SCInputRef InEnabled            = sc.Input[0];
    SCInputRef InSnapIntervalSec    = sc.Input[1];
    SCInputRef InMaxLevels          = sc.Input[2];
    SCInputRef InOutputFile         = sc.Input[3];
    SCInputRef InEmitFeatures       = sc.Input[4];
    SCInputRef InFeaturesFile       = sc.Input[5];

    if (sc.SetDefaults)
    {
        sc.GraphName            = "VX DOM Snapshot";
        sc.StudyDescription     = "Snapshots top N bid/ask depth levels every N seconds. "
                                  "Writes JSONL for VPS bridge pickup. Add to VX futures chart with depth feed.";
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
        InOutputFile.SetPathAndFileName("C:\\SierraChart\\Data\\vx_dom.jsonl");

        InEmitFeatures.Name = "Also Emit Derived Features";
        InEmitFeatures.SetYesNo(1);

        InFeaturesFile.Name = "Features Output File Path";
        InFeaturesFile.SetPathAndFileName("C:\\SierraChart\\Data\\vx_dom_features.csv");

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
    // sc.OpenFile() append mode is unreliable / not in the public enum;
    // standard CRT fopen("ab") is portable inside Sierra DLLs.
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
