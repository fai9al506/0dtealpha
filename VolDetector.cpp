// =============================================================================
// VolDetector.cpp — Sierra Chart ACSIL Study (Apollo-Style Order Flow)
// =============================================================================
//
// Apollo's method: one dot per bar based on NET DELTA direction.
// Strong negative delta = vol sellers (GREEN dot below bar)
// Strong positive delta = vol buyers (RED dot above bar)
// Never both on the same bar.
//
//   scsf_VolDetector (GraphRegion 0 — main price chart):
//     - GREEN dot BELOW bar = vol sellers dominating (bearish VIX = bullish SPX)
//     - RED dot ABOVE bar = vol buyers dominating (bullish VIX = bearish SPX)
//     - Only fires when |delta| exceeds threshold × 20-bar avg |delta|
//     - Writes signal file for bridge pickup on completed bars
//
//   scsf_VolDetectorPanel (GraphRegion 1 — optional subgraph):
//     - Delta per bar + CVD line
//
// Signal file format (vol_signal.txt — overwritten each signal):
//   Line 1: direction,price,delta,ask_vol,bid_vol,avg_delta,ratio,bar_ts
//   direction: -1 = vol sellers (bullish SPX), +1 = vol buyers (bearish SPX)
//
// =============================================================================

#include "sierrachart.h"
#include <cstdio>     // snprintf
#include <cstring>    // strlen

SCDLLName("VolDetector")

// =============================================================================
// Study 1: Vol Detector — Main Chart Dots + Signal File Output
// =============================================================================
SCSFExport scsf_VolDetector(SCStudyInterfaceRef sc)
{
    SCSubgraphRef VolSell   = sc.Subgraph[0];   // Green below = vol sellers
    SCSubgraphRef VolBuy    = sc.Subgraph[1];   // Red above = vol buyers

    SCInputRef InDeltaMult  = sc.Input[0];
    SCInputRef InDotSize    = sc.Input[1];
    SCInputRef InDotOffset  = sc.Input[2];
    SCInputRef InLookback   = sc.Input[3];
    SCInputRef InSignalFile = sc.Input[4];

    if (sc.SetDefaults)
    {
        sc.GraphName = "Vol Detector";
        sc.StudyDescription =
            "Apollo-style vol detection. GREEN dot below = vol sellers "
            "(aggressive selling, VIX down, bullish SPX). RED dot above = "
            "vol buyers (aggressive buying, VIX up, bearish SPX). "
            "Based on bar net delta vs rolling average. "
            "Writes signal file for VPS bridge pickup.";

        sc.AutoLoop = 1;
        sc.GraphRegion = 0;
        sc.MaintainAdditionalChartDataArrays = 1;

        VolSell.Name = "Vol Sellers (green below)";
        VolSell.DrawStyle = DRAWSTYLE_DIAMOND;
        VolSell.PrimaryColor = RGB(0, 255, 0);    // Green
        VolSell.LineWidth = 5;
        VolSell.DrawZeros = 0;

        VolBuy.Name = "Vol Buyers (red above)";
        VolBuy.DrawStyle = DRAWSTYLE_DIAMOND;
        VolBuy.PrimaryColor = RGB(255, 0, 0);     // Red
        VolBuy.LineWidth = 5;
        VolBuy.DrawZeros = 0;

        InDeltaMult.Name = "Delta Multiplier (x avg)";
        InDeltaMult.SetFloat(2.0f);
        InDeltaMult.SetFloatLimits(1.0f, 10.0f);

        InDotSize.Name = "Dot Size";
        InDotSize.SetInt(5);
        InDotSize.SetIntLimits(1, 20);

        InDotOffset.Name = "Dot Offset (ticks from H/L)";
        InDotOffset.SetInt(6);
        InDotOffset.SetIntLimits(1, 50);

        InLookback.Name = "Avg Delta Lookback (bars)";
        InLookback.SetInt(20);
        InLookback.SetIntLimits(5, 100);

        InSignalFile.Name = "Signal File Path";
        InSignalFile.SetPathAndFileName("C:\\SierraChart\\Data\\vol_signal.txt");

        return;
    }

    VolSell.LineWidth = InDotSize.GetInt();
    VolBuy.LineWidth  = InDotSize.GetInt();

    int idx = sc.Index;
    int lookback = InLookback.GetInt();
    float offset = InDotOffset.GetInt() * sc.TickSize;

    // Persistent: last bar index we wrote a signal for (avoid duplicates)
    int& LastSignalIdx = sc.GetPersistentInt(1);

    // Clear
    VolSell[idx] = 0;
    VolBuy[idx]  = 0;

    if (idx < lookback)
        return;

    // Only process completed bars (not the forming bar)
    if (idx == sc.ArraySize - 1)
        return;

    // Bar data
    float high   = sc.BaseDataIn[SC_HIGH][idx];
    float low    = sc.BaseDataIn[SC_LOW][idx];
    float close  = sc.BaseDataIn[SC_LAST][idx];
    float askVol = sc.BaseDataIn[SC_ASKVOL][idx];
    float bidVol = sc.BaseDataIn[SC_BIDVOL][idx];
    float delta  = askVol - bidVol;

    // 20-bar average |delta|
    float sumAbsDelta = 0;
    for (int i = idx - lookback; i < idx; i++)
    {
        float d = sc.BaseDataIn[SC_ASKVOL][i] - sc.BaseDataIn[SC_BIDVOL][i];
        sumAbsDelta += (d < 0) ? -d : d;
    }
    float avgAbsDelta = sumAbsDelta / (float)lookback;

    if (avgAbsDelta <= 0)
        return;

    float threshold = avgAbsDelta * InDeltaMult.GetFloat();
    float absDelta  = (delta < 0) ? -delta : delta;
    float ratio     = absDelta / avgAbsDelta;

    // Only one dot per bar — whichever direction the delta is
    if (absDelta >= threshold)
    {
        int direction = 0;

        if (delta < 0)
        {
            // Net sellers aggressive → vol sellers → GREEN below bar
            VolSell[idx] = low - offset;
            direction = -1;  // vol sellers = bullish SPX
        }
        else
        {
            // Net buyers aggressive → vol buyers → RED above bar
            VolBuy[idx] = high + offset;
            direction = 1;   // vol buyers = bearish SPX
        }

        // Write signal file for bridge (only once per bar)
        if (direction != 0 && idx > LastSignalIdx)
        {
            LastSignalIdx = idx;

            SCString SignalPath = InSignalFile.GetPathAndFileName();
            if (SignalPath.GetLength() > 0)
            {
                // Get bar datetime
                SCDateTime BarDT = sc.BaseDateTimeIn[idx];
                int Year, Month, Day, Hour, Minute, Second;
                BarDT.GetDateTimeYMDHMS(Year, Month, Day, Hour, Minute, Second);

                char buf[512];
                snprintf(buf, sizeof(buf),
                    "%d,%.2f,%.0f,%.0f,%.0f,%.1f,%.2f,%04d-%02d-%02dT%02d:%02d:%02d",
                    direction,    // -1 = vol sellers (bullish), +1 = vol buyers (bearish)
                    close,        // VX price at bar close
                    delta,        // net delta
                    askVol,       // total ask volume
                    bidVol,       // total bid volume
                    avgAbsDelta,  // 20-bar avg |delta|
                    ratio,        // how many x above avg
                    Year, Month, Day, Hour, Minute, Second);

                int hFile = 0;
                if (sc.OpenFile(SignalPath, true, hFile) == 0)
                {
                    unsigned int written = 0;
                    int len = (int)strlen(buf);
                    sc.WriteFile(hFile, buf, len, &written);
                    sc.CloseFile(hFile);
                }
            }
        }
    }
}


// =============================================================================
// Study 2: Vol Detector Panel — Delta & CVD (optional)
// =============================================================================
SCSFExport scsf_VolDetectorPanel(SCStudyInterfaceRef sc)
{
    SCSubgraphRef DeltaBar = sc.Subgraph[0];
    SCSubgraphRef CVDLine  = sc.Subgraph[1];
    SCSubgraphRef ZeroLine = sc.Subgraph[2];

    SCInputRef InShowCVD = sc.Input[0];

    if (sc.SetDefaults)
    {
        sc.GraphName = "Vol Detector Panel";
        sc.StudyDescription =
            "Delta per bar (green/red) + CVD line (gold). "
            "Optional companion to Vol Detector.";

        sc.AutoLoop = 1;
        sc.GraphRegion = 1;
        sc.MaintainAdditionalChartDataArrays = 1;

        DeltaBar.Name = "Delta per Bar";
        DeltaBar.DrawStyle = DRAWSTYLE_BAR;
        DeltaBar.PrimaryColor = RGB(0, 200, 83);
        DeltaBar.SecondaryColor = RGB(255, 68, 68);
        DeltaBar.SecondaryColorUsed = 1;
        DeltaBar.AutoColoring = AUTOCOLOR_POSNEG;
        DeltaBar.LineWidth = 3;
        DeltaBar.DrawZeros = 1;

        CVDLine.Name = "Cumulative Delta (CVD)";
        CVDLine.DrawStyle = DRAWSTYLE_LINE;
        CVDLine.PrimaryColor = RGB(255, 215, 0);
        CVDLine.LineWidth = 2;
        CVDLine.DrawZeros = 0;

        ZeroLine.Name = "Zero Line";
        ZeroLine.DrawStyle = DRAWSTYLE_LINE;
        ZeroLine.PrimaryColor = RGB(80, 80, 80);
        ZeroLine.LineWidth = 1;
        ZeroLine.DrawZeros = 1;

        InShowCVD.Name = "Show CVD Line";
        InShowCVD.SetYesNo(1);

        return;
    }

    int idx = sc.Index;

    float askVol = sc.BaseDataIn[SC_ASKVOL][idx];
    float bidVol = sc.BaseDataIn[SC_BIDVOL][idx];
    float delta  = askVol - bidVol;

    DeltaBar[idx] = delta;
    ZeroLine[idx] = 0.0f;

    if (InShowCVD.GetBoolean())
    {
        if (idx == 0)
            CVDLine[idx] = delta;
        else
            CVDLine[idx] = CVDLine[idx - 1] + delta;
    }
    else
    {
        CVDLine[idx] = 0.0f;
    }
}
