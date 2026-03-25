// =============================================================================
// ApolloMomentum.cpp — Sierra Chart ACSIL Study
// =============================================================================
//
// Apollo's Momentum Signal: 15-min rate-of-change overlay for SPX/ES.
// Shows spot momentum direction + magnitude. Trader manually confirms IV
// alignment on Volland (or uses the companion Python IV bridge for arrows).
//
// Subgraphs:
//   0 (RateOfChange)   — 15-min price change in points (line plot)
//   1 (MomentumLong)   — Up arrow when momentum >= +threshold
//   2 (MomentumShort)  — Down arrow when momentum <= -threshold
//   3 (ZeroLine)       — Zero reference line
//   4 (IVSignalLong)   — Up arrow from external IV bridge file (optional)
//   5 (IVSignalShort)  — Down arrow from external IV bridge file (optional)
//
// Inputs:
//   LookbackMinutes    — Lookback window in minutes (default 15)
//   SpotMoveThreshold  — Minimum move in pts to trigger signal (default 5.0)
//   ShowROCLine        — Toggle rate-of-change line (default Yes)
//   IVBridgeFilePath   — Path to Python IV bridge CSV (optional)
//   IVChangeThreshold  — Minimum IV change to confirm signal (default 0.05)
//
// Build:
//   1. Copy this file to Sierra Chart's ACS_Source folder
//   2. Analysis > Build Custom Studies DLL
//   3. Add study "Apollo Momentum" to chart
//
// Compatible with Sierra Chart v2500+, ACSIL interface version 4.
// =============================================================================

#include "SierraChart.h"
#include <cstring>   // strchr, memset
#include <cstdlib>   // atof

SCDLLName("ApolloMomentum")

// =============================================================================
// Study function
// =============================================================================
SCSFExport scsf_ApolloMomentum(SCStudyInterfaceRef sc)
{
    // ─── Subgraph references ────────────────────────────────────────────────
    SCSubgraphRef ROC           = sc.Subgraph[0];
    SCSubgraphRef MomentumLong  = sc.Subgraph[1];
    SCSubgraphRef MomentumShort = sc.Subgraph[2];
    SCSubgraphRef ZeroLine      = sc.Subgraph[3];
    SCSubgraphRef IVLong        = sc.Subgraph[4];
    SCSubgraphRef IVShort       = sc.Subgraph[5];

    // ─── Input references ───────────────────────────────────────────────────
    SCInputRef LookbackMinutes    = sc.Input[0];
    SCInputRef SpotMoveThreshold  = sc.Input[1];
    SCInputRef ShowROCLine        = sc.Input[2];
    SCInputRef IVBridgeFilePath   = sc.Input[3];
    SCInputRef IVChangeThreshold  = sc.Input[4];

    // ─── Defaults (called once on first apply) ──────────────────────────────
    if (sc.SetDefaults)
    {
        sc.GraphName = "Apollo Momentum";
        sc.StudyDescription =
            "15-min rate-of-change momentum signal for Apollo strategy. "
            "Shows spot momentum magnitude and direction. Optionally reads "
            "IV confirmation signals from external Python bridge file.";

        sc.AutoLoop = 1;
        sc.GraphRegion = 1;  // Separate region below price chart
        sc.FreeDivisor = 10;

        // Subgraph 0: Rate of Change line
        ROC.Name = "Rate of Change (pts)";
        ROC.DrawStyle = DRAWSTYLE_LINE;
        ROC.PrimaryColor = RGB(100, 149, 237);  // Cornflower blue
        ROC.LineWidth = 2;
        ROC.DrawZeros = 1;

        // Subgraph 1: Momentum Long arrows
        MomentumLong.Name = "Momentum LONG";
        MomentumLong.DrawStyle = DRAWSTYLE_ARROWUP;
        MomentumLong.PrimaryColor = RGB(0, 200, 83);   // Green
        MomentumLong.LineWidth = 3;
        MomentumLong.DrawZeros = 0;

        // Subgraph 2: Momentum Short arrows
        MomentumShort.Name = "Momentum SHORT";
        MomentumShort.DrawStyle = DRAWSTYLE_ARROWDOWN;
        MomentumShort.PrimaryColor = RGB(255, 68, 68);  // Red
        MomentumShort.LineWidth = 3;
        MomentumShort.DrawZeros = 0;

        // Subgraph 3: Zero line
        ZeroLine.Name = "Zero";
        ZeroLine.DrawStyle = DRAWSTYLE_LINE;
        ZeroLine.PrimaryColor = RGB(128, 128, 128);  // Gray
        ZeroLine.LineWidth = 1;
        ZeroLine.DrawZeros = 1;

        // Subgraph 4: IV-confirmed Long (from bridge file)
        IVLong.Name = "IV Confirmed LONG";
        IVLong.DrawStyle = DRAWSTYLE_ARROWUP;
        IVLong.PrimaryColor = RGB(0, 255, 127);    // Spring green (brighter)
        IVLong.LineWidth = 4;
        IVLong.DrawZeros = 0;

        // Subgraph 5: IV-confirmed Short (from bridge file)
        IVShort.Name = "IV Confirmed SHORT";
        IVShort.DrawStyle = DRAWSTYLE_ARROWDOWN;
        IVShort.PrimaryColor = RGB(255, 0, 100);    // Hot pink (brighter)
        IVShort.LineWidth = 4;
        IVShort.DrawZeros = 0;

        // Input 0: Lookback window
        LookbackMinutes.Name = "Lookback Minutes";
        LookbackMinutes.SetInt(15);
        LookbackMinutes.SetIntLimits(1, 120);

        // Input 1: Minimum spot move to flag as signal
        SpotMoveThreshold.Name = "Spot Move Threshold (pts)";
        SpotMoveThreshold.SetFloat(5.0f);
        SpotMoveThreshold.SetFloatLimits(0.5f, 50.0f);

        // Input 2: Show the ROC line
        ShowROCLine.Name = "Show ROC Line";
        ShowROCLine.SetYesNo(1);

        // Input 3: Path to IV bridge CSV file (optional)
        IVBridgeFilePath.Name = "IV Bridge File Path";
        IVBridgeFilePath.SetPathAndFileName("");

        // Input 4: IV change threshold
        IVChangeThreshold.Name = "IV Change Threshold";
        IVChangeThreshold.SetFloat(0.05f);
        IVChangeThreshold.SetFloatLimits(0.01f, 0.50f);

        return;
    }

    // ─── Main calculation loop (AutoLoop = 1, called per bar) ───────────────

    int CurrentIndex = sc.Index;

    // Calculate how many bars = LookbackMinutes
    // SecondsPerBar can be 0 for tick charts, handle gracefully
    int SecondsPerBar = sc.SecondsPerBar;
    int LookbackBars;

    if (SecondsPerBar > 0)
    {
        LookbackBars = (LookbackMinutes.GetInt() * 60) / SecondsPerBar;
    }
    else
    {
        // For tick/range/volume charts, use a fixed lookback of bars
        // User should adjust LookbackMinutes to approximate desired bar count
        LookbackBars = LookbackMinutes.GetInt();
    }

    if (LookbackBars < 1)
        LookbackBars = 1;

    // Zero line always drawn
    ZeroLine[CurrentIndex] = 0.0f;

    // Need enough history
    if (CurrentIndex < LookbackBars)
    {
        ROC[CurrentIndex] = 0.0f;
        MomentumLong[CurrentIndex] = 0.0f;
        MomentumShort[CurrentIndex] = 0.0f;
        return;
    }

    // ─── Rate of Change calculation ─────────────────────────────────────────
    // Current close minus close N bars ago
    float CurrentClose = sc.BaseDataIn[SC_LAST][CurrentIndex];
    float PriorClose   = sc.BaseDataIn[SC_LAST][CurrentIndex - LookbackBars];
    float RateOfChange = CurrentClose - PriorClose;

    // Set ROC subgraph
    if (ShowROCLine.GetBoolean())
    {
        ROC[CurrentIndex] = RateOfChange;

        // Color the ROC line: green when positive, red when negative
        if (RateOfChange > 0)
            ROC.DataColor[CurrentIndex] = RGB(0, 200, 83);
        else if (RateOfChange < 0)
            ROC.DataColor[CurrentIndex] = RGB(255, 68, 68);
        else
            ROC.DataColor[CurrentIndex] = RGB(128, 128, 128);
    }
    else
    {
        ROC[CurrentIndex] = 0.0f;
    }

    // ─── Signal detection ───────────────────────────────────────────────────
    float Threshold = SpotMoveThreshold.GetFloat();

    // Reset signal subgraphs
    MomentumLong[CurrentIndex] = 0.0f;
    MomentumShort[CurrentIndex] = 0.0f;

    // Check for momentum threshold breach
    // Arrows plotted at the ROC value for visibility in the study region
    if (RateOfChange >= Threshold)
    {
        MomentumLong[CurrentIndex] = RateOfChange;
    }
    else if (RateOfChange <= -Threshold)
    {
        MomentumShort[CurrentIndex] = RateOfChange;
    }

    // ─── Color main price bars based on momentum (optional) ─────────────────
    // This colors the price chart bars when momentum is active.
    // Uses sc.Subgraph[0].DataColor to tint — but since we are in region 1,
    // we use the bar coloring approach via extra arrays.
    //
    // For main chart bar coloring, the user should add a second instance of
    // a simpler "bar color" study, or use Sierra's built-in bar color tools.
    // ACSIL studies in region 1 cannot directly color region 0 bars.

    // ─── IV Bridge file reading (Approach #5 integration) ───────────────────
    // If IVBridgeFilePath is set, read signals from the Python-generated CSV.
    // File format: timestamp_unix,direction,iv_change,spot_momentum,spot_price
    //
    // We check the file periodically (not every bar) and overlay IV-confirmed
    // signals as brighter arrows.

    IVLong[CurrentIndex] = 0.0f;
    IVShort[CurrentIndex] = 0.0f;

    SCString BridgePath = IVBridgeFilePath.GetPathAndFileName();
    if (BridgePath.GetLength() > 0)
    {
        // Read the bridge file using Sierra's file I/O
        // We store the last read time in persistent int to avoid re-reading every bar
        int& LastReadBarIndex = sc.GetPersistentInt(1);
        float& LastIVDirection = sc.GetPersistentFloat(1);  // +1 = long, -1 = short, 0 = none
        float& LastIVSpotPrice = sc.GetPersistentFloat(2);
        float& LastIVChange = sc.GetPersistentFloat(3);

        // Re-read file every 10 bars to avoid excessive I/O
        if (CurrentIndex - LastReadBarIndex >= 10 || LastReadBarIndex == 0)
        {
            LastReadBarIndex = CurrentIndex;

            // Open and read the last line of the bridge file
            // Sierra ACSIL provides sc.OpenFile / sc.ReadFile — but the simplest
            // cross-platform approach is to use the Spreadsheet study or
            // sc.GetMainGraphFilePath for relative paths.
            //
            // For maximum compatibility, we use a text file with a single line
            // that gets overwritten by the Python bridge each cycle.

            HANDLE FileHandle;
            unsigned int BytesRead = 0;
            char FileBuffer[512];
            memset(FileBuffer, 0, sizeof(FileBuffer));

            if (sc.OpenFile(BridgePath, false, FileHandle) == 0)
            {
                // File opened successfully — read up to 511 bytes
                sc.ReadFile(FileHandle, FileBuffer, 511, &BytesRead);
                sc.CloseFile(FileHandle);

                if (BytesRead > 0)
                {
                    // Parse: direction,iv_change,spot_price
                    // Example: "1,0.07,5890.50" or "-1,0.09,5885.25"
                    // Use standard C string parsing for maximum ACSIL compatibility
                    char* p1 = strchr(FileBuffer, ',');
                    if (p1 != NULL)
                    {
                        char* p2 = strchr(p1 + 1, ',');
                        if (p2 != NULL)
                        {
                            // Null-terminate each field in-place
                            *p1 = '\0';
                            *p2 = '\0';

                            LastIVDirection = (float)atof(FileBuffer);   // "1", "-1", or "0"
                            LastIVChange    = (float)atof(p1 + 1);       // "0.0700"
                            LastIVSpotPrice = (float)atof(p2 + 1);       // "5890.50"
                        }
                    }
                }
            }
        }

        // Overlay IV-confirmed signals on the most recent bars
        // Only show if the IV signal spot price is within 2 points of current close
        // (ensures we are matching the right time window)
        if (LastIVDirection != 0.0f)
        {
            float SpotDiff = CurrentClose - LastIVSpotPrice;
            if (SpotDiff < 0) SpotDiff = -SpotDiff;

            if (SpotDiff <= 5.0f)  // Within 5 pts of the IV signal's spot
            {
                if (LastIVDirection > 0 && RateOfChange >= Threshold)
                {
                    // Both momentum AND IV confirm LONG
                    IVLong[CurrentIndex] = RateOfChange + 1.0f;  // Offset above momentum arrow
                }
                else if (LastIVDirection < 0 && RateOfChange <= -Threshold)
                {
                    // Both momentum AND IV confirm SHORT
                    IVShort[CurrentIndex] = RateOfChange - 1.0f;  // Offset below momentum arrow
                }
            }
        }
    }
}

// =============================================================================
// ApolloMomentumBarColor — Companion study for main price chart bar coloring
// =============================================================================
// This study goes in Region 0 (main price chart) and colors bars based on
// the 15-min rate of change. Green = bullish momentum, Red = bearish.
// Transparent/default when momentum is below threshold.
// =============================================================================

SCSFExport scsf_ApolloMomentumBarColor(SCStudyInterfaceRef sc)
{
    SCSubgraphRef BarColor = sc.Subgraph[0];

    SCInputRef LookbackMinutes   = sc.Input[0];
    SCInputRef SpotMoveThreshold = sc.Input[1];

    if (sc.SetDefaults)
    {
        sc.GraphName = "Apollo Momentum Bar Color";
        sc.StudyDescription =
            "Colors main chart bars green/red when 15-min momentum "
            "exceeds threshold. Companion to Apollo Momentum study.";

        sc.AutoLoop = 1;
        sc.GraphRegion = 0;  // Main price chart

        BarColor.Name = "Bar Color";
        BarColor.DrawStyle = DRAWSTYLE_COLOR_BAR;
        BarColor.PrimaryColor = RGB(0, 200, 83);    // Bullish
        BarColor.SecondaryColor = RGB(255, 68, 68);  // Bearish
        BarColor.SecondaryColorUsed = 1;
        BarColor.DrawZeros = 0;

        LookbackMinutes.Name = "Lookback Minutes";
        LookbackMinutes.SetInt(15);
        LookbackMinutes.SetIntLimits(1, 120);

        SpotMoveThreshold.Name = "Spot Move Threshold (pts)";
        SpotMoveThreshold.SetFloat(5.0f);
        SpotMoveThreshold.SetFloatLimits(0.5f, 50.0f);

        return;
    }

    int CurrentIndex = sc.Index;
    int SecondsPerBar = sc.SecondsPerBar;
    int LookbackBars;

    if (SecondsPerBar > 0)
        LookbackBars = (LookbackMinutes.GetInt() * 60) / SecondsPerBar;
    else
        LookbackBars = LookbackMinutes.GetInt();

    if (LookbackBars < 1) LookbackBars = 1;

    BarColor[CurrentIndex] = 0.0f;

    if (CurrentIndex < LookbackBars)
        return;

    float CurrentClose = sc.BaseDataIn[SC_LAST][CurrentIndex];
    float PriorClose   = sc.BaseDataIn[SC_LAST][CurrentIndex - LookbackBars];
    float RateOfChange  = CurrentClose - PriorClose;
    float Threshold     = SpotMoveThreshold.GetFloat();

    if (RateOfChange >= Threshold)
    {
        // Bullish momentum — color bar green
        BarColor[CurrentIndex] = 1.0f;
        BarColor.DataColor[CurrentIndex] = BarColor.PrimaryColor;
    }
    else if (RateOfChange <= -Threshold)
    {
        // Bearish momentum — color bar red
        BarColor[CurrentIndex] = 1.0f;
        BarColor.DataColor[CurrentIndex] = BarColor.SecondaryColor;
    }
}
