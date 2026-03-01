// ═══════════════════════════════════════════════════════════════════════
//  PositionReporter — NinjaTrader 8 Strategy
//  Writes account position & order state to position_state.json
//  for external automation (eval_trader.py).
//
//  SETUP:
//    1. Copy this file to: Documents\NinjaTrader 8\bin\Custom\Strategies\
//       Rename to: PositionReporter.cs
//    2. In NT8: open NinjaScript Editor and press F5 to compile
//    3. Open a 1-minute MES chart → right-click → Strategies → PositionReporter
//    4. Select the E2T account (e.g. falde5482-sim) → Enable
//
//  OUTPUT FILE:
//    Documents\NinjaTrader 8\position_state.json
//    Updated every 2 seconds via tick data.
//
//  IMPORTANT: This strategy does NOT place any orders. It is read-only.
// ═══════════════════════════════════════════════════════════════════════

#region Using declarations
using System;
using System.IO;
using System.Text;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
    public class PositionReporter : Strategy
    {
        private string filePath;
        private DateTime lastWrite;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description = "Writes account position and order state to position_state.json for eval_trader.";
                Name = "PositionReporter";
                Calculate = Calculate.OnEachTick;
                IsOverlay = true;
                IsExitOnSessionCloseStrategy = false;
            }
            else if (State == State.Configure)
            {
                filePath = Path.Combine(
                    NinjaTrader.Core.Globals.UserDataDir, "position_state.json");
                lastWrite = DateTime.MinValue;
            }
            else if (State == State.Terminated)
            {
                try
                {
                    if (filePath != null)
                        File.WriteAllText(filePath,
                            "{\"status\":\"offline\",\"timestamp\":\"" +
                            DateTime.Now.ToString("o") + "\"}");
                }
                catch { }
            }
        }

        protected override void OnBarUpdate()
        {
            if (State != State.Realtime)
                return;
            if ((DateTime.Now - lastWrite).TotalSeconds < 2)
                return;
            lastWrite = DateTime.Now;

            try
            {
                // ── Read account-level position ──
                string mp = "Flat";
                int qty = 0;
                double avgPrice = 0;

                if (Account != null && Account.Positions != null)
                {
                    lock (Account.Positions)
                    {
                        foreach (Position pos in Account.Positions)
                        {
                            if (pos != null
                                && pos.Instrument != null
                                && pos.Instrument.FullName == Instrument.FullName
                                && pos.MarketPosition != MarketPosition.Flat)
                            {
                                mp = pos.MarketPosition.ToString();
                                qty = pos.Quantity;
                                avgPrice = pos.AveragePrice;
                                break;
                            }
                        }
                    }
                }

                // ── Read active orders ──
                StringBuilder orders = new StringBuilder("[");
                bool first = true;

                if (Account != null && Account.Orders != null)
                {
                    lock (Account.Orders)
                    {
                        foreach (Order order in Account.Orders)
                        {
                            if (order != null
                                && order.Instrument != null
                                && order.Instrument.FullName == Instrument.FullName
                                && (order.OrderState == OrderState.Working
                                    || order.OrderState == OrderState.Accepted))
                            {
                                if (!first) orders.Append(",");
                                first = false;
                                orders.Append("{");
                                orders.Append("\"action\":\"" + order.OrderAction + "\",");
                                orders.Append("\"type\":\"" + order.OrderType + "\",");
                                orders.Append("\"qty\":" + order.Quantity + ",");
                                orders.Append("\"limit\":" + order.LimitPrice.ToString("F2") + ",");
                                orders.Append("\"stop\":" + order.StopPrice.ToString("F2"));
                                orders.Append("}");
                            }
                        }
                    }
                }
                orders.Append("]");

                // ── Write JSON ──
                string json =
                    "{" +
                    "\"status\":\"online\"," +
                    "\"account\":\"" + Account.Name + "\"," +
                    "\"instrument\":\"" + Instrument.FullName + "\"," +
                    "\"position\":\"" + mp + "\"," +
                    "\"quantity\":" + qty + "," +
                    "\"avg_price\":" + avgPrice.ToString("F2") + "," +
                    "\"orders\":" + orders.ToString() + "," +
                    "\"timestamp\":\"" + DateTime.Now.ToString("o") + "\"" +
                    "}";

                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                Print("PositionReporter error: " + ex.Message);
            }
        }
    }
}
