"""Quick test: can we get VIX futures (VX) from Rithmic on CFE?"""
import asyncio
import os
import time

async def main():
    from async_rithmic import RithmicClient, DataType, LastTradePresenceBits, BestBidOfferPresenceBits

    user = os.getenv("RITHMIC_USER", "faisal.a.d@msn.com")
    password = os.getenv("RITHMIC_PASSWORD", "7fPwgvH2$@uT2H5")
    system = os.getenv("RITHMIC_SYSTEM_NAME", "Rithmic Paper Trading")
    url = os.getenv("RITHMIC_URL", "wss://rprotocol.rithmic.com:443")

    print(f"Connecting to Rithmic ({system})...")
    client = RithmicClient(
        user=user,
        password=password,
        system_name=system,
        app_name="faal:vx_test",
        app_version="1.0",
        url=url,
    )

    await client.connect()
    print("Connected!")

    # Step 1: Try to resolve VX front month on CFE
    print("\n--- Resolving VX front month on CFE ---")
    try:
        vx_front = await client.get_front_month_contract("VX", "CFE")
        print(f"VX front month: {vx_front}")
    except Exception as e:
        print(f"ERROR resolving VX on CFE: {e}")
        # Try alternate exchange names
        for exchange in ["CBOE", "CBOT", "CME"]:
            try:
                vx_alt = await client.get_front_month_contract("VX", exchange)
                print(f"VX found on {exchange}: {vx_alt}")
                break
            except Exception as e2:
                print(f"  VX on {exchange}: {e2}")
        await client.disconnect()
        return

    # Also resolve ES for comparison
    try:
        es_front = await client.get_front_month_contract("ES", "CME")
        print(f"ES front month: {es_front}")
    except Exception as e:
        print(f"ES resolve: {e}")

    # Step 2: Subscribe to VX ticks
    print(f"\n--- Subscribing to {vx_front} on CFE ---")
    tick_count = 0
    start_time = time.time()

    latest_bbo = {"bid": None, "ask": None}

    async def on_tick(data):
        nonlocal tick_count
        if data["data_type"] == DataType.BBO:
            pb = data.get("presence_bits", 0)
            if pb & BestBidOfferPresenceBits.BID and "bid_price" in data:
                latest_bbo["bid"] = data["bid_price"]
            if pb & BestBidOfferPresenceBits.ASK and "ask_price" in data:
                latest_bbo["ask"] = data["ask_price"]
            if latest_bbo["bid"] and latest_bbo["ask"]:
                print(f"  [BBO] bid={latest_bbo['bid']:.2f} ask={latest_bbo['ask']:.2f}")

        elif data["data_type"] == DataType.LAST_TRADE:
            pb = data.get("presence_bits", 0)
            if not (pb & LastTradePresenceBits.LAST_TRADE):
                return
            price = data.get("trade_price")
            size = data.get("trade_size")
            aggressor = data.get("aggressor")
            if price and size:
                tick_count += 1
                agg_str = "BUY" if aggressor == 1 else "SELL" if aggressor == 2 else "?"
                print(f"  [TRADE #{tick_count}] price={price:.2f} size={size} agg={agg_str} "
                      f"bid={latest_bbo.get('bid')} ask={latest_bbo.get('ask')}")

    client.on_tick += on_tick

    try:
        await client.subscribe_to_market_data(
            vx_front, "CFE", DataType.LAST_TRADE | DataType.BBO
        )
        print(f"Subscribed! Listening for 60 seconds...")
    except Exception as e:
        print(f"ERROR subscribing to VX: {e}")
        await client.disconnect()
        return

    # Listen for 60 seconds
    for i in range(60):
        await asyncio.sleep(1)
        if i % 10 == 9:
            elapsed = time.time() - start_time
            print(f"  --- {elapsed:.0f}s elapsed, {tick_count} trades received ---")

    # Cleanup
    print(f"\n--- Results ---")
    print(f"Total VX trades received: {tick_count}")
    print(f"Last BBO: bid={latest_bbo.get('bid')} ask={latest_bbo.get('ask')}")

    try:
        await client.unsubscribe_from_market_data(
            vx_front, "CFE", DataType.LAST_TRADE | DataType.BBO
        )
    except:
        pass
    await client.disconnect()
    print("Disconnected.")


if __name__ == "__main__":
    asyncio.run(main())
