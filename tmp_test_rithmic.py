"""Quick Rithmic connection test — all plants, limited reconnect."""
import asyncio
from async_rithmic import RithmicClient, ReconnectionSettings
from datetime import datetime, timezone

async def test():
    client = RithmicClient(
        user="faisal.a.d@msn.com", password="7fPwgvH2$@uT2H5",
        system_name="Rithmic Paper Trading", app_name="faal:0dte_alpha",
        app_version="1.0", url="wss://rprotocol.rithmic.com:443",
        reconnection_settings=ReconnectionSettings(max_retries=2),
    )
    print("Connecting ALL plants (max 2 retries)...", flush=True)
    try:
        await client.connect()  # default = all plants
    except Exception as e:
        print(f"Connect failed: {e}", flush=True)
        return

    print("Connected! Waiting 5s to see if stable...", flush=True)
    await asyncio.sleep(5)
    print("Stable for 5s. Pulling 1 min of ticks...", flush=True)

    try:
        ticks = await client.get_historical_tick_data("ESH6", "CME",
            datetime(2026, 2, 23, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 2, 23, 14, 31, tzinfo=timezone.utc))
        print(f"Got {len(ticks)} ticks!", flush=True)
    except Exception as e:
        print(f"Data pull error: {e}", flush=True)

    try:
        await client.disconnect()
    except:
        pass
    print("Done", flush=True)

asyncio.run(test())
