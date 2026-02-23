"""
Rithmic Conformance Test Script
Connect to Rithmic Test order plant and stay logged in for Shyam to inspect.

Usage:
  python _conformance_rithmic.py

You will be prompted for credentials if not set as env vars.
Press Ctrl+C to disconnect.
"""

import os
import asyncio
from async_rithmic import RithmicClient

RITHMIC_TEST_URL = "rituz00100.rithmic.com:443"
SYSTEM_NAME = "Rithmic Test"
APP_NAME = "faal:0dte_alpha"
APP_VERSION = "1.0"


async def main():
    user = os.getenv("RITHMIC_USER", "").strip()
    password = os.getenv("RITHMIC_PASSWORD", "").strip()

    if not user:
        user = input("Rithmic username: ").strip()
    if not password:
        password = input("Rithmic password: ").strip()

    print(f"Connecting to {SYSTEM_NAME} at {RITHMIC_TEST_URL}")
    print(f"  app_name: {APP_NAME}")
    print(f"  user: {user}")

    client = RithmicClient(
        user=user,
        password=password,
        system_name=SYSTEM_NAME,
        app_name=APP_NAME,
        app_version=APP_VERSION,
        url=RITHMIC_TEST_URL,
    )

    await client.connect()
    print("\n--- CONNECTED ---")
    print("Leave this running and tell Shyam the app is logged in.")
    print("Press Ctrl+C to disconnect.\n")

    # List accounts to confirm order plant access
    try:
        accounts = await client.list_accounts()
        print(f"Accounts found: {accounts}")
    except Exception as e:
        print(f"list_accounts: {e} (may be normal for test env)")

    # Stay alive
    try:
        while True:
            await asyncio.sleep(30)
            print(".", end="", flush=True)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nDisconnecting...")

    await client.disconnect()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
