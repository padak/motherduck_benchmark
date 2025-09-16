#!/usr/bin/env python3
"""Minimal script to test MotherDuck connection."""

import os
import duckdb
from pathlib import Path


def main():
    # Load token from environment or .env file
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip().replace("export ", "")
                value = value.strip().strip('"').strip("'")
                if key:
                    os.environ.setdefault(key, value)

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        print("ERROR: MotherDuck token not found. Set MOTHERDUCK_TOKEN in environment or .env file.")
        return

    print(f"Token found: {token[:10]}...")

    # Try to connect with FORCE INSTALL
    try:
        print("\nAttempting connection with FORCE INSTALL motherduck...")
        con = duckdb.connect()
        con.execute("FORCE INSTALL motherduck;")
        con.close()

        # Now connect to MotherDuck
        print("Connecting to MotherDuck...")
        config = {"motherduck_token": token}
        con = duckdb.connect("md:sample_data", config=config)

        print("SUCCESS: Connected to MotherDuck!")

        # Test basic operations
        result = con.execute("SELECT 'Hello from MotherDuck!' as message").fetchone()
        print(f"Query result: {result[0]}")

        # Show version info
        version = con.execute("SELECT version()").fetchone()
        print(f"DuckDB version: {version[0]}")

        con.close()

    except duckdb.Error as e:
        print(f"ERROR: Failed to connect to MotherDuck: {e}")
        print("\nTrying alternative approach without FORCE INSTALL...")

        try:
            config = {"motherduck_token": token}
            con = duckdb.connect("md:", config=config)
            print("SUCCESS: Connected to MotherDuck without FORCE INSTALL!")
            con.close()
        except duckdb.Error as e2:
            print(f"ERROR: Alternative approach also failed: {e2}")


if __name__ == "__main__":
    main()