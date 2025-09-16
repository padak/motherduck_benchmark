#!/usr/bin/env python3
"""Test script to see DuckDB EXPLAIN output format."""

import duckdb
import os
from pathlib import Path

# Load token
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
if token:
    config = {"motherduck_token": token}
    con = duckdb.connect("md:contoso_benchmark", config=config)
else:
    con = duckdb.connect()

# Simple test query
test_query = "SELECT 1 as test, 'hello' as message"

print("Testing EXPLAIN output:")
print("-" * 40)

# Try EXPLAIN
result = con.execute(f"EXPLAIN {test_query}")
print("EXPLAIN columns:", result.description)
print("EXPLAIN result:")
for row in result.fetchall():
    print(f"  Type: {type(row)}, Value: {row}")

print("\n" + "-" * 40)
print("\nTesting EXPLAIN ANALYZE output:")
print("-" * 40)

# Try EXPLAIN ANALYZE
result = con.execute(f"EXPLAIN ANALYZE {test_query}")
print("EXPLAIN ANALYZE columns:", result.description)
print("EXPLAIN ANALYZE result:")
for row in result.fetchall():
    print(f"  Type: {type(row)}, Value: {row}")