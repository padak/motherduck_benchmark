# Utility Scripts

This directory contains utility scripts for testing, debugging, and additional operations related to the MotherDuck benchmark.

## Scripts Overview

### test_motherduck_connection.py

**Purpose**: Test and validate MotherDuck connection with various approaches

**Features**:
- Loads MotherDuck token from environment or .env file
- Attempts connection with `FORCE INSTALL motherduck` extension
- Tests basic query execution
- Shows DuckDB version information
- Falls back to alternative connection methods if primary fails

**Usage**:
```bash
python scripts/test_motherduck_connection.py
```

**When to use**:
- Troubleshooting connection issues
- Verifying MotherDuck extension installation
- Testing token authentication
- Debugging DuckDB version compatibility

### scale_further.py

**Purpose**: Scale an already-scaled table by a multiplication factor

**Features**:
- Works with existing scaled tables (not starting from scratch)
- Connects directly to MotherDuck database
- Shows current and target row counts
- Provides warnings for very large operations (>10B rows)
- Replaces the original table atomically
- Displays progress and timing information

**Usage**:
```bash
# Scale current table by 10x
python scripts/scale_further.py 10

# Scale by 100x (be careful with large tables!)
python scripts/scale_further.py 100
```

**Example workflow**:
```bash
# Starting with 240M rows, scale to 2.4B
python scripts/scale_further.py 10

# Then scale from 2.4B to 24B
python scripts/scale_further.py 10
```

**When to use**:
- Need to incrementally scale data beyond initial scaling
- Want to avoid re-scaling from the original 240K table
- Testing performance at various scale factors

### test_explain.py

**Purpose**: Debug and understand DuckDB's EXPLAIN output format

**Features**:
- Tests both `EXPLAIN` and `EXPLAIN ANALYZE` output
- Shows the structure of explain results (key-value tuples)
- Displays the ASCII tree visualization of query plans
- Helps understand how to parse and display explain output

**Usage**:
```bash
python scripts/test_explain.py
```

**Output includes**:
- Column structure of EXPLAIN results
- Physical query plan visualization
- Query profiling information (with EXPLAIN ANALYZE)
- Execution timing details

**When to use**:
- Debugging query plan display issues
- Understanding DuckDB's explain output format
- Testing query optimization strategies
- Verifying the explain functionality in the main application

## Common Patterns

All scripts follow these patterns:

1. **Environment handling**: Load .env file for configuration
```python
env_file = Path('.env')
if env_file.exists():
    for line in env_file.read_text().splitlines():
        # Parse and load environment variables
```

2. **Token authentication**: Get MotherDuck token from environment
```python
token = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('motherduck_token')
```

3. **Connection configuration**: Set up DuckDB/MotherDuck connection
```python
config = {'motherduck_token': token}
con = duckdb.connect('md:database_name', config=config)
```

## Adding New Scripts

When adding new utility scripts to this directory:

1. Follow the existing naming convention (snake_case.py)
2. Include a docstring at the top explaining the purpose
3. Handle environment and token loading consistently
4. Provide clear error messages and usage instructions
5. Update this README with the new script's documentation
6. Make the script executable: `chmod +x scripts/your_script.py`

## Dependencies

All scripts require:
- Python 3.8+
- duckdb package (version <1.4.0)
- Valid MotherDuck token in .env or environment
- Network connection to MotherDuck service

## Error Handling

Common issues and solutions:

**Token not found**:
```
Error: MOTHERDUCK_TOKEN not found in environment or .env file
```
Solution: Copy `.env.dist` to `.env` and add your token

**DuckDB version incompatibility**:
```
Your DuckDB version (v1.4.0) is not yet supported by MotherDuck
```
Solution: Downgrade to DuckDB <1.4.0: `pip install 'duckdb<1.4.0'`

**Connection failures**:
- Check network connectivity
- Verify token is valid and not expired
- Ensure MotherDuck service is accessible