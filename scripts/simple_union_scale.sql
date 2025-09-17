-- Simple UNION ALL approach to scale from 9.6B to 24B
-- Creates chunks using UNION ALL (no CROSS JOIN) and inserts repeatedly
-- More memory-efficient and faster

-- ============================================================
-- OPTION 1: SMALLER CHUNKS (600M per batch, insert 24 times)
-- Use this if you have memory constraints
-- ============================================================

-- Create a 600M row chunk (240k × 2,500)
-- Using recursive UNION ALL approach
CREATE OR REPLACE TABLE main.temp_chunk AS
WITH RECURSIVE multiplier AS (
    -- Start with base table
    SELECT *, 1 as copy_id FROM main.contoso_sales_240k

    UNION ALL

    -- Add copies until we reach 2,500x
    SELECT original.*, m.copy_id + 1
    FROM main.contoso_sales_240k original, multiplier m
    WHERE m.copy_id < 2500
    LIMIT 600000000  -- Safety limit
)
SELECT * FROM multiplier;

-- Alternative: Build 600M using staged UNION ALLs
-- Step 1: 240k × 10 = 2.4M
CREATE OR REPLACE TABLE main.temp_10x AS
SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k
UNION ALL SELECT * FROM main.contoso_sales_240k;

-- Step 2: 2.4M × 10 = 24M
CREATE OR REPLACE TABLE main.temp_100x AS
SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x
UNION ALL SELECT * FROM main.temp_10x;

DROP TABLE main.temp_10x;

-- Step 3: 24M × 25 = 600M (split into smaller UNION ALLs to avoid memory issues)
CREATE OR REPLACE TABLE main.temp_chunk AS
-- First 5x
SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x;

-- Add another 5x
INSERT INTO main.temp_chunk
SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x;

-- Add another 5x
INSERT INTO main.temp_chunk
SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x;

-- Add another 5x
INSERT INTO main.temp_chunk
SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x;

-- Add final 5x
INSERT INTO main.temp_chunk
SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x
UNION ALL SELECT * FROM main.temp_100x;

DROP TABLE main.temp_100x;

-- Verify chunk size (should be 600M)
SELECT COUNT(*) as chunk_size FROM main.temp_chunk;

-- ============================================================
-- Now INSERT this 600M chunk 24 times to add 14.4B rows
-- Current: 9.6B + (600M × 24) = 9.6B + 14.4B = 24B
-- ============================================================

-- Insert 1: 9.6B → 10.2B
INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_chunk;
SELECT COUNT(*) FROM main.contoso_sales_24b_scaled;

-- WAIT 15 SECONDS

-- Insert 2: 10.2B → 10.8B
INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_chunk;
SELECT COUNT(*) FROM main.contoso_sales_24b_scaled;

-- WAIT 15 SECONDS

-- Insert 3: 10.8B → 11.4B
INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_chunk;

-- WAIT 15 SECONDS

-- Insert 4: 11.4B → 12B
INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_chunk;

-- Continue pattern for inserts 5-24...
-- Each adds 600M rows
-- Total: 24 inserts × 600M = 14.4B rows added

-- ============================================================
-- OPTION 2: LARGER CHUNKS (1.2B per batch, insert 12 times)
-- Use this if you have more memory available
-- ============================================================

-- Build 1.2B chunk by doubling the 600M chunk
CREATE OR REPLACE TABLE main.temp_chunk_large AS
SELECT * FROM main.temp_chunk
UNION ALL
SELECT * FROM main.temp_chunk;

-- Then insert 12 times (1.2B × 12 = 14.4B)
-- This requires fewer insertions but uses more memory per operation

-- ============================================================
-- After all insertions are complete:
-- ============================================================

-- Clean up
DROP TABLE main.temp_chunk;
-- or
DROP TABLE main.temp_chunk_large;

-- Update view
CREATE OR REPLACE VIEW main.contoso_sales_24b AS
SELECT * FROM main.contoso_sales_24b_scaled;

-- Final verification
SELECT COUNT(*) as final_count FROM main.contoso_sales_24b_scaled;