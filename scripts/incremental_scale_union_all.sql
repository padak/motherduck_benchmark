-- Incremental Scaling from 9.6B to 24B rows using UNION ALL approach
-- Much more memory-efficient than CROSS JOIN
-- Execute each section separately with 15-30 second pauses between them

-- ============================================================
-- STEP 1: Create a base multiplier table (1.2M rows = 240k × 5)
-- This uses UNION ALL which is memory-efficient
-- ============================================================

CREATE OR REPLACE TABLE main.temp_base_5x AS
SELECT * FROM main.contoso_sales_240k
UNION ALL
SELECT * FROM main.contoso_sales_240k
UNION ALL
SELECT * FROM main.contoso_sales_240k
UNION ALL
SELECT * FROM main.contoso_sales_240k
UNION ALL
SELECT * FROM main.contoso_sales_240k;

-- Verify: should be 1,200,000 rows (240k × 5)
SELECT COUNT(*) as base_5x_count FROM main.temp_base_5x;

-- ============================================================
-- STEP 2: Create a larger multiplier table (12M rows = 1.2M × 10)
-- ============================================================

CREATE OR REPLACE TABLE main.temp_base_50x AS
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x
UNION ALL
SELECT * FROM main.temp_base_5x;

-- Clean up the 5x table
DROP TABLE main.temp_base_5x;

-- Verify: should be 12,000,000 rows (1.2M × 10)
SELECT COUNT(*) as base_50x_count FROM main.temp_base_50x;

-- ============================================================
-- STEP 3: Create an even larger multiplier table (120M rows = 12M × 10)
-- ============================================================

CREATE OR REPLACE TABLE main.temp_base_500x AS
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x
UNION ALL
SELECT * FROM main.temp_base_50x;

-- Clean up the 50x table
DROP TABLE main.temp_base_50x;

-- Verify: should be 120,000,000 rows (12M × 10)
SELECT COUNT(*) as base_500x_count FROM main.temp_base_500x;

-- ============================================================
-- STEP 4: Create the final batch table (1.2B rows = 120M × 10)
-- This will be our insertion unit
-- ============================================================

CREATE OR REPLACE TABLE main.temp_batch_1_2b AS
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x
UNION ALL
SELECT * FROM main.temp_base_500x;

-- Clean up the 500x table
DROP TABLE main.temp_base_500x;

-- Verify: should be 1,200,000,000 rows (120M × 10)
SELECT COUNT(*) as batch_count FROM main.temp_batch_1_2b;

-- Check current count before insertions
SELECT COUNT(*) as current_count FROM main.contoso_sales_24b_scaled;

-- ============================================================
-- NOW INSERT THE 1.2B BATCH 12 TIMES TO ADD 14.4B ROWS TOTAL
-- Wait 15-30 seconds between each insertion
-- ============================================================

-- ============================================================
-- INSERTION 1 of 12: Add 1.2B rows (9.6B → 10.8B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_1 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 2 of 12: Add 1.2B rows (10.8B → 12B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_2 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 3 of 12: Add 1.2B rows (12B → 13.2B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_3 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 4 of 12: Add 1.2B rows (13.2B → 14.4B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_4 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 5 of 12: Add 1.2B rows (14.4B → 15.6B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_5 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 6 of 12: Add 1.2B rows (15.6B → 16.8B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_6 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 7 of 12: Add 1.2B rows (16.8B → 18B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_7 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 8 of 12: Add 1.2B rows (18B → 19.2B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_8 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 9 of 12: Add 1.2B rows (19.2B → 20.4B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_9 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 10 of 12: Add 1.2B rows (20.4B → 21.6B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_10 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 11 of 12: Add 1.2B rows (21.6B → 22.8B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_11 FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS BEFORE NEXT INSERTION

-- ============================================================
-- INSERTION 12 of 12: Add 1.2B rows (22.8B → 24B)
-- ============================================================
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch_1_2b;

SELECT COUNT(*) as count_after_insert_12 FROM main.contoso_sales_24b_scaled;

-- ============================================================
-- CLEANUP AND FINALIZATION
-- ============================================================

-- Drop the temporary batch table
DROP TABLE main.temp_batch_1_2b;

-- Final verification (should be ~24B)
SELECT COUNT(*) as final_count FROM main.contoso_sales_24b_scaled;

-- Update the view to point to scaled table
CREATE OR REPLACE VIEW main.contoso_sales_24b AS
SELECT * FROM main.contoso_sales_24b_scaled;

-- Verify view works
SELECT COUNT(*) as view_count FROM main.contoso_sales_24b;

-- ============================================================
-- SUCCESS! You should now have ~24 billion rows
-- This approach uses UNION ALL which is much more memory-efficient than CROSS JOIN
-- ============================================================