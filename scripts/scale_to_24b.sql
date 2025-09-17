-- SQL script to scale from 19.2B rows to exactly 24B rows
-- This creates 4.8B rows from contoso_sales_240k and combines with existing 19.2B

-- Step 1: Create temporary table with 4.8B rows
-- 4,800,000,000 / 240,000 = 20,000x multiplier needed
CREATE OR REPLACE TABLE main.contoso_sales_4_8b_temp AS
SELECT
    original.*,
    replicate_id
FROM main.contoso_sales_240k AS original
CROSS JOIN (
    SELECT generate_series AS replicate_id
    FROM generate_series(1, 20000)
) AS replicator;

-- Verify the count (should be 4,800,000,000)
SELECT COUNT(*) as row_count FROM main.contoso_sales_4_8b_temp;

-- Step 2: Create the final 24B table by combining 19.2B + 4.8B
CREATE OR REPLACE TABLE main.contoso_sales_24b_final AS
SELECT * FROM main.contoso_sales_24b_scaled
UNION ALL
SELECT * FROM main.contoso_sales_4_8b_temp;

-- Verify the final count (should be 24,000,000,000)
SELECT COUNT(*) as final_count FROM main.contoso_sales_24b_final;

-- Step 3: Clean up and replace
DROP TABLE main.contoso_sales_24b_scaled;
ALTER TABLE main.contoso_sales_24b_final RENAME TO contoso_sales_24b_scaled;

-- Step 4: Update the view to point to the scaled table
CREATE OR REPLACE VIEW main.contoso_sales_24b AS
SELECT * FROM main.contoso_sales_24b_scaled;

-- Step 5: Clean up temporary table
DROP TABLE main.contoso_sales_4_8b_temp;

-- Final verification
SELECT
    'Final table size' as description,
    COUNT(*) as row_count
FROM main.contoso_sales_24b_scaled;