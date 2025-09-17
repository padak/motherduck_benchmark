-- Incremental Scaling from 9.6B to 24B rows
-- Execute each batch separately in MotherDuck UI with 15-30 second pauses between them
-- Total: 14 batches (1.4B + 13×1B = 14.4B rows added)

-- ============================================================
-- BATCH 1: Add 1.4B rows (1,400,000,000)
-- Current: 9.6B → Target after batch: 11B
-- ============================================================

-- Create temporary table with 1.4B rows (240k × 5,834 ≈ 1.4B)
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (
    SELECT generate_series AS batch_id
    FROM generate_series(1, 5834)
) AS replicator;

-- Insert into main table
INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch;

-- Clean up
DROP TABLE main.temp_batch;

-- Verify current count (should be ~11B)
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- ============================================================
-- WAIT 15-30 SECONDS BEFORE RUNNING BATCH 2
-- ============================================================

-- ============================================================
-- BATCH 2: Add 1B rows (1,000,000,000)
-- Current: 11B → Target after batch: 12B
-- ============================================================

CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (
    SELECT generate_series AS batch_id
    FROM generate_series(1, 4167)  -- 240k × 4,167 ≈ 1B
) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch;

DROP TABLE main.temp_batch;

SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- ============================================================
-- WAIT 15-30 SECONDS BEFORE RUNNING BATCH 3
-- ============================================================

-- ============================================================
-- BATCH 3: Add 1B rows
-- Current: 12B → Target after batch: 13B
-- ============================================================

CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (
    SELECT generate_series AS batch_id
    FROM generate_series(1, 4167)
) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch;

DROP TABLE main.temp_batch;

SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- ============================================================
-- WAIT 15-30 SECONDS BEFORE RUNNING BATCH 4
-- ============================================================

-- ============================================================
-- BATCH 4: Add 1B rows
-- Current: 13B → Target after batch: 14B
-- ============================================================

CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (
    SELECT generate_series AS batch_id
    FROM generate_series(1, 4167)
) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled
SELECT * FROM main.temp_batch;

DROP TABLE main.temp_batch;

SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- ============================================================
-- CONTINUE WITH BATCHES 5-14 USING THE SAME PATTERN
-- Each adds 1B rows (240k × 4,167)
-- ============================================================

-- ============================================================
-- BATCH 5: Add 1B rows (14B → 15B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 6: Add 1B rows (15B → 16B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 7: Add 1B rows (16B → 17B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 8: Add 1B rows (17B → 18B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 9: Add 1B rows (18B → 19B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 10: Add 1B rows (19B → 20B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 11: Add 1B rows (20B → 21B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 12: Add 1B rows (21B → 22B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 13: Add 1B rows (22B → 23B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;
SELECT COUNT(*) as current_rows FROM main.contoso_sales_24b_scaled;

-- WAIT 15-30 SECONDS

-- ============================================================
-- BATCH 14 (FINAL): Add 1B rows (23B → 24B)
-- ============================================================
CREATE OR REPLACE TABLE main.temp_batch AS
SELECT original.*
FROM main.contoso_sales_240k AS original
CROSS JOIN (SELECT generate_series AS batch_id FROM generate_series(1, 4167)) AS replicator;

INSERT INTO main.contoso_sales_24b_scaled SELECT * FROM main.temp_batch;
DROP TABLE main.temp_batch;

-- ============================================================
-- FINAL VERIFICATION AND VIEW UPDATE
-- ============================================================

-- Final count (should be ~24B)
SELECT COUNT(*) as final_rows FROM main.contoso_sales_24b_scaled;

-- Update the view to point to scaled table
CREATE OR REPLACE VIEW main.contoso_sales_24b AS
SELECT * FROM main.contoso_sales_24b_scaled;

-- Verify view works
SELECT COUNT(*) as view_count FROM main.contoso_sales_24b;

-- ============================================================
-- SUCCESS! You should now have ~24 billion rows
-- ============================================================