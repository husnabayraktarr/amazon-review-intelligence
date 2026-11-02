-- Stage 2c: Redshift Serverless - Spectrum + native star schema

-- 1. Connect Redshift to the Glue Data Catalog (Iceberg tables in S3)
CREATE EXTERNAL SCHEMA IF NOT EXISTS lake
FROM DATA CATALOG
DATABASE 'reviews_db'
IAM_ROLE default;

-- 2. Load native tables with distribution and sort keys
CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_date;
CREATE TABLE warehouse.dim_date DISTSTYLE ALL SORTKEY (date_key)
AS SELECT * FROM lake.dim_date;

DROP TABLE IF EXISTS warehouse.dim_product;
CREATE TABLE warehouse.dim_product DISTSTYLE ALL SORTKEY (product_key)
AS SELECT * FROM lake.dim_product;

-- Started as DISTKEY(product_key): measured 2.85x skew from popular products.
-- Dimensions are DISTSTYLE ALL, so joins never redistribute -> EVEN removes skew at no cost.
DROP TABLE IF EXISTS warehouse.fact_reviews;
CREATE TABLE warehouse.fact_reviews DISTSTYLE EVEN SORTKEY (date_key)
AS SELECT * FROM lake.fact_reviews;

DROP TABLE IF EXISTS warehouse.agg_product_monthly;
CREATE TABLE warehouse.agg_product_monthly DISTKEY (parent_asin) SORTKEY (month_start)
AS SELECT * FROM lake.agg_product_monthly;

-- 3. Precomputed quality alerts for dashboards
CREATE MATERIALIZED VIEW warehouse.mv_quality_alerts
AUTO REFRESH YES
AS
SELECT a.month_start, a.parent_asin, p.title, p.brand, p.price_band,
       a.review_count, a.baseline_one_star_rate, a.one_star_rate, a.one_star_rate_change
FROM warehouse.agg_product_monthly a
JOIN warehouse.dim_product p
  ON a.parent_asin = p.parent_asin AND p.is_current
WHERE a.quality_drop_flag;

-- 4. Checks
SELECT "table", diststyle, sortkey1, tbl_rows, skew_rows
FROM svv_table_info WHERE schema = 'warehouse' ORDER BY "table";

-- Stage 3: after the SCD2 rebuild, reload the versioned dimension and fact table
-- (drop the materialized view first, because it depends on these tables)
DROP MATERIALIZED VIEW IF EXISTS warehouse.mv_quality_alerts;
DROP TABLE IF EXISTS warehouse.dim_product;
CREATE TABLE warehouse.dim_product DISTSTYLE ALL SORTKEY (product_key) AS SELECT * FROM lake.dim_product;
DROP TABLE IF EXISTS warehouse.fact_reviews;
CREATE TABLE warehouse.fact_reviews DISTSTYLE EVEN SORTKEY (date_key) AS SELECT * FROM lake.fact_reviews;
-- then recreate warehouse.mv_quality_alerts (see above)
