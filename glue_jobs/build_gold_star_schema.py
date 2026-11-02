"""
Stage 2b - Glue job: silver -> gold star schema (Apache Iceberg)
Builds dim_date, dim_product (SCD2-ready), fact_reviews, agg_product_monthly
Job parameters: --BUCKET, --datalake-formats iceberg
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]
DB = "reviews_db"
GOLD = f"s3://{BUCKET}/gold"

conf = (
    SparkConf()
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/silver/")
)
sc = SparkContext(conf=conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def replace_table(df, name, partition_sql=""):
    """Atomically rebuild a gold Iceberg table (readers never see a half-written table)."""
    df.createOrReplaceTempView(f"v_{name}")
    spark.sql(f"""
        CREATE OR REPLACE TABLE glue_catalog.{DB}.{name}
        USING iceberg
        {partition_sql}
        LOCATION '{GOLD}/{name}'
        TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
        AS SELECT * FROM v_{name}
    """)


reviews = spark.table(f"glue_catalog.{DB}.silver_reviews")
products = spark.table(f"glue_catalog.{DB}.silver_products")

# ---------- dim_date: one row per calendar day ----------
dim_date = spark.sql("""
    SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
           d                                      AS full_date,
           year(d)                                AS year,
           quarter(d)                             AS quarter,
           month(d)                               AS month,
           date_format(d, 'MMMM')                 AS month_name,
           weekofyear(d)                          AS week_of_year,
           dayofweek(d)                           AS day_of_week,
           date_format(d, 'EEEE')                 AS day_name,
           dayofweek(d) IN (1, 7)                 AS is_weekend,
           trunc(d, 'MM')                         AS month_start
    FROM (SELECT explode(sequence(to_date('1996-01-01'), to_date('2023-12-31'), interval 1 day)) AS d)
""")

# ---------- dim_product: SCD2-ready (one version per product for now; price history arrives in stage 3) ----------
dim_product = (
    products
    .withColumn("valid_from", F.to_timestamp(F.lit("1900-01-01 00:00:00")))
    .withColumn("valid_to", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
    .withColumn("is_current", F.lit(True))
    .withColumn("product_key", F.xxhash64("parent_asin", "valid_from"))   # surrogate key per version
    .withColumn(
        "price_band",
        F.when(F.col("price").isNull(), "unknown")
         .when(F.col("price") < 25, "under_25")
         .when(F.col("price") < 100, "25_to_100")
         .when(F.col("price") < 500, "100_to_500")
         .otherwise("500_plus"),
    )
    .select("product_key", "parent_asin", "category", "main_category", "title", "brand",
            "category_path", "price", "price_band", "average_rating", "rating_number",
            "valid_from", "valid_to", "is_current")
)

# ---------- fact_reviews: grain = one review ----------
# Join on the product version that was valid when the review was written (SCD2 lookup).
# dim_product is small, so broadcast it to every worker instead of shuffling 2M reviews.
p = F.broadcast(dim_product.select("product_key", "parent_asin", "valid_from", "valid_to")).alias("p")
r = reviews.alias("r")
fact_reviews = (
    r.join(
        p,
        (F.col("r.parent_asin") == F.col("p.parent_asin"))
        & F.col("r.review_ts").between(F.col("p.valid_from"), F.col("p.valid_to")),
        "left",
    )
    .select(
        F.col("r.review_id"),
        F.col("p.product_key"),
        F.col("r.parent_asin"),
        F.col("r.user_key"),
        F.date_format("r.review_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("r.category"),
        F.col("r.rating"),
        (F.col("r.rating") == 1).cast("int").alias("is_one_star"),
        (F.col("r.rating") == 5).cast("int").alias("is_five_star"),
        F.col("r.helpful_vote"),
        F.col("r.verified_purchase"),
        F.col("r.has_images"),
        F.col("r.text_length"),
        F.col("r.review_ts"),
    )
).cache()

# ---------- agg_product_monthly: the quality signal ----------
monthly = (
    fact_reviews
    .withColumn("month_start", F.trunc(F.to_date("review_ts"), "month"))
    .groupBy("parent_asin", "category", "month_start")
    .agg(
        F.count("*").alias("review_count"),
        F.round(F.avg("rating"), 3).alias("avg_rating"),
        F.sum("is_one_star").alias("one_star_count"),
        F.round(F.avg(F.col("verified_purchase").cast("int")), 3).alias("verified_share"),
    )
    .withColumn("one_star_rate", F.round(F.col("one_star_count") / F.col("review_count"), 3))
)

# Compare each month with the product's previous 3 active months
prev_3 = Window.partitionBy("parent_asin").orderBy("month_start").rowsBetween(-3, -1)
agg_product_monthly = (
    monthly
    .withColumn("baseline_one_star_rate", F.round(F.avg("one_star_rate").over(prev_3), 3))
    .withColumn("baseline_review_count", F.sum("review_count").over(prev_3))
    .withColumn("one_star_rate_change",
                F.round(F.col("one_star_rate") - F.col("baseline_one_star_rate"), 3))
    .withColumn(
        "quality_drop_flag",
        F.coalesce(
            (F.col("review_count") >= 20)                      # enough reviews this month
            & (F.col("baseline_review_count") >= 30)           # enough history to compare
            & (F.col("one_star_rate_change") >= 0.15),         # 1-star rate up 15+ points
            F.lit(False),
        ),
    )
)

# ---------- Load ----------
replace_table(dim_date, "dim_date")
replace_table(dim_product, "dim_product", "PARTITIONED BY (category)")
replace_table(fact_reviews, "fact_reviews", "PARTITIONED BY (category, years(review_ts))")
replace_table(agg_product_monthly, "agg_product_monthly", "PARTITIONED BY (category)")

# ---------- Reconcile ----------
silver_n = reviews.count()
fact_n = fact_reviews.count()
unmatched = fact_reviews.filter(F.col("product_key").isNull()).count()
flagged = agg_product_monthly.filter("quality_drop_flag").count()
print(f"[recon] silver_reviews={silver_n} fact_reviews={fact_n} unmatched_product_key={unmatched} "
      f"dim_product={dim_product.count()} dim_date={dim_date.count()} "
      f"product_months={agg_product_monthly.count()} quality_drop_flags={flagged}")
if fact_n != silver_n:
    raise ValueError("Reconciliation failed: fact_reviews != silver_reviews")
if unmatched > 0:
    raise ValueError(f"{unmatched} reviews have no matching product version")

job.commit()
