"""
Stage 3c - Glue job: DMS CDC files -> dim_product_history (SCD Type 2, Apache Iceberg)
Reads the DMS full load + ongoing change files and builds one row per product version.
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
CDC_PATH = f"s3://{BUCKET}/bronze/cdc/public/products/"
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

EARLIEST = F.to_timestamp(F.lit("1900-01-01 00:00:00"))
FOREVER = F.to_timestamp(F.lit("9999-12-31 23:59:59"))

# ---------- Extract: full load (LOAD*.parquet) + changes (timestamped .parquet) ----------
cdc = (
    spark.read.parquet(CDC_PATH)
    .select(
        F.upper(F.col("Op")).alias("op"),                       # I = insert, U = update, D = delete
        F.col("cdc_ts").cast("timestamp").alias("cdc_ts"),       # when DMS captured the change
        F.trim(F.col("parent_asin")).alias("parent_asin"),
        F.col("title"),
        F.col("brand"),
        F.col("main_category"),
        F.col("price").cast("decimal(10,2)").alias("price"),
    )
    .withColumn("op_rank", F.when(F.col("op") == "I", 0).when(F.col("op") == "U", 1).otherwise(2))
)

# ---------- Keep only events that really change a tracked attribute ----------
by_time = Window.partitionBy("parent_asin").orderBy("cdc_ts", "op_rank")
events = (
    cdc
    .withColumn("attr_hash", F.sha2(F.concat_ws(
        "|",
        F.coalesce(F.col("title"), F.lit("")),
        F.coalesce(F.col("brand"), F.lit("")),
        F.coalesce(F.col("main_category"), F.lit("")),
        F.coalesce(F.col("price").cast("string"), F.lit("")),
    ), 256))
    .withColumn("prev_hash", F.lag("attr_hash").over(by_time))
    .withColumn("prev_op", F.lag("op").over(by_time))
)
is_change = (
    (F.col("op") == "D")
    | F.col("prev_hash").isNull()
    | (F.col("prev_op") == "D")
    | (F.col("attr_hash") != F.col("prev_hash"))
)
changes = events.filter(is_change).withColumn("next_ts", F.lead("cdc_ts").over(by_time))

# ---------- Build versions (deletes close the open version but don't open a new one) ----------
versions = (
    changes.filter(F.col("op") != "D")
    .withColumn("version", F.row_number().over(by_time))
    .withColumn("valid_from", F.when(F.col("version") == 1, EARLIEST).otherwise(F.col("cdc_ts")))
    .withColumn("valid_to", F.coalesce(F.col("next_ts"), FOREVER))
    .withColumn("is_current", F.col("next_ts").isNull())
)

# ---------- Enrich with attributes that only exist in the dataset (not in the operational DB) ----------
static_attrs = spark.table(f"glue_catalog.{DB}.silver_products").select(
    "parent_asin", "category", "category_path", "average_rating", "rating_number"
)
dim = (
    versions.join(static_attrs, "parent_asin", "left")
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Appliances")))
    .withColumn("product_key", F.xxhash64("parent_asin", "valid_from"))     # surrogate key per version
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
            "version", "valid_from", "valid_to", "is_current")
).cache()

# ---------- Load ----------
dim.createOrReplaceTempView("v_dim_product_history")
spark.sql(f"""
    CREATE OR REPLACE TABLE glue_catalog.{DB}.dim_product_history
    USING iceberg
    PARTITIONED BY (category)
    LOCATION '{GOLD}/dim_product_history'
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    AS SELECT * FROM v_dim_product_history
""")

# ---------- Reconcile ----------
rows = dim.count()
products = dim.select("parent_asin").distinct().count()
current = dim.filter("is_current").count()
with_history = dim.filter("version > 1").select("parent_asin").distinct().count()
dup_keys = rows - dim.select("product_key").distinct().count()
multi_current = dim.filter("is_current").groupBy("parent_asin").count().filter("count > 1").count()
print(f"[recon] cdc_events={cdc.count()} versions={rows} products={products} current={current} "
      f"discontinued={products - current} products_with_history={with_history} "
      f"duplicate_keys={dup_keys} products_with_2plus_current={multi_current}")
if dup_keys > 0 or multi_current > 0:
    raise ValueError("SCD2 integrity check failed")

job.commit()
