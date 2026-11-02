"""
Stage 2a - Glue job: products bronze -> silver (Apache Iceberg)
Job parameters: --BUCKET, --CATEGORY, --datalake-formats iceberg
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                               StructField, StructType)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "CATEGORY"])
BUCKET, CATEGORY = args["BUCKET"], args["CATEGORY"]

DB = "reviews_db"
TABLE = f"glue_catalog.{DB}.silver_products"
SRC_PATH = f"s3://{BUCKET}/bronze/products/category={CATEGORY}/"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/products/category={CATEGORY}/"

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

# ---------- Extract ----------
# price is read as text: in the source it is sometimes a number and sometimes the word "None"
PRODUCT_SCHEMA = StructType([
    StructField("parent_asin", StringType()),
    StructField("main_category", StringType()),
    StructField("title", StringType()),
    StructField("store", StringType()),
    StructField("price", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("rating_number", LongType()),
    StructField("categories", ArrayType(StringType())),
])
raw = spark.read.schema(PRODUCT_SCHEMA).json(SRC_PATH)


def clean_text(col_name):
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed == "", None).otherwise(trimmed)


# ---------- Transform ----------
typed = raw.select(
    clean_text("parent_asin").alias("parent_asin"),
    F.lit(CATEGORY).alias("category"),
    clean_text("main_category").alias("main_category"),
    clean_text("title").alias("title"),
    clean_text("store").alias("brand"),
    F.when(F.size("categories") > 0, F.concat_ws(" > ", "categories")).alias("category_path"),
    # "$1,299.00" -> 1299.00 ; "None" or junk -> null
    F.regexp_replace(clean_text("price"), "[$,]", "").cast("decimal(10,2)").alias("price"),
    F.col("average_rating"),
    F.col("rating_number").cast("int").alias("rating_number"),
    F.current_timestamp().alias("ingested_at"),
).cache()

# ---------- Validate ----------
is_valid = F.col("parent_asin").isNotNull()
valid = typed.filter(is_valid)
invalid = typed.filter(~is_valid).withColumn("dq_reason", F.lit("missing_product_id"))

# ---------- Deduplicate (one row per product, keep the most-rated version) ----------
keep_one = Window.partitionBy("parent_asin").orderBy(F.col("rating_number").desc_nulls_last())
silver = (
    valid.withColumn("_rn", F.row_number().over(keep_one))
    .filter("_rn = 1")
    .drop("_rn")
)

# ---------- Load ----------
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DB}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        parent_asin STRING, category STRING, main_category STRING, title STRING,
        brand STRING, category_path STRING, price DECIMAL(10,2),
        average_rating DOUBLE, rating_number INT, ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (category)
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
""")

silver.createOrReplaceTempView("incoming")
spark.sql(f"""
    MERGE INTO {TABLE} t
    USING incoming s
    ON t.parent_asin = s.parent_asin AND t.category = s.category
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

invalid.write.mode("overwrite").parquet(QUARANTINE_PATH)

# ---------- Reconcile ----------
raw_n, valid_n, invalid_n, silver_n = typed.count(), valid.count(), invalid.count(), silver.count()
no_price = silver.filter(F.col("price").isNull()).count()
print(f"[recon] category={CATEGORY} raw={raw_n} valid={valid_n} quarantined={invalid_n} "
      f"duplicates_removed={valid_n - silver_n} merged={silver_n} missing_price={no_price}")
if raw_n != valid_n + invalid_n:
    raise ValueError("Reconciliation failed: raw != valid + quarantined")

job.commit()
