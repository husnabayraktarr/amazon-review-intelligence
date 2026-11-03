"""
Stage 1 - Glue job: reviews bronze -> silver (Apache Iceberg)
Job parameters: --BUCKET, --CATEGORY, --HASH_SALT, --datalake-formats iceberg
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType,
                               MapType, StringType, StructField, StructType)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "CATEGORY"])
BUCKET, CATEGORY = args["BUCKET"], args["CATEGORY"]

# The hash salt lives in Secrets Manager, never in code or job parameters
import boto3
SALT = boto3.client("secretsmanager", region_name="us-east-1").get_secret_value(
    SecretId="review-project/hash-salt")["SecretString"]

DB = "reviews_db"
TABLE = f"glue_catalog.{DB}.silver_reviews"
SRC_PATH = f"s3://{BUCKET}/bronze/reviews/category={CATEGORY}/"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/reviews/category={CATEGORY}/"

# ---------- Iceberg + Glue Data Catalog setup ----------
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

# ---------- Extract (explicit schema) ----------
REVIEW_SCHEMA = StructType([
    StructField("rating", DoubleType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("images", ArrayType(MapType(StringType(), StringType()))),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("helpful_vote", LongType()),
    StructField("verified_purchase", BooleanType()),
])
raw = spark.read.schema(REVIEW_SCHEMA).json(SRC_PATH)


def clean_text(col_name):
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed == "", None).otherwise(trimmed)


# ---------- Transform ----------
typed = (
    raw.select(
        F.lit(CATEGORY).alias("category"),
        clean_text("parent_asin").alias("parent_asin"),
        clean_text("asin").alias("asin"),
        F.when(clean_text("user_id").isNotNull(),
               F.sha2(F.concat(clean_text("user_id"), F.lit(SALT)), 256)).alias("user_key"),
        F.col("rating"),
        clean_text("title").alias("title"),
        clean_text("text").alias("text"),
        (F.coalesce(F.size("images"), F.lit(0)) > 0).alias("has_images"),
        F.col("helpful_vote").cast("int").alias("helpful_vote"),
        F.col("verified_purchase"),
        (F.col("timestamp") / 1000).cast("timestamp").alias("review_ts"),
    )
    .withColumn("text_length", F.coalesce(F.length("text"), F.lit(0)))
    .withColumn("review_date", F.to_date("review_ts"))
    .withColumn("ingested_at", F.current_timestamp())
    .cache()
)

# ---------- Validate ----------
ts_ok = F.coalesce(F.col("review_ts").between(F.lit("1996-01-01"), F.lit("2024-01-01")), F.lit(False))
rating_ok = F.coalesce(F.col("rating").isin(1.0, 2.0, 3.0, 4.0, 5.0), F.lit(False))
is_valid = F.coalesce(
    F.col("parent_asin").isNotNull() & F.col("user_key").isNotNull() & ts_ok & rating_ok,
    F.lit(False),
)
dq_reason = F.concat_ws(
    ",",
    F.when(F.col("parent_asin").isNull(), F.lit("missing_product")),
    F.when(F.col("user_key").isNull(), F.lit("missing_user")),
    F.when(~ts_ok, F.lit("bad_timestamp")),
    F.when(~rating_ok, F.lit("bad_rating")),
)
valid = typed.filter(is_valid)
invalid = typed.filter(~is_valid).withColumn("dq_reason", dq_reason)

# ---------- Deduplicate ----------
with_id = valid.withColumn(
    "review_id",
    F.sha2(F.concat_ws("|", "user_key", "parent_asin", F.col("review_ts").cast("string")), 256),
)
keep_one = Window.partitionBy("review_id").orderBy(F.col("helpful_vote").desc_nulls_last())
silver = (
    with_id.withColumn("_rn", F.row_number().over(keep_one))
    .filter("_rn = 1")
    .drop("_rn")
    .withColumn("rating", F.col("rating").cast("int"))
    .select("review_id", "category", "parent_asin", "asin", "user_key", "rating", "title", "text",
            "text_length", "has_images", "helpful_vote", "verified_purchase",
            "review_ts", "review_date", "ingested_at")
)

# ---------- Load ----------
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DB}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        review_id STRING, category STRING, parent_asin STRING, asin STRING, user_key STRING,
        rating INT, title STRING, text STRING, text_length INT, has_images BOOLEAN,
        helpful_vote INT, verified_purchase BOOLEAN,
        review_ts TIMESTAMP, review_date DATE, ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (category, years(review_ts))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
""")

silver.createOrReplaceTempView("incoming")
spark.sql(f"""
    MERGE INTO {TABLE} t
    USING incoming s
    ON t.review_id = s.review_id AND t.category = s.category
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

invalid.write.mode("overwrite").parquet(QUARANTINE_PATH)

# ---------- Reconcile ----------
raw_n, valid_n, invalid_n, silver_n = typed.count(), valid.count(), invalid.count(), silver.count()
print(f"[recon] category={CATEGORY} raw={raw_n} valid={valid_n} quarantined={invalid_n} "
      f"duplicates_removed={valid_n - silver_n} merged={silver_n}")
if raw_n != valid_n + invalid_n:
    raise ValueError("Reconciliation failed: raw != valid + quarantined")

job.commit()
