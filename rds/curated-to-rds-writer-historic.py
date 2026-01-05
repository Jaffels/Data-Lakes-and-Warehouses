import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_date, lit, trim, when, regexp_replace
from pyspark.sql.types import DoubleType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME","S3_BUCKET","CURATED_CSV_KEY","JDBC_URL","JDBC_TABLE","JDBC_USER","JDBC_PASSWORD"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Build input path safely (handles trailing spaces/dots)
csv_key = args["CURATED_CSV_KEY"].strip().lstrip("/").rstrip(".")
input_path = f"s3://{args['S3_BUCKET']}/{csv_key}"
print(f"Reading curated CSV from: {input_path}")

if not csv_key.lower().endswith(".csv"):
    raise ValueError(f"CURATED_CSV_KEY must point to a .csv file. Got: {input_path}")

# Read CSV
df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .csv(input_path)
)

# Normalize column names
df = df.select([col(c).alias(c.lower()) for c in df.columns])

# Parse date
if "date" in df.columns:
    df = df.withColumn("date", to_date(col("date")))

# ---- Fix: jobless_claims is empty -> load NULLs as DOUBLE ----
if "jobless_claims" in df.columns:
    df = df.withColumn("jobless_claims", lit(None).cast(DoubleType()))

# ---- Robust numeric casting for CSV (prevents varchar->double errors) ----
# Treat common "missing" tokens as null + remove commas + cast numeric columns to double
missing_tokens = ["", " ", "NA", "N/A", "NaN", "nan", "null", "None", "."]

for c in df.columns:
    if c == "date":
        continue

    df = df.withColumn(
        c,
        when(trim(col(c)).isin(missing_tokens), None).otherwise(col(c))
    )
    df = df.withColumn(c, regexp_replace(col(c), ",", ""))  # e.g. "1,234.5"
    df = df.withColumn(c, col(c).cast(DoubleType()))

print(f"Columns: {len(df.columns)}")
df.printSchema()

# Keep JDBC load stable
df = df.repartition(8)

# Write to Postgres (repeatable full refresh)
(df.write
  .format("jdbc")
  .option("url", args["JDBC_URL"])
  .option("dbtable", args["JDBC_TABLE"])
  .option("user", args["JDBC_USER"])
  .option("password", args["JDBC_PASSWORD"])
  .option("driver", "org.postgresql.Driver")
  .option("truncate", "true")
  .mode("overwrite")
  .save()
)

print("✓ Successfully wrote curated CSV data into RDS PostgreSQL (full load)")
job.commit()
