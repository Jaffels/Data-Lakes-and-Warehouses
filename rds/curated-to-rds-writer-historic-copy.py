import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME","S3_BUCKET","CURATED_PARQUET_KEY","JDBC_URL","JDBC_TABLE","JDBC_USER","JDBC_PASSWORD"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

parquet_key = args["CURATED_PARQUET_KEY"].strip().lstrip("/").rstrip(".")
input_path = f"s3://{args['S3_BUCKET']}/{parquet_key}"
print(f"Reading curated Parquet from: {input_path}")

# Guardrail
if not parquet_key.lower().endswith(".parquet"):
    raise ValueError(f"CURATED_PARQUET_KEY must point to a .parquet file. Got: {input_path}")

df = spark.read.parquet(input_path)

# normalize column names
df = df.select([col(c).alias(c.lower()) for c in df.columns])

# enforce date type (safe even if already date)
if "date" in df.columns:
    df = df.withColumn("date", to_date(col("date")))

print(f"Columns: {len(df.columns)}")
df.printSchema()

# keep JDBC load stable
df = df.repartition(8)

#  Full refresh: overwrite table contents
(df.write
  .format("jdbc")
  .option("url", args["JDBC_URL"])
  .option("dbtable", args["JDBC_TABLE"])
  .option("user", args["JDBC_USER"])
  .option("password", args["JDBC_PASSWORD"])
  .option("driver","org.postgresql.Driver")
  .option("truncate","true")   # truncate existing rows
  .mode("overwrite")           # then write full dataset
  .save()
)

print("✓ Successfully wrote curated Parquet data into RDS PostgreSQL (full refresh)")
job.commit()
