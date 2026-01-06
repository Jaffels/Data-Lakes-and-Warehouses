import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import DateType

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME","S3_BUCKET","CURATED_PARQUET_KEY","JDBC_URL","JDBC_TABLE","JDBC_USER","JDBC_PASSWORD"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 1) Read curated Parquet
parquet_key = args["CURATED_PARQUET_KEY"].strip().lstrip("/").rstrip(".")
input_path = f"s3://{args['S3_BUCKET']}/{parquet_key}"
print(f"Reading curated Parquet from: {input_path}")

if not parquet_key.lower().endswith(".parquet"):
    raise ValueError(f"CURATED_PARQUET_KEY must point to a .parquet file. Got: {input_path}")

curated = spark.read.parquet(input_path)
curated = curated.select([col(c).alias(c.lower()) for c in curated.columns])

if "date" not in curated.columns:
    raise ValueError("Curated dataset must contain a 'date' column")

curated = curated.withColumn("date", to_date(col("date"))).dropDuplicates(["date"])

# 2) Find latest date in Postgres
jdbc_url = args["JDBC_URL"]
jdbc_table = args["JDBC_TABLE"]
jdbc_user = args["JDBC_USER"]
jdbc_password = args["JDBC_PASSWORD"]

max_date_query = f"(SELECT MAX(date) AS max_date FROM {jdbc_table}) AS t"

max_df = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", max_date_query)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
)

max_date = max_df.collect()[0]["max_date"]  # None if table empty
print(f"Max date currently in Postgres: {max_date}")

# 3) Filter curated rows that are strictly newer than max_date
if max_date is None:
    to_write = curated
    print("Postgres table appears empty; will append all curated rows.")
else:
    to_write = curated.filter(col("date") > lit(max_date).cast(DateType()))

# If nothing new, exit cheaply
if to_write.limit(1).count() == 0:
    print("No new data found. Exiting without writing.")
    job.commit()
    sys.exit(0)

# 4) Append to Postgres
to_write = to_write.repartition(1)  # small append, avoid multiple JDBC connections

(to_write.write
  .format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", jdbc_table)
  .option("user", jdbc_user)
  .option("password", jdbc_password)
  .option("driver", "org.postgresql.Driver")
  .mode("append")
  .save()
)

print("✓ Appended new rows into RDS PostgreSQL (daily incremental)")
job.commit()
