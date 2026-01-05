import sys
import boto3
import pandas as pd
import io
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, year, month, dayofmonth, to_date, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'asset_class', 'load_type', 'process_date'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# pyarrow writes nanosecond timestamps, Spark expects microseconds
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['source_bucket']
asset_class = args['asset_class']
load_type = args['load_type']
process_date = args['process_date']

print(f"Asset Class: {asset_class}")
print(f"Load Type: {load_type}")
print(f"Process Date: {process_date}")

s3 = boto3.client('s3')

# build source and target paths
if load_type == "historical":
    source_prefix = f"raw/asset_class={asset_class}/source=cryptocompare/load_type=historical/"
    target_prefix = f"transformed/asset_class={asset_class}/source=cryptocompare/load_type=historical/"
    filename = f"{asset_class}_cryptocompare_historical.parquet"
else:
    year_val, month_val, day_val = process_date.split('-')
    source_prefix = f"raw/asset_class={asset_class}/source=cryptocompare/load_type=daily/year={year_val}/month={month_val}/day={day_val}/"
    target_prefix = f"transformed/asset_class={asset_class}/source=cryptocompare/load_type=daily/year={year_val}/month={month_val}/day={day_val}/"
    date_str = year_val + month_val + day_val
    filename = f"{asset_class}_cryptocompare_daily_{date_str}.parquet"

print("Reading from: " + source_prefix)
print("Writing to: " + target_prefix + filename)

# read source files
response = s3.list_objects_v2(Bucket=bucket, Prefix=source_prefix)

all_dfs = []
found_files = False

for obj in response.get('Contents', []):
    key = obj['Key']
    if key.endswith('.parquet'):
        print("Reading file: " + key)
        found_files = True
        try:
            s3_obj = s3.get_object(Bucket=bucket, Key=key)
            pdf = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
            all_dfs.append(pdf)
        except Exception as e:
            print(f"Error reading {key}: {e}")

if not found_files:
    print("No parquet files found in " + source_prefix)
    job.commit()
    sys.exit(0)

if not all_dfs:
    print("Found files but failed to read content.")
    job.commit()
    sys.exit(1)

combined_pdf = pd.concat(all_dfs, ignore_index=True)

if 'date' in combined_pdf.columns:
    combined_pdf['date'] = pd.to_datetime(combined_pdf['date'])

print("Total records read: " + str(len(combined_pdf)))
print("Columns found: " + str(combined_pdf.columns.tolist()))

raw_df = spark.createDataFrame(combined_pdf)

# transform data
# volume_to -> volume (USD amount), volume_from -> volume_coin (token qty)

transformed_df = raw_df

if "date" in transformed_df.columns:
    transformed_df = transformed_df.withColumn("date", to_date(col("date")))
else:
    transformed_df = transformed_df.withColumn("date", to_date(lit(process_date)))

if "volume_to" in transformed_df.columns:
    transformed_df = transformed_df.withColumnRenamed("volume_to", "volume")
if "volume_from" in transformed_df.columns:
    transformed_df = transformed_df.withColumnRenamed("volume_from", "volume_coin")

# add metadata columns
transformed_df = transformed_df.withColumn("year", year("date"))
transformed_df = transformed_df.withColumn("month", month("date"))
transformed_df = transformed_df.withColumn("day", dayofmonth("date"))
transformed_df = transformed_df.withColumn("asset_class", lit(asset_class))
transformed_df = transformed_df.withColumn("source", lit("cryptocompare"))
transformed_df = transformed_df.withColumn("load_type", lit(load_type))
transformed_df = transformed_df.withColumn("transform_timestamp", current_timestamp())

# select output columns
available_cols = [c for c in transformed_df.columns]
output_cols = ["date"]

if "ticker" in available_cols:
    output_cols.append("ticker")
elif "symbol" in available_cols:
    transformed_df = transformed_df.withColumnRenamed("symbol", "ticker")
    output_cols.append("ticker")

for col_name in ["open", "high", "low", "close", "volume", "volume_coin"]:
    if col_name in available_cols:
        output_cols.append(col_name)

output_cols.extend(["asset_class", "source", "load_type", "year", "month", "day"])

if "fetch_timestamp" in available_cols:
    output_cols.append("fetch_timestamp")

output_cols.append("transform_timestamp")

final_df = transformed_df.select(*output_cols)

record_count = final_df.count()
print("Records to write: " + str(record_count))
final_df.show(5)

# write output
output_pdf = final_df.toPandas()
target_key = target_prefix + filename

print("Writing to: s3://" + bucket + "/" + target_key)

buffer = io.BytesIO()
output_pdf.to_parquet(buffer, engine='pyarrow', index=False)
buffer.seek(0)

s3.put_object(Bucket=bucket, Key=target_key, Body=buffer.getvalue())

print("Successfully wrote " + str(record_count) + " records")

job.commit()