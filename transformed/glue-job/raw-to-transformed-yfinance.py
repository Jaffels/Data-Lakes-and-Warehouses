import sys
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, year, month, dayofmonth, round as spark_round, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'asset_class', 'load_type', 'process_date'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['source_bucket']
asset_class = args['asset_class']
load_type = args['load_type']
process_date = args['process_date']

print("Asset Class: " + asset_class)
print("Load Type: " + load_type)
print("Process Date: " + process_date)

s3 = boto3.client('s3')


# build source and target paths

if load_type == "historical":
    source_prefix = "raw/asset_class=" + asset_class + "/source=yfinance/load_type=historical/"
    target_prefix = "transformed/asset_class=" + asset_class + "/source=yfinance/load_type=historical/"
    filename = asset_class + "_yfinance_historical.parquet"
else:
    year_val, month_val, day_val = process_date.split('-')
    source_prefix = "raw/asset_class=" + asset_class + "/source=yfinance/load_type=daily/year=" + year_val + "/month=" + month_val + "/day=" + day_val + "/"
    target_prefix = "transformed/asset_class=" + asset_class + "/source=yfinance/load_type=daily/year=" + year_val + "/month=" + month_val + "/day=" + day_val + "/"
    date_str = year_val + month_val + day_val
    filename = asset_class + "_yfinance_daily_" + date_str + ".parquet"

print("Reading from: " + source_prefix)
print("Writing to: " + target_prefix + filename)


# read source files

response = s3.list_objects_v2(Bucket=bucket, Prefix=source_prefix)

parquet_files = []
for obj in response.get('Contents', []):
    key = obj['Key']
    if key.endswith('.parquet'):
        print("Found file: " + key)
        s3_path = "s3://" + bucket + "/" + key
        parquet_files.append(s3_path)

if len(parquet_files) == 0:
    print("No parquet files found in " + source_prefix)
    job.commit()
    sys.exit(0)

print("Reading " + str(len(parquet_files)) + " parquet files with Spark...")

# pyarrow writes nanosecond timestamps, Spark expects microseconds
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

try:
    raw_df = spark.read.parquet(*parquet_files)
    print("Successfully read with Spark")
except Exception as e:
    print("Spark read failed (likely timestamp format issue): " + str(e))
    print("Falling back to pandas read with timestamp conversion...")

    import pandas as pd
    import io

    all_dfs = []
    for s3_path in parquet_files:
        key = s3_path.replace("s3://" + bucket + "/", "")
        print("Reading with pandas: " + key)
        s3_obj = s3.get_object(Bucket=bucket, Key=key)
        pdf = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))

        if load_type == "historical" and 'date' in pdf.columns:
            pdf['date'] = pd.to_datetime(pdf['date'])
            pdf = pdf[pdf['date'] >= '2018-01-01']

        # convert to microseconds for Spark compatibility
        for col_name in pdf.columns:
            if pd.api.types.is_datetime64_any_dtype(pdf[col_name]):
                pdf[col_name] = pdf[col_name].astype('datetime64[us]')

        all_dfs.append(pdf)

    combined_pdf = pd.concat(all_dfs, ignore_index=True)
    print("Records after pandas read and filter: " + str(len(combined_pdf)))
    raw_df = spark.createDataFrame(combined_pdf)
    print("Successfully converted pandas to Spark")

print("Total records read: " + str(raw_df.count()))


# transform data

if load_type == "historical":
    print("Filtering data from 2018-01-01 onwards...")
    raw_df = raw_df.filter(col("date") >= "2018-01-01")
    records_after_filter = raw_df.count()
    print("Records after date filter: " + str(records_after_filter))

transformed_df = raw_df.withColumnRenamed("adj_close", "adjusted_close")
transformed_df = transformed_df.withColumn("year", year("date"))
transformed_df = transformed_df.withColumn("month", month("date"))
transformed_df = transformed_df.withColumn("day", dayofmonth("date"))
transformed_df = transformed_df.filter(col("close").isNotNull())
transformed_df = transformed_df.filter(col("close") > 0)
transformed_df = transformed_df.filter(col("high") >= col("low"))
transformed_df = transformed_df.withColumn("open", spark_round("open", 4))
transformed_df = transformed_df.withColumn("high", spark_round("high", 4))
transformed_df = transformed_df.withColumn("low", spark_round("low", 4))
transformed_df = transformed_df.withColumn("close", spark_round("close", 4))
transformed_df = transformed_df.withColumn("adjusted_close", spark_round("adjusted_close", 4))
transformed_df = transformed_df.withColumn("asset_class", lit(asset_class))
transformed_df = transformed_df.withColumn("source", lit("yfinance"))
transformed_df = transformed_df.withColumn("load_type", lit(load_type))
transformed_df = transformed_df.withColumn("transform_timestamp", current_timestamp())

final_df = transformed_df.select(
    "date", "ticker", "asset_class", "source", "load_type",
    "open", "high", "low", "close", "adjusted_close", "volume",
    "year", "month", "day", "fetch_timestamp", "transform_timestamp"
)

record_count = final_df.count()
print("Records to write: " + str(record_count))
final_df.show(5)


# write output

target_s3_path = "s3://" + bucket + "/" + target_prefix + filename

print("Writing to: " + target_s3_path)

# ~100k records per partition, cap at 10
num_partitions = max(1, int(record_count / 100000))
num_partitions = min(num_partitions, 10)

print("Using " + str(num_partitions) + " partition(s) for writing")

if num_partitions == 1:
    final_df.coalesce(1).write.mode("overwrite").parquet(target_s3_path + ".tmp")
else:
    final_df.repartition(num_partitions).write.mode("overwrite").parquet(target_s3_path + ".tmp")

# consolidate output files
print("Consolidating output file...")

tmp_prefix = target_prefix + filename + ".tmp/"
tmp_response = s3.list_objects_v2(Bucket=bucket, Prefix=tmp_prefix)

parquet_files_written = []
for obj in tmp_response.get('Contents', []):
    key = obj['Key']
    if key.endswith('.parquet') and 'part-' in key:
        parquet_files_written.append(key)

print("Found " + str(len(parquet_files_written)) + " parquet file(s)")

if len(parquet_files_written) == 0:
    print("ERROR: Could not find any parquet files in temporary directory")
    job.commit()
    sys.exit(1)

if len(parquet_files_written) == 1:
    target_key = target_prefix + filename
    print("Copying single file to " + target_key)
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': parquet_files_written[0]},
        Key=target_key
    )
else:
    print("Merging " + str(len(parquet_files_written)) + " parquet files...")

    import pandas as pd
    import io

    merge_dfs = []
    for pf in parquet_files_written:
        print("Reading: " + pf)
        s3_obj = s3.get_object(Bucket=bucket, Key=pf)
        pdf_chunk = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
        merge_dfs.append(pdf_chunk)

    merged_pdf = pd.concat(merge_dfs, ignore_index=True)
    print("Merged " + str(len(merged_pdf)) + " total records")

    target_key = target_prefix + filename
    buffer = io.BytesIO()
    merged_pdf.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=target_key, Body=buffer.getvalue())
    print("Wrote merged file to " + target_key)

# cleanup temp files
print("Cleaning up temporary files...")
for obj in tmp_response.get('Contents', []):
    s3.delete_object(Bucket=bucket, Key=obj['Key'])

print("Successfully wrote " + str(record_count) + " records")

job.commit()