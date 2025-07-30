from pyspark.sql import SparkSession
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["s3_input_path", "redshift_temp_dir", "redshift_table", "redshift_db"])

spark = SparkSession.builder.appName("BronzeJob").getOrCreate()
df = spark.read.option("header", True).csv(args["s3_input_path"])

df = df.withColumn("load_date", current_date())

df.write \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://your-redshift-endpoint:5439/{args['redshift_db']}") \
    .option("dbtable", args["redshift_table"]) \
    .option("tempdir", args["redshift_temp_dir"]) \
    .option("user", "your_user") \
    .option("password", "your_password") \
    .mode("append") \
    .save()

"""1. bronze_glue_job.py
Reads raw S3 data

Adds ingestion_ts and partitions by yyyy/mm/dd

Writes to Redshift Bronze table using overwrite mode initially, append for subsequent runs"""