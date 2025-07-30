from pyspark.sql import SparkSession
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, sum

args = getResolvedOptions(sys.argv, ["redshift_temp_dir", "silver_table", "gold_table", "redshift_db"])

spark = SparkSession.builder.appName("GoldJob").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://your-redshift-endpoint:5439/{args['redshift_db']}") \
    .option("dbtable", args["silver_table"]) \
    .option("tempdir", args["redshift_temp_dir"]) \
    .option("user", "your_user") \
    .option("password", "your_password") \
    .load()

# Aggregation logic for Gold Layer
df_gold = df.groupBy("region").agg(sum("sales").alias("total_sales"))

df_gold.write \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://your-redshift-endpoint:5439/{args['redshift_db']}") \
    .option("dbtable", args["gold_table"]) \
    .option("tempdir", args["redshift_temp_dir"]) \
    .option("user", "your_user") \
    .option("password", "your_password") \
    .mode("overwrite") \
    .save()