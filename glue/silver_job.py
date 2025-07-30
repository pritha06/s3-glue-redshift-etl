from pyspark.sql import SparkSession
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["redshift_temp_dir", "bronze_table", "silver_table", "redshift_db"])

spark = SparkSession.builder.appName("SilverJob").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://your-redshift-endpoint:5439/{args['redshift_db']}") \
    .option("dbtable", args["bronze_table"]) \
    .option("tempdir", args["redshift_temp_dir"]) \
    .option("user", "your_user") \
    .option("password", "your_password") \
    .load()

# Data quality checks or business logic
df_clean = df.filter(col("status") == "ACTIVE")

df_clean.write \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://your-redshift-endpoint:5439/{args['redshift_db']}") \
    .option("dbtable", args["silver_table"]) \
    .option("tempdir", args["redshift_temp_dir"]) \
    .option("user", "your_user") \
    .option("password", "your_password") \
    .mode("overwrite") \
    .save()