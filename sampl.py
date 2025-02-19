# Sample PySpark code to test the checker
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("TestApp").getOrCreate()

df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "category"])
git_account = 'vernova'

# Violations
bad_df = df.select(F.lower(df.category))  # C9001
bad_df = df.select(F.lower(F.col('category'))) 
bad_df = df.withColumnRenamed("category", "cat")  # C9004
bad_df = df.withColumn("category", F.col("category").cast("string"))  # C9004
bad_df = df.withColumn("new_col", F.lit(""))  # C9005
bad_df = df.filter((F.col("id") == 1) & (F.col("category") != "A") & (F.col("id") > 0) & (F.col("category") != "B"))  # C9002

# Correct code
correct_df = df.select(F.lower(F.col("category")))  # C9001 fixed
correct_df = df.select(F.col("category").alias("cat"))  # C9004 fixed
correct_df = df.select(F.col("category").cast("string"))  # C9004 fixed
correct_df = df.select("new_col", F.lit(None))  # C9005 fixed

correct_df.show()
