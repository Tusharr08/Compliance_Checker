"""Pyspark Code"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("TestApp").getOrCreate()

df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "category"])
GIT_ACCOUNT = 'vernova'
# Correct code
correct_df = df.select(F.lower(F.col("category")))  # C1001 fixed
correct_df = df.select(F.col("category").alias("cat"))  # C1004 fixed
correct_df = df.select(F.col("category").cast("string"))  # C1004 fixed
correct_df = df.select("new_col", F.lit(None))  # C1005 fixed

correct_df.show()