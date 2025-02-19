from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W

spark = SparkSession.builder.appName("JoinCompliance").getOrCreate()

flights = spark.read.csv("flights.csv")
aircraft = spark.read.csv("aircraft.csv")

# Bad join
flights = flights.join(aircraft, 'aircraft_id')

# Good join
flights = flights.join(aircraft, 'aircraft_id', how='inner')

# Avoid right join
flights = aircraft.join(flights, 'aircraft_id', how='right')

# Avoid renaming all columns
columns = ['start_time', 'end_time', 'idle_time', 'total_time']
for col in columns:
    flights = flights.withColumnRenamed(col, 'flights_' + col)
    parking = parking.withColumnRenamed(col, 'parking_' + col)

flights = flights.join(parking, on='flight_code', how='left')

#--------------------------------------------------

df_nulls = spark.createDataFrame([('a', None), ('a', 1), ('a', 2), ('a', None)], ['key', 'num'])
w4 = W.partitionBy('key').orderBy('num')

#--------------------------------------------------
# Bad usage of analytic functions without ignorenulls
df_nulls.select('key', F.first('num').over(w4).alias('first')).collect()
df_nulls.select('key', F.last('num').over(w4).alias('last')).collect()

# Good usage of analytic functions with ignorenulls
df_nulls.select('key', F.first('num', ignorenulls=True).over(w4).alias('first')).collect()
df_nulls.select('key', F.last('num', ignorenulls=True).over(w4).alias('last')).collect()

#--------------------------------------------------
#bad (Dealing with nulls)
df_nulls.select('key', F.first('num').over(w4).alias('first')).collect()
# => [Row(key='a', first=None), Row(key='a', first=None), Row(key='a', first=None), Row(key='a', first=None)]
 
df_nulls.select('key', F.last('num').over(w4).alias('last')).collect()

#--------------------------------------------------
# bad
w1 = W.partitionBy('key')
w2 = W.partitionBy('key').orderBy('num')
  
df_nulls.select('key', F.sum('num').over(w1).alias('sum')).collect()
# => [Row(key='a', sum=10), Row(key='a', sum=10), Row(key='a', sum=10), Row(key='a', sum=10)]
 
# good
w3 = W.partitionBy('key').orderBy('num').rowsBetween(W.unboundedPreceding, 0)
w4 = W.partitionBy('key').orderBy('num').rowsBetween(W.unboundedPreceding, W.unboundedFollowing)

#--------------------------------------------------
# bad
w = W.partitionBy()