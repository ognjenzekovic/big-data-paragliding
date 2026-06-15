# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q6 - Dangerous Conditions by Season") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Definisi opasne uslove
df_dangerous = df.withColumn("dangerous",
    F.when(
        (F.col("wind_speed") > 12) |
        (F.col("cape") > 2000) |
        (F.col("cloud_base_m") < 100) |
        (F.col("tcc") > 0.9),
        1
    ).otherwise(0)
)

# Distribucija po sezoni i dobu dana
result = df_dangerous.groupBy("season", "time_of_day").agg(
    F.round(F.sum("dangerous") * 100.0 / F.count("*"), 1)
     .alias("dangerous_pct"),
    F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
    F.round(F.avg("cape"), 1).alias("avg_cape"),
    F.count("*").alias("total_hours")
).orderBy("season", "time_of_day")

# Window funkcija - rank po opasnosti unutar svake sezone
window_rank = Window.partitionBy("season").orderBy(
    F.col("dangerous_pct").desc()
)
result = result.withColumn("danger_rank", F.rank().over(window_rank))

print("\n=== Q6: OPASNI USLOVI PO SEZONI I DOBU DANA ===")
result.show(20, truncate=False)

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q6_dangerous_conditions/")

spark.stop()