# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q9 - BLH Development Through Day") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

df_season = df.filter(
    F.col("month").between(4, 9) &
    F.col("hour").between(6, 21)
)

result = df_season.groupBy("season", "hour").agg(
    F.round(F.avg("blh"), 0).alias("avg_blh"),
    F.round(F.avg("cape"), 1).alias("avg_cape"),
    F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base"),
    F.round(F.avg("temp_c"), 1).alias("avg_temp")
).orderBy("season", "hour")

window_max = Window.partitionBy("season").orderBy(F.col("avg_blh").desc())
result = result.withColumn("blh_rank", F.rank().over(window_max))

window_rolling = Window.partitionBy("season").orderBy("hour").rowsBetween(-1, 1)
result = result.withColumn(
    "blh_rolling_avg",
    F.round(F.avg("avg_blh").over(window_rolling), 0)
)

print("\n=== Q9: BLH RAZVOJ KROZ DAN PO SEZONI ===")
result.show(50)

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q9_blh_development/")

spark.stop()