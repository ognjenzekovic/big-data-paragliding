# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q10 - Climate Trend Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

result = df.filter(
    F.col("month").between(4, 9)
).groupBy("year").agg(
    F.round(F.avg("temp_c"), 2).alias("avg_temp"),
    F.round(F.avg("cape"), 2).alias("avg_cape"),
    F.round(F.avg("blh"), 2).alias("avg_blh"),
    F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
    F.round(
        F.sum(F.when(F.col("flying_category") != "not_flyable", 1).otherwise(0)) * 100.0 /
        F.count("*"), 1
    ).alias("flyable_pct")
).orderBy("year")

window_lag = Window.orderBy("year")
result = result \
    .withColumn("temp_trend",
        F.round(F.col("avg_temp") - F.lag("avg_temp", 1).over(window_lag), 2)
    ) \
    .withColumn("cape_trend",
        F.round(F.col("avg_cape") - F.lag("avg_cape", 1).over(window_lag), 2)
    ) \
    .withColumn("flyable_trend",
        F.round(F.col("flyable_pct") - F.lag("flyable_pct", 1).over(window_lag), 1)
    )

print("\n=== Q10: KLIMATSKI TREND PO GODINAMA ===")
result.show()

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q10_climate_trend/")

spark.stop()