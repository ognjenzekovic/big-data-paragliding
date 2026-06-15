# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q7 - Best Month for XC Flying") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# XC uslovi - jak CAPE, visok BLH, umeren vetar
df_xc = df.withColumn("xc_suitable",
    F.when(
        (F.col("cape") > 500) &
        (F.col("blh") > 1000) &
        (F.col("wind_speed").between(2, 10)) &
        (F.col("cloud_base_m") > 1000) &
        (F.col("tcc") < 0.7) &
        (F.col("hour").between(10, 16)),
        1
    ).otherwise(0)
)

n_grid = df.select("latitude", "longitude").distinct().count()
n_years = df.select("year").distinct().count()

result = df_xc.groupBy("month").agg(
    F.round(F.sum("xc_suitable") / n_grid / n_years, 1)
     .alias("xc_days"),
    F.round(F.avg("cape"), 1).alias("avg_cape"),
    F.round(F.avg("blh"), 1).alias("avg_blh"),
    F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base"),
    F.round(F.avg("wind_speed"), 2).alias("avg_wind")
).orderBy("month")

# Window funkcija - rank meseca po XC danima
window_rank = Window.orderBy(F.col("xc_days").desc())
result = result.withColumn("xc_rank", F.rank().over(window_rank))

# Window funkcija - razlika u odnosu na prethodni mesec
window_lag = Window.orderBy("month")
result = result.withColumn(
    "xc_days_change",
    F.round(F.col("xc_days") - F.lag("xc_days", 1).over(window_lag), 1)
)

print("\n=== Q7: NAJBOLJI MESECI ZA XC LETENJE ===")
result.show()

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q7_xc_best_months/")

spark.stop()