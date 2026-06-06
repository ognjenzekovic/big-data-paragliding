# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q3 - Best Time of Day") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Filtriramo letacku sezonu
df_season = df.filter(F.col("month").between(4, 9))

# Agregacija po satu - prosek parametara i broj flyable sati
hourly = df_season.groupBy("hour").agg(
    F.round(F.mean("cape"), 2).alias("avg_cape"),
    F.round(F.mean("blh"), 2).alias("avg_blh"),
    F.round(F.mean("wind_speed"), 2).alias("avg_wind"),
    F.round(F.mean("cloud_base_m"), 2).alias("avg_cloud_base"),
    F.round(
        F.sum(F.when(F.col("flying_category") != "not_flyable", 1).otherwise(0)) * 100.0 /
        F.count("*"), 1
    ).alias("flyable_pct")
).orderBy("hour")

# Window funkcija - rolling average CAPE kroz 3 sata
# Pokazuje kako se termicka aktivnost razvija kroz dan
window_rolling = Window.orderBy("hour").rowsBetween(-1, 1)

hourly = hourly \
    .withColumn(
        "cape_rolling_avg",
        F.round(F.avg("avg_cape").over(window_rolling), 2)
    ) \
    .withColumn(
        "blh_rolling_avg",
        F.round(F.avg("avg_blh").over(window_rolling), 2)
    )

print("\n=== Q3: USLOVI PO SATU (LETACKA SEZONA) ===")
hourly.show(24)

hourly.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q3_hourly_conditions/")

spark.stop()