# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q4 - Flying Hours by Time of Day and Category") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Filtriramo letacku sezonu i letacke sate
df_flying = df.filter(
    (F.col("month").between(4, 9)) &
    (F.col("hour").between(6, 21)) &
    (F.col("flying_category") != "not_flyable")
)

n_grid = df.select("latitude", "longitude").distinct().count()
n_years = df.select("year").distinct().count()

# Distribucija po satu i kategoriji
hourly = df_flying.groupBy("hour").agg(
    F.round(
        F.sum(F.when(F.col("flying_category") == "student", 1).otherwise(0)) / n_grid / n_years, 1
    ).alias("student_hours"),
    F.round(
        F.sum(F.when(F.col("flying_category") == "beginner", 1).otherwise(0)) / n_grid / n_years, 1
    ).alias("beginner_hours"),
    F.round(
        F.sum(F.when(F.col("flying_category") == "expert", 1).otherwise(0)) / n_grid / n_years, 1
    ).alias("expert_hours"),
    F.round(F.avg("cape"), 1).alias("avg_cape"),
    F.round(F.avg("blh"), 1).alias("avg_blh"),
    F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
    F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base")
).orderBy("hour")

# Window funkcija - rank sata po broju expert sati
window_lag = Window.orderBy("hour")

hourly = hourly \
    .withColumn("expert_rank", F.rank().over(Window.orderBy(F.col("expert_hours").desc()))) \
    .withColumn("cape_change",
        F.round(F.col("avg_cape") - F.lag("avg_cape", 1).over(window_lag), 1)
    )

print("\n=== Q4: LETACKI SATI PO SATU I KATEGORIJI (letacka sezona) ===")
hourly.show(24)

hourly.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q4_hourly_by_category/")

spark.stop()