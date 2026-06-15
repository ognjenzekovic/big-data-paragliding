# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q1 - Flying Hours by Month") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Agregacija po mesecu - broj sati po kategoriji
# Delimo sa brojem grid tacaka da dobijemo prosecne sate za lokaciju
n_grid = df.select("latitude", "longitude").distinct().count()

result = df.groupBy("month").agg(
    F.round(
        F.sum(F.when(F.col("flying_category") == "student", 1).otherwise(0)) / n_grid, 1
    ).alias("student_hours"),
    F.round(
        F.sum(F.when(F.col("flying_category") == "beginner", 1).otherwise(0)) / n_grid, 1
    ).alias("beginner_hours"),
    F.round(
        F.sum(F.when(F.col("flying_category") == "expert", 1).otherwise(0)) / n_grid, 1
    ).alias("expert_hours")
).orderBy("month")

# Window funkcija - kumulativni sati kroz godinu po kategoriji
window = Window.orderBy("month").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

result = result \
    .withColumn(
        "cumulative_student",
        F.round(F.sum("student_hours").over(window), 1)
    ) \
    .withColumn(
        "cumulative_expert",
        F.round(F.sum("expert_hours").over(window), 1)
    )

print("\n=== Q1: LETACKI SATI PO MESECU ===")
result.show()

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q1_flying_hours_by_month/")

spark.stop()