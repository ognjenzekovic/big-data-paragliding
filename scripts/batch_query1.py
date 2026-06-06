# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Paragliding - Batch Query 1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Učitaj iz HDFS
df = spark.read.parquet("hdfs://namenode:9000/data/raw/era5/")

print("=== SCHEMA ===")
df.printSchema()
print("=== BROJ REDOVA ===")
print(df.count())

# Batch upit 1: Broj letačkih sati po mesecu po kategoriji pilota
# Kriterijumi:
# Učenik:   wind_speed < 5, cape > 100, cloud_base_m > 300, tcc < 0.5
# Početnik: wind_speed < 8, cape > 200, cloud_base_m > 500, tcc < 0.6
# Iskusan:  wind_speed < 12, cape > 300, cloud_base_m > 800, tcc < 0.8

df_conditions = df.withColumn(
    "student_flyable",
    F.when(
        (F.col("wind_speed") < 5) &
        (F.col("cape") > 100) &
        (F.col("cloud_base_m") > 300) &
        (F.col("tcc") < 0.5),
        1
    ).otherwise(0)
).withColumn(
    "beginner_flyable",
    F.when(
        (F.col("wind_speed") < 8) &
        (F.col("cape") > 200) &
        (F.col("cloud_base_m") > 500) &
        (F.col("tcc") < 0.6),
        1
    ).otherwise(0)
).withColumn(
    "expert_flyable",
    F.when(
        (F.col("wind_speed") < 12) &
        (F.col("cape") > 300) &
        (F.col("cloud_base_m") > 800) &
        (F.col("tcc") < 0.8),
        1
    ).otherwise(0)
)

# Agregacija po mesecu - prosek po grid tackama pa suma po satu
result = df_conditions.groupBy("month").agg(
    F.round(F.sum("student_flyable") / F.countDistinct("latitude", "longitude"), 1)
     .alias("student_hours"),
    F.round(F.sum("beginner_flyable") / F.countDistinct("latitude", "longitude"), 1)
     .alias("beginner_hours"),
    F.round(F.sum("expert_flyable") / F.countDistinct("latitude", "longitude"), 1)
     .alias("expert_hours")
).orderBy("month")

print("\n=== LETAČKI SATI PO MESECU PO KATEGORIJI PILOTA ===")
result.show()

# Window funkcija - kumulativni letački sati kroz godinu
window_spec = Window.orderBy("month").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

result_cumulative = result.withColumn(
    "cumulative_expert_hours",
    F.sum("expert_hours").over(window_spec)
)

print("\n=== KUMULATIVNI LETAČKI SATI (ISKUSAN PILOT) ===")
result_cumulative.show()

# Sačuvaj u curated zonu
result.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/data/curated/flying_hours_by_month/"
)
print("\nRezultati sačuvani u HDFS curated zonu!")

spark.stop()