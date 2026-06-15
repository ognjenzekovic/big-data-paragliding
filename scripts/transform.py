# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import math

spark = SparkSession.builder \
    .appName("Paragliding - Transform") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_raw = spark.read.parquet("hdfs://namenode:9000/data/raw/era5/")

df_transformed = df_raw \
    .withColumn("year",  
        F.year(F.to_timestamp(F.col("time") / 1000000000))
    ) \
    .withColumn("month",
        F.month(F.to_timestamp(F.col("time") / 1000000000))
    ) \
    .withColumn("day",   
        F.dayofmonth(F.to_timestamp(F.col("time") / 1000000000))
    ) \
    .withColumn("hour",  
        F.hour(F.to_timestamp(F.col("time") / 1000000000))
    ) \
    .withColumn("temp_c",
        F.col("t2m") - 273.15
    ) \
    .withColumn("dewpoint_c",
        F.col("d2m") - 273.15
    ) \
    .withColumn("wind_speed",
        F.sqrt(F.col("u10")**2 + F.col("v10")**2)
    ) \
    .withColumn("wind_dir",
        (F.degrees(F.atan2(F.col("u10"), F.col("v10"))) + 180) % 360
    ) \
    .withColumn("cloud_base_m",
        ((F.col("t2m") - 273.15) - (F.col("d2m") - 273.15)) / 8 * 1000
    ) \
    .withColumn("fog_condition",
        F.col("cloud_base_m") < 0
    ) \
    .withColumn("flying_category",
        # Expert - jak vetar OK, jaka termika OK, visok cloud base
        F.when(
            (F.col("wind_speed").between(2, 12)) &
            (F.col("cape") > 300) &
            (F.col("cloud_base_m") > 800) &
            (F.col("tcc") < 0.8),
            "expert"
        # Beginner - srednji uslovi
        ).when(
            (F.col("wind_speed").between(1, 8)) &
            (F.col("cape").between(100, 800)) &
            (F.col("cloud_base_m") > 500) &
            (F.col("tcc") < 0.6),
            "beginner"
        # Student - samo blagi uslovi, slaba termika
        ).when(
            (F.col("wind_speed").between(1, 5)) &
            (F.col("cape").between(50, 300)) &
            (F.col("cloud_base_m") > 300) &
            (F.col("tcc") < 0.5),
            "student"
        ).otherwise("not_flyable")
    ) \
    .withColumn("thermal_strength",
        F.when(F.col("cape") < 100, "none")
         .when(F.col("cape") < 500, "weak")
         .when(F.col("cape") < 1000, "moderate")
         .otherwise("strong")
    ) \
    .withColumn("time_of_day",
        F.when((F.col("hour") >= 6) & (F.col("hour") < 10), "morning")
         .when((F.col("hour") >= 10) & (F.col("hour") < 14), "midday")
         .when((F.col("hour") >= 14) & (F.col("hour") < 18), "afternoon")
         .when((F.col("hour") >= 18) & (F.col("hour") < 21), "evening")
         .otherwise("non_flying")
    ) \
    .withColumn("season",
        F.when(F.col("month").isin(12, 1, 2), "winter")
         .when(F.col("month").isin(3, 4, 5), "spring")
         .when(F.col("month").isin(6, 7, 8), "summer")
         .otherwise("autumn")
    ) \
    .drop("t2m", "d2m", "u10", "v10")  # ukloni sirove kolone

print("Transformed schema:")
df_transformed.printSchema()

df_transformed.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("hdfs://namenode:9000/data/transformed/era5/")

print("Done!")
spark.stop()