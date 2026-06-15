# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q8 - Morning to Afternoon Prediction") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Samo letacka sezona maj-avgust
df_season = df.filter(F.col("month").between(5, 8))

# Jutarnji parametri (6-9h) - samo topla jutra
morning = df_season.filter(
    (F.col("hour").between(6, 9)) &
    (F.col("temp_c") > 10)
).groupBy("year", "month", "day", "latitude", "longitude") \
    .agg(
        F.round(F.avg("temp_c"), 2).alias("morning_temp"),
        F.round(F.avg("wind_speed"), 2).alias("morning_wind"),
        F.round(F.avg("cape"), 2).alias("morning_cape"),
        F.round(F.avg("tcc"), 2).alias("morning_tcc")
    ) \
    .withColumn("morning_sky",
        F.when(F.col("morning_tcc") < 0.2, "clear")
         .when(F.col("morning_tcc") < 0.5, "partly_cloudy")
         .when(F.col("morning_tcc") < 0.8, "mostly_cloudy")
         .otherwise("overcast")
    )

# Popodnevni uslovi (12-16h)
afternoon = df_season.filter(
    F.col("hour").between(12, 16)
).groupBy("year", "month", "day", "latitude", "longitude") \
    .agg(
        F.max("flying_category").alias("afternoon_category"),
        F.round(F.avg("cape"), 2).alias("afternoon_cape"),
        F.round(F.avg("blh"), 2).alias("afternoon_blh")
    )

# Join jutro i popodne
joined = morning.join(
    afternoon,
    ["year", "month", "day", "latitude", "longitude"],
    "inner"
)

# Verovatnoca dobrih uslova po jutarnjem nebu
result = joined.groupBy("morning_sky").agg(
    F.round(
        F.sum(F.when(
            F.col("afternoon_category") != "not_flyable", 1
        ).otherwise(0)) * 100.0 / F.count("*"), 1
    ).alias("flyable_pct"),
    F.round(F.avg("morning_temp"), 1).alias("avg_morning_temp"),
    F.round(F.avg("morning_wind"), 2).alias("avg_morning_wind"),
    F.count("*").alias("total_days")
).orderBy(F.col("flyable_pct").desc())

# Window funkcija
window_rank = Window.orderBy(F.col("flyable_pct").desc())
result = result.withColumn("rank", F.rank().over(window_rank))

print("\n=== Q8: PREDIKCIJA POPODNEVNIH USLOVA NA OSNOVU JUTARNJEG NEBA ===")
result.show()

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q8_afternoon_prediction/")

spark.stop()