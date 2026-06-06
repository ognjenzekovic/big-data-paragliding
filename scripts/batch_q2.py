# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q2 - Most Stable Location") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Filtriramo samo letacke sate (ne zelimo da zima kvari varijansu)
df_flying = df.filter(
    (F.col("month").between(4, 9)) &
    (F.col("hour").between(8, 18))
)

# Varijansa kljucnih parametara po lokaciji
stability = df_flying.groupBy("latitude", "longitude").agg(
    F.round(F.stddev("wind_speed"), 3).alias("wind_stddev"),
    F.round(F.stddev("cape"), 3).alias("cape_stddev"),
    F.round(F.stddev("cloud_base_m"), 3).alias("cloud_base_stddev"),
    F.round(F.mean("wind_speed"), 2).alias("avg_wind"),
    F.round(F.mean("cape"), 2).alias("avg_cape"),
    F.round(F.mean("cloud_base_m"), 2).alias("avg_cloud_base"),
    F.count("*").alias("total_hours")
)

# Kompozitni skor stabilnosti - manji je bolji
# Normalizujemo svaku standardnu devijaciju
stability = stability.withColumn(
    "stability_score",
    F.round(
        F.col("wind_stddev") * 0.4 +
        F.col("cape_stddev") * 0.001 +
        F.col("cloud_base_stddev") * 0.01,
        3
    )
)

# Window funkcija - rank lokacija po stabilnosti
window_rank = Window.orderBy("stability_score")

stability = stability.withColumn(
    "stability_rank",
    F.rank().over(window_rank)
)

print("\n=== Q2: TOP 10 NAJSTABILNIJIH LOKACIJA ===")
stability.orderBy("stability_rank").show(10)

print("\n=== Q2: TOP 10 NAJNESTABILNIJIH LOKACIJA ===")
stability.orderBy("stability_score", ascending=False).show(10)

stability.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q2_location_stability/")

spark.stop()