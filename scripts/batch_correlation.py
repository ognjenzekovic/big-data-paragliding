# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q5 - CAPE vs Cloud Base Correlation") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/transformed/era5/")

# Filtriramo letacku sezonu
df_season = df.filter(
    (F.col("month").between(4, 9)) &
    (F.col("hour").between(8, 18))
)

# Korelacija CAPE i cloud base po mesecu
result = df_season.groupBy("month").agg(
    F.round(F.corr("cape", "cloud_base_m"), 3).alias("cape_cloudbase_corr"),
    F.round(F.corr("cape", "blh"), 3).alias("cape_blh_corr"),
    F.round(F.avg("cape"), 1).alias("avg_cape"),
    F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base"),
    F.round(F.avg("blh"), 0).alias("avg_blh")
).orderBy("month")

# Window funkcija - rank meseca po korelaciji
window_rank = Window.orderBy(F.col("cape_cloudbase_corr").desc())
result = result.withColumn(
    "correlation_rank",
    F.rank().over(window_rank)
)

print("\n=== Q5: KORELACIJA CAPE I CLOUD BASE PO MESECU ===")
result.show()

result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/data/curated/q5_cape_cloudbase_correlation/")

spark.stop()