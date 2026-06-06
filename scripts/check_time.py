# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Check") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/data/raw/era5/")
df.select("time").show(5, truncate=False)
print("time dtype:", df.schema["time"].dataType)
spark.stop()