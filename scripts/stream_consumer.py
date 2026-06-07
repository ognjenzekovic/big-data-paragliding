# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Paragliding - Stream Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema za JSON poruke iz Kafke
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("location", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("temp_c", DoubleType()),
    StructField("dewpoint_c", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("wind_dir", DoubleType()),
    StructField("wind_gust", DoubleType()),
    StructField("cloud_cover", DoubleType()),
    StructField("visibility", DoubleType()),
    StructField("weather_desc", StringType()),
    StructField("cloud_base_m", DoubleType())
])

# Citaj iz Kafka topica
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parsiraj JSON
parsed = raw_stream \
    .select(F.from_json(
        F.col("value").cast("string"), schema
    ).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", F.to_timestamp("timestamp"))

# Stream transformacija 1 - flying score (0-10)
scored = parsed \
    .withColumn("flying_score",
        F.when(F.col("wind_speed") > 12, 0)
         .when(F.col("cloud_base_m") < 300, 0)
         .otherwise(
             F.least(F.lit(10.0),
                 F.col("cloud_base_m") / 300 * 2 +
                 (F.lit(10) - F.col("wind_speed")) * 0.5 +
                 F.when(F.col("cloud_cover") < 0.5,
                     F.lit(2)).otherwise(F.lit(0))
             )
         )
    ) \
    .withColumn("flyable",
        F.when(F.col("flying_score") >= 5, "YES")
         .otherwise("NO")
    )

# Stream transformacija 2 - window agregacija
# Prosek parametara po lokaciji u poslednjih 5 minuta
windowed = scored \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        F.window("event_time", "1 minute"),  # prozor 1 minut
        F.col("location")
    ) \
    .agg(
        F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
        F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base"),
        F.round(F.avg("flying_score"), 1).alias("avg_score"),
        F.round(F.avg("temp_c"), 1).alias("avg_temp"),
        F.max("flyable").alias("flyable")
    )

debug_query = windowed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="30 seconds") \
    .start()

# Ispis u konzolu za testiranje
def write_to_postgres(batch_df, batch_id):
    batch_df \
        .withColumn("window_start", F.col("window.start")) \
        .withColumn("window_end", F.col("window.end")) \
        .drop("window") \
        .write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/paragliding") \
        .option("dbtable", "weather_stream") \
        .option("user", "paragliding") \
        .option("password", "paragliding") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = windowed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="30 seconds") \
    .start()

debug_query.awaitTermination()
query.awaitTermination()