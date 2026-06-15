# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Paragliding - Stream Consumer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "weather-stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw_stream \
    .select(F.from_json(
        F.col("value").cast("string"), schema
    ).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", F.to_timestamp("timestamp"))

# Stream 1 - flying score
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

# Stream 2 - detekcija pogoršanja (lag kroz window)
# Poređenje trenutnog wind_speed sa prosekom prethodnih 5 min
wind_trend = scored \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        F.window("event_time", "10 minutes", "5 minutes"),
        F.col("location")
    ) \
    .agg(
        F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
        F.round(F.max("wind_speed"), 2).alias("max_wind"),
        F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base"),
        F.round(F.min("cloud_base_m"), 0).alias("min_cloud_base"),
        F.round(F.avg("flying_score"), 1).alias("avg_score")
    ) \
    .withColumn("wind_alert",
        F.when(F.col("max_wind") > 10, "HIGH_WIND")
         .when(F.col("max_wind") > 7, "MODERATE_WIND")
         .otherwise("OK")
    ) \
    .withColumn("cloud_alert",
        F.when(F.col("min_cloud_base") < 300, "LOW_CLOUD")
         .when(F.col("min_cloud_base") < 600, "MODERATE_CLOUD")
         .otherwise("OK")
    )

# Stream 3 - alert sistem
alerts = scored \
    .withColumn("alert",
        F.when(F.col("wind_speed") > 12, "DANGER_HIGH_WIND")
         .when(F.col("wind_gust") > 15, "DANGER_GUSTS")
         .when(F.col("cloud_base_m") < 200, "DANGER_LOW_CLOUD")
         .when(
             (F.col("wind_speed") > 8) &
             (F.col("cloud_base_m") < 500),
             "WARNING_COMBINED"
         )
         .otherwise("SAFE")
    ) \
    .select(
        "timestamp", "location", "lat", "lon",
        "wind_speed", "wind_gust", "cloud_base_m",
        "flying_score", "alert"
    ) \
    .filter(F.col("alert") != "SAFE")

# Stream 4 - batch join sa istorijskim prosecima
historical = spark.read.parquet(
    "hdfs://namenode:9000/data/curated/q3_hourly_conditions/"
)

current_with_historical = scored \
    .withColumn("hour", F.hour("event_time")) \
    .join(
        historical.select(
            F.col("hour").alias("hist_hour"),
            F.col("avg_wind").alias("hist_wind"),
            F.col("avg_cloud_base").alias("hist_cloud_base")
        ),
        F.col("hour") == F.col("hist_hour"),
        "left"
    ) \
    .drop("hist_hour") \
    .withColumn("wind_vs_historical",
        F.round(F.col("wind_speed") - F.col("hist_wind"), 2)
    ) \
    .withColumn("cloud_base_vs_historical",
        F.round(F.col("cloud_base_m") - F.col("hist_cloud_base"), 0)
    ) \
    .withColumn("conditions_vs_avg",
        F.when(F.col("wind_vs_historical") > 3, "WORSE_THAN_AVG")
         .when(F.col("wind_vs_historical") < -3, "BETTER_THAN_AVG")
         .otherwise("AVERAGE")
    ) \
    .select(
        "timestamp", "location", "lat", "lon",
        "wind_speed", "cloud_base_m", "temp_c",
        "hist_wind", "hist_cloud_base", "hour",
        "wind_vs_historical", "cloud_base_vs_historical",
        "conditions_vs_avg"
    )

# Stream 5 - wind shear između lokacija
# Detektuje velike razlike u vetru između lokacija u istom trenutku
windowed_locations = scored \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        F.window("event_time", "5 minutes"),
        F.col("location")
    ) \
    .agg(
        F.round(F.avg("wind_speed"), 2).alias("avg_wind"),
        F.round(F.avg("cloud_base_m"), 0).alias("avg_cloud_base"),
        F.round(F.avg("flying_score"), 1).alias("avg_score")
    )

# Funkcija za upis u PostgreSQL
def write_to_postgres(batch_df, batch_id, table):
    df = batch_df
    if "window" in batch_df.columns:
        df = df \
            .withColumn("window_start", F.col("window.start")) \
            .withColumn("window_end", F.col("window.end")) \
            .drop("window")
    
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/paragliding") \
        .option("dbtable", table) \
        .option("user", "paragliding") \
        .option("password", "paragliding") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Pokreni sve stream queries
q1 = windowed_locations.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "weather_stream")) \
    .trigger(processingTime="2 minutes") \
    .start()

q2 = wind_trend.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "wind_trend")) \
    .trigger(processingTime="2 minutes") \
    .start()

q3 = alerts.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "weather_alerts")) \
    .trigger(processingTime="2 minutes") \
    .start()

q4 = current_with_historical.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "historical_comparison")) \
    .trigger(processingTime="2 minutes") \
    .start()

q5 = windowed_locations.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "location_comparison")) \
    .trigger(processingTime="2 minutes") \
    .start()

q1.awaitTermination()