# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export to PostgreSQL") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

JDBC_URL = "jdbc:postgresql://postgres:5432/paragliding"
JDBC_PROPS = {
    "user": "paragliding",
    "password": "paragliding",
    "driver": "org.postgresql.Driver"
}

def export(hdfs_path, table):
    print("Exporting {} -> {}".format(hdfs_path, table))
    df = spark.read.parquet(hdfs_path)
    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode="overwrite",
        properties=JDBC_PROPS
    )
    print("Done! {} rows exported to {}".format(df.count(), table))

export("hdfs://namenode:9000/data/curated/q1_flying_hours_by_month/", "batch_q1")
export("hdfs://namenode:9000/data/curated/q3_hourly_conditions/", "batch_q3")
export("hdfs://namenode:9000/data/curated/q4_hourly_by_category/", "batch_q4")
export("hdfs://namenode:9000/data/curated/q5_cape_cloudbase_correlation/", "batch_q5")
export("hdfs://namenode:9000/data/curated/q6_dangerous_conditions/", "batch_q6")
export("hdfs://namenode:9000/data/curated/q7_xc_best_months/", "batch_q7")
export("hdfs://namenode:9000/data/curated/q9_blh_development/", "batch_q9")
export("hdfs://namenode:9000/data/curated/q10_climate_trend/", "batch_q10")

spark.stop()