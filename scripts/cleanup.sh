#!/bin/bash

echo "Deleting HDFS zones..."
docker exec namenode hdfs dfs -rm -r /data/raw/era5/
docker exec namenode hdfs dfs -rm -r /data/transformed/era5/
docker exec namenode hdfs dfs -rm -r /data/curated/

echo "Creating empty zones..."
docker exec namenode hdfs dfs -mkdir -p /data/raw/era5
docker exec namenode hdfs dfs -mkdir -p /data/transformed/era5
docker exec namenode hdfs dfs -mkdir -p /data/curated
docker exec namenode hdfs dfs -chmod -R 777 /data

echo "Deleting local additional files..."
rm -f data/*.idx
rm -f scripts/*_raw.parquet
rm -f scripts/*_processed.parquet
rm -f *_raw.parquet

echo "Deleting PostgreSQL stream data..."
docker exec postgres psql -U paragliding -d paragliding -c "TRUNCATE TABLE weather_stream;"

echo "Cleanup done!"