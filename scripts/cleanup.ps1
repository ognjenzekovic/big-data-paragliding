Write-Host "Deleting HDFS zones..."
docker exec namenode hdfs dfs -rm -r /data/raw/era5/
docker exec namenode hdfs dfs -rm -r /data/transformed/era5/
docker exec namenode hdfs dfs -rm -r /data/curated/

Write-Host "Creating empty zones..."
docker exec namenode hdfs dfs -mkdir -p /data/raw/era5
docker exec namenode hdfs dfs -mkdir -p /data/transformed/era5
docker exec namenode hdfs dfs -mkdir -p /data/curated
docker exec namenode hdfs dfs -chmod -R 777 /data

Write-Host "Deleting local additional files..."
Remove-Item -Path "data\*.idx" -ErrorAction SilentlyContinue
Remove-Item -Path "scripts\*_raw.parquet" -ErrorAction SilentlyContinue
Remove-Item -Path "scripts\*_processed.parquet" -ErrorAction SilentlyContinue
Remove-Item -Path "*_raw.parquet" -ErrorAction SilentlyContinue

Write-Host "Deleting PostgreSQL stream data..."
docker exec postgres psql -U paragliding -d paragliding -c "TRUNCATE TABLE weather_stream;"

Write-Host "Cleanup done!"