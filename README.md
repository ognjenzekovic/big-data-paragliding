# Paragliding Weather Analytics - Big Data System

## Prerequisites
- Docker Desktop
- Python 3.x
- Git

## Python Dependencies
```bash
pip install xarray cfgrib eccodes numpy pandas pyarrow fastparquet kafka-python requests
```

## Starting the System

### 1. Clone the Repository
```bash
git clone https://github.com/ognjenzekovic/big-data-paragliding
cd paragliding-project
```

### 2. Environment Variables
Copy `.env.example` to `.env` and fill in:
API_KEY=your_openweathermap_api_key


### 3. Start Containers
```bash
docker-compose up -d
```

### 4. HDFS Initialization
HDFS structure is initialized automatically via the `hdfs-init` service. Verify:
```bash
docker logs hdfs-init
```

### 5. Ingest ERA5 Data
Place GRIB files (names must be era5_{year}.grib) in the `data/` folder and run for each year:
```bash
py scripts/ingest.py data/era5_2025.grib 2025
```

### 6. Transform and Batch Processing
In Airflow UI (`http://localhost:8081`, admin/admin):
- Unpause `transform_pipeline` DAG
- Trigger `transform_pipeline`
- After completion, trigger `batch_pipeline`

### 7. Export Batch Results to PostgreSQL
```bash
docker cp scripts/export_to_postgres.py spark-master:/tmp/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.3.1 /tmp/export_to_postgres.py
```

### 8. Start Stream Processing
```powershell
.\scripts\submit_stream.ps1
```

### 9. Visualization
Open Grafana at `http://localhost:3000` (admin/admin)

---

## Services and Ports
| Service | URL |
|---------|-----|
| HDFS NameNode UI | http://localhost:9870 |
| Spark Master UI | http://localhost:8080 |
| Airflow UI | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| PostgreSQL | localhost:5433 |

---

## Useful Commands

### HDFS
```bash
# List files
docker exec namenode hdfs dfs -ls /data/raw/era5/

# Zone sizes
docker exec namenode hdfs dfs -du -h /data/

# Exit safe mode
docker exec namenode hdfs dfsadmin -safemode leave
```

### Spark
```bash
# Run batch query
docker cp scripts/batch_q1.py spark-master:/tmp/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /tmp/batch_q1.py

# Run consumer
docker cp scripts/export_to_postgres.py spark-master:/tmp/
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.3.1 /tmp/export_to_postgres.py

### Kafka
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Preview messages
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic weather-stream --from-beginning --max-messages 5
```

### PostgreSQL
```bash
# List tables
docker exec postgres psql -U paragliding -d paragliding -c "\dt"

# Row counts for stream tables
docker exec postgres psql -U paragliding -d paragliding -c "SELECT COUNT(*) FROM weather_stream;"

# Clear stream data
docker exec postgres psql -U paragliding -d paragliding -c "TRUNCATE TABLE weather_stream; TRUNCATE TABLE wind_trend; TRUNCATE TABLE weather_alerts; TRUNCATE TABLE historical_comparison; TRUNCATE TABLE location_comparison;"
```

### Airflow
```bash
# Unpause DAGs
docker exec airflow airflow dags unpause batch_pipeline
docker exec airflow airflow dags unpause transform_pipeline

# Trigger DAG
docker exec airflow airflow dags trigger batch_pipeline

# Check status
docker exec airflow airflow dags list-runs -d batch_pipeline
```

### Reset System
```powershell
.\scripts\cleanup.ps1
```

---

## Adding a New Year of Data
1. Download ERA5 GRIB file from https://cds.climate.copernicus.eu
2. Place in `data/` folder as `era5_XXXX.grib`
3. Run ingest: `py scripts/ingest.py data/era5_XXXX.grib XXXX`
4. Trigger `transform_pipeline` in Airflow
5. Trigger `batch_pipeline` in Airflow
6. Run export to PostgreSQL