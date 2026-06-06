# -*- coding: utf-8 -*-
import xarray as xr
import numpy as np
import pandas as pd
import subprocess
import os
import sys

def grib_to_parquet(input_file, output_file, year):
    print(f"Loading {input_file}...")
    ds = xr.open_dataset(input_file, engine="cfgrib")
    
    df = ds[['t2m', 'cape', 'blh', 'u10', 'v10', 'd2m', 'sp', 'tcc']] \
           .to_dataframe().reset_index()

    df = df.drop(columns=['number', 'step', 'surface',
                           'expver', 'valid_time'], errors='ignore')

    df.to_parquet(output_file, index=False)
    print(f"Saved: {output_file} ({os.path.getsize(output_file) / 1024**2:.1f} MB)")
    return output_file

def upload_to_hdfs(local_file, year):
    hdfs_path = f"/data/raw/era5/year={year}/"
    
    subprocess.run(["docker", "exec", "namenode",
                    "hdfs", "dfs", "-mkdir", "-p", hdfs_path])
    
    subprocess.run(["docker", "cp", local_file,
                    f"namenode:/tmp/era5_{year}.parquet"])
    
    subprocess.run(["docker", "exec", "namenode",
                    "hdfs", "dfs", "-put", "-f",
                    f"/tmp/era5_{year}.parquet", hdfs_path])
    
    print(f"Uploaded to HDFS: {hdfs_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: py ingest.py <grib_file> <year>")
        sys.exit(1)
    
    grib_file = sys.argv[1]
    year = int(sys.argv[2])
    parquet_file = f"era5_{year}_raw.parquet"
    
    grib_to_parquet(grib_file, parquet_file, year)
    upload_to_hdfs(parquet_file, year)
    print(f"Done! Year {year} ingested.")