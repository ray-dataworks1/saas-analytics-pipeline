# load_to_bigquery.py
import glob, os, pathlib
from google.cloud import bigquery

PROJECT   = "saas-analytics-pipeline"    
DATASET   = "raw"              # landing dataset
CLIENT    = bigquery.Client(project=PROJECT)

def load_folder(folder: str):
    for file in glob.glob(f"{folder}/*.parquet"):
        table_id = pathlib.Path(file).stem                 # users.parquet → users
        full_id  = f"{PROJECT}.{DATASET}.{table_id}"
        job_cfg  = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",   # or WRITE_APPEND
        )
        with open(file, "rb") as f:
            job = CLIENT.load_table_from_file(f, full_id, job_config=job_cfg)
        job.result()
        print(f"loaded {file} → {full_id}")

if __name__ == "__main__":
    load_folder("data")
