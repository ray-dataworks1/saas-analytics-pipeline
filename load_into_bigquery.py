# load_to_bigquery.py
import glob, os, pathlib
from google.cloud import bigquery

PROJECT   = "saas-analytics-pipeline"    
DATASET   = "raw"              # landing dataset
CLIENT    = bigquery.Client(project=PROJECT)

from google.cloud import bigquery

SCHEMAS = {
    "orgs": [
        bigquery.SchemaField("org_id", "STRING"),
        bigquery.SchemaField("org_name", "STRING"),
        bigquery.SchemaField("plan_id", "STRING"),
        bigquery.SchemaField("is_enterprise", "BOOL"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("billing_country", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ],
    "users": [
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("org_id", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("is_deleted", "BOOL"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ],
    "products": [
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("is_active", "BOOL"),
        bigquery.SchemaField("launched_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ],
    "orders": [
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("org_id", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("quantity", "INT64"),
        bigquery.SchemaField("unit_price", "NUMERIC"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("order_ts", "TIMESTAMP"),   # partition
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ],
    "payments": [
        bigquery.SchemaField("charge_id", "STRING"),
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("org_id", "STRING"),
        bigquery.SchemaField("amount", "NUMERIC"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("paid_ts", "TIMESTAMP"),    # partition
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("fee_amount", "NUMERIC"),
        bigquery.SchemaField("tax_amount", "NUMERIC"),
        bigquery.SchemaField("refund_amount", "NUMERIC"),
        bigquery.SchemaField("raw_payload", "STRING"), # change back to JSON in staging
    ],
    "events": [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("event_ts", "TIMESTAMP"),   # partition
        bigquery.SchemaField("received_ts", "TIMESTAMP"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("org_id", "STRING"),
        bigquery.SchemaField("event_type", "STRING"),
        bigquery.SchemaField("context", "STRING"), # change back to JSON type in staging
        bigquery.SchemaField("properties", "STRING"), # change back to JSON type in staging
    ],
}



def load_table(table_name, file_path):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
        schema=SCHEMAS[table_name],
    )

    with open(file_path, "rb") as f:
        load_job = CLIENT.load_table_from_file(
            f,
            f"{PROJECT}.{DATASET}.{table_name}",
            job_config=job_config,
        )
    load_job.result()
    print(f"Loaded {table_name} → {load_job.output_rows} rows")

for tbl in ["orgs", "users", "products", "orders", "payments", "events"]:
    load_table(tbl, f"data/{tbl}.parquet")
