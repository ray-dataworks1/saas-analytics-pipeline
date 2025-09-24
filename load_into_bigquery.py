# load_into_bigquery.py  (bronze layer loader with row-count audit)
import os, pathlib, glob, json, datetime
from google.cloud import bigquery

PROJECT  = "saas-analytics-pipeline"
DATASET  = "raw"          # landing dataset
AUDIT_T  = "raw__audit"   # (schema: table_name STRING, load_rows INT64,
                          #          bq_rows INT64, load_ts TIMESTAMP)
CLIENT   = bigquery.Client(project=PROJECT)

# ---------- static schema dict  ----------
from google.cloud import bigquery   # keep import local for autocompletion
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

# ---------- helpers ----------
def get_row_count(table: str) -> int:
    """Return COUNT(*) for a fully-qualified table."""
    sql = f"SELECT COUNT(*) AS c FROM `{PROJECT}.{DATASET}.{table}`"
    return list(CLIENT.query(sql))[0].c

# def log_audit(table: str, load_rows: int, bq_rows: int) -> None:
#     """Create the audit table if needed, then append one row using DML (no streaming)."""
#     audit_fqn = f"`{PROJECT}.{DATASET}.{AUDIT_T}`"   # full path, 3 parts
    
#     # Ensure table exists
#     CLIENT.query(f"""
#         CREATE TABLE IF NOT EXISTS {audit_fqn} (
#             table_name STRING,
#             load_rows  INT64,
#             bq_rows    INT64,
#             load_ts    TIMESTAMP
#         )
#     """).result()

#     # Append one row
#     CLIENT.query(
#         f"""
#         INSERT INTO {audit_fqn} (table_name, load_rows, bq_rows, load_ts)
#         VALUES (@t, @lr, @br, CURRENT_TIMESTAMP())
#         """,
#         job_config=bigquery.QueryJobConfig(
#             query_parameters=[
#                 bigquery.ScalarQueryParameter("t",  "STRING", table),
#                 bigquery.ScalarQueryParameter("lr", "INT64",  load_rows),
#                 bigquery.ScalarQueryParameter("br", "INT64",  bq_rows),
#             ]
#         ),
#     ).result()

def load_table(table_name: str, file_path: str):
    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.PARQUET,
        write_disposition = "WRITE_TRUNCATE",
        schema = SCHEMAS[table_name],
    )
    with open(file_path, "rb") as f:
        load_job = CLIENT.load_table_from_file(
            f,
            f"{PROJECT}.{DATASET}.{table_name}",
            job_config = job_config,
        )
    load_job.result()                       # blocks until done

    load_rows = load_job.output_rows        # what the API thinks it loaded
    bq_rows   = get_row_count(table_name)   # what’s actually present
    status    = "✅" if load_rows == bq_rows else f"❌ ({bq_rows-load_rows:+,} rows)"

    # Console log
    print(f"{table_name} → loader={load_rows:,} | bq={bq_rows:,} {status}")

    # Persist to audit table
    # log_audit(table_name, load_rows, bq_rows)

# ---------- main ----------
if __name__ == "__main__":
    for tbl in ["orgs", "users", "products", "orders", "payments", "events"]:
        load_table(tbl, f"data/{tbl}.parquet")
