# create_raw_bq_modular.py
import os, time
from google.cloud import bigquery

PROJECT = os.getenv("BQ_PROJECT", "saas-analytics-pipeline")
DATASET = os.getenv("BQ_DATASET", "raw")
LOCATION = "US"

client = bigquery.Client(project=PROJECT, location=LOCATION)

header = f"CREATE SCHEMA IF NOT EXISTS `{PROJECT}.{DATASET}` OPTIONS(location='{LOCATION}');"

TABLE_DDL = {
    "orgs": f"""
DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.orgs`;
CREATE TABLE `{PROJECT}.{DATASET}.orgs`
(
  org_id STRING,
  org_name STRING,
  plan_id STRING,
  is_enterprise BOOL,
  created_at TIMESTAMP,
  billing_country STRING,
  updated_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY org_id;
""",
    "users": f"""
DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.users`;
CREATE TABLE `{PROJECT}.{DATASET}.users`
(
  user_id STRING,
  org_id STRING,
  email STRING,
  full_name STRING,
  created_at TIMESTAMP,
  country_code STRING,
  is_deleted BOOL,
  updated_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY org_id, user_id;
""",
    "products": f"""
DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.products`;
CREATE TABLE `{PROJECT}.{DATASET}.products`
(
  product_id STRING,
  sku STRING,
  title STRING,
  category STRING,
  is_active BOOL,
  launched_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY DATE(launched_at)
CLUSTER BY category;
""",
    "orders": f"""
DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.orders`;
CREATE TABLE `{PROJECT}.{DATASET}.orders`
(
  order_id STRING,
  org_id STRING,
  user_id STRING,
  product_id STRING,
  quantity INT64,
  unit_price NUMERIC,
  currency STRING,
  status STRING,
  order_ts TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY DATE(order_ts)
CLUSTER BY org_id, user_id;
""",
    "payments": f"""
DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.payments`;
CREATE TABLE `{PROJECT}.{DATASET}.payments`
(
  charge_id STRING,
  order_id STRING,
  org_id STRING,
  amount NUMERIC,
  currency STRING,
  paid_ts TIMESTAMP,
  status STRING,
  fee_amount NUMERIC,
  tax_amount NUMERIC,
  refund_amount NUMERIC,
  raw_payload STRING
)
PARTITION BY DATE(paid_ts)
CLUSTER BY org_id, order_id;
""",
    "events": f"""
DROP TABLE IF EXISTS `{PROJECT}.{DATASET}.events`;
CREATE TABLE `{PROJECT}.{DATASET}.events`
(
  event_id STRING,
  event_ts TIMESTAMP,
  received_ts TIMESTAMP,
  user_id STRING,
  org_id STRING,
  event_type STRING,
  context STRING,
  properties STRING 
)
PARTITION BY DATE(event_ts)
CLUSTER BY org_id, event_type;
""",
}

def main():
    # one header job to ensure dataset exists
    client.query(header).result()
    for name, ddl in TABLE_DDL.items():
        print(f"creating {name}")
        client.query(ddl).result()
        time.sleep(3)  # tiny pause avoids burst-rate quota
    print("all tables rebuilt with partitions + clustering")

if __name__ == "__main__":
    main()


