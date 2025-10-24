"""
generate_and_load.py  –  v2 portfolio-friendly data generator
--------------------------------------------------------------
Lightweight generator for synthetic SaaS data with optional BigQuery load.

Generates CSVs for six entities:
   orgs, users, products, orders, payments, events

Maintains realistic foreign keys & timestamps
Replaces Parquet + Decimal complexity with simple CSV output
Optional --load flag loads directly to BigQuery using autodetect schema

Run examples:
    python data_gen/generate_and_load.py --scale xs
    python data_gen/generate_and_load.py --scale xs --load

Notes:
 - BigQuery NUMERIC/DECIMALs handled via CAST() in dbt models.
 - For clarity, audit tables & schema DDL were removed (handled by dbt + CI).
 - Scoped for AE-specifc work
"""

from __future__ import annotations
import argparse, json, random
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import numpy as np
import pandas as pd
from faker import Faker


# optional BigQuery
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
fake = Faker()
Faker.seed(42)
np.random.seed(42)

SCALE_PRESETS = {
    "xs": {"users": 100, "orgs": 20, "products": 10, "orders": 500, "payments": 200, "events": 1000},
    "s":  {"users": 10_000, "orgs": 1_000, "products": 500, "orders": 100_000, "payments": 40_000, "events": 300_000},
}

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def quantize(val: float | Decimal) -> Decimal:
    return Decimal(val).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def coerce_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_datetime(df[c], utc=True)
    return df

# ---------------------------------------------------------------------
# GENERATORS
# ---------------------------------------------------------------------
def generate_orgs(n: int) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=60)
    rows = [{
        "org_id": fake.uuid4(),
        "org_name": fake.company(),
        "plan_id": np.random.choice(["basic", "pro", "enterprise"]),
        "is_enterprise": fake.boolean(30),
        "created_at": base_date + timedelta(seconds=np.random.randint(0, 90 * 24 * 3600)),
        "billing_country": fake.country(),
        "updated_at": datetime.now(),
    } for _ in range(n)]
    return coerce_datetime(pd.DataFrame(rows), ["created_at", "updated_at"])

def generate_users(n: int, org_ids: pd.Series) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=60)
    rows = [{
        "user_id": fake.uuid4(),
        "org_id": np.random.choice(org_ids),
        "email": fake.email() if np.random.rand() > 0.02 else None,
        "full_name": fake.name(),
        "created_at": base_date + timedelta(seconds=np.random.randint(0, 60 * 24 * 3600)),
        "country_code": fake.country_code(),
        "is_deleted": fake.boolean(10),
        "updated_at": datetime.now(),
    } for _ in range(n)]
    return coerce_datetime(pd.DataFrame(rows), ["created_at", "updated_at"])

def generate_products(n: int) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=60)
    rows = [{
        "product_id": fake.uuid4(),
        "sku": fake.bothify("SKU-####"),
        "title": fake.word(),
        "category": np.random.choice(["apparel", "electronics", "books", "food"]),
        "is_active": fake.boolean(70),
        "launched_at": base_date + timedelta(seconds=np.random.randint(0, 365 * 24 * 3600)),
        "updated_at": datetime.now(),
    } for _ in range(n)]
    return coerce_datetime(pd.DataFrame(rows), ["launched_at", "updated_at"])

def generate_orders(n: int, org_ids, user_ids, product_ids) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=59)
    rows = []
    for _ in range(n):
        qty = max(0, int(np.random.exponential(1.5)))
        price = quantize(np.random.uniform(5, 500))
        if np.random.rand() < 0.002: price = -price
        if np.random.rand() < 0.005: qty = 0
        order_ts = base_date + timedelta(days=np.random.randint(0, 90))
        rows.append({
            "order_id": fake.uuid4(),
            "org_id": np.random.choice(org_ids),
            "user_id": np.random.choice(user_ids),
            "product_id": np.random.choice(product_ids),
            "quantity": qty,
            "unit_price": price,
            "currency": np.random.choice(["USD", "GBP", "EUR"]),
            "status": np.random.choice(["placed", "paid", "refunded", "partial_refund", "cancelled"]),
            "order_ts": order_ts,
            "updated_at": order_ts + timedelta(hours=np.random.randint(1, 48)),
        })
    return coerce_datetime(pd.DataFrame(rows), ["order_ts", "updated_at"])

def generate_payments(n: int, order_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    sample = order_df.sample(n=min(n, len(order_df)), replace=True)
    for _, order in sample.iterrows():
        amount = quantize(order.unit_price * max(order.quantity, 1))
        refund_factor = np.random.choice([0, 0, 0.1, 0.25])
        refund_amount = quantize(amount * Decimal(refund_factor))
        rows.append({
            "charge_id": fake.uuid4(),
            "order_id": order.order_id,
            "org_id": order.org_id,
            "amount": amount,
            "currency": order.currency,
            "paid_ts": order.order_ts + timedelta(seconds=np.random.randint(0, 86400)),
            "status": np.random.choice(["paid", "failed", "refunded"]),
            "fee_amount": quantize(amount * Decimal("0.03")),
            "tax_amount": quantize(amount * Decimal("0.20")),
            "refund_amount": refund_amount,
            "raw_payload": json.dumps({"gateway": "Stripe", "auth_id": fake.uuid4()}),
        })
    return coerce_datetime(pd.DataFrame(rows), ["paid_ts"])

def generate_events(n: int, org_ids, user_ids) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=30)
    rows = []
    for _ in range(n):
        event_ts = base_date + timedelta(seconds=np.random.randint(0, 30 * 24 * 3600))
        props = {"page": fake.word(), "cart_value": float(quantize(np.random.uniform(0, 300)))}
        if np.random.rand() < 0.05: props["new_key"] = fake.word()
        if np.random.rand() < 0.02: props["leaked_email"] = fake.email()
        rows.append({
            "event_id": fake.uuid4(),
            "event_ts": event_ts,
            "received_ts": event_ts + timedelta(seconds=np.random.randint(0, 10)),
            "user_id": np.random.choice(user_ids),
            "org_id": np.random.choice(org_ids),
            "event_type": np.random.choice(["page_view","add_to_cart","checkout_started","app_action_click"]),
            "context": json.dumps({"ip": fake.ipv4(),"browser": np.random.choice(["Chrome","Firefox","Safari"])}),
            "properties": json.dumps(props),
        })
    return coerce_datetime(pd.DataFrame(rows), ["event_ts", "received_ts"])

# ---------------------------------------------------------------------
# OPTIONAL: BigQuery loader
# ---------------------------------------------------------------------
def load_csv_to_bq(project, dataset, folder="raw"):
    if not bigquery:
        print(" google-cloud-bigquery not installed; skipping load.")
        return
    client = bigquery.Client(project=project)
    for csv_path in Path(folder).glob("*.csv"):
        table = csv_path.stem
        print(f"Loading {table}...")
        job = client.load_table_from_file(
            open(csv_path, "rb"),
            f"{project}.{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
                skip_leading_rows=1,
            ),
        )
        job.result()
        print(f"{table}: {job.output_rows:,} rows loaded.")

# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", choices=SCALE_PRESETS.keys(), default="xs")
    parser.add_argument("--out", default="raw")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--project", default="saas-analytics-pipeline")
    parser.add_argument("--dataset", default="raw")
    args = parser.parse_args()

    counts = SCALE_PRESETS[args.scale]
    Path(args.out).mkdir(parents=True, exist_ok=True)

    print(f"\nGenerating synthetic data at scale={args.scale}\n")
    orgs = generate_orgs(counts["orgs"]); orgs.to_csv(f"{args.out}/orgs.csv", index=False)
    users = generate_users(counts["users"], orgs.org_id); users.to_csv(f"{args.out}/users.csv", index=False)
    products = generate_products(counts["products"]); products.to_csv(f"{args.out}/products.csv", index=False)
    orders = generate_orders(counts["orders"], orgs.org_id, users.user_id, products.product_id)
    orders.to_csv(f"{args.out}/orders.csv", index=False)
    payments = generate_payments(counts["payments"], orders); payments.to_csv(f"{args.out}/payments.csv", index=False)
    events = generate_events(counts["events"], orgs.org_id, users.user_id); events.to_csv(f"{args.out}/events.csv", index=False)

    print(f"\n Generation complete → {args.out}\n")

    if args.load:
        load_csv_to_bq(args.project, args.dataset, args.out)
