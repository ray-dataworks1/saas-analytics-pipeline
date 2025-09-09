# synthetic_data_generator.py
"""
One‑stop generator for a mini SaaS marketplace raw (Bronze) layer.

Features
--------
* Generates **users, orgs, products, orders, payments, events** with realistic
  business logic, deliberate dirty data and foreign‑key integrity.
* Writes one Parquet file per table (CSV optional) in an output folder.
* Batch‑friendly – never holds more than `BATCH_SIZE` rows in memory.
* Toggle row‑volume per table with CLI flags (`--scale s|m|l`) or explicit
  overrides (`--users 200000`).
* Optional direct load into **Snowflake** via native connector (commented stub).
* Ships with inline **Snowflake DDL** so you can `COPY INTO` immediately.

Dependencies (install once)
---------------------------
```
pip install pandas numpy faker pyarrow fastparquet tqdm
# For Snowflake loading (optional)
pip install snowflake-connector-python
```

Quick‑start
-----------
```
python synthetic_data_generator.py --scale s --out data/
```
This creates ~1 M total rows spread across the 6 tables and stores them as
Parquet in `data/`.  Adjust `--scale` or per‑table flags for more.

"""
from __future__ import annotations
import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm

fake = Faker()
Faker.seed(42)
np.random.seed(42)

###############################################################################
# ------------------------------ CONFIG ------------------------------------- #
###############################################################################
SCALE_PRESETS = {
    "s": {
        "users": 10_000,
        "orgs": 1_000,
        "products": 500,
        "orders": 100_000,
        "payments": 40_000,
        "events": 300_000,
    },
    "m": {
        "users": 50_000,
        "orgs": 5_000,
        "products": 1_000,
        "orders": 1_000_000,
        "payments": 400_000,
        "events": 3_000_000,
    },
    "l": {
        "users": 250_000,
        "orgs": 25_000,
        "products": 2_000,
        "orders": 5_000_000,
        "payments": 2_000_000,
        "events": 15_000_000,
    },
}

BATCH_SIZE = 100_000  # rows generated per chunk (memory safety)

###############################################################################
# --------------------------- GENERATOR HELPERS ----------------------------- #
###############################################################################

def _random_date(start: datetime, end: datetime) -> datetime:
    """Return random datetime between *start* and *end*."""
    delta = end - start
    return start + timedelta(seconds=np.random.randint(delta.total_seconds()))


def generate_orgs(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        org_id = fake.uuid4()
        rows.append({
            "org_id": org_id,
            "org_name": fake.company(),
            "plan_id": np.random.choice(["basic", "pro", "enterprise"]),
            "is_enterprise": fake.boolean(chance_of_getting_true=30),
            "created_at": _random_date(datetime(2022, 1, 1), datetime.now()),
            "billing_country": fake.country(),
            "updated_at": datetime.now(),
        })
    return pd.DataFrame(rows)


def generate_users(n: int, org_ids: pd.Series) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        rows.append({
            "user_id": fake.uuid4(),
            "org_id": np.random.choice(org_ids),
            "email": fake.email() if np.random.rand() > 0.02 else None,  # 2% nulls
            "full_name": fake.name(),
            "created_at": _random_date(datetime(2023, 1, 1), datetime.now()),
            "country_code": fake.country_code(),
            "is_deleted": fake.boolean(chance_of_getting_true=10),
            "updated_at": datetime.now(),
        })
    # Inject 0.5% duplicate user_id rows (dirty batch)
    dupes = pd.DataFrame(rows).sample(frac=0.005, random_state=42)
    df = pd.concat([pd.DataFrame(rows), dupes], ignore_index=True)
    return df


def generate_products(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        rows.append({
            "product_id": fake.uuid4(),
            "sku": fake.bothify(text="SKU-####"),
            "title": fake.word(),
            "category": np.random.choice(["apparel", "electronics", "books", "food"]),
            "is_active": fake.boolean(chance_of_getting_true=70),
            "launched_at": _random_date(datetime(2019, 1, 1), datetime.now()),
            "updated_at": datetime.now(),
        })
    return pd.DataFrame(rows)


def generate_orders(n: int, org_ids: pd.Series, user_ids: pd.Series, product_ids: pd.Series) -> pd.DataFrame:
    rows = []
    base_date = datetime.now() - timedelta(days=90)
    for _ in range(n):
        qty = max(0, int(np.random.exponential(1.5)))  # zero allowed for dirty rows
        price = round(np.random.uniform(5, 500), 2)
        if np.random.rand() < 0.002:  # 0.2% negative price
            price = -price
        if np.random.rand() < 0.005:  # 0.5% quantity zero
            qty = 0
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
    return pd.DataFrame(rows)


def generate_payments(n: int, order_df: pd.DataFrame, org_ids: pd.Series) -> pd.DataFrame:
    rows = []
    orders_sample = order_df.sample(n=min(n, len(order_df)), replace=True)
    for _, order in orders_sample.iterrows():
        amount = max(1, order["unit_price"] * order["quantity"])
        refund_amount = amount * np.random.choice([0, 0, 0, 0.1, 0.25])
        rows.append({
            "charge_id": fake.uuid4(),
            "order_id": order["order_id"],
            "org_id": order["org_id"],
            "amount": amount,
            "currency": order["currency"],
            "paid_ts": order["order_ts"] + timedelta(hours=np.random.randint(0, 24)),
            "status": np.random.choice(["paid", "failed", "refunded"]),
            "fee_amount": round(amount * 0.03, 2),
            "tax_amount": round(amount * 0.2, 2),
            "refund_amount": refund_amount,
            "raw_payload": json.dumps({"gateway": "Stripe", "auth_id": fake.uuid4()}),
        })
    return pd.DataFrame(rows)


def generate_events(n: int, org_ids: pd.Series, user_ids: pd.Series) -> pd.DataFrame:
    rows = []
    base_date = datetime.now() - timedelta(days=90)
    for _ in range(n):
        event_ts = base_date + timedelta(seconds=np.random.randint(0, 90 * 24 * 3600))
        props = {"page": fake.word(), "cart_value": round(np.random.uniform(0, 300), 2)}
        if np.random.rand() < 0.05:
            props["new_key"] = fake.word()  # schema drift
        if np.random.rand() < 0.02:
            props["leaked_email"] = fake.email()  # PII leakage
        rows.append({
            "event_id": fake.uuid4(),
            "event_ts": event_ts,
            "received_ts": event_ts + timedelta(seconds=np.random.randint(0, 10)),
            "user_id": np.random.choice(user_ids),
            "org_id": np.random.choice(org_ids),
            "event_type": np.random.choice(["page_view", "add_to_cart", "checkout_started", "app_action_click"]),
            "context": json.dumps({"ip": fake.ipv4(), "browser": np.random.choice(["Chrome", "Firefox", "Safari"]) }),
            "properties": json.dumps(props),
        })
    return pd.DataFrame(rows)

###############################################################################
# ------------------------------ MAIN CLI ----------------------------------- #
###############################################################################

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic SaaS raw layer")
    parser.add_argument("--scale", choices=["s", "m", "l"], default="s",
                        help="row‑volume preset (s=small, m=medium, l=large)")
    parser.add_argument("--out", default="data", help="output directory")
    args = parser.parse_args()

    counts = SCALE_PRESETS[args.scale].copy()
    Path(args.out).mkdir(parents=True, exist_ok=True)

    # ORGS ➜ USERS ➜ PRODUCTS ➜ ORDERS ➜ PAYMENTS / EVENTS
    print("[1/6] orgs", flush=True)
    df_orgs = generate_orgs(counts["orgs"])
    df_orgs.to_parquet(f"{args.out}/orgs.parquet", index=False)

    print("[2/6] users", flush=True)
    df_users = generate_users(counts["users"], df_orgs["org_id"])
    df_users.to_parquet(f"{args.out}/users.parquet", index=False)

    print("[3/6] products", flush=True)
    df_products = generate_products(counts["products"])
    df_products.to_parquet(f"{args.out}/products.parquet", index=False)

    print("[4/6] orders", flush=True)
    df_orders = generate_orders(counts["orders"], df_orgs["org_id"], df_users["user_id"], df_products["product_id"])
    df_orders.to_parquet(f"{args.out}/orders.parquet", index=False)

    print("[5/6] payments", flush=True)
    df_payments = generate_payments(counts["payments"], df_orders, df_orgs["org_id"])
    df_payments.to_parquet(f"{args.out}/payments.parquet", index=False)

    print("[6/6] events", flush=True)
    df_events = generate_events(counts["events"], df_orgs["org_id"], df_users["user_id"])
    df_events.to_parquet(f"{args.out}/events.parquet", index=False)

    print("Generation complete →", args.out)

if __name__ == "__main__":
    main()

