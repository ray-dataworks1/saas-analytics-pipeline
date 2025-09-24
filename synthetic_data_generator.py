"""
synthetic_data_generator.py   TIMESTAMP- & DECIMAL-safe version
----------------------------------------------------------------
Generates realistic SaaS raw-layer tables (users, orgs, products, orders,
payments, events) with foreign-key integrity, TIMESTAMP partitions,
and DECIMAL(38,2) money columns to load cleanly into BigQuery NUMERICs.

Run:
    python synthetic_data_generator.py --scale s --out data/
"""
from __future__ import annotations
import argparse, json, os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from tqdm import tqdm
import random
from datetime import datetime, timedelta


# -----------------------------------------------------------------------------
# GLOBAL CONSTANTS
# -----------------------------------------------------------------------------
DECIMAL_38_2 = pa.decimal128(38, 2)  # BigQuery-compatible NUMERIC
PARQUET_OPTS = dict(
    coerce_timestamps="us",           # ns → µs (BigQuery requirement)
    allow_truncated_timestamps=True,
)

fake = Faker()
Faker.seed(42)
np.random.seed(42)

SCALE_PRESETS = {   

    "xs": {"users": 100, "orgs": 20, "products": 10, "orders": 500, "payments": 200, "events": 1000},
    "s": {"users": 10_000, "orgs": 1_000, "products": 500,
          "orders": 100_000, "payments": 40_000, "events": 300_000},
    "m": {"users": 50_000, "orgs": 5_000, "products": 1_000,
          "orders": 1_000_000, "payments": 400_000, "events": 3_000_000},
    "l": {"users": 250_000, "orgs": 25_000, "products": 2_000,
          "orders": 5_000_000, "payments": 2_000_000, "events": 15_000_000},
}

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def _random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=np.random.randint(delta.total_seconds()))

def coerce_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_datetime(df[c], utc=True)
    return df

def quantize(val: float | Decimal) -> Decimal:
    """Return Decimal rounded to 2 dp with HALF_UP (banker-safe)."""
    return Decimal(val).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def write_decimal_parquet(df: pd.DataFrame, path: str,
                          decimal_cols: list[str]) -> None:
    """
    Write *df* to Parquet, forcing *decimal_cols* to DECIMAL(38,2).
    """
    inferred = pa.Schema.from_pandas(df, preserve_index=False)
    fields = [
        pa.field(f.name, DECIMAL_38_2) if f.name in decimal_cols else f
        for f in inferred
    ]
    table = pa.Table.from_pandas(df, schema=pa.schema(fields),
                                 preserve_index=False)
    pq.write_table(table, path, **PARQUET_OPTS)


def _recent_date(days_back: int = 30) -> datetime:
    days = random.randint(0, days_back)
    dt = datetime.utcnow() - timedelta(days=days)
    # Clamp to not exceed current UTC time
    if dt > datetime.utcnow():
        dt = datetime.utcnow()
    return dt


# -----------------------------------------------------------------------------
# GENERATORS
# -----------------------------------------------------------------------------
def generate_orgs(n: int) -> pd.DataFrame:
    rows = [{
        "org_id": fake.uuid4(),
        "org_name": fake.company(),
        "plan_id": np.random.choice(["basic", "pro", "enterprise"]),
        "is_enterprise": fake.boolean(30),
        "created_at": _recent_date(365),
        "billing_country": fake.country(),
        "updated_at": datetime.now(),
    } for _ in range(n)]
    return coerce_datetime(pd.DataFrame(rows), ["created_at", "updated_at"])

def generate_users(n: int, org_ids: pd.Series) -> pd.DataFrame:
    rows = [{
        "user_id": fake.uuid4(),
        "org_id": np.random.choice(org_ids),
        "email": fake.email() if np.random.rand() > 0.02 else None,
        "full_name": fake.name(),
        "created_at": _recent_date(180),
        "country_code": fake.country_code(),
        "is_deleted": fake.boolean(10),
        "updated_at": datetime.now(),
    } for _ in range(n)]
    dupes = pd.DataFrame(rows).sample(frac=0.005, random_state=42)
    df = pd.concat([pd.DataFrame(rows), dupes], ignore_index=True)
    return coerce_datetime(df, ["created_at", "updated_at"])

def generate_products(n: int) -> pd.DataFrame:
    rows = [{
        "product_id": fake.uuid4(),
        "sku": fake.bothify("SKU-####"),
        "title": fake.word(),
        "category": np.random.choice(["apparel", "electronics", "books", "food"]),
        "is_active": fake.boolean(70),
        "launched_at":_recent_date(730),
        "updated_at": datetime.now(),
    } for _ in range(n)]
    return coerce_datetime(pd.DataFrame(rows), ["launched_at", "updated_at"])

def generate_orders(n: int, org_ids, user_ids, product_ids) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=90)
    rows = []
    for _ in range(n):
        qty = max(0, int(np.random.exponential(1.5)))
        price = quantize(np.random.uniform(5, 500))
        if np.random.rand() < 0.002:
            price = -price
        if np.random.rand() < 0.005:
            qty = 0
        order_ts = base_date + timedelta(days=np.random.randint(0, 90))
        rows.append({
            "order_id": fake.uuid4(),
            "org_id": np.random.choice(org_ids),
            "user_id": np.random.choice(user_ids),
            "product_id": np.random.choice(product_ids),
            "quantity": qty,
            "unit_price": price,                      # Decimal
            "currency": np.random.choice(["USD", "GBP", "EUR"]),
            "status": np.random.choice(
                ["placed", "paid", "refunded",
                 "partial_refund", "cancelled"]),
            "order_ts": order_ts,
            "updated_at": order_ts + timedelta(
                hours=np.random.randint(1, 48)),
        })
    df = pd.DataFrame(rows)
    return coerce_datetime(df, ["order_ts", "updated_at"])

def generate_payments(n: int, order_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    orders_sample = order_df.sample(n=min(n, len(order_df)), replace=True)
    for _, order in orders_sample.iterrows():
        amount = quantize(order.unit_price * max(order.quantity, 1))
        
        REFUND_FACTORS = [Decimal("0"), Decimal("0"), Decimal("0.10"), Decimal("0.25")]
        refund_factor  = np.random.choice(REFUND_FACTORS)
        refund_amount  = quantize(amount * refund_factor)

        rows.append({
            "charge_id": fake.uuid4(),
            "order_id": order.order_id,
            "org_id": order.org_id,
            "amount": amount,                        # Decimal
            "currency": order.currency,
            "paid_ts": order.order_ts + timedelta(
                hours=np.random.randint(0, 24)),
            "status": np.random.choice(
                ["paid", "failed", "refunded"]),
            "fee_amount": quantize(amount * Decimal("0.03")),
            "tax_amount": quantize(amount * Decimal("0.20")),
            "refund_amount": refund_amount,
            "raw_payload": json.dumps({
                "gateway": "Stripe", "auth_id": fake.uuid4()}),
        })
    df = pd.DataFrame(rows)
    return coerce_datetime(df, ["paid_ts"])

def generate_events(n: int, org_ids, user_ids) -> pd.DataFrame:
    base_date = datetime.now() - timedelta(days=30)
    rows = []
    for _ in range(n):
        event_ts = base_date + timedelta(
            seconds=np.random.randint(0, 30 * 24 * 3600))
        props = {"page": fake.word(),
                 "cart_value": float(quantize(np.random.uniform(0, 300)))}
        if np.random.rand() < 0.05:
            props["new_key"] = fake.word()
        if np.random.rand() < 0.02:
            props["leaked_email"] = fake.email()
        rows.append({
            "event_id": fake.uuid4(),
            "event_ts": event_ts,
            "received_ts": event_ts + timedelta(
                seconds=np.random.randint(0, 10)),
            "user_id": np.random.choice(user_ids),
            "org_id": np.random.choice(org_ids),
            "event_type": np.random.choice(["page_view", "add_to_cart",
                                            "checkout_started",
                                            "app_action_click"]),
            "context": json.dumps({"ip": fake.ipv4(),
                                   "browser": np.random.choice(
                                       ["Chrome", "Firefox", "Safari"])}),
            "properties": json.dumps(props),
        })
    df = pd.DataFrame(rows)
    return coerce_datetime(df, ["event_ts", "received_ts"])

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic SaaS raw layer "
                    "(TIMESTAMP & DECIMAL-safe)")
    parser.add_argument("--scale", choices=["xs", "s", "m", "l"], default="xs")
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    counts = SCALE_PRESETS[args.scale]
    Path(args.out).mkdir(parents=True, exist_ok=True)

    print("[1/6] orgs");     df_orgs = generate_orgs(counts["orgs"])
    df_orgs.to_parquet(f"{args.out}/orgs.parquet",
                       index=False, **PARQUET_OPTS)

    print("[2/6] users");    df_users = generate_users(
        counts["users"], df_orgs.org_id)
    df_users.to_parquet(f"{args.out}/users.parquet",
                        index=False, **PARQUET_OPTS)

    print("[3/6] products"); df_products = generate_products(
        counts["products"])
    df_products.to_parquet(f"{args.out}/products.parquet",
                           index=False, **PARQUET_OPTS)

    print("[4/6] orders");   df_orders = generate_orders(
        counts["orders"], df_orgs.org_id, df_users.user_id,
        df_products.product_id)
    write_decimal_parquet(df_orders,
                          f"{args.out}/orders.parquet",
                          decimal_cols=["unit_price"])

    print("[5/6] payments"); df_payments = generate_payments(
        counts["payments"], df_orders)
    write_decimal_parquet(df_payments,
                          f"{args.out}/payments.parquet",
                          decimal_cols=["amount", "fee_amount",
                                        "tax_amount", "refund_amount"])

    print("[6/6] events");   df_events = generate_events(
        counts["events"], df_orgs.org_id, df_users.user_id)
    df_events.to_parquet(f"{args.out}/events.parquet",
                         index=False, **PARQUET_OPTS)

    print(f"Generation complete → {args.out}")

if __name__ == "__main__":
    main()

