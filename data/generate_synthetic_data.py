"""
Synthetic data generator for DQ framework testing.

Generates three Delta tables with intentionally dirty data:
    - customers   (null violations, invalid emails, duplicate IDs)
    - orders      (range violations, orphaned foreign keys)
    - products    (regex violations, nulls, out-of-range prices)

Usage (Databricks notebook):
    %run ./data/generate_synthetic_data
    # or call directly:
    generate_all_tables(spark, base_path="dbfs:/dq_framework/data")
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
import random
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date(start_days_ago: int = 365, end_days_ago: int = 0) -> str:
    delta = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=delta)).strftime("%Y-%m-%d %H:%M:%S")


def maybe_null(value, null_probability: float = 0.05):
    """Return None with a given probability, otherwise return the value."""
    return None if random.random() < null_probability else value


# ---------------------------------------------------------------------------
# Customers table
# Intentional issues:
#   - ~5% null customer_id (critical — violates not_null + unique)
#   - ~8% duplicate customer_id (violates uniqueness)
#   - ~10% malformed emails (violates regex)
#   - ~5% null email
# ---------------------------------------------------------------------------

def generate_customers(n: int = 500) -> list[dict]:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.co.uk"]
    bad_emails = ["not-an-email", "missing@", "@nodomain", "double@@email.com", "plaintext"]
    records = []

    for i in range(1, n + 1):
        customer_id = maybe_null(f"CUST-{i:05d}", null_probability=0.05)

        # Inject ~8% duplicate IDs
        if random.random() < 0.08 and i > 10:
            customer_id = f"CUST-{random.randint(1, i - 1):05d}"

        # Email: mostly valid, some bad, some null
        r = random.random()
        if r < 0.05:
            email = None
        elif r < 0.15:
            email = random.choice(bad_emails)
        else:
            name = f"user{i}"
            email = f"{name}@{random.choice(domains)}"

        records.append({
            "customer_id": customer_id,
            "first_name": maybe_null(f"First{i}", null_probability=0.02),
            "last_name": f"Last{i}",
            "email": email,
            "country": random.choice(["UK", "US", "DE", "FR", "AU", None]),
            "created_at": random_date(start_days_ago=730),
        })

    return records


# ---------------------------------------------------------------------------
# Products table
# Intentional issues:
#   - ~5% null product_id
#   - ~8% price out of range (negative or > 10,000)
#   - ~6% null product_name
#   - ~5% invalid SKU format (should match: SKU-[A-Z]{3}-[0-9]{4})
# ---------------------------------------------------------------------------

def generate_products(n: int = 100) -> list[dict]:
    categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
    records = []

    for i in range(1, n + 1):
        product_id = maybe_null(f"PROD-{i:04d}", null_probability=0.05)

        # SKU: mostly valid, some bad
        if random.random() < 0.05:
            sku = random.choice(["BAD_SKU", "sku123", "SKU-12-ABCD", "NOPE"])
        else:
            letters = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=3))
            digits = f"{random.randint(1000, 9999)}"
            sku = f"SKU-{letters}-{digits}"

        # Price: mostly valid, some out of range
        r = random.random()
        if r < 0.04:
            price = round(random.uniform(-50, -0.01), 2)   # negative
        elif r < 0.08:
            price = round(random.uniform(10001, 99999), 2) # too high
        else:
            price = round(random.uniform(0.99, 9999.99), 2)

        records.append({
            "product_id": product_id,
            "product_name": maybe_null(f"Product {i}", null_probability=0.06),
            "sku": sku,
            "price": price,
            "category": random.choice(categories),
            "stock_quantity": random.randint(0, 500),
            "created_at": random_date(start_days_ago=365),
        })

    return records


# ---------------------------------------------------------------------------
# Orders table
# Intentional issues:
#   - ~5% null order_id
#   - ~10% orphaned customer_id (not in customers table)
#   - ~5% orphaned product_id (not in products table)
#   - ~7% order_amount out of range (negative or > 100,000)
#   - ~3% null order_amount
# ---------------------------------------------------------------------------

def generate_orders(n: int = 1000, n_customers: int = 500, n_products: int = 100) -> list[dict]:
    records = []

    for i in range(1, n + 1):
        order_id = maybe_null(f"ORD-{i:06d}", null_probability=0.05)

        # Customer ID: mostly valid, some orphaned
        if random.random() < 0.10:
            customer_id = f"CUST-{random.randint(n_customers + 1, n_customers + 100):05d}"
        else:
            customer_id = f"CUST-{random.randint(1, n_customers):05d}"

        # Product ID: mostly valid, some orphaned
        if random.random() < 0.05:
            product_id = f"PROD-{random.randint(n_products + 1, n_products + 50):04d}"
        else:
            product_id = f"PROD-{random.randint(1, n_products):04d}"

        # Amount: mostly valid, some out of range, some null
        r = random.random()
        if r < 0.03:
            amount = None
        elif r < 0.07:
            amount = round(random.uniform(-500, -0.01), 2)
        elif r < 0.10:
            amount = round(random.uniform(100001, 999999), 2)
        else:
            amount = round(random.uniform(1.00, 99999.99), 2)

        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_amount": amount,
            "quantity": random.randint(1, 20),
            "status": random.choice(["pending", "confirmed", "shipped", "delivered", "cancelled"]),
            "order_date": random_date(start_days_ago=365),
        })

    return records


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_all_tables(spark: SparkSession, base_path: str = "dbfs:/dq_framework/data") -> None:
    """
    Generates and writes all synthetic tables to Delta format.

    Args:
        spark:      Active SparkSession
        base_path:  Root path for Delta table output (DBFS or ADLS)
    """
    random.seed(42)

    print("Generating synthetic data...")

    # --- Customers ---
    customers_data = generate_customers(n=500)
    customers_df = spark.createDataFrame(customers_data)
    (
        customers_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{base_path}/customers")
    )
    print(f"  customers: {customers_df.count()} rows → {base_path}/customers")

    # --- Products ---
    products_data = generate_products(n=100)
    products_df = spark.createDataFrame(products_data)
    (
        products_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{base_path}/products")
    )
    print(f"  products:  {products_df.count()} rows → {base_path}/products")

    # --- Orders ---
    orders_data = generate_orders(n=1000, n_customers=500, n_products=100)
    orders_df = spark.createDataFrame(orders_data)
    (
        orders_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(f"{base_path}/orders")
    )
    print(f"  orders:    {orders_df.count()} rows → {base_path}/orders")

    print("\nDone. Summary of intentional issues injected:")
    print("  customers  — null IDs, duplicate IDs, malformed emails")
    print("  products   — null IDs, invalid SKUs, out-of-range prices")
    print("  orders     — null IDs, orphaned FK references, out-of-range amounts")


# ---------------------------------------------------------------------------
# Databricks notebook entry point
# Run this cell in a notebook to generate all tables:
#
#   from data.generate_synthetic_data import generate_all_tables
#   generate_all_tables(spark)
#
# Or if running as a standalone script with a local SparkSession:
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = SparkSession.builder.appName("dq_framework_datagen").getOrCreate()
    generate_all_tables(spark)
