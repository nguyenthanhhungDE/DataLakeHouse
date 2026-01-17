from dagster import asset, Output
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import polars as pl
import uuid  # <--- QUAN TRỌNG: Để sinh ID ngẫu nhiên

COMPUTE_KIND = "Python"
LAYER = "bronze"


@asset(
    description="Generate fake data for customers, products, orders, and order_items and insert to MySQL",
    required_resource_keys={"mysql_io_manager"},
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def generate_mysql_data(context) -> Output[None]:
    # --- 1️⃣ Generate customers ---
    # Dùng UUID để mỗi lần chạy sinh ra khách hàng mới
    num_customers = 10
    customers_pd = pd.DataFrame(
        {
            "customer_id": [
                f"cus_{uuid.uuid4().hex[:8]}" for _ in range(num_customers)
            ],
            "customer_unique_id": [
                f"uniq_{uuid.uuid4().hex[:8]}" for _ in range(num_customers)
            ],
            "customer_zip_code_prefix": np.random.randint(
                10000, 99999, size=num_customers
            ),
            "customer_city": [f"City_{i}" for i in range(1, num_customers + 1)],
            "customer_state": [f"ST_{i%50}" for i in range(1, num_customers + 1)],
        }
    )

    # --- 2️⃣ Generate products ---
    num_products = 8
    categories = ["electronics", "fashion", "toys", "furniture", "books", "beauty"]
    products_pd = pd.DataFrame(
        {
            "product_id": [f"prod_{uuid.uuid4().hex[:8]}" for _ in range(num_products)],
            "product_category_name": [
                random.choice(categories) for _ in range(num_products)
            ],
            "product_name_length": np.random.randint(5, 30, size=num_products),
            "product_description_length": np.random.randint(20, 400, size=num_products),
            "product_photos_qty": np.random.randint(0, 5, size=num_products),
            "product_weight_g": np.random.randint(100, 5000, size=num_products),
            "product_length_cm": np.round(
                np.random.uniform(5, 200, size=num_products), 2
            ),
            "product_height_cm": np.round(
                np.random.uniform(1, 100, size=num_products), 2
            ),
            "product_width_cm": np.round(
                np.random.uniform(1, 100, size=num_products), 2
            ),
        }
    )

    # --- 3️⃣ Generate orders (FK → customers) ---
    num_orders = 15
    orders_pd = pd.DataFrame(
        {
            "order_id": [str(uuid.uuid4()) for _ in range(num_orders)],
            "customer_id": np.random.choice(customers_pd["customer_id"], num_orders),
            "order_status": [
                random.choice(["delivered", "shipped", "processing", "canceled"])
                for _ in range(num_orders)
            ],
            "order_purchase_timestamp": [
                (datetime.now() - timedelta(days=random.randint(0, 365))).date()
                for _ in range(num_orders)
            ],
            "order_approved_at": [
                (datetime.now() - timedelta(days=random.randint(0, 365))).date()
                for _ in range(num_orders)
            ],
            "order_delivered_carrier_date": [
                (datetime.now() - timedelta(days=random.randint(0, 365))).date()
                for _ in range(num_orders)
            ],
            "order_delivered_customer_date": [
                (datetime.now() - timedelta(days=random.randint(0, 365))).date()
                for _ in range(num_orders)
            ],
            "order_estimated_delivery_date": [
                (datetime.now() - timedelta(days=random.randint(0, 365))).date()
                for _ in range(num_orders)
            ],
        }
    )

    # --- 4️⃣ Generate order_items (FK → orders, products, sellers) ---
    num_sellers = 10
    sellers_pd = pd.DataFrame(
        {
            "seller_id": [f"sel_{uuid.uuid4().hex[:8]}" for _ in range(num_sellers)],
            "seller_zip_code_prefix": np.random.randint(10000, 99999, size=num_sellers),
            "seller_city": [f"SellerCity_{i}" for i in range(1, num_sellers + 1)],
            "seller_state": [f"SS_{i%50}" for i in range(1, num_sellers + 1)],
        }
    )

    order_items_rows = []
    for oid in orders_pd["order_id"]:
        for _ in range(random.randint(1, 3)):
            prod = random.choice(products_pd["product_id"].tolist())
            seller = random.choice(sellers_pd["seller_id"].tolist())
            price = (
                float(
                    products_pd.loc[
                        products_pd["product_id"] == prod, "product_description_length"
                    ].values[0]
                )
                % 100
                + 10.0
            )
            order_items_rows.append(
                {
                    "order_id": oid,
                    "order_item_id": random.randint(1, 99),
                    "product_id": prod,
                    "seller_id": seller,
                    "shipping_limit_date": (
                        datetime.now() - timedelta(days=random.randint(0, 365))
                    ).date(),
                    "price": round(price, 2),
                    "freight_value": round(random.uniform(1, 50), 2),
                }
            )
    order_items_pd = pd.DataFrame(order_items_rows)

    # --- 5️⃣ payments ---
    payments_rows = []
    for oid in orders_pd["order_id"]:
        payments_rows.append(
            {
                "order_id": oid,
                "payment_sequential": 1,
                "payment_type": random.choice(["credit_card", "debit_card", "boleto"]),
                "payment_installments": float(random.choice([1, 2, 3])),
                "payment_value": round(random.uniform(20, 1000), 2),
            }
        )
    payments_pd = pd.DataFrame(payments_rows)

    # --- 6️⃣ order_reviews ---
    review_rows = []
    for oid in random.sample(
        list(orders_pd["order_id"]), k=max(1, int(len(orders_pd) * 0.6))
    ):
        review_rows.append(
            {
                "review_id": f"rev_{random.randint(100000,999999)}",
                "order_id": oid,
                "review_score": random.randint(1, 5),
                "review_comment_title": f"Title {random.randint(1,100)}",
                "review_comment_message": f"Message {random.randint(1,1000)}",
                "review_creation_date": (
                    datetime.now() - timedelta(days=random.randint(0, 365))
                ).date(),
                "review_answer_timestamp": (
                    datetime.now() - timedelta(days=random.randint(0, 365))
                ).date(),
            }
        )
    order_reviews_pd = pd.DataFrame(review_rows)

    # --- Convert pandas -> polars ---
    customers_pl = pl.from_pandas(customers_pd)
    sellers_pl = pl.from_pandas(sellers_pd)
    products_pl = pl.from_pandas(products_pd)
    orders_pl = pl.from_pandas(orders_pd)
    order_items_pl = pl.from_pandas(order_items_pd)
    payments_pl = pl.from_pandas(payments_pd)
    order_reviews_pl = pl.from_pandas(order_reviews_pd)

    # --- 7️⃣ Insert Data ---
    mysql = context.resources.mysql_io_manager

    class _DummyCtx:
        def __init__(self, table_name, log):
            self.metadata = {"table": table_name}
            self.log = log

    # --- INSERT LOOKUP TABLES ---
    # Bây giờ không cần Try/Except nữa. Nếu trùng 'electronics', nó sẽ tự Update đè lên.

    mysql.insert_output(
        _DummyCtx("product_category_name_translation", context.log),
        pl.from_pandas(
            pd.DataFrame(
                {
                    "product_category_name": [
                        "electronics",
                        "fashion",
                        "toys",
                        "furniture",
                        "books",
                        "beauty",
                    ],
                    "product_category_name_english": [
                        "Electronics",
                        "Fashion",
                        "Toys",
                        "Furniture",
                        "Books",
                        "Beauty",
                    ],
                }
            )
        ),
    )

    mysql.insert_output(
        _DummyCtx("geolocation", context.log),
        pl.from_pandas(
            pd.DataFrame(
                {
                    "geolocation_zip_code_prefix": [
                        int(x) for x in np.random.randint(10000, 99999, size=50)
                    ],
                    "geolocation_lat": np.random.uniform(-90, 90, size=50),
                    "geolocation_lng": np.random.uniform(-180, 180, size=50),
                    "geolocation_city": [f"City_{i}" for i in range(50)],
                    "geolocation_state": [f"ST_{i%50}" for i in range(50)],
                }
            )
        ),
    )

    # --- INSERT TRANSACTION TABLES ---
    mysql.insert_output(_DummyCtx("sellers", context.log), sellers_pl)
    mysql.insert_output(_DummyCtx("customers", context.log), customers_pl)
    mysql.insert_output(_DummyCtx("products", context.log), products_pl)
    mysql.insert_output(_DummyCtx("orders", context.log), orders_pl)
    mysql.insert_output(_DummyCtx("order_items", context.log), order_items_pl)
    mysql.insert_output(_DummyCtx("payments", context.log), payments_pl)
    mysql.insert_output(_DummyCtx("order_reviews", context.log), order_reviews_pl)

    context.log.info("✅ Inserted/Merged fake data into MySQL successfully!")

    return Output(value=None, metadata={"status": "Success", "mode": "Upsert"})
