from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl
import pandas as pd
from dagster import asset, Output
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import polars as pl
import uuid


COMPUTE_KIND = "Polars"
LAYER = "bronze"


# genre from my_sql
@asset(
    description="Load table 'customers' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "customer"],
    compute_kind="Parquet",
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_customer(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM customers;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "customers",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# Tables Sellers từ mysql
@asset(
    description="Load table 'sellers' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "seller"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_seller(context) -> Output[pl.DataFrame]:

    # Lấy watermark cũ từ metadata (last_update)
    asset_key = context.asset_key_for_output()
    latest_materialization_event = context.instance.get_latest_materialization_events(
        [asset_key]
    ).get(asset_key)

    watermark = "1970-01-01T00:00:00"  # Mặc định thời gian rất cũ
    if latest_materialization_event:
        materialization = (
            latest_materialization_event.dagster_event.event_specific_data.materialization
        )
        metadata = materialization.metadata
        last_update = metadata.get("last_update")
        if last_update:
            try:
                watermark = last_update.value  # Chuỗi ISO datetime
                context.log.info(f"Watermark cũ (last_update): {watermark}")
            except (ValueError, AttributeError):
                context.log.warning(
                    "Giá trị last_update không hợp lệ, dùng watermark mặc định"
                )
        else:
            context.log.info(
                "Không tìm thấy last_update trong metadata, sẽ lấy full load."
            )
    else:
        context.log.info("Không có materialization trước, sẽ lấy full load.")

    if watermark != "1970-01-01T00:00:00":
        # Incremental load với last_update
        query = f"""
            SELECT * FROM sellers 
            WHERE last_update > '{watermark}'
            ORDER BY last_update ASC;
        """
        context.log.info(f"Incremental load từ last_update > {watermark}")
    else:
        # Full load lần đầu
        query = "SELECT * FROM sellers ORDER BY seller_id ASC;"
        context.log.info("Full load lần đầu tiên hoặc không có cột last_update")

    # query = "SELECT * FROM sellers;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {query}")
    context.log.info(f"Table extracted with shape: {df_data.shape}")
    context.log.info(f"Table extracted with shape: {df_data.columns}")
    context.log.info(f"Table extracted with first 5 rows: {df_data.head().to_dict()}")
    # Đảm bảo cột last_updated là datetime
    df_data = df_data.with_columns(pl.col("last_update").cast(pl.Datetime))

    # Tìm watermark mới (max last_updated)
    new_watermark = df_data["last_update"].max().isoformat()

    return Output(
        value=df_data,
        metadata={
            "table": "sellers",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
            "last_update": new_watermark,
        },
    )


# Tables products from mysql
@asset(
    description="Load table 'products' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "product"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_product(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM products;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "products",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# Tables from mysql
@asset(
    description="Load table 'orders' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "order"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_order(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM orders;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "orders",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# Tables from mysql
@asset(
    description="Load table 'order_items' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "orderitem"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_order_item(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM order_items;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "order_items",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# Tables from mysql
@asset(
    description="Load table 'payments' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "payment"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_payment(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM payments;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "payments",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# Tables from mysql
@asset(
    description="Load table 'order_reviews' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "orderreview"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_order_review(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM order_reviews;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")
    # data = [
    #    ["An", 23, "Hà Nội"],
    #    ["Bình", 21, "Đà Nẵng"],
    #    ["Chi", 22, "Hồ Chí Minh"],
    #    ["Dũng", 24, "Hải Phòng"],
    # ]
    # df_data = pd.DataFrame(data, columns=["Tên", "Tuổi", "Thành Phố"])
    return Output(
        value=df_data,
        metadata={
            "table": "order_reviews",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# Tables from mysql
@asset(
    description="Load table 'product_category_name_translation' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "productcategory"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_product_category(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM product_category_name_translation;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "product_category",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


# --------------------#
# Tables from mysql
@asset(
    description="Load table 'geolocation' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "geolocation"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
# Extract data từ mysql
def bronze_geolocation(context) -> Output[pl.DataFrame]:
    query = "SELECT * FROM geolocation;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "geolocation",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
        },
    )


@asset(
    description="Generate fake data for customers, products, orders, and order_items and insert to MySQL",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "geolocation"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def generate_mysql_data(context) -> Output[None]:
    # --- 1 Generate customers ---
    num_customers = 10
    customers_pd = pd.DataFrame(
        {
            # "customer_id": [f"cus_{i:04d}" for i in range(1, num_customers + 1)],
            "customer_id": [
                f"cus_{uuid.uuid4().hex[:8]}" for _ in range(num_customers)
            ],
            "customer_unique_id": [
                f"uniq_{i:06d}" for i in range(1, num_customers + 1)
            ],
            "customer_zip_code_prefix": np.random.randint(
                10000, 99999, size=num_customers
            ),
            "customer_city": [f"City_{i}" for i in range(1, num_customers + 1)],
            "customer_state": [f"ST_{i%50}" for i in range(1, num_customers + 1)],
        }
    )

    # --- 2 Generate products ---
    num_products = 8
    categories = ["electronics", "fashion", "toys", "furniture", "books", "beauty"]
    products_pd = pd.DataFrame(
        {
            # "product_id": [f"prod_{i:05d}" for i in range(1, num_products + 1)],
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

    # --- 3 Generate orders ---
    num_orders = 15

    def rand_dt():
        return datetime.now() - timedelta(days=random.randint(0, 365))

    orders_pd = pd.DataFrame(
        {
            # "order_id": [f"ord_{i:06d}" for i in range(1, num_orders + 1)],
            "order_id": [str(uuid.uuid4()) for _ in range(num_orders)],
            "customer_id": np.random.choice(customers_pd["customer_id"], num_orders),
            "order_status": [
                random.choice(["delivered", "shipped", "processing", "canceled"])
                for _ in range(num_orders)
            ],
            "order_purchase_timestamp": [rand_dt() for _ in range(num_orders)],
            "order_approved_at": [rand_dt() for _ in range(num_orders)],
            "order_delivered_carrier_date": [rand_dt() for _ in range(num_orders)],
            "order_delivered_customer_date": [rand_dt() for _ in range(num_orders)],
            "order_estimated_delivery_date": [rand_dt() for _ in range(num_orders)],
        }
    )

    # --- 4 Generate sellers ---
    num_sellers = 10
    sellers_pd = pd.DataFrame(
        {
            # "seller_id": [f"sel_{i:05d}" for i in range(1, num_sellers + 1)],
            "seller_id": [f"sel_{uuid.uuid4().hex[:8]}" for _ in range(num_sellers)],
            "seller_zip_code_prefix": np.random.randint(10000, 99999, size=num_sellers),
            "seller_city": [f"SellerCity_{i}" for i in range(1, num_sellers + 1)],
            "seller_state": [f"SS_{i%50}" for i in range(1, num_sellers + 1)],
        }
    )

    # --- 5 Generate order_items ---
    order_items_rows = []
    for oid in orders_pd["order_id"]:
        for _ in range(random.randint(1, 3)):
            prod = random.choice(products_pd["product_id"].tolist())
            seller = random.choice(sellers_pd["seller_id"].tolist())
            qty = random.randint(1, 5)
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
                    "shipping_limit_date": rand_dt(),
                    "price": round(price, 2),
                    "freight_value": round(random.uniform(1, 50), 2),
                }
            )
    order_items_pd = pd.DataFrame(order_items_rows)

    # --- 6 payments ---
    payments_pd = pd.DataFrame(
        [
            {
                "order_id": oid,
                "payment_sequential": 1,
                "payment_type": random.choice(["credit_card", "debit_card", "boleto"]),
                "payment_installments": float(random.choice([1, 2, 3])),
                "payment_value": round(random.uniform(20, 1000), 2),
            }
            for oid in orders_pd["order_id"]
        ]
    )

    # --- 7 reviews ---
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
                "review_creation_date": rand_dt(),
                "review_answer_timestamp": rand_dt(),
            }
        )
    order_reviews_pd = pd.DataFrame(review_rows)

    # --- Convert pandas -> polars ---
    to_pl = lambda df: pl.from_pandas(df)

    customers_pl = to_pl(customers_pd)
    sellers_pl = to_pl(sellers_pd)
    products_pl = to_pl(products_pd)
    orders_pl = to_pl(orders_pd)
    order_items_pl = to_pl(order_items_pd)
    payments_pl = to_pl(payments_pd)
    order_reviews_pl = to_pl(order_reviews_pd)

    # --- Insert theo thứ tự ràng buộc ---
    mysql = context.resources.mysql_io_manager

    class _DummyCtx:
        def __init__(self, table_name, log):
            self.metadata = {"table": table_name}
            self.log = log

    # Insert lookup tables
    # mysql.insert_output(
    #     _DummyCtx("product_category_name_translation", context.log),
    #     to_pl(pd.DataFrame({
    #         "product_category_name": categories,
    #         "product_category_name_english": [c.title() for c in categories],
    #     })),
    # )

    mysql.insert_output(
        _DummyCtx("geolocation", context.log),
        to_pl(
            pd.DataFrame(
                {
                    "geolocation_zip_code_prefix": np.random.randint(
                        10000, 99999, size=50
                    ),
                    "geolocation_lat": np.random.uniform(-90, 90, size=50),
                    "geolocation_lng": np.random.uniform(-180, 180, size=50),
                    "geolocation_city": [f"City_{i}" for i in range(50)],
                    "geolocation_state": [f"ST_{i%50}" for i in range(50)],
                }
            )
        ),
    )

    # Insert main tables
    mysql.insert_output(_DummyCtx("sellers", context.log), sellers_pl)
    mysql.insert_output(_DummyCtx("customers", context.log), customers_pl)
    mysql.insert_output(_DummyCtx("products", context.log), products_pl)
    mysql.insert_output(_DummyCtx("orders", context.log), orders_pl)
    mysql.insert_output(_DummyCtx("order_items", context.log), order_items_pl)
    mysql.insert_output(_DummyCtx("payments", context.log), payments_pl)
    mysql.insert_output(_DummyCtx("order_reviews", context.log), order_reviews_pl)

    context.log.info("✅ Inserted fake data into MySQL tables successfully!")

    return Output(
        value=None,
        metadata={
            "customers": f"{customers_pd.shape[0]} rows × {customers_pd.shape[1]} cols",
            "sellers": f"{sellers_pd.shape[0]} rows × {sellers_pd.shape[1]} cols",
            "products": f"{products_pd.shape[0]} rows × {products_pd.shape[1]} cols",
            "orders": f"{orders_pd.shape[0]} rows × {orders_pd.shape[1]} cols",
            "order_items": f"{order_items_pd.shape[0]} rows × {order_items_pd.shape[1]} cols",
            "payments": f"{payments_pd.shape[0]} rows × {payments_pd.shape[1]} cols",
            "order_reviews": f"{order_reviews_pd.shape[0]} rows × {order_reviews_pd.shape[1]} cols",
        },
    )
