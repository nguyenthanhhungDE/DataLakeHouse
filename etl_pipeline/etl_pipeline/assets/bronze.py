from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl
import pandas as pd

COMPUTE_KIND = "SQL"
LAYER = "bronze"


# genre from my_sql
@asset(
    description="Load table 'customers' from MySQL database as polars DataFrame, and save to minIO",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "customer"],
    compute_kind=COMPUTE_KIND,
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
    query = "SELECT * FROM sellers;"
    df_data = context.resources.mysql_io_manager.extract_data(query)
    context.log.info(f"Table extracted with shape: {df_data.shape}")

    return Output(
        value=df_data,
        metadata={
            "table": "sellers",
            "row_count": df_data.shape[0],
            "column_count": df_data.shape[1],
            "columns": df_data.columns,
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
    #data = [
    #    ["An", 23, "Hà Nội"],
    #    ["Bình", 21, "Đà Nẵng"],
    #    ["Chi", 22, "Hồ Chí Minh"],
    #    ["Dũng", 24, "Hải Phòng"],
    #]
    #df_data = pd.DataFrame(data, columns=["Tên", "Tuổi", "Thành Phố"])
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
