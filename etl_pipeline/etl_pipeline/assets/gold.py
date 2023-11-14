from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl
import requests
import os
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import round, col

from pyspark.sql import DataFrame

from ..resources.spark_io_manager import get_spark_session
from pyspark.sql.functions import udf, col, regexp_replace, lower, when


COMPUTE_KIND = "PySpark"
LAYER = "gold"
@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_customers": AssetIn(
            key_prefix=["silver", "customer"]
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimcustomer"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_customers(context, silver_cleaned_customers: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_customers
    context.log.info("Got spark DataFrame, getting neccessary columns")

#     # Drop rows with null value in Language column
#     spark_df = spark_df.dropna(subset=["Language"])

#     # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
#     spark_df = spark_df.select(
#         "ISBN",
#         "Name",
#         "Authors",
#         "Language",
#         "PagesNumber",
#     )
#     spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_info",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )

#  #dim sellers
@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_sellers": AssetIn(
            key_prefix=["silver", "sellers"]
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimsellers"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_sellers(context, silver_cleaned_sellers: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_sellers
    context.log.info("Got spark DataFrame, getting neccessary columns")

#     # Drop rows with null value in Language column
#     spark_df = spark_df.dropna(subset=["Language"])

#     # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
#     spark_df = spark_df.select(
#         "ISBN",
#         "Name",
#         "Authors",
#         "Language",
#         "PagesNumber",
#     )
#     spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_info",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )
# # dim products
@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_products": AssetIn(
            key_prefix=["silver", "products"]
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimproducts"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_products(context, silver_cleaned_products: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_products
    context.log.info("Got spark DataFrame, getting neccessary columns")

#     # Drop rows with null value in Language column
#     spark_df = spark_df.dropna(subset=["Language"])

#     # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
#     spark_df = spark_df.select(
#         "ISBN",
#         "Name",
#         "Authors",
#         "Language",
#         "PagesNumber",
#     )
#     spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_info",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )
# dim orders

@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_orders": AssetIn(
            key_prefix=["silver", "orders"]
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimorders"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_orders(context, silver_cleaned_orders: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_orders
    context.log.info("Got spark DataFrame, getting neccessary columns")

#     # Drop rows with null value in Language column
#     spark_df = spark_df.dropna(subset=["Language"])

#     # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
#     spark_df = spark_df.select(
#         "ISBN",
#         "Name",
#         "Authors",
#         "Language",
#         "PagesNumber",
#     )
#     spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_info",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )
# # fact table
@asset(
    io_manager_key="spark_io_manager",
    ins={
        "dim_customers":    AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_sellers"  :    AssetIn(key_prefix=["gold", "dimsellers"]),
        "dim_products" :    AssetIn(key_prefix=["gold", "dimproducts"]),
        "dim_orders"   :    AssetIn(key_prefix=["gold", "dimorders"]),
        # "silver_cleaned_order_items":    AssetIn(key_prefix=["silver", "orderitems"]),
        # "silver_cleaned_payments" :    AssetIn(key_prefix=["silver", "payments"]),
        # "silver_cleaned_order_reviews":    AssetIn(key_prefix=["silver", "orderreviews"]),
        # "silver_cleaned_product_category":    AssetIn(key_prefix=["silver", "productcategory"]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER
)
def fact_table(context, dim_customers, dim_sellers,
                        dim_products, dim_orders
                       ):
    fact_table = (
        dim_customers
        .join(dim_sellers, on=["tpep_pickup_datetime", "tpep_dropoff_datetime"], how="left")
        .join(dim_products, on='DOLocationID', how="left")
        .join(dim_orders, on='passenger_count', how="left")
        .select([
            'VendorID', 'datetime_ID', 'passenger_count_ID',
            'trip_distance_ID', 'rate_code_ID', 'store_and_fwd_flag',
            'pickup_location_ID', 'dropoff_location_ID',
            'payment_type_ID', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount'
        ])
    )
    return Output(
        fact_table,
        metadata={
            "table": "fact_table",
            "records count": len(fact_table),
        }
    )
    