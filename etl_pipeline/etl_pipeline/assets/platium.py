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


@asset(
    description="data for visualizations",
    ins={
        "dim_order": AssetIn(key_prefix=["gold", "dimorder"]),
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_seller": AssetIn(key_prefix=["gold", "dimseller"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_date": AssetIn(key_prefix=["gold", "date"]),
        "fact_table": AssetIn(key_prefix=["gold", "facttable"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["platium", "sale"],
    compute_kind="PySpark",
    group_name="platium",
)
def Cube_sale(
    context,
    dim_order,
    dim_customer,
    dim_seller,
    dim_product,
    dim_date,
    fact_table: DataFrame,
):
    """
    Split book table to get basic info
    """
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS platium")

        data_mart = fact_table

        context.log.info("Got spark DataFrame, getting neccessary columns")

        data_mart = (
            data_mart.join(
                dim_order,
                on=["order_id"],
                how="inner",
            )
            .join(dim_product, on="product_id", how="inner")
            .join(dim_customer, on="customer_id", how="inner")
            .join(dim_seller, on="seller_id", how="inner")
            .join(dim_date, on="dateKey", how="inner")
        )
        data_mart = data_mart.dropDuplicates(subset=["order_id", "customer_unique_id"])

        # data_mart=data_mart.join(dim_geolocation,data_mart["customer_zip_code_prefix"]== dim_geolocation["geolocation_zip_code_prefix"],
        #         how="inner",
        #     )
        # .join(
        #     dim_geolocation,
        #     data_mart["customer_zip_code_prefix"]
        #     == dim_geolocation["geolocation_zip_code_prefix"],
        #     how="inner",
        # )
        # .join(
        #     dim_geolocation,
        #     data_mart["seller_zip_code_prefix"]
        #     == dim_geolocation["geolocation_zip_code_prefix"],
        #     how="inner",
        # )
        # .join(dim_geolocation,data_mart["customer_zip_code_prefix"]== dim_geolocation["geolocation_zip_code_prefix"], how="inner")

        return Output(
            value=data_mart,
            metadata={
                "table": "dim_customer",
                "row_count": data_mart.count(),
                "column_count": len(data_mart.columns),
                "columns": data_mart.columns,
            },
        )
