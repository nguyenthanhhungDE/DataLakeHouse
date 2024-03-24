from dagster import asset, AssetIn, Output, StaticPartitionsDefinition

import polars as pl
import requests
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


from pyspark.sql import DataFrame
from datetime import datetime, timedelta
from ..resources.spark_io_manager import get_spark_session


COMPUTE_KIND = "PySpark"
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


# Silver cleaned customer
@asset(
    description="Load 'customers' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    ins={
        "bronze_customer": AssetIn(
            key_prefix=["bronze", "customer"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "customer"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_customer(context, bronze_customer: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_customer.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.dropDuplicates()
        spark_df = spark_df.na.drop()
        # spark_df.cache()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_customer",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load 'seller' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_seller": AssetIn(
            key_prefix=["bronze", "seller"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "seller"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_seller(context, bronze_seller: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_seller.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.na.drop()
        spark_df = spark_df.dropDuplicates(subset=["seller_id"])
        # .dropDuplicates(subset=["customer_id"])
        # spark_df.cache()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_sellers",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load 'product' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_product": AssetIn(
            key_prefix=["bronze", "product"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "product"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_product(context, bronze_product: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_product.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.na.drop()
        spark_df = spark_df.dropDuplicates()
        # spark_df.cache()
        columns_to_convert = [
            "product_description_length",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]
        for column in columns_to_convert:
            spark_df = spark_df.withColumn(column, col(column).cast("integer"))
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_product",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load 'order_items' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_order_item": AssetIn(
            key_prefix=["bronze", "orderitem"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderitem"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order_item(context, bronze_order_item: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_order_item.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        # spark_df.cache()
        spark_df = spark_df.withColumn("price", round(col("price"), 2).cast("double"))
        spark_df = spark_df.withColumn(
            "freight_value", round(col("freight_value"), 2).cast("double")
        )
        spark_df = spark_df.na.drop()
        spark_df = spark_df.dropDuplicates()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_order_items",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load 'payment' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_payment": AssetIn(
            key_prefix=["bronze", "payment"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "payment"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_payment(context, bronze_payment: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_payment.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        # spark_df.cache()
        spark_df = spark_df.withColumn(
            "payment_value", round(col("payment_value"), 2).cast("double")
        )
        spark_df = spark_df.withColumn(
            "payment_installments", col("payment_installments").cast("integer")
        )
        spark_df = spark_df.na.drop()
        spark_df = spark_df.dropDuplicates()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_payments",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load 'order_review' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_order_review": AssetIn(
            key_prefix=["bronze", "orderreview"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderreview"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order_review(context, bronze_order_review: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_order_review.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        # spark_df.cache()
        spark_df = spark_df.drop("review_comment_title")
        spark_df = spark_df.na.drop()
        spark_df = spark_df.dropDuplicates()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_order_reviews",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="Load 'order_reviews' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_product_category": AssetIn(
            key_prefix=["bronze", "productcategory"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "productcategory"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_product_category(context, bronze_product_category: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_product_category.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.dropDuplicates()
        # spark_df.cache()
        spark_df = spark_df.na.drop()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_product_category",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


# # # ---------------------------------------#
@asset(
    description="Load 'orders' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_order": AssetIn(
            key_prefix=["bronze", "order"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "order"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order(context, bronze_order: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    context.log.debug("Start creating spark session")
    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_order.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.na.drop()
        # spark_df = spark_df.dropDuplicates()
        spark_df = spark_df.dropDuplicates(["order_id"])
        # spark_df.cache()
        context.log.info("Got Spark DataFrame")
        # spark_df.unpersist()
        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_orders",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )


@asset(
    description="silver date",
    ins={
        "bronze_order": AssetIn(
            key_prefix=["bronze", "order"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "date"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_date(context, bronze_order: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    context.log.info("Got Spark DataFrame")
    # spark_df.unpersist()
    context.log.debug("Start creating spark session")
    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        date_df = bronze_order.select("order_purchase_timestamp").to_pandas()
        context.log.debug(f"Converted to pandas DataFrame with shape: {date_df.shape}")
        date_df = spark.createDataFrame(date_df)
        date_df = date_df.na.drop()
        date_df = date_df.dropDuplicates()
        # spark_df.cache()
        context.log.info("Got Spark DataFrame")
        # spark_df.unpersist()

        return Output(
            value=date_df,
            metadata={
                "table": "silver_date",
                "row_count": date_df.count(),
                "column_count": len(date_df.columns),
                "columns": date_df.columns,
            },
        )


@asset(
    description="Load 'geo' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_geolocation": AssetIn(
            key_prefix=["bronze", "geolocation"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "geolocation"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_geolocation(context, bronze_geolocation: pl.DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    context.log.debug("Start creating spark session")

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        # Convert bronze_book from polars DataFrame to Spark DataFrame
        pandas_df = bronze_geolocation.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.dropDuplicates()
        # spark_df.cache()
        spark_df = spark_df.na.drop()
        # filter tọa độ cho đúng theo giới hạn của brazill
        spark_df = spark_df.filter(
            (col("geolocation_lat") <= 5.27438888)
            & (col("geolocation_lng") >= -73.98283055)
            & (col("geolocation_lat") >= -33.75116944)
            & (col("geolocation_lng") <= -34.79314722)
        )

        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_geolocation",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
