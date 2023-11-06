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
LAYER = "silver"
YEARLY = StaticPartitionsDefinition(
    [str(year) for year in range(1975, datetime.today().year)]
)


# Silver cleaned book
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
def silver_cleaned_customers(context, bronze_customer: pl.DataFrame):
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
        spark_df= spark_df.na.drop()
        # spark_df.cache()
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_customers",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
# Silver cleaned sellers
@asset(
    description="Load 'sellers' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_sellers": AssetIn(
            key_prefix=["bronze", "sellers"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "sellers"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_sellers(context, bronze_sellers: pl.DataFrame):
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
        pandas_df = bronze_sellers.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df= spark_df.na.drop()
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
# Silver cleaned products
@asset(
    description="Load 'products' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_products": AssetIn(
            key_prefix=["bronze", "products"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "products"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_products(context, bronze_products: pl.DataFrame):
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
        pandas_df = bronze_products.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.na.drop()
        # spark_df.cache()
        columns_to_convert = ["product_description_length",
                      "product_length_cm", "product_height_cm", "product_width_cm"]
        for column in columns_to_convert:
            spark_df = spark_df.withColumn(column, col(column).cast("integer"))
        context.log.info("Got Spark DataFrame")

        # spark_df.unpersist()

        return Output(
            value=spark_df,
            metadata={
                "table": "silver_cleaned_products",
                "row_count": spark_df.count(),
                "column_count": len(spark_df.columns),
                "columns": spark_df.columns,
            },
        )
# Silver cleaned orders
@asset(
    description="Load 'orders' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_orders": AssetIn(
            key_prefix=["bronze", "orders"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orders"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_orders(context, bronze_orders: pl.DataFrame):
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
        pandas_df = bronze_orders.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        spark_df = spark_df.na.drop()
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
# Silver cleaned book
@asset(
    description="Load 'order_items' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_order_items": AssetIn(
            key_prefix=["bronze", "orderitems"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderitems"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order_items(context, bronze_order_items: pl.DataFrame):
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
        pandas_df = bronze_order_items.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        # spark_df.cache()
        spark_df = spark_df.withColumn("price", round(col("price"), 2).cast("double"))
        spark_df = spark_df.withColumn("freight_value", round(col("freight_value"), 2).cast("double"))
        spark_df = spark_df.na.drop()
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
# Silver cleaned payments
@asset(
    description="Load 'payments' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_payments": AssetIn(
            key_prefix=["bronze", "payments"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "payments"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_payments(context, bronze_payments: pl.DataFrame):
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
        pandas_df = bronze_payments.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        # spark_df.cache()
        spark_df = spark_df.withColumn("payment_value", round(
            col("payment_value"), 2).cast("double"))
        spark_df = spark_df.withColumn(
            "payment_installments", col("payment_installments").cast("integer"))
        spark_df = spark_df.na.drop()
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
# Silver cleaned order_reviews
@asset(
    description="Load 'order_reviews' table from bronze layer in minIO, into a Spark dataframe, then clean data",
    # partitions_def=YEARLY,
    ins={
        "bronze_order_reviews": AssetIn(
            key_prefix=["bronze", "orderreviews"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderreviews"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order_reviews(context, bronze_order_reviews: pl.DataFrame):
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
        pandas_df = bronze_order_reviews.to_pandas()
        context.log.debug(
            f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
        spark_df = spark.createDataFrame(pandas_df)
        # spark_df.cache()
        spark_df =  spark_df.na.drop()
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
# Silver cleaned product_category_name_translation
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
        # spark_df.cache()
        spark_df =  spark_df.na.drop()
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