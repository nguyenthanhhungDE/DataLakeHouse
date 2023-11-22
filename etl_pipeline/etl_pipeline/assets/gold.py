from dagster import asset, AssetIn, Output, StaticPartitionsDefinition
from datetime import datetime
import polars as pl
import requests
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql import DataFrame

from ..resources.spark_io_manager import get_spark_session


COMPUTE_KIND = "PySpark"
LAYER = "gold"


@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_customer": AssetIn(key_prefix=["silver", "customer"]),
        "silver_cleaned_geolocation": AssetIn(key_prefix=["silver", "geolocation"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimcustomer"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_customer(
    context, silver_cleaned_customer, silver_cleaned_geolocation: DataFrame
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
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold")

        customer = silver_cleaned_customer
        geolocation = silver_cleaned_geolocation
        # Thực hiện left join
        joined_df = customer.join(
            geolocation,
            customer["customer_zip_code_prefix"]
            == geolocation["geolocation_zip_code_prefix"],
            how="left",
        )
        # Đổi tên các cột
        joined_df = joined_df.withColumnRenamed(
            "geolocation_lat", "customer_lat"
        ).withColumnRenamed("geolocation_lng", "customer_lng")

        # Thêm cột customer_city
        joined_df = joined_df.withColumn("customer_city", joined_df["geolocation_city"])

        # Thêm cột customer_state
        joined_df = joined_df.withColumn(
            "customer_state", joined_df["geolocation_state"]
        )

        # Xóa các cột không cần thiết
        joined_df = joined_df.drop(
            "geolocation_city",
            "geolocation_state",
            "customer_zip_code_prefix",
            "geolocation_zip_code_prefix",
        )

        # Lọc các dòng trùng lặp dựa trên cột customer_id
        joined_df = joined_df.dropDuplicates(subset=["customer_id"])
        context.log.info("Got spark DataFrame, getting neccessary columns")

        final_df = joined_df.select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "customer_lat",
            "customer_lng",
        )

        return Output(
            value=final_df,
            metadata={
                "table": "dim_customers",
                "row_count": final_df.count(),
                "column_count": len(final_df.columns),
                "columns": final_df.columns,
            },
        )


# @asset(
#     description="Split book table to get basic info",
#     ins={
#         "silver_cleaned_geolocation": AssetIn(key_prefix=["silver", "geolocation"]),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["gold", "dimgeolocation"],
#     compute_kind="PySpark",
#     group_name="gold",
# )
# def dim_geolocation(context, silver_cleaned_geolocation: DataFrame):
#     """
#     Split book table to get basic info
#     """
#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         spark_df = silver_cleaned_geolocation

#         context.log.info("Got spark DataFrame, getting neccessary columns")

#         spark_df = spark_df.select(
#             "geolocation_zip_code_prefix",
#             "geolocation_lat",
#             "geolocation_lng",
#             "geolocation_city",
#             "geolocation_state",
#         )

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "dim_geolocation",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_seller": AssetIn(key_prefix=["silver", "seller"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimseller"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_seller(context, silver_cleaned_seller: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_seller
    context.log.info("Got spark DataFrame, getting neccessary columns")

    #     # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
    spark_df = spark_df.select(
        "seller_id",
        "seller_zip_code_prefix",
    )
    #     spark_df.collect()

    return Output(
        value=spark_df,
        metadata={
            "table": "dim_seller",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimreview"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_review(context, silver_cleaned_order_review: DataFrame):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_order_review
    context.log.info("Got spark DataFrame, getting neccessary columns")

    spark_df = spark_df.select(
        "review_id",
        "review_score",
    )

    return Output(
        value=spark_df,
        metadata={
            "table": "dim_seller",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_product": AssetIn(key_prefix=["silver", "product"]),
        "silver_cleaned_product_category": AssetIn(
            key_prefix=["silver", "productcategory"]
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimproduct"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_product(
    context, silver_cleaned_product, silver_cleaned_product_category: DataFrame
):
    """
    Split book table to get basic info
    """

    spark_df = silver_cleaned_product.join(
        silver_cleaned_product_category, "product_category_name", "inner"
    )
    context.log.info("Got spark DataFrame, getting neccessary columns")

    #     # Drop rows with null value in Language column
    #     spark_df = spark_df.dropna(subset=["Language"])

    #     # Select columns ISBN, Name, Authors, Language, Description, PagesNumber
    spark_df = spark_df.select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )

    return Output(
        value=spark_df,
        metadata={
            "table": "gold_book_with_info",
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "columns": spark_df.columns,
        },
    )


@asset(
    description="Split book table to get basic info",
    ins={
        "silver_cleaned_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_cleaned_payment": AssetIn(key_prefix=["silver", "payment"]),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "dimorder"],
    compute_kind="PySpark",
    group_name="gold",
)
def dim_order(context, silver_cleaned_order, silver_cleaned_payment: DataFrame):
    """
    Split book table to get basic info
    """

    df2 = silver_cleaned_payment
    df1 = silver_cleaned_order
    df = df1.join(df2, "order_id", "inner")
    context.log.info("Got spark DataFrame, getting neccessary columns")

    df = df.select(
        "order_id",
        "order_status",
        "payment_type",
    )

    return Output(
        value=df,
        metadata={
            "table": "dim",
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
        },
    )


@asset(
    description="Fact table to star schema SCD1",
    io_manager_key="spark_io_manager",
    ins={
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_seller": AssetIn(key_prefix=["gold", "dimseller"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_order": AssetIn(key_prefix=["gold", "dimorder"]),
        "silver_cleaned_order_item": AssetIn(key_prefix=["silver", "orderitem"]),
        "silver_cleaned_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_cleaned_payment": AssetIn(key_prefix=["silver", "payment"]),
        "silver_cleaned_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
        "silver_cleaned_product": AssetIn(key_prefix=["silver", "product"]),
        "dim_date": AssetIn(key_prefix=["gold", "date"]),
    },
    key_prefix=[LAYER, "facttable"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def fact_table(
    context,
    dim_customer,
    dim_seller,
    dim_product,
    dim_order,
    silver_cleaned_order_item,
    silver_cleaned_order,
    silver_cleaned_payment,
    silver_cleaned_order_review,
    silver_cleaned_product,
    dim_date,
):
    Union_order = silver_cleaned_order
    Union_orderitems = silver_cleaned_order_item
    Union = Union_order.join(Union_orderitems, "order_id", "inner")
    fact_table = (
        Union.join(
            dim_order,
            on=["order_id"],
            how="inner",
        )
        .join(dim_product, on="product_id", how="inner")
        .join(dim_customer, on="customer_id", how="inner")
        .join(dim_seller, on="seller_id", how="inner")
        .join(silver_cleaned_payment, on="order_id", how="inner")
        .join(silver_cleaned_order_review, on="order_id", how="inner")
        .join(dim_product, on="product_id", how="inner")
        .join(
            dim_date,
            Union["order_purchase_timestamp"] == dim_date["full_date"],
            how="inner",
        )
        .select(
            [
                "order_id",
                "order_item_id",
                "customer_id",
                "product_id",
                "review_id",
                "seller_id",
                "dateKey",
                "price",
                "freight_value",
                # "payment_value",
                "payment_value",
                "payment_installments",
                "payment_sequential",
                # "review_score",
            ]
        )
    )
    return Output(
        fact_table,
        metadata={
            "table": "fact_table",
            "records count": len(fact_table.columns),
            "row_count": fact_table.count(),
        },
    )


@asset(
    description="dim date",
    ins={
        "silver_date": AssetIn(
            key_prefix=["silver", "date"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["gold", "date"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def dim_date(context, silver_date: DataFrame):
    """
    Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
    """

    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    spark_df = silver_date
    context.log.debug("Start creating spark session")
    start_date = spark_df.select(min("order_purchase_timestamp")).first()[0]
    end_date = spark_df.select(max("order_purchase_timestamp")).first()[0]

    with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
        date_range = spark.sparkContext.parallelize(
            [
                (start_date + timedelta(days=x))
                for x in range((end_date - start_date).days + 1)
            ]
        )
        date_df = date_range.map(lambda x: (x,)).toDF(["date"])

    date_df = (
        date_df.withColumn(
            "dateKey",
            year(col("date")) * 10000
            + month(col("date")) * 100
            + dayofmonth(col("date")),
        )
        .withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("week", weekofyear(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("day_of_year", dayofyear(col("date")))
        .withColumn("day_name_of_week", date_format(col("date"), "EEEE"))
        .withColumn("month_name_of_week", date_format(col("date"), "MMMM"))
        .withColumnRenamed("date", "full_date")
        .selectExpr(
            [
                "dateKey",
                "full_date",
                "year",
                "quarter",
                "month",
                "week",
                "day",
                "day_of_year",
                "day_name_of_week",
                "month_name_of_week",
            ]
        )
    )

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
