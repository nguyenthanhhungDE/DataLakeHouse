# from dagster import asset, AssetIn, Output
# import os
# import polars as pl
# from pyspark.sql.functions import (
#     col,
#     to_timestamp,
#     current_timestamp,
#     lit,
#     max as spark_max,
# )

# # 1. Import Resource
# from ..resources.spark_io_manager import get_spark_session
# from .etl_job.insert_job_log import insert_job_log

# # 2. Import Utils (Function chung đã build ở bài trước)
# from .etl_job.spark_utils import get_watermark_from_meta, update_watermark_meta

# COMPUTE_KIND = "PySpark"
# LAYER = "silver"


# @asset(
#     description="Load 'customers' raw data from Bronze, FILTER NEW DATA ONLY (Linear Logic), and append to Staging",
#     ins={
#         "bronze_customer": AssetIn(key_prefix=["bronze", "customer"]),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "customer"],
#     name="silver_stg_customer",
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_stg_customer(context, bronze_customer: pl.DataFrame):
#     """
#     Logic Incremental Load:
#     1. Lấy Watermark từ Metadata. Nếu chưa có (First Run) -> Trả về '1900-01-01'.
#     2. Filter Data > Watermark (Luôn luôn thực hiện bước này).
#     3. Append vào Staging.
#     4. Cập nhật Watermark mới nhất vào Metadata.
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     run_id = str(context.run.run_id).split("-")[0]

#     # --- CONFIG ---
#     target_table = "silver.stg_customer"
#     meta_table = "silver.etl_watermarks"  # Bảng quản lý watermark tập trung
#     watermark_col = "last_update"  # Cột ngày tháng trong data nguồn để so sánh
#     asset_key = "stg_customer"  # Key định danh trong bảng metadata

#     with get_spark_session(config, run_id) as spark:

#         # 0. Audit Log
#         insert_job_log(
#             spark=spark,
#             job_name="silver_stg_customer",
#             layer="silver",
#             source="bronze.customer",
#             target_table=target_table,
#             merge_key="N/A",
#             load_mode="incremental_append",
#             schedule="daily",
#             owner="hung.nguyen",
#             description="Ingest & Filter raw data from Bronze to Staging History",
#         )

#         # 1. Convert Data: Polars -> Spark
#         pandas_df = bronze_customer.to_pandas()
#         spark_df = spark.createDataFrame(pandas_df)
#         total_input_rows = spark_df.count()

#         # QUAN TRỌNG: Ép kiểu cột watermark về Timestamp để so sánh chính xác
#         if watermark_col in spark_df.columns:
#             spark_df = spark_df.withColumn(
#                 watermark_col, to_timestamp(col(watermark_col))
#             )

#         # =================================================================
#         # 2. LOGIC FILTER TUYẾN TÍNH (LINEAR FLOW)
#         # =================================================================

#         # Bước 2.1: Lấy Watermark (Nếu lần đầu, hàm này tự trả về '1900-01-01')
#         context.log.info(f"🕵️ Checking watermark for '{asset_key}'...")

#         low_watermark = get_watermark_from_meta(
#             spark, meta_table, asset_key, default_value="1900-01-01 00:00:00"
#         )
#         context.log.info(f"💧 Using Low Watermark: {low_watermark}")

#         # Bước 2.2: Filter Data
#         # - Lần đầu: date > 1900 -> True hết -> Lấy Full
#         # - Lần sau: date > 2024 -> Filter Incremental
#         spark_df = spark_df.filter(col(watermark_col) > lit(str(low_watermark)))

#         filtered_count = spark_df.count()
#         context.log.info(
#             f"🔍 Filtered Result: {filtered_count} rows (Skipped {total_input_rows - filtered_count})"
#         )

#         # =================================================================

#         # 3. Tính toán New Watermark (Max của batch hiện tại)
#         # Để lưu lại dùng cho ngày mai
#         new_batch_watermark = None
#         if filtered_count > 0:
#             try:
#                 new_batch_watermark = spark_df.agg(spark_max(watermark_col)).collect()[
#                     0
#                 ][0]
#             except Exception as e:
#                 context.log.warning(f"⚠️ Could not calculate new max watermark: {e}")

#         # 4. Update Metadata
#         # Chỉ update nếu tìm thấy watermark mới
#         if new_batch_watermark:
#             context.log.info(f"💾 Updating metadata to: {new_batch_watermark}")
#             update_watermark_meta(spark, meta_table, asset_key, new_batch_watermark)

#         # 5. Add Audit Columns & Return
#         spark_df = spark_df.withColumn("_ingested_at", current_timestamp()).withColumn(
#             "_job_run_id", lit(run_id)
#         )

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": target_table,
#                 "type": "staging_history",
#                 "watermark_used": str(low_watermark),
#                 "new_watermark_saved": str(new_batch_watermark),
#                 "input_rows": total_input_rows,
#                 "appended_rows": filtered_count,
#             },
#         )


# from dagster import asset, AssetIn, Output, StaticPartitionsDefinition, AssetKey

# import polars as pl
# import requests
# import os
# from pyspark.sql.functions import *
# from pyspark.sql.types import *


# from pyspark.sql import DataFrame
# from datetime import datetime, timedelta
# from ..resources.spark_io_manager import get_spark_session
# from .etl_job.insert_job_log import insert_job_log


# COMPUTE_KIND = "PySpark"
# LAYER = "silver"
# YEARLY = StaticPartitionsDefinition(
#     [str(year) for year in range(1975, datetime.today().year)]
# )


# # Silver cleaned customer
# @asset(
#     description="Load 'customers' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     ins={
#         "silver_cleaned_customer": AssetIn(
#             key_prefix=["silver", "customer"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "customer"],
#     compute_kind=COMPUTE_KIND,
#     # compute_kind={"python", "snowflake"},
#     group_name=LAYER,
# )
# def silver_stg_customer(context, silver_cleaned_customer: DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame

#         insert_job_log(
#             spark=spark,
#             job_name="silver_staging_customer",
#             layer="silver",
#             source="bronze.customer",
#             target_table="silver.customer_cleaned",
#             merge_key="customer_id",
#             load_mode="incremental",
#             schedule="daily",
#             owner="hung.nguyen",
#             description="Clean and deduplicate customer data",
#         )

#         # Làm sạch dữ liệu trực tiếp bằng Spark
#         context.log.debug(f"Input DataFrame rows: {silver_cleaned_customer.count()}")

#         spark_df = silver_cleaned_customer.dropDuplicates(["customer_id"]).na.drop()

#         context.log.info(f"Spark DataFrame cleaned with {spark_df.count()} rows")

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_customer",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#                 "merge_key": "customer_id",
#             },
#         )


# @asset(
#     description="Load 'seller' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_seller": AssetIn(
#             key_prefix=["bronze", "seller"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "seller"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_seller(context, bronze_seller: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:

#         insert_job_log(
#             spark=spark,
#             job_name="silver_cleaned_customer",
#             layer="silver",
#             source="bronze.customer",
#             target_table="silver.customer_cleaned",
#             merge_key="customer_id",
#             load_mode="incremental",
#             schedule="daily",
#             owner="hung.nguyen",
#             description="Clean and deduplicate customer data",
#         )

#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_seller.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         spark_df = spark_df.na.drop()
#         spark_df = spark_df.dropDuplicates(subset=["seller_id"])
#         # .dropDuplicates(subset=["customer_id"])
#         # spark_df.cache()
#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_sellers",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# @asset(
#     description="Load 'product' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_product": AssetIn(
#             key_prefix=["bronze", "product"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "product"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_product(context, bronze_product: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_product.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         spark_df = spark_df.na.drop()
#         spark_df = spark_df.dropDuplicates()
#         # spark_df.cache()
#         columns_to_convert = [
#             "product_description_length",
#             "product_length_cm",
#             "product_height_cm",
#             "product_width_cm",
#         ]
#         for column in columns_to_convert:
#             spark_df = spark_df.withColumn(column, col(column).cast("integer"))
#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_product",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# @asset(
#     description="Load 'order_items' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_order_item": AssetIn(
#             key_prefix=["bronze", "orderitem"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "orderitem"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_order_item(context, bronze_order_item: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_order_item.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         # spark_df.cache()
#         spark_df = spark_df.withColumn("price", round(col("price"), 2).cast("double"))
#         spark_df = spark_df.withColumn(
#             "freight_value", round(col("freight_value"), 2).cast("double")
#         )
#         spark_df = spark_df.na.drop()
#         spark_df = spark_df.dropDuplicates()
#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_order_items",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# @asset(
#     description="Load 'payment' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_payment": AssetIn(
#             key_prefix=["bronze", "payment"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "payment"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_payment(context, bronze_payment: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_payment.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         # spark_df.cache()
#         spark_df = spark_df.withColumn(
#             "payment_value", round(col("payment_value"), 2).cast("double")
#         )
#         spark_df = spark_df.withColumn(
#             "payment_installments", col("payment_installments").cast("integer")
#         )
#         spark_df = spark_df.na.drop()
#         spark_df = spark_df.dropDuplicates()
#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_payments",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# @asset(
#     description="Load 'order_review' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_order_review": AssetIn(
#             key_prefix=["bronze", "orderreview"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "orderreview"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_order_review(context, bronze_order_review: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_order_review.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )

#         spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         # spark_df.cache()
#         spark_df = spark_df.drop("review_comment_title")
#         spark_df = spark_df.na.drop()
#         spark_df = spark_df.dropDuplicates()
#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_order_reviews",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# @asset(
#     description="Load 'order_reviews' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_product_category": AssetIn(
#             key_prefix=["bronze", "productcategory"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "productcategory"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_product_category(context, bronze_product_category: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_product_category.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         spark_df = spark_df.dropDuplicates()
#         # spark_df.cache()
#         spark_df = spark_df.na.drop()
#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_product_category",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# # # # ---------------------------------------#
# @asset(
#     description="Load 'orders' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_order": AssetIn(
#             key_prefix=["bronze", "order"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "order"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_order(context, bronze_order: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """
#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }
#     context.log.debug("Start creating spark session")
#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_order.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         # spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver")
#         spark_df = spark.createDataFrame(pandas_df)
#         spark_df = spark_df.na.drop()
#         # spark_df = spark_df.dropDuplicates()
#         spark_df = spark_df.dropDuplicates(["order_id"])
#         # spark_df.cache()
#         context.log.info("Got Spark DataFrame")
#         # spark_df.unpersist()
#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_orders",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )


# @asset(
#     description="silver date",
#     ins={
#         "bronze_order": AssetIn(
#             key_prefix=["bronze", "order"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "date"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_date(context, bronze_order: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }
#     context.log.info("Got Spark DataFrame")
#     # spark_df.unpersist()
#     context.log.debug("Start creating spark session")
#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         date_df = bronze_order.select("order_purchase_timestamp").to_pandas()
#         context.log.debug(f"Converted to pandas DataFrame with shape: {date_df.shape}")
#         date_df = spark.createDataFrame(date_df)
#         date_df = date_df.na.drop()
#         date_df = date_df.dropDuplicates()
#         # spark_df.cache()
#         context.log.info("Got Spark DataFrame")
#         # spark_df.unpersist()

#         return Output(
#             value=date_df,
#             metadata={
#                 "table": "silver_date",
#                 "row_count": date_df.count(),
#                 "column_count": len(date_df.columns),
#                 "columns": date_df.columns,
#             },
#         )


# @asset(
#     description="Load 'geo' table from bronze layer in minIO, into a Spark dataframe, then clean data",
#     # partitions_def=YEARLY,
#     ins={
#         "bronze_geolocation": AssetIn(
#             key_prefix=["bronze", "geolocation"],
#         ),
#     },
#     io_manager_key="spark_io_manager",
#     key_prefix=["silver", "geolocation"],
#     compute_kind=COMPUTE_KIND,
#     group_name=LAYER,
# )
# def silver_cleaned_geolocation(context, bronze_geolocation: pl.DataFrame):
#     """
#     Load customers table from bronze layer in minIO, into a Spark dataframe, then clean data
#     """

#     config = {
#         "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#         "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#         "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     }

#     context.log.debug("Start creating spark session")

#     with get_spark_session(config, str(context.run.run_id).split("-")[0]) as spark:
#         # Convert bronze_book from polars DataFrame to Spark DataFrame
#         pandas_df = bronze_geolocation.to_pandas()
#         context.log.debug(
#             f"Converted to pandas DataFrame with shape: {pandas_df.shape}"
#         )
#         spark_df = spark.createDataFrame(pandas_df)
#         spark_df = spark_df.dropDuplicates()
#         # spark_df.cache()
#         spark_df = spark_df.na.drop()
#         # filter tọa độ cho đúng theo giới hạn của brazill
#         spark_df = spark_df.filter(
#             (col("geolocation_lat") <= 5.27438888)
#             & (col("geolocation_lng") >= -73.98283055)
#             & (col("geolocation_lat") >= -33.75116944)
#             & (col("geolocation_lng") <= -34.79314722)
#         )

#         context.log.info("Got Spark DataFrame")

#         # spark_df.unpersist()

#         return Output(
#             value=spark_df,
#             metadata={
#                 "table": "silver_cleaned_geolocation",
#                 "row_count": spark_df.count(),
#                 "column_count": len(spark_df.columns),
#                 "columns": spark_df.columns,
#             },
#         )

from dagster import asset, AssetIn, Output
import os
import polars as pl
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp,
    lit,
    max as spark_max,
)

# 1. Import Resource
from ..resources.spark_io_manager import get_spark_session
from .etl_job.insert_job_log import insert_job_log

# 2. Import Utils
from .etl_job.spark_utils import get_watermark_from_meta, update_watermark_meta

COMPUTE_KIND = "PySpark"
LAYER = "silver"


# ==============================================================================
# HELPER FUNCTION: INCREMENTAL LOAD LOGIC (Dùng chung cho tất cả các bảng)
# ==============================================================================
def _process_staging_load(
    context,
    bronze_df: pl.DataFrame,
    target_table: str,
    asset_key: str,
    source_name: str,
    watermark_col: str = "last_update",  # Cột để detect data mới
):
    """
    Hàm xử lý logic Staging chuẩn:
    1. Init Spark.
    2. Audit Log.
    3. Convert Polars -> Spark.
    4. Get Watermark -> Filter Data -> Calculate New Watermark.
    5. Save Data & Update Metadata.
    """
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }

    run_id = str(context.run.run_id).split("-")[0]
    meta_table = "silver.etl_watermarks"

    with get_spark_session(config, run_id) as spark:
        # 0. Audit Log
        insert_job_log(
            spark=spark,
            job_name=f"silver_{asset_key}",
            layer="silver",
            source=source_name,
            target_table=target_table,
            merge_key="N/A",
            load_mode="incremental_append",
            schedule="daily",
            owner="hung.nguyen",
            description=f"Ingest & Filter raw data from {source_name} to Staging History",
        )

        # 1. Convert Data: Polars -> Spark
        pandas_df = bronze_df.to_pandas()
        spark_df = spark.createDataFrame(pandas_df)
        total_input_rows = spark_df.count()

        # Ép kiểu cột watermark về Timestamp
        if watermark_col in spark_df.columns:
            spark_df = spark_df.withColumn(
                watermark_col, to_timestamp(col(watermark_col))
            )
        else:
            context.log.warning(
                f"⚠️ Column '{watermark_col}' not found in source. Skipping Incremental Filter."
            )

        # 2. LOGIC FILTER INCREMENTAL
        context.log.info(f"🕵️ Checking watermark for '{asset_key}'...")
        low_watermark = get_watermark_from_meta(
            spark, meta_table, asset_key, default_value="1900-01-01 00:00:00"
        )
        context.log.info(f"💧 Using Low Watermark: {low_watermark}")

        # Filter: Chỉ lấy dữ liệu mới hơn lần chạy trước
        if watermark_col in spark_df.columns:
            spark_df = spark_df.filter(col(watermark_col) > lit(str(low_watermark)))

        filtered_count = spark_df.count()
        context.log.info(
            f"🔍 Filtered Result: {filtered_count} rows (Skipped {total_input_rows - filtered_count})"
        )

        # 3. Tính toán New Watermark (Max của batch hiện tại)
        new_batch_watermark = None
        if filtered_count > 0 and watermark_col in spark_df.columns:
            try:
                new_batch_watermark = spark_df.agg(spark_max(watermark_col)).collect()[
                    0
                ][0]
            except Exception as e:
                context.log.warning(f"⚠️ Could not calculate new max watermark: {e}")

        # 4. Update Metadata (Nếu có data mới)
        if new_batch_watermark:
            context.log.info(f"💾 Updating metadata to: {new_batch_watermark}")
            update_watermark_meta(spark, meta_table, asset_key, new_batch_watermark)

        # 5. Add Audit Columns & Return
        spark_df = spark_df.withColumn("_ingested_at", current_timestamp()).withColumn(
            "_job_run_id", lit(run_id)
        )

        return Output(
            value=spark_df,
            metadata={
                "table": target_table,
                "type": "staging_history",
                "watermark_used": str(low_watermark),
                "new_watermark_saved": str(new_batch_watermark),
                "input_rows": total_input_rows,
                "appended_rows": filtered_count,
            },
        )


# ==============================================================================
# ASSET DEFINITIONS
# ==============================================================================


# 1. CUSTOMER (Giữ nguyên logic nhưng gọi hàm helper cho gọn)
@asset(
    name="silver_stg_customer",
    key_prefix=["silver", "customer"],
    io_manager_key="spark_io_manager",
    ins={"bronze_customer": AssetIn(key_prefix=["bronze", "customer"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_customer(context, bronze_customer: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_customer,
        target_table="silver.stg_customer",
        asset_key="stg_customer",
        source_name="bronze.customer",
        watermark_col="last_update",
    )


# 2. SELLER
@asset(
    name="silver_stg_seller",
    key_prefix=["silver", "seller"],
    io_manager_key="spark_io_manager",
    ins={"bronze_seller": AssetIn(key_prefix=["bronze", "seller"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_seller(context, bronze_seller: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_seller,
        target_table="silver.stg_seller",
        asset_key="stg_seller",
        source_name="bronze.seller",
        watermark_col="last_update",
    )


# 3. PRODUCT
@asset(
    name="silver_stg_product",
    key_prefix=["silver", "product"],
    io_manager_key="spark_io_manager",
    ins={"bronze_product": AssetIn(key_prefix=["bronze", "product"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_product(context, bronze_product: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_product,
        target_table="silver.stg_product",
        asset_key="stg_product",
        source_name="bronze.product",
        watermark_col="last_update",
    )


# 4. ORDER (Note: Orders thường dùng purchase_timestamp nếu không có last_update)
@asset(
    name="silver_stg_order",
    key_prefix=["silver", "order"],
    io_manager_key="spark_io_manager",
    ins={"bronze_order": AssetIn(key_prefix=["bronze", "order"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_order(context, bronze_order: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_order,
        target_table="silver.stg_order",
        asset_key="stg_order",
        source_name="bronze.order",
        # Nếu bảng bronze_order có cột last_update thì giữ nguyên
        # Nếu không, hãy đổi thành "order_purchase_timestamp"
        watermark_col="last_update",
    )


# 5. ORDER ITEM
@asset(
    name="silver_stg_order_item",
    key_prefix=["silver", "orderitem"],
    io_manager_key="spark_io_manager",
    ins={"bronze_order_item": AssetIn(key_prefix=["bronze", "orderitem"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_order_item(context, bronze_order_item: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_order_item,
        target_table="silver.stg_order_item",
        asset_key="stg_order_item",
        source_name="bronze.order_item",
        watermark_col="last_update",
    )


# 6. PAYMENT
@asset(
    name="silver_stg_payment",
    key_prefix=["silver", "payment"],
    io_manager_key="spark_io_manager",
    ins={"bronze_payment": AssetIn(key_prefix=["bronze", "payment"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_payment(context, bronze_payment: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_payment,
        target_table="silver.stg_payment",
        asset_key="stg_payment",
        source_name="bronze.payment",
        watermark_col="last_update",
    )


# 7. ORDER REVIEW
@asset(
    name="silver_stg_order_review",
    key_prefix=["silver", "orderreview"],
    io_manager_key="spark_io_manager",
    ins={"bronze_order_review": AssetIn(key_prefix=["bronze", "orderreview"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_order_review(context, bronze_order_review: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_order_review,
        target_table="silver.stg_order_review",
        asset_key="stg_order_review",
        source_name="bronze.order_review",
        watermark_col="last_update",
    )


# 8. PRODUCT CATEGORY
@asset(
    name="silver_stg_product_category",
    key_prefix=["silver", "productcategory"],
    io_manager_key="spark_io_manager",
    ins={"bronze_product_category": AssetIn(key_prefix=["bronze", "productcategory"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_product_category(context, bronze_product_category: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_product_category,
        target_table="silver.stg_product_category",
        asset_key="stg_product_category",
        source_name="bronze.product_category",
        watermark_col="last_update",
    )


# 9. GEOLOCATION
@asset(
    name="silver_stg_geolocation",
    key_prefix=["silver", "geolocation"],
    io_manager_key="spark_io_manager",
    ins={"bronze_geolocation": AssetIn(key_prefix=["bronze", "geolocation"])},
    group_name=LAYER,
    compute_kind=COMPUTE_KIND,
)
def silver_stg_geolocation(context, bronze_geolocation: pl.DataFrame):
    return _process_staging_load(
        context,
        bronze_geolocation,
        target_table="silver.stg_geolocation",
        asset_key="stg_geolocation",
        source_name="bronze.geolocation",
        watermark_col="last_update",
    )
