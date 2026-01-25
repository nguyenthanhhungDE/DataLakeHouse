
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
