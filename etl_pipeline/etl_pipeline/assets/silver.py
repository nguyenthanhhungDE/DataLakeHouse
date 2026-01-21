from dagster import asset, AssetIn, Output
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import polars as pl

from dagster import asset, AssetIn, Output
import os
from pyspark.sql import DataFrame
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.functions import col, round, concat, md5, lit

from dagster import asset_check, AssetCheckResult


# Import resource
from ..resources.spark_io_manager import get_spark_session
from .etl_job.insert_job_log import insert_job_log

# Import Utility vừa tạo
from .etl_job.spark_utils import (
    get_watermark_from_meta,
    process_dedup_logic,
    calculate_merge_metrics,
    update_watermark_meta,
    get_source_keys_jdbc,  # Hàm đọc ID từ MySQL
    sync_deleted_records,  # Hàm xử lý xóa (Anti Join)
)

COMPUTE_KIND = "PySpark"
LAYER = "silver"


META_TABLE = "silver.etl_watermarks"
WATERMARK_COL = "_ingested_at"


# ==============================================================================
# HELPER: LẤY CẤU HÌNH KẾT NỐI MYSQL (CONNECTION ONLY)
# ==============================================================================
def get_base_mysql_config():
    """
    Trả về thông tin kết nối Server MySQL từ biến môi trường.
    """
    db_name = os.getenv("OLIST_DB_NAME", "olist")

    return {
        "url": f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{db_name}",
        "user": os.getenv("MYSQL_ROOT_USER", "root"),  # admin (theo file .env của bạn)
        "password": os.getenv("MYSQL_PASSWORD"),  # admin
        "driver": "com.mysql.cj.jdbc.Driver",
    }


# ==============================================================================
# WRAPPER FUNCTION (LOGIC XỬ LÝ CHUNG CHO CÁC BẢNG SILVER)
# ==============================================================================
def _process_silver_asset(
    context,
    df: DataFrame,
    target_table: str,
    asset_key: str,
    merge_key: str,
    transform_func=None,
    mysql_table: str = None,
    mysql_key: str = None,
):
    config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    run_id = str(context.run.run_id).split("-")[0]

    with get_spark_session(config, run_id) as spark:

        insert_job_log(
            spark=spark,
            job_name=f"silver_{asset_key}",
            layer="silver",
            source="staging",
            target_table=target_table,
            merge_key=merge_key,
            load_mode="upsert",
            schedule="daily",
            owner="hung.nguyen",
            description=f"Clean & Upsert {asset_key}",
        )

        # 1. WATERMARK & INCREMENTAL FILTER
        context.log.info(f"🕵️ Checking watermark in {META_TABLE} for {asset_key}...")
        high_watermark = get_watermark_from_meta(spark, META_TABLE, asset_key)

        deduped_df, new_batch_watermark = process_dedup_logic(
            spark, df, merge_key, WATERMARK_COL, high_watermark
        )

        # 2. TRANSFORM
        if transform_func:
            deduped_df = transform_func(deduped_df)

        # Thêm cột is_active mặc định là True cho data mới/update
        deduped_df = deduped_df.withColumn("is_active", lit(True))

        # 3. METRICS (UPSERT)
        inserts, updates = 0, 0
        if not deduped_df.rdd.isEmpty():
            inserts, updates = calculate_merge_metrics(
                spark, deduped_df, target_table, merge_key
            )
            context.log.info(f"📦 Upsert Batch: {deduped_df.count()} rows.")
        else:
            context.log.info("💤 No new incremental data.")

        # =================================================================
        # 4. SYNC DELETES (AUTO CONFIG TỪ MYSQL)
        # =================================================================
        deleted_count = 0
        if mysql_table and mysql_key:
            context.log.info(
                f"🔌 Preparing MySQL connection for table '{mysql_table}'..."
            )

            jdbc_config = get_base_mysql_config()
            jdbc_config["table"] = mysql_table
            jdbc_config["key"] = mysql_key

            # --- ĐIỂM SỬA ĐỔI QUAN TRỌNG ---
            # Truyền merge_key vào đây để hàm util tự đổi tên cột MySQL thành tên cột Silver
            full_source_keys = get_source_keys_jdbc(
                spark, jdbc_config, target_col_name=merge_key, logger=context.log
            )
            print(full_source_keys.limit(5))

            if full_source_keys:
                context.log.info("🗑 Syncing Deletes (Anti Join)...")
                deleted_count = sync_deleted_records(
                    spark, target_table, full_source_keys, merge_key, logger=context.log
                )
            else:
                context.log.warning(
                    f"⚠️ Could not fetch keys from MySQL table {mysql_table}."
                )

        # 5. UPDATE WATERMARK
        if new_batch_watermark:
            update_watermark_meta(spark, META_TABLE, asset_key, new_batch_watermark)

        return Output(
            value=deduped_df,
            metadata={
                "table": target_table,
                "type": "incremental_merge_delete",
                "watermark_used": str(high_watermark),
                "inserts": inserts,
                "updates": updates,
                "soft_deletes": deleted_count,
            },
        )


# ==============================================================================
# ASSET: SILVER CLEANED CUSTOMER
# ==============================================================================
@asset(
    description="Clean & Deduplicate Customers (Sync Delete from MySQL)",
    ins={"silver_stg_customer": AssetIn(key_prefix=["silver", "customer"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "customer"],
    name="silver_cleaned_customer",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "customer_id"},
)
def silver_cleaned_customer(context, silver_stg_customer: DataFrame):

    # Logic transform riêng cho Customer (giữ nguyên logic cũ của bạn)
    def transform_logic(df):
        return df.na.drop()

    return _process_silver_asset(
        context,
        df=silver_stg_customer,
        target_table="silver.clean_customer",
        asset_key="clean_customer",
        merge_key="customer_id",
        transform_func=transform_logic,
        # Cấu hình Sync Delete: Chỉ cần tên bảng và tên cột Key ở MySQL
        mysql_table="customers",
        mysql_key="customer_id",
    )


@asset(
    description="Clean, Deduplicate & Sync Delete for Sellers",
    ins={
        # Lưu ý: Input bây giờ là Staging (Spark DF) thay vì Bronze (Polars)
        # để tận dụng luồng Incremental Load
        "silver_stg_seller": AssetIn(key_prefix=["silver", "seller"])
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "seller"],
    name="silver_cleaned_seller",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "seller_id"},
)
def silver_cleaned_seller(context, silver_stg_seller: DataFrame):
    """
    Pipeline xử lý bảng Sellers:
    1. Lấy dữ liệu mới (Incremental).
    2. Clean & Dedup.
    3. Đồng bộ xóa từ MySQL (Hard Delete -> Soft Delete).
    """

    # Logic Transform riêng cho Seller (nếu có)
    def transform_logic(df):
        # Ví dụ: Xóa dòng null, chuẩn hóa city...
        return df.na.drop()

    return _process_silver_asset(
        context,
        df=silver_stg_seller,
        target_table="silver.clean_seller",
        asset_key="clean_seller",
        merge_key="seller_id",
        transform_func=transform_logic,
        # --- CẤU HÌNH SYNC DELETE ---
        mysql_table="sellers",  # Tên bảng gốc trong MySQL
        mysql_key="seller_id",  # Tên cột ID trong MySQL
        logger=context.log,  # Logger để hiện UI đẹp
    )


@asset_check(
    asset=silver_cleaned_customer, description="Check data quality: ID must not be Null"
)
def check_customer_id_not_null(context):
    # 1. Lấy Config (Giả sử bạn load từ biến môi trường hoặc file config chung)
    # Nếu bạn dùng resource config phức tạp, cần extract từ context.
    # Ở đây mình ví dụ config đơn giản để init spark.
    spark_config = {
        "endpoint_url": "minio:9000",
        "minio_access_key": "admin",
        "minio_secret_key": "password",
        # ... các config khác
    }

    # Tạo Run ID giả lập cho session check
    check_run_id = f"check_cust_{context.run_id[:8]}"

    # 2. Mở Spark Session MỚI (Session của Asset cũ đã đóng rồi nên OK)
    with get_spark_session(spark_config, run_id=check_run_id) as spark:

        # 3. Đọc dữ liệu: Dùng spark.table để chắc chắn đọc đúng bảng vừa ghi
        # Thay vì .load("path"), ta dùng .table("tên_bảng")
        df = spark.table("silver.clean_customer")

        # 4. Logic kiểm tra
        total_rows = df.count()
        null_count = df.filter("customer_id IS NULL").count()

        # Log ra UI cho dễ nhìn
        context.log.info(f"🔍 Checked {total_rows} rows. Found {null_count} null IDs.")

        # 5. Trả về kết quả
        return AssetCheckResult(
            passed=(null_count == 0),
            metadata={
                "null_row_count": null_count,
                "total_rows": total_rows,
                "target_table": "silver.clean_customer",
            },
        )


# ==============================================================================
# 3. PRODUCT ASSET (Updated)
# ==============================================================================
@asset(
    description="Clean, Cast Types & Sync Delete for Products",
    ins={"silver_stg_product": AssetIn(key_prefix=["silver", "product"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "product"],
    name="silver_cleaned_product",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "product_id"},
)
def silver_cleaned_product(context, silver_stg_product: DataFrame):
    """
    Pipeline xử lý bảng Products:
    1. Cast các cột kích thước sang Integer/Double.
    2. Đồng bộ xóa từ MySQL.
    """

    # Logic Transform đặc thù của Product
    def transform_logic(df):
        # 1. Xóa dữ liệu rác
        df = df.na.drop()

        # 2. Ép kiểu dữ liệu (Cast Types) như code cũ của bạn
        # Lưu ý: Kiểm tra tên cột kỹ lưỡng
        cols_to_int = [
            "product_description_length",
            "product_photos_qty",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]

        for c in cols_to_int:
            # Chỉ cast nếu cột đó tồn tại trong DF
            if c in df.columns:
                df = df.withColumn(c, col(c).cast("integer"))

        # Ví dụ: product_weight_g cũng nên là int hoặc double
        if "product_weight_g" in df.columns:
            df = df.withColumn(
                "product_weight_g", col("product_weight_g").cast("integer")
            )

        return df

    return _process_silver_asset(
        context,
        df=silver_stg_product,
        target_table="silver.clean_product",
        asset_key="clean_product",
        merge_key="product_id",
        transform_func=transform_logic,
        # --- CẤU HÌNH SYNC DELETE ---
        mysql_table="products",  # Tên bảng gốc trong MySQL
        mysql_key="product_id",  # Tên cột ID trong MySQL
        logger=context.log,  # Logger
    )


# ==============================================================================
# 4. ORDER ITEMS (Sử dụng Single Key: order_item_id)
# ==============================================================================
@asset(
    description="Clean & Deduplicate Order Items",
    ins={"silver_stg_order_item": AssetIn(key_prefix=["silver", "orderitem"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderitem"],
    name="silver_cleaned_order_item",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "order_item_id"},
)
def silver_cleaned_order_item(context, silver_stg_order_item: DataFrame):

    def transform_logic(df):
        # Cast và Round số liệu
        df = df.withColumn("price", round(col("price"), 2).cast("double")).withColumn(
            "freight_value", round(col("freight_value"), 2).cast("double")
        )

        # Không cần tạo cột composite key nữa
        return df.na.drop()

    return _process_silver_asset(
        context,
        df=silver_stg_order_item,
        target_table="silver.clean_order_item",
        asset_key="clean_order_item",
        # Merge Key là order_item_id
        merge_key="order_item_id",
        transform_func=transform_logic,
        # --- CẤU HÌNH SYNC DELETE ---
        mysql_table="order_items",
        mysql_key="order_item_id",  # Lấy trực tiếp cột này làm mốc so sánh
        logger=context.log,
    )


# ==============================================================================
# 5. PAYMENTS (MD5 Key Strategy)
# ==============================================================================
@asset(
    description="Clean Payments with MD5 Composite Key",
    ins={"silver_stg_payment": AssetIn(key_prefix=["silver", "payment"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "payment"],
    name="silver_cleaned_payment",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "pk_hash"},
)
def silver_cleaned_payment(context, silver_stg_payment: DataFrame):

    def transform_logic(df):
        df = df.withColumn(
            "payment_value", round(col("payment_value"), 2).cast("double")
        )
        if "payment_installments" in df.columns:
            df = df.withColumn(
                "payment_installments", col("payment_installments").cast("integer")
            )

        # --- TẠO MD5 KEY ---
        # Logic: MD5(order_id + payment_sequential)
        df = df.withColumn(
            "pk_hash", md5(concat(col("order_id"), col("payment_sequential")))
        )
        return df.na.drop()

    return _process_silver_asset(
        context,
        df=silver_stg_payment,
        target_table="silver.clean_payment",
        asset_key="clean_payment",
        merge_key="pk_hash",
        transform_func=transform_logic,
        # --- MYSQL CONFIG ---
        mysql_table="order_payments",
        mysql_key="MD5(CONCAT(order_id, payment_sequential))",
        logger=context.log,
    )


# ==============================================================================
# 6. ORDER REVIEWS
# ==============================================================================
@asset(
    description="Clean & Sync Delete for Order Reviews",
    ins={"silver_stg_order_review": AssetIn(key_prefix=["silver", "orderreview"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderreview"],
    name="silver_cleaned_order_review",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "review_id"},
)
def silver_cleaned_order_review(context, silver_stg_order_review: DataFrame):

    def transform_logic(df):
        # Drop cột title nếu cần thiết như logic cũ
        if "review_comment_title" in df.columns:
            df = df.drop("review_comment_title")
        return df.na.drop()

    return _process_silver_asset(
        context,
        df=silver_stg_order_review,
        target_table="silver.clean_order_review",
        asset_key="clean_order_review",
        merge_key="review_id",
        transform_func=transform_logic,
        # Cấu hình Sync Delete
        mysql_table="order_reviews",
        mysql_key="review_id",
        logger=context.log,
    )


# ==============================================================================
# 7. ORDERS
# ==============================================================================
@asset(
    description="Clean & Sync Delete for Orders",
    ins={"silver_stg_order": AssetIn(key_prefix=["silver", "order"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "order"],
    name="silver_cleaned_order",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "order_id"},
)
def silver_cleaned_order(context, silver_stg_order: DataFrame):

    return _process_silver_asset(
        context,
        df=silver_stg_order,
        target_table="silver.clean_order",
        asset_key="clean_order",
        merge_key="order_id",
        transform_func=lambda df: df.na.drop(),
        # Cấu hình Sync Delete
        mysql_table="orders",
        mysql_key="order_id",
        logger=context.log,
    )


# ==============================================================================
# 8. PRODUCT CATEGORY
# ==============================================================================
@asset(
    description="Clean Product Categories",
    ins={
        "silver_stg_product_category": AssetIn(key_prefix=["silver", "productcategory"])
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "productcategory"],
    name="silver_cleaned_product_category",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "product_category_name"},
)
def silver_cleaned_product_category(context, silver_stg_product_category: DataFrame):

    return _process_silver_asset(
        context,
        df=silver_stg_product_category,
        target_table="silver.clean_product_category",
        asset_key="clean_product_category",
        merge_key="product_category_name",
        transform_func=lambda df: df.na.drop(),
        # Bảng này thường là Static hoặc Translation, có thể không cần sync delete
        # Nếu muốn sync: mysql_table="product_category_name_translation"
        mysql_table=None,
        mysql_key=None,
        logger=context.log,
    )


# ==============================================================================
# 9. GEOLOCATION
# ==============================================================================
@asset(
    description="Clean Geolocation Data (Filter Brazil Bounds)",
    ins={"silver_stg_geolocation": AssetIn(key_prefix=["silver", "geolocation"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "geolocation"],
    name="silver_cleaned_geolocation",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "geolocation_zip_code_prefix"},
)
def silver_cleaned_geolocation(context, silver_stg_geolocation: DataFrame):

    def transform_logic(df):
        df = df.na.drop()
        # Filter tọa độ Brazil
        df = df.filter(
            (col("geolocation_lat") <= 5.27438888)
            & (col("geolocation_lng") >= -73.98283055)
            & (col("geolocation_lat") >= -33.75116944)
            & (col("geolocation_lng") <= -34.79314722)
        )
        return df

    return _process_silver_asset(
        context,
        df=silver_stg_geolocation,
        target_table="silver.clean_geolocation",
        asset_key="clean_geolocation",
        # Geo data trùng lặp rất nhiều, dedup theo zip code prefix
        merge_key="geolocation_zip_code_prefix",
        transform_func=transform_logic,
        # Geo data thường append-only hoặc static, ít khi delete từng dòng
        mysql_table="geolocation",
        mysql_key="geolocation_zip_code_prefix",
        logger=context.log,
    )


# ==============================================================================
# 10. DATE DIMENSION (Derived from Orders)
# ==============================================================================
@asset(
    description="Extract Date Dimension from Orders",
    # Dùng Staging Order làm input để lấy các ngày mới phát sinh
    ins={"silver_stg_order": AssetIn(key_prefix=["silver", "order"])},
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "date"],
    name="silver_date",
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    metadata={"merge_key": "order_purchase_timestamp"},
)
def silver_date(context, silver_stg_order: DataFrame):
    """
    Tạo bảng Date Dimension từ Order Purchase Timestamp.
    Không cần Sync Delete vì đây là bảng Dimension dẫn xuất.
    """

    def transform_logic(df):
        # Chỉ lấy cột timestamp distinct
        return df.select("order_purchase_timestamp").na.drop().distinct()

    return _process_silver_asset(
        context,
        df=silver_stg_order,
        target_table="silver.date_dimension",
        asset_key="date_dimension",
        merge_key="order_purchase_timestamp",
        transform_func=transform_logic,
        # Không sync delete
        mysql_table=None,
        mysql_key=None,
        logger=context.log,
    )
