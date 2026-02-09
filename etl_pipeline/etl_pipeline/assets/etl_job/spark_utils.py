from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp, lit


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable


def get_watermark_from_meta(
    spark: SparkSession,
    meta_table: str,
    asset_key: str,
    default_value="1900-01-01 00:00:00",
):
    """
    Lấy watermark. Nếu chưa có trong DB (lần đầu chạy), trả về default_value (Low Watermark).
    """
    try:
        # 1. Đảm bảo bảng Metadata tồn tại (Idempotent)
        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {meta_table} (
                asset_key STRING,
                watermark_value TIMESTAMP,
                last_updated TIMESTAMP
            ) USING DELTA
        """
        )

        # 2. Query lấy watermark của asset_key cụ thể
        df = spark.sql(
            f"SELECT watermark_value FROM {meta_table} WHERE asset_key = '{asset_key}'"
        )
        rows = df.collect()

        # 3. Check kết quả
        if rows and rows[0]["watermark_value"]:
            # Nếu đã có watermark -> Trả về giá trị Timestamp từ DB
            return rows[0]["watermark_value"]

        # 4. Nếu KHÔNG tìm thấy (Empty list) -> Trả về Default (1900)
        print(
            f"ℹ️ No watermark record found for '{asset_key}'. Using default: {default_value}"
        )
        return default_value

    except Exception as e:
        # Nếu lỗi (ví dụ chưa có quyền tạo bảng) -> Vẫn trả về Default để job chạy tiếp (Full Load)
        print(f"⚠️ Error reading metadata table: {e}. Defaulting to {default_value}")
        return default_value


def update_watermark_meta(
    spark: SparkSession, meta_table: str, asset_key: str, new_watermark
):
    """
    Cập nhật (Upsert) watermark mới vào bảng Metadata sau khi xử lý xong.
    """
    if not new_watermark:
        return

    # Tạo DataFrame chứa thông tin mới
    new_data = [(asset_key, new_watermark)]
    df = spark.createDataFrame(new_data, ["asset_key", "watermark_value"]).withColumn(
        "last_updated", current_timestamp()
    )

    # Upsert vào bảng Metadata (Dùng Merge)
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, meta_table):
        delta_table = DeltaTable.forName(spark, meta_table)
        (
            delta_table.alias("t")
            .merge(df.alias("s"), "t.asset_key = s.asset_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Fallback tạo mới nếu chưa là Delta Table (lần đầu)
        df.write.format("delta").mode("append").saveAsTable(meta_table)


def process_dedup_logic(
    spark: SparkSession,
    df: DataFrame,
    merge_key: str,
    watermark_col: str,
    high_watermark=None,
):
    """
    Logic chung: Filter Data Mới -> Deduplicate lấy dòng mới nhất.
    """
    input_df = df

    # 1. Filter Incremental
    if high_watermark:
        input_df = df.filter(col(watermark_col) > high_watermark)

    if input_df.rdd.isEmpty():
        return input_df, None  # Không có data mới

    # 2. Tính toán Watermark mới (Max của batch hiện tại)
    # Để cập nhật vào metadata table sau này
    new_max_watermark = input_df.agg(spark_max(watermark_col)).collect()[0][0]

    # 3. Deduplicate Logic (SQL Style)
    view_name = f"v_temp_{merge_key}"
    input_df.createOrReplaceTempView(view_name)

    dedup_sql = f"""
        SELECT * FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY {merge_key} ORDER BY {watermark_col} DESC) as rn
            FROM {view_name}
        ) t WHERE rn = 1
    """
    deduped_df = spark.sql(dedup_sql).drop("rn")

    return deduped_df, new_max_watermark


def calculate_merge_metrics(
    spark: SparkSession, source_df: DataFrame, target_table: str, merge_key: str
):
    """
    Tính toán trước số dòng Insert/Update.
    """
    try:
        if not spark.catalog.tableExists(target_table):
            return source_df.count(), 0  # Table chưa có -> Toàn bộ là Insert

        source_count = source_df.count()
        if source_count == 0:
            return 0, 0

        # Tạo view cho source
        source_df.createOrReplaceTempView("v_metrics_source")

        # Đếm Update (Inner Join)
        sql = f"""
            SELECT COUNT(1) as cnt 
            FROM v_metrics_source s
            INNER JOIN {target_table} t ON s.{merge_key} = t.{merge_key}
        """
        updates = spark.sql(sql).collect()[0]["cnt"]
        inserts = source_count - updates

        return inserts, updates
    except Exception as e:
        print(f"Metrics Error: {e}")
        return -1, -1


# ==============================================================================
# HÀM 1: GET SOURCE KEYS (CÓ LOGGER)
# ==============================================================================
def get_source_keys_jdbc(
    spark: SparkSession,
    config: dict,
    target_col_name: str = None,
    logger=None,
):
    """
    Kết nối MySQL qua JDBC và lấy danh sách Key.
    """

    # Helper để log: Nếu có logger thì dùng logger, không thì print
    def log_msg(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    try:
        # 1. Lấy thông tin cấu hình
        jdbc_url = config.get("url")
        user = config.get("user")
        password = config.get("password")
        table_name = config.get("table")
        source_key_column = config.get("key")
        driver = config.get("driver", "com.mysql.cj.jdbc.Driver")

        # 2. Query tối ưu
        query = f"(SELECT {source_key_column} FROM {table_name}) AS t"

        log_msg(f"🔌 JDBC Connecting to {table_name}...")

        # Chỉ print user/url ra console để debug, hạn chế log pass lên UI
        if not logger:
            print(f"   - URL: {jdbc_url}")

        # 3. Đọc dữ liệu
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .option("fetchsize", "10000")
            .load()
        )

        # 4. Đổi tên cột
        final_col_name = target_col_name if target_col_name else source_key_column
        if source_key_column != final_col_name:
            log_msg(f"   - Renaming column '{source_key_column}' -> '{final_col_name}'")
            df = df.withColumnRenamed(source_key_column, final_col_name)

        return df

    except Exception as e:
        error_msg = f"❌ CRITICAL ERROR reading JDBC: {e}"
        if logger:
            logger.error(error_msg)
        else:
            print(error_msg)
        raise e


# ==============================================================================
# HÀM 2: SYNC DELETED RECORDS (CÓ LOGGER & SCHEMA STRING)
# ==============================================================================
def sync_deleted_records(
    spark: SparkSession,
    target_table: str,
    full_source_df: DataFrame,
    merge_key: str,
    logger=None,
):
    """
    Đồng bộ dữ liệu xóa.
    TÍNH NĂNG MỚI: Tự động thêm cột 'is_active' nếu bảng cũ chưa có.
    """

    def log_info(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    def log_error(msg):
        if logger:
            logger.error(msg)
        else:
            print(f"ERROR: {msg}")

    try:
        # 1. KIỂM TRA BẢNG CÓ TỒN TẠI KHÔNG (Dùng SQL Show Tables)
        table_exists = False
        if "." in target_table:
            db_name, table_name = target_table.split(".", 1)
            db_name = db_name.replace("`", "")
            table_name = table_name.replace("`", "")
            count = spark.sql(f"SHOW TABLES IN {db_name} LIKE '{table_name}'").count()
            table_exists = count > 0
        else:
            count = spark.sql(f"SHOW TABLES LIKE '{target_table}'").count()
            table_exists = count > 0

        if not table_exists:
            log_info(
                f"ℹ️ Target table '{target_table}' does not exist. Skip delete sync."
            )
            return 0

        # ======================================================================
        # 2. AUTO-MIGRATION: TỰ ĐỘNG THÊM CỘT IS_ACTIVE NẾU THIẾU
        # ======================================================================
        # Lấy danh sách cột hiện tại của bảng đích
        current_cols = spark.read.table(target_table).columns

        if "is_active" not in current_cols:
            log_info(
                f"⚠️ Column 'is_active' missing in {target_table}. Starting Auto-Migration..."
            )

            # Bước A: Thêm cột is_active vào cấu trúc bảng (Delta Lake)
            log_info(
                f"🔨 Executing: ALTER TABLE {target_table} ADD COLUMNS (is_active BOOLEAN)"
            )
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS (is_active BOOLEAN)")

            # Bước B: Cập nhật dữ liệu cũ -> Set is_active = true (Mặc định)
            # Lưu ý: Lệnh UPDATE này có thể tốn thời gian nếu bảng rất lớn
            log_info(f"🔨 Executing: UPDATE {target_table} SET is_active = true")
            spark.sql(f"UPDATE {target_table} SET is_active = true")

            log_info(f"✅ Auto-Migration completed. Table schema updated.")
        # ======================================================================

        # 3. LẤY DỮ LIỆU TARGET (CHỈ LẤY ACTIVE)
        # Lúc này chắc chắn đã có cột is_active nên không bị lỗi nữa
        target_df = (
            spark.read.table(target_table).filter("is_active = true").select(merge_key)
        )

        # In thông tin debug lên UI
        target_schema_str = target_df._jdf.schema().treeString()
        target_count = target_df.count()

        log_info(
            f"📋 SILVER TARGET ({target_table}):\n"
            f"   - Active Keys: {target_count}\n"
            f"   - Schema:\n{target_schema_str}"
        )

        # 4. LẤY DỮ LIỆU SOURCE
        source_keys = full_source_df.select(merge_key).distinct()
        source_count = source_keys.count()
        log_info(f"📊 MySQL Source Keys: {source_count}")

        # 5. TÌM ID CẦN XÓA (Anti Join)
        ids_to_delete = target_df.join(source_keys, on=merge_key, how="left_anti")
        delete_count = ids_to_delete.count()

        if delete_count == 0:
            log_info(
                f"✅ No records to delete via Anti-Join.\n"
                f"   (Source={source_count}, Target={target_count})"
            )
            return 0

        log_info(
            f"🗑 DETECTED: Found {delete_count} records to Soft Delete in {target_table}..."
        )

        # 6. THỰC HIỆN UPDATE (MERGE DELETE)
        delta_table = DeltaTable.forName(spark, target_table)

        (
            delta_table.alias("t")
            .merge(ids_to_delete.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenMatchedUpdate(
                set={"is_active": "false", "_ingested_at": "current_timestamp()"}
            )
            .execute()
        )

        log_info(f"✅ Soft Delete committed successfully.")
        return delete_count

    except Exception as e:
        log_error(f"⚠️ Error syncing deletes: {e}")
        # Return -1 để biết là có lỗi nhưng không làm crash pipeline
        return -1
