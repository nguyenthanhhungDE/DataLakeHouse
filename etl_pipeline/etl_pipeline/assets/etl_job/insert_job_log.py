from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def insert_job_log(
    spark,
    job_name: str,
    layer: str,
    source: str,
    target_table: str,
    merge_key: str,
    load_mode: str,
    schedule: str,
    owner: str,
    description: str,
):
    now = datetime.now()

    # --- BƯỚC 1: ĐẢM BẢO DATABASE VÀ TABLE TỒN TẠI ---
    # Tạo database nếu chưa có
    spark.sql("CREATE DATABASE IF NOT EXISTS metadata")

    # Tạo bảng log nếu chưa có (Schema khớp với dữ liệu bên dưới)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS metadata.etl_job (
            job_id LONG,
            job_name STRING,
            layer STRING,
            source STRING,
            target_table STRING,
            merge_key STRING,
            load_mode STRING,
            schedule STRING,
            owner STRING,
            description STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
    """
    )

    # --- BƯỚC 2: TÍNH TOÁN JOB_ID ---
    # Bây giờ bảng đã chắc chắn tồn tại, câu lệnh này sẽ chạy an toàn
    try:
        result = spark.sql(
            "SELECT COALESCE(MAX(job_id), 0) AS max_id FROM metadata.etl_job"
        ).collect()
        next_id = result[0]["max_id"] + 1
    except Exception:
        # Fallback phòng trường hợp lỗi lạ, mặc định là 1
        next_id = 1

    # --- BƯỚC 3: CHUẨN BỊ DỮ LIỆU ---
    data = [
        (
            next_id,
            job_name,
            layer,
            source,
            target_table,
            merge_key,
            load_mode,
            schedule,
            owner,
            description,
            now,
            now,
        )
    ]

    cols = [
        "job_id",
        "job_name",
        "layer",
        "source",
        "target_table",
        "merge_key",
        "load_mode",
        "schedule",
        "owner",
        "description",
        "created_at",
        "updated_at",
    ]

    # --- BƯỚC 4: THỰC HIỆN MERGE (UPSERT) ---
    df = spark.createDataFrame(data, cols)
    df.createOrReplaceTempView("new_job")

    # Logic Merge của bạn rất tốt: cập nhật nếu đã có job, insert nếu chưa có
    # Tuy nhiên, cần lưu ý: Nếu update, ta giữ nguyên created_at cũ, chỉ update updated_at mới
    spark.sql(
        """
        MERGE INTO metadata.etl_job AS tgt
        USING new_job AS src
        ON tgt.job_name = src.job_name AND tgt.layer = src.layer
        WHEN MATCHED THEN UPDATE SET
            tgt.source = src.source,
            tgt.target_table = src.target_table,
            tgt.merge_key = src.merge_key,
            tgt.load_mode = src.load_mode,
            tgt.schedule = src.schedule,
            tgt.owner = src.owner,
            tgt.description = src.description,
            tgt.updated_at = src.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    print(f"✅ Upsert job '{job_name}' vào metadata.etl_job thành công.")
