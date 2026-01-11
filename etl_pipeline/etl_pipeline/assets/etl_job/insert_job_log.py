from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from datetime import datetime


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

    # Lấy giá trị job_id lớn nhất hiện có
    result = spark.sql(
        "SELECT COALESCE(MAX(job_id), 0) AS max_id FROM metadata.etl_job"
    ).collect()
    next_id = result[0]["max_id"] + 1

    # data = [(next_id, job_name, layer, source, target_table, merge_key,
    #          load_mode, schedule, owner, description, now, now)]

    # columns = [
    #     "job_id", "job_name", "layer", "source", "target_table",
    #     "merge_key", "load_mode", "schedule", "owner", "description",
    #     "created_at", "updated_at"
    # ]

    # df = spark.createDataFrame(data, columns)

    # df.write.format("delta").mode("append").saveAsTable("metadata.etl_job")

    # print(f"✅ Inserted job '{job_name}' (ID={next_id}) vào bảng metadata.etl_job thành công.")

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
    df = spark.createDataFrame(data, cols)
    df.createOrReplaceTempView("new_job")

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
