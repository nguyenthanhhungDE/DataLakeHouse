from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame

from contextlib import contextmanager
from datetime import datetime
from delta.tables import DeltaTable
import json


@contextmanager
def get_spark_session(config, run_id="Spark IO Manager"):
    executor_memory = "1g" if run_id != "Spark IO Manager" else "1500m"
    try:
        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(run_id)
            .config(
                "spark.jars",
                "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,/usr/local/spark/jars/hadoop-aws-3.3.2.jar,/usr/local/spark/jars/delta-storage-2.2.0.jar,/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,/usr/local/spark/jars/s3-2.18.41.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['endpoint_url']}")
            .config("spark.hadoop.fs.s3a.access.key", str(config["minio_access_key"]))
            .config("spark.hadoop.fs.s3a.secret.key", str(config["minio_secret_key"]))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.sql.warehouse.dir", f"s3a://lakehouse/")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.sql.catalogImplementation", "hive")
            .enableHiveSupport()
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    # def handle_output(self, context: OutputContext, obj):
    #     """
    #     Enhanced version:
    #     - If asset returns (df, merge_key): do merge
    #     - If asset returns df only: overwrite (full load)
    #     """
    #     context.log.info("🚀 [SparkIOManager] Handling output ...")

    #     # 🧩 Phân tách input
    #     if isinstance(obj, tuple):
    #         df, merge_key = obj
    #     else:
    #         df, merge_key = obj, None

    #     layer, _, table = context.asset_key.path
    #     table_name = table.replace(f"{layer}_", "")
    #     table_full = f"{layer}.{table_name}"

    #     with get_spark_session(self._config, run_id=f"{layer}_{table_name}") as spark:
    #         try:
    #             if not self._table_exists(spark, table_full):
    #                 context.log.info(
    #                     f"🆕 Table {table_full} chưa tồn tại → full load (overwrite)."
    #                 )
    #                 df.write.format("delta").mode("overwrite").saveAsTable(table_full)
    #                 context.log.info(f"✅ Created table {table_full} successfully.")
    #             else:
    #                 if merge_key:
    #                     context.log.info(
    #                         f"🔁 Table {table_full} tồn tại → incremental merge theo key '{merge_key}'."
    #                     )

    #                     # Lấy Delta table
    #                     delta_table = DeltaTable.forName(spark, table_full)

    #                     # Điều kiện merge
    #                     condition = f"tgt.{merge_key} = src.{merge_key}"

    #                     # Thực hiện merge
    #                     (
    #                         delta_table.alias("tgt")
    #                         .merge(df.alias("src"), condition)
    #                         .whenMatchedUpdateAll()
    #                         .whenNotMatchedInsertAll()
    #                         .execute()
    #                     )

    #                     # ✅ Lấy thông tin metrics từ Delta transaction log
    #                     history_df = spark.sql(f"DESCRIBE HISTORY {table_full}")
    #                     last_op = history_df.select("operationMetrics").head()[0]

    #                     if isinstance(last_op, str):
    #                             metrics = json.loads(last_op)
    #                     elif isinstance(last_op, dict):
    #                             metrics = last_op
    #                     else:
    #                             metrics = {}

    #                     inserted = metrics.get("numTargetRowsInserted", 0)
    #                     updated = metrics.get("numTargetRowsUpdated", 0)
    #                     deleted = metrics.get("numTargetRowsDeleted", 0)

    #                     total_changed = int(inserted) + int(updated)
    #                     if total_changed == 0:
    #                             context.log.warn(f"⚠️ Không có bản ghi nào được thay đổi trong {table_full}.")
    #                     else:
    #                         context.log.info(
    #                                 f"✅ Merge completed for {table_full} → "
    #                                 f"Inserted: {inserted}, Updated: {updated}, Deleted: {deleted}"
    #                             )

    #                 else:
    #                     context.log.warning(
    #                         f"⚠️ No merge_key provided → fallback to overwrite."
    #                     )
    #                     df.write.format("delta").mode("overwrite").saveAsTable(
    #                         table_full
    #                     )
    #         except Exception as e:
    #             raise Exception(
    #                 f"(Spark handle_output) Error while writing output: {e}"
    #             )

    def _table_exists(self, spark, table_name: str) -> bool:
        """Check if Delta table exists in Hive Metastore"""
        try:
            spark.table(table_name)
            return True
        except Exception:
            return False

    def load_input(self, context: InputContext) -> DataFrame:
        """
        Load input from s3a (aka minIO) from parquet file to spark.sql.DataFrame
        """

        # E.g context.asset_key.path: ['silver', 'goodreads', 'book']
        context.log.debug(f"Loading input from {context.asset_key.path}...")
        layer, _, table = context.asset_key.path
        table_name = str(table.replace(f"{layer}_", ""))
        context.log.debug(f"loading input from {layer} layer - table {table_name}...")

        try:
            with get_spark_session(self._config) as spark:
                df = None
                df = spark.read.table(f"{layer}.{table_name}")
                context.log.debug(f"Loaded {df.count()} rows from {table_name}")
                return df
        except Exception as e:
            raise Exception(f"Error while loading input: {e}")

    # 🔹 NEW FUNCTION: chỉ dùng cho layer "silver"
    # def handle_output_silver_layer(self, context: OutputContext, df: DataFrame):
    #     """
    #     Silver layer: chỉ append dữ liệu mới, không merge, không overwrite
    #     """
    #     layer, _, table = context.asset_key.path
    #     table_name = table.replace(f"{layer}_", "")
    #     table_full = f"{layer}.{table_name}"

    #     with get_spark_session(self._config, run_id=f"{layer}_{table_name}") as spark:
    #         try:
    #             if not self._table_exists(spark, table_full):

    #                 context.log.info(
    #                     f"🆕 Table {table_full} chưa tồn tại → tạo mới bằng full load (overwrite)."
    #                 )
    #                 df.write.format("delta").mode("overwrite").saveAsTable(table_full)
    #                 context.log.info(f"✅ Created table {table_full} successfully.")
    #             else:
    #                 before_count = spark.table(table_full).count()
    #                 append_count = df.count()

    #                 context.log.info(f"➕ Append {append_count} bản ghi vào {table_full} (trước đó có {before_count}).")
    #                 df.write.format("delta").mode("append").saveAsTable(table_full)

    #                 after_count = spark.table(table_full).count()
    #                 context.log.info(
    #                     f"✅ Append completed for {table_full}. "
    #                     f"Tổng số bản ghi sau append: {after_count} (tăng {after_count - before_count})."
    #                 )
    #         except Exception as e:
    #             raise Exception(f"(Spark handle_output_silver_layer) Error: {e}")

    def handle_output(self, context, obj):
        """
        Dispatcher — chọn đúng hàm xử lý theo layer (bronze/silver/gold)
        """
        asset_key = (
            context.asset_key.path
        )  # Ví dụ: ['silver', 'customer', 'silver_cleaned_customer']
        layer = asset_key[0] if asset_key else None

        if layer == "silver":
            return self.handle_output_silver_layer(context, obj)
        elif layer == "bronze":
            return self.handle_output_bronze_layer(context, obj)
        elif layer == "gold":
            return self.handle_output_gold_layer(context, obj)
        else:
            raise Exception(f"⚠️ Unknown layer for asset: {asset_key}")

    def handle_output_silver_layer(self, context, df):
        """
        Ghi dữ liệu Spark DataFrame vào Delta table trong layer 'silver'
        (3 cấp: layer/domain/table). Phân biệt loại bảng theo tên asset.
        """
        path_parts = (
            context.asset_key.path
        )  # ví dụ: ['silver', 'customer', 'silver_cleaned_customer']

        if len(path_parts) != 3:
            raise ValueError(
                f"Unexpected asset key path: {path_parts}. Expected ['silver', 'domain', 'asset_name']."
            )

        layer, domain, asset_name = path_parts

        # Tách tên hàm asset để lấy loại bảng (vd: 'cleaned' trong 'silver_cleaned_customer')
        name_parts = asset_name.split("_")
        if len(name_parts) >= 3:
            _, table_type, table_base = name_parts  # ['silver', 'cleaned', 'customer']
        elif len(name_parts) == 2:
            _, table_base = name_parts
            table_type = "stg"  # default
        else:
            raise ValueError(f"Unexpected asset name pattern: {asset_name}")

        # Sinh tên bảng chuẩn
        table_full = f"{layer}.{domain}_{table_type}_{table_base}"
        run_id = f"{layer}_{table_type}_{table_base}"

        context.log.info(f"🧩 Saving to table: {table_full} | run_id={run_id}")

        with get_spark_session(self._config, run_id=run_id) as spark:
            try:
                if not self._table_exists(spark, table_full):
                    context.log.info(
                        f"🆕 Table {table_full} chưa tồn tại → tạo mới bằng full load (overwrite)."
                    )
                    df.write.format("delta").mode("overwrite").saveAsTable(table_full)
                    context.log.info(f"✅ Created table {table_full} successfully.")
                else:
                    before_count = spark.table(table_full).count()
                    append_count = df.count()

                    context.log.info(
                        f"➕ Append {append_count} bản ghi vào {table_full} (trước đó có {before_count})."
                    )

                    df.write.format("delta").mode("append").saveAsTable(table_full)

                    after_count = spark.table(table_full).count()
                    context.log.info(
                        f"✅ Append completed for {table_full}. "
                        f"Tổng số bản ghi sau append: {after_count} (tăng {after_count - before_count})."
                    )

            except Exception as e:
                raise Exception(f"(Spark handle_output_silver_layer) Error: {e}")
