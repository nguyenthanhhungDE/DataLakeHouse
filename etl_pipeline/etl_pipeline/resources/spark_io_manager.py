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
        # spark = (
        #     SparkSession.builder.master("spark://spark-master:7077")
        #     .appName(run_id)
        #     # .config(
        #     #     "spark.jars",
        #     #     "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,/usr/local/spark/jars/hadoop-aws-3.3.2.jar,/usr/local/spark/jars/delta-storage-2.2.0.jar,/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,/usr/local/spark/jars/s3-2.18.41.jar,/usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
        #     # )
        #     # .config(
        #     #     "spark.sql.catalog.spark_catalog",
        #     #     "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        #     # )
        #     .config(
        #         "spark.jars",
        #         "/opt/spark/jars/delta-core_2.12-2.3.0.jar,/opt/spark/jars/delta-storage-2.3.0.jar",
        #     )
        #     .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
        #     .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
        #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        #     .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['endpoint_url']}")
        #     .config("spark.hadoop.fs.s3a.access.key", str(config["minio_access_key"]))
        #     .config("spark.hadoop.fs.s3a.secret.key", str(config["minio_secret_key"]))
        #     .config("spark.hadoop.fs.s3a.path.style.access", "true")
        #     .config("spark.hadoop.fs.connection.ssl.enabled", "false")
        #     .config(
        #         "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        #     )
        #     .config(
        #         "spark.hadoop.fs.s3a.aws.credentials.provider",
        #         "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        #     )
        #     .config("spark.sql.warehouse.dir", f"s3a://lakehouse/")
        #     .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        #     .config("spark.sql.catalogImplementation", "hive")
        #     .enableHiveSupport()
        #     .getOrCreate()
        # )

        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(run_id)
            # 1. Khai báo JARs: Nên thêm cả AWS Bundle vào đây để Spark khởi tạo S3A ngay từ đầu
            .config(
                "spark.jars",
                "/opt/spark/jars/delta-core_2.12-2.3.0.jar,"
                "/opt/spark/jars/delta-storage-2.3.0.jar,"
                "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            # 2. CẤU HÌNH QUAN TRỌNG NHẤT: Ép Spark sử dụng Delta Catalog cho Hive Metastore
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # 3. ClassPath cho Driver và Executor
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
            .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
            # 4. Delta Extensions
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # 5. Cấu hình kết nối MinIO (S3A)
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
            # 6. Warehouse và Metastore
            .config("spark.sql.warehouse.dir", "s3a://lakehouse/")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.sql.catalogImplementation", "hive")
            .enableHiveSupport()
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


# class SparkIOManager(IOManager):
#     def __init__(self, config):
#         self._config = config

#     # def handle_output(self, context: OutputContext, obj):
#     #     """
#     #     Enhanced version:
#     #     - If asset returns (df, merge_key): do merge
#     #     - If asset returns df only: overwrite (full load)
#     #     """
#     #     context.log.info("🚀 [SparkIOManager] Handling output ...")

#     #     # 🧩 Phân tách input
#     #     if isinstance(obj, tuple):
#     #         df, merge_key = obj
#     #     else:
#     #         df, merge_key = obj, None

#     #     layer, _, table = context.asset_key.path
#     #     table_name = table.replace(f"{layer}_", "")
#     #     table_full = f"{layer}.{table_name}"

#     #     with get_spark_session(self._config, run_id=f"{layer}_{table_name}") as spark:
#     #         try:
#     #             if not self._table_exists(spark, table_full):
#     #                 context.log.info(
#     #                     f"🆕 Table {table_full} chưa tồn tại → full load (overwrite)."
#     #                 )
#     #                 df.write.format("delta").mode("overwrite").saveAsTable(table_full)
#     #                 context.log.info(f"✅ Created table {table_full} successfully.")
#     #             else:
#     #                 if merge_key:
#     #                     context.log.info(
#     #                         f"🔁 Table {table_full} tồn tại → incremental merge theo key '{merge_key}'."
#     #                     )

#     #                     # Lấy Delta table
#     #                     delta_table = DeltaTable.forName(spark, table_full)

#     #                     # Điều kiện merge
#     #                     condition = f"tgt.{merge_key} = src.{merge_key}"

#     #                     # Thực hiện merge
#     #                     (
#     #                         delta_table.alias("tgt")
#     #                         .merge(df.alias("src"), condition)
#     #                         .whenMatchedUpdateAll()
#     #                         .whenNotMatchedInsertAll()
#     #                         .execute()
#     #                     )

#     #                     # ✅ Lấy thông tin metrics từ Delta transaction log
#     #                     history_df = spark.sql(f"DESCRIBE HISTORY {table_full}")
#     #                     last_op = history_df.select("operationMetrics").head()[0]

#     #                     if isinstance(last_op, str):
#     #                             metrics = json.loads(last_op)
#     #                     elif isinstance(last_op, dict):
#     #                             metrics = last_op
#     #                     else:
#     #                             metrics = {}

#     #                     inserted = metrics.get("numTargetRowsInserted", 0)
#     #                     updated = metrics.get("numTargetRowsUpdated", 0)
#     #                     deleted = metrics.get("numTargetRowsDeleted", 0)

#     #                     total_changed = int(inserted) + int(updated)
#     #                     if total_changed == 0:
#     #                             context.log.warn(f"⚠️ Không có bản ghi nào được thay đổi trong {table_full}.")
#     #                     else:
#     #                         context.log.info(
#     #                                 f"✅ Merge completed for {table_full} → "
#     #                                 f"Inserted: {inserted}, Updated: {updated}, Deleted: {deleted}"
#     #                             )

#     #                 else:
#     #                     context.log.warning(
#     #                         f"⚠️ No merge_key provided → fallback to overwrite."
#     #                     )
#     #                     df.write.format("delta").mode("overwrite").saveAsTable(
#     #                         table_full
#     #                     )
#     #         except Exception as e:
#     #             raise Exception(
#     #                 f"(Spark handle_output) Error while writing output: {e}"
#     #             )

#     def _table_exists(self, spark, table_name: str) -> bool:
#         """Check if Delta table exists in Hive Metastore"""
#         try:
#             spark.table(table_name)
#             return True
#         except Exception:
#             return False

#     def load_input(self, context: InputContext) -> DataFrame:
#         """
#         Load input from s3a (aka minIO) from parquet file to spark.sql.DataFrame
#         """

#         # E.g context.asset_key.path: ['silver', 'goodreads', 'book']
#         context.log.debug(f"Loading input from {context.asset_key.path}...")
#         layer, _, table = context.asset_key.path
#         table_name = str(table.replace(f"{layer}_", ""))
#         context.log.debug(f"loading input from {layer} layer - table {table_name}...")

#         try:
#             with get_spark_session(self._config) as spark:
#                 df = None
#                 df = spark.read.table(f"{layer}.{table_name}")
#                 context.log.debug(f"Loaded {df.count()} rows from {table_name}")
#                 return df
#         except Exception as e:
#             raise Exception(f"Error while loading input: {e}")

#     # 🔹 NEW FUNCTION: chỉ dùng cho layer "silver"
#     # def handle_output_silver_layer(self, context: OutputContext, df: DataFrame):
#     #     """
#     #     Silver layer: chỉ append dữ liệu mới, không merge, không overwrite
#     #     """
#     #     layer, _, table = context.asset_key.path
#     #     table_name = table.replace(f"{layer}_", "")
#     #     table_full = f"{layer}.{table_name}"

#     #     with get_spark_session(self._config, run_id=f"{layer}_{table_name}") as spark:
#     #         try:
#     #             if not self._table_exists(spark, table_full):

#     #                 context.log.info(
#     #                     f"🆕 Table {table_full} chưa tồn tại → tạo mới bằng full load (overwrite)."
#     #                 )
#     #                 df.write.format("delta").mode("overwrite").saveAsTable(table_full)
#     #                 context.log.info(f"✅ Created table {table_full} successfully.")
#     #             else:
#     #                 before_count = spark.table(table_full).count()
#     #                 append_count = df.count()

#     #                 context.log.info(f"➕ Append {append_count} bản ghi vào {table_full} (trước đó có {before_count}).")
#     #                 df.write.format("delta").mode("append").saveAsTable(table_full)

#     #                 after_count = spark.table(table_full).count()
#     #                 context.log.info(
#     #                     f"✅ Append completed for {table_full}. "
#     #                     f"Tổng số bản ghi sau append: {after_count} (tăng {after_count - before_count})."
#     #                 )
#     #         except Exception as e:
#     #             raise Exception(f"(Spark handle_output_silver_layer) Error: {e}")

#     def handle_output(self, context, obj):
#         """
#         Dispatcher — chọn đúng hàm xử lý theo layer (bronze/silver/gold)
#         """
#         asset_key = (
#             context.asset_key.path
#         )  # Ví dụ: ['silver', 'customer', 'silver_cleaned_customer']
#         layer = asset_key[0] if asset_key else None

#         if layer == "silver":
#             return self.handle_output_silver_layer(context, obj)
#         elif layer == "bronze":
#             return self.handle_output_bronze_layer(context, obj)
#         elif layer == "gold":
#             return self.handle_output_gold_layer(context, obj)
#         else:
#             raise Exception(f"⚠️ Unknown layer for asset: {asset_key}")

#     def handle_output_silver_layer(self, context, df):
#         """
#         Ghi dữ liệu Spark DataFrame vào Delta table trong layer 'silver'
#         (3 cấp: layer/domain/table). Phân biệt loại bảng theo tên asset.
#         """
#         path_parts = (
#             context.asset_key.path
#         )  # ví dụ: ['silver', 'customer', 'silver_cleaned_customer']

#         if len(path_parts) != 3:
#             raise ValueError(
#                 f"Unexpected asset key path: {path_parts}. Expected ['silver', 'domain', 'asset_name']."
#             )

#         layer, domain, asset_name = path_parts

#         # Tách tên hàm asset để lấy loại bảng (vd: 'cleaned' trong 'silver_cleaned_customer')
#         name_parts = asset_name.split("_")
#         if len(name_parts) >= 3:
#             _, table_type, table_base = name_parts  # ['silver', 'cleaned', 'customer']
#         elif len(name_parts) == 2:
#             _, table_base = name_parts
#             table_type = "stg"  # default
#         else:
#             raise ValueError(f"Unexpected asset name pattern: {asset_name}")

#         # Sinh tên bảng chuẩn
#         table_full = f"{layer}.{domain}_{table_type}_{table_base}"
#         run_id = f"{layer}_{table_type}_{table_base}"

#         context.log.info(f"🧩 Saving to table: {table_full} | run_id={run_id}")

#         with get_spark_session(self._config, run_id=run_id) as spark:
#             try:
#                 if not self._table_exists(spark, table_full):
#                     context.log.info(
#                         f"🆕 Table {table_full} chưa tồn tại → tạo mới bằng full load (overwrite)."
#                     )
#                     df.write.format("delta").mode("overwrite").saveAsTable(table_full)
#                     context.log.info(f"✅ Created table {table_full} successfully.")
#                 else:
#                     before_count = spark.table(table_full).count()
#                     append_count = df.count()

#                     context.log.info(
#                         f"➕ Append {append_count} bản ghi vào {table_full} (trước đó có {before_count})."
#                     )

#                     df.write.format("delta").mode("append").saveAsTable(table_full)

#                     after_count = spark.table(table_full).count()
#                     context.log.info(
#                         f"✅ Append completed for {table_full}. "
#                         f"Tổng số bản ghi sau append: {after_count} (tăng {after_count - before_count})."
#                     )

#             except Exception as e:
#                 raise Exception(f"(Spark handle_output_silver_layer) Error: {e}")

from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from contextlib import contextmanager

# ... (Giữ nguyên hàm get_spark_session của bạn ở đây) ...


# class SparkIOManager(IOManager):
#     def __init__(self, config):
#         self._config = config

#     def _table_exists(self, spark, table_name: str) -> bool:
#         """Kiểm tra bảng đã tồn tại trong Hive Metastore chưa"""
#         try:
#             spark.table(table_name)
#             return True
#         except Exception:
#             return False

#     def load_input(self, context: InputContext) -> DataFrame:
#         # ... (Giữ nguyên logic load_input cũ của bạn) ...
#         context.log.debug(f"Loading input from {context.asset_key.path}...")
#         layer, _, table = context.asset_key.path
#         table_name = str(table.replace(f"{layer}_", ""))

#         # Logic fix để load đúng tên bảng (nếu cần)
#         # Ví dụ load từ silver.seller_stg_seller
#         full_table_name = f"{layer}.{table_name}"

#         with get_spark_session(self._config) as spark:
#             return spark.read.table(full_table_name)

#     def handle_output(self, context, obj):
#         asset_key = context.asset_key.path
#         layer = asset_key[0] if asset_key else None

#         if layer == "silver":
#             return self.handle_output_silver_layer(context, obj)
#         elif layer == "bronze":
#             # return self.handle_output_bronze_layer(context, obj)
#             pass
#         elif layer == "gold":
#             # return self.handle_output_gold_layer(context, obj)
#             pass
#         else:
#             raise Exception(f"⚠️ Unknown layer for asset: {asset_key}")

#     def handle_output_silver_layer(self, context, df: DataFrame):
#         """
#         Xử lý thông minh:
#         - Nếu là Staging -> Append
#         - Nếu là Cleaned -> Upsert (Merge)
#         """
#         path_parts = (
#             context.asset_key.path
#         )  # ['silver', 'customer', 'silver_cleaned_customer']

#         if len(path_parts) != 3:
#             raise ValueError(f"Unexpected path: {path_parts}")

#         layer, domain, asset_name = path_parts

#         # --- 1. Phân loại bảng dựa trên tên Asset ---
#         # Quy ước tên asset: silver_stg_seller HOẶC silver_cleaned_seller
#         name_parts = asset_name.split("_")

#         # Logic parse tên linh hoạt hơn
#         if "cleaned" in name_parts:
#             table_type = "cleaned"
#             # Lấy phần còn lại làm tên bảng (vd: seller)
#             # silver_cleaned_seller -> base=seller
#             table_base = "_".join(name_parts[name_parts.index("cleaned") + 1 :])
#         elif "stg" in name_parts or "staging" in name_parts:
#             table_type = "stg"
#             table_base = "_".join(name_parts[name_parts.index("stg") + 1 :])
#         else:
#             # Mặc định nếu không tìm thấy keyword
#             table_type = "stg"
#             table_base = asset_name.replace(f"{layer}_", "")

#         # Tên bảng đích trong Hive/Delta: silver.seller_cleaned_seller
#         target_table_name = f"{layer}.{domain}_{table_type}_{table_base}"

#         context.log.info(
#             f"🎯 Target Table: {target_table_name} | Type: {table_type.upper()}"
#         )

#         with get_spark_session(self._config, run_id=asset_name) as spark:

#             # --- CASE 1: Bảng chưa tồn tại -> Luôn tạo mới ---
#             if not self._table_exists(spark, target_table_name):
#                 context.log.info(
#                     f"🆕 Table {target_table_name} chưa tồn tại -> Creating new..."
#                 )
#                 df.write.format("delta").mode("overwrite").saveAsTable(
#                     target_table_name
#                 )
#                 return

#             # --- CASE 2: Bảng đã tồn tại -> Xử lý theo Type ---

#             # === A. STAGING (Append Only) ===
#             if table_type == "stg":
#                 context.log.info(f"➕ Mode: APPEND (Staging)")
#                 df.write.format("delta").mode("append").saveAsTable(target_table_name)

#             # === B. CLEANED (Upsert / Merge) ===
#             elif table_type == "cleaned":
#                 context.log.info(f"🔄 Mode: MERGE/UPSERT (Cleaned)")

#                 # Cần Merge Key.
#                 # Cách 1: Lấy từ metadata của Asset (Khuyên dùng)
#                 # Cách 2: Hardcode logic (Tạm thời dùng cách này cho demo)

#                 # Lấy merge_key từ metadata asset definition (nếu có)
#                 merge_key = context.definition_metadata.get("merge_key", "id")

#                 # Nếu metadata là 'customer_id', spark sql cần dạng "t.customer_id = s.customer_id"
#                 if isinstance(merge_key, str):
#                     condition = f"target.{merge_key} = source.{merge_key}"
#                 elif isinstance(merge_key, list):
#                     condition = " AND ".join(
#                         [f"target.{k} = source.{k}" for k in merge_key]
#                     )

#                 context.log.info(f"🔑 Merge Condition: {condition}")

#                 delta_table = DeltaTable.forName(spark, target_table_name)

#                 (
#                     delta_table.alias("target")
#                     .merge(df.alias("source"), condition)
#                     .whenMatchedUpdateAll()
#                     .whenNotMatchedInsertAll()
#                     .execute()
#                 )
#                 context.log.info(f"✅ Upsert completed for {target_table_name}")

#             else:
#                 context.log.warning(
#                     f"⚠️ Unknown table type '{table_type}', defaulting to Append."
#                 )
#                 df.write.format("delta").mode("append").saveAsTable(target_table_name)


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _table_exists(self, spark, table_name: str) -> bool:
        try:
            spark.table(table_name)
            return True
        except Exception:
            return False

    def _get_table_info(self, context):
        """Hàm Helper sinh tên bảng chuẩn xác"""
        asset_key = context.asset_key.path
        layer = asset_key[0]
        original_name = asset_key[-1]

        base_name = original_name.replace(f"{layer}_", "")

        if base_name.startswith("cleaned_"):
            base_name = base_name.replace("cleaned_", "clean_")

        if base_name.startswith("stg_"):
            table_type = "stg"
        elif base_name.startswith("clean_"):
            table_type = "clean"
        else:
            table_type = "other"

        full_table_name = f"{layer}.{base_name}"
        return full_table_name, table_type, original_name

    def _get_metadata(self, context: OutputContext):
        """
        Hàm Helper lấy metadata an toàn tuyệt đối (Safe Access).
        """
        metadata = {}

        try:
            # CÁCH 1: Lấy từ definition_metadata (Dagster mới)
            # Sử dụng getattr để tránh lỗi AttributeError
            def_meta = getattr(context, "definition_metadata", None)
            if def_meta:
                return def_meta

            # CÁCH 2: Lấy thông qua assets_def (Chuẩn cho @asset)
            assets_def = getattr(context, "assets_def", None)
            if assets_def:
                # 2a. Thử lookup trực tiếp
                meta = assets_def.metadata_by_key.get(context.asset_key)
                if meta:
                    return meta

                # 2b. Thử lookup bằng string (Phòng trường hợp object key không khớp)
                target_key_str = context.asset_key.to_user_string()
                for key, m in assets_def.metadata_by_key.items():
                    if key.to_user_string() == target_key_str:
                        return m

            # CÁCH 3: Lấy thông qua op_def (Safe access)
            op_def = getattr(context, "op_def", None)
            if op_def:
                # Dùng getattr để không bị crash nếu không có 'metadata'
                return getattr(op_def, "metadata", {})

        except Exception as e:
            context.log.warning(f"⚠️ Could not retrieve metadata: {e}")

        return {}

    def load_input(self, context: InputContext) -> DataFrame:
        full_table_name, _, _ = self._get_table_info(context)
        context.log.info(f"📖 Loading input from table: {full_table_name}")
        with get_spark_session(self._config) as spark:
            return spark.read.table(full_table_name)

    def handle_output(self, context, obj):
        asset_key = context.asset_key.path
        layer = asset_key[0]

        if layer == "silver":
            return self.handle_output_silver_layer(context, obj)
        elif layer == "bronze":
            pass
        else:
            raise Exception(f"⚠️ Unknown layer: {layer}")

    def handle_output_silver_layer(self, context, df: DataFrame):
        target_table_name, table_type, run_id = self._get_table_info(context)
        context.log.info(
            f"🎯 Target Table: {target_table_name} | Type: {table_type.upper()}"
        )

        with get_spark_session(self._config, run_id=run_id) as spark:

            # --- CASE 1: Tạo mới ---
            if not self._table_exists(spark, target_table_name):
                context.log.info(f"🆕 Creating new table {target_table_name}...")
                df.write.format("delta").mode("overwrite").saveAsTable(
                    target_table_name
                )
                return

            # --- CASE 2: Xử lý theo loại ---
            if table_type == "stg":
                context.log.info(f"➕ Mode: APPEND (Staging)")
                (
                    df.write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(target_table_name)
                )

            elif table_type == "clean":
                context.log.info(f"🔄 Mode: MERGE/UPSERT (Clean)")

                # 1. Lấy metadata & Merge Key
                metadata = self._get_metadata(context)
                merge_key = metadata.get("merge_key")

                # Fallback thông minh
                if not merge_key:
                    if "customer" in target_table_name:
                        merge_key = "customer_id"
                    else:
                        merge_key = "id"

                # 2. Tạo điều kiện Merge
                if isinstance(merge_key, str):
                    condition = f"target.{merge_key} = source.{merge_key}"
                elif isinstance(merge_key, list):
                    condition = " AND ".join(
                        [f"target.{k} = source.{k}" for k in merge_key]
                    )

                context.log.info(f"📝 Merge Condition: {condition}")

                delta_table = DeltaTable.forName(spark, target_table_name)

                # 3. THỰC HIỆN MERGE
                (
                    delta_table.alias("target")
                    .merge(df.alias("source"), condition)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )

                # 4. LẤY METRICS TỪ LỊCH SỬ (Mới thêm vào)
                # Lấy 1 commit gần nhất (chính là lệnh merge vừa chạy)
                try:
                    last_history = (
                        delta_table.history(1).select("operationMetrics").collect()
                    )

                    if last_history:
                        metrics = last_history[0]["operationMetrics"]
                        # Các key metric chuẩn của Delta Lake
                        num_inserted = metrics.get("numTargetRowsInserted", "0")
                        num_updated = metrics.get("numTargetRowsUpdated", "0")
                        num_deleted = metrics.get("numTargetRowsDeleted", "0")

                        context.log.info(
                            f"📊 MERGE REPORT for {target_table_name}:\n"
                            f"   - ✅ Rows Inserted (New): {num_inserted}\n"
                            f"   - 🔄 Rows Updated (Exist): {num_updated}\n"
                            f"   - ❌ Rows Deleted: {num_deleted}"
                        )
                except Exception as e:
                    context.log.warning(f"⚠️ Could not fetch merge metrics: {e}")

                context.log.info(f"✅ Upsert completed for {target_table_name}")
