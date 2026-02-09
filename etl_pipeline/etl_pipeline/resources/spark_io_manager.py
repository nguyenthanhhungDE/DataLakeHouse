from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame

from contextlib import contextmanager
from datetime import datetime
from delta.tables import DeltaTable
import json


@contextmanager
def get_spark_session(config, run_id="Spark IO Manager"):
    # Logic cấp phát RAM: Job thường lấy 1GB, IO Manager (Merge nặng) lấy 1.5GB
    executor_memory = "1g" if run_id != "Spark IO Manager" else "1500m"

    try:
        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(run_id)
            # ==================================================================
            # 🚀 CẤU HÌNH TỐI ƯU TÀI NGUYÊN (Resource Optimization)
            # Giúp chạy được 3 Job song song trên Worker 12 Cores / 6.4GB RAM
            # ==================================================================
            .config("spark.executor.cores", "4")  # Mỗi Job chỉ lấy 4 Core
            .config("spark.cores.max", "4")  # Giới hạn cứng tổng Core (Quan trọng)
            .config(
                "spark.executor.memory", executor_memory
            )  # 1g hoặc 1500m tùy loại job
            # ==================================================================
            # 1. Khai báo JARs
            .config(
                "spark.jars",
                "/opt/spark/jars/delta-core_2.12-2.3.0.jar,"
                "/opt/spark/jars/delta-storage-2.3.0.jar,"
                "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            )
            # 2. Delta Catalog
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # 3. ClassPath
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
            .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
            # 4. Delta Extensions
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # 5. MinIO (S3A) Configs
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
            # 6. Warehouse & Hive Metastore
            .config("spark.sql.warehouse.dir", "s3a://lakehouse/")
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.sql.catalogImplementation", "hive")
            .enableHiveSupport()
            .getOrCreate()
        )

        yield spark

    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from contextlib import contextmanager


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
        """Hàm Helper sinh tên bảng"""
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
        if obj is None:
            context.log.info(
                f"🛑 Output là None (do Asset '{context.asset_key.path[-1]}' trả về rỗng). IO Manager sẽ bỏ qua bước ghi."
            )
            return  # Thoát ngay, không làm gì cả

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
