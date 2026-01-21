# import os
# from dagster import Definitions, load_assets_from_modules
# from dagstermill import ConfigurableLocalOutputNotebookIOManager

# from . import assets
# from .job import reload_data
# from .schedule import  reload_data_schedule
# from .resources.minio_io_manager import MinIOIOManager
# from .resources.mysql_io_manager import MySQLIOManager
# from .resources.spark_io_manager import SparkIOManager


# MYSQL_CONFIG = {
#     "host": os.getenv("MYSQL_HOST"),
#     "port": os.getenv("MYSQL_PORT"),
#     "database": os.getenv("MYSQL_DATABASES"),
#     "user": os.getenv("MYSQL_ROOT_USER"),
#     "password": os.getenv("MYSQL_ROOT_PASSWORD"),
# }


# MINIO_CONFIG = {
#     "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#     "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#     "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
#     "bucket": os.getenv("DATALAKE_BUCKET"),
# }

# SPARK_CONFIG = {
#     "spark_master": os.getenv("spark://spark-master:7077"),
#     "endpoint_url": os.getenv("MINIO_ENDPOINT"),
#     "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
#     "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
# }

# resources = {
#     "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
#     "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
#     "spark_io_manager": SparkIOManager(SPARK_CONFIG),
#     "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
# }

# defs = Definitions(assets=load_assets_from_modules([assets]),
#                        jobs=[reload_data],
#                        schedules=[reload_data_schedule],
#                        resources=resources,)
import os
from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)
from dagstermill import ConfigurableLocalOutputNotebookIOManager

# Import package assets (nơi chứa file __init__.py ở Bước 1)
from . import assets

from .job import reload_data
from .schedule import reload_data_schedule
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.spark_io_manager import SparkIOManager

# --- CONFIGURATION ---
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),  # Ép kiểu int cho Port
    "database": os.getenv("MYSQL_DATABASES"),
    "user": os.getenv("MYSQL_ROOT_USER"),
    "password": os.getenv("MYSQL_ROOT_PASSWORD"),
}

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
}

SPARK_CONFIG = {
    # SỬA LỖI: Tham số 1 là tên biến môi trường, tham số 2 là giá trị mặc định
    "spark_master": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

# --- RESOURCES ---
resources = {
    # QUAN TRỌNG: Thêm 2 dấu sao (**) để bung dict thành keyword arguments
    # Ví dụ: MySQLIOManager(host=..., user=...) thay vì MySQLIOManager({'host':...})
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG),
    "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
}

# --- DEFINITIONS ---
defs = Definitions(
    # 1. Load Assets: Quét từ package assets
    assets=load_assets_from_modules([assets]),
    # 2. Load Checks: Cũng quét từ package assets (Tự động tìm @asset_check)
    asset_checks=load_asset_checks_from_modules([assets]),
    jobs=[reload_data],
    schedules=[reload_data_schedule],
    resources=resources,
)
