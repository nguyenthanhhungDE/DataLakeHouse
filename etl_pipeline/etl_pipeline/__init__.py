import os
from dagster import Definitions, load_assets_from_modules


from . import assets
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.spark_io_manager import SparkIOManager


MYSQL_CONFIG = {
    "host": os.getenv('MYSQL_HOST'),
    "port": os.getenv('MYSQL_PORT'),
    "database": os.getenv('MYSQL_DATABASES'),
    "user": os.getenv('MYSQL_ROOT_USER'),
    "password": os.getenv('MYSQL_ROOT_PASSWORD')
}


MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket": os.getenv("DATALAKE_BUCKET")
}

SPARK_CONFIG = {
    "spark_master": os.getenv("spark://spark-master:7077"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("SPARK_MASTER_URL")
}

resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG)
} 

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources
)