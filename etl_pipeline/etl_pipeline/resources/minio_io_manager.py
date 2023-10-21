from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import polars as pl

from contextlib import contextmanager
from datetime import datetime
from typing import Union
import os


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("minio_access_key"),
        secret_key=config.get("minio_secret_key"),
        secure=False,
    )

    try:
        yield client
    except Exception as e:
        raise e


def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists")


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        # context.asset_key.path: ['bronze', 'schema_name', 'table_name']
        layer, schema, table = context.asset_key.path

        context.log.debug(context.asset_key.path)
        # note: bronze/schema_name/table_name
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        context.log.debug(key)
        # note: /tmp/file_bronze_schema_table_xxxxxxx.parquet
        tmp_file_path = "/tmp/file_{}_{}.parquet".format(
            "_".join(context.asset_key.path), datetime.today().strftime("%Y%m%d%H%M%S")
        )

        if context.has_partition_key:
            # partition_str = table_2020
            partition_str = str(table) + "_" + context.asset_partition_key
            # bronze/schema/table/table_2020.parquet
            # /tmp/file_bronze_schema_table_xxxxxxxxxx.parquet
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path
        else:
            # bronze/schema/table.parquet
            return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        """ """
        key_name, tmp_file_path = self._get_path(context)

        # convert from polars Dataframe to parquet
        obj.write_parquet(tmp_file_path)

        # Save to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                # Make bucket if not exists
                make_bucket(client, bucket_name)

                # Upload to MinIO

                client.fput_object(bucket_name, key_name, tmp_file_path)
                context.log.info(
                    f"(MinIO handle_output) Number of rows and columns: {obj.shape}"
                )
                context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})

                # Clean tmp file
                os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: InputContext):
        """
           Prepares input and downloads parquet file from MinIO and convert to Polars Dataframe 
        """

        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)

        try:
            with connect_minio(self._config) as client:

                # Make bucket if not exists
                make_bucket(client, bucket_name)

                # Ex: bucket_name: lakehouse
                # key_name: bronze/stock/stocks.parquet
                # tmp_file_path: /tmp/file_bronze_stock_bronze_stocks_xxxxxxxxx.parquet
                context.log.info(f"(MinIO load_input) from key_name: {key_name}")
                client.fget_object(bucket_name, key_name, tmp_file_path)
                df_data = pl.read_parquet(tmp_file_path)
                context.log.info(
                    f"(MinIO load_input) Got polars dataframe with shape: {df_data.shape}"
                )
                os.remove(tmp_file_path)
                return df_data
        except Exception as e:
            raise e
