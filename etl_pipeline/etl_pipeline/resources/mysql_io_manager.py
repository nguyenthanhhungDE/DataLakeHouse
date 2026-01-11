from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from sqlalchemy import create_engine
import polars as pl


def connect_mysql_generate_data(config) -> str:
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn_info


def connect_mysql(config) -> str:
    conn_info = (
        f"mysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn_info


class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext):
        pass

    def insert_output(self, context: OutputContext, obj: pl.DataFrame):
        """Insert Polars DataFrame vào MySQL"""
        table_name = context.metadata.get("table")
        if not table_name:
            raise ValueError("Missing table name in asset metadata")

        conn_info = connect_mysql_generate_data(self._config)
        engine = create_engine(conn_info)

        try:
            df = obj.to_pandas()
            with engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )
            context.log.info(f"✅ Inserted {len(df)} rows into table: {table_name}")
        except Exception as e:
            context.log.error(f"❌ Insert failed for {table_name}: {e}")
            raise
        finally:
            engine.dispose()

    def extract_data(self, sql: str) -> pl.DataFrame:
        """
        Extract data from MySQL database as polars DataFrame
        """
        conn_info = connect_mysql(self._config)
        df_data = pl.read_database(query=sql, connection_uri=conn_info)
        return df_data
