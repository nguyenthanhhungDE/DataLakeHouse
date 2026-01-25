from dagster import IOManager, InputContext, OutputContext
import pandas as pd
import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert as mysql_insert


class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

        # Tạo Connection String
        conn_info = (
            f"mysql+pymysql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}"
            f"/{config['database']}"
        )
        # Khởi tạo Engine
        try:
            self._engine = create_engine(conn_info)
        except Exception as e:
            raise Exception(f"Failed to create MySQL engine: {e}")

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext):
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        """
        Extract data from MySQL database as polars DataFrame
        Fix lỗi 'unexpected keyword argument connection' bằng cách qua cầu Pandas
        """
        try:
            # Dùng Pandas đọc trước vì tương thích tốt với SQLAlchemy Engine
            df_pandas = pd.read_sql(sql, self._engine)
            return pl.from_pandas(df_pandas)
        except Exception as e:
            raise Exception(f"Error extracting data from MySQL: {e}")

    def _upsert_method(self, table, conn, keys, data_iter):
        """
        Custom method: INSERT ... ON DUPLICATE KEY UPDATE
        """
        data = [dict(zip(keys, row)) for row in data_iter]
        if not data:
            return

        stmt = mysql_insert(table.table).values(data)
        update_stmt = stmt.on_duplicate_key_update(
            {col.name: col for col in stmt.inserted}
        )
        conn.execute(update_stmt)

    def insert_output(self, context, df):
        """
        Ghi dữ liệu xuống MySQL dùng Upsert
        """
        table_name = context.metadata.get("table")
        if not table_name:
            raise ValueError("Missing table name in asset metadata")

        if hasattr(df, "to_pandas"):
            df = df.to_pandas()

        context.log.info(
            f"Writing {len(df)} rows to MySQL table '{table_name}' using UPSERT..."
        )

        try:
            df.to_sql(
                table_name,
                con=self._engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method=self._upsert_method,
            )
            context.log.info(
                f"✅ Successfully merged/upserted data into '{table_name}'."
            )
        except Exception as e:
            context.log.error(f"❌ Insert failed for {table_name}: {e}")
            raise Exception(f"Error writing to MySQL: {e}")
