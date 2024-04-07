from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.sqlalchemy import URL as SF_URL
import sqlalchemy
import pandas as pd
import json


class SnowflakeDataExtract:

    # default constructor
    def __init__(self, conn_id, query=None, file_path=None, chunk_size=None):
        self.file_path = file_path
        self.conn_id = conn_id
        self.chunk_size = chunk_size
        self.query = query

    def snowflake_extractor(self):
        dw_hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        conn = dw_hook.get_conn()
        if self.file_path is not None:
            file = open(self.file_path, 'r')
            self.query = file.read()
            file.close()

        df = pd.DataFrame()
        count = 0
        if self.chunk_size is not None:
            for chunk in pd.read_sql(self.query, con=conn, chunksize=self.chunk_size):
                if count == 0:
                    df = chunk
                else:
                    df = df.append(chunk, ignore_index=True)
                count = count + 1
                print("Chunk added")
        else:
            df = pd.read_sql(self.query, conn, self.chunk_size)
        conn.close()
        df_data = df

        return df_data

    def get_sqla_engine(self):
        """Get initialized SQLAlchemy engine
        :param airflow.models.Connection conn: Airflow connection object
        :return: Initialized SQLAlchemy engine
        :rtype: sqlalchemy.engine.Engine
        """
        conn_type = self.conn_id.conn_type
        print(conn_type)
        print(self.conn_id.host)

        if conn_type == "Snowflake" or conn_type.lower() == "snowflake":
            extras = json.loads(self.conn_id.extra)
            url = SF_URL(
                account=extras["account"],
                user=self.conn_id.login,
                password=self.conn_id.password,
                database=extras["database"],
                warehouse=extras["warehouse"],
                role=extras["role"],
            )
        elif conn_type == "mysql":
            extras = json.loads(self.conn_id.extra)
            url = f"""mysql+pymysql://{self.conn_id.login}:{self.conn_id.password}@{self.conn_id.host}
            /{extras['database']}"""
        else:
            raise NotImplementedError(f"{conn_type} is not supported")

        engine = sqlalchemy.create_engine(url, echo=True)

        return engine
