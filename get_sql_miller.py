import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

class get_sql:
    def __init__(self, db_protocol, username, password, host, port, database, min_conn=10, max_conn=100):
        self.db_protocol = db_protocol.upper()
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.min_conn = min_conn
        self.max_conn = max_conn
        self.engine = self.create_engine()

    def create_engine(self):
        if self.db_protocol == "SQLSERVER":
            driver = 'ODBC+Driver+17+for+SQL+Server'
            engine_url = f'mssql+pyodbc://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?driver={driver}'
        elif self.db_protocol == "MYSQL":
            engine_url = f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        else:
            raise ValueError(f"不支持的数据库协议: {self.db_protocol}")
        
        return create_engine(
            engine_url,
            poolclass=QueuePool,
            pool_size=self.max_conn,
            max_overflow=self.max_conn - self.min_conn,
        )

    def execute(self, sql, params=None):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql), params or {})
                conn.commit()
        except SQLAlchemyError as e:
            print(f"执行 SQL 时发生错误: {e}")

    def read_sql(self, sql, params=None):
        try:
            with self.engine.connect() as conn:
                return pd.read_sql_query(text(sql), conn, params=params)
        except SQLAlchemyError as e:
            print(f"读取 SQL 时发生错误: {e}")
            return pd.DataFrame()

    def to_sql(self, dataframe, table_name, if_exists='append', index=False):
        if not isinstance(dataframe, pd.DataFrame):
            raise ValueError("dataframe参数必须是dataframe")
        try:
            dataframe.to_sql(name=table_name, con=self.engine, if_exists=if_exists, index=index)
        except SQLAlchemyError as e:
            print(f"数据写入时发生错误: {e}")

# 示例子类
class ConnectorJavaTest(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="MYSQL",
            username='root',
            password='Zhcx?2021',
            host='10.158.158.47',
            port='3306',
            database='test_zhcxkj_center_mercado'
        )

class ConnectorDw(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="MYSQL",
            username='app_admin',
            password='fs9upeK82qTvZgLy',
            host='172.23.42.189',
            port='9030',
            database='zhcxkj_dw_bi'
        )

class ConnectorPublish(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="MYSQL",
            username='root',
            password='fxVAvF3IK1LXS5RVLZg1',
            host='139.159.245.112',
            port='3306',
            database='irobotbox_marketpublish'
        )

class Connector22(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="SQLSERVER",
            username='irobotbox',
            password='V414oTNNiRFINsiz',
            host='10.10.55.22',
            port='1433',
            database='metabase'
        )

class Connector11(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="SQLSERVER",
            username='majianli',
            password='molihua1118',
            host='10.10.7.11',
            port='1433',
            database='LogisticsDB'
        )

class Connector131(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="SQLSERVER",
            username='majianli',
            password='molihua1118.',
            host='10.10.7.131',
            port='1433',
            database='LogisticsDB'
        )

class Connector114(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="SQLSERVER",
            username='irobotbox',
            password='Qwer.1234!',
            host='10.10.55.114',
            port='1433',
            database='Summary_db'
        )

class ConnectorJava(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="MYSQL",
            username='root',
            password='4lVNDYy#FW71JDr6',
            host='124.71.96.19',
            port='3306',
            database='zhcxkj_center_mercado'
        )

class ConnectorJavaTest1(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="MYSQL",
            username='root',
            password='Zhcx?2021',
            host='10.158.158.47',
            port='3306'
            # database 没有提供，如果这是故意的则保持不变，否则应该添加相应的数据库名称
        )

class Connector124(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="SQLSERVER",
            username='irobotbox',
            password='Qwer.1234!',
            host='10.10.55.124',
            port='1433',
            database='irobotbox'
        )

class Connector158(get_sql):
    def __init__(self):
        super().__init__(
            db_protocol="SQLSERVER",
            username='irobotboxuser',
            password='HskwPanda047RAHRAxqktlref',
            host='erpdb-readonly.zhcxkj.com',
            port='1433',
            database='irobotbox'
        )
