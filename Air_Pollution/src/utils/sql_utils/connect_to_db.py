import os.path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from Air_Pollution.src.utils.configuration import  Configuration
from Air_Pollution.src.utils.data_fetcher import DataFetcher


class SQLManagement:
    def __init__(self):
        self.config = Configuration()
        self.data = DataFetcher()
        self.host = self.config.get("MySQL", "host")
        self.port = self.config.get("MySQL", "port")
        self.user = self.config.get("MySQL", "user")
        self.password = self.config.get("MySQL", "password")



    def connect_to_sql(self,database=None):
        try:
            if database is None:
                engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}")
                engine.connect()
                print("Connection estabilised succesfully.")
                return engine
            else:
                engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{database}")
                print(f"Connection estabilised succesfully for database: {database}")
                return engine
        except SQLAlchemyError as s:
            raise RuntimeError(f"No connection was estabilished: {s}")



    def create_schema(self):
        connection = self.connect_to_sql()
        file_path = os.path.join(os.getcwd(), "create_schema.sql")
        try:
            with open (file_path, "r") as sql_file:
                sql_statements = sql_file.read()

            with connection.connect() as conn:
                conn.execute(text(sql_statements))
                conn.commit()

            print(f"File: {file_path} was executed succesfully.")
        except Exception as e:
            raise RuntimeError(f"There was some error connected with executing the commands: {e}")

    def create_table(self, database_name:str = "air_pollution"):
        connection = self.connect_to_sql()
        file_path = os.path.join(os.getcwd(), "create_table.sql")
        try:
            with open(file_path, "r") as sql_file:
                sql_statements = sql_file.read()

            with connection.connect() as conn:
                conn.execute(text(f"USE {database_name};"))
                conn.execute(text(sql_statements))
                conn.commit()

            print(f"File: {file_path} was executed succesfully.")

        except Exception as e:
            raise RuntimeError(f"There was some error connected with executing the commands: {e}")


    def save_data_to_sql(self, save=True,table_name:str = "raw_data"):
        try:
            if save:
                engine = self.connect_to_sql(database="123")
                data_fetcher = DataFetcher()
                data = data_fetcher.get_data()
                data.to_sql(name=table_name, con=engine, index=False, if_exists="append")
            else:
                pass
        except SQLAlchemyError as s:
            raise Exception(f"There's some problems with connection. {s}")






test = SQLManagement()
test.save_data_to_sql()





















