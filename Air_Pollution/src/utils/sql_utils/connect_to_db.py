import os.path

import pandas as pd
import sqlalchemy.exc
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

            else:
                engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{database}")
                with engine.connect() as conn:
                    conn.execute(text(f"USE {database}"))
                    print(f"Connection to the {database} is estabilished.")
            return engine
        except SQLAlchemyError as s:
            raise RuntimeError(f"No connection was estabilished: {s}")



    def create_schema(self):
        """
        Function that reades the "create_schema.sql" file and executes the command within it.
        :return:
        Schema within the MySQL server.
        """
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
        """
        Function that reades the "create_table.sql" file and executes the command within it.
        :param database_name:
        :return:
        Empty table within "database_name".
        """
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


    def save_data_to_sql(self, save:bool=True,table_name:str = "raw_data"):
        """
        Function that saves the data into MySQL server.
        :param save: Indicates if we want to save the data within the "table_name" table.
        :param table_name: Name of the table in which we want to save the data.
        :return:
        Loaded data within the "table_name" table.
        """
        try:
            if save:
                engine = self.connect_to_sql(database="air_pollution")
                data_fetcher = DataFetcher()
                data = data_fetcher.get_data()
                data.to_sql(name=table_name, con=engine, index=False, if_exists="append")
            else:
                pass
        except SQLAlchemyError as s:
            raise Exception(f"There's some problems with connection. {s}")


    def read_data_from_sql(self)->pd.DataFrame:
        """
        Function that reades the "read_table.sql" and returns it as pandas DataFrame.
        :return:
        DataFrame with all the data within the MySQL table.
        """
        try:
            engine = self.connect_to_sql(database="air_pollution")
            with open("read_table.sql","r") as sql_file:
                command = sql_file.read()
                df = pd.read_sql(command, con=engine)
            return df
        except sqlalchemy.exc.ProgrammingError as pe:
            raise Exception(f"You've enetered wrong name of the table. {pe}")























