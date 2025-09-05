import os.path
import sys

from pygments.lexers import find_lexer_class_by_name
from streamlit import connection

sys.path.append("/Users/grzegorznaporowski/Desktop/Portfolio")

import pandas as pd
from sqlalchemy import create_engine, text, exc
from air_pollution.src.utils.configuration import  Configuration
from air_pollution.src.utils.data_fetcher import DataFetcher


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
        except exc.SQLAlchemyError as s:
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

            print(f"File: {file_path} was executed successfully.")

        except Exception as e:
            raise RuntimeError(f"There was some error connected with executing the commands: {e}")


    def create_view(self, database_name:str = "air_pollution"):
        """
        Function that creates the view with the latest data based on "load_date"
        :param database_name: name of the database
        :return:
        View inside MySQL with the latest load_date.
        """
        connection = self.connect_to_sql()
        file_path = os.path.join(os.getcwd(), "create_view.sql")
        try:
            with open(file_path, "r") as sql_file:
                sql_statements = sql_file.read()

            with connection.connect() as conn:
                conn.execute(text(f"USE {database_name};"))
                conn.execute(text(sql_statements))
                conn.commit()
            print(f"File: {file_path} was executed successfully.")
        except Exception as e:
            raise RuntimeError(f"There was some error connected with executing the commands: {e}")

    def create_voivodeships(self, database_name:str = "air_pollution"):
        """
        Function that creates three tables based on the "create_voivodeships.sql" file.
        - cities: Consists names of all cities in Poland
        - voi: Consists names of all the voivodeships in Poland
        - cities_voivodeships: Table with mapped cities to their corresponding voivodeships
        :param database_name: Name of the database
        :return:
        Created tables within MySQL server.
        """
        connection = self.connect_to_sql()
        dir_path = os.path.dirname(__file__)
        file_path = os.path.join(dir_path, "create_voivodeships.sql")
        try:
            with open(file_path, "r") as sql_commands:
                command = sql_commands.read()

            with connection.connect() as conn:
                conn.execute(text(f"USE {database_name};"))
                for file in command.split(";"):
                    conn.execute(text(file+";"))
                conn.commit()
            print(f"File: {file_path} was executed successfully.")
        except FileNotFoundError:
            raise FileNotFoundError(f"File: {file_path} was not found.")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

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
        except exc.SQLAlchemyError as s:
            raise Exception(f"There's some problems with connection. {s}")


    def read_data_from_sql(self, mode="all")->pd.DataFrame:
        """
        Reades data from SQL based on the selected mode.
        Modes:
            - all: All records for all "load_dates"
            - all_latest_datetime: All records for all load_dates but for the newest datetime.
            - view: All records for the latest load_date
            - view_latest_datetime: All records for the latest load_date and the latest datetime

        :param mode: Particular mode for data reading.
        :return:
        Pandas DataFrame based on the mode parameter.
        """
        try:
            engine = self.connect_to_sql(database="air_pollution")
            dir_path = os.path.dirname(__file__)
            if mode == "all":
                file_name = "read_data_all.sql"
            elif mode == "all_latest_datetime":
                file_name = "read_data_all_latest.sql"
            elif mode == "view":
                file_name = "read_data_view.sql"
            elif mode == "view_latest_datetime":
                file_name = "read_data_view_latest.sql"
            else:
                raise ValueError("Invalid mode. Choose from 'all', 'all_latest_datetime', 'view', 'view_latest_datetime'")

            file_path = os.path.join(dir_path, file_name)
            with open(file_path, "r") as sql_commands:
                command = sql_commands.read()

            df = pd.read_sql(command, con=engine)
            df = df[(df[['o3', 'pm10', 'pm25', 'so2']]>=0).all(axis=1)]
            return df
        except exc.ProgrammingError as pe:
            raise Exception(f"SQL error: {pe}")
        except FileNotFoundError:
            raise Exception(f"SQL file not found: {file_path}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")






















