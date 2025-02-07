import os.path

import mysql.connector
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


    def connect_to_sql(self):
        db_config = {
            'host':self.host,
            'port':self.port,
            'user':self.user,
            'password':self.password
        }
        try:
            conn = mysql.connector.connect(**db_config)
            return conn
        except mysql.connector.Error as err:
            print(f"Error: {err}")


    def create_schema(self):
        connection = self.connect_to_sql()
        cursor = connection.cursor()
        file_path = os.path.join(os.getcwd(), "create_schema.sql")
        with open(file_path, "r") as sql:
            cursor.execute(sql.read())
            sql.close()
            cursor.close()
        print(f"Schema has been succesfully created.")

    def create_table(self, schema:str = 'air_pollution'):
        connection = self.connect_to_sql()
        cursor = connection.cursor()
        file_path = os.path.join(os.getcwd(), "create_table.sql")
        try:
            cursor.execute(f"USE {schema};")
        except mysql.connector.Error:
            raise Exception(f"Schema {schema} probably does not exists.")
        try:
            with open(file_path, "r") as sql:
                cursor.execute(sql.read())
                sql.close()
                cursor.close()
        except mysql.connector.Error as err:
            print(f"Error: {err}")


test = SQLManagement()
test.create_table()



















