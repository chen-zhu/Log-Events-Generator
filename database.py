import mysql.connector
import os
from dotenv import load_dotenv
from pypika import Query, Column, Table, Field

load_dotenv()
SOURCE_HOST = os.getenv('SOURCE_HOST')
SOURCE_USER = os.getenv('SOURCE_USER')
SOURCE_PASSWORD = os.getenv('SOURCE_PASSWORD')
SOURCE_DATABASE = os.getenv('SOURCE_DATABASE')

TARGET_HOST = os.getenv('TARGET_HOST')
TARGET_USER = os.getenv('TARGET_USER')
TARGET_PASSWORD = os.getenv('TARGET_PASSWORD')
TARGET_DATABASE = os.getenv('TARGET_DATABASE')


class database:

    def __init__(self):
        self.source_db = mysql.connector.connect(
            host=SOURCE_HOST,
            user=SOURCE_USER,
            password=SOURCE_PASSWORD,
            database=SOURCE_DATABASE,
            raw=False
        )
        self.source_cursor = self.source_db.cursor()
        self.target_db = mysql.connector.connect(
            host=TARGET_HOST,
            user=TARGET_USER,
            password=TARGET_PASSWORD,
            raw=False
        )
        self.target_cursor = self.target_db.cursor()
        self.target_cursor.execute("CREATE DATABASE IF NOT EXISTS " + TARGET_DATABASE)
        self.target_cursor.execute("USE " + TARGET_DATABASE)
        self.table_config_cache = {}

    def table_columns_info(self, table_name):
        # only run if there is no such table cached!
        if table_name not in self.table_config_cache:
            query = "SHOW FIELDS FROM " + table_name
            # query = "SELECT DISTINCT COLUMN_NAME, DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS " \
            #        "WHERE TABLE_NAME = '" + table_name + "'"
            self.source_cursor.execute(query)
            table_config = self.source_cursor.fetchall()
            self.table_config_cache[table_name] = {}
            for row_info in table_config:
                self.table_config_cache[table_name][row_info[0]] = row_info[1].decode()
        # print("table_columns_info", self.table_config_cache)

    def prepare_target_tables(self, table_with_config: dict):
        for table_name in table_with_config:
            # The first target column must always be surrogate key!
            params = [Column(table_name + "_PK", "BIGINT AUTO_INCREMENT")]
            for field in table_with_config[table_name]:
                params.append(Column(field, table_with_config[table_name][field]))
            query = Query.create_table(table_name).columns(*params).primary_key(table_name + "_PK").get_sql(
                quote_char="`")
            # print(query)
            try:
                self.target_cursor.execute(query)
            except Exception as ex:
                print("[ERROR]: Unable to create Target Event table <" + table_name + ">. Abort! DB Response: ", ex)
                continue
                # return
            print("[INFO]: Event table <" + table_name + "> created.")

    def execute_query(self, query, run_on_source, need_commit=False):
        # print("Received query", query)
        if run_on_source:
            using_db = self.source_db
        else:
            using_db = self.target_db
        running_cursor = using_db.cursor()
        running_cursor.execute(query)
        rows = running_cursor.fetchall()

        if need_commit:
            using_db.commit()
        return rows


if __name__ == "__main__":
    db = database()
    # db.prepare_target_db()
    # db.source_cursor.execute("SHOW DATABASES")
    # for x in db.source_cursor:
    #    print(x)
    db.table_columns_info("nodeinstancelog")
    # db.prepare_target_tables(1,1)
