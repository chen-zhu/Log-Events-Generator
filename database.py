import mysql.connector
import os
from dotenv import load_dotenv

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
            database=SOURCE_DATABASE
        )
        self.source_cursor = self.source_db.cursor()
        #self.source_cursor.execute("SHOW TABLES")
        #for x in self.source_cursor:
        #    print(x)
        self.target_db = mysql.connector.connect(
            host=SOURCE_HOST,
            user=SOURCE_USER,
            password=SOURCE_PASSWORD,
            database=SOURCE_DATABASE
        )
        self.target_cursor = self.target_db.cursor()

    def prepare_target_db(self):
        self.target_cursor.execute("CREATE DATABASE IF NOT EXISTS " + TARGET_DATABASE)
        self.target_cursor.execute("USE " + TARGET_DATABASE)


    #def list_tables(self):



if __name__ == "__main__":
    db = database()
    db.prepare_target_db()
    db.source_cursor.execute("SHOW DATABASES")
    for x in db.source_cursor:
        print(x)