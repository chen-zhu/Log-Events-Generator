import pathlib
import os
from dotenv import load_dotenv
import csv
import json
from datetime import date, datetime
import pandas as pd

load_dotenv()
CASE_ID_FIELD = os.getenv('CASE_ID_FIELD')
OUTPUT_DIR = os.getenv('OUTPUT_DIR')
EVENT_DATE = os.getenv('EVENT_DATE')

def write_csv(event_name, row):
    if CASE_ID_FIELD not in row:
        print("[ERROR]: Event <" + event_name + "> does not have the following Case ID field registered: " + CASE_ID_FIELD)
        return
    if EVENT_DATE not in row:
        print("[ERROR]: Event <" + event_name + "> does not have the following Event Time field registered: " + EVENT_DATE)
        return

    file_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR + str(row[CASE_ID_FIELD]) + ".csv"
    with open(file_path, "a+") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([row[CASE_ID_FIELD], event_name, row[EVENT_DATE], json.dumps(row, default=json_serial)])
    csv_file.close()
    #sort_file(file_path)

def sort_file(csv_file_path):
    df = pd.read_csv(csv_file_path, names=["CASE_ID", "EVENT_NAME", "EVENT_DATE", "DATA"])
    sorted_df = df.sort_values(by=["EVENT_DATE"], ascending=True)
    sorted_df.to_csv(csv_file_path, index=False, header=False)

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))

def sorting_csv_files():
    dir_path = str(pathlib.Path().absolute()) + "/" + OUTPUT_DIR
    for file_name in os.listdir(dir_path):
        if '.csv' in file_name:
            sort_file(dir_path + file_name)
