import os
from parser import *
from validator import *
from database import *
from interpreter import *
from pprint import pprint
import pathlib
from dotenv import load_dotenv
from fileManager import sorting_csv_files
import time
from multiprocessing import Pool

load_dotenv()
RULES_DIR = os.getenv('RULES_DIR')


def events_generator(file_name):
    data_path = str(pathlib.Path().absolute()) + "/" + RULES_DIR
    p = parser()
    parsed_result = p.parsingFile(data_path + file_name)
    #parsed_result = p.parsingFile(rule_file_path)
    p.structurePrettyPrint(parsed_result)

    db = database()
    v = validator(p, db)
    v.validate_rule_parameters()
    v.validate_source_database()

    i = interpreter(p, db)
    i.field_mappings()

    db.prepare_target_tables(i.target_col_type)
    i.events_generator()
    sorting_csv_files()

if __name__ == "__main__":
    data_path = str(pathlib.Path().absolute()) + "/" + RULES_DIR
    start_time = time.time()

    all_files = os.listdir(data_path)
    for useless in [".DS_Store", "__pycache__"]:
        if useless in all_files:
            all_files.remove(useless)
    pool = Pool()
    pool.map(events_generator, all_files)

    #for file_name in os.listdir(data_path):
    #    if file_name in [".DS_Store", "__pycache__"]:
        #if file_name not in ["UserWorksOnTask"]:
    #        continue
    #    events_generator(file_name)

    print("--- %s seconds ---" % (time.time() - start_time))
