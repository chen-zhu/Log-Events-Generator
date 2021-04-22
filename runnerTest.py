import os
from parser import *
from validator import *
from database import *
from interpreter import *
from pprint import pprint
import pathlib
from dotenv import load_dotenv

load_dotenv()
RULES_DIR = os.getenv('RULES_DIR')

if __name__ == "__main__":
    data_path = str(pathlib.Path().absolute()) + "/" + RULES_DIR
    arr = os.listdir(data_path)

    size_check = len(arr)
    if '.DS_Store' in arr:
        size_check -= 1

    for file_name in os.listdir(data_path):
        #if file_name in [".DS_Store", "MultiInstance", "Script"]:
        if file_name not in ["UserSubmitsTask"]:
            continue
        p = parser()
        parsed_result = p.parsingFile(data_path + file_name)
        p.structurePrettyPrint(parsed_result)

        db = database()
        v = validator(p, db)
        v.validate_rule_parameters()
        v.validate_source_database()

        i = interpreter(p, db)
        i.field_mappings()

        db.prepare_target_tables(i.target_col_type)
        i.events_generator()
