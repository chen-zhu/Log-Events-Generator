'''
1. Grammar and Parser
2. Run DB Connector to gather information
3. Run Validator (syntax check + column check)
4. Run interpreter
'''
import os
from parser import *
from validator import *
from database import *
from interpreter import *
from pprint import pprint
import pathlib
import dotenv
from fileManager import sorting_csv_files, directory_prepare, count_dir_lines
import time



if __name__ == "__main__":
    db_list = ["12000_tasks", "16000_tasks", "20000_tasks", "24000_tasks", "28000_tasks", "36000_tasks"]

    for db_name in db_list:
        dotenv_file = dotenv.find_dotenv()
        dotenv.set_key(dotenv_file, "SOURCE_DATABASE", db_name)

        dotenv_file = dotenv.find_dotenv()
        dotenv.load_dotenv(dotenv_file, override=True)
        RULES_DIR = os.getenv('RULES_DIR')
        print("[current source DB]: " + db_name + ", -- SOURCE_DATABASE: ", os.getenv('SOURCE_DATABASE'))

        execution_time = []
        for i in range(6):
            directory_prepare()
            # clean up target
            database().cleanup_target()
            data_path = str(pathlib.Path().absolute()) + "/" + RULES_DIR
            start_time = time.time()
            total_event = 0

            for file_name in os.listdir(data_path):
                if file_name in [".DS_Store", "__pycache__"]:
                # if file_name not in ["UserSubmitsTask"]:
                    continue
                p = parser()
                parsed_result = p.parsingFile(data_path + file_name)
                p.structurePrettyPrint(parsed_result)

                db = database(db_name)
                v = validator(p, db)
                v.validate_rule_parameters()
                v.validate_source_database()

                i = interpreter(p, db)
                i.field_mappings()

                db.prepare_target_tables(i.target_col_type)
                i.events_generator()
                total_event += i.event_count

            sorting_csv_files()
            exe_time = time.time() - start_time
            print("Execution Time - ", exe_time, total_event)
            execution_time.append(exe_time)
            execution_time.append(total_event)
            count_dir_lines()
            directory_prepare(True)
            time.sleep(5)

        print("Execution Time List: ", execution_time, db_name)


