'''
1. Grammar and Parser
2. Run DB Connector to gather information
3. Run Validator (syntax check + column check)
4. Run interpreter
'''
import os
from parser import *
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
        if file_name in [".DS_Store", "MultiInstance", "Script"]:
            continue
        parsed_result = parser().parsingFile(data_path + file_name)
        parser().structurePrettyPrint(parsed_result)




