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
        if file_name not in ["UserClaimsTask"]:
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


        '''
        from pypika import MySQLQuery, Query, Column, Tables, Field
        history, customers = Tables('history', 'customers')
        sample = (history.customer_id == customers.id) & (history.customer_id2 == customers.id2)
        q = MySQLQuery \
            .from_(history) \
            .join(customers) \
            .on(sample) \
            .select(history.star) \
            .where(customers.id == 5).where(customers.abd.isnull())

        print(q)

        from pyparsing import Word, alphas, printables

        # define grammar of a greeting
        greet = Word(printables + " ")

        hello = 'Hel123!@##$$%^&**lo, W"o"rld!'
        print(hello, "->", greet.parseString(hello))
        '''


