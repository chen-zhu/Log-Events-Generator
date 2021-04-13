from grammar import *
from pprint import pprint

class parser:

    def __init__(self):
        self.mapping_name = ""
        self.raw_mapping_body = []
        self.raw_mapping_header = []
        # body dir: contains table index and columns
        self.body_columns = {}
        # body condition dir: contains table index and conditional columns info
        self.body_condition = {}
        # header dir: contains table index and columns
        self.header_columns = {}
        # header condition dir: contains table index and conditional columns info
        self.header_condition = {}

    def parsingFile(self, file_path):
        grammar_structure = grammar().Syntax()
        try:
            parsed_result = grammar_structure.parseFile(file_path)
        except Exception as ex:
            print(file_path + " parsing failed: ")
            print(ex)
            return False
        return parsed_result

    def tokenize(self, parsed_result):
        if not parsed_result:
            return
        self.mapping_name = parsed_result[0]
        self.raw_mapping_body = parsed_result[1]
        self.raw_mapping_header = parsed_result[2]
        for body in self.raw_mapping_body:
            table_name = body[0]
            self.body_columns[table_name] = []
            self.body_condition[table_name] = []
            for col in body[1]:
                if isinstance(col, (float, int, str)):
                    self.body_columns[table_name].append(col)
                else:
                    self.body_condition[table_name].append(col)
        for header in self.raw_mapping_header:
            event_name = header[0]
            self.header_columns[event_name] = []
            self.header_condition[event_name] = []
            for col in header[1]:
                if isinstance(col, (float, int, str)):
                    self.header_columns[event_name].append(col)
                else:
                    self.header_condition[event_name].append(col)
        print("Body Column: ", self.body_columns)
        print("Body Condition: ", self.body_condition)
        print("Header Column: ", self.header_columns)
        print("Header Condition: ", self.header_condition)

    def structurePrettyPrint(self, parsed_result):
        if not parsed_result:
            return
        self.mapping_name = parsed_result[0]
        self.raw_mapping_body = parsed_result[1]
        self.raw_mapping_header = parsed_result[2]
        print("---------------" + self.mapping_name + "---------------")
        for body in self.raw_mapping_body:
            print("[Mapping Body]: ", body)
        for header in self.raw_mapping_header:
            print("[Mapping Header]: ", header)
        self.tokenize(parsed_result)
        print("\n\n")
