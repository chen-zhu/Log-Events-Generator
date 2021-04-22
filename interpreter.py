from pypika import MySQLQuery, Query, Column, Table, Field
from pprint import pprint


class interpreter:

    def __init__(self, parser_obj, database_obj):
        self.parser = parser_obj
        self.db = database_obj
        # header col -> type
        self.target_col_type = {}
        self.target_col_name = {}
        self.contain_nested_tables = []

    def field_mappings(self):
        # 1. map type
        # 2. map 'rename' field
        all_source_type = {}
        field_unknown_type = []
        for table_name, dic in self.db.table_config_cache.items():
            for field in dic:
                all_source_type[field] = dic[field]
        # print("all_source_type", all_source_type)

        # check and obtain column type here!
        for event_name in self.parser.header_simple_columns:
            self.target_col_type[event_name] = {}
            for col in self.parser.header_simple_columns[event_name]:
                if col in all_source_type:
                    self.target_col_type[event_name][col] = all_source_type[col]
                else:
                    self.target_col_type[event_name][col] = "varchar(255)"
                    field_unknown_type.append(col)

        for event_name in self.parser.header_condition_columns:
            for condition in self.parser.header_condition_columns[event_name]:
                # print("Condition Row: ", condition)
                if len(condition) == 3 and condition[2] in self.parser.body_variable_map:
                    source_column = self.parser.body_variable_map[condition[2]][0]
                    target_column = condition[0]
                    if source_column in all_source_type:
                        self.target_col_type[event_name][target_column] = all_source_type[source_column]
                    else:
                        self.target_col_type[event_name][target_column] = "varchar(255)"
                        field_unknown_type.append(target_column)
                else:
                    # it is an unknown column, which means that it might be nested table!
                    # TODO: this handle is terrible~ Tryna fix this later!
                    if '<' in list(condition) and '>(' in list(condition) and ')' in list(condition):
                        nested_table_name = condition[0]
                        self.target_col_type[nested_table_name] = {event_name + "_FK": "BIGINT"}
                        nested_table_params = list(condition)[
                                              list(condition).index('>(') + 1:list(condition).index(')')]
                        # print(nested_table_params)
                        for param in nested_table_params:
                            source_column = None
                            target_column = None
                            if isinstance(param, str):
                                source_column = param
                                target_column = param
                            else:
                                source_column = self.parser.body_variable_map[param[2]][0]
                                target_column = param[0]
                            # print("Source and Target: ", source_column, target_column)
                            if source_column in all_source_type:
                                self.target_col_type[nested_table_name][target_column] = all_source_type[source_column]
                            else:
                                self.target_col_type[nested_table_name][target_column] = "varchar(255)"
                                field_unknown_type.append(target_column)
                        self.contain_nested_tables.append(event_name)
                    else:
                        self.target_col_type[event_name][condition[0]] = "varchar(255)"
                        field_unknown_type.append(condition[0])

        if len(field_unknown_type) > 0:
            print("[WARNING]: The following event attribute cannot be mapped to corresponding data type. "
                  "Set to Varchar(255) by default.", field_unknown_type)
        print("\nTarget Column Mapping", self.target_col_type, "\n")

    def query_generator(self):
        # process join tables - [BODY]
        tables = []
        table_obj_map = {}
        join_condition_queue = {}
        base_table_name = ""
        for table in self.parser.body_all_columns:
            base_table_name = table if len(base_table_name) == 0 else base_table_name
            table_obj = Table(table)
            tables.append(table_obj)
            table_obj_map[table] = table_obj
            join_condition_queue[table] = []
        from_table = tables[0]
        join = table_obj_map.copy()
        join.pop(base_table_name)
        print(join)

        # process join conditions - [BODY]
        for var in self.parser.body_variable_map:
            # only join when more then 1 table name exist!
            if len(self.parser.body_variable_map[var]) > 2:
                previous_table = ""
                for table_name in self.parser.body_variable_map[var][1:]:
                    if len(previous_table) == 0:
                        previous_table = table_name
                    else:
                        pre = previous_table.split('.')
                        curr = table_name.split('.')
                        join_condition_queue[curr[0]].append([getattr(table_obj_map[pre[0]], pre[1]),
                                                              getattr(table_obj_map[curr[0]], curr[1])])
                        # join_condition_queue.append([getattr(table_obj_map[pre[0]], pre[1]),
                        #                            getattr(table_obj_map[curr[0]], curr[1])])

        # process where filters - [BODY]
        where = []
        for table_name in self.parser.body_condition_columns:
            for condition in self.parser.body_condition_columns[table_name]:
                # it is not a variable, then it is a filter! Apply it as where statement!
                if condition[2] not in self.parser.body_variable_map:
                    # TODO: make it handle complicated filter!
                    if condition[2].lower() in ["null", "notnull"]:
                        op = "isnull" if condition[2].lower() == "null" else "notnull"
                        column_body = getattr(table_obj_map[table_name], condition[0])
                        where.append(getattr(column_body, op)())
                    elif condition[2].lower() in ["true", "false"]:
                        op = True if condition[2].lower() == "true" else False
                        where.append(getattr(table_obj_map[table_name], condition[0]) == op)
                    else:
                        where.append(getattr(table_obj_map[table_name], condition[0]) == condition[2].strip('"'))

        q = MySQLQuery.from_(from_table)

        print(join_condition_queue, "\n")

        #join must be followed by ON~ multiple joins should be separated as well!
        for join_table_name in join:
            #print(join_table_name, join[join_table_name])
            q = q.join(join[join_table_name])

            j_c = None
            for j_condition in join_condition_queue[join_table_name]:
                if j_c is None:
                    j_c = (j_condition[0] == j_condition[1])
                else:
                    j_c = j_c & (j_condition[0] == j_condition[1])
            # ONLY perform join condition if it is not empty!
            if j_c is not None:
                q = q.on(j_c)

        for w_statement in where:
            q = q.where(w_statement)

        # final_query = []
        query_objects = {}

        # be aware! the header might contain more than one event table!
        for event in self.parser.header_simple_columns:
            select = []
            # process select - [HEADER]
            for attribute in self.parser.header_simple_columns[event]:
                select.append(getattr(table_obj_map[self.parser.reverse_column_map[attribute]], attribute))

            # process additional select - [HEADER]
            for condition in self.parser.header_condition_columns[event]:
                # TODO: make it handle complicated condition!

                # Handle Column Renaming Here
                if condition[2] in self.parser.body_variable_map:
                    new_name = condition[0]
                    origin_column = self.parser.body_variable_map[condition[2]][1].split('.')
                    new_col = getattr(table_obj_map[origin_column[0]], origin_column[1])
                    select.append(new_col.as_(new_name))

            mysql_query_obj = q.select(*select)
            query_objects[event] = mysql_query_obj

        # print('[Generated Query]:\n', query_objects)
        return query_objects

    def events_generator(self):
        query_objects = self.query_generator()

        page_size = 4
        for event_table in query_objects:
            q_obj = query_objects[event_table]
            offset = 0
            while True:
                query = q_obj[offset:page_size].get_sql()
                print('[Generated Query]:\n', query)
                # TODO: Remove Break here!
                break
                rows = self.db.execute_query(query, True)
                if event_table in self.contain_nested_tables:
                    self.process_source_data_for_nested_table(event_tables, rows)
                else:
                    self.batch_process_source_data(event_table, rows)
                if len(rows) >= page_size:
                    offset += page_size
                    # print("Nest Page: ", offset)
                else:
                    # print("Final Rows:\n", rows)
                    break

    def batch_process_source_data(self, event_table, rows):
        print("Rows count: ", event_table, len(rows))
        bucket = list(self.bucket_split(rows))
        target_table = Table(event_table)
        for each_bucket in bucket:
            q = MySQLQuery.into(target_table)
            # TODO: some other necessary manipulation before writing data!
            for row in each_bucket:
                # print(row)
                q = q.insert((None,) + row)
            self.db.execute_query(q.get_sql(), False, True)
            # print("query result: ", q.get_sql())

    # Special handle for the situation when nested_table is involved
    def process_source_data_for_nested_table(self, event_tables):
        return 0

    def bucket_split(self, in_list):
        bucket_size = 40
        for i in range(0, len(in_list), bucket_size):
            yield in_list[i:i + bucket_size]
