from pypika import MySQLQuery, Query, Column, Table, Field, CustomFunction, Order
from pprint import pprint
from fileManager import write_csv
from dotenv import load_dotenv
import os

class interpreter:

    def __init__(self, parser_obj, database_obj):
        self.parser = parser_obj
        self.db = database_obj
        # header col -> type
        self.target_col_type = {}
        #self.target_col_name = {}
        self.contain_nested_tables = []
        self.nested_table_elements = {}
        self.group_by_attributes = {}
        self.all_source_type = {}
        self.query_selected_cols = {}
        self.LRU_cache = {}
        # hold the fact that, variable -> target_name
        self.rename_map = {}
        self.cached_group_key = {}
        load_dotenv()

    def field_mappings(self):
        # 1. map type
        # 2. map 'rename' field
        all_source_type = self.all_source_type
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
                        self.nested_table_elements[event_name + '.' + nested_table_name] = []
                        self.group_by_attributes[event_name + '.' + nested_table_name] = ()
                        self.target_col_type[nested_table_name] = {event_name + "_FK": "BIGINT"}
                        # a. Extract out group by info
                        group_by = list(condition)[
                                              list(condition).index('<') + 1:list(condition).index('>(')]
                        self.group_by_attributes[event_name + '.' + nested_table_name] = (group_by, )

                        # b. Extract out params info
                        nested_table_params = list(condition)[
                                              list(condition).index('>(') + 1:list(condition).index(')')]
                        # print(nested_table_params)
                        for param in nested_table_params:
                            self.nested_table_elements[event_name + '.' + nested_table_name].append(param)
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
        #print("\nTarget Column Mapping", self.target_col_type, "\n")
        # print("self.group_by_attributes", self.group_by_attributes)

    def query_generator(self):
        # special handle here to prevent date messed up during query process~
        date_format = CustomFunction('DATE_FORMAT', ['date_field', 'format'])
        format = '%m/%d/%Y %H:%i:%s'

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
        #print(join)
        #print(self.all_source_type)

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
                        exp1 = getattr(table_obj_map[pre[0]], pre[1])
                        exp2 = getattr(table_obj_map[curr[0]], curr[1])
                        if self.is_a_time_field(curr[1]):
                            #print("Target Column Type: ", self.all_source_type[curr[1]])
                            exp1 = date_format(exp1, format)
                            exp2 = date_format(exp2, format)
                        join_condition_queue[curr[0]].append([exp1, exp2])
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
                    elif self.is_a_time_field(condition[0]):
                        # Special handle for date field~
                        where.append(date_format(getattr(table_obj_map[table_name], condition[0]), format) ==
                                     date_format(condition[2].strip('"'), format))
                    else:
                        where.append(getattr(table_obj_map[table_name], condition[0]) == condition[2].strip('"'))

        q = MySQLQuery.from_(from_table)

        #join must be followed by ON~ multiple joins should be separated as well!
        for join_table_name in join:
            #print(join_table_name, join[join_table_name])
            #q = q.join(join[join_table_name])
            q = q.left_join(join[join_table_name])

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
            select_object = []
            select_col_names = []
            # process select - [HEADER]
            for attribute in self.parser.header_simple_columns[event]:
                select_object.append(getattr(table_obj_map[self.parser.reverse_column_map[attribute]], attribute))
                select_col_names.append(attribute)

            # process additional select - [HEADER]
            for condition in self.parser.header_condition_columns[event]:
                # TODO: make it handle complicated condition!
                # Handle Column Renaming Here
                if condition[2] in self.parser.body_variable_map and len(condition) < 5: # Ahhhh this is very risky to do this! Nested table statment by min have 5 elements
                    new_name = condition[0]
                    origin_column = self.parser.body_variable_map[condition[2]][1].split('.')
                    new_col = getattr(table_obj_map[origin_column[0]], origin_column[1])
                    select_object.append(new_col.as_(new_name))
                    self.rename_map[condition[2]] = new_name
                    select_col_names.append(new_name)

            self.query_selected_cols[event] = select_col_names.copy()

            # process nested table attributes - [HEADER]
            for event_name_dot_nested_table_name in self.nested_table_elements:
                if event+"." in event_name_dot_nested_table_name: # in case of multi-sugar~
                    nest_col_name = []
                    #print(event_name_dot_nested_table_name)
                    for attribute in self.nested_table_elements[event_name_dot_nested_table_name]:
                        if isinstance(attribute, str):
                            select_object.append(getattr(table_obj_map[self.parser.reverse_column_map[attribute]], attribute))
                            nest_col_name.append(attribute)
                        elif attribute[2] in self.parser.body_variable_map:
                            new_name = attribute[0]
                            origin_column = self.parser.body_variable_map[attribute[2]][1].split('.')
                            new_col = getattr(table_obj_map[origin_column[0]], origin_column[1])
                            select_object.append(new_col.as_(new_name))
                            self.rename_map[attribute[2]] = new_name
                            nest_col_name.append(new_name)
                self.query_selected_cols[event_name_dot_nested_table_name] = nest_col_name.copy()

            mysql_query_obj = q.select(*select_object)
            query_objects[event] = mysql_query_obj

        #print('self.nested_table_elements', self.nested_table_elements)
        #print('self.query_selected_cols', self.query_selected_cols)
        return query_objects

    def events_generator(self):
        query_objects = self.query_generator()
        EVENT_DATE = os.getenv('EVENT_DATE')
        CASE_ID_FIELD = os.getenv('CASE_ID_FIELD')

        page_size = 400
        for event_table in query_objects:
            q_obj = query_objects[event_table]

            #q_obj.orderby(EVENT_DATE, order=Order.desc) # Sorting Column: EVENT_DATE
            q_obj.orderby(CASE_ID_FIELD, order=Order.desc)

            offset = 0
            while True:
                query = q_obj[offset:page_size].get_sql()
                #print('\n[Generated Query]:\n', query)
                rows = self.db.execute_query(query, True, False, False, True)
                if event_table in self.contain_nested_tables:
                    #rows = self.db.execute_query(query, True, False, False, True)
                    self.process_source_data_for_nested_table(event_table, rows)
                else:
                    #rows = self.db.execute_query(query, True, False, False, True)
                    self.batch_process_source_data(event_table, rows)
                if len(rows) >= page_size:
                    offset += page_size
                else:
                    break

    def batch_process_source_data(self, event_table, rows):
        #print("Rows count: ", event_table, len(rows))
        bucket = list(self.bucket_split(rows))
        target_table = Table(event_table)
        for each_bucket in bucket:
            q = MySQLQuery.into(target_table)
            # TODO: some other necessary manipulation before writing data!
            for row in each_bucket:
                write_csv(event_table, row)
                insert_tpl = (None,)
                for key, val in row.items():
                    insert_tpl += (val,)
                q = q.insert(insert_tpl)
            ret = self.db.execute_query(q.get_sql(), False, True)
            # print("query result: ", q.get_sql())

    # Special handle for the situation when nested_table is involved
    def process_source_data_for_nested_table(self, event_table, rows):
        #print("Rows count: ", event_table, len(rows), "\n")
        for row in rows:
            #write_csv(event_table, row)
            for event, value in self.query_selected_cols.items():
                insert_data = (None, )
                if event == event_table and not self.skip_insert(event_table, row):
                    for col_name in value:
                        insert_data = insert_data + (row[col_name], )
                    q = MySQLQuery.into(Table(event_table))
                    q = q.insert(insert_data)
                    last_insert_id = self.db.execute_query(q.get_sql(), False, True, True)
                    self.set_nested_cache(row, last_insert_id)
                    write_csv(event_table, row)
                    # print("last inserted id: ", last_insert_id)
                    # print("insert_data ", insert_data)
                    # print("Insert command: ", q.get_sql())
                elif event_table+'.' in event:
                    # print("Nested Event: ", event)
                    cached_value = self.get_cache(self.obtain_nested_event_key(event, row))
                    # put foreign key here!
                    insert_data = insert_data + (cached_value, )
                    for col_name in value:
                        insert_data = insert_data + (row[col_name],)
                    q = MySQLQuery.into(Table(event.replace(event_table+'.', "")))
                    q = q.insert(insert_data)
                    query = q.get_sql()
                    #print('nested table query: ', query)
                    self.db.execute_query(query, False, True)
                    # print("corresponding nested event table detected.")

    def skip_insert(self, event_table, row):
        if len(self.cached_group_key) == 0:
            # print("self.group_by_attributes", self.group_by_attributes)
            # print("self.rename_map", self.rename_map, "\n")
            for name, l in self.group_by_attributes.items():
                if event_table not in name:
                    continue
                self.cached_group_key[name] = []
                for variable in l[0]:
                    self.cached_group_key[name].append(self.rename_map[variable])
        # print('self.cached_group_key', self.cached_group_key)
        for nested_table_name, cols in self.cached_group_key.items():
            cache_key = nested_table_name
            for col in cols:
                cache_key = cache_key + "." + str(row[col])
            #print('Cache Key: ', cache_key)
            if self.get_cache(cache_key) is None:
                # print("DO NOT SKIP INSERT", cache_key)
                return False
        return True

    def obtain_nested_event_key(self, nested_event_name, row):
        cache_key = nested_event_name
        for col in self.cached_group_key[nested_event_name]:
            cache_key = cache_key + "." + str(row[col])
        return cache_key
        #print("generated index: ", cache_key)

    def set_nested_cache(self, row, value):
        for nested_table_name, cols in self.cached_group_key.items():
            cache_key = nested_table_name
            for col in cols:
                cache_key = cache_key + "." + str(row[col])
                self.set_cache(cache_key, value)

    def bucket_split(self, in_list):
        bucket_size = 40
        for i in range(0, len(in_list), bucket_size):
            yield in_list[i:i + bucket_size]

    def is_a_time_field(self, field_name):
        mysql_date_type_keywords = ["DATE", "TIME", "YEAR"]
        for i in mysql_date_type_keywords:
            if i.lower() in self.all_source_type[field_name].lower():
                return True
        return False

    def set_cache(self, name, value):
        cache_size = 10000
        self.LRU_cache[name] = value
        if len(self.LRU_cache) > cache_size:
            for key in self.LRU_cache:
                del self.LRU_cache[key]
                break

    def get_cache(self, name):
        return self.LRU_cache.get(name)

