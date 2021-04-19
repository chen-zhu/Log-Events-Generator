from pypika import MySQLQuery, Query, Column, Table, Field


class interpreter:

    def __init__(self, parser_obj, database_obj):
        self.a = 1
        self.parser = parser_obj
        self.db = database_obj
        # header col -> type
        self.target_col_type = {}
        self.target_col_name = {}

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
                    self.target_col_type[event_name][condition[0]] = "varchar(255)"
                    field_unknown_type.append(condition[0])

        if len(field_unknown_type) > 0:
            print("[WARNING]: The following event attribute cannot be mapped to corresponding data type. "
                  "Set to Varchar(255) by default.")
        print("\nTarget Column Mapping", self.target_col_type, "\n")

    def query_generator(self):
        select = []

        # process join tables
        tables = []
        table_obj_map = {}
        for table in self.parser.body_all_columns:
            table_obj = Table(table)
            tables.append(table_obj)
            table_obj_map[table] = table_obj
        from_table = tables[0]
        join = tables[1:]

        # TODO: figure out a way to process join_condition
        join_condition_queue = []
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
                        join_condition_queue.append([getattr(table_obj_map[pre[0]], pre[1]),
                                           getattr(table_obj_map[curr[0]], curr[1])])

        # process select
        for event in self.parser.header_simple_columns:
            for attribute in self.parser.header_simple_columns[event]:
                select.append(getattr(table_obj_map[self.parser.reverse_column_map[attribute]], attribute))
            # TODO: remove this break to handle multiple event!
            break

        # process where statement
        where = []
        for table_name in self.parser.body_condition_columns:
            for condition in self.parser.body_condition_columns[table_name]:
                #it is not a variable, then it is a filter! Apply it as where statement!
                if condition[2] not in self.parser.body_variable_map:
                    #TODO: make it handle complicated filter!
                    where.append(getattr(table_obj_map[table_name], condition[0]) == condition[2].strip('"'))

        # process remaining header's condition statement!
        for event in self.parser.header_condition_columns:
            for condition in self.parser.header_condition_columns[event]:
                #TODO: make it handle complicated condition!

                #Handle Rename Here
                if condition[2] in self.parser.body_variable_map:
                    new_name = condition[0]
                    origin_column = self.parser.body_variable_map[condition[2]][1].split('.')
                    new_col = getattr(table_obj_map[origin_column[0]], origin_column[1])
                    select.append(new_col.as_(condition[0]))


        q = MySQLQuery.from_(tables[0]).select(
            *select
        )

        for j_table in join:
            q = q.join(j_table)

        for j_condition in join_condition_queue:
            q = q.on(j_condition[0] == j_condition[1])

        for w_statement in where:
            q = q.where(w_statement)

        # select = [Field('id'), Field('fname'), Field('lname'), Field('phone').as_('alias_sample')]

        print("[Generated Query]: ", q.get_sql())
        #print("\n", q2.get_sql(quote_char="`"))
        return 0
