#!/usr/bin/env python3

def delete_where_in_mega_value_list_query_statement(size, period):
    values = [str(x) for x in range(0, size-1) if x % period == 0]

    return "delete from table1 where id in ({});".format(','.join(values))

delete_query_sql_file = open("delete-query.sql", "w")

delete_query_sql_file.write(delete_where_in_mega_value_list_query_statement(1000000, 100))

delete_query_sql_file.close()
