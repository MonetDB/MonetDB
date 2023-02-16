import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

port = None
dbname = os.getenv('TSTDB', 'demo')
host = None
user = 'monetdb'
passwd = 'monetdb'
approve = None
check = None
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run check queries on a database')
    parser.add_argument('--host', action='store', default=host,
                        help='hostname where the server runs')
    parser.add_argument('--port', action='store', type=int, default=port,
                        help='port the server listens on')
    parser.add_argument('--database', action='store', default=dbname,
                        help='name of the database')
    parser.add_argument('--user', action='store', default=user,
                        help='user name to login to the database with')
    parser.add_argument('--password', action='store', default=passwd,
                        help='password to use to login to the database with')
    parser.add_argument('--approve', action='store', default=approve,
                        type=argparse.FileType('w'),
                        help='file in which to produce a new .test file '
                        'with updated results')
    parser.add_argument('check', nargs='*', help='name of test')
    opts = parser.parse_args()
    port = opts.port
    dbname = opts.database
    host = opts.host
    user = opts.user
    passwd = opts.password
    approve = opts.approve
    check = opts.check

xit = 0
output = []

sys_pkeys = [
    ('schemas', 'id'),
    ('_tables', 'id'),
    ('tables', 'id'),
    ('_columns', 'id'),
    ('columns', 'id'),
    ('functions', 'id'),
    ('args', 'id'),
    ('types', 'id'),
    ('objects', 'id, nr'),
    ('keys', 'id'),
    ('idxs', 'id'),
    ('triggers', 'id'),
    ('sequences', 'id'),
    ('dependency_types', 'dependency_type_id'),
    ('dependencies', 'id, depend_id'),
    ('auths', 'id'),
    ('users', 'name'),
    ('user_role', 'login_id, role_id'),
    ('privileges', 'obj_id, auth_id, privileges'),
    ('querylog_catalog', 'id'),
    ('querylog_calls', 'id'),
    ('querylog_history', 'id'),
    ('optimizers', 'name'),
    ('environment', 'name'),
    ('db_user_info', 'name'),
    ('statistics', 'column_id'),
    ('"storage"()', 'schema, table, column'),
    ('storagemodelinput', 'schema, table, column'),

    ('rejects', 'rowid'),

    ('keywords', 'keyword'),
    ('table_types', 'table_type_id'),

    ('function_languages', 'language_id'),
    ('function_types', 'function_type_id'),
    ('index_types', 'index_type_id'),
    ('key_types', 'key_type_id'),
    ('privilege_codes', 'privilege_code_id'),

    ('comments', 'id'),
    ('ids', 'id'),
    ('var_values', 'var_name'),

    ('table_partitions', 'id'),
    ('range_partitions', 'table_id, partition_id, minimum'),
    ('value_partitions', 'table_id, partition_id, "value"'),

    ('queue', 'tag'),
    ('sessions', 'sessionid'),

    ('fkey_actions', 'action_id'),
    ('fkeys', 'id'),
]

sys_akeys = [
    ('schemas', 'name'),
    ('_tables', 'schema_id, name'),
    ('tables', 'schema_id, name'),
    ('_columns', 'table_id, name'),
    ('columns', 'table_id, name'),
    ('_columns', 'table_id, number'),
    ('columns', 'table_id, number'),

    ('(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) as T', 'T.id'),
    ('(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys.tables UNION ALL SELECT id FROM sys.columns UNION ALL SELECT id FROM sys.functions) as T', 'T.id'),

    ('functions f join sys.args a on f.id=a.func_id', 'schema_id, f.name, func, mod, language, f.type, side_effect, varres, vararg, a.id'),
    ('args', 'func_id, name, inout'),
    ('types', 'schema_id, systemname, sqlname'),
    ('objects', 'id, name'),
    ('keys', 'table_id, name'),
    ('idxs', 'table_id, name'),
    ('triggers', 'table_id, name'),
    ('sequences', 'schema_id, name'),
    ('dependency_types', 'dependency_type_name'),
    ('auths', 'name'),
    ('optimizers', 'def'),

    ('table_types', 'table_type_name'),
    ('function_types', 'function_type_name'),
    ('function_languages', 'language_name'),
    ('index_types', 'index_type_name'),
    ('key_types', 'key_type_name'),
    ('privilege_codes', 'privilege_code_name'),
    ('comments', 'id'),

    ('table_partitions WHERE column_id IS NOT NULL', 'table_id, column_id'),
    ('table_partitions WHERE "expression" IS NOT NULL', 'table_id, "expression"'),
    ('range_partitions', 'table_id, partition_id, "maximum"'),

    ('fkey_actions', 'action_name'),
    ('fkeys', 'table_id, name'),
]

sys_fkeys = [
    ('schemas', 'authorization', 'id', 'auths'),
    ('schemas', 'owner', 'id', 'auths'),
    ('_tables', 'schema_id', 'id', 'schemas'),
    ('tables', 'schema_id', 'id', 'schemas'),
    ('_tables', 'type', 'table_type_id', 'table_types'),
    ('tables', 'type', 'table_type_id', 'table_types'),
    ('_columns', 'table_id', 'id', '_tables'),
    ('columns', 'table_id', 'id', 'tables'),
    ('_columns', 'type', 'sqlname', 'types'),
    ('columns', 'type', 'sqlname', 'types'),
    ('functions', 'schema_id', 'id', 'schemas'),
    ('functions', 'type', 'function_type_id', 'function_types'),
    ('functions', 'language', 'language_id', 'function_languages'),

    ('functions WHERE system AND ', 'schema_id', 'id', 'schemas WHERE system'),
    ('args', 'func_id', 'id', 'functions'),
    ('args', 'type', 'sqlname', 'types'),
    ('types', 'schema_id', 'id', 'schemas'),

    ('objects', 'id', 'id', 'ids'),
    ('ids WHERE obj_type IN (\'key\', \'index\') AND ', 'id', 'id', 'objects'),
    ('keys', 'id', 'id', 'objects'),
    ('keys', 'table_id', 'id', '_tables'),
    ('keys', 'table_id', 'id', 'tables'),
    ('keys', 'type', 'key_type_id', 'key_types'),
    ('keys WHERE rkey <> -1 AND ', 'rkey', 'id', 'keys'),

    ('idxs', 'id', 'id', 'objects'),
    ('idxs', 'table_id', 'id', '_tables'),
    ('idxs', 'table_id', 'id', 'tables'),
    ('idxs', 'type', 'index_type_id', 'index_types'),
    ('sequences', 'schema_id', 'id', 'schemas'),
    ('triggers', 'table_id', 'id', '_tables'),
    ('triggers', 'table_id', 'id', 'tables'),
    ('comments', 'id', 'id', 'ids'),
    ('dependencies', 'id', 'id', 'ids'),
    ('dependencies', 'depend_id', 'id', 'ids'),
    ('dependencies', 'depend_type', 'dependency_type_id', 'dependency_types'),
    ('dependencies', 'id, depend_id, depend_type', 'v.id, v.used_by_id, v.depend_type', 'dependencies_vw v'),
    ('auths WHERE grantor > 0 AND ', 'grantor', 'id', 'auths'),
    ('users', 'name', 'name', 'auths'),
    ('users', 'default_schema', 'id', 'schemas'),
    ('db_user_info', 'name', 'name', 'auths'),
    ('db_user_info', 'default_schema', 'id', 'schemas'),
    ('user_role', 'login_id', 'id', 'auths'),
    ('user_role', 'login_id', 'a.id', 'auths a WHERE a.name IN (SELECT u.name FROM sys.users u)'),
    ('user_role', 'role_id', 'id', 'auths'),
    ('user_role', 'role_id', 'a.id', 'auths a WHERE a.name IN (SELECT u.name FROM sys.users u)'),
    ('user_role', 'role_id', 'id', 'roles'),
    ('privileges', 'obj_id', 'id', '(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) as t'),
    ('privileges', 'auth_id', 'id', 'auths'),
    ('privileges WHERE grantor > 0 AND ', 'grantor', 'id', 'auths'),
    ('privileges', 'privileges', 'privilege_code_id', 'privilege_codes'),
    ('querylog_catalog', 'owner', 'name', 'users'),
    ('querylog_catalog', 'pipe', 'name', 'optimizers'),
    ('querylog_calls', 'id', 'id', 'querylog_catalog'),
    ('querylog_history', 'id', 'id', 'querylog_catalog'),
    ('querylog_history', 'owner', 'name', 'users'),
    ('querylog_history', 'pipe', 'name', 'optimizers'),

    ('sessions', '"username"', 'name', 'users'),
    ('sessions', 'optimizer', 'name', 'optimizers'),
    ('statistics', 'column_id', 'id', '(SELECT id FROM sys._columns UNION ALL SELECT id FROM tmp._columns) as c'),
    ('statistics', 'type', 'sqlname', 'types'),
    ('storage()', 'schema', 'name', 'schemas'),
    ('storage()', 'table', 'name', '(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t'),
    ('storage()', 'schema, table', 'sname, tname', '(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t'),
    ('storage()', 'column', 'name', '(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c'),
    ('storage()', 'type', 'sqlname', 'types'),
    ('storage', 'schema', 'name', 'schemas'),
    ('storage', 'table', 'name', '(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t'),
    ('storage', 'schema, table', 'sname, tname', '(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t'),
    ('storage', 'column', 'name', '(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c'),
    ('storage', 'type', 'sqlname', 'types'),
    ('storagemodel', 'schema', 'name', 'schemas'),
    ('storagemodel', 'table', 'name', '(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t'),
    ('storagemodel', 'schema, table', 'sname, tname', '(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t'),
    ('storagemodel', 'column', 'name', '(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c'),
    ('storagemodel', 'type', 'sqlname', 'types'),
    ('storagemodelinput', 'schema', 'name', 'schemas'),
    ('storagemodelinput', 'table', 'name', '(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t'),
    ('storagemodelinput', 'schema, table', 'sname, tname', '(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t'),
    ('storagemodelinput', 'column', 'name', '(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c'),
    ('storagemodelinput', 'type', 'sqlname', 'types'),
    ('tablestoragemodel', 'schema', 'name', 'schemas'),
    ('tablestoragemodel', 'table', 'name', '(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t'),
    ('tablestoragemodel', 'schema, table', 'sname, tname', '(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t'),

    ('schemastorage', 'schema', 'name', 'schemas'),
    ('tablestorage', 'schema', 'name', 'schemas'),
    ('tablestorage', 'table', 'name', '(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t'),
    ('tablestorage', 'schema, table', 'sname, tname', '(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t'),
    ('table_partitions', 'table_id', 'id', '_tables'),
    ('table_partitions WHERE column_id IS NOT NULL AND ', 'column_id', 'id', '_columns'),
    ('range_partitions', 'table_id', 'id', '_tables'),
    ('range_partitions', 'partition_id', 'id', 'table_partitions'),
    ('value_partitions', 'table_id', 'id', '_tables'),
    ('value_partitions', 'partition_id', 'id', 'table_partitions'),

    ('keys WHERE action >= 0 AND ', 'cast(((action >> 8) & 255) as smallint)', 'action_id', 'fkey_actions'),
    ('keys WHERE action >= 0 AND ', 'cast((action & 255) as smallint)', 'action_id', 'fkey_actions'),
    ('fkeys', 'id, table_id, type, name, rkey', 'id, table_id, type, name, rkey', 'keys'),
    ('fkeys', 'update_action_id', 'action_id', 'fkey_actions'),
    ('fkeys', 'delete_action_id', 'action_id', 'fkey_actions'),

]

sys_notnull = [
    ('_columns', 'id'),
    ('_columns', 'name'),
    ('_columns', 'type'),
    ('_columns', 'type_digits'),
    ('_columns', 'type_scale'),
    ('_columns', 'table_id'),
    ('_columns', '"null"'),
    ('_columns', 'number'),
    ('_tables', 'id'),
    ('_tables', 'name'),
    ('_tables', 'schema_id'),
    ('_tables', 'type'),
    ('_tables', 'system'),
    ('_tables', 'commit_action'),
    ('_tables', 'access'),
    ('args', 'id'),
    ('args', 'func_id'),
    ('args', 'name'),
    ('args', 'type'),
    ('args', 'type_digits'),
    ('args', 'type_scale'),
    ('args', 'inout'),
    ('args', 'number'),
    ('auths', 'id'),
    ('auths', 'name'),
    ('auths', 'grantor'),
    ('db_user_info', 'name'),
    ('db_user_info', 'fullname'),
    ('db_user_info', 'default_schema'),
    ('dependencies', 'id'),
    ('dependencies', 'depend_id'),
    ('dependencies', 'depend_type'),
    ('function_languages', 'language_id'),
    ('function_languages', 'language_name'),
    ('function_types', 'function_type_id'),
    ('function_types', 'function_type_name'),
    ('function_types', 'function_type_keyword'),
    ('functions', 'id'),
    ('functions', 'name'),
    ('functions', 'func'),
    ('functions', 'mod'),
    ('functions', 'language'),
    ('functions', 'type'),
    ('functions', 'side_effect'),
    ('functions', 'varres'),
    ('functions', 'vararg'),
    ('functions', 'schema_id'),
    ('functions', 'system'),
    ('idxs', 'id'),
    ('idxs', 'table_id'),
    ('idxs', 'type'),
    ('idxs', 'name'),
    ('index_types', 'index_type_id'),
    ('index_types', 'index_type_name'),
    ('key_types', 'key_type_id'),
    ('key_types', 'key_type_name'),
    ('keys', 'id'),
    ('keys', 'table_id'),
    ('keys', 'type'),
    ('keys', 'name'),
    ('keys', 'rkey'),
    ('keys', 'action'),
    ('keywords', 'keyword'),
    ('objects', 'id'),
    ('objects', 'name'),
    ('objects', 'nr'),
    ('optimizers', 'name'),
    ('optimizers', 'def'),
    ('optimizers', 'status'),
    ('privilege_codes', 'privilege_code_id'),
    ('privilege_codes', 'privilege_code_name'),
    ('privileges', 'obj_id'),
    ('privileges', 'auth_id'),
    ('privileges', 'privileges'),
    ('privileges', 'grantor'),
    ('privileges', 'grantable'),
    ('schemas', 'id'),
    ('schemas', 'name'),
    ('schemas', 'authorization'),
    ('schemas', 'owner'),
    ('schemas', 'system'),
    ('sequences', 'id'),
    ('sequences', 'schema_id'),
    ('sequences', 'name'),
    ('sequences', 'start'),
    ('sequences', 'minvalue'),
    ('sequences', 'maxvalue'),
    ('sequences', 'increment'),
    ('sequences', 'cacheinc'),
    ('sequences', 'cycle'),
    ('statistics', 'column_id'),
    ('statistics', '"schema"'),
    ('statistics', '"table"'),
    ('statistics', '"column"'),
    ('statistics', 'type'),
    ('statistics', 'width'),
    ('statistics', 'count'),
    ('statistics', '"unique"'),
    ('statistics', 'nils'),
    ('statistics', 'sorted'),
    ('statistics', 'revsorted'),

    ('"storage"()', 'schema'),
    ('"storage"()', 'table'),
    ('"storage"()', 'column'),
    ('"storage"()', 'type'),
    ('"storage"()', 'mode'),
    ('"storage"()', 'location'),
    ('"storage"()', 'count'),
    ('"storage"()', 'typewidth'),
    ('"storage"()', 'columnsize'),
    ('"storage"()', 'heapsize'),
    ('"storage"()', 'hashes'),
    ('"storage"()', 'phash'),
    ('"storage"()', 'imprints'),
    ('"storage"()', 'orderidx'),
    ('storagemodelinput', 'schema'),
    ('storagemodelinput', 'table'),
    ('storagemodelinput', 'column'),
    ('storagemodelinput', 'type'),
    ('storagemodelinput', 'typewidth'),
    ('storagemodelinput', 'count'),
    ('storagemodelinput', '"distinct"'),
    ('storagemodelinput', 'atomwidth'),
    ('storagemodelinput', 'reference'),
    ('storagemodelinput', 'sorted'),
    ('storagemodelinput', '"unique"'),
    ('storagemodelinput', 'isacolumn'),
    ('table_types', 'table_type_id'),
    ('table_types', 'table_type_name'),
    ('tables', 'id'),
    ('tables', 'name'),
    ('tables', 'schema_id'),
    ('tables', 'type'),
    ('tables', 'system'),
    ('tables', 'commit_action'),
    ('tables', 'access'),
    ('tables', 'temporary'),
    ('tracelog', 'ticks'),
    ('tracelog', 'stmt'),
    ('triggers', 'id'),
    ('triggers', 'name'),
    ('triggers', 'table_id'),
    ('triggers', 'time'),
    ('triggers', 'orientation'),
    ('triggers', 'event'),
    ('triggers', 'statement'),
    ('types', 'id'),
    ('types', 'systemname'),
    ('types', 'sqlname'),
    ('types', 'digits'),
    ('types', 'scale'),
    ('types', 'radix'),
    ('types', 'eclass'),
    ('types', 'schema_id'),
    ('user_role', 'login_id'),
    ('user_role', 'role_id'),
    ('users', 'name'),
    ('users', 'fullname'),
    ('users', 'default_schema'),
    ('var_values', 'var_name'),
    ('var_values', 'value'),

    ('range_partitions', 'table_id'),
    ('range_partitions', 'partition_id'),
    ('range_partitions', 'with_nulls'),
    ('table_partitions', 'id'),
    ('table_partitions', 'table_id'),
    ('table_partitions', 'type'),
    ('value_partitions', 'table_id'),
    ('value_partitions', 'partition_id'),
    ('value_partitions', 'value'),

    ('fkey_actions', 'action_id'),
    ('fkey_actions', 'action_name'),
    ('fkeys', 'id'),
    ('fkeys', 'table_id'),
    ('fkeys', 'type'),
    ('fkeys', 'name'),
    ('fkeys', 'rkey'),
    ('fkeys', 'update_action_id'),
    ('fkeys', 'update_action'),
    ('fkeys', 'delete_action_id'),
    ('fkeys', 'delete_action')
]

# add queries to dump the system tables, but avoid dumping IDs since
# they are too volatile, and if it makes sense, dump an identifier
# from a referenced table
out = r'''
-- helper function
create function pcre_replace(origin string, pat string, repl string, flags string) returns string external name pcre.replace;
-- schemas
select 'sys.schemas', s.name, a1.name as authorization, a2.name as owner, system, c.remark as comment from sys.schemas s left outer join sys.auths a1 on s.authorization = a1.id left outer join sys.auths a2 on s.owner = a2.id left outer join sys.comments c on c.id = s.id order by s.name;
-- _tables
select 'sys._tables', s.name, t.name, replace(replace(pcre_replace(pcre_replace(t.query, E'--.*\n*', '', ''), E'[ \t\n]+', ' ', ''), '( ', '('), ' )', ')') as query, tt.table_type_name as type, t.system, ca.action_name as commit_action, at.value as access, c.remark as comment from sys._tables t left outer join sys.schemas s on t.schema_id = s.id left outer join sys.table_types tt on t.type = tt.table_type_id left outer join (values (0, 'COMMIT'), (1, 'DELETE'), (2, 'PRESERVE'), (3, 'DROP'), (4, 'ABORT')) as ca (action_id, action_name) on t.commit_action = ca.action_id left outer join (values (0, 'WRITABLE'), (1, 'READONLY'), (2, 'APPENDONLY')) as at (id, value) on t.access = at.id left outer join sys.comments c on c.id = t.id order by s.name, t.name;
-- _columns
select 'sys._columns', t.name, c.name, c.type, c.type_digits, c.type_scale, c."default", c."null", c.number, c.storage, r.remark as comment from sys._tables t, sys._columns c left outer join sys.comments r on r.id = c.id where t.id = c.table_id order by t.name, c.number;
-- partitioned tables (these three should be empty)
select 'sys.table_partitions', t.name, c.name, p.expression from sys.table_partitions p left outer join sys._tables t on p.table_id = t.id left outer join sys._columns c on p.column_id = c.id;
select 'sys.range_partitions', t.name, p.expression, r.minimum, r.maximum, r.with_nulls from sys.range_partitions r left outer join sys._tables t on t.id = r.table_id left outer join sys.table_partitions p on r.partition_id = p.id;
select 'sys.value_partitions', t.name, p.expression, v.value from sys.value_partitions v left outer join sys._tables t on t.id = v.table_id left outer join sys.table_partitions p on v.partition_id = p.id;
-- external functions that don't reference existing MAL function (should be empty)
with funcs as (select name, pcre_replace(func, E'--.*\n*', '', '') as func, schema_id from sys.functions), x (sname, name, modfunc) as (select s.name, f.name, replace(pcre_replace(f.func, '.*external name (.*);.*', '$1', 'ims'), '"', '') from funcs f left outer join sys.schemas s on f.schema_id = s.id where f.func ilike '% external name %') select 'dangling external functions', * from x where x.modfunc not in (select m.module || '.' || m."function" from sys.malfunctions() m);
-- args
'''
# generate a monster query to get all functions with all their
# arguments on a single row of a table

# maximum number of arguments used in any standard function (also
# determines the number of joins in the query and the number of
# columns in the result):
MAXARGS = 16
# columns of the args table we're interested in
args = ['name', 'type', 'type_digits', 'type_scale', 'inout']

out += r"select 'sys.functions', s.name, f.name, case f.system when true then 'SYSTEM' else '' end as system, replace(replace(replace(pcre_replace(pcre_replace(pcre_replace(f.func, E'--.*\n', '', ''), E'[ \t\n]+', ' ', 'm'), '^ ', '', ''), '( ', '('), ' )', ')'), 'create system ', 'create ') as query, f.mod, fl.language_name, ft.function_type_name as func_type, f.side_effect, f.varres, f.vararg, f.semantics, c.remark as comment"
for i in range(0, MAXARGS):
    for a in args[:-1]:
        out += ", a%d.%s as %s%d" % (i, a, a, i)
    out += ", case a%d.inout when 0 then 'out' when 1 then 'in' end as inout%d" % (i, i)
out += " from sys.functions f"
out += " left outer join sys.schemas s on f.schema_id = s.id"
out += " left outer join sys.function_types as ft on f.type = ft.function_type_id"
out += " left outer join sys.function_languages fl on f.language = fl.language_id"
out += " left outer join sys.comments c on c.id = f.id"
for i in range(0, MAXARGS):
    out += " left outer join sys.args a%d on a%d.func_id = f.id and a%d.number = %d" % (i, i, i, i)
out += " order by s.name, f.name, query, func_type"
for i in range(0, MAXARGS):
    for a in args:
        out += ", %s%d nulls first" % (a, i)
out += ";"

out += '''
-- auths
select 'sys.auths', a1.name as name, a2.name as grantor from sys.auths a1 left outer join sys.auths a2 on a1.grantor = a2.id order by a1.name;
-- db_user_info
select 'sys.db_user_info', u.name, u.fullname, s.name, u.schema_path, u.max_memory, u.max_workers, u.optimizer, a.name as default_role from sys.db_user_info u left outer join sys.schemas s on u.default_schema = s.id left outer join sys.auths a on u.default_role = a.id order by u.name;
-- dependencies
select 'function used by function', s1.name, f1.name, s2.name, f2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys.functions f1, sys.functions f2, sys.schemas s1, sys.schemas s2 where d.id = f1.id and d.depend_id = f2.id and f1.schema_id = s1.id and f2.schema_id = s2.id order by s2.name, f2.name, s1.name, f1.name;
select 'table used by function', s1.name, t.name, s2.name, f.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys._tables t, sys.schemas s1, sys.functions f, sys.schemas s2 where d.id = t.id and d.depend_id = f.id and t.schema_id = s1.id and f.schema_id = s2.id order by s2.name, f.name, s1.name, t.name;
select 'column used by function', s1.name, t.name, c.name, s2.name, f.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys._columns c, sys._tables t, sys.schemas s1, sys.functions f, sys.schemas s2 where d.id = c.id and d.depend_id = f.id and c.table_id = t.id and t.schema_id = s1.id and f.schema_id = s2.id order by s2.name, f.name, s1.name, t.name, c.name;
select 'function used by view', s1.name, f1.name, s2.name, t2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys.schemas s1, sys.functions f1, sys.schemas s2, sys._tables t2 where d.id = f1.id and f1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, f1.name;
select 'table used by view', s1.name, t1.name, s2.name, t2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys.schemas s1, sys._tables t1, sys.schemas s2, sys._tables t2 where d.id = t1.id and t1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, t1.name;
select 'column used by view', s1.name, t1.name, c1.name, s2.name, t2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys.schemas s1, sys._tables t1, sys._columns c1, sys.schemas s2, sys._tables t2 where d.id = c1.id and c1.table_id = t1.id and t1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, t1.name, c1.name;
select 'column used by key', s1.name, t1.name, c1.name, s2.name, t2.name, k2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys._tables t1, sys._tables t2, sys.schemas s1, sys.schemas s2, sys._columns c1, sys.keys k2 where d.id = c1.id and d.depend_id = k2.id and c1.table_id = t1.id and t1.schema_id = s1.id and k2.table_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, k2.name, s1.name, t1.name, c1.name;
select 'column used by index', s1.name, t1.name, c1.name, s2.name, t2.name, i2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys._tables t1, sys._tables t2, sys.schemas s1, sys.schemas s2, sys._columns c1, sys.idxs i2 where d.id = c1.id and d.depend_id = i2.id and c1.table_id = t1.id and t1.schema_id = s1.id and i2.table_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, i2.name, s1.name, t1.name, c1.name;
select 'type used by function', t.systemname, t.sqlname, s.name, f.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys.types t, sys.functions f, sys.schemas s where d.id = t.id and d.depend_id = f.id and f.schema_id = s.id order by s.name, f.name, t.systemname, t.sqlname;
-- idxs
select 'sys.idxs', t.name, i.name, it.index_type_name, c.remark as comment from sys.idxs i left outer join sys._tables t on t.id = i.table_id left outer join sys.index_types as it on i.type = it.index_type_id left outer join sys.comments c on c.id = i.id order by t.name, i.name;
-- keys
select 'sys.keys', t.name, k.name, kt.key_type_name, k2.name, k.action from sys.keys k left outer join sys.keys k2 on k.rkey = k2.id left outer join sys._tables t on k.table_id = t.id left outer join sys.key_types kt on k.type = kt.key_type_id order by t.name, k.name;
-- objects
select 'sys.objects', o.name, case when nr < 2000 then cast(nr as string) else s1.name || '.' || t1.name end as nr, s2.name || '.' || t2.name as sub from sys.objects o left outer join sys._tables t1 on o.nr = t1.id left outer join sys.schemas s1 on t1.schema_id = s1.id left outer join sys._tables t2 on o.sub = t2.id left outer join sys.schemas s2 on t2.schema_id = s2.id order by name, nr, sub;
-- privileges
--  schemas
select 'default schema of user', s.name, u.name from sys.schemas s, sys.users u where s.id = u.default_schema order by s.name, u.name;
--  tables
select 'grant on table', t.name, a.name, pc.privilege_code_name, g.name, p.grantable from sys._tables t, sys.privileges p left outer join sys.auths g on p.grantor = g.id left outer join sys.privilege_codes pc on p.privileges = pc.privilege_code_id, sys.auths a where t.id = p.obj_id and p.auth_id = a.id order by t.name, a.name;
--  columns
select 'grant on column', t.name, c.name, a.name, pc.privilege_code_name, g.name, p.grantable from sys._tables t, sys._columns c, sys.privileges p left outer join sys.auths g on p.grantor = g.id left outer join sys.privilege_codes pc on p.privileges = pc.privilege_code_id, sys.auths a where c.id = p.obj_id and c.table_id = t.id and p.auth_id = a.id order by t.name, c.name, a.name;
--  functions
select 'grant on function', s.name, f.name, a.name, pc.privilege_code_name, g.name, p.grantable from sys.functions f left outer join sys.schemas s on f.schema_id = s.id, sys.privileges p left outer join sys.auths g on p.grantor = g.id left outer join sys.privilege_codes pc on p.privileges = pc.privilege_code_id, sys.auths a where f.id = p.obj_id and p.auth_id = a.id order by s.name, f.name, a.name;
-- sequences
select 'sys.sequences', s.name, q.name, q.start, q.minvalue, q.maxvalue, q.increment, q.cacheinc, q.cycle, c.remark as comment from sys.sequences q left outer join sys.schemas s on q.schema_id = s.id left outer join sys.comments c on c.id = s.id order by s.name, q.name;
-- statistics (expect empty)
select count(*) from sys.statistics;
-- storagemodelinput (expect empty)
select count(*) from sys.storagemodelinput;
-- triggers
select 'sys.triggers', t.name, g.name, case g.time when 0 then 'BEFORE' when 1 then 'AFTER' when 2 then 'INSTEAD OF' end as time, case g.orientation when 0 then 'ROW' when 1 then 'STATEMENT' end as orientation, case g.event when 0 then 'insert' when 1 then 'DELETE' when 2 then 'UPDATE' end as event, g.old_name, g.new_name, g.condition, g.statement from sys.triggers g left outer join sys._tables t on g.table_id = t.id order by t.name, g.name;
-- types
select 'sys.types', s.name, t.systemname, t.sqlname, t.digits, t.scale, t.radix, et.value as eclass from sys.types t left outer join sys.schemas s on s.id = t.schema_id left outer join (values (0, 'ANY'), (1, 'TABLE'), (2, 'BIT'), (3, 'CHAR'), (4, 'STRING'), (5, 'BLOB'), (6, 'POS'), (7, 'NUM'), (8, 'MONTH'), (9, 'SEC'), (10, 'DEC'), (11, 'FLT'), (12, 'TIME'), (13, 'TIME_TZ'), (14, 'DATE'), (15, 'TIMESTAMP'), (16, 'TIMESTAMP_TZ'), (17, 'GEOM'), (18, 'EXTERNAL')) as et (id, value) on t.eclass = et.id order by s.name, t.systemname, t.sqlname, t.digits, t.scale, t.radix, eclass;
-- user_role
select 'sys.user_role', a1.name, a2.name from sys.auths a1, sys.auths a2, sys.user_role ur where a1.id = ur.login_id and a2.id = ur.role_id order by a1.name, a2.name;
-- keywords
select 'sys.keywords', keyword from sys.keywords order by keyword;
-- table_types
select 'sys.table_types', table_type_id, table_type_name from sys.table_types order by table_type_id, table_type_name;
-- function_types
select 'sys.function_types', function_type_name, function_type_keyword from sys.function_types order by function_type_keyword, function_type_name;
-- function_languages
select 'sys.function_languages', language_name, language_keyword from sys.function_languages order by language_keyword nulls first, language_name;
-- key_types
select 'sys.key_types', key_type_name from sys.key_types order by key_type_name;
-- index_types
select 'sys.index_types', index_type_name from sys.index_types order by index_type_name;
-- privilege_codes
select 'sys.privilege_codes', privilege_code_name from sys.privilege_codes order by privilege_code_name;
-- dependency_types
select 'sys.dependency_types', dependency_type_id, dependency_type_name from sys.dependency_types order by dependency_type_id, dependency_type_name;
-- drop helper function
drop function pcre_replace(string, string, string, string);
'''

for table, columns in sys_pkeys:
    qtable = table.replace('"', '')
    out += f'select \'duplicates in {qtable}\', count(*), {columns} from sys.{table} group by {columns} having count(*) > 1;\n'

for table, columns in sys_akeys:
    qtable = table.replace('"', '')
    if table.startswith('('):
        schema = ''
    else:
        schema = 'sys.'
    out += f'select \'duplicates in {qtable}\', count(*), {columns} from {schema}{table} group by {columns} having count(*) > 1;\n'

for table, columns, ref_columns, ref_table in sys_fkeys:
    if 'WHERE' in table:
        where = ''
    else:
        where = ' where'
    if '.' in ref_table:
        schema = ''
    else:
        schema = 'sys.'
    qtable = table.split(' ')[0]
    qcolumns = columns.replace('"', '')
    out += f'select \'missing reference in {qtable} {qcolumns}\', {columns}, * from sys.{table}{where} ({columns}) not in (select {ref_columns} from {schema}{ref_table});\n'

for table, column in sys_notnull:
    qtable = table.replace('"', '')
    qcolumn = column.replace('"', '')
    out += f'select \'null in {qtable}.{qcolumn}\', {column}, * from sys.{table} where {column} is null;\n'

output.append(out)

with process.client('sql', interactive=True, echo=False, format='test',
                    host=host, port=port, dbname=dbname, user=user, passwd=passwd,
                    stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as clt:

    out, err = clt.communicate(out)

    output.append(out)
    sys.stderr.write(err)
    if err:
        xit = 1

if check:
    output = ''.join(output).splitlines(keepends=True)
    stableout = 'check.stable.out.32bit' if os.getenv('TST_BITS', '') == '32bit' else 'check.stable.out.int128' if os.getenv('HAVE_HGE') else 'check.stable.out'
    stable = open(stableout).readlines()
    import difflib
    for line in difflib.unified_diff(stable, output, fromfile='test', tofile=stableout):
        sys.stderr.write(line)
        xit = 1
elif approve:
    approve.writelines(output)
else:
    sys.stdout.writelines(output)

sys.exit(xit)
