import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

clt = process.client('sql', format = 'csv', echo = False,
                   stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

for c in 'ntvsf':
    clt.stdin.write("select '\\\\d%s';\n" % c)

for c in 'ntvsf':
    clt.stdin.write("select '\\\\dS%s';\n" % c)

clt.stdin.write("select '\\\\dn ' || name from sys.schemas order by name;\n")

clt.stdin.write("select '\\\\dSt ' || s.name || '.' || t.name from sys._tables t, sys.schemas s where t.schema_id = s.id and t.query is null order by s.name, t.name;\n")

clt.stdin.write("select '\\\\dSv ' || s.name || '.' || t.name from sys._tables t, sys.schemas s where t.schema_id = s.id and t.query is not null order by s.name, t.name;\n")

clt.stdin.write("select distinct '\\\\dSf ' || s.name || '.\"' || f.name || '\"' from sys.functions f, sys.schemas s where f.language between 1 and 2 and f.schema_id = s.id and s.name = 'sys' order by s.name, f.name;\n")

out, err = clt.communicate()
out = out.replace('"\n', '\n').replace('\n"', '\n').replace('""', '"').replace(r'\\', '\\')

sys.stdout.write(out)
sys.stderr.write(err)

clt = process.client('sql', interactive = True,
                   stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

out, err = clt.communicate(out)

# do some normalization of the output:
# remove SQL comments and empty lines
out = re.sub('^[ \t]*(?:--.*)?\n', '', out, flags = re.M)
out = re.sub('[\t ]*--.*', '', out)
out = re.sub(r'/\*.*?\*/[\n\t ]*', '', out, flags = re.DOTALL)

wsre = re.compile('[\n\t ]+')
pos = 0
nout = ''
for res in re.finditer(r'\bbegin\b.*?\bend\b[\n\t ]*;', out, flags = re.DOTALL | re.IGNORECASE):
    nout += out[pos:res.start(0)] + wsre.sub(' ', res.group(0)).replace('( ', '(').replace(' )', ')')
    pos = res.end(0)
nout += out[pos:]
out = nout

pos = 0
nout = ''
for res in re.finditer(r'(?<=\n)(?:create|select)\b.*?;', out, flags = re.DOTALL | re.IGNORECASE):
    nout += out[pos:res.start(0)] + wsre.sub(' ', res.group(0)).replace('( ', '(').replace(' )', ')')
    pos = res.end(0)
nout += out[pos:]
out = nout

sys.stdout.write(out)
sys.stderr.write(err)

# add queries to dump the system tables, but avoid dumping IDs since
# they are too volatile, and if it makes sense, dump an identifier
# from a referenced table
out = r'''
-- helper function
create function pcre_replace(origin string, pat string, repl string, flags string) returns string external name pcre.replace;
-- schemas
select name, authorization, owner, system from sys.schemas order by name;
-- _tables
select s.name, t.name, replace(replace(pcre_replace(pcre_replace(t.query, '--.*\n*', '', ''), '[ \t\n]+', ' ', ''), '( ', '('), ' )', ')') as query, tt.table_type_name as type, t.system, ca.action_name as commit_action, at.value as access from sys._tables t left outer join sys.schemas s on t.schema_id = s.id left outer join sys.table_types tt on t.type = tt.table_type_id left outer join (values (0, 'COMMIT'), (1, 'DELETE'), (2, 'PRESERVE'), (3, 'DROP'), (4, 'ABORT')) as ca (action_id, action_name) on t.commit_action = ca.action_id left outer join (values (0, 'WRITABLE'), (1, 'READONLY'), (2, 'APPENDONLY')) as at (id, value) on t.access = at.id order by s.name, t.name;
-- _columns
select t.name, c.name, c.type, c.type_digits, c.type_scale, c."default", c."null", c.number, c.storage from sys._tables t, sys._columns c where t.id = c.table_id order by t.name, c.number;
-- partitioned tables (these three should be empty)
select t.name, c.name, p.expression from sys.table_partitions p left outer join sys._tables t on p.table_id = t.id left outer join sys._columns c on p.column_id = c.id;
select t.name, p.expression, r.minimum, r.maximum, r.with_nulls from sys.range_partitions r left outer join sys._tables t on t.id = r.table_id left outer join sys.table_partitions p on r.partition_id = p.id;
select t.name, p.expression, v.value from sys.value_partitions v left outer join sys._tables t on t.id = v.table_id left outer join sys.table_partitions p on v.partition_id = p.id;
-- external functions that don't reference existing MAL function (should be empty)
with funcs as (select name, pcre_replace(func, '--.*\n*', '', '') as func, schema_id from sys.functions), x (sname, name, modfunc) as (select s.name, f.name, replace(pcre_replace(f.func, '.*external name (.*);.*', '$1', 'ims'), '"', '') from funcs f left outer join sys.schemas s on f.schema_id = s.id where f.func ilike '% external name %') select * from x where x.modfunc not in (select m.module || '.' || m."function" from sys.malfunctions() m);
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

out += r"select s.name, f.name, case f.system when true then 'SYSTEM' else '' end as system, replace(replace(replace(pcre_replace(pcre_replace(pcre_replace(f.func, '--.*\n', '', ''), '[ \t\n]+', ' ', 'm'), '^ ', '', ''), '( ', '('), ' )', ')'), 'create system ', 'create ') as query, f.mod, fl.language_name, ft.function_type_name, f.side_effect, f.varres, f.vararg"
for i in range(0, MAXARGS):
    for a in args[:-1]:
        out += ", a%d.%s as %s%d" % (i, a, a, i)
    out += ", case a%d.inout when 0 then 'out' when 1 then 'in' end as inout%d" % (i, i)
out += " from sys.functions f"
out += " left outer join sys.schemas s on f.schema_id = s.id"
out += " left outer join sys.function_types as ft on f.type = ft.function_type_id"
out += " left outer join sys.function_languages fl on f.language = fl.language_id"
for i in range(0, MAXARGS):
    out += " left outer join sys.args a%d on a%d.func_id = f.id and a%d.number = %d" % (i, i, i, i)
out += " order by s.name, f.name, query"
for i in range(0, MAXARGS):
    for a in args:
        out += ", %s%d" % (a, i)
out += ";"

out += '''
-- auths
select name, grantor from sys.auths;
-- comments
select s.name, c.remark from sys.comments c, sys.schemas s where s.id = c.id order by s.name;
select s.name, t.name, c.remark from sys.schemas s, sys._tables t, sys.comments c where s.id = t.schema_id and t.id = c.id order by s.name, t.name;
select s.name, t.name, col.name, c.remark from sys.schemas s, sys._tables t, sys._columns col, sys.comments c where s.id = t.schema_id and t.id = col.table_id and col.id = c.id order by s.name, t.name, col.name;
select s.name, t.name, i.name, c.remark from sys.schemas s, sys._tables t, sys.idxs i, sys.comments c where s.id = t.schema_id and t.id = i.table_id and i.id = c.id order by s.name, t.name, i.name;
select s.name, q.name, c.remark from sys.schemas s, sys.sequences q, sys.comments c where s.id = q.schema_id and q.id = c.id order by s.name, q.name;
select s.name, f.name, c.remark from sys.schemas s, sys.functions f, sys.comments c where s.id = f.schema_id and f.id = c.id order by s.name, f.name;
-- db_user_info
select u.name, u.fullname, s.name from sys.db_user_info u left outer join sys.schemas s on u.default_schema = s.id order by u.name;
-- dependencies
select s1.name, f1.name, s2.name, f2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys.functions f1, sys.functions f2, sys.schemas s1, sys.schemas s2 where d.id = f1.id and d.depend_id = f2.id and f1.schema_id = s1.id and f2.schema_id = s2.id order by s2.name, f2.name, s1.name, f1.name;
select s1.name, t.name, s2.name, f.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys._tables t, sys.schemas s1, sys.functions f, sys.schemas s2 where d.id = t.id and d.depend_id = f.id and t.schema_id = s1.id and f.schema_id = s2.id order by s2.name, f.name, s1.name, t.name;
select s1.name, t.name, c.name, s2.name, f.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, sys._columns c, sys._tables t, sys.schemas s1, sys.functions f, sys.schemas s2 where d.id = c.id and d.depend_id = f.id and c.table_id = t.id and t.schema_id = s1.id and f.schema_id = s2.id order by s2.name, f.name, s1.name, t.name, c.name;
select s1.name, f1.name, s2.name, t2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, schemas s1, functions f1, schemas s2, _tables t2 where d.id = f1.id and f1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, f1.name;
select s1.name, t1.name, s2.name, t2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, schemas s1, _tables t1, schemas s2, _tables t2 where d.id = t1.id and t1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, t1.name;
select s1.name, t1.name, c1.name, s2.name, t2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, schemas s1, _tables t1, _columns c1, schemas s2, _tables t2 where d.id = c1.id and c1.table_id = t1.id and t1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, t1.name, c1.name;
select s1.name, t1.name, c1.name, s2.name, t2.name, k2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, _tables t1, _tables t2, schemas s1, schemas s2, _columns c1, keys k2 where d.id = c1.id and d.depend_id = k2.id and c1.table_id = t1.id and t1.schema_id = s1.id and k2.table_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, k2.name, s1.name, t1.name, c1.name;
select s1.name, t1.name, c1.name, s2.name, t2.name, i2.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, _tables t1, _tables t2, schemas s1, schemas s2, _columns c1, idxs i2 where d.id = c1.id and d.depend_id = i2.id and c1.table_id = t1.id and t1.schema_id = s1.id and i2.table_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, i2.name, s1.name, t1.name, c1.name;
select t.systemname, t.sqlname, s.name, f.name, dt.dependency_type_name from sys.dependencies d left outer join sys.dependency_types dt on d.depend_type = dt.dependency_type_id, types t, functions f, schemas s where d.id = t.id and d.depend_id = f.id and f.schema_id = s.id order by s.name, f.name, t.systemname, t.sqlname;
-- idxs
select t.name, i.name, it.index_type_name from sys.idxs i left outer join sys._tables t on t.id = i.table_id left outer join sys.index_types as it on i.type = it.index_type_id order by t.name, i.name;
-- keys
select t.name, k.name, kt.key_type_name, k2.name, k.action from sys.keys k left outer join sys.keys k2 on k.rkey = k2.id left outer join sys._tables t on k.table_id = t.id left outer join sys.key_types kt on k.type = kt.key_type_id order by t.name, k.name;
-- objects
select name, nr from sys.objects order by name, nr;
-- privileges
--  schemas
select s.name, u.name from sys.schemas s, sys.users u where s.id = u.default_schema order by s.name, u.name;
--  tables
select t.name, a.name, pc.privilege_code_name, g.name, p.grantable from sys._tables t, sys.privileges p left outer join sys.auths g on p.grantor = g.id left outer join sys.privilege_codes pc on p.privileges = pc.privilege_code_id, sys.auths a where t.id = p.obj_id and p.auth_id = a.id order by t.name, a.name;
--  columns
select t.name, c.name, a.name, pc.privilege_code_name, g.name, p.grantable from sys._tables t, sys._columns c, sys.privileges p left outer join sys.auths g on p.grantor = g.id left outer join sys.privilege_codes pc on p.privileges = pc.privilege_code_id, sys.auths a where c.id = p.obj_id and c.table_id = t.id and p.auth_id = a.id order by t.name, c.name, a.name;
--  functions
select f.name, a.name, pc.privilege_code_name, g.name, p.grantable from sys.functions f, sys.privileges p left outer join sys.auths g on p.grantor = g.id left outer join sys.privilege_codes pc on p.privileges = pc.privilege_code_id, sys.auths a where f.id = p.obj_id and p.auth_id = a.id order by f.name, a.name;
-- sequences
select s.name, q.name, q.start, q.minvalue, q.maxvalue, q.increment, q.cacheinc, q.cycle from sys.sequences q left outer join sys.schemas s on q.schema_id = s.id order by s.name, q.name;
-- statistics (expect empty)
select count(*) from sys.statistics;
-- storagemodelinput (expect empty)
select count(*) from sys.storagemodelinput;
-- triggers
select t.name, g.name, case g.time when 0 then 'BEFORE' when 1 then 'AFTER' when 2 then 'INSTEAD OF' end as time, case g.orientation when 0 then 'ROW' when 1 then 'STATEMENT' end as orientation, case g.event when 0 then 'insert' when 1 then 'DELETE' when 2 then 'UPDATE' end as event, g.old_name, g.new_name, g.condition, g.statement from sys.triggers g left outer join sys._tables t on g.table_id = t.id order by t.name, g.name;
-- types
select s.name, t.systemname, t.sqlname, t.digits, t.scale, t.radix, et.value as eclass from sys.types t left outer join sys.schemas s on s.id = t.schema_id left outer join (values (0, 'ANY'), (1, 'TABLE'), (2, 'BIT'), (3, 'CHAR'), (4, 'STRING'), (5, 'BLOB'), (6, 'POS'), (7, 'NUM'), (8, 'MONTH'), (9, 'SEC'), (10, 'DEC'), (11, 'FLT'), (12, 'TIME'), (13, 'DATE'), (14, 'TIMESTAMP'), (15, 'GEOM'), (16, 'EXTERNAL')) as et (id, value) on t.eclass = et.id order by s.name, t.systemname, t.sqlname, t.digits, t.scale, t.radix, t.eclass;
-- user_role
select a1.name, a2.name from sys.auths a1, sys.auths a2, sys.user_role ur where a1.id = ur.login_id and a2.id = ur.role_id order by a1.name, a2.name;
-- keywords
select keyword from sys.keywords order by keyword;
-- table_types
select table_type_id, table_type_name from sys.table_types order by table_type_id, table_type_name;
-- dependency_types
select dependency_type_id, dependency_type_name from sys.dependency_types order by dependency_type_id, dependency_type_name;
-- drop helper function
drop function pcre_replace(string, string, string, string);
'''

sys.stdout.write(out)

clt = process.client('sql', interactive = True,
                   stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

out, err = clt.communicate(out)

sys.stdout.write(out)
sys.stderr.write(err)
