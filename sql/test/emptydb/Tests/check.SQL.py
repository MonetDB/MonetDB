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
select s.name, t.name, replace(replace(pcre_replace(pcre_replace(pcre_replace(t.query, '--.*\n', '', ''), '[ \t\n]+', ' ', 'm'), '^ ', '', ''), '( ', '('), ' )', ')') as query, t.type, t.system, t.commit_action, t.access from sys._tables t left outer join sys.schemas s on t.schema_id = s.id order by s.name, t.name;
-- _columns
select t.name, c.name, c.type, c.type_digits, c.type_scale, c."default", c."null", c.number, c.storage from sys._tables t, sys._columns c where t.id = c.table_id order by t.name, c.number;
-- functions
select s.name, f.name, replace(replace(pcre_replace(pcre_replace(pcre_replace(f.func, '--.*\n', '', ''), '[ \t\n]+', ' ', 'm'), '^ ', '', ''), '( ', '('), ' )', ')') as query, f.mod, f.language, f.type, f.side_effect, f.varres, f.vararg from sys.functions f left outer join sys.schemas s on f.schema_id = s.id order by s.name, f.name, query;
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

out += "with\n"
for i in range(1, MAXARGS + 1):
    out += "arg%d (id" % i
    for j in range(1, i + 1):
        for k in ['id'] + args:
            out += ", %s%d" % (k, j)
    out += ") as (select "
    if i == 1:
        out += "f.id"
        for k in ['id'] + args:
            out += ", a%d.%s" % (i, k)
        out += " from sys.functions f left outer join args a%d on a%d.func_id = f.id" % (i, i)
    else:
        out += "arg%d.*" % (i - 1)
        for k in ['id'] + args:
            out += ", a%d.%s" % (i, k)
        out += " from arg%d left outer join args a%d on a%d.func_id = arg%d.id" % (i - 1, i, i, i - 1)
    out += " and a%d.number = %d)" % (i, i)
    if i < MAXARGS:
        out += ","
    out += "\n"
out += "select s.name, f.name"
for i in range(1, MAXARGS):
    for k in args:
        out += ", arg%d.%s%d" % (MAXARGS, k, i)
out += " from arg%d, sys.schemas s, sys.functions f where s.id = f.schema_id and f.id = arg%d.id order by s.name, f.name" % (MAXARGS, MAXARGS)
for i in range(1, MAXARGS):
    for k in args:
        out += ", arg%d.%s%d" % (MAXARGS, k, i)
out += ";"
out += '''
-- auths
select name, grantor from sys.auths;
-- db_user_info
select u.name, u.fullname, s.name from sys.db_user_info u left outer join sys.schemas s on u.default_schema = s.id order by u.name;
-- dependencies
select s1.name, f1.name, s2.name, f2.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), sys.dependencies d, sys.functions f1, sys.functions f2, sys.schemas s1, sys.schemas s2 where d.depend_type = dt.id and d.id = f1.id and d.depend_id = f2.id and f1.schema_id = s1.id and f2.schema_id = s2.id order by s2.name, f2.name, s1.name, f1.name;
select s1.name, t.name, s2.name, f.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), sys.dependencies d, sys._tables t, sys.schemas s1, sys.functions f, sys.schemas s2 where d.depend_type = dt.id and d.id = t.id and d.depend_id = f.id and t.schema_id = s1.id and f.schema_id = s2.id order by s2.name, f.name, s1.name, t.name;
select s1.name, t.name, c.name, s2.name, f.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), sys.dependencies d, sys._columns c, sys._tables t, sys.schemas s1, sys.functions f, sys.schemas s2 where d.depend_type = dt.id and d.id = c.id and d.depend_id = f.id and c.table_id = t.id and t.schema_id = s1.id and f.schema_id = s2.id order by s2.name, f.name, s1.name, t.name, c.name;
select s1.name, f1.name, s2.name, t2.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), schemas s1, functions f1, schemas s2, _tables t2, dependencies d where d.depend_type = dt.id and d.id = f1.id and f1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, f1.name;
select s1.name, t1.name, s2.name, t2.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), schemas s1, _tables t1, schemas s2, _tables t2, dependencies d where d.depend_type = dt.id and d.id = t1.id and t1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, t1.name;
select s1.name, t1.name, c1.name, s2.name, t2.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), schemas s1, _tables t1, _columns c1, schemas s2, _tables t2, dependencies d where d.depend_type = dt.id and d.id = c1.id and c1.table_id = t1.id and t1.schema_id = s1.id and d.depend_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, s1.name, t1.name;
select s1.name, t1.name, c1.name, s2.name, t2.name, k2.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), dependencies d, _tables t1, _tables t2, schemas s1, schemas s2, _columns c1, keys k2 where d.depend_type = dt.id and d.id = c1.id and d.depend_id = k2.id and c1.table_id = t1.id and t1.schema_id = s1.id and k2.table_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, k2.name, s1.name, t1.name, c1.name;
select s1.name, t1.name, c1.name, s2.name, t2.name, i2.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), dependencies d, _tables t1, _tables t2, schemas s1, schemas s2, _columns c1, idxs i2 where d.depend_type = dt.id and d.id = c1.id and d.depend_id = i2.id and c1.table_id = t1.id and t1.schema_id = s1.id and i2.table_id = t2.id and t2.schema_id = s2.id order by s2.name, t2.name, i2.name, s1.name, t1.name, c1.name;
select t.systemname, t.sqlname, s.name, f.name, dt.name from (values (1, 'SCHEMA_DEPENDENCY'), (2, 'TABLE_DEPENDENCY'), (3, 'COLUMN_DEPENDENCY'), (4, 'KEY_DEPENDENCY'), (5, 'VIEW_DEPENDENCY'), (6, 'USER_DEPENDENCY'), (7, 'FUNC_DEPENDENCY'), (8, 'TRIGGER_DEPENDENCY'), (9, 'OWNER_DEPENDENCY'), (10, 'INDEX_DEPENDENCY'), (11, 'FKEY_DEPENDENCY'), (12, 'SEQ_DEPENDENCY'), (13, 'PROC_DEPENDENCY'), (14, 'BEDROPPED_DEPENDENCY'), (15, 'TYPE_DEPENDENCY')) as dt (id, name), types t, functions f, schemas s, dependencies d where d.depend_type = dt.id and d.id = t.id and d.depend_id = f.id and f.schema_id = s.id order by s.name, f.name, t.systemname, t.sqlname;
-- idxs
select t.name, i.name, i.type from sys.idxs i left outer join sys._tables t on t.id = i.table_id order by t.name, i.name;
-- keys
with x as (select k.id as id, t.name as tname, k.name as kname, k.type as type, k.rkey as rkey, k.action as action from sys.keys k left outer join sys._tables t on t.id = k.table_id) select x.tname, x.kname, x.type, y.kname, x.action from x left outer join x y on x.rkey = y.id order by x.tname, x.kname;
-- objects
select name, nr from sys.objects order by name, nr;
-- privileges
--  schemas
select s.name, u.name from sys.schemas s, sys.users u where s.id = u.default_schema order by s.name, u.name;
--  tables
select t.name, a.name, p.privileges, g.name, p.grantable from sys._tables t, sys.privileges p left outer join sys.auths g on p.grantor = g.id, sys.auths a where t.id = p.obj_id and p.auth_id = a.id order by t.name, a.name;
--  columns
select t.name, c.name, a.name, p.privileges, g.name, p.grantable from sys._tables t, sys._columns c, sys.privileges p left outer join sys.auths g on p.grantor = g.id, sys.auths a where c.id = p.obj_id and c.table_id = t.id and p.auth_id = a.id order by t.name, c.name, a.name;
--  functions
select f.name, a.name, p.privileges, g.name, p.grantable from sys.functions f, sys.privileges p left outer join sys.auths g on p.grantor = g.id, sys.auths a where f.id = p.obj_id and p.auth_id = a.id order by f.name, a.name;
-- sequences
select s.name, q.name, q.start, q.minvalue, q.maxvalue, q.increment, q.cacheinc, q.cycle from sys.sequences q left outer join sys.schemas s on q.schema_id = s.id order by s.name, q.name;
-- statistics (expect empty)
select count(*) from sys.statistics;
-- storagemodelinput (expect empty)
select count(*) from sys.storagemodelinput;
-- systemfunctions
select f.name from sys.systemfunctions s left outer join sys.functions f on s.function_id = f.id order by f.name;
-- triggers
select t.name, g.name, g.time, g.orientation, g.event, g.old_name, g.new_name, g.condition, g.statement from sys.triggers g left outer join sys._tables t on g.table_id = t.id order by t.name, g.name;
-- types
select s.name, t.systemname, t.sqlname, t.digits, t.scale, t.radix, t.eclass from sys.types t left outer join sys.schemas s on s.id = t.schema_id order by s.name, t.systemname, t.sqlname, t.digits, t.scale, t.radix, t.eclass;
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
