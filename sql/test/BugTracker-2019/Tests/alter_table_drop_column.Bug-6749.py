import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


def client(input):
    c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('''\
create table t (a int, b int, c int);\
alter table t add unique (b);
''')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('alter table t drop column c;')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('alter table t drop column b; --error, b has a depenency')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('''\
select count(*) from objects inner join dependencies on objects.id = dependencies.depend_id inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select count(*) from dependencies inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select keys.type, keys.name, keys.rkey, keys.action from keys inner join tables on tables.id = keys.table_id where tables.name = 't';\
select idxs.type, idxs.name from idxs inner join tables on tables.id = idxs.table_id where tables.name = 't';\
alter table t drop column b cascade;\
select count(*) from objects inner join dependencies on objects.id = dependencies.depend_id inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select count(*) from dependencies inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select keys.type, keys.name, keys.rkey, keys.action from keys inner join tables on tables.id = keys.table_id where tables.name = 't';\
select idxs.type, idxs.name from idxs inner join tables on tables.id = idxs.table_id where tables.name = 't';
''')
server_stop(s)

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client('''\
drop table t;\
start transaction;\
create table t (a int, b int, c int);\
alter table t add unique (b);\
select * from t;\
select count(*) from objects inner join dependencies on objects.id = dependencies.depend_id inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select count(*) from dependencies inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select keys.type, keys.name, keys.rkey, keys.action from keys inner join tables on tables.id = keys.table_id where tables.name = 't';\
select idxs.type, idxs.name from idxs inner join tables on tables.id = idxs.table_id where tables.name = 't';\
alter table t drop column b cascade;\
select count(*) from objects inner join dependencies on objects.id = dependencies.depend_id inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select count(*) from dependencies inner join columns on dependencies.id = columns.id inner join tables on columns.table_id = tables.id where tables.name = 't';\
select keys.type, keys.name, keys.rkey, keys.action from keys inner join tables on tables.id = keys.table_id where tables.name = 't';\
select idxs.type, idxs.name from idxs inner join tables on tables.id = idxs.table_id where tables.name = 't';\
select * from t;\
commit;\
select * from t;\
drop table t;
''')
server_stop(s)
