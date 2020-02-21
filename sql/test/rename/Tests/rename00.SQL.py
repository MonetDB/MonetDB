import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    c = process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
create table "something" (a int);\
alter table "something" rename to "newname";\
insert into "newname" values (1);\
select "a" from "newname";
'''

script2 = '''\
select "name" from sys.tables where "name" = 'newname';\
insert into "newname" values (2);\
select "a" from "newname";\
drop table "newname";
'''

s = process.server(args=[], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
client(script1)
server_stop(s)
s = process.server(args=[], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
client(script2)
server_stop(s)
