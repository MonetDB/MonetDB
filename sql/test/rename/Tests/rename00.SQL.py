import os, socket, sys, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    with process.client('sql', port=myport, dbname='db1', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
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

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin = process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        client(script1)
        server_stop(s)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        client(script2)
        server_stop(s)
