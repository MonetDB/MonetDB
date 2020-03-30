import os, socket, sys, tempfile, shutil
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

farm_dir = tempfile.mkdtemp()
os.mkdir(os.path.join(farm_dir, 'db1'))
myport = freeport()

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    c = process.client('sql', port = myport, dbname='db1', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
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

s = None
try:
    s = process.server(mapiport=myport, dbname='db1',
                       dbfarm=os.path.join(farm_dir, 'db1'),
                       stdin = process.PIPE,
                       stdout=process.PIPE, stderr=process.PIPE)
    client(script1)
    server_stop(s)
    s = process.server(mapiport=myport, dbname='db1',
                       dbfarm=os.path.join(farm_dir, 'db1'),
                       stdin=process.PIPE,
                       stdout=process.PIPE, stderr=process.PIPE)
    client(script2)
    server_stop(s)
finally:
    if s is not None:
        s.terminate()
    shutil.rmtree(farm_dir)
