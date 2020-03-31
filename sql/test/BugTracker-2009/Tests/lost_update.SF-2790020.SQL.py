import sys, os, socket, sys, tempfile, shutil

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

def server():
    return process.server(mapiport=myport, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE,
                   stdout = process.PIPE, stderr = process.PIPE)

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
create table lost_update_t2 (a int);
insert into lost_update_t2 values (1);
update lost_update_t2 set a = 2;
'''
script2 = '''\
update lost_update_t2 set a = 3;
create table lost_update_t1 (a int);
insert into lost_update_t1 values (1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
insert into lost_update_t1 (select * from lost_update_t1);
update lost_update_t1 set a = 2;
call sys.flush_log();
'''
script3 = '''\
select a from lost_update_t2;
'''
cleanup = '''\
drop table lost_update_t1;
drop table lost_update_t2;
'''

try:
    try:
        s = server()
        client(script1)
        server_stop(s)
    finally:
        s.terminate()

    try:
        s = server()
        client(script2)
        server_stop(s)
    finally:
        s.terminate()

    try:
        s = server()
        client(script3)
        server_stop(s)
    finally:
        s.terminate()

    try:
        s = server()
        client(cleanup)
        server_stop(s)
    finally:
        s.terminate()
finally:
    shutil.rmtree(farm_dir)
