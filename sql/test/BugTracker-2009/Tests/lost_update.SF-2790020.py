import sys
import os
import time
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server():
    return process.server(stdin = process.PIPE,
                          stdout = process.PIPE,
                          stderr = process.PIPE)

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    c = process.client('sql',
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
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
'''
script3 = '''\
select a from lost_update_t2;
'''
cleanup = '''\
drop table lost_update_t1;
drop table lost_update_t2;
'''

def main():
    s = server()
    client(script1)
    server_stop(s)

    s = server()
    client(script2)
    time.sleep(20)                      # wait until log is flushed originally 60 sec
    server_stop(s)

    s = server()
    client(script3)
    server_stop(s)

    s = server()
    client(cleanup)
    server_stop(s)

if __name__ == '__main__':
    main()
