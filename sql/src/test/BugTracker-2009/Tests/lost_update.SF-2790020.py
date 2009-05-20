import sys
import os
import time
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def server():
    s = subprocess.Popen("%s --dbinit='include sql;'" % os.getenv('MSERVER'),
                         shell = True,
                         stdin = subprocess.PIPE,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    s.stdin.write('\nio.printf("\\nReady.\\n");\n')
    s.stdin.flush()
    while True:
        ln = s.stdout.readline()
        if not ln:
            print 'Unexpected EOF from server'
            sys.exit(1)
        sys.stdout.write(ln)
        if 'Ready' in ln:
            break
    return s

def client():
    c = subprocess.Popen("%s" % os.getenv('SQL_CLIENT'),
                         shell = True,
                         stdin = subprocess.PIPE,
                         stdout = subprocess.PIPE,
                         stderr = subprocess.PIPE)
    return c

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
    c = client()
    o, e = c.communicate(script1)
    sys.stdout.write(o)
    sys.stderr.write(e)
    o, e = s.communicate()
    sys.stdout.write(o)
    sys.stderr.write(e)

    s = server()
    c = client()
    o, e = c.communicate(script2)
    sys.stdout.write(o)
    sys.stderr.write(e)
    time.sleep(60)                      # wait until log is flushed
    o, e = s.communicate()
    sys.stdout.write(o)
    sys.stderr.write(e)

    s = server()
    c = client()
    o, e = c.communicate(script3)
    sys.stdout.write(o)
    sys.stderr.write(e)
    o, e = s.communicate()
    sys.stdout.write(o)
    sys.stderr.write(e)

    s = server()
    c = client()
    o, e = c.communicate(cleanup)
    sys.stdout.write(o)
    sys.stderr.write(e)
    o, e = s.communicate()
    sys.stdout.write(o)
    sys.stderr.write(e)

if __name__ == '__main__':
    main()
