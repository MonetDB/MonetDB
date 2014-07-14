import sys
import os
try:
    from MonetDBtesting import process
except ImportError:
    import process

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
create table t1 (a int);
'''

script2 = '''\
create table t2 (a int);
'''

script3 = '''\
insert into t1 (a) values ( 1 );
'''

script4 = '''\
select * from t1;
'''

script5 = '''\
drop table t2;
'''
script6 = '''\
create table t3 (a) as select * from t1 with data;
'''

script7 = '''\
create table t4 (a) as select * from t1 with no data;
'''

script8 = '''\
drop table t1;
'''

script9 = '''\
create table t5 ( like t1 );
'''

script10 = '''\
create temporary table t6 ( a int);
'''

script11 = '''\
create local temporary table t7 ( a int );
'''

script12 = '''\
create global temporary table t8 ( a int );
'''

script13 = '''\
update t1 set a = 2 where a = 1;
'''

script14 = '''\
delete from t1 where a = 1;
'''

def main():
    s = process.server(args = [],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    client(script1)
    client(script3)
    client(script4)
    client(script2)
    client(script5)
    server_stop(s)
    s = process.server(args = ["--readonly"],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    client(script8)
    client(script4)
    client(script2)
    client(script6)
    client(script7)
    client(script9)
    client(script10)
    client(script11)
    client(script12)
    client(script3)
    client(script13)
    client(script14)
    server_stop(s)

if __name__ == '__main__':
    main()
