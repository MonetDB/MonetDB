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
    c = process.client('sql',
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
select * from t1;
'''

script2 = '''\
create view v1 as select * from t1;
'''

script3 = '''\
create view v2 as select * from t1;
'''

script4 = '''\
drop view v2;
'''

script5 = '''\
select * from v1;
'''

script6 = '''\
drop view v1;
'''

script7 = '''\
insert into v1 (a) values ( 1 );
'''

script8 = '''\
update v1 set a = 2 where a = 1;
'''

script9 = '''\
delete from v1 where a = 1;
'''

script10 = '''\
insert into v1 (a) values ( 2 );
'''

script11 = '''\
update v1 set a = 3 where a = 2;
'''

script12 = '''\
delete from v1 where a = 3;
'''

def main():
    s = process.server(args = [],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    client(script1)
    client(script2)
    client(script3)
    client(script4)
    client(script10)
    client(script11)
    client(script12)
    server_stop(s)
    s = process.server(args = ["--readonly"],
                       stdin = process.PIPE,
                       stdout = process.PIPE,
                       stderr = process.PIPE)
    client(script1)
    client(script3)
    client(script6)
    client(script5)
    client(script7)
    client(script8)
    client(script9)
    server_stop(s)

if __name__ == '__main__':
    main()
