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

myport = freeport()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with process.client('sql', port=myport, dbname='db1',
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as c:
            out, err = c.communicate('''\
start transaction;
create table table3282 (i int);
insert into table3282 values (0);
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
insert into table3282 select * from table3282;
select * from table3282 offset 2097140;
commit;
''')
            sys.stdout.write(out)
            sys.stderr.write(err)
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with process.client('sql', port=myport, dbname='db1',
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as c:
            out, err = c.communicate('select * from table3282 offset 2097140;\n')
            sys.stdout.write(out)
            sys.stderr.write(err)
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with process.client('sql', port=myport, dbname='db1', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
            out, err = c.communicate('select * from table3282 offset 2097140;\n'
                                     'drop table table3282;\n')
            sys.stdout.write(out)
            sys.stderr.write(err)
        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
