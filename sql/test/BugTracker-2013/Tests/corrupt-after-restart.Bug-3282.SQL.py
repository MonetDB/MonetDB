import os, sys, tempfile, pymonetdb
try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        cli = pymonetdb.connect(port=s.dbport,database='db1',autocommit=True)
        cur = cli.cursor()
        cur.execute('''
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
        commit;
        ''')
        cur.execute('select count(*) from table3282;')
        if cur.fetchall()[0][0] != 2097152:
            sys.stderr.write('Expected 2097152')
        cur.close()
        cli.close()
        s.communicate()
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        cli = pymonetdb.connect(port=s.dbport,database='db1',autocommit=True)
        cur = cli.cursor()
        cur.execute('select count(*) from table3282;')
        if cur.fetchall()[0][0] != 2097152:
            sys.stderr.write('Expected 2097152')
        cur.close()
        cli.close()
        s.communicate()
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        cli = pymonetdb.connect(port=s.dbport,database='db1',autocommit=True)
        cur = cli.cursor()
        cur.execute('select count(*) from table3282;')
        if cur.fetchall()[0][0] != 2097152:
            sys.stderr.write('Expected 2097152')
        cur.execute('drop table table3282;')
        cur.close()
        cli.close()
        s.communicate()
