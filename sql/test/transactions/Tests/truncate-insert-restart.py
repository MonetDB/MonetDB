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
        cur.execute("CREATE TABLE foo(i) AS VALUES (10), (20), (30);")
        cur.close()
        cli.close()
        s.communicate()
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        # client 1 is the first to connect and initializes a long running transaction
        cli_1 = pymonetdb.connect(port=s.dbport,database='db1',autocommit=True)
        cur = cli_1.cursor()
        cur.execute('START TRANSACTION;')


        # client 2 starts in autocommit mode after client 1 and truncates and repopulates table foo
        cli_2 = pymonetdb.connect(port=s.dbport,database='db1',autocommit=True)
        cur = cli_2.cursor()
        cur.execute('TRUNCATE foo;')
        cur.execute('INSERT INTO foo VALUES (40);')
        cur.close()
        cli_1.close()
        cli_2.close()
        s.communicate()
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        cli = pymonetdb.connect(port=s.dbport,database='db1',autocommit=True)
        cur = cli.cursor()
        """
        cur.execute('''
        select * from sys.storage('sys', 'foo');
        select count(*) from foo;
        select count(i) from foo;
        select max(i) from foo;
        ''')
        """
        cur.execute("select count(*) = count(i) from foo;")
        if cur.fetchall()[0][0] != True:
            sys.stderr.write('Expected table size and column size to be equal')
        cur.close()
        cli.close()
        s.communicate()
