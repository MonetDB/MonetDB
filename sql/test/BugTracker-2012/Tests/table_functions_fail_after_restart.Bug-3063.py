import os, sys, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db'))

    with process.server(mapiport='0', dbname='db',
                        dbfarm=os.path.join(farm_dir, 'db'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as dproc:
        client1 = pymonetdb.connect(database='db', port=dproc.dbport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute("""
        CREATE schema ft;
        CREATE FUNCTION ft.func() RETURNS TABLE (sch varchar(100)) RETURN TABLE (SELECT '1');
        """)
        cur1.execute("select * from ft.func() as ftf;")
        if cur1.fetchall() != [('1',)]:
            sys.stderr.write('Expected [(\'1\',)]')
        cur1.close()
        client1.close()

        dproc.communicate()

    with process.server(mapiport='0', dbname='db',
                        dbfarm=os.path.join(farm_dir, 'db'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as dproc:
        client1 = pymonetdb.connect(database='db', port=dproc.dbport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute("select * from ft.func() as ftf;")
        if cur1.fetchall() != [('1',)]:
            sys.stderr.write('Expected [(\'1\',)]')
        cur1.execute("""
        drop function ft.func;
        drop schema ft;
        """)
        cur1.close()
        client1.close()

        dproc.communicate()
